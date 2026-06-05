<#
.SYNOPSIS
    Creates a Fabric Lakehouse shortcut from the dicom-output ADLS Gen2 container
    into the HDS Bronze Lakehouse.

.DESCRIPTION
    This script:
    1. Resolves the Fabric workspace identity (service principal) from the workspace name
    2. Ensures the workspace identity has Storage Blob Data Contributor on the storage account
    3. Applies ADLS Gen2 ACLs (access + default) on the dicom-output container recursively
    4. Creates the /Files/Ingest/Imaging/DICOM folder path in the Bronze Lakehouse
    5. Creates a Fabric cloud connection to the ADLS Gen2 storage account
    6. Creates a shortcut named DICOM-HDS pointing to the dicom-output ADLS Gen2 container
       (the shortcut itself IS the DICOM-HDS folder — matches the FHIR-HDS pattern for FHIR data)

.PARAMETER FabricWorkspaceName
    The Fabric workspace name. Default: "med-device-rti-hds"

.PARAMETER ResourceGroupName
    Azure resource group containing the FHIR infrastructure. Default: "rg-medtech-rti-fhir"

.PARAMETER BronzeLakehouseName
    Name of the Bronze Lakehouse in the Fabric workspace. Default: "healthcare1_msft_bronze"

.PARAMETER DicomContainerName
    Name of the blob container with DICOM files. Default: "dicom-output"

.PARAMETER ShortcutName
    Name of the shortcut inside the lakehouse. Default: "DICOM-HDS"
    The shortcut itself becomes the DICOM-HDS folder (like FHIR-HDS for FHIR data).

.PARAMETER ShortcutFolderPath
    The lakehouse folder path (under /Files/) where the shortcut is created.
    Default: "Files/Ingest/Imaging/DICOM"

.EXAMPLE
    .\storage-access-trusted-workspace.ps1 -FabricWorkspaceName "med-device-rti-hds"
#>

# Requires Az.Accounts, Az.Resources, Az.Storage (loaded in Step 0)
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$FabricWorkspaceName,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [string]$BronzeLakehouseName = "healthcare1_msft_bronze",
    [string]$DicomContainerName = "dicom-output",
    [string]$ShortcutName = "DICOM-HDS",
    [string]$ShortcutFolderPath = "Files/Ingest/Imaging/DICOM",

    [string]$ImagingPipelineName = "healthcare1_msft_imaging_with_clinical_foundation_ingestion",
    [string]$ClinicalPipelineName = "healthcare1_msft_clinical_data_foundation_ingestion",
    [string]$OmopPipelineName = "healthcare1_msft_omop_analytics"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$VerbosePreference = 'Continue'
$InformationPreference = 'Continue'

$FabricManagementEndpoint = 'https://api.fabric.microsoft.com'
$OneLakeEndpoint = 'https://onelake.dfs.fabric.microsoft.com'

# ═══════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════

function Write-Log {
    param(
        [Parameter(Mandatory)][string]$Message,
        [ValidateSet('INFO','WARN','ERROR','DEBUG')][string]$Level = 'INFO'
    )
    $ts = Get-Date -Format 'u'
    switch ($Level) {
        'INFO'  { Write-Information "[$ts][INFO]  $Message" }
        'WARN'  { Write-Warning     "[$ts][WARN]  $Message" }
        'ERROR' { Write-Error       "[$ts][ERROR] $Message" }
        'DEBUG' { Write-Verbose     "[$ts][DEBUG] $Message" }
    }
}

# ═══════════════════════════════════════════════════════════════════════
# TOKEN HELPERS  (adapted from hds-dicom-infra.ps1)
# ═══════════════════════════════════════════════════════════════════════

function Convert-SecureStringToPlainText {
    param([Parameter(Mandatory)][Security.SecureString]$SecureString)
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureString)
    try   { [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

function Resolve-TokenValue {
    param([Parameter(Mandatory)]$TokenResponse)
    $tokenProp = $TokenResponse.PSObject.Properties['Token']
    if (-not $tokenProp -or $null -eq $tokenProp.Value) {
        throw 'Token response did not include a usable token value.'
    }
    if ($tokenProp.Value -is [Security.SecureString]) {
        return Convert-SecureStringToPlainText -SecureString $tokenProp.Value
    }
    return [string]$tokenProp.Value
}

$script:AccessTokenCache = @{}
function Get-CachedTokenValue {
    param(
        [Parameter(Mandatory)][string]$Key,
        [string]$ResourceUrl = '',
        [string]$ResourceTypeName = ''
    )
    $cached = $script:AccessTokenCache[$Key]
    if ($cached -and $cached.ExpiresOn -gt (Get-Date).AddMinutes(5)) { return $cached.Token }
    if ($ResourceTypeName) {
        $resp = Get-AzAccessToken -ResourceTypeName $ResourceTypeName -ErrorAction Stop
    } else {
        $resp = Get-AzAccessToken -ResourceUrl $ResourceUrl -ErrorAction Stop
    }
    $token = Resolve-TokenValue -TokenResponse $resp
    $script:AccessTokenCache[$Key] = @{ Token = $token; ExpiresOn = $resp.ExpiresOn }
    return $token
}

function Get-OneLakeAccessToken {
    Write-Log 'Acquiring OneLake (storage) access token...' 'INFO'
    $token = Get-CachedTokenValue -Key 'storage' -ResourceTypeName Storage
    Write-Log 'OneLake access token acquired.' 'INFO'
    return $token
}

function Get-FabricApiAccessToken {
    Write-Log 'Acquiring Fabric API access token...' 'INFO'
    $token = Get-CachedTokenValue -Key 'fabric' -ResourceUrl $FabricManagementEndpoint
    Write-Log 'Fabric API access token acquired.' 'INFO'
    return $token
}

function Get-FabricApiHeaders {
    param([Parameter(Mandatory)][string]$AccessToken)
    return @{ Authorization = "Bearer $AccessToken"; 'Content-Type' = 'application/json' }
}

# ═══════════════════════════════════════════════════════════════════════
# FABRIC API HELPERS  (adapted from hds-dicom-infra.ps1)
# ═══════════════════════════════════════════════════════════════════════

function Invoke-FabricApiRequest {
    param(
        [Parameter(Mandatory)][ValidateSet('Get','Post','Put','Delete','Patch','Head')][string]$Method,
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][hashtable]$Headers,
        [object]$Body,
        [string]$Description = ''
    )
    Write-Log "FABRIC API: $Method $Uri ($Description)" 'INFO'

    $invokeParams = @{ Method = $Method; Uri = $Uri; Headers = $Headers; ErrorAction = 'Stop' }
    if ($null -ne $Body) {
        $json = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 10 }
        $invokeParams['Body'] = $json
        $invokeParams['ContentType'] = 'application/json'
        Write-Log "  Body: $json" 'DEBUG'
    }
    $cmd = Get-Command Invoke-WebRequest
    if ($cmd.Parameters.ContainsKey('SkipHttpErrorCheck')) { $invokeParams['SkipHttpErrorCheck'] = $true }

    $raw = Invoke-WebRequest @invokeParams
    $sc = [int]$raw.StatusCode
    $content = [string]$raw.Content

    if ($sc -lt 200 -or $sc -ge 300) {
        $msg = "FABRIC API $Method $Uri returned $sc. Body: $content"
        Write-Log $msg 'ERROR'
        throw [System.Net.Http.HttpRequestException]::new($msg)
    }
    Write-Log "  Response: $sc" 'INFO'

    $parsed = $null
    if (-not [string]::IsNullOrWhiteSpace($content)) {
        try { $parsed = $content | ConvertFrom-Json -Depth 50 } catch { $parsed = $content }
    }
    return [pscustomobject]@{ Response = $parsed; StatusCode = $sc; Headers = $raw.Headers; RawContent = $content }
}

# ═══════════════════════════════════════════════════════════════════════
# ONELAKE DIRECTORY HELPERS  (from hds-dicom-infra.ps1)
# ═══════════════════════════════════════════════════════════════════════

function Test-OneLakeDirectoryExists {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string[]]$PathSegments,
        [Parameter(Mandatory)][string]$AccessToken
    )
    $rel = ($PathSegments | ForEach-Object { [Uri]::EscapeDataString($_) }) -join '/'
    $uri = "$OneLakeEndpoint/$WorkspaceId/$LakehouseId/Files/$rel"
    $headers = @{ Authorization = "Bearer $AccessToken"; 'x-ms-version' = '2021-06-08'; 'x-ms-date' = (Get-Date -Format 'R') }
    try {
        Invoke-RestMethod -Method Head -Uri $uri -Headers $headers -TimeoutSec 30 -ErrorAction Stop | Out-Null
        Write-Log "  Directory exists: /$rel" 'DEBUG'
        return $true
    } catch {
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode.value__ -eq 404) {
            Write-Log "  Directory not found: /$rel" 'DEBUG'
            return $false
        }
        throw
    }
}

function New-OneLakeDirectory {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string[]]$PathSegments,
        [Parameter(Mandatory)][string]$AccessToken
    )
    $rel = ($PathSegments | ForEach-Object { [Uri]::EscapeDataString($_) }) -join '/'
    $uri = "$OneLakeEndpoint/$WorkspaceId/$LakehouseId/Files/$rel`?resource=directory"
    $headers = @{
        Authorization = "Bearer $AccessToken"
        'x-ms-version' = '2021-06-08'
        'x-ms-date'    = (Get-Date -Format 'R')
        'Content-Length' = '0'
    }
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            Invoke-RestMethod -Method Put -Uri $uri -Headers $headers -TimeoutSec 60 -ErrorAction Stop
            Write-Log "  Created directory: /$rel" 'INFO'
            return
        } catch {
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode.value__ -eq 409) {
                Write-Log "  Directory already exists: /$rel" 'DEBUG'
                return
            }
            if ($attempt -ge 3) { throw }
            Write-Log "  Retry $attempt for /$rel ..." 'WARN'
            Start-Sleep -Seconds ([math]::Pow(2, $attempt))
        }
    }
}

function New-LakehouseDirectoryPath {
    param(
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string[]]$PathSegments,
        [Parameter(Mandatory)][string]$AccessToken
    )
    for ($i = 0; $i -lt $PathSegments.Count; $i++) {
        $current = $PathSegments[0..$i]
        if (-not (Test-OneLakeDirectoryExists -WorkspaceId $WorkspaceId -LakehouseId $LakehouseId -PathSegments $current -AccessToken $AccessToken)) {
            New-OneLakeDirectory -WorkspaceId $WorkspaceId -LakehouseId $LakehouseId -PathSegments $current -AccessToken $AccessToken
        }
    }
}

# ═══════════════════════════════════════════════════════════════════════
# RBAC + ACL HELPERS  (from hds-dicom-infra.ps1)
# ═══════════════════════════════════════════════════════════════════════

function Ensure-RoleAssignment {
    param(
        [Parameter(Mandatory)][string]$Scope,
        [Parameter(Mandatory)][string]$PrincipalId,
        [Parameter(Mandatory)][string]$RoleDefinitionName,
        [Parameter(Mandatory)][string]$PrincipalType,
        [string]$Description = ''
    )
    $existing = Get-AzRoleAssignment -Scope $Scope -ObjectId $PrincipalId -RoleDefinitionName $RoleDefinitionName -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Log "  RBAC '$RoleDefinitionName' already assigned to $Description on scope." 'INFO'
        return $false  # no new assignment
    } else {
        Write-Log "  Assigning '$RoleDefinitionName' to $Description ..." 'INFO'
        try {
            New-AzRoleAssignment -Scope $Scope -ObjectId $PrincipalId -RoleDefinitionName $RoleDefinitionName -ObjectType $PrincipalType -ErrorAction Stop | Out-Null
            Write-Log "  RBAC assigned successfully." 'INFO'
            return $true  # new assignment
        } catch {
            if ($_.Exception.Message -match 'Conflict|RoleAssignmentExists|already exists') {
                Write-Log "  RBAC already exists (race condition)." 'DEBUG'
                return $false
            } else { throw }
        }
    }
}

function Set-AdlsContainerAcl {
    param(
        [Parameter(Mandatory)][string]$StorageAccountName,
        [Parameter(Mandatory)][string]$ResourceGroupName,
        [Parameter(Mandatory)][string]$ContainerName,
        [Parameter(Mandatory)][string]$PrincipalId,
        [ValidateSet('user','group','sp','other')][string]$PrincipalType = 'sp',
        [string]$Permissions = 'rwx'
    )
    $context = (Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName).Context
    $aclType = if ($PrincipalType -eq 'group') { 'group' } else { 'user' }

    Write-Log "  Setting ACL on container '$ContainerName' for principal '$PrincipalId' (${aclType}:${Permissions})..." 'INFO'

    $fs = Get-AzDataLakeGen2Item -Context $context -FileSystem $ContainerName -ErrorAction Stop
    $currentAcl = $fs.ACL

    $accessPattern  = "^${aclType}:${PrincipalId}:"
    $defaultPattern = "^default:${aclType}:${PrincipalId}:"
    $hasAccess  = $currentAcl | Where-Object { $_.ToString() -match $accessPattern }
    $hasDefault = $currentAcl | Where-Object { $_.ToString() -match $defaultPattern }

    if ($hasAccess -and $hasDefault) {
        Write-Log "  ACL entries already exist for principal '$PrincipalId' on '$ContainerName'." 'INFO'
        return
    }

    $acl = Set-AzDataLakeGen2ItemAclObject -AccessControlType $aclType -EntityId $PrincipalId -Permission $Permissions
    $acl = Set-AzDataLakeGen2ItemAclObject -AccessControlType $aclType -EntityId $PrincipalId -Permission $Permissions -DefaultScope -InputObject $acl

    Write-Log "  Applying ACL recursively on container '$ContainerName'..." 'INFO'
    Update-AzDataLakeGen2AclRecursive -Context $context -FileSystem $ContainerName -Acl $acl -ErrorAction Stop | Out-Null
    Write-Log "  ACL applied recursively." 'INFO'
}

# ═══════════════════════════════════════════════════════════════════════
# FABRIC CONNECTION HELPERS  (from hds-dicom-infra.ps1)
# ═══════════════════════════════════════════════════════════════════════

function Get-FabricConnectionByDisplayName {
    param(
        [Parameter(Mandatory)][string]$AccessToken,
        [Parameter(Mandatory)][string]$DisplayName
    )
    $headers = Get-FabricApiHeaders -AccessToken $AccessToken
    $uri = "$FabricManagementEndpoint/v1/connections"
    try {
        $result = Invoke-FabricApiRequest -Method Get -Uri $uri -Headers $headers -Description 'List connections'
    } catch {
        Write-Log "  Could not list connections: $($_.Exception.Message)" 'WARN'
        return $null
    }
    $items = @()
    if ($result.Response.PSObject.Properties['value']) { $items = @($result.Response.value) }
    return $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
}

function New-FabricAdlsConnection {
    param(
        [Parameter(Mandatory)][string]$AccessToken,
        [Parameter(Mandatory)][string]$DisplayName,
        [Parameter(Mandatory)][string]$StorageAccountName,
        [Parameter(Mandatory)][string]$ContainerName
    )

    # Check for existing connection
    $existing = Get-FabricConnectionByDisplayName -AccessToken $AccessToken -DisplayName $DisplayName
    if ($existing -and $existing.PSObject.Properties['id']) {
        Write-Log "  Reusing existing connection '$DisplayName' (ID: $($existing.id))." 'INFO'
        return [string]$existing.id
    }

    $dfsHost = "$StorageAccountName.dfs.core.windows.net"
    $dfsUrl  = "https://$dfsHost"

    # Discover supported ADLS connection type
    $headers = Get-FabricApiHeaders -AccessToken $AccessToken
    $typesResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/connections/supportedConnectionTypes" -Headers $headers -Description 'List supported connection types'
    $entries = @()
    if ($typesResult.Response.PSObject.Properties['value']) { $entries = @($typesResult.Response.value) }

    $adlsMeta = $entries | Where-Object {
        $_.type -match 'AdlsGen2|AzureDataLakeStorage' -and $_.supportedCredentialTypes -contains 'WorkspaceIdentity'
    } | Select-Object -First 1

    $connType = if ($adlsMeta) { $adlsMeta.type } else { 'AdlsGen2' }
    $methodName = $connType
    $encOption = 'NotEncrypted'
    $parameterObjects = @()

    if ($adlsMeta -and $adlsMeta.PSObject.Properties['supportedConnectionEncryptionTypes']) {
        $sup = @($adlsMeta.supportedConnectionEncryptionTypes)
        if ($sup -contains 'Encrypted') { $encOption = 'Encrypted' } elseif ($sup.Count -gt 0) { $encOption = $sup[0] }
    }

    if ($adlsMeta -and $adlsMeta.PSObject.Properties['creationMethods']) {
        $method = $adlsMeta.creationMethods | Select-Object -First 1
        if ($method.PSObject.Properties['name']) { $methodName = $method.name }
        if ($method.PSObject.Properties['parameters']) {
            foreach ($p in $method.parameters) {
                $pn = [string]$p.name
                $cn = ($pn -replace '[^a-zA-Z0-9]','').ToLowerInvariant()
                $val = $null
                if     ($cn -match 'server|host')                     { $val = $dfsHost }
                elseif ($cn -match 'account|endpoint|url|location')   { $val = $dfsUrl }
                elseif ($cn -match 'filesystem|container|root')       { $val = $ContainerName }
                elseif ($cn -eq 'path' -or $cn -match 'fullpath')     { $val = $ContainerName }
                elseif ($cn -match 'subpath|relativepath|folder|dir') { $val = '' }
                if ($p.required -and [string]::IsNullOrWhiteSpace($val)) {
                    if ($cn -match 'container|root|filesystem|path') { $val = $ContainerName }
                    else { throw "Cannot map required param '$pn' for ADLS connection." }
                }
                if (-not [string]::IsNullOrWhiteSpace($val)) {
                    $parameterObjects += @{
                        name     = $pn
                        dataType = if ($p.PSObject.Properties['dataType']) { $p.dataType } else { 'Text' }
                        value    = $val
                    }
                }
            }
        }
    }

    if ($parameterObjects.Count -eq 0) {
        $parameterObjects = @(
            @{ name = 'server'; dataType = 'Text'; value = $dfsHost }
            @{ name = 'path';   dataType = 'Text'; value = $ContainerName }
        )
    }

    $body = @{
        connectivityType  = 'ShareableCloud'
        displayName       = $DisplayName
        privacyLevel      = 'Organizational'
        connectionDetails = @{
            type           = $connType
            creationMethod = $methodName
            parameters     = $parameterObjects
        }
        credentialDetails = @{
            singleSignOnType     = 'None'
            connectionEncryption = $encOption
            skipTestConnection   = $false
            credentials          = @{ credentialType = 'WorkspaceIdentity' }
        }
    }

    Write-Log "  Creating ADLS connection '$DisplayName' (type=$connType, method=$methodName)..." 'INFO'

    $uri = "$FabricManagementEndpoint/v1/connections"
    try {
        $result = Invoke-FabricApiRequest -Method Post -Uri $uri -Headers $headers -Body $body -Description "Create ADLS connection '$DisplayName'"
    } catch {
        if ($_.Exception.Message -match '409|DuplicateConnectionName') {
            Write-Log "  Connection '$DisplayName' already exists (409). Looking up..." 'WARN'
            $retry = Get-FabricConnectionByDisplayName -AccessToken $AccessToken -DisplayName $DisplayName
            if ($retry -and $retry.PSObject.Properties['id']) { return [string]$retry.id }
        }
        throw
    }

    if ($result.Response -and $result.Response.PSObject.Properties['id']) {
        $cid = [string]$result.Response.id
        Write-Log "  Connection created: $cid" 'INFO'
        return $cid
    }
    throw "Connection response did not include an ID for '$DisplayName'."
}

# ═══════════════════════════════════════════════════════════════════════
# SHORTCUT HELPERS  (from hds-dicom-infra.ps1)
# ═══════════════════════════════════════════════════════════════════════

function Get-FabricShortcutByName {
    param(
        [Parameter(Mandatory)][string]$AccessToken,
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$LakehouseId,
        [Parameter(Mandatory)][string]$ShortcutName,
        [Parameter(Mandatory)][string]$ShortcutPath
    )
    $headers = Get-FabricApiHeaders -AccessToken $AccessToken
    $uri = "$FabricManagementEndpoint/v1/workspaces/$WorkspaceId/items/$LakehouseId/shortcuts"
    try {
        $result = Invoke-FabricApiRequest -Method Get -Uri $uri -Headers $headers -Description "List shortcuts"
    } catch {
        Write-Log "  Could not list shortcuts: $($_.Exception.Message)" 'WARN'
        return $null
    }
    $items = @()
    if ($result.Response.PSObject.Properties['value']) { $items = @($result.Response.value) }
    return $items | Where-Object { $_.name -eq $ShortcutName -and $_.path -eq $ShortcutPath } | Select-Object -First 1
}

# ═══════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  DICOM ADLS Gen2 → Fabric Lakehouse Shortcut Deployment" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Workspace:    $FabricWorkspaceName" -ForegroundColor White
Write-Host "  Resource Group: $ResourceGroupName" -ForegroundColor White
Write-Host "  Lakehouse:    $BronzeLakehouseName" -ForegroundColor White
Write-Host "  Container:    $DicomContainerName" -ForegroundColor White
Write-Host "  Shortcut Path: $ShortcutFolderPath/$ShortcutName" -ForegroundColor White
Write-Host ""

# ── Step tracking ──
$overallTimer = [System.Diagnostics.Stopwatch]::StartNew()
$stepResults = @()

function Record-Step {
    param([string]$Name, [string]$Status, [double]$Seconds)
    $script:stepResults += [pscustomobject]@{
        Step     = $Name
        Status   = $Status
        Duration = if ($Seconds -ge 60) { "{0:N1} min" -f ($Seconds / 60) } else { "{0:N0} sec" -f $Seconds }
    }
}

# ── Step 0: Import modules ──
foreach ($mod in @('Az.Accounts','Az.Storage')) {
    if (-not (Get-Module -Name $mod)) { Import-Module $mod -ErrorAction Stop }
}

# ── Step 1: Resolve storage account from deployment outputs ──
$step1Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 1: Resolving infrastructure from deployment outputs ───' 'INFO'

$fhirOutputs = az deployment group show --resource-group $ResourceGroupName --name fhir-infra --query properties.outputs 2>$null
$storageAccountName = $null
if ($LASTEXITCODE -eq 0 -and $fhirOutputs) {
    $fhirJson = $fhirOutputs | ConvertFrom-Json
    $storageAccountName = $fhirJson.storageAccountName.value
}

# Fallback: if deployment failed (e.g. RoleAssignmentExists) but resources exist, find storage account directly
if (-not $storageAccountName) {
    Write-Log "  Deployment outputs not available — searching for storage account in resource group..." 'WARN'
    $storageAccountName = (az storage account list -g $ResourceGroupName `
        --query "[?starts_with(name,'stfhir')].name" -o tsv 2>$null) | Select-Object -Last 1
    if ($storageAccountName) {
        # Sanitize: lowercase, alphanumeric only, max 24 chars
        $storageAccountName = ($storageAccountName.Trim().ToLower() -replace '[^a-z0-9]', '')
        if ($storageAccountName.Length -gt 24) { $storageAccountName = $storageAccountName.Substring(0, 24) }
    }
    if (-not $storageAccountName -or $storageAccountName.Length -lt 3) {
        throw "Cannot find FHIR storage account in resource group '$ResourceGroupName'. Ensure deploy-fhir.ps1 has been run."
    }
}

Write-Log "  Storage Account: $storageAccountName" 'INFO'

# Verify the storage account has HNS enabled (ADLS Gen2)
$storageAccount = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $storageAccountName -ErrorAction Stop
if (-not $storageAccount.EnableHierarchicalNamespace) {
    throw "Storage account '$storageAccountName' does not have HNS (ADLS Gen2) enabled."
}
Write-Log "  HNS (ADLS Gen2) confirmed." 'INFO'

# Verify dicom-output container exists
$ctx = $storageAccount.Context
$container = Get-AzStorageContainer -Name $DicomContainerName -Context $ctx -ErrorAction SilentlyContinue
if (-not $container) {
    throw "Container '$DicomContainerName' not found in storage account '$storageAccountName'."
}
$blobCount = (Get-AzStorageBlob -Container $DicomContainerName -Context $ctx -MaxCount 5 | Measure-Object).Count
Write-Log "  Container '$DicomContainerName' exists with blobs (sampled $blobCount)." 'INFO'
$step1Timer.Stop()
Record-Step -Name 'Resolve Infrastructure' -Status 'OK' -Seconds $step1Timer.Elapsed.TotalSeconds

# ── Step 2: Resolve Fabric workspace identity ──
$step2Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 2: Resolving Fabric workspace identity ───' 'INFO'

# Get workspace identity from Fabric API (preferred) with az ad fallback
$workspacePrincipalId = $null
$workspaceAppId = $null

# First, try Fabric API to get workspace identity
try {
    $earlyFabToken = Get-FabricApiAccessToken
    $earlyFabHeaders = Get-FabricApiHeaders -AccessToken $earlyFabToken

    # Try provisionIdentity (returns identity if already exists or creates one)
    try {
        $identityResult = Invoke-FabricApiRequest -Method Post `
            -Uri "$FabricManagementEndpoint/v1/workspaces" `
            -Headers $earlyFabHeaders -Description 'List workspaces for identity'

        # Actually, let's get workspace list first to find our workspace ID, then query identity
        $wsListResult = Invoke-FabricApiRequest -Method Get `
            -Uri "$FabricManagementEndpoint/v1/workspaces" `
            -Headers $earlyFabHeaders -Description 'List workspaces'
        $targetWs = $wsListResult.Response.value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
        if ($targetWs) {
            $earlyWsId = $targetWs.id
            # Try to provision identity (will return existing or create new)
            try {
                $idResult = Invoke-FabricApiRequest -Method Post `
                    -Uri "$FabricManagementEndpoint/v1/workspaces/$earlyWsId/provisionIdentity" `
                    -Headers $earlyFabHeaders -Description 'Provision workspace identity'
                if ($idResult.Response) {
                    $workspacePrincipalId = $idResult.Response.servicePrincipalId
                    $workspaceAppId = $idResult.Response.applicationId
                }
            } catch {
                # Identity already exists — try getting workspace details
                try {
                    $wsDetailResult = Invoke-FabricApiRequest -Method Get `
                        -Uri "$FabricManagementEndpoint/v1/workspaces/$earlyWsId" `
                        -Headers $earlyFabHeaders -Description 'Get workspace details'
                    if ($wsDetailResult.Response.identity) {
                        $workspacePrincipalId = $wsDetailResult.Response.identity.servicePrincipalId
                        $workspaceAppId = $wsDetailResult.Response.identity.applicationId
                    }
                } catch {}
            }
        }
    } catch {}
} catch {
    Write-Log "  Could not query Fabric API for workspace identity: $($_.Exception.Message)" 'DEBUG'
}

# Fallback to az ad if Fabric API didn't return the SP ID
if (-not $workspacePrincipalId) {
    Write-Log "  Falling back to az ad sp lookup..." 'INFO'
    $workspaceSP = Get-AzADServicePrincipal -DisplayName $FabricWorkspaceName -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($workspaceSP) {
        $workspacePrincipalId = $workspaceSP.Id
        $workspaceAppId = $workspaceSP.AppId
    }
}

if (-not $workspacePrincipalId) {
    throw "Cannot find service principal for workspace '$FabricWorkspaceName'. Ensure workspace managed identity exists."
}
Write-Log "  Workspace '$FabricWorkspaceName' → Service Principal ID: $workspacePrincipalId" 'INFO'
if ($workspaceAppId) { Write-Log "    App ID: $workspaceAppId" 'INFO' }
$step2Timer.Stop()
Record-Step -Name 'Resolve Workspace Identity' -Status 'OK' -Seconds $step2Timer.Elapsed.TotalSeconds

# ── Step 3: RBAC — Storage Blob Data Contributor on storage account ──
$step3Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 3: Ensuring RBAC role assignment ───' 'INFO'

$rbacChanged = $false

$result1 = Ensure-RoleAssignment -Scope $storageAccount.Id `
    -PrincipalId $workspacePrincipalId `
    -RoleDefinitionName 'Storage Blob Data Contributor' `
    -PrincipalType 'ServicePrincipal' `
    -Description "workspace identity '$FabricWorkspaceName'"
if ($result1) { $rbacChanged = $true }

# Also ensure the current user has Storage Blob Data Owner (required for ACL operations)
$currentUser = (Get-AzContext).Account
Write-Log "  Current user: $($currentUser.Id)" 'INFO'
$currentUserObj = Get-AzADUser -UserPrincipalName $currentUser.Id -ErrorAction SilentlyContinue
if ($currentUserObj) {
    $result2 = Ensure-RoleAssignment -Scope $storageAccount.Id `
        -PrincipalId $currentUserObj.Id `
        -RoleDefinitionName 'Storage Blob Data Owner' `
        -PrincipalType 'User' `
        -Description "current user '$($currentUser.Id)'"
    if ($result2) { $rbacChanged = $true }
}

if ($rbacChanged) {
    Write-Log '  New RBAC assignments detected. Waiting 60 seconds for propagation...' 'INFO'
    Start-Sleep -Seconds 60
} else {
    Write-Log '  All RBAC assignments already in place. Skipping wait.' 'INFO'
}
$step3Timer.Stop()
Record-Step -Name 'RBAC Role Assignment' -Status $(if ($rbacChanged) { 'ASSIGNED' } else { 'SKIPPED' }) -Seconds $step3Timer.Elapsed.TotalSeconds

# ── Step 4: ADLS Gen2 ACLs on dicom-output container ──
$step4Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 4: Setting ADLS Gen2 ACLs on dicom-output container ───' 'INFO'

$aclMaxRetries = 5
$aclSuccess = $false
for ($aclAttempt = 1; $aclAttempt -le $aclMaxRetries; $aclAttempt++) {
    try {
        Set-AdlsContainerAcl -StorageAccountName $storageAccountName `
            -ResourceGroupName $ResourceGroupName `
            -ContainerName $DicomContainerName `
            -PrincipalId $workspacePrincipalId `
            -PrincipalType 'sp' `
            -Permissions 'rwx'
        $aclSuccess = $true
        break
    } catch {
        if ($aclAttempt -lt $aclMaxRetries -and $_.Exception.Message -match '403|AuthorizationPermissionMismatch') {
            Write-Log "  ACL attempt $aclAttempt failed (RBAC not yet propagated). Waiting 60s before retry..." 'WARN'
            Start-Sleep -Seconds 60
        } else {
            throw
        }
    }
}
if (-not $aclSuccess) {
    throw "Failed to set ACLs after $aclMaxRetries attempts."
}

Write-Log '  ACLs set successfully.' 'INFO'
$step4Timer.Stop()
Record-Step -Name 'ADLS Gen2 ACLs' -Status 'OK' -Seconds $step4Timer.Elapsed.TotalSeconds

# ── Step 5: Resolve Fabric workspace + lakehouse IDs ──
$step5Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 5: Resolving Fabric workspace and lakehouse IDs ───' 'INFO'

$fabricToken = Get-FabricApiAccessToken
$fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken

# Resolve workspace ID from name
$wsResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces" -Headers $fabHeaders -Description 'List workspaces'
$workspace = $wsResult.Response.value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
if (-not $workspace) {
    throw "Fabric workspace '$FabricWorkspaceName' not found."
}
$workspaceId = $workspace.id
Write-Log "  Workspace ID: $workspaceId" 'INFO'

# Resolve lakehouse ID from name
$lhResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items?type=Lakehouse" -Headers $fabHeaders -Description 'List lakehouses'
$lakehouse = $lhResult.Response.value | Where-Object { $_.displayName -eq $BronzeLakehouseName } | Select-Object -First 1
if (-not $lakehouse) {
    throw "Lakehouse '$BronzeLakehouseName' not found in workspace '$FabricWorkspaceName'."
}
$lakehouseId = $lakehouse.id
Write-Log "  Lakehouse ID: $lakehouseId" 'INFO'
$step5Timer.Stop()
Record-Step -Name 'Resolve Workspace/Lakehouse' -Status 'OK' -Seconds $step5Timer.Elapsed.TotalSeconds

# ── Step 6: Create folder path in lakehouse ──
$step6Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 6: Creating folder path in lakehouse ───' 'INFO'

$oneLakeToken = Get-OneLakeAccessToken

# Parse the folder path into segments (strip leading "Files/")
$folderParts = $ShortcutFolderPath.TrimStart('/').Split('/', [System.StringSplitOptions]::RemoveEmptyEntries)
if ($folderParts[0] -eq 'Files') { $folderParts = $folderParts[1..($folderParts.Count - 1)] }

Write-Log "  Ensuring path segments: $($folderParts -join ' → ')" 'INFO'
New-LakehouseDirectoryPath -WorkspaceId $workspaceId -LakehouseId $lakehouseId -PathSegments $folderParts -AccessToken $oneLakeToken
Write-Log "  Folder path created/verified." 'INFO'
$step6Timer.Stop()
Record-Step -Name 'Create Lakehouse Folders' -Status 'OK' -Seconds $step6Timer.Elapsed.TotalSeconds

# ── Step 7: Create Fabric ADLS Gen2 connection ──
$step7Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 7: Creating Fabric ADLS Gen2 cloud connection ───' 'INFO'

$connectionDisplayName = "fab-$storageAccountName-dicom-adls-conn"
$connectionId = New-FabricAdlsConnection -AccessToken $fabricToken `
    -DisplayName $connectionDisplayName `
    -StorageAccountName $storageAccountName `
    -ContainerName $DicomContainerName

Write-Log "  Connection ID: $connectionId" 'INFO'
$step7Timer.Stop()
Record-Step -Name 'Create ADLS Connection' -Status 'OK' -Seconds $step7Timer.Elapsed.TotalSeconds

# ── Step 8: Create shortcut ──
$step8Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 8: Creating Fabric lakehouse shortcut ───' 'INFO'

# Refresh OneLake token (may have expired during RBAC/ACL steps)
$oneLakeToken = Get-OneLakeAccessToken

$existingShortcut = Get-FabricShortcutByName -AccessToken $fabricToken `
    -WorkspaceId $workspaceId -LakehouseId $lakehouseId `
    -ShortcutName $ShortcutName -ShortcutPath $ShortcutFolderPath

if ($existingShortcut) {
    Write-Log "  Shortcut '$ShortcutName' already exists at '$ShortcutFolderPath'. Skipping creation." 'INFO'
} else {
    # Remove any existing directory at the shortcut path (a folder blocks shortcut creation)
    try {
        $olPath = "$workspaceId/$lakehouseId/$ShortcutFolderPath/$ShortcutName"
        $olHeaders = @{ Authorization = "Bearer $oneLakeToken" }
        $null = Invoke-WebRequest -Method HEAD -Uri "$OneLakeEndpoint/$olPath`?action=getStatus" -Headers $olHeaders -ErrorAction Stop
        # Directory exists — delete it so the shortcut can be created
        Write-Log "  Removing existing directory '$ShortcutName' at '$ShortcutFolderPath' to create shortcut..." 'WARN'
        $null = Invoke-RestMethod -Method DELETE -Uri "$OneLakeEndpoint/$olPath`?recursive=true" -Headers $olHeaders
        Write-Log "  Removed conflicting directory." 'INFO'
    } catch {
        # No directory to remove — expected
    }

    $dfsUrl = "https://$storageAccountName.dfs.core.windows.net"
    $shortcutBody = @{
        path   = $ShortcutFolderPath
        name   = $ShortcutName
        target = @{
            adlsGen2 = @{
                location     = $dfsUrl
                subpath      = "/$DicomContainerName"
                connectionId = $connectionId
            }
        }
    }

    $uri = "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$lakehouseId/shortcuts?shortcutConflictPolicy=Abort"
    try {
        Invoke-FabricApiRequest -Method Post -Uri $uri -Headers $fabHeaders -Body $shortcutBody -Description "Create shortcut '$ShortcutName'"
        Write-Log "  Shortcut created: $ShortcutFolderPath/$ShortcutName → $dfsUrl/$DicomContainerName" 'INFO'
    } catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -match '409|EntityConflict|shortcut.*already exists') {
            Write-Log "  Shortcut already exists (409 conflict). Continuing." 'WARN'
        } elseif ($errMsg -match 'Unauthorized|denied|Access|400') {
            Write-Log \"\" 'WARN'
            Write-Log \"  ✗ Shortcut creation FAILED: $errMsg\" 'WARN'
            Write-Log \"\" 'WARN'
            Write-Host \"  ┌──────────────────────────────────────────────────────────────┐\" -ForegroundColor Yellow
            Write-Host \"  │  HOW TO FIX: Create the shortcut manually in the Fabric     │\" -ForegroundColor Yellow
            Write-Host \"  │  portal, then re-run this script.                            │\" -ForegroundColor Yellow
            Write-Host \"  └──────────────────────────────────────────────────────────────┘\" -ForegroundColor Yellow
            Write-Host \"\" -ForegroundColor White
            Write-Host \"  Steps:\" -ForegroundColor White
            Write-Host \"    1. Open the Fabric portal: https://app.fabric.microsoft.com\" -ForegroundColor Gray
            Write-Host \"    2. Navigate to workspace '$FabricWorkspaceName'\" -ForegroundColor Gray
            Write-Host \"    3. Open the Bronze Lakehouse\" -ForegroundColor Gray
            Write-Host \"    4. Navigate to Files → $ShortcutFolderPath\" -ForegroundColor Gray
            Write-Host \"    5. Right-click → 'New shortcut' → 'Azure Data Lake Storage Gen2'\" -ForegroundColor Gray
            Write-Host \"    6. Connection URL: $dfsUrl\" -ForegroundColor Cyan
            Write-Host \"    7. Container/subpath: $DicomContainerName\" -ForegroundColor Cyan
            Write-Host \"    8. Shortcut name: $ShortcutName\" -ForegroundColor Cyan
            Write-Host \"    9. Auth: Workspace Identity\" -ForegroundColor Gray
            Write-Host \"\" -ForegroundColor White
            Write-Host \"  Workspace Identity Details:\" -ForegroundColor White
            Write-Host \"    SP Object ID: $workspacePrincipalId\" -ForegroundColor Cyan
            if ($workspaceAppId) { Write-Host \"    App ID:       $workspaceAppId\" -ForegroundColor Cyan }
            Write-Host \"\" -ForegroundColor White
            Write-Host \"  After creating the shortcut, re-run:\" -ForegroundColor White
            Write-Host \"    .\\storage-access-trusted-workspace.ps1 -FabricWorkspaceName '$FabricWorkspaceName'\" -ForegroundColor Cyan
            Write-Host \"\" -ForegroundColor White

            $step8Timer.Stop()
            Record-Step -Name 'Create Lakehouse Shortcut' -Status 'FAILED (manual required)' -Seconds $step8Timer.Elapsed.TotalSeconds
            throw \"Shortcut creation failed — manual step required. See instructions above.\"
        } else { throw }
    }
}
$step8Timer.Stop()
$shortcutStatus = if ($existingShortcut) { 'EXISTS' } else { 'OK' }
Record-Step -Name 'Create Lakehouse Shortcut' -Status $shortcutStatus -Seconds $step8Timer.Elapsed.TotalSeconds

# ── Step 8.5: Ensure scipy is in HDS Environment ──
$step85Timer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 8.5: Ensuring scipy is in HDS Environment ───' 'INFO'
Write-Log '  The HDS flattening notebooks require scipy.' 'INFO'
Write-Log '  Adding scipy==1.11.4 to the Spark environment...' 'INFO'

$fabricToken = Get-FabricApiAccessToken
$fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken

try {
    # Find the HDS environment item
    $envResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items?type=Environment" -Headers $fabHeaders -Description 'List environments'
    $hdsEnv = $envResult.Response.value | Where-Object { $_.displayName -match "healthcare.*environment" }
    if ($hdsEnv -is [array]) { $hdsEnv = $hdsEnv[0] }

    if ($hdsEnv) {
        $envId = $hdsEnv.id
        $envName = $hdsEnv.displayName
        Write-Log "  ✓ Environment: $envName ($envId)" 'INFO'

        # Check published libraries for scipy
        $scipyAlreadyPublished = $false
        try {
            $pubLibsResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId/libraries?beta=False" -Headers $fabHeaders -Description 'Check published libraries for scipy'
            $pubLibs = $pubLibsResult.Response
            $scipyLib = $pubLibs.libraries | Where-Object { $_.name -eq "scipy" }
            if ($scipyLib) {
                $scipyAlreadyPublished = $true
                Write-Log "  ✓ scipy already published (v$($scipyLib.version))" 'INFO'
            }
        } catch {
            Write-Log "  Could not query published libraries. Proceeding to verify staging." 'DEBUG'
        }

        if (-not $scipyAlreadyPublished) {
            # Check environment state — cannot publish if already publishing
            $envMetaResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId" -Headers $fabHeaders -Description 'Get environment details'
            $envState = $envMetaResult.Response.properties.publishDetails.state
            if ($envState -and $envState -ne "Success" -and $envState -ne "Failed" -and $envState -ne "Cancelled") {
                Write-Log "  ⚠ Environment is currently '$envState' — waiting for current publish to finish..." 'WARN'
                $envWaitStart = Get-Date
                while ((New-TimeSpan -Start $envWaitStart).TotalMinutes -lt 15) {
                    Start-Sleep -Seconds 30
                    $envMetaResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId" -Headers $fabHeaders -Description 'Get environment details'
                    $envState = $envMetaResult.Response.properties.publishDetails.state
                    if ($envState -eq "Success" -or $envState -eq "Failed" -or $envState -eq "Cancelled" -or -not $envState) {
                        break
                    }
                    $elapsed = [math]::Round((New-TimeSpan -Start $envWaitStart).TotalMinutes, 1)
                    Write-Log "    Still $envState (${elapsed}m)..." 'INFO'
                }
            }

            # Export current external libraries YAML (to preserve existing)
            $existingYml = ""
            try {
                $existingYml = Invoke-RestMethod -Method GET `
                    -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId/staging/libraries/exportExternalLibraries" `
                    -Headers $fabHeaders
            } catch {
                # No existing external libs — expected for fresh HDS deploy
            }

            # Build updated environment.yml with scipy
            $scipyEntry = "scipy==1.11.4"
            if ($existingYml -and $existingYml -match "scipy") {
                Write-Log "  ✓ scipy already in staging libraries (pending publish)" 'INFO'
            } else {
                if ($existingYml -and $existingYml -match "- pip:") {
                    # Append scipy to existing pip list
                    $newYml = $existingYml.TrimEnd() + "`n      - $scipyEntry`n"
                } else {
                    $newYml = @"
dependencies:
  - pip:
      - $scipyEntry
"@
                }

                Write-Log "  Importing scipy==1.11.4 into staging..." 'INFO'

                # Upload via importExternalLibraries
                $importHeaders = @{
                    "Authorization" = "Bearer $fabricToken"
                    "Content-Type"  = "application/octet-stream"
                }
                $ymlBytes = [System.Text.Encoding]::UTF8.GetBytes($newYml)
                $null = Invoke-RestMethod -Method POST `
                    -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId/staging/libraries/importExternalLibraries" `
                    -Headers $importHeaders `
                    -Body $ymlBytes
                Write-Log "  ✓ scipy==1.11.4 added to staging" 'INFO'
            }

            # Publish the environment
            Write-Log "  Publishing environment (this takes 3-10 min)..." 'INFO'
            $pubResp = Invoke-WebRequest -Method POST `
                -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId/staging/publish?beta=False" `
                -Headers $fabHeaders `
                -UseBasicParsing

            # Poll for publish completion (max 15 min)
            $pubStart = Get-Date
            $maxPubMin = 15
            $pubSuccess = $false
            while ((New-TimeSpan -Start $pubStart).TotalMinutes -lt $maxPubMin) {
                Start-Sleep -Seconds 30
                $elapsed = [math]::Round((New-TimeSpan -Start $pubStart).TotalMinutes, 1)

                try {
                    $envMetaResult = Invoke-FabricApiRequest -Method Get -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/environments/$envId" -Headers $fabHeaders -Description 'Get environment details'
                    $pubState = $envMetaResult.Response.properties.publishDetails.state
                    if ($pubState -eq "Success") {
                        Write-Log "  ✓ Environment published — scipy==1.11.4 is now available" 'INFO'
                        $pubSuccess = $true
                        break
                    } elseif ($pubState -eq "Failed" -or $pubState -eq "Cancelled") {
                        Write-Log "  ✗ Environment publish $pubState" 'WARN'
                        break
                    } else {
                        Write-Log "    Publish status: $pubState (${elapsed}m elapsed)" 'INFO'
                    }
                } catch {
                    Write-Log "    Poll error: $($_.Exception.Message)" 'WARN'
                }
            }

            if (-not $pubSuccess -and (New-TimeSpan -Start $pubStart).TotalMinutes -ge $maxPubMin) {
                Write-Log "  ⚠ Environment still publishing after ${maxPubMin}m — continuing" 'WARN'
                Write-Log "    Check status in Fabric portal → Environments." 'WARN'
            }
        }
        $step85Timer.Stop()
        Record-Step -Name 'Ensure scipy in Spark Env' -Status 'OK' -Seconds $step85Timer.Elapsed.TotalSeconds
    } else {
        Write-Log "  ⚠ HDS Spark environment not found in workspace." 'WARN'
        Write-Log "    Ensure Healthcare Data Foundations is deployed first." 'WARN'
        Write-Log "    Then manually add scipy==1.11.4 to the environment." 'WARN'
        $step85Timer.Stop()
        Record-Step -Name 'Ensure scipy in Spark Env' -Status 'NOT FOUND' -Seconds $step85Timer.Elapsed.TotalSeconds
    }
} catch {
    $envErr = $_.Exception.Message
    try { $envErr = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
    Write-Log "  ✗ Could not update environment: $envErr" 'WARN'
    Write-Log "    Manually add scipy==1.11.4 to the HDS Spark environment." 'WARN'
    $step85Timer.Stop()
    Record-Step -Name 'Ensure scipy in Spark Env' -Status 'FAILED' -Seconds $step85Timer.Elapsed.TotalSeconds
}

# ── Step 9: Run Clinical Ingestion Pipeline, then Imaging Ingestion Pipeline, then OMOP ──
# The clinical pipeline must run first. Once it completes successfully, the imaging pipeline runs.
# OMOP must run after both complete successfully.
$step9Timer = [System.Diagnostics.Stopwatch]::StartNew()
$step9bTimer = [System.Diagnostics.Stopwatch]::StartNew()
Write-Log '─── Step 9: Running HDS Ingestion Pipelines sequentially ───' 'INFO'

# Refresh token
$fabricToken = Get-FabricApiAccessToken
$fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken

# List data pipelines in the workspace
$pipelineResult = Invoke-FabricApiRequest -Method Get `
    -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items?type=DataPipeline" `
    -Headers $fabHeaders -Description 'List data pipelines'

$pipelines = @()
if ($pipelineResult.Response.PSObject.Properties['value']) {
    $pipelines = @($pipelineResult.Response.value)
}

# Find pipelines
$clinicalPipeline = $pipelines | Where-Object { $_.displayName -eq $ClinicalPipelineName } | Select-Object -First 1
$imagingPipeline = $pipelines | Where-Object { $_.displayName -eq $ImagingPipelineName } | Select-Object -First 1

$clinInvoked = $false
$clinicalCompleted = $false
$clinicalFailed = $false

if ($clinicalPipeline) {
    $clinicalId = $clinicalPipeline.id
    Write-Log "  Found '$ClinicalPipelineName' (ID: $clinicalId)" 'INFO'
    $clinRunUri = "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$clinicalId/jobs/Pipeline/instances"
    Write-Log "  Invoking clinical pipeline run..." 'INFO'
    try {
        Invoke-FabricApiRequest -Method Post -Uri $clinRunUri -Headers $fabHeaders -Description "Run pipeline '$ClinicalPipelineName'"
        Write-Log "  ✓ Clinical pipeline invoked successfully." 'INFO'
        $clinInvoked = $true
    } catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -match '409|already running|TooManyRequestsForJobs') {
            Write-Log "  Clinical pipeline is already running or recently invoked — will poll for completion." 'WARN'
            $clinInvoked = $true
        } else {
            Write-Log "  ⚠ Could not invoke clinical pipeline: $errMsg" 'WARN'
        }
    }
} else {
    Write-Log "  Clinical pipeline '$ClinicalPipelineName' not found." 'WARN'
}

if ($clinInvoked) {
    Write-Log "  Waiting for Clinical pipeline to complete (polling every 30s, max 60 min)..." 'INFO'
    $maxPollMin = 60
    $pollStart = Get-Date

    while ((New-TimeSpan -Start $pollStart).TotalMinutes -lt $maxPollMin) {
        if ($clinicalCompleted -or $clinicalFailed) {
            break
        }

        Start-Sleep -Seconds 30
        $pollElapsed = [math]::Round((New-TimeSpan -Start $pollStart).TotalMinutes, 1)

        # Refresh token periodically
        if ([math]::Floor($pollElapsed) % 10 -eq 0 -and $pollElapsed -gt 0) {
            $fabricToken = Get-FabricApiAccessToken
            $fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken
        }

        # Poll Clinical Pipeline
        try {
            $clinJobsResult = Invoke-FabricApiRequest -Method Get `
                -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$clinicalId/jobs/instances?limit=1" `
                -Headers $fabHeaders -Description 'Poll clinical pipeline status'
            $clinLatestJob = $null
            if ($clinJobsResult.Response.PSObject.Properties['value']) {
                $clinLatestJob = $clinJobsResult.Response.value | Select-Object -First 1
            }

            if ($clinLatestJob) {
                $clinJobStatus = $clinLatestJob.status
                Write-Log "  Clinical pipeline status: $clinJobStatus ($pollElapsed min elapsed)" 'INFO'

                if ($clinJobStatus -eq 'Completed') {
                    Write-Log "  ✓ Clinical pipeline completed successfully!" 'INFO'
                    $clinicalCompleted = $true
                    $step9bTimer.Stop()
                } elseif ($clinJobStatus -in @('Failed', 'Cancelled')) {
                    Write-Log "  ✗ Clinical pipeline $clinJobStatus!" 'WARN'
                    $clinicalFailed = $true
                    $step9bTimer.Stop()
                }
            }
        } catch {
            Write-Log "  Poll error for Clinical pipeline: $($_.Exception.Message). Retrying..." 'WARN'
        }
    }

    if ($step9bTimer.IsRunning) { $step9bTimer.Stop() }
    if (-not $clinicalCompleted -and -not $clinicalFailed) {
        Write-Log "  ⚠ Clinical pipeline did not complete within $maxPollMin min." 'WARN'
    }
} else {
    $step9bTimer.Stop()
}

$imgInvoked = $false
$imgCompleted = $false
$imgFailed = $false

if ($clinicalCompleted) {
    Write-Log '─── Step 9b: Running Imaging Ingestion Pipeline ───' 'INFO'
    if ($imagingPipeline) {
        $imagingId = $imagingPipeline.id
        Write-Log "  Found '$ImagingPipelineName' (ID: $imagingId)" 'INFO'
        $imgRunUri = "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$imagingId/jobs/Pipeline/instances"
        Write-Log "  Invoking imaging pipeline run..." 'INFO'
        try {
            Invoke-FabricApiRequest -Method Post -Uri $imgRunUri -Headers $fabHeaders -Description "Run pipeline '$ImagingPipelineName'"
            Write-Log "  ✓ Imaging pipeline invoked successfully." 'INFO'
            $imgInvoked = $true
        } catch {
            $errMsg = $_.Exception.Message
            if ($errMsg -match '409|already running|TooManyRequestsForJobs') {
                Write-Log "  Imaging pipeline is already running or recently invoked — will poll for completion." 'WARN'
                $imgInvoked = $true
            } else {
                Write-Log "  ⚠ Could not invoke imaging pipeline: $errMsg" 'WARN'
            }
        }
    } else {
        Write-Log "  Imaging pipeline '$ImagingPipelineName' not found." 'ERROR'
    }

    if ($imgInvoked) {
        Write-Log "  Waiting for Imaging pipeline to complete (polling every 30s, max 60 min)..." 'INFO'
        $maxPollMin = 60
        $pollStart = Get-Date

        while ((New-TimeSpan -Start $pollStart).TotalMinutes -lt $maxPollMin) {
            if ($imgCompleted -or $imgFailed) {
                break
            }

            Start-Sleep -Seconds 30
            $pollElapsed = [math]::Round((New-TimeSpan -Start $pollStart).TotalMinutes, 1)

            # Refresh token periodically
            if ([math]::Floor($pollElapsed) % 10 -eq 0 -and $pollElapsed -gt 0) {
                $fabricToken = Get-FabricApiAccessToken
                $fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken
            }

            # Poll Imaging Pipeline
            try {
                $jobsResult = Invoke-FabricApiRequest -Method Get `
                    -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$imagingId/jobs/instances?limit=1" `
                    -Headers $fabHeaders -Description 'Poll imaging pipeline status'
                $latestJob = $null
                if ($jobsResult.Response.PSObject.Properties['value']) {
                    $latestJob = $jobsResult.Response.value | Select-Object -First 1
                }

                if ($latestJob) {
                    $jobStatus = $latestJob.status
                    Write-Log "  Imaging pipeline status: $jobStatus ($pollElapsed min elapsed)" 'INFO'

                    if ($jobStatus -eq 'Completed') {
                        Write-Log "  ✓ Imaging pipeline completed successfully!" 'INFO'
                        $imgCompleted = $true
                        $step9Timer.Stop()
                    } elseif ($jobStatus -in @('Failed', 'Cancelled')) {
                        Write-Log "  ✗ Imaging pipeline $jobStatus!" 'WARN'
                        $imgFailed = $true
                        $step9Timer.Stop()
                    }
                }
            } catch {
                Write-Log "  Poll error for Imaging pipeline: $($_.Exception.Message). Retrying..." 'WARN'
            }
        }

        if ($step9Timer.IsRunning) { $step9Timer.Stop() }
        if (-not $imgCompleted -and -not $imgFailed) {
            Write-Log "  ⚠ Imaging pipeline did not complete within $maxPollMin min." 'WARN'
        }
    } else {
        $step9Timer.Stop()
    }
} else {
    Write-Log '─── Step 9b: SKIPPING Imaging Ingestion Pipeline (Clinical pipeline was not completed successfully) ───' 'WARN'
    $step9Timer.Stop()
}

Record-Step -Name 'Clinical Pipeline' -Status $(if ($clinicalCompleted) { 'COMPLETED' } elseif ($clinicalPipeline) { 'FAILED/TIMEOUT/WARN' } else { 'NOT FOUND' }) -Seconds $step9bTimer.Elapsed.TotalSeconds
Record-Step -Name 'Imaging Pipeline' -Status $(if ($imgCompleted) { 'COMPLETED' } elseif (-not $clinicalCompleted) { 'SKIPPED' } else { 'FAILED/TIMEOUT/WARN' }) -Seconds $step9Timer.Elapsed.TotalSeconds

# ── Step 10: Run OMOP Analytics pipeline (MUST run AFTER imaging + clinical pipelines complete) ──
# IMPORTANT: OMOP cannot run in parallel with any other HDS pipeline.
$step10Timer = [System.Diagnostics.Stopwatch]::StartNew()
$omopPipeline = $null  # Initialize to avoid unset variable errors in summary

if (-not $imgCompleted -or -not $clinicalCompleted) {
    Write-Log '─── Step 10: SKIPPING OMOP pipeline (imaging or clinical pipeline did not complete successfully) ───' 'WARN'
    Write-Log "  OMOP cannot run in parallel or if core pipelines are incomplete." 'WARN'
    Write-Log "  Run OMOP manually after both pipelines finish successfully." 'WARN'
    $step10Timer.Stop()
    Record-Step -Name 'OMOP Pipeline' -Status 'SKIPPED (prerequisites incomplete)' -Seconds $step10Timer.Elapsed.TotalSeconds
} else {

Write-Log '─── Step 10: Running OMOP Analytics pipeline ───' 'INFO'
Write-Log "  (Imaging pipeline completed — safe to start OMOP)" 'INFO'

# Refresh token
$fabricToken = Get-FabricApiAccessToken
$fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken

# Re-fetch pipelines in case the list is stale
$pipelineResult2 = Invoke-FabricApiRequest -Method Get `
    -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items?type=DataPipeline" `
    -Headers $fabHeaders -Description 'List data pipelines (for OMOP)'

$pipelines2 = @()
if ($pipelineResult2.Response.PSObject.Properties['value']) {
    $pipelines2 = @($pipelineResult2.Response.value)
}

$omopPipeline = $pipelines2 | Where-Object { $_.displayName -eq $OmopPipelineName } | Select-Object -First 1
if (-not $omopPipeline) {
    Write-Log "  OMOP pipeline '$OmopPipelineName' not found." 'WARN'
    Write-Log "  Available pipelines:" 'INFO'
    foreach ($p in $pipelines2) { Write-Log "    - $($p.displayName) ($($p.id))" 'INFO' }
    Write-Log "  Skipping OMOP pipeline — it may not be deployed yet." 'WARN'
    $step10Timer.Stop()
    Record-Step -Name 'OMOP Pipeline' -Status 'NOT FOUND' -Seconds $step10Timer.Elapsed.TotalSeconds
} else {
    $omopId = $omopPipeline.id
    Write-Log "  Found '$OmopPipelineName' (ID: $omopId)" 'INFO'

    $omopRunUri = "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$omopId/jobs/Pipeline/instances"
    Write-Log "  Invoking OMOP analytics pipeline run..." 'INFO'

    $omopInvoked = $false
    $omopCompleted = $false
    $omopFailed = $false

    try {
        Invoke-FabricApiRequest -Method Post -Uri $omopRunUri -Headers $fabHeaders -Description "Run pipeline '$OmopPipelineName'"
        Write-Log "  OMOP pipeline invoked successfully (202 Accepted)." 'INFO'
        $omopInvoked = $true
    } catch {
        $errMsg = $_.Exception.Message
        if ($errMsg -match '409|already running|TooManyRequestsForJobs') {
            Write-Log "  OMOP pipeline is already running or recently invoked — will poll for completion." 'WARN'
            $omopInvoked = $true
        } else {
            Write-Log "  ⚠ Could not invoke OMOP pipeline: $errMsg" 'WARN'
            $step10Timer.Stop()
            Record-Step -Name 'OMOP Pipeline' -Status 'FAILED' -Seconds $step10Timer.Elapsed.TotalSeconds
            throw
        }
    }

    if ($omopInvoked) {
        Write-Log "  Waiting for OMOP pipeline to complete (polling every 30s, max 60 min)..." 'INFO'
        $maxPollMin = 60
        $pollStart = Get-Date

        while ((New-TimeSpan -Start $pollStart).TotalMinutes -lt $maxPollMin) {
            if ($omopCompleted -or $omopFailed) {
                break
            }

            Start-Sleep -Seconds 30
            $pollElapsed = [math]::Round((New-TimeSpan -Start $pollStart).TotalMinutes, 1)

            # Refresh token periodically
            if ([math]::Floor($pollElapsed) % 10 -eq 0 -and $pollElapsed -gt 0) {
                $fabricToken = Get-FabricApiAccessToken
                $fabHeaders = Get-FabricApiHeaders -AccessToken $fabricToken
            }

            # Poll OMOP Pipeline
            try {
                $jobsResult = Invoke-FabricApiRequest -Method Get `
                    -Uri "$FabricManagementEndpoint/v1/workspaces/$workspaceId/items/$omopId/jobs/instances?limit=1" `
                    -Headers $fabHeaders -Description 'Poll OMOP pipeline status'
                $latestJob = $null
                if ($jobsResult.Response.PSObject.Properties['value']) {
                    $latestJob = $jobsResult.Response.value | Select-Object -First 1
                }

                if ($latestJob) {
                    $jobStatus = $latestJob.status
                    Write-Log "  OMOP pipeline status: $jobStatus ($pollElapsed min elapsed)" 'INFO'

                    if ($jobStatus -eq 'Completed') {
                        Write-Log "  ✓ OMOP pipeline completed successfully!" 'INFO'
                        $omopCompleted = $true
                        $step10Timer.Stop()
                        Record-Step -Name 'OMOP Pipeline' -Status 'COMPLETED' -Seconds $step10Timer.Elapsed.TotalSeconds
                    } elseif ($jobStatus -in @('Failed', 'Cancelled')) {
                        Write-Log "  ✗ OMOP pipeline $jobStatus!" 'WARN'
                        $omopFailed = $true
                        $step10Timer.Stop()
                        Record-Step -Name 'OMOP Pipeline' -Status 'FAILED' -Seconds $step10Timer.Elapsed.TotalSeconds
                    }
                }
            } catch {
                Write-Log "  Poll error for OMOP pipeline: $($_.Exception.Message). Retrying..." 'WARN'
            }
        }

        if ($step10Timer.IsRunning) { $step10Timer.Stop() }
        if (-not $omopCompleted -and -not $omopFailed) {
            Write-Log "  ⚠ OMOP pipeline did not complete within $maxPollMin min." 'WARN'
            Record-Step -Name 'OMOP Pipeline' -Status 'TIMEOUT' -Seconds $step10Timer.Elapsed.TotalSeconds
        }
    }
}
} # end: if ($imgCompleted) — OMOP only runs after imaging completes

# ── Summary ──
$overallTimer.Stop()
$totalSeconds = $overallTimer.Elapsed.TotalSeconds
$totalDisplay = if ($totalSeconds -ge 60) { "{0:N1} min" -f ($totalSeconds / 60) } else { "{0:N0} sec" -f $totalSeconds }

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Step Summary:" -ForegroundColor White
Write-Host "  $('─' * 55)" -ForegroundColor DarkGray

foreach ($r in $stepResults) {
    $icon = if ($r.Status -in @('OK','COMPLETED','INVOKED','SKIPPED','ALREADY RUNNING','ASSIGNED')) { '✓' } else { '✗' }
    $color = if ($icon -eq '✓') { 'Green' } else { 'Yellow' }
    $line = "  $icon  $($r.Step.PadRight(30)) $($r.Status.PadRight(15)) $($r.Duration)"
    Write-Host $line -ForegroundColor $color
}

Write-Host "  $('─' * 55)" -ForegroundColor DarkGray
Write-Host "  Total: $totalDisplay" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Resources:" -ForegroundColor White
Write-Host "    Shortcut:  $ShortcutFolderPath/$ShortcutName" -ForegroundColor DarkGray
Write-Host "    Target:    https://$storageAccountName.dfs.core.windows.net/$DicomContainerName" -ForegroundColor DarkGray
Write-Host "    Lakehouse: $BronzeLakehouseName ($lakehouseId)" -ForegroundColor DarkGray
Write-Host "    Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor DarkGray
Write-Host ""
Write-Host "  The HDS pipeline sequence:" -ForegroundColor Cyan
Write-Host "    1. Imaging with Clinical Foundation pipeline — $(if ($imgCompleted) { 'COMPLETED ✓' } else { 'IN PROGRESS / NOT COMPLETED' })" -ForegroundColor $(if ($imgCompleted) { 'Green' } else { 'Yellow' })
if ($omopPipeline) {
    Write-Host "    2. OMOP Analytics pipeline — $(if ($imgCompleted) { 'INVOKED ✓' } else { 'SKIPPED (waiting for imaging)' })" -ForegroundColor $(if ($imgCompleted) { 'Green' } else { 'Yellow' })
}
Write-Host ""

# Build direct pipeline URLs
$fabricPortalBase = "https://app.fabric.microsoft.com/groups/$workspaceId"
if ($imagingPipeline) {
    Write-Host "  Pipeline URLs:" -ForegroundColor White
    Write-Host "    Imaging Pipeline:  $fabricPortalBase/datapipelines/$imagingId" -ForegroundColor DarkCyan
}
if ($omopPipeline) {
    $omopUrl = "$fabricPortalBase/datapipelines/$omopId"
    Write-Host "    OMOP Pipeline:     $omopUrl" -ForegroundColor DarkCyan
}
Write-Host ""

if (-not $imgCompleted) {
    Write-Host "  ┌──────────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
    Write-Host "  │  NEXT STEP: Launch the OMOP Gold pipeline manually after    │" -ForegroundColor Yellow
    Write-Host "  │  the Imaging pipeline completes.                            │" -ForegroundColor Yellow
    Write-Host "  └──────────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  1. Wait for the Imaging pipeline to finish:" -ForegroundColor White
    if ($imagingPipeline) {
        Write-Host "     $fabricPortalBase/datapipelines/$imagingId" -ForegroundColor Cyan
    }
    Write-Host "  2. Then launch the OMOP Gold pipeline:" -ForegroundColor White
    if ($omopPipeline) {
        Write-Host "     $omopUrl" -ForegroundColor Cyan
    }
    Write-Host ""
} elseif (-not $omopPipeline) {
    Write-Host "  ⚠ OMOP pipeline not found — deploy it via HDS, then run manually." -ForegroundColor Yellow
    Write-Host ""
} else {
    Write-Host "  ┌──────────────────────────────────────────────────────────────┐" -ForegroundColor Green
    if ($omopCompleted) {
        Write-Host "  │  OMOP Gold pipeline completed successfully!                  │" -ForegroundColor Green
    } elseif ($omopFailed) {
        Write-Host "  │  ⚠ OMOP Gold pipeline failed. Check the Fabric portal.      │" -ForegroundColor Red
    } else {
        Write-Host "  │  OMOP Gold pipeline has been launched. Monitor progress:    │" -ForegroundColor Green
    }
    Write-Host "  │  $($omopUrl.PadRight(58))│" -ForegroundColor Green
    Write-Host "  └──────────────────────────────────────────────────────────────┘" -ForegroundColor Green
    Write-Host ""
}
Write-Host "  Monitor pipeline progress in the Fabric portal:" -ForegroundColor DarkGray
Write-Host "  https://app.fabric.microsoft.com" -ForegroundColor DarkGray
Write-Host ""
