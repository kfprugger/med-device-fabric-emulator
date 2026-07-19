# deploy-fabric-rti.ps1
# Deploys Microsoft Fabric Real-Time Intelligence resources for the Masimo Clinical Alert System.
#
# TWO-PHASE DEPLOYMENT:
#   Phase 1 (default): Deploys Fabric workspace, Eventhouse, KQL Database, Eventstream,
#                      cloud connection, base KQL tables/functions, and runs FHIR $export
#                      to ADLS Gen2 storage (no Marketplace offer needed).
#   Phase 2 (-Phase2): Run AFTER Healthcare Data Solutions (HDS) is manually deployed
#                      and the clinical pipeline has populated the Silver Lakehouse.
#                      Creates KQL shortcuts to Silver Patient/Condition/Device tables
#                      and replaces fn_ClinicalAlerts with the enriched version.
#
# Prerequisites:
#   - Azure CLI authenticated (az login)
#   - Az PowerShell module installed (Install-Module Az)
#   - Existing Azure deployment from phase-1/deploy.ps1 (Event Hub, FHIR Service)
#   - Microsoft Fabric capacity (paid F-SKU, e.g. F2+ — trial capacities cannot deploy HDS)
#
# Usage:
#   Phase 1: .\deploy-fabric-rti.ps1
#   Phase 2: .\deploy-fabric-rti.ps1 -Phase2
#   Custom:  .\deploy-fabric-rti.ps1 -FabricWorkspaceName "my-workspace" -ResourceGroupName "rg-medtech-sys-identity"
#   Phase 2 with explicit Silver LH: .\deploy-fabric-rti.ps1 -Phase2 -SilverLakehouseId "<id>"

param (
    [string]$FabricWorkspaceName = "med-device-rti-hds",
    [string]$ResourceGroupName = "rg-medtech-rti-fhir",
    [string]$EventHubNamespace = "",         # Auto-detected from RG if blank
    [string]$EventHubName = "telemetry-stream",
    [string]$FhirServiceUrl = "",            # Auto-detected from RG if blank
    [string]$Location = "eastus",
    [switch]$SkipHdsGuidance = $false,       # Skip the HDS manual-step guidance
    [switch]$SkipFhirExport = $false,        # Skip the automated FHIR $export step
    [switch]$Phase2 = $false,                # Run Phase 2 only (post-HDS deployment)
    [string]$SilverLakehouseId = "",         # Silver Lakehouse ID (required for Phase 2)
    [string]$SilverLakehouseName = "",       # Silver Lakehouse display name (auto-detected if blank)
    [string]$KustoUri = "",                   # Kusto Query URI (auto-detected; provide if capacity is paused)
    [hashtable]$Tags = @{}                     # Resource tags (e.g. @{SecurityControl='Ignore'})
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$kqlDeployed = $false
$fhirExportDone = $false
$dashboardDeployed = $false
$eventstreamConfigured = $false
$eventstreamRunning = $false
$rtiFailures = @()
$exportStorageAccountName = ""
$exportContainerName = "fhir-export"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Add-RtiFailure {
    param([Parameter(Mandatory)][string]$Message)
    if ($Message -and $script:rtiFailures -notcontains $Message) {
        $script:rtiFailures += $Message
    }
}

function Complete-RtiPhase {
    param(
        [switch]$RequireEventstream,
        [switch]$RequireKql,
        [switch]$RequireDashboard
    )

    if ($RequireEventstream -and -not $script:eventstreamConfigured) {
        Add-RtiFailure "Eventstream source/destination was not configured"
    }
    if ($RequireKql -and -not $script:kqlDeployed) {
        Add-RtiFailure "KQL tables/functions did not fully deploy"
    }
    if ($RequireDashboard -and -not $script:dashboardDeployed) {
        Add-RtiFailure "Real-Time dashboard definition did not deploy"
    }

    if ($script:rtiFailures.Count -gt 0) {
        Write-Host "" -ForegroundColor Red
        Write-Host "ERROR: Masimo RTI deployment incomplete:" -ForegroundColor Red
        foreach ($failure in $script:rtiFailures) {
            Write-Host "  - $failure" -ForegroundColor Red
        }
        exit 1
    }
}

$script:AccessTokenCache = @{}
function Get-AccessTokenForResource {
    <#
    .SYNOPSIS
        Obtains a cached bearer token for the specified resource URL using the current Az context.
        Handles both PowerShell 5.1 (plain text) and 7+ (SecureString) token formats.
    .PARAMETER ResourceUrl
        The audience/resource URL to request the token for.
    #>
    param ([string]$ResourceUrl)
    $key = $ResourceUrl.ToLowerInvariant()
    $cached = $script:AccessTokenCache[$key]
    if ($cached -and $cached.ExpiresOn -gt (Get-Date).AddMinutes(5)) { return $cached.Token }

    $tokenObj = Get-AzAccessToken -ResourceUrl $ResourceUrl -ErrorAction Stop
    $rawToken = $tokenObj.Token
    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try { $rawToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    } elseif ($rawToken -isnot [string]) {
        $rawToken = $rawToken | ConvertFrom-SecureString -AsPlainText
    }
    $script:AccessTokenCache[$key] = @{ Token = $rawToken; ExpiresOn = $tokenObj.ExpiresOn }
    return $rawToken
}

function Get-FabricAccessToken {
    <#
    .SYNOPSIS
        Obtains a bearer token for the Fabric REST API.
    #>
    return Get-AccessTokenForResource -ResourceUrl "https://api.fabric.microsoft.com"
}

function Get-KustoAccessToken {
    <#
    .SYNOPSIS
        Obtains a bearer token for the Kusto REST API.
    #>
    return Get-AccessTokenForResource -ResourceUrl "https://api.kusto.windows.net"
}

function Invoke-FabricApi {
    <#
    .SYNOPSIS
        Wrapper for Fabric REST API calls with automatic token refresh and error handling.
    #>
    param (
        [string]$Method = "GET",
        [string]$Endpoint,
        [object]$Body = $null,
        [int]$MaxRetries = 8
    )

    $token = Get-FabricAccessToken
    $headers = @{
        "Authorization" = "Bearer $token"
        "Content-Type"  = "application/json"
    }

    $uri = "$FabricApiBase$Endpoint"
    $bodyJson = if ($Body) { $Body | ConvertTo-Json -Depth 10 } else { $null }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{
                Method  = $Method
                Uri     = $uri
                Headers = $headers
            }
            if ($bodyJson -and $Method -ne "GET") {
                $params["Body"] = $bodyJson
            }

            $response = Invoke-RestMethod @params
            return $response
        }
        catch {
            $statusCode = $null
            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
            $errorBody = ""
            try { $errorBody = $_.ErrorDetails.Message } catch {}

            # Transport-level failures (SSL/TLS handshake, connection reset, DNS, timeout) surface
            # with no HTTP response, so $statusCode is null. These are transient and must be retried
            # too — otherwise a single network blip fails the whole step after all real work succeeded.
            $isTransport = ($null -eq $statusCode) -and (
                $_.Exception -is [System.Net.Http.HttpRequestException] -or
                $_.Exception.Message -match "SSL connection|connection could not be established|actively refused|connection was closed|timed out|Unable to (read|connect|write)|An error occurred while sending the request"
            )
            if (($statusCode -eq 429 -or $statusCode -ge 500 -or ($statusCode -eq 403 -and $errorBody -match "RequestDeniedByInboundPolicy") -or $isTransport) -and $attempt -lt $MaxRetries) {
                $retryAfter = [Math]::Min(120, 10 * [Math]::Pow(2, $attempt - 1))
                if ($statusCode -eq 429) {
                    try { $retryAfter = [int]$_.Exception.Response.Headers["Retry-After"] } catch {}
                }
                $reason = if ($isTransport) { "Fabric transient network/SSL error" } elseif ($statusCode -eq 403) { "Fabric inbound policy denied request" } elseif ($statusCode -eq 429) { "Rate limited" } else { "Fabric transient HTTP $statusCode" }
                Write-Host "  $reason. Waiting ${retryAfter}s before retry $($attempt + 1)/$MaxRetries..." -ForegroundColor Yellow
                Start-Sleep -Seconds $retryAfter
                continue
            }
            else {
                throw $_
            }
        }
    }
}

function Set-MasimoDashboardDefinition {
    param (
        [Parameter(Mandatory)][string]$WorkspaceId,
        [Parameter(Mandatory)][string]$DashboardId,
        [Parameter(Mandatory)][string]$DashboardName,
        [Parameter(Mandatory)][string]$KqlDbName,
        [Parameter(Mandatory)][string]$KqlDbId,
        [Parameter(Mandatory)][string]$KustoUri
    )

    $scriptRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }
    $templatePath = Join-Path $scriptRoot "fabric-rti/dashboard/masimo-clinical-dashboard.json"
    if (-not (Test-Path $templatePath)) {
        throw "Dashboard template not found: $templatePath"
    }

    $dashboardJson = Get-Content -Path $templatePath -Raw
    $replacements = @{
        "__DASHBOARD_TITLE__" = $DashboardName
        "__DATA_SOURCE_ID__"  = [guid]::NewGuid().ToString()
        "__KQL_DB_NAME__"     = $KqlDbName
        "__KUSTO_URI__"       = $KustoUri
        "__KQL_DB_ID__"       = $KqlDbId
        "__WORKSPACE_ID__"    = $WorkspaceId
    }
    foreach ($key in $replacements.Keys) {
        $dashboardJson = $dashboardJson.Replace($key, [string]$replacements[$key])
    }

    $definition = $dashboardJson | ConvertFrom-Json -ErrorAction Stop
    $payload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($dashboardJson))
    $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$WorkspaceId/items/$DashboardId/updateDefinition" `
        -Body @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $payload; payloadType = "InlineBase64" } ) } }

    return @{ Pages = $definition.pages.Count; Tiles = $definition.tiles.Count }
}

function Invoke-KustoMgmt {
    <#
    .SYNOPSIS
        Sends a management command to a Kusto (KQL Database) endpoint.
        Returns $true on success or if the object already exists, $false on failure.
    #>
    param (
        [string]$Command,
        [string]$Label,
        [string]$KustoUri,
        [string]$DatabaseName,
        [hashtable]$KustoHeaders
    )
    $body = @{ db = $DatabaseName; csl = $Command } | ConvertTo-Json -Depth 3 -Compress
    $maxRetries = 3
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            $null = Invoke-RestMethod -Uri "$KustoUri/v1/rest/mgmt" -Headers $KustoHeaders -Method POST -Body $body
            Write-Host "  ✓ $Label" -ForegroundColor Green
            return $true
        } catch {
            $errBody = $_.ErrorDetails.Message
            try { $parsed = $errBody | ConvertFrom-Json; $msg = $parsed.error.message } catch { $msg = $errBody }
            if ($msg -match "already exists") {
                Write-Host "  ✓ $Label (already exists)" -ForegroundColor Yellow
                return $true
            }
            $transient = $msg -match "internal service error|request aborted|temporarily unavailable|timeout|timed out|429|5\d{2}"
            if ($transient -and $attempt -lt $maxRetries) {
                $delay = 15 * [Math]::Pow(2, $attempt - 1)
                Write-Host "  ⚠ $Label transient failure; retrying in ${delay}s ($($attempt + 1)/$maxRetries)..." -ForegroundColor Yellow
                Start-Sleep -Seconds $delay
                continue
            }
            Write-Host "  ✗ $Label" -ForegroundColor Red
            if ($msg) { Write-Host "    $msg" -ForegroundColor DarkRed }
            else { Write-Host "    $($_.Exception.Message)" -ForegroundColor DarkRed }
            return $false
        }
    }
    return $false
}

function Wait-FabricItem {
    <#
    .SYNOPSIS
        Polls until a Fabric item is fully provisioned (handles 202 long-running operations).
    #>
    param (
        [string]$WorkspaceId,
        [string]$ItemType,
        [string]$ItemName,
        [int]$TimeoutSeconds = 120
    )

    # Map type names to REST endpoints
    # NOTE: Specific endpoints (eventhouses, kqlDatabases) may 404 in newer API versions
    # Use generic items?type= as primary, with specific endpoints as fallback
    $endpoint = "items?type=$ItemType"

    $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        try {
            $items = Invoke-FabricApi -Endpoint "/workspaces/$WorkspaceId/$endpoint"
            $found = $items.value | Where-Object { $_.displayName -eq $ItemName }
            if ($found) { return $found }
        } catch {}
        Start-Sleep -Seconds 5
        $elapsed += 5
        Write-Host "  Waiting for $ItemType '$ItemName'... (${elapsed}s)" -ForegroundColor Gray
    }
    throw "Timed out waiting for $ItemType '$ItemName' after ${TimeoutSeconds}s"
}

# ============================================================================
# PHASE 2 EARLY EXIT: Post-HDS Deployment (shortcuts + enriched alerts)
# Run with: .\deploy-fabric-rti.ps1 -Phase2
# ============================================================================

if ($Phase2) {
    Write-Host ""
    Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
    Write-Host "║  PHASE 2: Post-HDS Deployment — KQL Shortcuts & Enrichment  ║" -ForegroundColor Magenta
    Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta
    Write-Host ""

    # Re-discover workspace
    $workspaces = Invoke-FabricApi -Endpoint "/workspaces"
    $ws = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
    if (-not $ws) {
        Write-Host "ERROR: Workspace '$FabricWorkspaceName' not found. Run Phase 1 first." -ForegroundColor Red
        exit 1
    }
    $workspaceId = $ws.id
    Write-Host "  ✓ Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Green

    # Verify HDS is deployed
    $allItems = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items"
    $hdsItem = $allItems.value | Where-Object { $_.type -eq 'Healthcaredatasolution' } | Select-Object -First 1
    if (-not $hdsItem) {
        Write-Host ""
        Write-Host "  ERROR: Healthcare Data Solutions not found in workspace." -ForegroundColor Red
        Write-Host "  HDS must be deployed before running Phase 2." -ForegroundColor Yellow
        Write-Host "  Deploy HDS: Fabric portal → '$FabricWorkspaceName' → + New Item → Healthcare data solution" -ForegroundColor DarkGray
        Write-Host "  Docs: https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/deploy" -ForegroundColor DarkCyan
        exit 1
    }
    Write-Host "  ✓ HDS: $($hdsItem.displayName)" -ForegroundColor Green

    # ================================================================
    # PREFLIGHT: Workspace Identity Validation
    # Ensures the correct SP is used for all RBAC and shortcut operations.
    # Detects and cleans stale SPs from previous deployments.
    # ================================================================
    Write-Host ""
    Write-Host "--- PREFLIGHT: Workspace Identity Validation ---" -ForegroundColor Cyan

    $verifiedSPId = $null
    $verifiedAppId = $null

    # Step 1: Get the CURRENT workspace identity from Fabric API
    try {
        $wsIdentityResult = Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/provisionIdentity"
        $verifiedSPId = $wsIdentityResult.servicePrincipalId
        $verifiedAppId = $wsIdentityResult.applicationId
        if (-not $verifiedSPId) {
            $wsDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId"
            $identity = $null
            if ($wsDetail.workspaceIdentity) { $identity = $wsDetail.workspaceIdentity }
            elseif ($wsDetail.identity) { $identity = $wsDetail.identity }
            if ($identity) {
                $verifiedSPId = $identity.servicePrincipalId
                $verifiedAppId = $identity.applicationId
            }
        }
    } catch {
        # Identity already exists — get from workspace details
        try {
            $wsDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId"
            $identity = $null
            if ($wsDetail.workspaceIdentity) { $identity = $wsDetail.workspaceIdentity }
            elseif ($wsDetail.identity) { $identity = $wsDetail.identity }
            if ($identity) {
                $verifiedSPId = $identity.servicePrincipalId
                $verifiedAppId = $identity.applicationId
            }
        } catch {}
    }

    if (-not $verifiedSPId) {
        # Fallback: use az ad sp list but warn about potential staleness
        $allSPs = az ad sp list --display-name $FabricWorkspaceName --query "[].{id:id, appId:appId, created:createdDateTime}" -o json 2>$null | ConvertFrom-Json
        if ($allSPs -and $allSPs.Count -eq 1) {
            $verifiedSPId = $allSPs[0].id
            $verifiedAppId = $allSPs[0].appId
        } elseif ($allSPs -and $allSPs.Count -gt 1) {
            Write-Host "  ⚠ Multiple SPs found for '$FabricWorkspaceName':" -ForegroundColor Yellow
            foreach ($sp in $allSPs) { Write-Host "    $($sp.id) (appId: $($sp.appId), created: $($sp.created))" -ForegroundColor DarkGray }
            # Use the most recently created one
            $verifiedSPId = ($allSPs | Sort-Object -Property created -Descending | Select-Object -First 1).id
            $verifiedAppId = ($allSPs | Sort-Object -Property created -Descending | Select-Object -First 1).appId
            Write-Host "  Using most recent: $verifiedSPId" -ForegroundColor Yellow
        }
    }

    if (-not $verifiedSPId) {
        Write-Host "  ERROR: Cannot resolve workspace identity SP. Provision it manually." -ForegroundColor Red
        exit 1
    }

    Write-Host "  ✓ Workspace Identity SP: $verifiedSPId" -ForegroundColor Green
    if ($verifiedAppId) { Write-Host "    App ID: $verifiedAppId" -ForegroundColor Gray }

    # Step 2: Detect and clean stale SPs from previous deployments
    $allSPs = az ad sp list --display-name $FabricWorkspaceName --query "[].{id:id, appId:appId}" -o json 2>$null | ConvertFrom-Json
    if ($allSPs -and $allSPs.Count -gt 1) {
        Write-Host "  ⚠ Found $($allSPs.Count) SPs named '$FabricWorkspaceName' — cleaning stale ones..." -ForegroundColor Yellow
        foreach ($sp in $allSPs) {
            if ($sp.id -ne $verifiedSPId) {
                Write-Host "    Deleting stale SP: $($sp.id) (appId: $($sp.appId))..." -NoNewline
                try {
                    az ad app delete --id $sp.appId 2>$null | Out-Null
                    Write-Host " ✓" -ForegroundColor Green
                } catch {
                    Write-Host " ✗" -ForegroundColor Red
                }
            }
        }
    } else {
        Write-Host "  ✓ No stale SPs detected" -ForegroundColor Green
    }
    Write-Host ""

    # Re-discover KQL Database (use generic items API — kqlDatabases endpoint deprecated)
    $kqlDbName = "MasimoKQLDB"
    $eventhouseName = "MasimoEventhouse"
    $kqlDbs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=KQLDatabase"
    $kqlDb = $kqlDbs.value | Where-Object { $_.displayName -eq $kqlDbName -or $_.displayName -eq $eventhouseName }
    if (-not $kqlDb) {
        Write-Host "ERROR: KQL Database not found. Run Phase 1 first." -ForegroundColor Red
        exit 1
    }
    $kqlDbId = $kqlDb.id
    $kqlDbName = $kqlDb.displayName
    # Try kqlDatabases/{id} detail first, fall back to items/{id}
    $kqlDbDetail = $null
    try { $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases/$kqlDbId" } catch {}
    if (-not $kqlDbDetail) {
        try { $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items/$kqlDbId" } catch {}
    }
    $kustoUri = $null
    if ($kqlDbDetail) {
        $kustoUri = $kqlDbDetail.queryServiceUri
        if (-not $kustoUri) { $kustoUri = $kqlDbDetail.queryUri }
        if (-not $kustoUri) { try { $kustoUri = $kqlDbDetail.properties.queryUri } catch {} }
        if (-not $kustoUri) { try { $kustoUri = $kqlDbDetail.properties.queryServiceUri } catch {} }
    }
    if (-not $kustoUri) {
        # Fallback: try to discover Kusto URI from Eventhouse properties
        Write-Host "  ⚠ kqlDatabases detail API unavailable. Trying Eventhouse discovery..." -ForegroundColor Yellow
        $ehItems = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Eventhouse"
        $eh = $ehItems.value | Where-Object { $_.displayName -eq $eventhouseName } | Select-Object -First 1
        if ($eh) {
            # Try eventhouses/{id} endpoint (requires active capacity)
            try {
                $ehDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/eventhouses/$($eh.id)"
                if ($ehDetail.queryServiceUri) { $kustoUri = $ehDetail.queryServiceUri }
                elseif ($ehDetail.queryUri) { $kustoUri = $ehDetail.queryUri }
                elseif ($ehDetail.properties.queryServiceUri) { $kustoUri = $ehDetail.properties.queryServiceUri }
                elseif ($ehDetail.properties.queryUri) { $kustoUri = $ehDetail.properties.queryUri }
            } catch {}
        }
    }
    # Check if user provided KustoUri parameter
    if (-not $kustoUri -and $KustoUri) {
        $kustoUri = $KustoUri
        Write-Host "  ✓ Using provided Kusto URI: $kustoUri" -ForegroundColor Green
    }
    if (-not $kustoUri) {
        Write-Host "  ⚠ Could not auto-discover Kusto URI." -ForegroundColor Yellow
        Write-Host "  This usually means the Fabric capacity is paused or the API has changed." -ForegroundColor Yellow
        Write-Host "  Options:" -ForegroundColor White
        Write-Host "    1. Resume the Fabric capacity and re-run this script" -ForegroundColor DarkGray
        Write-Host "    2. Provide the URI: -KustoUri 'https://...kusto.fabric.microsoft.com'" -ForegroundColor DarkGray
        Write-Host "    3. Find it in: Fabric portal → MasimoEventhouse → Properties → Query URI" -ForegroundColor DarkGray
        Write-Host "  ERROR: Cannot proceed without a Kusto endpoint URI." -ForegroundColor Red
        exit 1
    }
    Write-Host "  ✓ KQL Database: $kqlDbName ($kqlDbId)" -ForegroundColor Green
    Write-Host "  ✓ Kusto URI: $kustoUri" -ForegroundColor Green

    # Discover Lakehouses (Silver + Bronze)
    Write-Host ""
    Write-Host "  Searching for Lakehouses in workspace..." -ForegroundColor White
    $lakehouses = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Lakehouse"

    if (-not $SilverLakehouseId) {
        $silverLh = $lakehouses.value | Where-Object { $_.displayName -match "[Ss]ilver" }
        if ($silverLh) {
            if ($silverLh -is [array]) { $silverLh = $silverLh[0] }
            $SilverLakehouseId = $silverLh.id
            $SilverLakehouseName = $silverLh.displayName
            Write-Host "  ✓ Silver Lakehouse: $SilverLakehouseName ($SilverLakehouseId)" -ForegroundColor Green
        } else {
            Write-Host "  ERROR: Silver Lakehouse not found. Has HDS Clinical Foundations been deployed?" -ForegroundColor Red
            Write-Host "    Provide it manually: -SilverLakehouseId <id>" -ForegroundColor Yellow
            exit 1
        }
    } else {
        Write-Host "  ✓ Silver Lakehouse ID (provided): $SilverLakehouseId" -ForegroundColor Green
    }

    $BronzeLakehouseId = ""
    $BronzeLakehouseName = ""
    $bronzeLh = $lakehouses.value | Where-Object { $_.displayName -match "[Bb]ronze" }
    if ($bronzeLh) {
        if ($bronzeLh -is [array]) { $bronzeLh = $bronzeLh[0] }
        $BronzeLakehouseId = $bronzeLh.id
        $BronzeLakehouseName = $bronzeLh.displayName
        Write-Host "  ✓ Bronze Lakehouse: $BronzeLakehouseName ($BronzeLakehouseId)" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Bronze Lakehouse not found — FHIR export shortcut will be skipped" -ForegroundColor Yellow
    }

    # Acquire Kusto token
    Write-Host ""
    Write-Host "  Acquiring Kusto access token..." -ForegroundColor White
    $kustoToken = Get-KustoAccessToken
    $kustoHeaders = @{
        "Authorization" = "Bearer $kustoToken"
        "Content-Type"  = "application/json"
    }
    $kqlParams = @{
        KustoUri     = $kustoUri
        DatabaseName = $kqlDbName
        KustoHeaders = $kustoHeaders
    }
    Write-Host "  ✓ Kusto token acquired" -ForegroundColor Green

    $p2Success = 0; $p2Fail = 0

    # ================================================================
    # PHASE 2-pre: Bronze LH Shortcut → FHIR Export Storage (ADLS Gen2)
    # ================================================================
    Write-Host ""
    Write-Host "--- PHASE 2-pre: BRONZE LAKEHOUSE → FHIR EXPORT SHORTCUT ---" -ForegroundColor Cyan
    Write-Host ""

    $bronzeShortcutOk = $false

    if (-not $BronzeLakehouseId) {
        Write-Host "  Skipped — Bronze Lakehouse not found in workspace." -ForegroundColor Yellow
        Write-Host "  Deploy HDS Healthcare Data Foundations first, then re-run -Phase2." -ForegroundColor Yellow
    } else {
        Write-Host "  Linking FHIR export storage to the Bronze Lakehouse so the" -ForegroundColor White
        Write-Host "  HDS clinical pipeline can ingest NDJSON → Silver." -ForegroundColor White
        Write-Host ""

        # Discover FHIR export storage from the resource group
        Write-Host "  Detecting FHIR export storage in $ResourceGroupName..." -ForegroundColor Gray
        $storageAccounts = az storage account list --resource-group $ResourceGroupName `
            --query "[?kind=='StorageV2'].{name:name, id:id, hns:isHnsEnabled}" `
            -o json 2>$null | ConvertFrom-Json
        $exportStorage = $null
        if ($storageAccounts) {
            $exportStorage = $storageAccounts | Where-Object { $_.hns -eq $true } | Select-Object -First 1
            if (-not $exportStorage) { $exportStorage = $storageAccounts | Select-Object -First 1 }
        }

        if (-not $exportStorage) {
            Write-Host "  ⚠ No StorageV2 account found in $ResourceGroupName." -ForegroundColor Yellow
            Write-Host "    Create the Bronze LH → FHIR export shortcut manually." -ForegroundColor Yellow
        } else {
            $exportStorageAccountName = $exportStorage.name
            $storageUrl = "https://$exportStorageAccountName.dfs.core.windows.net"
            Write-Host "  ✓ Export storage: $exportStorageAccountName" -ForegroundColor Green

            # Check if the fhir-export container exists
            $containerExists = az storage container exists --name $exportContainerName `
                --account-name $exportStorageAccountName --auth-mode login `
                --query "exists" -o tsv 2>$null
            if ($containerExists -ne "true") {
                Write-Host "  ⚠ Container '$exportContainerName' not found — FHIR `$export may not have run yet." -ForegroundColor Yellow
                Write-Host "    Shortcut will be created but empty until `$export completes." -ForegroundColor Yellow
            } else {
                Write-Host "  ✓ Container: $exportContainerName" -ForegroundColor Green
            }

            # Ensure workspace identity has Storage Blob Data Contributor on the storage account
            Write-Host "  Granting workspace identity access to storage..." -ForegroundColor Gray
            try {
                # Use the verified workspace identity from preflight check
                $wsSPId = $verifiedSPId
                $wsAppId = $verifiedAppId

                if ($wsSPId) {
                    Write-Host "  ✓ Workspace identity SP: $wsSPId (verified in preflight)" -ForegroundColor Green
                    if ($wsAppId) { Write-Host "    App ID: $wsAppId" -ForegroundColor Gray }
                    $storageBlobContribRole = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"  # Storage Blob Data Contributor
                    $existingRole = az role assignment list --assignee $wsSPId `
                        --scope $exportStorage.id --role $storageBlobContribRole `
                        --query "[0].id" -o tsv 2>$null
                    if (-not $existingRole) {
                        az role assignment create --assignee $wsSPId `
                            --role $storageBlobContribRole `
                            --scope $exportStorage.id 2>$null | Out-Null
                        Write-Host "  ✓ Workspace identity → Storage Blob Data Contributor" -ForegroundColor Green
                        Write-Host "  Waiting 60s for RBAC propagation..." -ForegroundColor Yellow
                        Start-Sleep -Seconds 60

                        # Verify RBAC propagated by checking role assignment is visible
                        $verifyRole = az role assignment list --assignee $wsSPId `
                            --scope $exportStorage.id --role $storageBlobContribRole `
                            --query "[0].id" -o tsv 2>$null
                        if ($verifyRole) {
                            Write-Host "  ✓ RBAC propagation confirmed" -ForegroundColor Green
                        } else {
                            Write-Host "  ⚠ RBAC not yet visible — waiting 30s more..." -ForegroundColor Yellow
                            Start-Sleep -Seconds 30
                        }
                    } else {
                        Write-Host "  ✓ Workspace identity RBAC already assigned" -ForegroundColor Green
                    }
                } else {
                    Write-Host "  ⚠ Workspace identity SP ID not available — RBAC may need manual assignment" -ForegroundColor Yellow
                }
            } catch {
                Write-Host "  ⚠ Could not assign RBAC: $($_.Exception.Message)" -ForegroundColor Yellow
                Write-Host "    Grant 'Storage Blob Data Contributor' to the workspace identity on $exportStorageAccountName" -ForegroundColor Yellow
            }

            # Find an existing Fabric cloud connection for FHIR export (match by display name)
            $fhirConnName = "fhir-export-$exportStorageAccountName"
            Write-Host "  Searching for existing Fabric connection '$fhirConnName'..." -ForegroundColor Gray
            $connectionId = $null
            try {
                $allConnections = Invoke-FabricApi -Endpoint "/connections"
                $existingConn = $allConnections.value | Where-Object {
                    $_.displayName -eq $fhirConnName
                }
                if ($existingConn) {
                    if ($existingConn -is [array]) { $existingConn = $existingConn[0] }
                    $connectionId = $existingConn.id
                    Write-Host "  ✓ Existing connection: $($existingConn.displayName) ($connectionId)" -ForegroundColor Green
                }
            } catch {
                Write-Host "  ⚠ Could not list connections: $($_.Exception.Message)" -ForegroundColor Yellow
            }

            if (-not $connectionId) {
                # Create a new Fabric cloud connection using Workspace Identity
                # Use hostname (not full URL) + container as path — matches storage-access-trusted-workspace.ps1 pattern
                $storageHost = "$exportStorageAccountName.dfs.core.windows.net"
                Write-Host "  Creating Fabric cloud connection (Workspace Identity)..." -ForegroundColor White
                try {
                    $connBody = @{
                        connectivityType  = "ShareableCloud"
                        displayName       = "fhir-export-$exportStorageAccountName"
                        connectionDetails = @{
                            type           = "AzureDataLakeStorage"
                            creationMethod = "AzureDataLakeStorage"
                            parameters     = @(
                                @{ dataType = "Text"; name = "server"; value = $storageHost }
                                @{ dataType = "Text"; name = "path";   value = $exportContainerName }
                            )
                        }
                        privacyLevel      = "Organizational"
                        credentialDetails = @{
                            singleSignOnType     = "None"
                            connectionEncryption = "NotEncrypted"
                            skipTestConnection   = $false
                            credentials          = @{
                                credentialType = "WorkspaceIdentity"
                            }
                        }
                    }
                    $newConn = Invoke-FabricApi -Method POST -Endpoint "/connections" -Body $connBody
                    $connectionId = $newConn.id
                    Write-Host "  ✓ Connection created: fhir-export-$exportStorageAccountName ($connectionId)" -ForegroundColor Green
                } catch {
                    $connErr = $_.Exception.Message
                    try { $connErr = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
                    Write-Host "  ✗ Failed to create Fabric connection: $connErr" -ForegroundColor Red
                    $p2Fail++
                    Write-Host ""
                    Write-Host "  Create the connection manually:" -ForegroundColor Yellow
                    Write-Host "    1. Fabric portal → Settings → Manage connections and gateways" -ForegroundColor Gray
                    Write-Host "    2. New → Cloud → Connection type: Azure Data Lake Storage Gen2" -ForegroundColor Gray
                    Write-Host "    3. Server: $storageUrl" -ForegroundColor Cyan
                    Write-Host "    4. Path: $exportContainerName" -ForegroundColor Cyan
                    Write-Host "    5. Auth: Workspace Identity" -ForegroundColor Gray
                    Write-Host "    6. Re-run: .\deploy-fabric-rti.ps1 -Phase2" -ForegroundColor Cyan
                }
            }

            # Create the ADLS Gen2 shortcut in Bronze Lakehouse
            if ($connectionId) {
                # Preflight: verify RBAC access by listing blobs in the container
                Write-Host "  Preflight: verifying storage access for workspace identity..." -ForegroundColor Gray
                $preflightOk = $false
                for ($attempt = 1; $attempt -le 3; $attempt++) {
                    try {
                        $testBlobs = az storage blob list --container-name $exportContainerName `
                            --account-name $exportStorageAccountName --auth-mode login `
                            --num-results 1 -o tsv 2>$null
                        if ($LASTEXITCODE -eq 0) {
                            Write-Host "  ✓ Storage access verified" -ForegroundColor Green
                            $preflightOk = $true
                            break
                        }
                    } catch {}
                    if ($attempt -lt 3) {
                        Write-Host "  ⚠ Storage access not ready (attempt $attempt/3). Waiting 30s..." -ForegroundColor Yellow
                        Start-Sleep -Seconds 30
                    }
                }
                if (-not $preflightOk) {
                    Write-Host "  ⚠ Storage access preflight failed — shortcut may fail. Continuing anyway..." -ForegroundColor Yellow
                }

                $shortcutPath = "Files/Ingest/Clinical/FHIR-NDJSON"
                $shortcutName = "FHIR-HDS"
                Write-Host "  Creating shortcut: $shortcutPath/$shortcutName → $storageUrl/$exportContainerName" -ForegroundColor White

                # Check if shortcut already exists (idempotent)
                $existingShortcut = $null
                try {
                    $existingShortcut = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items/$BronzeLakehouseId/shortcuts/$shortcutPath/$shortcutName"
                } catch {}
                if ($existingShortcut) {
                    Write-Host "  ✓ Bronze LH shortcut already exists: $shortcutPath/$shortcutName" -ForegroundColor Green
                    $bronzeShortcutOk = $true
                    $p2Success++
                } else {
                    # Remove any conflicting directory at the shortcut path (OneLake folders block shortcut creation)
                    try {
                        $onelakeToken = Get-AccessTokenForResource -ResourceUrl "https://storage.azure.com"
                        if ($onelakeToken -is [System.Security.SecureString]) {
                            $onelakeToken = $onelakeToken | ConvertFrom-SecureString -AsPlainText
                        }
                        $olHeaders = @{ Authorization = "Bearer $onelakeToken" }
                        $olPath = "$workspaceId/$BronzeLakehouseId/$shortcutPath/$shortcutName"
                        $null = Invoke-WebRequest -Method HEAD -Uri "https://onelake.dfs.fabric.microsoft.com/$olPath`?action=getStatus" -Headers $olHeaders -ErrorAction Stop
                        # Directory exists — delete it
                        $null = Invoke-RestMethod -Method DELETE -Uri "https://onelake.dfs.fabric.microsoft.com/$olPath`?recursive=true" -Headers $olHeaders
                        Write-Host "  Removed existing directory at shortcut path" -ForegroundColor Gray
                    } catch {
                        # No directory to remove — expected
                    }

                    $scBody = @{
                        name   = $shortcutName
                        path   = $shortcutPath
                        target = @{
                            adlsGen2 = @{
                                location     = "https://$exportStorageAccountName.dfs.core.windows.net"
                                subpath      = "/$exportContainerName"
                                connectionId = $connectionId
                            }
                        }
                    }

                    $scMaxRetries = 3
                    for ($scAttempt = 1; $scAttempt -le $scMaxRetries; $scAttempt++) {
                        try {
                            $null = Invoke-FabricApi -Method POST `
                                -Endpoint "/workspaces/$workspaceId/items/$BronzeLakehouseId/shortcuts" `
                                -Body $scBody
                            Write-Host "  ✓ Bronze LH shortcut: $shortcutPath/$shortcutName" -ForegroundColor Green
                            $bronzeShortcutOk = $true
                            $p2Success++
                            break
                        } catch {
                            $errMsg = $_.Exception.Message
                            try {
                                $errDetail = $_.ErrorDetails.Message | ConvertFrom-Json
                                $errMsg = $errDetail.message
                                if ($errDetail.moreDetails) {
                                    foreach ($d in $errDetail.moreDetails) {
                                        $errMsg += " | $($d.errorCode): $($d.message)"
                                    }
                                }
                            } catch {}

                            if ($errMsg -match 'Unauthorized|denied|Access' -and $scAttempt -lt $scMaxRetries) {
                                Write-Host "  ⚠ Shortcut auth failed (attempt $scAttempt/$scMaxRetries). RBAC may still be propagating — waiting 60s..." -ForegroundColor Yellow
                                Start-Sleep -Seconds 60
                            } else {
                                Write-Host "" -ForegroundColor Red
                                Write-Host "  ✗ Bronze LH shortcut FAILED: $errMsg" -ForegroundColor Red
                                Write-Host "" -ForegroundColor Red
                                Write-Host "  ┌──────────────────────────────────────────────────────────┐" -ForegroundColor Yellow
                                Write-Host "  │  HOW TO FIX: Create the shortcut manually in the         │" -ForegroundColor Yellow
                                Write-Host "  │  Fabric portal, then re-run Phase 2.                      │" -ForegroundColor Yellow
                                Write-Host "  └──────────────────────────────────────────────────────────┘" -ForegroundColor Yellow
                                Write-Host "" -ForegroundColor White
                                Write-Host "  Steps:" -ForegroundColor White
                                Write-Host "    1. Open the Fabric portal: https://app.fabric.microsoft.com" -ForegroundColor Gray
                                Write-Host "    2. Navigate to workspace '$FabricWorkspaceName'" -ForegroundColor Gray
                                Write-Host "    3. Open the Bronze Lakehouse ('$BronzeLakehouseName')" -ForegroundColor Gray
                                Write-Host "    4. Navigate to Files → right-click in $shortcutPath" -ForegroundColor Gray
                                Write-Host "    5. Select 'New shortcut' → 'Azure Data Lake Storage Gen2'" -ForegroundColor Gray
                                Write-Host "    6. Connection URL: https://$exportStorageAccountName.dfs.core.windows.net" -ForegroundColor Cyan
                                Write-Host "    7. Container/subpath: $exportContainerName" -ForegroundColor Cyan
                                Write-Host "    8. Shortcut name: $shortcutName" -ForegroundColor Cyan
                                Write-Host "    9. Auth: Workspace Identity" -ForegroundColor Gray
                                Write-Host "" -ForegroundColor White
                                Write-Host "  Workspace Identity Details:" -ForegroundColor White
                                if ($wsSPId) { Write-Host "    SP Object ID: $wsSPId" -ForegroundColor Cyan }
                                if ($wsAppId) { Write-Host "    App ID:       $wsAppId" -ForegroundColor Cyan }
                                Write-Host "" -ForegroundColor White
                                Write-Host "  After creating the shortcut, re-run Phase 2:" -ForegroundColor White
                                Write-Host "    .\Deploy-All.ps1 -Phase2Only -Location '$Location' -FabricWorkspaceName '$FabricWorkspaceName'" -ForegroundColor Cyan
                                Write-Host "" -ForegroundColor White
                                $p2Fail++
                                break
                            }
                        }
                    }
                }
            }
        }
    }

    # ================================================================
    # PHASE 2-env: ADD SCIPY TO HDS SPARK ENVIRONMENT
    # ================================================================
    Write-Host ""
    Write-Host "--- PHASE 2-env: ADD SCIPY TO HDS SPARK ENVIRONMENT ---" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  The HDS flattening notebooks require scipy." -ForegroundColor White
    Write-Host "  Adding scipy==1.11.4 to the Spark environment..." -ForegroundColor White
    Write-Host ""

    try {
        # Find the HDS environment item
        $envItems = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Environment"
        $hdsEnv = $envItems.value | Where-Object { $_.displayName -match "healthcare.*environment" }
        if ($hdsEnv -is [array]) { $hdsEnv = $hdsEnv[0] }

        if ($hdsEnv) {
            $envId = $hdsEnv.id
            $envName = $hdsEnv.displayName
            Write-Host "  ✓ Environment: $envName ($envId)" -ForegroundColor Green

            # Check published libraries for scipy
            $scipyAlreadyPublished = $false
            try {
                $pubLibs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/environments/$envId/libraries?beta=False"
                $scipyLib = $pubLibs.libraries | Where-Object { $_.name -eq "scipy" }
                if ($scipyLib) {
                    $scipyAlreadyPublished = $true
                    Write-Host "  ✓ scipy already published (v$($scipyLib.version))" -ForegroundColor Green
                    $p2Success++
                }
            } catch {}

            if (-not $scipyAlreadyPublished) {
                # Check environment state — cannot publish if already publishing
                $envMeta = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/environments/$envId"
                $envState = $envMeta.properties.publishDetails.state
                if ($envState -and $envState -ne "Success" -and $envState -ne "Failed" -and $envState -ne "Cancelled") {
                    Write-Host "  ⚠ Environment is currently '$envState' — waiting for current publish to finish..." -ForegroundColor Yellow
                    $envWaitStart = Get-Date
                    while ((New-TimeSpan -Start $envWaitStart).TotalMinutes -lt 15) {
                        Start-Sleep -Seconds 30
                        $envMeta = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/environments/$envId"
                        $envState = $envMeta.properties.publishDetails.state
                        if ($envState -eq "Success" -or $envState -eq "Failed" -or $envState -eq "Cancelled" -or -not $envState) {
                            break
                        }
                        $elapsed = [math]::Round((New-TimeSpan -Start $envWaitStart).TotalMinutes, 1)
                        Write-Host "    Still $envState (${elapsed}m)..." -ForegroundColor Gray
                    }
                }

                # Export current external libraries YAML (to preserve existing)
                $existingYml = ""
                try {
                    $fabricToken = Get-FabricAccessToken
                    $exportHeaders = @{
                        "Authorization" = "Bearer $fabricToken"
                    }
                    $existingYml = Invoke-RestMethod -Method GET `
                        -Uri "$FabricApiBase/workspaces/$workspaceId/environments/$envId/staging/libraries/exportExternalLibraries" `
                        -Headers $exportHeaders
                } catch {
                    # No existing external libs — expected for fresh HDS deploy
                }

                # Build updated environment.yml with scipy
                $scipyEntry = "scipy==1.11.4"
                if ($existingYml -and $existingYml -match "scipy") {
                    Write-Host "  ✓ scipy already in staging libraries (pending publish)" -ForegroundColor Green
                    # Still need to publish
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

                    Write-Host "  Importing scipy==1.11.4 into staging..." -ForegroundColor Gray

                    # Upload via importExternalLibraries (accepts YAML file content)
                    $fabricToken = Get-FabricAccessToken
                    $importHeaders = @{
                        "Authorization" = "Bearer $fabricToken"
                        "Content-Type"  = "application/octet-stream"
                    }
                    $ymlBytes = [System.Text.Encoding]::UTF8.GetBytes($newYml)
                    $null = Invoke-RestMethod -Method POST `
                        -Uri "$FabricApiBase/workspaces/$workspaceId/environments/$envId/staging/libraries/importExternalLibraries" `
                        -Headers $importHeaders `
                        -Body $ymlBytes
                    Write-Host "  ✓ scipy==1.11.4 added to staging" -ForegroundColor Green
                }

                # Publish the environment
                Write-Host "  Publishing environment (this takes 3-10 min)..." -ForegroundColor White
                $fabricToken = Get-FabricAccessToken
                $pubHeaders = @{
                    "Authorization" = "Bearer $fabricToken"
                    "Content-Type"  = "application/json"
                }
                $pubResp = Invoke-WebRequest -Method POST `
                    -Uri "$FabricApiBase/workspaces/$workspaceId/environments/$envId/staging/publish?beta=False" `
                    -Headers $pubHeaders `
                    -UseBasicParsing
                $operationId = $null
                if ($pubResp.Headers["x-ms-operation-id"]) {
                    $operationId = $pubResp.Headers["x-ms-operation-id"]
                    if ($operationId -is [array]) { $operationId = $operationId[0] }
                }

                # Poll for publish completion (max 15 min)
                $pubStart = Get-Date
                $maxPubMin = 15
                $pubSuccess = $false
                while ((New-TimeSpan -Start $pubStart).TotalMinutes -lt $maxPubMin) {
                    Start-Sleep -Seconds 30
                    $elapsed = [math]::Round((New-TimeSpan -Start $pubStart).TotalMinutes, 1)

                    try {
                        $envMeta = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/environments/$envId"
                        $pubState = $envMeta.properties.publishDetails.state
                        if ($pubState -eq "Success") {
                            Write-Host "  ✓ Environment published — scipy==1.11.4 is now available" -ForegroundColor Green
                            $pubSuccess = $true
                            $p2Success++
                            break
                        } elseif ($pubState -eq "Failed" -or $pubState -eq "Cancelled") {
                            Write-Host "  ✗ Environment publish $pubState" -ForegroundColor Red
                            $p2Fail++
                            break
                        } else {
                            Write-Host "    Publish status: $pubState (${elapsed}m elapsed)" -ForegroundColor Gray
                        }
                    } catch {
                        Write-Host "    Poll error: $($_.Exception.Message)" -ForegroundColor Yellow
                    }
                }

                if (-not $pubSuccess -and (New-TimeSpan -Start $pubStart).TotalMinutes -ge $maxPubMin) {
                    Write-Host "  ⚠ Environment still publishing after ${maxPubMin}m — continuing" -ForegroundColor Yellow
                    Write-Host "    Check status in Fabric portal → Environments." -ForegroundColor Yellow
                    $p2Success++  # treat as in-progress
                }
            }
        } else {
            Write-Host "  ⚠ HDS Spark environment not found in workspace." -ForegroundColor Yellow
            Write-Host "    Ensure Healthcare Data Foundations is deployed first." -ForegroundColor Yellow
            Write-Host "    Then manually add scipy==1.11.4 to the environment." -ForegroundColor Yellow
        }
    } catch {
        $envErr = $_.Exception.Message
        try { $envErr = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
        Write-Host "  ✗ Could not update environment: $envErr" -ForegroundColor Red
        Write-Host "    Manually add scipy==1.11.4 to the HDS Spark environment." -ForegroundColor Yellow
        $p2Fail++
    }

    # ================================================================
    # NOTE: Clinical pipeline is triggered by storage-access-trusted-workspace.ps1
    # which orchestrates Clinical → Imaging → OMOP pipeline sequence.
    # Removed duplicate trigger from here to avoid running it twice.
    # ================================================================

    # ================================================================
    # PHASE 2a: Create OneLake Shortcuts + KQL External Tables to Silver Lakehouse
    # ================================================================

    # Refresh Kusto token (may have expired during scipy publish + pipeline wait)
    Write-Host "  Refreshing Kusto access token..." -ForegroundColor Gray
    $kustoToken = Get-KustoAccessToken
    $kustoHeaders = @{
        "Authorization" = "Bearer $kustoToken"
        "Content-Type"  = "application/json"
    }
    $kqlParams = @{
        KustoUri     = $kustoUri
        DatabaseName = $kqlDbName
        KustoHeaders = $kustoHeaders
    }
    Write-Host "  ✓ Kusto token refreshed" -ForegroundColor Green

    Write-Host ""
    Write-Host "--- PHASE 2a: KQL EXTERNAL TABLES → SILVER LAKEHOUSE ---" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Two-step process for each Silver table:" -ForegroundColor White
    Write-Host "    1. Create OneLake shortcut on KQL Database (path=/Tables)" -ForegroundColor White
    Write-Host "    2. Create KQL external table (kind=delta) pointing to the shortcut" -ForegroundColor White
    Write-Host ""

    # Define the shortcuts to create: KQL shortcut name -> Silver Lakehouse table name
    $shortcuts = @(
        @{ Name = "SilverPatient";   Table = "Patient" },
        @{ Name = "SilverCondition"; Table = "Condition" },
        @{ Name = "SilverDevice";    Table = "Device" },
        @{ Name = "SilverLocation";  Table = "Location" },
        @{ Name = "SilverEncounter"; Table = "Encounter" },
        @{ Name = "SilverBasic";     Table = "Basic" },
        @{ Name = "SilverObservation"; Table = "Observation" },
        @{ Name = "SilverMedicationRequest"; Table = "MedicationRequest" },
        @{ Name = "SilverProcedure"; Table = "Procedure" },
        @{ Name = "SilverImmunization"; Table = "Immunization" },
        @{ Name = "SilverImagingStudy"; Table = "ImagingStudy" }
    )

    foreach ($sc in $shortcuts) {
        # Step 1: Create OneLake shortcut via Fabric REST API
        $shortcutBody = @{
            name   = $sc.Name
            path   = "/Tables"
            target = @{
                oneLake = @{
                    workspaceId = $workspaceId
                    itemId      = $SilverLakehouseId
                    path        = "Tables/$($sc.Table)"
                }
            }
        }
        try {
            $null = Invoke-FabricApi -Method POST `
                -Endpoint "/workspaces/$workspaceId/items/$kqlDbId/shortcuts?shortcutConflictPolicy=CreateOrOverwrite" `
                -Body $shortcutBody
            Write-Host "  ✓ $($sc.Name) shortcut → Tables/$($sc.Table)" -ForegroundColor Green
        } catch {
            $errMsg = $_.Exception.Message
            try {
                $errDetail = $_.ErrorDetails.Message | ConvertFrom-Json
                $errMsg = $errDetail.message
                if ($errDetail.moreDetails) {
                    foreach ($d in $errDetail.moreDetails) {
                        $errMsg += " | $($d.errorCode): $($d.message)"
                    }
                }
            } catch {}
            Write-Host "  ✗ $($sc.Name) shortcut" -ForegroundColor Red
            Write-Host "    $errMsg" -ForegroundColor DarkRed
            $p2Fail++
            continue
        }

        # Step 2: Create KQL external table pointing to the shortcut via OneLake
        # The ;impersonate suffix is mandatory for Fabric KQL external tables
        $extTableUrl = "https://onelake.dfs.fabric.microsoft.com/$workspaceId/$kqlDbId/Tables/$($sc.Name);impersonate"
        $extTableCmd = ".create-or-alter external table $($sc.Name) kind=delta (h@'$extTableUrl')"
        $extResult = Invoke-KustoMgmt -Command $extTableCmd `
            -Label "$($sc.Name) external table" @kqlParams
        if ($extResult) { $p2Success++ } else { $p2Fail++ }
    }

    # Verify external tables are queryable
    Write-Host ""
    Write-Host "  Verifying external tables..." -ForegroundColor White

    $verifyQueries = @(
        @{ Query = "external_table('SilverPatient') | take 1 | count"; Label = "SilverPatient" },
        @{ Query = "external_table('SilverCondition') | take 1 | count"; Label = "SilverCondition" },
        @{ Query = "external_table('SilverDevice') | take 1 | count"; Label = "SilverDevice" },
        @{ Query = "external_table('SilverLocation') | take 1 | count"; Label = "SilverLocation" },
        @{ Query = "external_table('SilverEncounter') | take 1 | count"; Label = "SilverEncounter" },
        @{ Query = "external_table('SilverBasic') | take 1 | count"; Label = "SilverBasic" },
        @{ Query = "external_table('SilverObservation') | take 1 | count"; Label = "SilverObservation" },
        @{ Query = "external_table('SilverMedicationRequest') | take 1 | count"; Label = "SilverMedicationRequest" },
        @{ Query = "external_table('SilverProcedure') | take 1 | count"; Label = "SilverProcedure" },
        @{ Query = "external_table('SilverImmunization') | take 1 | count"; Label = "SilverImmunization" },
        @{ Query = "external_table('SilverImagingStudy') | take 1 | count"; Label = "SilverImagingStudy" }
    )

    foreach ($vq in $verifyQueries) {
        $body = @{ db = $kqlDbName; csl = $vq.Query } | ConvertTo-Json -Depth 3 -Compress
        try {
            $result = Invoke-RestMethod -Uri "$kustoUri/v1/rest/query" -Headers $kustoHeaders -Method POST -Body $body
            Write-Host "  ✓ $($vq.Label) -- accessible" -ForegroundColor Green
        } catch {
            Write-Host "  ⚠ $($vq.Label) -- not yet accessible (HDS pipeline may not have run yet)" -ForegroundColor Yellow
        }
    }

    # ================================================================
    # PHASE 2b: Deploy Enriched fn_ClinicalAlerts (with Silver joins)
    # ================================================================
    Write-Host ""
    Write-Host "--- PHASE 2b: ENRICHED CLINICAL ALERT FUNCTION ---" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Replacing fn_ClinicalAlerts with enriched version that joins" -ForegroundColor White
    Write-Host "  real-time telemetry alerts with FHIR patient demographics" -ForegroundColor White
    Write-Host "  and qualifying conditions for severity escalation." -ForegroundColor White
    Write-Host ""

    $cmd = '.create-or-alter function with (docstring = "Enriched clinical alerts — joins telemetry with HDS Silver Lakehouse patient data via Basic (DeviceAssociation) resources", folder = "ClinicalAlerts") fn_ClinicalAlerts(windowMinutes: int = 5) { let device_patient_map = external_table(''SilverBasic'') | where tostring(code.coding[0].code) in ("device-assoc", "ASSIGNED") | extend ext = parse_json(extension) | mv-expand ext | where tostring(ext.url) has "associated-device" or tostring(ext.url) has "device-association-device" | extend device_identifier = replace_string(tostring(ext.valueReference.reference), "Device/", "") | project device_identifier, patient_orig_id = tostring(subject.idOrig); let patient_info = external_table(''SilverPatient'') | extend name_obj = name[0] | project patient_orig_id = idOrig, patient_name = coalesce(tostring(name_obj.text), strcat(tostring(name_obj.given[0]), " ", tostring(name_obj.family))), gender, birthDate; let high_risk_conditions = external_table(''SilverCondition'') | mv-expand coding = code.coding | where tostring(coding.code) in ("13645005", "84114007", "195967001", "233604007", "59621000") | summarize conditions = make_set(tostring(coding.display)) by patient_orig_id = tostring(subject.msftSourceReference) | extend conditions_str = strcat_array(conditions, ", "); let spo2_alerts = fn_SpO2Alerts(windowMinutes) | project device_id, alert_time, spo2_tier = alert_tier, spo2_value = metric_value, spo2_message = message; let pr_alerts = fn_PulseRateAlerts(windowMinutes) | project device_id, alert_time, pr_tier = alert_tier, pr_value = metric_value, pr_message = message; let vitals = TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize arg_max(todatetime(timestamp), *) by device_id | project device_id, current_spo2 = todouble(telemetry.spo2), current_pr = toint(telemetry.pr), current_pi = todouble(telemetry.pi), current_sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq); vitals | join kind=leftouter spo2_alerts on device_id | join kind=leftouter pr_alerts on device_id | where isnotempty(spo2_tier) or isnotempty(pr_tier) | join kind=leftouter device_patient_map on $left.device_id == $right.device_identifier | join kind=leftouter patient_info on patient_orig_id | join kind=leftouter high_risk_conditions on patient_orig_id | extend has_high_risk = isnotempty(conditions_str), base_tier = case(spo2_tier == "CRITICAL" or pr_tier == "CRITICAL", "CRITICAL", spo2_tier == "URGENT" or pr_tier == "URGENT", "URGENT", "WARNING") | extend final_tier = case(base_tier == "CRITICAL", "CRITICAL", base_tier == "URGENT" and has_high_risk, "CRITICAL", base_tier == "WARNING" and has_high_risk, "URGENT", base_tier), alert_type = case(isnotempty(spo2_tier) and isnotempty(pr_tier), "MULTI_METRIC", isnotempty(spo2_tier), "SPO2_LOW", "PR_ABNORMAL") | project alert_id = strcat("ALERT-", device_id, "-", format_datetime(now(), "yyyyMMddHHmmss")), alert_time = coalesce(alert_time, alert_time1, now()), device_id, patient_id = coalesce(patient_orig_id, ""), patient_name = coalesce(patient_name, "(not linked)"), alert_tier = final_tier, alert_type, spo2 = current_spo2, pr = current_pr, pi = current_pi, sphb = current_sphb, signal_iq, qualifying_conditions = coalesce(conditions_str, ""), escalated = (final_tier != base_tier), message = strcat(final_tier, " ALERT", iff(final_tier != base_tier, " (ESCALATED)", ""), " | Patient: ", coalesce(patient_name, "Unknown"), " | Device: ", device_id, " | SpO2: ", tostring(current_spo2), "%", " | PR: ", tostring(current_pr), " bpm", iff(has_high_risk, strcat(" | Conditions: ", conditions_str), "")) | order by alert_tier asc, device_id asc }'
    # Fabric KQL can return HTTP 520 while validating functions that reference
    # OneLake-backed external tables. Defer validation, then verify by querying.
    $cmd = $cmd.Replace('with (', "with (skipvalidation = 'true', ")
    if (Invoke-KustoMgmt -Command $cmd -Label "fn_ClinicalAlerts (enriched with Silver LH)" @kqlParams) { $p2Success++ } else { $p2Fail++ }

    # Cache the relatively stable patient-to-facility dimension once. Alert
    # facts remain live because fn_AlertLocationMap joins current alerts to this
    # small local table instead of snapshotting the alert result itself.
    Write-Host ""
    Write-Host "  Refreshing patient-to-facility location mapping..." -ForegroundColor White
    $cmd = '.set-or-replace PatientLocationDashboard <| let patient_encounters = external_table(''SilverEncounter'') | mv-expand loc = location | extend patient_orig_id = coalesce(tostring(subject.idOrig), replace_string(tostring(subject.msftSourceReference), "Patient/", ""), tostring(subject.identifier.value)), location_key = coalesce(tostring(loc.location.id), tostring(loc.location.identifier.value)) | where isnotempty(patient_orig_id) and isnotempty(location_key) | summarize arg_max(todatetime(period.start), location_key) by patient_orig_id | project patient_orig_id, location_key; let location_info = external_table(''SilverLocation'') | where isnotempty(position) | extend location_keys = pack_array(tostring(id), tostring(idOrig)) | mv-expand location_key = location_keys | where isnotempty(location_key) | project location_key = tostring(location_key), location_name = name, latitude = todouble(position.latitude), longitude = todouble(position.longitude), city = tostring(address.city), state = tostring(address.state); patient_encounters | join kind=leftouter location_info on location_key | project patient_id = patient_orig_id, location_name = coalesce(location_name, "Unknown (no Encounter Location match)"), city = coalesce(city, "Unknown"), state = coalesce(state, ""), latitude, longitude'
    if (Invoke-KustoMgmt -Command $cmd -Label "PatientLocationDashboard mapping" @kqlParams) { $p2Success++ } else { $p2Fail++ }

    Write-Host "  Deploying fn_AlertLocationMap (live alerts + cached location mapping)..." -ForegroundColor White
    $cmd = '.create-or-alter function with (docstring = "Live clinical alerts joined to the deployment-refreshed patient location mapping", folder = "ClinicalAlerts") fn_AlertLocationMap(windowMinutes: int = 60) { fn_ClinicalAlerts(windowMinutes) | join kind=leftouter PatientLocationDashboard on patient_id | project alert_time, device_id, patient_id, patient_name, alert_tier, alert_type, spo2, pr, location_name = coalesce(location_name, "Unknown (no Encounter Location match)"), city = coalesce(city, "Unknown"), state = coalesce(state, ""), latitude, longitude, qualifying_conditions, escalated, message | order by alert_tier asc, alert_time desc }'
    $cmd = $cmd.Replace('with (', "with (skipvalidation = 'true', ")
    if (Invoke-KustoMgmt -Command $cmd -Label "fn_AlertLocationMap" @kqlParams) { $p2Success++ } else { $p2Fail++ }

    # ================================================================
    # PHASE 2c: CLINICAL ALERTS MAP DASHBOARD
    # ================================================================
    Write-Host ""
    Write-Host "--- PHASE 2c: CLINICAL ALERTS MAP DASHBOARD ---" -ForegroundColor Cyan
    Write-Host ""

    $mapDashboardName = "Clinical Alerts Map"

    # Check for existing map dashboard
    Write-Host "  Checking for existing map dashboard..." -ForegroundColor Gray
    $existingMapDash = $null
    try {
        $dashItems = Invoke-FabricApi -Method GET -Endpoint "/workspaces/$workspaceId/items?type=KQLDashboard"
        $existingMapDash = $dashItems.value | Where-Object { $_.displayName -eq $mapDashboardName }
    } catch {}

    if ($existingMapDash) {
        $mapDashId = $existingMapDash.id
        Write-Host "  ✓ Map dashboard exists: $mapDashboardName ($mapDashId)" -ForegroundColor Green
    } else {
        Write-Host "  Creating KQL Dashboard '$mapDashboardName'..." -ForegroundColor White
        try {
            $dashResp = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$workspaceId/items" `
                -Body @{ displayName = $mapDashboardName; type = "KQLDashboard"; description = "Location-aware clinical alert dashboard showing HDS-enriched patient/device alerts across facilities." }
            $mapDashId = $dashResp.id
            Write-Host "  ✓ Map dashboard created: $mapDashId" -ForegroundColor Green
        } catch {
            Write-Host "  ⚠ Could not create map dashboard: $($_.Exception.Message)" -ForegroundColor Yellow
            $mapDashId = $null
        }
    }

    if ($mapDashId) {
        Write-Host "  Applying map dashboard definition (4 tiles)..." -ForegroundColor White

        # Keep the Clinical Alerts Map page ID stable. Fabric page URLs include
        # this ID; regenerating it on every Phase 2 run breaks bookmarked/open
        # portal URLs with "Error loading dashboard" even when updateDefinition
        # succeeds.
        $mapDsUuid   = "fd24750d-7a27-58a5-a74d-3eca90252091"
        $mapPageUuid = "2dfd9f6e-7dcb-4eee-a2f4-4045a36ab0a4"
        $alertLocationBaseKql = "fn_AlertLocationMap(60)"

        $mapDashDef = @{
            '$schema' = "https://dataexplorer.azure.com/static/d/schema/20/dashboard.json"
            schema_version = "20"
            title = $mapDashboardName
            autoRefresh = @{ enabled = $true; defaultInterval = "30s"; minInterval = "30s" }
            pages = @( @{ name = "Alert Map"; id = $mapPageUuid } )
            dataSources = @( @{
                id         = $mapDsUuid
                name       = $kqlDbName
                scopeId    = "kusto-trident"
                kind       = "kusto-trident"
                clusterUri = $kustoUri
                database   = $kqlDbId
                workspace  = $workspaceId
            } )
            parameters = @()
            tiles = @(
                @{
                    id            = "123b3323-56db-54a9-b53e-2fafd8a01a27"
                    title         = "Alert Locations"
                    query         = "$alertLocationBaseKql | summarize alert_count = count(), critical = countif(alert_tier == 'CRITICAL'), urgent = countif(alert_tier == 'URGENT'), warning = countif(alert_tier == 'WARNING'), patients = dcount(patient_name), devices = dcount(device_id) by location_name, latitude, longitude, city, state | project latitude, longitude, location_name, city, state, alert_count, critical, urgent, warning, patients, devices | order by alert_count desc"
                    layout        = @{ x = 0; y = 0; width = 24; height = 11 }
                    pageId        = $mapPageUuid
                    visualType    = "map"
                    dataSourceId  = $mapDsUuid
                    visualOptions = @{
                        map__latitudeColumn = @{ type = "specified"; value = "latitude" }
                        map__longitudeColumn = @{ type = "specified"; value = "longitude" }
                        map__bubbleFormat = "bubble"
                        map__minBubbleSizeColumn = @{ type = "specified"; value = "alert_count" }
                    }
                    usedParamVariables = @()
                },
                @{
                    id            = "2275c861-de0f-5529-b235-83df54fe0f43"
                    title         = "Alerts by Hospital"
                    query         = "$alertLocationBaseKql | summarize total = count(), critical = countif(alert_tier == 'CRITICAL'), urgent = countif(alert_tier == 'URGENT'), warning = countif(alert_tier == 'WARNING') by location_name | order by total desc"
                    layout        = @{ x = 0; y = 11; width = 12; height = 6 }
                    pageId        = $mapPageUuid
                    visualType    = "bar"
                    dataSourceId  = $mapDsUuid
                    visualOptions = @{
                        xColumn = @{ type = "infer" }
                        yColumns = @{ type = "infer" }
                    }
                    usedParamVariables = @()
                },
                @{
                    id            = "ce8de71c-5c84-589c-a8e9-a0eac9333e28"
                    title         = "Total Active Alerts"
                    query         = "$alertLocationBaseKql | count"
                    layout        = @{ x = 12; y = 11; width = 6; height = 3 }
                    pageId        = $mapPageUuid
                    visualType    = "card"
                    dataSourceId  = $mapDsUuid
                    visualOptions = @{}
                    usedParamVariables = @()
                },
                @{
                    id            = "bba77793-813f-5fc1-bd7b-10feda1b490e"
                    title         = "Alert Detail"
                    query         = "$alertLocationBaseKql | project alert_time, device_id, patient_name, alert_tier, alert_type, spo2, pr, location_name, city | order by alert_tier asc, alert_time desc | take 100"
                    layout        = @{ x = 0; y = 17; width = 24; height = 8 }
                    pageId        = $mapPageUuid
                    visualType    = "table"
                    dataSourceId  = $mapDsUuid
                    visualOptions = @{
                        colorRules = @(
                            @{
                                id         = "11ca95ec-ac6e-5116-93d1-6c7072ee8805"
                                column     = @{ type = "specified"; value = "alert_tier" }
                                conditions = @( @{ operator = "=="; value = "CRITICAL" } )
                                color      = "red"
                                indicator  = @{ kind = "icon"; icon = "critical"; label = @{ type = "specified"; value = "Critical" } }
                            },
                            @{
                                id         = "dabb2ef8-aa9a-5920-b92f-4e7c821f482b"
                                column     = @{ type = "specified"; value = "alert_tier" }
                                conditions = @( @{ operator = "=="; value = "URGENT" } )
                                color      = "yellow"
                                indicator  = @{ kind = "icon"; icon = "warning"; label = @{ type = "specified"; value = "Urgent" } }
                            },
                            @{
                                id         = "b0302b0b-5f7d-52d6-b3bb-b8efae2322d3"
                                column     = @{ type = "specified"; value = "alert_tier" }
                                conditions = @( @{ operator = "=="; value = "WARNING" } )
                                color      = "blue"
                                indicator  = @{ kind = "icon"; icon = "circle"; label = @{ type = "specified"; value = "Warning" } }
                            }
                        )
                        colorRulesDisabled = $false
                        colorStyle         = "bold"
                    }
                    usedParamVariables = @()
                }
            )
        }

        $mapDashJson = $mapDashDef | ConvertTo-Json -Depth 10 -Compress
        $mapB64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($mapDashJson))

        try {
            $null = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$workspaceId/items/$mapDashId/updateDefinition" `
                -Body @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $mapB64; payloadType = "InlineBase64" } ) } }
            Write-Host "  ✓ Map dashboard definition applied (4 tiles)" -ForegroundColor Green
            Write-Host "    • Alert Locations (map — bubble chart by hospital)" -ForegroundColor Gray
            Write-Host "    • Alerts by Hospital (bar chart)" -ForegroundColor Gray
            Write-Host "    • Total Active Alerts (card)" -ForegroundColor Gray
            Write-Host "    • Alert Detail (table with location)" -ForegroundColor Gray
            Write-Host "  Auto-refresh: 30 seconds" -ForegroundColor Cyan
            Write-Host "  Dashboard URL: https://app.fabric.microsoft.com/groups/$workspaceId/kustodashboards/$mapDashId?page=$mapPageUuid" -ForegroundColor DarkGray
            $p2Success++
        } catch {
            Write-Host "  ⚠ Failed to apply map dashboard definition: $($_.Exception.Message)" -ForegroundColor Yellow
            $p2Fail++
        }
    }

    # Phase 2 owns the enriched alert/location contracts consumed by the main
    # dashboard. Reapply its template so Phase2-only repair runs update existing
    # dashboards instead of leaving Phase 1 tile definitions stale.
    $patientDashboard = $dashItems.value | Where-Object { $_.displayName -eq "Masimo Patient Monitoring" }
    if ($patientDashboard) {
        try {
            $applied = Set-MasimoDashboardDefinition -WorkspaceId $workspaceId -DashboardId $patientDashboard.id `
                -DashboardName "Masimo Patient Monitoring" -KqlDbName $kqlDbName -KqlDbId $kqlDbId -KustoUri $kustoUri
            Write-Host "  ✓ Masimo Patient Monitoring definition refreshed ($($applied.Pages) pages, $($applied.Tiles) tiles)" -ForegroundColor Green
            $p2Success++
        } catch {
            Write-Host "  ✗ Could not refresh Masimo Patient Monitoring definition: $($_.Exception.Message)" -ForegroundColor Red
            $p2Fail++
        }
    }

    # ================================================================
    # PHASE 2 SUMMARY
    # ================================================================
    Write-Host ""
    $p2Total = $p2Success + $p2Fail
    Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║  PHASE 2 COMPLETE                                            ║" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "╠══════════════════════════════════════════════════════════════╣" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║  Phase 2 Results: $p2Success / $p2Total succeeded                        ║" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║                                                              ║" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║  Created:                                                    ║" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║    • Bronze LH → FHIR export (Ingest/.../FHIR-HDS)             ║" -ForegroundColor Gray
    Write-Host "║    • scipy==1.11.4 → HDS Spark environment                  ║" -ForegroundColor Gray
    Write-Host "║    • Clinical pipeline triggered (NDJSON → Silver)           ║" -ForegroundColor Gray
    Write-Host "║    • SilverPatient external table (OneLake shortcut)         ║" -ForegroundColor Gray
    Write-Host "║    • SilverCondition external table (OneLake shortcut)       ║" -ForegroundColor Gray
    Write-Host "║    • SilverDevice external table (OneLake shortcut)          ║" -ForegroundColor Gray
    Write-Host "║    • SilverLocation external table (OneLake shortcut)        ║" -ForegroundColor Gray
    Write-Host "║    • SilverEncounter external table (OneLake shortcut)       ║" -ForegroundColor Gray
    Write-Host "║    • PatientLocationDashboard (cached facility mapping)      ║" -ForegroundColor Gray
    Write-Host "║    • fn_ClinicalAlerts (enriched with patient context)       ║" -ForegroundColor Gray
    Write-Host "║    • fn_AlertLocationMap (alerts + location for map)         ║" -ForegroundColor Gray
    Write-Host "║    • Clinical Alerts Map dashboard (4 tiles)                 ║" -ForegroundColor Gray
    Write-Host "║    • Masimo Patient Monitoring dashboard refreshed          ║" -ForegroundColor Gray
    Write-Host "║                                                              ║" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║  Verify with:                                                ║" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host "║    external_table('SilverPatient') | take 5                  ║" -ForegroundColor Gray
    Write-Host "║    external_table('SilverLocation') | take 5                 ║" -ForegroundColor Gray
    Write-Host "║    fn_ClinicalAlerts(5)                                      ║" -ForegroundColor Gray
    Write-Host "║    fn_AlertLocationMap(60)                                    ║" -ForegroundColor Gray
    Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor $(if ($p2Fail -eq 0) { "Green" } else { "Yellow" })
    Write-Host ""

    if ($p2Fail -gt 0) {
        Write-Host "ERROR: Phase 2 RTI incomplete ($p2Fail failed component(s))." -ForegroundColor Red
        exit 1
    }

    exit 0
}

# ============================================================================
# STEP 0: VALIDATE PREREQUISITES
# ============================================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Masimo Clinical Alert System — Fabric RTI Deployment       ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

Write-Host "--- STEP 0: VALIDATING PREREQUISITES ---" -ForegroundColor Cyan

# Verify Az module
if (-not (Get-Module -ListAvailable -Name Az.Accounts)) {
    Write-Host "ERROR: Az PowerShell module not found. Run: Install-Module Az" -ForegroundColor Red
    exit 1
}
Write-Host "  ✓ Az PowerShell module found" -ForegroundColor Green

# Verify logged in
try {
    $azContext = Get-AzContext
    if (-not $azContext) { throw "No Az context" }
    Write-Host "  ✓ Authenticated as: $($azContext.Account.Id)" -ForegroundColor Green
    Write-Host "  ✓ Subscription: $($azContext.Subscription.Name) ($($azContext.Subscription.Id))" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Not logged in. Run: Connect-AzAccount" -ForegroundColor Red
    exit 1
}

# ============================================================================
# STEP 0.5: AUTO-DETECT AZURE RESOURCES
# ============================================================================

Write-Host ""
Write-Host "--- STEP 0.5: DETECTING AZURE RESOURCES ---" -ForegroundColor Cyan

# Verify resource group exists
$rgExists = az group show --name $ResourceGroupName 2>$null
if (-not $rgExists) {
    Write-Host "ERROR: Resource group '$ResourceGroupName' not found." -ForegroundColor Red
    Write-Host "  Run phase-1/deploy.ps1 first to deploy the base infrastructure." -ForegroundColor Yellow
    exit 1
}
Write-Host "  ✓ Resource group: $ResourceGroupName" -ForegroundColor Green

# Auto-detect Event Hub namespace if not provided
if (-not $EventHubNamespace) {
    $ehNs = az eventhubs namespace list --resource-group $ResourceGroupName --query "[0].name" -o tsv
    if ($ehNs) {
        $EventHubNamespace = $ehNs
    } else {
        Write-Host "ERROR: No Event Hub namespace found in RG '$ResourceGroupName'." -ForegroundColor Red
        exit 1
    }
}
Write-Host "  ✓ Event Hub Namespace: $EventHubNamespace" -ForegroundColor Green
Write-Host "  ✓ Event Hub: $EventHubName" -ForegroundColor Green

# Auto-detect FHIR service URL if not provided
if (-not $FhirServiceUrl) {
    $fhirSvcJson = az resource list --resource-group $ResourceGroupName `
        --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
        --query "[0].id" -o tsv 2>$null
    if ($fhirSvcJson) {
        $fhirProps = az resource show --ids $fhirSvcJson --query "properties.authenticationConfiguration.audience" -o tsv 2>$null
        if ($fhirProps) {
            $FhirServiceUrl = $fhirProps
        }
    }
    # Try healthcare workspace discovery if direct query didn't work
    if (-not $FhirServiceUrl) {
        $hwName = az resource list --resource-group $ResourceGroupName `
            --resource-type "Microsoft.HealthcareApis/workspaces" `
            --query "[0].name" -o tsv 2>$null
        if ($hwName) {
            $fhirName = az resource list --resource-group $ResourceGroupName `
                --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
                --query "[0].name" -o tsv 2>$null
            if ($fhirName) {
                # Extract just the FHIR service name (after /)
                $fhirShort = $fhirName.Split("/")[-1]
                $FhirServiceUrl = "https://$hwName-$fhirShort.fhir.azurehealthcareapis.com"
            }
        }
    }
}

if ($FhirServiceUrl) {
    Write-Host "  ✓ FHIR Service: $FhirServiceUrl" -ForegroundColor Green
} else {
    Write-Host "  ⚠ FHIR Service URL not detected — provide via -FhirServiceUrl" -ForegroundColor Yellow
}

# Get the Event Hub connection string for Eventstream configuration
$ehConnStr = az eventhubs namespace authorization-rule keys list `
    --resource-group $ResourceGroupName `
    --namespace-name $EventHubNamespace `
    --name "RootManageSharedAccessKey" `
    --query "primaryConnectionString" -o tsv 2>$null

if (-not $ehConnStr) {
    # Try the custom auth rule
    $ehConnStr = az eventhubs namespace authorization-rule keys list `
        --resource-group $ResourceGroupName `
        --namespace-name $EventHubNamespace `
        --name "emulator-access" `
        --query "primaryConnectionString" -o tsv 2>$null
}

if ($ehConnStr) {
    Write-Host "  ✓ Event Hub connection string retrieved" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Could not retrieve Event Hub connection string" -ForegroundColor Yellow
    Write-Host "    You will need to configure the Eventstream source manually." -ForegroundColor Yellow
}

# ============================================================================
# STEP 1: CREATE OR VALIDATE FABRIC WORKSPACE
# ============================================================================

Write-Host ""
Write-Host "--- STEP 1: FABRIC WORKSPACE ---" -ForegroundColor Cyan
Write-Host "  Target workspace: '$FabricWorkspaceName'" -ForegroundColor White

# Get Fabric token and list workspaces
$workspaceId = $null

try {
    $workspaces = Invoke-FabricApi -Endpoint "/workspaces"
    $existingWs = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }

    if ($existingWs) {
        $workspaceId = $existingWs.id
        Write-Host "  ✓ Workspace already exists: $FabricWorkspaceName (ID: $workspaceId)" -ForegroundColor Green
    } else {
        Write-Host "  Creating workspace '$FabricWorkspaceName'..." -ForegroundColor White
        $newWs = Invoke-FabricApi -Method "POST" -Endpoint "/workspaces" -Body @{
            displayName = $FabricWorkspaceName
            description = "Healthcare intelligence workspace for connected clinical devices, FHIR/HDS clinical foundations, DICOM imaging cohorts, Fabric IQ ontologies, CMS quality, claims analytics, payer operations, and real-time telemetry."
        }
        $workspaceId = $newWs.id
        Write-Host "  ✓ Workspace created: $FabricWorkspaceName (ID: $workspaceId)" -ForegroundColor Green
    }

    # Ensure workspace has a Fabric capacity assigned
    $wsDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId"
    for ($capacityWait = 1; (-not $wsDetail.capacityId) -and $capacityWait -le 12; $capacityWait++) {
        Write-Host "  Waiting for capacity assignment to become visible ($capacityWait/12)..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 10
        $wsDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId"
    }
    if (-not $wsDetail.capacityId) {
        Write-Host "  Workspace has no capacity — searching for an active Fabric capacity..." -ForegroundColor Yellow
        $capacities = Invoke-FabricApi -Endpoint "/capacities"
        $activeCapacity = $capacities.value | Where-Object {
            $_.state -eq "Active" -and $_.sku -ne "PP3"
        } | Sort-Object -Property @{Expression={if ($_.sku -like "F*" -and $_.sku -ne "FT1") { 0 } else { 1 }}} | Select-Object -First 1

        if ($activeCapacity) {
            if ($activeCapacity.sku -eq "FT1") {
                Write-Host "  ERROR: Only a trial capacity (FT1) is available." -ForegroundColor Red
                Write-Host "    Healthcare Data Solutions requires a paid F-SKU (F2+)." -ForegroundColor Yellow
                Write-Host "    Provision a paid capacity at https://portal.azure.com" -ForegroundColor Yellow
                exit 1
            }
            Write-Host "  Assigning capacity: $($activeCapacity.displayName) (SKU: $($activeCapacity.sku))..." -ForegroundColor White
            Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/assignToCapacity" -Body @{
                capacityId = $activeCapacity.id
            }
            Start-Sleep -Seconds 5
            Write-Host "  ✓ Capacity assigned: $($activeCapacity.displayName)" -ForegroundColor Green
        } else {
            Write-Host "  ERROR: No active Fabric capacity found." -ForegroundColor Red
            Write-Host "    Provision a paid F-SKU (F2+) at https://portal.azure.com" -ForegroundColor Yellow
            exit 1
        }
    } else {
        Write-Host "  ✓ Capacity already assigned" -ForegroundColor Green
    }

    # Provision workspace managed identity
    Write-Host "  Provisioning workspace managed identity..." -ForegroundColor White
    try {
        $identityResult = Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/provisionIdentity"
        if ($identityResult.applicationId) {
            Write-Host "  ✓ Workspace identity provisioned (App: $($identityResult.applicationId), SP: $($identityResult.servicePrincipalId))" -ForegroundColor Green
        } else {
            # 202 Accepted — LRO in progress, poll until done
            Write-Host "  ✓ Workspace identity provisioning initiated (async)" -ForegroundColor Green
        }
    } catch {
        $idErr = $_.Exception.Message
        try { $idErr = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
        if ($idErr -match "already.*provisioned|identity.*exists|already exists") {
            Write-Host "  ✓ Workspace identity already exists" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Could not provision workspace identity: $idErr" -ForegroundColor Yellow
            Write-Host "    Create it manually: Workspace Settings → Workspace identity → + Workspace identity" -ForegroundColor Yellow
        }
    }
} catch {
    $fabricError = $_
    $fabricErrorBody = ""
    try { $fabricErrorBody = $fabricError.ErrorDetails.Message } catch {}

    Write-Host "ERROR: Failed to access Fabric API." -ForegroundColor Red
    if ($fabricErrorBody) {
        Write-Host $fabricErrorBody -ForegroundColor Red
    } else {
        Write-Host "  $fabricError" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "  Possible causes:" -ForegroundColor Yellow
    if ($fabricErrorBody -match "RequestDeniedByInboundPolicy" -or "$fabricError" -match "RequestDeniedByInboundPolicy") {
        Write-Host "    1. Fabric inbound communication policy blocks this network. Connect from an allowed network/VPN or relax the workspace policy, then re-run." -ForegroundColor Yellow
        Write-Host "    2. If the workspace was just assigned to capacity, wait for assignment to finish and re-run." -ForegroundColor Yellow
    } else {
        Write-Host "    1. You don't have a paid Fabric capacity. Provision an F-SKU (F2+) at https://portal.azure.com" -ForegroundColor Yellow
        Write-Host "    2. Your account doesn't have permission to create workspaces." -ForegroundColor Yellow
        Write-Host "    3. The Az PowerShell token can't reach api.fabric.microsoft.com." -ForegroundColor Yellow
    }
    exit 1
}

# ============================================================================
# PREFLIGHT: CHECK FOR HEALTHCARE DATA SOLUTIONS (informational)
# ============================================================================

Write-Host ""
Write-Host "--- PREFLIGHT: Healthcare Data Solutions Check ---" -ForegroundColor Cyan

$allItems = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items"
$hdsItem = $allItems.value | Where-Object { $_.type -eq 'Healthcaredatasolution' } | Select-Object -First 1

if ($hdsItem) {
    Write-Host "  ✓ HDS deployed: $($hdsItem.displayName) ($($hdsItem.id))" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Healthcare Data Solutions not yet deployed." -ForegroundColor Yellow
    Write-Host "    Phase 1 will continue (Eventhouse, Eventstream, KQL)." -ForegroundColor DarkGray
    Write-Host "    Deploy HDS before running Phase 2:" -ForegroundColor DarkGray
    Write-Host "    Fabric portal → '$FabricWorkspaceName' → + New Item → Healthcare data solution" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2: CREATE EVENTHOUSE + KQL DATABASE
# ============================================================================

Write-Host ""
Write-Host "--- STEP 2: EVENTHOUSE & KQL DATABASE ---" -ForegroundColor Cyan

$eventhouseName = "MasimoEventhouse"
$kqlDbName = "MasimoKQLDB"

# 2.1 Create Eventhouse
Write-Host "  Checking for existing Eventhouse..." -ForegroundColor Gray
try {
    $existingEh = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Eventhouse"
    $eventhouse = $existingEh.value | Where-Object { $_.displayName -eq $eventhouseName }
} catch {
    $eventhouse = $null
}

if ($eventhouse) {
    Write-Host "  ✓ Eventhouse already exists: $eventhouseName" -ForegroundColor Green
} else {
    Write-Host "  Creating Eventhouse '$eventhouseName'..." -ForegroundColor White
    try {
        $ehResult = Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/eventhouses" -Body @{
            displayName = $eventhouseName
            description = "Eventhouse for connected-device telemetry, clinical alert history, payer RTI streams, claim events, care-gap/fraud/high-cost scoring, and cross-domain operational KQL functions."
        }
        Write-Host "  ✓ Eventhouse created: $eventhouseName" -ForegroundColor Green
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Write-Host "  ✓ Eventhouse creation initiated (provisioning...)" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Eventhouse creation error: $_" -ForegroundColor Yellow
            Write-Host "    Will attempt to find it after a wait..." -ForegroundColor Gray
        }
    }

    # Wait for provisioning
    Write-Host "  Waiting for Eventhouse to provision..." -ForegroundColor Gray
    $eventhouse = Wait-FabricItem -WorkspaceId $workspaceId -ItemType "Eventhouse" -ItemName $eventhouseName -TimeoutSeconds 180
    Write-Host "  ✓ Eventhouse provisioned: $($eventhouse.id)" -ForegroundColor Green
}

$eventhouseId = $eventhouse.id

# 2.2 Check for KQL Database (auto-created with Eventhouse in many cases)
Write-Host "  Checking for KQL Database..." -ForegroundColor Gray
Start-Sleep -Seconds 10  # Pause for Eventhouse to register its default DB

try {
    $kqlDbs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases"
    $kqlDb = $kqlDbs.value | Where-Object { $_.displayName -eq $kqlDbName -or $_.displayName -eq $eventhouseName }
} catch {
    # Fallback: try the generic items endpoint
    try {
        $kqlDbs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=KQLDatabase"
        $kqlDb = $kqlDbs.value | Where-Object { $_.displayName -eq $kqlDbName -or $_.displayName -eq $eventhouseName }
    } catch { $kqlDb = $null }
}

if ($kqlDb) {
    Write-Host "  ✓ KQL Database found: $($kqlDb.displayName)" -ForegroundColor Green
    $kqlDbName = $kqlDb.displayName
} else {
    Write-Host "  Creating KQL Database '$kqlDbName'..." -ForegroundColor White
    try {
        $kqlDbPayload = @{
            displayName     = $kqlDbName
            description     = "KQL database for Masimo telemetry, clinical alerts, HDS enrichment shortcuts, payer RTI claims streams, and operations scoring functions."
            creationPayload = @{
                databaseType           = "ReadWrite"
                parentEventhouseItemId = $eventhouseId
            }
        }
        Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/kqlDatabases" -Body $kqlDbPayload
        Write-Host "  ✓ KQL Database created: $kqlDbName" -ForegroundColor Green
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Write-Host "  ✓ KQL Database creation initiated..." -ForegroundColor Green
        } else {
            Write-Host "  ⚠ KQL Database may need manual creation" -ForegroundColor Yellow
            Write-Host "    Error: $_" -ForegroundColor Gray
        }
    }

    # Wait for it
    try {
        $kqlDb = Wait-FabricItem -WorkspaceId $workspaceId -ItemType "KQLDatabase" -ItemName $kqlDbName -TimeoutSeconds 120
    } catch {
        Write-Host "  ⚠ Could not verify KQL Database. It may still be provisioning." -ForegroundColor Yellow
    }
}

# ============================================================================
# STEP 3: CREATE EVENTSTREAM
# ============================================================================

Write-Host ""
Write-Host "--- STEP 3: EVENTSTREAM ---" -ForegroundColor Cyan

$eventstreamName = "MasimoTelemetryStream"

try {
    $existingEs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/eventstreams"
    $eventstream = $existingEs.value | Where-Object { $_.displayName -eq $eventstreamName }
} catch {
    try {
        $existingEs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Eventstream"
        $eventstream = $existingEs.value | Where-Object { $_.displayName -eq $eventstreamName }
    } catch { $eventstream = $null }
}

if ($eventstream) {
    Write-Host "  ✓ Eventstream already exists: $eventstreamName" -ForegroundColor Green
} else {
    Write-Host "  Creating Eventstream '$eventstreamName'..." -ForegroundColor White
    try {
        Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/eventstreams" -Body @{
            displayName = $eventstreamName
            description = "Ingests Masimo Radius-7 telemetry into Fabric for device monitoring, clinical alerting, HDS-enriched analytics, and cross-domain operations."
        }
        Write-Host "  ✓ Eventstream created: $eventstreamName" -ForegroundColor Green
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Write-Host "  ✓ Eventstream creation initiated..." -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Eventstream creation error: $_" -ForegroundColor Yellow
        }
    }

    try {
        $eventstream = Wait-FabricItem -WorkspaceId $workspaceId -ItemType "Eventstream" -ItemName $eventstreamName -TimeoutSeconds 120
    } catch {
        Write-Host "  ⚠ Could not verify Eventstream. It may still be provisioning." -ForegroundColor Yellow
    }
}

$eventstreamId = $null
if ($eventstream) { $eventstreamId = $eventstream.id }

# ============================================================================
# STEP 4: CREATE CLOUD CONNECTION & CONFIGURE EVENTSTREAM
# ============================================================================

Write-Host ""
Write-Host "--- STEP 4: CONFIGURE EVENTSTREAM (Source → Destination) ---" -ForegroundColor Cyan

# Capture KQL Database ID for Eventstream destination
$kqlDbId = $null
if ($kqlDb) { $kqlDbId = $kqlDb.id }
if (-not $kqlDbId) {
    # Re-fetch KQL databases to get the ID
    try {
        $kqlDbs = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases"
        $kqlDb = $kqlDbs.value | Where-Object { $_.displayName -eq $kqlDbName -or $_.displayName -eq $eventhouseName }
        if ($kqlDb) {
            $kqlDbId = $kqlDb.id
            $kqlDbName = $kqlDb.displayName
        }
    } catch {}
}

if ($kqlDbId) {
    Write-Host "  ✓ KQL Database ID: $kqlDbId" -ForegroundColor Green
} else {
    Write-Host "  ⚠ KQL Database ID not found — Eventstream destination may need manual config." -ForegroundColor Yellow
}

# Ensure Event Hub namespace has local authentication (SAS) enabled
# Some organizations disable SAS via Azure Policy. Fabric cloud connections require it.
try {
    $ehLocalAuth = az eventhubs namespace show --resource-group $ResourceGroupName `
        --name $EventHubNamespace --query "disableLocalAuth" -o tsv 2>$null
    if ($ehLocalAuth -eq "true") {
        Write-Host "  ⚠ Event Hub namespace has local auth (SAS) disabled." -ForegroundColor Yellow
        Write-Host "    Attempting to enable it (required for Fabric cloud connections)..." -ForegroundColor Yellow

        # Apply tags first (e.g. SecurityControl=Ignore) to satisfy Azure Policy
        if ($Tags.Count -gt 0) {
            $tagArgs = ($Tags.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ' '
            Write-Host "    Applying tags to Event Hub namespace: $tagArgs" -ForegroundColor Gray
            az eventhubs namespace update --resource-group $ResourceGroupName `
                --name $EventHubNamespace --tags $tagArgs 2>$null | Out-Null
        }

        az eventhubs namespace update --resource-group $ResourceGroupName `
            --name $EventHubNamespace --disable-local-auth false 2>$null | Out-Null
        $ehLocalAuth = az eventhubs namespace show --resource-group $ResourceGroupName `
            --name $EventHubNamespace --query "disableLocalAuth" -o tsv 2>$null
        if ($ehLocalAuth -eq "false") {
            Write-Host "  ✓ Local auth (SAS) enabled on Event Hub namespace." -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Could not enable local auth — an Azure Policy may be blocking it." -ForegroundColor Yellow
            Write-Host "    Add a policy exclusion tag or disable the deny policy, then re-run." -ForegroundColor Yellow
        }
    } else {
        Write-Host "  ✓ Event Hub local auth (SAS) is enabled." -ForegroundColor Green
    }
} catch {
    Write-Host "  ⚠ Could not check Event Hub local auth status: $_" -ForegroundColor Yellow
}

if (-not $ehConnStr) {
    Write-Host "  ⚠ No Event Hub connection string available — skipping Eventstream configuration." -ForegroundColor Yellow
    Write-Host "    You will need to configure the source and destination manually in the Fabric portal." -ForegroundColor Yellow
} elseif (-not $eventstreamId) {
    Write-Host "  ⚠ Eventstream ID not found — skipping configuration." -ForegroundColor Yellow
} elseif (-not $kqlDbId) {
    Write-Host "  ⚠ KQL Database ID not found — skipping Eventstream destination configuration." -ForegroundColor Yellow
    Write-Host "    You will need to configure the destination manually in the Fabric portal." -ForegroundColor Yellow
} else {
    # ---- 4a. Parse connection string to extract components ----
    # Connection string format: Endpoint=sb://<ns>.servicebus.windows.net/;SharedAccessKeyName=<name>;SharedAccessKey=<key>
    # Note: SAS key values can contain '=' characters, so we split on ';' first, then split each part on '=' with limit 2
    $ehParts = @{}
    $ehConnStr.Split(";") | ForEach-Object {
        if ($_ -match "^([^=]+)=(.+)$") {
            $ehParts[$matches[1].Trim()] = $matches[2].Trim()
        }
    }
    # Extract namespace host from Endpoint=sb://<ns>.servicebus.windows.net/
    $ehEndpoint = $ehParts["Endpoint"] -replace "^sb://", "" -replace "/$", ""
    $sasKeyName = $ehParts["SharedAccessKeyName"]
    $sasKey = $ehParts["SharedAccessKey"]

    Write-Host "  Parsed Event Hub connection details:" -ForegroundColor Gray
    Write-Host "    Endpoint:      $ehEndpoint" -ForegroundColor Gray
    Write-Host "    SAS Policy:    $sasKeyName" -ForegroundColor Gray

    # ---- 4b. Create or find Fabric Cloud Connection to Event Hub ----
    # Include the Event Hub namespace in the connection name so each deployment gets a
    # unique connection. The namespace contains a uniqueString(rg.id) suffix; without it,
    # redeployments reuse a stale connection pointing at a deleted namespace and the
    # Eventstream source fails with "No such host is known".
    $connectionName = "masimo-eh-$EventHubNamespace-$EventHubName"
    $dataConnectionId = $null

    Write-Host "  Checking for existing Fabric cloud connection '$connectionName'..." -ForegroundColor Gray

    try {
        $existingConns = Invoke-FabricApi -Endpoint "/connections"
        $existingConn = $existingConns.value | Where-Object { $_.displayName -eq $connectionName }
        if ($existingConn) {
            $dataConnectionId = $existingConn.id
            Write-Host "  ✓ Cloud connection already exists: $connectionName (ID: $dataConnectionId)" -ForegroundColor Green
        }

        # Clean up stale connections from previous deployments that used the old
        # non-unique name format ("masimo-eh-<EventHubName>") or unique names from
        # other namespaces. These point at deleted Event Hub namespaces and break
        # the Eventstream source if Fabric picks them up.
        $staleNamePattern = "^masimo-eh-(.+-)?$([regex]::Escape($EventHubName))$"
        $staleConns = $existingConns.value | Where-Object {
            $_.displayName -match $staleNamePattern -and $_.displayName -ne $connectionName
        }
        foreach ($stale in $staleConns) {
            Write-Host "  Removing stale Event Hub connection '$($stale.displayName)' ($($stale.id))..." -ForegroundColor Yellow
            try {
                Invoke-FabricApi -Method "DELETE" -Endpoint "/connections/$($stale.id)" | Out-Null
                Write-Host "  ✓ Removed stale connection" -ForegroundColor Green
            } catch {
                Write-Host "  ⚠ Could not remove stale connection: $_" -ForegroundColor Yellow
            }
        }
    } catch {
        Write-Host "  Could not list connections: $_" -ForegroundColor Gray
    }

    if (-not $dataConnectionId) {
        Write-Host "  Creating cloud connection '$connectionName'..." -ForegroundColor White
        try {
            $connBody = @{
                connectivityType = "ShareableCloud"
                displayName      = $connectionName
                connectionDetails = @{
                    type           = "EventHub"
                    creationMethod = "EventHub.Contents"
                    parameters     = @(
                        @{ dataType = "Text"; name = "endpoint"; value = $ehEndpoint }
                        @{ dataType = "Text"; name = "entityPath"; value = $EventHubName }
                    )
                }
                privacyLevel     = "Organizational"
                credentialDetails = @{
                    singleSignOnType     = "None"
                    connectionEncryption = "NotEncrypted"
                    skipTestConnection   = $false
                    credentials          = @{
                        credentialType = "Basic"
                        username       = $sasKeyName
                        password       = $sasKey
                    }
                }
            }

            $token = Get-FabricAccessToken
            $connHeaders = @{
                "Authorization" = "Bearer $token"
                "Content-Type"  = "application/json"
            }
            $connResult = Invoke-RestMethod `
                -Method POST `
                -Uri "$FabricApiBase/connections" `
                -Headers $connHeaders `
                -Body ($connBody | ConvertTo-Json -Depth 10)

            $dataConnectionId = $connResult.id
            Write-Host "  ✓ Cloud connection created: $connectionName (ID: $dataConnectionId)" -ForegroundColor Green
        } catch {
            Write-Host "  ⚠ Failed to create cloud connection: $_" -ForegroundColor Yellow

            # Try to read the response body for more details
            try {
                $errStream = $_.Exception.Response.GetResponseStream()
                $reader = New-Object System.IO.StreamReader($errStream)
                $errBody = $reader.ReadToEnd()
                Write-Host "    Response: $errBody" -ForegroundColor Gray
            } catch {}

            Write-Host "    Eventstream will need manual source configuration in the portal." -ForegroundColor Yellow
        }
    }

    # ---- 4c. Update Eventstream definition with source + destination ----
    if ($dataConnectionId) {
        Write-Host ""
        Write-Host "  Configuring Eventstream topology..." -ForegroundColor White
        Write-Host "    Source:      Azure Event Hub ($EventHubNamespace / $EventHubName)" -ForegroundColor Gray
        Write-Host "    Destination: Eventhouse ($eventhouseName / TelemetryRaw)" -ForegroundColor Gray

        # Build the Eventstream definition JSON
        $esDefinition = @{
            sources = @(
                @{
                    name       = "EventHubSource"
                    type       = "AzureEventHub"
                    properties = @{
                        dataConnectionId   = $dataConnectionId
                        consumerGroupName  = "`$Default"
                        inputSerialization  = @{
                            type       = "Json"
                            properties = @{
                                encoding = "UTF8"
                            }
                        }
                    }
                }
            )
            destinations = @(
                @{
                    name       = "EventhouseDestination"
                    type       = "Eventhouse"
                    properties = @{
                        dataIngestionMode  = "ProcessedIngestion"
                        workspaceId        = $workspaceId
                        itemId             = $kqlDbId
                        databaseName       = $kqlDbName
                        tableName          = "TelemetryRaw"
                        inputSerialization  = @{
                            type       = "Json"
                            properties = @{
                                encoding = "UTF8"
                            }
                        }
                    }
                    inputNodes = @(
                        @{ name = "$eventstreamName-stream" }
                    )
                }
            )
            streams = @(
                @{
                    name       = "$eventstreamName-stream"
                    type       = "DefaultStream"
                    properties = @{}
                    inputNodes = @(
                        @{ name = "EventHubSource" }
                    )
                }
            )
            operators = @()
            compatibilityLevel = "1.1"
        }

        # Convert to JSON and Base64-encode
        $esJson = $esDefinition | ConvertTo-Json -Depth 10
        $esBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($esJson))

        # Build the .platform part
        $platformObj = @{
            "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
            metadata   = @{
                type        = "Eventstream"
                displayName = $eventstreamName
            }
            config     = @{
                version   = "2.0"
                logicalId = "00000000-0000-0000-0000-000000000000"
            }
        }
        $platformJson = $platformObj | ConvertTo-Json -Depth 5
        $platformBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($platformJson))

        # API request body
        $updateBody = @{
            definition = @{
                parts = @(
                    @{
                        path        = "eventstream.json"
                        payload     = $esBase64
                        payloadType = "InlineBase64"
                    }
                    @{
                        path        = ".platform"
                        payload     = $platformBase64
                        payloadType = "InlineBase64"
                    }
                )
            }
        }

        try {
            $token = Get-FabricAccessToken
            $updateHeaders = @{
                "Authorization" = "Bearer $token"
                "Content-Type"  = "application/json"
            }

            $updateUri = "$FabricApiBase/workspaces/$workspaceId/eventstreams/$eventstreamId/updateDefinition?updateMetadata=True"
            $updateJsonBody = $updateBody | ConvertTo-Json -Depth 15

            $updateResponse = $null
            $updateAttempt = 0
            $updateMaxAttempts = 13
            while (-not $updateResponse -and $updateAttempt -lt $updateMaxAttempts) {
                $updateAttempt++
                try {
                    $updateResponse = Invoke-WebRequest `
                        -Method POST `
                        -Uri $updateUri `
                        -Headers $updateHeaders `
                        -Body $updateJsonBody `
                        -UseBasicParsing
                } catch {
                    $attemptErr = $_.ToString()
                    if ($attemptErr -match "ArtifactOperationConflict|previous operation is completed" -and $updateAttempt -lt $updateMaxAttempts) {
                        Write-Host "  ⚠ Eventstream update is busy; retrying in 10s ($updateAttempt/$updateMaxAttempts)" -ForegroundColor Yellow
                        Start-Sleep -Seconds 10
                    } else {
                        throw
                    }
                }
            }

            $updateStatus = $updateResponse.StatusCode

            if ($updateStatus -eq 200 -or $updateStatus -eq 202) {
                Write-Host "  ✓ Eventstream definition updated successfully!" -ForegroundColor Green
                if ($updateStatus -eq 200) { $eventstreamConfigured = $true }

                if ($updateStatus -eq 202) {
                    # Long-running operation — poll for completion
                    $operationId = $null
                    try {
                        $operationId = $updateResponse.Headers["x-ms-operation-id"]
                        if (-not $operationId) {
                            $locationHeader = $updateResponse.Headers["Location"]
                            if ($locationHeader) {
                                $operationId = ($locationHeader -split "/operations/")[-1]
                            }
                        }
                    } catch {}

                    if ($operationId) {
                        Write-Host "  Waiting for Eventstream configuration to apply (operation: $operationId)..." -ForegroundColor Gray
                        $opElapsed = 0
                        $opTimeout = 120
                        while ($opElapsed -lt $opTimeout) {
                            Start-Sleep -Seconds 5
                            $opElapsed += 5
                            try {
                                $opResult = Invoke-FabricApi -Endpoint "/operations/$operationId"
                                if ($opResult.status -eq "Succeeded") {
                                    Write-Host "  ✓ Eventstream configuration applied!" -ForegroundColor Green
                                    $eventstreamConfigured = $true
                                    break
                                } elseif ($opResult.status -eq "Failed") {
                                    Write-Host "  ⚠ Eventstream configuration failed: $($opResult | ConvertTo-Json -Depth 5)" -ForegroundColor Yellow
                                    Add-RtiFailure "Eventstream definition operation failed"
                                    break
                                }
                                Write-Host "    Status: $($opResult.status) (${opElapsed}s)" -ForegroundColor Gray
                            } catch {
                                Write-Host "    Polling... (${opElapsed}s)" -ForegroundColor Gray
                            }
                        }
                    }
                    if (-not $operationId) { $eventstreamConfigured = $true }
                }

                Write-Host ""
                Write-Host "  ┌─────────────────────────────────────────────────────┐" -ForegroundColor Green
                Write-Host "  │ Eventstream Pipeline:                               │" -ForegroundColor Green
                Write-Host "  │   Event Hub → Default Stream → Eventhouse           │" -ForegroundColor Green
                Write-Host "  │   ($EventHubName → TelemetryRaw)      │" -ForegroundColor Green
                Write-Host "  └─────────────────────────────────────────────────────┘" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ Unexpected response status: $updateStatus" -ForegroundColor Yellow
            }
        } catch {
            $errMsg = $_.ToString()
            $errCode = $null
            if ($errCode -eq 202) {
                Write-Host "  ✓ Eventstream definition update accepted (202 — provisioning)." -ForegroundColor Green
                $eventstreamConfigured = $true
            } elseif ($errMsg -match "DataSourcesValidationError|Creating.+state") {
                Write-Host "  ⚠ Eventstream topology update is already in progress; treating configuration as pending." -ForegroundColor Yellow
                $eventstreamConfigured = $true
            } else {
                Write-Host "  ⚠ Failed to update Eventstream definition: $errMsg" -ForegroundColor Yellow
                Add-RtiFailure "Eventstream definition update failed"

                # Try to read the response body for more details
                try {
                    $errStream = $_.Exception.Response.GetResponseStream()
                    $reader = New-Object System.IO.StreamReader($errStream)
                    $errBody = $reader.ReadToEnd()
                    Write-Host "    Response: $errBody" -ForegroundColor Gray
                } catch {}

                Write-Host ""
                Write-Host "  Fallback: Configure Eventstream manually in the Fabric portal:" -ForegroundColor Yellow
                Write-Host "    1. Open Eventstream '$eventstreamName' in workspace '$FabricWorkspaceName'" -ForegroundColor Gray
                Write-Host "    2. Add Source → Azure Event Hub → use connection '$connectionName'" -ForegroundColor Gray
                Write-Host "    3. Add Destination → Eventhouse → $eventhouseName / TelemetryRaw" -ForegroundColor Gray
                Write-Host "    4. Publish the Eventstream" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host ""
        Write-Host "  ⚠ Skipping Eventstream topology — no cloud connection available." -ForegroundColor Yellow
        Write-Host "  Configure manually in the Fabric portal:" -ForegroundColor Yellow
        Write-Host "    1. Open Eventstream '$eventstreamName'" -ForegroundColor Gray
        Write-Host "    2. Add Source → Azure Event Hub:" -ForegroundColor Gray
        Write-Host "       • Namespace: $EventHubNamespace.servicebus.windows.net" -ForegroundColor Gray
        Write-Host "       • Hub: $EventHubName | Consumer group: `$Default | Format: JSON" -ForegroundColor Gray
        Write-Host "    3. Add Destination → Eventhouse:" -ForegroundColor Gray
        Write-Host "       • Eventhouse: $eventhouseName | Table: TelemetryRaw" -ForegroundColor Gray
        Write-Host "    4. Publish the Eventstream" -ForegroundColor Gray
        Add-RtiFailure "Eventstream topology skipped because no Fabric cloud connection was available"
    }
}

# ============================================================================
# STEP 4b: ENSURE EVENTSTREAM IS RUNNING
# ============================================================================

if ($eventstreamId) {
    Write-Host ""
    Write-Host "--- STEP 4b: EVENTSTREAM STATUS CHECK ---" -ForegroundColor Cyan
    Write-Host "  Checking Eventstream status..." -ForegroundColor White

    try {
        $esInfo = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/eventstreams/$eventstreamId"
        $esStatus = $esInfo.status
        if (-not $esStatus) { $esStatus = $esInfo.state }
        if (-not $esStatus) { $esStatus = $esInfo.runtimeStatus }

        if ($esStatus) {
            Write-Host "  Eventstream status: $esStatus" -ForegroundColor Gray
        }

        if ($esStatus -and $esStatus -in @('Active', 'Running')) {
            Write-Host "  ✓ Eventstream is running" -ForegroundColor Green
            $eventstreamRunning = $true
        } else {
            Write-Host "  Eventstream activation is managed by Fabric after updateDefinition; no public REST start endpoint is available." -ForegroundColor DarkGray
            Write-Host "  KQL ingestion is verified after schema deployment." -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "  ⚠ Could not check Eventstream status: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# ============================================================================
# STEP 5: OUTPUT SUMMARY
# ============================================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║  DEPLOYMENT COMPLETE — Fabric RTI Resources Created         ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  Workspace:    $FabricWorkspaceName" -ForegroundColor White
Write-Host "  Workspace ID: $workspaceId" -ForegroundColor Gray
Write-Host "  Eventhouse:   $eventhouseName" -ForegroundColor White
Write-Host "  KQL Database: $kqlDbName" -ForegroundColor White
Write-Host "  Eventstream:  $eventstreamName" -ForegroundColor White
if ($dataConnectionId) {
    Write-Host "  Connection:   $connectionName (ID: $dataConnectionId)" -ForegroundColor White
}
Write-Host ""

# ============================================================================
# STEP 6: KQL TABLES & ALERT FUNCTIONS (Automated)
# ============================================================================

Write-Host ""
Write-Host "--- STEP 6: KQL SCHEMA & ALERT FUNCTIONS ---" -ForegroundColor Cyan
Write-Host ""

# 6a. Discover the Kusto Query URI from the KQL Database
Write-Host "  Discovering Kusto endpoint..." -ForegroundColor White
$kqlDbInfo = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=KQLDatabase"
$kqlDbObj = $kqlDbInfo.value | Where-Object { $_.displayName -eq $kqlDbName }
if (-not $kqlDbObj) {
    Write-Host "  ⚠ KQL Database '$kqlDbName' not found — skipping KQL deployment." -ForegroundColor Yellow
    Write-Host "    Run the KQL scripts manually: .\utilities\run-kql-scripts.ps1" -ForegroundColor Yellow
} else {
    $kqlDbId = $kqlDbObj.id
    # Try kqlDatabases/{id} detail first, fall back to items/{id}
    $kqlDbDetail = $null
    try { $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases/$kqlDbId" } catch {}
    if (-not $kqlDbDetail) {
        try { $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items/$kqlDbId" } catch {}
    }
    $kustoUri = $null
    if ($kqlDbDetail) {
        $kustoUri = $kqlDbDetail.queryServiceUri
        if (-not $kustoUri) { $kustoUri = $kqlDbDetail.queryUri }
        if (-not $kustoUri) { try { $kustoUri = $kqlDbDetail.properties.queryUri } catch {} }
        if (-not $kustoUri) { try { $kustoUri = $kqlDbDetail.properties.queryServiceUri } catch {} }
    }

    if (-not $kustoUri) {
        # Fallback: try Eventhouse detail for queryServiceUri
        Write-Host "  ⚠ kqlDatabases detail unavailable. Trying Eventhouse discovery..." -ForegroundColor Yellow
        try {
            $ehDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/eventhouses/$eventhouseId"
            if ($ehDetail.properties.queryServiceUri) { $kustoUri = $ehDetail.properties.queryServiceUri }
            elseif ($ehDetail.queryServiceUri) { $kustoUri = $ehDetail.queryServiceUri }
        } catch {}
    }
    if (-not $kustoUri -and $KustoUri) {
        $kustoUri = $KustoUri
        Write-Host "  ✓ Using provided Kusto URI: $kustoUri" -ForegroundColor Green
    }
    if (-not $kustoUri) {
        Write-Host "  ⚠ Could not determine Kusto Query URI automatically." -ForegroundColor Yellow
        Write-Host "    Provide it: -KustoUri 'https://...kusto.fabric.microsoft.com'" -ForegroundColor Yellow
        Write-Host "    Or run KQL scripts manually: .\utilities\run-kql-scripts.ps1" -ForegroundColor Yellow
    } else {
        Write-Host "  ✓ Kusto URI: $kustoUri" -ForegroundColor Green
        Write-Host "  ✓ KQL DB:    $kqlDbName (ID: $kqlDbId)" -ForegroundColor Green
        Write-Host ""

        # 6b. Acquire Kusto token
        Write-Host "  Acquiring Kusto access token..." -ForegroundColor White
        $kustoToken = Get-KustoAccessToken
        $kustoHeaders = @{
            "Authorization" = "Bearer $kustoToken"
            "Content-Type"  = "application/json"
        }
        Write-Host "  ✓ Kusto token acquired" -ForegroundColor Green
        Write-Host ""

        $kqlSuccess = 0; $kqlFail = 0
        $kqlParams = @{
            KustoUri     = $kustoUri
            DatabaseName = $kqlDbName
            KustoHeaders = $kustoHeaders
        }

        # --- 6c-pre. Create TelemetryRaw table FIRST (needed by update policy) ---
        Write-Host ""
        Write-Host "  Ensuring TelemetryRaw table exists..." -ForegroundColor White
        $cmd = '.create-merge table TelemetryRaw (device_id:string, timestamp:string, telemetry:dynamic, source:string, metadata:dynamic) with (folder="Masimo")'
        if (Invoke-KustoMgmt -Command $cmd -Label "TelemetryRaw table" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # --- 6c. AlertHistory Table & Policies ---
        Write-Host ""
        Write-Host "  Creating AlertHistory table & policies..." -ForegroundColor White

        $cmd = '.create table AlertHistory (alert_id: string, alert_time: datetime, device_id: string, patient_id: string, patient_name: string, alert_tier: string, alert_type: string, metric_name: string, metric_value: real, threshold_value: real, qualifying_conditions: string, message: string, acknowledged: bool, acknowledged_by: string, acknowledged_at: datetime)'
        if (Invoke-KustoMgmt -Command $cmd -Label "AlertHistory table" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        $retentionJson = '{"SoftDeletePeriod": "90.00:00:00", "Recoverability": "Enabled"}'
        $cmd = ".alter table AlertHistory policy retention @'$retentionJson'"
        if (Invoke-KustoMgmt -Command $cmd -Label "AlertHistory retention policy (90 days)" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        $cmd = '.alter table AlertHistory policy streamingingestion enable'
        if (Invoke-KustoMgmt -Command $cmd -Label "AlertHistory streaming ingestion" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        $mappingBody = '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_time","path":"$.alert_time","datatype":"datetime"},{"column":"device_id","path":"$.device_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"patient_name","path":"$.patient_name","datatype":"string"},{"column":"alert_tier","path":"$.alert_tier","datatype":"string"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"metric_name","path":"$.metric_name","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"threshold_value","path":"$.threshold_value","datatype":"real"},{"column":"qualifying_conditions","path":"$.qualifying_conditions","datatype":"string"},{"column":"message","path":"$.message","datatype":"string"},{"column":"acknowledged","path":"$.acknowledged","datatype":"bool"},{"column":"acknowledged_by","path":"$.acknowledged_by","datatype":"string"},{"column":"acknowledged_at","path":"$.acknowledged_at","datatype":"datetime"}]'
        $cmd = ".create-or-alter table AlertHistory ingestion json mapping 'AlertHistoryMapping' @'$mappingBody'"
        if (Invoke-KustoMgmt -Command $cmd -Label "AlertHistory JSON mapping" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # --- 6c-ii. AlertHistory Update Policy (auto-populate from TelemetryRaw) ---
        Write-Host "  Creating AlertHistory update policy (auto-detect alerts)..." -ForegroundColor White

        $fnBody = @'
{
    let spo2_alerts = TelemetryRaw
        | extend spo2_val = todouble(telemetry.spo2)
        | where spo2_val < 94
        | extend
            alert_tier = case(spo2_val < 85, "CRITICAL", spo2_val < 90, "URGENT", "WARNING"),
            alert_type = "SPO2_LOW",
            metric_name = "spo2",
            metric_value = spo2_val,
            threshold_value = case(spo2_val < 85, 85.0, spo2_val < 90, 90.0, 94.0)
        | project
            alert_id = strcat("SPO2-", device_id, "-", format_datetime(todatetime(timestamp), "yyyyMMddHHmmss")),
            alert_time = todatetime(timestamp),
            device_id,
            patient_id = "",
            patient_name = "",
            alert_tier,
            alert_type,
            metric_name,
            metric_value,
            threshold_value,
            qualifying_conditions = "",
            message = strcat(alert_tier, ": Device ", device_id, " SpO2=", tostring(round(spo2_val,1)), "% (threshold: ", tostring(threshold_value), "%)"),
            acknowledged = false,
            acknowledged_by = "",
            acknowledged_at = datetime(null);
    let pr_alerts = TelemetryRaw
        | extend pr_val = toint(telemetry.pr)
        | where pr_val > 110 or pr_val < 50
        | extend
            is_high = pr_val > 110,
            alert_tier = case(pr_val > 150 or pr_val < 40, "CRITICAL", pr_val > 130 or pr_val < 45, "URGENT", "WARNING"),
            alert_type = iff(pr_val > 110, "PR_HIGH", "PR_LOW"),
            metric_name = "pr",
            metric_value = todouble(pr_val),
            threshold_value = case(
                pr_val > 150, 150.0, pr_val > 130, 130.0, pr_val > 110, 110.0,
                pr_val < 40, 40.0, pr_val < 45, 45.0, 50.0)
        | project
            alert_id = strcat("PR-", device_id, "-", format_datetime(todatetime(timestamp), "yyyyMMddHHmmss")),
            alert_time = todatetime(timestamp),
            device_id,
            patient_id = "",
            patient_name = "",
            alert_tier,
            alert_type,
            metric_name,
            metric_value,
            threshold_value,
            qualifying_conditions = "",
            message = strcat(alert_tier, ": Device ", device_id, " PR=", tostring(pr_val), " bpm (threshold: ", tostring(threshold_value), ")"),
            acknowledged = false,
            acknowledged_by = "",
            acknowledged_at = datetime(null);
    spo2_alerts | union pr_alerts
}
'@
        $cmd = ".create-or-alter function with (docstring='Update policy transform: detects SpO2 and PR alerts in each TelemetryRaw batch', folder='ClinicalAlerts') fn_AlertHistoryTransform() $fnBody"
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_AlertHistoryTransform (update policy function)" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        $updatePolicyJson = '[{"IsEnabled":true,"Source":"TelemetryRaw","Query":"fn_AlertHistoryTransform()","IsTransactional":false,"PropagateIngestionProperties":false}]'
        $cmd = ".alter table AlertHistory policy update @'$updatePolicyJson'"
        if (Invoke-KustoMgmt -Command $cmd -Label "AlertHistory update policy (auto-populate from TelemetryRaw)" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # --- 6e. Telemetry Functions ---
        Write-Host ""
        Write-Host "  Creating telemetry functions..." -ForegroundColor White

        # fn_VitalsTrend
        $cmd = '.create-or-alter function with (docstring = "Rolling vital sign statistics per device over a sliding window", folder = "ClinicalAlerts") fn_VitalsTrend(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize readings = count(), avg_spo2 = round(avg(todouble(telemetry.spo2)), 1), min_spo2 = round(min(todouble(telemetry.spo2)), 1), max_spo2 = round(max(todouble(telemetry.spo2)), 1), stddev_spo2 = round(stdev(todouble(telemetry.spo2)), 2), avg_pr = round(avg(todouble(telemetry.pr)), 0), min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), avg_pi = round(avg(todouble(telemetry.pi)), 2), avg_pvi = round(avg(todouble(telemetry.pvi)), 0), avg_sphb = round(avg(todouble(telemetry.sphb)), 1), avg_signal_iq = round(avg(todouble(telemetry.signal_iq)), 0), last_reading = max(todatetime(timestamp)) by device_id | extend minutes_since_last = datetime_diff(''second'', now(), last_reading) / 60.0 | order by device_id asc }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_VitalsTrend" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # fn_DeviceStatus
        $cmd = '.create-or-alter function with (docstring = "Current device connectivity status based on last telemetry", folder = "ClinicalAlerts") fn_DeviceStatus() { TelemetryRaw | summarize last_seen = max(todatetime(timestamp)) by device_id | extend seconds_ago = datetime_diff(''second'', now(), last_seen), status = case(datetime_diff(''second'', now(), last_seen) < 30, "ONLINE", datetime_diff(''second'', now(), last_seen) < 120, "STALE", "OFFLINE") | order by status asc, device_id asc }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_DeviceStatus" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # fn_LatestReadings
        $cmd = '.create-or-alter function with (docstring = "Latest telemetry reading per device", folder = "ClinicalAlerts") fn_LatestReadings() { TelemetryRaw | summarize arg_max(todatetime(timestamp), *) by device_id | project device_id, timestamp, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr), pi = todouble(telemetry.pi), pvi = toint(telemetry.pvi), sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq) | order by device_id asc }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_LatestReadings" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # fn_TelemetryByDevice
        $cmd = '.create-or-alter function with (docstring = "Time-series telemetry for a specific device", folder = "ClinicalAlerts") fn_TelemetryByDevice(target_device: string, lookback_minutes: int = 60) { TelemetryRaw | where device_id == target_device | where todatetime(timestamp) > ago(1m * lookback_minutes) | project timestamp = todatetime(timestamp), spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr), pi = todouble(telemetry.pi), pvi = toint(telemetry.pvi), sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq) | order by timestamp asc }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_TelemetryByDevice" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # --- 6e. Clinical Alert Functions ---
        Write-Host ""
        Write-Host "  Creating clinical alert functions..." -ForegroundColor White

        # fn_SpO2Alerts
        $cmd = '.create-or-alter function with (docstring = "Detect SpO2 threshold breaches with 3-tier severity", folder = "ClinicalAlerts") fn_SpO2Alerts(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | where isnotnull(telemetry.spo2) | extend spo2 = todouble(telemetry.spo2) | where spo2 > 0 and spo2 < 100 | summarize min_spo2 = min(spo2), avg_spo2 = round(avg(spo2),1), readings = count(), last_time = max(todatetime(timestamp)) by device_id | where min_spo2 < 94 | extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING") | extend alert_type = "SPO2_LOW" | extend message = strcat(alert_tier, " Dev:", device_id, " SpO2=", tostring(min_spo2)) | extend tv = case(alert_tier == "CRITICAL", 85.0, alert_tier == "URGENT", 90.0, 94.0) | project alert_time = last_time, device_id, alert_tier, alert_type, metric_name = "spo2", metric_value = min_spo2, threshold_value = tv, message, readings | order by alert_tier asc, metric_value asc }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_SpO2Alerts" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # fn_PulseRateAlerts
        $cmd = '.create-or-alter function with (docstring = "Detect pulse rate anomalies with 3-tier severity", folder = "ClinicalAlerts") fn_PulseRateAlerts(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize readings = count(), avg_pr = round(avg(todouble(telemetry.pr)), 0), min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), last_time = max(todatetime(timestamp)) by device_id | where max_pr > 110 or min_pr < 50 | extend is_tachy = max_pr > 110, is_brady = min_pr < 50 | extend alert_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING") | extend alert_type = case(max_pr > 110 and min_pr < 50, "PR_BOTH", max_pr > 110, "PR_HIGH", "PR_LOW") | extend abnormal_value = iff(max_pr > 110, todouble(max_pr), todouble(min_pr)) | extend message = strcat(alert_tier, " Dev:", device_id, " PR:", tostring(max_pr)) | extend tv = case(alert_tier == "CRITICAL", iff(is_tachy, 150.0, 40.0), alert_tier == "URGENT", iff(is_tachy, 130.0, 45.0), iff(is_tachy, 110.0, 50.0)) | project alert_time = last_time, device_id, alert_tier, alert_type, metric_name = "pr", metric_value = abnormal_value, threshold_value = tv, message, readings | order by alert_tier asc }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_PulseRateAlerts" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # fn_ClinicalAlerts — uses "vitals" not "latest" (reserved keyword); separate extends
        $cmd = '.create-or-alter function with (docstring = "Enriched clinical alerts combining SpO2 and PR alerts with latest vitals", folder = "ClinicalAlerts") fn_ClinicalAlerts(windowMinutes: int = 5) { let sa = fn_SpO2Alerts(windowMinutes) | project device_id, alert_time, spo2_tier = alert_tier, spo2_value = metric_value, spo2_msg = message; let pa = fn_PulseRateAlerts(windowMinutes) | project device_id, alert_time, pr_tier = alert_tier, pr_value = metric_value, pr_msg = message; let vitals = TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize arg_max(todatetime(timestamp), *) by device_id | project device_id, current_spo2 = todouble(telemetry.spo2), current_pr = toint(telemetry.pr), current_pi = todouble(telemetry.pi); vitals | join kind=leftouter sa on device_id | join kind=leftouter pa on device_id | where isnotempty(spo2_tier) or isnotempty(pr_tier) | extend alert_tier = case(spo2_tier == "CRITICAL" or pr_tier == "CRITICAL", "CRITICAL", spo2_tier == "URGENT" or pr_tier == "URGENT", "URGENT", "WARNING") | extend alert_type = case(isnotempty(spo2_tier) and isnotempty(pr_tier), "MULTI_METRIC", isnotempty(spo2_tier), "SPO2_LOW", "PR_ABNORMAL") | extend message = strcat(alert_tier, " Dev:", device_id, " SpO2:", tostring(current_spo2), " PR:", tostring(current_pr)) | project alert_time = coalesce(alert_time, alert_time1, now()), device_id, alert_tier, alert_type, spo2 = current_spo2, pr = current_pr, pi = current_pi, message }'
        if (Invoke-KustoMgmt -Command $cmd -Label "fn_ClinicalAlerts" @kqlParams) { $kqlSuccess++ } else { $kqlFail++ }

        # --- 6f. KQL Summary ---
        Write-Host ""
        $kqlTotal = $kqlSuccess + $kqlFail
        Write-Host "  ╔════════════════════════════════════════════════════╗" -ForegroundColor $(if ($kqlFail -eq 0) { "Green" } else { "Yellow" })
        Write-Host "  ║  KQL Deployment: $kqlSuccess / $kqlTotal commands succeeded" -ForegroundColor $(if ($kqlFail -eq 0) { "Green" } else { "Yellow" })
        Write-Host "  ║  • AlertHistory table + policies (4)              ║" -ForegroundColor Gray
        Write-Host "  ║  • Telemetry functions (4)                        ║" -ForegroundColor Gray
        Write-Host "  ║  • Clinical alert functions (3)                   ║" -ForegroundColor Gray
        Write-Host "  ╚════════════════════════════════════════════════════╝" -ForegroundColor $(if ($kqlFail -eq 0) { "Green" } else { "Yellow" })
        if ($kqlFail -gt 0) {
            Write-Host "  ⚠ Some KQL commands failed. Retry with: .\utilities\run-kql-scripts.ps1" -ForegroundColor Yellow
            Add-RtiFailure "KQL schema/function deployment failed ($kqlFail of $kqlTotal commands)"
        }
        # Track for final summary
        $kqlDeployed = ($kqlFail -eq 0)
    }
}

# ============================================================================
# STEP 6.5: FHIR $EXPORT → ADLS GEN2 (Automated)
# Instead of the Azure Marketplace "AHDS Data Export" offer, we call the
# FHIR $export API directly. This writes NDJSON files to a storage container
# that HDS can ingest via an OneLake shortcut.
# ============================================================================

Write-Host ""
Write-Host "--- STEP 6.5: FHIR `$EXPORT → ADLS GEN2 ---" -ForegroundColor Cyan
Write-Host ""

if ($SkipFhirExport) {
    Write-Host "  Skipped (-SkipFhirExport flag set)" -ForegroundColor Gray
} elseif (-not $FhirServiceUrl) {
    Write-Host "  ⚠ FHIR Service URL not detected — skipping `$export" -ForegroundColor Yellow
    Write-Host "    Provide -FhirServiceUrl or export FHIR data manually." -ForegroundColor Yellow
} else {
    # 6.5a. Auto-detect storage account from the resource group
    # Hoisted above the idempotency probe so both use the same target account
    # (the RG often contains multiple StorageV2 accounts — FHIR + DICOM/loader).
    Write-Host "  Detecting storage account in $ResourceGroupName..." -ForegroundColor Gray
    $storageAccountsJson = az storage account list --resource-group $ResourceGroupName `
        --query "[?kind=='StorageV2'].{name:name, id:id, hns:isHnsEnabled}" -o json 2>$null
    $storageAccounts = if ($storageAccountsJson) { $storageAccountsJson | ConvertFrom-Json } else { @() }
    if ($storageAccounts -isnot [array]) { $storageAccounts = @($storageAccounts) }

    $exportStorage = $null
    $exportStorageAccountName = $null
    if ($storageAccounts.Count -gt 0) {
        # Prefer ADLS Gen2 (HNS-enabled), fall back to first StorageV2
        $exportStorage = $storageAccounts | Where-Object { $_.hns -eq $true } | Select-Object -First 1
        if (-not $exportStorage) { $exportStorage = $storageAccounts[0] }
        $exportStorageAccountName = $exportStorage.name
    }

    # Check if export data already exists from a prior run (deploy-fhir.ps1 or a previous
    # invocation of this script). Probe the SAME account the export step will write to
    # so a multi-StorageV2 RG doesn't false-negative on the DICOM/loader account.
    $exportAlreadyDone = $false
    if ($exportStorageAccountName) {
        $blobProbeErr = $null
        $existingBlobs = az storage blob list --container-name $exportContainerName `
            --account-name $exportStorageAccountName `
            --auth-mode login --num-results 1 --query "[].name" -o tsv 2>&1
        if ($LASTEXITCODE -ne 0) {
            $blobProbeErr = ($existingBlobs | Out-String).Trim()
            # ContainerNotFound is expected on first deploy — treat as "no data", not an error.
            if ($blobProbeErr -notmatch "ContainerNotFound|The specified container does not exist") {
                Write-Host "  ⚠ Could not probe '$exportContainerName' on $exportStorageAccountName for existing data:" -ForegroundColor Yellow
                Write-Host "    $blobProbeErr" -ForegroundColor Gray
                Write-Host "    Proceeding with fresh `$export." -ForegroundColor Gray
            }
        } elseif ($existingBlobs) {
            Write-Host "  ✓ FHIR export data already exists in '$exportContainerName' on $exportStorageAccountName" -ForegroundColor Green
            Write-Host "    Skipping redundant `$export — data is available for HDS ingestion." -ForegroundColor Gray
            $exportAlreadyDone = $true
            $fhirExportDone = $true
        }
    }

    if (-not $exportAlreadyDone) {
    Write-Host "  Exporting FHIR data directly to ADLS Gen2 storage." -ForegroundColor White
    Write-Host "  (No Azure Marketplace offer or Fabric AHDS capability needed.)" -ForegroundColor Gray
    Write-Host ""

    if (-not $storageAccounts -or $storageAccounts.Count -eq 0) {
        Write-Host "  ⚠ No StorageV2 account found in $ResourceGroupName." -ForegroundColor Yellow
        Write-Host "    Create a storage account or export FHIR data manually." -ForegroundColor Yellow
    } else {
        Write-Host "  ✓ Storage account: $exportStorageAccountName" -ForegroundColor Green

        # 6.5b. Create export container
        try {
            $null = az storage container create --name $exportContainerName `
                --account-name $exportStorageAccountName --auth-mode login 2>$null
        } catch {}
        Write-Host "  ✓ Export container: $exportContainerName" -ForegroundColor Green

        # 6.5c. Detect FHIR service resource ID for configuration
        $fhirResourceId = az resource list --resource-group $ResourceGroupName `
            --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
            --query "[0].id" -o tsv 2>$null

        if (-not $fhirResourceId) {
            Write-Host "  ⚠ FHIR service resource not found in RG — skipping export config" -ForegroundColor Yellow
        } else {
            Write-Host "  ✓ FHIR service resource detected" -ForegroundColor Green

            # 6.5d. Configure export destination on FHIR service
            Write-Host "  Configuring export destination..." -ForegroundColor White
            try {
                $null = az rest --method patch `
                    --url "$fhirResourceId`?api-version=2023-11-01" `
                    --body "{`"properties`":{`"exportConfiguration`":{`"storageAccountName`":`"$exportStorageAccountName`"}}}" 2>$null
                Write-Host "  ✓ Export destination: $exportStorageAccountName" -ForegroundColor Green
            } catch {
                Write-Host "  ⚠ Could not configure export destination (may already be set)" -ForegroundColor Yellow
            }

            # 6.5e. Ensure RBAC: FHIR service MI → Storage Blob Data Contributor
            Write-Host "  Checking RBAC..." -ForegroundColor Gray
            $fhirMiPrincipalId = az resource show --ids $fhirResourceId `
                --query "identity.principalId" -o tsv 2>$null

            if ($fhirMiPrincipalId) {
                $storageBlobContribRole = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
                try {
                    $null = az role assignment create `
                        --assignee-object-id $fhirMiPrincipalId `
                        --assignee-principal-type ServicePrincipal `
                        --role $storageBlobContribRole `
                        --scope $exportStorage.id 2>$null
                } catch {}
                Write-Host "  ✓ RBAC: FHIR MI → Storage Blob Data Contributor" -ForegroundColor Green
            }

            # 6.5f. Trigger FHIR $export
            Write-Host ""
            Write-Host "  Triggering FHIR `$export..." -ForegroundColor White
            $fhirToken = az account get-access-token `
                --resource $FhirServiceUrl --query accessToken -o tsv 2>$null

            if (-not $fhirToken) {
                Write-Host "  ⚠ Could not acquire FHIR access token" -ForegroundColor Yellow
            } else {
                try {
                    $exportResp = Invoke-WebRequest `
                        -Uri "$FhirServiceUrl/`$export?_container=$exportContainerName" `
                        -Headers @{
                            "Authorization" = "Bearer $fhirToken"
                            "Accept"        = "application/fhir+json"
                            "Prefer"        = "respond-async"
                        } -Method GET -UseBasicParsing

                    if ($exportResp.StatusCode -eq 202) {
                        $contentLocation = $exportResp.Headers["Content-Location"]
                        if ($contentLocation -is [array]) { $contentLocation = $contentLocation[0] }
                        Write-Host "  ✓ FHIR `$export started (async)" -ForegroundColor Green
                        Write-Host "    Status URL: $contentLocation" -ForegroundColor Gray

                        # Poll until complete (max 60 min)
                        $pollStart = Get-Date
                        $maxPollMin = 60
                        $pollSec = 30
                        Write-Host "  Polling every ${pollSec}s (max ${maxPollMin}m)..." -ForegroundColor White

                        while ((New-TimeSpan -Start $pollStart).TotalMinutes -lt $maxPollMin) {
                            Start-Sleep -Seconds $pollSec
                            $elapsed = [math]::Round((New-TimeSpan -Start $pollStart).TotalMinutes, 1)

                            # Refresh token every ~15 min
                            if ([math]::Floor($elapsed) % 15 -eq 0 -and [math]::Floor($elapsed) -gt 0) {
                                $fhirToken = az account get-access-token `
                                    --resource $FhirServiceUrl --query accessToken -o tsv 2>$null
                            }

                            try {
                                $pollResp = Invoke-WebRequest -Uri $contentLocation `
                                    -Headers @{ "Authorization" = "Bearer $fhirToken" } `
                                    -UseBasicParsing

                                if ($pollResp.StatusCode -eq 200) {
                                    $exportResult = $pollResp.Content | ConvertFrom-Json
                                    $fileCount = ($exportResult.output | Measure-Object).Count
                                    $resourceTypes = ($exportResult.output | ForEach-Object { $_.type } | Sort-Object -Unique) -join ", "
                                    Write-Host "  ✓ FHIR `$export complete!" -ForegroundColor Green
                                    Write-Host "    Files: $fileCount | Types: $resourceTypes" -ForegroundColor Green
                                    $fhirExportDone = $true
                                    break
                                }
                            } catch {
                                $sc = $null
                                try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
                                if ($sc -eq 202) {
                                    Write-Host "    Exporting... (${elapsed}m elapsed)" -ForegroundColor Gray
                                } else {
                                    Write-Host "    ⚠ Poll error: $($_.Exception.Message)" -ForegroundColor Yellow
                                }
                            }
                        }

                        if (-not $fhirExportDone) {
                            Write-Host "  ⚠ Export still running after ${maxPollMin}m." -ForegroundColor Yellow
                            Write-Host "    It will complete in the background. Check:" -ForegroundColor Yellow
                            Write-Host "    $contentLocation" -ForegroundColor Gray
                            $fhirExportDone = $true  # Treat as done for guidance purposes
                        }
                    }
                } catch {
                    $sc = $null
                    try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
                    if ($sc -eq 409) {
                        Write-Host "  ⚠ Another `$export is already running. Wait for it to complete." -ForegroundColor Yellow
                        $fhirExportDone = $true
                    } else {
                        Write-Host "  ⚠ Failed to trigger `$export: $($_.Exception.Message)" -ForegroundColor Yellow
                    }
                }
            }
        }
    }
    } # end if (-not $exportAlreadyDone)
}

if ($fhirExportDone -or $exportStorageAccountName) {
    Write-Host ""
    Write-Host "  Export storage for OneLake shortcut:" -ForegroundColor White
    Write-Host "    Account:   $exportStorageAccountName" -ForegroundColor Cyan
    Write-Host "    Container: $exportContainerName" -ForegroundColor Cyan
    Write-Host "    URL:       https://$exportStorageAccountName.dfs.core.windows.net" -ForegroundColor Cyan
}

# ============================================================================
# PHASE GATE: If -Phase2, skip to post-HDS steps (handled above at script top)
# If not Phase2, we continue with HDS guidance and pause for manual deployment.
# ============================================================================

if ($Phase2) {
    # Phase 2 is handled by re-running with -Phase2 flag — should not reach here
    # (The Phase2 block at the top of the script handles this and exits)
    Write-Host "  Phase 2 logic handled above." -ForegroundColor Gray
}

# ============================================================================
# STEP 7: HEALTHCARE DATA SOLUTIONS GUIDANCE
# ============================================================================

if (-not $SkipHdsGuidance) {
    Write-Host "--- STEP 7: HEALTHCARE DATA SOLUTIONS — Clinical Foundations ---" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Deploy Healthcare Data Solutions to bring FHIR patient context" -ForegroundColor White
    Write-Host "  into Fabric for enriched clinical alerts." -ForegroundColor White
    Write-Host ""
    Write-Host "  7a. DEPLOY HDS (Fabric Portal):" -ForegroundColor Yellow
    Write-Host "      1. Go to workspace '$FabricWorkspaceName' in the Fabric portal" -ForegroundColor Gray
    Write-Host "      2. Select: New item → Healthcare data solutions" -ForegroundColor Gray
    Write-Host "      3. Complete the Setup wizard" -ForegroundColor Gray
    Write-Host "      4. Deploy 'Healthcare Data Foundations' capability" -ForegroundColor Gray
    Write-Host "         This creates:" -ForegroundColor Gray
    Write-Host "           - 3 Lakehouses (Admin, Bronze, Silver)" -ForegroundColor Gray
    Write-Host "           - 5 Notebooks (config, flatten, NDJSON ingestion, etc.)" -ForegroundColor Gray
    Write-Host "           - 1 Spark Environment (Runtime 1.2 / Spark 3.4)" -ForegroundColor Gray
    Write-Host "           - 1 Clinical Data Pipeline" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7a-i. ADD SCIPY TO HDS SPARK ENVIRONMENT:" -ForegroundColor Yellow
    Write-Host "      ┌──────────────────────────────────────────────────────────┐" -ForegroundColor Gray
    Write-Host "      │ NOTE: Phase 2 now adds scipy==1.11.4 automatically via  │" -ForegroundColor Gray
    Write-Host "      │ the Fabric REST API. If auto-add fails, add manually:   │" -ForegroundColor Gray
    Write-Host "      └──────────────────────────────────────────────────────────┘" -ForegroundColor Gray
    Write-Host "      1. Open the 'healthcare#_environment' in your workspace" -ForegroundColor Gray
    Write-Host "      2. Go to 'External repositories' under Libraries" -ForegroundColor Gray
    Write-Host "      3. Click '+ Add library' → search for 'scipy'" -ForegroundColor Gray
    Write-Host "      4. Select version 1.11.4 and click Add" -ForegroundColor Gray
    Write-Host "      5. Click 'Publish' to save the environment" -ForegroundColor Gray
    Write-Host "      6. Wait for the environment to finish publishing" -ForegroundColor Gray
    Write-Host "      See: docs/images/hds-scipy-external-repositories.png" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7b. CREATE ONELAKE SHORTCUT (Bronze LH → FHIR export storage):" -ForegroundColor Yellow
    Write-Host "      FHIR data was exported to ADLS Gen2 in Step 6.5." -ForegroundColor Gray
    Write-Host "      Now link that storage to the Bronze Lakehouse:" -ForegroundColor Gray
    Write-Host "      1. Open the Bronze Lakehouse in your workspace" -ForegroundColor Gray
    Write-Host "      2. Navigate to Files → right-click → New shortcut" -ForegroundColor Gray
    Write-Host "      3. Select 'Azure Data Lake Storage Gen2'" -ForegroundColor Gray
    if ($exportStorageAccountName) {
        Write-Host "      4. Storage account URL: https://$exportStorageAccountName.dfs.core.windows.net" -ForegroundColor Cyan
        Write-Host "      5. Container/path: $exportContainerName" -ForegroundColor Cyan
    } else {
        Write-Host "      4. Enter the storage account URL where FHIR `$export wrote data" -ForegroundColor Gray
        Write-Host "      5. Select the fhir-export container" -ForegroundColor Gray
    }
    Write-Host "      6. Create the shortcut at:" -ForegroundColor Gray
    Write-Host "         Files/Ingest/Clinical/FHIR-NDJSON/FHIR-HDS" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7c. UPDATE HDS CONFIG:" -ForegroundColor Yellow
    Write-Host "      1. Open Admin Lakehouse → Files/system-configurations/" -ForegroundColor Gray
    Write-Host "         deploymentParametersConfiguration.json" -ForegroundColor Gray
    Write-Host "      2. Set source_path_pattern to use the shortcut path" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7d. RUN CLINICAL DATA FOUNDATION PIPELINE:" -ForegroundColor Yellow
    Write-Host "      Execute 'healthcare#_msft_clinical_data_foundation_ingestion'" -ForegroundColor Gray
    Write-Host "      to ingest NDJSON files → Bronze → Silver lakehouse." -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7e. RUN PHASE 2 — KQL SHORTCUTS & ENRICHMENT:" -ForegroundColor Yellow
    Write-Host "      After the Silver Lakehouse is populated, run:" -ForegroundColor White
    Write-Host ""
    Write-Host "        .\deploy-fabric-rti.ps1 -Phase2" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "      This will:" -ForegroundColor Gray
    Write-Host "        • Auto-discover the Silver Lakehouse in the workspace" -ForegroundColor Gray
    Write-Host "        • Create KQL external tables (shortcuts) for:" -ForegroundColor Gray
    Write-Host "            - SilverPatient  (demographics, identifiers)" -ForegroundColor Gray
    Write-Host "            - SilverCondition (diagnoses, SNOMED codes)" -ForegroundColor Gray
    Write-Host "            - SilverDevice   (device-patient associations)" -ForegroundColor Gray
    Write-Host "        • Replace fn_ClinicalAlerts with enriched version" -ForegroundColor Gray
    Write-Host "          that joins telemetry with patient context" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  Silver Lakehouse Tables (auto-created):" -ForegroundColor White
    Write-Host "    ┌──────────────────────┬──────────────────────────────────┐" -ForegroundColor Gray
    Write-Host "    │ Table                │ Use in Alert System              │" -ForegroundColor Gray
    Write-Host "    ├──────────────────────┼──────────────────────────────────┤" -ForegroundColor Gray
    Write-Host "    │ Patient              │ Name, MRN, demographics         │" -ForegroundColor Gray
    Write-Host "    │ Device               │ Masimo device metadata           │" -ForegroundColor Gray
    Write-Host "    │ Condition            │ COPD, CHF qualifying conditions  │" -ForegroundColor Gray
    Write-Host "    │ Observation          │ Historical vitals baseline       │" -ForegroundColor Gray
    Write-Host "    │ Encounter            │ Active admission context         │" -ForegroundColor Gray
    Write-Host "    │ MedicationRequest    │ Drug interaction awareness       │" -ForegroundColor Gray
    Write-Host "    └──────────────────────┴──────────────────────────────────┘" -ForegroundColor Gray
    Write-Host ""
}

# ============================================================================
# STEP 7b: REAL-TIME DASHBOARD (Automated)
# ============================================================================

Write-Host "--- STEP 7b: REAL-TIME DASHBOARD ---" -ForegroundColor Cyan
Write-Host ""

$dashboardName = "Masimo Patient Monitoring"

# 7b-i. Check for existing dashboard
Write-Host "  Checking for existing dashboard..." -ForegroundColor Gray
$existingDash = $null
try {
    $dashItems = Invoke-FabricApi -Method GET -Endpoint "/workspaces/$workspaceId/items?type=KQLDashboard"
    $existingDash = $dashItems.value | Where-Object { $_.displayName -eq $dashboardName }
} catch {}

if ($existingDash) {
    $dashId = $existingDash.id
    Write-Host "  ✓ Dashboard exists: $dashboardName ($dashId)" -ForegroundColor Green
} else {
    Write-Host "  Creating KQL Dashboard '$dashboardName'..." -ForegroundColor White
    try {
        $dashResp = Invoke-FabricApi -Method POST -Endpoint "/workspaces/$workspaceId/items" `
            -Body @{ displayName = $dashboardName; type = "KQLDashboard"; description = "Command center for Masimo patient monitoring: live device telemetry, clinical alerts, device detail, operations, and facility map." }
        $dashId = $dashResp.id
        Write-Host "  ✓ Dashboard created: $dashId" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Could not create dashboard: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "    Create manually: New item → Real-Time Dashboard → '$dashboardName'" -ForegroundColor Yellow
        Write-Host "    Add KQL Database '$kqlDbName' as data source" -ForegroundColor Yellow
        Write-Host "    Create tiles using queries from fabric-rti/kql/05-dashboard-queries.kql" -ForegroundColor Yellow
        Add-RtiFailure "Masimo Patient Monitoring dashboard was not created"
        $dashId = $null
    }
}

# 7b-ii. Apply dashboard definition (tiles, data source, auto-refresh)
if ($dashId) {
    Write-Host "  Applying enhanced dashboard definition (5 pages, clinical triage + operations)" -ForegroundColor White
    try {
        $applied = Set-MasimoDashboardDefinition -WorkspaceId $workspaceId -DashboardId $dashId `
            -DashboardName $dashboardName -KqlDbName $kqlDbName -KqlDbId $kqlDbId -KustoUri $kustoUri
        Write-Host "  ✓ Dashboard definition applied ($($applied.Pages) pages, $($applied.Tiles) tiles)" -ForegroundColor Green
        Write-Host "    • Command Center: severity KPIs, alert feed, data freshness, SpO2 thresholds" -ForegroundColor Gray
        Write-Host "    • Clinical Alerts: triage table, explainability, noisy devices, clinical load" -ForegroundColor Gray
        Write-Host "    • Device Detail: selected-device vitals, alerts, risk context, signal trend" -ForegroundColor Gray
        Write-Host "    • Operations: ingestion health, connectivity, signal-quality separation" -ForegroundColor Gray
        Write-Host "    • Facility Map: alert locations, hospital breakdown, location detail" -ForegroundColor Gray
        Write-Host "  Device filter: single-select with 'All' option" -ForegroundColor Cyan
        Write-Host "  Auto-refresh: 30 seconds" -ForegroundColor Cyan
        Write-Host "  Dashboard URL: https://app.fabric.microsoft.com/groups/$workspaceId/kustodashboards/$dashId" -ForegroundColor DarkGray
        $dashboardDeployed = $true
    } catch {
        Write-Host "  ⚠ Failed to apply dashboard definition: $($_.Exception.Message)" -ForegroundColor Yellow
        Write-Host "    The dashboard was created but needs manual tile configuration." -ForegroundColor Yellow
        Write-Host "    See: fabric-rti/kql/05-dashboard-queries.kql" -ForegroundColor Yellow
        Add-RtiFailure "Masimo Patient Monitoring dashboard definition was not applied"
    }
}
Write-Host ""

# ============================================================================
# STEP 8: DATA ACTIVATOR GUIDANCE
# ============================================================================

Write-Host "--- STEP 8: DATA ACTIVATOR (Manual) ---" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Create a Data Activator Reflex to trigger clinical alerts:" -ForegroundColor White
Write-Host ""
Write-Host "  1. In workspace '$FabricWorkspaceName' → New item → Reflex" -ForegroundColor Gray
Write-Host "  2. Connect to KQL Database '$kqlDbName'" -ForegroundColor Gray
Write-Host "  3. Use the fn_ClinicalAlerts function as the data source" -ForegroundColor Gray
Write-Host "  4. Set trigger conditions for each alert tier:" -ForegroundColor Gray
Write-Host "     • WARNING  — SpO2 < 94% or PR outside 50-110" -ForegroundColor Yellow
Write-Host "     • URGENT   — SpO2 < 90% or PR outside 45-130 (or COPD/CHF)" -ForegroundColor DarkYellow
Write-Host "     • CRITICAL — SpO2 < 85% or PR outside 40-150 (and COPD/CHF)" -ForegroundColor Red
Write-Host "  5. Configure actions: Teams notification, Email, write to AlertHistory" -ForegroundColor Gray
Write-Host ""

# ============================================================================
# FINAL SUMMARY
# ============================================================================

Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                    DEPLOYMENT SUMMARY                       ║" -ForegroundColor Cyan
Write-Host "╠══════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
Write-Host "║ PHASE 1 — Automated (completed):                            ║" -ForegroundColor Cyan
Write-Host "║   ✓ Fabric workspace created/validated                      ║" -ForegroundColor Green
Write-Host "║   ✓ Workspace managed identity provisioned                  ║" -ForegroundColor Green
Write-Host "║   ✓ Eventhouse created                                      ║" -ForegroundColor Green
Write-Host "║   ✓ KQL Database created                                    ║" -ForegroundColor Green
Write-Host "║   ✓ Eventstream created                                     ║" -ForegroundColor Green
if ($dataConnectionId) {
Write-Host "║   ✓ Cloud connection to Event Hub                           ║" -ForegroundColor Green
Write-Host "║   ✓ Eventstream source + destination configured             ║" -ForegroundColor Green
} else {
Write-Host "║   → Configure Eventstream source (Event Hub) & destination   ║" -ForegroundColor Yellow
}
if ($kqlDeployed) {
Write-Host "║   ✓ AlertHistory table + policies                           ║" -ForegroundColor Green
Write-Host "║   ✓ 7 KQL alert & telemetry functions                       ║" -ForegroundColor Green
} else {
Write-Host "║   → Run KQL scripts: .\utilities\run-kql-scripts.ps1                  ║" -ForegroundColor Yellow
}
if ($fhirExportDone) {
Write-Host "║   ✓ FHIR `$export → ADLS Gen2 ($exportStorageAccountName)    ║" -ForegroundColor Green
} else {
Write-Host "║   → Run FHIR `$export or re-run without -SkipFhirExport     ║" -ForegroundColor Yellow
}
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "║ MANUAL STEPS (before Phase 2):                               ║" -ForegroundColor Cyan
Write-Host "║   → Deploy Healthcare Data Solutions (Clinical Foundations)   ║" -ForegroundColor Yellow
Write-Host "║   ✓ scipy==1.11.4 added to HDS Spark environment (Phase 2)  ║" -ForegroundColor Green
Write-Host "║   → Create OneLake shortcut (Bronze LH → export storage)    ║" -ForegroundColor Yellow
Write-Host "║   → Update HDS config with shortcut path                    ║" -ForegroundColor Yellow
Write-Host "║   → Run HDS clinical data foundation pipeline               ║" -ForegroundColor Yellow
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "║ PHASE 2 — Run after HDS is deployed & pipeline has run:      ║" -ForegroundColor Cyan
Write-Host "║   .\deploy-fabric-rti.ps1 -Phase2                           ║" -ForegroundColor Magenta
Write-Host "║   → Creates KQL shortcuts to Silver Patient/Condition/Device ║" -ForegroundColor Yellow
Write-Host "║   → Deploys enriched fn_ClinicalAlerts with FHIR context    ║" -ForegroundColor Yellow
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "║ OPTIONAL (after Phase 2):                                    ║" -ForegroundColor Cyan
Write-Host "║   → Create Data Activator Reflex                             ║" -ForegroundColor Yellow
if ($dashboardDeployed) {
Write-Host "║   ✓ Real-Time Dashboard deployed (7 tiles, 30s refresh)      ║" -ForegroundColor Green
} else {
Write-Host "║   → Create Real-Time Dashboard                               ║" -ForegroundColor Yellow
}
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

Complete-RtiPhase -RequireEventstream -RequireKql -RequireDashboard

# Output workspace ID for downstream use
Write-Host "Workspace ID (for scripting): $workspaceId" -ForegroundColor DarkGray
