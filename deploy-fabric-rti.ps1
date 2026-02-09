# deploy-fabric-rti.ps1
# Deploys Microsoft Fabric Real-Time Intelligence resources for the Masimo Clinical Alert System.
# Prerequisites:
#   - Azure CLI authenticated (az login)
#   - Az PowerShell module installed (Install-Module Az)
#   - Existing Azure deployment from deploy.ps1 (Event Hub, FHIR Service)
#   - Microsoft Fabric capacity (trial or paid)
#
# Usage:
#   .\deploy-fabric-rti.ps1
#   .\deploy-fabric-rti.ps1 -FabricWorkspaceName "my-workspace" -ResourceGroupName "rg-medtech-sys-identity"

param (
    [string]$FabricWorkspaceName = "med-device-real-time",
    [string]$ResourceGroupName = "rg-medtech-sys-identity",
    [string]$EventHubNamespace = "",         # Auto-detected from RG if blank
    [string]$EventHubName = "telemetry-stream",
    [string]$FhirServiceUrl = "",            # Auto-detected from RG if blank
    [string]$Location = "eastus",
    [switch]$SkipHdsGuidance = $false        # Skip the HDS manual-step guidance
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$kqlDeployed = $false

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Get-FabricAccessToken {
    <#
    .SYNOPSIS
        Obtains a bearer token for the Fabric REST API using the current Az context.
        Handles both PowerShell 5.1 (plain text) and 7+ (SecureString) token formats.
    #>
    $tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
    $rawToken = $tokenObj.Token

    if ($rawToken -is [System.Security.SecureString]) {
        # PowerShell 7+ returns SecureString
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try {
            return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        } finally {
            [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        }
    }
    elseif ($rawToken -is [string]) {
        # PowerShell 5.1 returns plain string
        return $rawToken
    }
    else {
        # Fallback: try ConvertFrom-SecureString
        return $rawToken | ConvertFrom-SecureString -AsPlainText
    }
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
        [int]$MaxRetries = 3
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

            if ($statusCode -eq 429 -and $attempt -lt $MaxRetries) {
                # Rate limited - back off
                $retryAfter = 30
                try {
                    $retryAfter = [int]$_.Exception.Response.Headers["Retry-After"]
                } catch {}
                Write-Host "  Rate limited. Waiting ${retryAfter}s... (attempt $attempt/$MaxRetries)" -ForegroundColor Yellow
                Start-Sleep -Seconds $retryAfter
                continue
            }
            else {
                throw $_
            }
        }
    }
}

function Get-KustoAccessToken {
    <#
    .SYNOPSIS
        Obtains a bearer token for the Kusto REST API.
        Handles both PowerShell 5.1 (plain text) and 7+ (SecureString) token formats.
    #>
    $tokenObj = Get-AzAccessToken -ResourceUrl "https://kusto.kusto.windows.net"
    $rawToken = $tokenObj.Token

    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try {
            return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        } finally {
            [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        }
    }
    elseif ($rawToken -is [string]) {
        return $rawToken
    }
    else {
        return $rawToken | ConvertFrom-SecureString -AsPlainText
    }
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
        Write-Host "  ✗ $Label" -ForegroundColor Red
        Write-Host "    $msg" -ForegroundColor DarkRed
        return $false
    }
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
    $typeEndpoints = @{
        "Eventhouse"  = "eventhouses"
        "KQLDatabase" = "kqlDatabases"
        "Eventstream" = "eventstreams"
    }
    $endpoint = if ($typeEndpoints.ContainsKey($ItemType)) { $typeEndpoints[$ItemType] } else { "items?type=$ItemType" }

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
    Write-Host "  Run deploy.ps1 first to deploy the base infrastructure." -ForegroundColor Yellow
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
            description = "Masimo Clinical Alert System — Real-Time Intelligence workspace for medical device telemetry monitoring and clinical alerting."
        }
        $workspaceId = $newWs.id
        Write-Host "  ✓ Workspace created: $FabricWorkspaceName (ID: $workspaceId)" -ForegroundColor Green
    }

    # Ensure workspace has a Fabric capacity assigned
    $wsDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId"
    if (-not $wsDetail.capacityId) {
        Write-Host "  Workspace has no capacity — searching for an active Fabric capacity..." -ForegroundColor Yellow
        $capacities = Invoke-FabricApi -Endpoint "/capacities"
        $activeCapacity = $capacities.value | Where-Object {
            $_.state -eq "Active" -and $_.sku -ne "PP3"
        } | Sort-Object -Property @{Expression={if ($_.sku -like "F*" -and $_.sku -ne "FT1") { 0 } elseif ($_.sku -eq "FT1") { 1 } else { 2 }}} | Select-Object -First 1

        if ($activeCapacity) {
            Write-Host "  Assigning capacity: $($activeCapacity.displayName) (SKU: $($activeCapacity.sku))..." -ForegroundColor White
            Invoke-FabricApi -Method "POST" -Endpoint "/workspaces/$workspaceId/assignToCapacity" -Body @{
                capacityId = $activeCapacity.id
            }
            Start-Sleep -Seconds 5
            Write-Host "  ✓ Capacity assigned: $($activeCapacity.displayName)" -ForegroundColor Green
        } else {
            Write-Host "  ERROR: No active Fabric capacity found." -ForegroundColor Red
            Write-Host "    Start a Fabric trial at https://app.fabric.microsoft.com or provision a capacity." -ForegroundColor Yellow
            exit 1
        }
    } else {
        Write-Host "  ✓ Capacity already assigned" -ForegroundColor Green
    }
} catch {
    Write-Host "ERROR: Failed to access Fabric API." -ForegroundColor Red
    Write-Host "  $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Possible causes:" -ForegroundColor Yellow
    Write-Host "    1. You don't have a Fabric capacity or trial. Start one at https://app.fabric.microsoft.com" -ForegroundColor Yellow
    Write-Host "    2. Your account doesn't have permission to create workspaces." -ForegroundColor Yellow
    Write-Host "    3. The Az PowerShell token can't reach api.fabric.microsoft.com." -ForegroundColor Yellow
    exit 1
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
    $existingEh = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/eventhouses"
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
            description = "Stores real-time Masimo pulse oximeter telemetry and clinical alert history."
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
            description     = "KQL database for Masimo telemetry and clinical alerts."
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
            description = "Ingests real-time telemetry from Masimo Radius-7 pulse oximeters via Azure Event Hub."
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
    $connectionName = "masimo-eh-$EventHubName"
    $dataConnectionId = $null

    Write-Host "  Checking for existing Fabric cloud connection '$connectionName'..." -ForegroundColor Gray

    try {
        $existingConns = Invoke-FabricApi -Endpoint "/connections"
        $existingConn = $existingConns.value | Where-Object { $_.displayName -eq $connectionName }
        if ($existingConn) {
            $dataConnectionId = $existingConn.id
            Write-Host "  ✓ Cloud connection already exists: $connectionName (ID: $dataConnectionId)" -ForegroundColor Green
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

            $updateResponse = Invoke-WebRequest `
                -Method POST `
                -Uri $updateUri `
                -Headers $updateHeaders `
                -Body $updateJsonBody `
                -UseBasicParsing

            $updateStatus = $updateResponse.StatusCode

            if ($updateStatus -eq 200 -or $updateStatus -eq 202) {
                Write-Host "  ✓ Eventstream definition updated successfully!" -ForegroundColor Green

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
                                    break
                                } elseif ($opResult.status -eq "Failed") {
                                    Write-Host "  ⚠ Eventstream configuration failed: $($opResult | ConvertTo-Json -Depth 5)" -ForegroundColor Yellow
                                    break
                                }
                                Write-Host "    Status: $($opResult.status) (${opElapsed}s)" -ForegroundColor Gray
                            } catch {
                                Write-Host "    Polling... (${opElapsed}s)" -ForegroundColor Gray
                            }
                        }
                    }
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
            try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}

            if ($errCode -eq 202) {
                Write-Host "  ✓ Eventstream definition update accepted (202 — provisioning)." -ForegroundColor Green
            } else {
                Write-Host "  ⚠ Failed to update Eventstream definition: $errMsg" -ForegroundColor Yellow

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
$kqlDbInfo = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases"
$kqlDbObj = $kqlDbInfo.value | Where-Object { $_.displayName -eq $kqlDbName }
if (-not $kqlDbObj) {
    Write-Host "  ⚠ KQL Database '$kqlDbName' not found — skipping KQL deployment." -ForegroundColor Yellow
    Write-Host "    Run the KQL scripts manually: .\run-kql-scripts.ps1" -ForegroundColor Yellow
} else {
    $kqlDbId = $kqlDbObj.id
    $kqlDbDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/kqlDatabases/$kqlDbId"
    $kustoUri = $kqlDbDetail.queryServiceUri
    if (-not $kustoUri) {
        # Fallback: try queryUri or properties
        $kustoUri = $kqlDbDetail.queryUri
    }
    if (-not $kustoUri) {
        # Last resort: try to extract from properties
        try { $kustoUri = $kqlDbDetail.properties.queryUri } catch {}
    }

    if (-not $kustoUri) {
        Write-Host "  ⚠ Could not determine Kusto Query URI automatically." -ForegroundColor Yellow
        Write-Host "    Run the KQL scripts manually: .\run-kql-scripts.ps1" -ForegroundColor Yellow
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

        # --- 6c. AlertHistory Table & Policies ---
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

        # --- 6d. Telemetry Functions ---
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
        $cmd = '.create-or-alter function with (docstring = "Detect SpO2 threshold breaches with 3-tier severity", folder = "ClinicalAlerts") fn_SpO2Alerts(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | where isnotnull(telemetry.spo2) | extend spo2 = todouble(telemetry.spo2) | where spo2 > 0 and spo2 < 100 | summarize min_spo2 = min(spo2), avg_spo2 = round(avg(spo2),1), readings = count(), last_time = max(todatetime(timestamp)) by device_id | where min_spo2 < 94 | extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING") | extend alert_type = "SPO2_LOW" | extend message = strcat(alert_tier, " Dev:", device_id, " SpO2=", tostring(min_spo2)) | extend tv = case(alert_tier == "CRITICAL", 85.0, alert_tier == "URGENT", 90.0, 94.0) | project alert_time = last_time, device_id, alert_tier, alert_type, metric_name = "spo2", metric_value = min_spo2, threshold_value = tv, message, readings | order by alert_tier asc, min_spo2 asc }'
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
            Write-Host "  ⚠ Some KQL commands failed. Retry with: .\run-kql-scripts.ps1" -ForegroundColor Yellow
        }
        # Track for final summary
        $kqlDeployed = ($kqlFail -eq 0)
    }
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
    Write-Host "  7b. DEPLOY AHDS DATA EXPORT (Azure Marketplace):" -ForegroundColor Yellow
    Write-Host "      1. Azure Portal → Create a resource → search:" -ForegroundColor Gray
    Write-Host "         'Healthcare data solutions in Microsoft Fabric'" -ForegroundColor Gray
    Write-Host "      2. Configure:" -ForegroundColor Gray
    Write-Host "         - Resource group:  $ResourceGroupName" -ForegroundColor Gray
    Write-Host "         - Region:          $Location" -ForegroundColor Gray
    if ($FhirServiceUrl) {
        Write-Host "         - FHIR Server URI: $FhirServiceUrl" -ForegroundColor Gray
    }
    Write-Host "      3. This deploys an Azure Function + Key Vault + Storage" -ForegroundColor Gray
    Write-Host "         for automated FHIR `$export to OneLake" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7c. CREATE ONELAKE SHORTCUT:" -ForegroundColor Yellow
    Write-Host "      In the Bronze lakehouse, create a shortcut at:" -ForegroundColor Gray
    Write-Host "        Files\External\Clinical\FHIR-NDJSON\AHDS-FHIR\" -ForegroundColor Gray
    Write-Host "      pointing to the ADLS Gen2 export storage container." -ForegroundColor Gray
    Write-Host ""
    Write-Host "  7d. RUN CLINICAL PIPELINE:" -ForegroundColor Yellow
    Write-Host "      Execute 'healthcare#_msft_clinical_ahds_fhirservice_export'" -ForegroundColor Gray
    Write-Host "      to bulk-export FHIR data → Bronze → Silver lakehouse." -ForegroundColor Gray
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
# STEP 7b: REAL-TIME DASHBOARD GUIDANCE
# ============================================================================

Write-Host "--- STEP 7b: REAL-TIME DASHBOARD ---" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Create a Real-Time Dashboard for clinical monitoring:" -ForegroundColor White
Write-Host ""
Write-Host "  1. In workspace '$FabricWorkspaceName' → New item → Real-Time Dashboard" -ForegroundColor Gray
Write-Host "  2. Name it: 'Masimo Clinical Alerts'" -ForegroundColor Gray
Write-Host "  3. Add KQL Database '$kqlDbName' as data source" -ForegroundColor Gray
Write-Host "  4. Create 7 tiles using queries from fabric-rti/kql/05-dashboard-queries.kql" -ForegroundColor Gray
Write-Host "  5. Enable auto-refresh at 30-second intervals" -ForegroundColor Gray
Write-Host ""
Write-Host "  See: fabric-rti/dashboard/DASHBOARD-GUIDE.md" -ForegroundColor Yellow
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
Write-Host "║ Automated (completed):                                      ║" -ForegroundColor Cyan
Write-Host "║   ✓ Fabric workspace created/validated                      ║" -ForegroundColor Green
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
Write-Host "║   → Run KQL scripts: .\run-kql-scripts.ps1                  ║" -ForegroundColor Yellow
}
Write-Host "║                                                              ║" -ForegroundColor Cyan
Write-Host "║ Manual steps remaining:                                      ║" -ForegroundColor Cyan
Write-Host "║   → Deploy Healthcare Data Solutions (Clinical Foundations)   ║" -ForegroundColor Yellow
Write-Host "║   → Deploy AHDS Data Export (Azure Marketplace)              ║" -ForegroundColor Yellow
Write-Host "║   → Create Data Activator Reflex                             ║" -ForegroundColor Yellow
Write-Host "║   → Create Real-Time Dashboard (optional)                    ║" -ForegroundColor Yellow
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Output workspace ID for downstream use
Write-Host "Workspace ID (for scripting): $workspaceId" -ForegroundColor DarkGray
