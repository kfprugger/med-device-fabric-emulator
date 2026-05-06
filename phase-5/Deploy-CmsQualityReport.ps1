<#
.SYNOPSIS
    Deploys CMS Quality & Claims materialization notebook + Power BI report.

.DESCRIPTION
    Phase 5 deployment:
      1. Discovers the Silver and Reporting Gold lakehouses in the workspace.
      2. Creates / updates the materialization notebook with a default lakehouse
         attachment so its `<lakehouse>.dbo.<table>` references resolve.
      3. Runs the notebook (LRO) and waits for completion (max 30 min).
      4. Reads the PBIP definition for the CMS Quality Scorecard report,
         patches the SQL endpoint placeholders, and publishes the
         SemanticModel + Report items via the Fabric REST API.
      5. Takes ownership of the semantic model, binds OAuth2 credentials, and
         triggers a refresh so the report has data on first load.

.PARAMETER FabricWorkspaceName
    Target Fabric workspace name (must contain the Silver + Gold lakehouses).

.PARAMETER ReportingLhName
    Reporting Gold lakehouse display name. Default: healthcare1_reporting_gold

.PARAMETER SilverLhName
    Silver lakehouse display name. Default: healthcare1_msft_silver

.PARAMETER ReportSourcePath
    Path containing materialize_claims_quality.py + cms-quality-report\*.
    Default: this script's directory.

.PARAMETER SkipNotebookRun
    Build/publish the notebook but don't run it (faster smoke test).

.PARAMETER SkipNotebookDeploy
    Skip the notebook publish step entirely — useful when iterating on the
    SemanticModel/Report only and the notebook already exists in the workspace.

.PARAMETER NotebookTimeoutMin
    Max minutes to wait for notebook completion. Default: 30.

.EXAMPLE
    .\Deploy-CmsQualityReport.ps1 -FabricWorkspaceName "med-0505"
#>
param(
    [Parameter(Mandatory)][string]$FabricWorkspaceName,
    [string]$ReportingLhName = "healthcare1_reporting_gold",
    [string]$SilverLhName = "healthcare1_msft_silver",
    [string]$ReportSourcePath = "",
    [switch]$SkipNotebookRun,
    [switch]$SkipNotebookDeploy,
    [int]$NotebookTimeoutMin = 30
)

$ErrorActionPreference = "Stop"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# Pin to Brakekat tenant — agent-launched pwsh may default to a different tenant
$script:FabricTenantId = "8d038e6a-9b7d-4cb8-bbcf-e84dff156478"
$ctxMatch = Get-AzContext -ListAvailable | Where-Object { $_.Tenant.Id -eq $script:FabricTenantId } | Select-Object -First 1
if ($ctxMatch) { $null = Set-AzContext -Context $ctxMatch }

if (-not $ReportSourcePath) {
    $ReportSourcePath = Split-Path -Parent $MyInvocation.MyCommand.Path
}

# ============================================================================
# HELPERS
# ============================================================================

function Get-FabricToken {
    $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -TenantId $script:FabricTenantId).Token
    if ($t -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    return $t
}

function Get-PbiToken {
    $t = (Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api" -TenantId $script:FabricTenantId).Token
    if ($t -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    return $t
}

function Invoke-FabricApi {
    param([string]$Method = "GET", [string]$Endpoint, [object]$Body = $null)
    $token = Get-FabricToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $uri = "$FabricApiBase$Endpoint"
    $params = @{ Method = $Method; Uri = $uri; Headers = $headers }
    if ($Body -and $Method -ne "GET") {
        $params["Body"] = ($Body | ConvertTo-Json -Depth 20)
    }
    Invoke-RestMethod @params
}

function To-B64 ([string]$Text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

# Resolve the operation ID + result for a 202 LRO response from a Fabric call.
function Resolve-LroResult {
    param(
        [Parameter(Mandatory)] $Response,
        [int]$TimeoutSec = 180,
        [string]$DiscoveryEndpoint = $null,
        [string]$DiscoveryDisplayName = $null
    )
    if ($Response.StatusCode -eq 201 -or $Response.StatusCode -eq 200) {
        try {
            $obj = $Response.Content | ConvertFrom-Json
            if ($obj -and $obj.id) { return $obj.id }
        } catch {}
    }
    if ($Response.StatusCode -ne 202) { return $null }

    $opId = $Response.Headers["x-ms-operation-id"]
    if ($opId -is [array]) { $opId = $opId[0] }
    if (-not $opId) {
        $loc = $Response.Headers["Location"]
        if ($loc -is [array]) { $loc = $loc[0] }
        if ($loc -match "/operations/([0-9a-fA-F-]+)") { $opId = $Matches[1] }
    }

    if ($opId) {
        $start = Get-Date
        $token = Get-FabricToken
        $headers = @{ "Authorization" = "Bearer $token" }
        while ((New-TimeSpan -Start $start).TotalSeconds -lt $TimeoutSec) {
            Start-Sleep -Seconds 3
            try {
                $op = Invoke-RestMethod -Uri "$FabricApiBase/operations/$opId" -Headers $headers
                if ($op.status -eq "Succeeded") {
                    try {
                        $result = Invoke-RestMethod -Uri "$FabricApiBase/operations/$opId/result" -Headers $headers
                        if ($result -and $result.id) { return $result.id }
                    } catch {}
                    break
                } elseif ($op.status -in @('Failed', 'Cancelled')) {
                    Write-Host "    LRO $($op.status): $($op.error.message)" -ForegroundColor Red
                    return $null
                }
            } catch {}
        }
    }

    # Fall back to listing the workspace's items by display name to recover the id
    if ($DiscoveryEndpoint -and $DiscoveryDisplayName) {
        try {
            Start-Sleep -Seconds 5
            $listed = (Invoke-FabricApi -Endpoint $DiscoveryEndpoint).value |
                Where-Object { $_.displayName -eq $DiscoveryDisplayName } |
                Select-Object -First 1
            if ($listed) { return $listed.id }
        } catch {}
    }
    return $null
}

# ============================================================================
# DISCOVER WORKSPACE + LAKEHOUSES
# ============================================================================

Write-Host ""
Write-Host "  --- Deploying CMS Quality Scorecard to Fabric ---" -ForegroundColor Cyan
Write-Host ""

$ws = (Invoke-FabricApi -Endpoint "/workspaces").value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if (-not $ws) { throw "Workspace '$FabricWorkspaceName' not found" }
$workspaceId = $ws.id
Write-Host "  ✓ Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Green

$lakehouses = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses").value

$silverLh = $lakehouses | Where-Object { $_.displayName -eq $SilverLhName } | Select-Object -First 1
if (-not $silverLh) {
    # Fall back to the default healthcare1 silver name pattern
    $silverLh = $lakehouses | Where-Object { $_.displayName -match "[Ss]ilver" } | Select-Object -First 1
}
if (-not $silverLh) { throw "Silver Lakehouse not found in workspace '$FabricWorkspaceName'" }
$silverLhId = $silverLh.id
Write-Host "  ✓ Silver Lakehouse: $($silverLh.displayName) ($silverLhId)" -ForegroundColor Green

$reportingLh = $lakehouses | Where-Object { $_.displayName -eq $ReportingLhName } | Select-Object -First 1
if (-not $reportingLh) {
    $reportingLh = $lakehouses | Where-Object { $_.displayName -match "[Rr]eporting.*[Gg]old" } | Select-Object -First 1
}
if (-not $reportingLh) {
    $reportingLh = $lakehouses | Where-Object { $_.displayName -match "[Gg]old" } | Select-Object -First 1
}
if (-not $reportingLh) { throw "Reporting Gold Lakehouse not found in workspace '$FabricWorkspaceName'" }
$reportingLhId = $reportingLh.id
Write-Host "  ✓ Reporting Lakehouse: $($reportingLh.displayName) ($reportingLhId)" -ForegroundColor Green

# Get Reporting Lakehouse SQL endpoint
$reportingLhDetail = Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/lakehouses/$reportingLhId"
$reportingServer = $reportingLhDetail.properties.sqlEndpointProperties.connectionString
$reportingDbName = $reportingLh.displayName
if (-not $reportingServer) {
    throw "Could not discover Reporting Lakehouse SQL endpoint connection string. The lakehouse may still be provisioning its SQL endpoint."
}
Write-Host "  ✓ Reporting SQL: $reportingServer / $reportingDbName" -ForegroundColor Green

# ============================================================================
# STEP 10a — CREATE & RUN MATERIALIZATION NOTEBOOK
# ============================================================================

Write-Host ""
Write-Host "  --- Step 10a: Claims & Quality Materialization ---" -ForegroundColor Cyan

$qualityNotebookPath = Join-Path $ReportSourcePath "materialize_claims_quality.py"
if (-not (Test-Path $qualityNotebookPath)) {
    throw "Materialization notebook source not found at: $qualityNotebookPath"
}

$pyContent = Get-Content $qualityNotebookPath -Raw

# Patch the lakehouse name constants so the notebook always uses the
# values we discovered (handles workspaces with non-default lakehouse names).
$pyContent = $pyContent -replace 'SILVER_LAKEHOUSE\s*=\s*"[^"]*"',
    "SILVER_LAKEHOUSE = `"$($silverLh.displayName)`""
$pyContent = $pyContent -replace 'GOLD_LAKEHOUSE\s*=\s*"[^"]*"',
    "GOLD_LAKEHOUSE = `"$($reportingLh.displayName)`""

# Build an .ipynb with the Reporting Gold lakehouse attached as the default
# so cross-lakehouse `<name>.dbo.<table>` references resolve at runtime.
$cellSource = @($pyContent -split "`n") | ForEach-Object { "$_`n" }
$ipynb = [ordered]@{
    nbformat = 4
    nbformat_minor = 5
    metadata = [ordered]@{
        language_info = @{ name = "python" }
        kernel_info = @{ name = "synapse_pyspark" }
        kernelspec = @{ name = "synapse_pyspark"; display_name = "Synapse PySpark" }
        # `trident` is the modern Fabric-notebook lakehouse-binding key
        trident = [ordered]@{
            lakehouse = [ordered]@{
                default_lakehouse = $reportingLhId
                default_lakehouse_name = $reportingLh.displayName
                default_lakehouse_workspace_id = $workspaceId
                known_lakehouses = @(
                    @{ id = $reportingLhId },
                    @{ id = $silverLhId }
                )
            }
        }
    }
    cells = @(
        @{
            cell_type = "code"
            source = $cellSource
            metadata = @{}
            outputs = @()
            execution_count = $null
        }
    )
}

$ipynbJson = $ipynb | ConvertTo-Json -Depth 20
$ipynbB64 = To-B64 $ipynbJson

$nbName = "NB_Materialize_Claims_Quality"
$nbParts = @(
    @{ path = "notebook-content.ipynb"; payload = $ipynbB64; payloadType = "InlineBase64" }
)

# Look up existing notebook
$existingNb = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Notebook").value |
    Where-Object { $_.displayName -eq $nbName } | Select-Object -First 1

if ($SkipNotebookDeploy) {
    if ($existingNb) {
        $nbId = $existingNb.id
        Write-Host "  ⏭ Skipping notebook deploy — using existing '$nbName' ($nbId)" -ForegroundColor DarkGray
    } else {
        Write-Host "  ⏭ Skipping notebook deploy — no existing notebook found" -ForegroundColor DarkGray
        $nbId = $null
    }
}
else {

$nbId = $null
$token = Get-FabricToken
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

if ($existingNb) {
    $nbId = $existingNb.id
    Write-Host "  Notebook '$nbName' exists ($nbId) — updating definition..." -ForegroundColor White
    $updateBody = @{ definition = @{ format = "ipynb"; parts = $nbParts } } | ConvertTo-Json -Depth 20
    $updateOk = $false
    try {
        $resp = Invoke-WebRequest -Method POST `
            -Uri "$FabricApiBase/workspaces/$workspaceId/items/$nbId/updateDefinition?updateMetadata=true" `
            -Headers $headers -Body $updateBody -UseBasicParsing
        if ($resp.StatusCode -eq 202) {
            $null = Resolve-LroResult -Response $resp -TimeoutSec 120
        }
        $updateOk = $true
        Write-Host "  ✓ Notebook definition applied" -ForegroundColor Green
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            $updateOk = $true
        } else {
            Write-Host "  ⚠ Update failed ($errCode): $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }

    if (-not $updateOk) {
        # Fall back to delete + recreate (handles part-path mismatch on
        # legacy notebooks created with a different content path).
        Write-Host "  Falling back to delete + recreate..." -ForegroundColor Yellow
        try {
            Invoke-RestMethod -Method DELETE `
                -Uri "$FabricApiBase/workspaces/$workspaceId/items/$nbId" `
                -Headers $headers | Out-Null
            Start-Sleep -Seconds 5
            $existingNb = $null
            $nbId = $null
        } catch {
            Write-Host "  ⚠ Could not delete: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

if (-not $existingNb -or -not $nbId) {
    Write-Host "  Creating notebook '$nbName'..." -ForegroundColor White
    $createBody = @{
        displayName = $nbName
        type = "Notebook"
        definition = @{ format = "ipynb"; parts = $nbParts }
    } | ConvertTo-Json -Depth 20

    # When we just deleted an existing notebook, the display name can be
    # held for ~1 min and create returns ItemDisplayNameNotAvailableYet (409).
    # Retry up to 12x with 15s gaps (~3 min) — well within the documented hold.
    $createOk = $false
    for ($attempt = 1; $attempt -le 12 -and -not $createOk; $attempt++) {
        try {
            $resp = Invoke-WebRequest -Method POST `
                -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
                -Headers $headers -Body $createBody -UseBasicParsing
            $nbId = Resolve-LroResult -Response $resp -TimeoutSec 180 `
                -DiscoveryEndpoint "/workspaces/$workspaceId/items?type=Notebook" `
                -DiscoveryDisplayName $nbName
            $createOk = $true
        } catch {
            $errCode = $null
            $errBody = $null
            try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
            try { $errBody = $_.ErrorDetails.Message } catch {}
            if ($errCode -eq 202) {
                Start-Sleep -Seconds 5
                $existingNb = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Notebook").value |
                    Where-Object { $_.displayName -eq $nbName } | Select-Object -First 1
                $nbId = $existingNb.id
                $createOk = $true
            } elseif ($errBody -and $errBody -match "ItemDisplayNameNotAvailableYet") {
                Write-Host "    Display name still held — retrying in 15s ($attempt/12)..." -ForegroundColor DarkGray
                Start-Sleep -Seconds 15
            } else {
                throw
            }
        }
    }
    if (-not $nbId) {
        throw "Failed to create notebook '$nbName' — no ID returned and not discoverable in workspace listing"
    }
    Write-Host "  ✓ Notebook: $nbName ($nbId)" -ForegroundColor Green
}
} # end if (-not $SkipNotebookDeploy)

# Run the notebook
if (-not $SkipNotebookRun -and -not $SkipNotebookDeploy -and $nbId) {
    Write-Host "  Running materialization notebook (max $NotebookTimeoutMin min)..." -ForegroundColor White
    try {
        $token = Get-FabricToken
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        Invoke-WebRequest -Method POST `
            -Uri "$FabricApiBase/workspaces/$workspaceId/items/$nbId/jobs/instances?jobType=RunNotebook" `
            -Headers $headers -Body '{}' -UseBasicParsing | Out-Null
        Write-Host "  ✓ Notebook run invoked — polling for completion..." -ForegroundColor Green

        $nbStart = Get-Date
        while ((New-TimeSpan -Start $nbStart).TotalMinutes -lt $NotebookTimeoutMin) {
            Start-Sleep 30
            try {
                $token = Get-FabricToken
                $headers = @{ "Authorization" = "Bearer $token" }
                $nbJobs = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/items/$nbId/jobs/instances?limit=1" -Headers $headers).value
                $nbElapsed = [math]::Round((New-TimeSpan -Start $nbStart).TotalMinutes, 1)
                if (-not $nbJobs -or $nbJobs.Count -eq 0) {
                    Write-Host "    [$nbElapsed min] (no job instances yet)" -ForegroundColor DarkGray
                    continue
                }
                $jobStatus = $nbJobs[0].status
                Write-Host "    [$nbElapsed min] Status: $jobStatus" -ForegroundColor DarkGray
                if ($jobStatus -eq 'Completed') {
                    Write-Host "  ✓ Materialization complete ($nbElapsed min)" -ForegroundColor Green
                    break
                } elseif ($jobStatus -in @('Failed', 'Cancelled')) {
                    Write-Host "  ⚠ Notebook $jobStatus after $nbElapsed min" -ForegroundColor Yellow
                    if ($nbJobs[0].failureReason) {
                        Write-Host "    Reason: $($nbJobs[0].failureReason.message)" -ForegroundColor Yellow
                    }
                    break
                }
            } catch {
                Write-Host "    Poll error (will retry): $($_.Exception.Message)" -ForegroundColor DarkGray
            }
        }
    } catch {
        Write-Host "  ⚠ Could not run notebook: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# ============================================================================
# STEP 10b — DEPLOY CMS QUALITY SCORECARD REPORT
# ============================================================================

Write-Host ""
Write-Host "  --- Step 10b: CMS Quality Scorecard Report ---" -ForegroundColor Cyan

$reportRoot = Join-Path $ReportSourcePath "cms-quality-report"
$smRoot     = Join-Path $reportRoot "CMS Quality Scorecard.SemanticModel"
$rptRoot    = Join-Path $reportRoot "CMS Quality Scorecard.Report"
if (-not (Test-Path $smRoot))  { throw "Semantic Model source not found: $smRoot" }
if (-not (Test-Path $rptRoot)) { throw "Report source not found: $rptRoot" }

# ── Build Semantic Model parts ──────────────────────────────────────────────
Write-Host "  Building Semantic Model definition..." -ForegroundColor White

$smDir = Join-Path $smRoot "definition"
$smParts = @()

# .platform
$smPlatform = @{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
    metadata  = @{ type = "SemanticModel"; displayName = "CMS Quality Scorecard" }
    config    = @{ version = "2.0"; logicalId = [guid]::NewGuid().ToString() }
} | ConvertTo-Json -Depth 5
$smParts += @{ path = ".platform"; payload = (To-B64 $smPlatform); payloadType = "InlineBase64" }

# definition.pbism
$pbismPath = Join-Path $smRoot "definition.pbism"
$smParts += @{ path = "definition.pbism"; payload = (To-B64 (Get-Content $pbismPath -Raw)); payloadType = "InlineBase64" }

# All TMDL files under definition/, with placeholder patching
Get-ChildItem $smDir -Recurse -File -Filter "*.tmdl" | ForEach-Object {
    $rel = "definition/" + $_.FullName.Substring($smDir.Length + 1).Replace("\","/")
    $content = Get-Content $_.FullName -Raw -Encoding UTF8
    if ($_.Name -eq "expressions.tmdl") {
        $content = $content -replace '"__SQL_ENDPOINT__"',  "`"$reportingServer`""
        $content = $content -replace '"__DATABASE_NAME__"', "`"$reportingDbName`""
    }
    $smParts += @{ path = $rel; payload = (To-B64 $content); payloadType = "InlineBase64" }
}
Write-Host "  ✓ Semantic Model: $($smParts.Count) definition parts" -ForegroundColor Green

# ── Build Report parts ──────────────────────────────────────────────────────
Write-Host "  Building Report definition..." -ForegroundColor White

$rptDir = Join-Path $rptRoot "definition"
$rptParts = @()

# .platform
$rptPlatform = @{
    '$schema' = "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json"
    metadata  = @{ type = "Report"; displayName = "CMS Quality Scorecard" }
    config    = @{ version = "2.0"; logicalId = [guid]::NewGuid().ToString() }
} | ConvertTo-Json -Depth 5
$rptParts += @{ path = ".platform"; payload = (To-B64 $rptPlatform); payloadType = "InlineBase64" }

if (Test-Path $rptDir) {
    Get-ChildItem $rptDir -Recurse -File | ForEach-Object {
        $rel = "definition/" + $_.FullName.Substring($rptDir.Length + 1).Replace("\","/")
        $content = Get-Content $_.FullName -Raw -Encoding UTF8
        $rptParts += @{ path = $rel; payload = (To-B64 $content); payloadType = "InlineBase64" }
    }
}
Write-Host "  ✓ Report: $($rptParts.Count) definition parts (initial — pbir patched after SM creation)" -ForegroundColor Green

# ── Create or update Semantic Model ─────────────────────────────────────────
Write-Host ""
Write-Host "  Deploying Semantic Model..." -ForegroundColor White

$smName = "CMS Quality Scorecard"
$existingSm = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=SemanticModel").value |
    Where-Object { $_.displayName -eq $smName } | Select-Object -First 1

$smId = $null
$token = Get-FabricToken
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

if ($existingSm) {
    $smId = $existingSm.id
    Write-Host "  ✓ Existing: $smName ($smId) — updating definition" -ForegroundColor Green
} else {
    Write-Host "  Creating Semantic Model '$smName'..." -ForegroundColor White
    $createBody = @{
        displayName = $smName
        type        = "SemanticModel"
        definition  = @{ parts = $smParts }
    } | ConvertTo-Json -Depth 20
    try {
        $resp = Invoke-WebRequest -Method POST `
            -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
            -Headers $headers -Body $createBody -UseBasicParsing
        $smId = Resolve-LroResult -Response $resp -TimeoutSec 180 `
            -DiscoveryEndpoint "/workspaces/$workspaceId/items?type=SemanticModel" `
            -DiscoveryDisplayName $smName
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Start-Sleep -Seconds 10
            $existingSm = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=SemanticModel").value |
                Where-Object { $_.displayName -eq $smName } | Select-Object -First 1
            $smId = $existingSm.id
        } else {
            throw
        }
    }
    if (-not $smId) { throw "Failed to create Semantic Model '$smName'" }
    Write-Host "  ✓ Created: $smName ($smId)" -ForegroundColor Green
}

# Push the latest definition (handles existing-with-stale-defn and the "create just returned id" case)
try {
    $token = Get-FabricToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $updateBody = @{ definition = @{ parts = $smParts } } | ConvertTo-Json -Depth 20
    $resp = Invoke-WebRequest -Method POST `
        -Uri "$FabricApiBase/workspaces/$workspaceId/items/$smId/updateDefinition?updateMetadata=true" `
        -Headers $headers -Body $updateBody -UseBasicParsing
    if ($resp.StatusCode -eq 202) {
        $null = Resolve-LroResult -Response $resp -TimeoutSec 120
    }
    Write-Host "  ✓ Semantic Model definition applied" -ForegroundColor Green
} catch {
    $errCode = $null
    try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
    if ($errCode -eq 202) {
        Write-Host "  ✓ Semantic Model definition update accepted (202)" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Failed to update Semantic Model: $($_.Exception.Message)" -ForegroundColor Yellow
        try { Write-Host "    $($_.ErrorDetails.Message)" -ForegroundColor DarkRed } catch {}
    }
}

# ── Create or update Report ─────────────────────────────────────────────────
Write-Host ""
Write-Host "  Deploying Report..." -ForegroundColor White

$rptName = "CMS Quality Scorecard"

# Inject definition.pbir referencing the semantic model byConnection
$pbir = @{
    '$schema'        = "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json"
    version          = "4.0"
    datasetReference = @{
        byConnection = @{
            connectionString = "Data Source=pbiazure://api.powerbi.com;Initial Catalog=$smName;semanticModelId=$smId;Integrated Security=ClaimsToken"
        }
    }
} | ConvertTo-Json -Depth 10

$rptParts = $rptParts | Where-Object { $_.path -ne "definition.pbir" }
$rptParts = @(@{ path = "definition.pbir"; payload = (To-B64 $pbir); payloadType = "InlineBase64" }) + $rptParts

$existingRpt = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Report").value |
    Where-Object { $_.displayName -eq $rptName } | Select-Object -First 1

$rptId = $null
if ($existingRpt) {
    $rptId = $existingRpt.id
    Write-Host "  ✓ Existing: $rptName ($rptId) — updating definition" -ForegroundColor Green
} else {
    Write-Host "  Creating Report '$rptName'..." -ForegroundColor White
    $createBody = @{
        displayName = $rptName
        type        = "Report"
        definition  = @{ parts = $rptParts }
    } | ConvertTo-Json -Depth 20
    try {
        $token = Get-FabricToken
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        $resp = Invoke-WebRequest -Method POST `
            -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
            -Headers $headers -Body $createBody -UseBasicParsing
        $rptId = Resolve-LroResult -Response $resp -TimeoutSec 180 `
            -DiscoveryEndpoint "/workspaces/$workspaceId/items?type=Report" `
            -DiscoveryDisplayName $rptName
    } catch {
        $errCode = $null
        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
        if ($errCode -eq 202) {
            Start-Sleep -Seconds 10
            $existingRpt = (Invoke-FabricApi -Endpoint "/workspaces/$workspaceId/items?type=Report").value |
                Where-Object { $_.displayName -eq $rptName } | Select-Object -First 1
            $rptId = $existingRpt.id
        } else {
            throw
        }
    }
    if (-not $rptId) { throw "Failed to create Report '$rptName'" }
    Write-Host "  ✓ Created: $rptName ($rptId)" -ForegroundColor Green
}

# Push the latest definition
try {
    $token = Get-FabricToken
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $updateBody = @{ definition = @{ parts = $rptParts } } | ConvertTo-Json -Depth 20
    $resp = Invoke-WebRequest -Method POST `
        -Uri "$FabricApiBase/workspaces/$workspaceId/items/$rptId/updateDefinition?updateMetadata=true" `
        -Headers $headers -Body $updateBody -UseBasicParsing
    if ($resp.StatusCode -eq 202) {
        $null = Resolve-LroResult -Response $resp -TimeoutSec 120
    }
    Write-Host "  ✓ Report definition applied" -ForegroundColor Green
} catch {
    $errCode = $null
    try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
    if ($errCode -eq 202) {
        Write-Host "  ✓ Report definition update accepted (202)" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Failed to update Report: $($_.Exception.Message)" -ForegroundColor Yellow
        try { Write-Host "    $($_.ErrorDetails.Message)" -ForegroundColor DarkRed } catch {}
    }
}

# ============================================================================
# BIND CREDENTIALS + REFRESH
# ============================================================================

Write-Host ""
Write-Host "  Configuring data source credentials..." -ForegroundColor White

$pbiToken = Get-PbiToken
$pbiHeaders = @{ "Authorization" = "Bearer $pbiToken"; "Content-Type" = "application/json" }

try {
    Invoke-RestMethod -Method POST `
        -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$smId/Default.TakeOver" `
        -Headers $pbiHeaders | Out-Null
    Write-Host "  ✓ Took ownership of semantic model" -ForegroundColor Green
} catch {}

Start-Sleep -Seconds 5
$credentialsBound = $false
try {
    $gwSources = Invoke-RestMethod `
        -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$smId/Default.GetBoundGatewayDataSources" `
        -Headers $pbiHeaders
    foreach ($ds in $gwSources.value) {
        if ($ds.gatewayId -and $ds.gatewayId -ne "00000000-0000-0000-0000-000000000000") {
            $credBody = @{
                credentialDetails = @{
                    credentialType      = "OAuth2"
                    credentials         = '{"credentialData":[]}'
                    encryptedConnection = "Encrypted"
                    encryptionAlgorithm = "None"
                    privacyLevel        = "Organizational"
                }
            } | ConvertTo-Json -Depth 5
            Invoke-RestMethod -Method PATCH `
                -Uri "https://api.powerbi.com/v1.0/myorg/gateways/$($ds.gatewayId)/datasources/$($ds.id)" `
                -Headers $pbiHeaders -Body $credBody | Out-Null
            $credentialsBound = $true
        }
    }
} catch {
    Write-Host "  ⚠ Auto-bind failed: $($_.Exception.Message)" -ForegroundColor Yellow
}

if ($credentialsBound) {
    Write-Host "  ✓ Data source credentials bound automatically" -ForegroundColor Green
    try {
        Invoke-WebRequest -Method POST `
            -Uri "https://api.powerbi.com/v1.0/myorg/groups/$workspaceId/datasets/$smId/refreshes" `
            -Headers $pbiHeaders -Body '{"type":"Full"}' -UseBasicParsing | Out-Null
        Write-Host "  ✓ Refresh triggered" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Could not trigger refresh: $($_.Exception.Message)" -ForegroundColor Yellow
    }
} else {
    Write-Host "  ⚠ Could not auto-bind credentials. Manual configuration required:" -ForegroundColor Yellow
    Write-Host "    Settings: https://app.fabric.microsoft.com/groups/$workspaceId/settings/datasets/$smId" -ForegroundColor Cyan
}

# ============================================================================
# OUTPUT
# ============================================================================

Write-Host ""
Write-Host "  ✓ CMS Quality Scorecard deployment complete" -ForegroundColor Green
Write-Host "    Semantic Model ID: $smId" -ForegroundColor DarkGray
Write-Host "    Report ID:         $rptId" -ForegroundColor DarkGray
Write-Host "    Report URL:        https://app.fabric.microsoft.com/groups/$workspaceId/reports/$rptId" -ForegroundColor Cyan
Write-Host ""

# Emit machine-parseable result for the orchestrator to pick up
[PSCustomObject]@{
    SemanticModelId = $smId
    ReportId        = $rptId
    ReportUrl       = "https://app.fabric.microsoft.com/groups/$workspaceId/reports/$rptId"
    NotebookId      = $nbId
    WorkspaceId     = $workspaceId
} | ConvertTo-Json -Compress | Write-Output
