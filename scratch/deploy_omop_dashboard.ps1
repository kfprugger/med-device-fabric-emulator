# deploy_omop_dashboard.ps1
# Deploys the OMOP Academic Research Dashboard report and semantic model to workspace med-0528-f

$WorkspaceId = "90911f80-867f-46bc-ae31-76eec7159d74"
$FabricWorkspaceName = "med-0528-f"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BaseReportDir = Join-Path (Split-Path $ScriptDir -Parent) "phase-2\omop-research-report"

Write-Host "=============================================================" -ForegroundColor Cyan
Write-Host "   DEPLOYING OMOP ACADEMIC RESEARCH DASHBOARD TO FABRIC      " -ForegroundColor Cyan
Write-Host "=============================================================" -ForegroundColor Cyan
Write-Host "Workspace: $FabricWorkspaceName ($WorkspaceId)" -ForegroundColor White
Write-Host "Source:    $BaseReportDir" -ForegroundColor White
Write-Host ""

# 1. Acquire access token for Fabric API
Write-Host "Acquiring access token..." -ForegroundColor Gray
$tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -ErrorAction Stop
$token = $tokenObj.Token
if ($token -is [System.Security.SecureString]) {
    $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($token)
    try { $token = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

# Helper to base64 encode a file
function Get-Base64FileContent {
    param([string]$FilePath)
    $bytes = [System.IO.File]::ReadAllBytes($FilePath)
    return [System.Convert]::ToBase64String($bytes)
}

# Helper to check if an item exists and delete it to prevent naming conflicts
function Remove-ExistingItem {
    param([string]$DisplayName, [string]$Type)
    Write-Host "Checking for existing $Type '$DisplayName'..." -ForegroundColor Gray
    try {
        $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=$Type" -Headers $headers -Method Get).value
        $matched = $items | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
        if ($matched) {
            Write-Host "  Found existing $($Type): $($matched.id). Deleting..." -ForegroundColor Yellow
            Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$($matched.id)" -Headers $headers -Method Delete | Out-Null
            Write-Host "  ✓ Deleted existing item." -ForegroundColor Green
            Start-Sleep -Seconds 2
        } else {
            Write-Host "  No existing item found." -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "  ⚠ Warning during check: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# ═══════════════════════════════════════════════════════════════════════
# STEP 1: DEPLOY SEMANTIC MODEL
# ═══════════════════════════════════════════════════════════════════════

$modelFolderName = "OMOP Academic Research Dashboard.SemanticModel"
$modelDir = Join-Path $BaseReportDir $modelFolderName

if (-not (Test-Path $modelDir)) {
    Write-Host "ERROR: Semantic Model source directory not found at $modelDir" -ForegroundColor Red
    exit 1
}

# Delete existing model if any
Remove-ExistingItem -DisplayName "OMOP Academic Research Dashboard" -Type "SemanticModel"

Write-Host "Staging Semantic Model definition parts..." -ForegroundColor Cyan
$modelParts = @()

# Gather all files in the model folder recursively
$modelFiles = Get-ChildItem -Path $modelDir -File -Recurse
foreach ($file in $modelFiles) {
    # Get relative path with forward slashes for Fabric API
    $relPath = $file.FullName.Substring($modelDir.Length + 1).Replace("\", "/")
    
    # Skip platform-specific hidden files if any
    if ($relPath -like "*.DS_Store*") { continue }
    
    $base64Content = Get-Base64FileContent -FilePath $file.FullName
    $modelParts += @{
        path = $relPath
        payloadType = "InlineBase64"
        payload = $base64Content
    }
    Write-Host "  Added model part: $relPath" -ForegroundColor DarkGray
}

$modelBody = @{
    displayName = "OMOP Academic Research Dashboard"
    type = "SemanticModel"
    definition = @{
        parts = $modelParts
    }
} | ConvertTo-Json -Depth 100

Write-Host "Uploading Semantic Model to Fabric workspace..." -ForegroundColor Yellow
try {
    $modelResponse = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $headers -Method Post -Body $modelBody -UseBasicParsing
    Write-Host "  Model upload initiated. Polling workspace for item creation..." -ForegroundColor Gray
    
    $newModelId = $null
    $startPoll = Get-Date
    while ((New-TimeSpan -Start $startPoll).TotalSeconds -lt 60) {
        Start-Sleep -Seconds 3
        $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Headers $headers -Method Get).value
        $matched = $items | Where-Object { $_.displayName -eq "OMOP Academic Research Dashboard" } | Select-Object -First 1
        if ($matched) {
            $newModelId = $matched.id
            break
        }
    }
    
    if (-not $newModelId) {
        throw "Timeout waiting for Semantic Model creation in workspace."
    }
    
    Write-Host "✓ Semantic Model deployed successfully!" -ForegroundColor Green
    Write-Host "  ID: $newModelId" -ForegroundColor Gray
} catch {
    Write-Host "✗ FAILED to deploy Semantic Model: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $respBody = $reader.ReadToEnd()
        Write-Host "  Response Detail: $respBody" -ForegroundColor Red
    }
    exit 1
}

# ═══════════════════════════════════════════════════════════════════════
# STEP 2: DEPLOY REPORT
# ═══════════════════════════════════════════════════════════════════════

$reportFolderName = "OMOP Academic Research Dashboard.Report"
$reportDir = Join-Path $BaseReportDir $reportFolderName

if (-not (Test-Path $reportDir)) {
    Write-Host "ERROR: Report source directory not found at $reportDir" -ForegroundColor Red
    exit 1
}

# Delete existing report if any
Remove-ExistingItem -DisplayName "OMOP Academic Research Dashboard" -Type "Report"

Write-Host "Staging Report definition parts..." -ForegroundColor Cyan
$reportParts = @()

# Gather all files in the report folder recursively
$reportFiles = Get-ChildItem -Path $reportDir -File -Recurse
foreach ($file in $reportFiles) {
    # Get relative path with forward slashes
    $relPath = $file.FullName.Substring($reportDir.Length + 1).Replace("\", "/")
    
    if ($relPath -like "*.DS_Store*") { continue }
    
    # If this is report.json, we must patch the dataset connection reference with the newly created model ID
    if ($relPath -eq "definition/report.json") {
        $reportJsonObj = Get-Content $file.FullName -Raw | ConvertFrom-Json
        $reportJsonObj.datasetReference.byConnection = @{
            connectionString = $null
            pbiServiceModelId = $null
            pbiModelVirtualConnectionId = $null
            datasetId = $newModelId
        }
        $patchedJsonStr = $reportJsonObj | ConvertTo-Json -Depth 10
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($patchedJsonStr)
        $base64Content = [System.Convert]::ToBase64String($bytes)
        Write-Host "  Patched datasetReference in report.json to model $newModelId" -ForegroundColor Magenta
    } else {
        $base64Content = Get-Base64FileContent -FilePath $file.FullName
    }
    
    $reportParts += @{
        path = $relPath
        payloadType = "InlineBase64"
        payload = $base64Content
    }
    Write-Host "  Added report part: $relPath" -ForegroundColor DarkGray
}

$reportBody = @{
    displayName = "OMOP Academic Research Dashboard"
    type = "Report"
    definition = @{
        parts = $reportParts
    }
} | ConvertTo-Json -Depth 100

Write-Host "Uploading Report to Fabric workspace..." -ForegroundColor Yellow
try {
    $reportResponse = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $headers -Method Post -Body $reportBody -UseBasicParsing
    Write-Host "  Report upload initiated. Polling workspace for item creation..." -ForegroundColor Gray
    
    $newReportId = $null
    $startPoll = Get-Date
    while ((New-TimeSpan -Start $startPoll).TotalSeconds -lt 60) {
        Start-Sleep -Seconds 3
        $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Report" -Headers $headers -Method Get).value
        $matched = $items | Where-Object { $_.displayName -eq "OMOP Academic Research Dashboard" } | Select-Object -First 1
        if ($matched) {
            $newReportId = $matched.id
            break
        }
    }
    
    if (-not $newReportId) {
        throw "Timeout waiting for Report creation in workspace."
    }
    
    Write-Host "✓ Report deployed successfully!" -ForegroundColor Green
    Write-Host "  ID: $newReportId" -ForegroundColor Gray
} catch {
    Write-Host "✗ FAILED to deploy Report: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
        $respBody = $reader.ReadToEnd()
        Write-Host "  Response Detail: $respBody" -ForegroundColor Red
    }
    exit 1
}

# ═══════════════════════════════════════════════════════════════════════
# STEP 3: ORGANIZE INTO REPORTS FOLDER
# ═══════════════════════════════════════════════════════════════════════

Write-Host ""
Write-Host "Organizing items into folder 'Reports and Semantic Models'..." -ForegroundColor Cyan

# 1. Get or create folder
$folders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" -Headers $headers -Method Get).value
$folder = $folders | Where-Object { $_.displayName -eq "Reports and Semantic Models" } | Select-Object -First 1
if (-not $folder) {
    Write-Host "  Creating folder..." -ForegroundColor Yellow
    $folderBody = @{ displayName = "Reports and Semantic Models" } | ConvertTo-Json -Depth 3
    $folder = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" -Headers $headers -Method Post -Body $folderBody
    Write-Host "  ✓ Created folder 'Reports and Semantic Models'" -ForegroundColor Green
}

# 2. Move semantic model
try {
    $moveBody = @{ targetFolderId = $folder.id } | ConvertTo-Json -Depth 3
    Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$newModelId/move" -Headers $headers -Method Post -Body $moveBody | Out-Null
    Write-Host "  ✓ Moved Semantic Model into 'Reports and Semantic Models'" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Could not move Semantic Model: $($_.Exception.Message)" -ForegroundColor Yellow
}

# 3. Move report
try {
    Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$newReportId/move" -Headers $headers -Method Post -Body $moveBody | Out-Null
    Write-Host "  ✓ Moved Report into 'Reports and Semantic Models'" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Could not move Report: $($_.Exception.Message)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=============================================================" -ForegroundColor Green
Write-Host "  OMOP ACADEMIC RESEARCH DASHBOARD FULLY DEPLOYED & ORGANIZED" -ForegroundColor Green
Write-Host "=============================================================" -ForegroundColor Green
Write-Host ""
