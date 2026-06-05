# organize-med-0528-f.ps1
# Script to organize med-0528-f workspace items into clean folders.

$FabricWorkspaceName = "med-0528-f"
$WorkspaceId = "90911f80-867f-46bc-ae31-76eec7159d74"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# Get token
Write-Host "Acquiring access token for Microsoft Fabric API..." -ForegroundColor Gray
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

function Move-Items {
    param(
        [string]$FolderName,
        [array]$ItemTypes
    )
    
    Write-Host ""
    Write-Host "Organizing types ($($ItemTypes -join ', ')) into folder '$FolderName'..." -ForegroundColor Cyan
    
    # 1. Get or create folder
    $folders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" -Headers $headers -Method Get).value
    $folder = $folders | Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
    if (-not $folder) {
        Write-Host "  Folder '$FolderName' not found. Creating..." -ForegroundColor Yellow
        $folderBody = @{ displayName = $FolderName } | ConvertTo-Json -Depth 3
        $folder = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" -Headers $headers -Method Post -Body $folderBody
        Write-Host "  ✓ Created folder '$FolderName'" -ForegroundColor Green
    } else {
        Write-Host "  ✓ Folder '$FolderName' already exists (ID: $($folder.id))" -ForegroundColor Green
    }
    
    # 2. Get items of specified types
    $items = @()
    foreach ($type in $ItemTypes) {
        try {
            $typeItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=$type" -Headers $headers -Method Get).value
            if ($typeItems) { $items += $typeItems }
        } catch {
            Write-Host "    ⚠ Could not query items of type '$type': $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
    
    if ($items.Count -eq 0) {
        Write-Host "  No items found to organize." -ForegroundColor DarkGray
        return
    }
    
    # 3. Move items
    $moved = 0
    foreach ($item in $items) {
        if ($item.folderId -eq $folder.id) {
            Write-Host "  - '$($item.displayName)' ($($item.type)) is already in folder" -ForegroundColor DarkGray
            continue
        }
        
        Write-Host "  Moving '$($item.displayName)' ($($item.type)) into '$FolderName'..." -ForegroundColor Gray
        $moveBody = @{ targetFolderId = $folder.id } | ConvertTo-Json -Depth 3
        
        for ($attempt = 1; $attempt -le 4; $attempt++) {
            try {
                Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$($item.id)/move" -Headers $headers -Method Post -Body $moveBody | Out-Null
                $moved++
                Write-Host "    ✓ Moved successfully" -ForegroundColor Green
                break
            } catch {
                $errCode = $null
                try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
                if ($errCode -eq 429 -and $attempt -lt 4) {
                    $sleepSec = 5 * $attempt
                    Write-Host "    Throttled — retrying in ${sleepSec}s..." -ForegroundColor DarkYellow
                    Start-Sleep -Seconds $sleepSec
                } else {
                    Write-Host "    ⚠ Failed to move: $($_.Exception.Message)" -ForegroundColor Red
                    break
                }
            }
        }
    }
    
    Write-Host "  ✓ Folder '$FolderName' complete ($moved moved, $($items.Count) total)" -ForegroundColor Green
}

# Run the moves!
# 1. Notebooks
Move-Items -FolderName "Notebooks" -ItemTypes @("Notebook")

# 2. Pipelines
Move-Items -FolderName "Pipelines" -ItemTypes @("DataPipeline")

# 3. Reports & Semantic Models
Move-Items -FolderName "Reports and Semantic Models" -ItemTypes @("Report", "SemanticModel")

# 4. Real-Time
Move-Items -FolderName "Real-Time" -ItemTypes @("KQLDashboard", "Eventstream", "Reflex")

# 5. Data Agents
Move-Items -FolderName "Agents" -ItemTypes @("DataAgent")

Write-Host ""
Write-Host "Workspace organization complete!" -ForegroundColor Green
