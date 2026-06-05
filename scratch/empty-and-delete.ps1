# empty-and-delete.ps1
$WorkspaceId = "90911f80-867f-46bc-ae31-76eec7159d74"
$FolderId = "74e9577b-bf40-42f1-8044-c38e8086b00d"
$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# Get token
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

# 1. Find pipelines
Write-Host "Fetching pipelines..." -ForegroundColor Cyan
$pipelines = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=DataPipeline" -Headers $headers -Method Get).value

# 2. Move them to root (targetFolderId = null)
Write-Host "Moving pipelines to root..." -ForegroundColor Cyan
foreach ($p in $pipelines) {
    if ($p.folderId -eq $FolderId) {
        Write-Host "  Moving '$($p.displayName)' to root..."
        $moveBody = @{ targetFolderId = $null } | ConvertTo-Json -Depth 3
        try {
            Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$($p.id)/move" -Headers $headers -Method Post -Body $moveBody | Out-Null
            Write-Host "    ✓ Moved" -ForegroundColor Green
        } catch {
            Write-Host "    ⚠ Failed to move: $_" -ForegroundColor Red
        }
    }
}

# 3. Delete the folder
Write-Host "Deleting empty folder..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders/$FolderId" -Headers $headers -Method Delete | Out-Null
    Write-Host "✓ Deleted successfully!" -ForegroundColor Green
} catch {
    Write-Host "⚠ Delete failed: $_" -ForegroundColor Red
}
