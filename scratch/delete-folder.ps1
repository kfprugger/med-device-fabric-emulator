# delete-folder.ps1
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

Write-Host "Deleting folder $FolderId..." -ForegroundColor Cyan
try {
    # Fabric uses DELETE on /folders/{id} to delete folders
    $resp = Invoke-WebRequest -Method DELETE -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders/$FolderId" -Headers $headers -UseBasicParsing
    Write-Host "✓ Deleted successfully!" -ForegroundColor Green
} catch {
    Write-Host "⚠ Delete failed: $_" -ForegroundColor Red
}
