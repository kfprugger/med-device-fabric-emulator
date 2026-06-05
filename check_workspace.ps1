$token = az account get-access-token --resource "https://api.fabric.microsoft.com/" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token" }
Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/3e4b3074-c565-4a4e-9333-88b30d166886" -Headers $headers | ConvertTo-Json
