$ErrorActionPreference = "Stop"
$tenantId = "8d038e6a-9b7d-4cb8-bbcf-e84dff156478"
$ctxs = Get-AzContext -ListAvailable | Where-Object { $_.Tenant.Id -eq $tenantId } | Select-Object -First 1
if (-not $ctxs) { throw "No Az context for tenant $tenantId" }
$null = Set-AzContext -Context $ctxs
$tok = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -TenantId $tenantId).Token
if ($tok -is [System.Security.SecureString]) {
    $tok = [System.Net.NetworkCredential]::new("", $tok).Password
}
$h = @{ "Authorization" = "Bearer $tok"; "Content-Type" = "application/json" }
$wsId = "635782cc-415d-4868-9745-54cea2477e8c"
$nbId = "4dfd9862-6ff8-43ab-8673-b6d8384e7ae3"
$base = "https://api.fabric.microsoft.com/v1"

Write-Host "Submitting notebook run..." -ForegroundColor Cyan
$resp = Invoke-WebRequest -Method POST `
    -Uri "$base/workspaces/$wsId/items/$nbId/jobs/instances?jobType=RunNotebook" `
    -Headers $h -Body '{}' -UseBasicParsing
$loc = $resp.Headers.Location
if ($loc -is [array]) { $loc = $loc[0] }
Write-Host "  Location: $loc" -ForegroundColor DarkGray

$start = Get-Date
$timeoutMin = 30
while ((New-TimeSpan -Start $start).TotalMinutes -lt $timeoutMin) {
    Start-Sleep 20
    $tok = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -TenantId $tenantId).Token
    if ($tok -is [System.Security.SecureString]) { $tok = [System.Net.NetworkCredential]::new("", $tok).Password }
    $h = @{ "Authorization" = "Bearer $tok" }
    $jobs = (Invoke-RestMethod -Uri "$base/workspaces/$wsId/items/$nbId/jobs/instances?limit=1" -Headers $h).value
    $elapsed = [math]::Round((New-TimeSpan -Start $start).TotalMinutes, 1)
    if (-not $jobs -or $jobs.Count -eq 0) {
        Write-Host "  [$elapsed min] no jobs yet..." -ForegroundColor DarkGray
        continue
    }
    $j = $jobs[0]
    Write-Host "  [$elapsed min] status=$($j.status) start=$($j.startTimeUtc)" -ForegroundColor White
    if ($j.status -eq "Completed") {
        Write-Host "  ✓ Notebook completed successfully" -ForegroundColor Green
        exit 0
    }
    if ($j.status -in @("Failed", "Cancelled", "Deduped")) {
        Write-Host "  ✗ Notebook $($j.status)" -ForegroundColor Red
        $j | ConvertTo-Json -Depth 10
        exit 1
    }
}
Write-Host "  Timed out after $timeoutMin min" -ForegroundColor Yellow
exit 1
