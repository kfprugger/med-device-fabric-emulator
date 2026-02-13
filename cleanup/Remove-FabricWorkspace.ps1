# Remove-FabricWorkspace.ps1
# Deletes the Microsoft Fabric workspace (Eventhouse, KQL DB, Eventstream, Lakehouse, etc.)
#
# Usage:
#   .\cleanup\Remove-FabricWorkspace.ps1
#   .\cleanup\Remove-FabricWorkspace.ps1 -FabricWorkspaceName "my-workspace"

param(
    [string]$FabricWorkspaceName = "med-device-real-time",
    [switch]$Force           # Skip confirmation prompt
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|            FABRIC WORKSPACE CLEANUP                        |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host ""

# ── Get Fabric token ──
Write-Host "Authenticating to Fabric API..." -ForegroundColor Cyan
$fabricToken = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
if (-not $fabricToken) {
    Write-Host "ERROR: Failed to get Fabric API token. Run 'az login' first." -ForegroundColor Red
    exit 1
}

$headers = @{
    Authorization = "Bearer $fabricToken"
    "Content-Type" = "application/json"
}

# ── Find workspace ──
Write-Host "Looking for workspace '$FabricWorkspaceName'..." -ForegroundColor Cyan
$workspaces = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $headers
$ws = $workspaces.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }

if (-not $ws) {
    Write-Host "Workspace '$FabricWorkspaceName' not found." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Available workspaces:" -ForegroundColor DarkGray
    $workspaces.value | ForEach-Object { Write-Host "  - $($_.displayName)" -ForegroundColor DarkGray }
    exit 0
}

$workspaceId = $ws.id
Write-Host "  Found: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Cyan
Write-Host ""

# ── List items in workspace ──
Write-Host "Items in workspace:" -ForegroundColor Yellow
try {
    $items = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/items" -Headers $headers
    if ($items.value.Count -gt 0) {
        $items.value | ForEach-Object {
            Write-Host "  $($_.type.PadRight(20)) $($_.displayName)" -ForegroundColor DarkGray
        }
    } else {
        Write-Host "  (empty workspace)" -ForegroundColor DarkGray
    }
} catch {
    Write-Host "  (could not list items)" -ForegroundColor DarkGray
}
Write-Host ""

# ── Confirmation ──
if (-not $Force) {
    $confirm = Read-Host "Delete Fabric workspace '$FabricWorkspaceName' and ALL items above? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 0
    }
}

# ── Delete ──
Write-Host ""
Write-Host "Deleting workspace '$FabricWorkspaceName'..." -ForegroundColor Yellow
Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId" `
    -Method DELETE -Headers $headers

Write-Host "Workspace '$FabricWorkspaceName' deleted." -ForegroundColor Green
Write-Host ""
Write-Host "Done." -ForegroundColor Green
