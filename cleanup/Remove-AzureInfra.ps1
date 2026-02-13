# Remove-AzureInfra.ps1
# Deletes the Azure resource group and all resources within it (FHIR, ACR, Storage, UAMI, ACI, etc.)
#
# Usage:
#   .\cleanup\Remove-AzureInfra.ps1
#   .\cleanup\Remove-AzureInfra.ps1 -ResourceGroupName "my-rg"
#   .\cleanup\Remove-AzureInfra.ps1 -ResourceGroupName "my-rg" -Wait

param(
    [string]$ResourceGroupName = "rg-medtech-sys-identity",
    [switch]$Wait,           # Wait for deletion to complete (default: async)
    [switch]$Force           # Skip confirmation prompt
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|            AZURE INFRASTRUCTURE CLEANUP                     |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host ""

# ── Verify resource group exists ──
$rgInfo = az group show --name $ResourceGroupName 2>$null | ConvertFrom-Json
if (-not $rgInfo) {
    Write-Host "Resource group '$ResourceGroupName' does not exist or was already deleted." -ForegroundColor Yellow
    exit 0
}

Write-Host "Resource Group : $ResourceGroupName" -ForegroundColor Cyan
Write-Host "Location       : $($rgInfo.location)" -ForegroundColor Cyan
Write-Host ""

# ── List resources that will be deleted ──
Write-Host "Resources that will be deleted:" -ForegroundColor Yellow
$resources = az resource list --resource-group $ResourceGroupName --query "[].{Name:name, Type:type}" -o table 2>$null
if ($resources) {
    $resources | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Host "  (no resources found)" -ForegroundColor DarkGray
}
Write-Host ""

# ── Confirmation ──
if (-not $Force) {
    $confirm = Read-Host "Delete resource group '$ResourceGroupName' and ALL resources above? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 0
    }
}

# ── Delete ──
Write-Host ""
if ($Wait) {
    Write-Host "Deleting resource group '$ResourceGroupName' (waiting for completion)..." -ForegroundColor Yellow
    az group delete --name $ResourceGroupName --yes
    Write-Host "Resource group '$ResourceGroupName' deleted." -ForegroundColor Green
} else {
    Write-Host "Deleting resource group '$ResourceGroupName' (async)..." -ForegroundColor Yellow
    az group delete --name $ResourceGroupName --yes --no-wait
    Write-Host "Deletion initiated. Run the following to check status:" -ForegroundColor Green
    Write-Host "  az group show --name `"$ResourceGroupName`" --query `"properties.provisioningState`" -o tsv" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
