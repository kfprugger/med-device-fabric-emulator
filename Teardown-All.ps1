<#
.SYNOPSIS
    Complete teardown of all resources: Azure RGs + Fabric workspace + DICOM viewer.

.DESCRIPTION
    Destroys everything deployed by Deploy-All.ps1 across all phases:
    - Phase 1 Azure resources (FHIR, Event Hub, ACR, Storage, ACI)
    - Fabric workspace (Eventhouse, Eventstream, KQL, dashboards, agents, HDS, lakehouses)
    - Phase 3 DICOM Viewer resources (Container App, Static Web App, ACR)

    No -Location, -AdminSecurityGroup, or -PatientCount parameters needed.

.PARAMETER ResourceGroupName
    Azure resource group for Phase 1 infrastructure.

.PARAMETER FabricWorkspaceName
    Fabric workspace name.

.PARAMETER DicomViewerResourceGroup
    Azure resource group for the DICOM viewer (Phase 3).

.PARAMETER SkipAzure
    Skip Azure resource group deletion (Fabric-only cleanup).

.PARAMETER SkipFabric
    Skip Fabric workspace cleanup (Azure-only teardown).

.PARAMETER Force
    Skip confirmation prompts.

.PARAMETER Wait
    Block until all Azure RG deletions complete.

.EXAMPLE
    .\Teardown-All.ps1 -FabricWorkspaceName "med-device-rti-hds" -Force -Wait

.EXAMPLE
    .\Teardown-All.ps1 -FabricWorkspaceName "med-device-rti-hds-0326" `
        -ResourceGroupName "rg-medtech-rti-fhir-0326" -Force -Wait
#>

param(
    [Parameter(Mandatory)][string]$FabricWorkspaceName,
    [string]$ResourceGroupName = "rg-medtech-rti-fhir",
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer",
    [switch]$SkipAzure,
    [switch]$SkipFabric,
    [switch]$Force,
    [switch]$Wait
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$timer = [System.Diagnostics.Stopwatch]::StartNew()

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|             COMPLETE TEARDOWN — ALL PHASES                 |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host ""
Write-Host "  Phase 1 RG:        $ResourceGroupName" -ForegroundColor White
Write-Host "  Viewer RG:         $DicomViewerResourceGroup $(if ($DicomViewerResourceGroup -eq $ResourceGroupName) { '(same as Phase 1)' })" -ForegroundColor White
Write-Host "  Fabric Workspace:  $FabricWorkspaceName" -ForegroundColor White
Write-Host ""

# ── Step 1: Teardown Phase 1 Azure + Fabric ──
Write-Host "── Step 1: Phase 1 Azure + Fabric ──" -ForegroundColor Cyan

$removeArgs = @{
    ResourceGroupName    = $ResourceGroupName
    FabricWorkspaceName  = $FabricWorkspaceName
    DeleteWorkspace      = $true
}
if ($SkipAzure) { $removeArgs['SkipAzure'] = $true }
if ($SkipFabric) { $removeArgs['SkipFabric'] = $true }
if ($Force) { $removeArgs['Force'] = $true }
if ($Wait) { $removeArgs['Wait'] = $true }

& "$ScriptDir\cleanup\Remove-AllResources.ps1" @removeArgs

# ── Step 2: Teardown DICOM Viewer RG (if separate from Phase 1) ──
if (-not $SkipAzure -and $DicomViewerResourceGroup -ne $ResourceGroupName) {
    $viewerExists = az group exists --name $DicomViewerResourceGroup 2>$null
    if ($viewerExists -eq "true") {
        Write-Host ""
        Write-Host "── Step 2: Phase 3 DICOM Viewer RG ──" -ForegroundColor Cyan
        Write-Host "  Deleting resource group '$DicomViewerResourceGroup'..." -ForegroundColor White
        az group delete --name $DicomViewerResourceGroup --yes --no-wait 2>$null
        Write-Host "  Deletion initiated (async)." -ForegroundColor Green

        if ($Wait) {
            Write-Host "  Waiting for deletion..." -ForegroundColor DarkGray
            for ($i = 0; $i -lt 30; $i++) {
                Start-Sleep 30
                $still = az group exists --name $DicomViewerResourceGroup 2>$null
                if ($still -eq "false") {
                    Write-Host "  ✓ DICOM Viewer RG deleted." -ForegroundColor Green
                    break
                }
                Write-Host "    Still deleting..." -ForegroundColor DarkGray
            }
        }
    } else {
        Write-Host ""
        Write-Host "── Step 2: Phase 3 DICOM Viewer RG ──" -ForegroundColor Cyan
        Write-Host "  '$DicomViewerResourceGroup' does not exist — skipping." -ForegroundColor DarkGray
    }
}

$timer.Stop()
$totalMin = [math]::Round($timer.Elapsed.TotalMinutes, 1)

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|             TEARDOWN COMPLETE                              |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "  Duration: $totalMin min" -ForegroundColor Cyan
Write-Host "  Phase 1 RG '$ResourceGroupName': DELETED" -ForegroundColor Green
Write-Host "  Phase 3 RG '$DicomViewerResourceGroup': $(if ($SkipAzure) { 'SKIPPED' } else { 'DELETED' })" -ForegroundColor Green
Write-Host "  Workspace '$FabricWorkspaceName': $(if ($SkipFabric) { 'SKIPPED' } else { 'DELETED' })" -ForegroundColor Green
Write-Host ""
Write-Host "  To redeploy:" -ForegroundColor White
Write-Host "    .\Deploy-All.ps1 -FabricWorkspaceName '$FabricWorkspaceName' -Location eastus -AdminSecurityGroup sg-azure-admins -Tags @{SecurityControl='Ignore'}" -ForegroundColor DarkGray
Write-Host ""
