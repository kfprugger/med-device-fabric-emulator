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
    [string]$FabricWorkspaceName,
    [string]$ResourceGroupName,
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer",
    [switch]$SkipAzure,
    [switch]$SkipFabric,
    [switch]$Force,
    [switch]$Wait
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$timer = [System.Diagnostics.Stopwatch]::StartNew()

# Read deployment state for defaults if parameters not provided
$stateFile = Join-Path $ScriptDir ".deployment-state.json"
if (Test-Path $stateFile) {
    $state = Get-Content $stateFile -Raw | ConvertFrom-Json
    $lastPhase = $state.phases | Select-Object -Last 1
    if ($lastPhase.resources) {
        if (-not $FabricWorkspaceName -and $lastPhase.resources.FabricWorkspaceName) {
            $FabricWorkspaceName = $lastPhase.resources.FabricWorkspaceName
            Write-Host "  (Using workspace from .deployment-state.json: $FabricWorkspaceName)" -ForegroundColor DarkGray
        }
        if (-not $ResourceGroupName -and $lastPhase.resources.ResourceGroupName) {
            $ResourceGroupName = $lastPhase.resources.ResourceGroupName
            Write-Host "  (Using RG from .deployment-state.json: $ResourceGroupName)" -ForegroundColor DarkGray
        }
    }
}

if (-not $FabricWorkspaceName) { throw "Parameter '-FabricWorkspaceName' is required (no .deployment-state.json found)" }
if (-not $ResourceGroupName) { $ResourceGroupName = "rg-medtech-rti-fhir" }

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|             COMPLETE TEARDOWN — ALL PHASES                 |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host ""
Write-Host "  Phase 1 RG:        $ResourceGroupName" -ForegroundColor White
Write-Host "  Viewer RG:         $DicomViewerResourceGroup $(if ($DicomViewerResourceGroup -eq $ResourceGroupName) { '(same as Phase 1)' })" -ForegroundColor White
Write-Host "  Fabric Workspace:  $FabricWorkspaceName" -ForegroundColor White
Write-Host ""

# Validate resource group exists before attempting deletion
if (-not $SkipAzure) {
    $rgExists = az group exists --name $ResourceGroupName 2>$null
    if ($rgExists -ne "true") {
        Write-Host "  ⚠ Resource group '$ResourceGroupName' does not exist in Azure." -ForegroundColor Yellow
        Write-Host "    Available RGs with similar names:" -ForegroundColor DarkGray
        $similar = az group list --query "[?contains(name,'med') || contains(name,'rti') || contains(name,'fhir')].name" -o tsv 2>$null
        if ($similar) {
            $similar -split "`n" | ForEach-Object { if ($_) { Write-Host "      - $_" -ForegroundColor Cyan } }
        } else {
            Write-Host "      (none found)" -ForegroundColor DarkGray
        }
        Write-Host ""
        if (-not $Force) {
            $confirm = Read-Host "  Continue with Fabric-only teardown? (yes/no)"
            if ($confirm -ne "yes") { Write-Host "  Aborted."; exit 0 }
        }
        $SkipAzure = $true
        Write-Host "  Skipping Azure RG deletion (does not exist)." -ForegroundColor Yellow
        Write-Host ""
    }
}

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

# Clean up state file
if (Test-Path $stateFile) {
    Remove-Item $stateFile -Force
    Write-Host "  Deployment state file removed." -ForegroundColor DarkGray
}

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
Write-Host "    .\Deploy-All.ps1 -FabricWorkspaceName '$FabricWorkspaceName' -ResourceGroupName '$ResourceGroupName' -Location eastus -AdminSecurityGroup sg-azure-admins -Tags @{SecurityControl='Ignore'}" -ForegroundColor DarkGray
Write-Host ""
