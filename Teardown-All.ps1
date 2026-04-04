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
$stateFile = $null
# Try workspace-specific state file first, fall back to legacy
if ($FabricWorkspaceName) {
    $stateFile = Join-Path $ScriptDir ".deployment-state-$FabricWorkspaceName.json"
}
if (-not $stateFile -or -not (Test-Path $stateFile)) {
    # Fall back to legacy state file
    $legacyStateFile = Join-Path $ScriptDir ".deployment-state.json"
    if (Test-Path $legacyStateFile) { $stateFile = $legacyStateFile }
}
if ($stateFile -and (Test-Path $stateFile)) {
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

if (-not $FabricWorkspaceName) { throw "Parameter '-FabricWorkspaceName' is required (no state file found)" }
if (-not $ResourceGroupName) { $ResourceGroupName = "rg-medtech-rti-fhir" }

# ============================================================================
# HELPER: Fuzzy "Did you mean?" suggestion
# ============================================================================

function Get-FuzzySuggestion {
    param(
        [string]$SearchTerm,
        [string[]]$Candidates,
        [int]$MaxResults = 5
    )
    # Score each candidate: shared words, prefix overlap, partial word matches
    $scored = foreach ($c in $Candidates) {
        $score = 0
        # Exact substring match in either direction
        if ($c -like "*$SearchTerm*" -or $SearchTerm -like "*$c*") { $score += 100 }
        # Shared word overlap (split on - and _)
        $searchWords = $SearchTerm -split '[-_\s]' | Where-Object { $_.Length -gt 1 }
        $candWords = $c -split '[-_\s]' | Where-Object { $_.Length -gt 1 }
        foreach ($w in $searchWords) {
            if ($candWords -contains $w) { $score += 20 }
            elseif ($candWords | Where-Object { $_ -like "*$w*" }) { $score += 10 }
        }
        # Reverse: candidate words found in search term (catches partial mismatches)
        foreach ($w in $candWords) {
            if ($searchWords -contains $w) { } # already counted above
            elseif ($searchWords | Where-Object { $_ -like "*$w*" }) { $score += 5 }
        }
        # Prefix overlap (strong signal — "rg-med-device-" matches 14 chars)
        $prefixLen = 0
        for ($i = 0; $i -lt [math]::Min($SearchTerm.Length, $c.Length); $i++) {
            if ($SearchTerm[$i] -eq $c[$i]) { $prefixLen++ } else { break }
        }
        $score += $prefixLen * 2
        # Suffix overlap (catches matching date suffixes like "0331")
        $suffixLen = 0
        for ($i = 1; $i -le [math]::Min($SearchTerm.Length, $c.Length); $i++) {
            if ($SearchTerm[$SearchTerm.Length - $i] -eq $c[$c.Length - $i]) { $suffixLen++ } else { break }
        }
        if ($suffixLen -ge 3) { $score += $suffixLen * 3 }
        [PSCustomObject]@{ Name = $c; Score = $score }
    }
    return ($scored | Where-Object { $_.Score -gt 0 } | Sort-Object Score -Descending | Select-Object -First $MaxResults).Name
}

# ============================================================================
# VALIDATE: Fabric Workspace + Azure Resource Group exist before teardown
# ============================================================================

$wsFound = $false
$rgFound = $false
$wsNotFound = $false
$rgNotFound = $false

# --- Validate Fabric workspace ---
if (-not $SkipFabric) {
    try {
        $fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
        if ($fabricToken -is [System.Security.SecureString]) {
            $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($fabricToken)
            $fabricToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        }
        $fabricHeaders = @{ Authorization = "Bearer $fabricToken" }
        $allWorkspaces = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $fabricHeaders).value
        $matchedWs = $allWorkspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName }

        if ($matchedWs) {
            $wsFound = $true
        } else {
            Write-Host ""
            Write-Host "  ✗ Fabric workspace '$FabricWorkspaceName' not found." -ForegroundColor Red
            $wsNames = $allWorkspaces | ForEach-Object { $_.displayName }
            $suggestions = Get-FuzzySuggestion -SearchTerm $FabricWorkspaceName -Candidates $wsNames
            if ($suggestions) {
                Write-Host "    Did you mean:" -ForegroundColor Yellow
                foreach ($s in $suggestions) {
                    Write-Host "      → $s" -ForegroundColor Cyan
                }
            } else {
                Write-Host "    No similar workspaces found in your tenant." -ForegroundColor DarkGray
            }
            Write-Host ""
            $wsNotFound = $true
            $SkipFabric = $true
        }
    } catch {
        Write-Host "  ⚠ Could not validate Fabric workspace: $($_.Exception.Message)" -ForegroundColor Yellow
    }
} else {
    $wsFound = $true  # User chose to skip — treat as resolved
}

# --- Validate Azure resource group ---
if (-not $SkipAzure) {
    $rgExists = az group exists --name $ResourceGroupName 2>$null
    if ($rgExists -eq "true") {
        $rgFound = $true
    } else {
        Write-Host "  ✗ Resource group '$ResourceGroupName' not found in Azure." -ForegroundColor Red
        $allRgs = az group list --query "[].name" -o tsv 2>$null
        if ($allRgs) {
            $rgList = $allRgs -split "`n" | Where-Object { $_ }
            $suggestions = Get-FuzzySuggestion -SearchTerm $ResourceGroupName -Candidates $rgList
            if ($suggestions) {
                Write-Host "    Did you mean:" -ForegroundColor Yellow
                foreach ($s in $suggestions) {
                    Write-Host "      → $s" -ForegroundColor Cyan
                }
            } else {
                Write-Host "    No similar resource groups found." -ForegroundColor DarkGray
            }
        }
        Write-Host ""
        $rgNotFound = $true
        $SkipAzure = $true
    }
} else {
    $rgFound = $true  # User chose to skip — treat as resolved
}

# --- Handle partial matches ---
if ($wsNotFound -and $rgNotFound) {
    Write-Host "  Teardown aborted — neither workspace nor resource group found. Fix the names above and retry." -ForegroundColor Red
    Write-Host ""
    exit 1
}

if ($wsNotFound -or $rgNotFound) {
    # One was found, the other wasn't — confirm partial teardown
    $foundItems = @()
    $missingItems = @()
    if ($wsFound)    { $foundItems += "Fabric workspace '$FabricWorkspaceName'" }
    if ($rgFound)    { $foundItems += "Resource group '$ResourceGroupName'" }
    if ($wsNotFound) { $missingItems += "Fabric workspace '$FabricWorkspaceName'" }
    if ($rgNotFound) { $missingItems += "Resource group '$ResourceGroupName'" }

    Write-Host "  Found:   $($foundItems -join ', ')" -ForegroundColor Green
    Write-Host "  Missing: $($missingItems -join ', ') (will be skipped)" -ForegroundColor Yellow
    Write-Host ""

    if (-not $Force) {
        $confirm = Read-Host "  Proceed with partial teardown of found resources? (yes/no)"
        if ($confirm -ne "yes") {
            Write-Host "  Aborted." -ForegroundColor Red
            exit 0
        }
        Write-Host ""
    }
}

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

# Clean up state file (workspace-specific)
$wsStateFile = Join-Path $ScriptDir ".deployment-state-$FabricWorkspaceName.json"
if (Test-Path $wsStateFile) {
    Remove-Item $wsStateFile -Force
    Write-Host "  Deployment state file removed (.deployment-state-$FabricWorkspaceName.json)" -ForegroundColor DarkGray
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
