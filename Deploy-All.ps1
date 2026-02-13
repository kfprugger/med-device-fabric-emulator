# Deploy-All.ps1
# End-to-end orchestrator for the Masimo Medical Device + Fabric RTI pipeline.
#
# Sequence:
#   Step 1 — Base Azure infrastructure (Event Hub, ACR, emulator container)
#   Step 2 — FHIR Service + Synthea patient generation + FHIR data load
#   Step 3 — Fabric RTI Phase 1 (workspace, Eventhouse, KQL, FHIR $export)
#   Step 4 — Guidance for Healthcare Data Solutions (manual Fabric portal step)
#   Step 5 — Fabric RTI Phase 2 [optional, after HDS deployed]:
#              a. Bronze LH shortcut → FHIR export ADLS Gen2 storage
#              b. Trigger HDS clinical pipeline (NDJSON → Bronze → Silver)
#              c. KQL shortcuts to Silver Lakehouse
#              d. Enriched fn_ClinicalAlerts function
#
# Usage:
#   .\Deploy-All.ps1                                                  # Full pipeline up to Phase 1
#   .\Deploy-All.ps1 -ResourceGroupName "my-rg" -PatientCount 100     # Custom RG and patient count
#   .\Deploy-All.ps1 -SkipBaseInfra                                   # Skip emulator infra (already deployed)
#   .\Deploy-All.ps1 -SkipFhir                                        # Skip FHIR + Synthea (already loaded)
#   .\Deploy-All.ps1 -Phase2Only -SilverLakehouseId "<id>"             # Run only Fabric Phase 2
#   .\Deploy-All.ps1 -RebuildContainers                                # Force ACR image rebuilds

param (
    # ── Azure ──
    [string]$ResourceGroupName = "rg-medtech-rti-fhir",
    [string]$Location = "eastus",
    [string]$AdminSecurityGroup = "sg-azure-admins",

    # ── FHIR / Synthea ──
    [int]$PatientCount = 500,

    # ── Fabric ──
    [string]$FabricWorkspaceName = "med-device-rti-hds",

    # ── Fabric Phase 2 (post-HDS) ──
    [string]$SilverLakehouseId = "",
    [string]$SilverLakehouseName = "",

    # ── Step control ──
    [switch]$SkipBaseInfra,          # Skip deploy.ps1 (emulator infra already exists)
    [switch]$SkipFhir,               # Skip deploy-fhir.ps1 (FHIR data already loaded)
    [switch]$SkipFabric,             # Skip deploy-fabric-rti.ps1 entirely
    [switch]$Phase2Only,             # Run only Fabric Phase 2
    [switch]$RebuildContainers,      # Force container image rebuilds
    [switch]$SkipFhirExport,         # Skip FHIR $export step in Fabric Phase 1

    # ── Cleanup ──
    [switch]$Teardown                # Run cleanup scripts instead of deployment
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $ScriptDir

# ============================================================================
# HELPERS
# ============================================================================

$stepNumber = 0
$stepResults = @()
$overallTimer = [System.Diagnostics.Stopwatch]::StartNew()

function Write-Banner {
    param([string]$Text, [ConsoleColor]$Color = 'Cyan')
    $width = 60
    $border = '=' * $width
    $pad = $width - $Text.Length
    $padLeft = [math]::Floor($pad / 2)
    $padRight = $pad - $padLeft
    $line = ' ' * $padLeft + $Text + ' ' * $padRight
    Write-Host ""
    Write-Host "+$border+" -ForegroundColor $Color
    Write-Host "|$line|" -ForegroundColor $Color
    Write-Host "+$border+" -ForegroundColor $Color
}

function Write-StepHeader {
    param([string]$Title, [string]$Description = "")
    $script:stepNumber++
    Write-Banner -Text "STEP $($script:stepNumber): $($Title.ToUpper())" -Color Cyan
    if ($Description) {
        Write-Host "  $Description" -ForegroundColor DarkGray
    }
    Write-Host ""
}

function Write-StepResult {
    param([string]$StepName, [bool]$Success, [string]$Duration, [string]$Detail = "")
    $icon = if ($Success) { "✓" } else { "✗" }
    $color = if ($Success) { "Green" } else { "Red" }
    $script:stepResults += @{
        Name     = $StepName
        Success  = $Success
        Duration = $Duration
        Detail   = $Detail
    }
    Write-Host ""
    Write-Host "  $icon  $StepName — $Duration" -ForegroundColor $color
    if ($Detail) { Write-Host "     $Detail" -ForegroundColor DarkGray }
    Write-Host ""
}

function Invoke-Step {
    param(
        [string]$StepName,
        [string]$Description,
        [scriptblock]$Action
    )
    Write-StepHeader -Title $StepName -Description $Description
    $timer = [System.Diagnostics.Stopwatch]::StartNew()

    try {
        & $Action
        $timer.Stop()
        Write-StepResult -StepName $StepName -Success $true `
            -Duration "$([math]::Round($timer.Elapsed.TotalMinutes, 1)) min"
    }
    catch {
        $timer.Stop()
        Write-StepResult -StepName $StepName -Success $false `
            -Duration "$([math]::Round($timer.Elapsed.TotalMinutes, 1)) min" `
            -Detail $_.Exception.Message
        Write-Host "ERROR: Step failed. Stopping pipeline." -ForegroundColor Red
        Write-Summary
        Pop-Location
        exit 1
    }
}

function Write-Summary {
    $overallTimer.Stop()
    $totalMin = [math]::Round($overallTimer.Elapsed.TotalMinutes, 1)

    Write-Banner -Text "DEPLOYMENT SUMMARY" -Color Magenta
    Write-Host ""

    foreach ($r in $script:stepResults) {
        $icon = if ($r.Success) { "✓" } else { "✗" }
        $color = if ($r.Success) { "Green" } else { "Red" }
        Write-Host "  $icon  $($r.Name.PadRight(40)) $($r.Duration)" -ForegroundColor $color
        if ($r.Detail) { Write-Host "       $($r.Detail)" -ForegroundColor DarkGray }
    }

    Write-Host ""
    $allPassed = ($script:stepResults | Where-Object { -not $_.Success }).Count -eq 0
    if ($allPassed) {
        Write-Host "  All steps completed successfully." -ForegroundColor Green
    } else {
        Write-Host "  Some steps failed. See above for details." -ForegroundColor Red
    }
    Write-Host "  Total time: $totalMin min" -ForegroundColor Cyan
    Write-Host ""
}

# ============================================================================
# BANNER
# ============================================================================

Write-Banner -Text "MASIMO CLINICAL ALERT SYSTEM - FULL DEPLOYMENT" -Color Yellow
Write-Host ""
Write-Host "  Resource Group      : $ResourceGroupName" -ForegroundColor White
Write-Host "  Location            : $Location" -ForegroundColor White
Write-Host "  Patient Count       : $PatientCount" -ForegroundColor White
Write-Host "  Fabric Workspace    : $FabricWorkspaceName" -ForegroundColor White
Write-Host "  Admin Group         : $AdminSecurityGroup" -ForegroundColor White
Write-Host ""

if ($Teardown) {
    Write-Host "  MODE: TEARDOWN (destroying all resources)" -ForegroundColor Red
} elseif ($Phase2Only) {
    Write-Host "  MODE: Fabric Phase 2 only" -ForegroundColor Yellow
} else {
    $skips = @()
    if ($SkipBaseInfra) { $skips += "Base Infra" }
    if ($SkipFhir) { $skips += "FHIR/Synthea" }
    if ($SkipFabric) { $skips += "Fabric" }
    if ($skips.Count -gt 0) {
        Write-Host "  SKIPPING: $($skips -join ', ')" -ForegroundColor Yellow
    } else {
        Write-Host "  MODE: Full deployment" -ForegroundColor Green
    }
    if ($RebuildContainers) {
        Write-Host "  REBUILD: Container images will be force-rebuilt" -ForegroundColor Yellow
    }
}
Write-Host ""

# ============================================================================
# TEARDOWN MODE
# ============================================================================

if ($Teardown) {
    Invoke-Step -StepName "Delete Fabric Workspace" -Description "Removing $FabricWorkspaceName" -Action {
        & "$ScriptDir\cleanup\Remove-FabricWorkspace.ps1" `
            -FabricWorkspaceName $FabricWorkspaceName -Force
    }

    Invoke-Step -StepName "Delete Azure Infrastructure" -Description "Removing resource group $ResourceGroupName" -Action {
        & "$ScriptDir\cleanup\Remove-AzureInfra.ps1" `
            -ResourceGroupName $ResourceGroupName -Force -Wait
    }

    Write-Summary
    Pop-Location
    exit 0
}

# ============================================================================
# PHASE 2 ONLY MODE
# ============================================================================

if ($Phase2Only) {
    Invoke-Step -StepName "Fabric RTI Phase 2" `
        -Description "Bronze shortcut, clinical pipeline, KQL shortcuts, enriched alerts" -Action {
        $phase2Args = @{
            Phase2              = $true
            FabricWorkspaceName = $FabricWorkspaceName
            ResourceGroupName   = $ResourceGroupName
            Location            = $Location
        }
        if ($SilverLakehouseId) { $phase2Args['SilverLakehouseId'] = $SilverLakehouseId }
        if ($SilverLakehouseName) { $phase2Args['SilverLakehouseName'] = $SilverLakehouseName }

        & "$ScriptDir\deploy-fabric-rti.ps1" @phase2Args
    }

    Write-Summary
    Pop-Location
    exit 0
}

# ============================================================================
# STEP 1 — BASE AZURE INFRASTRUCTURE
# ============================================================================

if (-not $SkipBaseInfra) {
    # Check if base infra already exists
    Write-Host "  Checking for existing base infrastructure..." -ForegroundColor DarkGray
    $baseInfraExists = $false
    $baseDeployment = az deployment group show `
        --resource-group $ResourceGroupName `
        --name infra `
        --query properties.outputs 2>$null

    if ($LASTEXITCODE -eq 0 -and $baseDeployment) {
        Write-Host "  Found deployment record, verifying resources..." -ForegroundColor DarkGray
        $baseJson = $baseDeployment | ConvertFrom-Json
        $existingAcr = $baseJson.acrName.value
        $existingEhNs = $baseJson.eventHubNamespace.value

        if ($existingAcr -and $existingEhNs) {
            Write-Host "  Verifying ACR '$existingAcr' is healthy..." -ForegroundColor DarkGray
            $acrCheck = az acr show --name $existingAcr --query "provisioningState" -o tsv 2>$null
            if ($acrCheck -eq "Succeeded") {
                $baseInfraExists = $true
            }
        }
    } else {
        Write-Host "  No existing deployment found in '$ResourceGroupName'" -ForegroundColor DarkGray
    }

    if ($baseInfraExists) {
        $script:stepNumber++
        Write-Host ""
        Write-Host "  Base Azure infrastructure already exists -- skipping" -ForegroundColor Green
        Write-Host "    ACR             : $existingAcr" -ForegroundColor DarkGray
        Write-Host "    Event Hub NS    : $existingEhNs" -ForegroundColor DarkGray
        Write-Host ""
        $script:stepResults += @{
            Name     = "Base Azure Infrastructure"
            Success  = $true
            Duration = "skipped"
            Detail   = "Already deployed (ACR: $existingAcr)"
        }
    } else {
        Invoke-Step -StepName "Base Azure Infrastructure" `
            -Description "Event Hub, ACR, emulator container (deploy.ps1)" -Action {
            Write-Host "  [1/4] Creating resource group '$ResourceGroupName'..." -ForegroundColor White
            Write-Host "  [2/4] Deploying Event Hub, ACR, Key Vault (bicep/infra.bicep)..." -ForegroundColor White
            Write-Host "  [3/4] Building emulator container image in ACR..." -ForegroundColor White
            Write-Host "  [4/4] Deploying emulator ACI container (bicep/emulator.bicep)..." -ForegroundColor White
            Write-Host ""
            & "$ScriptDir\deploy.ps1" `
                -ResourceGroupName $ResourceGroupName `
                -Location $Location `
                -AdminSecurityGroup $AdminSecurityGroup
        }
    }
} else {
    Write-Host "  >>  Skipping base infrastructure (--SkipBaseInfra)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2 — FHIR SERVICE + SYNTHEA + LOADER
# ============================================================================

if (-not $SkipFhir) {
    Invoke-Step -StepName "FHIR Service + Synthea + Loader" `
        -Description "$PatientCount patients -> FHIR (deploy-fhir.ps1)" -Action {
        Write-Host "  This step will:" -ForegroundColor White
        Write-Host "    [1/5] Deploy FHIR infrastructure (HDS workspace, FHIR R4, storage, UAMI)" -ForegroundColor DarkGray
        Write-Host "    [2/5] Build Synthea + Loader container images in ACR" -ForegroundColor DarkGray
        Write-Host "    [3/5] Run Synthea to generate $PatientCount synthetic patients" -ForegroundColor DarkGray
        Write-Host "    [4/5] Upload FHIR bundles, providers, and devices" -ForegroundColor DarkGray
        Write-Host "    [5/5] Create device associations for qualifying patients" -ForegroundColor DarkGray
        if ($RebuildContainers) {
            Write-Host "    (Container images will be force-rebuilt)" -ForegroundColor Yellow
        }
        Write-Host ""

        $fhirArgs = @{
            ResourceGroupName  = $ResourceGroupName
            Location           = $Location
            AdminSecurityGroup = $AdminSecurityGroup
            PatientCount       = $PatientCount
        }
        if ($RebuildContainers) { $fhirArgs['RebuildContainers'] = $true }

        & "$ScriptDir\deploy-fhir.ps1" @fhirArgs
    }
} else {
    Write-Host "  >>  Skipping FHIR / Synthea (--SkipFhir)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 3 — FABRIC RTI PHASE 1
# ============================================================================

if (-not $SkipFabric) {
    Invoke-Step -StepName "Fabric RTI Phase 1" `
        -Description "Workspace, Eventhouse, KQL DB, Eventstream, FHIR export" -Action {
        Write-Host "  This step will:" -ForegroundColor White
        Write-Host "    [1/6] Create Fabric workspace '$FabricWorkspaceName'" -ForegroundColor DarkGray
        Write-Host "    [2/6] Create Eventhouse + KQL Database" -ForegroundColor DarkGray
        Write-Host "    [3/6] Deploy KQL tables and functions" -ForegroundColor DarkGray
        Write-Host "    [4/6] Create Event Hub cloud connection" -ForegroundColor DarkGray
        Write-Host "    [5/6] Create Eventstream (telemetry ingest)" -ForegroundColor DarkGray
        if (-not $SkipFhirExport) {
            Write-Host "    [6/6] Run FHIR `$export to ADLS Gen2" -ForegroundColor DarkGray
        } else {
            Write-Host "    [6/6] FHIR `$export (skipped)" -ForegroundColor Yellow
        }
        Write-Host ""

        $fabricArgs = @{
            FabricWorkspaceName = $FabricWorkspaceName
            ResourceGroupName   = $ResourceGroupName
            Location            = $Location
        }
        if ($SkipFhirExport) { $fabricArgs['SkipFhirExport'] = $true }

        & "$ScriptDir\deploy-fabric-rti.ps1" @fabricArgs
    }
} else {
    Write-Host "  >>  Skipping Fabric RTI (--SkipFabric)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 4 — HDS GUIDANCE (informational only)
# ============================================================================

if (-not $SkipFabric) {
    $script:stepNumber++
    Write-Banner -Text "STEP $($script:stepNumber): HEALTHCARE DATA SOLUTIONS (MANUAL)" -Color Yellow
    Write-Host ""
    Write-Host "  All automated steps are complete. The remaining setup requires" -ForegroundColor White
    Write-Host "  manual configuration in the Microsoft Fabric portal." -ForegroundColor White
    Write-Host ""
    Write-Host "  What to do next:" -ForegroundColor White
    Write-Host "    [1] Open https://app.fabric.microsoft.com" -ForegroundColor DarkGray
    Write-Host "    [2] Navigate to workspace '$FabricWorkspaceName'" -ForegroundColor DarkGray
    Write-Host "    [3] Deploy Healthcare Data Solutions (HDS) from the Data Hub" -ForegroundColor DarkGray
    Write-Host "        https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/deploy" -ForegroundColor DarkCyan
    Write-Host "    [4] Configure the HDS clinical data pipeline" -ForegroundColor DarkGray
    Write-Host "        https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/healthcare-data-foundations" -ForegroundColor DarkCyan
    Write-Host "    [5] Wait for the Silver Lakehouse to populate (5-15 min)" -ForegroundColor DarkGray
    Write-Host "        https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/deploy#deploy-healthcare-data-foundations" -ForegroundColor DarkCyan
    Write-Host ""
    Write-Host "  Once the Silver Lakehouse has data, run Phase 2:" -ForegroundColor White
    Write-Host "    .\Deploy-All.ps1 -Phase2Only" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Or specify the Silver Lakehouse ID explicitly:" -ForegroundColor White
    Write-Host "    .\Deploy-All.ps1 -Phase2Only -SilverLakehouseId `"<id>`"" -ForegroundColor Cyan
    Write-Host ""

    $script:stepResults += @{
        Name     = "HDS Guidance"
        Success  = $true
        Duration = "—"
        Detail   = "Manual step: deploy HDS, then run Phase 2"
    }
}

# ============================================================================
# STEP 5 — DATA AGENTS (after Phase 2)
# ============================================================================

# Deploy Data Agents if running Phase 2 or if the Silver Lakehouse is available
if ($Phase2Only) {
    Invoke-Step -StepName "Data Agents" `
        -Description "Deploy Patient 360 + Clinical Triage agents" -Action {
        Write-Host "  This step will:" -ForegroundColor White
        Write-Host "    [1/2] Create/update Patient 360 Data Agent" -ForegroundColor DarkGray
        Write-Host "    [2/2] Create/update Clinical Triage Data Agent" -ForegroundColor DarkGray
        Write-Host "  Architecture: KQL (TelemetryRaw + AlertHistory) + Lakehouse (Silver tables)" -ForegroundColor DarkGray
        Write-Host ""

        & "$ScriptDir\deploy-data-agents.ps1" `
            -FabricWorkspaceName $FabricWorkspaceName
    }
}

# ============================================================================
# SUMMARY
# ============================================================================

Write-Summary
Pop-Location
