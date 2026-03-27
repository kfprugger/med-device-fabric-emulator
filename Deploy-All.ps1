# Deploy-All.ps1
# End-to-end orchestrator for the Masimo Medical Device + Fabric RTI pipeline.
#
# Sequence:
#   Step 1  — Base Azure infrastructure (Event Hub, ACR, emulator container)
#   Step 1b — Fabric workspace (created early so HDS can be deployed in parallel)
#   Step 2  — FHIR Service + Synthea patient generation + FHIR data load
#   Step 2b — DICOM infrastructure + TCIA download, re-tag, upload
#   Step 3  — Fabric RTI Phase 1 (Eventhouse, KQL, Eventstream, FHIR $export)
#   Step 4  — Guidance for Healthcare Data Solutions (manual Fabric portal step)
#   Step 5  — Fabric RTI Phase 2 [optional, after HDS deployed]:
#              a. Bronze LH shortcut → FHIR export ADLS Gen2 storage
#              b. KQL shortcuts to Silver Lakehouse
#              c. Enriched fn_ClinicalAlerts function
#   Step 5b — DICOM shortcut + HDS imaging (incl. clinical) and OMOP pipelines
#   Step 6  — Data Agents (Patient 360 + Clinical Triage)
#   Step 7  — Phase 3: Cohorting Agent + DICOM Viewer + Imaging Report
#              (requires Gold OMOP pipeline to have completed)
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
    [Parameter(Mandatory)][string]$Location,
    [string]$AdminSecurityGroup = "sg-msft-hds-dicom-project",

    # ── FHIR / Synthea ──
    [int]$PatientCount = 100,

    # ── Fabric ──
    [Parameter(Mandatory)][string]$FabricWorkspaceName,

    # ── Fabric Phase 2 (post-HDS) ──
    [string]$SilverLakehouseId = "",
    [string]$SilverLakehouseName = "",

    # ── Step control ──
    [switch]$SkipBaseInfra,          # Skip deploy.ps1 (emulator infra already exists)
    [switch]$SkipFhir,               # Skip deploy-fhir.ps1 (FHIR data already loaded)
    [switch]$SkipDicom,              # Skip DICOM infra + loader
    [switch]$SkipFabric,             # Skip deploy-fabric-rti.ps1 entirely
    [switch]$Phase2Only,             # Run only Fabric Phase 2
    [switch]$Phase3Only,             # Run only Phase 3 (Cohorting Agent + DICOM Viewer)
    [switch]$RebuildContainers,      # Force container image rebuilds
    [hashtable]$Tags = @{},            # Resource tags (e.g. @{SecurityControl='Ignore'})
    [switch]$SkipFhirExport,         # Skip FHIR $export step in Fabric Phase 1

    # ── Phase 3 (FabricDicomCohortingToolkit) ──
    [string]$DicomToolkitPath = "C:\git\FabricDicomCohortingToolkit",
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer",

    # ── Cleanup ──
    [switch]$Teardown                # Run cleanup scripts instead of deployment
)

$ErrorActionPreference = "Stop"

# Validate conditionally-required parameters
if (-not $Teardown -and -not $Phase2Only -and -not $Phase3Only -and -not $AdminSecurityGroup) {
    throw "Parameter '-AdminSecurityGroup' is required for deployment. Only -Teardown, -Phase2Only, and -Phase3Only can omit it."
}

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
    param([string]$Title = "DEPLOYMENT SUMMARY")
    $overallTimer.Stop()
    $totalMin = [math]::Round($overallTimer.Elapsed.TotalMinutes, 1)

    Write-Banner -Text $Title -Color Magenta
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

function Assert-StorageAccountName {
    param([string]$Name, [string]$Context = "Storage account")
    if ($Name -cne $Name.ToLower()) {
        throw "$Context name '$Name' must be lowercase. Got uppercase characters."
    }
    if ($Name -notmatch '^[a-z0-9]+$') {
        throw "$Context name '$Name' must be alphanumeric only (a-z, 0-9). No hyphens, underscores, or special characters."
    }
    if ($Name.Length -gt 24) {
        throw "$Context name '$Name' exceeds 24 characters (got $($Name.Length)). Azure storage accounts must be 3-24 characters."
    }
    if ($Name.Length -lt 3) {
        throw "$Context name '$Name' is too short (got $($Name.Length)). Azure storage accounts must be 3-24 characters."
    }
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
} elseif ($Phase3Only) {
    Write-Host "  MODE: Phase 3 only (Cohorting Agent + DICOM Viewer)" -ForegroundColor Magenta
} else {
    $skips = @()
    if ($SkipBaseInfra) { $skips += "Base Infra" }
    if ($SkipFhir) { $skips += "FHIR/Synthea" }
    if ($SkipDicom) { $skips += "DICOM" }
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
        if ($Tags.Count -gt 0) { $phase2Args['Tags'] = $Tags }

        & "$ScriptDir\deploy-fabric-rti.ps1" @phase2Args
    }

    # DICOM shortcut + HDS pipelines (clinical, imaging, OMOP)
    if (-not $SkipDicom) {
        Invoke-Step -StepName "DICOM Shortcut + HDS Pipelines" `
            -Description "Shortcut for DICOM data, then run clinical, imaging, and OMOP pipelines" -Action {
            & "$ScriptDir\storage-access-trusted-workspace.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName `
                -ResourceGroupName $ResourceGroupName
        }
    }


    Write-Summary -Title "PHASE 2 DEPLOYMENT SUMMARY"
    Pop-Location

    # If not Phase3Only, exit here
    if (-not $Phase3Only) { exit 0 }
}

# ============================================================================
# STEP 1 — BASE AZURE INFRASTRUCTURE
# ============================================================================

if (-not $Phase3Only -and -not $SkipBaseInfra) {
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
        Write-Host "  Base Azure infrastructure already exists -- skipping deployment" -ForegroundColor Green
        Write-Host "    ACR             : $existingAcr" -ForegroundColor DarkGray
        Write-Host "    Event Hub NS    : $existingEhNs" -ForegroundColor DarkGray

        # Verify emulator ACI exists and is running
        Write-Host "  Verifying emulator container..." -ForegroundColor DarkGray
        $emulatorContainers = az container list -g $ResourceGroupName `
            --query "[?contains(name,'emulator')].{name:name, state:provisioningState, principalId:identity.principalId}" `
            -o json 2>$null | ConvertFrom-Json
        if ($emulatorContainers -and $emulatorContainers.Count -gt 0) {
            $emulatorAci = $emulatorContainers[0]
            Write-Host "    Emulator ACI    : $($emulatorAci.name) ($($emulatorAci.state))" -ForegroundColor DarkGray

            # Verify RBAC: emulator MI must have Event Hubs Data Sender
            if ($emulatorAci.principalId) {
                $ehNsId = az eventhubs namespace show -g $ResourceGroupName -n $existingEhNs --query id -o tsv 2>$null
                $senderRole = az role assignment list --assignee $emulatorAci.principalId `
                    --scope $ehNsId --role "Azure Event Hubs Data Sender" `
                    --query "[0].id" -o tsv 2>$null
                if (-not $senderRole) {
                    Write-Host "    ⚠ Emulator MI missing 'Event Hubs Data Sender' RBAC — assigning..." -ForegroundColor Yellow
                    az role assignment create --assignee-object-id $emulatorAci.principalId `
                        --assignee-principal-type ServicePrincipal `
                        --role "Azure Event Hubs Data Sender" `
                        --scope $ehNsId -o none 2>$null
                    Write-Host "    ✓ RBAC assigned. Restarting emulator..." -ForegroundColor Green
                    Start-Sleep -Seconds 30
                    az container restart -g $ResourceGroupName -n $emulatorAci.name 2>$null
                    Write-Host "    ✓ Emulator restarted" -ForegroundColor Green
                } else {
                    Write-Host "    ✓ Emulator RBAC verified (Event Hubs Data Sender)" -ForegroundColor DarkGray
                }
            }
        } else {
            Write-Host "    ⚠ Emulator ACI not found — running deploy.ps1 to create it..." -ForegroundColor Yellow
            & "$ScriptDir\deploy.ps1" `
                -ResourceGroupName $ResourceGroupName `
                -Location $Location `
                -AdminSecurityGroup $AdminSecurityGroup `
                -Tags $Tags
        }

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
                -AdminSecurityGroup $AdminSecurityGroup `
                -Tags $Tags
        }
    }
} else {
    Write-Host "  >>  Skipping base infrastructure (--SkipBaseInfra)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 1b — FABRIC WORKSPACE (created early so HDS can be deployed during FHIR/DICOM steps)
# ============================================================================

if (-not $Phase3Only -and -not $SkipFabric) {
    Invoke-Step -StepName "Fabric Workspace" `
        -Description "Create workspace '$FabricWorkspaceName' + assign capacity + provision identity" -Action {

        function Get-FabricToken {
            $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
            if ($t -is [System.Security.SecureString]) {
                $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
                try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
                finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
            }
            return $t
        }

        $fabToken = Get-FabricToken
        $fabHeaders = @{ "Authorization" = "Bearer $fabToken"; "Content-Type" = "application/json" }
        $fabBase = "https://api.fabric.microsoft.com/v1"

        # Check if workspace exists
        $wsResp = Invoke-RestMethod -Uri "$fabBase/workspaces" -Headers $fabHeaders
        $existingWs = $wsResp.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }

        if ($existingWs) {
            $script:fabricWorkspaceId = $existingWs.id
            Write-Host "  ✓ Workspace already exists: $FabricWorkspaceName ($($script:fabricWorkspaceId))" -ForegroundColor Green
        } else {
            Write-Host "  Creating workspace '$FabricWorkspaceName'..." -ForegroundColor White
            $newWs = Invoke-RestMethod -Uri "$fabBase/workspaces" -Headers $fabHeaders -Method POST `
                -Body (@{
                    displayName = $FabricWorkspaceName
                    description = "Masimo Clinical Alert System — Real-Time Intelligence workspace for medical device telemetry monitoring and clinical alerting."
                } | ConvertTo-Json)
            $script:fabricWorkspaceId = $newWs.id
            Write-Host "  ✓ Workspace created: $FabricWorkspaceName ($($script:fabricWorkspaceId))" -ForegroundColor Green
        }

        # Ensure capacity is assigned
        $wsDetail = Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)" -Headers $fabHeaders
        if (-not $wsDetail.capacityId) {
            Write-Host "  Searching for an active Fabric capacity..." -ForegroundColor Yellow
            $caps = Invoke-RestMethod -Uri "$fabBase/capacities" -Headers $fabHeaders
            $activeCap = $caps.value | Where-Object {
                $_.state -eq "Active" -and $_.sku -ne "PP3"
            } | Sort-Object -Property @{Expression={if ($_.sku -like "F*" -and $_.sku -ne "FT1") { 0 } elseif ($_.sku -eq "FT1") { 1 } else { 2 }}} | Select-Object -First 1

            if ($activeCap) {
                Write-Host "  Assigning capacity: $($activeCap.displayName) (SKU: $($activeCap.sku))..." -ForegroundColor White
                Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)/assignToCapacity" `
                    -Headers $fabHeaders -Method POST `
                    -Body (@{ capacityId = $activeCap.id } | ConvertTo-Json) | Out-Null
                Start-Sleep -Seconds 5
                Write-Host "  ✓ Capacity assigned" -ForegroundColor Green
            } else {
                throw "No active Fabric capacity found. Start a trial at https://app.fabric.microsoft.com"
            }
        } else {
            Write-Host "  ✓ Capacity already assigned" -ForegroundColor Green
        }

        # Provision workspace managed identity
        Write-Host "  Provisioning workspace managed identity..." -ForegroundColor White
        try {
            Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)/provisionIdentity" `
                -Headers $fabHeaders -Method POST | Out-Null
            Write-Host "  ✓ Workspace identity provisioned" -ForegroundColor Green
        } catch {
            if ($_.Exception.Message -match "already|exists") {
                Write-Host "  ✓ Workspace identity already exists" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ Could not provision workspace identity: $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }

        Write-Host ""
        Write-Host "  Workspace is ready. You can now deploy HDS in the Fabric portal" -ForegroundColor DarkGray
        Write-Host "  while the remaining Azure steps (FHIR, DICOM) continue below." -ForegroundColor DarkGray
    }
}

# ============================================================================
# STEP 2 — FHIR SERVICE + SYNTHEA + LOADER
# ============================================================================

if (-not $Phase3Only -and -not $SkipFhir) {
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
            SkipDicom          = $true
        }
        if ($RebuildContainers) { $fhirArgs['RebuildContainers'] = $true }
        if ($Tags.Count -gt 0) { $fhirArgs['Tags'] = $Tags }

        & "$ScriptDir\deploy-fhir.ps1" @fhirArgs
    }
} else {
    Write-Host "  >>  Skipping FHIR / Synthea (--SkipFhir)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2b — DICOM SERVICE + TCIA LOADER
# ============================================================================

if (-not $Phase3Only -and -not $SkipDicom -and -not $SkipFhir) {
    Invoke-Step -StepName "DICOM Service + Loader" `
        -Description "DICOM infra, TCIA download, re-tag, upload (deploy-fhir.ps1 -RunDicom)" -Action {
        Write-Host "  This step will:" -ForegroundColor White
        Write-Host "    [1/3] Build DICOM Loader container image in ACR" -ForegroundColor DarkGray
        Write-Host "    [2/3] Deploy DICOM service into HDS workspace" -ForegroundColor DarkGray
        Write-Host "    [3/3] Run DICOM Loader (TCIA download, re-tag, STOW-RS upload)" -ForegroundColor DarkGray
        Write-Host ""

        $dicomArgs = @{
            ResourceGroupName  = $ResourceGroupName
            Location           = $Location
            AdminSecurityGroup = $AdminSecurityGroup
            RunDicom           = $true
        }
        if ($RebuildContainers) { $dicomArgs['RebuildContainers'] = $true }
        if ($Tags.Count -gt 0) { $dicomArgs['Tags'] = $Tags }

        & "$ScriptDir\deploy-fhir.ps1" @dicomArgs
    }
} elseif ($SkipDicom) {
    Write-Host "  >>  Skipping DICOM (--SkipDicom)" -ForegroundColor DarkGray
} else {
    Write-Host "  >>  Skipping DICOM (FHIR was skipped)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 3 — FABRIC RTI PHASE 1
# ============================================================================

if (-not $Phase3Only -and -not $SkipFabric) {
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
        if ($Tags.Count -gt 0) { $fabricArgs['Tags'] = $Tags }

        & "$ScriptDir\deploy-fabric-rti.ps1" @fabricArgs
    }
} else {
    Write-Host "  >>  Skipping Fabric RTI (--SkipFabric)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 4 — HDS GUIDANCE (informational only)
# ============================================================================

if (-not $Phase3Only -and -not $SkipFabric) {
    $script:stepNumber++
    Write-Banner -Text "STEP $($script:stepNumber): HEALTHCARE DATA SOLUTIONS (MANUAL)" -Color Yellow
    Write-Host ""
    Write-Host "  All automated steps are complete. The remaining setup requires" -ForegroundColor White
    Write-Host "  manual configuration in the Microsoft Fabric portal." -ForegroundColor White
    Write-Host ""
    Write-Host "  What to do next:" -ForegroundColor White
    Write-Host "    [1] Open https://app.fabric.microsoft.com" -ForegroundColor DarkGray
    Write-Host "    [2] Navigate to workspace '$FabricWorkspaceName'" -ForegroundColor DarkGray
    Write-Host "    [3] Deploy Healthcare Data Solutions (HDS) with Healthcare Data Foundations" -ForegroundColor DarkGray
    Write-Host "        https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/deploy" -ForegroundColor DarkCyan
    Write-Host "    [4] Add the DICOM Data Transformation modality to HDS" -ForegroundColor DarkGray
    Write-Host "        https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/dicom-data-transformation-configure#deploy-dicom-data-transformation" -ForegroundColor DarkCyan
    Write-Host "    [5] Wait for the modalities to finish deploying, then run Phase 2 below" -ForegroundColor DarkGray
    Write-Host ""

    # Build the Phase 2 example command with pre-populated values from Phase 1
    $phase2Cmd = "    .\Deploy-All.ps1 -Phase2Only ``"
    $phase2Cmd += "`n        -Location `"$Location`" ``"
    $phase2Cmd += "`n        -FabricWorkspaceName `"$FabricWorkspaceName`""
    if ($Tags.Count -gt 0) {
        $tagPairs = ($Tags.GetEnumerator() | ForEach-Object { "$($_.Key)='$($_.Value)'" }) -join ';'
        $phase2Cmd += " ``"
        $phase2Cmd += "`n        -Tags @{$tagPairs}"
    }

    Write-Host "  Once the Bronze and Silver Lakehouses are deployed, run Phase 2:" -ForegroundColor White
    Write-Host $phase2Cmd -ForegroundColor Cyan
    Write-Host ""

    $script:stepResults += @{
        Name     = "HDS Guidance"
        Success  = $true
        Duration = "—"
        Detail   = "Manual step: deploy HDS, then run Phase 2"
    }
}

# ============================================================================
# STEP 6 — DATA AGENTS (after Phase 2 + OMOP)
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
# STEP 7 — PHASE 3: COHORTING AGENT + DICOM VIEWER (FabricDicomCohortingToolkit)
# Requires: Gold OMOP pipeline completed, Silver + Gold lakehouses populated
# ============================================================================

if ($Phase2Only -or $Phase3Only) {
    # Phase 3 preflight: verify Gold OMOP lakehouse has data
    $runPhase3 = $true

    if ($Phase3Only -or $Phase2Only) {
        Invoke-Step -StepName "Phase 3: Imaging Toolkit" `
            -Description "Cohorting Agent + DICOM Viewer (FabricDicomCohortingToolkit)" -Action {

            # Validate toolkit path
            if (-not (Test-Path "$DicomToolkitPath\Deploy-DataAgent.ps1")) {
                throw "FabricDicomCohortingToolkit not found at '$DicomToolkitPath'. Clone it: git clone https://github.com/kfprugger/FabricDicomCohortingToolkit '$DicomToolkitPath'"
            }

            Write-Host "  ┌──────────────────────────────────────────────────────────────┐" -ForegroundColor Magenta
            Write-Host "  │  PHASE 3: FabricDicomCohortingToolkit Deployment            │" -ForegroundColor Magenta
            Write-Host "  └──────────────────────────────────────────────────────────────┘" -ForegroundColor Magenta
            Write-Host ""

            # Preflight: Check Gold OMOP lakehouse has data
            Write-Host "  --- PREFLIGHT: Gold OMOP Lakehouse Check ---" -ForegroundColor Cyan
            try {
                function Get-FabricTokenLocal {
                    $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
                    if ($t -is [System.Security.SecureString]) {
                        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
                        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
                        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
                    }
                    return $t
                }
                $p3Token = Get-FabricTokenLocal
                $p3Headers = @{ Authorization = "Bearer $p3Token"; "Content-Type" = "application/json" }
                $p3Base = "https://api.fabric.microsoft.com/v1"

                $p3Ws = (Invoke-RestMethod -Uri "$p3Base/workspaces" -Headers $p3Headers).value |
                    Where-Object { $_.displayName -eq $FabricWorkspaceName }
                $p3WsId = $p3Ws.id

                $p3Items = (Invoke-RestMethod -Uri "$p3Base/workspaces/$p3WsId/items?type=Lakehouse" -Headers $p3Headers).value
                $goldLh = $p3Items | Where-Object { $_.displayName -match 'gold_omop' } | Select-Object -First 1

                if (-not $goldLh) {
                    throw "Gold OMOP Lakehouse not found. Ensure the OMOP pipeline has completed before running Phase 3."
                }
                Write-Host "  ✓ Gold OMOP Lakehouse: $($goldLh.displayName) ($($goldLh.id))" -ForegroundColor Green
            } catch {
                Write-Host "  ✗ Gold OMOP preflight failed: $($_.Exception.Message)" -ForegroundColor Red
                throw "Phase 3 requires the Gold OMOP pipeline to have completed. Run the OMOP pipeline first."
            }

            # Step 3a: Deploy Cohorting Data Agent
            Write-Host ""
            Write-Host "  --- Step 3a: Cohorting Data Agent ---" -ForegroundColor Cyan
            Write-Host "  Deploying HDS Multi-Layer Imaging Cohort Agent..." -ForegroundColor White
            Write-Host "    Source: $DicomToolkitPath\Deploy-DataAgent.ps1" -ForegroundColor DarkGray
            Write-Host ""

            & "$DicomToolkitPath\Deploy-DataAgent.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName

            Write-Host ""

            # Step 3b: Deploy DICOM Viewer (Azure infra + OHIF)
            Write-Host "  --- Step 3b: DICOM Viewer ---" -ForegroundColor Cyan
            Write-Host "  Deploying OHIF Viewer + DICOMweb Proxy to Azure..." -ForegroundColor White
            Write-Host "    Source: $DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1" -ForegroundColor DarkGray
            Write-Host "    Resource Group: $DicomViewerResourceGroup" -ForegroundColor DarkGray
            Write-Host ""

            & "$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1" `
                -ResourceGroup $DicomViewerResourceGroup `
                -FabricWorkspaceName $FabricWorkspaceName `
                -Location $Location

            Write-Host ""

            # Step 3c: Create Reporting Lakehouse + Materialize Notebook
            Write-Host "  --- Step 3c: Reporting Tables ---" -ForegroundColor Cyan
            Write-Host "  Creating reporting lakehouse and running materialization notebook..." -ForegroundColor White

            # Create reporting lakehouse if it doesn't exist
            $p3Token2 = Get-FabricTokenLocal
            $p3H2 = @{ Authorization = "Bearer $p3Token2"; "Content-Type" = "application/json" }
            $existingLh = (Invoke-RestMethod -Uri "$p3Base/workspaces/$p3WsId/lakehouses" -Headers $p3H2).value |
                Where-Object { $_.displayName -eq "healthcare1_reporting_gold" }
            if (-not $existingLh) {
                Write-Host "  Creating healthcare1_reporting_gold lakehouse..." -ForegroundColor White
                $lhBody = '{"displayName":"healthcare1_reporting_gold","type":"Lakehouse"}'
                Invoke-RestMethod -Uri "$p3Base/workspaces/$p3WsId/items" -Headers $p3H2 -Method Post -Body $lhBody | Out-Null
                Write-Host "  ✓ Reporting lakehouse created" -ForegroundColor Green
            } else {
                Write-Host "  ✓ Reporting lakehouse already exists" -ForegroundColor Green
            }

            # Deploy + run notebook (auto-discovers OHIF URL from Azure)
            & "$DicomToolkitPath\deploy-notebook.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName `
                -DicomViewerResourceGroup $DicomViewerResourceGroup
            Write-Host ""

            # Step 3d: Deploy Power BI Direct Lake Report
            Write-Host "  --- Step 3d: Power BI Imaging Report (Direct Lake) ---" -ForegroundColor Cyan
            & "$DicomToolkitPath\Deploy-ImagingReport.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName
            Write-Host ""
        }
    }
}

# ============================================================================
# SUMMARY
# ============================================================================

$summaryTitle = if ($Phase3Only) { "PHASE 3 DEPLOYMENT SUMMARY" } elseif ($Phase2Only) { "PHASE 2+3 DEPLOYMENT SUMMARY" } else { "PHASE 1 DEPLOYMENT SUMMARY" }
Write-Summary -Title $summaryTitle
Pop-Location

