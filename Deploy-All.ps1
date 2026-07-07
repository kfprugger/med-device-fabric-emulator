# Deploy-All.ps1
# End-to-end orchestrator for the Masimo Medical Device + Fabric RTI pipeline.
#
# Sequence:
#   ── Phase 1: Data Fabric Foundation ──
#   Step 1   — Fabric workspace + identity (created first so HDS can be deployed in parallel)
#   Step 1b  — Base Azure infrastructure (Event Hub, ACR, emulator container)
#   Step 2   — FHIR Service + Synthea patient generation + FHIR data load
#   Step 2b  — DICOM loader: TCIA download, re-tag, ADLS upload, ImagingStudy linkage
#   ── Phase 2: Active Patient Telemetry ──
#   Step 3   — Fabric RTI ingest (Eventhouse, KQL, Eventstream, FHIR $export)
#   Step 5   — Fabric RTI enrichment (Bronze shortcut, KQL shortcuts, enriched alerts)
#   ── Phase 3: HDS Bridge + Row Gates ──
#   Step 4   — (Manual) Detect HDS deployment in Fabric portal
#   Step 5b  — DICOM shortcut + HDS pipelines (clinical, imaging, OMOP, optional CMA) + row gates
#   ── Phase 4: Semantic Intelligence & UX ──
#   Step 7   — Cohorting Agent + DICOM Viewer + Imaging Report
#              (requires Gold OMOP pipeline to have completed)
#   Step 8   — Ontology (DeviceAssociation table + ClinicalDeviceOntology + agent binding)
#   Step 8d  — Ontology-aware Data Agents (Patient 360 + Clinical Triage)
#   ── Phase 5: Bedside Alerting & Action ──
#   Step 9   — Data Activator (ClinicalAlertActivator Reflex + email rule)
#   ── Phase 6: Population Health & Quality ──
#   CMS Quality & Claims analytics, Star Ratings, HCC, PDC, readmission, cost/utilization
#   ── Phase 7: Payer RTI & Ops ──
#   Claim stream, payer scoring, activator, operations agents, graph agent
#
# Usage:
#   .\Deploy-All.ps1                                                  # Full pipeline (all phases)
#   .\Deploy-All.ps1 -ResourceGroupName "my-rg" -PatientCount 100     # Custom RG and patient count
#   .\Deploy-All.ps1 -SkipBaseInfra                                   # Skip emulator infra (already deployed)
#   .\Deploy-All.ps1 -SkipFhir                                        # Skip FHIR + Synthea (already loaded)
#   .\Deploy-All.ps1 -Phase2                                          # Run Phase 2 only (after manual HDS)
#   .\Deploy-All.ps1 -Phase3                                          # Run Phase 3 only (imaging toolkit)
#   .\Deploy-All.ps1 -Phase4 -AlertEmail "nurse@hospital.com"          # Run Phase 4 only (ontology + activator)
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
    [string]$CapacitySubscriptionId = "",
    [string]$CapacityResourceGroup = "",
    [string]$CapacityName = "",


    # ── Fabric Phase 2 (post-HDS) ──
    [string]$SilverLakehouseId = "",
    [string]$SilverLakehouseName = "",
    
    # ── Data Copying ──
    [string]$SourceResourceGroup = "",

    # ── Step control ──
    [switch]$SkipBaseInfra,          # Skip deploy.ps1 (emulator infra already exists)
    [switch]$SkipFhir,               # Skip deploy-fhir.ps1 (FHIR data already loaded)
    [switch]$SkipDicom,              # Skip TCIA DICOM download + ADLS/FHIR ImagingStudy load
    [switch]$SkipFabric,             # Skip deploy-fabric-rti.ps1 entirely
    [switch]$Phase2,             # Run only Fabric Phase 2
    [switch]$Phase3,             # Run only Phase 3 (Cohorting Agent + DICOM Viewer)
    [switch]$Phase4,             # Run only Phase 4 (Ontology + Agent binding)
    [switch]$Phase5,             # Run only Phase 5 (CMS Quality & Claims)
    [switch]$Phase7,                 # Run only Phase 7 (Payer RTI & Ops)
    [switch]$SkipPhase7,             # Skip Payer RTI & Ops entirely
    [switch]$SkipPayerRti,           # Skip claim-stream, payer KQL, claim emulator, Eventstream extension
    [switch]$SkipPayerActivator,     # Skip PayerOpsActivator Reflex
    [switch]$SkipOpsAgent,           # Skip HealthcareOpsAgent + Payer Ops Triage
    [switch]$SkipGraphAgent,         # Skip Healthcare Graph Agent shell/manual instructions
    [switch]$RebuildContainers,      # Force container image rebuilds
    [switch]$ReusePatients,          # Reuse existing patients — skip Synthea/Loader, keep emulator
    [switch]$UseCachedSynthea,       # Use cached/prepackaged Synthea patient bundles
    [hashtable]$Tags = @{},            # Resource tags (e.g. @{SecurityControl='Ignore'})
    [switch]$SkipFhirExport,         # Skip FHIR $export step in Fabric Phase 1

    # ── Granular component skips ──
    [switch]$SkipSynthea,            # Skip Synthea patient generation (implies SkipDeviceAssoc)
    [switch]$SkipDeviceAssoc,        # Skip Device resource creation / association
    [switch]$SkipRtiPhase2,          # Skip RTI Phase 2 (KQL shortcuts + enriched alerts)
    [switch]$SkipHdsPipelines,       # Skip DICOM shortcut + HDS pipeline triggers
    [switch]$SkipDataAgents,         # Skip Patient 360 + Clinical Triage agents
    [switch]$SkipImaging,            # Skip Imaging Toolkit (Cohorting Agent, DICOM Viewer, PBI)
    [switch]$SkipOntology,           # Skip ClinicalDeviceOntology + agent binding
    [switch]$ReplaceOntology,        # Delete/recreate ClinicalDeviceOntology instead of preserving existing item
    [switch]$SkipActivator,          # Skip Data Activator (Reflex + email rule)
    [switch]$SkipQualityMeasures,    # Skip Population Health & Quality Dashboard (claims, measures, risk, utilization, report)
    [switch]$RequireBronzeClinicalFhir, # Require Bronze ClinicalFhir rows after synthesized clinical pipeline runs
    [switch]$RequireBronzeImagingDicom, # Require Bronze ImagingDicom rows after synthesized imaging pipeline runs

    # ── Phase 3 (FabricDicomCohortingToolkit) ──
    [string]$DicomToolkitPath = "C:\git\FabricDicomCohortingToolkit",
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer",

    # ── Phase 4 (Activator) ──
    [string]$AlertEmail = "",               # Email for clinical alert notifications (e.g. joey@brakeat.com)
    [string]$AlertTierThreshold = "URGENT", # Minimum tier to send email: WARNING, URGENT, or CRITICAL
    [int]$AlertCooldownMinutes = 15,         # Suppress duplicate alerts per device within this window
    [string]$PayerOpsEmail = "",    # Email for payer operations alerts; falls back to -AlertEmail only when empty
    [int]$ClaimEventRatePerMinute = 60,

    # ── SPN Credentials ──
    [string]$SpnClientId = "",
    [string]$SpnClientSecret = "",
    [string]$SpnTenantId = "",

    # ── JSON Logging ──
    [switch]$JsonLogs,

    # ── Cleanup ──
    [switch]$Teardown                # Run cleanup scripts instead of deployment
)

$ErrorActionPreference = "Stop"

# Prevent interactive `az` extension-install prompts from hanging the orchestrator.
# The orchestrator launches pwsh with -NonInteractive; any extension prompt would hang forever.
# See #fix-2.
$null = az config set extension.use_dynamic_install=yes_without_prompt --only-show-errors 2>$null

$script:AccessTokenCache = @{}
function Get-CachedAccessToken {
    param([Parameter(Mandatory)][string]$ResourceUrl)
    $key = $ResourceUrl.ToLowerInvariant()
    $cached = $script:AccessTokenCache[$key]
    if ($cached -and $cached.ExpiresOn -gt (Get-Date).AddMinutes(5)) { return $cached.Token }
    $tokenObj = Get-AzAccessToken -ResourceUrl $ResourceUrl -ErrorAction Stop
    $token = $tokenObj.Token
    if ($token -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($token)
        try { $token = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    $script:AccessTokenCache[$key] = @{ Token = $token; ExpiresOn = $tokenObj.ExpiresOn }
    return $token
}

function Test-FabricPaidFsku {
    param([AllowNull()][string]$Sku)
    if ([string]::IsNullOrWhiteSpace($Sku)) { return $false }
    $normalized = $Sku.ToUpperInvariant()
    return $normalized.StartsWith("F") -and -not $normalized.StartsWith("FT") -and $normalized -ne "PP3"
}

function Resolve-SelectedFabricCapacity {
    param(
        [Parameter(Mandatory)]$Capacities,
        [Parameter(Mandatory)][string]$Name
    )

    $matches = @($Capacities.value | Where-Object { $_.displayName -eq $Name -or $_.name -eq $Name })
    if ($matches.Count -eq 0) {
        throw "Selected Fabric capacity '$Name' was not returned by Fabric. Refresh the UI capacity list and try again."
    }

    $activePaidMatches = @($matches | Where-Object { $_.state -eq "Active" -and (Test-FabricPaidFsku -Sku $_.sku) })
    if ($activePaidMatches.Count -eq 0) {
        $seen = ($matches | ForEach-Object { "$($_.displayName) [$($_.sku), $($_.state)]" }) -join ", "
        throw "Selected Fabric capacity '$Name' is not an active paid F-SKU. Seen: $seen"
    }

    if ($activePaidMatches.Count -gt 1) {
        Write-Host "  ⚠ Multiple active paid Fabric capacities named '$Name' were returned by Fabric; using the first match." -ForegroundColor Yellow
    }

    return $activePaidMatches | Select-Object -First 1
}

# Validate conditionally-required parameters
if (-not $Teardown -and -not $Phase2 -and -not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $AdminSecurityGroup) {
    throw "Parameter '-AdminSecurityGroup' is required for deployment. Only -Teardown, -Phase2, -Phase3, -Phase4, -Phase5, and -Phase7 can omit it."
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $ScriptDir

$DicomToolkitRepoUrl = "https://github.com/kfprugger/FabricDicomCohortingToolkit.git"

function Resolve-DicomToolkitPath {
    param([string]$RequestedPath)

    if (-not [string]::IsNullOrWhiteSpace($RequestedPath) -and $RequestedPath -ne "C:\git\FabricDicomCohortingToolkit") {
        return $RequestedPath
    }

    if ($IsMacOS -or $IsLinux) {
        $siblingPath = Join-Path (Split-Path -Parent $ScriptDir) "FabricDicomCohortingToolkit"
        if (Test-Path $siblingPath) { return $siblingPath }
        if ([string]::IsNullOrWhiteSpace($RequestedPath) -or $RequestedPath -eq "C:\git\FabricDicomCohortingToolkit") { return $siblingPath }
    }

    return $RequestedPath
}

function Ensure-DicomToolkitRepo {
    param([Parameter(Mandatory)][string]$Path)

    if (Test-Path (Join-Path $Path "Deploy-DataAgent.ps1")) {
        return @{ Success = $true; Cloned = $false; Message = "DICOM Toolkit found at $Path" }
    }

    $git = Get-Command git -ErrorAction SilentlyContinue
    if (-not $git) {
        return @{ Success = $false; Cloned = $false; Message = "DICOM Toolkit missing at '$Path' and Git is not installed." }
    }

    $parent = Split-Path -Parent $Path
    if (-not (Test-Path $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }

    if (Test-Path $Path) {
        $hasContent = @(Get-ChildItem -LiteralPath $Path -Force -ErrorAction SilentlyContinue).Count -gt 0
        if ($hasContent) {
            return @{ Success = $false; Cloned = $false; Message = "DICOM Toolkit path '$Path' exists but does not contain Deploy-DataAgent.ps1. Set -DicomToolkitPath or move/remove the incomplete directory." }
        }
    }

    Write-Host "  → DICOM Toolkit missing; cloning $DicomToolkitRepoUrl to $Path" -ForegroundColor Yellow
    git clone $DicomToolkitRepoUrl $Path
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path (Join-Path $Path "Deploy-DataAgent.ps1"))) {
        return @{ Success = $false; Cloned = $false; Message = "Failed to clone DICOM Toolkit from $DicomToolkitRepoUrl to '$Path'." }
    }

    return @{ Success = $true; Cloned = $true; Message = "DICOM Toolkit cloned to $Path" }
}

$DicomToolkitPath = Resolve-DicomToolkitPath -RequestedPath $DicomToolkitPath
if ($DicomToolkitPath -ne "C:\git\FabricDicomCohortingToolkit") {
    Write-Host "  [PATH] DICOM Toolkit path: $DicomToolkitPath" -ForegroundColor DarkGray
}

# ── Granular flag reconciliation ──────────────────────────────────────
# Fine-grained skip flags from the Orchestrator UI are translated into
# the coarser switches consumed by the existing step logic.
# SkipSynthea implies the FHIR loader has nothing new to load; however
# the FHIR *service* infra still deploys so HDS / $export keep working.
if ($SkipSynthea) { $ReusePatients = $true }
if ($Phase4 -or $Phase5) { $SkipPhase7 = $true }
if ($SkipFabric) {
    $SkipRtiPhase2 = $true
    $SkipActivator = $true
    $SkipPhase7 = $true
    $SkipPayerRti = $true
    $SkipPayerActivator = $true
    $SkipOpsAgent = $true
    $SkipGraphAgent = $true
}
if ($SkipPhase7) {
    $SkipPayerRti = $true
    $SkipPayerActivator = $true
    $SkipOpsAgent = $true
    $SkipGraphAgent = $true
}
if ([string]::IsNullOrWhiteSpace($PayerOpsEmail) -and -not [string]::IsNullOrWhiteSpace($AlertEmail)) { $PayerOpsEmail = $AlertEmail }
if ([string]::IsNullOrWhiteSpace($PayerOpsEmail)) { $SkipPayerActivator = $true }
# Additional granular skips are checked inline at each step entry point.
# The skip variables are available as $SkipRtiPhase2, $SkipHdsPipelines,
# $SkipDataAgents, $SkipImaging, $SkipOntology, $SkipActivator,
# $SkipDeviceAssoc.

# ============================================================================
# PREFLIGHT PREREQUISITE CHECKS
# ============================================================================

function Test-Prerequisites {
    <#
    .SYNOPSIS
        Validates all deployment prerequisites before starting.
        Exits with actionable error messages if any check fails.
    #>
    Write-Host ""
    Write-Host "+============================================================+" -ForegroundColor Cyan
    Write-Host "|              PREFLIGHT PREREQUISITE CHECKS                 |" -ForegroundColor Cyan
    Write-Host "+============================================================+" -ForegroundColor Cyan
    Write-Host ""

    $failures = @()
    $warnings = @()

    # 1. PowerShell version (7+)
    if ($PSVersionTable.PSVersion.Major -ge 7) {
        Write-Host "  ✓ PowerShell $($PSVersionTable.PSVersion)" -ForegroundColor Green
    } else {
        $failures += "PowerShell 7+ required (current: $($PSVersionTable.PSVersion)). Install from https://aka.ms/powershell"
        Write-Host "  ✗ PowerShell $($PSVersionTable.PSVersion) — version 7+ required" -ForegroundColor Red
    }

    # 2. Az PowerShell module
    $azModule = Get-Module -ListAvailable -Name Az.Accounts | Select-Object -First 1
    if ($azModule) {
        Write-Host "  ✓ Az module $($azModule.Version)" -ForegroundColor Green
    } else {
        $failures += "Az PowerShell module not found. Run: Install-Module Az -Scope CurrentUser"
        Write-Host "  ✗ Az module not installed" -ForegroundColor Red
    }

    # 3. Azure CLI
    try {
        $azVer = az version --output json 2>$null | ConvertFrom-Json
        $cliVer = $azVer.'azure-cli'
        if ($cliVer -match '(\d+)\.(\d+)\.(\d+)') {
            $major = [int]$Matches[1]
            $minor = [int]$Matches[2]
            if ($major -lt 2 -or ($major -eq 2 -and $minor -lt 50)) {
                $failures += "Azure CLI version $cliVer is too old. Version 2.50.0 or higher is required."
                Write-Host "  ✗ Azure CLI $cliVer — version 2.50.0+ required" -ForegroundColor Red
            } else {
                Write-Host "  ✓ Azure CLI $cliVer" -ForegroundColor Green
            }
        } else {
            $warnings += "Azure CLI version check inconclusive ($cliVer)."
            Write-Host "  ⚠ Azure CLI version unrecognized" -ForegroundColor Yellow
        }
    } catch {
        $failures += "Azure CLI not found. Install from https://aka.ms/installazurecli"
        Write-Host "  ✗ Azure CLI not installed" -ForegroundColor Red
    }

    # 4. Bicep
    try {
        $bicepOutput = (az bicep version 2>$null) -join ' '
        if ($bicepOutput -match '(\d+)\.(\d+)\.(\d+)') {
            $bMajor = [int]$Matches[1]
            $bMinor = [int]$Matches[2]
            if ($bMajor -eq 0 -and $bMinor -lt 20) {
                $failures += "Bicep CLI version $($Matches[1]).$($Matches[2]).$($Matches[3]) is too old. Version 0.20.0 or higher is required."
                Write-Host "  ✗ Bicep $($Matches[1]).$($Matches[2]).$($Matches[3]) — version 0.20.0+ required" -ForegroundColor Red
            } else {
                Write-Host "  ✓ Bicep $($Matches[1]).$($Matches[2]).$($Matches[3])" -ForegroundColor Green
            }
        } else {
            $warnings += "Bicep version check inconclusive. Run: az bicep install"
            Write-Host "  ⚠ Bicep version unknown" -ForegroundColor Yellow
        }
    } catch {
        $failures += "Bicep not installed. Run: az bicep install"
        Write-Host "  ✗ Bicep not installed" -ForegroundColor Red
    }

    # 5. Azure login
    try {
        $account = az account show --output json 2>$null | ConvertFrom-Json
        if ($account.id) {
            Write-Host "  ✓ Azure login: $($account.name) ($($account.id.Substring(0,8))...)" -ForegroundColor Green
        } else {
            $failures += "Not logged in to Azure. Run: az login"
            Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
        }
    } catch {
        $failures += "Not logged in to Azure. Run: az login"
        Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
    }

    # 6. Python 3.10+
    try {
        $pyVer = python --version 2>&1
        if ($pyVer -match "(\d+)\.(\d+)\.(\d+)") {
            $major = [int]$Matches[1]; $minor = [int]$Matches[2]
            if ($major -ge 3 -and $minor -ge 10) {
                Write-Host "  ✓ Python $($Matches[0])" -ForegroundColor Green
            } else {
                $failures += "Python 3.10+ required (current: $($Matches[0])). Install from https://python.org"
                Write-Host "  ✗ Python $($Matches[0]) — version 3.10+ required" -ForegroundColor Red
            }
        }
    } catch {
        $warnings += "Python not found (only needed for device associations). Install from https://python.org"
        Write-Host "  ⚠ Python not found (optional for Phase 1)" -ForegroundColor Yellow
    }

    # 7. Git (needed for Phase 3 — DICOM toolkit)
    try {
        $gitVer = git --version 2>$null
        if ($gitVer -match "(\d+\.\d+\.\d+)") {
            Write-Host "  ✓ Git $($Matches[1])" -ForegroundColor Green
        }
    } catch {
        $warnings += "Git not found (only needed for Phase 3 DICOM toolkit)"
        Write-Host "  ⚠ Git not found (optional)" -ForegroundColor Yellow
    }

    # 8. Admin Security Group exists in Entra ID
    if ($AdminSecurityGroup -and -not $Phase2 -and -not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7) {
        try {
            $grp = az ad group show --group $AdminSecurityGroup --query "id" -o tsv 2>$null
            if ($grp) {
                Write-Host "  ✓ Admin group '$AdminSecurityGroup' found" -ForegroundColor Green
            } else {
                $failures += "Security group '$AdminSecurityGroup' not found in Entra ID"
                Write-Host "  ✗ Security group '$AdminSecurityGroup' not found" -ForegroundColor Red
            }
        } catch {
            $failures += "Security group '$AdminSecurityGroup' not found in Entra ID"
            Write-Host "  ✗ Security group '$AdminSecurityGroup' not found" -ForegroundColor Red
        }
    }

    # 9. Fabric API reachable (tests token acquisition)
    try {
        $fabToken = Get-CachedAccessToken "https://api.fabric.microsoft.com"
        if ($fabToken -is [System.Security.SecureString]) {
            $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($fabToken)
            try { $fabToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
            finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
        }
        $fabHeaders = @{ "Authorization" = "Bearer $fabToken" }
        $caps = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/capacities" -Headers $fabHeaders

        if (-not [string]::IsNullOrWhiteSpace($CapacityName)) {
            try {
                $cap = Resolve-SelectedFabricCapacity -Capacities $caps -Name $CapacityName
                $scope = if ($CapacitySubscriptionId -or $CapacityResourceGroup) { " [$CapacitySubscriptionId/$CapacityResourceGroup]" } else { "" }
                Write-Host "  ✓ Selected Fabric capacity: $($cap.displayName) (SKU: $($cap.sku), State: $($cap.state))$scope" -ForegroundColor Green
            } catch {
                $failures += $_.Exception.Message
                Write-Host "  ✗ Selected Fabric capacity '$CapacityName' is not usable: $($_.Exception.Message)" -ForegroundColor Red
            }
        } else {
            $allPaidCaps = $caps.value | Where-Object { Test-FabricPaidFsku -Sku $_.sku }

            if ($allPaidCaps.Count -gt 0) {
                $activePaidCaps = $allPaidCaps | Where-Object { $_.state -eq "Active" }
                if ($activePaidCaps.Count -gt 0) {
                    $cap = $activePaidCaps | Select-Object -First 1
                    Write-Host "  ✓ Fabric capacity: $($cap.displayName) (SKU: $($cap.sku))" -ForegroundColor Green
                } else {
                    $cap = $allPaidCaps | Select-Object -First 1
                    $failures += "Fabric capacity '$($cap.displayName)' ($($cap.sku)) is currently paused ($($cap.state)). Please resume it before deploying."
                    Write-Host "  ✗ Fabric capacity: $($cap.displayName) (SKU: $($cap.sku)) — currently paused ($($cap.state))" -ForegroundColor Red
                }
            } else {
                $activeTrialCaps = $caps.value | Where-Object { $_.sku -like "FT*" -and $_.state -eq "Active" }
                if ($activeTrialCaps.Count -gt 0) {
                    $cap = $activeTrialCaps | Select-Object -First 1
                    $failures += "Trial capacity ($($cap.sku)) cannot deploy Healthcare Data Solutions. A paid F-SKU (F2+) is required."
                    Write-Host "  ✗ Fabric capacity: $($cap.displayName) (SKU: $($cap.sku)) — trial not supported" -ForegroundColor Red
                } else {
                    $failures += "No active paid Fabric capacity found. Please create or resume a paid F-SKU (F2+) at https://app.fabric.microsoft.com"
                    Write-Host "  ✗ No active paid Fabric capacity found" -ForegroundColor Red
                }
            }
        }
    } catch {
        $fabricCheckError = $_
        $fabricCheckBody = ""
        try { $fabricCheckBody = $fabricCheckError.ErrorDetails.Message } catch {}

        $workspaceCapacityId = $null
        try {
            $wsResp = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $fabHeaders
            $existingWs = $wsResp.value | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
            if ($existingWs) {
                $wsDetail = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($existingWs.id)" -Headers $fabHeaders
                $workspaceCapacityId = $wsDetail.capacityId
            }
        } catch {}

        if ($workspaceCapacityId) {
            Write-Host "  ✓ Fabric workspace '$FabricWorkspaceName' already has capacity assigned ($workspaceCapacityId)" -ForegroundColor Green
            if ($fabricCheckBody -match "RequestDeniedByInboundPolicy") {
                Write-Host "    Note: /capacities is blocked by Fabric inbound policy; using workspace capacity state instead." -ForegroundColor DarkGray
            }
        } else {
            if ($fabricCheckBody -match "RequestDeniedByInboundPolicy") {
                $failures += "Fabric API capacities endpoint is blocked by inbound communication policy and workspace '$FabricWorkspaceName' has no visible capacity assignment. Connect from an allowed network/VPN or relax the Fabric policy."
            } else {
                $failures += "Cannot access Fabric API. Ensure Az module login has Fabric permissions."
            }
            Write-Host "  ✗ Fabric API unreachable: $($fabricCheckError.Exception.Message)" -ForegroundColor Red
        }
    }

    # 10. DicomToolkitPath (Phase 3 / imaging toolkit)
    $requiresDicomToolkit = -not $SkipImaging -and ($Phase2 -or $Phase3 -or (-not $Phase4 -and -not $Phase5 -and -not $Phase7))
    if ($requiresDicomToolkit) {
        $toolkit = Ensure-DicomToolkitRepo -Path $DicomToolkitPath
        if ($toolkit.Success) {
            Write-Host "  ✓ $($toolkit.Message)" -ForegroundColor Green
        } else {
            $failures += $toolkit.Message
            Write-Host "  ✗ $($toolkit.Message)" -ForegroundColor Red
        }
    }

    # 11. Azure Health Data Services region availability
    if (-not $SkipFhir) {
        try {
            $ahdsLocations = az provider show --namespace Microsoft.HealthcareApis `
                --query "resourceTypes[?resourceType=='workspaces'].locations[]" -o json 2>$null | ConvertFrom-Json

            if ($ahdsLocations -and $ahdsLocations.Count -gt 0) {
                # ARM returns display names ("East US"); normalise for comparison
                $normalised = $ahdsLocations | ForEach-Object { ($_ -replace '\s','').ToLower() }
                $locationKey = ($Location -replace '\s','').ToLower()

                if ($locationKey -in $normalised) {
                    Write-Host "  ✓ AHDS available in '$Location'" -ForegroundColor Green
                } else {
                    $supported = ($ahdsLocations | Sort-Object) -join ", "
                    $failures += "Azure Health Data Services is NOT available in '$Location'. Supported regions: $supported"
                    Write-Host "  ✗ AHDS not available in '$Location'" -ForegroundColor Red
                    Write-Host "    Supported regions:" -ForegroundColor Yellow
                    $ahdsLocations | Sort-Object | ForEach-Object { Write-Host "      • $_" -ForegroundColor White }
                }
            } else {
                $warnings += "Could not query AHDS region availability from ARM"
                Write-Host "  ⚠ Could not verify AHDS region availability" -ForegroundColor Yellow
            }
        } catch {
            $warnings += "Could not query AHDS region availability: $($_.Exception.Message)"
            Write-Host "  ⚠ Could not verify AHDS region availability" -ForegroundColor Yellow
        }
    }

    Write-Host ""

    # Report warnings
    if ($warnings.Count -gt 0) {
        Write-Host "  Warnings ($($warnings.Count)):" -ForegroundColor Yellow
        foreach ($w in $warnings) {
            Write-Host "    ⚠ $w" -ForegroundColor Yellow
        }
        Write-Host ""
    }

    # Fail on errors
    if ($failures.Count -gt 0) {
        Write-Host "  PREFLIGHT FAILED — $($failures.Count) issue(s) must be resolved:" -ForegroundColor Red
        Write-Host ""
        foreach ($f in $failures) {
            Write-Host "    ✗ $f" -ForegroundColor Red
        }
        Write-Host ""
        Write-Host "  Fix the above issues and re-run the deployment." -ForegroundColor Red
        Write-Host ""
        Pop-Location
        exit 1
    }

    Write-Host "  All preflight checks passed ✓" -ForegroundColor Green
    Write-Host ""
}

# Run preflight checks (skip for teardown)
if (-not $Teardown) {
    Test-Prerequisites
}

# ============================================================================
# DEPLOYMENT STATE FILE — shared context across phases
# Stored in state-tracking/ subfolder (fallback for standalone runs;
# the orchestrator persists resources to SQLite as primary store).
# ============================================================================

$stateDir = Join-Path $ScriptDir "state-tracking"
if (-not (Test-Path $stateDir)) { New-Item -ItemType Directory -Path $stateDir -Force | Out-Null }
$stateFile = Join-Path $stateDir ".deployment-state-$FabricWorkspaceName.json"

function Read-DeploymentState {
    if (Test-Path $stateFile) {
        return Get-Content $stateFile -Raw | ConvertFrom-Json
    }
    return @{ phases = @() }
}

function Write-DeploymentState {
    param([object]$State)
    $State | ConvertTo-Json -Depth 10 | Set-Content $stateFile -Encoding UTF8
}

function Save-PhaseResult {
    param(
        [string]$PhaseName,
        [hashtable]$Resources = @{},
        [array]$Steps = @()
    )
    $state = Read-DeploymentState
    # Convert to hashtable if it's a PSCustomObject (from JSON)
    if ($state -is [PSCustomObject]) {
        $phases = @($state.phases)
    } else {
        $phases = @($state.phases)
    }
    $phaseEntry = @{
        phase     = $PhaseName
        timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
        resources = $Resources
        steps     = $Steps
    }
    $phases += $phaseEntry
    Write-DeploymentState @{ phases = $phases }

    # Emit resource markers for orchestrator SQLite capture
    foreach ($k in $Resources.Keys) {
        Write-Host "##ORCH_RESOURCE:$k=$($Resources[$k])"
    }
}

function Get-PhaseResources {
    param([string]$PhaseName)
    $state = Read-DeploymentState
    $phase = $state.phases | Where-Object { $_.phase -eq $PhaseName } | Select-Object -Last 1
    if ($phase) { return $phase.resources }
    return @{}
}

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

function Emit-PhaseTransition {
    param(
        [Parameter(Mandatory)][int]$Phase,
        [Parameter(Mandatory)][string]$Label,
        [int]$StepCount = 0
    )
    # Structured line for the Python orchestrator to parse.
    # Format: @@PHASE|<number>|<label>|<stepCount>@@
    Write-Host "@@PHASE|$Phase|$Label|$StepCount@@"
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
        Write-Summary -PhaseName "Failed"
        Pop-Location
        exit 1
    }
}

function Assert-LastExternalCommandSucceeded {
    param(
        [Parameter(Mandatory)][string]$CommandName
    )
    $exitCode = $global:LASTEXITCODE
    if ($null -ne $exitCode -and $exitCode -ne 0) {
        throw "$CommandName failed with exit code $exitCode"
    }
}


function Write-Summary {
    param(
        [string]$Title = "DEPLOYMENT SUMMARY",
        [string]$PhaseName = "",
        [hashtable]$PhaseResources = @{}
    )
    $overallTimer.Stop()
    $totalMin = [math]::Round($overallTimer.Elapsed.TotalMinutes, 1)

    # Save phase results to state file
    if ($PhaseName) {
        $stepData = $script:stepResults | ForEach-Object {
            @{ name = $_.Name; success = $_.Success; duration = $_.Duration; detail = $_.Detail }
        }
        Save-PhaseResult -PhaseName $PhaseName -Resources $PhaseResources -Steps $stepData
    }

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

    # Show cross-phase history if multiple phases have run
    $state = Read-DeploymentState
    if ($state.phases -and @($state.phases).Count -gt 1) {
        Write-Host "  ┌─────────────────────────────────────────────────────────────┐" -ForegroundColor DarkCyan
        Write-Host "  │  ALL PHASES                                                │" -ForegroundColor DarkCyan
        Write-Host "  ├─────────────────────────────────────────────────────────────┤" -ForegroundColor DarkCyan
        foreach ($p in $state.phases) {
            $pSteps = @($p.steps)
            $pPassed = ($pSteps | Where-Object { $_.success }).Count
            $pTotal = $pSteps.Count
            $pIcon = if ($pPassed -eq $pTotal) { "✓" } else { "⚠" }
            $pColor = if ($pPassed -eq $pTotal) { "Green" } else { "Yellow" }
            Write-Host "  │  $pIcon $($p.phase.PadRight(20)) $pPassed/$pTotal steps   $($p.timestamp)  │" -ForegroundColor $pColor
        }
        Write-Host "  └─────────────────────────────────────────────────────────────┘" -ForegroundColor DarkCyan
        Write-Host ""
    }

    # Show key resources from state
    if ($state.phases) {
        $allResources = @{}
        foreach ($p in $state.phases) {
            if ($p.resources) {
                $p.resources.PSObject.Properties | ForEach-Object { $allResources[$_.Name] = $_.Value }
            }
        }
        if ($allResources.Count -gt 0) {
            Write-Host "  Resources (from state-tracking/.deployment-state-$FabricWorkspaceName.json):" -ForegroundColor DarkGray
            foreach ($k in $allResources.Keys | Sort-Object) {
                Write-Host "    $($k): $($allResources[$k])" -ForegroundColor DarkGray
            }
            Write-Host ""
        }
    }
}

function Resolve-StorageAccountName {
    param([string]$Name, [string]$Context = "Storage account")
    # Sanitize: lowercase, alphanumeric only, 3-24 chars
    $sanitized = ($Name.ToLower() -replace '[^a-z0-9]', '')
    if ($sanitized.Length -gt 24) { $sanitized = $sanitized.Substring(0, 24) }
    if ($sanitized.Length -lt 3)  { throw "$Context name '$Name' is too short after sanitization (got '$sanitized')." }
    if ($sanitized -ne $Name) {
        Write-Host "  ⚠ $Context name sanitized: '$Name' → '$sanitized'" -ForegroundColor Yellow
    }
    return $sanitized
}

function Move-FabricItemsToFolder {
    param(
        [Parameter(Mandatory)][string]$FabricWorkspaceName,
        [string]$WorkspaceId,
        [Parameter(Mandatory)][string]$FolderName,
        [Parameter(Mandatory)][array]$ItemTypes,
        [string]$FabricApiBase = "https://api.fabric.microsoft.com/v1"
    )

    try {
        Write-Host "  Organizing items of type ($($ItemTypes -join ', ')) into folder '$FolderName'..." -ForegroundColor White
        $token = Get-CachedAccessToken "https://api.fabric.microsoft.com"
        $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

        if (-not $WorkspaceId) {
            $ws = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces" -Headers $headers -Method Get).value |
                Where-Object { $_.displayName -eq $FabricWorkspaceName } |
                Select-Object -First 1
            if (-not $ws) { throw "Workspace '$FabricWorkspaceName' not found" }
            $WorkspaceId = $ws.id
        }

        $items = @()
        foreach ($type in $ItemTypes) {
            try {
                $typeItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=$type" -Headers $headers -Method Get).value
                if ($typeItems) { $items += $typeItems }
            } catch {
                Write-Host "    ⚠ Could not retrieve items of type '$type': $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }

        if ($items.Count -eq 0) {
            Write-Host "  No items found to organize for types: $($ItemTypes -join ', ')." -ForegroundColor DarkGray
            return
        }

        # Find or create folder ONLY if there are items to move
        $folders = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" -Headers $headers -Method Get).value
        $folder = $folders | Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
        if (-not $folder) {
            $folderBody = @{ displayName = $FolderName } | ConvertTo-Json -Depth 3
            $folder = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/folders" -Headers $headers -Method Post -Body $folderBody
            Write-Host "  ✓ Created folder '$FolderName'" -ForegroundColor Green
        }

        $moved = 0
        foreach ($item in $items) {
            if ($item.folderId -eq $folder.id) { continue }
            $moveBody = @{ targetFolderId = $folder.id } | ConvertTo-Json -Depth 3
            for ($attempt = 1; $attempt -le 4; $attempt++) {
                try {
                    Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$($item.id)/move" -Headers $headers -Method Post -Body $moveBody | Out-Null
                    $moved++
                    break
                } catch {
                    $errCode = $null
                    try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
                    if ($errCode -eq 429 -and $attempt -lt 4) {
                        $sleepSec = 10 * $attempt
                        Write-Host "    Throttled moving '$($item.displayName)' — retrying in ${sleepSec}s..." -ForegroundColor DarkYellow
                        Start-Sleep -Seconds $sleepSec
                    } else {
                        Write-Host "    ⚠ Could not move '$($item.displayName)': $($_.Exception.Message)" -ForegroundColor Yellow
                        break
                    }
                }
            }
        }
        Write-Host "  ✓ Folder '$FolderName' ready ($moved moved, $($items.Count) total)" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Could not organize items into folder '$FolderName': $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

function Move-FabricNotebooksToFolder {
    param(
        [Parameter(Mandatory)][string]$FabricWorkspaceName,
        [string]$WorkspaceId,
        [string]$FolderName = "Notebooks",
        [string]$FabricApiBase = "https://api.fabric.microsoft.com/v1"
    )
    Move-FabricItemsToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $WorkspaceId -FolderName $FolderName -ItemTypes @("Notebook") -FabricApiBase $FabricApiBase
}

# ============================================================================
# PHASE 3 DIAGNOSTICS — Query lakehouse tables for row counts at checkpoints
# ============================================================================

function Write-Phase3Diagnostics {
    param(
        [string]$Checkpoint,             # e.g. "PRE-VIEWER", "POST-NOTEBOOK"
        [string]$WorkspaceId,
        [string]$FabricApiBase = "https://api.fabric.microsoft.com/v1"
    )

    $diagTimestamp = Get-Date -Format "HH:mm:ss"
    Write-Host ""
    Write-Host "  ┌── DIAGNOSTICS [$Checkpoint] $diagTimestamp ──" -ForegroundColor DarkYellow
    Write-Host "  │" -ForegroundColor DarkYellow

    try {
        # Get cached tokens for API calls.
        $fabToken = Get-CachedAccessToken "https://api.fabric.microsoft.com"

        # Get a token for SQL access — try multiple resource URL formats
        $sqlToken = $null
        foreach ($sqlResource in @("https://database.windows.net/", "https://database.windows.net/.default", "https://database.windows.net")) {
            try {
                $sqlToken = Get-CachedAccessToken $sqlResource
                break
            } catch { continue }
        }
        if (-not $sqlToken) {
            Write-Host "  │  ⚠ Could not acquire SQL token — skipping SQL diagnostics" -ForegroundColor Yellow
        }

        # Discover lakehouses via Fabric API
        $fabH = @{ Authorization = "Bearer $fabToken"; "Content-Type" = "application/json" }
        $lakehouses = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses" -Headers $fabH).value

        # Helper: run a diagnostic query via python using the PS-acquired token
        function Invoke-DiagQuery {
            param([string]$Server, [string]$Database, [string]$Token, [string]$Query)
            # Pass the token and query via env vars so Python doesn't need to re-authenticate
            # and we avoid any string escaping issues between PS and Python
            $env:_DIAG_SQL_TOKEN = $Token
            $env:_DIAG_SQL_QUERY = $Query
            $pyScript = @"
import pyodbc, struct, os
token = os.environ['_DIAG_SQL_TOKEN']
query = os.environ['_DIAG_SQL_QUERY']
tb = token.encode('UTF-16-LE')
ts = struct.pack(f'<I{len(tb)}s', len(tb), tb)
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};SERVER=$Server;DATABASE=$Database;Encrypt=Yes;',
    attrs_before={1256: ts})
cur = conn.cursor()
cur.execute(query)
for row in cur.fetchall():
    cols = '|'.join(str(c) if c is not None else '' for c in row)
    print(cols)
conn.close()
"@
            $result = $pyScript | python - 2>&1
            Remove-Item Env:\_DIAG_SQL_TOKEN -ErrorAction SilentlyContinue
            Remove-Item Env:\_DIAG_SQL_QUERY -ErrorAction SilentlyContinue
            return $result
        }

        # --- Silver Lakehouse ---
        $silverLh = $lakehouses | Where-Object { $_.displayName -match '[Ss]ilver' } | Select-Object -First 1
        if ($silverLh) {
            $silverDetail = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($silverLh.id)" -Headers $fabH
            $silverServer = $silverDetail.properties.sqlEndpointProperties.connectionString
            $silverDb = $silverLh.displayName
            Write-Host "  │  Silver Lakehouse ($silverDb):" -ForegroundColor DarkYellow

            if ($silverServer -and $sqlToken) {
                $diagQuery = @"
SELECT 'ImagingMetastore' AS tbl, COUNT(*) AS cnt, COUNT(filePath) AS fp FROM dbo.ImagingMetastore
UNION ALL SELECT 'ImagingStudy', COUNT(*), 0 FROM dbo.ImagingStudy
UNION ALL SELECT 'Patient', COUNT(*), 0 FROM dbo.Patient
UNION ALL SELECT 'Condition', COUNT(*), 0 FROM dbo.Condition
"@
                $rows = Invoke-DiagQuery -Server $silverServer -Database $silverDb -Token $sqlToken -Query $diagQuery
                foreach ($line in $rows) {
                    if ($line -match '^(.+?)\|(\d+)\|(\d+)$') {
                        $tbl = $Matches[1].Trim()
                        $cnt = [int]$Matches[2]
                        $fp = [int]$Matches[3]
                        $icon = if ($cnt -gt 0) { "✓" } else { "○" }
                        $extra = if ($tbl -eq 'ImagingMetastore') { " (filePath: $fp)" } else { "" }
                        Write-Host "  │    $icon $($tbl.PadRight(25)) $($cnt.ToString().PadLeft(8)) rows$extra" -ForegroundColor $(if ($cnt -gt 0) { 'Green' } else { 'DarkGray' })
                    } elseif ($line -and $line -notmatch '^\s*$') {
                        Write-Host "  │    ! $line" -ForegroundColor DarkGray
                    }
                }
            } elseif (-not $silverServer) {
                Write-Host "  │    ⚠ SQL endpoint not available yet" -ForegroundColor Yellow
            } else {
                Write-Host "  │    ⚠ No SQL token — skipping row counts" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  │  ⚠ Silver Lakehouse not found" -ForegroundColor Yellow
        }

        # --- Reporting Gold Lakehouse ---
        $reportingLh = $lakehouses | Where-Object { $_.displayName -eq 'healthcare1_reporting_gold' } | Select-Object -First 1
        if ($reportingLh) {
            $rptDetail = Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($reportingLh.id)" -Headers $fabH
            $rptServer = $rptDetail.properties.sqlEndpointProperties.connectionString
            $rptDb = $reportingLh.displayName
            Write-Host "  │  Reporting Lakehouse ($rptDb):" -ForegroundColor DarkYellow

            if ($rptServer -and $sqlToken) {
                $rptQuery = @"
SELECT 'DicomFileReporting' AS tbl, COUNT(*) AS cnt FROM dbo.DicomFileReporting
UNION ALL SELECT 'ImagingStudyReporting', COUNT(*) FROM dbo.ImagingStudyReporting
UNION ALL SELECT 'PatientReporting', COUNT(*) FROM dbo.PatientReporting
"@
                $rptRows = Invoke-DiagQuery -Server $rptServer -Database $rptDb -Token $sqlToken -Query $rptQuery
                foreach ($line in $rptRows) {
                    if ($line -match '^(.+?)\|(\d+)$') {
                        $tbl = $Matches[1].Trim()
                        $cnt = [int]$Matches[2]
                        $icon = if ($cnt -gt 0) { "✓" } else { "○" }
                        Write-Host "  │    $icon $($tbl.PadRight(25)) $($cnt.ToString().PadLeft(8)) rows" -ForegroundColor $(if ($cnt -gt 0) { 'Green' } else { 'DarkGray' })
                    } elseif ($line -and $line -notmatch '^\s*$') {
                        Write-Host "  │    ! $line" -ForegroundColor DarkGray
                    }
                }
            } elseif (-not $rptServer) {
                Write-Host "  │    SQL endpoint not ready" -ForegroundColor DarkGray
            } else {
                Write-Host "  │    ⚠ No SQL token — skipping row counts" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  │  Reporting Lakehouse not yet created" -ForegroundColor DarkGray
        }

    } catch {
        Write-Host "  │  ⚠ Diagnostics query failed: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    Write-Host "  │" -ForegroundColor DarkYellow
    Write-Host "  └── END DIAGNOSTICS ──" -ForegroundColor DarkYellow
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
} elseif ($Phase2) {
    Write-Host "  MODE: Phase 2 (RTI Phase 2 + HDS Pipelines + Data Agents)" -ForegroundColor Yellow
} elseif ($Phase3) {
    Write-Host "  MODE: Phase 3 only (Cohorting Agent + DICOM Viewer + Imaging Report)" -ForegroundColor Magenta
} elseif ($Phase4) {
    Write-Host "  MODE: Phase 4 only (Ontology + Agent binding + Data Activator)" -ForegroundColor Blue
} elseif ($Phase5) {
    Write-Host "  MODE: Phase 5 only (CMS Quality & Claims)" -ForegroundColor DarkYellow
} elseif ($Phase7) {
    Write-Host "  MODE: Phase 7 only (Payer RTI & Ops)" -ForegroundColor Cyan
} else {
    $skips = @()
    if ($SkipBaseInfra) { $skips += "Base Infra" }
    if ($SkipFhir) { $skips += "FHIR/Synthea" }
    if ($SkipDicom) { $skips += "DICOM" }
    if ($SkipFabric) { $skips += "Fabric" }
    if ($SkipPhase7) { $skips += "Payer RTI & Ops" }
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

    Write-Summary -PhaseName "Teardown"
    Pop-Location
    exit 0
}

# ============================================================================
# PHASE 2 ONLY MODE
# ============================================================================

if ($Phase2) {
    Emit-PhaseTransition -Phase 2 -Label "Active Patient Telemetry" -StepCount 1

    if (-not $SkipRtiPhase2) {
        Invoke-Step -StepName "Phase 2: Fabric RTI Enrichment" `
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

            & "$ScriptDir/deploy-fabric-rti.ps1" @phase2Args
            if ($LASTEXITCODE -ne 0) { throw "deploy-fabric-rti.ps1 -Phase2 failed with exit code $LASTEXITCODE" }
        }
    } # end if (-not $SkipRtiPhase2)

    # DICOM shortcut + HDS pipelines (clinical, imaging, OMOP, optional CMA).
    # Do not gate this on -SkipDicom: resumed runs skip the DICOM loader after it succeeds,
    # but still need to create shortcuts and trigger HDS pipelines if that phase failed.
    if (-not $SkipHdsPipelines) {
        Emit-PhaseTransition -Phase 3 -Label "HDS Bridge + Row Gates" -StepCount 1
        Invoke-Step -StepName "Phase 3: DICOM Shortcut + HDS Pipelines" `
            -Description "Shortcut for DICOM data, then run clinical, imaging, OMOP, and optional CMA pipelines" -Action {
            $hdsPipelineArgs = @{
                FabricWorkspaceName = $FabricWorkspaceName
                ResourceGroupName   = $ResourceGroupName
            }
            if ($RequireBronzeClinicalFhir) { $hdsPipelineArgs['RequireClinicalFhirData'] = $true }
            if ($RequireBronzeImagingDicom) { $hdsPipelineArgs['RequireImagingDicomData'] = $true }
            $global:LASTEXITCODE = 0
            & "$ScriptDir/phase-2/storage-access-trusted-workspace.ps1" @hdsPipelineArgs
            Assert-LastExternalCommandSucceeded "storage-access-trusted-workspace.ps1"
        }
    }

    # Data Agents depend on ClinicalDeviceOntology. Do not deploy unbound agents in Phase 2-only mode.
    if (-not $SkipDataAgents) {
        if (-not $SkipOntology) {
            Write-Host "  ⚠ Skipping Data Agents in -Phase2 mode because ClinicalDeviceOntology is not deployed until Phase 4." -ForegroundColor Yellow
            Write-Host "    Run -Phase4 next, or use a full deployment, to deploy ontology-aware agents." -ForegroundColor DarkGray
        } else {
            Invoke-Step -StepName "Phase 2: Data Agents" `
                -Description "Deploy Patient 360 + Clinical Triage agents (ontology explicitly skipped)" -Action {
                Write-Host "  ⚠ Deploying Data Agents without ontology because -SkipOntology is set" -ForegroundColor Yellow
                Write-Host "  This step will:" -ForegroundColor White
                Write-Host "    [1/2] Create/update Patient 360 Data Agent" -ForegroundColor DarkGray
                Write-Host "    [2/2] Create/update Clinical Triage Data Agent" -ForegroundColor DarkGray
                Write-Host "  Architecture: KQL (TelemetryRaw + AlertHistory) + Lakehouse (Silver tables)" -ForegroundColor DarkGray
                Write-Host ""

                $global:LASTEXITCODE = 0
                & "$ScriptDir\phase-2\deploy-data-agents.ps1" `
                    -FabricWorkspaceName $FabricWorkspaceName `
                    -IncludeDicomImaging:((-not $SkipImaging) -and (-not $SkipDicom))
                Assert-LastExternalCommandSucceeded "deploy-data-agents.ps1"
            }
        }
    } # end if (-not $SkipDataAgents)

    Write-Summary -Title "PHASE 2 DEPLOYMENT SUMMARY" -PhaseName "Phase2" -PhaseResources @{
        FabricWorkspaceName = $FabricWorkspaceName
        ResourceGroupName   = $ResourceGroupName
    }
    Pop-Location
    exit 0
}

# ============================================================================
# PHASE 3 STANDALONE — Skip steps 1-6, run only imaging toolkit
# ============================================================================

if ($Phase3 -and -not $Phase2) {
    Write-Host "  >>  Skipping Phase 1 (infrastructure + ingestion)  (-Phase3)" -ForegroundColor DarkGray
    Write-Host "  >>  Skipping Phase 2 (RTI Phase 2 + Data Agents)  (-Phase3)" -ForegroundColor DarkGray
}

# ============================================================================
# PHASE 4 STANDALONE — Skip steps 1-7, run only ontology + activator
# ============================================================================

if ($Phase4 -and -not $Phase2 -and -not $Phase3) {
    Write-Host "  >>  Skipping Phase 1 (infrastructure + ingestion)  (-Phase4)" -ForegroundColor DarkGray
    Write-Host "  >>  Skipping Phase 2 (RTI Phase 2 + Data Agents)  (-Phase4)" -ForegroundColor DarkGray
    Write-Host "  >>  Skipping Phase 3 (imaging toolkit)             (-Phase4)" -ForegroundColor DarkGray
}

if ($Phase5 -and -not $Phase2 -and -not $Phase3 -and -not $Phase4) {
    Write-Host "  >>  Skipping Phase 1-5 (-Phase5); running CMS Quality & Claims only" -ForegroundColor DarkGray
}

if ($Phase7 -and -not $Phase2 -and -not $Phase3 -and -not $Phase4 -and -not $Phase5) {
    Write-Host "  >>  Skipping Phase 1-6 (-Phase7)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 1 — FABRIC WORKSPACE + IDENTITY (created first so HDS can be deployed during Azure steps)
# ============================================================================

if (-not $Phase5 -and -not $Phase7) { Emit-PhaseTransition -Phase 1 -Label "Data Fabric Foundation" -StepCount 4 }

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7) {
    Invoke-Step -StepName "Phase 1: Fabric Workspace" `
        -Description "Create workspace '$FabricWorkspaceName' + assign capacity + provision identity" -Action {

        function Get-FabricToken {
            $t = Get-CachedAccessToken "https://api.fabric.microsoft.com"
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
                    description = "Healthcare intelligence workspace for connected clinical devices, FHIR/HDS clinical foundations, DICOM imaging cohorts, Fabric IQ ontologies, CMS quality, claims analytics, payer operations, and real-time telemetry."
                } | ConvertTo-Json)
            $script:fabricWorkspaceId = $newWs.id
            Write-Host "  ✓ Workspace created: $FabricWorkspaceName ($($script:fabricWorkspaceId))" -ForegroundColor Green
        }

        # Ensure the workspace is assigned to the selected paid Fabric capacity.
        $selectedCapacitySpecified = -not [string]::IsNullOrWhiteSpace($CapacityName)
        $wsDetail = Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)" -Headers $fabHeaders
        $targetCap = $null

        if ($selectedCapacitySpecified -or -not $wsDetail.capacityId) {
            if ($selectedCapacitySpecified) {
                Write-Host "  Resolving selected Fabric capacity '$CapacityName'..." -ForegroundColor White
            } else {
                Write-Host "  Searching for an active paid Fabric capacity..." -ForegroundColor Yellow
            }

            $caps = Invoke-RestMethod -Uri "$fabBase/capacities" -Headers $fabHeaders
            $activePaidCaps = $caps.value | Where-Object {
                $_.state -eq "Active" -and $_.sku -like "F*" -and $_.sku -notlike "FT*" -and $_.sku -ne "PP3"
            }

            if ($selectedCapacitySpecified) {
                $targetCap = $activePaidCaps | Where-Object {
                    $_.displayName -eq $CapacityName -or $_.name -eq $CapacityName
                } | Select-Object -First 1
                if (-not $targetCap) {
                    $selectedCapacityRef = $CapacityName
                    if (-not [string]::IsNullOrWhiteSpace($CapacityResourceGroup)) { $selectedCapacityRef = "$CapacityResourceGroup/$selectedCapacityRef" }
                    if (-not [string]::IsNullOrWhiteSpace($CapacitySubscriptionId)) { $selectedCapacityRef = "$CapacitySubscriptionId/$selectedCapacityRef" }
                    throw "Selected Fabric capacity '$selectedCapacityRef' was not found as an active paid F-SKU capacity. Select a running paid F-SKU capacity; trial FT capacities are not supported."
                }
            } else {
                $targetCap = $activePaidCaps | Select-Object -First 1
                if (-not $targetCap) {
                    throw "No active paid Fabric F-SKU capacity found. Provision or resume a paid F-SKU (F2+) at https://portal.azure.com; trial FT capacities are not supported."
                }
            }
        }

        if ($selectedCapacitySpecified) {
            if ($wsDetail.capacityId -ne $targetCap.id) {
                $capacityVerb = if ($wsDetail.capacityId) { "Reassigning" } else { "Assigning" }
                Write-Host "  $capacityVerb workspace to selected capacity: $($targetCap.displayName) (SKU: $($targetCap.sku))..." -ForegroundColor White
                Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)/assignToCapacity" `
                    -Headers $fabHeaders -Method POST `
                    -Body (@{ capacityId = $targetCap.id } | ConvertTo-Json) | Out-Null
                Start-Sleep -Seconds 5
                Write-Host "  ✓ Selected capacity assigned" -ForegroundColor Green
            } else {
                Write-Host "  ✓ Workspace already assigned to selected capacity: $CapacityName" -ForegroundColor Green
            }
        } elseif (-not $wsDetail.capacityId) {
            Write-Host "  Assigning capacity: $($targetCap.displayName) (SKU: $($targetCap.sku))..." -ForegroundColor White
            Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)/assignToCapacity" `
                -Headers $fabHeaders -Method POST `
                -Body (@{ capacityId = $targetCap.id } | ConvertTo-Json) | Out-Null
            Start-Sleep -Seconds 5
            Write-Host "  ✓ Capacity assigned" -ForegroundColor Green
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
# STEP 1b — BASE AZURE INFRASTRUCTURE
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $SkipBaseInfra) {
    $deployArgs = @{
        ResourceGroupName   = $ResourceGroupName
        Location            = $Location
        AdminSecurityGroup  = $AdminSecurityGroup
        Tags                = $Tags
        SpnClientId         = $SpnClientId
        SpnClientSecret     = $SpnClientSecret
        SpnTenantId         = $SpnTenantId
        FabricWorkspaceName = $FabricWorkspaceName
    }
    if ($SkipFabric) { $deployArgs['SkipTelemetry'] = $true }

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
            $global:LASTEXITCODE = 0
            & "$ScriptDir\phase-1\deploy.ps1" @deployArgs
            Assert-LastExternalCommandSucceeded "phase-1 deploy.ps1"
        }

        Write-Host ""
        $script:stepResults += @{
            Name     = "Phase 1: Base Azure Infrastructure"
            Success  = $true
            Duration = "skipped"
            Detail   = "Already deployed (ACR: $existingAcr)"
        }
    } else {
        $stepDesc = if ($SkipFabric) { "ACR, Key Vault (deploy.ps1)" } else { "Event Hub, ACR, emulator container (deploy.ps1)" }
        Invoke-Step -StepName "Phase 1: Base Azure Infrastructure" `
            -Description $stepDesc -Action {
            if ($SkipFabric) {
                Write-Host "  [1/2] Creating resource group '$ResourceGroupName'..." -ForegroundColor White
                Write-Host "  [2/2] Deploying ACR, Key Vault (bicep/infra.bicep)..." -ForegroundColor White
            } else {
                Write-Host "  [1/4] Creating resource group '$ResourceGroupName'..." -ForegroundColor White
                Write-Host "  [2/4] Deploying Event Hub, ACR, Key Vault (bicep/infra.bicep)..." -ForegroundColor White
                Write-Host "  [3/4] Building emulator container image in ACR..." -ForegroundColor White
                Write-Host "  [4/4] Deploying emulator ACI container (bicep/emulator.bicep)..." -ForegroundColor White
            }
            Write-Host ""
            $global:LASTEXITCODE = 0
            & "$ScriptDir\phase-1\deploy.ps1" @deployArgs
            Assert-LastExternalCommandSucceeded "phase-1 deploy.ps1"
        }
    }
} else {
    Write-Host "  >>  Skipping base infrastructure (--SkipBaseInfra)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2 — FHIR SERVICE + SYNTHEA + LOADER (MODULAR)
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and (-not $SkipFhir -or -not $SkipDicom)) {
    if ($ReusePatients -and -not $SourceResourceGroup -and -not $SkipFhir) {
        Write-Host "  >>  Reusing existing patients — skipping Synthea + FHIR Loader" -ForegroundColor Yellow
        Write-Host "      Existing patient/device data in FHIR will be preserved." -ForegroundColor DarkGray
    } else {
        $stepName = if ($SkipFhir) { "Phase 1: Shared HDS Infrastructure" } else { "Phase 1: FHIR Service + Synthea + Loader" }
        $stepDesc = if ($SkipFhir) { "Deploy shared HDS workspace and storage" } else { "$PatientCount patients -> FHIR (deploy-fhir.ps1)" }

        Invoke-Step -StepName $stepName `
            -Description $stepDesc -Action {
            Write-Host "  This step will:" -ForegroundColor White
            if ($SkipFhir) {
                Write-Host "    [1/2] Deploy shared HDS workspace and ADLS storage account (FHIR Service bypassed)" -ForegroundColor DarkGray
                Write-Host "    [2/2] Skip Synthea & FHIR Loader (FHIR is unselected)" -ForegroundColor DarkGray
            } else {
                Write-Host "    [1/5] Deploy FHIR infrastructure (HDS workspace, FHIR R4, storage, UAMI)" -ForegroundColor DarkGray
                Write-Host "    [2/5] Build Synthea + Loader container images in ACR" -ForegroundColor DarkGray
                Write-Host "    [3/5] Run Synthea to generate $PatientCount synthetic patients" -ForegroundColor DarkGray
                Write-Host "    [4/5] Upload FHIR bundles, providers, and devices" -ForegroundColor DarkGray
                Write-Host "    [5/5] Create device associations for qualifying patients" -ForegroundColor DarkGray
            }
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
            if ($SkipFhir) { $fhirArgs['SkipFhir'] = $true }
            if ($RebuildContainers) { $fhirArgs['RebuildContainers'] = $true }
            if ($UseCachedSynthea) { $fhirArgs['UseCachedSynthea'] = $true }
            if ($Tags.Count -gt 0) { $fhirArgs['Tags'] = $Tags }
            if ($SourceResourceGroup) { $fhirArgs['SourceResourceGroup'] = $SourceResourceGroup }
            if ($ReusePatients) { $fhirArgs['ReusePatients'] = $true }

            & "$ScriptDir\phase-1\deploy-fhir.ps1" @fhirArgs
            if ($LASTEXITCODE -ne 0) { throw "deploy-fhir.ps1 failed with exit code $LASTEXITCODE" }
        }
    }
} else {
    Write-Host "  >>  Skipping FHIR / Synthea (--SkipFhir and DICOM also skipped)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2b — DICOM LOADER
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $SkipDicom) {
    Invoke-Step -StepName "Phase 1: DICOM Loader" `
        -Description "TCIA download, re-tag, ADLS upload, FHIR ImagingStudy creation (deploy-fhir.ps1 -RunDicom)" -Action {
        Write-Host "  This step will:" -ForegroundColor White
        Write-Host "    [1/2] Build DICOM Loader container image in ACR" -ForegroundColor DarkGray
        Write-Host "    [2/2] Run DICOM Loader (TCIA download, re-tag, ADLS upload, FHIR ImagingStudy)" -ForegroundColor DarkGray

        Write-Host ""

        $dicomArgs = @{
            ResourceGroupName  = $ResourceGroupName
            Location           = $Location
            AdminSecurityGroup = $AdminSecurityGroup
            RunDicom           = $true
        }
        if ($SkipFhir) { $dicomArgs['SkipFhir'] = $true }
        if ($RebuildContainers) { $dicomArgs['RebuildContainers'] = $true }
        if ($Tags.Count -gt 0) { $dicomArgs['Tags'] = $Tags }
        if ($ReusePatients) { $dicomArgs['ReusePatients'] = $true }
        if ($SourceResourceGroup) { $dicomArgs['SourceResourceGroup'] = $SourceResourceGroup }

        & "$ScriptDir\phase-1\deploy-fhir.ps1" @dicomArgs
        if ($LASTEXITCODE -ne 0) { throw "deploy-fhir.ps1 -RunDicom failed with exit code $LASTEXITCODE" }
    }
} else {
    Write-Host "  >>  Skipping DICOM (--SkipDicom)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2c — FHIR $EXPORT (ensure data exists for HDS pipelines)
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $SkipFhir -and $ReusePatients) {
    Invoke-Step -StepName "Phase 1: FHIR `$export (catch-up)" `
        -Description "Export existing FHIR data to ADLS Gen2 for HDS pipelines" -Action {
        Write-Host "  Ensuring FHIR `$export data exists for downstream HDS ingestion." -ForegroundColor White
        Write-Host "  (Synthea/Loader were skipped — but HDS needs the export files.)" -ForegroundColor DarkGray
        Write-Host ""

        $exportArgs = @{
            ResourceGroupName  = $ResourceGroupName
            Location           = $Location
            AdminSecurityGroup = $AdminSecurityGroup
            InfraOnly          = $true
        }
        if ($Tags.Count -gt 0) { $exportArgs['Tags'] = $Tags }

        # InfraOnly will verify infra then exit, but deploy-fhir.ps1 now has
        # the Step 8 catch-up $export that fires regardless of mode.
        # Instead, invoke deploy-fhir.ps1 in a minimal mode that only triggers export.
        & "$ScriptDir\phase-1\deploy-fhir.ps1" @exportArgs

        # The InfraOnly mode exits before Step 8. Call $export directly.
        # Find FHIR URL from existing deployment
        $fhirResource = az resource list -g $ResourceGroupName `
            --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
            --query "[0].name" -o tsv 2>`$null
        if ($fhirResource) {
            $parts = $fhirResource -split "/"
            if ($parts.Count -eq 2) {
                $fhirUrl = "https://$($parts[0])-$($parts[1]).fhir.azurehealthcareapis.com"
                # Check if export data already exists
                $stAcct = az storage account list -g $ResourceGroupName `
                    --query "[?kind=='StorageV2'].name | [0]" -o tsv 2>`$null
                $hasExport = $false
                if ($stAcct) {
                    $existingBlob = az storage blob list --container-name "fhir-export" `
                        --account-name $stAcct --auth-mode login --num-results 1 `
                        --query "[0].name" -o tsv 2>`$null
                    if ($existingBlob) { $hasExport = $true }
                }
                if (-not $hasExport) {
                    Write-Host "  No FHIR export data found — triggering `$export now..." -ForegroundColor Yellow
                    # Source the Invoke-FhirExport function from deploy-fhir.ps1 is not available here,
                    # so use the Fabric RTI script's export via a direct API call approach
                    Write-Host "  FHIR URL: $fhirUrl" -ForegroundColor DarkGray
                    $fhirToken = az account get-access-token --resource $fhirUrl --query accessToken -o tsv 2>`$null
                    if ($fhirToken) {
                        # Ensure export container exists
                        if ($stAcct) {
                            az storage container create --name "fhir-export" --account-name $stAcct --auth-mode login 2>`$null | Out-Null
                        }
                        # Configure export destination
                        $fhirResId = az resource list -g $ResourceGroupName `
                            --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
                            --query "[0].id" -o tsv 2>`$null
                        if ($fhirResId -and $stAcct) {
                            az rest --method patch --url "$fhirResId`?api-version=2023-11-01" `
                                --body "{`"properties`":{`"exportConfiguration`":{`"storageAccountName`":`"$stAcct`"}}}" 2>`$null | Out-Null
                            # Ensure RBAC
                            $fhirMi = az resource show --ids $fhirResId --query "identity.principalId" -o tsv 2>`$null
                            $stId = az storage account show -n $stAcct -g $ResourceGroupName --query id -o tsv 2>`$null
                            if ($fhirMi -and $stId) {
                                az role assignment create --assignee-object-id $fhirMi --assignee-principal-type ServicePrincipal `
                                    --role "ba92f5b4-2d11-453d-a403-e96b0029c9fe" --scope $stId 2>`$null | Out-Null
                            }
                        }
                        # Trigger export
                        try {
                            $exportResp = Invoke-WebRequest `
                                -Uri "$fhirUrl/`$export?_container=fhir-export" `
                                -Headers @{ Authorization = "Bearer $fhirToken"; Accept = "application/fhir+json"; Prefer = "respond-async" } `
                                -Method GET -UseBasicParsing
                            if ($exportResp.StatusCode -eq 202) {
                                $statusUrl = $exportResp.Headers["Content-Location"]
                                if ($statusUrl -is [array]) { $statusUrl = $statusUrl[0] }
                                Write-Host "  ✓ FHIR `$export started" -ForegroundColor Green
                                Write-Host "    Polling for completion..." -ForegroundColor DarkGray
                                $pollStart = Get-Date
                                while ((New-TimeSpan -Start $pollStart).TotalMinutes -lt 30) {
                                    Start-Sleep -Seconds 15
                                    $elapsed = [math]::Round((New-TimeSpan -Start $pollStart).TotalMinutes, 1)
                                    if ([math]::Floor($elapsed) % 5 -eq 0 -and $elapsed -gt 0) {
                                        $fhirToken = az account get-access-token --resource $fhirUrl --query accessToken -o tsv 2>`$null
                                    }
                                    try {
                                        $pollResp = Invoke-WebRequest -Uri $statusUrl `
                                            -Headers @{ Authorization = "Bearer $fhirToken" } -UseBasicParsing
                                        if ($pollResp.StatusCode -eq 200) {
                                            $exportResult = $pollResp.Content | ConvertFrom-Json
                                            $fileCount = ($exportResult.output | Measure-Object).Count
                                            Write-Host "  ✓ FHIR `$export complete — $fileCount files" -ForegroundColor Green
                                            break
                                        }
                                    } catch {
                                        $sc = $null; try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
                                        if ($sc -eq 202) { Write-Host "    Exporting... (${elapsed}m)" -ForegroundColor DarkGray }
                                    }
                                }
                            }
                        } catch {
                            $sc = $null; try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
                            if ($sc -eq 409) { Write-Host "  ⚠ Export already running" -ForegroundColor Yellow }
                            else { Write-Host "  ⚠ Export trigger failed: $($_.Exception.Message)" -ForegroundColor Yellow }
                        }
                    }
                } else {
                    Write-Host "  ✓ FHIR export data already exists — no action needed" -ForegroundColor Green
                }
            }
        }
    }
}

# ============================================================================
# STEP 3 — FABRIC RTI PHASE 1
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $SkipFabric) {
    Emit-PhaseTransition -Phase 2 -Label "Active Patient Telemetry" -StepCount 2
}

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $SkipFabric) {
    Invoke-Step -StepName "Phase 2: Fabric RTI" `
        -Description "Eventhouse, KQL DB, Eventstream, FHIR export, and core telemetry dashboard" -Action {
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

        & "$ScriptDir/deploy-fabric-rti.ps1" @fabricArgs
        if ($LASTEXITCODE -ne 0) { throw "deploy-fabric-rti.ps1 failed with exit code $LASTEXITCODE" }
    }
} else {
    Write-Host "  >>  Skipping Fabric RTI (--SkipFabric)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 4 — HDS GUIDANCE (manual Fabric portal step)
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and -not $SkipHdsPipelines) {
    $script:stepNumber++
    Write-Banner -Text "STEP $($script:stepNumber): PHASE 3: HDS DEPLOYMENT DETECTION (MANUAL)" -Color Yellow
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
    Write-Host "    [5] Wait for the modalities to finish deploying, then resume Phase 2" -ForegroundColor DarkGray
    Write-Host ""

    # Build the resume check loop
    $instanceId = $env:ORCHESTRATOR_INSTANCE_ID
    $resumeFile = ""
    if ($instanceId) {
        $resumeFile = Join-Path $ScriptDir "orchestrator/logs/$instanceId.resume"
    }

    Write-Host "[WAITING_FOR_HDS] Waiting for Healthcare Data Solutions (HDS) deployment. Please deploy it in the Fabric portal, then click Continue in the UI." -ForegroundColor Yellow
    if ($resumeFile) {
        Write-Host "  (Alternatively, you can touch/create the file: $resumeFile to resume)" -ForegroundColor DarkGray
    }

    $hdsDetected = $false
    while ($true) {
        # Check if Bronze and Silver lakehouses exist in the workspace
        $hasBronze = $null
        $hasSilver = $null
        try {
            $autoToken = Get-CachedAccessToken "https://api.fabric.microsoft.com"
            if ($autoToken -is [System.Security.SecureString]) {
                $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($autoToken)
                try { $autoToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
                finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
            }
            $autoHeaders = @{ Authorization = "Bearer $autoToken"; "Content-Type" = "application/json" }
            $autoWs = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $autoHeaders).value |
                Where-Object { $_.displayName -eq $FabricWorkspaceName }
            if ($autoWs) {
                $autoItems = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($autoWs.id)/items" -Headers $autoHeaders).value
                $hasBronze = $autoItems | Where-Object { $_.displayName -match 'bronze' -and $_.type -eq 'Lakehouse' }
                $hasSilver = $autoItems | Where-Object { $_.displayName -match 'silver' -and $_.type -eq 'Lakehouse' }
            }
        } catch {
            # Silently ignore token/API errors during polling
        }

        if ($hasBronze -and $hasSilver) {
            Write-Host ""
            Write-Host "  [AUTO-DETECT] Healthcare Data Solutions detected! Proceeding to Phase 2..." -ForegroundColor Green
            $hdsDetected = $true
            break
        }

        # Check if the UI resume flag file exists
        if ($resumeFile -and (Test-Path $resumeFile)) {
            Write-Host ""
            Write-Host "  [UI-RESUME] Continue button clicked in the UI. Proceeding to Phase 2..." -ForegroundColor Green
            $hdsDetected = $true
            break
        }

        Start-Sleep -Seconds 10
    }

    # Clean up the resume file if it exists
    if ($resumeFile -and (Test-Path $resumeFile)) {
        Remove-Item $resumeFile -ErrorAction SilentlyContinue
    }

    if ($hdsDetected) {
        # HDS creates a large number of Fabric notebooks at workspace root.
        # Keep the workspace tidy before continuing with automated pipeline runs.
        Move-FabricNotebooksToFolder -FabricWorkspaceName $FabricWorkspaceName

        # Run Phase 2 inline
        $Phase2 = $true

        Emit-PhaseTransition -Phase 2 -Label "Active Patient Telemetry" -StepCount 1

        if (-not $SkipRtiPhase2) {
            Invoke-Step -StepName "Phase 2: Fabric RTI Enrichment (auto)" `
                -Description "Bronze shortcut, clinical pipeline, KQL shortcuts, enriched alerts" -Action {
                $phase2Args = @{
                    Phase2              = $true
                    FabricWorkspaceName = $FabricWorkspaceName
                    ResourceGroupName   = $ResourceGroupName
                    Location            = $Location
                }
                if ($Tags.Count -gt 0) { $phase2Args['Tags'] = $Tags }
                & "$ScriptDir/deploy-fabric-rti.ps1" @phase2Args
                if ($LASTEXITCODE -ne 0) { throw "deploy-fabric-rti.ps1 -Phase2 failed with exit code $LASTEXITCODE" }
            }
        }

        if (-not $SkipHdsPipelines) {
            Emit-PhaseTransition -Phase 3 -Label "HDS Bridge + Row Gates" -StepCount 1
            Invoke-Step -StepName "Phase 3: DICOM Shortcut + HDS Pipelines (auto)" `
                -Description "Shortcut for DICOM data, then run clinical, imaging, OMOP, and optional CMA pipelines" -Action {
                $hdsPipelineArgs = @{
                    FabricWorkspaceName = $FabricWorkspaceName
                    ResourceGroupName   = $ResourceGroupName
                }
                if ($RequireBronzeClinicalFhir) { $hdsPipelineArgs['RequireClinicalFhirData'] = $true }
                if ($RequireBronzeImagingDicom) { $hdsPipelineArgs['RequireImagingDicomData'] = $true }
                $global:LASTEXITCODE = 0
                & "$ScriptDir/phase-2/storage-access-trusted-workspace.ps1" @hdsPipelineArgs
                Assert-LastExternalCommandSucceeded "storage-access-trusted-workspace.ps1"
                Move-FabricNotebooksToFolder -FabricWorkspaceName $FabricWorkspaceName

                # Stage OMOP Academic Research Dashboard
                $omopReportDir = Join-Path $ScriptDir "phase-2\omop-research-report"
                if (Test-Path $omopReportDir) {
                    Write-Host ""
                    Write-Host "  --- OMOP Academic Research Dashboard ---" -ForegroundColor Cyan
                    Write-Host "  ✓ OMOP Academic Research Dashboard artifacts staged for deployment" -ForegroundColor Green
                    Write-Host "    (4 pages: Cohort Feasibility & Attrition, Clinical Journeys & Pathways," -ForegroundColor DarkGray
                    Write-Host "     Comorbidity & Baseline Characteristics, Measurement Density & Outliers)" -ForegroundColor DarkGray
                }
            }
        }

        $script:stepResults += @{
            Name     = "Phase 3: HDS Deployment Detection"
            Success  = $true
            Duration = "—"
            Detail   = "Auto-resumed: HDS deployed successfully"
        }
    }
}

# Continuation runs can skip the already-completed HDS pipeline step. They still
# need RTI Phase 2 enrichment: otherwise a resume with -SkipHdsPipelines refreshes
# only core Eventhouse/Eventstream assets and then downstream phases run against
# stale/missing Silver shortcuts and enriched alert functions.
if (-not $Phase3 -and -not $Phase4 -and -not $Phase5 -and -not $Phase7 -and $SkipHdsPipelines) {
    if (-not $SkipFabric -and -not $SkipRtiPhase2) {
        Emit-PhaseTransition -Phase 2 -Label "Active Patient Telemetry" -StepCount 1
        Invoke-Step -StepName "Phase 2: Fabric RTI Enrichment (resume)" `
            -Description "KQL shortcuts and enriched alerts; HDS pipelines already completed" -Action {
            $phase2Args = @{
                Phase2              = $true
                FabricWorkspaceName = $FabricWorkspaceName
                ResourceGroupName   = $ResourceGroupName
                Location            = $Location
            }
            if ($SilverLakehouseId) { $phase2Args['SilverLakehouseId'] = $SilverLakehouseId }
            if ($SilverLakehouseName) { $phase2Args['SilverLakehouseName'] = $SilverLakehouseName }
            if ($Tags.Count -gt 0) { $phase2Args['Tags'] = $Tags }

            & "$ScriptDir/deploy-fabric-rti.ps1" @phase2Args
            if ($LASTEXITCODE -ne 0) { throw "deploy-fabric-rti.ps1 -Phase2 failed with exit code $LASTEXITCODE" }
        }
    }
    $Phase2 = $true
}

# ============================================================================
# STEP 6 — DATA AGENTS (after Phase 2 + OMOP)
# ============================================================================

# Deploy Data Agents only after ontology exists. If ontology is enabled, Phase 4 deploys
# ClinicalDeviceOntology first and then creates/updates ontology-aware agents.
if ($Phase2 -and -not $SkipDataAgents) {
    if (-not $SkipOntology) {
        Write-Host "  >>  Deferring Data Agents until Phase 4 ontology is deployed" -ForegroundColor DarkGray
    } else {
        Invoke-Step -StepName "Phase 2: Data Agents" `
            -Description "Deploy Patient 360 + Clinical Triage agents (ontology explicitly skipped)" -Action {
            Write-Host "  ⚠ Deploying Data Agents without ontology because -SkipOntology is set" -ForegroundColor Yellow
            Write-Host "  This step will:" -ForegroundColor White
            Write-Host "    [1/2] Create/update Patient 360 Data Agent" -ForegroundColor DarkGray
            Write-Host "    [2/2] Create/update Clinical Triage Data Agent" -ForegroundColor DarkGray
            Write-Host "  Architecture: KQL (TelemetryRaw + AlertHistory) + Lakehouse (Silver tables)" -ForegroundColor DarkGray
            Write-Host ""

            $global:LASTEXITCODE = 0
            & "$ScriptDir\phase-2\deploy-data-agents.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName `
                -IncludeDicomImaging:((-not $SkipImaging) -and (-not $SkipDicom))
            Assert-LastExternalCommandSucceeded "deploy-data-agents.ps1"
        }
    }
}

# ============================================================================
# STEP 7 — PHASE 3: COHORTING AGENT + DICOM VIEWER (FabricDicomCohortingToolkit)
# Requires: Gold OMOP pipeline completed, Silver + Gold lakehouses populated
# ============================================================================

if (-not $Phase5 -and -not $Phase7) { Emit-PhaseTransition -Phase 3 -Label "Multimodal Cohorting & Imaging" -StepCount 1 }

if (($Phase2 -or $Phase3) -and -not $SkipImaging) {
    # Phase 3 preflight: verify Gold OMOP lakehouse has data
    $runPhase3 = $true

    if ($Phase3 -or $Phase2) {
        Invoke-Step -StepName "Phase 3: Imaging & Reporting" `
            -Description "Cohorting Agent + DICOM Viewer (FabricDicomCohortingToolkit)" -Action {

            # Validate or bootstrap toolkit path
            $toolkit = Ensure-DicomToolkitRepo -Path $DicomToolkitPath
            if (-not $toolkit.Success) {
                throw $toolkit.Message
            }

            Write-Host "  ┌──────────────────────────────────────────────────────────────┐" -ForegroundColor Magenta
            Write-Host "  │  PHASE 3: FabricDicomCohortingToolkit Deployment            │" -ForegroundColor Magenta
            Write-Host "  └──────────────────────────────────────────────────────────────┘" -ForegroundColor Magenta
            Write-Host ""

            # Preflight: Check Gold OMOP lakehouse has data
            Write-Host "  --- PREFLIGHT: Gold OMOP Lakehouse Check ---" -ForegroundColor Cyan
            try {
                function Get-FabricTokenLocal {
                    $t = Get-CachedAccessToken "https://api.fabric.microsoft.com"
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

                function Invoke-P3FabricRest {
                    param(
                        [Parameter(Mandatory)][string]$Uri,
                        [string]$Method = 'GET',
                        [object]$Body = $null,
                        [string]$Label = 'Fabric request'
                    )
                    for ($attempt = 1; $attempt -le 8; $attempt++) {
                        try {
                            $p3Token = Get-FabricTokenLocal
                            $headers = @{ Authorization = "Bearer $p3Token"; "Content-Type" = "application/json" }
                            if ($Body -ne $null) {
                                return Invoke-RestMethod -Uri $Uri -Headers $headers -Method $Method -Body $Body -ErrorAction Stop
                            }
                            return Invoke-RestMethod -Uri $Uri -Headers $headers -Method $Method -ErrorAction Stop
                        } catch {
                            $statusCode = $null
                            try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
                            $errBody = $_.ErrorDetails.Message
                            $isTransient = $statusCode -in @(403, 429, 500, 502, 503, 504) -and ($errBody -match 'RequestDeniedByInboundPolicy|Forbidden|Too Many Requests|Internal|Unavailable|Gateway' -or $statusCode -ne 403)
                            if ($isTransient -and $attempt -lt 8) {
                                $delay = [Math]::Min(15 * $attempt, 90)
                                Write-Host "  $Label transient HTTP ${statusCode}; retrying in ${delay}s... ($attempt/8)" -ForegroundColor Yellow
                                if ($errBody) { Write-Host "    $errBody" -ForegroundColor DarkGray }
                                Start-Sleep -Seconds $delay
                                continue
                            }
                            throw
                        }
                    }
                }

                $p3Ws = (Invoke-P3FabricRest -Uri "$p3Base/workspaces" -Label 'List workspaces').value |
                    Where-Object { $_.displayName -eq $FabricWorkspaceName }
                $p3WsId = $p3Ws.id

                $p3Items = (Invoke-P3FabricRest -Uri "$p3Base/workspaces/$p3WsId/items?type=Lakehouse" -Label 'List Phase 3 lakehouses').value
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
            Write-Host "  --- Step 7a: Cohorting Data Agent ---" -ForegroundColor Cyan
            Write-Host "  Deploying HDS Multi-Layer Imaging Cohort Agent..." -ForegroundColor White
            Write-Host "    Source: $DicomToolkitPath/Deploy-DataAgent.ps1" -ForegroundColor DarkGray
            Write-Host ""

            $global:LASTEXITCODE = 0
            & "$DicomToolkitPath/Deploy-DataAgent.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName `
                -WorkspaceId $p3WsId
            Assert-LastExternalCommandSucceeded "Phase 3 Deploy-DataAgent.ps1"

            Write-Host ""

            # ── DIAGNOSTIC CHECKPOINT: Before DICOM Viewer ──
            Write-Phase3Diagnostics -Checkpoint "PRE-VIEWER (7b)" -WorkspaceId $p3WsId

            # Step 3b: Deploy DICOM Viewer (Azure infra + OHIF)
            # Step 3b: Deploy DICOM Viewer FIRST (viewer URL needed by notebook)
            # Use the main Azure RG so all resources stay together
            $viewerRg = if ($DicomViewerResourceGroup -eq "rg-hds-dicom-viewer") { $ResourceGroupName } else { $DicomViewerResourceGroup }
            Write-Host "  --- Step 7b: DICOM Viewer ---" -ForegroundColor Cyan
            Write-Host "  Deploying OHIF Viewer + DICOMweb Proxy to Azure..." -ForegroundColor White
            Write-Host "    Resource Group: $viewerRg (shared with Phase 1)" -ForegroundColor DarkGray
            Write-Host ""

            $global:LASTEXITCODE = 0
            & "$DicomToolkitPath/dicom-viewer/Deploy-DicomViewer.ps1" `
                -ResourceGroup $viewerRg `
                -FabricWorkspaceName $FabricWorkspaceName `
                -Location $Location
            Assert-LastExternalCommandSucceeded "Deploy-DicomViewer.ps1"

            Write-Host ""

            # Resolve OHIF SWA URL once and pass it downstream to notebook + report deploy.
            $ohifViewerBaseUrl = ""
            try {
                $swaHost = az staticwebapp list -g $viewerRg --query "[0].defaultHostname" -o tsv 2>$null
                if ($swaHost) {
                    $ohifViewerBaseUrl = "https://$swaHost/viewer?StudyInstanceUIDs="
                    Write-Host "  ✓ OHIF Viewer URL: $ohifViewerBaseUrl" -ForegroundColor Green
                }
            } catch {
                Write-Host "  ⚠ Could not resolve OHIF SWA URL from Azure CLI" -ForegroundColor Yellow
            }

            # Step 3c: Create Reporting Lakehouse + Materialize Notebook
            Write-Host "  --- Step 7c: Reporting Tables ---" -ForegroundColor Cyan
            Write-Host "  Creating reporting lakehouse and running materialization notebook..." -ForegroundColor White

            # Create reporting lakehouse if it doesn't exist
            $existingLh = (Invoke-P3FabricRest -Uri "$p3Base/workspaces/$p3WsId/lakehouses" -Label 'List reporting lakehouses').value |
                Where-Object { $_.displayName -eq "healthcare1_reporting_gold" }
            if (-not $existingLh) {
                Write-Host "  Creating healthcare1_reporting_gold lakehouse..." -ForegroundColor White
                $lhBody = '{"displayName":"healthcare1_reporting_gold","type":"Lakehouse"}'
                Invoke-P3FabricRest -Uri "$p3Base/workspaces/$p3WsId/items" -Method Post -Body $lhBody -Label 'Create reporting lakehouse' | Out-Null
                Write-Host "  ✓ Reporting lakehouse created" -ForegroundColor Green
            } else {
                Write-Host "  ✓ Reporting lakehouse already exists" -ForegroundColor Green
            }

            # Deploy + run notebook (pass explicit OHIF URL when available)
            $nbArgs = @{
                FabricWorkspaceName      = $FabricWorkspaceName
                WorkspaceId               = $p3WsId
                DicomViewerResourceGroup = $viewerRg
            }
            if ($ohifViewerBaseUrl) { $nbArgs['OhifViewerBaseUrl'] = $ohifViewerBaseUrl }
            $global:LASTEXITCODE = 0
            & "$DicomToolkitPath/deploy-notebook.ps1" @nbArgs
            Assert-LastExternalCommandSucceeded "deploy-notebook.ps1"
            Move-FabricNotebooksToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $p3WsId
            Write-Host ""

            # ── DIAGNOSTIC CHECKPOINT: After notebook materialization ──
            Write-Phase3Diagnostics -Checkpoint "POST-NOTEBOOK (7c)" -WorkspaceId $p3WsId

            # Step 3d: Deploy Power BI Direct Lake Report
            Write-Host "  --- Step 7d: Power BI Imaging Report (Direct Lake) ---" -ForegroundColor Cyan
            $reportArgs = @{ FabricWorkspaceName = $FabricWorkspaceName }
            if ($ohifViewerBaseUrl) { $reportArgs['OhifViewerBaseUrl'] = $ohifViewerBaseUrl }
            $global:LASTEXITCODE = 0
            & "$DicomToolkitPath\Deploy-ImagingReport.ps1" @reportArgs
            Assert-LastExternalCommandSucceeded "Deploy-ImagingReport.ps1"
            Write-Host ""

            # ── DIAGNOSTIC CHECKPOINT: After PBI report, before final checks ──
            Write-Phase3Diagnostics -Checkpoint "POST-REPORT (7d)" -WorkspaceId $p3WsId

            # Step 3e: Add proxy MI to Fabric workspace + verify DICOM index
            Write-Host "  --- Step 7e: DICOM Viewer Permissions + Index ---" -ForegroundColor Cyan
            try {
                # Get proxy MI principal ID
                $proxyPrincipalId = az containerapp show -g $viewerRg -n "hds-dicom-proxy" `
                    --query "identity.principalId" -o tsv 2>$null
                if ($proxyPrincipalId) {
                    Write-Host "  Proxy MI: $proxyPrincipalId" -ForegroundColor DarkGray

                    # Add proxy MI as Contributor on Fabric workspace (for OneLake DFS reads)
                    # Check if already assigned
                    $existingRoles = (Invoke-P3FabricRest -Uri "$p3Base/workspaces/$p3WsId/roleAssignments" -Label 'List workspace role assignments').value
                    $alreadyAssigned = $existingRoles | Where-Object { $_.principal.id -eq $proxyPrincipalId }

                    if ($alreadyAssigned) {
                        Write-Host "  ✓ Proxy MI already has workspace access ($($alreadyAssigned.role))" -ForegroundColor Green
                    } else {
                        $roleBody = @{
                            principal = @{ id = $proxyPrincipalId; type = "ServicePrincipal" }
                            role = "Contributor"
                        } | ConvertTo-Json -Depth 3

                        Invoke-P3FabricRest -Uri "$p3Base/workspaces/$p3WsId/roleAssignments" `
                            -Method POST -Body $roleBody -Label 'Assign proxy workspace role' | Out-Null
                        Write-Host "  ✓ Added proxy MI as Contributor on workspace (required for OneLake DFS reads)" -ForegroundColor Green
                    }
                } else {
                    Write-Host "  ⚠ Could not find proxy MI — viewer may not have OneLake access" -ForegroundColor Yellow
                }

                # Check if DICOM index has studies
                $proxyFqdn = az containerapp show -g $viewerRg -n "hds-dicom-proxy" `
                    --query "properties.configuration.ingress.fqdn" -o tsv 2>$null
                if (-not $proxyFqdn) {
                    $viewerStateFile = Join-Path $DicomToolkitPath "dicom-viewer/state-tracking/.deployment-state.json"
                    if (Test-Path $viewerStateFile) {
                        try {
                            $viewerState = Get-Content $viewerStateFile -Raw | ConvertFrom-Json
                            if ($viewerState.proxyUrl) {
                                $proxyFqdn = ([uri]$viewerState.proxyUrl).Host
                                Write-Host "  Proxy FQDN recovered from deployment state: $proxyFqdn" -ForegroundColor DarkGray
                            }
                        } catch { }
                    }
                }
                if ($proxyFqdn) {
                    try {
                        $healthResp = Invoke-RestMethod -Uri "https://$proxyFqdn/health" -TimeoutSec 10
                        $studyCount = $healthResp.studies
                        Write-Host "  DICOM index: $studyCount studies" -ForegroundColor $(if ($studyCount -gt 0) { 'Green' } else { 'Yellow' })

                        if ($studyCount -eq 0) {
                            # Auto-rebuild: the index was built in 3b before data was fully available
                            Write-Host "  ⚠ DICOM index is empty — auto-rebuilding from current Silver data..." -ForegroundColor Yellow

                            try {
                                $global:LASTEXITCODE = 0
                                & "$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1" `
                                    -ResourceGroup $viewerRg `
                                    -FabricWorkspaceName $FabricWorkspaceName `
                                    -Location $Location -SkipOhifBuild -Force
                                Assert-LastExternalCommandSucceeded "Deploy-DicomViewer.ps1 -SkipOhifBuild -Force"

                                # Re-check after rebuild
                                Start-Sleep -Seconds 5
                                $healthResp2 = Invoke-RestMethod -Uri "https://$proxyFqdn/health" -TimeoutSec 10
                                $studyCount2 = $healthResp2.studies
                                if ($studyCount2 -gt 0) {
                                    Write-Host "  ✓ DICOM index rebuilt: $studyCount2 studies" -ForegroundColor Green
                                } else {
                                    Write-Host "  ⚠ Index still empty after rebuild. Manual rebuild:" -ForegroundColor Yellow
                                    Write-Host "    & `"$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1`" ``" -ForegroundColor Cyan
                                    Write-Host "        -ResourceGroup `"$viewerRg`" ``" -ForegroundColor Cyan
                                    Write-Host "        -FabricWorkspaceName `"$FabricWorkspaceName`" ``" -ForegroundColor Cyan
                                    Write-Host "        -Location `"$Location`" -SkipOhifBuild -Force" -ForegroundColor Cyan
                                    throw "DICOM index is still empty after rebuild"
                                }
                            } catch {
                                Write-Host "  ⚠ Auto-rebuild failed: $($_.Exception.Message)" -ForegroundColor Yellow
                                Write-Host "    Manual rebuild:" -ForegroundColor Yellow
                                Write-Host "    & `"$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1`" ``" -ForegroundColor Cyan
                                Write-Host "        -ResourceGroup `"$viewerRg`" ``" -ForegroundColor Cyan
                                Write-Host "        -FabricWorkspaceName `"$FabricWorkspaceName`" ``" -ForegroundColor Cyan
                                Write-Host "        -Location `"$Location`" -SkipOhifBuild -Force" -ForegroundColor Cyan
                                throw "DICOM index auto-rebuild failed: $($_.Exception.Message)"
                            }
                        }
                    } catch {
                        Write-Host "  ⚠ Could not reach proxy health endpoint" -ForegroundColor Yellow
                        throw "DICOM proxy health endpoint is not reachable: $($_.Exception.Message)"
                    }
                }
                else {
                    throw "DICOM proxy FQDN not found for hds-dicom-proxy"
                }
            } catch {
                Write-Host "  ⚠ Post-deploy check failed: $($_.Exception.Message)" -ForegroundColor Yellow
                throw
            }

            # ── DIAGNOSTIC CHECKPOINT: Final state after all Phase 3 steps ──
            Write-Phase3Diagnostics -Checkpoint "FINAL (Phase 3 complete)" -WorkspaceId $p3WsId
            Write-Host ""
        }
    }
}

# ============================================================================
# STEPS 8-9 — PHASE 4: ONTOLOGY + AGENT BINDING
# Requires: Silver Lakehouse populated, clinical pipeline completed,
#           Eventhouse with TelemetryRaw + AlertHistory tables
# ============================================================================

if (-not $Phase3 -and -not $Phase5 -and -not $Phase7) { Emit-PhaseTransition -Phase 4 -Label "Connected Semantic Intelligence" -StepCount 1 }

if (($Phase4 -or ($Phase2 -and -not $Phase3)) -and -not $SkipOntology) {
    Invoke-Step -StepName "Phase 4: Ontology" `
        -Description "Clinical pipeline check, ontology deployment, agent binding" -Action {

        function Get-FabricTokenLocal {
            $t = Get-CachedAccessToken "https://api.fabric.microsoft.com"
            if ($t -is [System.Security.SecureString]) {
                $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
                try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
                finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
            }
            return $t
        }

        $p4Token = Get-FabricTokenLocal
        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
        $p4Base = "https://api.fabric.microsoft.com/v1"

        # Resolve workspace
        $p4Ws = (Invoke-RestMethod -Uri "$p4Base/workspaces" -Headers $p4Headers).value |
            Where-Object { $_.displayName -eq $FabricWorkspaceName }
        if (-not $p4Ws) { throw "Workspace '$FabricWorkspaceName' not found" }
        $p4WsId = $p4Ws.id
        Write-Host "  ✓ Workspace: $FabricWorkspaceName ($p4WsId)" -ForegroundColor Green

        # ── Step 8a: Verify clinical pipeline has completed ──
        Write-Host ""
        Write-Host "  --- Step 8a: Clinical Pipeline Verification ---" -ForegroundColor Cyan

        $clinPipelineName = "healthcare1_msft_clinical_data_foundation_ingestion"
        $pipelines = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items?type=DataPipeline" -Headers $p4Headers).value
        $clinPipeline = $pipelines | Where-Object { $_.displayName -eq $clinPipelineName } | Select-Object -First 1

        $clinVerified = $false
        if ($clinPipeline) {
            $clinRuns = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items/$($clinPipeline.id)/jobs/instances?limit=1" -Headers $p4Headers).value
            if ($clinRuns -and $clinRuns[0].status -eq 'Completed') {
                Write-Host "  ✓ Clinical pipeline last run: Completed ($($clinRuns[0].endTimeUtc))" -ForegroundColor Green
                $clinVerified = $true
            } elseif ($clinRuns -and $clinRuns[0].status -eq 'InProgress') {
                Write-Host "  Clinical pipeline is still running — waiting..." -ForegroundColor Yellow
                $clinStart = Get-Date
                while ((New-TimeSpan -Start $clinStart).TotalMinutes -lt 30) {
                    Start-Sleep 30
                    $p4Token = Get-FabricTokenLocal
                    $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                    $clinRuns = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items/$($clinPipeline.id)/jobs/instances?limit=1" -Headers $p4Headers).value
                    $clinElapsed = [math]::Round((New-TimeSpan -Start $clinStart).TotalMinutes, 1)
                    Write-Host "    [$clinElapsed min] Status: $($clinRuns[0].status)" -ForegroundColor DarkGray
                    if ($clinRuns[0].status -eq 'Completed') { $clinVerified = $true; break }
                    if ($clinRuns[0].status -in @('Failed', 'Cancelled')) { break }
                }
                if ($clinVerified) {
                    Write-Host "  ✓ Clinical pipeline completed" -ForegroundColor Green
                } else {
                    Write-Host "  ⚠ Clinical pipeline did not complete — ontology deployment will fail" -ForegroundColor Yellow
                }
            } elseif (-not $clinRuns) {
                Write-Host "  ⚠ Clinical pipeline has never been run — invoking now..." -ForegroundColor Yellow
                try {
                    Invoke-WebRequest -Method POST `
                        -Uri "$p4Base/workspaces/$p4WsId/items/$($clinPipeline.id)/jobs/Pipeline/instances" `
                        -Headers $p4Headers -UseBasicParsing | Out-Null
                    Write-Host "  Clinical pipeline invoked — waiting for completion..." -ForegroundColor White
                    $clinStart = Get-Date
                    while ((New-TimeSpan -Start $clinStart).TotalMinutes -lt 30) {
                        Start-Sleep 30
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $clinRuns = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items/$($clinPipeline.id)/jobs/instances?limit=1" -Headers $p4Headers).value
                        $clinElapsed = [math]::Round((New-TimeSpan -Start $clinStart).TotalMinutes, 1)
                        Write-Host "    [$clinElapsed min] Status: $($clinRuns[0].status)" -ForegroundColor DarkGray
                        if ($clinRuns[0].status -eq 'Completed') { $clinVerified = $true; break }
                        if ($clinRuns[0].status -in @('Failed', 'Cancelled')) { break }
                    }
                    if ($clinVerified) {
                        Write-Host "  ✓ Clinical pipeline completed" -ForegroundColor Green
                    } else {
                        Write-Host "  ⚠ Clinical pipeline did not complete in time" -ForegroundColor Yellow
                    }
                } catch {
                    Write-Host "  ⚠ Could not invoke clinical pipeline: $($_.Exception.Message)" -ForegroundColor Yellow
                }
            } else {
                Write-Host "  ⚠ Clinical pipeline last status: $($clinRuns[0].status)" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  ⚠ Clinical pipeline not found in workspace" -ForegroundColor Yellow
        }
        if (-not $clinVerified) {
            throw "Clinical pipeline was not verified as Completed; ontology deployment requires completed Silver clinical data"
        }


        # ── Step 8b: Materialize DeviceAssociation table ──
        Write-Host ""
        Write-Host "  --- Step 8b: DeviceAssociation Table ---" -ForegroundColor Cyan

        $deviceAssociationMaterialized = $false
        if ($SkipFabric) {
            Write-Host "  Bypassing DeviceAssociation table materialization (--SkipFabric active)" -ForegroundColor Yellow
        } else {
            Write-Host "  Materializing DeviceAssociation from Basic table (Silver Lakehouse)..." -ForegroundColor White

            # Find Silver Lakehouse
            $p4Token = Get-FabricTokenLocal
            $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
            try {
                $p4Lakehouses = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/lakehouses" -Headers $p4Headers -ErrorAction Stop).value
            } catch {
                $p4StatusCode = $null
                try { $p4StatusCode = [int]$_.Exception.Response.StatusCode } catch {}
                if ($p4StatusCode -eq 404) {
                    $p4Lakehouses = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items" -Headers $p4Headers -ErrorAction Stop).value | Where-Object { $_.type -eq 'Lakehouse' }
                } else {
                    throw $_
                }
            }
            $p4SilverLh = $p4Lakehouses | Where-Object { $_.displayName -match '[Ss]ilver' } | Select-Object -First 1

            if ($p4SilverLh) {
            $p4SilverLhId = $p4SilverLh.id
            Write-Host "  ✓ Silver Lakehouse: $($p4SilverLh.displayName) ($p4SilverLhId)" -ForegroundColor Green

            # Read the notebook content
            $daNotebookPath = Join-Path $ScriptDir "fabric-rti\sql\create-device-association-table.ipynb"
            if (Test-Path $daNotebookPath) {
                # Build ipynb using PySpark logic that resolves by name first and falls back to absolute OneLake paths
                $pysparkCode = @'
from pyspark.sql import functions as F
from pyspark.sql.functions import get_json_object
import sys

WORKSPACE_ID = "WORKSPACE_ID_PLACEHOLDER"
LAKEHOUSE_ID = "LAKEHOUSE_ID_PLACEHOLDER"
INCLUDE_FHIR = INCLUDE_FHIR_PLACEHOLDER
INCLUDE_DICOM = INCLUDE_DICOM_PLACEHOLDER

def read_delta_table(name):
    print(f"Attempting to load table {name} by name...")
    try:
        df = spark.read.table(name)
        print(f"Successfully loaded {name} table by name.")
        return df
    except Exception as e:
        print(f"Failed to load {name} table by name: {e}")
        print("Falling back to absolute OneLake ABFSS path...")
        path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/{name}"
        return spark.read.format("delta").load(path)

def save_delta_table(df, name):
    print(f"Attempting to save table {name} by name...")
    try:
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(name)
        print(f"Successfully saved {name} table by name.")
    except Exception as e:
        print(f"Failed to save {name} table by name: {e}")
        print("Falling back to absolute OneLake ABFSS path...")
        path = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/{name}"
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path)
        print(f"{name} table materialized successfully via abfss fallback.")

def present(df, name):
    return name in df.columns

def maybe_col(df, name, data_type="string"):
    return F.col(name) if present(df, name) else F.lit(None).cast(data_type)

def select_col(df, name, data_type="string"):
    return maybe_col(df, name, data_type).alias(name)

def json_col(df, name, path):
    return get_json_object(F.col(name), path) if present(df, name) else F.lit(None).cast("string")

def non_empty(expr):
    return F.when(expr.isNotNull() & (F.length(F.trim(expr.cast("string"))) > 0), expr.cast("string"))

def patient_id_expr(df, column_name, imaging=False):
    # HDS 1.3.x clears FHIR Reference.reference and preserves the original
    # resource link under msftSourceReference, with idOrig as the normalized UUID.
    # ImagingStudy also receives identifier.value and should prefer it first.
    identifier = non_empty(json_col(df, column_name, "$.identifier.value"))
    msft = non_empty(F.regexp_replace(json_col(df, column_name, "$.msftSourceReference"), "^Patient/", ""))
    id_orig = non_empty(json_col(df, column_name, "$.idOrig"))
    if imaging:
        return F.coalesce(identifier, msft, id_orig)
    return F.coalesce(msft, identifier, id_orig)


basic_df = read_delta_table("Basic")
device_df = basic_df.filter(get_json_object(F.col("code_string"), "$.coding[0].code") == "device-assoc")
device_association_df = device_df.select(
    select_col(device_df, "id"),
    select_col(device_df, "idOrig"),
    F.regexp_replace(json_col(device_df, "extension", "$[0].valueReference.reference"), "^Device/", "").alias("device_ref"),
    json_col(device_df, "subject_string", "$.display").alias("patient_name"),
    patient_id_expr(device_df, "subject_string").alias("patient_id"),
    json_col(device_df, "code_string", "$.coding[0].code").alias("assoc_code"),
    json_col(device_df, "code_string", "$.coding[0].display").alias("assoc_display")
)
save_delta_table(device_association_df, "DeviceAssociation")

if INCLUDE_FHIR:
    encounter_df = read_delta_table("Encounter")
    save_delta_table(encounter_df.select(
        select_col(encounter_df, "idOrig"),
        select_col(encounter_df, "class_string"),
        select_col(encounter_df, "status"),
        F.coalesce(non_empty(maybe_col(encounter_df, "period_start")), non_empty(json_col(encounter_df, "period_string", "$.start"))).alias("period_start"),
        patient_id_expr(encounter_df, "subject_string").alias("patient_id")
    ), "EncounterOntology")

    condition_df = read_delta_table("Condition")
    save_delta_table(condition_df.select(
        select_col(condition_df, "idOrig"),
        select_col(condition_df, "code_string"),
        select_col(condition_df, "clinicalStatus_string"),
        patient_id_expr(condition_df, "subject_string").alias("patient_id")
    ), "ConditionOntology")

    med_df = read_delta_table("MedicationRequest")
    save_delta_table(med_df.select(
        select_col(med_df, "idOrig"),
        select_col(med_df, "medicationCodeableConcept_string"),
        select_col(med_df, "status"),
        select_col(med_df, "authoredOn"),
        patient_id_expr(med_df, "subject_string").alias("patient_id")
    ), "MedicationRequestOntology")

    obs_df = read_delta_table("Observation")
    save_delta_table(obs_df.select(
        select_col(obs_df, "idOrig"),
        select_col(obs_df, "code_string"),
        select_col(obs_df, "valueQuantity_value", "double"),
        select_col(obs_df, "valueQuantity_unit"),
        select_col(obs_df, "effectiveDateTime"),
        patient_id_expr(obs_df, "subject_string").alias("patient_id")
    ), "ObservationOntology")
else:
    print("Skipping FHIR ontology projection tables because INCLUDE_FHIR is false.")

if INCLUDE_DICOM:
    img_df = read_delta_table("ImagingStudy")
    save_delta_table(img_df.select(
        select_col(img_df, "idOrig"),
        select_col(img_df, "description"),
        patient_id_expr(img_df, "subject_string", imaging=True).alias("patient_id"),
        select_col(img_df, "numberOfSeries", "long"),
        select_col(img_df, "numberOfInstances", "long")
    ), "ImagingStudyOntology")
else:
    print("Skipping ImagingStudyOntology because INCLUDE_DICOM is false.")

print("Ontology projection tables materialized successfully.")
'@

                # Interpolate workspace and lakehouse IDs into the PySpark template
                $pyIncludeFhir = if ($SkipFhir -and -not $Phase4) { "False" } else { "True" }
                $pyIncludeDicom = if ($SkipDicom -and -not $Phase4) { "False" } else { "True" }
                $pysparkCode = $pysparkCode.Replace("WORKSPACE_ID_PLACEHOLDER", $p4WsId).Replace("LAKEHOUSE_ID_PLACEHOLDER", $p4SilverLhId).Replace("INCLUDE_FHIR_PLACEHOLDER", $pyIncludeFhir).Replace("INCLUDE_DICOM_PLACEHOLDER", $pyIncludeDicom)

                $daIpynb = @{
                    nbformat = 4
                    nbformat_minor = 5
                    metadata = @{
                        kernel_info = @{ name = "synapse_pyspark" }
                        kernelspec = @{ name = "synapse_pyspark"; display_name = "Synapse PySpark" }
                        language_info = @{ name = "python" }
                    }
                    cells = @(
                        @{ cell_type = "code"; source = ($pysparkCode -split '\r?\n') | ForEach-Object { "$_`n" }; metadata = @{}; outputs = @() }
                    )
                }

                $daIpynbJson = $daIpynb | ConvertTo-Json -Depth 10 -Compress
                $daIpynbBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($daIpynbJson))

                # Check for existing notebook and delete
                $p4Items = $null
                for ($listAttempt = 1; $listAttempt -le 8; $listAttempt++) {
                    try {
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $p4Items = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items" -Headers $p4Headers -ErrorAction Stop).value
                        break
                    } catch {
                        $listStatusCode = $null
                        try { $listStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
                        $listBody = $_.ErrorDetails.Message
                        if (($listStatusCode -in @(429, 500, 502, 503, 504) -or $listStatusCode -eq 403) -and $listAttempt -lt 8) {
                            $delay = [Math]::Min(15 * $listAttempt, 90)
                            Write-Host "    Fabric item list transient HTTP $listStatusCode — retrying in ${delay}s... ($listAttempt/8)" -ForegroundColor Yellow
                            if ($listBody) { Write-Host $listBody -ForegroundColor DarkGray }
                            Start-Sleep $delay
                            continue
                        }
                        throw $_
                    }
                }
                $existingDaNb = $p4Items | Where-Object { $_.displayName -match '^create_device_association_table(_\d+)?$' -and $_.type -eq "Notebook" }
                if ($existingDaNb) {
                    if ($existingDaNb -is [array]) { $existingDaNb = $existingDaNb[0] }
                    Write-Host "  Deleting existing notebook..." -ForegroundColor DarkGray
                    try {
                        Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items/$($existingDaNb.id)" -Headers $p4Headers -Method Delete
                    } catch {
                        Write-Host "  ⚠ Could not delete notebook: $($_.Exception.Message)" -ForegroundColor Yellow
                    }
                    Start-Sleep 5
                }

                $daNotebookDisplayName = "create_device_association_table"

                # Create notebook
                $daNbBody = @{
                    displayName = $daNotebookDisplayName
                    type = "Notebook"
                    definition = @{
                        format = "ipynb"
                        parts = @(
                            @{
                                path = "artifact.content.ipynb"
                                payload = $daIpynbBase64
                                payloadType = "InlineBase64"
                            }
                        )
                    }
                } | ConvertTo-Json -Depth 5

                $daNbCreated = $false
                for ($attempt = 1; $attempt -le 8; $attempt++) {
                    try {
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $daNbResp = Invoke-WebRequest -Uri "$p4Base/workspaces/$p4WsId/items" `
                            -Headers $p4Headers -Method Post -Body $daNbBody -UseBasicParsing -ErrorAction Stop
                        if ($daNbResp.StatusCode -eq 202) {
                            $daNbOpId = $daNbResp.Headers["x-ms-operation-id"]
                            if ($daNbOpId -is [array]) { $daNbOpId = $daNbOpId[0] }
                            Start-Sleep 10
                        }
                        $daNbCreated = $true
                        break
                    } catch {
                        $errCode = $null
                        try { $errCode = [int]$_.Exception.Response.StatusCode } catch {}
                        $errBody = $_.ErrorDetails.Message
                        if ($errCode -eq 409) {
                            if ($attempt -lt 3) {
                                Write-Host "    409 Conflict (Name Reservation Lock) — retrying in 10s ($attempt/3)" -ForegroundColor Yellow
                                Start-Sleep 10
                            } else {
                                $timestamp = Get-Date -Format "yyyyMMddHHmmss"
                                $daNotebookDisplayName = "create_device_association_table_$timestamp"
                                Write-Host "    409 Conflict persisted. Bypassing lock by suffixing notebook to: $daNotebookDisplayName" -ForegroundColor Cyan
                                $daNbBodyObj = $daNbBody | ConvertFrom-Json
                                $daNbBodyObj.displayName = $daNotebookDisplayName
                                $daNbBody = $daNbBodyObj | ConvertTo-Json -Depth 5
                            }
                        } elseif (($errCode -in @(429, 500, 502, 503, 504) -or $errCode -eq 403) -and $attempt -lt 8) {
                            $delay = [Math]::Min(15 * $attempt, 90)
                            Write-Host "    Notebook create transient HTTP $errCode — retrying in ${delay}s... ($attempt/8)" -ForegroundColor Yellow
                            if ($errBody) { Write-Host $errBody -ForegroundColor DarkGray }
                            Start-Sleep $delay
                        } else { throw }
                    }
                }

                if ($daNbCreated) {
                    # Find the notebook and run it
                    $p4Token = Get-FabricTokenLocal
                    $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                    $daNb = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items" -Headers $p4Headers).value |
                        Where-Object { $_.type -eq "Notebook" -and $_.displayName -eq $daNotebookDisplayName }
                    Move-FabricNotebooksToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $p4WsId
                    if ($daNb) {
                        Write-Host "  Running notebook (attached to Silver Lakehouse)..." -ForegroundColor White
                        try {
                            $runBody = @{
                                executionData = @{
                                    defaultLakehouse = @{
                                        id = $p4SilverLhId
                                        workspaceId = $p4WsId
                                    }
                                }
                            } | ConvertTo-Json -Depth 5
                            Invoke-WebRequest -Method POST `
                                -Uri "$p4Base/workspaces/$p4WsId/items/$($daNb.id)/jobs/instances?jobType=RunNotebook" `
                                -Headers $p4Headers -Body $runBody -UseBasicParsing | Out-Null
                            Write-Host "  ✓ DeviceAssociation notebook invoked" -ForegroundColor Green

                            # Wait for notebook to complete (~1-2 min)
                            $daStart = Get-Date
                            while ((New-TimeSpan -Start $daStart).TotalMinutes -lt 10) {
                                Start-Sleep 15
                                $p4Token = Get-FabricTokenLocal
                                $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                                try {
                                    $daJobs = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items/$($daNb.id)/jobs/instances?limit=1" -Headers $p4Headers -ErrorAction Stop).value
                                } catch {
                                    $jobStatusCode = $null
                                    try { $jobStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
                                    $jobBody = $_.ErrorDetails.Message
                                    if ($jobStatusCode -in @(429, 500, 502, 503, 504) -or ($jobStatusCode -eq 403 -and $jobBody -match "RequestDeniedByInboundPolicy")) {
                                        Write-Host "    DeviceAssociation job status transient HTTP $jobStatusCode — retrying..." -ForegroundColor Yellow
                                        continue
                                    }
                                    throw $_
                                }
                                if ($daJobs -and $daJobs[0].status -eq 'Completed') {
                                    Write-Host "  ✓ DeviceAssociation table materialized" -ForegroundColor Green
                                    $deviceAssociationMaterialized = $true
                                    break
                                } elseif ($daJobs -and $daJobs[0].status -in @('Failed', 'Cancelled')) {
                                    throw "DeviceAssociation notebook $($daJobs[0].status)"
                                }
                                $daElapsed = [math]::Round((New-TimeSpan -Start $daStart).TotalMinutes, 1)
                                Write-Host "    [$daElapsed min] Notebook: $($daJobs[0].status)" -ForegroundColor DarkGray
                            }
                            if (-not $deviceAssociationMaterialized) {
                                throw "DeviceAssociation notebook did not complete within 10 minutes"
                            }
                        } catch {
                            throw "Could not run DeviceAssociation notebook: $($_.Exception.Message)"
                        }
                    }
                    else {
                        throw "DeviceAssociation notebook was created but not found for execution"
                    }
                }
                else {
                    throw "DeviceAssociation notebook item was not created"
                }
            } else {
                throw "Notebook source not found at: $daNotebookPath"
            }
        } else {
            throw "Silver Lakehouse not found — cannot materialize DeviceAssociation"
        }
    }

        if (-not $SkipFabric -and -not $deviceAssociationMaterialized) {
            throw "DeviceAssociation table was not materialized"
        }

        # ── Step 8c: Deploy Ontology ──
        Write-Host ""
        Write-Host "  --- Step 8c: Ontology Deployment ---" -ForegroundColor Cyan
        Write-Host "  Deploying ClinicalDeviceOntology and DevicePayerOntology..." -ForegroundColor White

        Invoke-Step -StepName "Phase 4: Ontology Deployment" -Description "Deploying clinical and payer ontologies" -Action {
            Write-Host "  Deploying ClinicalDeviceOntology (clinical device graph only)..." -ForegroundColor White

            $clinicalOntologyArgs = @{
                FabricWorkspaceName = $FabricWorkspaceName
                OntologyName        = "ClinicalDeviceOntology"
                IncludeFhir         = [bool]($Phase4 -or (-not $SkipFhir))
                IncludeDicom        = [bool]($Phase4 -or (-not $SkipDicom))
                IncludeTelemetry    = [bool](-not $SkipFabric)
                IncludeGold         = $false
                ReplaceExisting     = [bool]$ReplaceOntology
            }
            $global:LASTEXITCODE = 0
            & "$ScriptDir\phase-4\deploy-ontology.ps1" @clinicalOntologyArgs
            Assert-LastExternalCommandSucceeded "deploy-ontology.ps1 ClinicalDeviceOntology"

            if (-not $SkipQualityMeasures) {
                Write-Host ""
                Write-Host "  Deploying DevicePayerOntology (device + patient + diagnosis + claims graph)..." -ForegroundColor White
                $payerOntologyArgs = @{
                    FabricWorkspaceName = $FabricWorkspaceName
                    OntologyName        = "DevicePayerOntology"
                    IncludeFhir         = [bool](-not $SkipFhir)
                    IncludeDicom        = [bool](-not $SkipDicom)
                    IncludeTelemetry    = [bool](-not $SkipFabric)
                    IncludeGold         = $true
                }
                $global:LASTEXITCODE = 0
                & "$ScriptDir\phase-4\deploy-ontology.ps1" @payerOntologyArgs
                Assert-LastExternalCommandSucceeded "deploy-ontology.ps1 DevicePayerOntology"
            } else {
                Write-Host "  ⚠ Skipping DevicePayerOntology because -SkipQualityMeasures is set" -ForegroundColor Yellow
            }
            Write-Host ""
        }

        # ── Step 8d: Deploy ontology-aware clinical Data Agents ──
        if (-not $SkipDataAgents) {
            Write-Host "  --- Step 8d: Ontology-Aware Clinical Data Agents ---" -ForegroundColor Cyan
            Invoke-Step -StepName "Phase 4: Ontology-Aware Data Agents" `
                -Description "Deploy Patient 360 + Clinical Triage after ClinicalDeviceOntology exists" -Action {
                Write-Host "  This step will:" -ForegroundColor White
                Write-Host "    [1/2] Create/update Patient 360 Data Agent with ClinicalDeviceOntology" -ForegroundColor DarkGray
                Write-Host "    [2/2] Create/update Clinical Triage Data Agent with ClinicalDeviceOntology" -ForegroundColor DarkGray
                Write-Host "  Architecture: KQL + Silver Lakehouse + ClinicalDeviceOntology" -ForegroundColor DarkGray
                Write-Host ""

                $global:LASTEXITCODE = 0
                & "$ScriptDir\phase-2\deploy-data-agents.ps1" `
                    -FabricWorkspaceName $FabricWorkspaceName `
                    -IncludeDicomImaging:((-not $SkipImaging) -and (-not $SkipDicom))
                Assert-LastExternalCommandSucceeded "deploy-data-agents.ps1"
            }
            Write-Host ""
        }

        # ── Step 8d: Bind ontologies to the right Data Agents ──
        Write-Host "  --- Step 8d: Agent Ontology Binding ---" -ForegroundColor Cyan

        function Add-OntologyDatasourceToAgents {
            param(
                [Parameter(Mandatory)][string]$OntologyName,
                [Parameter(Mandatory)][string[]]$AgentNames,
                [Parameter(Mandatory)][string]$UserDescription,
                [Parameter(Mandatory)][string]$DataSourceInstructions,
                [string[]]$RemoveOntologyNames = @(),
                [switch]$OptionalAgents
            )

            $p4Token = Get-FabricTokenLocal
            $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
            function Invoke-P4RestWithRetry {
                param([Parameter(Mandatory)][string]$Uri, [int]$MaxRetries = 8)
                for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
                    try {
                        return Invoke-RestMethod -Uri $Uri -Headers $p4Headers -ErrorAction Stop
                    } catch {
                        $statusCode = $null
                        try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
                        $errBody = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
                        if (($statusCode -eq 403 -and $errBody -match 'RequestDeniedByInboundPolicy|Forbidden') -or $statusCode -in @(429, 500, 502, 503, 504)) {
                            if ($attempt -lt $MaxRetries) {
                                $delay = [Math]::Min(120, 10 * [Math]::Pow(2, $attempt - 1))
                                Write-Host "  Fabric API transient HTTP ${statusCode}; retrying in ${delay}s... ($attempt/$MaxRetries)" -ForegroundColor Yellow
                                Start-Sleep -Seconds $delay
                                continue
                            }
                        }
                        throw $_
                    }
                }
            }

            $ontologies = (Invoke-P4RestWithRetry -Uri "$p4Base/workspaces/$p4WsId/ontologies").value
            $ontology = $ontologies | Where-Object { $_.displayName -eq $OntologyName } | Select-Object -First 1
            if (-not $ontology) { throw "Ontology '$OntologyName' not found — agent binding skipped" }

            $ontologyId = $ontology.id
            Write-Host "  ✓ Ontology found: $OntologyName ($ontologyId)" -ForegroundColor Green

            $agents = (Invoke-P4RestWithRetry -Uri "$p4Base/workspaces/$p4WsId/items?type=DataAgent").value
            $bindingFailures = @()
            foreach ($agentName in $AgentNames) {
                $agent = $agents | Where-Object { $_.displayName -eq $agentName } | Select-Object -First 1
                if (-not $agent) {
                    $msg = "Agent '$agentName' not found"
                    if ($OptionalAgents) { Write-Host "  ⚠ $msg; skipping optional ontology binding" -ForegroundColor Yellow; continue }
                    $bindingFailures += $msg
                    continue
                }

                Write-Host "  Binding $OntologyName to '$agentName'..." -ForegroundColor White
                $bound = $false
                for ($bindAttempt = 1; $bindAttempt -le 6 -and -not $bound; $bindAttempt++) {
                    try {
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $defResp = Invoke-WebRequest -Method POST `
                            -Uri "$p4Base/workspaces/$p4WsId/items/$($agent.id)/getDefinition" `
                            -Headers $p4Headers -UseBasicParsing -ErrorAction Stop
                        $defOpId = $defResp.Headers["x-ms-operation-id"]
                        if ($defOpId -is [array]) { $defOpId = $defOpId[0] }
                        Start-Sleep 5
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $defResult = Invoke-RestMethod -Uri "$p4Base/operations/$defOpId/result" -Headers $p4Headers -ErrorAction Stop
                        $existingParts = @($defResult.definition.parts)

                        $ontologyNamesToRemove = @($RemoveOntologyNames + $OntologyName) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique
                        foreach ($removeName in $ontologyNamesToRemove) {
                            $safeRemoveName = [regex]::Escape("ontology-$removeName")
                            $existingParts = @($existingParts | Where-Object { $_.path -notmatch $safeRemoveName })
                        }

                        $ontDatasourceJson = @{
                            '$schema'              = "1.0.0"
                            artifactId             = $ontologyId
                            workspaceId            = $p4WsId
                            displayName            = $OntologyName
                            type                   = "ontology"
                            userDescription        = $UserDescription
                            dataSourceInstructions = $DataSourceInstructions
                        } | ConvertTo-Json -Depth 10

                        $ontFewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = @() } | ConvertTo-Json -Depth 5)
                        $ontFolderName = "ontology-$OntologyName"
                        $ontDsPart = @{
                            path        = "Files/Config/draft/$ontFolderName/datasource.json"
                            payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ontDatasourceJson))
                            payloadType = "InlineBase64"
                        }
                        $ontFsPart = @{
                            path        = "Files/Config/draft/$ontFolderName/fewshots.json"
                            payload     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ontFewShotsJson))
                            payloadType = "InlineBase64"
                        }

                        $updatedParts = @($existingParts) + @($ontDsPart, $ontFsPart)
                        $updateBody = @{ definition = @{ parts = $updatedParts } }
                        $updateResp = Invoke-WebRequest -Method POST `
                            -Uri "$p4Base/workspaces/$p4WsId/items/$($agent.id)/updateDefinition" `
                            -Headers $p4Headers `
                            -Body ($updateBody | ConvertTo-Json -Depth 20) `
                            -UseBasicParsing -ErrorAction Stop

                        if ($updateResp.StatusCode -in @(200, 202)) {
                            if ($updateResp.StatusCode -eq 202) {
                                $upOpId = $updateResp.Headers["x-ms-operation-id"]
                                if ($upOpId -is [array]) { $upOpId = $upOpId[0] }
                                Start-Sleep 10
                            }
                            Write-Host "  ✓ $OntologyName datasource applied to '$agentName'" -ForegroundColor Green
                            $bound = $true
                        } else {
                            throw "Ontology datasource update returned HTTP $($updateResp.StatusCode) for '$agentName'"
                        }
                    } catch {
                        $bindStatusCode = $null
                        try { $bindStatusCode = [int]$_.Exception.Response.StatusCode } catch {}
                        $bindBody = $_.ErrorDetails.Message
                        if (($bindStatusCode -in @(429, 500, 502, 503, 504) -or $bindStatusCode -eq 403) -and $bindAttempt -lt 6) {
                            $delay = [Math]::Min(20 * $bindAttempt, 120)
                            Write-Host "    Agent binding transient HTTP $bindStatusCode for '$agentName' — retrying in ${delay}s... ($bindAttempt/6)" -ForegroundColor Yellow
                            if ($bindBody) { Write-Host $bindBody -ForegroundColor DarkGray }
                            Start-Sleep $delay
                            continue
                        }
                        $bindingFailures += "Could not bind $OntologyName to '$agentName': $($_.Exception.Message)"
                    }
                }
            }

            if ($bindingFailures.Count -gt 0) {
                throw "Ontology agent binding failed: $($bindingFailures -join '; ')"
            }
        }

        $clinicalAgentNames = @("Patient 360", "Clinical Triage")
        if (-not $SkipImaging -and -not $SkipDicom) { $clinicalAgentNames += "HDS Multi-Layer Imaging Cohort Agent" }

        Add-OntologyDatasourceToAgents `
            -OntologyName "ClinicalDeviceOntology" `
            -AgentNames $clinicalAgentNames `
            -UserDescription "Clinical device semantic layer for Patient, Device, Encounter, Condition, MedicationRequest, Observation, ImagingStudy, DeviceAssociation, and real-time DeviceTelemetry." `
            -DataSourceInstructions "Use this ontology for clinical device vocabulary and relationship grounding only. It maps Patient↔Device, Patient→Encounter, Patient→Condition, Patient→Observation, Patient→MedicationRequest, Patient→ImagingStudy, and Device→DeviceTelemetry across Lakehouse and Eventhouse sources. Clinical alerts remain available through the KQL/Data Activator path (fn_ClinicalAlerts / ClinicalAlertActivator) rather than this ontology until Fabric exposes actionable AlertHistory ontology import diagnostics. The actual patient-device assignment rows and patient home/location demographics must still be queried from the Lakehouse dbo.Basic and dbo.Patient tables. Do not use it for payer/claims reasoning." `
            -RemoveOntologyNames @("DevicePayerOntology")

        if (-not $SkipQualityMeasures) {
            Add-OntologyDatasourceToAgents `
                -OntologyName "DevicePayerOntology" `
                -AgentNames @("Payer Ops Triage", "Healthcare Graph Agent", "HealthcareOpsAgent") `
                -UserDescription "Payer-oriented device ontology linking Patient, Device, Diagnosis, Claim, Payer, CareGap, PatientRisk, HighCostClaimant, clinical alerts, and telemetry." `
                -DataSourceInstructions "Use this ontology for claims, payer operations, care gaps, high-cost claimant, RAF/risk, payer-category, and device-to-payer questions. It keeps payer semantics out of the clinical-device ontology while preserving patient/device/diagnosis/claim relationships." `
                -RemoveOntologyNames @("ClinicalDeviceOntology") `
                -OptionalAgents
        }

        Write-Host ""
    }
}

    # ============================================================================
    # PHASE 5 — Bedside Alerting & Action
    # ============================================================================

    Emit-PhaseTransition -Phase 5 -Label "Bedside Alerting & Action" -StepCount 1

    # ── Step 9: Data Activator ──
    Invoke-Step -StepName "Phase 5: Data Activator" `
        -Description "Reflex item with KQL source and email alerting rule" -Action {

        if ($SkipActivator) {
            Write-Host "  ⚠ Skipping Activator deployment (-SkipActivator is set)" -ForegroundColor Yellow
            return
        }

        function Get-FabricTokenLocal {
            $t = Get-CachedAccessToken "https://api.fabric.microsoft.com"
            if ($t -is [System.Security.SecureString]) {
                $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
                try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
                finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
            }
            return $t
        }

        if (-not $AlertEmail) {
            Write-Host "  ⚠ No -AlertEmail specified — skipping Activator deployment" -ForegroundColor Yellow
            Write-Host "    Re-run with -AlertEmail 'nurse@hospital.com' to enable email alerts" -ForegroundColor DarkGray
        } else {
            Write-Host "  Alert email:     $AlertEmail" -ForegroundColor White
            Write-Host "  Tier threshold:  $AlertTierThreshold" -ForegroundColor White
            Write-Host "  Cooldown:        $AlertCooldownMinutes min" -ForegroundColor White

            $p4Token = Get-FabricTokenLocal
            $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
            $p4Base = "https://api.fabric.microsoft.com/v1"

            function Test-FabricTransientError {
                param([object]$StatusCode, [string]$ErrorText = "")
                $status = $null
                try { if ($null -ne $StatusCode) { $status = [int]$StatusCode } } catch { return $false }
                if ($status -in @(429, 500, 502, 503, 504)) { return $true }
                return ($status -eq 403 -and $ErrorText -match 'RequestDeniedByInboundPolicy|inbound communication policy|Forbidden')
            }

            function Invoke-P4ActivatorRest {
                param(
                    [Parameter(Mandatory)][string]$Uri,
                    [string]$Method = 'GET',
                    [object]$Body = $null,
                    [string]$Label = 'Fabric request'
                )
                for ($attempt = 1; $attempt -le 8; $attempt++) {
                    try {
                        $token = Get-FabricTokenLocal
                        $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
                        if ($Body -ne $null) { return Invoke-RestMethod -Uri $Uri -Headers $headers -Method $Method -Body $Body -ErrorAction Stop }
                        return Invoke-RestMethod -Uri $Uri -Headers $headers -Method $Method -ErrorAction Stop
                    } catch {
                        $statusCode = $null
                        try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
                        $errBody = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
                        if ((Test-FabricTransientError -StatusCode $statusCode -ErrorText $errBody) -and $attempt -lt 8) {
                            $delay = [Math]::Min(15 * $attempt, 90)
                            Write-Host "  $Label transient HTTP ${statusCode}; retrying in ${delay}s... ($attempt/8)" -ForegroundColor Yellow
                            if ($errBody) { Write-Host "    $errBody" -ForegroundColor DarkGray }
                            Start-Sleep -Seconds $delay
                            continue
                        }
                        throw
                    }
                }
            }

            function Invoke-P4ActivatorWeb {
                param(
                    [Parameter(Mandatory)][string]$Uri,
                    [string]$Method = 'GET',
                    [object]$Body = $null,
                    [string]$Label = 'Fabric request'
                )
                for ($attempt = 1; $attempt -le 8; $attempt++) {
                    try {
                        $token = Get-FabricTokenLocal
                        $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
                        if ($Body -ne $null) { return Invoke-WebRequest -Uri $Uri -Headers $headers -Method $Method -Body $Body -UseBasicParsing -ErrorAction Stop }
                        return Invoke-WebRequest -Uri $Uri -Headers $headers -Method $Method -UseBasicParsing -ErrorAction Stop
                    } catch {
                        $statusCode = $null
                        try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
                        $errBody = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
                        if ((Test-FabricTransientError -StatusCode $statusCode -ErrorText $errBody) -and $attempt -lt 8) {
                            $delay = [Math]::Min(15 * $attempt, 90)
                            Write-Host "  $Label transient HTTP ${statusCode}; retrying in ${delay}s... ($attempt/8)" -ForegroundColor Yellow
                            if ($errBody) { Write-Host "    $errBody" -ForegroundColor DarkGray }
                            Start-Sleep -Seconds $delay
                            continue
                        }
                        throw
                    }
                }
            }

            # Resolve workspace ID
            $p4Workspaces = (Invoke-P4ActivatorRest -Uri "$p4Base/workspaces" -Label 'List workspaces').value
            $p4Ws = $p4Workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
            if (-not $p4Ws) {
                throw "Workspace '$FabricWorkspaceName' not found — cannot deploy Activator"
            }
            $p4WsId = $p4Ws.id

            $reflexName = "ClinicalAlertActivator"

            # Check for existing Reflex
            $p4Items = (Invoke-P4ActivatorRest -Uri "$p4Base/workspaces/$p4WsId/items" -Label 'List workspace items').value
            $existingReflex = $p4Items | Where-Object { $_.displayName -eq $reflexName -and $_.type -eq 'Reflex' }

            $reflexId = $null
            if ($existingReflex) {
                if ($existingReflex -is [array]) { $existingReflex = $existingReflex[0] }
                $reflexId = $existingReflex.id
                Write-Host "  ✓ Reflex '$reflexName' already exists ($reflexId)" -ForegroundColor Yellow
            }

            # Discover KQL Database for KQL source (itemId must be KQL DB, not Eventhouse)
            $p4KqlDbs = (Invoke-P4ActivatorRest -Uri "$p4Base/workspaces/$p4WsId/kqlDatabases" -Label 'List KQL databases').value
            $p4KqlDb = $p4KqlDbs | Where-Object { $_.displayName -match 'Masimo' } | Select-Object -First 1
            if (-not $p4KqlDb) { $p4KqlDb = $p4KqlDbs | Select-Object -First 1 }
            if ($p4KqlDb -is [array]) { $p4KqlDb = $p4KqlDb[0] }

            if (-not $p4KqlDb) {
                throw "No KQL Database found — cannot configure ClinicalAlertActivator data source"
            } else {
                Write-Host "  ✓ KQL Database: $($p4KqlDb.displayName) ($($p4KqlDb.id))" -ForegroundColor Green

                # Generate GUIDs for entities
                $containerId   = [guid]::NewGuid().ToString()
                $kqlSourceId   = [guid]::NewGuid().ToString()
                $eventViewId   = [guid]::NewGuid().ToString()
                $objectViewId  = [guid]::NewGuid().ToString()
                $attrDeviceId  = [guid]::NewGuid().ToString()
                $attrAlertTier = [guid]::NewGuid().ToString()
                $attrSpo2Id    = [guid]::NewGuid().ToString()
                $attrPrId      = [guid]::NewGuid().ToString()
                $attrPatientId = [guid]::NewGuid().ToString()
                $attrMessageId = [guid]::NewGuid().ToString()

                $ruleApplied = $false
                $kqlQuery = "fn_ClinicalAlerts($AlertCooldownMinutes) | where alert_tier in ('CRITICAL', 'URGENT') | project device_id, alert_tier, spo2, pr, patient_name, message, alert_time"

                # Build instance strings as raw JSON (PowerShell ConvertTo-Json corrupts nested instance strings)
                $srcEvtInst = '{"templateId":"SourceEvent","templateVersion":"1.1","steps":[{"name":"SourceEventStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"SourceSelector","kind":"SourceReference","arguments":[{"name":"entityId","type":"string","value":"' + $kqlSourceId + '"}]}]}]}'
                $idPartInst = '{"templateId":"IdentityPartAttribute","templateVersion":"1.1","steps":[{"name":"IdPartStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"TypeAssertion","kind":"TypeAssertion","arguments":[{"name":"op","type":"string","value":"Text"},{"name":"format","type":"string","value":""}]}]}]}'

                function New-BasicAttrInstance([string]$evId, [string]$fieldName, [string]$dataType) {
                    '{"templateId":"BasicEventAttribute","templateVersion":"1.1","steps":[{"name":"EventSelectStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"EventSelector","kind":"Event","arguments":[{"kind":"EventReference","type":"complex","arguments":[{"name":"entityId","type":"string","value":"' + $evId + '"}],"name":"event"}]},{"name":"EventFieldSelector","kind":"EventField","arguments":[{"name":"fieldName","type":"string","value":"' + $fieldName + '"}]}]},{"name":"EventComputeStep","id":"' + [guid]::NewGuid().ToString() + '","rows":[{"name":"TypeAssertion","kind":"TypeAssertion","arguments":[{"name":"op","type":"string","value":"' + $dataType + '"},{"name":"format","type":"string","value":""}]}]}]}'
                }

                # Build entities array (KQL source uses itemId+workspaceId, NOT targetUniqueIdentifier)
                $entities = @(
                    @{uniqueIdentifier=$containerId; payload=@{name="Clinical Alerts";type="kqlQueries"}; type="container-v1"},
                    @{uniqueIdentifier=$kqlSourceId; payload=@{name="fn_ClinicalAlerts"; runSettings=@{executionIntervalInSeconds=($AlertCooldownMinutes*60)}; query=@{queryString=$kqlQuery}; eventhouseItem=@{itemId=$p4KqlDb.id; workspaceId=$p4WsId; itemType="KustoDatabase"}; parentContainer=@{targetUniqueIdentifier=$containerId}}; type="kqlSource-v1"},
                    @{uniqueIdentifier=$eventViewId; payload=@{name="Clinical alert events"; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Event"; instance=$srcEvtInst}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$objectViewId; payload=@{name="Device"; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Object"}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$attrDeviceId; payload=@{name="device_id"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=$idPartInst}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$attrAlertTier; payload=@{name="alert_tier"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "alert_tier" "Text")}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$attrSpo2Id; payload=@{name="spo2"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "spo2" "Number")}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$attrPrId; payload=@{name="pr"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "pr" "Number")}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$attrPatientId; payload=@{name="patient_name"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "patient_name" "Text")}}; type="timeSeriesView-v1"},
                    @{uniqueIdentifier=$attrMessageId; payload=@{name="message"; parentObject=@{targetUniqueIdentifier=$objectViewId}; parentContainer=@{targetUniqueIdentifier=$containerId}; definition=@{type="Attribute"; instance=(New-BasicAttrInstance $eventViewId "message" "Text")}}; type="timeSeriesView-v1"}
                )

                $entitiesJson = ConvertTo-Json -InputObject $entities -Depth 30 -Compress

                # Build rule entity as raw JSON using EventTrigger v1.2.4 (reverse-engineered from portal-created rule)
                # Key differences from docs: EventTrigger not AttributeTrigger, OnEveryValue not EachTime,
                # no parentObject, email fields use EventFieldReference arrays, context uses NameReferencePair
                function FR([string]$f) { '{"arguments":[{"name":"fieldName","type":"string","value":"'+$f+'"}],"kind":"EventFieldReference","type":"complex"}' }
                function NR([string]$f) { '{"arguments":[{"name":"name","type":"string","value":"'+$f+'"},{"arguments":[{"name":"fieldName","type":"string","value":"'+$f+'"}],"kind":"EventFieldReference","name":"reference","type":"complexReference"}],"kind":"NameReferencePair","type":"complex"}' }

                $ruleInst = '{"templateId":"EventTrigger","templateVersion":"1.2.4","steps":[' +
                    '{"id":"' + [guid]::NewGuid().ToString() + '","name":"FieldsDefaultsStep","rows":[{"arguments":[{"arguments":[{"name":"entityId","type":"string","value":"' + $eventViewId + '"}],"kind":"EventReference","name":"event","type":"complex"}],"kind":"Event","name":"EventSelector"}]},' +
                    '{"id":"' + [guid]::NewGuid().ToString() + '","name":"EventDetectStep","rows":[{"arguments":[],"kind":"OnEveryValue","name":"OnEveryValue"}]},' +
                    '{"id":"' + [guid]::NewGuid().ToString() + '","name":"ActStep","rows":[{"arguments":[' +
                        '{"name":"messageLocale","type":"string","value":"en-us"},' +
                        '{"name":"sentTo","type":"array","values":[{"type":"string","value":"' + $AlertEmail + '"}]},' +
                        '{"name":"copyTo","type":"array","values":[]},' +
                        '{"name":"bCCTo","type":"array","values":[]},' +
                        '{"name":"subject","type":"array","values":[{"name":"string","type":"string","value":"CLINICAL ALERT - SpO2 low on "},' + (FR 'device_id') + ']},' +
                        '{"name":"headline","type":"array","values":[' + (FR 'alert_tier') + ',{"name":"string","type":"string","value":" ALERT: "},' + (FR 'patient_name') + ',{"name":"string","type":"string","value":" - SpO2 "},' + (FR 'spo2') + ']},' +
                        '{"name":"optionalMessage","type":"array","values":[{"name":"string","type":"string","value":"SpO2: "},' + (FR 'spo2') + ',{"name":"string","type":"string","value":"% | PR: "},' + (FR 'pr') + ',{"name":"string","type":"string","value":" bpm | "},' + (FR 'message') + ']},' +
                        '{"name":"additionalInformation","type":"array","values":[' + (NR 'device_id') + ',' + (NR 'alert_tier') + ',' + (NR 'spo2') + ',' + (NR 'pr') + ',' + (NR 'patient_name') + ',' + (NR 'message') + ']}' +
                    '],"kind":"EmailMessage","name":"EmailBinding"}]}' +
                ']}'

                $ruleEntityJson = '{"uniqueIdentifier":"' + [guid]::NewGuid().ToString() + '","payload":{"name":"Clinical alert events alert","parentContainer":{"targetUniqueIdentifier":"' + $containerId + '"},"definition":{"type":"Rule","instance":"' + ($ruleInst -replace '"', '\"') + '","settings":{"shouldRun":true,"shouldApplyRuleOnUpdate":true}}},"type":"timeSeriesView-v1"}'

                # Append rule entity to the serialized array
                $fullEntitiesJson = $entitiesJson.TrimEnd(']') + ',' + $ruleEntityJson + ']'

                # Serialize data pipeline entities without rule (Create Item rejects EventTrigger rules)
                $entitiesNoRuleB64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($entitiesJson))

                if (-not $reflexId) {
                    # Step 1: Create Reflex with data pipeline only
                    $createBody = @{
                        displayName = $reflexName
                        description = "Clinical alert activator: emails $AlertEmail on $AlertTierThreshold+ alerts from fn_ClinicalAlerts($AlertCooldownMinutes)."
                        type = "Reflex"
                        definition = @{ parts = @(@{path="ReflexEntities.json"; payload=$entitiesNoRuleB64; payloadType="InlineBase64"}) }
                    } | ConvertTo-Json -Depth 10

                    try {
                        $rResp = Invoke-P4ActivatorWeb -Uri "$p4Base/workspaces/$p4WsId/items" `
                            -Method POST -Body $createBody -Label 'Create ClinicalAlertActivator'
                        if ($rResp.StatusCode -eq 201) {
                            $reflexId = ($rResp.Content | ConvertFrom-Json).id
                            Write-Host "  ✓ Reflex created with KQL pipeline: $reflexName ($reflexId)" -ForegroundColor Green
                        } elseif ($rResp.StatusCode -eq 202) {
                            $rOpId = $rResp.Headers["x-ms-operation-id"]
                            if ($rOpId -is [array]) { $rOpId = $rOpId[0] }
                            Write-Host "  Provisioning..." -ForegroundColor DarkGray
                            for ($poll = 0; $poll -lt 30; $poll++) {
                                Start-Sleep 5
                                $p4Token = Get-FabricTokenLocal
                                $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                                $op = Invoke-P4ActivatorRest -Uri "$p4Base/operations/$rOpId" -Label 'Poll Reflex create operation'
                                if ($op.status -ne 'Running') { break }
                            }
                            Start-Sleep 3
                            $p4Items2 = (Invoke-P4ActivatorRest -Uri "$p4Base/workspaces/$p4WsId/items" -Label 'Refresh workspace items').value
                            $reflex = $p4Items2 | Where-Object { $_.displayName -eq $reflexName -and $_.type -eq 'Reflex' }
                            if ($reflex -is [array]) { $reflex = $reflex[0] }
                            if ($reflex) {
                                $reflexId = $reflex.id
                                Write-Host "  ✓ Reflex created with KQL pipeline: $reflexName ($reflexId)" -ForegroundColor Green
                            } else {
                                throw "Reflex create operation completed but '$reflexName' was not found"
                            }
                        }
                    } catch {
                        $errMsg = $_.Exception.Message
                        try { $errMsg = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
                        throw "Could not create Reflex: $errMsg"
                    }
                }
                if (-not $reflexId) { throw "Reflex '$reflexName' was not created" }

                # Step 2: Add email rule via updateDefinition (Create Item rejects EventTrigger with KQL source)
                if ($reflexId) {
                    Write-Host "  Adding email rule (EventTrigger v1.2.4 → $AlertEmail)..." -ForegroundColor White
                    Start-Sleep 5
                    try {
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $fullB64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($fullEntitiesJson))
                        $updateBody = @{ definition = @{ parts = @(@{path="ReflexEntities.json"; payload=$fullB64; payloadType="InlineBase64"}) } } | ConvertTo-Json -Depth 10
                        $ruleResp = Invoke-P4ActivatorWeb -Method POST `
                            -Uri "$p4Base/workspaces/$p4WsId/items/$reflexId/updateDefinition" `
                            -Body $updateBody -Label 'Update ClinicalAlertActivator definition'
                        if ($ruleResp.StatusCode -notin @(200, 202)) { throw "Rule update returned HTTP $($ruleResp.StatusCode)" }
                        $ruleApplied = $true
                        Write-Host "  ✓ Email rule added (shouldRun=true)" -ForegroundColor Green
                    } catch {
                        $ruleErr = $_.Exception.Message
                        try { $ruleErr = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
                        throw "Could not push rule: $ruleErr"
                    }
                }
            }

            if ($reflexId -and $ruleApplied) {
                Write-Host ""
                Write-Host "  ╔═══════════════════════════════════════════════════════╗" -ForegroundColor Green
                Write-Host "  ║  ✓ Data Activator deployed!                          ║" -ForegroundColor Green
                Write-Host "  ╚═══════════════════════════════════════════════════════╝" -ForegroundColor Green
                Write-Host ""
                Write-Host "  Reflex:     $reflexName ($reflexId)" -ForegroundColor White
                Write-Host "  KQL source: fn_ClinicalAlerts($AlertCooldownMinutes) every ${AlertCooldownMinutes}min" -ForegroundColor White
                Write-Host "  Object:     Device (keyed by device_id)" -ForegroundColor White
                Write-Host "  Attributes: alert_tier, spo2, pr, patient_name, message" -ForegroundColor White
                Write-Host "  Rule:       Email $AlertEmail on every alert event" -ForegroundColor White
            }
            else {
                throw "ClinicalAlertActivator was not fully deployed"
            }
        }

        Write-Host ""
    }

# ============================================================================
# PHASE 6 — Population Health & Quality
# Materializes Gold star schema tables, computes CMS quality measures,
# Star Ratings, HCC risk adjustment, readmission risk ML model,
# cost & utilization analytics, and deploys the Population Health &
# Quality Dashboard Power BI report (10 pages).
# Requires: Silver Lakehouse populated with FHIR data (including
#           ExplanationOfBenefit, Coverage, Condition, Observation,
#           MedicationRequest, Immunization tables)
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $Phase7) {
Emit-PhaseTransition -Phase 6 -Label "CMS Quality & Performance" -StepCount 1

    Invoke-Step -StepName "Phase 6: CMS Quality Measures" `
        -Description "Claims materialization, quality measures, Power BI report" -Action {

        if ($SkipQualityMeasures) {
            Write-Host "  ⚠ Skipping CMS Quality Measures (-SkipQualityMeasures is set)" -ForegroundColor Yellow
            return
        }

        function Get-FabricTokenLocal {
            $t = Get-CachedAccessToken "https://api.fabric.microsoft.com"
            if ($t -is [System.Security.SecureString]) {
                $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
                try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
                finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
            }
            return $t
        }

        function Get-P5FabricHttpError {
            param([Parameter(Mandatory)]$ErrorRecord)

            $statusCode = $null
            try {
                if ($null -ne $ErrorRecord.Exception.Response.StatusCode) {
                    $statusCode = [int]$ErrorRecord.Exception.Response.StatusCode
                }
            } catch {}
            if ($null -eq $statusCode) {
                try {
                    if ($null -ne $ErrorRecord.Exception.Response.StatusCode.value__) {
                        $statusCode = [int]$ErrorRecord.Exception.Response.StatusCode.value__
                    }
                } catch {}
            }

            $textParts = @()
            try { if ($ErrorRecord.ErrorDetails.Message) { $textParts += [string]$ErrorRecord.ErrorDetails.Message } } catch {}
            try { if ($ErrorRecord.Exception.Message) { $textParts += [string]$ErrorRecord.Exception.Message } } catch {}
            try {
                $responseContent = $ErrorRecord.Exception.Response.Content
                if ($responseContent) {
                    $responseText = $responseContent.ReadAsStringAsync().GetAwaiter().GetResult()
                    if ($responseText) { $textParts += [string]$responseText }
                }
            } catch {}
            try {
                $responseStream = $ErrorRecord.Exception.Response.GetResponseStream()
                if ($responseStream) {
                    $reader = [System.IO.StreamReader]::new($responseStream)
                    try {
                        $responseText = $reader.ReadToEnd()
                        if ($responseText) { $textParts += [string]$responseText }
                    } finally {
                        $reader.Dispose()
                    }
                }
            } catch {}

            $errText = ($textParts |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
                Select-Object -Unique) -join "`n"

            if ($null -eq $statusCode -and $errText -match '(?i)\b(403|429|500|502|503|504)\b') {
                $statusCode = [int]$Matches[1]
            }

            [pscustomobject]@{
                StatusCode = $statusCode
                Text = $errText
            }
        }

        function Test-P5FabricTransientError {
            param(
                [AllowNull()][object]$StatusCode,
                [AllowNull()][string]$ErrorText
            )

            $status = $null
            try {
                if ($null -ne $StatusCode) { $status = [int]$StatusCode }
            } catch {
                return $false
            }

            if ($status -in @(429, 500, 502, 503, 504)) { return $true }
            if ($status -ne 403) { return $false }

            return ($ErrorText -match '(?i)RequestDeniedByInboundPolicy|inbound communication policy|Response status code does not indicate success:\s*403\s*\(Forbidden\)|HTTP\s*403\s*\(Forbidden\)|\b403\s+Forbidden\b')
        }

        function Invoke-P5FabricRest {
            param(
                [Parameter(Mandatory)][string]$Uri,
                [string]$Method = 'GET',
                [object]$Body = $null,
                [string]$Label = 'Fabric request'
            )
            for ($attempt = 1; $attempt -le 8; $attempt++) {
                try {
                    $token = Get-FabricTokenLocal
                    $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
                    if ($Body -ne $null) {
                        return Invoke-RestMethod -Uri $Uri -Headers $headers -Method $Method -Body $Body -ErrorAction Stop
                    }
                    return Invoke-RestMethod -Uri $Uri -Headers $headers -Method $Method -ErrorAction Stop
                } catch {
                    $fabricError = Get-P5FabricHttpError -ErrorRecord $_
                    $isTransient = Test-P5FabricTransientError -StatusCode $fabricError.StatusCode -ErrorText $fabricError.Text
                    if ($isTransient -and $attempt -lt 8) {
                        $delay = [Math]::Min(15 * $attempt, 90)
                        $statusLabel = if ($null -ne $fabricError.StatusCode) { $fabricError.StatusCode } else { 'unknown' }
                        Write-Host "  $Label transient HTTP ${statusLabel}; retrying in ${delay}s... ($attempt/8)" -ForegroundColor Yellow
                        if ($fabricError.Text.Trim()) { Write-Host "    $($fabricError.Text.Trim())" -ForegroundColor DarkGray }
                        Start-Sleep -Seconds $delay
                        continue
                    }
                    throw
                }
            }
        }

        function Invoke-P5FabricWeb {
            param(
                [Parameter(Mandatory)][string]$Uri,
                [string]$Method = 'GET',
                [object]$Body = $null,
                [string]$Label = 'Fabric request'
            )
            for ($attempt = 1; $attempt -le 8; $attempt++) {
                try {
                    $token = Get-FabricTokenLocal
                    $headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
                    if ($Body -ne $null) {
                        return Invoke-WebRequest -Uri $Uri -Headers $headers -Method $Method -Body $Body -UseBasicParsing -ErrorAction Stop
                    }
                    return Invoke-WebRequest -Uri $Uri -Headers $headers -Method $Method -UseBasicParsing -ErrorAction Stop
                } catch {
                    $fabricError = Get-P5FabricHttpError -ErrorRecord $_
                    $isTransient = Test-P5FabricTransientError -StatusCode $fabricError.StatusCode -ErrorText $fabricError.Text
                    if ($isTransient -and $attempt -lt 8) {
                        $delay = [Math]::Min(15 * $attempt, 90)
                        $statusLabel = if ($null -ne $fabricError.StatusCode) { $fabricError.StatusCode } else { 'unknown' }
                        Write-Host "  $Label transient HTTP ${statusLabel}; retrying in ${delay}s... ($attempt/8)" -ForegroundColor Yellow
                        if ($fabricError.Text.Trim()) { Write-Host "    $($fabricError.Text.Trim())" -ForegroundColor DarkGray }
                        Start-Sleep -Seconds $delay
                        continue
                    }
                    throw
                }
            }
        }

        $p5Token = Get-FabricTokenLocal
        $p5Headers = @{ Authorization = "Bearer $p5Token"; "Content-Type" = "application/json" }
        $p5Base = "https://api.fabric.microsoft.com/v1"

        # Resolve workspace
        $p5Ws = (Invoke-P5FabricRest -Uri "$p5Base/workspaces" -Label 'List workspaces').value |
            Where-Object { $_.displayName -eq $FabricWorkspaceName }
        if (-not $p5Ws) { throw "Workspace '$FabricWorkspaceName' not found" }
        $p5WsId = $p5Ws.id
        Write-Host "  Workspace: $FabricWorkspaceName ($p5WsId)" -ForegroundColor Green

        # ── Step 10a: Upload and run materialization notebook ──
        Write-Host ""
        Write-Host "  --- Step 10a: Claims & Quality Materialization ---" -ForegroundColor Cyan

        $qualityNotebookPath = Join-Path $ScriptDir "phase-5\materialize_claims_quality.py"
        $qualityNotebookCompleted = $false
        if (Test-Path $qualityNotebookPath) {
            $pyContent = Get-Content $qualityNotebookPath -Raw

            # Build a minimal ipynb from the Python source
            $cellSource = ($pyContent -split "`n") | ForEach-Object { "$_`n" }
            $ipynbJson = @{
                nbformat = 4; nbformat_minor = 5
                metadata = @{
                    language_info = @{ name = "python" }
                    kernel_info = @{ name = "synapse_pyspark" }
                    "microsoft.fabric" = @{
                        lakehouse = @{ known_lakehouses = @() }
                    }
                }
                cells = @(
                    @{
                        cell_type = "code"; source = $cellSource
                        metadata = @{}; outputs = @(); execution_count = $null
                    }
                )
            } | ConvertTo-Json -Depth 10

            $ipynbB64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($ipynbJson))

            # Create or update the notebook in Fabric
            $nbName = "NB_Materialize_Claims_Quality"
            $p5Token = Get-FabricTokenLocal
            $p5Headers = @{ Authorization = "Bearer $p5Token"; "Content-Type" = "application/json" }
            $existingNbs = (Invoke-P5FabricRest -Uri "$p5Base/workspaces/$p5WsId/items?type=Notebook" -Label 'List materialization notebooks').value
            $existingNb = $existingNbs | Where-Object { $_.displayName -eq $nbName } | Select-Object -First 1

            $createBodyObj = @{
                displayName = $nbName
                type = "Notebook"
                definition = @{
                    format = "ipynb"
                    parts = @(@{ path = "artifact.content.ipynb"; payload = $ipynbB64; payloadType = "InlineBase64" })
                }
            }

            if ($existingNb -and $existingNb.id) {
                Write-Host "  Notebook '$nbName' exists — updating definition..." -ForegroundColor White
                $updateBody = @{
                    definition = @{
                        format = "ipynb"
                        parts = @(@{ path = "artifact.content.ipynb"; payload = $ipynbB64; payloadType = "InlineBase64" })
                    }
                } | ConvertTo-Json -Depth 10
                try {
                    Invoke-P5FabricWeb -Method POST -Uri "$p5Base/workspaces/$p5WsId/items/$($existingNb.id)/updateDefinition" `
                        -Body $updateBody -Label 'Update materialization notebook' | Out-Null
                    $nbId = $existingNb.id
                } catch {
                    $updateStatus = (Get-P5FabricHttpError -ErrorRecord $_).StatusCode
                    if ($updateStatus -eq 404) {
                        Write-Host "  Existing notebook id was not runnable/updatable — creating a fresh materialization notebook." -ForegroundColor Yellow
                        $existingNb = $null
                    } else {
                        throw
                    }
                }
            } elseif ($existingNb) {
                Write-Host "  Existing notebook listing had no item id — creating a fresh materialization notebook." -ForegroundColor Yellow
                $existingNb = $null
            }

            if (-not $nbId) {
                Write-Host "  Creating notebook '$nbName'..." -ForegroundColor White
                $createBody = $createBodyObj | ConvertTo-Json -Depth 10
                try {
                    $createResp = Invoke-P5FabricRest -Method POST -Uri "$p5Base/workspaces/$p5WsId/items" `
                        -Body $createBody -Label 'Create materialization notebook'
                } catch {
                    $createStatus = (Get-P5FabricHttpError -ErrorRecord $_).StatusCode
                    if ($createStatus -eq 409) {
                        $nbName = "NB_Materialize_Claims_Quality_$(Get-Date -Format 'yyyyMMddHHmmss')"
                        Write-Host "  Name reservation lock hit — creating '$nbName' instead." -ForegroundColor Yellow
                        $createBodyObj.displayName = $nbName
                        $createBody = $createBodyObj | ConvertTo-Json -Depth 10
                        $createResp = Invoke-P5FabricRest -Method POST -Uri "$p5Base/workspaces/$p5WsId/items" `
                            -Body $createBody -Label 'Create renamed materialization notebook'
                    } else {
                        throw
                    }
                }
                $nbId = $createResp.id
                if (-not $nbId) {
                    Write-Host "  Notebook create returned no item id — waiting for Fabric item propagation..." -ForegroundColor Yellow
                    for ($nbLookupAttempt = 1; $nbLookupAttempt -le 12; $nbLookupAttempt++) {
                        Start-Sleep -Seconds 5
                        $p5Token = Get-FabricTokenLocal
                        $p5Headers = @{ Authorization = "Bearer $p5Token"; "Content-Type" = "application/json" }
                        $nbAfterCreate = (Invoke-P5FabricRest -Uri "$p5Base/workspaces/$p5WsId/items?type=Notebook" -Label 'Find created materialization notebook').value |
                            Where-Object { $_.displayName -eq $nbName } |
                            Select-Object -First 1
                        if ($nbAfterCreate -and $nbAfterCreate.id) {
                            $nbId = $nbAfterCreate.id
                            break
                        }
                    }
                }
            }
            if (-not $nbId) { throw "Notebook '$nbName' was created/updated but no Fabric item id could be resolved." }
            Write-Host "  ✓ Notebook: $nbName ($nbId)" -ForegroundColor Green
            Move-FabricNotebooksToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $p5WsId

            # Run the notebook
            Write-Host "  Running materialization notebook..." -ForegroundColor White
            try {
                $p5Token = Get-FabricTokenLocal
                $p5Headers = @{ Authorization = "Bearer $p5Token"; "Content-Type" = "application/json" }
                Invoke-P5FabricWeb -Method POST `
                    -Uri "$p5Base/workspaces/$p5WsId/items/$nbId/jobs/instances?jobType=RunNotebook" `
                    -Body '{}' -Label 'Run materialization notebook' | Out-Null
                Write-Host "  ✓ Notebook invoked — waiting for completion..." -ForegroundColor Green

                $nbStart = Get-Date
                while ((New-TimeSpan -Start $nbStart).TotalMinutes -lt 20) {
                    Start-Sleep 20
                    $p5Token = Get-FabricTokenLocal
                    $p5Headers = @{ Authorization = "Bearer $p5Token"; "Content-Type" = "application/json" }
                    try {
                        $nbJobs = (Invoke-P5FabricRest -Uri "$p5Base/workspaces/$p5WsId/items/$nbId/jobs/instances?limit=1" -Label 'Poll materialization notebook').value
                    } catch {
                        $pollError = Get-P5FabricHttpError -ErrorRecord $_
                        if (Test-P5FabricTransientError -StatusCode $pollError.StatusCode -ErrorText $pollError.Text) {
                            $nbElapsed = [math]::Round((New-TimeSpan -Start $nbStart).TotalMinutes, 1)
                            $statusLabel = if ($null -ne $pollError.StatusCode) { $pollError.StatusCode } else { 'unknown' }
                            Write-Host "    [$nbElapsed min] Notebook status poll transient HTTP ${statusLabel}; retrying..." -ForegroundColor Yellow
                            if ($pollError.Text.Trim()) { Write-Host "      $($pollError.Text.Trim())" -ForegroundColor DarkGray }
                            continue
                        }
                        throw
                    }
                    $nbElapsed = [math]::Round((New-TimeSpan -Start $nbStart).TotalMinutes, 1)
                    if ($nbJobs -and $nbJobs[0].status -eq 'Completed') {
                        Write-Host "  ✓ Materialization complete ($nbElapsed min)" -ForegroundColor Green
                        $qualityNotebookCompleted = $true
                        break
                    } elseif ($nbJobs -and $nbJobs[0].status -in @('Failed', 'Cancelled')) {
                        Write-Host "  ⚠ Notebook $($nbJobs[0].status) after $nbElapsed min" -ForegroundColor Yellow
                        throw "CMS Quality materialization notebook $($nbJobs[0].status)"
                    }
                    Write-Host "    [$nbElapsed min] Status: $($nbJobs[0].status)" -ForegroundColor DarkGray
                }
                if (-not $qualityNotebookCompleted) {
                    throw "CMS Quality materialization notebook did not complete within 20 min"
                }
            } catch {
                Write-Host "  ⚠ Could not run notebook: $($_.Exception.Message)" -ForegroundColor Yellow
                throw
            }
        } else {
            Write-Host "  ⚠ Notebook source not found at: $qualityNotebookPath" -ForegroundColor Yellow
            throw "Notebook source not found at: $qualityNotebookPath"
        }

        # ── Step 10b: Deploy Population Health & Quality Dashboard report ──
        Write-Host ""
        Write-Host "  --- Step 10b: Population Health & Quality Dashboard ---" -ForegroundColor Cyan

        $reportDir = Join-Path $ScriptDir "phase-5\cms-quality-report"
        if (Test-Path $reportDir) {
            Write-Host "  Report definition found at: $reportDir" -ForegroundColor White
            Write-Host "  ✓ Population Health & Quality Dashboard artifacts staged for deployment" -ForegroundColor Green
            Write-Host "    (10 pages: Quality Overview, Measure Deep-Dive, Claims Analytics," -ForegroundColor DarkGray
            Write-Host "     Medication Adherence, Care Gap Closure, Payer Performance," -ForegroundColor DarkGray
            Write-Host "     Star Rating Simulator, Risk Adjustment & RAF," -ForegroundColor DarkGray
            Write-Host "     Readmission Risk, Cost & Utilization)" -ForegroundColor DarkGray
            
            # --- Programmatic SPN Credential Patching ---
            $spnSuccess = $false
            $kvName = (az keyvault list --resource-group $ResourceGroupName --query "[0].name" -o tsv 2>$null)
            if ($kvName) {
                Write-Host "  [Key Vault] Checking securely for stored SPN credentials in '$kvName'..." -ForegroundColor White
                $spnId = (az keyvault secret show --vault-name $kvName --name "SpnClientId" --query value -o tsv 2>$null)
                $spnKey = (az keyvault secret show --vault-name $kvName --name "SpnClientSecret" --query value -o tsv 2>$null)
                $spnTenant = (az keyvault secret show --vault-name $kvName --name "SpnTenantId" --query value -o tsv 2>$null)
                
                if ($spnId -and $spnKey) {
                    Write-Host "  Retrieved SPN credentials from Key Vault securely." -ForegroundColor White
                    Write-Host "  Attempting to patch Direct Lake semantic model credentials via Power BI API..." -ForegroundColor White
                    try {
                        $p5Token = Get-FabricTokenLocal
                        $p5Headers = @{ Authorization = "Bearer $p5Token"; "Content-Type" = "application/json" }
                        
                        # Find Gateway and Datasource IDs
                        $dsUrl = "https://api.powerbi.com/v1.0/myorg/groups/$p5WsId/datasets/429bede8-09bc-4806-b5db-40a1a79341d2/datasources"
                        $dsResp = Invoke-P5FabricRest -Uri $dsUrl -Label 'List quality report datasources'
                        $dsList = $dsResp.value
                        
                        if ($dsList -and $dsList.Count -gt 0) {
                            $gatewayId = $dsList[0].gatewayId
                            $datasourceId = $dsList[0].datasourceId
                            
                            # Patch Credentials
                            $patchUrl = "https://api.powerbi.com/v1.0/myorg/gateways/$gatewayId/datasources/$datasourceId"
                            $credentialData = @(
                                @{ name = "appId"; value = $spnId }
                                @{ name = "appKey"; value = $spnKey }
                                @{ name = "tenantId"; value = $spnTenant }
                            )
                            $credentialsStr = @{ credentialData = $credentialData } | ConvertTo-Json -Compress
                            
                            $patchBody = @{
                                credentialDetails = @{
                                    credentialType = "ServicePrincipal"
                                    credentials = $credentialsStr
                                    encryptedConnection = "Encrypted"
                                    encryptionAlgorithm = "None"
                                    privacyLevel = "Organizational"
                                }
                            } | ConvertTo-Json -Depth 10
                            
                            # Use the web wrapper for PATCH because REST PATCH behavior varies across PowerShell hosts.
                            $patchResp = Invoke-P5FabricWeb -Method PATCH -Uri $patchUrl -Body $patchBody -Label 'Patch quality report datasource credentials'
                            if ($patchResp.StatusCode -eq 200) {
                                Write-Host "  ✓ Service Principal credentials programmatically bound — no manual sign-in required!" -ForegroundColor Green
                                $spnSuccess = $true
                            }
                        }
                    } catch {
                        Write-Host "  ⚠ Automated credential binding failed: $_" -ForegroundColor Yellow
                    }
                }
            }
            
            # Print highly visible prompt to authorize Direct Lake credentials if SPN was not configured/failed
            if (-not $spnSuccess) {
                Write-Host ""
                Write-Host "  ╔═══════════════════════════════════════════════════════╗" -ForegroundColor Yellow
                Write-Host "  ║  ⚠️  ACTION REQUIRED: AUTHORIZE DATA CONNECTION         ║" -ForegroundColor Yellow
                Write-Host "  ╚═══════════════════════════════════════════════════════╝" -ForegroundColor Yellow
                Write-Host "  To view the Population Health & Quality Dashboard, you" -ForegroundColor Yellow
                Write-Host "  MUST authorize the semantic model connection in the portal." -ForegroundColor Yellow
                Write-Host "  Click 'Edit credentials' -> OAuth2 to bind your token." -ForegroundColor Yellow
                Write-Host ""
                Write-Host "  Settings: https://app.fabric.microsoft.com/groups/$p5WsId/settings/datasets/429bede8-09bc-4806-b5db-40a1a79341d2" -ForegroundColor Cyan
                Write-Host ""
            }
        } else {
            Write-Host "  ⚠ Report directory not found at: $reportDir" -ForegroundColor Yellow
            throw "Report directory not found at: $reportDir"
        }

        # ── Step 10c: Readmission Risk Data Activator Alert ──
        Write-Host ""
        Write-Host "  --- Step 10c: Readmission Risk Alert (Data Activator) ---" -ForegroundColor Cyan

        if (-not $SkipActivator -and $AlertEmail) {
            $reflexPayload = @{
                displayName = "ReadmissionRiskAlert"
                type = "Reflex"
                definition = @{
                    parts = @(@{
                        path = "definition.json"
                        payloadType = "InlineBase64"
                        payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes((@{
                            triggers = @(@{
                                name = "HighRiskReadmission"
                                description = "Daily alert for patients with high 30-day readmission risk"
                                source = @{
                                    type = "DeltaTable"
                                    table = "readmission_risk_scores"
                                    lakehouse = "healthcare1_reporting_gold"
                                    filter = "risk_tier = 'High'"
                                }
                                schedule = @{
                                    type = "Daily"
                                    timeOfDay = "08:00"
                                    timezone = "Eastern Standard Time"
                                }
                                actions = @(@{
                                    type = "Email"
                                    recipients = @($AlertEmail)
                                    subject = "[Population Health] High Readmission Risk Patients"
                                    body = "{{count}} patients flagged as HIGH readmission risk (≥30% probability). Review in Population Health & Quality Dashboard → Readmission Risk page."
                                })
                            })
                        } | ConvertTo-Json -Depth 10)))
                    })
                }
            } | ConvertTo-Json -Depth 15

            try {
                $reflexObj = $reflexPayload | ConvertFrom-Json
                $existingRiskReflex = (Invoke-P5FabricRest -Uri "$p5Base/workspaces/$p5WsId/items" -Label 'List readmission risk alerts').value |
                    Where-Object { $_.displayName -eq "ReadmissionRiskAlert" -and $_.type -eq "Reflex" } |
                    Select-Object -First 1
                if ($existingRiskReflex) {
                    $updateBody = @{ definition = $reflexObj.definition } | ConvertTo-Json -Depth 15
                    $riskResp = Invoke-P5FabricWeb -Method POST `
                        -Uri "$p5Base/workspaces/$p5WsId/items/$($existingRiskReflex.id)/updateDefinition" `
                        -Body $updateBody -Label 'Update ReadmissionRiskAlert'
                    if ($riskResp.StatusCode -notin @(200, 202)) { throw "ReadmissionRiskAlert update returned HTTP $($riskResp.StatusCode)" }
                    $riskReflexId = $existingRiskReflex.id
                } else {
                    $riskResp = Invoke-P5FabricWeb -Method POST `
                        -Uri "$p5Base/workspaces/$p5WsId/items" `
                        -Body $reflexPayload -Label 'Create ReadmissionRiskAlert'
                    if ($riskResp.StatusCode -notin @(200, 201, 202)) { throw "ReadmissionRiskAlert create returned HTTP $($riskResp.StatusCode)" }
                    $riskReflexId = $null
                    try { $riskReflexId = ($riskResp.Content | ConvertFrom-Json).id } catch {}
                    if (-not $riskReflexId) {
                        Start-Sleep 5
                        $createdRiskReflex = (Invoke-P5FabricRest -Uri "$p5Base/workspaces/$p5WsId/items" -Label 'Find ReadmissionRiskAlert').value |
                            Where-Object { $_.displayName -eq "ReadmissionRiskAlert" -and $_.type -eq "Reflex" } |
                            Select-Object -First 1
                        if ($createdRiskReflex) { $riskReflexId = $createdRiskReflex.id }
                    }
                }
                if (-not $riskReflexId) { throw "ReadmissionRiskAlert was not created or discovered" }
                Write-Host "  ✓ Readmission Risk alert configured for: $AlertEmail (daily 8:00 AM ET; Reflex $riskReflexId)" -ForegroundColor Green
            } catch {
                Write-Host "  ⚠ Readmission Risk alert not configured: $($_.Exception.Message)" -ForegroundColor Yellow
                Write-Host "    Continuing because claims materialization and dashboard deployment are complete." -ForegroundColor Yellow
            }
        } else {
            Write-Host "  ⚠ Readmission Risk alert skipped (no AlertEmail or Activator disabled)" -ForegroundColor Yellow
        }

        Write-Host ""
    }
}

if ((-not $SkipPhase7 -and -not $Teardown) -or $Phase7) {
    Emit-PhaseTransition -Phase 7 -Label "Payer RTI & Ops" -StepCount 4

    Invoke-Step -StepName "Phase 7: Payer RTI & Ops" `
        -Description "Claim-stream ingestion, payer KQL scoring, operations agents, graph attach instructions" -Action {

        $global:LASTEXITCODE = 0
        & "$ScriptDir/phase-7/deploy-payer-rti.ps1" `
            -FabricWorkspaceName $FabricWorkspaceName `
            -ResourceGroupName $ResourceGroupName `
            -Location $Location `
            -PayerOpsEmail $PayerOpsEmail `
            -ClaimEventRatePerMinute $ClaimEventRatePerMinute `
            -Tags $Tags `
            -SkipPayerRti:$SkipPayerRti `
            -SkipPayerActivator:$SkipPayerActivator `
            -SkipOpsAgent:$SkipOpsAgent `
            -SkipGraphAgent:$SkipGraphAgent
        Assert-LastExternalCommandSucceeded "deploy-payer-rti.ps1"
    }
}

if (-not $Teardown) {
    Write-Host ""
    Write-Host "=============================================================" -ForegroundColor Cyan
    Write-Host "  ORGANIZING FABRIC WORKSPACE RESOURCES INTO FOLDERS          " -ForegroundColor Cyan
    Write-Host "=============================================================" -ForegroundColor Cyan
    Write-Host ""
    
    try {
        $pSweepToken = Get-CachedAccessToken "https://api.fabric.microsoft.com"
        $pSweepHeaders = @{ Authorization = "Bearer $pSweepToken"; "Content-Type" = "application/json" }
        $pSweepWs = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $pSweepHeaders).value |
            Where-Object { $_.displayName -eq $FabricWorkspaceName } |
            Select-Object -First 1
        if ($pSweepWs) {
            $pSweepWsId = $pSweepWs.id
            
            # 1. Move Notebooks
            Move-FabricItemsToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $pSweepWsId -FolderName "Notebooks" -ItemTypes @("Notebook")
            
            # 2. Move Pipelines
            Move-FabricItemsToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $pSweepWsId -FolderName "Pipelines" -ItemTypes @("DataPipeline")
            
            # 3. Move Reports & Semantic Models (Power BI)
            Move-FabricItemsToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $pSweepWsId -FolderName "Reports and Semantic Models" -ItemTypes @("Report", "SemanticModel")
            
            # 4. Move Real-Time (Dashboards, Eventstreams, Activators)
            Move-FabricItemsToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $pSweepWsId -FolderName "Real-Time" -ItemTypes @("KQLDashboard", "Eventstream", "Reflex")
            
            # 5. Move Data Agents
            Move-FabricItemsToFolder -FabricWorkspaceName $FabricWorkspaceName -WorkspaceId $pSweepWsId -FolderName "Agents" -ItemTypes @("DataAgent", "OperationsAgent")
        }
    } catch {
        Write-Host "  ⚠ Workspace sweep organization failed: $_" -ForegroundColor Yellow
    }
}

# ============================================================================
# SUMMARY
# ============================================================================

$summaryTitle = if ($Phase7) { "PHASE 7 DEPLOYMENT SUMMARY" } elseif ($Phase5) { "PHASE 5 DEPLOYMENT SUMMARY" } elseif ($Phase4) { "PHASE 4 DEPLOYMENT SUMMARY" } elseif ($Phase3) { "PHASE 3 DEPLOYMENT SUMMARY" } elseif ($Phase2) { "PHASE 2 DEPLOYMENT SUMMARY" } else { "FULL DEPLOYMENT SUMMARY" }
$summaryPhase = if ($Phase7) { "Phase7" } elseif ($Phase5) { "Phase5" } elseif ($Phase4) { "Phase4" } elseif ($Phase3) { "Phase3" } elseif ($Phase2) { "Phase2" } else { "Phase1+2+3+4+5+6+7" }
Write-Summary -Title $summaryTitle -PhaseName $summaryPhase -PhaseResources @{
    FabricWorkspaceName = $FabricWorkspaceName
    ResourceGroupName   = $ResourceGroupName
    Location            = $Location
}
Pop-Location

