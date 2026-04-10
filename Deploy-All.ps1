# Deploy-All.ps1
# End-to-end orchestrator for the Masimo Medical Device + Fabric RTI pipeline.
#
# Sequence:
#   ── Phase 1 ──
#   Step 1   — Fabric workspace + identity (created first so HDS can be deployed in parallel)
#   Step 1b  — Base Azure infrastructure (Event Hub, ACR, emulator container)
#   Step 2   — FHIR Service + Synthea patient generation + FHIR data load
#   Step 2b  — DICOM infrastructure + TCIA download, re-tag, upload
#   Step 3   — Fabric RTI Phase 1 (Eventhouse, KQL, Eventstream, FHIR $export)
#   Step 4   — (Manual) Deploy HDS in Fabric portal
#   ── Phase 2 ──
#   Step 5   — Fabric RTI Phase 2 (Bronze shortcut, KQL shortcuts, enriched alerts)
#   Step 5b  — DICOM shortcut + HDS pipelines (clinical, imaging, OMOP)
#   Step 6   — Data Agents (Patient 360 + Clinical Triage)
#   ── Phase 3 ──
#   Step 7   — Cohorting Agent + DICOM Viewer + Imaging Report
#              (requires Gold OMOP pipeline to have completed)
#   ── Phase 4 ──
#   Step 8   — Ontology (DeviceAssociation table + ClinicalDeviceOntology + agent binding)
#   Step 9   — Data Activator (ClinicalAlertActivator Reflex + email rule)
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

    # ── Fabric Phase 2 (post-HDS) ──
    [string]$SilverLakehouseId = "",
    [string]$SilverLakehouseName = "",

    # ── Step control ──
    [switch]$SkipBaseInfra,          # Skip deploy.ps1 (emulator infra already exists)
    [switch]$SkipFhir,               # Skip deploy-fhir.ps1 (FHIR data already loaded)
    [switch]$SkipDicom,              # Skip DICOM infra + loader
    [switch]$SkipFabric,             # Skip deploy-fabric-rti.ps1 entirely
    [switch]$Phase2,             # Run only Fabric Phase 2
    [switch]$Phase3,             # Run only Phase 3 (Cohorting Agent + DICOM Viewer)
    [switch]$Phase4,             # Run only Phase 4 (Ontology + Agent binding)
    [switch]$RebuildContainers,      # Force container image rebuilds
    [switch]$ReusePatients,          # Reuse existing patients — skip Synthea/Loader, keep emulator
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
    [switch]$SkipActivator,          # Skip Data Activator (Reflex + email rule)

    # ── Phase 3 (FabricDicomCohortingToolkit) ──
    [string]$DicomToolkitPath = "C:\git\FabricDicomCohortingToolkit",
    [string]$DicomViewerResourceGroup = "rg-hds-dicom-viewer",

    # ── Phase 4 (Activator) ──
    [string]$AlertEmail = "",               # Email for clinical alert notifications (e.g. joey@brakeat.com)
    [string]$AlertTierThreshold = "URGENT", # Minimum tier to send email: WARNING, URGENT, or CRITICAL
    [int]$AlertCooldownMinutes = 15,         # Suppress duplicate alerts per device within this window

    # ── Cleanup ──
    [switch]$Teardown                # Run cleanup scripts instead of deployment
)

$ErrorActionPreference = "Stop"

# Validate conditionally-required parameters
if (-not $Teardown -and -not $Phase2 -and -not $Phase3 -and -not $Phase4 -and -not $AdminSecurityGroup) {
    throw "Parameter '-AdminSecurityGroup' is required for deployment. Only -Teardown, -Phase2, -Phase3, and -Phase4 can omit it."
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $ScriptDir

# ── Granular flag reconciliation ──────────────────────────────────────
# Fine-grained skip flags from the Orchestrator UI are translated into
# the coarser switches consumed by the existing step logic.
# SkipSynthea implies the FHIR loader has nothing new to load; however
# the FHIR *service* infra still deploys so HDS / $export keep working.
if ($SkipSynthea) { $ReusePatients = $true }
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
        Write-Host "  ✓ Azure CLI $cliVer" -ForegroundColor Green
    } catch {
        $failures += "Azure CLI not found. Install from https://aka.ms/installazurecli"
        Write-Host "  ✗ Azure CLI not installed" -ForegroundColor Red
    }

    # 4. Bicep
    try {
        $bicepOutput = (az bicep version 2>$null) -join ' '
        if ($bicepOutput -match "(\d+\.\d+\.\d+)") {
            Write-Host "  ✓ Bicep $($Matches[1])" -ForegroundColor Green
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
    if ($AdminSecurityGroup -and -not $Phase2 -and -not $Phase3 -and -not $Phase4) {
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
        $fabToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
        if ($fabToken -is [System.Security.SecureString]) {
            $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($fabToken)
            try { $fabToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
            finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
        }
        $fabHeaders = @{ "Authorization" = "Bearer $fabToken" }
        $caps = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/capacities" -Headers $fabHeaders
        $activeCaps = $caps.value | Where-Object { $_.state -eq "Active" -and $_.sku -ne "PP3" }
        $paidCaps = $activeCaps | Where-Object { $_.sku -like "F*" -and $_.sku -ne "FT1" }

        if ($paidCaps.Count -gt 0) {
            $cap = $paidCaps | Select-Object -First 1
            Write-Host "  ✓ Fabric capacity: $($cap.displayName) (SKU: $($cap.sku))" -ForegroundColor Green
        } elseif ($activeCaps.Count -gt 0) {
            $cap = $activeCaps | Select-Object -First 1
            $failures += "Trial capacity ($($cap.sku)) cannot deploy Healthcare Data Solutions. A paid F-SKU (F2+) is required."
            Write-Host "  ✗ Fabric capacity: $($cap.displayName) (SKU: $($cap.sku)) — trial not supported" -ForegroundColor Red
        } else {
            $failures += "No active Fabric capacity found. Resume or create one at https://app.fabric.microsoft.com"
            Write-Host "  ✗ No active Fabric capacity" -ForegroundColor Red
        }
    } catch {
        $failures += "Cannot access Fabric API. Ensure Az module login has Fabric permissions."
        Write-Host "  ✗ Fabric API unreachable: $($_.Exception.Message)" -ForegroundColor Red
    }

    # 10. DicomToolkitPath (Phase 3 only)
    if ($Phase3 -and $DicomToolkitPath) {
        if (Test-Path $DicomToolkitPath) {
            Write-Host "  ✓ DICOM Toolkit found at $DicomToolkitPath" -ForegroundColor Green
        } else {
            $failures += "DICOM Toolkit not found at '$DicomToolkitPath'. Clone from GitHub or set -DicomToolkitPath"
            Write-Host "  ✗ DICOM Toolkit not found at $DicomToolkitPath" -ForegroundColor Red
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
# ============================================================================

$stateFile = Join-Path $ScriptDir ".deployment-state-$FabricWorkspaceName.json"

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
            Write-Host "  Resources (from .deployment-state-$FabricWorkspaceName.json):" -ForegroundColor DarkGray
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
        # Get a fresh Fabric token for API calls (self-contained, no dependency on Get-FabricTokenLocal)
        $fabTokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        $fabToken = $fabTokenObj.Token
        if ($fabToken -is [System.Security.SecureString]) {
            $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($fabToken)
            try { $fabToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
            finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
        }

        # Get a fresh token for SQL access — try multiple resource URL formats
        $sqlToken = $null
        foreach ($sqlResource in @("https://database.windows.net/", "https://database.windows.net/.default", "https://database.windows.net")) {
            try {
                $sqlTokenObj = Get-AzAccessToken -ResourceUrl $sqlResource -ErrorAction Stop
                $sqlToken = $sqlTokenObj.Token
                if ($sqlToken -is [System.Security.SecureString]) {
                    $bstr2 = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($sqlToken)
                    try { $sqlToken = [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr2) }
                    finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr2) }
                }
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

    Write-Summary -PhaseName "Teardown"
    Pop-Location
    exit 0
}

# ============================================================================
# PHASE 2 ONLY MODE
# ============================================================================

if ($Phase2) {
    Emit-PhaseTransition -Phase 2 -Label "Analytics & AI Agents" -StepCount 3

    if (-not $SkipRtiPhase2) {
    Invoke-Step -StepName "Phase 2: Fabric RTI" `
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
    } # end if (-not $SkipRtiPhase2)

    # DICOM shortcut + HDS pipelines (clinical, imaging, OMOP)
    if (-not $SkipDicom -and -not $SkipHdsPipelines) {
        Invoke-Step -StepName "Phase 2: DICOM Shortcut + HDS Pipelines" `
            -Description "Shortcut for DICOM data, then run clinical, imaging, and OMOP pipelines" -Action {
            & "$ScriptDir\storage-access-trusted-workspace.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName `
                -ResourceGroupName $ResourceGroupName
        }
    }

    # Data Agents (Patient 360 + Clinical Triage) — part of Phase 2
    if (-not $SkipDataAgents) {
    Invoke-Step -StepName "Phase 2: Data Agents" `
        -Description "Deploy Patient 360 + Clinical Triage agents" -Action {
        Write-Host "  This step will:" -ForegroundColor White
        Write-Host "    [1/2] Create/update Patient 360 Data Agent" -ForegroundColor DarkGray
        Write-Host "    [2/2] Create/update Clinical Triage Data Agent" -ForegroundColor DarkGray
        Write-Host "  Architecture: KQL (TelemetryRaw + AlertHistory) + Lakehouse (Silver tables)" -ForegroundColor DarkGray
        Write-Host ""

        & "$ScriptDir\deploy-data-agents.ps1" `
            -FabricWorkspaceName $FabricWorkspaceName
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

# ============================================================================
# STEP 1 — FABRIC WORKSPACE + IDENTITY (created first so HDS can be deployed during Azure steps)
# ============================================================================

Emit-PhaseTransition -Phase 1 -Label "Infrastructure & Data" -StepCount 6

if (-not $Phase3 -and -not $Phase4 -and -not $SkipFabric) {
    Invoke-Step -StepName "Phase 1: Fabric Workspace" `
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
            } | Sort-Object -Property @{Expression={if ($_.sku -like "F*" -and $_.sku -ne "FT1") { 0 } else { 1 }}} | Select-Object -First 1

            if ($activeCap) {
                if ($activeCap.sku -eq "FT1") {
                    throw "Only a trial capacity (FT1) is available. Healthcare Data Solutions requires a paid F-SKU (F2+). Provision a paid capacity at https://portal.azure.com"
                }
                Write-Host "  Assigning capacity: $($activeCap.displayName) (SKU: $($activeCap.sku))..." -ForegroundColor White
                Invoke-RestMethod -Uri "$fabBase/workspaces/$($script:fabricWorkspaceId)/assignToCapacity" `
                    -Headers $fabHeaders -Method POST `
                    -Body (@{ capacityId = $activeCap.id } | ConvertTo-Json) | Out-Null
                Start-Sleep -Seconds 5
                Write-Host "  ✓ Capacity assigned" -ForegroundColor Green
            } else {
                throw "No active Fabric capacity found. Provision a paid F-SKU (F2+) at https://portal.azure.com"
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
# STEP 1b — BASE AZURE INFRASTRUCTURE
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $SkipBaseInfra) {
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
            Name     = "Phase 1: Base Azure Infrastructure"
            Success  = $true
            Duration = "skipped"
            Detail   = "Already deployed (ACR: $existingAcr)"
        }
    } else {
        Invoke-Step -StepName "Phase 1: Base Azure Infrastructure" `
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
# STEP 2 — FHIR SERVICE + SYNTHEA + LOADER
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $SkipFhir) {
    if ($ReusePatients) {
        Write-Host "  >>  Reusing existing patients — skipping Synthea + FHIR Loader" -ForegroundColor Yellow
        Write-Host "      Existing patient/device data in FHIR will be preserved." -ForegroundColor DarkGray
    } else {
    Invoke-Step -StepName "Phase 1: FHIR Service + Synthea + Loader" `
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
    }
} else {
    Write-Host "  >>  Skipping FHIR / Synthea (--SkipFhir)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2b — DICOM SERVICE + TCIA LOADER
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $SkipDicom -and -not $SkipFhir) {
    if ($ReusePatients) {
        Write-Host "  >>  Reusing existing patients — skipping DICOM Loader" -ForegroundColor Yellow
        Write-Host "      Existing DICOM/ImagingStudy data will be preserved." -ForegroundColor DarkGray
    } else {
    Invoke-Step -StepName "Phase 1: DICOM Service + Loader" `
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
    }
} elseif ($SkipDicom) {
    Write-Host "  >>  Skipping DICOM (--SkipDicom)" -ForegroundColor DarkGray
} else {
    Write-Host "  >>  Skipping DICOM (FHIR was skipped)" -ForegroundColor DarkGray
}

# ============================================================================
# STEP 2c — FHIR $EXPORT (ensure data exists for HDS pipelines)
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $SkipFhir -and $ReusePatients) {
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
        & "$ScriptDir\deploy-fhir.ps1" @exportArgs

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

if (-not $Phase3 -and -not $Phase4 -and -not $SkipFabric) {
    Invoke-Step -StepName "Phase 1: Fabric RTI" `
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
# STEP 4 — HDS GUIDANCE (manual Fabric portal step)
# ============================================================================

if (-not $Phase3 -and -not $Phase4 -and -not $SkipFabric) {
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
    $phase2Cmd = "    .\Deploy-All.ps1 -Phase2 ``"
    $phase2Cmd += "`n        -ResourceGroupName `"$ResourceGroupName`" ``"
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
        Name     = "Phase 1: HDS Guidance"
        Success  = $true
        Duration = "—"
        Detail   = "Manual step: deploy HDS, then run Phase 2"
    }

    # ── Auto-detect if HDS is already deployed ──
    # If Bronze and Silver lakehouses exist, the user already deployed HDS.
    # Automatically proceed to Phase 2 + 3 without requiring a separate run.
    try {
        $autoToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
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

            if ($hasBronze -and $hasSilver) {
                Write-Host ""
                Write-Host "  ╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
                Write-Host "  ║  HDS DETECTED — Bronze and Silver lakehouses already exist  ║" -ForegroundColor Green
                Write-Host "  ║  The user has already performed the manual steps of          ║" -ForegroundColor Green
                Write-Host "  ║  deploying HDS to the Fabric workspace.                     ║" -ForegroundColor Green
                Write-Host "  ║  Proceeding on to Phases 2 and 3.                           ║" -ForegroundColor Green
                Write-Host "  ╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
                Write-Host ""

                # Run Phase 2 inline
                $Phase2 = $true

                Emit-PhaseTransition -Phase 2 -Label "Analytics & AI Agents" -StepCount 3

                if (-not $SkipRtiPhase2) {
                Invoke-Step -StepName "Phase 2: Fabric RTI (auto)" `
                    -Description "Bronze shortcut, clinical pipeline, KQL shortcuts, enriched alerts" -Action {
                    $phase2Args = @{
                        Phase2              = $true
                        FabricWorkspaceName = $FabricWorkspaceName
                        ResourceGroupName   = $ResourceGroupName
                        Location            = $Location
                    }
                    if ($Tags.Count -gt 0) { $phase2Args['Tags'] = $Tags }
                    & "$ScriptDir\deploy-fabric-rti.ps1" @phase2Args
                }
                } # end if (-not $SkipRtiPhase2)

                if (-not $SkipDicom -and -not $SkipHdsPipelines) {
                    Invoke-Step -StepName "Phase 2: DICOM Shortcut + HDS Pipelines (auto)" `
                        -Description "Shortcut for DICOM data, then run clinical, imaging, and OMOP pipelines" -Action {
                        & "$ScriptDir\storage-access-trusted-workspace.ps1" `
                            -FabricWorkspaceName $FabricWorkspaceName `
                            -ResourceGroupName $ResourceGroupName
                    }
                }
            }
        }
    } catch {
        # Non-fatal — if detection fails, just show the manual guidance as before
        Write-Host "  (Could not auto-detect HDS status: $($_.Exception.Message))" -ForegroundColor DarkGray
    }
}

# ============================================================================
# STEP 6 — DATA AGENTS (after Phase 2 + OMOP)
# ============================================================================

# Deploy Data Agents if running Phase 2 or if the Silver Lakehouse is available
if ($Phase2 -and -not $SkipDataAgents) {
    Invoke-Step -StepName "Phase 2: Data Agents" `
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

Emit-PhaseTransition -Phase 3 -Label "Imaging & Reporting" -StepCount 1

if (($Phase2 -or $Phase3) -and -not $SkipImaging) {
    # Phase 3 preflight: verify Gold OMOP lakehouse has data
    $runPhase3 = $true

    if ($Phase3 -or $Phase2) {
        Invoke-Step -StepName "Phase 3: Imaging & Reporting" `
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
            Write-Host "  --- Step 7a: Cohorting Data Agent ---" -ForegroundColor Cyan
            Write-Host "  Deploying HDS Multi-Layer Imaging Cohort Agent..." -ForegroundColor White
            Write-Host "    Source: $DicomToolkitPath\Deploy-DataAgent.ps1" -ForegroundColor DarkGray
            Write-Host ""

            & "$DicomToolkitPath\Deploy-DataAgent.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName

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

            & "$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1" `
                -ResourceGroup $viewerRg `
                -FabricWorkspaceName $FabricWorkspaceName `
                -Location $Location

            Write-Host ""

            # Step 3c: Create Reporting Lakehouse + Materialize Notebook
            Write-Host "  --- Step 7c: Reporting Tables ---" -ForegroundColor Cyan
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
                -DicomViewerResourceGroup $viewerRg
            Write-Host ""

            # ── DIAGNOSTIC CHECKPOINT: After notebook materialization ──
            Write-Phase3Diagnostics -Checkpoint "POST-NOTEBOOK (7c)" -WorkspaceId $p3WsId

            # Step 3d: Deploy Power BI Direct Lake Report
            Write-Host "  --- Step 7d: Power BI Imaging Report (Direct Lake) ---" -ForegroundColor Cyan
            & "$DicomToolkitPath\Deploy-ImagingReport.ps1" `
                -FabricWorkspaceName $FabricWorkspaceName
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
                    $p3Token3 = Get-FabricTokenLocal
                    $p3H3 = @{ Authorization = "Bearer $p3Token3"; "Content-Type" = "application/json" }

                    # Check if already assigned
                    $existingRoles = (Invoke-RestMethod -Uri "$p3Base/workspaces/$p3WsId/roleAssignments" -Headers $p3H3).value
                    $alreadyAssigned = $existingRoles | Where-Object { $_.principal.id -eq $proxyPrincipalId }

                    if ($alreadyAssigned) {
                        Write-Host "  ✓ Proxy MI already has workspace access ($($alreadyAssigned.role))" -ForegroundColor Green
                    } else {
                        $roleBody = @{
                            principal = @{ id = $proxyPrincipalId; type = "ServicePrincipal" }
                            role = "Contributor"
                        } | ConvertTo-Json -Depth 3

                        Invoke-RestMethod -Uri "$p3Base/workspaces/$p3WsId/roleAssignments" `
                            -Headers $p3H3 -Method POST -Body $roleBody | Out-Null
                        Write-Host "  ✓ Added proxy MI as Contributor on workspace (required for OneLake DFS reads)" -ForegroundColor Green
                    }
                } else {
                    Write-Host "  ⚠ Could not find proxy MI — viewer may not have OneLake access" -ForegroundColor Yellow
                }

                # Check if DICOM index has studies
                $proxyFqdn = az containerapp show -g $viewerRg -n "hds-dicom-proxy" `
                    --query "properties.configuration.ingress.fqdn" -o tsv 2>$null
                if ($proxyFqdn) {
                    try {
                        $healthResp = Invoke-RestMethod -Uri "https://$proxyFqdn/health" -TimeoutSec 10
                        $studyCount = $healthResp.studies
                        Write-Host "  DICOM index: $studyCount studies" -ForegroundColor $(if ($studyCount -gt 0) { 'Green' } else { 'Yellow' })

                        if ($studyCount -eq 0) {
                            # Auto-rebuild: the index was built in 3b before data was fully available
                            Write-Host "  ⚠ DICOM index is empty — auto-rebuilding from current Silver data..." -ForegroundColor Yellow

                            try {
                                & "$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1" `
                                    -ResourceGroup $viewerRg `
                                    -FabricWorkspaceName $FabricWorkspaceName `
                                    -Location $Location -SkipOhifBuild -Force

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
                                }
                            } catch {
                                Write-Host "  ⚠ Auto-rebuild failed: $($_.Exception.Message)" -ForegroundColor Yellow
                                Write-Host "    Manual rebuild:" -ForegroundColor Yellow
                                Write-Host "    & `"$DicomToolkitPath\dicom-viewer\Deploy-DicomViewer.ps1`" ``" -ForegroundColor Cyan
                                Write-Host "        -ResourceGroup `"$viewerRg`" ``" -ForegroundColor Cyan
                                Write-Host "        -FabricWorkspaceName `"$FabricWorkspaceName`" ``" -ForegroundColor Cyan
                                Write-Host "        -Location `"$Location`" -SkipOhifBuild -Force" -ForegroundColor Cyan
                            }
                        }
                    } catch {
                        Write-Host "  ⚠ Could not reach proxy health endpoint" -ForegroundColor Yellow
                    }
                }
            } catch {
                Write-Host "  ⚠ Post-deploy check failed: $($_.Exception.Message)" -ForegroundColor Yellow
            }

            # ── DIAGNOSTIC CHECKPOINT: Final state after all Phase 3 steps ──
            Write-Phase3Diagnostics -Checkpoint "FINAL (Phase 3 complete)" -WorkspaceId $p3WsId
            Write-Host ""
        }
    }
}

# ============================================================================
# STEPS 8-9 — PHASE 4: ONTOLOGY + AGENT BINDING + DATA ACTIVATOR
# Requires: Silver Lakehouse populated, clinical pipeline completed,
#           Eventhouse with TelemetryRaw + AlertHistory tables
# ============================================================================

Emit-PhaseTransition -Phase 4 -Label "Semantic Layer & Alerts" -StepCount 2

if (($Phase4 -or ($Phase2 -and -not $Phase3)) -and -not $SkipOntology) {
    Invoke-Step -StepName "Phase 4: Ontology" `
        -Description "Clinical pipeline check, ontology deployment, agent binding" -Action {

        function Get-FabricTokenLocal {
            $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
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
                    Write-Host "  ⚠ Clinical pipeline did not complete — ontology will still be deployed" -ForegroundColor Yellow
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
            Write-Host "  ⚠ Clinical pipeline not found in workspace — skipping verification" -ForegroundColor Yellow
        }

        # ── Step 8b: Materialize DeviceAssociation table ──
        Write-Host ""
        Write-Host "  --- Step 8b: DeviceAssociation Table ---" -ForegroundColor Cyan
        Write-Host "  Materializing DeviceAssociation from Basic table (Silver Lakehouse)..." -ForegroundColor White

        # Find Silver Lakehouse
        $p4Token = Get-FabricTokenLocal
        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
        $p4Lakehouses = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/lakehouses" -Headers $p4Headers).value
        $p4SilverLh = $p4Lakehouses | Where-Object { $_.displayName -match '[Ss]ilver' } | Select-Object -First 1

        if ($p4SilverLh) {
            $p4SilverLhId = $p4SilverLh.id
            Write-Host "  ✓ Silver Lakehouse: $($p4SilverLh.displayName) ($p4SilverLhId)" -ForegroundColor Green

            # Read the notebook content
            $daNotebookPath = Join-Path $ScriptDir "fabric-rti\sql\create-device-association-table.ipynb"
            if (Test-Path $daNotebookPath) {
                # Build ipynb from the SQL cells
                $daSql = @"
CREATE OR REPLACE TABLE DeviceAssociation AS
SELECT
    id,
    idOrig,
    get_json_object(extension, '`$[0].valueReference.reference') AS device_ref,
    get_json_object(subject_string, '`$.display')                AS patient_name,
    get_json_object(subject_string, '`$.idOrig')                 AS patient_id,
    get_json_object(code_string, '`$.coding[0].code')            AS assoc_code,
    get_json_object(code_string, '`$.coding[0].display')         AS assoc_display
FROM Basic
WHERE get_json_object(code_string, '`$.coding[0].code') = 'device-assoc'
"@
                $daVerifySql = "SELECT COUNT(*) AS device_association_count FROM DeviceAssociation"

                $daIpynb = @{
                    nbformat = 4
                    nbformat_minor = 5
                    metadata = @{
                        kernel_info = @{ name = "synapse_pyspark" }
                        kernelspec = @{ name = "synapse_pyspark"; display_name = "Synapse PySpark" }
                        language_info = @{ name = "python" }
                    }
                    cells = @(
                        @{ cell_type = "code"; source = @("%%sql`n$daSql"); metadata = @{}; outputs = @() },
                        @{ cell_type = "code"; source = @("%%sql`n$daVerifySql"); metadata = @{}; outputs = @() }
                    )
                }

                $daIpynbJson = $daIpynb | ConvertTo-Json -Depth 10 -Compress
                $daIpynbBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($daIpynbJson))

                # Check for existing notebook and delete
                $p4Items = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items" -Headers $p4Headers).value
                $existingDaNb = $p4Items | Where-Object { $_.displayName -eq "create_device_association_table" -and $_.type -eq "Notebook" }
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

                # Create notebook
                $daNbBody = @{
                    displayName = "create_device_association_table"
                    type = "Notebook"
                    definition = @{
                        format = "ipynb"
                        parts = @(
                            @{
                                path = "notebook-content.py"
                                payload = $daIpynbBase64
                                payloadType = "InlineBase64"
                            }
                        )
                    }
                } | ConvertTo-Json -Depth 5

                $daNbCreated = $false
                for ($attempt = 1; $attempt -le 3; $attempt++) {
                    try {
                        $daNbResp = Invoke-WebRequest -Uri "$p4Base/workspaces/$p4WsId/items" `
                            -Headers $p4Headers -Method Post -Body $daNbBody -UseBasicParsing
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
                        if ($errCode -eq 409 -and $attempt -lt 3) {
                            Write-Host "    409 Conflict — retrying in 10s ($attempt/3)" -ForegroundColor Yellow
                            Start-Sleep 10
                        } else { throw }
                    }
                }

                if ($daNbCreated) {
                    # Find the notebook and run it
                    $p4Token = Get-FabricTokenLocal
                    $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                    $daNb = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items?type=Notebook" -Headers $p4Headers).value |
                        Where-Object { $_.displayName -eq "create_device_association_table" }
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
                                -Uri "$p4Base/workspaces/$p4WsId/items/$($daNb.id)/jobs/RunNotebook/instances?jobType=RunNotebook" `
                                -Headers $p4Headers -Body $runBody -UseBasicParsing | Out-Null
                            Write-Host "  ✓ DeviceAssociation notebook invoked" -ForegroundColor Green

                            # Wait for notebook to complete (~1-2 min)
                            $daStart = Get-Date
                            while ((New-TimeSpan -Start $daStart).TotalMinutes -lt 10) {
                                Start-Sleep 15
                                $p4Token = Get-FabricTokenLocal
                                $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                                $daJobs = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items/$($daNb.id)/jobs/instances?limit=1" -Headers $p4Headers).value
                                if ($daJobs -and $daJobs[0].status -eq 'Completed') {
                                    Write-Host "  ✓ DeviceAssociation table materialized" -ForegroundColor Green
                                    break
                                } elseif ($daJobs -and $daJobs[0].status -in @('Failed', 'Cancelled')) {
                                    Write-Host "  ⚠ Notebook $($daJobs[0].status) — DeviceAssociation may not be created" -ForegroundColor Yellow
                                    break
                                }
                                $daElapsed = [math]::Round((New-TimeSpan -Start $daStart).TotalMinutes, 1)
                                Write-Host "    [$daElapsed min] Notebook: $($daJobs[0].status)" -ForegroundColor DarkGray
                            }
                        } catch {
                            Write-Host "  ⚠ Could not run notebook: $($_.Exception.Message)" -ForegroundColor Yellow
                        }
                    }
                }
            } else {
                Write-Host "  ⚠ Notebook source not found at: $daNotebookPath" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  ⚠ Silver Lakehouse not found — skipping DeviceAssociation" -ForegroundColor Yellow
        }

        # ── Step 8c: Deploy Ontology ──
        Write-Host ""
        Write-Host "  --- Step 8c: Ontology Deployment ---" -ForegroundColor Cyan
        Write-Host "  Deploying ClinicalDeviceOntology..." -ForegroundColor White

        & "$ScriptDir\deploy-ontology.ps1" `
            -FabricWorkspaceName $FabricWorkspaceName

        Write-Host ""

        # ── Step 8d: Bind ontology to Data Agents ──
        Write-Host "  --- Step 8d: Agent Ontology Binding ---" -ForegroundColor Cyan

        # Discover the deployed ontology
        $p4Token = Get-FabricTokenLocal
        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
        $ontologies = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/ontologies" -Headers $p4Headers).value
        $ontology = $ontologies | Where-Object { $_.displayName -eq "ClinicalDeviceOntology" } | Select-Object -First 1

        if ($ontology) {
            $ontologyId = $ontology.id
            Write-Host "  ✓ Ontology found: ClinicalDeviceOntology ($ontologyId)" -ForegroundColor Green

            # Find Data Agents — bind ontology to all three agents
            $agents = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items?type=DataAgent" -Headers $p4Headers).value

            foreach ($agentName in @("Patient 360", "Clinical Triage", "HDS Multi-Layer Imaging Cohort Agent")) {
                $agent = $agents | Where-Object { $_.displayName -eq $agentName } | Select-Object -First 1
                if (-not $agent) {
                    Write-Host "  ⚠ Agent '$agentName' not found — skipping" -ForegroundColor Yellow
                    continue
                }

                Write-Host "  Adding ontology datasource to '$agentName'..." -ForegroundColor White

                # Get current agent definition
                try {
                    $defResp = Invoke-WebRequest -Method POST `
                        -Uri "$p4Base/workspaces/$p4WsId/items/$($agent.id)/getDefinition" `
                        -Headers $p4Headers -UseBasicParsing
                    $defOpId = $defResp.Headers["x-ms-operation-id"]
                    if ($defOpId -is [array]) { $defOpId = $defOpId[0] }
                    Start-Sleep 5
                    $p4Token = Get-FabricTokenLocal
                    $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                    $defResult = Invoke-RestMethod -Uri "$p4Base/operations/$defOpId/result" -Headers $p4Headers
                    $existingParts = $defResult.definition.parts

                    # Build ontology datasource JSON
                    $ontDatasourceJson = @{
                        '$schema'              = "1.0.0"
                        artifactId             = $ontologyId
                        workspaceId            = $p4WsId
                        displayName            = "ClinicalDeviceOntology"
                        type                   = "ontology"
                        userDescription        = "Semantic layer: 9 entity types (Patient, Device, Encounter, Condition, MedicationRequest, Observation, DeviceAssociation, ClinicalAlert, DeviceTelemetry) with relationships across Lakehouse and Eventhouse."
                        dataSourceInstructions = "Use this ontology to understand entity relationships. It maps Patient↔Device, Patient→Encounter, Patient→Condition, Device→DeviceTelemetry, Device→ClinicalAlert."
                    } | ConvertTo-Json -Depth 10

                    $ontFewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = @() } | ConvertTo-Json -Depth 5)

                    # Add ontology datasource parts to existing definition
                    $ontFolderName = "ontology-ClinicalDeviceOntology"
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

                    # Always update ontology datasource to ensure artifactId matches current ontology
                    $hasOntDs = $existingParts | Where-Object { $_.path -match "ontology-ClinicalDeviceOntology" }
                    if ($hasOntDs) {
                        # Remove existing ontology parts — will re-add with current ID
                        $existingParts = @($existingParts | Where-Object { $_.path -notmatch "ontology-ClinicalDeviceOntology" })
                    }

                    # Merge: keep all existing parts + add ontology parts
                    $updatedParts = @($existingParts) + @($ontDsPart, $ontFsPart)

                    # Update definition
                    $updateBody = @{
                        definition = @{
                            parts = $updatedParts
                        }
                    }
                    $updateResp = Invoke-WebRequest -Method POST `
                        -Uri "$p4Base/workspaces/$p4WsId/items/$($agent.id)/updateDefinition" `
                        -Headers $p4Headers `
                        -Body ($updateBody | ConvertTo-Json -Depth 20) `
                        -UseBasicParsing

                    if ($updateResp.StatusCode -in @(200, 202)) {
                        if ($updateResp.StatusCode -eq 202) {
                            $upOpId = $updateResp.Headers["x-ms-operation-id"]
                            if ($upOpId -is [array]) { $upOpId = $upOpId[0] }
                            Start-Sleep 10
                        }
                        $action = if ($hasOntDs) { "updated" } else { "added" }
                        Write-Host "  ✓ Ontology datasource $action on '$agentName' (ID: $ontologyId)" -ForegroundColor Green
                    }
                } catch {
                    Write-Host "  ⚠ Could not bind ontology to '$agentName': $($_.Exception.Message)" -ForegroundColor Yellow
                }
            }
        } else {
            Write-Host "  ⚠ Ontology not found after deployment — agent binding skipped" -ForegroundColor Yellow
        }

        Write-Host ""
    }

    # ── Step 9: Data Activator ──
    if (-not $SkipActivator) {
    Invoke-Step -StepName "Phase 4: Data Activator" `
        -Description "Reflex item with KQL source and email alerting rule" -Action {

        function Get-FabricTokenLocal {
            $t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
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

            # Resolve workspace ID
            $p4Workspaces = (Invoke-RestMethod -Uri "$p4Base/workspaces" -Headers $p4Headers).value
            $p4Ws = $p4Workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
            if (-not $p4Ws) {
                Write-Host "  ✗ Workspace '$FabricWorkspaceName' not found — cannot deploy Activator" -ForegroundColor Red
                return
            }
            $p4WsId = $p4Ws.id

            $reflexName = "ClinicalAlertActivator"

            # Check for existing Reflex
            $p4Items = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items" -Headers $p4Headers).value
            $existingReflex = $p4Items | Where-Object { $_.displayName -eq $reflexName -and $_.type -eq 'Reflex' }

            $reflexId = $null
            if ($existingReflex) {
                if ($existingReflex -is [array]) { $existingReflex = $existingReflex[0] }
                $reflexId = $existingReflex.id
                Write-Host "  ✓ Reflex '$reflexName' already exists ($reflexId)" -ForegroundColor Yellow
            }

            # Discover KQL Database for KQL source (itemId must be KQL DB, not Eventhouse)
            $p4KqlDbs = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/kqlDatabases" -Headers $p4Headers).value
            $p4KqlDb = $p4KqlDbs | Where-Object { $_.displayName -match 'Masimo' } | Select-Object -First 1
            if (-not $p4KqlDb) { $p4KqlDb = $p4KqlDbs | Select-Object -First 1 }
            if ($p4KqlDb -is [array]) { $p4KqlDb = $p4KqlDb[0] }

            if (-not $p4KqlDb) {
                Write-Host "  ⚠ No KQL Database found — cannot configure data source" -ForegroundColor Yellow
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
                        $rResp = Invoke-WebRequest -Uri "$p4Base/workspaces/$p4WsId/items" `
                            -Headers $p4Headers -Method POST -Body $createBody -UseBasicParsing -ErrorAction Stop
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
                                $op = Invoke-RestMethod -Uri "$p4Base/operations/$rOpId" -Headers $p4Headers
                                if ($op.status -ne 'Running') { break }
                            }
                            Start-Sleep 3
                            $p4Items2 = (Invoke-RestMethod -Uri "$p4Base/workspaces/$p4WsId/items" -Headers $p4Headers).value
                            $reflex = $p4Items2 | Where-Object { $_.displayName -eq $reflexName -and $_.type -eq 'Reflex' }
                            if ($reflex -is [array]) { $reflex = $reflex[0] }
                            $reflexId = $reflex.id
                            Write-Host "  ✓ Reflex created with KQL pipeline: $reflexName ($reflexId)" -ForegroundColor Green
                        }
                    } catch {
                        $errMsg = $_.Exception.Message
                        try { $errMsg = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
                        Write-Host "  ⚠ Could not create Reflex: $errMsg" -ForegroundColor Yellow
                        $reflexId = $null
                    }
                }

                # Step 2: Add email rule via updateDefinition (Create Item rejects EventTrigger with KQL source)
                if ($reflexId) {
                    Write-Host "  Adding email rule (EventTrigger v1.2.4 → $AlertEmail)..." -ForegroundColor White
                    Start-Sleep 5
                    try {
                        $p4Token = Get-FabricTokenLocal
                        $p4Headers = @{ Authorization = "Bearer $p4Token"; "Content-Type" = "application/json" }
                        $fullB64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($fullEntitiesJson))
                        $updateBody = @{ definition = @{ parts = @(@{path="ReflexEntities.json"; payload=$fullB64; payloadType="InlineBase64"}) } } | ConvertTo-Json -Depth 10
                        Invoke-WebRequest -Method POST `
                            -Uri "$p4Base/workspaces/$p4WsId/items/$reflexId/updateDefinition" `
                            -Headers $p4Headers -Body $updateBody -UseBasicParsing | Out-Null
                        Write-Host "  ✓ Email rule added (shouldRun=true)" -ForegroundColor Green
                    } catch {
                        $ruleErr = $_.Exception.Message
                        try { $ruleErr = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
                        Write-Host "  ⚠ Could not push rule: $ruleErr" -ForegroundColor Yellow
                        Write-Host "    Add rule manually in Fabric portal: spo2 < 94 → Email $AlertEmail" -ForegroundColor DarkGray
                    }
                }
            }

            if ($reflexId) {
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
        }

        Write-Host ""
    }
    } # end if (-not $SkipActivator)
}

# ============================================================================
# SUMMARY
# ============================================================================

$summaryTitle = if ($Phase4) { "PHASE 4 DEPLOYMENT SUMMARY" } elseif ($Phase3) { "PHASE 3 DEPLOYMENT SUMMARY" } elseif ($Phase2) { "PHASE 2 DEPLOYMENT SUMMARY" } else { "FULL DEPLOYMENT SUMMARY" }
$summaryPhase = if ($Phase4) { "Phase4" } elseif ($Phase3) { "Phase3" } elseif ($Phase2) { "Phase2" } else { "Phase1+2+3+4" }
Write-Summary -Title $summaryTitle -PhaseName $summaryPhase -PhaseResources @{
    FabricWorkspaceName = $FabricWorkspaceName
    ResourceGroupName   = $ResourceGroupName
    Location            = $Location
}
Pop-Location

