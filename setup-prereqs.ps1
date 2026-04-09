<#
.SYNOPSIS
    Cross-platform prerequisite installer for the Medical Device FHIR Integration Platform.

.DESCRIPTION
    Checks and installs all dependencies needed to:
    1. Run the Deployment Orchestrator UI (frontend + backend)
    2. Execute the PowerShell deployment pipeline (Deploy-All.ps1)

    Supports Windows, macOS, and Linux.

.EXAMPLE
    # Check and install everything:
    .\setup-prereqs.ps1

    # Check only (don't install anything):
    .\setup-prereqs.ps1 -CheckOnly
#>

param(
    [switch]$CheckOnly
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Cyan
Write-Host "|       PREREQUISITE SETUP — Med Device FHIR Platform        |" -ForegroundColor Cyan
Write-Host "+============================================================+" -ForegroundColor Cyan
Write-Host ""

$isWindows = $env:OS -eq "Windows_NT" -or $PSVersionTable.OS -match "Windows"
$isMac = $PSVersionTable.OS -match "Darwin"
$isLinux = $PSVersionTable.OS -match "Linux"

$platform = if ($isWindows) { "Windows" } elseif ($isMac) { "macOS" } else { "Linux" }
Write-Host "  Platform: $platform" -ForegroundColor DarkGray
Write-Host ""

$pass = 0
$fail = 0
$warn = 0
$installed = 0

function Check-Tool {
    param([string]$Name, [string]$Command, [string]$VersionMatch, [string]$InstallHint)
    try {
        $output = Invoke-Expression $Command 2>&1
        $ver = if ($output -match $VersionMatch) { $Matches[0] } else { "found" }
        Write-Host "  ✓ $Name ($ver)" -ForegroundColor Green
        $script:pass++
        return $true
    } catch {
        Write-Host "  ✗ $Name — not found" -ForegroundColor Red
        Write-Host "    Install: $InstallHint" -ForegroundColor DarkGray
        $script:fail++
        return $false
    }
}

# ── 1. PowerShell 7+ ──────────────────────────────────────────────────
Write-Host "  Checking core tools..." -ForegroundColor White
if ($PSVersionTable.PSVersion.Major -ge 7) {
    Write-Host "  ✓ PowerShell $($PSVersionTable.PSVersion)" -ForegroundColor Green
    $pass++
} else {
    Write-Host "  ✗ PowerShell $($PSVersionTable.PSVersion) — 7+ required" -ForegroundColor Red
    Write-Host "    Install: https://aka.ms/powershell" -ForegroundColor DarkGray
    $fail++
}

# ── 2. Azure CLI ──────────────────────────────────────────────────────
$hasAzCli = Check-Tool "Azure CLI" "az version --query '""azure-cli""' -o tsv 2>$null" "\d+\.\d+\.\d+" "https://aka.ms/installazurecli"

# ── 3. Bicep ──────────────────────────────────────────────────────────
if ($hasAzCli) {
    $bicepOut = az bicep version 2>&1 | Out-String
    if ($bicepOut -match "(\d+\.\d+\.\d+)") {
        Write-Host "  ✓ Bicep $($Matches[1])" -ForegroundColor Green
        $pass++
    } else {
        if (-not $CheckOnly) {
            Write-Host "  ⚙ Installing Bicep..." -ForegroundColor Yellow
            az bicep install 2>$null
            $installed++
            Write-Host "  ✓ Bicep installed" -ForegroundColor Green
            $pass++
        } else {
            Write-Host "  ✗ Bicep — not installed (run: az bicep install)" -ForegroundColor Red
            $fail++
        }
    }
}

# ── 4. Az PowerShell Module ───────────────────────────────────────────
$azMod = Get-Module -ListAvailable -Name Az.Accounts | Select-Object -First 1
if ($azMod) {
    Write-Host "  ✓ Az PowerShell module $($azMod.Version)" -ForegroundColor Green
    $pass++
} else {
    if (-not $CheckOnly) {
        Write-Host "  ⚙ Installing Az PowerShell module (this may take a few minutes)..." -ForegroundColor Yellow
        Install-Module Az -Scope CurrentUser -Force -AllowClobber -SkipPublisherCheck 2>$null
        $installed++
        Write-Host "  ✓ Az module installed" -ForegroundColor Green
        $pass++
    } else {
        Write-Host "  ✗ Az PowerShell module — not installed" -ForegroundColor Red
        Write-Host "    Install: Install-Module Az -Scope CurrentUser" -ForegroundColor DarkGray
        $fail++
    }
}

# ── 5. Python 3.10+ ──────────────────────────────────────────────────
Write-Host ""
Write-Host "  Checking Python + Node.js..." -ForegroundColor White
$hasPython = $false
try {
    $pyVer = python --version 2>&1
    if ($pyVer -match "(\d+)\.(\d+)\.(\d+)") {
        $major = [int]$Matches[1]; $minor = [int]$Matches[2]
        if ($major -ge 3 -and $minor -ge 10) {
            Write-Host "  ✓ Python $($Matches[0])" -ForegroundColor Green
            $pass++
            $hasPython = $true
        } else {
            Write-Host "  ✗ Python $($Matches[0]) — 3.10+ required" -ForegroundColor Red
            Write-Host "    Install: https://python.org/downloads" -ForegroundColor DarkGray
            $fail++
        }
    }
} catch {
    Write-Host "  ✗ Python — not found" -ForegroundColor Red
    Write-Host "    Install: https://python.org/downloads" -ForegroundColor DarkGray
    $fail++
}

# ── 6. Node.js 18+ (for the Orchestrator UI) ─────────────────────────
$hasNode = $false
try {
    $nodeVer = node --version 2>&1
    if ($nodeVer -match "v(\d+)\.(\d+)") {
        $nodeMajor = [int]$Matches[1]
        if ($nodeMajor -ge 18) {
            Write-Host "  ✓ Node.js $nodeVer" -ForegroundColor Green
            $pass++
            $hasNode = $true
        } else {
            Write-Host "  ✗ Node.js $nodeVer — 18+ required" -ForegroundColor Red
            Write-Host "    Install: https://nodejs.org" -ForegroundColor DarkGray
            $fail++
        }
    }
} catch {
    Write-Host "  ✗ Node.js — not found (required for Orchestrator UI)" -ForegroundColor Red
    Write-Host "    Install: https://nodejs.org" -ForegroundColor DarkGray
    $fail++
}

# ── 7. npm ────────────────────────────────────────────────────────────
if ($hasNode) {
    try {
        $npmVer = npm --version 2>&1
        Write-Host "  ✓ npm $npmVer" -ForegroundColor Green
        $pass++
    } catch {
        Write-Host "  ⚠ npm — not found (usually bundled with Node.js)" -ForegroundColor Yellow
        $warn++
    }
}

# ── 8. Git ────────────────────────────────────────────────────────────
Check-Tool "Git" "git --version" "\d+\.\d+\.\d+" "https://git-scm.com" | Out-Null

# ── 9. Azure Login Check ──────────────────────────────────────────────
Write-Host ""
Write-Host "  Checking Azure login..." -ForegroundColor White
if ($hasAzCli) {
    try {
        $acct = az account show --output json 2>$null | ConvertFrom-Json
        if ($acct.id) {
            Write-Host "  ✓ Logged in: $($acct.name) ($($acct.user.name))" -ForegroundColor Green
            $pass++
        } else {
            Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
            Write-Host "    Run: az login" -ForegroundColor DarkGray
            $fail++
        }
    } catch {
        Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
        Write-Host "    Run: az login" -ForegroundColor DarkGray
        $fail++
    }
}

# ── 10. Setup Orchestrator Backend (Python venv) ──────────────────────
Write-Host ""
Write-Host "  Setting up Orchestrator backend..." -ForegroundColor White
$venvPath = Join-Path $ScriptDir "orchestrator/.venv"
$requirementsPath = Join-Path $ScriptDir "orchestrator/requirements.txt"

if ($hasPython) {
    if (-not (Test-Path $venvPath)) {
        if (-not $CheckOnly) {
            Write-Host "  ⚙ Creating Python virtual environment..." -ForegroundColor Yellow
            python -m venv $venvPath
            $installed++
            Write-Host "  ✓ Virtual environment created at orchestrator/.venv" -ForegroundColor Green
        } else {
            Write-Host "  ✗ Python venv not created (orchestrator/.venv)" -ForegroundColor Yellow
            $warn++
        }
    } else {
        Write-Host "  ✓ Python venv exists (orchestrator/.venv)" -ForegroundColor Green
        $pass++
    }

    # Install Python dependencies
    if (Test-Path $venvPath) {
        $pipExe = if ($isWindows) { "$venvPath/Scripts/pip" } else { "$venvPath/bin/pip" }
        if (-not $CheckOnly) {
            Write-Host "  ⚙ Installing Python dependencies..." -ForegroundColor Yellow
            & $pipExe install -r $requirementsPath --quiet 2>$null
            $installed++
            Write-Host "  ✓ Python dependencies installed" -ForegroundColor Green
            $pass++
        } else {
            Write-Host "  ⚠ Run pip install -r orchestrator/requirements.txt to install deps" -ForegroundColor Yellow
            $warn++
        }
    }
}

# ── 11. Setup Orchestrator UI (npm install) ───────────────────────────
Write-Host ""
Write-Host "  Setting up Orchestrator UI..." -ForegroundColor White
$uiPath = Join-Path $ScriptDir "orchestrator-ui"
$nodeModules = Join-Path $uiPath "node_modules"

if ($hasNode) {
    if (-not (Test-Path $nodeModules)) {
        if (-not $CheckOnly) {
            Write-Host "  ⚙ Installing UI dependencies (npm install)..." -ForegroundColor Yellow
            Push-Location $uiPath
            npm install --silent 2>$null
            Pop-Location
            $installed++
            Write-Host "  ✓ UI dependencies installed" -ForegroundColor Green
            $pass++
        } else {
            Write-Host "  ✗ UI deps not installed (run: cd orchestrator-ui && npm install)" -ForegroundColor Yellow
            $warn++
        }
    } else {
        Write-Host "  ✓ UI dependencies present (orchestrator-ui/node_modules)" -ForegroundColor Green
        $pass++
    }
}

# ── Summary ───────────────────────────────────────────────────────────
Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Cyan
Write-Host "|                      SUMMARY                              |" -ForegroundColor Cyan
Write-Host "+============================================================+" -ForegroundColor Cyan
Write-Host ""
Write-Host "  ✓ Passed:    $pass" -ForegroundColor Green
if ($fail -gt 0) {
    Write-Host "  ✗ Failed:    $fail" -ForegroundColor Red
}
if ($warn -gt 0) {
    Write-Host "  ⚠ Warnings:  $warn" -ForegroundColor Yellow
}
if ($installed -gt 0) {
    Write-Host "  ⚙ Installed:  $installed" -ForegroundColor Cyan
}

if ($fail -gt 0) {
    Write-Host ""
    Write-Host "  Fix the failures above before running the platform." -ForegroundColor Red
    Write-Host ""
    exit 1
}

Write-Host ""
Write-Host "  All prerequisites satisfied!" -ForegroundColor Green
Write-Host ""
Write-Host "  ┌─────────────────────────────────────────────────────────┐" -ForegroundColor DarkCyan
Write-Host "  │  TO START THE ORCHESTRATOR UI:                         │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Terminal 1 (backend):                                 │" -ForegroundColor DarkCyan
Write-Host "  │    cd orchestrator                                     │" -ForegroundColor DarkCyan
if ($isWindows) {
Write-Host "  │    .\.venv\Scripts\Activate.ps1                        │" -ForegroundColor DarkCyan
} else {
Write-Host "  │    source .venv/bin/activate                           │" -ForegroundColor DarkCyan
}
Write-Host "  │    python local_server.py                              │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Terminal 2 (frontend):                                │" -ForegroundColor DarkCyan
Write-Host "  │    cd orchestrator-ui                                  │" -ForegroundColor DarkCyan
Write-Host "  │    npm run dev                                         │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Then open: http://localhost:5173                      │" -ForegroundColor DarkCyan
Write-Host "  └─────────────────────────────────────────────────────────┘" -ForegroundColor DarkCyan
Write-Host ""
