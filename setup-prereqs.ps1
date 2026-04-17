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

# Use PowerShell 7+ built-in automatic variables ($IsWindows, $IsMacOS, $IsLinux)
# Fallback for PS 5.1 where they don't exist
if (-not (Get-Variable -Name IsWindows -ErrorAction SilentlyContinue)) {
    $script:_isWin   = $env:OS -eq "Windows_NT"
    $script:_isMac   = $false
    $script:_isLinux  = $false
} else {
    $script:_isWin   = $IsWindows
    $script:_isMac   = $IsMacOS
    $script:_isLinux  = $IsLinux
}

$platform = if ($script:_isWin) { "Windows" } elseif ($script:_isMac) { "macOS" } else { "Linux" }
Write-Host "  Platform: $platform" -ForegroundColor DarkGray
Write-Host ""

$pass = 0
$fail = 0
$warn = 0
$installed = 0

function Write-InstallHint {
    param([string]$WinHint, [string]$MacHint, [string]$LinuxHint, [string]$Url = "")
    if ($Url) {
        Write-Host "    Download: $Url" -ForegroundColor DarkCyan
    }
    if ($script:_isWin -and $WinHint) {
        Write-Host "    Install:  $WinHint" -ForegroundColor DarkGray
    } elseif ($script:_isMac -and $MacHint) {
        Write-Host "    Install:  $MacHint" -ForegroundColor DarkGray
    } elseif ($script:_isLinux -and $LinuxHint) {
        Write-Host "    Install:  $LinuxHint" -ForegroundColor DarkGray
    }
}

# ── 1. PowerShell 7+ ──────────────────────────────────────────────────
Write-Host "  Checking core tools..." -ForegroundColor White
if ($PSVersionTable.PSVersion.Major -ge 7) {
    Write-Host "  ✓ PowerShell $($PSVersionTable.PSVersion)" -ForegroundColor Green
    $pass++
} else {
    Write-Host "  ✗ PowerShell $($PSVersionTable.PSVersion) — 7+ required" -ForegroundColor Red
    Write-InstallHint `
        -Url    "https://aka.ms/powershell" `
        -WinHint   "winget install Microsoft.PowerShell" `
        -MacHint   "brew install --cask powershell" `
        -LinuxHint "sudo apt-get install -y powershell  # or: sudo dnf install -y powershell"
    $fail++
}

# ── 2. Azure CLI ──────────────────────────────────────────────────────
$hasAzCli = $false
try {
    $azVerJson = az version --output json 2>$null | ConvertFrom-Json
    if ($azVerJson.'azure-cli') {
        Write-Host "  ✓ Azure CLI ($($azVerJson.'azure-cli'))" -ForegroundColor Green
        $pass++
        $hasAzCli = $true
    } else {
        throw "no version"
    }
} catch {
    Write-Host "  ✗ Azure CLI — not found" -ForegroundColor Red
    Write-InstallHint `
        -Url    "https://aka.ms/installazurecli" `
        -WinHint   "winget install Microsoft.AzureCLI" `
        -MacHint   "brew install azure-cli" `
        -LinuxHint "curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash"
    $fail++
}

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
        Write-Host "    Install: Install-Module Az -Scope CurrentUser -Force" -ForegroundColor DarkGray
        if ($script:_isMac -or $script:_isLinux) {
            Write-Host "    Note:    Requires pwsh (PowerShell Core) — see PowerShell section above" -ForegroundColor DarkGray
        }
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
            Write-InstallHint `
                -Url    "https://python.org/downloads" `
                -WinHint   "winget install Python.Python.3.12" `
                -MacHint   "brew install python@3.12" `
                -LinuxHint "sudo apt-get install -y python3 python3-venv python3-pip"
            $fail++
        }
    }
} catch {
    Write-Host "  ✗ Python — not found" -ForegroundColor Red
    Write-InstallHint `
        -Url    "https://python.org/downloads" `
        -WinHint   "winget install Python.Python.3.12" `
        -MacHint   "brew install python@3.12" `
        -LinuxHint "sudo apt-get install -y python3 python3-venv python3-pip"
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
            Write-InstallHint `
                -Url    "https://nodejs.org" `
                -WinHint   "winget install OpenJS.NodeJS.LTS" `
                -MacHint   "brew install node@20" `
                -LinuxHint "curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - && sudo apt-get install -y nodejs"
            $fail++
        }
    }
} catch {
    Write-Host "  ✗ Node.js — not found (required for Orchestrator UI)" -ForegroundColor Red
    Write-InstallHint `
        -Url    "https://nodejs.org" `
        -WinHint   "winget install OpenJS.NodeJS.LTS" `
        -MacHint   "brew install node@20" `
        -LinuxHint "curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - && sudo apt-get install -y nodejs"
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
try {
    $gitOutput = git --version 2>&1
    if ($gitOutput -match "(\d+\.\d+\.\d+)") {
        Write-Host "  ✓ Git ($($Matches[0]))" -ForegroundColor Green
        $pass++
    } else { throw "no version" }
} catch {
    Write-Host "  ✗ Git — not found" -ForegroundColor Red
    Write-InstallHint `
        -Url    "https://git-scm.com" `
        -WinHint   "winget install Git.Git" `
        -MacHint   "brew install git" `
        -LinuxHint "sudo apt-get install -y git"
    $fail++
}

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
            if ($script:_isWin) {
                Write-Host "    Tip: az login opens a browser for interactive sign-in" -ForegroundColor DarkGray
            } else {
                Write-Host "    Tip: az login --use-device-code  (if no browser available)" -ForegroundColor DarkGray
            }
            $fail++
        }
    } catch {
        Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
        Write-Host "    Run: az login" -ForegroundColor DarkGray
        if ($script:_isWin) {
            Write-Host "    Tip: az login opens a browser for interactive sign-in" -ForegroundColor DarkGray
        } else {
            Write-Host "    Tip: az login --use-device-code  (if no browser available)" -ForegroundColor DarkGray
        }
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
        $pipExe = if ($script:_isWin) { "$venvPath/Scripts/pip" } else { "$venvPath/bin/pip" }
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
Write-Host "  │    .\Start-WebUI.ps1          # start both servers     │" -ForegroundColor DarkCyan
Write-Host "  │    .\Start-WebUI.ps1 -Stop    # stop both servers      │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Then open: http://localhost:5173                      │" -ForegroundColor DarkCyan
Write-Host "  └─────────────────────────────────────────────────────────┘" -ForegroundColor DarkCyan

if (-not $CheckOnly) {
    Write-Host ""
    $startAnswer = Read-Host "  Start the Orchestrator UI now? [Y/n]"
    if (-not $startAnswer -or $startAnswer -match '^[Yy]') {
        & "$PSScriptRoot\Start-WebUI.ps1" -Force
    }
}
Write-Host ""
