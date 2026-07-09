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

$isWin = $env:OS -eq "Windows_NT" -or $PSVersionTable.OS -match "Windows"
$isMac = $PSVersionTable.OS -match "Darwin"
$isLnx = $PSVersionTable.OS -match "Linux"

$platform = if ($isWin) { "Windows" } elseif ($isMac) { "macOS" } else { "Linux" }
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
# Direct invocation (don't use Check-Tool/Invoke-Expression; the latter has
# quoting/exit-code interactions that occasionally report `az` as not found
# even when it works at the prompt).
$hasAzCli = $false
$azCmd = Get-Command az -ErrorAction SilentlyContinue
if ($azCmd) {
    try {
        $azVerJson = az version --output json 2>$null | ConvertFrom-Json
        $cliVer = $azVerJson.'azure-cli'
        if ($cliVer) {
            Write-Host "  ✓ Azure CLI $cliVer" -ForegroundColor Green
            $pass++
            $hasAzCli = $true
        } else {
            Write-Host "  ⚠ Azure CLI present but version unreadable" -ForegroundColor Yellow
            $warn++
            $hasAzCli = $true  # binary exists; downstream checks can still try
        }
    } catch {
        Write-Host "  ⚠ Azure CLI present but `az version` failed: $($_.Exception.Message)" -ForegroundColor Yellow
        $warn++
        $hasAzCli = $true
    }
} else {
    Write-Host "  ✗ Azure CLI — not found" -ForegroundColor Red
    Write-Host "    Install: https://aka.ms/installazurecli" -ForegroundColor DarkGray
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

    # ── 3b. Azure CLI extension dynamic-install ──────────────────────
    # The orchestrator runs pwsh with -NonInteractive. If az needs to install an
    # extension (e.g. `healthcareapis`) and the default `yes_prompt` setting is in
    # effect, the deployment hangs forever waiting on stdin.
    $dynInstall = az config get extension.use_dynamic_install --query value -o tsv 2>$null
    if ($dynInstall -eq "yes_without_prompt") {
        Write-Host "  ✓ Az CLI extension auto-install (yes_without_prompt)" -ForegroundColor Green
        $pass++
    } else {
        if (-not $CheckOnly) {
            Write-Host "  ⚙ Configuring az CLI to auto-install extensions without prompt..." -ForegroundColor Yellow
            $null = az config set extension.use_dynamic_install=yes_without_prompt --only-show-errors 2>$null
            $installed++
            Write-Host "  ✓ Az CLI extension auto-install set to yes_without_prompt" -ForegroundColor Green
            $pass++
        } else {
            Write-Host "  ⚠ Az CLI extension auto-install is '$dynInstall' (will hang under -NonInteractive)" -ForegroundColor Yellow
            Write-Host "    Fix: az config set extension.use_dynamic_install=yes_without_prompt" -ForegroundColor DarkGray
            $warn++
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

# ── 5. Python 3.10-3.13 ───────────────────────────────────────────────
Write-Host ""
Write-Host "  Checking Python + Node.js..." -ForegroundColor White
$hasPython = $false
$pythonFile = $null
$pythonArgs = @()
$pythonLabel = $null
$pythonCandidates = if ($isWin) {
    @(
        @{ File = "py"; Args = @("-3.13") },
        @{ File = "py"; Args = @("-3.12") },
        @{ File = "py"; Args = @("-3.11") },
        @{ File = "py"; Args = @("-3.10") },
        @{ File = "python"; Args = @() }
    )
} else {
    @(
        @{ File = "python3.13"; Args = @() },
        @{ File = "python3.12"; Args = @() },
        @{ File = "python3.11"; Args = @() },
        @{ File = "python3.10"; Args = @() },
        @{ File = "python3"; Args = @() },
        @{ File = "python"; Args = @() }
    )
}

function Select-SupportedPython {
    foreach ($candidate in $pythonCandidates) {
        if (-not (Get-Command $candidate.File -ErrorAction SilentlyContinue)) { continue }
        $candidateArgs = @($candidate.Args)
        $candidateLabel = ($candidate.File + " " + ($candidateArgs -join " ")).Trim()
        $pyVer = & $candidate.File @candidateArgs --version 2>&1
        if ($LASTEXITCODE -ne 0) { continue }
        if ($pyVer -match "(\d+)\.(\d+)\.(\d+)") {
            $major = [int]$Matches[1]; $minor = [int]$Matches[2]
            if ($major -eq 3 -and $minor -ge 10 -and $minor -le 13) {
                Write-Host "  ✓ Python $($Matches[0]) via $candidateLabel" -ForegroundColor Green
                $script:pass++
                $script:hasPython = $true
                $script:pythonFile = $candidate.File
                $script:pythonArgs = $candidateArgs
                $script:pythonLabel = $candidateLabel
                return $true
            }
            if ($major -eq 3 -and $minor -ge 14) {
                Write-Host "  ⚠ Python $($Matches[0]) via $candidateLabel is too new for this repo's Windows native dependencies" -ForegroundColor Yellow
                $script:warn++
            }
        }
    }
    return $false
}

Select-SupportedPython | Out-Null

if (-not $hasPython -and $isWin -and -not $CheckOnly) {
    $wingetCmd = Get-Command winget -ErrorAction SilentlyContinue
    if ($wingetCmd) {
        Write-Host "  ⚙ Installing Python 3.13 with winget..." -ForegroundColor Yellow
        winget install --id Python.Python.3.13 -e --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -eq 0) {
            $installed++
            # The Python launcher may be available immediately even if PATH is not refreshed.
            Select-SupportedPython | Out-Null
        } else {
            Write-Host "  ✗ Python 3.13 winget install failed" -ForegroundColor Red
            $fail++
        }
    } else {
        Write-Host "  ✗ Python 3.10-3.13 — not found and winget is unavailable" -ForegroundColor Red
        Write-Host "    Install: https://www.python.org/downloads/windows/" -ForegroundColor DarkGray
        $fail++
    }
}

if (-not $hasPython) {
    Write-Host "  ✗ Python 3.10-3.13 — not found" -ForegroundColor Red
    Write-Host "    Python 3.14+ is not supported here yet: cryptography may build from source and require Visual Studio C++ link.exe on Windows ARM64." -ForegroundColor DarkGray
    Write-Host "    Install: winget install --id Python.Python.3.13 -e --accept-package-agreements --accept-source-agreements" -ForegroundColor DarkGray
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
    $venvPython = if ($isWin) { Join-Path $venvPath "Scripts/python.exe" } else { Join-Path $venvPath "bin/python" }
    $venvNeedsRecreate = $false

    if (Test-Path $venvPython) {
        $venvVer = & $venvPython --version 2>&1
        if ($venvVer -match "(\d+)\.(\d+)\.(\d+)") {
            $venvMajor = [int]$Matches[1]; $venvMinor = [int]$Matches[2]
            if (-not ($venvMajor -eq 3 -and $venvMinor -ge 10 -and $venvMinor -le 13)) {
                Write-Host "  ⚠ Existing venv uses Python $($Matches[0]); recreating with $pythonLabel" -ForegroundColor Yellow
                $warn++
                $venvNeedsRecreate = $true
            } else {
                Write-Host "  ✓ Python venv exists (orchestrator/.venv, Python $($Matches[0]))" -ForegroundColor Green
                $pass++
            }
        } else {
            Write-Host "  ⚠ Existing venv Python version unreadable; recreating with $pythonLabel" -ForegroundColor Yellow
            $warn++
            $venvNeedsRecreate = $true
        }
    } elseif (Test-Path $venvPath) {
        Write-Host "  ⚠ Python venv folder exists but interpreter is missing; recreating with $pythonLabel" -ForegroundColor Yellow
        $warn++
        $venvNeedsRecreate = $true
    }

    if ($venvNeedsRecreate) {
        if ($CheckOnly) {
            Write-Host "  ✗ Python venv must be recreated" -ForegroundColor Red
            Write-Host "    Fix: Remove-Item -Recurse -Force .\orchestrator\.venv; .\setup-prereqs.ps1" -ForegroundColor DarkGray
            $fail++
        } else {
            Remove-Item -Recurse -Force $venvPath -ErrorAction SilentlyContinue
        }
    }

    if (-not (Test-Path $venvPath) -and -not $CheckOnly) {
        Write-Host "  ⚙ Creating Python virtual environment with $pythonLabel..." -ForegroundColor Yellow
        & $pythonFile @pythonArgs -m venv $venvPath
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  ✗ Python virtual environment creation failed" -ForegroundColor Red
            $fail++
        } else {
            $installed++
            Write-Host "  ✓ Virtual environment created at orchestrator/.venv" -ForegroundColor Green
        }
    } elseif (-not (Test-Path $venvPath) -and $CheckOnly) {
        Write-Host "  ✗ Python venv not created (orchestrator/.venv)" -ForegroundColor Yellow
        $warn++
    }

    # Install/verify Python dependencies using the venv interpreter. Do not hide
    # pip failures: a partial/stale venv is worse than no venv because Start-WebUI
    # will find it and then fail later with ModuleNotFoundError.
    if (Test-Path $venvPath) {
        $venvPython = if ($isWin) { Join-Path $venvPath "Scripts/python.exe" } else { Join-Path $venvPath "bin/python" }
        if (-not (Test-Path $venvPython)) {
            Write-Host "  ✗ Python venv interpreter missing at $venvPython" -ForegroundColor Red
            $fail++
        } elseif (-not $CheckOnly) {
            Write-Host "  ⚙ Installing Python dependencies..." -ForegroundColor Yellow
            & $venvPython -m pip install --upgrade pip
            if ($LASTEXITCODE -ne 0) {
                Write-Host "  ✗ pip upgrade failed" -ForegroundColor Red
                $fail++
            } else {
                & $venvPython -m pip install --no-cache-dir --only-binary cryptography -r $requirementsPath
                if ($LASTEXITCODE -ne 0) {
                    Write-Host "  ✗ Python dependency install failed" -ForegroundColor Red
                    Write-Host "    Retry: $venvPython -m pip install --no-cache-dir --only-binary cryptography -r $requirementsPath" -ForegroundColor DarkGray
                    Write-Host "    If cryptography still tries to build from source, install Visual Studio Build Tools with the C++ workload or use Windows x64 Python under emulation." -ForegroundColor DarkGray
                    $fail++
                } else {
                    & $venvPython -c "import fastapi, uvicorn, pydantic"
                    if ($LASTEXITCODE -ne 0) {
                        Write-Host "  ✗ Python dependency verification failed (fastapi/uvicorn/pydantic import)" -ForegroundColor Red
                        $fail++
                    } else {
                        $installed++
                        Write-Host "  ✓ Python dependencies installed and verified" -ForegroundColor Green
                        $pass++
                    }
                }
            }
        } else {
            & $venvPython -c "import fastapi, uvicorn, pydantic" 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  ✓ Python dependencies present" -ForegroundColor Green
                $pass++
            } else {
                Write-Host "  ✗ Python dependencies missing from orchestrator/.venv" -ForegroundColor Red
                Write-Host "    Fix: $venvPython -m pip install --no-cache-dir --only-binary cryptography -r $requirementsPath" -ForegroundColor DarkGray
                $fail++
            }
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
