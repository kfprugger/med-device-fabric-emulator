<#
.SYNOPSIS
    Start or stop the Orchestrator UI (backend + frontend).

.DESCRIPTION
    Manages the FastAPI backend (port 7071) and Vite frontend (port 5173).
    Detects existing processes on those ports and prompts before killing them.

.PARAMETER Stop
    Stop both servers instead of starting them.

.PARAMETER Force
    Skip confirmation prompts when killing existing processes.

.PARAMETER SkipPrereqCheck
    Skip running setup-prereqs.ps1 -CheckOnly before starting. Use if you've
    already verified dependencies are installed.

.PARAMETER InstallPrereqs
    Run setup-prereqs.ps1 in install mode (not -CheckOnly) before starting.
    This will install any missing dependencies.

.EXAMPLE
    .\Start-WebUI.ps1                   # Check prereqs, then start both servers
    .\Start-WebUI.ps1 -Stop              # Stop both servers
    .\Start-WebUI.ps1 -Force             # Start, auto-kill any existing processes
    .\Start-WebUI.ps1 -InstallPrereqs    # Install missing prereqs then start
    .\Start-WebUI.ps1 -SkipPrereqCheck   # Skip prereq verification
#>

param(
    [switch]$Stop,
    [switch]$Force,
    [switch]$SkipPrereqCheck,
    [switch]$InstallPrereqs
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendDir = Join-Path $ScriptDir "orchestrator"
$FrontendDir = Join-Path $ScriptDir "orchestrator-ui"
$VenvPython = Join-Path $BackendDir ".venv\Scripts\python.exe"
$BackendScript = Join-Path $BackendDir "local_server.py"
$PrereqScript = Join-Path $ScriptDir "setup-prereqs.ps1"
$BackendPort = 7071
$FrontendPort = 5173

# ── Helpers ────────────────────────────────────────────────────────────

function Write-Banner {
    param([string]$Title)
    Write-Host ""
    Write-Host "  ╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host "  ║  $($Title.PadRight(54))  ║" -ForegroundColor Cyan
    Write-Host "  ╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
}

function Get-PortProcess {
    param([int]$Port)
    $conns = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
        Where-Object { $_.OwningProcess -ne 0 -and $_.State -eq "Listen" }
    if ($conns) {
        $procIds = $conns | Select-Object -ExpandProperty OwningProcess -Unique
        foreach ($procId in $procIds) {
            Get-Process -Id $procId -ErrorAction SilentlyContinue
        }
    }
}

function Stop-PortProcess {
    param([int]$Port, [string]$Label)
    $procs = Get-PortProcess -Port $Port
    if (-not $procs) {
        Write-Host "  ✓ Port $Port ($Label) — not in use" -ForegroundColor DarkGray
        return
    }
    foreach ($proc in $procs) {
        $desc = "$($proc.ProcessName) (PID $($proc.Id))"
        if (-not $Force) {
            Write-Host "  ⚠ Port $Port ($Label) is in use by: $desc" -ForegroundColor Yellow
            $answer = Read-Host "    Kill this process? [Y/n]"
            if ($answer -and $answer -notmatch '^[Yy]') {
                Write-Host "    Skipped — $desc left running" -ForegroundColor DarkGray
                return $false
            }
        } else {
            Write-Host "  ⚠ Killing $desc on port $Port ($Label)" -ForegroundColor Yellow
        }
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 500
        Write-Host "  ✓ Killed $desc" -ForegroundColor Green
    }
    return $true
}

# ── Stop mode ──────────────────────────────────────────────────────────

if ($Stop) {
    Write-Banner "STOPPING ORCHESTRATOR UI"

    Stop-PortProcess -Port $BackendPort -Label "Backend"  | Out-Null
    Stop-PortProcess -Port $FrontendPort -Label "Frontend" | Out-Null

    Write-Host ""
    Write-Host "  ✓ Servers stopped" -ForegroundColor Green
    Write-Host ""
    exit 0
}

# ── Start mode ─────────────────────────────────────────────────────────

Write-Banner "STARTING ORCHESTRATOR UI"

# ── Prereq verification ───────────────────────────────────────────────

if (-not $SkipPrereqCheck) {
    if (-not (Test-Path $PrereqScript)) {
        Write-Host "  ⚠ setup-prereqs.ps1 not found at: $PrereqScript" -ForegroundColor Yellow
        Write-Host "    Skipping prereq verification." -ForegroundColor DarkGray
    } else {
        if ($InstallPrereqs) {
            Write-Host "  Running setup-prereqs.ps1 (install mode)..." -ForegroundColor White
            & $PrereqScript
        } else {
            Write-Host "  Verifying prerequisites (setup-prereqs.ps1 -CheckOnly)..." -ForegroundColor White
            & $PrereqScript -CheckOnly
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Host ""
            Write-Host "  ✗ Prerequisite check failed." -ForegroundColor Red
            if (-not $InstallPrereqs) {
                Write-Host "    Run: .\Start-WebUI.ps1 -InstallPrereqs" -ForegroundColor DarkGray
                Write-Host "    Or:  .\setup-prereqs.ps1" -ForegroundColor DarkGray
                Write-Host "    Or skip this check with: .\Start-WebUI.ps1 -SkipPrereqCheck" -ForegroundColor DarkGray
            }
            exit 1
        }
        Write-Host "  ✓ Prerequisites verified" -ForegroundColor Green
        Write-Host ""
    }
}

# ── Preflight checks ──────────────────────────────────────────────────

if (-not (Test-Path $VenvPython)) {
    Write-Host "  ✗ Python venv not found at: $VenvPython" -ForegroundColor Red
    Write-Host "    Run .\setup-prereqs.ps1 first" -ForegroundColor DarkGray
    exit 1
}

if (-not (Test-Path (Join-Path $FrontendDir "node_modules"))) {
    Write-Host "  ✗ Frontend dependencies not installed" -ForegroundColor Red
    Write-Host "    Run: cd orchestrator-ui && npm install" -ForegroundColor DarkGray
    exit 1
}

# ── Check for existing processes ──────────────────────────────────────

Write-Host "  Checking for existing servers..." -ForegroundColor White

$backendBlocked = $false
$frontendBlocked = $false

$existingBackend = Get-PortProcess -Port $BackendPort
if ($existingBackend) {
    $result = Stop-PortProcess -Port $BackendPort -Label "Backend"
    if ($result -eq $false) { $backendBlocked = $true }
}
else {
    Write-Host "  ✓ Port $BackendPort (Backend) — available" -ForegroundColor DarkGray
}

$existingFrontend = Get-PortProcess -Port $FrontendPort
if ($existingFrontend) {
    $result = Stop-PortProcess -Port $FrontendPort -Label "Frontend"
    if ($result -eq $false) { $frontendBlocked = $true }
}
else {
    Write-Host "  ✓ Port $FrontendPort (Frontend) — available" -ForegroundColor DarkGray
}

if ($backendBlocked -or $frontendBlocked) {
    Write-Host ""
    Write-Host "  ✗ Cannot start — ports still in use" -ForegroundColor Red
    exit 1
}

# ── Start backend ─────────────────────────────────────────────────────

Write-Host ""
Write-Host "  Starting backend (port $BackendPort)..." -ForegroundColor White

$backendProc = Start-Process -FilePath $VenvPython -ArgumentList $BackendScript `
    -WorkingDirectory $BackendDir -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $BackendDir "backend-stdout.log") `
    -RedirectStandardError (Join-Path $BackendDir "backend-stderr.log")

# Wait for backend to come up (max 10 seconds)
$waited = 0
$backendReady = $false
while ($waited -lt 10) {
    Start-Sleep -Milliseconds 500
    $waited += 0.5
    $listener = Get-NetTCPConnection -LocalPort $BackendPort -ErrorAction SilentlyContinue |
        Where-Object { $_.State -eq "Listen" }
    if ($listener) {
        $backendReady = $true
        break
    }
    if ($backendProc.HasExited) {
        Write-Host "  ✗ Backend process exited immediately (exit code: $($backendProc.ExitCode))" -ForegroundColor Red
        Write-Host "    Check: $BackendDir\backend-stderr.log" -ForegroundColor DarkGray
        exit 1
    }
}

if ($backendReady) {
    Write-Host "  ✓ Backend running — http://localhost:$BackendPort (PID $($backendProc.Id))" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Backend started but port $BackendPort not yet listening after ${waited}s" -ForegroundColor Yellow
    Write-Host "    PID: $($backendProc.Id) — check backend-stderr.log" -ForegroundColor DarkGray
}

# ── Start frontend ────────────────────────────────────────────────────

Write-Host "  Starting frontend (port $FrontendPort)..." -ForegroundColor White

$frontendProc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c","npm","run","dev" `
    -WorkingDirectory $FrontendDir -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $FrontendDir "frontend-stdout.log") `
    -RedirectStandardError (Join-Path $FrontendDir "frontend-stderr.log")

# Wait for frontend to come up (max 15 seconds)
$waited = 0
$frontendReady = $false
while ($waited -lt 15) {
    Start-Sleep -Milliseconds 500
    $waited += 0.5
    $listener = Get-NetTCPConnection -LocalPort $FrontendPort -ErrorAction SilentlyContinue |
        Where-Object { $_.State -eq "Listen" }
    if ($listener) {
        $frontendReady = $true
        break
    }
    if ($frontendProc.HasExited) {
        Write-Host "  ✗ Frontend process exited immediately (exit code: $($frontendProc.ExitCode))" -ForegroundColor Red
        Write-Host "    Check: $FrontendDir\frontend-stderr.log" -ForegroundColor DarkGray
        exit 1
    }
}

if ($frontendReady) {
    Write-Host "  ✓ Frontend running — http://localhost:$FrontendPort (PID $($frontendProc.Id))" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Frontend started but port $FrontendPort not yet listening after ${waited}s" -ForegroundColor Yellow
    Write-Host "    PID: $($frontendProc.Id) — check frontend-stderr.log" -ForegroundColor DarkGray
}

# ── Summary ───────────────────────────────────────────────────────────

Write-Host ""
Write-Host "  ┌─────────────────────────────────────────────────────────┐" -ForegroundColor DarkCyan
Write-Host "  │  Orchestrator UI                                       │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Frontend:  http://localhost:$FrontendPort                      │" -ForegroundColor DarkCyan
Write-Host "  │  Backend:   http://localhost:$BackendPort                       │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Logs:                                                 │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator\orchestrator.log                       │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator\backend-stderr.log                     │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator-ui\frontend-stderr.log                 │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Stop:  .\Start-WebUI.ps1 -Stop                       │" -ForegroundColor DarkCyan
Write-Host "  └─────────────────────────────────────────────────────────┘" -ForegroundColor DarkCyan
Write-Host ""
