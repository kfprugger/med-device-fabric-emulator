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

.EXAMPLE
    .\Start-WebUI.ps1              # Start both servers
    .\Start-WebUI.ps1 -Stop        # Stop both servers
    .\Start-WebUI.ps1 -Force       # Start, auto-kill any existing processes
#>

param(
    [switch]$Stop,
    [switch]$Force,
    [switch]$SelfTest
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendDir = Join-Path $ScriptDir "orchestrator"
$FrontendDir = Join-Path $ScriptDir "orchestrator-ui"
$VenvPython = Join-Path $BackendDir ".venv/bin/python"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = Join-Path $BackendDir ".venv\Scripts\python.exe"
}
$BackendScript = Join-Path $BackendDir "local_server.py"
$BackendPort = 7071
$FrontendPort = 5173
$BackendBaseUrl = "http://127.0.0.1:$BackendPort"
$FrontendBaseUrl = "http://127.0.0.1:$FrontendPort"
$SessionId = Get-Date -Format "yyyyMMdd-HHmmss"
$BackendSessionLog = Join-Path $BackendDir "orchestrator-session-$SessionId.log"



# Prefer Joey's isolated BrakeKat Azure CLI profile for this repo. The backend
# launches PowerShell deployment scripts and Azure CLI scans; letting it inherit
# the global CLI profile can point Fabric/Azure calls at the wrong tenant.
$BrakeKatAzureConfig = Join-Path $HOME ".azure-isolated/BrakeKat"
if (-not $env:AZURE_CONFIG_DIR -and (Test-Path $BrakeKatAzureConfig)) {
    $env:AZURE_CONFIG_DIR = $BrakeKatAzureConfig
}
if ($env:AZURE_CONFIG_DIR -eq $BrakeKatAzureConfig -and -not $env:AZURE_TENANT_ID) {
    $env:AZURE_TENANT_ID = "8d038e6a-9b7d-4cb8-bbcf-e84dff156478"
}
if ($env:AZURE_CONFIG_DIR) {
    Write-Host "  Azure CLI profile: $env:AZURE_CONFIG_DIR" -ForegroundColor DarkGray
}

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
    if ($IsWindows) {
        $conns = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
            Where-Object { $_.OwningProcess -ne 0 -and $_.State -eq "Listen" }
        if ($conns) {
            $procIds = $conns | Select-Object -ExpandProperty OwningProcess -Unique
            foreach ($procId in $procIds) {
                Get-Process -Id $procId -ErrorAction SilentlyContinue
            }
        }
    } else {
        $pidStr = (lsof -t -i :$Port -s TCP:LISTEN 2>/dev/null)
        if ($pidStr) {
            $pids = $pidStr -split "\n" | Where-Object { $_ -match '^\d+$' }
            foreach ($foundPid in $pids) {
                Get-Process -Id ([int]$foundPid) -ErrorAction SilentlyContinue
            }
        }
    }
}

function Test-PortListening {
    param([int]$Port)
    if ($IsWindows) {
        $conn = Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue |
            Where-Object { $_.State -eq "Listen" }
        return $conn -ne $null
    } else {
        $lsofOut = (lsof -i :$Port -s TCP:LISTEN 2>/dev/null)
        return -not [string]::IsNullOrEmpty($lsofOut)
    }
}

function Test-HttpEndpoint {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][string]$Label,
        [int]$TimeoutSeconds = 3,
        [switch]$Quiet
    )

    try {
        $response = Invoke-WebRequest -Method Get -Uri $Uri -UseBasicParsing -TimeoutSec $TimeoutSeconds -ErrorAction Stop
        $ok = [int]$response.StatusCode -ge 200 -and [int]$response.StatusCode -lt 400
        if (-not $Quiet) {
            if ($ok) {
                Write-Host "  ✓ $Label HTTP probe passed ($($response.StatusCode)): $Uri" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ $Label HTTP probe returned $($response.StatusCode): $Uri" -ForegroundColor Yellow
            }
        }
        return $ok
    } catch {
        if (-not $Quiet) {
            Write-Host "  ⚠ $Label HTTP probe failed: $Uri — $($_.Exception.Message)" -ForegroundColor Yellow
        }
        return $false
    }
}

function Start-DetachedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory = $true)][string]$StandardOutputPath,
        [Parameter(Mandatory = $true)][string]$StandardErrorPath
    )

    $params = @{
        WorkingDirectory       = $WorkingDirectory
        PassThru               = $true
        RedirectStandardOutput = $StandardOutputPath
        RedirectStandardError  = $StandardErrorPath
    }

    if ($IsWindows) {
        $params["WindowStyle"] = "Hidden"
        return Start-Process -FilePath $FilePath -ArgumentList $ArgumentList @params
    }

    $nohup = Get-Command "nohup" -ErrorAction SilentlyContinue
    if ($nohup) {
        return Start-Process -FilePath $nohup.Source -ArgumentList (@($FilePath) + $ArgumentList) @params
    }

    return Start-Process -FilePath $FilePath -ArgumentList $ArgumentList @params
}

function Show-RecentLog {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [int]$Tail = 80
    )

    if (-not (Test-Path $Path)) {
        Write-Host "    Log not found: $Path" -ForegroundColor DarkGray
        return
    }

    Write-Host "    Last $Tail lines from ${Path}:" -ForegroundColor DarkGray
    Get-Content -Path $Path -Tail $Tail -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Host "      $_" -ForegroundColor DarkGray
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

function Assert-SelfTest {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw "Self-test failed: $Message" }
}

function Start-TestHttpResponder {
    $readyPath = Join-Path ([System.IO.Path]::GetTempPath()) ("start-webui-selftest-{0}.ready" -f ([guid]::NewGuid().ToString("N")))
    $job = Start-Job -ArgumentList $readyPath -ScriptBlock {
        param([string]$ReadyPath)
        $ErrorActionPreference = 'Stop'
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse("127.0.0.1"), 0)
        $client = $null
        $listener.Start()
        $port = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
        Set-Content -LiteralPath $ReadyPath -Value $port -Encoding ASCII
        try {
            $client = $listener.AcceptTcpClient()
            $stream = $client.GetStream()
            $buffer = New-Object byte[] 1024
            [void]$stream.Read($buffer, 0, $buffer.Length)
            $body = "ok"
            $response = "HTTP/1.1 200 OK`r`nContent-Length: $($body.Length)`r`nConnection: close`r`n`r`n$body"
            $bytes = [System.Text.Encoding]::ASCII.GetBytes($response)
            $stream.Write($bytes, 0, $bytes.Length)
        } finally {
            if ($null -ne $client) { $client.Close() }
            $listener.Stop()
        }
    }

    $deadline = [DateTime]::UtcNow.AddSeconds(5)
    while (-not (Test-Path -LiteralPath $readyPath)) {
        if ($job.State -ne 'Running') {
            $jobOutput = Receive-Job -Job $job -Keep -ErrorAction SilentlyContinue | Out-String
            Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
            throw "Self-test responder failed to start: $($job.State). $jobOutput"
        }
        if ([DateTime]::UtcNow -gt $deadline) {
            Stop-Job -Job $job -ErrorAction SilentlyContinue
            Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
            Remove-Item -LiteralPath $readyPath -Force -ErrorAction SilentlyContinue
            throw "Self-test responder did not become ready"
        }
        Start-Sleep -Milliseconds 50
    }

    try {
        $port = [int]((Get-Content -LiteralPath $readyPath -Raw).Trim())
    } catch {
        Stop-Job -Job $job -ErrorAction SilentlyContinue
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $readyPath -Force -ErrorAction SilentlyContinue
        throw "Self-test responder did not publish a valid port"
    }

    return [pscustomobject]@{ Port = $port; Job = $job; ReadyPath = $readyPath }
}

function Invoke-StartWebUiSelfTest {
    Assert-SelfTest ($BackendBaseUrl -eq "http://127.0.0.1:$BackendPort") "BackendBaseUrl must use IPv4 loopback"
    Assert-SelfTest ($FrontendBaseUrl -eq "http://127.0.0.1:$FrontendPort") "FrontendBaseUrl must use IPv4 loopback"

    $server = Start-TestHttpResponder
    try {
        Assert-SelfTest (Test-HttpEndpoint -Uri "http://127.0.0.1:$($server.Port)/" -Label "Self-test HTTP 200" -Quiet) "Test-HttpEndpoint should return true for HTTP 200"
        $completedJob = Wait-Job -Job $server.Job -Timeout 2
        Assert-SelfTest ($null -ne $completedJob) "Self-test HTTP responder job did not complete"
        Assert-SelfTest ($server.Job.State -eq "Completed") "Self-test HTTP responder job ended in state $($server.Job.State)"
    } finally {
        Remove-Item -LiteralPath $server.ReadyPath -Force -ErrorAction SilentlyContinue
        Remove-Job -Job $server.Job -Force -ErrorAction SilentlyContinue
    }

    $closedListener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Parse("127.0.0.1"), 0)
    $closedListener.Start()
    $closedPort = ([System.Net.IPEndPoint]$closedListener.LocalEndpoint).Port
    $closedListener.Stop()
    Assert-SelfTest (-not (Test-HttpEndpoint -Uri "http://127.0.0.1:$closedPort/" -Label "Self-test closed port" -TimeoutSeconds 1 -Quiet)) "Test-HttpEndpoint should return false for refused connections"

    Write-Host "  ✓ Start-WebUI helper self-test passed" -ForegroundColor Green
}

if ($SelfTest) {
    Invoke-StartWebUiSelfTest
    exit 0
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

# ── Preflight checks ──────────────────────────────────────────────────

if (-not (Test-Path $VenvPython)) {
    Write-Host "  ✗ Python venv not found at: $VenvPython" -ForegroundColor Red
    Write-Host "    Run .\setup-prereqs.ps1 first" -ForegroundColor DarkGray
    exit 1
}

$venvVersion = & $VenvPython --version 2>&1
if ($venvVersion -match "(\d+)\.(\d+)\.(\d+)") {
    $venvMajor = [int]$Matches[1]
    $venvMinor = [int]$Matches[2]
    if (-not ($venvMajor -eq 3 -and $venvMinor -ge 10 -and $venvMinor -le 13)) {
        Write-Host "  ✗ Backend venv uses Python $($Matches[0]); use Python 3.10-3.13 for Windows native dependency wheels" -ForegroundColor Red
        Write-Host "    Fix: Remove-Item -Recurse -Force .\orchestrator\.venv; .\setup-prereqs.ps1" -ForegroundColor DarkGray
        exit 1
    }
}

# The venv can exist while dependencies are missing if setup-prereqs.ps1 was
# interrupted or an older version swallowed pip failures. Fail here with the
# exact repair command instead of starting a backend that immediately crashes.
& $VenvPython -c "import fastapi, uvicorn, pydantic" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Backend Python dependencies are missing from orchestrator/.venv" -ForegroundColor Red
    Write-Host "    Fix: & '$VenvPython' -m pip install -r '$(Join-Path $BackendDir "requirements.txt")'" -ForegroundColor DarkGray
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

$env:ORCHESTRATOR_LOG_SESSION = $SessionId

# ── Start backend ─────────────────────────────────────────────────────

Write-Host ""
Write-Host "  Starting backend (port $BackendPort)..." -ForegroundColor White

$backendProc = Start-DetachedProcess `
    -FilePath $VenvPython `
    -WorkingDirectory $BackendDir `
    -ArgumentList @($BackendScript) `
    -StandardOutputPath (Join-Path $BackendDir "backend-stdout.log") `
    -StandardErrorPath (Join-Path $BackendDir "backend-stderr.log")

# Wait for backend HTTP liveness to come up (max 15 seconds)
$waited = 0
$backendReady = $false
$backendProbeUri = "$BackendBaseUrl/api/live"
$backendFallbackProbeUri = "$BackendBaseUrl/openapi.json"
while ($waited -lt 15) {
    Start-Sleep -Milliseconds 500
    $waited += 0.5
    if ($backendProc.HasExited) {
        Write-Host "  ✗ Backend process exited immediately (exit code: $($backendProc.ExitCode))" -ForegroundColor Red
        Show-RecentLog -Path (Join-Path $BackendDir "backend-stderr.log")
        Show-RecentLog -Path (Join-Path $BackendDir "backend-crash-dump.log")
        Write-Host "    Check: $BackendDir\backend-stderr.log" -ForegroundColor DarkGray
        Write-Host "    Check: $BackendDir\backend-crash-dump.log" -ForegroundColor DarkGray
        exit 1
    }
    if (Test-PortListening -Port $BackendPort) {
        if (Test-HttpEndpoint -Uri $backendProbeUri -Label "Backend liveness" -Quiet) {
            $backendReady = $true
            break
        }
        if (Test-HttpEndpoint -Uri $backendFallbackProbeUri -Label "Backend OpenAPI" -Quiet) {
            $backendProbeUri = $backendFallbackProbeUri
            $backendReady = $true
            break
        }
    }
}

if ($backendReady) {
    Write-Host "  ✓ Backend HTTP ready — $backendProbeUri (PID $($backendProc.Id))" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Backend process started but HTTP probe did not pass after ${waited}s" -ForegroundColor Yellow
    Write-Host "    PID: $($backendProc.Id) — check backend-stderr.log" -ForegroundColor DarkGray
    Show-RecentLog -Path (Join-Path $BackendDir "backend-stderr.log") -Tail 25
    exit 1
}

# ── Start frontend ────────────────────────────────────────────────────

Write-Host "  Starting frontend (port $FrontendPort)..." -ForegroundColor White

$npmExe = if ($IsWindows) { "npm.cmd" } else { "npm" }
$frontendProc = Start-DetachedProcess `
    -FilePath $npmExe `
    -WorkingDirectory $FrontendDir `
    -ArgumentList @("run", "dev") `
    -StandardOutputPath (Join-Path $FrontendDir "frontend-stdout.log") `
    -StandardErrorPath (Join-Path $FrontendDir "frontend-stderr.log")

# Wait for frontend HTTP endpoint to come up (max 15 seconds)
$waited = 0
$frontendReady = $false
$frontendProbeUri = "$FrontendBaseUrl/"
while ($waited -lt 15) {
    Start-Sleep -Milliseconds 500
    $waited += 0.5
    if ($frontendProc.HasExited) {
        Write-Host "  ✗ Frontend process exited immediately (exit code: $($frontendProc.ExitCode))" -ForegroundColor Red
        Show-RecentLog -Path (Join-Path $FrontendDir "frontend-stderr.log")
        Write-Host "    Check: $FrontendDir\frontend-stderr.log" -ForegroundColor DarkGray
        exit 1
    }
    if ((Test-PortListening -Port $FrontendPort) -and (Test-HttpEndpoint -Uri $frontendProbeUri -Label "Frontend" -Quiet)) {
        $frontendReady = $true
        break
    }
}

if ($frontendReady) {
    Write-Host "  ✓ Frontend HTTP ready — $frontendProbeUri (PID $($frontendProc.Id))" -ForegroundColor Green
} else {
    Write-Host "  ⚠ Frontend process started but HTTP probe did not pass after ${waited}s" -ForegroundColor Yellow
    Write-Host "    PID: $($frontendProc.Id) — check frontend-stderr.log" -ForegroundColor DarkGray
    Show-RecentLog -Path (Join-Path $FrontendDir "frontend-stderr.log") -Tail 25
    exit 1
}

$proxyReady = $false
if ($backendReady -and $frontendReady) {
    $proxyUri = "$FrontendBaseUrl/api/live"
    $proxyReady = Test-HttpEndpoint -Uri $proxyUri -Label "Frontend API proxy" -Quiet
    if ($proxyReady) {
        Write-Host "  ✓ Frontend API proxy verified — $proxyUri" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Frontend API proxy did not respond at $proxyUri" -ForegroundColor Red
        Show-RecentLog -Path (Join-Path $FrontendDir "frontend-stderr.log") -Tail 25
        exit 1
    }
}

try { $backendProc.Refresh() } catch { }
if ($backendProc.HasExited) {
    Write-Host "  ✗ Backend exited during startup (exit code: $($backendProc.ExitCode))" -ForegroundColor Red
    Show-RecentLog -Path (Join-Path $BackendDir "backend-stderr.log")
    Show-RecentLog -Path (Join-Path $BackendDir "backend-crash-dump.log")
    exit 1
}

# ── Summary ───────────────────────────────────────────────────────────

Write-Host ""
Write-Host "  ┌─────────────────────────────────────────────────────────┐" -ForegroundColor DarkCyan
Write-Host "  │  Orchestrator UI                                       │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Frontend:  $FrontendBaseUrl                      │" -ForegroundColor DarkCyan
Write-Host "  │  Backend:   $BackendBaseUrl                       │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Logs:                                                 │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator\orchestrator.log                       │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator\backend-stderr.log                     │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator\backend-crash-dump.log                 │" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator\$([System.IO.Path]::GetFileName($BackendSessionLog).PadRight(33))│" -ForegroundColor DarkCyan
Write-Host "  │    orchestrator-ui\frontend-stderr.log                 │" -ForegroundColor DarkCyan
Write-Host "  │                                                        │" -ForegroundColor DarkCyan
Write-Host "  │  Stop:  .\Start-WebUI.ps1 -Stop                       │" -ForegroundColor DarkCyan
Write-Host "  └─────────────────────────────────────────────────────────┘" -ForegroundColor DarkCyan
Write-Host ""
