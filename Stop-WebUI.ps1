<#
.SYNOPSIS
    Stop the Orchestrator UI (backend + frontend).

.DESCRIPTION
    Stops the FastAPI backend (port 7071) and Vite frontend (port 5173).
    Detects existing processes on those ports and prompts before killing them.

.PARAMETER Force
    Skip confirmation prompts when killing existing processes.

.EXAMPLE
    .\Stop-WebUI.ps1              # Stop both servers with confirmation
    .\Stop-WebUI.ps1 -Force       # Stop both servers without confirmation
#>

param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
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

# ── Main ───────────────────────────────────────────────────────────────

Write-Banner "STOPPING ORCHESTRATOR UI"

Stop-PortProcess -Port $BackendPort -Label "Backend"  | Out-Null
Stop-PortProcess -Port $FrontendPort -Label "Frontend" | Out-Null

Write-Host ""
Write-Host "  ✓ Servers stopped" -ForegroundColor Green
Write-Host ""
