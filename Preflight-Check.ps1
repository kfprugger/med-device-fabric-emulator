# Preflight-Check.ps1
# Runs only the prerequisite checks from Deploy-All.ps1 without deploying.
# Used by the orchestrator web UI for pre-deployment validation.
#
# Usage:
#   .\Preflight-Check.ps1 -FabricWorkspaceName "my-ws" -Location "eastus" -AdminSecurityGroup "sg-admins"

param (
    [string]$FabricWorkspaceName = "",
    [string]$Location = "eastus",
    [string]$AdminSecurityGroup = "",
    [string]$DicomToolkitPath = "",
    [switch]$Phase3
)

$ErrorActionPreference = "Stop"

# Source the prerequisite check function from Deploy-All.ps1
# We duplicate it here to avoid running the full script
Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Cyan
Write-Host "|              PREFLIGHT PREREQUISITE CHECKS                 |" -ForegroundColor Cyan
Write-Host "+============================================================+" -ForegroundColor Cyan
Write-Host ""

$failures = @()
$warnings = @()
$checks = @()

# 1. PowerShell version (7+)
if ($PSVersionTable.PSVersion.Major -ge 7) {
    $checks += @{ name = "PowerShell"; status = "pass"; detail = "v$($PSVersionTable.PSVersion)" }
    Write-Host "  ✓ PowerShell $($PSVersionTable.PSVersion)" -ForegroundColor Green
} else {
    $checks += @{ name = "PowerShell"; status = "fail"; detail = "v$($PSVersionTable.PSVersion) — need 7+" }
    $failures += "PowerShell 7+ required (current: $($PSVersionTable.PSVersion)). Install from https://aka.ms/powershell"
    Write-Host "  ✗ PowerShell $($PSVersionTable.PSVersion) — version 7+ required" -ForegroundColor Red
}

# 2. Az PowerShell module
$azModule = Get-Module -ListAvailable -Name Az.Accounts | Select-Object -First 1
if ($azModule) {
    $checks += @{ name = "Az Module"; status = "pass"; detail = "v$($azModule.Version)" }
    Write-Host "  ✓ Az module $($azModule.Version)" -ForegroundColor Green
} else {
    $checks += @{ name = "Az Module"; status = "fail"; detail = "Not installed" }
    $failures += "Az PowerShell module not found. Run: Install-Module Az -Scope CurrentUser"
    Write-Host "  ✗ Az module not installed" -ForegroundColor Red
}

# 3. Azure CLI
try {
    $azVer = az version --output json 2>$null | ConvertFrom-Json
    $cliVer = $azVer.'azure-cli'
    $checks += @{ name = "Azure CLI"; status = "pass"; detail = "v$cliVer" }
    Write-Host "  ✓ Azure CLI $cliVer" -ForegroundColor Green
} catch {
    $checks += @{ name = "Azure CLI"; status = "fail"; detail = "Not installed" }
    $failures += "Azure CLI not found. Install from https://aka.ms/installazurecli"
    Write-Host "  ✗ Azure CLI not installed" -ForegroundColor Red
}

# 4. Bicep
try {
    $bicepOutput = (az bicep version 2>$null) -join ' '
    if ($bicepOutput -match '(\d+\.\d+\.\d+)') {
        $checks += @{ name = "Bicep"; status = "pass"; detail = "v$($Matches[1])" }
        Write-Host "  ✓ Bicep $($Matches[1])" -ForegroundColor Green
    } else {
        $checks += @{ name = "Bicep"; status = "warn"; detail = "Version unknown" }
        $warnings += "Bicep version check inconclusive. Run: az bicep install"
        Write-Host "  ⚠ Bicep version unknown" -ForegroundColor Yellow
    }
} catch {
    $checks += @{ name = "Bicep"; status = "fail"; detail = "Not installed" }
    $failures += "Bicep not installed. Run: az bicep install"
    Write-Host "  ✗ Bicep not installed" -ForegroundColor Red
}

# 5. Azure login
try {
    $account = az account show --output json 2>$null | ConvertFrom-Json
    if ($account.id) {
        $checks += @{ name = "Azure Login"; status = "pass"; detail = "$($account.name) ($($account.id.Substring(0,8))...)" }
        Write-Host "  ✓ Azure login: $($account.name)" -ForegroundColor Green
    } else {
        $checks += @{ name = "Azure Login"; status = "fail"; detail = "Not logged in" }
        $failures += "Not logged in to Azure. Run: az login"
        Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
    }
} catch {
    $checks += @{ name = "Azure Login"; status = "fail"; detail = "Not logged in" }
    $failures += "Not logged in to Azure. Run: az login"
    Write-Host "  ✗ Not logged in to Azure" -ForegroundColor Red
}

# 6. Python 3.10+
try {
    $pyVer = python --version 2>&1
    if ($pyVer -match "(\d+)\.(\d+)\.(\d+)") {
        $major = [int]$Matches[1]; $minor = [int]$Matches[2]
        if ($major -ge 3 -and $minor -ge 10) {
            $checks += @{ name = "Python"; status = "pass"; detail = "v$($Matches[0])" }
            Write-Host "  ✓ Python $($Matches[0])" -ForegroundColor Green
        } else {
            $checks += @{ name = "Python"; status = "fail"; detail = "v$($Matches[0]) — need 3.10+" }
            $failures += "Python 3.10+ required (current: $($Matches[0]))"
            Write-Host "  ✗ Python $($Matches[0]) — version 3.10+ required" -ForegroundColor Red
        }
    }
} catch {
    $checks += @{ name = "Python"; status = "warn"; detail = "Not found (optional)" }
    $warnings += "Python not found (only needed for device associations)"
    Write-Host "  ⚠ Python not found (optional)" -ForegroundColor Yellow
}

# 7. Admin Security Group
if ($AdminSecurityGroup) {
    try {
        $grp = az ad group show --group $AdminSecurityGroup --query "id" -o tsv 2>$null
        if ($grp) {
            $checks += @{ name = "Admin Group"; status = "pass"; detail = "'$AdminSecurityGroup' found" }
            Write-Host "  ✓ Admin group '$AdminSecurityGroup' found" -ForegroundColor Green
        } else {
            $checks += @{ name = "Admin Group"; status = "fail"; detail = "'$AdminSecurityGroup' not found" }
            $failures += "Security group '$AdminSecurityGroup' not found in Entra ID"
            Write-Host "  ✗ Security group '$AdminSecurityGroup' not found" -ForegroundColor Red
        }
    } catch {
        $checks += @{ name = "Admin Group"; status = "fail"; detail = "'$AdminSecurityGroup' not found" }
        $failures += "Security group '$AdminSecurityGroup' not found in Entra ID"
        Write-Host "  ✗ Security group '$AdminSecurityGroup' not found" -ForegroundColor Red
    }
}

# 8. Fabric capacity
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
        $checks += @{ name = "Fabric Capacity"; status = "pass"; detail = "$($cap.displayName) (SKU: $($cap.sku))" }
        Write-Host "  ✓ Fabric capacity: $($cap.displayName) ($($cap.sku))" -ForegroundColor Green
    } elseif ($activeCaps.Count -gt 0) {
        $cap = $activeCaps | Select-Object -First 1
        $checks += @{ name = "Fabric Capacity"; status = "fail"; detail = "$($cap.displayName) (SKU: $($cap.sku)) — trial not supported" }
        $failures += "Trial capacity ($($cap.sku)) cannot deploy Healthcare Data Solutions. A paid F-SKU (F2+) is required."
        Write-Host "  ✗ Fabric capacity: $($cap.displayName) ($($cap.sku)) — trial not supported" -ForegroundColor Red
    } else {
        $checks += @{ name = "Fabric Capacity"; status = "fail"; detail = "No active capacity" }
        $failures += "No active Fabric capacity found. Resume or create at https://app.fabric.microsoft.com"
        Write-Host "  ✗ No active Fabric capacity" -ForegroundColor Red
    }
} catch {
    $checks += @{ name = "Fabric Capacity"; status = "fail"; detail = "API unreachable" }
    $failures += "Cannot access Fabric API. Ensure Az login has Fabric permissions."
    Write-Host "  ✗ Fabric API unreachable" -ForegroundColor Red
}

Write-Host ""

# Output JSON result for the API
$result = @{
    passed   = ($failures.Count -eq 0)
    checks   = $checks
    failures = $failures
    warnings = $warnings
}
$result | ConvertTo-Json -Depth 5

if ($failures.Count -gt 0) {
    exit 1
} else {
    exit 0
}
