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
$azCliAccount = $null
$azPsContext = $null

# 0. Host architecture visibility
try {
    $hostArch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    $procArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
    if ($hostArch -eq "Arm64" -and $procArch -ne "Arm64") {
        $checks += @{ name = "Architecture"; status = "warn"; detail = "Host=$hostArch, PowerShell=$procArch (emulation)" }
        $warnings += "ARM64 host detected but PowerShell is running as $procArch. Use native ARM64 tooling where possible for better performance."
        Write-Host "  ⚠ Architecture: Host=$hostArch, PowerShell=$procArch (emulation)" -ForegroundColor Yellow
    } else {
        $checks += @{ name = "Architecture"; status = "pass"; detail = "Host=$hostArch, PowerShell=$procArch" }
        Write-Host "  ✓ Architecture: Host=$hostArch, PowerShell=$procArch" -ForegroundColor Green
    }
} catch {
    $checks += @{ name = "Architecture"; status = "warn"; detail = "Unable to detect" }
    $warnings += "Could not detect host/process architecture."
    Write-Host "  ⚠ Architecture: Unable to detect" -ForegroundColor Yellow
}

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
    $azCliAccount = az account show --output json 2>$null | ConvertFrom-Json
    if ($azCliAccount.id) {
        $checks += @{ name = "Azure CLI Login"; status = "pass"; detail = "$($azCliAccount.name) ($($azCliAccount.id.Substring(0,8))...)" }
        Write-Host "  ✓ Azure CLI login: $($azCliAccount.name)" -ForegroundColor Green
    } else {
        $checks += @{ name = "Azure CLI Login"; status = "fail"; detail = "Not logged in" }
        $failures += "Not logged in to Azure CLI. Run: az login"
        Write-Host "  ✗ Not logged in to Azure CLI" -ForegroundColor Red
    }
} catch {
    $checks += @{ name = "Azure CLI Login"; status = "fail"; detail = "Not logged in" }
    $failures += "Not logged in to Azure CLI. Run: az login"
    Write-Host "  ✗ Not logged in to Azure CLI" -ForegroundColor Red
}

# 6. Az PowerShell login
try {
    $azPsContext = Get-AzContext -ErrorAction Stop
    if ($azPsContext -and $azPsContext.Subscription -and $azPsContext.Subscription.Id) {
        $checks += @{ name = "Az PowerShell Login"; status = "pass"; detail = "$($azPsContext.Subscription.Name) ($($azPsContext.Subscription.Id.Substring(0,8))...)" }
        Write-Host "  ✓ Az PowerShell login: $($azPsContext.Subscription.Name)" -ForegroundColor Green
    } else {
        $checks += @{ name = "Az PowerShell Login"; status = "fail"; detail = "No active Az context" }
        $failures += "Not logged in to Azure PowerShell. Run: Connect-AzAccount"
        Write-Host "  ✗ Not logged in to Azure PowerShell" -ForegroundColor Red
    }
} catch {
    $checks += @{ name = "Az PowerShell Login"; status = "fail"; detail = "No active Az context" }
    $failures += "Not logged in to Azure PowerShell. Run: Connect-AzAccount"
    Write-Host "  ✗ Not logged in to Azure PowerShell" -ForegroundColor Red
}

# 7. Account context alignment (Azure CLI vs Az PowerShell)
if ($azCliAccount -and $azCliAccount.id -and $azPsContext -and $azPsContext.Subscription -and $azPsContext.Subscription.Id) {
    $cliSub = "$($azCliAccount.id)".Trim().ToLower()
    $psSub = "$($azPsContext.Subscription.Id)".Trim().ToLower()
    $cliTenant = "$($azCliAccount.tenantId)".Trim().ToLower()
    $psTenant = "$($azPsContext.Tenant.Id)".Trim().ToLower()

    if ($cliSub -eq $psSub -and $cliTenant -eq $psTenant) {
        $checks += @{ name = "Azure Context"; status = "pass"; detail = "Subscription and tenant aligned" }
        Write-Host "  ✓ Azure context aligned (CLI + Az PowerShell)" -ForegroundColor Green
    } else {
        $checks += @{ name = "Azure Context"; status = "fail"; detail = "CLI/Az subscription or tenant mismatch" }
        $failures += "Azure CLI and Az PowerShell contexts do not match. Align with: az account set -s <subscription> and Set-AzContext -Subscription <subscriptionId>"
        Write-Host "  ✗ Azure context mismatch between CLI and Az PowerShell" -ForegroundColor Red
        Write-Host "    CLI sub/tenant: $cliSub / $cliTenant" -ForegroundColor DarkGray
        Write-Host "    Az  sub/tenant: $psSub / $psTenant" -ForegroundColor DarkGray
    }
} else {
    $checks += @{ name = "Azure Context"; status = "fail"; detail = "Cannot compare CLI/Az contexts" }
    $failures += "Cannot validate Azure account context because one or both login contexts are missing."
    Write-Host "  ✗ Could not validate Azure context alignment" -ForegroundColor Red
}

# 8. Required Azure control-plane access
if ($azCliAccount -and $azCliAccount.id) {
    try {
        $subInfo = az rest --method get --url "https://management.azure.com/subscriptions/$($azCliAccount.id)?api-version=2022-12-01" -o json 2>$null | ConvertFrom-Json
        if ($subInfo.subscriptionId) {
            $checks += @{ name = "ARM Access"; status = "pass"; detail = "Can read subscription metadata" }
            Write-Host "  ✓ ARM access: subscription metadata readable" -ForegroundColor Green
        } else {
            $checks += @{ name = "ARM Access"; status = "fail"; detail = "Subscription metadata unreadable" }
            $failures += "Cannot read Azure subscription metadata via ARM. Ensure account has at least Reader access."
            Write-Host "  ✗ ARM access check failed" -ForegroundColor Red
        }
    } catch {
        $checks += @{ name = "ARM Access"; status = "fail"; detail = "Subscription metadata unreadable" }
        $failures += "Cannot read Azure subscription metadata via ARM. Ensure account has at least Reader access."
        Write-Host "  ✗ ARM access check failed" -ForegroundColor Red
    }

    try {
        $requiredProviders = @(
            "Microsoft.Resources",
            "Microsoft.KeyVault",
            "Microsoft.ContainerRegistry",
            "Microsoft.EventHub",
            "Microsoft.ContainerInstance",
            "Microsoft.Storage",
            "Microsoft.HealthcareApis"
        )

        $notRegistered = @()
        foreach ($provider in $requiredProviders) {
            $state = az provider show -n $provider --query registrationState -o tsv 2>$null
            if (-not $state) {
                $notRegistered += "$provider (unknown)"
            } elseif ($state -ne "Registered") {
                $notRegistered += "$provider ($state)"
            }
        }

        if ($notRegistered.Count -eq 0) {
            $checks += @{ name = "Resource Providers"; status = "pass"; detail = "All required providers registered" }
            Write-Host "  ✓ Required resource providers registered" -ForegroundColor Green
        } else {
            $checks += @{ name = "Resource Providers"; status = "warn"; detail = ($notRegistered -join ", ") }
            $warnings += "Some required Azure resource providers are not fully registered: $($notRegistered -join ', '). Register with: az provider register --namespace <provider>."
            Write-Host "  ⚠ Resource provider registration incomplete" -ForegroundColor Yellow
            Write-Host "    $($notRegistered -join ', ')" -ForegroundColor DarkGray
        }
    } catch {
        $checks += @{ name = "Resource Providers"; status = "warn"; detail = "Unable to validate provider registration" }
        $warnings += "Could not validate required Azure resource providers."
        Write-Host "  ⚠ Could not validate resource provider registration" -ForegroundColor Yellow
    }
} else {
    $checks += @{ name = "ARM Access"; status = "fail"; detail = "Skipped (no Azure CLI context)" }
    $failures += "Cannot validate ARM access without Azure CLI login context."
    Write-Host "  ✗ ARM access check skipped (no Azure CLI context)" -ForegroundColor Red
}

# 9. Python 3.10+
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

# 9b. Azure CLI dynamic-install setting
# The orchestrator runs pwsh with -NonInteractive; if `az` ever needs to install an
# extension on-the-fly (e.g. `healthcareapis`) and the prompt is enabled, the deployment
# hangs forever waiting on stdin. Force `yes_without_prompt` so installs are automatic.
# See #fix-4.
try {
    $dynInstall = az config get extension.use_dynamic_install --query value -o tsv 2>$null
    if (-not $dynInstall) { $dynInstall = "yes_prompt" }  # CLI default when unset
    if ($dynInstall -eq "yes_without_prompt") {
        $checks += @{ name = "Az CLI Extension Auto-Install"; status = "pass"; detail = "yes_without_prompt" }
        Write-Host "  ✓ Az CLI extension auto-install: yes_without_prompt" -ForegroundColor Green
    } elseif ($dynInstall -eq "no") {
        $checks += @{ name = "Az CLI Extension Auto-Install"; status = "fail"; detail = "disabled (no)" }
        $failures += "Azure CLI extension dynamic-install is disabled. Run: az config set extension.use_dynamic_install=yes_without_prompt"
        Write-Host "  ✗ Az CLI extension auto-install disabled — deployment will fail when an extension is needed" -ForegroundColor Red
    } else {
        # 'yes_prompt' (default) — will hang under -NonInteractive
        $checks += @{ name = "Az CLI Extension Auto-Install"; status = "warn"; detail = "$dynInstall (will prompt)" }
        $warnings += "Azure CLI is set to '$dynInstall' for extension installs. Under -NonInteractive (orchestrator) this will hang the deployment. Run: az config set extension.use_dynamic_install=yes_without_prompt"
        Write-Host "  ⚠ Az CLI extension auto-install: $dynInstall — will hang under -NonInteractive" -ForegroundColor Yellow
        Write-Host "    Fix: az config set extension.use_dynamic_install=yes_without_prompt" -ForegroundColor DarkGray
    }
} catch {
    $checks += @{ name = "Az CLI Extension Auto-Install"; status = "warn"; detail = "unable to read az config" }
    $warnings += "Could not read 'extension.use_dynamic_install' from az config."
    Write-Host "  ⚠ Could not read az config for extension.use_dynamic_install" -ForegroundColor Yellow
}

# 10. Admin Security Group
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

# 11. Fabric capacity
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
