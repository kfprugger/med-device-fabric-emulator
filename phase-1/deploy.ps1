# deploy.ps1
param (
    [string]$ResourceGroupName = "rg-medtech",
    [string]$Location = "eastus",
    [string]$AdminSecurityGroup = "sg-azure-admins",
    [hashtable]$Tags = @{}
)

$ErrorActionPreference = "Stop"

# Resolve repo root (one level up from phase-1/) so relative paths work
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Push-Location $RepoRoot

# Fix Azure CLI Unicode encoding issue on Windows (az acr build log streaming)
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Prevent interactive `az` extension-install prompts from hanging the orchestrator
# (which runs pwsh with -NonInteractive — any prompt = infinite hang). See #fix-2.
$null = az config set extension.use_dynamic_install=yes_without_prompt --only-show-errors 2>$null

$hostArch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
$procArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture.ToString()
Write-Host "Host architecture: $hostArch (PowerShell process: $procArch)" -ForegroundColor Gray
if ($hostArch -eq "Arm64" -and $procArch -ne "Arm64") {
    Write-Host "  ⚠ ARM64 host detected with non-native PowerShell process architecture. Performance may be slower under emulation." -ForegroundColor Yellow
}

function Show-ArmDeploymentDiagnostics {
    param(
        [Parameter(Mandatory)][string]$ResourceGroup,
        [Parameter(Mandatory)][string]$DeploymentName,
        [int]$MaxRecentOperations = 8
    )

    $summaryRaw = az deployment group show `
        --resource-group $ResourceGroup `
        --name $DeploymentName `
        --query "{state:properties.provisioningState, timestamp:properties.timestamp, correlationId:properties.correlationId, error:properties.error}" `
        -o json 2>$null

    if ($LASTEXITCODE -eq 0 -and $summaryRaw) {
        $summary = $summaryRaw | ConvertFrom-Json
        Write-Host "    state: $($summary.state)" -ForegroundColor DarkGray
        if ($summary.timestamp) {
            Write-Host "    timestamp: $($summary.timestamp)" -ForegroundColor DarkGray
        }
        if ($summary.correlationId) {
            Write-Host "    correlationId: $($summary.correlationId)" -ForegroundColor DarkGray
        }
        if ($summary.error) {
            $errorText = ($summary.error | ConvertTo-Json -Compress -Depth 12)
            if ($errorText.Length -gt 300) {
                $errorText = $errorText.Substring(0, 300) + "..."
            }
            Write-Host "    error: $errorText" -ForegroundColor Yellow
        }
    }

    $opsRaw = az deployment operation group list `
        --resource-group $ResourceGroup `
        --name $DeploymentName `
        --query "[].{state:properties.provisioningState, name:properties.targetResource.resourceName, type:properties.targetResource.resourceType, status:properties.statusMessage}" `
        -o json 2>$null

    if ($LASTEXITCODE -eq 0 -and $opsRaw) {
        $ops = @($opsRaw | ConvertFrom-Json)
        if ($ops.Count -gt 0) {
            $failedOps = @($ops | Where-Object { $_.state -and $_.state -notin @("Succeeded", "Running") })
            Write-Host "    operations: $($ops.Count), non-success: $($failedOps.Count)" -ForegroundColor DarkGray

            $recentOps = @($ops | Select-Object -Last $MaxRecentOperations)
            foreach ($op in $recentOps) {
                $resourceName = if ($op.name) { $op.name } else { "(deployment scope)" }
                $resourceType = if ($op.type) { $op.type } else { "n/a" }
                $state = if ($op.state) { $op.state } else { "Unknown" }
                Write-Host "      [$state] $resourceName ($resourceType)" -ForegroundColor DarkGray
            }

            foreach ($op in ($failedOps | Select-Object -Last 3)) {
                $statusText = ""
                if ($op.status) {
                    $statusText = ($op.status | ConvertTo-Json -Compress -Depth 12)
                }
                if ($statusText.Length -gt 300) {
                    $statusText = $statusText.Substring(0, 300) + "..."
                }
                if ($statusText) {
                    Write-Host "      detail: $statusText" -ForegroundColor Yellow
                }
            }
        }
    }
}

function Invoke-ArmGroupDeployment {
    param(
        [Parameter(Mandatory)][string]$ResourceGroup,
        [Parameter(Mandatory)][string]$DeploymentName,
        [Parameter(Mandatory)][string]$TemplateFile,
        [string[]]$ParameterArgs = @(),
        [string]$Query,
        [switch]$OnlyShowErrors
    )

    $cmd = @(
        "deployment", "group", "create",
        "--resource-group", $ResourceGroup,
        "--name", $DeploymentName,
        "--template-file", $TemplateFile
    )

    if ($ParameterArgs -and $ParameterArgs.Count -gt 0) {
        $cmd += $ParameterArgs
    }
    if ($OnlyShowErrors) {
        $cmd += "--only-show-errors"
    }
    $cmd += "--no-wait"

    Write-Host "  ARM deployment started: $DeploymentName (template: $TemplateFile)" -ForegroundColor DarkGray
    Write-Host "    Waiting for provisioning result (heartbeat every ~15s)..." -ForegroundColor DarkGray

    $result = az @cmd 2>&1
    $createExitCode = $LASTEXITCODE

    if ($createExitCode -ne 0) {
        $global:LASTEXITCODE = $createExitCode
        return $result
    }

    $startTime = Get-Date
    $lastHeartbeat = [datetime]::MinValue
    $state = ""
    while ($true) {
        $stateRaw = az deployment group show `
            --resource-group $ResourceGroup `
            --name $DeploymentName `
            --query "properties.provisioningState" -o tsv 2>$null

        if ($LASTEXITCODE -eq 0 -and $stateRaw) {
            $state = "$stateRaw".Trim()
        } else {
            $state = "Running"
        }

        $now = Get-Date
        if ($lastHeartbeat -eq [datetime]::MinValue -or ($now - $lastHeartbeat).TotalSeconds -ge 15) {
            $elapsed = [math]::Round((New-TimeSpan -Start $startTime -End $now).TotalMinutes, 1)
            Write-Host "    [$elapsed min] ARM status: $state" -ForegroundColor DarkGray
            $lastHeartbeat = $now
        }

        if ($state -in @("Succeeded", "Failed", "Canceled")) {
            break
        }

        Start-Sleep -Seconds 10
    }

    if ($state -eq "Succeeded") {
        Write-Host "    ARM deployment completed successfully." -ForegroundColor Green
        $exitCode = 0
    } else {
        Write-Host "    ARM deployment completed with status: $state" -ForegroundColor Yellow
        $exitCode = 1
    }

    if ($Query) {
        $result = az deployment group show `
            --resource-group $ResourceGroup `
            --name $DeploymentName `
            --query $Query -o json 2>&1
        if ($LASTEXITCODE -ne 0 -and $exitCode -eq 0) {
            $exitCode = $LASTEXITCODE
        }
    }

    Write-Host "  ARM deployment diagnostics: $DeploymentName" -ForegroundColor DarkGray
    Show-ArmDeploymentDiagnostics -ResourceGroup $ResourceGroup -DeploymentName $DeploymentName

    $global:LASTEXITCODE = $exitCode
    return $result
}

# Serialize tags for Bicep parameter passing
$tagsParamFile = Join-Path $env:TEMP "deploy-tags-$(Get-Random).json"
$tagsParamContent = @{
    '`$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
    contentVersion = '1.0.0.0'
    parameters = @{ resourceTags = @{ value = if ($Tags.Count -gt 0) { $Tags } else { @{} } } }
}
$tagsParamContent | ConvertTo-Json -Depth 5 | Set-Content $tagsParamFile -Encoding utf8
$tagsParamRef = "@$tagsParamFile"

Write-Host "--- STEP 1: GENERATING PYTHON CODE ---" -ForegroundColor Cyan

# 1.1 Python Code - Using Managed Identity for Event Hub (no connection strings needed)
# Simulates 100 Masimo Radius-7 pulse oximeter devices with deterministic IDs
$pythonCode = @"
import os, sys, time, json, random, traceback
from datetime import datetime

# Force stdout/stderr to be unbuffered for ACI logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("=== MULTI-DEVICE EMULATOR STARTING ===", flush=True)

try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.identity import ManagedIdentityCredential
    print("Imports successful", flush=True)
except Exception as e:
    print(f"Import error: {e}", flush=True)
    traceback.print_exc()
    time.sleep(60)
    sys.exit(1)

# Configuration - Using Managed Identity (no connection strings/secrets needed!)
EVENT_HUB_NAMESPACE = os.getenv('EVENT_HUB_NAMESPACE')  # e.g., masimo-eh-ns
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')
DEVICE_COUNT = int(os.getenv('DEVICE_COUNT', '100'))

print(f"EVENT_HUB_NAMESPACE: {EVENT_HUB_NAMESPACE}", flush=True)
print(f"EVENT_HUB_NAME: {EVENT_HUB_NAME}", flush=True)
print(f"DEVICE_COUNT: {DEVICE_COUNT}", flush=True)

# Generate deterministic device IDs that match FHIR Device resources
DEVICE_IDS = [f"MASIMO-RADIUS7-{i:04d}" for i in range(1, DEVICE_COUNT + 1)]
print(f"Devices: {DEVICE_IDS[0]} to {DEVICE_IDS[-1]}", flush=True)

class MasimoSimulator:
    """Simulates a single Masimo Radius-7 pulse oximeter"""
    def __init__(self, device_id: str):
        self.device_id = device_id
        # Initialize with slightly different baselines per device
        seed = hash(device_id) % 1000
        random.seed(seed)
        self.spo2 = 95.0 + random.uniform(0, 4)
        self.pr = 65.0 + random.uniform(0, 20)
        self.pi = 2.5 + random.uniform(0, 2)
        self.pvi = 10.0 + random.uniform(0, 8)
        random.seed()  # Re-randomize
        
    def generate_reading(self):
        # Simulate realistic vital sign variations
        self.spo2 += random.uniform(-0.5, 0.5)
        self.pr += random.uniform(-2, 2)
        self.pi += random.uniform(-0.1, 0.1)
        self.pvi += random.uniform(-1, 1)
        
        # Clamp to realistic ranges
        self.spo2 = max(88, min(100, self.spo2))
        self.pr = max(50, min(140, self.pr))
        self.pi = max(0.5, min(10, self.pi))
        self.pvi = max(5, min(30, self.pvi))
        
        payload = {
            "device_id": self.device_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "telemetry": {
                "spo2": round(self.spo2, 1),
                "pr": int(self.pr),
                "pi": round(self.pi, 2),
                "pvi": int(self.pvi),
                "sphb": round(12.5 + random.uniform(-1, 1), 1),
                "signal_iq": random.randint(90, 100)
            }
        }
        return payload

def run():
    try:
        print("Connecting to Event Hub using Managed Identity...", flush=True)
        credential = ManagedIdentityCredential()
        
        # Use fully qualified namespace with credential
        fully_qualified_namespace = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net"
        print(f"FQNS: {fully_qualified_namespace}", flush=True)
        
        print("Creating producer...", flush=True)
        producer = EventHubProducerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=EVENT_HUB_NAME,
            credential=credential
        )
        
        # Create simulators for all devices
        simulators = {device_id: MasimoSimulator(device_id) for device_id in DEVICE_IDS}
        print(f"Created {len(simulators)} device simulators", flush=True)
        
        print("Entering producer context...", flush=True)
        with producer:
            print("Starting multi-device telemetry loop...", flush=True)
            cycle = 0
            while True:
                # Create a batch with readings from all devices
                batch = producer.create_batch()
                
                for device_id in DEVICE_IDS:
                    sim = simulators[device_id]
                    data = sim.generate_reading()
                    try:
                        batch.add(EventData(json.dumps(data)))
                    except ValueError:
                        # Batch is full, send it and create a new one
                        producer.send_batch(batch)
                        batch = producer.create_batch()
                        batch.add(EventData(json.dumps(data)))
                
                producer.send_batch(batch)
                cycle += 1
                
                # Log progress every 10 cycles
                if cycle % 10 == 0:
                    print(f"Cycle {cycle}: Sent telemetry for {len(DEVICE_IDS)} devices", flush=True)
                
                # Wait 1 second between cycles (all devices report every second)
                time.sleep(1)
                
    except Exception as e:
        print(f"!!! Fatal Error: {e}", flush=True)
        traceback.print_exc()
        sys.stdout.flush()
        sys.stderr.flush()
        print("Sleeping 120s for log capture...", flush=True)
        time.sleep(120)
        exit(1)

if __name__ == "__main__":
    run()
"@
Set-Content -Path "emulator.py" -Value $pythonCode

# 1.2 Dockerfile
$dockerfile = @"
FROM mcr.microsoft.com/cbl-mariner/base/python:3
ENV PYTHONUNBUFFERED=1
RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN pip install azure-eventhub azure-identity azure-keyvault-secrets
COPY emulator.py /app/emulator.py
WORKDIR /app
CMD ["python", "-u", "emulator.py"]
"@
Set-Content -Path "Dockerfile" -Value $dockerfile

Write-Host "--- STEP 2: DEPLOYING INFRASTRUCTURE ---" -ForegroundColor Cyan
az group create --name $ResourceGroupName --location $Location | Out-Null

# Check for and purge any soft-deleted Key Vaults with matching name pattern
$deletedVaults = az keyvault list-deleted --query "[?starts_with(name, 'masimo')].name" -o tsv 2>$null
foreach ($vault in $deletedVaults) {
    if ($vault) {
        Write-Host "Purging soft-deleted Key Vault: $vault" -ForegroundColor Yellow
        az keyvault purge --name $vault --no-wait 2>$null
        Start-Sleep -Seconds 5
    }
}

# Get admin group object ID if specified
$adminGroupObjectId = ""
if ($AdminSecurityGroup) {
    $adminGroupObjectId = az ad group show --group $AdminSecurityGroup --query id -o tsv 2>$null
    if ($adminGroupObjectId) {
        Write-Host "Admin security group found: $AdminSecurityGroup ($adminGroupObjectId)"
    } else {
        Write-Host "WARNING: Admin security group '$AdminSecurityGroup' not found" -ForegroundColor Yellow
    }
}

$infra = Invoke-ArmGroupDeployment `
    -ResourceGroup $ResourceGroupName `
    -DeploymentName "infra" `
    -TemplateFile "bicep/infra.bicep" `
    -ParameterArgs @("--parameters", "adminGroupObjectId=$adminGroupObjectId", "--parameters", $tagsParamRef) `
    -Query "properties.outputs" `
    -OnlyShowErrors

if ($LASTEXITCODE -ne 0) {
    $infraStr = $infra -join "`n"
    if ($infraStr -match 'DeploymentActive') {
        Write-Host "  A previous deployment is still active. Waiting 60s and retrying..." -ForegroundColor Yellow
        Start-Sleep -Seconds 60
        $infra = Invoke-ArmGroupDeployment `
            -ResourceGroup $ResourceGroupName `
            -DeploymentName "infra" `
            -TemplateFile "bicep/infra.bicep" `
            -ParameterArgs @("--parameters", "adminGroupObjectId=$adminGroupObjectId", "--parameters", $tagsParamRef) `
            -Query "properties.outputs" `
            -OnlyShowErrors
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERROR: Infrastructure deployment failed after retry." -ForegroundColor Red
            Write-Host "  $infra" -ForegroundColor Red
            Write-Host "" -ForegroundColor Red
            Write-Host "  To retry, wait for the active deployment to finish, then run:" -ForegroundColor Yellow
            Write-Host "    .\Deploy-All.ps1 -ResourceGroupName '$ResourceGroupName' -Location '$Location' ..." -ForegroundColor Cyan
            exit 1
        }
    } else {
        Write-Host "ERROR: Infrastructure deployment failed." -ForegroundColor Red
        Write-Host "  $infra" -ForegroundColor Red
        Write-Host "" -ForegroundColor Red
        Write-Host "  To retry:" -ForegroundColor Yellow
        Write-Host "    .\Deploy-All.ps1 -ResourceGroupName '$ResourceGroupName' -Location '$Location' ..." -ForegroundColor Cyan
        exit 1
    }
}

$infraJson = $infra | ConvertFrom-Json
$acrName = $infraJson.acrName.value
$acrLoginServer = $infraJson.acrLoginServer.value
$ehName = $infraJson.eventHubName.value
$ehNamespace = $infraJson.eventHubNamespace.value
$kvName = $infraJson.keyVaultName.value

if (-not $acrName) {
    Write-Host "ERROR: Infrastructure deployment failed - ACR name is empty" -ForegroundColor Red
    exit 1
}

Write-Host "Infrastructure ready. Event Hub Namespace: $ehNamespace" -ForegroundColor Green
if ($adminGroupObjectId) {
    Write-Host "RBAC roles assigned to $AdminSecurityGroup via Bicep deployment" -ForegroundColor Green
}

Write-Host "--- STEP 3: BUILDING IMAGE IN AZURE ---" -ForegroundColor Cyan
Write-Host "  ⏳ This is a long running operation (2-5 min). Building container image in ACR..." -ForegroundColor Yellow
# Force UTF-8 to avoid charmap encoding errors on Windows (az CLI uses colorama → cp1252 crash)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONLEGACYWINDOWSSTDIO = "0"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Build from a clean staging directory containing ONLY the files the Dockerfile needs.
# `az acr build` ignores `.dockerignore` reliably on Windows, which historically caused
# 40+ minute hangs uploading the entire 1+ GB repo. Staging keeps the upload to ~5 KB.
$emulatorStaging = Join-Path $env:TEMP ("masimo-emulator-build-" + [Guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $emulatorStaging -Force | Out-Null
Copy-Item -Path (Join-Path $RepoRoot "Dockerfile")   -Destination $emulatorStaging -Force
Copy-Item -Path (Join-Path $RepoRoot "emulator.py")  -Destination $emulatorStaging -Force
Write-Host "  Build context: $emulatorStaging (Dockerfile + emulator.py only)" -ForegroundColor DarkGray

$acrBuildErrLog = Join-Path $env:TEMP ("acr-build-" + [Guid]::NewGuid().ToString() + ".log")
Push-Location $emulatorStaging
try {
    $acrBuildOutput = az acr build --registry $acrName --image "masimo-emulator:v1" . --no-logs 2>$acrBuildErrLog
    $acrBuildExitCode = $LASTEXITCODE
} finally {
    Pop-Location
    Remove-Item -Recurse -Force $emulatorStaging -ErrorAction SilentlyContinue
}
$acrBuildOutput | ForEach-Object {
    $line = $_ -replace '[^\x20-\x7E]', ''
    if ($line.Trim()) { Write-Host "  $line" }
}
if ($acrBuildExitCode -ne 0) {
    Write-Host "  ⚠ ACR build may have failed (exit code $acrBuildExitCode) — checking if image exists..." -ForegroundColor Yellow

    if (Test-Path $acrBuildErrLog) {
        $stderrTail = Get-Content $acrBuildErrLog -ErrorAction SilentlyContinue | Select-Object -Last 20
        if ($stderrTail) {
            Write-Host "  ACR stderr (tail):" -ForegroundColor Yellow
            $stderrTail | ForEach-Object {
                $errLine = $_ -replace '[^\x20-\x7E]', ''
                if ($errLine.Trim()) { Write-Host "    $errLine" -ForegroundColor Yellow }
            }
        }
    }

    $imageExists = $null
    for ($attempt = 1; $attempt -le 6; $attempt++) {
        $imageExists = az acr repository show-tags --name $acrName --repository "masimo-emulator" --query "[?contains(@, 'v1')]" -o tsv 2>$null
        if ($imageExists) { break }
        Write-Host "  Waiting for ACR tag visibility (attempt $attempt/6)..." -ForegroundColor Yellow
        Start-Sleep -Seconds 10
    }

    if ($imageExists) {
        Write-Host "  ✓ Image masimo-emulator:v1 exists in ACR (build succeeded despite log error)" -ForegroundColor Green
    } else {
        throw "ACR build failed and image not found. Check Azure portal for build logs."
    }
}

if (Test-Path $acrBuildErrLog) {
    Remove-Item $acrBuildErrLog -ErrorAction SilentlyContinue
}

Write-Host "--- STEP 4: DEPLOYING SYSTEM-IDENTITY EMULATOR ---" -ForegroundColor Cyan
$fullImageTag = "$acrLoginServer/masimo-emulator:v1"

$null = Invoke-ArmGroupDeployment `
    -ResourceGroup $ResourceGroupName `
    -DeploymentName "emulator" `
    -TemplateFile "bicep/emulator.bicep" `
    -ParameterArgs @(
        "--parameters", "acrName=$acrName",
        "imageName=$fullImageTag",
        "eventHubName=$ehName",
        "eventHubNamespace=$ehNamespace",
        "--parameters", $tagsParamRef
    )

if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Emulator deployment failed." -ForegroundColor Red
        exit 1
}

Write-Host "--- SUCCESS ---" -ForegroundColor Green
Write-Host "Emulator running with System-Assigned Identity (using Entra ID for Event Hub)."

Pop-Location


