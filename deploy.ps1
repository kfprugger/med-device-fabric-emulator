# deploy.ps1
param (
    [string]$ResourceGroupName = "rg-medtech",
    [string]$Location = "eastus",
    [string]$AdminSecurityGroup = "sg-azure-admins"
)

$ErrorActionPreference = "Stop"

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
FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
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

$infra = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file bicep/infra.bicep `
    --parameters adminGroupObjectId="$adminGroupObjectId" `
    --query properties.outputs 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: $infra" -ForegroundColor Red
    exit 1
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
az acr build --registry $acrName --image "masimo-emulator:v1" .

Write-Host "--- STEP 4: DEPLOYING SYSTEM-IDENTITY EMULATOR ---" -ForegroundColor Cyan
$fullImageTag = "$acrLoginServer/masimo-emulator:v1"

az deployment group create `
  --resource-group $ResourceGroupName `
  --template-file bicep/emulator.bicep `
  --parameters acrName=$acrName `
               imageName=$fullImageTag `
               eventHubName=$ehName `
               eventHubNamespace=$ehNamespace

Write-Host "--- SUCCESS ---" -ForegroundColor Green
Write-Host "Emulator running with System-Assigned Identity (using Entra ID for Event Hub)."