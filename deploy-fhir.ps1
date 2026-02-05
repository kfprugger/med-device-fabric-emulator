# deploy-fhir.ps1
# Deploys Azure FHIR Service, generates synthetic patient data with Synthea, and uploads to FHIR

param (
    [string]$ResourceGroupName = "rg-medtech-sys-identity",
    [string]$Location = "eastus",
    [string]$AdminSecurityGroup = "sg-azure-admins",
    [int]$PatientCount = 10000
)

$ErrorActionPreference = "Stop"

# Change to script directory so relative paths work
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $ScriptDir
Write-Host "Working directory: $(Get-Location)"

try {

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  FHIR Service Deployment with Synthea" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Resource Group: $ResourceGroupName"
Write-Host "Location: $Location"
Write-Host "Patient Count: $PatientCount"
Write-Host ""

# ============================================
# STEP 1: Deploy FHIR Infrastructure
# ============================================
Write-Host "--- STEP 1: DEPLOYING FHIR INFRASTRUCTURE ---" -ForegroundColor Cyan

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

$fhirInfra = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file fhir-infra.bicep `
    --parameters adminGroupObjectId="$adminGroupObjectId" `
    --query properties.outputs 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR deploying FHIR infrastructure: $fhirInfra" -ForegroundColor Red
    exit 1
}

$fhirJson = $fhirInfra | ConvertFrom-Json
$fhirServiceUrl = $fhirJson.fhirServiceUrl.value
$storageAccountName = $fhirJson.storageAccountName.value
$containerName = $fhirJson.containerName.value
$workspaceName = $fhirJson.workspaceName.value
$fhirServiceName = $fhirJson.fhirServiceName.value

Write-Host "FHIR Service URL: $fhirServiceUrl" -ForegroundColor Green
Write-Host "Storage Account: $storageAccountName"
Write-Host "Blob Container: $containerName"

# Get ACR name from existing infrastructure
$existingInfra = az deployment group show `
    --resource-group $ResourceGroupName `
    --name infra `
    --query properties.outputs 2>$null

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Existing infrastructure not found. Please run deploy.ps1 first." -ForegroundColor Red
    exit 1
}

$existingJson = $existingInfra | ConvertFrom-Json
$acrName = $existingJson.acrName.value
$acrLoginServer = $existingJson.acrLoginServer.value

Write-Host "Using existing ACR: $acrName"

# ============================================
# STEP 2: Build Synthea Container
# ============================================
Write-Host ""
Write-Host "--- STEP 2: BUILDING SYNTHEA CONTAINER ---" -ForegroundColor Cyan

Push-Location synthea
try {
    az acr build --registry $acrName --image "synthea-generator:v1" .
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR building Synthea container" -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
}

Write-Host "Synthea container built successfully" -ForegroundColor Green

# ============================================
# STEP 3: Run Synthea Job
# ============================================
Write-Host ""
Write-Host "--- STEP 3: RUNNING SYNTHEA GENERATOR ---" -ForegroundColor Cyan
Write-Host "Generating $PatientCount synthetic patients for Atlanta, GA..."
Write-Host "This may take 15-30 minutes..." -ForegroundColor Yellow

# Delete existing Synthea job if it exists
az container delete --resource-group $ResourceGroupName --name synthea-generator-job --yes 2>$null

$syntheaImage = "$acrLoginServer/synthea-generator:v1"

az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file synthea-job.bicep `
    --parameters acrName=$acrName `
                 imageName=$syntheaImage `
                 storageAccountName=$storageAccountName `
                 containerName=$containerName `
                 patientCount=$PatientCount

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR deploying Synthea job" -ForegroundColor Red
    exit 1
}

# Wait for Synthea job to complete
Write-Host "Waiting for Synthea generation to complete..."
$maxWaitMinutes = 60
$waitedMinutes = 0

while ($waitedMinutes -lt $maxWaitMinutes) {
    $state = az container show `
        --resource-group $ResourceGroupName `
        --name synthea-generator-job `
        --query "instanceView.state" -o tsv 2>$null
    
    if ($state -eq "Succeeded") {
        Write-Host "Synthea generation completed successfully!" -ForegroundColor Green
        break
    } elseif ($state -eq "Failed") {
        Write-Host "ERROR: Synthea generation failed" -ForegroundColor Red
        az container logs --resource-group $ResourceGroupName --name synthea-generator-job
        exit 1
    } elseif ($state -eq "Terminated") {
        # Check exit code
        $exitCode = az container show `
            --resource-group $ResourceGroupName `
            --name synthea-generator-job `
            --query "containers[0].instanceView.currentState.exitCode" -o tsv 2>$null
        
        if ($exitCode -eq "0") {
            Write-Host "Synthea generation completed successfully!" -ForegroundColor Green
            break
        } else {
            Write-Host "ERROR: Synthea generation failed with exit code $exitCode" -ForegroundColor Red
            az container logs --resource-group $ResourceGroupName --name synthea-generator-job
            exit 1
        }
    }
    
    Write-Host "  Status: $state (waited $waitedMinutes minutes)"
    Start-Sleep -Seconds 60
    $waitedMinutes++
}

if ($waitedMinutes -ge $maxWaitMinutes) {
    Write-Host "ERROR: Synthea generation timed out" -ForegroundColor Red
    exit 1
}

# Show Synthea logs
Write-Host ""
Write-Host "Synthea generation logs:" -ForegroundColor Gray
az container logs --resource-group $ResourceGroupName --name synthea-generator-job 2>$null | Select-Object -Last 20

# ============================================
# STEP 4: Build FHIR Loader Container
# ============================================
Write-Host ""
Write-Host "--- STEP 4: BUILDING FHIR LOADER CONTAINER ---" -ForegroundColor Cyan

Push-Location fhir-loader
try {
    az acr build --registry $acrName --image "fhir-loader:v1" .
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR building FHIR Loader container" -ForegroundColor Red
        exit 1
    }
} finally {
    Pop-Location
}

Write-Host "FHIR Loader container built successfully" -ForegroundColor Green

# ============================================
# STEP 5: Run FHIR Loader Job
# ============================================
Write-Host ""
Write-Host "--- STEP 5: RUNNING FHIR LOADER ---" -ForegroundColor Cyan
Write-Host "Uploading synthetic data to FHIR service..."
Write-Host "This may take 30-60 minutes..." -ForegroundColor Yellow

# Delete existing FHIR loader job if it exists
az container delete --resource-group $ResourceGroupName --name fhir-loader-job --yes 2>$null

$loaderImage = "$acrLoginServer/fhir-loader:v1"

az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file fhir-loader-job.bicep `
    --parameters acrName=$acrName `
                 imageName=$loaderImage `
                 storageAccountName=$storageAccountName `
                 containerName=$containerName `
                 fhirServiceUrl=$fhirServiceUrl

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR deploying FHIR Loader job" -ForegroundColor Red
    exit 1
}

# Get the loader's managed identity principal ID and assign FHIR Data Contributor role
Write-Host "Assigning FHIR Data Contributor role to loader identity..."
$loaderPrincipalId = az container show `
    --resource-group $ResourceGroupName `
    --name fhir-loader-job `
    --query "identity.principalId" -o tsv

$fhirResourceId = az healthcareapis workspace fhir-service show `
    --resource-group $ResourceGroupName `
    --workspace-name $workspaceName `
    --fhir-service-name $fhirServiceName `
    --query id -o tsv

# Assign FHIR Data Contributor role
az role assignment create `
    --role "5a1fc7df-4bf1-4951-a576-89034ee01acd" `
    --assignee-object-id $loaderPrincipalId `
    --assignee-principal-type ServicePrincipal `
    --scope $fhirResourceId 2>$null

Write-Host "RBAC role assigned, waiting for propagation..."
Start-Sleep -Seconds 30

# Restart the container to pick up the new role
az container restart --resource-group $ResourceGroupName --name fhir-loader-job 2>$null

# Wait for FHIR Loader to complete
Write-Host "Waiting for FHIR data upload to complete..."
$maxWaitMinutes = 90
$waitedMinutes = 0

while ($waitedMinutes -lt $maxWaitMinutes) {
    $state = az container show `
        --resource-group $ResourceGroupName `
        --name fhir-loader-job `
        --query "instanceView.state" -o tsv 2>$null
    
    if ($state -eq "Succeeded") {
        Write-Host "FHIR data upload completed successfully!" -ForegroundColor Green
        break
    } elseif ($state -eq "Failed") {
        Write-Host "ERROR: FHIR data upload failed" -ForegroundColor Red
        az container logs --resource-group $ResourceGroupName --name fhir-loader-job
        exit 1
    } elseif ($state -eq "Terminated") {
        $exitCode = az container show `
            --resource-group $ResourceGroupName `
            --name fhir-loader-job `
            --query "containers[0].instanceView.currentState.exitCode" -o tsv 2>$null
        
        if ($exitCode -eq "0") {
            Write-Host "FHIR data upload completed successfully!" -ForegroundColor Green
            break
        } else {
            Write-Host "ERROR: FHIR data upload failed with exit code $exitCode" -ForegroundColor Red
            az container logs --resource-group $ResourceGroupName --name fhir-loader-job
            exit 1
        }
    }
    
    Write-Host "  Status: $state (waited $waitedMinutes minutes)"
    Start-Sleep -Seconds 60
    $waitedMinutes++
}

if ($waitedMinutes -ge $maxWaitMinutes) {
    Write-Host "ERROR: FHIR data upload timed out" -ForegroundColor Red
    exit 1
}

# Show FHIR Loader logs
Write-Host ""
Write-Host "FHIR Loader logs:" -ForegroundColor Gray
az container logs --resource-group $ResourceGroupName --name fhir-loader-job 2>$null | Select-Object -Last 30

# ============================================
# STEP 6: Verification & Summary
# ============================================
Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "FHIR Service URL: $fhirServiceUrl" -ForegroundColor Cyan
Write-Host ""
Write-Host "Resources deployed:"
Write-Host "  - Health Data Services Workspace: $workspaceName"
Write-Host "  - FHIR Service: $fhirServiceName"
Write-Host "  - Storage Account: $storageAccountName"
Write-Host "  - Synthetic Patients: ~$PatientCount"
Write-Host "  - Masimo Devices: 100"
Write-Host "  - Device Associations: Up to 100 (patients with qualifying conditions)"
Write-Host ""
Write-Host "Atlanta Providers included:"
Write-Host "  - Emory Healthcare"
Write-Host "  - Piedmont Healthcare"
Write-Host "  - Grady Health System"
Write-Host "  - Northside Hospital"
Write-Host "  - WellStar Health System"
Write-Host "  - Children's Healthcare of Atlanta (pediatric only)"
Write-Host ""
Write-Host "Device linkage:"
Write-Host "  - Device IDs: MASIMO-RADIUS7-0001 through MASIMO-RADIUS7-0100"
Write-Host "  - Linked to patients with: COPD, Asthma, Heart Failure, Sleep Apnea, etc."
Write-Host ""
Write-Host "To query FHIR data, use:" -ForegroundColor Yellow
Write-Host "  az rest --method GET --url '$fhirServiceUrl/Patient?_count=10' --resource https://fhir.azurehealthcareapis.com"

} finally {
    Pop-Location
}
