# deploy-fhir.ps1
# Deploys Azure FHIR Service, generates synthetic patient data with Synthea, and uploads to FHIR
#
# Usage:
#   .\deploy-fhir.ps1                          # Full run (infra + synthea + loader)
#   .\deploy-fhir.ps1 -InfraOnly               # Deploy infrastructure only
#   .\deploy-fhir.ps1 -RunSynthea              # Generate patients only (infra must exist)
#   .\deploy-fhir.ps1 -RunLoader               # Load FHIR data only (infra + blobs must exist)
#   .\deploy-fhir.ps1 -RunSynthea -RunLoader   # Generate + load (infra must exist)
#   .\deploy-fhir.ps1 -RunSynthea -RebuildContainers  # Force rebuild of container images
#   .\deploy-fhir.ps1 -RunDicom                # DICOM infra + TCIA download + upload only
#   .\deploy-fhir.ps1 -SkipDicom               # Full run but skip DICOM steps

param (
    [string]$ResourceGroupName = "rg-medtech-rti-fhir",
    [string]$Location = "eastus",
    [string]$AdminSecurityGroup = "sg-azure-admins",
    [int]$PatientCount = 10,
    [switch]$InfraOnly,
    [switch]$RunSynthea,
    [switch]$RunLoader,
    [switch]$RebuildContainers,
    [switch]$SkipDicom,
    [switch]$RunDicom,
    [hashtable]$Tags = @{}
)

# Determine which steps to run
$selectiveMode = $InfraOnly -or $RunSynthea -or $RunLoader -or $RunDicom
$doInfra = -not $selectiveMode -or $InfraOnly
$doSynthea = -not $selectiveMode -or $RunSynthea
$doLoader = -not $selectiveMode -or $RunLoader
$doDicom = (-not $selectiveMode -and -not $SkipDicom) -or $RunDicom

$ErrorActionPreference = "Stop"

# Fix Azure CLI Unicode encoding issue on Windows (az acr build log streaming)
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Serialize tags for Bicep parameter passing
# az CLI cannot reliably receive JSON objects inline on Windows
# Write a temp params file and reference it
$tagsParamFile = Join-Path $env:TEMP "deploy-tags-$(Get-Random).json"
$tagsParamContent = @{
    '`$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
    contentVersion = '1.0.0.0'
    parameters = @{ resourceTags = @{ value = if ($Tags.Count -gt 0) { $Tags } else { @{} } } }
}
$tagsParamContent | ConvertTo-Json -Depth 5 | Set-Content $tagsParamFile -Encoding utf8
$tagsParamRef = "@$tagsParamFile"

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
if ($selectiveMode) {
    $modes = @()
    if ($InfraOnly) { $modes += "InfraOnly" }
    if ($RunSynthea) { $modes += "RunSynthea" }
    if ($RunLoader) { $modes += "RunLoader" }
    if ($RunDicom) { $modes += "RunDicom" }
    if ($RebuildContainers) { $modes += "RebuildContainers" }
    Write-Host "Mode: $($modes -join ' + ')" -ForegroundColor Yellow
} else {
    Write-Host "Mode: Full deployment (all steps)" -ForegroundColor Yellow
}
Write-Host ""

# ============================================
# STEP 1: FHIR Infrastructure
# ============================================

# Always check for existing infrastructure first
Write-Host "--- STEP 1: CHECKING FHIR INFRASTRUCTURE ---" -ForegroundColor Cyan

$infraExists = $false

# Check if the FHIR deployment already exists
$fhirDeployment = az deployment group show `
    --resource-group $ResourceGroupName `
    --name fhir-infra `
    --query properties.outputs 2>$null

if ($LASTEXITCODE -eq 0 -and $fhirDeployment) {
    $fhirJson = $fhirDeployment | ConvertFrom-Json
    $fhirServiceUrl = $fhirJson.fhirServiceUrl.value
    $storageAccountName = $fhirJson.storageAccountName.value
    $containerName = $fhirJson.containerName.value
    $workspaceName = $fhirJson.workspaceName.value
    $fhirServiceName = $fhirJson.fhirServiceName.value
    $aciIdentityId = $fhirJson.aciIdentityId.value
    $aciIdentityClientId = $fhirJson.aciIdentityClientId.value

    # Verify the FHIR service is actually reachable
    $fhirCheck = az healthcareapis workspace fhir-service show `
        --resource-group $ResourceGroupName `
        --workspace-name $workspaceName `
        --fhir-service-name $fhirServiceName `
        --query provisioningState -o tsv 2>$null

    if ($fhirCheck -eq "Succeeded") {
        $infraExists = $true
        Write-Host "FHIR infrastructure already exists - skipping deployment" -ForegroundColor Green
        Write-Host "  FHIR Service URL: $fhirServiceUrl"
        Write-Host "  Storage Account: $storageAccountName"
        Write-Host "  Blob Container: $containerName"
        Write-Host "  ACI Identity: $aciIdentityId"

        # Ensure FHIR RBAC roles for admin security group (even on re-runs)
        if ($AdminSecurityGroup) {
            $adminGroupObjectId = az ad group show --group $AdminSecurityGroup --query id -o tsv 2>$null
            if ($adminGroupObjectId) {
                $fhirServiceId = az resource list -g $ResourceGroupName `
                    --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
                    --query "[0].id" -o tsv 2>$null
                if ($fhirServiceId) {
                    Write-Host "  Ensuring FHIR RBAC for $AdminSecurityGroup..." -ForegroundColor Cyan
                    # FHIR Data Contributor (5a1fc7df-4bf1-4951-a576-89034ee01acd)
                    az role assignment create --assignee $adminGroupObjectId --role "FHIR Data Contributor" `
                        --scope $fhirServiceId --assignee-object-id $adminGroupObjectId `
                        --assignee-principal-type Group --output none 2>$null
                    # FHIR Data Reader (4c8d0bbc-75d3-4935-991f-5f3c56d81508)
                    az role assignment create --assignee $adminGroupObjectId --role "FHIR Data Reader" `
                        --scope $fhirServiceId --assignee-object-id $adminGroupObjectId `
                        --assignee-principal-type Group --output none 2>$null
                    # Storage Blob Data Contributor on the storage account
                    $storageId = az storage account show -n $storageAccountName -g $ResourceGroupName --query id -o tsv 2>$null
                    if ($storageId) {
                        az role assignment create --assignee $adminGroupObjectId --role "Storage Blob Data Contributor" `
                            --scope $storageId --assignee-object-id $adminGroupObjectId `
                            --assignee-principal-type Group --output none 2>$null
                    }
                    Write-Host "  FHIR RBAC roles verified for $AdminSecurityGroup" -ForegroundColor Green
                }
            } else {
                Write-Host "  WARNING: Admin security group '$AdminSecurityGroup' not found - skipping RBAC" -ForegroundColor Yellow
            }
        }
    }
}

if (-not $infraExists) {
    if (-not $doInfra -and -not $doSynthea -and -not $doLoader -and -not $doDicom) {
        Write-Host "ERROR: FHIR infrastructure not found. Run without mode flags or with -InfraOnly first." -ForegroundColor Red
        exit 1
    }

    Write-Host "Deploying FHIR infrastructure. This is a long running operation. Be patient..." -ForegroundColor Cyan

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
        --template-file bicep/fhir-infra.bicep `
        --parameters adminGroupObjectId="$adminGroupObjectId" `
        --parameters $tagsParamRef `
        --query properties.outputs `
        --only-show-errors 2>&1

    if ($LASTEXITCODE -ne 0) {
        $fhirInfraStr = $fhirInfra -join "`n"

        # RoleAssignmentExists is non-fatal — the resources were created, just the RBAC already existed
        if ($fhirInfraStr -match 'RoleAssignmentExists') {
            Write-Host "  ⚠ Bicep reported RoleAssignmentExists (non-fatal — role already assigned)" -ForegroundColor Yellow
            Write-Host "  Fetching deployment outputs..." -ForegroundColor Gray
            $fhirInfra = az deployment group show `
                --resource-group $ResourceGroupName `
                --name fhir-infra `
                --query properties.outputs 2>$null
            if ($LASTEXITCODE -ne 0 -or -not $fhirInfra) {
                Write-Host "ERROR: Could not retrieve FHIR deployment outputs after RoleAssignmentExists." -ForegroundColor Red
                Write-Host "  The FHIR infrastructure may not have deployed correctly." -ForegroundColor Red
                Write-Host "" -ForegroundColor Red
                Write-Host "  To retry:" -ForegroundColor Yellow
                Write-Host "    .\deploy-fhir.ps1 -ResourceGroupName '$ResourceGroupName' -Location '$Location'" -ForegroundColor Cyan
                exit 1
            }
        } elseif ($fhirInfraStr -match 'DeploymentActive') {
            Write-Host "  A previous FHIR deployment is still active. Waiting 60s and retrying..." -ForegroundColor Yellow
            Start-Sleep -Seconds 60
            $fhirInfra = az deployment group create `
                --resource-group $ResourceGroupName `
                --template-file bicep/fhir-infra.bicep `
                --parameters adminGroupObjectId="$adminGroupObjectId" `
                --parameters $tagsParamRef `
                --query properties.outputs `
                --only-show-errors 2>&1
            if ($LASTEXITCODE -ne 0) {
                Write-Host "ERROR: FHIR infrastructure deployment failed after retry." -ForegroundColor Red
                Write-Host "  $fhirInfra" -ForegroundColor Red
                Write-Host "" -ForegroundColor Red
                Write-Host "  To retry, wait for the active deployment to finish, then run:" -ForegroundColor Yellow
                Write-Host "    .\deploy-fhir.ps1 -ResourceGroupName '$ResourceGroupName' -Location '$Location'" -ForegroundColor Cyan
                exit 1
            }
        } else {
            Write-Host "ERROR: FHIR infrastructure deployment failed." -ForegroundColor Red
            Write-Host "  $fhirInfra" -ForegroundColor Red
            Write-Host "" -ForegroundColor Red
            Write-Host "  To retry:" -ForegroundColor Yellow
            Write-Host "    .\deploy-fhir.ps1 -ResourceGroupName '$ResourceGroupName' -Location '$Location'" -ForegroundColor Cyan
            exit 1
        }
    }

    $fhirJson = $fhirInfra | ConvertFrom-Json
    $fhirServiceUrl = $fhirJson.fhirServiceUrl.value
    $storageAccountName = $fhirJson.storageAccountName.value
    $containerName = $fhirJson.containerName.value
    $workspaceName = $fhirJson.workspaceName.value
    $fhirServiceName = $fhirJson.fhirServiceName.value
    $aciIdentityId = $fhirJson.aciIdentityId.value
    $aciIdentityClientId = $fhirJson.aciIdentityClientId.value

    Write-Host "FHIR infrastructure deployed successfully" -ForegroundColor Green
    Write-Host "  FHIR Service URL: $fhirServiceUrl"
    Write-Host "  Storage Account: $storageAccountName"
    Write-Host "  Blob Container: $containerName"
}

if ($InfraOnly) {
    Write-Host ""
    Write-Host "Infrastructure-only mode complete." -ForegroundColor Green
    Write-Host "Run with -RunSynthea to generate patients, or -RunLoader to load FHIR data."
    exit 0
}

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
if ($doSynthea) {
Write-Host ""
Write-Host "--- STEP 2: SYNTHEA CONTAINER IMAGE ---" -ForegroundColor Cyan

$syntheaImageExists = az acr repository show-tags --name $acrName --repository synthea-generator --query "contains(@, 'v1')" -o tsv 2>$null

if ($syntheaImageExists -eq "true" -and -not $RebuildContainers) {
    Write-Host "Synthea image already exists in ACR - skipping build" -ForegroundColor Green
    Write-Host "  Use -RebuildContainers to force a rebuild" -ForegroundColor DarkGray
} else {
    if ($RebuildContainers) {
        Write-Host "Rebuilding Synthea container (forced)..." -ForegroundColor Cyan
    } else {
        Write-Host "Building Synthea container (first time)..." -ForegroundColor Cyan
    }
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
}

# ============================================
# STEP 3: Run Synthea Job
# ============================================
Write-Host ""
Write-Host "--- STEP 3: RUNNING SYNTHEA GENERATOR ---" -ForegroundColor Cyan
Write-Host "Generating $PatientCount synthetic patients for Atlanta, GA..."
Write-Host "This may take 15-30 minutes..." -ForegroundColor Yellow

# Clear existing blobs so only the new batch is loaded
Write-Host "Clearing previous Synthea output from blob storage if it exists..." -ForegroundColor DarkGray
az storage blob delete-batch --account-name $storageAccountName --source $containerName --auth-mode login --pattern "*.json" 2>$null | Out-Null
Write-Host "  Previous files cleared" -ForegroundColor DarkGray

# Delete existing Synthea job (new identity will get a unique role assignment via Bicep GUID)
Write-Host "Removing previous Synthea container job if it exists..." -ForegroundColor DarkGray
az container delete --resource-group $ResourceGroupName --name synthea-generator-job --yes 2>$null | Out-Null

$syntheaImage = "$acrLoginServer/synthea-generator:v1"

az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file bicep/synthea-job.bicep `
    --parameters acrName=$acrName `
                 imageName=$syntheaImage `
                 storageAccountName=$storageAccountName `
                 containerName=$containerName `
                 patientCount=$PatientCount `
                 aciIdentityId=$aciIdentityId `
                 aciIdentityClientId=$aciIdentityClientId `
    --parameters $tagsParamRef

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR deploying Synthea job" -ForegroundColor Red
    exit 1
}

# Wait for Synthea job to complete with live log streaming
Write-Host "Waiting for Synthea generation to complete..."
Write-Host ""
$maxWaitMinutes = 60
$waitedMinutes = 0
$lastLogLines = 0

while ($waitedMinutes -lt $maxWaitMinutes) {
    $state = az container show `
        --resource-group $ResourceGroupName `
        --name synthea-generator-job `
        --query "instanceView.state" -o tsv 2>$null
    
    if ($state -eq "Succeeded") {
        Write-Host ""
        Write-Host "Synthea generation completed successfully!" -ForegroundColor Green
        break
    } elseif ($state -eq "Failed") {
        Write-Host ""
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
            Write-Host ""
            Write-Host "Synthea generation completed successfully!" -ForegroundColor Green
            break
        } else {
            Write-Host ""
            Write-Host "ERROR: Synthea generation failed with exit code $exitCode" -ForegroundColor Red
            az container logs --resource-group $ResourceGroupName --name synthea-generator-job
            exit 1
        }
    }

    # Stream progress from container logs
    if ($state -eq "Running") {
        $logs = az container logs --resource-group $ResourceGroupName --name synthea-generator-job 2>$null
        if ($logs) {
            $logLines = @($logs -split "`n")
            if ($logLines.Count -gt $lastLogLines) {
                $newLines = $logLines[$lastLogLines..($logLines.Count - 1)]
                foreach ($line in $newLines) {
                    if ($line -match "Running|Patient|Generated|Upload|Complete|files|FHIR|blob") {
                        Write-Host "  [Synthea] $line" -ForegroundColor DarkCyan
                    }
                }
                $lastLogLines = $logLines.Count
            }
        }
    }
    
    Write-Host "  Status: $state (waited $waitedMinutes min)" -ForegroundColor DarkGray
    Start-Sleep -Seconds 30
    $waitedMinutes += 0.5
}

if ($waitedMinutes -ge $maxWaitMinutes) {
    Write-Host "ERROR: Synthea generation timed out" -ForegroundColor Red
    exit 1
}

# Show final Synthea logs
Write-Host ""
Write-Host "Synthea generation logs (last 20 lines):" -ForegroundColor Gray
az container logs --resource-group $ResourceGroupName --name synthea-generator-job 2>$null | Select-Object -Last 20

} else {
    Write-Host ""
    Write-Host "--- STEP 2-3: SKIPPING SYNTHEA (not selected) ---" -ForegroundColor DarkGray
}

# ============================================
# STEP 4: Build FHIR Loader Container
# ============================================
if ($doLoader) {
Write-Host ""
Write-Host "--- STEP 4: FHIR LOADER CONTAINER IMAGE ---" -ForegroundColor Cyan

$loaderImageExists = az acr repository show-tags --name $acrName --repository fhir-loader --query "contains(@, 'v1')" -o tsv 2>$null

if ($loaderImageExists -eq "true" -and -not $RebuildContainers) {
    Write-Host "FHIR Loader image already exists in ACR - skipping build" -ForegroundColor Green
    Write-Host "  Use -RebuildContainers to force a rebuild" -ForegroundColor DarkGray
} else {
    if ($RebuildContainers) {
        Write-Host "Rebuilding FHIR Loader container (forced)..." -ForegroundColor Cyan
    } else {
        Write-Host "Building FHIR Loader container (first time)..." -ForegroundColor Cyan
    }
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
}

# ============================================
# STEP 5: Run FHIR Loader Job
# ============================================
Write-Host ""
Write-Host "--- STEP 5: RUNNING FHIR LOADER ---" -ForegroundColor Cyan
Write-Host "Uploading synthetic data to FHIR service..."
Write-Host "This may take 30-60 minutes..." -ForegroundColor Yellow

# Delete existing FHIR loader job (new identity will get a unique role assignment via Bicep GUID)
Write-Host "Removing previous Loader container job..." -ForegroundColor DarkGray
az container delete --resource-group $ResourceGroupName --name fhir-loader-job --yes 2>$null | Out-Null

$loaderImage = "$acrLoginServer/fhir-loader:v1"

az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file bicep/fhir-loader-job.bicep `
    --parameters acrName=$acrName `
                 imageName=$loaderImage `
                 storageAccountName=$storageAccountName `
                 containerName=$containerName `
                 fhirServiceUrl=$fhirServiceUrl `
                 aciIdentityId=$aciIdentityId `
                 aciIdentityClientId=$aciIdentityClientId `
    --parameters $tagsParamRef

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR deploying FHIR Loader job" -ForegroundColor Red
    exit 1
}

# Wait for FHIR Loader to complete with live log streaming
Write-Host "Waiting for FHIR data upload to complete..."
Write-Host ""
$maxWaitMinutes = 90
$waitedMinutes = 0
$lastLogLines = 0

while ($waitedMinutes -lt $maxWaitMinutes) {
    $state = az container show `
        --resource-group $ResourceGroupName `
        --name fhir-loader-job `
        --query "instanceView.state" -o tsv 2>$null
    
    if ($state -eq "Succeeded") {
        Write-Host ""
        Write-Host "FHIR data upload completed successfully!" -ForegroundColor Green
        break
    } elseif ($state -eq "Failed") {
        Write-Host ""
        Write-Host "ERROR: FHIR data upload failed" -ForegroundColor Red
        az container logs --resource-group $ResourceGroupName --name fhir-loader-job
        exit 1
    } elseif ($state -eq "Terminated") {
        $exitCode = az container show `
            --resource-group $ResourceGroupName `
            --name fhir-loader-job `
            --query "containers[0].instanceView.currentState.exitCode" -o tsv 2>$null
        
        if ($exitCode -eq "0") {
            Write-Host ""
            Write-Host "FHIR data upload completed successfully!" -ForegroundColor Green
            break
        } else {
            Write-Host ""
            Write-Host "ERROR: FHIR data upload failed with exit code $exitCode" -ForegroundColor Red
            az container logs --resource-group $ResourceGroupName --name fhir-loader-job
            exit 1
        }
    }
    
    # Stream progress from container logs (loader has batch progress output)
    if ($state -eq "Running") {
        $logs = az container logs --resource-group $ResourceGroupName --name fhir-loader-job 2>$null
        if ($logs) {
            $logLines = @($logs -split "`n")
            if ($logLines.Count -gt $lastLogLines) {
                $newLines = $logLines[$lastLogLines..($logLines.Count - 1)]
                foreach ($line in $newLines) {
                    if ($line -match "batch|Uploaded|Downloaded|Processing|Patient|Device|Bundle|Organization|Error|FHIR|yielding|Complete") {
                        Write-Host "  [Loader] $line" -ForegroundColor DarkCyan
                    }
                }
                $lastLogLines = $logLines.Count
            }
        }
    }

    Write-Host "  Status: $state (waited $waitedMinutes min)" -ForegroundColor DarkGray
    Start-Sleep -Seconds 30
    $waitedMinutes += 0.5
}

if ($waitedMinutes -ge $maxWaitMinutes) {
    Write-Host "ERROR: FHIR data upload timed out" -ForegroundColor Red
    exit 1
}

# Show FHIR Loader logs
Write-Host ""
Write-Host "FHIR Loader logs (last 30 lines):" -ForegroundColor Gray
az container logs --resource-group $ResourceGroupName --name fhir-loader-job 2>$null | Select-Object -Last 30

    # Note: Device associations are created by the FHIR loader container (load_fhir.py)
    # using basic-resource-type|device-assoc code. The standalone create-device-associations.py
    # script is for manual/ad-hoc use only — do NOT run it here as it uses a different code system
    # (v3-RoleCode|ASSIGNED) which overwrites the loader's resources and breaks the DICOM loader.

} else {
    Write-Host ""
    Write-Host "--- STEP 4-5: SKIPPING FHIR LOADER (not selected) ---" -ForegroundColor DarkGray
}

# ============================================
# STEP 6: Build DICOM Loader Container
# ============================================
if ($doDicom) {

# Pre-flight: verify device-associations.json exists in blob storage
Write-Host ""
Write-Host "--- PRE-FLIGHT: Device Association Check ---" -ForegroundColor Cyan
try {
    $blobCheck = az storage blob show --account-name $storageAccountName --container-name synthea-output --name "device-associations.json" --auth-mode login --query "properties.contentLength" -o tsv 2>$null
    if ($blobCheck -and [int]$blobCheck -gt 10) {
        Write-Host "  ✓ device-associations.json found in blob ($blobCheck bytes)" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ device-associations.json not found — DICOM loader will fall back to FHIR search" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ⚠ Could not check blob — DICOM loader will fall back to FHIR search" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "--- STEP 6: DICOM LOADER CONTAINER IMAGE ---" -ForegroundColor Cyan

$dicomImageExists = az acr repository show-tags --name $acrName --repository dicom-loader --query "contains(@, 'v1')" -o tsv 2>$null

if ($dicomImageExists -eq "true" -and -not $RebuildContainers) {
    Write-Host "DICOM Loader image already exists in ACR - skipping build" -ForegroundColor Green
    Write-Host "  Use -RebuildContainers to force a rebuild" -ForegroundColor DarkGray
} else {
    if ($RebuildContainers) {
        Write-Host "Rebuilding DICOM Loader container (forced)..." -ForegroundColor Cyan
    } else {
        Write-Host "Building DICOM Loader container (first time)..." -ForegroundColor Cyan
    }
    Push-Location dicom-loader
    try {
        az acr build --registry $acrName --image "dicom-loader:v1" --no-logs .
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERROR building DICOM Loader container" -ForegroundColor Red
            exit 1
        }
    } finally {
        Pop-Location
    }
    Write-Host "DICOM Loader container built successfully" -ForegroundColor Green
}

# ============================================
# STEP 7: Run DICOM Loader Job
# ============================================
Write-Host ""
Write-Host "--- STEP 7: RUNNING DICOM LOADER ---" -ForegroundColor Cyan
Write-Host "Downloading TCIA studies, re-tagging, and uploading to ADLS Gen2..."
Write-Host "This may take 30-60 minutes..." -ForegroundColor Yellow

# Delete existing DICOM loader job
Write-Host "Removing previous DICOM Loader container job..." -ForegroundColor DarkGray
az container delete --resource-group $ResourceGroupName --name dicom-loader-job --yes 2>$null | Out-Null

$dicomImage = "$acrLoginServer/dicom-loader:v1"

az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file bicep/dicom-loader-job.bicep `
    --parameters acrName=$acrName `
                 imageName=$dicomImage `
                 storageAccountName=$storageAccountName `
                 fhirServiceUrl=$fhirServiceUrl `
                 aciIdentityId=$aciIdentityId `
                 aciIdentityClientId=$aciIdentityClientId `
    --parameters $tagsParamRef

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR deploying DICOM Loader job" -ForegroundColor Red
    exit 1
}

# Wait for DICOM Loader to complete with live log streaming
Write-Host "Waiting for DICOM processing to complete..."
Write-Host ""
$maxWaitMinutes = 120
$waitedMinutes = 0
$lastLogLines = 0

while ($waitedMinutes -lt $maxWaitMinutes) {
    $state = az container show `
        --resource-group $ResourceGroupName `
        --name dicom-loader-job `
        --query "instanceView.state" -o tsv 2>$null

    if ($state -eq "Succeeded") {
        Write-Host ""
        Write-Host "DICOM processing completed successfully!" -ForegroundColor Green
        break
    } elseif ($state -eq "Failed") {
        Write-Host ""
        Write-Host "ERROR: DICOM processing failed" -ForegroundColor Red
        az container logs --resource-group $ResourceGroupName --name dicom-loader-job
        exit 1
    } elseif ($state -eq "Terminated") {
        $exitCode = az container show `
            --resource-group $ResourceGroupName `
            --name dicom-loader-job `
            --query "containers[0].instanceView.currentState.exitCode" -o tsv 2>$null

        if ($exitCode -eq "0") {
            Write-Host ""
            Write-Host "DICOM processing completed successfully!" -ForegroundColor Green
            break
        } else {
            Write-Host ""
            Write-Host "ERROR: DICOM processing failed with exit code $exitCode" -ForegroundColor Red
            az container logs --resource-group $ResourceGroupName --name dicom-loader-job
            exit 1
        }
    }

    # Stream progress from container logs
    if ($state -eq "Running") {
        $logs = az container logs --resource-group $ResourceGroupName --name dicom-loader-job 2>$null
        if ($logs) {
            $logLines = @($logs -split "`n")
            if ($logLines.Count -gt $lastLogLines) {
                $newLines = $logLines[$lastLogLines..($logLines.Count - 1)]
                foreach ($line in $newLines) {
                    if ($line -match "Patient|Download|Upload|Re-tag|DICOM|study|Complete|Error|series|instances") {
                        Write-Host "  [DICOM] $line" -ForegroundColor DarkCyan
                    }
                }
                $lastLogLines = $logLines.Count
            }
        }
    }

    Write-Host "  Status: $state (waited $waitedMinutes min)" -ForegroundColor DarkGray
    Start-Sleep -Seconds 30
    $waitedMinutes += 0.5
}

if ($waitedMinutes -ge $maxWaitMinutes) {
    Write-Host "ERROR: DICOM processing timed out" -ForegroundColor Red
    exit 1
}

# Show DICOM Loader logs
Write-Host ""
Write-Host "DICOM Loader logs (last 30 lines):" -ForegroundColor Gray
az container logs --resource-group $ResourceGroupName --name dicom-loader-job 2>$null | Select-Object -Last 30

} else {
    Write-Host ""
    Write-Host "--- STEP 6-7: SKIPPING DICOM (not selected or --SkipDicom) ---" -ForegroundColor DarkGray
}

# ============================================
# STEP 9: Verification & Summary
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


