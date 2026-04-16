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

# ── Reusable FHIR $export function ────────────────────────────────────
# Triggers a bulk FHIR $export to ADLS Gen2, waits for completion, and
# returns $true on success. Safe to call multiple times — each export
# creates a new timestamped subfolder (never overwrites).
function Invoke-FhirExport {
    param(
        [Parameter(Mandatory)][string]$ResourceGroupName,
        [Parameter(Mandatory)][string]$FhirServiceUrl,
        [string]$ExportContainerName = "fhir-export",
        [string]$Label = "FHIR `$export"
    )

    Write-Host ""
    Write-Host "--- $Label → ADLS GEN2 ---" -ForegroundColor Cyan

    if (-not $FhirServiceUrl) {
        Write-Host "  ⚠ FHIR Service URL not available — skipping `$export" -ForegroundColor Yellow
        return $false
    }

    # Detect storage account
    Write-Host "  Detecting storage account in $ResourceGroupName..." -ForegroundColor Gray
    $storageAccounts = az storage account list --resource-group $ResourceGroupName `
        --query "[?kind=='StorageV2'].{name:name, id:id, hns:isHnsEnabled}" `
        -o json 2>$null | ConvertFrom-Json

    if (-not $storageAccounts -or $storageAccounts.Count -eq 0) {
        Write-Host "  ⚠ No StorageV2 account found — skipping `$export" -ForegroundColor Yellow
        return $false
    }

    $exportStorage = $storageAccounts | Where-Object { $_.hns -eq $true } | Select-Object -First 1
    if (-not $exportStorage) { $exportStorage = $storageAccounts[0] }
    $exportStorageName = $exportStorage.name
    Write-Host "  ✓ Storage account: $exportStorageName" -ForegroundColor Green

    # Create export container
    try { $null = az storage container create --name $ExportContainerName --account-name $exportStorageName --auth-mode login 2>$null } catch {}
    Write-Host "  ✓ Export container: $ExportContainerName" -ForegroundColor Green

    # Detect FHIR service resource
    $fhirResourceId = az resource list --resource-group $ResourceGroupName `
        --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
        --query "[0].id" -o tsv 2>$null

    if (-not $fhirResourceId) {
        Write-Host "  ⚠ FHIR service resource not found — skipping `$export" -ForegroundColor Yellow
        return $false
    }
    Write-Host "  ✓ FHIR service resource detected" -ForegroundColor Green

    # Configure export destination
    try {
        $null = az rest --method patch `
            --url "$fhirResourceId`?api-version=2023-11-01" `
            --body "{`"properties`":{`"exportConfiguration`":{`"storageAccountName`":`"$exportStorageName`"}}}" 2>$null
        Write-Host "  ✓ Export destination: $exportStorageName" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Could not configure export destination (may already be set)" -ForegroundColor Yellow
    }

    # Ensure RBAC
    $fhirMiPrincipalId = az resource show --ids $fhirResourceId --query "identity.principalId" -o tsv 2>$null
    if ($fhirMiPrincipalId) {
        try {
            $null = az role assignment create `
                --assignee-object-id $fhirMiPrincipalId `
                --assignee-principal-type ServicePrincipal `
                --role "ba92f5b4-2d11-453d-a403-e96b0029c9fe" `
                --scope $exportStorage.id 2>$null
        } catch {}
        Write-Host "  ✓ RBAC: FHIR MI → Storage Blob Data Contributor" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ FHIR service has no managed identity — RBAC assignment skipped" -ForegroundColor Yellow
        Write-Host "    Export may fail if the FHIR service can't write to storage" -ForegroundColor DarkGray
    }

    # Trigger $export
    Write-Host "  Triggering FHIR `$export..." -ForegroundColor White
    $fhirToken = az account get-access-token --resource $FhirServiceUrl --query accessToken -o tsv 2>$null
    if (-not $fhirToken) {
        Write-Host "  ⚠ Could not acquire FHIR access token" -ForegroundColor Yellow
        return $false
    }

    $exportDone = $false
    try {
        $exportResp = Invoke-WebRequest `
            -Uri "$FhirServiceUrl/`$export?_container=$ExportContainerName" `
            -Headers @{
                "Authorization" = "Bearer $fhirToken"
                "Accept"        = "application/fhir+json"
                "Prefer"        = "respond-async"
            } -Method GET -UseBasicParsing

        if ($exportResp.StatusCode -eq 202) {
            $contentLocation = $exportResp.Headers["Content-Location"]
            if ($contentLocation -is [array]) { $contentLocation = $contentLocation[0] }
            Write-Host "  ✓ FHIR `$export started (async)" -ForegroundColor Green
            Write-Host "    Status URL: $contentLocation" -ForegroundColor Gray

            # Poll until complete (max 60 min)
            $pollStart = Get-Date
            $maxPollMin = 60
            $pollSec = 30
            Write-Host "  Polling every ${pollSec}s (max ${maxPollMin}m)..." -ForegroundColor Gray

            while ((New-TimeSpan -Start $pollStart).TotalMinutes -lt $maxPollMin) {
                Start-Sleep -Seconds $pollSec
                $elapsed = [math]::Round((New-TimeSpan -Start $pollStart).TotalMinutes, 1)

                # Refresh token every 15 min
                if ([math]::Floor($elapsed) % 15 -eq 0 -and [math]::Floor($elapsed) -gt 0) {
                    $fhirToken = az account get-access-token --resource $FhirServiceUrl --query accessToken -o tsv 2>$null
                }

                try {
                    $pollResp = Invoke-WebRequest -Uri $contentLocation `
                        -Headers @{ "Authorization" = "Bearer $fhirToken" } -UseBasicParsing

                    if ($pollResp.StatusCode -eq 200) {
                        $exportResult = $pollResp.Content | ConvertFrom-Json
                        $fileCount = ($exportResult.output | Measure-Object).Count
                        $resourceTypes = ($exportResult.output | ForEach-Object { $_.type } | Sort-Object -Unique) -join ", "
                        Write-Host "  ✓ FHIR `$export API reports complete!" -ForegroundColor Green
                        Write-Host "    Files: $fileCount | Types: $resourceTypes" -ForegroundColor Green

                        # Verify blobs actually exist in storage (FHIR may report success but fail to write)
                        Write-Host "  Verifying blobs in storage..." -ForegroundColor DarkGray
                        Start-Sleep -Seconds 5  # Brief wait for storage consistency
                        $verifyBlobs = az storage blob list --container-name $ExportContainerName `
                            --account-name $exportStorageName --auth-mode login `
                            --num-results 1 --query "[0].name" -o tsv 2>$null
                        if ($verifyBlobs) {
                            Write-Host "  ✓ Verified: blobs exist in '$ExportContainerName'" -ForegroundColor Green
                            $exportDone = $true
                        } else {
                            # Try with prefix from the first output URL
                            $firstUrl = $exportResult.output[0].url
                            if ($firstUrl -match "/fhir-export/([^/]+)/") {
                                $prefix = $matches[1]
                                Write-Host "  Checking prefix '$prefix'..." -ForegroundColor DarkGray
                                $verifyBlobs2 = az storage blob list --container-name $ExportContainerName `
                                    --account-name $exportStorageName --auth-mode login `
                                    --prefix $prefix --num-results 1 --query "[0].name" -o tsv 2>$null
                                if ($verifyBlobs2) {
                                    Write-Host "  ✓ Verified: blobs exist at prefix '$prefix'" -ForegroundColor Green
                                    $exportDone = $true
                                } else {
                                    Write-Host "  ⚠ FHIR `$export reported success but NO blobs found in storage!" -ForegroundColor Red
                                    Write-Host "    Expected URL: $firstUrl" -ForegroundColor DarkGray
                                    Write-Host "    This may be a FHIR service export config issue or RBAC timing." -ForegroundColor DarkGray
                                    Write-Host "    Try running `$export manually: az rest --method get --url '$FhirServiceUrl/`$export?_container=$ExportContainerName'" -ForegroundColor DarkGray
                                }
                            } else {
                                Write-Host "  ⚠ FHIR `$export reported success but blob verification failed" -ForegroundColor Yellow
                            }
                        }
                        break
                    }
                } catch {
                    $sc = $null
                    try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
                    if ($sc -eq 202) {
                        Write-Host "    Exporting... (${elapsed}m elapsed)" -ForegroundColor Gray
                    } else {
                        Write-Host "    ⚠ Poll error: $($_.Exception.Message)" -ForegroundColor Yellow
                    }
                }
            }

            if (-not $exportDone) {
                Write-Host "  ⚠ Export still running after ${maxPollMin}m — will complete in background" -ForegroundColor Yellow
                $exportDone = $true
            }
        } else {
            Write-Host "  ⚠ Unexpected FHIR `$export response: HTTP $($exportResp.StatusCode)" -ForegroundColor Yellow
            Write-Host "    Expected 202 Accepted." -ForegroundColor DarkGray
            try { Write-Host "    Body: $($exportResp.Content.Substring(0, [math]::Min(200, $exportResp.Content.Length)))" -ForegroundColor DarkGray } catch {}
        }
    } catch {
        $sc = $null
        try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
        if ($sc -eq 409) {
            Write-Host "  ⚠ Another `$export is already running — will wait for it" -ForegroundColor Yellow
            $exportDone = $true
        } else {
            Write-Host "  ⚠ Failed to trigger `$export: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }

    return $exportDone
}

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

# Change to repo root so relative paths (bicep/, synthea/, etc.) work
$ScriptDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
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
        --parameters location="$Location" `
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
                --parameters location="$Location" `
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
    Write-Host "ERROR: Existing infrastructure not found. Please run phase-1/deploy.ps1 first." -ForegroundColor Red
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
    # Validate the existing image is healthy
    $syntheaManifest = az acr manifest list-metadata --registry $acrName --name synthea-generator --query "[?tags[?contains(@, 'v1')]].{digest:digest, createdTime:createdTime, lastUpdateTime:lastUpdateTime}" -o json 2>$null | ConvertFrom-Json
    if ($syntheaManifest) {
        Write-Host "Synthea image already exists in ACR — skipping build" -ForegroundColor Green
        Write-Host "  Image: $acrName.azurecr.io/synthea-generator:v1" -ForegroundColor DarkGray
        Write-Host "  Created: $($syntheaManifest[0].createdTime)" -ForegroundColor DarkGray
        Write-Host "  Digest:  $($syntheaManifest[0].digest.Substring(0, 19))..." -ForegroundColor DarkGray
    } else {
        Write-Host "Synthea image tag exists but manifest not readable — skipping build" -ForegroundColor Yellow
    }
    Write-Host "  Use -RebuildContainers to force a rebuild" -ForegroundColor DarkGray
} else {
    if ($RebuildContainers) {
        Write-Host "Rebuilding Synthea container (forced)..." -ForegroundColor Cyan
    } else {
        Write-Host "Building Synthea container (first time)..." -ForegroundColor Cyan
    }
    Push-Location synthea
    try {
        $acrBuildErrLog = Join-Path $env:TEMP "acr-build-synthea-$(Get-Random).log"
        Write-Host "  Building image: $acrName/synthea-generator:v1" -ForegroundColor DarkGray
        Write-Host "  Context: $(Get-Location)" -ForegroundColor DarkGray
        Write-Host "  Stderr log: $acrBuildErrLog" -ForegroundColor DarkGray
        az acr build --registry $acrName --image "synthea-generator:v1" . --no-logs 2>$acrBuildErrLog
        $buildExitCode = $LASTEXITCODE
        Write-Host "  az acr build exit code: $buildExitCode" -ForegroundColor DarkGray
        if ($buildExitCode -ne 0) {
            Write-Host "  ⚠ ACR build command returned non-zero exit code ($buildExitCode)" -ForegroundColor Yellow
            $stderrTail = Get-Content $acrBuildErrLog -ErrorAction SilentlyContinue | Select-Object -Last 10
            if ($stderrTail) {
                Write-Host "  ACR stderr (tail):" -ForegroundColor Yellow
                $stderrTail | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
            Write-Host "  Checking if image exists in ACR despite build error..." -ForegroundColor DarkGray
            $tagCheck = az acr repository show-tags --name $acrName --repository synthea-generator --query "contains(@, 'v1')" -o tsv 2>$null
            Write-Host "  Tag check result: $tagCheck" -ForegroundColor DarkGray
            if ($tagCheck -eq "true") {
                Write-Host "  ✓ Image exists in ACR despite build log error — continuing" -ForegroundColor Yellow
            } else {
                Write-Host "  ✗ Image NOT found in ACR — build actually failed" -ForegroundColor Red
                exit 1
            }
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

# Pre-flight: Verify the Masimo emulator is running (new patients need active device telemetry)
Write-Host "  Checking Masimo emulator status..." -ForegroundColor DarkGray
$emulatorState = az container show --resource-group $ResourceGroupName --name masimo-emulator-grp `
    --query "{state:instanceView.state, deviceCount:containers[0].environmentVariables[?name=='DEVICE_COUNT'].value | [0]}" -o json 2>$null | ConvertFrom-Json
if ($emulatorState -and $emulatorState.state -eq "Running") {
    $emDevices = if ($emulatorState.deviceCount) { $emulatorState.deviceCount } else { "100" }
    Write-Host "  ✓ Masimo emulator is running ($emDevices devices generating telemetry)" -ForegroundColor Green
    Write-Host "    New patients will be associated with existing device IDs (MASIMO-RADIUS7-0001...$emDevices)" -ForegroundColor DarkGray
} elseif ($emulatorState) {
    Write-Host "  ⚠ Masimo emulator exists but state=$($emulatorState.state) — telemetry may not be flowing" -ForegroundColor Yellow
    Write-Host "    Consider re-running phase-1/deploy.ps1 to restart the emulator after data loading" -ForegroundColor Yellow
} else {
    Write-Host "  ⚠ Masimo emulator not found in $ResourceGroupName" -ForegroundColor Yellow
    Write-Host "    Run phase-1/deploy.ps1 first to create the emulator container" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "Generating $PatientCount synthetic patients for Atlanta, GA..."
Write-Host "This may take 15-30 minutes..." -ForegroundColor Yellow

# Clear existing blobs so only the new batch is loaded
Write-Host "Clearing previous Synthea output from blob storage if it exists..." -ForegroundColor DarkGray
az storage blob delete-batch --account-name $storageAccountName --source $containerName --auth-mode login --pattern "*.json" 2>$null | Out-Null
Write-Host "  Previous files cleared" -ForegroundColor DarkGray

# Delete existing Synthea job (new identity will get a unique role assignment via Bicep GUID)
Write-Host "Removing previous Synthea container job if it exists..." -ForegroundColor DarkGray
$prevSyntheaState = az container show --resource-group $ResourceGroupName --name synthea-generator-job --query "{state:instanceView.state, exitCode:containers[0].instanceView.currentState.exitCode, startTime:containers[0].instanceView.currentState.startTime}" -o json 2>$null | ConvertFrom-Json
if ($prevSyntheaState) {
    Write-Host "  Previous Synthea run: state=$($prevSyntheaState.state), exitCode=$($prevSyntheaState.exitCode), started=$($prevSyntheaState.startTime)" -ForegroundColor DarkGray
}
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

    # Verify Synthea blobs actually landed in synthea-output container
    Write-Host ""
    Write-Host "  Verifying Synthea blobs in storage..." -ForegroundColor DarkGray
    try {
        $syntheaBlobCount = az storage blob list --container-name $containerName `
            --account-name $storageAccountName --auth-mode login `
            --query "length(@)" -o tsv 2>$null
        if ($syntheaBlobCount -and [int]$syntheaBlobCount -gt 0) {
            Write-Host "  ✓ Verified: $syntheaBlobCount Synthea blobs in '$containerName' container" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Synthea reported success but NO blobs found in '$containerName'!" -ForegroundColor Red
            Write-Host "    Storage account: $storageAccountName" -ForegroundColor DarkGray
            Write-Host "    The FHIR Loader will have no data to process." -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "  ⚠ Could not verify Synthea blobs: $($_.Exception.Message)" -ForegroundColor Yellow
    }

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
    $loaderManifest = az acr manifest list-metadata --registry $acrName --name fhir-loader --query "[?tags[?contains(@, 'v1')]].{digest:digest, createdTime:createdTime}" -o json 2>$null | ConvertFrom-Json
    if ($loaderManifest) {
        Write-Host "FHIR Loader image already exists in ACR — skipping build" -ForegroundColor Green
        Write-Host "  Image: $acrName.azurecr.io/fhir-loader:v1" -ForegroundColor DarkGray
        Write-Host "  Created: $($loaderManifest[0].createdTime)" -ForegroundColor DarkGray
        Write-Host "  Digest:  $($loaderManifest[0].digest.Substring(0, 19))..." -ForegroundColor DarkGray
    } else {
        Write-Host "FHIR Loader image tag exists but manifest not readable — skipping build" -ForegroundColor Yellow
    }
    Write-Host "  Use -RebuildContainers to force a rebuild" -ForegroundColor DarkGray
} else {
    if ($RebuildContainers) {
        Write-Host "Rebuilding FHIR Loader container (forced)..." -ForegroundColor Cyan
    } else {
        Write-Host "Building FHIR Loader container (first time)..." -ForegroundColor Cyan
    }
    Push-Location fhir-loader
    try {
        $acrBuildErrLog = Join-Path $env:TEMP "acr-build-fhir-loader-$(Get-Random).log"
        Write-Host "  Building image: $acrName/fhir-loader:v1" -ForegroundColor DarkGray
        Write-Host "  Context: $(Get-Location)" -ForegroundColor DarkGray
        Write-Host "  Stderr log: $acrBuildErrLog" -ForegroundColor DarkGray
        az acr build --registry $acrName --image "fhir-loader:v1" . --no-logs 2>$acrBuildErrLog
        $buildExitCode = $LASTEXITCODE
        Write-Host "  az acr build exit code: $buildExitCode" -ForegroundColor DarkGray
        if ($buildExitCode -ne 0) {
            Write-Host "  ⚠ ACR build command returned non-zero exit code ($buildExitCode)" -ForegroundColor Yellow
            $stderrTail = Get-Content $acrBuildErrLog -ErrorAction SilentlyContinue | Select-Object -Last 10
            if ($stderrTail) {
                Write-Host "  ACR stderr (tail):" -ForegroundColor Yellow
                $stderrTail | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
            # Check if image actually exists despite the stream error
            Write-Host "  Checking if image exists in ACR despite build error..." -ForegroundColor DarkGray
            $tagCheck = az acr repository show-tags --name $acrName --repository fhir-loader --query "contains(@, 'v1')" -o tsv 2>$null
            Write-Host "  Tag check result: $tagCheck" -ForegroundColor DarkGray
            if ($tagCheck -eq "true") {
                Write-Host "  ✓ Image exists in ACR despite build log error — continuing" -ForegroundColor Yellow
            } else {
                Write-Host "  ✗ Image NOT found in ACR — build actually failed" -ForegroundColor Red
                exit 1
            }
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
$prevLoaderState = az container show --resource-group $ResourceGroupName --name fhir-loader-job --query "{state:instanceView.state, exitCode:containers[0].instanceView.currentState.exitCode, startTime:containers[0].instanceView.currentState.startTime}" -o json 2>$null | ConvertFrom-Json
if ($prevLoaderState) {
    Write-Host "  Previous FHIR Loader run: state=$($prevLoaderState.state), exitCode=$($prevLoaderState.exitCode), started=$($prevLoaderState.startTime)" -ForegroundColor DarkGray
}
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

    # FHIR $export after loader — captures Patient, Condition, Device, Observation, etc.
    if ($fhirServiceUrl) {
        Write-Host "  Triggering POST-LOADER FHIR `$export..." -ForegroundColor White
        $postLoaderExportResult = Invoke-FhirExport -ResourceGroupName $ResourceGroupName `
            -FhirServiceUrl $fhirServiceUrl `
            -Label "POST-LOADER: FHIR `$export (clinical data)"
        if (-not $postLoaderExportResult) {
            Write-Host "  ⚠ POST-LOADER `$export did not complete — HDS pipelines may have incomplete data" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  ⚠ Skipping POST-LOADER `$export — FHIR URL not available" -ForegroundColor Yellow
    }

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
    $dicomManifest = az acr manifest list-metadata --registry $acrName --name dicom-loader --query "[?tags[?contains(@, 'v1')]].{digest:digest, createdTime:createdTime}" -o json 2>$null | ConvertFrom-Json
    if ($dicomManifest) {
        Write-Host "DICOM Loader image already exists in ACR — skipping build" -ForegroundColor Green
        Write-Host "  Image: $acrName.azurecr.io/dicom-loader:v1" -ForegroundColor DarkGray
        Write-Host "  Created: $($dicomManifest[0].createdTime)" -ForegroundColor DarkGray
        Write-Host "  Digest:  $($dicomManifest[0].digest.Substring(0, 19))..." -ForegroundColor DarkGray
    } else {
        Write-Host "DICOM Loader image tag exists but manifest not readable — skipping build" -ForegroundColor Yellow
    }
    Write-Host "  Use -RebuildContainers to force a rebuild" -ForegroundColor DarkGray
} else {
    if ($RebuildContainers) {
        Write-Host "Rebuilding DICOM Loader container (forced)..." -ForegroundColor Cyan
    } else {
        Write-Host "Building DICOM Loader container (first time)..." -ForegroundColor Cyan
    }
    Push-Location dicom-loader
    try {
        $acrBuildErrLog = Join-Path $env:TEMP "acr-build-dicom-loader-$(Get-Random).log"
        Write-Host "  Building image: $acrName/dicom-loader:v1" -ForegroundColor DarkGray
        Write-Host "  Context: $(Get-Location)" -ForegroundColor DarkGray
        Write-Host "  Stderr log: $acrBuildErrLog" -ForegroundColor DarkGray
        az acr build --registry $acrName --image "dicom-loader:v1" . --no-logs 2>$acrBuildErrLog
        $buildExitCode = $LASTEXITCODE
        Write-Host "  az acr build exit code: $buildExitCode" -ForegroundColor DarkGray
        if ($buildExitCode -ne 0) {
            Write-Host "  ⚠ ACR build command returned non-zero exit code ($buildExitCode)" -ForegroundColor Yellow
            $stderrTail = Get-Content $acrBuildErrLog -ErrorAction SilentlyContinue | Select-Object -Last 10
            if ($stderrTail) {
                Write-Host "  ACR stderr (tail):" -ForegroundColor Yellow
                $stderrTail | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
            }
            Write-Host "  Checking if image exists in ACR despite build error..." -ForegroundColor DarkGray
            $tagCheck = az acr repository show-tags --name $acrName --repository dicom-loader --query "contains(@, 'v1')" -o tsv 2>$null
            Write-Host "  Tag check result: $tagCheck" -ForegroundColor DarkGray
            if ($tagCheck -eq "true") {
                Write-Host "  ✓ Image exists in ACR despite build log error — continuing" -ForegroundColor Yellow
            } else {
                Write-Host "  ✗ Image NOT found in ACR — build actually failed" -ForegroundColor Red
                exit 1
            }
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
$prevDicomState = az container show --resource-group $ResourceGroupName --name dicom-loader-job --query "{state:instanceView.state, exitCode:containers[0].instanceView.currentState.exitCode, startTime:containers[0].instanceView.currentState.startTime}" -o json 2>$null | ConvertFrom-Json
if ($prevDicomState) {
    Write-Host "  Previous DICOM Loader run: state=$($prevDicomState.state), exitCode=$($prevDicomState.exitCode), started=$($prevDicomState.startTime)" -ForegroundColor DarkGray
}
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

    # Verify DICOM blobs actually landed in dicom-output container
    Write-Host ""
    Write-Host "  Verifying DICOM blobs in storage..." -ForegroundColor DarkGray
    try {
        $dicomBlobCount = az storage blob list --container-name "dicom-output" `
            --account-name $storageAccountName --auth-mode login `
            --query "length(@)" -o tsv 2>$null
        if ($dicomBlobCount -and [int]$dicomBlobCount -gt 0) {
            Write-Host "  ✓ Verified: $dicomBlobCount DICOM blobs in 'dicom-output' container" -ForegroundColor Green
        } else {
            # Try filesystem API for HNS-enabled accounts
            $dicomFiles = az storage fs file list --file-system "dicom-output" `
                --account-name $storageAccountName --auth-mode login `
                --query "length(@)" -o tsv 2>$null
            if ($dicomFiles -and [int]$dicomFiles -gt 0) {
                Write-Host "  ✓ Verified: $dicomFiles DICOM files in 'dicom-output' filesystem" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ DICOM Loader reported success but NO blobs found in 'dicom-output'!" -ForegroundColor Red
                Write-Host "    Storage account: $storageAccountName" -ForegroundColor DarkGray
                Write-Host "    The DICOM loader may not have uploaded studies to storage." -ForegroundColor DarkGray
            }
        }
    } catch {
        Write-Host "  ⚠ Could not verify DICOM blobs: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    # FHIR $export after DICOM — now includes ImagingStudy resources in addition to clinical data
    if ($fhirServiceUrl) {
        Write-Host "  Triggering POST-DICOM FHIR `$export..." -ForegroundColor White
        $postDicomExportResult = Invoke-FhirExport -ResourceGroupName $ResourceGroupName `
            -FhirServiceUrl $fhirServiceUrl `
            -Label "POST-DICOM: FHIR `$export (clinical + imaging data)"
        if (-not $postDicomExportResult) {
            Write-Host "  ⚠ POST-DICOM `$export did not complete — ImagingStudy data may be missing" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  ⚠ Skipping POST-DICOM `$export — FHIR URL not available" -ForegroundColor Yellow
    }

} else {
    Write-Host ""
    Write-Host "--- STEP 6-7: SKIPPING DICOM (not selected or --SkipDicom) ---" -ForegroundColor DarkGray
}

# ============================================
# STEP 8: FHIR $EXPORT (catch-up if not already done)
# ============================================
# When reusing patients or when only selective steps ran, the per-loader
# exports won't fire. This ensures FHIR data is always exported to ADLS Gen2
# for HDS pipelines, Bronze Lakehouse shortcuts, and Silver/Gold tables.
Write-Host ""
Write-Host "--- STEP 8: FHIR `$EXPORT CHECK ---" -ForegroundColor Cyan

if (-not $fhirServiceUrl) {
    Write-Host "  ⚠ FHIR Service URL not available — cannot run `$export" -ForegroundColor Yellow
    Write-Host "    This usually means the FHIR infrastructure was not detected." -ForegroundColor DarkGray
} else {
    Write-Host "  FHIR URL: $fhirServiceUrl" -ForegroundColor DarkGray
    # Check if any export data already exists
    $exportNeeded = $true
    $stAcct = $null
    try {
        $stAcct = az storage account list -g $ResourceGroupName `
            --query "[?kind=='StorageV2'].name | [0]" -o tsv 2>$null
        Write-Host "  Storage account: $stAcct" -ForegroundColor DarkGray
        if ($stAcct) {
            $existingExport = az storage blob list --container-name "fhir-export" `
                --account-name $stAcct --auth-mode login --num-results 1 `
                --query "[0].name" -o tsv 2>$null
            if ($existingExport) {
                Write-Host "  ✓ FHIR export data already exists in 'fhir-export' container — skipping" -ForegroundColor Green
                $exportNeeded = $false
            } else {
                Write-Host "  No export data found in 'fhir-export' container — export needed" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  ⚠ No StorageV2 account found in $ResourceGroupName" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "  ⚠ Export check failed: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    if ($exportNeeded) {
        Write-Host "  Triggering FHIR `$export..." -ForegroundColor White
        $exportResult = Invoke-FhirExport -ResourceGroupName $ResourceGroupName `
            -FhirServiceUrl $fhirServiceUrl `
            -Label "STEP 8: FHIR `$export (ensure HDS pipeline data)"
        if ($exportResult) {
            Write-Host "  ✓ FHIR `$export completed successfully" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ FHIR `$export may not have completed — check storage" -ForegroundColor Yellow
        }
    }
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


