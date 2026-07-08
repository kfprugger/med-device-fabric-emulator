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
#   .\deploy-fhir.ps1 -RunDicom                # TCIA DICOM download + ADLS/FHIR ImagingStudy load only
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
    [switch]$UseCachedSynthea,
    [switch]$ReusePatients,
    [string]$SourceResourceGroup = "",
    [hashtable]$Tags = @{},
    [switch]$SkipFhir
)

# Determine which steps to run
$selectiveMode = $InfraOnly -or $RunSynthea -or $RunLoader -or $RunDicom
$doInfra = -not $selectiveMode -or $InfraOnly
$doSynthea = -not $selectiveMode -or $RunSynthea
$doLoader = -not $selectiveMode -or $RunLoader
$doDicom = (-not $selectiveMode -and -not $SkipDicom) -or $RunDicom

if ($SkipFhir) {
    $doSynthea = $false
    $doLoader = $false
}

$ErrorActionPreference = "Stop"

# Ensure cross-platform temp directory is populated in $env:TEMP
if (-not $env:TEMP) {
    $env:TEMP = [System.IO.Path]::GetTempPath()
}

# Fix Azure CLI Unicode encoding issue on Windows (az acr build log streaming)
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Prevent interactive `az` extension-install prompts from hanging the orchestrator
# (which runs pwsh with -NonInteractive — any prompt = infinite hang). See #fix-2.
$null = az config set extension.use_dynamic_install=yes_without_prompt --only-show-errors 2>$null

function ConvertTo-StrictIntFromCliOutput {
    param(
        [AllowNull()]$Value,
        [Parameter(Mandatory)][string]$Label,
        [switch]$EmptyAsZero
    )

    $lines = @()
    foreach ($item in @($Value)) {
        if ($null -eq $item) { continue }
        $lines += [regex]::Split("$item", "`r?`n")
    }

    foreach ($line in $lines) {
        $trimmed = "$line".Trim()
        if ($trimmed -match '^-?\d+$') {
            return [int]$trimmed
        }
    }

    $nonEmptyLines = @($lines | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
    if ($EmptyAsZero -and $nonEmptyLines.Count -eq 0) { return 0 }

    $preview = (($nonEmptyLines | Select-Object -First 3) -join " | ")
    if (-not $preview) { $preview = "<empty>" }
    throw "Azure CLI did not return a numeric value for $Label. Output: $preview"
}



$script:DeploymentSubscriptionId = $null
try {
    $azContext = Get-AzContext -ErrorAction SilentlyContinue
    if ($azContext -and $azContext.Subscription -and $azContext.Subscription.Id) {
        $script:DeploymentSubscriptionId = $azContext.Subscription.Id
    }
} catch { }
if (-not $script:DeploymentSubscriptionId) {
    $script:DeploymentSubscriptionId = (az account show --query id -o tsv 2>$null)
}

function Use-DeploymentAzSubscription {
    if ($script:DeploymentSubscriptionId) {
        az account set --subscription $script:DeploymentSubscriptionId 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "Azure CLI context drifted and could not be reset to subscription $script:DeploymentSubscriptionId"
        }
    }
}

function Get-AciContainerState {
    param(
        [Parameter(Mandatory)][string]$ResourceGroup,
        [Parameter(Mandatory)][string]$Name
    )

    Use-DeploymentAzSubscription
    $stateRaw = az container show `
        --resource-group $ResourceGroup `
        --name $Name `
        --query "{container:containers[0].instanceView.currentState.state, group:instanceView.state}" -o json 2>$null

    if ($LASTEXITCODE -ne 0 -or -not $stateRaw) {
        return ""
    }

    try {
        $stateInfo = $stateRaw | ConvertFrom-Json
        if ($stateInfo.container) { return "$($stateInfo.container)".Trim() }
        if ($stateInfo.group) { return "$($stateInfo.group)".Trim() }
    } catch { }

    return ""
}

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

    Use-DeploymentAzSubscription
    $result = az @cmd 2>&1
    $createExitCode = $LASTEXITCODE

    if ($createExitCode -ne 0) {
        $global:LASTEXITCODE = $createExitCode
        return $result
    }

    $startTime = Get-Date
    $lastHeartbeat = [datetime]::MinValue
    $state = ""
    $statusFailureCount = 0
    while ($true) {
        $stateRaw = az deployment group show `
            --resource-group $ResourceGroup `
            --name $DeploymentName `
            --query "properties.provisioningState" -o tsv 2>&1

        if ($LASTEXITCODE -eq 0 -and $stateRaw) {
            $state = "$stateRaw".Trim()
            $statusFailureCount = 0
        } else {
            $statusFailureCount++
            $statusError = "$stateRaw".Trim()
            if ($statusError -match "ResourceGroupNotFound") {
                Write-Host "    ARM status lookup failed: $statusError" -ForegroundColor Yellow
                $state = "Failed"
            } elseif ($statusError -match "DeploymentNotFound|could not be found|was not found" -and $statusFailureCount -ge 3) {
                Write-Host "    ARM status lookup failed: $statusError" -ForegroundColor Yellow
                $state = "Failed"
            } elseif ($statusFailureCount -ge 18) {
                Write-Host "    ARM status lookup failed $statusFailureCount consecutive times; treating deployment as failed." -ForegroundColor Yellow
                if ($statusError) { Write-Host "    Last status error: $statusError" -ForegroundColor DarkGray }
                $state = "Failed"
            } else {
                $state = "Running"
            }
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
        Use-DeploymentAzSubscription
        $result = az deployment group show `
            --resource-group $ResourceGroup `
            --name $DeploymentName `
            --query $Query -o json 2>&1
        if ($LASTEXITCODE -ne 0 -and $exitCode -eq 0) {
            $exitCode = $LASTEXITCODE
        }
    }

    Use-DeploymentAzSubscription
    Write-Host "  ARM deployment diagnostics: $DeploymentName" -ForegroundColor DarkGray
    Show-ArmDeploymentDiagnostics -ResourceGroup $ResourceGroup -DeploymentName $DeploymentName

    $global:LASTEXITCODE = $exitCode
    return $result
}

function Get-AcrImageMetadata {
    param(
        [Parameter(Mandatory)][string]$Registry,
        [Parameter(Mandatory)][string]$Repository,
        [Parameter(Mandatory)][string]$Tag
    )

    $raw = az acr manifest list-metadata --registry $Registry --name $Repository --query "[?tags[?contains(@, '$Tag')]][0].{digest:digest, createdTime:createdTime, lastUpdateTime:lastUpdateTime}" -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw) -or $raw -eq "null") { return $null }
    return $raw | ConvertFrom-Json
}

function Test-AcrImageUpdated {
    param(
        [object]$Metadata,
        [string]$PreviousDigest,
        [datetime]$StartedUtc
    )

    if (-not $Metadata) { return $false }
    if (-not $PreviousDigest) { return $true }
    if ($Metadata.digest -and $Metadata.digest -ne $PreviousDigest) { return $true }

    $lastUpdate = $null
    if ($Metadata.lastUpdateTime) {
        try { $lastUpdate = [datetime]::Parse($Metadata.lastUpdateTime).ToUniversalTime() } catch { $lastUpdate = $null }
    }
    return ($lastUpdate -and $lastUpdate -ge $StartedUtc.AddMinutes(-1))
}

function Invoke-AcrBuildWithTagVerification {
    param(
        [Parameter(Mandatory)][string]$Registry,
        [Parameter(Mandatory)][string]$Repository,
        [Parameter(Mandatory)][string]$Tag,
        [Parameter(Mandatory)][string]$ContextPath,
        [Parameter(Mandatory)][string]$ErrorLog,
        [int]$RemoteCompletionWaitSeconds = 300,
        [switch]$NoLogs
    )

    $before = Get-AcrImageMetadata -Registry $Registry -Repository $Repository -Tag $Tag
    $previousDigest = if ($before) { $before.digest } else { $null }
    $startedUtc = (Get-Date).ToUniversalTime()

    $buildArgs = @("acr", "build", "--registry", $Registry, "--image", "${Repository}:${Tag}", $ContextPath)
    if ($NoLogs) { $buildArgs += "--no-logs" }
    az @buildArgs 2>$ErrorLog
    $buildExitCode = $LASTEXITCODE
    Write-Host "  az acr build exit code: $buildExitCode" -ForegroundColor DarkGray

    if ($buildExitCode -eq 0) { return }

    Write-Host "  ⚠ ACR build command returned non-zero exit code ($buildExitCode)" -ForegroundColor Yellow
    $stderrTail = Get-Content $ErrorLog -ErrorAction SilentlyContinue | Select-Object -Last 10
    if ($stderrTail) {
        Write-Host "  ACR stderr (tail):" -ForegroundColor Yellow
        $stderrTail | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
    }

    Write-Host "  ACR command failed before confirming remote completion; polling for ${Repository}:${Tag}..." -ForegroundColor Yellow
    $deadline = (Get-Date).AddSeconds($RemoteCompletionWaitSeconds)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 15
        $metadata = Get-AcrImageMetadata -Registry $Registry -Repository $Repository -Tag $Tag
        if (Test-AcrImageUpdated -Metadata $metadata -PreviousDigest $previousDigest -StartedUtc $startedUtc) {
            Write-Host "  ✓ Image ${Repository}:${Tag} is present/updated in ACR after command disconnect — continuing" -ForegroundColor Yellow
            return
        }
        Write-Host "  Waiting for remote ACR build result..." -ForegroundColor DarkGray
    }

    Write-Host "  ✗ Image ${Repository}:${Tag} was not published after ACR command failure" -ForegroundColor Red
    exit $buildExitCode
}

function Ensure-FhirStorageNetworkAccess {
    param(
        [Parameter(Mandatory)][string]$ResourceGroup,
        [Parameter(Mandatory)][string]$StorageAccountName
    )

    $storageStateRaw = az storage account show --resource-group $ResourceGroup --name $StorageAccountName --query "{publicNetworkAccess:publicNetworkAccess, defaultAction:networkRuleSet.defaultAction, bypass:networkRuleSet.bypass}" -o json 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($storageStateRaw)) {
        Write-Host "  ⚠ Could not inspect storage network rules for $StorageAccountName" -ForegroundColor Yellow
        return
    }

    $storageState = $storageStateRaw | ConvertFrom-Json
    $needsUpdate = $false
    $updateArgs = @("storage", "account", "update", "--resource-group", $ResourceGroup, "--name", $StorageAccountName)

    if ($storageState.publicNetworkAccess -ne "Enabled") {
        Write-Host "  Enabling storage public network access so ACI Synthea/FHIR loader jobs can reach blob endpoints..." -ForegroundColor Yellow
        $updateArgs += @("--public-network-access", "Enabled")
        $needsUpdate = $true
    }
    if ($storageState.defaultAction -ne "Allow") {
        Write-Host "  Setting storage default network action to Allow for deployment data-plane jobs..." -ForegroundColor Yellow
        $updateArgs += @("--default-action", "Allow")
        $needsUpdate = $true
    }
    if ($storageState.bypass -notmatch "AzureServices") {
        Write-Host "  Enabling AzureServices storage firewall bypass..." -ForegroundColor Yellow
        $updateArgs += @("--bypass", "AzureServices")
        $needsUpdate = $true
    }

    if ($needsUpdate) {
        az @updateArgs --output none
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  ✗ Failed to update storage network rules for $StorageAccountName" -ForegroundColor Red
            exit 1
        }
        Write-Host "  ✓ Storage network rules allow deployment data-plane access" -ForegroundColor Green
    }
}
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
        throw "FHIR Service URL not available; cannot run required `$export"
    }

    # Detect storage account
    Use-DeploymentAzSubscription
    Write-Host "  Detecting storage account in $ResourceGroupName..." -ForegroundColor Gray
    $storageAccounts = az storage account list --resource-group $ResourceGroupName `
        --query "[?kind=='StorageV2'].{name:name, id:id, hns:isHnsEnabled}" `
        -o json 2>$null | ConvertFrom-Json

    if (-not $storageAccounts -or $storageAccounts.Count -eq 0) {
        throw "No StorageV2 account found; cannot run required FHIR `$export"
    }

    $exportStorage = $storageAccounts | Where-Object { $_.hns -eq $true } | Select-Object -First 1
    if (-not $exportStorage) { $exportStorage = $storageAccounts[0] }
    $exportStorageName = $exportStorage.name
    Write-Host "  ✓ Storage account: $exportStorageName" -ForegroundColor Green

    # Create export container
    Use-DeploymentAzSubscription
    try { $null = az storage container create --name $ExportContainerName --account-name $exportStorageName --auth-mode login 2>$null } catch {}
    Write-Host "  ✓ Export container: $ExportContainerName" -ForegroundColor Green

    # Detect FHIR service resource
    Use-DeploymentAzSubscription
    $fhirResourceId = az resource list --resource-group $ResourceGroupName `
        --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" `
        --query "[0].id" -o tsv 2>$null

    if (-not $fhirResourceId) {
        throw "FHIR service resource not found; cannot run required `$export"
    }
    Write-Host "  ✓ FHIR service resource detected" -ForegroundColor Green

    # Configure export destination
    try {
        Use-DeploymentAzSubscription
        $null = az rest --method patch `
            --url "$fhirResourceId`?api-version=2023-11-01" `
            --body "{`"properties`":{`"exportConfiguration`":{`"storageAccountName`":`"$exportStorageName`"}}}" 2>$null
        Write-Host "  ✓ Export destination: $exportStorageName" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Could not configure export destination (may already be set)" -ForegroundColor Yellow
    }

    # Ensure RBAC
    Use-DeploymentAzSubscription
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
        throw "Could not acquire FHIR access token for required `$export"
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
                                    throw "FHIR `$export reported success but no blobs were found in '$ExportContainerName'. Expected URL: $firstUrl"
                                }
                            } else {
                                throw "FHIR `$export reported success but blob verification failed"
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
                throw "FHIR `$export did not complete within ${maxPollMin}m"
            }
        } else {
            throw "Unexpected FHIR `$export response: HTTP $($exportResp.StatusCode); expected 202 Accepted."
        }
    } catch {
        $sc = $null
        try { $sc = $_.Exception.Response.StatusCode.value__ } catch {}
        if ($sc -eq 409) {
            throw "Another FHIR `$export is already running; deployment cannot verify required export output"
        } else {
            throw "Failed to trigger FHIR `$export: $($_.Exception.Message)"
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
$ScriptDir = Split-Path -Parent $PSScriptRoot
Push-Location $ScriptDir
Write-Host "Working directory: $(Get-Location)"

try {

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  FHIR Service Deployment with Synthea" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Resource Group: $ResourceGroupName"
Write-Host "Location: $Location"
Write-Host "Patient Count: $PatientCount"
if ($UseCachedSynthea) {
    Write-Host "Use Cached Synthea: True (bypassing generation container)" -ForegroundColor Yellow
}
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

    # Verify the FHIR service is actually reachable.
    # Use `az resource show` (built-in) instead of `az healthcareapis workspace fhir-service show`
    # so we don't depend on the `healthcareapis` CLI extension. Under -NonInteractive,
    # a missing extension would silently hang waiting on a y/n prompt. See #fix-3.
    $fhirCheck = az resource show `
        --resource-group $ResourceGroupName `
        --namespace Microsoft.HealthcareApis `
        --parent "workspaces/$workspaceName" `
        --resource-type fhirservices `
        --name $fhirServiceName `
        --query "properties.provisioningState" -o tsv 2>$null

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

    $bicepParams = @("--parameters", "adminGroupObjectId=$adminGroupObjectId", "--parameters", "location=$Location", "--parameters", $tagsParamRef)
    if ($SkipFhir) {
        $bicepParams += @("--parameters", "deployFhirService=false")
    }

    $fhirInfra = Invoke-ArmGroupDeployment `
        -ResourceGroup $ResourceGroupName `
        -DeploymentName "fhir-infra" `
        -TemplateFile "bicep/fhir-infra.bicep" `
        -ParameterArgs $bicepParams `
        -Query "properties.outputs" `
        -OnlyShowErrors

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
            $fhirInfra = Invoke-ArmGroupDeployment `
                -ResourceGroup $ResourceGroupName `
                -DeploymentName "fhir-infra" `
                -TemplateFile "bicep/fhir-infra.bicep" `
                -ParameterArgs $bicepParams `
                -Query "properties.outputs" `
                -OnlyShowErrors
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

Ensure-FhirStorageNetworkAccess -ResourceGroup $ResourceGroupName -StorageAccountName $storageAccountName

if ($InfraOnly) {
    Write-Host ""
    Write-Host "Infrastructure-only mode complete." -ForegroundColor Green
    Write-Host "Run with -RunSynthea to generate patients, or -RunLoader to load FHIR data."
    
    # Catch-up export (since Deploy-All.ps1 uses InfraOnly to trigger the export pipeline)
    Invoke-FhirExport -ResourceGroupName $ResourceGroupName -FhirServiceUrl $fhirServiceUrl -Label "FHIR `$export"
    Write-Host "  ✓ FHIR `$export completed successfully" -ForegroundColor Green
    
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
$containerName = "synthea-output"

# ============================================
# STEP 1.5: Reuse / Clone Patients Validation
# ============================================
if ($ReusePatients -or $SourceResourceGroup) {
    Write-Host ""
    Write-Host "--- STEP 1.5: REUSE PATIENTS / CLONE DATA ---" -ForegroundColor Cyan

    $syntheaBlobs = $null
    $dicomBlobs = $null

    if ($SourceResourceGroup) {
        Write-Host "Cloning data from source resource group: $SourceResourceGroup..." -ForegroundColor Cyan
        $sourceStorageName = az storage account list --resource-group $SourceResourceGroup --query "[?starts_with(name, 'stfhir')].name" -o tsv 2>$null | Select-Object -First 1
        
        if ($sourceStorageName) {
            Write-Host "  Source storage account found: $sourceStorageName. Copying blobs to $storageAccountName..." -ForegroundColor DarkGray
            
            Write-Host "  Copying synthea-output container..." -ForegroundColor DarkGray
            az storage blob copy start-batch --account-name $storageAccountName --destination-container $containerName --source-account-name $sourceStorageName --source-container $containerName --auth-mode login 2>$null | Out-Null
            
            Write-Host "  Copying dicom-output container..." -ForegroundColor DarkGray
            az storage blob copy start-batch --account-name $storageAccountName --destination-container "dicom-output" --source-account-name $sourceStorageName --source-container "dicom-output" --auth-mode login 2>$null | Out-Null
            
            Write-Host "  Copy operation started. Waiting a few seconds..." -ForegroundColor DarkGray
            Start-Sleep -Seconds 10
        } else {
            Write-Host "  ⚠ Source Resource Group not found or has no FHIR storage account. Continuing..." -ForegroundColor Yellow
        }
    }

    Write-Host "Validating target storage account data..." -ForegroundColor DarkGray
    $syntheaBlobs = az storage blob list --account-name $storageAccountName --container-name $containerName --num-results 1 --auth-mode login --query "[0].name" -o tsv 2>$null
    $dicomBlobs = az storage blob list --account-name $storageAccountName --container-name "dicom-output" --num-results 1 --auth-mode login --query "[0].name" -o tsv 2>$null

    if (-not $syntheaBlobs) {
        Write-Host "  ⚠ Warning: Reuse patients requested, but synthea-output is empty!" -ForegroundColor Yellow
        Write-Host "    Falling back to data generation..." -ForegroundColor DarkGray
        $doSynthea = $true
        $doLoader = $true
        $UseCachedSynthea = $false
        $ReusePatients = $false
    } else {
        Write-Host "  ✓ Verified: synthea data exists" -ForegroundColor Green
        if ($ReusePatients) {
            $doSynthea = $false
            if (-not $SourceResourceGroup) {
                $doLoader = $false
            }
            $UseCachedSynthea = $false
        }
    }

    if (-not $dicomBlobs) {
        Write-Host "  ⚠ Warning: Reuse patients requested, but dicom-output is empty!" -ForegroundColor Yellow
        Write-Host "    Falling back to TCIA download..." -ForegroundColor DarkGray
        $doDicom = $true
        $SkipDicom = $false
        $ReusePatients = $false
    } else {
        Write-Host "  ✓ Verified: dicom data exists" -ForegroundColor Green
        if ($ReusePatients -and -not $SourceResourceGroup) {
            $doDicom = $false
        }
    }
}

Write-Host "Using existing ACR: $acrName"

# ============================================
# STEP 2: Build Synthea Container
# ============================================
if ($doSynthea -and -not $UseCachedSynthea) {
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
        Invoke-AcrBuildWithTagVerification -Registry $acrName -Repository "synthea-generator" -Tag "v1" -ContextPath "." -ErrorLog $acrBuildErrLog -NoLogs
    } finally {
        Pop-Location
    }
    Write-Host "Synthea container built successfully" -ForegroundColor Green
}

if ($doSynthea -and $UseCachedSynthea) {
    Write-Host ""
    Write-Host "--- STEP 2: SYNTHEA CONTAINER IMAGE ---" -ForegroundColor Cyan
    Write-Host "  [Bypass] UseCachedSynthea specified. Skipping ACR build of Synthea image." -ForegroundColor Green
}

# ============================================
# STEP 3: Run Synthea Job
# ============================================
Write-Host ""
Write-Host "--- STEP 3: RUNNING SYNTHEA GENERATOR ---" -ForegroundColor Cyan

if ($UseCachedSynthea) {
    Write-Host "Uploading pre-generated Synthea patient bundles..." -ForegroundColor Cyan
    
    # Clear existing blobs so only the new batch is loaded
    Write-Host "Clearing previous Synthea output from blob storage if it exists..." -ForegroundColor DarkGray
    az storage blob delete-batch --account-name $storageAccountName --source $containerName --auth-mode login --pattern "*.json" 2>$null | Out-Null
    Write-Host "  Previous files cleared" -ForegroundColor DarkGray
    
    $localPrepackagedPath = Join-Path $ScriptDir "synthea/prepackaged"
    Write-Host "  Uploading prepackaged patient bundles from $localPrepackagedPath to $containerName container..." -ForegroundColor Cyan
    $uploadResult = az storage blob upload-batch --account-name $storageAccountName --destination $containerName --source $localPrepackagedPath --auth-mode login 2>&1
    $uploadExit = $LASTEXITCODE
    if ($uploadExit -ne 0) {
        Write-Host "ERROR uploading prepackaged bundles: $uploadResult" -ForegroundColor Red
        exit 1
    }
    Write-Host "  ✓ Prepackaged bundles uploaded successfully!" -ForegroundColor Green
} else {
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

    $null = Invoke-ArmGroupDeployment `
        -ResourceGroup $ResourceGroupName `
        -DeploymentName "synthea-job" `
        -TemplateFile "bicep/synthea-job.bicep" `
        -ParameterArgs @(
            "--parameters", "acrName=$acrName",
            "imageName=$syntheaImage",
            "storageAccountName=$storageAccountName",
            "containerName=$containerName",
            "patientCount=$PatientCount",
            "aciIdentityId=$aciIdentityId",
            "aciIdentityClientId=$aciIdentityClientId",
            "--parameters", $tagsParamRef
        )

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR deploying Synthea job" -ForegroundColor Red
        exit 1
    }

    # Wait for Synthea job to complete with live log streaming
    Write-Host "Waiting for Synthea generation to complete..."
    Write-Host ""
    Write-Host ""
    $maxWaitMinutes = 60
    $waitedMinutes = 0
    $lastLogLines = 0

    while ($waitedMinutes -lt $maxWaitMinutes) {
        $state = Get-AciContainerState -ResourceGroup $ResourceGroupName -Name "synthea-generator-job"
        
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
}

    # Verify Synthea blobs actually landed in synthea-output container
    Write-Host ""
    Write-Host "  Verifying Synthea blobs in storage..." -ForegroundColor DarkGray
    try {
        $syntheaBlobCountRaw = az storage blob list --container-name $containerName `
            --account-name $storageAccountName --auth-mode login `
            --query "length(@)" -o tsv 2>$null
        $syntheaBlobCount = ConvertTo-StrictIntFromCliOutput -Value $syntheaBlobCountRaw -Label "Synthea blob count"
        if ($syntheaBlobCount -gt 0) {
            Write-Host "  ✓ Verified: $syntheaBlobCount Synthea blobs in '$containerName' container" -ForegroundColor Green
        } else {
            Write-Host "  ⚠ Synthea reported success but NO blobs found in '$containerName'!" -ForegroundColor Red
            Write-Host "    Storage account: $storageAccountName" -ForegroundColor DarkGray
            Write-Host "    The FHIR Loader will have no data to process." -ForegroundColor DarkGray
            exit 1
        }
    } catch {
        Write-Host "  ✗ Could not verify Synthea blobs: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
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
        Invoke-AcrBuildWithTagVerification -Registry $acrName -Repository "fhir-loader" -Tag "v1" -ContextPath "." -ErrorLog $acrBuildErrLog -NoLogs
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

$null = Invoke-ArmGroupDeployment `
    -ResourceGroup $ResourceGroupName `
    -DeploymentName "fhir-loader-job" `
    -TemplateFile "bicep/fhir-loader-job.bicep" `
    -ParameterArgs @(
        "--parameters", "acrName=$acrName",
        "imageName=$loaderImage",
        "storageAccountName=$storageAccountName",
        "containerName=$containerName",
        "fhirServiceUrl=$fhirServiceUrl",
        "aciIdentityId=$aciIdentityId",
        "aciIdentityClientId=$aciIdentityClientId",
        "--parameters", $tagsParamRef
    )

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
    $state = Get-AciContainerState -ResourceGroup $ResourceGroupName -Name "fhir-loader-job"
    
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
            throw "POST-LOADER FHIR `$export did not complete"
        }
    } else {
        throw "Skipping POST-LOADER `$export — FHIR URL not available"
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
    $blobCheckRaw = az storage blob show --account-name $storageAccountName --container-name synthea-output --name "device-associations.json" --auth-mode login --query "properties.contentLength" -o tsv 2>$null
    $blobCheck = ConvertTo-StrictIntFromCliOutput -Value $blobCheckRaw -Label "device-associations.json content length"
    if ($blobCheck -gt 10) {
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
        Invoke-AcrBuildWithTagVerification -Registry $acrName -Repository "dicom-loader" -Tag "v1" -ContextPath "." -ErrorLog $acrBuildErrLog -NoLogs
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

$null = Invoke-ArmGroupDeployment `
    -ResourceGroup $ResourceGroupName `
    -DeploymentName "dicom-loader-job" `
    -TemplateFile "bicep/dicom-loader-job.bicep" `
    -ParameterArgs @(
        "--parameters", "acrName=$acrName",
        "imageName=$dicomImage",
        "storageAccountName=$storageAccountName",
        "fhirServiceUrl=$fhirServiceUrl",
        "aciIdentityId=$aciIdentityId",
        "aciIdentityClientId=$aciIdentityClientId",
        "--parameters", $tagsParamRef
    )

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
    $state = Get-AciContainerState -ResourceGroup $ResourceGroupName -Name "dicom-loader-job"

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
        $dicomBlobCountRaw = az storage blob list --container-name "dicom-output" `
            --account-name $storageAccountName --auth-mode login `
            --query "length(@)" -o tsv 2>$null
        $dicomBlobCount = ConvertTo-StrictIntFromCliOutput -Value $dicomBlobCountRaw -Label "DICOM blob count" -EmptyAsZero
        if ($dicomBlobCount -gt 0) {
            Write-Host "  ✓ Verified: $dicomBlobCount DICOM blobs in 'dicom-output' container" -ForegroundColor Green
        } else {
            # Try filesystem API for HNS-enabled accounts
            $dicomFilesRaw = az storage fs file list --file-system "dicom-output" `
                --account-name $storageAccountName --auth-mode login `
                --query "length(@)" -o tsv 2>$null
            $dicomFiles = ConvertTo-StrictIntFromCliOutput -Value $dicomFilesRaw -Label "DICOM filesystem file count" -EmptyAsZero
            if ($dicomFiles -gt 0) {
                Write-Host "  ✓ Verified: $dicomFiles DICOM files in 'dicom-output' filesystem" -ForegroundColor Green
            } else {
                Write-Host "  ⚠ DICOM Loader reported success but NO blobs found in 'dicom-output'!" -ForegroundColor Red
                Write-Host "    Storage account: $storageAccountName" -ForegroundColor DarkGray
                Write-Host "    The DICOM loader may not have uploaded studies to storage." -ForegroundColor DarkGray
                exit 1
            }
        }
    } catch {
        Write-Host "  ✗ Could not verify DICOM blobs: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

    # FHIR $export after DICOM — now includes ImagingStudy resources in addition to clinical data
    if ($fhirServiceUrl) {
        Write-Host "  Triggering POST-DICOM FHIR `$export..." -ForegroundColor White
        $postDicomExportResult = Invoke-FhirExport -ResourceGroupName $ResourceGroupName `
            -FhirServiceUrl $fhirServiceUrl `
            -Label "POST-DICOM: FHIR `$export (clinical + imaging data)"
        if (-not $postDicomExportResult) {
            throw "POST-DICOM FHIR `$export did not complete"
        }
    } else {
        throw "Skipping POST-DICOM `$export — FHIR URL not available"
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
    throw "FHIR Service URL not available — cannot run required `$export"
} else {
    Write-Host "  FHIR URL: $fhirServiceUrl" -ForegroundColor DarkGray
    # Check if any export data already exists
    $exportNeeded = $true
    $stAcct = $null
    try {
        Use-DeploymentAzSubscription
        $stAcct = az storage account list -g $ResourceGroupName `
            --query "[?kind=='StorageV2'].name | [0]" -o tsv 2>$null
        Write-Host "  Storage account: $stAcct" -ForegroundColor DarkGray
        if ($stAcct) {
            $existingExport = az storage blob list --container-name "fhir-export" `
                --account-name $stAcct --auth-mode login --num-results 1 `
                --query "[0].name" -o tsv 2>$null
            if ($existingExport -and -not $ReusePatients) {
                Write-Host "  ✓ FHIR export data already exists in 'fhir-export' container — skipping" -ForegroundColor Green
                $exportNeeded = $false
            } elseif ($existingExport) {
                Write-Host "  FHIR export data exists, but -ReusePatients specified — forcing new export to ensure sync" -ForegroundColor Yellow
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
            throw "FHIR `$export may not have completed — check storage"
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


