# Remove-FhirData.ps1
# Purges all data from the FHIR server using $bulk-delete without destroying infrastructure.
# Useful when you want to re-run the loader with fresh data but keep the FHIR service.
#
# Usage:
#   .\cleanup\Remove-FhirData.ps1 -FhirUrl "https://hdws...-fhir....fhir.azurehealthcareapis.com"
#   .\cleanup\Remove-FhirData.ps1 -ResourceGroupName "rg-medtech-sys-identity"
#   .\cleanup\Remove-FhirData.ps1 -ResourceGroupName "rg-medtech-sys-identity" -ResourceType "Location"

param(
    [string]$FhirUrl = "",
    [string]$ResourceGroupName = "",
    [string]$ResourceType = "",              # Optional: delete only this resource type
    [switch]$Force,                          # Skip confirmation prompt
    [switch]$Wait,                           # Poll until deletion completes
    [int]$PollIntervalSeconds = 10           # Polling interval when -Wait
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|            FHIR DATA CLEANUP                               |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host ""

# ── Resolve FHIR URL ──
if (-not $FhirUrl -and $ResourceGroupName) {
    Write-Host "Discovering FHIR URL from resource group '$ResourceGroupName'..." -ForegroundColor Cyan
    $hdsWorkspace = az resource list --resource-group $ResourceGroupName `
        --resource-type "Microsoft.HealthcareApis/workspaces" --query "[0].name" -o tsv 2>$null
    if (-not $hdsWorkspace) {
        Write-Host "ERROR: No Health Data Services workspace found in '$ResourceGroupName'" -ForegroundColor Red
        exit 1
    }
    $fhirService = az resource list --resource-group $ResourceGroupName `
        --resource-type "Microsoft.HealthcareApis/workspaces/fhirservices" --query "[0].name" -o tsv 2>$null
    if (-not $fhirService) {
        Write-Host "ERROR: No FHIR service found in '$ResourceGroupName'" -ForegroundColor Red
        exit 1
    }
    # fhirService name is "workspaceName/serviceName" — extract just the service name
    $serviceName = $fhirService.Split("/")[-1]
    $FhirUrl = "https://$hdsWorkspace-$serviceName.fhir.azurehealthcareapis.com"
    Write-Host "  Resolved: $FhirUrl" -ForegroundColor Cyan
} elseif (-not $FhirUrl) {
    Write-Host "ERROR: Provide either -FhirUrl or -ResourceGroupName" -ForegroundColor Red
    exit 1
}

# ── Get token ──
$token = az account get-access-token --resource $FhirUrl --query accessToken -o tsv
if (-not $token) {
    Write-Host "ERROR: Failed to get access token for $FhirUrl" -ForegroundColor Red
    exit 1
}

$headers = @{
    Authorization  = "Bearer $token"
    Accept         = "application/fhir+json"
    Prefer         = "respond-async"
}

# ── Show current counts ──
Write-Host ""
Write-Host "Current resource counts:" -ForegroundColor Yellow
$readHeaders = @{ Authorization = "Bearer $token"; Accept = "application/fhir+json" }
$types = @('Patient','Organization','Practitioner','Location','Encounter','Condition','Observation','Device','Basic')
foreach ($rt in $types) {
    try {
        $count = (Invoke-RestMethod -Uri "$FhirUrl/$rt`?_summary=count" -Headers $readHeaders).total
        Write-Host "  $($rt.PadRight(20)) $count" -ForegroundColor $(if ($count -gt 0) { "Cyan" } else { "DarkGray" })
    } catch {
        Write-Host "  $($rt.PadRight(20)) (error)" -ForegroundColor DarkGray
    }
}
Write-Host ""

# ── Build delete URL ──
if ($ResourceType) {
    $deleteUrl = "$FhirUrl/$ResourceType/`$bulk-delete"
    $scope = "all $ResourceType resources"
} else {
    $deleteUrl = "$FhirUrl/`$bulk-delete"
    $scope = "ALL resources"
}

# ── Confirmation ──
if (-not $Force) {
    $confirm = Read-Host "Delete $scope from FHIR server? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Aborted." -ForegroundColor Yellow
        exit 0
    }
}

# ── Execute bulk delete ──
Write-Host ""
Write-Host "Initiating bulk delete of $scope..." -ForegroundColor Yellow

$response = Invoke-WebRequest -Uri $deleteUrl -Method DELETE -Headers $headers
$statusCode = $response.StatusCode

if ($statusCode -eq 202) {
    $pollingUrl = $response.Headers['Content-Location']
    if ($pollingUrl -is [array]) { $pollingUrl = $pollingUrl[0] }
    Write-Host "  Accepted (202). Deletion in progress." -ForegroundColor Green
    
    if ($pollingUrl) {
        Write-Host "  Polling URL: $pollingUrl" -ForegroundColor DarkGray
        
        if ($Wait) {
            Write-Host ""
            Write-Host "Waiting for completion..." -ForegroundColor Cyan
            $pollHeaders = @{ Authorization = "Bearer $token" }
            
            while ($true) {
                Start-Sleep -Seconds $PollIntervalSeconds
                try {
                    $pollResponse = Invoke-WebRequest -Uri $pollingUrl -Headers $pollHeaders
                    if ($pollResponse.StatusCode -eq 200) {
                        Write-Host "  Bulk delete completed." -ForegroundColor Green
                        break
                    }
                    Write-Host "  Still deleting..." -ForegroundColor DarkGray
                } catch {
                    $pollStatus = $_.Exception.Response.StatusCode.value__
                    if ($pollStatus -eq 202) {
                        Write-Host "  Still deleting..." -ForegroundColor DarkGray
                    } else {
                        Write-Host "  Poll returned $pollStatus — assuming complete." -ForegroundColor Yellow
                        break
                    }
                }
            }
        } else {
            Write-Host ""
            Write-Host "Run with -Wait to block until completion, or poll manually:" -ForegroundColor DarkGray
            Write-Host "  Invoke-RestMethod -Uri `"$pollingUrl`" -Headers @{Authorization=`"Bearer `$token`"}" -ForegroundColor DarkGray
        }
    }
} else {
    Write-Host "  Unexpected status: $statusCode" -ForegroundColor Yellow
    Write-Host "  $($response.Content)" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
