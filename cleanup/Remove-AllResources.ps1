<#
.SYNOPSIS
    Destroys all Azure resources and Fabric workspace items for the Medical Device FHIR Integration Platform.

.DESCRIPTION
    This script performs a complete teardown:
    1. Deletes the Azure resource group (async — FHIR, storage, ACR, ACI, Event Hub, Key Vault, managed identity)
    2. Deletes ALL Fabric workspace items (Eventhouse, Eventstream, KQL DB, dashboards, agents, lakehouses,
       HDS solution, notebooks, pipelines, environment, ontology, connections)
    3. Optionally deletes the Fabric workspace itself (-DeleteWorkspace)

    Items are deleted in dependency order to avoid conflicts:
    Agents/Dashboards → Streams → Eventhouse → Notebooks → Pipelines → Lakehouses → HDS → Environment

.PARAMETER ResourceGroupName
    Azure resource group to delete. Default: "rg-medtech-rti-fhir"

.PARAMETER FabricWorkspaceName
    Fabric workspace to clean. Default: "med-device-rti-hds"

.PARAMETER SkipAzure
    Skip Azure resource group deletion (Fabric-only cleanup).

.PARAMETER SkipFabric
    Skip Fabric workspace cleanup (Azure-only teardown).

.PARAMETER Force
    Skip confirmation prompts.

.PARAMETER Wait
    Block until Azure RG deletion completes.

.PARAMETER DeleteWorkspace
    Also delete the Fabric workspace itself (not just its items).

.EXAMPLE
    .\cleanup\Remove-AllResources.ps1 -Force
    .\cleanup\Remove-AllResources.ps1 -SkipAzure -Force     # Fabric only
    .\cleanup\Remove-AllResources.ps1 -SkipFabric -Force     # Azure only
    .\cleanup\Remove-AllResources.ps1 -Force -Wait           # Block until Azure RG is gone
    .\cleanup\Remove-AllResources.ps1 -Force -DeleteWorkspace # Also delete the workspace
#>

param(
    [string]$ResourceGroupName = "rg-medtech-rti-fhir",
    [string]$FabricWorkspaceName = "med-device-rti-hds",
    [switch]$SkipAzure,
    [switch]$SkipFabric,
    [switch]$Force,
    [switch]$Wait,
    [switch]$DeleteWorkspace
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host "|          FULL TEARDOWN — ALL RESOURCES                     |" -ForegroundColor Red
Write-Host "+============================================================+" -ForegroundColor Red
Write-Host ""
Write-Host "  Azure Resource Group: $ResourceGroupName" -ForegroundColor White
Write-Host "  Fabric Workspace:    $FabricWorkspaceName" -ForegroundColor White
Write-Host "  Skip Azure:          $SkipAzure" -ForegroundColor White
Write-Host "  Skip Fabric:         $SkipFabric" -ForegroundColor White
Write-Host "  Delete Workspace:    $DeleteWorkspace" -ForegroundColor White
Write-Host ""

if (-not $Force) {
    Write-Host "  THIS WILL PERMANENTLY DELETE:" -ForegroundColor Yellow
    if (-not $SkipAzure) {
        Write-Host "    - All Azure resources in '$ResourceGroupName'" -ForegroundColor Yellow
    }
    if (-not $SkipFabric) {
        Write-Host "    - ALL items in Fabric workspace '$FabricWorkspaceName'" -ForegroundColor Yellow
        Write-Host "      (Eventhouse, Eventstream, dashboards, agents, lakehouses," -ForegroundColor Yellow
        Write-Host "       HDS solution, notebooks, pipelines, environment, etc.)" -ForegroundColor Yellow
    }
    if ($DeleteWorkspace) {
        Write-Host "    - The Fabric workspace '$FabricWorkspaceName' ITSELF" -ForegroundColor Red
    }
    Write-Host ""
    $confirm = Read-Host "  Type 'yes' to confirm"
    if ($confirm -ne 'yes') {
        Write-Host "  Aborted." -ForegroundColor Yellow
        exit 0
    }
    Write-Host ""
}

$overallTimer = [System.Diagnostics.Stopwatch]::StartNew()

# ═══════════════════════════════════════════════════════════════════════
# STEP 1: DELETE AZURE RESOURCE GROUP
# ═══════════════════════════════════════════════════════════════════════

if (-not $SkipAzure) {
    Write-Host "─── Step 1: Azure Resource Group ───" -ForegroundColor Cyan

    $rgExists = az group exists -n $ResourceGroupName 2>$null
    if ($rgExists -eq 'true') {
        Write-Host "  Deleting resource group '$ResourceGroupName'..." -ForegroundColor Yellow
        az group delete --name $ResourceGroupName --yes --no-wait 2>&1 | Out-Null
        Write-Host "  Azure RG deletion initiated (async)." -ForegroundColor Green

        if ($Wait) {
            Write-Host "  Waiting for Azure RG deletion to complete..." -ForegroundColor Yellow
            while ($true) {
                $exists = az group exists -n $ResourceGroupName 2>$null
                if ($exists -eq 'false') {
                    Write-Host "  Azure RG deleted." -ForegroundColor Green
                    break
                }
                Write-Host "    Still deleting..." -ForegroundColor DarkGray
                Start-Sleep -Seconds 30
            }
        }
    } else {
        Write-Host "  Resource group '$ResourceGroupName' does not exist. Skipping." -ForegroundColor DarkGray
    }
} else {
    Write-Host "─── Step 1: SKIPPED (Azure) ───" -ForegroundColor DarkGray
}

# ═══════════════════════════════════════════════════════════════════════
# STEP 2: DELETE ALL FABRIC WORKSPACE ITEMS
# ═══════════════════════════════════════════════════════════════════════

if (-not $SkipFabric) {
    Write-Host ""
    Write-Host "─── Step 2: Fabric Workspace Items ───" -ForegroundColor Cyan

    $token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
    $headers = @{ Authorization = "Bearer $token" }

    # Resolve workspace ID
    $workspaces = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $headers).value
    $ws = $workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName } | Select-Object -First 1
    if (-not $ws) {
        Write-Host "  Workspace '$FabricWorkspaceName' not found. Skipping." -ForegroundColor Yellow
    } else {
        $wsId = $ws.id
        Write-Host "  Workspace: $FabricWorkspaceName ($wsId)" -ForegroundColor White

        $items = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items" -Headers $headers).value
        Write-Host "  Items to delete: $($items.Count)" -ForegroundColor White

        if ($items.Count -gt 0) {
            # Delete in dependency order to minimize conflicts
            $deleteOrder = @(
                'DataAgent', 'OperationsAgent',
                'KQLDashboard', 'Ontology', 'GraphModel', 'Reflex', 'AnomalyDetector',
                'Eventstream',
                'KQLDatabase', 'Eventhouse',
                'SemanticModel',
                'Notebook',
                'DataPipeline',
                'Lakehouse',
                'Healthcaredatasolution',
                'Environment',
                'SQLEndpoint'
            )

            $deleted = 0
            $failed = 0

            # First pass: ordered types
            foreach ($type in $deleteOrder) {
                $typeItems = $items | Where-Object { $_.type -eq $type }
                foreach ($item in $typeItems) {
                    Write-Host "  DEL $($item.type.PadRight(25)) $($item.displayName)..." -NoNewline
                    try {
                        Invoke-RestMethod -Method Delete `
                            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$($item.id)" `
                            -Headers $headers -ErrorAction Stop | Out-Null
                        Write-Host " OK" -ForegroundColor Green
                        $deleted++
                    } catch {
                        Write-Host " SKIP" -ForegroundColor DarkGray
                        $failed++
                    }
                    Start-Sleep -Milliseconds 300
                }
            }

            # Second pass: catch any types not in the ordered list
            $items2 = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items" -Headers $headers).value
            foreach ($item in $items2) {
                Write-Host "  DEL $($item.type.PadRight(25)) $($item.displayName) (retry)..." -NoNewline
                try {
                    Invoke-RestMethod -Method Delete `
                        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$($item.id)" `
                        -Headers $headers -ErrorAction Stop | Out-Null
                    Write-Host " OK" -ForegroundColor Green
                    $deleted++
                } catch {
                    Write-Host " FAIL" -ForegroundColor Red
                }
                Start-Sleep -Milliseconds 300
            }

            Write-Host ""
            Write-Host "  Deleted: $deleted items" -ForegroundColor Green
        } else {
            Write-Host "  Workspace is already empty." -ForegroundColor DarkGray
        }

        # Delete Fabric connections created by the deployment scripts
        Write-Host ""
        Write-Host "  Cleaning up Fabric connections..." -ForegroundColor White
        $connections = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/connections" -Headers $headers).value
        $matchPatterns = @('masimo', 'fhir', 'dicom', 'stfhir', 'EventHub', 'telemetry', 'fab-')
        $connToDelete = $connections | Where-Object {
            $name = $_.displayName
            $matchPatterns | Where-Object { $name -match $_ }
        }

        if ($connToDelete) {
            foreach ($conn in $connToDelete) {
                Write-Host "  DEL Connection: $($conn.displayName)..." -NoNewline
                try {
                    Invoke-RestMethod -Method Delete `
                        -Uri "https://api.fabric.microsoft.com/v1/connections/$($conn.id)" `
                        -Headers $headers -ErrorAction Stop | Out-Null
                    Write-Host " OK" -ForegroundColor Green
                } catch {
                    Write-Host " SKIP" -ForegroundColor DarkGray
                }
            }
        } else {
            Write-Host "  No matching connections found." -ForegroundColor DarkGray
        }

        # Final verification
        Write-Host ""
        $finalItems = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items" -Headers $headers).value
        Write-Host "  Items remaining: $($finalItems.Count)" -ForegroundColor $(if ($finalItems.Count -eq 0) { 'Green' } else { 'Yellow' })
        foreach ($item in $finalItems) {
            Write-Host "    $($item.type): $($item.displayName)" -ForegroundColor DarkGray
        }
    }
} else {
    Write-Host ""
    Write-Host "─── Step 2: SKIPPED (Fabric) ───" -ForegroundColor DarkGray
}

# ═══════════════════════════════════════════════════════════════════════
# STEP 2b: DEPROVISION WORKSPACE IDENTITY
# ═══════════════════════════════════════════════════════════════════════

if (-not $SkipFabric) {
    Write-Host ""
    Write-Host "─── Step 2b: Deprovision Workspace Identity ───" -ForegroundColor Cyan

    if (-not $wsId) {
        $token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
        if ($token -is [System.Security.SecureString]) { $token = $token | ConvertFrom-SecureString -AsPlainText }
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        $wsResp = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $headers
        $ws = $wsResp.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
        $wsId = $ws.id
    }

    if ($wsId) {
        try {
            Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/deprovisionIdentity" `
                -Headers $headers -Method POST | Out-Null
            Write-Host "  ✓ Workspace identity deprovisioned." -ForegroundColor Green
        } catch {
            $sc = [int]$_.Exception.Response.StatusCode
            if ($sc -eq 404 -or $_.Exception.Message -match 'not found|does not exist|not provisioned') {
                Write-Host "  No workspace identity to deprovision." -ForegroundColor DarkGray
            } else {
                Write-Host "  ⚠ Could not deprovision workspace identity: $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }

        # Delete ALL Entra ID app registrations matching the workspace name (handles stale SPs from previous deployments)
        Write-Host "  Checking for Entra app registrations matching '$FabricWorkspaceName'..." -ForegroundColor Gray
        try {
            $allApps = az ad app list --display-name $FabricWorkspaceName --query "[].{id:id, appId:appId}" -o json 2>$null | ConvertFrom-Json
            if ($allApps -and $allApps.Count -gt 0) {
                Write-Host "  Found $($allApps.Count) app registration(s) to delete" -ForegroundColor Gray
                foreach ($app in $allApps) {
                    try {
                        az ad app delete --id $app.id 2>$null | Out-Null
                        Write-Host "  ✓ Deleted app registration: $($app.id) (appId: $($app.appId))" -ForegroundColor Green
                    } catch {
                        Write-Host "  ⚠ Could not delete app $($app.id): $($_.Exception.Message)" -ForegroundColor Yellow
                    }
                }
            } else {
                Write-Host "  No matching app registrations found." -ForegroundColor DarkGray
            }
        } catch {
            Write-Host "  ⚠ Could not list app registrations: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  Workspace not found — skipping identity deprovision." -ForegroundColor DarkGray
    }
}

# ═══════════════════════════════════════════════════════════════════════
# STEP 3: DELETE FABRIC WORKSPACE (optional)
# ═══════════════════════════════════════════════════════════════════════

if ($DeleteWorkspace -and -not $SkipFabric) {
    Write-Host ""
    Write-Host "─── Step 3: Delete Fabric Workspace ───" -ForegroundColor Cyan

    if (-not $wsId) {
        # Resolve workspace ID if we skipped Step 2
        $token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
        if ($token -is [System.Security.SecureString]) { $token = $token | ConvertFrom-SecureString -AsPlainText }
        $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
        $wsResp = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $headers
        $ws = $wsResp.value | Where-Object { $_.displayName -eq $FabricWorkspaceName }
        $wsId = $ws.id
    }

    if ($wsId) {
        Write-Host "  Deleting workspace '$FabricWorkspaceName' ($wsId)..." -ForegroundColor Yellow
        try {
            Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId" `
                -Headers $headers -Method Delete | Out-Null
            Write-Host "  ✓ Workspace deleted." -ForegroundColor Green
        } catch {
            $sc = [int]$_.Exception.Response.StatusCode
            if ($sc -eq 404) {
                Write-Host "  Workspace already deleted (404)." -ForegroundColor DarkGray
            } else {
                Write-Host "  ✗ Failed to delete workspace: $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "  Workspace '$FabricWorkspaceName' not found — nothing to delete." -ForegroundColor DarkGray
    }
}

# ═══════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════

$overallTimer.Stop()
$totalMin = [math]::Round($overallTimer.Elapsed.TotalMinutes, 1)

Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Green
Write-Host "|          TEARDOWN COMPLETE                                 |" -ForegroundColor Green
Write-Host "+============================================================+" -ForegroundColor Green
Write-Host "  Duration: $totalMin min" -ForegroundColor White
if (-not $SkipAzure) {
    $rgState = az group exists -n $ResourceGroupName 2>$null
    Write-Host "  Azure RG '$ResourceGroupName': $(if ($rgState -eq 'true') { 'still deleting (async)' } else { 'DELETED' })" -ForegroundColor White
}
if (-not $SkipFabric) {
    if ($DeleteWorkspace) {
        Write-Host "  Fabric workspace '$FabricWorkspaceName': DELETED" -ForegroundColor White
    } else {
        Write-Host "  Fabric workspace '$FabricWorkspaceName': items cleared" -ForegroundColor White
    }
}
Write-Host ""
Write-Host "  To redeploy: .\Deploy-All.ps1 -FabricWorkspaceName '$FabricWorkspaceName'" -ForegroundColor DarkGray
Write-Host ""
