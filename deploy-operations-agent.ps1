#!/usr/bin/env pwsh
# ============================================================================
# deploy-operations-agent.ps1
# Creates a Clinical Deterioration Monitor Operations Agent via Fabric REST API.
#
# This agent monitors real-time Masimo telemetry for sustained vital sign
# deterioration trends (not just single-threshold crossings) and recommends
# clinical escalation actions with patient context.
#
# The script:
#   1. Discovers the workspace and KQL database
#   2. Creates the Operations Agent item via REST API
#   3. Outputs portal configuration steps for goals/instructions/actions
#
# Prerequisites:
#   - az login completed
#   - Eventhouse with TelemetryRaw + AlertHistory tables
#   - Operations Agent preview enabled on Fabric tenant
#   - Copilot and Azure OpenAI Service enabled on tenant
#   - NOT on a trial capacity (Operations Agents require paid capacity)
#
# Usage:
#   .\deploy-operations-agent.ps1
#   .\deploy-operations-agent.ps1 -FabricWorkspaceName "my-workspace"
# ============================================================================

[CmdletBinding()]
param (
    [string]$FabricWorkspaceName = "med-device-rti-hds",
    [string]$AgentName           = "ClinicalDeteriorationMonitor",
    [string]$FabricApiBase       = "https://api.fabric.microsoft.com/v1"
)

$ErrorActionPreference = "Stop"

# ============================================================================
# AUTH HELPERS
# ============================================================================

function Get-FabricAccessToken {
    $tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
    $rawToken = $tokenObj.Token
    if ($rawToken -is [System.Security.SecureString]) {
        $bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($rawToken)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
    }
    elseif ($rawToken -is [string]) { return $rawToken }
    else { return $rawToken | ConvertFrom-SecureString -AsPlainText }
}

function ConvertTo-Base64 {
    param ([string]$Text)
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

# ============================================================================
# DISCOVER WORKSPACE + EVENTHOUSE
# ============================================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Operations Agent — Clinical Deterioration Monitor          ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# --- Workspace ---
Write-Host "  Discovering workspace..." -ForegroundColor White
$token = Get-FabricAccessToken
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$workspaces = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces" -Headers $headers).value
$ws = $workspaces | Where-Object { $_.displayName -eq $FabricWorkspaceName }
if (-not $ws) {
    Write-Host "ERROR: Workspace '$FabricWorkspaceName' not found." -ForegroundColor Red
    exit 1
}
$workspaceId = $ws.id
Write-Host "  ✓ Workspace: $FabricWorkspaceName ($workspaceId)" -ForegroundColor Green

# --- Eventhouse ---
$eventhouses = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/eventhouses" -Headers $headers).value
$eventhouse = $eventhouses | Where-Object { $_.displayName -match "Masimo" }
if (-not $eventhouse) { $eventhouse = $eventhouses | Select-Object -First 1 }
if (-not $eventhouse) {
    Write-Host "ERROR: Eventhouse not found. Operations Agent requires an Eventhouse." -ForegroundColor Red
    exit 1
}
if ($eventhouse -is [array]) { $eventhouse = $eventhouse[0] }
Write-Host "  ✓ Eventhouse: $($eventhouse.displayName) ($($eventhouse.id))" -ForegroundColor Green

# --- KQL Database ---
$kqlDbs = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/kqlDatabases" -Headers $headers).value
$kqlDb = $kqlDbs | Where-Object { $_.displayName -eq "MasimoKQLDB" -or $_.displayName -eq $eventhouse.displayName }
if (-not $kqlDb) { $kqlDb = $kqlDbs | Select-Object -First 1 }
if ($kqlDb -is [array]) { $kqlDb = $kqlDb[0] }
Write-Host "  ✓ KQL Database: $($kqlDb.displayName) ($($kqlDb.id))" -ForegroundColor Green

# --- Check existing ---
Write-Host ""
Write-Host "  Checking for existing operations agent..." -ForegroundColor White
$existingAgents = $null
try {
    $existingAgents = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/OperationsAgents" -Headers $headers).value
} catch {}
$existing = $existingAgents | Where-Object { $_.displayName -eq $AgentName }
if ($existing) {
    Write-Host "  ✓ Operations Agent '$AgentName' already exists ($($existing.id))." -ForegroundColor Yellow
    $agentId = $existing.id
} else {
    # ============================================================================
    # CREATE OPERATIONS AGENT
    # ============================================================================

    Write-Host ""
    Write-Host "  Creating Operations Agent '$AgentName'..." -ForegroundColor White

    $createBody = '{"displayName":"'+$AgentName+'","description":"Monitors Masimo telemetry for sustained SpO2/PR deterioration trends and recommends clinical escalation."}'

    try {
        $createResp = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/OperationsAgents" `
            -Headers $headers -Method POST -Body $createBody -ErrorAction Stop
        $createStatus = [int]$createResp.StatusCode

        if ($createStatus -eq 201) {
            $result = $createResp.Content | ConvertFrom-Json
            $agentId = $result.id
            Write-Host "  ✓ Created: $($result.displayName) ($agentId)" -ForegroundColor Green
        }
        elseif ($createStatus -eq 202) {
            $opId = $createResp.Headers["x-ms-operation-id"]
            if ($opId -is [array]) { $opId = $opId[0] }
            Write-Host "  Long-running operation ($opId), polling..." -ForegroundColor Gray
            for ($poll = 0; $poll -lt 60; $poll++) {
                Start-Sleep -Seconds 5
                $pH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
                $op = Invoke-RestMethod -Uri "$FabricApiBase/operations/$opId" -Headers $pH
                Write-Host "    Status: $($op.status)... ($($poll * 5)s)" -ForegroundColor DarkGray
                if ($op.status -eq "Succeeded") { break }
                if ($op.status -eq "Failed") {
                    $ed = if ($op.error) { $op.error.message } else { "Unknown" }
                    throw "Create failed: $ed"
                }
            }
            # Fetch agent ID
            Start-Sleep 3
            $agents = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/OperationsAgents" `
                -Headers @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }).value
            $created = $agents | Where-Object { $_.displayName -eq $AgentName }
            if ($created -is [array]) { $created = $created[0] }
            $agentId = $created.id
            Write-Host "  ✓ Created: $AgentName ($agentId)" -ForegroundColor Green
        }
    } catch {
        Write-Host "  ✗ Failed to create Operations Agent: $_" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Troubleshooting:" -ForegroundColor Yellow
        Write-Host "    - Operations Agent requires PAID capacity (not trial)" -ForegroundColor White
        Write-Host "    - Ensure 'Operations agent (preview)' is enabled in tenant admin" -ForegroundColor White
        Write-Host "    - Ensure 'Copilot and Azure OpenAI Service' is enabled" -ForegroundColor White
        exit 1
    }
}

# ============================================================================
# CREATE REFLEX (DATA ACTIVATOR) FOR AGENT ACTIONS
# ============================================================================

Write-Host ""
Write-Host "  Creating Data Activator (Reflex) for agent actions..." -ForegroundColor White

$reflexName = "DeteriorationEscalation"
$existingReflex = $null
try {
    $items = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
        -Headers @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }).value
    $existingReflex = $items | Where-Object { $_.displayName -eq $reflexName -and $_.type -eq "Reflex" }
} catch {}

if ($existingReflex) {
    if ($existingReflex -is [array]) { $existingReflex = $existingReflex[0] }
    $reflexId = $existingReflex.id
    Write-Host "  ✓ Reflex '$reflexName' already exists ($reflexId)" -ForegroundColor Yellow
} else {
    # Create empty Reflex (definition must be configured in portal due to Activator ALM requirements)
    $reflexBody = '{"displayName":"'+$reflexName+'","description":"Data Activator for Clinical Deterioration Monitor - connect to Operations Agent actions.","type":"Reflex"}'

    try {
        $rToken = Get-FabricAccessToken
        $rHeaders = @{ "Authorization" = "Bearer $rToken"; "Content-Type" = "application/json" }
        $rResp = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
            -Headers $rHeaders -Method POST -Body $reflexBody -ErrorAction Stop
        $rStatus = [int]$rResp.StatusCode

        if ($rStatus -eq 201) {
            $rResult = $rResp.Content | ConvertFrom-Json
            $reflexId = $rResult.id
        } elseif ($rStatus -eq 202) {
            $rOpId = $rResp.Headers["x-ms-operation-id"]
            if ($rOpId -is [array]) { $rOpId = $rOpId[0] }
            Write-Host "  Provisioning..." -ForegroundColor Gray
            for ($poll = 0; $poll -lt 30; $poll++) {
                Start-Sleep -Seconds 5
                $pH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
                $op = Invoke-RestMethod -Uri "$FabricApiBase/operations/$rOpId" -Headers $pH
                if ($op.status -ne "Running") { break }
            }
            Start-Sleep 3
            $items2 = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
                -Headers @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }).value
            $reflex = $items2 | Where-Object { $_.displayName -eq $reflexName -and $_.type -eq "Reflex" }
            if ($reflex -is [array]) { $reflex = $reflex[0] }
            $reflexId = $reflex.id
        }
        Write-Host "  ✓ Reflex created: $reflexName ($reflexId)" -ForegroundColor Green
    } catch {
        $errMsg = $_.Exception.Message
        try { $errMsg = ($_.ErrorDetails.Message | ConvertFrom-Json).message } catch {}
        if ($errMsg -match "NotAvailableYet") {
            Write-Host "  ⚠ Name not available yet (recent delete). Waiting..." -ForegroundColor Yellow
            Start-Sleep 45
            try {
                $rResp2 = Invoke-WebRequest -Uri "$FabricApiBase/workspaces/$workspaceId/items" `
                    -Headers @{ "Authorization" = "Bearer $(Get-FabricAccessToken)"; "Content-Type" = "application/json" } `
                    -Method POST -Body $reflexBody -ErrorAction Stop
                $rResult2 = $rResp2.Content | ConvertFrom-Json
                $reflexId = $rResult2.id
                Write-Host "  ✓ Reflex created on retry: $reflexName ($reflexId)" -ForegroundColor Green
            } catch {
                Write-Host "  ⚠ Could not create Reflex: $_" -ForegroundColor Yellow
                $reflexId = $null
            }
        } else {
            Write-Host "  ⚠ Could not create Reflex: $errMsg" -ForegroundColor Yellow
            $reflexId = $null
        }
    }
}

# ============================================================================
# PUSH DEFINITION (goals, instructions, data source, actions)
# ============================================================================

Write-Host ""
Write-Host "  Pushing configuration (goals, instructions, data source, actions)..." -ForegroundColor White

$goalsText = "Detect sustained clinical deterioration in remotely monitored patients by identifying downward SpO2 trends and abnormal pulse rate patterns over sliding time windows. Notify the care team before patients cross critical alert thresholds, enabling proactive intervention rather than reactive alerting."

$instructionsText = @"
You are a clinical deterioration detection agent monitoring 100 Masimo Radius-7 pulse oximeters streaming real-time telemetry to the MasimoKQLDB Eventhouse.

DATA SOURCES:
- TelemetryRaw: Real-time vital signs (device_id, timestamp, telemetry.spo2, telemetry.pr, telemetry.pi, telemetry.pvi, telemetry.sphb, telemetry.signal_iq)
- AlertHistory: Historical triggered alerts (alert_id, alert_time, device_id, patient_id, patient_name, alert_tier, alert_type, metric_name, metric_value)

DETERIORATION DETECTION RULES:
1. SpO2 TREND: Flag when a device's average SpO2 drops by >2% over a 15-minute sliding window compared to its 1-hour baseline (e.g., baseline 97% -> current 15min avg 94.5%)
2. PR INSTABILITY: Flag when pulse rate standard deviation exceeds 15 bpm over a 10-minute window (indicates hemodynamic instability)
3. MULTI-METRIC: Flag when BOTH SpO2 is trending down AND PR is trending up simultaneously over 10 minutes (classic deterioration pattern)
4. SIGNAL QUALITY: Ignore readings where signal_iq < 70 (unreliable data from poor sensor placement)

SEVERITY CLASSIFICATION:
- WATCH: SpO2 drop 1-2% from baseline, OR PR stddev 10-15 bpm
- CONCERN: SpO2 drop 2-4% from baseline, OR PR stddev 15-25 bpm
- ESCALATE: SpO2 drop >4% from baseline, OR PR stddev >25 bpm, OR multi-metric pattern detected

IMPORTANT CONTEXT:
- The timestamp column in TelemetryRaw is STRING type. Always wrap with todatetime(timestamp).
- Device IDs follow the pattern MASIMO-RADIUS7-NNNN (e.g., MASIMO-RADIUS7-0001).
- High-risk conditions (COPD SNOMED 13645005, CHF 84114007) should lower the ESCALATE threshold by 1% for SpO2.
- If AlertHistory shows recent CRITICAL/URGENT alerts for a device, prioritize that device's trend analysis.

KQL PATTERNS:
- 15-min window: | where todatetime(timestamp) > ago(15m)
- Baseline (1h): | where todatetime(timestamp) between(ago(1h) .. ago(15m))
- SpO2 trend: | summarize avg_spo2=avg(todouble(telemetry.spo2)) by device_id, bin(todatetime(timestamp), 5m)
- PR variability: | summarize pr_stddev=stdev(todouble(telemetry.pr)) by device_id

OUTPUT FORMAT:
When recommending an action, include: device_id, severity (WATCH/CONCERN/ESCALATE), current SpO2 avg, baseline SpO2 avg, delta, PR trend, and recommended clinical action.
"@

# Escape the instructions for JSON embedding (newlines, quotes)
$instrEscaped = $instructionsText -replace '\\', '\\\\' -replace '"', '\"' -replace "`r`n", '\n' -replace "`n", '\n' -replace "`t", '\t'
$goalsEscaped = $goalsText -replace '"', '\"'

# Build action IDs
$action1Id = [guid]::NewGuid().ToString()
$action2Id = [guid]::NewGuid().ToString()

# Build the Configurations.json content
$configJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/operationsAgents/definition/1.0.0/schema.json","configuration":{"goals":"'+$goalsEscaped+'","instructions":"'+$instrEscaped+'","dataSources":{"kqldb1":{"id":"'+$($kqlDb.id)+'","type":"KustoDatabase","workspaceId":"'+$workspaceId+'"}},"actions":{"escalate":{"id":"'+$action1Id+'","displayName":"Escalate to Care Team","description":"Send an urgent notification to the clinical care team when a patient shows sustained deterioration requiring immediate assessment.","kind":"PowerAutomateAction","parameters":[{"name":"device_id","description":"The Masimo device identifier"},{"name":"patient_name","description":"Patient name"},{"name":"severity","description":"WATCH, CONCERN, or ESCALATE"},{"name":"spo2_current","description":"Current 15-min average SpO2"},{"name":"spo2_baseline","description":"1-hour baseline SpO2"},{"name":"clinical_summary","description":"Brief description of findings"}]},"logEvent":{"id":"'+$action2Id+'","displayName":"Log Deterioration Event","description":"Record a deterioration detection event for audit trail and trend analysis. Use for WATCH-level findings.","kind":"PowerAutomateAction","parameters":[{"name":"device_id","description":"The Masimo device identifier"},{"name":"severity","description":"WATCH, CONCERN, or ESCALATE"},{"name":"details","description":"Full analysis details"}]}}},"shouldRun":false}'

$platformJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json","metadata":{"type":"OperationsAgent","displayName":"'+$AgentName+'","description":"Monitors Masimo telemetry for sustained SpO2/PR deterioration trends and recommends clinical escalation."},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'

# Build the update definition body
$updateBody = '{"definition":{"format":"OperationsAgentV1","parts":[{"path":"Configurations.json","payload":"'+(ConvertTo-Base64 $configJson)+'","payloadType":"InlineBase64"},{"path":".platform","payload":"'+(ConvertTo-Base64 $platformJson)+'","payloadType":"InlineBase64"}]}}'

$updateToken = Get-FabricAccessToken
$updateHeaders = @{ "Authorization" = "Bearer $updateToken"; "Content-Type" = "application/json" }
$updateUri = "$FabricApiBase/workspaces/$workspaceId/OperationsAgents/$agentId/updateDefinition?updateMetadata=True"

try {
    $updateResp = Invoke-WebRequest -Uri $updateUri -Headers $updateHeaders -Method POST -Body $updateBody -ErrorAction Stop
    $updateStatus = [int]$updateResp.StatusCode

    if ($updateStatus -eq 202) {
        $updateOpId = $updateResp.Headers["x-ms-operation-id"]
        if ($updateOpId -is [array]) { $updateOpId = $updateOpId[0] }
        Write-Host "  Long-running operation ($updateOpId), polling..." -ForegroundColor Gray
        for ($poll = 0; $poll -lt 30; $poll++) {
            Start-Sleep -Seconds 5
            $pH = @{ "Authorization" = "Bearer $(Get-FabricAccessToken)" }
            $op = Invoke-RestMethod -Uri "$FabricApiBase/operations/$updateOpId" -Headers $pH
            Write-Host "    Status: $($op.status)... ($($poll * 5)s)" -ForegroundColor DarkGray
            if ($op.status -eq "Succeeded") { break }
            if ($op.status -eq "Failed") {
                $ed = if ($op.error) { $op.error.message } else { "Unknown" }
                throw "Definition update failed: $ed"
            }
        }
    }
    Write-Host "  ✓ Configuration pushed successfully" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Failed to push configuration: $_" -ForegroundColor Red
    Write-Host "    You can configure manually in the Fabric portal." -ForegroundColor Yellow
}

# ============================================================================
# DONE
# ============================================================================

Write-Host ""
Write-Host "  ╔═══════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "  ║  ✓ Operations Agent deployed!                        ║" -ForegroundColor Green
Write-Host "  ╚═══════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "  Agent: $AgentName" -ForegroundColor White
Write-Host "  ID:    $agentId" -ForegroundColor White
Write-Host ""
Write-Host "  Configuration:" -ForegroundColor Cyan
Write-Host "    Goals:        Clinical deterioration detection" -ForegroundColor White
Write-Host "    Data source:  $($kqlDb.displayName) (KQL Database)" -ForegroundColor White
Write-Host "    Actions:      Escalate to Care Team, Log Deterioration Event" -ForegroundColor White
Write-Host "    Status:       Inactive (start manually when ready)" -ForegroundColor White
Write-Host ""
Write-Host "  Next steps:" -ForegroundColor Yellow
Write-Host "    1. Open the agent in the Fabric portal" -ForegroundColor White
Write-Host "    2. Review the generated playbook (entities, rules, properties)" -ForegroundColor White
Write-Host "    3. Connect actions to the '$reflexName' Reflex:" -ForegroundColor White
Write-Host "       Click each action → Select '$reflexName' as Activator" -ForegroundColor Gray
Write-Host "       → Copy connection string → Open flow builder → Paste → Save" -ForegroundColor Gray
Write-Host "    4. Click START to activate the agent" -ForegroundColor White
Write-Host "    5. Install 'Fabric Operations Agent' Teams app to receive messages" -ForegroundColor White
