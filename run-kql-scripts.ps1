# run-kql-scripts.ps1 - Execute KQL management scripts 01-03 against MasimoEventhouse
#
# KEY ADAPTATIONS:
# 1. TelemetryRaw.timestamp is STRING (auto-created by Eventstream) — uses todatetime() casts
# 2. PS7 Get-AzAccessToken returns SecureString — uses ConvertFrom-SecureString
# 3. "latest" is a Kusto reserved keyword — uses "vitals" as variable name
# 4. Complex strcat()/case() expressions must use separate extend steps (not inline in project)

param(
    [string]$KustoUri = "https://trd-utbn2kk6k5c2cx8van.z5.kusto.fabric.microsoft.com",
    [string]$DatabaseName = "MasimoEventhouse"
)
$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "=== Acquiring Kusto token ===" -ForegroundColor Cyan
$tokenObj = Get-AzAccessToken -ResourceUrl "https://kusto.kusto.windows.net"
if ($tokenObj.Token -is [System.Security.SecureString]) {
    $kustoToken = $tokenObj.Token | ConvertFrom-SecureString -AsPlainText
} else {
    $kustoToken = $tokenObj.Token
}
$headers = @{ "Authorization" = "Bearer $kustoToken"; "Content-Type" = "application/json" }
Write-Host "Token acquired (expires $($tokenObj.ExpiresOn))." -ForegroundColor Green

function Invoke-KustoMgmt {
    param([string]$Command, [string]$Label)
    $body = @{ db = $DatabaseName; csl = $Command } | ConvertTo-Json -Depth 3 -Compress
    try {
        $null = Invoke-RestMethod -Uri "$KustoUri/v1/rest/mgmt" -Headers $headers -Method POST -Body $body
        Write-Host "  OK: $Label" -ForegroundColor Green
        return $true
    } catch {
        $errBody = $_.ErrorDetails.Message
        try { $parsed = $errBody | ConvertFrom-Json; $msg = $parsed.error.message } catch { $msg = $errBody }
        if ($msg -match "already exists") {
            Write-Host "  SKIP: $Label (already exists)" -ForegroundColor Yellow
            return $true
        }
        Write-Host "  FAIL: $Label" -ForegroundColor Red
        Write-Host "        $msg" -ForegroundColor DarkRed
        return $false
    }
}

$success = 0; $fail = 0

# ============================================================================
# SCRIPT 01: AlertHistory table, policies, and mapping
# ============================================================================
Write-Host ""
Write-Host "=== Script 01: AlertHistory Table & Policies ===" -ForegroundColor Cyan

$cmd = '.create table AlertHistory (alert_id: string, alert_time: datetime, device_id: string, patient_id: string, patient_name: string, alert_tier: string, alert_type: string, metric_name: string, metric_value: real, threshold_value: real, qualifying_conditions: string, message: string, acknowledged: bool, acknowledged_by: string, acknowledged_at: datetime)'
if (Invoke-KustoMgmt $cmd "Create AlertHistory table") { $success++ } else { $fail++ }

$retentionJson = '{"SoftDeletePeriod": "90.00:00:00", "Recoverability": "Enabled"}'
$cmd = ".alter table AlertHistory policy retention @'$retentionJson'"
if (Invoke-KustoMgmt $cmd "AlertHistory retention policy (90 days)") { $success++ } else { $fail++ }

$cmd = '.alter table AlertHistory policy streamingingestion enable'
if (Invoke-KustoMgmt $cmd "AlertHistory streaming ingestion") { $success++ } else { $fail++ }

$mappingBody = '[{"column":"alert_id","path":"$.alert_id","datatype":"string"},{"column":"alert_time","path":"$.alert_time","datatype":"datetime"},{"column":"device_id","path":"$.device_id","datatype":"string"},{"column":"patient_id","path":"$.patient_id","datatype":"string"},{"column":"patient_name","path":"$.patient_name","datatype":"string"},{"column":"alert_tier","path":"$.alert_tier","datatype":"string"},{"column":"alert_type","path":"$.alert_type","datatype":"string"},{"column":"metric_name","path":"$.metric_name","datatype":"string"},{"column":"metric_value","path":"$.metric_value","datatype":"real"},{"column":"threshold_value","path":"$.threshold_value","datatype":"real"},{"column":"qualifying_conditions","path":"$.qualifying_conditions","datatype":"string"},{"column":"message","path":"$.message","datatype":"string"},{"column":"acknowledged","path":"$.acknowledged","datatype":"bool"},{"column":"acknowledged_by","path":"$.acknowledged_by","datatype":"string"},{"column":"acknowledged_at","path":"$.acknowledged_at","datatype":"datetime"}]'
$cmd = ".create-or-alter table AlertHistory ingestion json mapping 'AlertHistoryMapping' @'$mappingBody'"
if (Invoke-KustoMgmt $cmd "AlertHistory JSON mapping") { $success++ } else { $fail++ }

# ============================================================================
# SCRIPT 02: Telemetry Functions (with todatetime(timestamp) casts)
# ============================================================================
Write-Host ""
Write-Host "=== Script 02: Telemetry Functions ===" -ForegroundColor Cyan

# fn_VitalsTrend
$cmd = '.create-or-alter function with (docstring = "Rolling vital sign statistics per device over a sliding window", folder = "ClinicalAlerts") fn_VitalsTrend(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize readings = count(), avg_spo2 = round(avg(todouble(telemetry.spo2)), 1), min_spo2 = round(min(todouble(telemetry.spo2)), 1), max_spo2 = round(max(todouble(telemetry.spo2)), 1), stddev_spo2 = round(stdev(todouble(telemetry.spo2)), 2), avg_pr = round(avg(todouble(telemetry.pr)), 0), min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), avg_pi = round(avg(todouble(telemetry.pi)), 2), avg_pvi = round(avg(todouble(telemetry.pvi)), 0), avg_sphb = round(avg(todouble(telemetry.sphb)), 1), avg_signal_iq = round(avg(todouble(telemetry.signal_iq)), 0), last_reading = max(todatetime(timestamp)) by device_id | extend minutes_since_last = datetime_diff(''second'', now(), last_reading) / 60.0 | order by device_id asc }'
if (Invoke-KustoMgmt $cmd "fn_VitalsTrend") { $success++ } else { $fail++ }

# fn_DeviceStatus
$cmd = '.create-or-alter function with (docstring = "Current device connectivity status based on last telemetry", folder = "ClinicalAlerts") fn_DeviceStatus() { TelemetryRaw | summarize last_seen = max(todatetime(timestamp)) by device_id | extend seconds_ago = datetime_diff(''second'', now(), last_seen), status = case(datetime_diff(''second'', now(), last_seen) < 30, "ONLINE", datetime_diff(''second'', now(), last_seen) < 120, "STALE", "OFFLINE") | order by status asc, device_id asc }'
if (Invoke-KustoMgmt $cmd "fn_DeviceStatus") { $success++ } else { $fail++ }

# fn_LatestReadings
$cmd = '.create-or-alter function with (docstring = "Latest telemetry reading per device", folder = "ClinicalAlerts") fn_LatestReadings() { TelemetryRaw | summarize arg_max(todatetime(timestamp), *) by device_id | project device_id, timestamp, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr), pi = todouble(telemetry.pi), pvi = toint(telemetry.pvi), sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq) | order by device_id asc }'
if (Invoke-KustoMgmt $cmd "fn_LatestReadings") { $success++ } else { $fail++ }

# fn_TelemetryByDevice
$cmd = '.create-or-alter function with (docstring = "Time-series telemetry for a specific device", folder = "ClinicalAlerts") fn_TelemetryByDevice(target_device: string, lookback_minutes: int = 60) { TelemetryRaw | where device_id == target_device | where todatetime(timestamp) > ago(1m * lookback_minutes) | project timestamp = todatetime(timestamp), spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr), pi = todouble(telemetry.pi), pvi = toint(telemetry.pvi), sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq) | order by timestamp asc }'
if (Invoke-KustoMgmt $cmd "fn_TelemetryByDevice") { $success++ } else { $fail++ }

# ============================================================================
# SCRIPT 03: Clinical Alert Functions
# NOTE: "latest" is a Kusto reserved keyword — use "vitals" instead
# NOTE: Complex strcat/case in project causes BadRequest — use separate extend
# ============================================================================
Write-Host ""
Write-Host "=== Script 03: Clinical Alert Functions ===" -ForegroundColor Cyan

# fn_SpO2Alerts — separate extend for message and threshold_value
$cmd = '.create-or-alter function with (docstring = "Detect SpO2 threshold breaches with 3-tier severity", folder = "ClinicalAlerts") fn_SpO2Alerts(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | where isnotnull(telemetry.spo2) | extend spo2 = todouble(telemetry.spo2) | where spo2 > 0 and spo2 < 100 | summarize min_spo2 = min(spo2), avg_spo2 = round(avg(spo2),1), readings = count(), last_time = max(todatetime(timestamp)) by device_id | where min_spo2 < 94 | extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING") | extend alert_type = "SPO2_LOW" | extend message = strcat(alert_tier, " Dev:", device_id, " SpO2=", tostring(min_spo2)) | extend tv = case(alert_tier == "CRITICAL", 85.0, alert_tier == "URGENT", 90.0, 94.0) | project alert_time = last_time, device_id, alert_tier, alert_type, metric_name = "spo2", metric_value = min_spo2, threshold_value = tv, message, readings | order by alert_tier asc, min_spo2 asc }'
if (Invoke-KustoMgmt $cmd "fn_SpO2Alerts") { $success++ } else { $fail++ }

# fn_PulseRateAlerts — separate extend for message and threshold
$cmd = '.create-or-alter function with (docstring = "Detect pulse rate anomalies with 3-tier severity", folder = "ClinicalAlerts") fn_PulseRateAlerts(windowMinutes: int = 5) { TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize readings = count(), avg_pr = round(avg(todouble(telemetry.pr)), 0), min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), last_time = max(todatetime(timestamp)) by device_id | where max_pr > 110 or min_pr < 50 | extend is_tachy = max_pr > 110, is_brady = min_pr < 50 | extend alert_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING") | extend alert_type = case(max_pr > 110 and min_pr < 50, "PR_BOTH", max_pr > 110, "PR_HIGH", "PR_LOW") | extend abnormal_value = iff(max_pr > 110, todouble(max_pr), todouble(min_pr)) | extend message = strcat(alert_tier, " Dev:", device_id, " PR:", tostring(max_pr)) | extend tv = case(alert_tier == "CRITICAL", iff(is_tachy, 150.0, 40.0), alert_tier == "URGENT", iff(is_tachy, 130.0, 45.0), iff(is_tachy, 110.0, 50.0)) | project alert_time = last_time, device_id, alert_tier, alert_type, metric_name = "pr", metric_value = abnormal_value, threshold_value = tv, message, readings | order by alert_tier asc }'
if (Invoke-KustoMgmt $cmd "fn_PulseRateAlerts") { $success++ } else { $fail++ }

# fn_ClinicalAlerts — uses "vitals" not "latest" (reserved keyword); separate extends
$cmd = '.create-or-alter function with (docstring = "Enriched clinical alerts combining SpO2 and PR alerts with latest vitals", folder = "ClinicalAlerts") fn_ClinicalAlerts(windowMinutes: int = 5) { let sa = fn_SpO2Alerts(windowMinutes) | project device_id, alert_time, spo2_tier = alert_tier, spo2_value = metric_value, spo2_msg = message; let pa = fn_PulseRateAlerts(windowMinutes) | project device_id, alert_time, pr_tier = alert_tier, pr_value = metric_value, pr_msg = message; let vitals = TelemetryRaw | where todatetime(timestamp) > ago(1m * windowMinutes) | summarize arg_max(todatetime(timestamp), *) by device_id | project device_id, current_spo2 = todouble(telemetry.spo2), current_pr = toint(telemetry.pr), current_pi = todouble(telemetry.pi); vitals | join kind=leftouter sa on device_id | join kind=leftouter pa on device_id | where isnotempty(spo2_tier) or isnotempty(pr_tier) | extend alert_tier = case(spo2_tier == "CRITICAL" or pr_tier == "CRITICAL", "CRITICAL", spo2_tier == "URGENT" or pr_tier == "URGENT", "URGENT", "WARNING") | extend alert_type = case(isnotempty(spo2_tier) and isnotempty(pr_tier), "MULTI_METRIC", isnotempty(spo2_tier), "SPO2_LOW", "PR_ABNORMAL") | extend message = strcat(alert_tier, " Dev:", device_id, " SpO2:", tostring(current_spo2), " PR:", tostring(current_pr)) | project alert_time = coalesce(alert_time, alert_time1, now()), device_id, alert_tier, alert_type, spo2 = current_spo2, pr = current_pr, pi = current_pi, message }'
if (Invoke-KustoMgmt $cmd "fn_ClinicalAlerts") { $success++ } else { $fail++ }

# ============================================================================
# Summary
# ============================================================================
Write-Host ""
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "  KQL Scripts 01-03 Complete" -ForegroundColor Cyan
Write-Host "  Succeeded: $success / $($success + $fail)" -ForegroundColor $(if ($fail -eq 0) { "Green" } else { "Yellow" })
if ($fail -gt 0) { Write-Host "  Failed:    $fail" -ForegroundColor Red }
Write-Host "  Script 04: SKIPPED (HDS not yet deployed)" -ForegroundColor Yellow
Write-Host "  Script 05: Reference queries only" -ForegroundColor DarkGray
Write-Host "===========================================" -ForegroundColor Cyan
