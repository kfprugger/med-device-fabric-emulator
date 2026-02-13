#!/usr/bin/env pwsh
# ============================================================================
# update-agents-inline.ps1
# Updates both Data Agents with:
#   - KQL datasource: TelemetryRaw + AlertHistory (native tables only)
#   - Lakehouse datasource: Silver Lakehouse (Patient, Condition, Device, etc.)
#   - Inline KQL query patterns in AI instructions and few-shot examples
# No functions, no materialized tables, no external tables as KQL elements.
# ============================================================================

$ErrorActionPreference = "Stop"
$wsId           = "8032178a-bf84-45c3-a038-8c93f0c8889f"
$kqlDbId        = "507cf91a-92e0-4f8c-814f-324af06de474"
$kqlDbName      = "MasimoEventhouse"
$silverLhId     = "bd5b2f65-4810-4e54-a784-80914696b0af"
$silverLhName   = "healthcare1_msft_silver"
$apiBase        = "https://api.fabric.microsoft.com/v1"

# --- Auth ---
function Get-Token([string]$Resource) {
    $t = (Get-AzAccessToken -ResourceUrl $Resource).Token
    if ($t -is [System.Security.SecureString]) {
        $b = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($t)
        try { return [System.Runtime.InteropServices.Marshal]::PtrToStringBSTR($b) }
        finally { [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($b) }
    }
    return $t
}
function ConvertTo-B64([string]$Text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Text))
}

# --- KQL elements: ONLY native tables ---
$kqlElements = @(
    @{ id = [guid]::NewGuid().ToString(); display_name = "TelemetryRaw";  type = "kusto.table"; is_selected = $true },
    @{ id = [guid]::NewGuid().ToString(); display_name = "AlertHistory";  type = "kusto.table"; is_selected = $true }
)

# --- Silver Lakehouse elements: Patient, Condition, Device, Location, Encounter, Basic ---
$lakehouseElements = @(
    @{
        id           = [guid]::NewGuid().ToString()
        display_name = "Tables"
        type         = "lakehouse_tables"
        is_selected  = $true
        children     = @(
            @{
                id           = [guid]::NewGuid().ToString()
                display_name = "dbo"
                type         = "lakehouse_tables.schema"
                is_selected  = $true
                children     = @(
                    @{ id = [guid]::NewGuid().ToString(); display_name = "Patient";   type = "lakehouse_tables.table"; is_selected = $true },
                    @{ id = [guid]::NewGuid().ToString(); display_name = "Condition"; type = "lakehouse_tables.table"; is_selected = $true },
                    @{ id = [guid]::NewGuid().ToString(); display_name = "Device";    type = "lakehouse_tables.table"; is_selected = $true },
                    @{ id = [guid]::NewGuid().ToString(); display_name = "Location";  type = "lakehouse_tables.table"; is_selected = $true },
                    @{ id = [guid]::NewGuid().ToString(); display_name = "Encounter"; type = "lakehouse_tables.table"; is_selected = $true },
                    @{ id = [guid]::NewGuid().ToString(); display_name = "Basic";     type = "lakehouse_tables.table"; is_selected = $true }
                )
            }
        )
    }
)

# ============================================================================
# PATIENT 360
# ============================================================================

$p360Instructions = @"
You are a Patient 360 clinical assistant for a medical device monitoring system.
You help clinicians get a unified, comprehensive view of any patient by combining
FHIR R4 clinical data with real-time pulse oximeter telemetry.

======================================================================
QUERY ROUTING RULES (READ FIRST)
======================================================================
You have TWO datasources. Route queries based on these rules:

KEYWORDS THAT REQUIRE KQL (MasimoEventhouse):
  SpO2, oxygen, saturation, pulse rate, heart rate, vitals, readings,
  telemetry, device status, online, offline, alerts, trend, waveform
  -> ALWAYS query KQL for these. Never skip KQL when these words appear.

KEYWORDS THAT REQUIRE LAKEHOUSE (healthcare1_msft_silver):
  patient, name, demographics, condition, diagnosis, respiratory,
  encounter, location, device assignment, FHIR
  -> ALWAYS query Lakehouse for these.

WHEN BOTH KEYWORD TYPES APPEAR IN ONE QUESTION:
  You MUST query BOTH datasources in separate steps. Do NOT stop after
  querying just one. Example: "patients with respiratory conditions and
  low SpO2" contains BOTH respiratory (lakehouse) AND SpO2 (KQL), so
  you MUST run TWO queries: one SQL against lakehouse, one KQL query.

ORDER: For questions mentioning vitals/SpO2/alerts, query KQL FIRST
  to get the real-time data, then query Lakehouse for patient context.
  For questions focused on patient demographics, query Lakehouse first.

======================================================================
DATASOURCE 1: KQL Database (MasimoEventhouse)
======================================================================
Contains real-time Masimo pulse oximeter telemetry.
Tables: TelemetryRaw, AlertHistory

TelemetryRaw columns:
  - device_id (string): e.g. "SIM-MASIMO-001"
  - timestamp (STRING — ALWAYS wrap with todatetime(timestamp))
  - telemetry (dynamic bag): spo2, pr, pi, pvi, sphb, signal_iq
  Extract values: todouble(telemetry.spo2), toint(telemetry.pr), etc.

KQL QUERY PATTERNS:

1) LATEST READINGS PER DEVICE:
   TelemetryRaw
   | summarize arg_max(todatetime(timestamp), *) by device_id
   | project device_id, timestamp, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr), pi = todouble(telemetry.pi), pvi = toint(telemetry.pvi), sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq)

2) DEVICE STATUS — ONLINE/STALE/OFFLINE:
   TelemetryRaw
   | summarize last_seen = max(todatetime(timestamp)) by device_id
   | extend status = case(datetime_diff('second', now(), last_seen) < 30, "ONLINE",
                          datetime_diff('second', now(), last_seen) < 120, "STALE", "OFFLINE")

3) VITALS TREND (replace Xm with e.g. 30m):
   TelemetryRaw | where todatetime(timestamp) > ago(Xm)
   | summarize readings=count(), avg_spo2=round(avg(todouble(telemetry.spo2)),1), min_spo2=round(min(todouble(telemetry.spo2)),1), max_spo2=round(max(todouble(telemetry.spo2)),1), avg_pr=round(avg(todouble(telemetry.pr)),0), min_pr=min(toint(telemetry.pr)), max_pr=max(toint(telemetry.pr)), last_reading=max(todatetime(timestamp)) by device_id

4) SPO2 ALERTS (devices with SpO2 < 94):
   TelemetryRaw | where todatetime(timestamp) > ago(5m)
   | summarize min_spo2=round(min(todouble(telemetry.spo2)),1), avg_spo2=round(avg(todouble(telemetry.spo2)),1), readings=count(), last_time=max(todatetime(timestamp)) by device_id
   | where min_spo2 < 94
   | extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")

5) PULSE RATE ALERTS (brady/tachycardia):
   TelemetryRaw | where todatetime(timestamp) > ago(5m)
   | summarize min_pr=min(toint(telemetry.pr)), max_pr=max(toint(telemetry.pr)), readings=count(), last_time=max(todatetime(timestamp)) by device_id
   | where max_pr > 110 or min_pr < 50
   | extend alert_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING")

ALERT THRESHOLDS: SpO2: CRITICAL <85%, URGENT 85-89%, WARNING 90-93%. PR: CRITICAL >150/<40, URGENT 120-150/40-50.

======================================================================
DATASOURCE 2: Silver Lakehouse (healthcare1_msft_silver)
======================================================================
Contains FHIR R4 clinical data. Query with SQL.

TABLES AND RELATIONSHIPS:
  dbo.Patient — Patient demographics. Key columns: id, idOrig, name (Object), name_string (String/JSON), gender, birthDate
  dbo.Condition — Diagnoses/conditions. Key columns: code (Object), code_string (String/JSON), subject (Object), subject_string (String/JSON)
  dbo.Device — Device records: identifier, type_coding, serialNumber, status
  dbo.Basic — DeviceAssociation records linking devices to patients (100 records). This is THE key table.
  dbo.Location — Facility/location info
  dbo.Encounter — Patient visits

DEVICE-TO-PATIENT LINKING VIA dbo.Basic (CRITICAL):
  The Basic table has 100 DeviceAssociation records. Key columns:
  - code_string: JSON object (NOT array) containing the code. The code value is 'device-assoc' (NOT 'ASSIGNED', NOT 'device-association')
    Example: {"coding":[{"code":"device-assoc","display":"Device Association",...}],...}
  - extension: STRING containing JSON array with the device reference.
    Example: [{"url":"...associated-device","valueReference":{"reference":"Device/MASIMO-RADIUS7-0099","display":"Masimo Radius-7 (MASIMO-RADIUS7-0099)"}},...] 
    To extract device_id: look for 'Device/' in the valueReference.reference and strip the prefix.
  - subject_string: JSON containing patient info INCLUDING the patient name directly in the display field!
    Example: {"display":"Gail741 Zack583 Lowe577","idOrig":"12a89f61-...","msftSourceReference":"Patient/12a89f61-..."}
    You can get patient name directly from subject_string display field WITHOUT joining to dbo.Patient!

  EXAMPLE SQL for device-patient mapping:
  SELECT
    JSON_VALUE(b.extension, '$[0].valueReference.reference') AS device_ref,
    JSON_VALUE(b.subject_string, '$.display') AS patient_name,
    JSON_VALUE(b.subject_string, '$.idOrig') AS patient_id
  FROM dbo.Basic b
  WHERE JSON_VALUE(b.code_string, '$.coding[0].code') = 'device-assoc'

  To match specific device IDs from KQL results:
  SELECT
    JSON_VALUE(b.extension, '$[0].valueReference.reference') AS device_ref,
    JSON_VALUE(b.subject_string, '$.display') AS patient_name
  FROM dbo.Basic b
  WHERE JSON_VALUE(b.code_string, '$.coding[0].code') = 'device-assoc'
    AND JSON_VALUE(b.extension, '$[0].valueReference.display') LIKE '%MASIMO-RADIUS7-0033%'

CONDITION LOOKUP:
  dbo.Condition links to Patient via: JSON_VALUE(c.subject_string, '$.msftSourceReference') = p.idOrig
  Condition names are in code_string JSON: JSON_VALUE(code_string, '$.coding[0].display')
  For text match: code_string LIKE '%asthma%' OR code_string LIKE '%copd%' OR code_string LIKE '%pneumonia%' etc.
  For respiratory conditions, search for: asthma, copd, pneumonia, lung, respiratory, bronchitis

======================================================================
CROSS-DATASOURCE WORKFLOW (CRITICAL)
======================================================================
When a question involves BOTH patient clinical data AND device telemetry,
you MUST query BOTH datasources in separate steps. Examples:

  Q: \"Which patients with respiratory conditions have low SpO2?\"
  STEP 1 — Query KQL: Get devices with SpO2 < 94% from TelemetryRaw
  STEP 2 — Query LAKEHOUSE: Get device-to-patient mapping from Basic (filter code = 'device-assoc'), join Condition for respiratory
  STEP 3 — Correlate device IDs between the two result sets

  Q: \"Give me a full patient summary for Smith\"
  STEP 1 — Query LAKEHOUSE: Get patient demographics, conditions, device assignments from Basic
  STEP 2 — Query KQL: Get latest vitals for the patient's assigned device
  STEP 3 — Present unified 360 view

  Q: \"Check SpO2 and look up patient info\"
  STEP 1 — Query KQL: Get SpO2 data from TelemetryRaw
  STEP 2 — Query LAKEHOUSE: Look up patient info from Basic table (code = 'device-assoc'), patient name is in subject_string display

DEVICE ID FORMAT: MASIMO-RADIUS7-NNNN (e.g., MASIMO-RADIUS7-0001, MASIMO-RADIUS7-0033)
  This format appears in TelemetryRaw.device_id (KQL) and Basic.extension valueReference (Lakehouse).

IMPORTANT:
- All queries are read-only. Never INSERT, UPDATE, or DELETE.
- For ANY question involving patients + vitals/telemetry, ALWAYS query BOTH datasources.
- Query the KQL datasource for: vitals, telemetry, SpO2, pulse rate, device status, alerts.
- Query the Lakehouse datasource for: patient demographics, conditions, diagnoses, devices, encounters, locations.
- Do NOT try to query patient data from KQL — it is only in the Lakehouse.
- Do NOT try to query telemetry from the Lakehouse — it is only in KQL.
- NEVER answer a question about SpO2, vitals, or alerts without querying KQL.
- NEVER stop after querying only one datasource if the question spans both clinical and telemetry domains.
- In dbo.Basic, the code for device associations is 'device-assoc'. NOT 'ASSIGNED', NOT 'device-association'.
"@

$p360KqlDsInstructions = @"
Real-time Masimo pulse oximeter telemetry. Tables: TelemetryRaw, AlertHistory.
timestamp is STRING — ALWAYS wrap with todatetime(timestamp).
Telemetry values are in a dynamic bag: todouble(telemetry.spo2), toint(telemetry.pr), etc.
ALWAYS query this datasource when the user asks about: SpO2, oxygen, pulse rate, heart rate, vitals, readings, telemetry, device status, alerts, trends.
If the question ALSO mentions patients/conditions/diagnoses, query this datasource FIRST for the vitals data, THEN query the Lakehouse for patient context. NEVER skip this datasource for vitals questions.
"@

$p360FewShots = @(
    @{
        id       = "a1b2c3d4-1111-4000-a000-000000000001"
        question = "Show me the latest vital signs for all devices"
        query    = @"
TelemetryRaw
| summarize arg_max(todatetime(timestamp), *) by device_id
| project device_id, timestamp,
          spo2 = todouble(telemetry.spo2),
          pr = toint(telemetry.pr),
          pi = todouble(telemetry.pi),
          pvi = toint(telemetry.pvi),
          sphb = todouble(telemetry.sphb),
          signal_iq = toint(telemetry.signal_iq)
| order by device_id asc
"@
    },
    @{
        id       = "a1b2c3d4-2222-4000-a000-000000000002"
        question = "How many devices are currently online vs offline?"
        query    = @"
TelemetryRaw
| summarize last_seen = max(todatetime(timestamp)) by device_id
| extend status = case(
    datetime_diff('second', now(), last_seen) < 30, "ONLINE",
    datetime_diff('second', now(), last_seen) < 120, "STALE",
    "OFFLINE")
| summarize count() by status
"@
    },
    @{
        id       = "a1b2c3d4-3333-4000-a000-000000000003"
        question = "Show the vitals trend for device MASIMO-RADIUS7-0001 over the last 30 minutes"
        query    = @"
TelemetryRaw
| where device_id == "MASIMO-RADIUS7-0001"
| where todatetime(timestamp) > ago(30m)
| project timestamp = todatetime(timestamp),
          spo2 = todouble(telemetry.spo2),
          pr = toint(telemetry.pr),
          pi = todouble(telemetry.pi),
          pvi = toint(telemetry.pvi),
          sphb = todouble(telemetry.sphb),
          signal_iq = toint(telemetry.signal_iq)
| order by timestamp asc
"@
    },
    @{
        id       = "a1b2c3d4-4444-4000-a000-000000000004"
        question = "Which devices have SpO2 alerts right now?"
        query    = @"
TelemetryRaw
| where todatetime(timestamp) > ago(5m)
| summarize
    min_spo2 = round(min(todouble(telemetry.spo2)), 1),
    avg_spo2 = round(avg(todouble(telemetry.spo2)), 1),
    readings = count(),
    last_time = max(todatetime(timestamp))
  by device_id
| where min_spo2 < 94
| extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")
| project device_id, alert_tier, min_spo2, avg_spo2, readings, last_time
| order by alert_tier asc, min_spo2 asc
"@
    },
    @{
        id       = "a1b2c3d4-5555-4000-a000-000000000005"
        question = "Show rolling vitals statistics for all devices over the last 10 minutes"
        query    = @"
TelemetryRaw
| where todatetime(timestamp) > ago(10m)
| summarize
    readings = count(),
    avg_spo2 = round(avg(todouble(telemetry.spo2)), 1),
    min_spo2 = round(min(todouble(telemetry.spo2)), 1),
    max_spo2 = round(max(todouble(telemetry.spo2)), 1),
    avg_pr = round(avg(todouble(telemetry.pr)), 0),
    min_pr = min(toint(telemetry.pr)),
    max_pr = max(toint(telemetry.pr)),
    last_reading = max(todatetime(timestamp))
  by device_id
| extend minutes_since_last = round(datetime_diff('second', now(), last_reading) / 60.0, 1)
| order by device_id asc
"@
    }
)

# ============================================================================
# CLINICAL TRIAGE
# ============================================================================

$triageInstructions = @"
You are a Clinical Triage Assistant for a hospital device monitoring system.
You help clinical staff prioritize patients based on real-time vital sign alerts
and alert severity. Your role is to support rapid triage decisions by presenting
the most critical situations first.

DATA SOURCES:
- TelemetryRaw (KQL table): Real-time Masimo pulse oximeter data. Columns: device_id (string), timestamp (STRING — always wrap with todatetime(timestamp)), telemetry (dynamic bag with: spo2, pr, pi, pvi, sphb, signal_iq).
- AlertHistory (KQL table): Historical alert records. NOTE: AlertHistory may be empty or stale. ALWAYS prefer TelemetryRaw for current alert detection.
- healthcare1_msft_silver (Lakehouse): FHIR R4 patient data. This is the ONLY source for patient-device mapping, patient names, and clinical conditions.

CRITICAL: timestamp is STRING. ALWAYS use todatetime(timestamp).
CRITICAL: Telemetry values are in a dynamic bag. Extract: todouble(telemetry.spo2), toint(telemetry.pr), etc.
CRITICAL: Patient-device associations are ONLY in the Lakehouse (dbo.Basic table), NOT in KQL. AlertHistory does NOT have patient information.

ALERT TIER THRESHOLDS:
- SpO2: CRITICAL <85%, URGENT 85-89%, WARNING 90-93%
- Pulse Rate: CRITICAL >150 or <40 bpm, URGENT 120-150 or 40-50, WARNING mildly abnormal
- MULTI_METRIC: Both SpO2 and PR abnormal simultaneously (highest priority)

COMMON QUERY PATTERNS — USE THESE INLINE:

1) COMBINED CLINICAL ALERTS (replaces fn_ClinicalAlerts):
   // Get SpO2 alerts
   let spo2_alerts = TelemetryRaw
       | where todatetime(timestamp) > ago(Xm)
       | summarize min_spo2 = min(todouble(telemetry.spo2)), last_time = max(todatetime(timestamp)) by device_id
       | where min_spo2 < 94
       | extend spo2_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")
       | project device_id, spo2_tier, spo2_value = min_spo2, spo2_time = last_time;
   // Get PR alerts
   let pr_alerts = TelemetryRaw
       | where todatetime(timestamp) > ago(Xm)
       | summarize min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), last_time = max(todatetime(timestamp)) by device_id
       | where max_pr > 110 or min_pr < 50
       | extend pr_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING"),
                pr_value = iff(max_pr > 110, todouble(max_pr), todouble(min_pr))
       | project device_id, pr_tier, pr_value, pr_time = last_time;
   // Get latest vitals
   let vitals = TelemetryRaw
       | where todatetime(timestamp) > ago(Xm)
       | summarize arg_max(todatetime(timestamp), *) by device_id
       | project device_id, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr),
                 pi = todouble(telemetry.pi), sphb = todouble(telemetry.sphb);
   // Combine
   vitals
   | join kind=leftouter spo2_alerts on device_id
   | join kind=leftouter pr_alerts on device_id
   | where isnotempty(spo2_tier) or isnotempty(pr_tier)
   | extend alert_tier = case(spo2_tier == "CRITICAL" or pr_tier == "CRITICAL", "CRITICAL",
                               spo2_tier == "URGENT" or pr_tier == "URGENT", "URGENT", "WARNING"),
            alert_type = case(isnotempty(spo2_tier) and isnotempty(pr_tier), "MULTI_METRIC",
                              isnotempty(spo2_tier), "SPO2_LOW", "PR_ABNORMAL")

2) SPO2 ALERTS ONLY:
   TelemetryRaw | where todatetime(timestamp) > ago(Xm)
   | summarize min_spo2 = round(min(todouble(telemetry.spo2)),1), avg_spo2 = round(avg(todouble(telemetry.spo2)),1), readings = count(), last_time = max(todatetime(timestamp)) by device_id
   | where min_spo2 < 94
   | extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")

3) PULSE RATE ALERTS ONLY:
   TelemetryRaw | where todatetime(timestamp) > ago(Xm)
   | summarize min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), readings = count(), last_time = max(todatetime(timestamp)) by device_id
   | where max_pr > 110 or min_pr < 50
   | extend alert_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING")

4) LATEST READINGS:
   TelemetryRaw
   | summarize arg_max(todatetime(timestamp), *) by device_id
   | project device_id, timestamp, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr), pi = todouble(telemetry.pi), sphb = todouble(telemetry.sphb), signal_iq = toint(telemetry.signal_iq)

5) DEVICE STATUS:
   TelemetryRaw
   | summarize last_seen = max(todatetime(timestamp)) by device_id
   | extend status = case(datetime_diff('second', now(), last_seen) < 30, "ONLINE", datetime_diff('second', now(), last_seen) < 120, "STALE", "OFFLINE")

TRIAGE GUIDANCE:
- Always present CRITICAL alerts first, then URGENT, then WARNING
- Highlight MULTI_METRIC alerts (both SpO2 + PR abnormal) as highest priority
- For "triage summary", show counts by tier, then list CRITICAL devices, then URGENT
- All queries are read-only. Never attempt INSERT, UPDATE, or DELETE.

CROSS-DATASOURCE WORKFLOW (for patient identification + condition lookups):
When a question asks about BOTH vitals/alerts AND patient info or conditions:
1. FIRST: Query TelemetryRaw (KQL) to find alerting device IDs and their vitals.
2. THEN: Query the Lakehouse with those EXACT device IDs to find patients and conditions.
   IMPORTANT: You MUST pass the actual device IDs (e.g. MASIMO-RADIUS7-0001, MASIMO-RADIUS7-0033)
   from the KQL results into the Lakehouse query. Do NOT use placeholder values or temp table references.
   The Lakehouse is a separate SQL endpoint — it cannot access KQL tables or results directly.
3. Combine the results to present a unified clinical picture.

Example cross-datasource flow:
- KQL finds MASIMO-RADIUS7-0021 has SpO2=88% (URGENT)
- Lakehouse query: SELECT ... FROM dbo.Basic b JOIN dbo.Condition c ... WHERE ... extension LIKE '%MASIMO-RADIUS7-0021%'
- Result: Patient John Smith has diabetes, hypertension
"@

$triageKqlDsInstructions = @"
This KQL database has real-time Masimo pulse oximeter data in TelemetryRaw and historical alerts in AlertHistory. The timestamp column is STRING — always wrap with todatetime(timestamp). Telemetry values are in a dynamic bag: todouble(telemetry.spo2), toint(telemetry.pr), etc. Write inline KQL queries against TelemetryRaw for alerts, triage, and device status — do not call functions.
"@

$triageFewShots = @(
    @{
        id       = "b1b2c3d4-1111-4000-b000-000000000001"
        question = "Show me all active critical alerts"
        query    = @"
let spo2_alerts = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize min_spo2 = min(todouble(telemetry.spo2)), last_time = max(todatetime(timestamp)) by device_id
    | where min_spo2 < 94
    | extend spo2_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")
    | project device_id, spo2_tier, spo2_value = min_spo2, spo2_time = last_time;
let pr_alerts = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), last_time = max(todatetime(timestamp)) by device_id
    | where max_pr > 110 or min_pr < 50
    | extend pr_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING"),
             pr_value = iff(max_pr > 110, todouble(max_pr), todouble(min_pr))
    | project device_id, pr_tier, pr_value, pr_time = last_time;
let vitals = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize arg_max(todatetime(timestamp), *) by device_id
    | project device_id, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr),
              pi = todouble(telemetry.pi), sphb = todouble(telemetry.sphb);
vitals
| join kind=leftouter spo2_alerts on device_id
| join kind=leftouter pr_alerts on device_id
| where isnotempty(spo2_tier) or isnotempty(pr_tier)
| extend alert_tier = case(spo2_tier == "CRITICAL" or pr_tier == "CRITICAL", "CRITICAL",
                            spo2_tier == "URGENT" or pr_tier == "URGENT", "URGENT", "WARNING"),
         alert_type = case(isnotempty(spo2_tier) and isnotempty(pr_tier), "MULTI_METRIC",
                           isnotempty(spo2_tier), "SPO2_LOW", "PR_ABNORMAL")
| where alert_tier == "CRITICAL"
| project device_id, alert_tier, alert_type, spo2, pr, pi, sphb
| order by device_id asc
"@
    },
    @{
        id       = "b1b2c3d4-2222-4000-b000-000000000002"
        question = "Give me a triage summary"
        query    = @"
let spo2_alerts = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize min_spo2 = min(todouble(telemetry.spo2)), last_time = max(todatetime(timestamp)) by device_id
    | where min_spo2 < 94
    | extend spo2_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")
    | project device_id, spo2_tier, spo2_value = min_spo2;
let pr_alerts = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)), last_time = max(todatetime(timestamp)) by device_id
    | where max_pr > 110 or min_pr < 50
    | extend pr_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING")
    | project device_id, pr_tier;
let vitals = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize arg_max(todatetime(timestamp), *) by device_id
    | project device_id, spo2 = todouble(telemetry.spo2), pr = toint(telemetry.pr);
vitals
| join kind=leftouter spo2_alerts on device_id
| join kind=leftouter pr_alerts on device_id
| where isnotempty(spo2_tier) or isnotempty(pr_tier)
| extend alert_tier = case(spo2_tier == "CRITICAL" or pr_tier == "CRITICAL", "CRITICAL",
                            spo2_tier == "URGENT" or pr_tier == "URGENT", "URGENT", "WARNING")
| summarize alert_count = count() by alert_tier
| order by alert_tier asc
"@
    },
    @{
        id       = "b1b2c3d4-3333-4000-b000-000000000003"
        question = "Which devices have low SpO2 right now?"
        query    = @"
TelemetryRaw
| where todatetime(timestamp) > ago(5m)
| summarize
    min_spo2 = round(min(todouble(telemetry.spo2)), 1),
    avg_spo2 = round(avg(todouble(telemetry.spo2)), 1),
    readings = count(),
    last_time = max(todatetime(timestamp))
  by device_id
| where min_spo2 < 94
| extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")
| project device_id, alert_tier, min_spo2, avg_spo2, readings, last_time
| order by alert_tier asc, min_spo2 asc
"@
    },
    @{
        id       = "b1b2c3d4-4444-4000-b000-000000000004"
        question = "How many alerts are there by severity?"
        query    = @"
let spo2_alerts = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize min_spo2 = min(todouble(telemetry.spo2)) by device_id
    | where min_spo2 < 94
    | extend alert_tier = case(min_spo2 < 85, "CRITICAL", min_spo2 < 90, "URGENT", "WARNING")
    | project device_id, alert_tier, alert_type = "SPO2_LOW";
let pr_alerts = TelemetryRaw
    | where todatetime(timestamp) > ago(10m)
    | summarize min_pr = min(toint(telemetry.pr)), max_pr = max(toint(telemetry.pr)) by device_id
    | where max_pr > 110 or min_pr < 50
    | extend alert_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL", max_pr > 130 or min_pr < 45, "URGENT", "WARNING")
    | project device_id, alert_tier, alert_type = "PR_ABNORMAL";
union spo2_alerts, pr_alerts
| summarize count() by alert_tier, alert_type
| order by alert_tier asc, alert_type asc
"@
    },
    @{
        id       = "b1b2c3d4-5555-4000-b000-000000000005"
        question = "Show the latest readings for all devices"
        query    = @"
TelemetryRaw
| summarize arg_max(todatetime(timestamp), *) by device_id
| project device_id, timestamp,
          spo2 = todouble(telemetry.spo2),
          pr = toint(telemetry.pr),
          pi = todouble(telemetry.pi),
          pvi = toint(telemetry.pvi),
          sphb = todouble(telemetry.sphb),
          signal_iq = toint(telemetry.signal_iq)
| order by device_id asc
"@
    },
    @{
        id       = "b1b2c3d4-6666-4000-b000-000000000006"
        question = "Which devices have pulse rate anomalies?"
        query    = @"
TelemetryRaw
| where todatetime(timestamp) > ago(5m)
| summarize
    min_pr = min(toint(telemetry.pr)),
    max_pr = max(toint(telemetry.pr)),
    avg_pr = round(avg(todouble(telemetry.pr)), 0),
    readings = count(),
    last_time = max(todatetime(timestamp))
  by device_id
| where max_pr > 110 or min_pr < 50
| extend alert_tier = case(max_pr > 150 or min_pr < 40, "CRITICAL",
                            max_pr > 130 or min_pr < 45, "URGENT", "WARNING"),
         alert_type = case(max_pr > 110 and min_pr < 50, "PR_BOTH",
                           max_pr > 110, "PR_HIGH", "PR_LOW")
| project device_id, alert_tier, alert_type, min_pr, max_pr, avg_pr, readings, last_time
| order by alert_tier asc
"@
    }
)

# ============================================================================
# BUILD & PUSH DEFINITIONS
# ============================================================================

function Push-AgentDefinition {
    param(
        [string]$AgentName,
        [string]$AgentId,
        [string]$AiInstructions,
        [array]$DataSources  # Array of @{ FolderName; DatasourceJson; FewShotsJson }
    )

    Write-Host "=== $AgentName ===" -ForegroundColor Cyan

    $dataAgentJson = @{ '$schema' = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json" } | ConvertTo-Json -Depth 5
    $stageConfigJson = @{
        '$schema'      = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json"
        aiInstructions = $AiInstructions
    } | ConvertTo-Json -Depth 5

    $parts = [System.Collections.ArrayList]@(
        @{ path = "Files/Config/data_agent.json";         payload = (ConvertTo-B64 $dataAgentJson);   payloadType = "InlineBase64" },
        @{ path = "Files/Config/draft/stage_config.json"; payload = (ConvertTo-B64 $stageConfigJson); payloadType = "InlineBase64" }
    )
    foreach ($ds in $DataSources) {
        $null = $parts.Add(@{ path = "Files/Config/draft/$($ds.FolderName)/datasource.json"; payload = (ConvertTo-B64 $ds.DatasourceJson); payloadType = "InlineBase64" })
        $null = $parts.Add(@{ path = "Files/Config/draft/$($ds.FolderName)/fewshots.json";   payload = (ConvertTo-B64 $ds.FewShotsJson);   payloadType = "InlineBase64" })
    }

    $definition = @{ definition = @{ parts = @($parts) } }

    $token = Get-Token "https://api.fabric.microsoft.com"
    $headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
    $uri = "$apiBase/workspaces/$wsId/items/$AgentId/updateDefinition"
    $bodyJson = $definition | ConvertTo-Json -Depth 20 -Compress

    $resp = Invoke-WebRequest -Uri $uri -Headers $headers -Method POST -Body $bodyJson -SkipHttpErrorCheck
    $sc = $resp.StatusCode
    if ($sc -lt 300) {
        Write-Host "  Definition pushed (status $sc)" -ForegroundColor Green
    } else {
        Write-Host "  FAILED (status $sc): $($resp.Content)" -ForegroundColor Red
    }
}

# --- Discover agent IDs ---
$token = Get-Token "https://api.fabric.microsoft.com"
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$items = (Invoke-RestMethod -Uri "$apiBase/workspaces/$wsId/items?type=DataAgent" -Headers $headers).value

$p360 = $items | Where-Object { $_.displayName -eq "Patient 360" }
$ct   = $items | Where-Object { $_.displayName -eq "Clinical Triage" }

if (-not $p360) { Write-Host "Patient 360 not found!" -ForegroundColor Red; exit 1 }
if (-not $ct)   { Write-Host "Clinical Triage not found!" -ForegroundColor Red; exit 1 }

Write-Host "Patient 360:     $($p360.id)" -ForegroundColor Gray
Write-Host "Clinical Triage: $($ct.id)" -ForegroundColor Gray
Write-Host "KQL elements: TelemetryRaw + AlertHistory" -ForegroundColor Gray
Write-Host "Lakehouse elements: Patient, Condition, Device, Location, Encounter, Basic" -ForegroundColor Gray
Write-Host ""

# --- Build datasource arrays ---
$kqlDatasourceJson = (@{
    '$schema'              = "1.0.0"
    artifactId             = $kqlDbId
    workspaceId            = $wsId
    displayName            = $kqlDbName
    type                   = "kusto"
    userDescription        = "KQL database with real-time Masimo pulse oximeter telemetry"
    dataSourceInstructions = $p360KqlDsInstructions
    elements               = $kqlElements
} | ConvertTo-Json -Depth 10)
$kqlFewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = $p360FewShots } | ConvertTo-Json -Depth 10)

$lhDsInstructions = @"
FHIR R4 clinical data. Query with SQL.
Tables: dbo.Patient, dbo.Condition, dbo.Device, dbo.Basic, dbo.Location, dbo.Encounter.

CRITICAL — dbo.Basic device-to-patient linking:
- Filter: WHERE JSON_VALUE(code_string, '$.coding[0].code') = 'device-assoc'
  The code is 'device-assoc'. NOT 'ASSIGNED'. NOT 'device-association'.
  NOTE: code_string is a JSON OBJECT not an array. Do NOT use $[0] prefix. Use $.coding[0].code
- Device ref: JSON_VALUE(extension, '$[0].valueReference.reference') → 'Device/MASIMO-RADIUS7-NNNN'
- Patient name: JSON_VALUE(subject_string, '$.display') → patient name directly (no Patient join needed for names!)
- Patient id: JSON_VALUE(subject_string, '$.idOrig') → patient UUID for joining to Condition table

Other relationships:
- dbo.Condition links to Patient via: JSON_VALUE(subject_string, '$.msftSourceReference') = Patient.idOrig. Condition name: JSON_VALUE(code_string, '$.coding[0].display')
- JOIN Basic to Condition (find conditions for device-associated patients):
    ON JSON_VALUE(b.subject_string, '$.idOrig') = JSON_VALUE(c.subject_string, '$.msftSourceReference')
    Both values are bare UUIDs — this join works directly.
- dbo.Patient has patient demographics.

If the user asks about SpO2, vitals, pulse rate, or alerts, you MUST ALSO query the KQL datasource. This datasource has NO telemetry.
"@

$lhFewShots = @(
    @{
        id       = "c1c2c3c4-1111-4000-c000-000000000001"
        question = "List all patients"
        query    = "SELECT TOP 100 * FROM dbo.Patient"
    },
    @{
        id       = "c1c2c3c4-2222-4000-c000-000000000002"
        question = "What conditions do patients have?"
        query    = "SELECT TOP 100 * FROM dbo.Condition"
    },
    @{
        id       = "c1c2c3c4-3333-4000-c000-000000000003"
        question = "Show device association records that link devices to patients"
        query    = "SELECT JSON_VALUE(extension, '$[0].valueReference.reference') AS device_ref, JSON_VALUE(subject_string, '$.display') AS patient_name, JSON_VALUE(subject_string, '$.idOrig') AS patient_id FROM dbo.Basic WHERE JSON_VALUE(code_string, '$.coding[0].code') = 'device-assoc'"
    },
    @{
        id       = "c1c2c3c4-4444-4000-c000-000000000004"
        question = "Show all devices"
        query    = "SELECT TOP 100 * FROM dbo.Device"
    },
    @{
        id       = "c1c2c3c4-5555-4000-c000-000000000005"
        question = "Find patients linked to specific devices like MASIMO-RADIUS7-0033"
        query    = "SELECT JSON_VALUE(b.extension, '$[0].valueReference.display') AS device_name, JSON_VALUE(b.subject_string, '$.display') AS patient_name FROM dbo.Basic b WHERE JSON_VALUE(b.code_string, '$.coding[0].code') = 'device-assoc' AND JSON_VALUE(b.extension, '$[0].valueReference.reference') LIKE '%MASIMO-RADIUS7-0033%'"
    },
    @{
        id       = "c1c2c3c4-6666-4000-c000-000000000006"
        question = "Find patients and their conditions for alerting devices MASIMO-RADIUS7-0021 and MASIMO-RADIUS7-0085"
        query    = "SELECT JSON_VALUE(b.extension, '$[0].valueReference.reference') AS device_ref, JSON_VALUE(b.subject_string, '$.display') AS patient_name, JSON_VALUE(c.code_string, '$.coding[0].display') AS condition_name FROM dbo.Basic b INNER JOIN dbo.Condition c ON JSON_VALUE(b.subject_string, '$.idOrig') = JSON_VALUE(c.subject_string, '$.msftSourceReference') WHERE JSON_VALUE(b.code_string, '$.coding[0].code') = 'device-assoc' AND (JSON_VALUE(b.extension, '$[0].valueReference.reference') LIKE '%MASIMO-RADIUS7-0021%' OR JSON_VALUE(b.extension, '$[0].valueReference.reference') LIKE '%MASIMO-RADIUS7-0085%')"
    },
    @{
        id       = "c1c2c3c4-7777-4000-c000-000000000007"
        question = "List all conditions for patients linked to any device"
        query    = "SELECT JSON_VALUE(b.extension, '$[0].valueReference.reference') AS device_ref, JSON_VALUE(b.subject_string, '$.display') AS patient_name, JSON_VALUE(c.code_string, '$.coding[0].display') AS condition_name, c.onsetDateTime FROM dbo.Basic b INNER JOIN dbo.Condition c ON JSON_VALUE(b.subject_string, '$.idOrig') = JSON_VALUE(c.subject_string, '$.msftSourceReference') WHERE JSON_VALUE(b.code_string, '$.coding[0].code') = 'device-assoc' ORDER BY patient_name, c.onsetDateTime DESC"
    }
)

$lhDatasourceJson = (@{
    '$schema'              = "1.0.0"
    artifactId             = $silverLhId
    workspaceId            = $wsId
    displayName            = $silverLhName
    type                   = "lakehouse"
    userDescription        = "FHIR R4 Silver Lakehouse with Patient, Condition, Device, Location, Encounter, Basic tables"
    dataSourceInstructions = $lhDsInstructions
    elements               = $lakehouseElements
} | ConvertTo-Json -Depth 20)
$lhFewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = $lhFewShots } | ConvertTo-Json -Depth 10)

$p360DataSources = @(
    @{ FolderName = "kusto-$kqlDbName";       DatasourceJson = $kqlDatasourceJson; FewShotsJson = $kqlFewShotsJson },
    @{ FolderName = "lakehouse-$silverLhName"; DatasourceJson = $lhDatasourceJson;  FewShotsJson = $lhFewShotsJson }
)

# Clinical Triage: same KQL datasource, different instructions + few-shots, plus lakehouse
$ctKqlDatasourceJson = (@{
    '$schema'              = "1.0.0"
    artifactId             = $kqlDbId
    workspaceId            = $wsId
    displayName            = $kqlDbName
    type                   = "kusto"
    userDescription        = "KQL database with real-time Masimo pulse oximeter telemetry"
    dataSourceInstructions = $triageKqlDsInstructions
    elements               = $kqlElements
} | ConvertTo-Json -Depth 10)
$ctKqlFewShotsJson = (@{ '$schema' = "1.0.0"; fewShots = $triageFewShots } | ConvertTo-Json -Depth 10)

$ctDataSources = @(
    @{ FolderName = "kusto-$kqlDbName";       DatasourceJson = $ctKqlDatasourceJson; FewShotsJson = $ctKqlFewShotsJson },
    @{ FolderName = "lakehouse-$silverLhName"; DatasourceJson = $lhDatasourceJson;    FewShotsJson = $lhFewShotsJson }
)

Push-AgentDefinition -AgentName "Patient 360"    -AgentId $p360.id -AiInstructions $p360Instructions   -DataSources $p360DataSources
Push-AgentDefinition -AgentName "Clinical Triage" -AgentId $ct.id   -AiInstructions $triageInstructions -DataSources $ctDataSources

Write-Host ""
Write-Host "Both agents updated." -ForegroundColor Green
Write-Host "  KQL: TelemetryRaw + AlertHistory (inline query patterns)" -ForegroundColor Green
Write-Host "  Lakehouse: $silverLhName (Patient, Condition, Device, Location, Encounter, Basic)" -ForegroundColor Green
Write-Host ""
Write-Host "Patient 360:     https://app.fabric.microsoft.com/groups/$wsId/dataAgents/$($p360.id)" -ForegroundColor Cyan
Write-Host "Clinical Triage: https://app.fabric.microsoft.com/groups/$wsId/dataAgents/$($ct.id)" -ForegroundColor Cyan
