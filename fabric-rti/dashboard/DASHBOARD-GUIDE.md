# Masimo Patient Monitoring Real-Time Dashboard

The automated Fabric RTI deployment creates the `Masimo Patient Monitoring` KQL Dashboard from `fabric-rti/dashboard/masimo-clinical-dashboard.json` and injects the target Eventhouse data source at deploy time.

Manual query reference: `fabric-rti/kql/05-dashboard-queries.kql`.

## Data Source

Connect the dashboard to the `MasimoEventhouse` KQL database. The dashboard uses the Fabric `kusto-trident` data source kind in automated deployments.

## Device Parameter

The dashboard includes one global parameter:

- Display name: `Device`
- Variable: `_selectedDevices`
- Selection: `single-all`
- Values: `TelemetryRaw | distinct device_id | order by device_id asc`

Tiles that use `_selectedDevices` are rendered by Fabric with the selected device or `All` substituted at runtime.

## Pages

### 1. Command Center

Purpose: live command view for clinicians.

Tiles:

- `Online Devices` — card, online device count.
- `Open Clinical Alerts` — card, current `fn_ClinicalAlerts(60)` count.
- `Critical Alerts`, `Urgent Alerts`, `Warning Alerts` — severity KPI cards.
- `Ingestion Lag (seconds)` — latest telemetry freshness.
- `Clinical Alert Feed` — color-coded active alert table with age, patient, vitals, risk context, and recommended action.
- `SpO₂ Trend + Alert Markers + Thresholds` — selected-device SpO₂ trend with alert markers and 94/90/85 threshold lines.
- `Data Freshness / Throughput` — latest event timestamp, events/minute, active devices, and observed throughput.
- `Pulse Rate Trend — Last 60 Minutes` — selected-device pulse trend.
- `Alert Markers — Selected Device` — selected-device active alert table.

### 2. Clinical Alerts

Purpose: triage and explain active clinical risk.

Tiles:

- `Clinical Alert Triage Queue` — enriched alert table with patient ID/name, SpO₂, PR, signal IQ, condition context, repeat/new status, and action guidance.
- `Why Alerts Fired` — explainability table showing threshold reason, clinical context, and recommended action.
- `Clinical Load by Severity — 24h` — severity trend from `AlertHistory`.
- `Top Noisy Devices — 24h` — devices with the highest alert volume.

### 3. Device Detail

Purpose: selected-device drilldown.

Tiles:

- `Selected Device Vitals` — latest bedside vitals and status labels.
- `Selected Device SpO₂ + Alert Markers + Thresholds` — two-hour trend with thresholds and active alert markers.
- `Selected Device Pulse Rate` — pulse trend.
- `Selected Device Alert History` — recent selected-device alerts.
- `Patient Risk Context` — linked patient and condition escalation context.
- `Signal Quality Trend` — selected-device signal quality over time.

### 4. Operations

Purpose: device fleet and ingestion health.

Tiles:

- `Device Connectivity Status` — color-coded online/stale/offline table.
- `Signal Quality vs Clinical Risk` — separates low SpO₂ clinical risk from likely sensor issues.
- `Devices Needing Sensor / Clinical Review` — actionable signal/vital watchlist.
- `Events per Minute — Last 60 Minutes` — ingestion throughput trend.
- `Data Freshness / Throughput` — event and active-device freshness summary.

### 5. Facility Map

Purpose: location-aware clinical alert view from `fn_AlertLocationMap(60)`.

Tiles:

- `Alert Locations` — map visual by facility/location.
- `Alerts by Hospital` — severity counts by facility.
- `Facility Alert Detail` — color-coded alert table with facility context.

## Color Rules

Severity tables use consistent `alert_tier` formatting:

- `CRITICAL` → red critical icon.
- `URGENT` → yellow warning icon.
- `WARNING` → blue circle icon.

Device status tables use:

- `OFFLINE` → red critical icon.
- `STALE` → yellow warning icon.
- `ONLINE` → green/blue normal icon, depending on visual schema support.

Signal quality tables separate:

- `Low SpO2 + poor signal` — clinical + sensor risk.
- `Poor signal only` — sensor check.
- `Low SpO2 + good signal` — likely clinical risk.

## Auto Refresh

Automated deployment enables 30-second auto-refresh:

```json
"autoRefresh": {
  "enabled": true,
  "defaultInterval": "30s",
  "minInterval": "30s"
}
```

## Automated Deployment

Use the normal RTI deployment path. Step 7b reads `fabric-rti/dashboard/masimo-clinical-dashboard.json`, replaces runtime placeholders, and posts `RealTimeDashboard.json` through the Fabric REST API.

Runtime placeholders in the template:

- `__DASHBOARD_TITLE__`
- `__DATA_SOURCE_ID__`
- `__KQL_DB_NAME__`
- `__KUSTO_URI__`
- `__KQL_DB_ID__`
- `__WORKSPACE_ID__`

## Manual Setup

If the automated dashboard update fails:

1. Create or open the `Masimo Patient Monitoring` Real-Time Dashboard.
2. Add the `MasimoEventhouse` KQL database as the data source.
3. Create the `_selectedDevices` global parameter.
4. Use `fabric-rti/kql/05-dashboard-queries.kql` to recreate the page/tile queries.
5. Apply the color rules above to all alert/status tables.
6. Enable 30-second auto-refresh.
