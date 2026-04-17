# Real-Time Dashboard Setup Guide

> **Prerequisites:** All local tools must be installed before deploying. Run `setup-prereqs.ps1` from the repo root to verify — it works for both the Orchestrator UI and command-line deployments and provides OS-specific install commands.

Create a Fabric Real-Time Dashboard using the pre-built queries from `fabric-rti/kql/05-dashboard-queries.kql`.

## Quick Setup

1. **Create Dashboard**: In workspace `med-device-real-time`, select **New item → Real-Time Dashboard**
2. **Name it**: `Masimo Clinical Alerts`
3. **Connect data source**: Add your KQL Database (`MasimoEventhouse`) as a data source

## Dashboard Panels

Create **7 tiles** using the queries below. Each tile can be added via **+ Add tile** → paste the KQL query → choose the visual type.

### Panel 1: Device Status (Donut Chart)
```kql
fn_DeviceStatus()
| summarize device_count = count() by status
| order by status asc
```
- **Visual**: Donut / Pie chart
- **Layout**: Top-left, small

### Panel 2: Active Clinical Alerts (Table)
```kql
fn_ClinicalAlerts(5)
| project alert_tier, device_id, alert_type, spo2, pr, message
| order by alert_tier asc
```
- **Visual**: Table with conditional formatting
  - `alert_tier = CRITICAL` → Red background
  - `alert_tier = URGENT` → Orange background
  - `alert_tier = WARNING` → Yellow background
- **Layout**: Top-right, wide

### Panel 3: SpO2 Heatmap (Line Chart)
```kql
TelemetryRaw
| where todatetime(timestamp) > ago(30m)
| summarize avg_spo2 = round(avg(todouble(telemetry.spo2)), 1)
  by device_id, bin(todatetime(timestamp), 1m)
| order by timestamp asc, device_id asc
```
- **Visual**: Multi-line chart (series per device_id)
- **Layout**: Middle-left

### Panel 4: Alert Trend — 24h (Stacked Bar)
```kql
AlertHistory
| where alert_time > ago(24h)
| summarize alert_count = count() by alert_tier, bin(alert_time, 15m)
| order by alert_time asc
```
- **Visual**: Stacked bar chart (series by alert_tier)
- **Layout**: Middle-right

### Panel 5: Top Alerting Devices (Bar Chart)
```kql
AlertHistory
| where alert_time > ago(24h)
| summarize
    total_alerts = count(),
    critical = countif(alert_tier == "CRITICAL"),
    urgent   = countif(alert_tier == "URGENT"),
    warning  = countif(alert_tier == "WARNING")
  by device_id
| top 10 by total_alerts desc
```
- **Visual**: Horizontal bar chart
- **Layout**: Bottom-left

### Panel 6: Vital Signs Snapshot (Table)
```kql
fn_LatestReadings()
| extend
    spo2_status = case(spo2 < 85, "🔴", spo2 < 90, "🟠", spo2 < 94, "🟡", "🟢"),
    pr_status   = case(pr > 150 or pr < 40, "🔴", pr > 130 or pr < 45, "🟠",
                       pr > 110 or pr < 50, "🟡", "🟢")
| project device_id, spo2_status, spo2, pr_status, pr, pi, pvi, sphb, signal_iq, timestamp
| order by spo2 asc
```
- **Visual**: Table
- **Layout**: Bottom-right, wide

### Panel 7: Degraded Signal Quality (Table)
```kql
fn_LatestReadings()
| where signal_iq < 95
| project device_id, signal_iq, spo2, pr, timestamp
| order by signal_iq asc
```
- **Visual**: Table (filtered — only devices with degraded signal)
- **Layout**: Bottom, half-width

## Auto-Refresh

After creating all tiles, enable **Auto-refresh** at 30-second intervals:
- Click the ⚙️ gear icon in the dashboard toolbar
- Set **Auto refresh** → **On**
- Interval: **30 seconds**

## Layout Reference

```
┌───────────────┬───────────────────────────────┐
│ Device Status │     Active Clinical Alerts     │
│  (Donut)      │         (Table)                │
├───────────────┼───────────────┬────────────────┤
│ SpO2 Heatmap  │               │  Alert Trend   │
│ (Line Chart)  │               │  (Stacked Bar) │
├───────────────┼───────────────┴────────────────┤
│ Top Alerting  │     Vital Signs Snapshot        │
│ (Bar Chart)   │         (Table)                 │
├───────────────┴─────────────────────────────────┤
│     Degraded Signal Quality (Table)             │
└─────────────────────────────────────────────────┘
```

## Dashboard JSON Template

A machine-readable tile definition is available at:
`fabric-rti/dashboard/masimo-clinical-dashboard.json`

This can be used as a reference when programmatically creating dashboards via the Fabric API.
