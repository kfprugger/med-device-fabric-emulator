# Session Log — February 12, 2026

## Overview

This session resolved **persistent warning triangles** on KQL function elements in Fabric Data Agents through three progressive approaches, culminating in **materializing KQL functions into native tables**. Clinical Triage was deleted and recreated from scratch after the Fabric API experienced a temporary outage.

---

## 1. Problem: Warning Triangles on KQL Functions

### Background

Both Data Agents (Patient 360 and Clinical Triage) showed warning triangles (⚠) on all `fn_*` KQL function elements in the portal, despite the functions being valid and queryable. The warnings prevented the agents from reliably using the functions.

### Root Causes (identified by user)

1. **External table cold-start**: `fn_*` functions reference `external_table('SilverPatient')` etc., which can timeout during `.getschema` introspection
2. **Dynamic schema**: Functions returning `dynamic` columns can't be schema-validated by the Data Agent framework
3. **Parameter functions**: Functions with parameters (e.g., `fn_ClinicalAlerts(windowMinutes)`) prevent the agent framework from calling `.getschema` automatically

---

## 2. Approach 1 — Zero-Parameter Wrapper Functions (Failed)

Created `06-agent-wrapper-functions.kql` with 8 zero-parameter wrappers (e.g., `agent_VitalsTrend`) that called the underlying `fn_*` functions with default parameters.

**Result**: Still showed warnings. The chained function calls and external table references were still problematic for schema introspection.

---

## 3. Approach 2 — Self-Contained Wrapper Functions (Failed)

Rewrote all 8 wrappers to be fully self-contained — inlining logic directly against `TelemetryRaw`, no `fn_*` calls, no `external_table()` references, explicit scalar type casting on all columns.

**Result**: Deployed successfully and schema-validated via query endpoint, but warnings still persisted in the portal.

---

## 4. Approach 3 — Materialized Native Tables (Success)

### Strategy

Materialize the function outputs into native KQL tables using `.set-or-replace`, then configure agents to reference tables instead of functions.

### Table Materialization

Created `materialize-agent-tables.ps1` (Step 1) using `.set-or-replace` commands:

| Agent Table | Source Query | Folder | Row Count |
|-------------|-------------|--------|-----------|
| `AgentVitalsTrend` | `agent_VitalsTrend()` | AgentTables | 100 |
| `AgentDeviceStatus` | `agent_DeviceStatus()` | AgentTables | 100 |
| `AgentLatestReadings` | `agent_LatestReadings()` | AgentTables | 100 |
| `AgentTelemetryByDevice` | `agent_TelemetryByDevice()` | AgentTables | 352,600 |
| `AgentSpO2Alerts` | `agent_SpO2Alerts()` | AgentTables | 100 |
| `AgentPulseRateAlerts` | `agent_PulseRateAlerts()` | AgentTables | 100 |
| `AgentClinicalAlerts` | `agent_ClinicalAlerts()` | AgentTables | 100 |
| `AgentAlertLocationMap` | `agent_AlertLocationMap()` | AgentTables | 100 |

All 8 tables created successfully with data.

### Cleanup

- Dropped all `agent_*` wrapper functions from the KQL database
- Only `fn_*` functions remain (for live real-time queries)

---

## 5. Patient 360 Agent Update (Success)

Updated Patient 360's KQL datasource to reference 10 native tables:

| Element | Type | Status |
|---------|------|--------|
| TelemetryRaw | kusto.table | ✅ Selected |
| AlertHistory | kusto.table | ✅ Selected |
| AgentVitalsTrend | kusto.table | ✅ Selected |
| AgentDeviceStatus | kusto.table | ✅ Selected |
| AgentLatestReadings | kusto.table | ✅ Selected |
| AgentTelemetryByDevice | kusto.table | ✅ Selected |
| AgentSpO2Alerts | kusto.table | ✅ Selected |
| AgentPulseRateAlerts | kusto.table | ✅ Selected |
| AgentClinicalAlerts | kusto.table | ✅ Selected |
| AgentAlertLocationMap | kusto.table | ✅ Selected |

Agent ID: `3be232a1-e4af-4395-bee9-f667f6574f28` (unchanged)

---

## 6. Clinical Triage Agent — Phantom Elements Problem

### Problem

After updating Clinical Triage's definition to use Agent* tables, the auto-discovered element list kept returning **phantom `agent_*` function elements** (even after they were dropped from the KQL database) and never included the Agent* tables. Multiple approaches attempted:

1. **Two-pass push** (strip functions → re-GET with real IDs): Agent* tables never appeared in auto-discovered elements
2. **Copy datasource from Patient 360**: Failed due to LRO Location header parsing issue
3. **Direct definition push**: Same phantom function elements returned

### Diagnosis

The Fabric Data Agent API experienced a **service outage** — `getDefinition`, `updateDefinition`, and `POST /items` (for DataAgent type) all returned `404 EndpointNotFound`. Basic `GET /items` still worked, confirming the agents existed but definition endpoints were down.

### Resolution

1. **Deleted** the old Clinical Triage agent (succeeded before API went fully down)
2. Created `rebuild-clinical-triage.ps1` — a self-contained script that:
   - Discovers or creates the Clinical Triage agent
   - Builds the full definition from scratch (AI instructions, datasource with 10 tables, 6 few-shot examples)
   - Pushes via `updateDefinition`
   - Includes retry logic for name reservation delays
3. After capacity came back online, ran the script successfully

**New Clinical Triage agent ID**: `8d55f57f-1bb4-4a44-b738-e8ba1cea9322`

---

## 7. Technical Issues Encountered

### LRO Location Header Parsing

The Fabric API returns `Location` headers as arrays in PowerShell's `Invoke-WebRequest`. Multiple patterns tried:

```powershell
# Pattern that failed
$opUrl = [string]$locHeader  # Can produce "System.String[]" or empty

# Pattern that works
$opUrl = $resp.Headers['Location']
if ($opUrl -is [array]) { $opUrl = $opUrl[0] }
$opUrl = "$opUrl"  # String interpolation, not [string] cast
```

### OrderedDictionary Method

PowerShell `[ordered]@{}` (OrderedDictionary) uses `.Contains()` not `.ContainsKey()`:

```powershell
# Wrong
$dict.ContainsKey("foo")  # Method not found

# Correct
$dict.Contains("foo")
```

### Fabric API Outage

Between approximately 14:00–15:30 UTC on Feb 12, Data Agent definition endpoints returned 404. Item listing and basic GET worked. The outage was caused by the Fabric capacity being offline (user confirmed).

---

## 8. KQL Database Final State

### Tables
| Table | Folder | Purpose |
|-------|--------|---------|
| TelemetryRaw | (root) | Real-time Masimo pulse oximeter telemetry |
| AlertHistory | (root) | Historical alert records |
| AgentVitalsTrend | AgentTables | Materialized vitals trend snapshot |
| AgentDeviceStatus | AgentTables | Materialized device status snapshot |
| AgentLatestReadings | AgentTables | Materialized latest readings snapshot |
| AgentTelemetryByDevice | AgentTables | Materialized telemetry by device |
| AgentSpO2Alerts | AgentTables | Materialized SpO2 alerts snapshot |
| AgentPulseRateAlerts | AgentTables | Materialized pulse rate alerts snapshot |
| AgentClinicalAlerts | AgentTables | Materialized clinical alerts snapshot |
| AgentAlertLocationMap | AgentTables | Materialized alert location map snapshot |

### Functions
| Function | Folder | Purpose |
|----------|--------|---------|
| fn_VitalsTrend | ClinicalAlerts | Live vitals trend (parameterized) |
| fn_DeviceStatus | ClinicalAlerts | Live device status |
| fn_LatestReadings | ClinicalAlerts | Live latest readings |
| fn_TelemetryByDevice | ClinicalAlerts | Live telemetry by device |
| fn_ClinicalAlerts | ClinicalAlerts | Live enriched clinical alerts |
| fn_SpO2Alerts | ClinicalAlerts | Live SpO2 alerts |
| fn_PulseRateAlerts | ClinicalAlerts | Live pulse rate alerts |
| fn_AlertLocationMap | ClinicalAlerts | Live alert location map |

**Note**: All `agent_*` wrapper functions have been dropped.

---

## Files Created / Modified

| File | Action | Purpose |
|------|--------|---------|
| `materialize-agent-tables.ps1` | Created | Materializes agent functions into native KQL tables + updates agent definitions |
| `rebuild-clinical-triage.ps1` | Created | Creates Clinical Triage agent from scratch with Agent* table elements |
| `06-agent-wrapper-functions.kql` | Modified | Self-contained wrappers (superseded by materialized tables) |
| `deploy-agent-wrappers.ps1` | Modified | Fixed regex splitting, iteration; superseded by materialized tables approach |

### Temp Scripts (can be cleaned up)

| File | Purpose |
|------|---------|
| `fix-clinical-triage.ps1` | Two-pass element update attempt |
| `final-agent-update.ps1` | Both-agent update attempt |
| `fix-ct-final.ps1` | Clinical Triage fix attempt |
| `copy-kql-datasource.ps1` | Copy P360 datasource to CT attempt |
| `fix-agent-tables-step2.ps1` | Step 2 fix attempt (may be deleted) |

---

## Outstanding Items

1. **Portal verification**: Check both agents for green checks on all KQL table elements
2. **Agent* table refresh**: Tables are point-in-time snapshots — need a refresh strategy (scheduled `.set-or-replace`, update policies, or materialized views)
3. **deploy-data-agents.ps1 update**: Still references `fn_*` functions in `$kqlElements` array; needs updating to use Agent* tables
4. **deploy-fabric-rti.ps1 integration**: Incorporate table materialization and agent creation into the main deployment script
5. **Temp script cleanup**: Remove the 5 temp scripts listed above
