# Phase 7 — Payer RTI & Ops

Phase 7 adds payer claim streaming, KQL-native RTI scoring, unified operations triage, and a graph-agent manual ontology attach path on top of the existing Fabric RTI + claims/quality platform.

## med-0619 deployment command

```bash
cd /Users/joey/git/med-device-fabric-emulator
export AZURE_CONFIG_DIR=/Users/joey/.azure-isolated/BrakeKat
pwsh -NoProfile -ExecutionPolicy Bypass -File ./Deploy-All.ps1 \
  -Phase7 \
  -ResourceGroupName rg-med-0619 \
  -Location eastus \
  -FabricWorkspaceName med-0619 \
  -PayerOpsEmail joey@brakekat.com \
  -ClaimEventRatePerMinute 120
```

## Deployed Azure assets

- Event Hub: `claim-stream` under the existing namespace.
- Container group: `claim-emulator-grp`.
- Container image: `claim-emulator:v1` in the existing ACR.
- Managed identity: system-assigned ACI identity with `Azure Event Hubs Data Sender` on the namespace.

## KQL table contract

Input tables:

- `claims_events(event_id:string, event_timestamp:datetime, event_type:string, claim_id:string, patient_id:string, provider_id:string, facility_id:string, payer_id:string, diagnosis_code:string, procedure_code:string, claim_type:string, claim_amount:real, latitude:real, longitude:real, injected_fraud_flags:string)`
- `adt_events(event_id:string, event_timestamp:datetime, event_type:string, patient_id:string, facility_id:string, facility_name:string, admission_type:string, primary_diagnosis:string, latitude:real, longitude:real, has_open_care_gaps:bool, open_gap_measures:string)`
- `rx_events(event_id:string, event_timestamp:datetime, event_type:string, patient_id:string, provider_id:string, medication_code:string, medication_name:string, drug_class:string, quantity:int, days_supply:int, latitude:real, longitude:real)`

Output snapshot tables:

- `fraud_scores(score_id:string, score_timestamp:datetime, claim_id:string, patient_id:string, provider_id:string, fraud_score:real, fraud_flags:string, risk_tier:string, latitude:real, longitude:real)`
- `care_gap_alerts(alert_id:string, alert_timestamp:datetime, patient_id:string, facility_id:string, measure_id:string, measure_name:string, gap_days_overdue:int, alert_priority:string, alert_text:string, latitude:real, longitude:real)`
- `highcost_alerts(alert_id:string, alert_timestamp:datetime, patient_id:string, rolling_spend_30d:real, rolling_spend_90d:real, ed_visits_30d:int, readmission_flag:bool, risk_tier:string, cost_trend:string, latitude:real, longitude:real)`

Streaming ingestion is enabled only on `claims_events`, `adt_events`, and `rx_events`. JSON mappings are named `claims_events_mapping`, `adt_events_mapping`, and `rx_events_mapping`.

## KQL function contract

- `fn_FraudRisk(windowMinutes:int = 60)` scores claims with provider velocity, amount outliers, denial-pattern flags, and upcoding. Tiers: `CRITICAL >= 80`, `HIGH >= 50`, `MEDIUM >= 25`, else `LOW`.
- `fn_HighCostTrajectory(lookbackDays:int = 90)` computes 30-day and 90-day spend, ED visits, risk tier, and `ACCELERATING` / `RISING` / `STABLE` cost trend.
- `fn_CareGapOnAlert(windowMinutes:int = 60)` enriches high-risk payer events with `GoldCareGaps` when the external table exists; otherwise the deployment installs an empty-schema fallback so worklist queries keep running.
- `fn_PayerOpsWorklist(windowMinutes:int = 60)` unions high-priority fraud, high-cost, and care-gap alerts into one KQL source for Reflex and agents.
- Agent wrappers: `agent_FraudRisk()`, `agent_HighCostTrajectory()`, and `agent_PayerOpsWorklist()` are self-contained and do not call parameterized functions or external tables.

## Fabric items

- `PayerOpsActivator`: Reflex over `fn_PayerOpsWorklist(60)`.
- `HealthcareOpsAgent`: OperationsAgent when the tenant supports it; otherwise explicit DataAgent fallback.
- `Payer Ops Triage`: DataAgent with KQL and optional Gold Lakehouse datasources.
- `Healthcare Graph Agent`: DataAgent shell with KQL + Gold/Silver datasource context.

## Eventstream fallback

The deployment first tries to extend `MasimoTelemetryStream` with both telemetry and claim source chains:

- `EventHubSource` → `MasimoTelemetryStream-stream` → `EventhouseDestination` → `TelemetryRaw`
- `ClaimEventHubSource` → `ClaimEventsStream` → `ClaimsEventhouseDestination` → `claims_events`

If Fabric rejects that update, deployment automatically creates `ClaimsRTIStream` with `claim-stream` → `claims_events`. The `med-0619` run used this fallback because Fabric rejected two default streams in one Eventstream topology.

## Manual graph attach instructions

1. Open Fabric workspace `<FabricWorkspaceName>`.
2. Open `ClinicalDeviceOntology`.
3. Select Preview and run `Refresh graph model`.
4. Open Data Agent `Healthcare Graph Agent`.
5. Add `ClinicalDeviceOntology` as a datasource in the Fabric portal and publish the agent.
6. Validate with: `For patient <patient_id>, trace device, clinical alerts, claims, payer, and open care gaps.`

## Verification queries

```kql
claims_events
| where provider_id == "TEST-PROVIDER"
| summarize count(), max(event_timestamp)
```

```kql
fn_FraudRisk(60)
| where provider_id == "TEST-PROVIDER"
| project claim_id, provider_id, fraud_score, risk_tier, fraud_flags, claims_per_hour, amount_zscore
| take 20
```

```kql
fn_HighCostTrajectory(90)
| project patient_id, rolling_spend_30d, rolling_spend_90d, ed_visits_30d, risk_tier, cost_trend
| take 20
```

```kql
fn_PayerOpsWorklist(60)
| summarize count() by alert_domain, priority
```

```kql
fn_CareGapOnAlert(60)
| getschema
```

## Nightly F64 pause note

Verification near 6 PM ET may require temporarily disabling schedule `Pause F64 med-0618 Daily 6PM ET` in Automation account `aza-rjb-wu3` because that runbook pauses capacity `fabrjbwu2`. Restore the schedule after verification.
