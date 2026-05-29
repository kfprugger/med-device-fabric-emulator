# Changelog

## [Unreleased] — May 28, 2026

### Phase Monitor & Gantt Matching Upgrades
- **Fixed** Gantt chart in-progress highlighting (pulsing yellow stripes) and updated slow phase threshold from `> 5m` to `> 6m` in Orchestrator UI.
- **Improved** Gantt component pattern matching logic to reliably map custom-normalized step labels and avoid name mismatches.
- **Updated** Phase Monitor complete card action button text from `"After Action Support"` to `"Post Deployment Results"`.
- **Fixed** local FastAPI server (`local_server.py`) `NameError` inside `start_teardown` endpoint by correctly handling partial vs full teardown mode evaluation.

## [Unreleased] — May 27, 2026

### Population Health & Quality Dashboard (4 New Features)
- **Renamed** "CMS Quality Scorecard" → **"Population Health & Quality Dashboard"** (10-page Power BI report)
- **Added** Star Rating Simulator (Step 8)
  - Computes weighted CMS Star Ratings from 7 eCQMs + 3 PDC measures using 2025 cut points
  - What-if simulation: "close N gaps → new star" scenarios (N=10, 25, 50, 100, 250)
  - New Gold tables: `star_rating_detail`, `star_rating_simulation`
  - New report page 7: Star Rating Simulator
- **Added** HCC Risk Adjustment / RAF Scores (Step 9)
  - CMS-HCC V28 model with ~36 SNOMED/ICD-10 → HCC condition mappings
  - Hierarchy rules (keep only most severe per disease group)
  - Demographic RAF coefficients (age/sex bands, Community Non-Dual)
  - Revenue-at-risk calculation ($1,000 PMPM benchmark)
  - New Gold tables: `dim_hcc`, `fact_patient_hcc`, `agg_risk_scores`, `agg_risk_summary`, `revenue_opportunity`
  - New report page 8: Risk Adjustment & RAF
- **Added** 30-Day Readmission Risk ML Model (Step 10)
  - Scikit-learn LogisticRegression trained on 12 features (demographics, LOS, comorbidities, prior utilization, chronic disease flags)
  - 30-day readmission labels computed from Encounter self-join
  - Risk tiers: Low (<15%), Medium (15-30%), High (≥30%)
  - Model performance transparency: AUC, accuracy, precision, recall, feature coefficients
  - New Gold tables: `readmission_risk_scores`, `readmission_risk_summary`, `readmission_model_performance`
  - New report page 9: Readmission Risk
  - **Data Activator**: Daily email alert for high-risk readmission patients
- **Added** Cost & Utilization Analytics (Step 11)
  - Standard utilization metrics: PMPM, IP/1K, ED/1K, ALOS, Bed Days/1K
  - Benchmark comparisons (PMPM $950, IP/1K 300, ED/1K 500, ALOS 5.0)
  - High-cost claimants (≥95th percentile) with condition profiles
  - Condition-specific PMPM (7 chronic conditions)
  - Payer stratification across all metrics
  - New Gold tables: `agg_utilization_summary`, `agg_utilization_by_payer`, `agg_cost_by_category`, `agg_high_cost_claimants`, `agg_condition_pmpm`
  - New report page 10: Cost & Utilization
- **Added** 31 new DAX measures (Star Rating: 4, HCC: 9, Readmission: 5, Utilization: 13) — total now 58
- **Added** 5 new semantic model relationships for cross-table analysis
- **Updated** Orchestrator UI (DeployWizard, PhaseMonitor) with new naming and expanded pattern matching
- Gold Lakehouse grows from 8 → 23 tables; PySpark notebook grows from 760 → 1,529 lines



### Phase 5: Payer-Specific Quality Stratification
- **Moved** all Phase 5 deployment artifacts under `phase-5/` to match the existing `phase-1/`, `phase-2/`, `phase-4/` convention:
  - `cms-quality-report/` → `phase-5/cms-quality-report/`
  - `fabric-rti/sql/materialize_claims_quality.py` → `phase-5/materialize_claims_quality.py`
  - Updated `Deploy-All.ps1`, `.dockerignore`, and docs to reference the new paths
- **Added** `payer_category` denormalized column on `dim_payer`, `fact_claim`, `agg_quality_measures`, `agg_quality_summary` (Medicare / Medicaid / Commercial / Uninsured / Other)
- **Added** `patient_payer` lookup in `materialize_claims_quality.py` — picks each patient's most recent active `Coverage` and propagates payer bucket to facts and quality aggregates
- **Added** `agg_quality_summary` is now computed per measure × payer_category instead of per measure only — enables side-by-side payer comparisons in Direct Lake
- **Added** 14 payer-stratified DAX measures in `_Measures` (Quality Rate / Collection Rate / Denial Rate / Total Paid / Patients Measured per payer)
- **Updated** `docs/phase-5-cms-quality-and-claims.md` with payer stratification section and suggested visuals for the Payer Performance page
- **Backwards compatible**: payer columns default to "Unknown" when Coverage data is absent; no schema-breaking changes (uses `mergeSchema` on overwrite)

## [Unreleased] — April 24, 2026

### Phase 5: CMS Quality & Claims
- **Added** Claims data generation — enabled `Claim`, `ExplanationOfBenefit`, `Coverage` FHIR resources in Synthea properties (flows through existing FHIR → HDS → Silver pipeline)
- **Added** Gold materialization notebook (`materialize_claims_quality.py`) — transforms Silver FHIR tables into star schema: `dim_payer`, `dim_diagnosis`, `fact_claim`, `fact_diagnosis`
- **Added** 7 CMS eCQM quality measures (CMS122 Diabetes HbA1c, CMS165 Blood Pressure, CMS69 BMI Screening, CMS127 Pneumococcal, CMS147 Influenza, CMS134 Diabetes Nephropathy, CMS144 Heart Failure Beta-Blocker)
- **Added** 3 HEDIS medication adherence PDC classes (PDC-DR Diabetes, PDC-RASA RAS Antagonists, PDC-STA Statins) → `agg_medication_adherence`
- **Added** Care gap identification → `care_gaps` table with recommended clinical actions
- **Added** CMS Quality Scorecard Power BI report (Direct Lake, 6 pages, 14 DAX measures)
- **Added** 5 ontology entities (Claim, Payer, Diagnosis, PatientDiagnosis, MedAdherence) + 4 relationships bound to Gold Lakehouse
- **Added** Phase 5 checkbox in Orchestrator UI (DeployWizard + mockDeployment)
- **Added** Phase 5 step in Deploy-All.ps1 (`-Phase5`, `-SkipQualityMeasures`)
- **Added** Backend orchestrator activity (`deploy_quality_measures.py`) + function_app.py wiring

## [Unreleased] — March 28, 2026

### Data Agent Lakehouse Datasource Fix
- **Fixed** `PowerBIEntityNotFound` error in Data Agent UI — lakehouse datasource `type` must be `"lakehouse_tables"` (not `"lakehouse"`), folder prefix must be `lakehouse_tables-` (not `lakehouse-`), and elements must use flat `dbo` schema → table structure without random GUIDs or wrapper grouping. Pattern now matches the working Cohorting Agent (FabricDicomCohortingToolkit).
- **Fixed** `update-agents-inline.ps1` with the same lakehouse datasource corrections

## [Unreleased] — March 26, 2026

### DICOM Loader Fixes
- **Fixed** Python 3.9 compatibility: `str | None` → `Optional[str]`, `tuple[str, str]` → `Tuple[str, str]` in `dicom_retagger.py` and `tcia_client.py`
- **Fixed** `from __future__ import annotations` position in `load_dicom.py` — must be first statement after docstring (was after imports, causing SyntaxError)
- **Fixed** `az acr build` charmap Unicode crash for DICOM loader — added `--no-logs` flag in `deploy-fhir.ps1`

### KQL Deployment
- **Fixed** KQL execution order in `deploy-fabric-rti.ps1` — TelemetryRaw table is now created **before** `fn_AlertHistoryTransform` and the AlertHistory update policy (was created after, causing `General_BadRequest` on fresh deploys)

### Phase 3: Cohorting Toolkit Integration
- **Added** Phase 3 deployment documentation for FabricDicomCohortingToolkit (imaging report, DICOM viewer, cohorting agent)
- **Added** DICOM viewer proxy RBAC requirement — Container App managed identity needs Contributor on Fabric workspace for OneLake file reads
- **Added** OHIF Viewer and TCIA to acknowledgments

### FabricDicomCohortingToolkit
- **Changed** `materialize_reporting.py` — removed all hardcoded workspace/lakehouse GUIDs; now uses `notebookutils.fabric.resolve_workspace_id()` and Fabric REST API to resolve lakehouse IDs by display name
- **Changed** `deploy-notebook.ps1` — auto-discovers OHIF Viewer URL from Azure Static Web App before uploading notebook; patches URL into notebook code at deploy time
- **Changed** Deployment order: DICOM Viewer → Notebook → Report (viewer must deploy first so its URL flows into the reporting data)

## [Unreleased] — March 14-18, 2026

### Deployment Flow
- **Added** Step 1b: Fabric workspace creation early in Phase 1 (before FHIR/DICOM)
- **Fixed** Step 2 to use `-SkipDicom` to prevent duplicate DICOM execution
- **Removed** redundant clinical pipeline trigger — imaging pipeline includes clinical data foundation
- Pipeline sequence: Imaging (includes clinical) → OMOP (was: Clinical → Imaging → OMOP)

### Data Agents
- **Fixed** invalid Observation fewshot query — changed `valueQuantity_value`/`valueQuantity_unit` to `JSON_VALUE(valueQuantity_string, '$.value')`/`JSON_VALUE(valueQuantity_string, '$.unit')`
- **Added** 2 new fewshot examples for full patient summary + demographics by device
- **Added** cross-datasource sample questions (KQL + Lakehouse + DICOM imaging) for both Patient 360 and Clinical Triage agents
- **Fixed** Data Agent portal URL format: `/dataAgents/` → `/aiskills/`

### Deployment Pipeline (Deploy-All.ps1)
- **Added** `-FabricWorkspaceName` as mandatory parameter
- **Added** `-AdminSecurityGroup` as conditionally required (not needed for `-Teardown`/`-Phase2Only`)
- **Changed** `-Location` to mandatory (no default)
- **Changed** `-ResourceGroupName` has default `rg-medtech-rti-fhir` (not mandatory)
- **Added** `-Tags` passthrough to all sub-scripts (`deploy.ps1`, `deploy-fhir.ps1`, `deploy-fabric-rti.ps1`)
- **Added** DICOM shortcut + HDS pipeline step (clinical, imaging, OMOP) in Phase2Only flow
- **Added** pre-populated Phase 2 command in HDS guidance step (auto-fills `-Location`, `-FabricWorkspaceName`, `-Tags` from Phase 1 values)
- **Added** DICOM Data Transformation modality instruction in HDS manual step
- **Fixed** Phase2Only no longer exits early — continues to DICOM shortcuts + Data Agents
- **Fixed** `DeploymentActive` error in `deploy.ps1` — waits 60s and retries
- **Fixed** `RoleAssignmentExists` error in `deploy-fhir.ps1` — treated as non-fatal, falls back to `deployment group show`
- **Fixed** Unicode encoding crash in `az acr build` — added `[Console]::OutputEncoding = UTF8`

### HDS Pipeline Integration
- **Added** OMOP pipeline (`healthcare1_msft_omop_analytics`) as Step 11 in `storage-access-trusted-workspace.ps1`
- **Added** OMOP pipeline parameter to `storage-access-trusted-workspace.ps1`
- Pipeline sequence: Clinical → Imaging → OMOP

### Fabric RTI (deploy-fabric-rti.ps1)
- **Added** `-Tags` parameter — applies tags to Event Hub namespace before enabling SAS auth
- **Added** RBAC propagation wait (60s + verification) after assigning Storage Blob Data Contributor
- **Added** storage access preflight check before shortcut creation
- **Added** 3-attempt retry with 60s wait for Bronze LH shortcut creation
- **Added** Kusto token refresh before KQL external table creation (prevents 401 after long pipeline waits)
- **Added** Workspace identity resolution via Fabric API (`provisionIdentity` → `GET /workspaces/{id}`) with `az ad sp` fallback
- **Added** detailed remediation instructions on shortcut creation failure (SP IDs, portal steps, re-run command)
- **Fixed** `/workspaces/{id}/lakehouses` → `/workspaces/{id}/items?type=Lakehouse` (deprecated endpoint)

### Cleanup (Remove-AllResources.ps1)
- **Added** `-DeleteWorkspace` parameter to delete the Fabric workspace itself
- **Added** Step 2b: deprovision workspace identity + delete Entra app registration
- **Added** `Delete Workspace:` display in teardown banner

### Documentation
- **Updated** README.md: configuration options table with Required column, pre-populated CLI examples, Deploy-All orchestrator section, cleanup section, OMOP in diagrams
- **Updated** PRD.md: OMOP pipeline in artifacts table, data flow diagram, deployment sequence, script descriptions
- **Updated** HDS-SETUP-GUIDE.md: added imaging + OMOP pipelines to artifacts table
- **Updated** all Mermaid diagrams to include OMOP pipeline flow
