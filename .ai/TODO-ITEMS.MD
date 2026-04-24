# TODO Items — Medical Device FHIR Integration Platform

## Priority 1 — Analytics & Reporting

### 1. Power BI Dashboards & Semantic Models
- [ ] Create a **semantic model** over the Silver Lakehouse (`healthcare1_msft_silver`) that exposes Patient, Encounter, Condition, Observation, MedicationRequest, Procedure, Immunization, ImagingStudy, Device, and Basic (DeviceAssociation) tables with proper relationships
- [ ] Build a **Patient Population Overview** dashboard (demographics, condition prevalence, encounter volumes, age/gender distributions)
- [ ] Build a **Remote Patient Monitoring** dashboard sourced from KQL (TelemetryRaw, AlertHistory) showing SpO2/Pulse Rate trends, alert frequency by patient, device utilization rates
- [ ] Build a **Clinical Outcomes** dashboard (readmission rates, condition progression, medication adherence, immunization coverage)
- [ ] Create **row-level security (RLS)** roles on the semantic model for provider-scoped access
- [ ] Publish dashboards to a Fabric-managed Power BI workspace and configure scheduled refresh

### 1b. CMS Quality & Claims (Phase 5)
- [x] Enable `Claim`, `ExplanationOfBenefit`, `Coverage` in Synthea properties
- [x] Create Gold star schema: `dim_payer`, `dim_diagnosis`, `fact_claim`, `fact_diagnosis`
- [x] Compute 7 CMS eCQM quality measures (CMS122, CMS165, CMS69, CMS127, CMS147, CMS134, CMS144)
- [x] Compute 3 HEDIS PDC adherence scores (PDC-DR, PDC-RASA, PDC-STA)
- [x] Identify care gaps with recommended actions
- [x] Build CMS Quality Scorecard Power BI report (6 pages, 14 DAX measures, Direct Lake)
- [x] Extend ontology with 5 claims/quality entities + 4 relationships
- [x] Add Phase 5 to Orchestrator UI, Deploy-All.ps1, and backend function_app.py
- [ ] Add payer-specific quality stratification (Medicare vs Medicaid vs Commercial rates)
- [ ] Add SDOH (Social Determinants of Health) integration from zip-code-level public datasets

---

## Priority 2 — Documentation & Communication

### 2. Demonstration Video
- [x] Record a walkthrough video covering:
  - End-to-end deployment (`Deploy-All.ps1` → Azure infra → FHIR data → Fabric RTI → Data Agents)
  - Masimo device emulator streaming telemetry to Eventstream/Eventhouse
  - Healthcare Data Solutions (HDS) ingestion from FHIR to Silver Lakehouse
  - Real-time KQL dashboards and alert detection
  - Patient 360 and Clinical Triage Data Agent interactions (natural language → KQL + SQL queries)
  - EST timestamp enforcement in vital sign responses
- [ ] Keep the video under 15 minutes; host on Microsoft Stream or YouTube (unlisted)
- [ ] Add the video link to the README.md

### 3. Feedback & Improvement Form
- [ ] Create a Microsoft Form to collect:
  - Overall solution rating (1–5)
  - Most valuable feature
  - Pain points or confusion areas
  - Feature requests
  - Use-case fit (clinical trials, remote monitoring, population health, etc.)
- [ ] Embed or link the form in the README.md and in each Data Agent's greeting message
- [ ] Set up a Power Automate flow to route submissions to a Teams channel or shared mailbox

---

## Priority 3 — Fabric Best Practices for Healthcare Data

### 4. Data Quality & Governance
- [ ] Enable **Microsoft Purview** integration on the Fabric workspace for data cataloging, lineage tracking, and sensitivity labeling of PHI/PII columns (Patient name, birthDate, address, SSN)
- [ ] Implement **data quality rules** on the Silver Lakehouse (e.g., null checks on `subject_string`, valid SNOMED-CT / ICD-10 code validation, referential integrity between Patient ↔ Encounter ↔ Condition)
- [ ] Add a **Bronze → Silver → Gold** medallion layer: ~~create a Gold Lakehouse with pre-aggregated, de-identified, or cohort-level tables optimized for analytics and ML workloads~~ (Gold Lakehouse exists via Phase 3 + Phase 5; expand with additional cohort-level and de-identified tables)
- [ ] Set up **OneLake data access roles** to enforce least-privilege access across engineering, clinical, and analytics teams

### 5. Real-Time Alerting with Data Activator (Reflex)
- [ ] Create **Data Activator triggers** on KQL Eventhouse for:
  - SpO2 < 90% sustained for ≥ 2 minutes → Teams/email alert to care team
  - Pulse Rate > 120 bpm or < 50 bpm sustained for ≥ 3 minutes → escalation alert
  - Device offline (no telemetry received in > 15 minutes) → maintenance alert
- [ ] Route alerts through **Power Automate** or **Teams Adaptive Cards** for actionable clinical notifications
- [ ] Log all triggered alerts back to the `AlertHistory` KQL table for audit trail

### 6. HIPAA Compliance & Security Hardening
- [ ] Enable **audit logging** on all Fabric items (Lakehouse, Eventhouse, Data Agents) and route logs to a centralized Log Analytics workspace
- [ ] Implement **customer-managed keys (CMK)** for encryption at rest on OneLake storage
- [ ] Review and document a **HIPAA Business Associate Agreement (BAA)** checklist for the deployed resources
- [ ] Apply **sensitivity labels** to all datasets containing Protected Health Information (PHI)

### 7. CI/CD & Infrastructure as Code
- [ ] Move agent definitions (instructions, few-shots, datasource configs) into **version-controlled JSON/YAML files** rather than inline PowerShell strings
- [ ] Create a **GitHub Actions** (or Azure DevOps) pipeline that:
  - Lints and validates Bicep templates
  - Deploys infrastructure (FHIR, Fabric RTI, Eventhouse, Lakehouse) to dev/staging/prod environments
  - Pushes Data Agent definition updates via the Fabric REST API
  - Runs integration tests (e.g., verify agent responds to sample NL queries correctly)
- [ ] Add **Bicep parameter files** per environment (dev, staging, prod) with distinct resource naming conventions

### 8. Advanced Analytics & Machine Learning
- [ ] Build a **patient risk stratification** ML model in Fabric using Spark notebooks:
  - Features: vital sign trends, condition history, medication count, encounter frequency, age
  - Target: 30-day readmission or clinical deterioration
  - Register the model in MLflow within the Fabric workspace
- [ ] Create a **patient cohort builder** notebook that filters populations by condition, medication, age range, and device assignment status — useful for clinical trial recruitment
- [ ] Implement **anomaly detection** on TelemetryRaw using KQL's `series_decompose_anomalies()` to flag unusual vital sign patterns beyond simple threshold alerts

### 9. Interoperability & Data Integration
- [ ] Add a **SMART on FHIR** app registration to enable third-party EHR integration (Epic, Cerner) for bidirectional data exchange. **STRETCH GOAL**
- [ ] Implement **FHIR Bulk Export** ($export) on a schedule to keep the Silver Lakehouse in sync with the FHIR Service as new data arrives (beyond the initial HDS load)
- [ ] Add support for **HL7v2 ADT feeds** via an Azure Integration account or Logic App for real-time admit/discharge/transfer events. **STRETCH GOAL**
- [ ] Explore **DICOM → ImagingStudy** enrichment using Azure Health Data Services DICOM service to link radiology images with FHIR ImagingStudy resources ✅ **IMPLEMENTED** — see `dicom-loader/`, `bicep/dicom-infra.bicep`, `deploy-fhir.ps1 -RunDicom`

### 10. Performance & Cost Optimization
- [ ] Enable **Eventhouse data retention policies** to age-off raw telemetry older than 90 days (archive to OneLake cold tier)
- [ ] Implement **Lakehouse table optimization** (V-Order, Z-Order on `subject_string` and `timestamp` columns) for faster query performance
- [ ] Review **Fabric capacity utilization** and right-size the SKU (F2 → F4 → F8) based on actual query and ingestion workloads
- [ ] Add **query caching** hints in the Data Agent KQL datasource instructions for expensive aggregation queries

---

## Priority 4 — Fabric IQ / Ontology (Semantic Layer)

### 11. Create ClinicalDeviceOntology
- [ ] Create `DeviceAssociation` managed table via Spark SQL notebook on Silver Lakehouse (upload `fabric-rti/sql/create-device-association-table.ipynb`)
- [ ] Run `deploy-ontology.ps1` to create ontology with all entity types, data bindings, and relationships via REST API
- [ ] Validate entity instance counts and graph traversal in the Preview experience
- [ ] Connect ontology as a datasource on the **Patient 360** Data Agent
- [ ] Connect ontology as a datasource on the **Clinical Triage** Data Agent
- [ ] Test cross-domain agent queries (e.g., "Show conditions for the patient on device MASIMO-RADIUS7-0033")

### 12. Ontology Maintenance
- [ ] Add ontology deployment to `Deploy-All.ps1` orchestrator
- [ ] Version-control ontology definition as JSON in the repo

---

## Priority 5 — Deployment Orchestrator Web App

### 13. Durable Functions Backend (`orchestrator/`)
- [x] Scaffold project structure (function_app.py, activities/, shared/)
- [x] Implement shared Azure SDK client (ARM deployments, ACR, ACI, RBAC)
- [x] Implement shared Fabric REST API client (workspaces, items, agents, shortcuts)
- [x] Implement shared Kusto management client (KQL scripts, queries)
- [x] Implement Pydantic models (DeploymentConfig, DeploymentState, PhaseResult)
- [x] Implement deploy_all_orchestrator with 9-phase sequencing + HDS human interaction gate
- [x] Implement teardown_orchestrator (reverse-order deletion)
- [x] Implement activity functions for all phases (1, 1b, 2, 2b, 3, 4, 4b, 5, 6)
- [x] Implement HTTP trigger API layer (start, status, resume-hds, cancel, teardown, history)
- [ ] Port remaining PowerShell edge cases (soft-deleted Key Vault purge, role assignment conflicts)
- [ ] Add `-DryRun` / validation mode to test prerequisites before deployment
- [ ] Unit tests with mocked Azure SDK calls for each activity
- [ ] Integration test: deploy to test RG + test Fabric workspace
- [ ] Resilience test: kill Function App mid-phase, verify checkpoint recovery

### 14. React SPA Frontend (`orchestrator-ui/`)
- [x] Scaffold Vite + TypeScript + Fluent UI project
- [x] Implement Layout with tab navigation (Deploy, History, Teardown)
- [x] Implement Deploy Wizard (multi-step form with phase control)
- [x] Implement Phase Monitor (real-time status, HDS gate, resource viewer)
- [x] Implement Deployment History (table with status badges)
- [x] Implement Teardown View (workspace + RG deletion)
- [x] Implement API client (typed fetch wrappers)
- [ ] Add log streaming to Phase Monitor (poll activity function logs)
- [ ] Add per-phase retry button on failure
- [ ] Playwright smoke tests for deploy wizard flow
- [ ] Responsive design for smaller screens

### 15. Infrastructure & CI/CD
- [ ] Create Bicep template for orchestrator (Function App + Static Web App + Storage + MI)
- [ ] GitHub Actions: build + deploy Functions app
- [ ] GitHub Actions: build + deploy Static Web App
- [ ] Configure Function App Managed Identity with Contributor + Fabric API permissions

### 16. Fabric Extensibility Toolkit Integration (Future)
- [ ] Fork the [Extensibility Toolkit Starter-Kit](https://aka.ms/fabric-extensibility-starter-kit)
- [ ] Create a custom **"Deployment Manager"** item type
- [ ] Manifest declares workspace permissions, OneLake access, iFrame relaxation
- [ ] iFrame renders the React SPA from Azure Static Web Apps
- [ ] Item definition stores deployment state in OneLake (replaces Durable Functions entity state)
- [ ] Native Fabric navigation, search, and sharing
- [ ] Investigate [PackageInstallerItem](https://github.com/microsoft/Microsoft-Fabric-tools-workload) pattern for automated Fabric item deployment
- [ ] Evaluate publishing to Workload Hub for broader consumption

---

## Completed Items

- [x] Deploy Azure infrastructure (FHIR, ACR, Storage, Managed Identity)
- [x] Generate and load 10K synthetic patients via Synthea + FHIR Loader
- [x] Create 100 Masimo device associations
- [x] Deploy Fabric RTI (Eventstream, Eventhouse, KQL scripts, HDS Silver Lakehouse)
- [x] Configure Patient 360 and Clinical Triage Data Agents with all 11 tables
- [x] Add enhanced immunization few-shot queries to both agents
- [x] Enforce EST timestamps on all Masimo vital sign responses (3-layer: AI instructions, KQL datasource instructions, few-shot queries)
- [x] Add KQL shortcuts for Observation, MedicationRequest, Procedure, Immunization, ImagingStudy
- [x] Create KQL clinical dashboard (SpO2, Pulse Rate, Alert History panels)
