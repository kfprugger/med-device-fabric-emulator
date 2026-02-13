# TODO Items — Medical Device FHIR Integration Platform

## Priority 1 — Analytics & Reporting

### 1. Power BI Dashboards & Semantic Models
- [ ] Create a **semantic model** over the Silver Lakehouse (`healthcare1_msft_silver`) that exposes Patient, Encounter, Condition, Observation, MedicationRequest, Procedure, Immunization, ImagingStudy, Device, and Basic (DeviceAssociation) tables with proper relationships
- [ ] Build a **Patient Population Overview** dashboard (demographics, condition prevalence, encounter volumes, age/gender distributions)
- [ ] Build a **Remote Patient Monitoring** dashboard sourced from KQL (TelemetryRaw, AlertHistory) showing SpO2/Pulse Rate trends, alert frequency by patient, device utilization rates
- [ ] Build a **Clinical Outcomes** dashboard (readmission rates, condition progression, medication adherence, immunization coverage)
- [ ] Create **row-level security (RLS)** roles on the semantic model for provider-scoped access
- [ ] Publish dashboards to a Fabric-managed Power BI workspace and configure scheduled refresh

---

## Priority 2 — Documentation & Communication

### 2. Demonstration Video
- [ ] Record a walkthrough video covering:
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
- [ ] Add a **Bronze → Silver → Gold** medallion layer: create a Gold Lakehouse with pre-aggregated, de-identified, or cohort-level tables optimized for analytics and ML workloads
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
- [ ] Explore **DICOM → ImagingStudy** enrichment using Azure Health Data Services DICOM service to link radiology images with FHIR ImagingStudy resources

### 10. Performance & Cost Optimization
- [ ] Enable **Eventhouse data retention policies** to age-off raw telemetry older than 90 days (archive to OneLake cold tier)
- [ ] Implement **Lakehouse table optimization** (V-Order, Z-Order on `subject_string` and `timestamp` columns) for faster query performance
- [ ] Review **Fabric capacity utilization** and right-size the SKU (F2 → F4 → F8) based on actual query and ingestion workloads
- [ ] Add **query caching** hints in the Data Agent KQL datasource instructions for expensive aggregation queries

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
