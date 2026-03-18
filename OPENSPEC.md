---
openspec: "1.0"
title: "Medical Device FHIR Integration Platform"
id: "med-device-fabric-emulator"
version: "1.0.0"
status: "implemented"
author: "Joey Brakefield"
updated: "2026-03-18"
repository: "https://github.com/kfprugger/med-device-fabric-emulator"
---

# Medical Device FHIR Integration Platform

## Overview

A fully deployable reference architecture that unifies clinical EHR data (FHIR R4), real-time medical device telemetry (Masimo pulse oximeters), and DICOM medical imaging into Microsoft Fabric for unified clinical analytics and AI-powered patient insights.

### Problem

Healthcare organizations cannot correlate fragmented data across EHR systems (FHIR), device gateways (telemetry), and imaging archives (PACS/DICOM). Clinicians cannot ask cross-domain questions like *"Show SpO2 trends for patients with COPD who have recent chest CTs"* because data lives in separate systems with no common query layer.

### Solution

A single Microsoft Fabric workspace that:
- Ingests 5M+ FHIR R4 clinical resources via Healthcare Data Solutions
- Streams real-time pulse oximeter telemetry through Event Hub → Eventstream → Eventhouse
- Loads DICOM medical imaging via OneLake shortcuts to ADLS Gen2
- Deploys AI Data Agents for natural-language clinical queries across all data domains
- Transforms data through HDS pipelines: Clinical → Imaging → OMOP CDM v5.4

---

## Goals

| ID | Goal | Success Metric |
|----|------|---------------|
| G1 | Unify clinical + telemetry + imaging in one platform | All 3 data streams queryable from single workspace |
| G2 | Enable real-time clinical alerting | SpO2/PR alerts generated within 30s of telemetry receipt |
| G3 | Deploy end-to-end with single command | `Deploy-All.ps1` completes Phase 1 in <60 min |
| G4 | Support natural-language clinical queries | Data Agents answer cross-datasource questions accurately |
| G5 | Demonstrate OMOP CDM transformation | Gold Lakehouse populated with OMOP v5.4 tables |
| G6 | Fully idempotent deployment | Re-running any script produces no errors or duplicates |
| G7 | Clean teardown | All Azure + Fabric + Entra resources removed in <10 min |

---

## User Personas

### Clinical User
- **Role:** Clinician, nurse, respiratory therapist
- **Needs:** Real-time patient monitoring, alert triage, unified patient view
- **Interaction:** Data Agents (natural language), Real-Time Dashboards

### Platform Engineer
- **Role:** Healthcare IT, cloud architect
- **Needs:** One-command deployment, idempotent scripts, clean teardown
- **Interaction:** PowerShell CLI, Bicep templates, Fabric REST API

### Data Analyst
- **Role:** Clinical data analyst, population health researcher
- **Needs:** OMOP CDM queries, FHIR R4 Silver tables, KQL analytics
- **Interaction:** KQL queries, Lakehouse SQL, Power BI

---

## Features

### F1: Synthetic Patient Generation
**Status:** ✅ Implemented

Generate realistic FHIR R4 patient populations using Synthea.

| Requirement | Detail |
|------------|--------|
| Patient count | Configurable (default 100, tested up to 10,000) |
| Demographics | Atlanta, GA metro area |
| Resource types | Patient, Encounter, Condition, Observation, MedicationRequest, Procedure, Immunization, Practitioner, Organization, Location |
| Coding systems | ICD-10, SNOMED CT, LOINC, RxNorm |
| Data volume | ~50 resources per patient average |

**Acceptance Criteria:**
- [ ] Synthea generates the configured number of patients
- [ ] FHIR bundles are uploaded to Azure Blob Storage
- [ ] FHIR Loader transforms and uploads to FHIR R4 service
- [ ] Device associations link 100 Masimo devices to qualifying patients

### F2: Real-Time Telemetry Streaming
**Status:** ✅ Implemented

Simulate 100 Masimo Radius-7 pulse oximeters streaming vitals.

| Requirement | Detail |
|------------|--------|
| Devices | 100 Masimo Radius-7 (IDs: MASIMO-RADIUS7-0001..0100) |
| Metrics | SpO2, pulse rate, perfusion index, PVI, SpHb, signal IQ |
| Frequency | Every 10 seconds per device (600 msgs/min total) |
| Transport | Azure Event Hub → Fabric Eventstream → Eventhouse |
| Auth | System-Assigned Managed Identity (Event Hubs Data Sender) |

**Acceptance Criteria:**
- [ ] Emulator ACI runs continuously, streaming to Event Hub
- [ ] Eventstream routes messages to TelemetryRaw table
- [ ] Real-time dashboard shows device status within 30s
- [ ] Clinical alert functions detect SpO2 < 94% and PR anomalies

### F3: DICOM Medical Imaging
**Status:** ✅ Implemented

Download real DICOM studies from TCIA, re-tag with patient identifiers, and ingest via HDS.

| Requirement | Detail |
|------------|--------|
| Source | The Cancer Imaging Archive (TCIA) — LIDC-IDRI collection |
| Modalities | CT (chest), CR (chest X-ray) |
| Re-tagging | pydicom replaces patient demographics + UIDs, preserves pixel data |
| Storage | ADLS Gen2 `dicom-output` container, organized by `{patientId}/{studyUID}/{seriesUID}/` |
| FHIR | ImagingStudy resources created linking studies to patients |
| Ingestion | OneLake shortcut → HDS Imaging Pipeline → Silver ImagingStudy table |

**Acceptance Criteria:**
- [ ] DICOM files uploaded to ADLS Gen2 (50-100 studies, 5000+ files)
- [ ] ImagingStudy FHIR resources created in FHIR service
- [ ] OneLake shortcut created in Bronze Lakehouse
- [ ] HDS Imaging Pipeline processes DICOM metadata into Silver

### F4: Healthcare Data Solutions Integration
**Status:** ✅ Implemented

Leverage Microsoft's HDS for FHIR → Bronze → Silver → Gold (OMOP) transformation.

| Requirement | Detail |
|------------|--------|
| Pipelines | Imaging with Clinical Foundation (includes clinical steps), OMOP Analytics |
| Lakehouses | Bronze (raw FHIR/DICOM), Silver (normalized FHIR R4), Gold (OMOP CDM v5.4) |
| Sequence | Imaging (includes clinical) → OMOP (orchestrated by storage-access-trusted-workspace.ps1) |
| Dependency | scipy 1.11.4 required in HDS Spark environment (auto-installed) |

**Acceptance Criteria:**
- [ ] Bronze shortcut links FHIR export → Bronze Lakehouse
- [ ] Clinical pipeline populates Silver Lakehouse (11 tables)
- [ ] Imaging pipeline populates Silver ImagingStudy table
- [ ] OMOP pipeline populates Gold Lakehouse

### F5: KQL Analytics Layer
**Status:** ✅ Implemented

KQL functions and external tables for real-time analytics and cross-datasource queries.

| Requirement | Detail |
|------------|--------|
| External tables | 11 Silver Lakehouse shortcuts (Patient, Condition, Device, Location, Encounter, Basic, Observation, MedicationRequest, Procedure, Immunization, ImagingStudy) |
| Functions | fn_SpO2Alerts, fn_PulseRateAlerts, fn_ClinicalAlerts (enriched), fn_AlertLocationMap + 3 base functions |
| Alert model | WARNING (<94%), URGENT (<90% or COPD/CHF), CRITICAL (<85% and COPD/CHF) |
| Dashboards | Masimo Patient Monitoring (7 tiles), Clinical Alerts Map (4 tiles) |

**Acceptance Criteria:**
- [ ] All 11 external tables accessible via `external_table('Silver...')`
- [ ] fn_ClinicalAlerts joins telemetry with patient demographics + conditions
- [ ] Dashboards auto-refresh every 30 seconds
- [ ] Alert tiers correctly escalate based on conditions

### F6: AI Data Agents
**Status:** ✅ Implemented

Natural-language clinical query agents federated across KQL + Lakehouse.

| Agent | Datasources | Purpose |
|-------|-------------|---------|
| Patient 360 | KQL (TelemetryRaw, AlertHistory) + Lakehouse (Silver tables) | Unified patient view: vitals + demographics + conditions + medications + imaging |
| Clinical Triage | KQL (TelemetryRaw, AlertHistory) + Lakehouse (Silver tables) | Alert prioritization with patient context for risk-based triage |

| Requirement | Detail |
|------------|--------|
| Few-shot examples | 17 Lakehouse SQL + 5 KQL per agent |
| Cross-datasource | Agents query both KQL and SQL, correlating by device ID |
| Timestamp rule | All vital signs include EST timestamp in response |
| Query routing | Keywords route to correct datasource (SpO2→KQL, conditions→Lakehouse) |

**Acceptance Criteria:**
- [ ] Agents answer single-datasource questions (KQL or Lakehouse)
- [ ] Agents answer cross-datasource questions (vitals + patient info)
- [ ] Agents answer imaging questions (ImagingStudy + vitals)
- [ ] Few-shot examples validated (no invalid column references)

### F7: Automated Deployment Pipeline
**Status:** ✅ Implemented

Single orchestrator with two-phase deployment pattern.

| Phase | Steps | Duration |
|-------|-------|----------|
| Phase 1 | Azure infra → Fabric workspace → FHIR (no DICOM) → DICOM → Fabric RTI | ~60 min |
| Manual | Deploy HDS + DICOM modality in Fabric portal | ~15 min |
| Phase 2 | Bronze shortcut → HDS imaging pipeline (incl. clinical) → OMOP → KQL shortcuts → Data Agents | ~30 min |

| Requirement | Detail |
|------------|--------|
| Mandatory params | `-Location`, `-FabricWorkspaceName` |
| Conditional params | `-AdminSecurityGroup` (not needed for Phase2/Teardown) |
| Idempotency | Re-running any step produces no errors |
| Error recovery | DeploymentActive retry, RoleAssignmentExists non-fatal, RBAC propagation wait |
| Pre-populated Phase 2 | HDS guidance step outputs exact Phase 2 command with values from Phase 1 |

**Acceptance Criteria:**
- [ ] Phase 1 succeeds from clean state
- [ ] Phase 2 succeeds after HDS deployed
- [ ] Re-running Phase 1 or Phase 2 is idempotent
- [ ] Teardown removes all Azure + Fabric + Entra resources

### F8: Clean Teardown
**Status:** ✅ Implemented

Complete resource cleanup including orphaned identity artifacts.

| Requirement | Detail |
|------------|--------|
| Azure | Delete resource group (async + wait) |
| Fabric items | Delete in dependency order (agents → streams → eventhouse → notebooks → pipelines → lakehouses → HDS) |
| Fabric connections | Delete cloud connections (Event Hub, ADLS Gen2) |
| Workspace identity | Deprovision via Fabric API |
| Entra ID | Delete app registration matching workspace name |
| Workspace | Optional `-DeleteWorkspace` flag |

**Acceptance Criteria:**
- [ ] `Remove-AllResources.ps1 -Force -Wait -DeleteWorkspace` leaves no artifacts
- [ ] No Entra SP or app registration remains after teardown
- [ ] Fresh deploy after teardown succeeds without conflicts

---

## Architecture

### Azure Resources

| Resource | Type | Purpose |
|----------|------|---------|
| Health Data Services Workspace | `Microsoft.HealthcareApis/workspaces` | Hosts FHIR R4 service |
| FHIR Service | `Microsoft.HealthcareApis/workspaces/fhirservices` | Clinical data store |
| ADLS Gen2 Storage | `Microsoft.Storage/storageAccounts` | Synthea output, FHIR export, DICOM files |
| Container Registry | `Microsoft.ContainerRegistry/registries` | Container images (4 apps) |
| Event Hub | `Microsoft.EventHub/namespaces` | Telemetry ingestion |
| Managed Identity | `Microsoft.ManagedIdentity/userAssignedIdentities` | ACI job authentication |
| Container Instances (×4) | `Microsoft.ContainerInstance/containerGroups` | Synthea, FHIR Loader, DICOM Loader, Emulator |

### Fabric Workspace

| Item | Workload | Purpose |
|------|----------|---------|
| Eventhouse + KQL Database | Real-Time Intelligence | TelemetryRaw, AlertHistory, 7 functions, 11 external tables |
| Eventstream | Real-Time Intelligence | Event Hub → Eventhouse routing |
| 2× KQL Dashboards | Real-Time Intelligence | Patient monitoring (7 tiles), Alerts map (4 tiles) |
| 3× Lakehouses | Data Engineering | Bronze, Silver (FHIR R4), Gold (OMOP CDM) |
| 3× Data Pipelines | Data Engineering | Imaging (includes clinical), OMOP |
| 2× Data Agents | Data Science | Patient 360, Clinical Triage |

### Data Flow

```
Phase 1 (Automated):
  Step 1:  Azure infra (Event Hub, ACR, emulator)
  Step 1b: Fabric workspace (create + capacity + identity)
  Step 2:  Synthea → Blob → FHIR Loader → FHIR Service → $export → ADLS Gen2
  Step 2b: TCIA → DICOM Loader → ADLS Gen2 (dicom-output)
  Step 3:  Eventhouse, Eventstream, KQL, Dashboard
  Emulator → Event Hub → Eventstream → Eventhouse

Manual:
  Deploy HDS with Healthcare Data Foundations + DICOM modality

Phase 2 (After HDS):
  Step 4:  ADLS Gen2 → OneLake Shortcut → Bronze LH, scipy, KQL shortcuts
  Step 4b: DICOM shortcut → Imaging Pipeline (incl. clinical) → Silver LH
           Silver LH → OMOP Pipeline → Gold LH
  Step 5:  Data Agents (Patient 360 + Clinical Triage)
```

---

## Configuration

| Parameter | Default | Required | Description |
|-----------|---------|----------|-------------|
| `-ResourceGroupName` | `rg-medtech-rti-fhir` | Yes | Azure resource group |
| `-Location` | — | **Yes** | Azure region (e.g. `eastus`) |
| `-FabricWorkspaceName` | — | **Yes** | Fabric workspace name |
| `-AdminSecurityGroup` | — | **Yes**¹ | Azure AD security group |
| `-PatientCount` | `100` | No | Synthea patient count |
| `-Tags` | `@{}` | No | Resource tags (e.g. `@{SecurityControl='Ignore'}`) |
| `-Phase2Only` | `false` | No | Run only Phase 2 |
| `-SkipBaseInfra` | `false` | No | Skip Azure infrastructure |
| `-SkipFhir` | `false` | No | Skip FHIR + Synthea |
| `-SkipDicom` | `false` | No | Skip DICOM steps |
| `-Teardown` | `false` | No | Destroy all resources |
| `-DeleteWorkspace` | `false` | No | Also delete workspace (teardown only) |

> ¹ Not required for `-Teardown` or `-Phase2Only`

---

## Dependencies

### Runtime
- PowerShell 7+
- Azure CLI (`az`)
- Az PowerShell modules: `Az.Accounts`, `Az.Storage`
- Python 3.10+ (containerized — not needed locally)

### Azure Services
- Azure Health Data Services (FHIR R4)
- Azure Event Hub
- Azure Container Registry
- Azure Container Instances
- Azure Storage (ADLS Gen2)

### Fabric
- Microsoft Fabric capacity (F2+)
- Healthcare Data Solutions (deployed manually)
- Data Agent tenant settings enabled

### External
- [Synthea](https://github.com/synthetichealth/synthea) (containerized)
- [TCIA REST API](https://www.cancerimagingarchive.net/) (LIDC-IDRI collection)
- [pydicom](https://pydicom.github.io/) (DICOM re-tagging)

---

## Constraints

| Constraint | Impact | Mitigation |
|-----------|--------|-----------|
| HDS requires manual portal deployment | Cannot fully automate end-to-end | Two-phase approach with pre-populated Phase 2 command |
| Fabric shortcuts need RBAC propagation | First attempt may fail with 401 | 3-attempt retry with 60s backoff |
| Azure Policy may block SAS auth | Event Hub cloud connection fails | `-Tags @{SecurityControl='Ignore'}` applied to namespace |
| Kusto tokens expire after ~60 min | KQL operations fail mid-deployment | Token refresh before Phase 2a operations |
| TCIA rate limits | Large downloads may timeout | Retry logic + configurable study count |
| Data Agents can't cross-join | No single query across KQL + SQL | Agents run separate queries, correlate in response |

---

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Azure Policy blocks deployment | High | Blocks Event Hub, storage shortcuts | SecurityControl tag parameterized on all resources |
| HDS deployment changes | Medium | Manual steps may differ | HDS-SETUP-GUIDE.md maintained with current steps |
| Fabric API deprecation | Medium | Endpoints return 404 | Use `/items?type=X` generically, not type-specific endpoints |
| TCIA collection unavailable | Low | No DICOM data | Fallback to RSNA Pneumonia collection |
| Workspace identity stale | Medium | Shortcuts fail after teardown/recreate | Fabric API identity resolution + Entra cleanup |

---

## File Inventory

### Deployment Scripts
| File | Purpose |
|------|---------|
| `Deploy-All.ps1` | Full orchestrator (Phase 1 + Phase 2 + Teardown) |
| `deploy.ps1` | Azure infrastructure (Event Hub, ACR, emulator) |
| `deploy-fhir.ps1` | FHIR infrastructure + Synthea + FHIR Loader + DICOM Loader |
| `deploy-fabric-rti.ps1` | Fabric RTI (workspace, Eventhouse, KQL, Eventstream, Phase 2) |
| `deploy-data-agents.ps1` | Patient 360 + Clinical Triage Data Agents |
| `storage-access-trusted-workspace.ps1` | DICOM shortcut + HDS pipeline orchestration (imaging, OMOP) |
| `cleanup/Remove-AllResources.ps1` | Full teardown (Azure + Fabric + Entra) |

### Applications
| File | Language | Purpose |
|------|----------|---------|
| `fhir-loader/load_fhir.py` | Python | FHIR bundle transform + upload + device associations |
| `dicom-loader/load_dicom.py` | Python | TCIA download + re-tag + ADLS upload |
| `emulator.py` | Python | Masimo telemetry emulator (Event Hub) |

### Infrastructure as Code
| File | Purpose |
|------|---------|
| `bicep/infra.bicep` | Event Hub, ACR, Key Vault |
| `bicep/emulator.bicep` | Emulator ACI |
| `bicep/fhir-infra.bicep` | FHIR Service, Storage, Managed Identity |
| `bicep/fhir-loader-job.bicep` | FHIR Loader ACI job |
| `bicep/synthea-job.bicep` | Synthea generator ACI job |
| `bicep/dicom-loader-job.bicep` | DICOM Loader ACI job |

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for detailed change history.

---

## Future Work

See [TODO-ITEMS.MD](TODO-ITEMS.MD) for the prioritized backlog. Key items:
- Power BI semantic models + population health dashboards
- Data Activator for real-time alert triggers (Teams/email)
- SMART on FHIR integration (Epic, Cerner)
- ML-based patient risk stratification in Fabric Spark
- CI/CD pipeline (GitHub Actions)
- HIPAA compliance checklist (audit logging, CMK, sensitivity labels)
