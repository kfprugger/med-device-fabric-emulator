# Product Requirements Document (PRD)

## Medical Device FHIR Integration Platform

**Version:** 1.0
**Author:** Joey Brakefield
**Last Updated:** March 2026

---

## 1. Problem Statement

Healthcare organizations need to unify fragmented clinical data — EHR records, real-time medical device telemetry, and DICOM medical imaging — into a single analytics platform. Today, these data streams live in separate systems (FHIR servers, device gateways, PACS/DICOM archives) with no common query layer, making it impossible for clinicians to ask simple cross-domain questions like *"Show me the SpO2 trends for patients with COPD who have recent chest CTs."*

This project demonstrates that Microsoft Fabric can serve as that unified platform, combining Real-Time Intelligence, Healthcare Data Solutions, Data Agents, and OneLake into a single workspace where all three data streams converge.

---

## 2. Solution Overview

A fully deployable reference architecture that:

1. **Generates** 10,000 synthetic FHIR R4 patients (Synthea) for the Atlanta, GA metro area
2. **Loads** 5M+ clinical resources into Azure Health Data Services (FHIR R4)
3. **Links** 100 Masimo Radius-7 pulse oximeters to patients with qualifying respiratory/cardiac conditions
4. **Streams** real-time telemetry (SpO2, pulse rate, perfusion index) through Event Hub → Fabric Eventstream → Eventhouse
5. **Ingests** FHIR data into Fabric via Healthcare Data Solutions (HDS) clinical pipeline → Silver Lakehouse
6. **Downloads** real DICOM chest CT studies from TCIA, re-tags with Synthea patient identifiers, uploads to ADLS Gen2, and ingests into Fabric via HDS imaging pipeline
7. **Deploys** two AI Data Agents (Patient 360 + Clinical Triage) that federate across KQL telemetry and Lakehouse clinical data
8. **Creates** a Fabric IQ Ontology for semantic graph queries across all entity types

The entire solution deploys with a single command (`Deploy-All.ps1`) and touches six Fabric workloads: Real-Time Intelligence, Data Engineering, Data Warehouse, Data Science, Data Agents, and Power BI.

---

## 3. Architecture

### 3.1 Azure Resources (Resource Group)

| Resource | Type | Purpose |
|----------|------|---------|
| Health Data Services Workspace | `Microsoft.HealthcareApis/workspaces` | Hosts FHIR R4 service |
| FHIR Service | `Microsoft.HealthcareApis/workspaces/fhirservices` | Stores clinical data (Patient, Condition, Encounter, Observation, MedicationRequest, Device, ImagingStudy, Basic) |
| ADLS Gen2 Storage Account | `Microsoft.Storage/storageAccounts` | Stores Synthea output (`synthea-output`), FHIR `$export` output (`fhir-export`), re-tagged DICOM files (`dicom-output`) |
| Azure Container Registry | `Microsoft.ContainerRegistry/registries` | Hosts container images: `synthea-generator`, `fhir-loader`, `dicom-loader`, `masimo-emulator` |
| Event Hub Namespace + Hub | `Microsoft.EventHub/namespaces` | Receives real-time telemetry from emulator |
| User-Assigned Managed Identity | `Microsoft.ManagedIdentity/userAssignedIdentities` | `id-aci-fhir-jobs` — shared identity for all ACI containers |
| Azure Container Instances (×4) | `Microsoft.ContainerInstance/containerGroups` | Run-once jobs: synthea-generator, fhir-loader, dicom-loader; long-running: masimo-emulator |

### 3.2 Fabric Workspace

| Item | Type | Purpose |
|------|------|---------|
| Eventhouse + KQL Database | Real-Time Intelligence | Stores `TelemetryRaw` and `AlertHistory` tables |
| Eventstream | Real-Time Intelligence | Routes Event Hub → Eventhouse |
| Real-Time Dashboard | Real-Time Intelligence | 7-tile clinical monitoring dashboard (auto-refresh 30s) |
| Clinical Alerts Map | Real-Time Intelligence | 4-tile geospatial alert dashboard |
| KQL Functions (×7) | Real-Time Intelligence | Telemetry analytics + clinical alert detection |
| KQL External Tables (×6) | Real-Time Intelligence | OneLake shortcuts to Silver Lakehouse delta tables |
| HDS Bronze Lakehouse | Data Engineering | Raw FHIR + DICOM data (managed by HDS) |
| HDS Silver Lakehouse | Data Engineering | Normalized FHIR R4 tables (Patient, Condition, etc.) |
| HDS Gold Lakehouse | Data Engineering | OMOP CDM v5.4 tables |
| HDS Clinical Pipeline | Data Engineering | Bronze → Silver FHIR flattening |
| HDS Imaging Pipeline | Data Engineering | DICOM metadata ingestion |
| HDS OMOP Pipeline | Data Engineering | Silver → Gold OMOP CDM v5.4 transformation |
| OneLake Shortcut | Data Engineering | ADLS Gen2 `dicom-output` → Bronze Lakehouse `/Files/Ingest/Imaging/DICOM/DICOM-HDS/` |
| Patient 360 Agent | Data Science | Natural-language patient queries (KQL + Lakehouse) |
| Clinical Triage Agent | Data Science | Alert-based clinical triage (KQL + Lakehouse) |
| ClinicalDeviceOntology | Fabric IQ | Semantic graph: 9 entity types, 8 relationships |

### 3.3 Data Flow

```
Synthea (ACI) → Blob Storage → FHIR Loader (ACI) → FHIR Service
                                                        ↓
                                              FHIR $export → ADLS Gen2
                                                        ↓
                                              HDS Clinical Pipeline
                                                        ↓
                                              Silver Lakehouse (Patient, Condition, ...)
                                                        ↓
                                              KQL Shortcuts (SilverPatient, SilverCondition, ...)
                                                        ↓
                                              Data Agents (Patient 360 + Clinical Triage)

TCIA (Internet) → DICOM Loader (ACI) → ADLS Gen2 (dicom-output)
                        ↓                        ↓
                  FHIR ImagingStudy     OneLake Shortcut → Bronze LH
                                                        ↓
                                              HDS Imaging Pipeline
                                                        ↓
                                              Silver Lakehouse (ImagingStudy)
                                                        ↓
                                              HDS OMOP Pipeline
                                                        ↓
                                              Gold Lakehouse (OMOP CDM v5.4)

Masimo Emulator (ACI) → Event Hub → Eventstream → Eventhouse (TelemetryRaw)
                                                        ↓
                                              KQL fn_ClinicalAlerts
                                                        ↓
                                              AlertHistory + Dashboard
```

---

## 4. Deployment Sequence

The deployment is orchestrated by `Deploy-All.ps1` and follows this sequence:

| Step | Script | What It Does | Duration |
|------|--------|-------------|----------|
| 1 | `deploy.ps1` | Creates RG, Event Hub, ACR, Key Vault, builds emulator container, deploys emulator ACI | ~10 min |
| 2 | `deploy-fhir.ps1` | Creates HDS workspace, FHIR service, storage, managed identity, builds Synthea + Loader containers, generates 10K patients, uploads to FHIR | ~60 min |
| 2b | `deploy-fhir.ps1 -RunDicom` | Builds DICOM loader container, downloads TCIA studies, re-tags, uploads .dcm to ADLS Gen2, creates ImagingStudy FHIR resources | ~30 min |
| 2c | `storage-access-trusted-workspace.ps1` | Grants workspace identity RBAC + ACLs on storage, creates OneLake shortcut, invokes HDS clinical, imaging, and OMOP pipelines | ~5 min |
| 3 | `deploy-fabric-rti.ps1` | Creates Fabric workspace, Eventhouse, KQL DB, Eventstream, cloud connection, KQL tables/functions, FHIR $export, real-time dashboard | ~15 min |
| — | *Manual* | Deploy HDS in Fabric portal, add scipy 1.11.4, run clinical pipeline | ~15 min |
| 4 | `deploy-fabric-rti.ps1 -Phase2` | Creates KQL shortcuts to Silver tables, enriched alert functions, Clinical Alerts Map dashboard | ~10 min |
| 5 | `deploy-data-agents.ps1` | Creates Patient 360 + Clinical Triage Data Agents with instructions + few-shots | ~5 min |
| 6 | `deploy-ontology.ps1` | Creates ClinicalDeviceOntology (9 entity types, 8 relationships) | ~5 min |

**Total automated time:** ~2 hours (excluding manual HDS step)

---

## 5. Key Design Decisions

### 5.1 Synthea for Synthetic Data
- **Why:** Generates realistic, standards-compliant FHIR R4 bundles with complete medical histories
- **Configuration:** Atlanta demographics, 10K patients, ICD-10 + SNOMED coding
- **Trade-off:** Synthea bundles contain `urn:uuid:` references that require transformation before upload

### 5.2 DICOM via TCIA (Not Azure DICOM Service)
- **Why:** Eliminates Azure DICOM service dependency and managed identity complexity
- **Approach:** Download real TCIA studies → re-tag with pydicom → store as `.dcm` files in ADLS Gen2
- **Trade-off:** No DICOMweb query capability (WADO-RS/QIDO-RS), but files are directly accessible via OneLake

### 5.3 Condition-to-Modality Mapping
- **How:** SNOMED codes on patient conditions map to TCIA collections (COPD→CT chest, Asthma→CR chest X-ray)
- **Config:** `dicom-loader/condition_modality_map.json`

### 5.4 Dual-Datasource Data Agents
- **Why:** Fabric Data Agents cannot join across KQL and Lakehouse in a single query
- **Approach:** Agents run separate queries against each datasource and correlate results in their response
- **Key:** Device associations (`Basic` resources) provide the Patient↔Device linkage bridge

### 5.5 HDS for FHIR Ingestion (Not Custom ETL)
- **Why:** Microsoft's native connector handles FHIR→Delta table transformation, identity management, and incremental updates
- **Trade-off:** Requires manual portal setup and `scipy` dependency

### 5.6 Run-Once ACI Jobs
- **Why:** Container Instances with `restartPolicy: Never` are cheap, disposable, and don't require AKS
- **Pattern:** Build image in ACR → Deploy ACI via Bicep → Wait for completion → Stream logs → Clean up

---

## 6. File Inventory

### 6.1 Deployment Scripts (PowerShell)

| File | Lines | Purpose |
|------|-------|---------|
| `Deploy-All.ps1` | 396 | Full orchestrator with `Invoke-Step` pattern, step timing, summary |
| `deploy-fhir.ps1` | 584 | FHIR infra + Synthea + Loader + DICOM loader pipeline |
| `deploy-fabric-rti.ps1` | 2279 | Fabric workspace, Eventhouse, Eventstream, KQL, dashboards, FHIR $export |
| `deploy-data-agents.ps1` | 1003 | Patient 360 + Clinical Triage Data Agent creation/update |
| `deploy-ontology.ps1` | 402 | Fabric IQ Ontology REST API deployment |
| `deploy-operations-agent.ps1` | 312 | Operations agent (standalone) |
| `deploy.ps1` | 203 | Legacy emulator-only deployment (Event Hub, ACR, emulator ACI) |
| `storage-access-trusted-workspace.ps1` | 630 | DICOM OneLake shortcut + RBAC/ACL + HDS pipeline trigger (clinical, imaging, OMOP) |
| `update-agents-inline.ps1` | 791 | Quick-update agent definitions (hardcoded workspace/item IDs) |
| `run-kql-scripts.ps1` | 101 | Standalone KQL script runner |

### 6.2 Cleanup Scripts

| File | Lines | Purpose |
|------|-------|---------|
| `cleanup/Remove-AzureInfra.ps1` | 58 | Deletes the Azure resource group |
| `cleanup/Remove-FabricWorkspace.ps1` | 72 | Deletes the Fabric workspace and all items |
| `cleanup/Remove-FhirData.ps1` | 151 | Purges FHIR data via `$bulk-delete` |

### 6.3 Python Applications

| File | Lines | Purpose |
|------|-------|---------|
| `fhir-loader/load_fhir.py` | 1055 | Download Synthea bundles, transform, upload to FHIR, create device associations |
| `dicom-loader/load_dicom.py` | 319 | TCIA download, re-tag, ADLS upload, ImagingStudy creation |
| `dicom-loader/tcia_client.py` | 45 | TCIA REST API client with retry logic |
| `dicom-loader/dicom_retagger.py` | 81 | pydicom-based DICOM re-tagging |
| `emulator.py` | 123 | Masimo pulse oximeter telemetry emulator (streams to Event Hub) |
| `create-device-associations.py` | 236 | Standalone device-patient linkage script |

### 6.4 Infrastructure as Code (Bicep)

| File | Lines | Purpose |
|------|-------|---------|
| `bicep/infra.bicep` | 111 | Event Hub, ACR, Key Vault |
| `bicep/emulator.bicep` | 68 | Emulator ACI container |
| `bicep/fhir-infra.bicep` | 204 | HDS workspace, FHIR service, storage, managed identity, RBAC |
| `bicep/fhir-loader-job.bicep` | 75 | FHIR loader ACI job |
| `bicep/synthea-job.bicep` | 71 | Synthea generator ACI job |
| `bicep/dicom-infra.bicep` | 49 | DICOM service (unused — replaced by ADLS approach) |
| `bicep/dicom-loader-job.bicep` | 90 | DICOM loader ACI job |

### 6.5 Configuration & Data

| File | Purpose |
|------|---------|
| `fhir-loader/device_registry.json` | 100 Masimo device definitions + qualifying condition codes |
| `fhir-loader/atlanta_providers.json` | Atlanta healthcare organizations + practitioners |
| `dicom-loader/condition_modality_map.json` | SNOMED code → TCIA collection/modality mapping |
| `synthea/synthea.properties` | Synthea generator configuration (Atlanta demographics) |

### 6.6 Documentation

| File | Purpose |
|------|---------|
| `README.md` | Project overview, architecture diagrams, deployment guide |
| `PRD.md` | This document |
| `TODO-ITEMS.MD` | Prioritized backlog |
| `fabric-rti/HDS-SETUP-GUIDE.md` | Healthcare Data Solutions manual setup walkthrough |
| `docs/FABRIC-IQ-ONTOLOGY-PLAN.md` | Ontology design plan |
| `docs/ONTOLOGY-SETUP-GUIDE.md` | Ontology manual setup guide |
| `fabric-rti/dashboard/DASHBOARD-GUIDE.md` | Real-time dashboard setup |

---

## 7. Prerequisites

### 7.1 Azure
- Azure subscription with Contributor access
- Azure CLI installed and authenticated (`az login`)
- Permissions to create: Resource Groups, HDS workspaces, FHIR services, Storage Accounts, Container Registry, Container Instances, Managed Identities, Event Hubs

### 7.2 Fabric
- Microsoft Fabric capacity (F2 or higher)
- Fabric workspace with admin permissions
- Healthcare Data Solutions tenant settings enabled

### 7.3 Local Development
- PowerShell 7+
- Python 3.10+ (for local testing of DICOM loader)
- Azure CLI
- Az PowerShell modules (`Az.Accounts`, `Az.Storage`)

---

## 8. Environment Variables

### 8.1 DICOM Loader Container

| Variable | Description | Default |
|----------|-------------|---------|
| `FHIR_SERVICE_URL` | FHIR service endpoint | Required |
| `STORAGE_ACCOUNT` | ADLS Gen2 storage account name | Required |
| `DICOM_CONTAINER` | Blob container for .dcm output | `dicom-output` |
| `AZURE_CLIENT_ID` | Managed identity client ID | Required |
| `TCIA_COLLECTION` | TCIA collection name | `LIDC-IDRI` |
| `STUDY_COUNT` | Max studies to process | `100` |

### 8.2 FHIR Loader Container

| Variable | Description | Default |
|----------|-------------|---------|
| `FHIR_SERVICE_URL` | FHIR service endpoint | Required |
| `STORAGE_ACCOUNT` | Storage account with Synthea output | Required |
| `CONTAINER_NAME` | Blob container with FHIR bundles | Required |
| `DEVICE_COUNT` | Number of Masimo devices to create | `100` |
| `AZURE_CLIENT_ID` | Managed identity client ID | Required |

---

## 9. FHIR Resource Model

| Resource | Count | Key Fields |
|----------|-------|------------|
| Patient | ~10,000 | name, birthDate, gender, address (Atlanta, GA) |
| Encounter | ~250,000 | type, period, practitioner, organization, location |
| Condition | ~300,000 | code (SNOMED), clinicalStatus, onsetDateTime |
| Observation | ~3,000,000 | code, valueQuantity, effectiveDateTime |
| MedicationRequest | ~150,000 | medicationCodeableConcept, status, authoredOn |
| Procedure | ~100,000 | code, performedDateTime |
| Immunization | ~50,000 | vaccineCode, occurrenceDateTime |
| Device | 100 | Masimo Radius-7, serialNumber MSM7-ATL-NNNNN |
| Basic (DeviceAssociation) | ≤100 | subject → Patient, extension → Device |
| ImagingStudy | ≤100 | subject → Patient, modality (CT/CR), series, instances |

---

## 10. Clinical Alert Model

| Tier | SpO₂ Threshold | Pulse Rate | Condition Escalation |
|------|----------------|------------|---------------------|
| ⚠️ Warning | < 94% | > 110 or < 50 bpm | Any patient |
| 🔶 Urgent | < 90% | > 130 or < 45 bpm | OR has COPD/CHF |
| 🔴 Critical | < 85% | > 150 or < 40 bpm | AND has COPD/CHF |

---

## 11. Known Limitations

1. **HDS requires manual portal setup** — Clinical Foundations and scipy cannot be deployed via REST API
2. **DICOM metadata only** — Pixel data is stored in ADLS Gen2 but not rendered; no DICOM viewer is included
3. **Data Agents cannot cross-datasource join** — Agents run separate KQL and SQL queries, correlating in the response
4. **TCIA rate limits** — DICOM downloads may time out for large collections; the loader retries but is limited to ~100 studies per run
5. **Synthea data is synthetic** — Patient demographics are statistically realistic but not real; clinical narratives are formulaic
6. **No CI/CD pipeline** — Deployment is manual via PowerShell; no GitHub Actions or Azure DevOps integration
7. **Single-region** — All resources deploy to one Azure region (default: eastus)

---

## 12. Future Roadmap

See `TODO-ITEMS.MD` for the full prioritized backlog. Key items:

- **Power BI dashboards** — Semantic models + patient population + clinical outcomes reports
- **Data Activator** — Real-time alert triggers via Teams/email
- **SMART on FHIR** — Third-party EHR integration (Epic, Cerner)
- **ML risk stratification** — Patient readmission prediction model in Fabric Spark
- **CI/CD** — GitHub Actions pipeline for Bicep validation + agent deployment + integration testing
- **HIPAA compliance** — Audit logging, CMK, sensitivity labels, BAA checklist

---

## 13. How to Extend This Project

### Add a New FHIR Resource Type
1. Ensure Synthea generates the resource (check `synthea.properties`)
2. The FHIR loader (`load_fhir.py`) handles all Bundle resources generically — no code changes needed
3. HDS will automatically ingest the new resource type into the Silver Lakehouse
4. Add a KQL shortcut in `04-hds-enrichment-example.kql` for the new Silver table
5. Update Data Agent instructions to include the new table

### Add a New TCIA Collection
1. Edit `dicom-loader/condition_modality_map.json` to add mappings
2. Pass `-tciaCollection` parameter to the Bicep template or environment variable
3. The loader will download from the specified collection

### Add a New Data Agent
1. Copy the pattern in `deploy-data-agents.ps1`
2. Define instructions, few-shot examples, and datasource bindings
3. Use the Fabric REST API to create: Agent → add datasources → set instructions → add examples

### Deploy to a Different Region
1. Change `-Location` parameter on `Deploy-All.ps1`
2. Ensure the chosen region supports HDS, Fabric, and Event Hub

---

*This document serves as the technical specification and architectural reference for the Medical Device FHIR Integration Platform. It is intended for developers who want to understand, maintain, or extend the solution.*
