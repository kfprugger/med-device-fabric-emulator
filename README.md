# Medical Device FHIR Integration Platform

A complete, deployable reference architecture that unifies healthcare EHR data and real-time medical device telemetry on Microsoft Fabric — from ingestion to AI-powered clinical queries in a single workspace.

![30,000 ft Data Journey — From Source Data to Business Intelligence: Real-Time vs. Batch](docs/images/Simple%20Diagram.png)

> **Ready to deploy?** [Skip to Quick Start ⬇](#-quick-start)


#### Video Explainer
**▶ [Watch the video explainer](https://aka.ms/fabrichlsrti)**

**What this solution demonstrates:**
- **Real-Time Intelligence** — Masimo pulse oximeter telemetry streams through Eventstream into Eventhouse with KQL-based clinical alert detection (SpO2 drops, abnormal pulse rates) in seconds
- **Healthcare Data Solutions** — 10K synthetic FHIR R4 patients (5M+ clinical resources) flow into a Silver Lakehouse via Fabric's native HDS connector with zero custom ETL
- **DICOM Medical Imaging** — Real TCIA chest CT studies are downloaded, re-tagged with Synthea patient identifiers, stored in ADLS Gen2, and ingested into Fabric HDS via a OneLake shortcut and the imaging pipeline
- **Data Agents** — Two natural-language AI agents (Patient 360 + Clinical Triage) let users ask questions like *"Show me all patients with SpO2 below 90 and their active conditions"* — federating across KQL telemetry and Lakehouse clinical data in one response
- **Cohorting Toolkit** — Power BI imaging report (Direct Lake) + OHIF DICOM Viewer + Cohorting Data Agent deployed via the companion [FabricDicomCohortingToolkit](../FabricDicomCohortingToolkit/) repo
- **Fabric IQ Ontology** — A 9-entity semantic layer (Patient, Device, Encounter, Condition, MedicationRequest, Observation, DeviceAssociation, ClinicalAlert, DeviceTelemetry) with relationships across Lakehouse and Eventhouse, bound to all Data Agents
- **Data Activator** — A Reflex item with KQL source (`fn_ClinicalAlerts`), Device object, 6 attributes, and an email rule that alerts on CRITICAL/URGENT SpO2 events — deployed fully programmatically via the Fabric REST API
- **CMS Quality & Claims** — 7 CMS eCQM quality measures (CMS122, CMS165, CMS69, CMS127, CMS147, CMS134, CMS144), 3 HEDIS medication adherence PDC classes (diabetes, RAS antagonists, statins), claims analytics from Synthea-generated ExplanationOfBenefit/Coverage data, and a 6-page CMS Quality Scorecard Power BI report
- **OneLake** — One copy of the data, queryable from KQL, Spark, SQL, and Power BI without duplication

The entire solution deploys in under 2 hours via the **Orchestrator UI** (browser-based deployment wizard) or a single command (`Deploy-All.ps1`) and touches eight Fabric workloads: Real-Time Intelligence, Data Engineering, Data Warehouse, Data Science, Data Agents, Data Activator, Power BI, and Healthcare Data Solutions.

---

## 📑 Table of Contents

| Phase | Description | Guide |
|-------|-------------|-------|
| **Phase 1** | Azure infrastructure, FHIR + DICOM data generation, Fabric RTI pipeline, manual HDS deployment | [Phase 1 — Infrastructure & Data](docs/phase-1-infrastructure-and-ingestion.md) |
| **Phase 2** | HDS Silver Lakehouse shortcuts, enriched clinical alerts, HDS pipelines, Data Agents | [Phase 2 — Analytics & AI Agents](docs/phase-2-hds-enrichment-and-agents.md) |
| **Phase 3** | Cohorting Agent, OHIF DICOM Viewer, materialization notebook, Power BI report | [Phase 3 — Imaging & Reporting](docs/phase-3-imaging-and-cohorting.md) |
| **Phase 4** | Ontology deployment, agent ontology binding, Data Activator (email alerts) | [Phase 4 — Semantic Layer & Alerts](docs/phase-4-ontology-and-activator.md) |
| **Phase 5** | Claims materialization, CMS quality measures (7 eCQMs), medication adherence (PDC), Power BI Quality Scorecard | [Phase 5 — CMS Quality & Claims](docs/phase-5-cms-quality-and-claims.md) |

**Additional guides:**
- [Orchestrator UI](orchestrator/README.md) — Setup and usage for the browser-based deployment dashboard
- [HDS Setup Guide](fabric-rti/HDS-SETUP-GUIDE.md) — Manual HDS deployment walkthrough
- [Dashboard Guide](fabric-rti/dashboard/DASHBOARD-GUIDE.md) — Real-time dashboard details
- [Ontology Setup Guide](docs/ONTOLOGY-SETUP-GUIDE.md) — Fabric IQ Ontology configuration
- [Ontology Design Plan](.ai/FABRIC-IQ-ONTOLOGY-PLAN.md) — Data model and entity relationships

**AI/planning artifacts** (in [`.ai/`](.ai/)):
- [OpenSpec](.ai/OPENSPEC.md) — Full project specification
- [PRD](.ai/PRD.md) — Product Requirements Document
- [TODO Items](.ai/TODO-ITEMS.MD) — Prioritized backlog
- [Ontology Design Plan](.ai/FABRIC-IQ-ONTOLOGY-PLAN.md) — Entity model design
- [Theming](.ai/themeing.md) — Fabric UI color/theme reference

---

## 🏗️ Architecture

### Platform Architecture Overview

A high-altitude view of the platform — data sources flow through Azure's ingestion layer into Fabric's four phased workloads, and out to the clinical consumers who actually use it.

```mermaid
flowchart LR
    classDef sources fill:#f5f5f5,stroke:#888,stroke-width:1px,color:#333
    classDef azure fill:#E5F1FB,stroke:#0078D4,stroke-width:2px,color:#004578
    classDef fabric fill:#E0F2F1,stroke:#117865,stroke-width:2px,color:#0A4A3E
    classDef phase1 fill:#FFF4E5,stroke:#FF8C00,stroke-width:1.5px,color:#663300
    classDef phase2 fill:#E8F8E8,stroke:#107C10,stroke-width:1.5px,color:#0B4D0B
    classDef phase3 fill:#FDE7E9,stroke:#D13438,stroke-width:1.5px,color:#7A1F22
    classDef phase4 fill:#E8E8FF,stroke:#5C2D91,stroke-width:1.5px,color:#2E1757
    classDef consumer fill:#FFFCE5,stroke:#B89229,stroke-width:1.5px,color:#5C4615

    subgraph SRC["🌐 Data Sources"]
        direction TB
        S1["🧬 Synthea<br/>Patient Generator"]
        S2["🩻 TCIA<br/>Public DICOM"]
        S3["📟 Masimo Emulator<br/>Pulse Oximetry"]
    end

    subgraph AZ["☁️ Azure Ingestion & Foundational Layer"]
        direction TB
        A1["📡 Event Hub"]
        A2["🏥 FHIR + DICOM<br/>Services"]
        A3["💾 ADLS Gen2"]
    end

    subgraph FAB["🟢 Microsoft Fabric Workspace"]
        direction TB
        subgraph P1["Phase 1 · Real-Time Intelligence"]
            F1["⚡ Eventstream → Eventhouse<br/>KQL Clinical Alerts"]
        end
        subgraph P2["Phase 2 · HDS & AI Agents"]
            F2["🥉→🥈→🥇 Medallion Lakehouses"]
        end
        subgraph P3["Phase 3 · Imaging & Cohorting"]
            F3["🖼️ Reporting Lakehouse"]
        end
        subgraph P4["Phase 4 · Semantic Layer"]
            F4["🧠 Fabric IQ Ontology<br/>+ Data Activator"]
        end
    end

    subgraph OUT["👥 Clinical Consumers"]
        direction TB
        C1["📊 Power BI<br/>Direct Lake"]
        C2["🔍 OHIF<br/>DICOM Viewer"]
        C3["💬 Natural-Language Clinical Q&A<br/>Patient 360 · Triage · Cohorting Agents"]
        C4["📧 Email Alerts<br/>SpO₂ Critical"]
    end

    S1 --> A2
    S2 --> A3
    S3 --> A1

    A1 --> F1
    A2 --> A3
    A3 --> F2

    F1 --> F2
    F2 --> F3
    F2 --> F4
    F1 --> F4

    F3 --> C1
    F3 --> C2
    F2 --> C3
    F4 --> C3
    F4 --> C4

    class S1,S2,S3 sources
    class A1,A2,A3 azure
    class F1 phase1
    class F2 phase2
    class F3 phase3
    class F4 phase4
    class C1,C2,C3,C4 consumer
    class SRC sources
    class AZ azure
    class FAB fabric
    class OUT consumer
```

<details>
<summary><strong>🔍 View the detailed draw.io architecture diagram</strong></summary>

Open [`docs/images/architecture-diagram.drawio`](docs/images/architecture-diagram.drawio) in [draw.io](https://app.diagrams.net/) or the VS Code Draw.io Integration extension for the full component-level view.

</details>

### End-to-End Data Flow

```mermaid
flowchart TB
    subgraph EXT["External Sources"]
        SYNTH["Synthea\n(Patient Generator)"]
        TCIA["TCIA\n(Public DICOM)"]
        EMUL["Masimo Emulator\n(Pulse Oximeter)"]
    end

    subgraph AZ["Azure Resource Group"]
        EH["Event Hub\n(telemetry-stream)"]
        FHIR_SVC["FHIR R4 Service"]
        DICOM_SVC["DICOM Service"]
        ADLS["ADLS Gen2\n(fhir-export +\ndicom-output)"]
        ACR["Container Registry"]
    end

    subgraph FAB["Microsoft Fabric Workspace"]
        direction TB

        subgraph P1["Phase 1 — Real-Time Intelligence"]
            ES["Eventstream"]
            EVH["Eventhouse\n(MasimoKQLDB)"]
            DASH1["Real-Time Dashboard"]
        end

        subgraph P2["Phase 2 — HDS Enrichment"]
            BZ["Bronze Lakehouse"]
            SLV["Silver Lakehouse"]
            GOLD["Gold OMOP Lakehouse"]
            SC["KQL Shortcuts\n(6 Silver tables)"]
            MAP["Clinical Alerts Map"]
            DA["Data Agents\n(Patient 360 +\nClinical Triage)"]
        end

        subgraph P3["Phase 3 — Imaging"]
            RPT["Reporting Lakehouse"]
            PBI["Power BI Report\n(Direct Lake)"]
            COHORT["Cohorting Agent"]
        end

        subgraph P4["Phase 4 — Ontology & Activator"]
            ONT["ClinicalDeviceOntology\n(9 entity types)"]
            ACT["Data Activator\n(Email Alerts)"]
        end
    end

    subgraph VIEWER["Azure (Viewer)"]
        OHIF["OHIF Viewer\n(Static Web App)"]
    end

    EMUL --> EH --> ES --> EVH --> DASH1
    SYNTH --> ADLS --> FHIR_SVC
    TCIA --> ADLS
    FHIR_SVC -->|"$export"| ADLS
    ADLS -->|"Shortcut"| BZ --> SLV --> GOLD
    SLV -.->|"Delta shortcuts"| SC --> EVH
    SC --> MAP
    EVH --> DA
    SLV --> DA
    GOLD --> COHORT
    SLV --> RPT --> PBI
    RPT -.-> OHIF
    ONT -.-> DA
    ONT -.-> COHORT
    EVH --> ACT

    style EXT fill:#f5f5f5,stroke:#999,stroke-dasharray:5
    style AZ fill:#e6f3ff,stroke:#0078d4,stroke-width:2px
    style FAB fill:#f0e6ff,stroke:#8000d4,stroke-width:2px
    style VIEWER fill:#e6f3ff,stroke:#0078d4,stroke-width:2px
    style P1 fill:#fff3e6,stroke:#ff8c00
    style P2 fill:#e6ffe6,stroke:#00a000
    style P3 fill:#ffe6e6,stroke:#d40000
    style P4 fill:#e6e6ff,stroke:#4040d4
```

### Deployment Sequence

| Step | Script | Phase | What It Does |
|------|--------|-------|--------------|
| 1 | `phase-1/deploy.ps1` | 1 | Event Hub, ACR, Key Vault, emulator ACI |
| 1b | Fabric API (inline) | 1 | Fabric workspace, capacity, managed identity |
| 2 | `phase-1/deploy-fhir.ps1 -SkipDicom` | 1 | FHIR Service, Synthea, FHIR Loader, Device Associations |
| 2b | `phase-1/deploy-fhir.ps1 -RunDicom` | 1 | DICOM loader, TCIA download, re-tag, ADLS upload |
| 3 | `deploy-fabric-rti.ps1` | 1 | Eventhouse, Eventstream, KQL, dashboard, FHIR $export |
| 4 | **Manual** (Fabric portal) | — | Deploy HDS + add scipy + run pipelines |
| 5 | `deploy-fabric-rti.ps1 -Phase2` | 2 | Silver shortcuts, enriched alerts, alerts map |
| 5b | `phase-2/storage-access-trusted-workspace.ps1` | 2 | DICOM shortcut + HDS clinical/imaging/OMOP pipelines |
| 6 | `phase-2/deploy-data-agents.ps1` | 2 | Patient 360 + Clinical Triage agents |
| 7 | FabricDicomCohortingToolkit | 3 | Cohorting Agent, DICOM Viewer, reporting notebook, PBI report |
| 8 | `phase-4/deploy-ontology.ps1` | 4 | ClinicalDeviceOntology (9 entity types, 5 relationships) |
| 9 | `Deploy-All.ps1 -Phase4` | 4 | Ontology binding to agents, Data Activator (Reflex) with email rule |

### FHIR Resource Relationships

```mermaid
erDiagram
    Patient ||--o{ Encounter : "has"
    Patient ||--o{ Condition : "has"
    Patient ||--o{ MedicationRequest : "prescribed"
    Patient ||--o{ Observation : "has"
    Patient ||--o{ Immunization : "received"
    Patient ||--o{ Procedure : "underwent"
    Patient ||--o{ DeviceAssociation : "linked to"
    Patient ||--o{ ImagingStudy : "has imaging"
    
    Encounter }o--|| Practitioner : "performed by"
    Encounter }o--|| Organization : "at"
    Encounter }o--|| Location : "location"
    
    MedicationRequest }o--|| Practitioner : "prescribed by"
    MedicationRequest }o--|| Encounter : "during"
    
    Condition }o--|| Encounter : "diagnosed during"
    
    DeviceAssociation }o--|| Device : "uses"
    
    Device {
        string id PK
        string serialNumber
        string model "Masimo Radius-7"
        string manufacturer "Masimo"
        code type "Pulse Oximeter"
    }
    
    Patient {
        string id PK
        string name
        date birthDate
        code gender
        address address "Atlanta, GA"
    }
```

### Fabric IQ Ontology (Semantic Layer)

```mermaid
graph LR
    Patient -->|has| Encounter
    Patient -->|has| Condition
    Patient -->|has| Observation
    Patient -->|has| MedicationRequest
    Patient -->|linkedTo| Device
    Device -->|generates| DeviceTelemetry
    Device -->|triggers| ClinicalAlert
    ClinicalAlert -->|concerns| Patient

    style Patient fill:#4CAF50,color:#fff
    style Device fill:#2196F3,color:#fff
    style DeviceTelemetry fill:#FF9800,color:#fff
    style ClinicalAlert fill:#f44336,color:#fff
    style Encounter fill:#9C27B0,color:#fff
    style Condition fill:#009688,color:#fff
    style Observation fill:#795548,color:#fff
    style MedicationRequest fill:#607D8B,color:#fff
```

---

## 🚀 Quick Start

![Orchestrator UI deploy walkthrough](docs/gif/demo_deploy.gif)

> **The Orchestrator UI is the supported, recommended path for every deployment.** The CLI scripts exist for automation, CI/CD, and advanced scenarios — but interactive deployments, monitoring, and teardown should all go through the browser-based wizard.

### Prerequisites

> **These prerequisites are required for both the Orchestrator UI and command-line (`Deploy-All.ps1`) deployments.** The setup script detects your OS and provides platform-specific install commands for anything that's missing.

Run the setup script to check and install all dependencies:

```powershell
# Windows (PowerShell)
.\setup-prereqs.ps1

# macOS / Linux (bash — installs PowerShell Core if missing)
chmod +x setup-prereqs.sh
./setup-prereqs.sh
```

This checks and installs: PowerShell 7+, Azure CLI + Bicep, Az PowerShell module, Python 3.10+, Node.js 18+, Git, plus it creates the Python virtual environment and installs both backend and frontend dependencies.

To check without installing anything: `.\setup-prereqs.ps1 -CheckOnly`

**Required Azure/Fabric:**
- Azure subscription with permissions to create resource groups, Health Data Services, ACR, ACI, Storage, and Managed Identities
- Microsoft Fabric capacity (**paid F-SKU** such as F2 or F64 — trial capacities cannot deploy Healthcare Data Solutions)
- **NOTE:** If you do not use a paid F-SKU, you will not be able to deploy Healthcare Data Solutions which is core to the entire solution
- Fabric tenant settings enabled: **Data Activator**, **Copilot**, and **Azure OpenAI Service**

### Deploy with the Orchestrator UI

The Orchestrator UI is a browser-based deployment dashboard that handles the entire lifecycle — wizard-driven deploys, real-time phase monitoring, parallel teardowns, resource scanning, lock protection, and deployment history. A background resource scan fires the moment the UI loads, so every tab has its data ready before you click.

**1. Start everything with one command:**

```powershell
.\Start-WebUI.ps1
```

That's it. `Start-WebUI.ps1` will:
- Run `setup-prereqs.ps1 -CheckOnly` to verify PowerShell, Azure CLI, Python, Node.js, venv, and npm dependencies are all in place (use `-InstallPrereqs` to auto-install anything missing).
- Start the FastAPI backend on port **7071** (activating the venv for you).
- Start the Vite frontend on port **5173**.
- Detect and offer to reclaim ports already in use.

To stop everything later:

```powershell
.\Stop-WebUI.ps1 -Force
```

**2. Open the UI:** Navigate to [http://localhost:5173](http://localhost:5173).

**3. Deploy:**
- Click the **Deploy** tab.
- The Azure subscription and Fabric capacity pickers are preloaded by the background scan.
- Enter a deployment name (e.g., `med-rojo-0408`), patient count, and alert email.
- Click **Start Deployment** — the UI orchestrates all four phases automatically and streams progress.

**4. Monitor:** The deployment monitor shows real-time milestone tracking, phased log streaming, elapsed time, and a resource panel populated as each component comes up. You can safely close and re-open the browser tab; progress resumes from the backend.

**5. Teardown:**
- Click the **Teardown** tab — resource candidates (Fabric workspaces, Azure RGs, orphaned Entra SPNs) are already listed from the background scan.
- Check the items you want to remove. Paired workspace + RG deployments are highlighted together automatically.
- Click **Delete Selected** — each selected workspace and each selected Azure RG fires as its own independent parallel teardown job. The UI navigates to history so you can watch them all progress side-by-side.
- Fabric workspace deletion cascades to every item inside (no item-by-item loop); only the workspace managed identity is cleaned up separately as its own phase.

![Deployment Monitor](docs/images/example%20deploy%20and%20teardown.png)

<details>
<summary><strong>Manually starting the servers (if you don't want to use Start-WebUI.ps1)</strong></summary>

```powershell
# Terminal 1 — backend
cd orchestrator
.\.venv\Scripts\Activate.ps1   # Windows
# source .venv/bin/activate     # macOS/Linux
python local_server.py

# Terminal 2 — frontend
cd orchestrator-ui
npm run dev
```

</details>

---

### CLI & Automation (Advanced)

<details>
<summary><strong>Click to expand — PowerShell-only deployment for CI/CD or headless scenarios</strong></summary>

The UI calls the same underlying scripts shown below. Use these directly only if you are scripting deployments, embedding in a pipeline, or debugging a specific phase.

```powershell
# Full deploy (all phases, ~90 min):
.\Deploy-All.ps1 `
    -ResourceGroupName "rg-medtech-rti-fhir" `
    -Location "eastus" `
    -FabricWorkspaceName "med-device-rti-hds" `
    -AdminSecurityGroup "sg-azure-admins" `
    -PatientCount 100 `
    -AlertEmail "nurse@hospital.com" `
    -Tags @{SecurityControl='Ignore'}

# ── Or run individual phases: ──

# Phase 1: Azure infra + FHIR data + Fabric RTI (~25 min)
.\Deploy-All.ps1 `
    -ResourceGroupName "rg-medtech-rti-fhir" `
    -Location "eastus" `
    -FabricWorkspaceName "med-device-rti-hds" `
    -AdminSecurityGroup "sg-azure-admins" `
    -PatientCount 100 `
    -Tags @{SecurityControl='Ignore'}

# ── Manual: Deploy HDS in Fabric portal (see Phase 1 guide) ──

# Phase 2: HDS enrichment + Data Agents (~35 min)
.\Deploy-All.ps1 -Phase2 `
    -ResourceGroupName "rg-medtech-rti-fhir" `
    -Location "eastus" `
    -FabricWorkspaceName "med-device-rti-hds" `
    -Tags @{SecurityControl='Ignore'}

# Phase 3: Imaging toolkit (~10 min)
.\Deploy-All.ps1 -Phase3 `
    -FabricWorkspaceName "med-device-rti-hds" `
    -Location "eastus" `
    -ResourceGroupName "rg-medtech-rti-fhir" `
    -DicomToolkitPath "C:\git\FabricDicomCohortingToolkit"

# Phase 4: Ontology + Data Activator (~5 min)
.\Deploy-All.ps1 -Phase4 `
    -FabricWorkspaceName "med-device-rti-hds" `
    -Location "eastus" `
    -AlertEmail "nurse@hospital.com" `
    -AlertTierThreshold "URGENT" `
    -AlertCooldownMinutes 15
```

**CLI Teardown:**

```powershell
# Full teardown: Azure RGs + Fabric workspace + DICOM viewer
.\Teardown-All.ps1 -FabricWorkspaceName "med-device-rti-hds" `
    -ResourceGroupName "rg-med-device-rti" -Force -Wait
```

> For interactive teardown, prefer the UI — it scans all resources, highlights paired workspace + RG pairs, fires each deletion in its own parallel job, and uses the fast Fabric workspace-cascade path instead of iterating items.

</details>

---

## 📁 Project Structure

```
med-device-fabric-emulator/
├── setup-prereqs.ps1           # Cross-platform prerequisite installer
├── Deploy-All.ps1              # Full orchestrator (all phases)
├── deploy-fabric-rti.ps1       # Phase 1 + 2: Fabric RTI
├── Teardown-All.ps1            # Cleanup orchestrator
├── phase-1/
│   ├── deploy.ps1              # Azure infra (Event Hub, ACR, emulator ACI)
│   └── deploy-fhir.ps1         # FHIR + DICOM pipeline
├── phase-2/
│   ├── deploy-data-agents.ps1  # Data Agents (Patient 360 + Clinical Triage)
│   └── storage-access-trusted-workspace.ps1  # HDS pipeline triggers
├── phase-4/
│   └── deploy-ontology.ps1     # Fabric IQ Ontology
├── utilities/
│   ├── update-agents-inline.ps1  # Quick-update agent definitions
│   └── run-kql-scripts.ps1     # Manual KQL script runner
├── create-device-associations.py  # Link devices to patients
├── emulator.py                 # Masimo device emulator
├── Dockerfile                  # Emulator container
├── orchestrator/               # Deployment backend (FastAPI + Python)
│   ├── local_server.py         # Backend API server (port 7071)
│   ├── requirements.txt        # Python dependencies
│   ├── activities/             # Deployment activities (PowerShell invocation)
│   └── shared/                 # Fabric client, Kusto client, database
├── orchestrator-ui/            # Deployment frontend (React + Fluent UI)
│   ├── package.json            # Node.js dependencies
│   └── src/
│       ├── pages/              # Deploy wizard, History, Teardown, Monitor
│       └── components/         # PhaseCard, AllLogsStream, ResourcesPanel
├── bicep/                      # ARM/Bicep templates
├── cleanup/                    # Teardown scripts
├── dicom-loader/               # TCIA download + DICOM re-tagging
├── .ai/                        # AI/planning artifacts (specs, PRD, TODOs)
├── docs/
│   ├── phase-1-infrastructure-and-ingestion.md
│   ├── phase-2-hds-enrichment-and-agents.md
│   ├── phase-3-imaging-and-cohorting.md
│   ├── phase-4-ontology-and-activator.md
│   ├── ONTOLOGY-SETUP-GUIDE.md
│   └── images/
├── fabric-rti/                 # KQL scripts, dashboards, HDS guide
├── fhir-loader/                # FHIR bundle loader
└── synthea/                    # Patient generator config
```

---

## 🔐 Authentication & Security

The solution uses **User-Assigned Managed Identities** for all service-to-service communication:
- `FHIR Data Contributor` — read/write FHIR Service
- `Storage Blob Data Contributor` — access Synthea output + $export blobs
- `Azure Event Hubs Data Sender` — emulator → Event Hub
- `AcrPull` — pull container images from ACR

No connection strings or secrets are stored in code. The Fabric workspace uses a provisioned managed identity for trusted workspace access to ADLS Gen2.

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Synthea](https://synthetichealth.github.io/synthea/) - Synthetic patient generator
- [Azure Health Data Services](https://azure.microsoft.com/en-us/products/health-data-services/) - FHIR platform emulating an EHR integration
- [Masimo](https://www.masimo.com/) - Medical device specifications reference
- [Microsoft Fabric](https://www.microsoft.com/en-us/microsoft-fabric) - Real-Time Intelligence, Analytics and Full Data Estate Managmement platform
- [Healthcare Data Solutions](https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/overview) - FHIR data foundations on Fabric
- [Fabric IQ](https://learn.microsoft.com/fabric/iq/overview) - Unified semantic layer and ontology workload
- [Ontology (preview)](https://learn.microsoft.com/fabric/iq/ontology/overview) - Enterprise vocabulary and data binding
- [OHIF Viewer](https://ohif.org) - Open-source DICOM viewer (MIT)
- [TCIA](https://www.cancerimagingarchive.net/) - The Cancer Imaging Archive (public DICOM studies)