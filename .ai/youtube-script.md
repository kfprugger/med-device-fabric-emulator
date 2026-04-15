# YouTube Script — Medical Device FHIR Integration Platform

**Target Length:** 12–15 minutes  
**Tone:** Technical walkthrough, conversational, demo-driven  
**Audience:** Fabric developers, healthcare data engineers, solution architects  

---

## INTRO (0:00–0:45)

**[ARTIFACT: Title card — "Medical Device FHIR Integration Platform on Microsoft Fabric" with the 30,000 ft architecture diagram as background]**

> What if you could stand up a complete healthcare data platform — real-time device telemetry, 10,000 synthetic patients, DICOM imaging, AI agents, and clinical alerting — all on Microsoft Fabric, in under two hours?

> That's exactly what this repo does. And in this video I'll walk you through the architecture, show you how it deploys, and highlight some recent changes we made to the repo structure that I think are worth stealing for your own projects.

> I'm Joey — let's get into it.

**[ARTIFACT: Quick flash of the GitHub repo page / star count]**

---

## THE PROBLEM (0:45–1:30)

**[ARTIFACT: Simple slide — "The Problem" with 3-4 bullet points appearing one at a time]**

> Healthcare data is fragmented. You've got EHR systems speaking FHIR, medical devices streaming telemetry in real time, DICOM imaging sitting in separate archives, and clinical teams who just want to ask a question like "show me all patients with dropping SpO2 and their active medications."

> Traditionally you'd need separate pipelines, separate stores, and a team of engineers to wire it all together. Fabric changes that — OneLake gives you one copy of data queryable from KQL, Spark, SQL, and Power BI.

> But nobody had wired up a reference implementation that touches *all* the workloads together. So we built one.

---

## DATA JOURNEY — 30,000 FT (1:30–3:30)

**[ARTIFACT: The "30,000 ft Data Journey" diagram (docs/images/Simple Diagram.png) — walk across it left to right]**

> Let me walk you through the data journey. Left to right.

> **Source data.** We have a Masimo pulse oximeter emulator — that's a Python script running in Azure Container Instances — streaming SpO2, pulse rate, and perfusion index every few seconds per device. Alongside that, Synthea generates 10,000 FHIR R4 patients with full medical histories — conditions, encounters, medications, observations. And we pull real chest CT studies from The Cancer Imaging Archive.

**[ARTIFACT: Quick cut to the emulator UI in browser (localhost:5173) showing the dashboard with devices streaming]**

> **Ingestion.** The real-time telemetry flows through Azure Event Hubs. The batch patient data goes through Azure FHIR Service. The DICOM files land in ADLS Gen2.

> **Processing — the Fabric layer.** This is where it gets interesting. Event Hubs feeds into an Eventstream, which lands in an Eventhouse with a KQL database. KQL functions detect clinical alerts in real time — SpO2 drops, abnormal pulse rates. Meanwhile, Fabric's Healthcare Data Solutions connector pulls the FHIR data into Bronze, Silver, and Gold lakehouses with zero custom ETL. DICOM data comes in through a OneLake shortcut.

**[ARTIFACT: Screenshot of the Fabric workspace showing all the items — Eventhouse, Lakehouses, Eventstream, Dashboards, Data Agents]**

> **The output layer.** Real-time dashboards, AI data agents that can query across both KQL and Lakehouse in natural language, Power BI reports with Direct Lake, an OHIF DICOM viewer, and Data Activator sending email alerts when SpO2 drops below threshold.

---

## FOUR PHASES (3:30–6:30)

**[ARTIFACT: Phase diagram — 4 colored boxes in a horizontal timeline, matching the Orchestrator UI stepper]**

> The deployment is organized into four phases. Each phase has its own scripts, its own docs, and — as of recently — its own folder.

### Phase 1 — Infrastructure & Data (3:45)

**[ARTIFACT: Screenshot of Orchestrator UI showing Phase 1 steps running / completed]**

> Phase 1 lays the foundation. It deploys the Azure resource group — Event Hub, Container Registry, Key Vault, the emulator container. Then it spins up a FHIR Service, generates synthetic patients with Synthea, loads them via a containerized FHIR Loader, and creates device-patient associations. Then it creates the Fabric workspace, Eventhouse, Eventstream, KQL database, and a real-time monitoring dashboard.

**[ARTIFACT: Split screen — left: terminal running Deploy-All.ps1 with green checkmarks; right: the Fabric portal showing the created Eventhouse]**

> Phase 1 takes about 45 minutes and touches about 15 Azure and Fabric resources.

### Phase 2 — Analytics & Agents (4:30)

**[ARTIFACT: Screenshot of the Clinical Alerts Map dashboard in Fabric]**

> Phase 2 is where the data starts talking to each other. It creates KQL shortcuts from the Silver Lakehouse into the Eventhouse — so you can join real-time telemetry with patient demographics in KQL. It deploys enriched clinical alert functions. It triggers the HDS pipelines for clinical, imaging, and OMOP data.

**[ARTIFACT: Screen recording of the Patient 360 Data Agent answering a question like "Show me the last 24 hours of vitals for patient Horacio317"]**

> And then it deploys two Data Agents. Patient 360 gives you a natural-language interface to patient data. Clinical Triage lets you ask questions that span both real-time telemetry and historical clinical records. These agents federate across KQL and Lakehouse — something you can't do with a single query today.

### Phase 3 — Imaging & Reporting (5:15)

**[ARTIFACT: Screenshot of the OHIF DICOM Viewer showing a chest CT scan, embedded in the Power BI report]**

> Phase 3 brings in the imaging toolkit from a companion repo — FabricDicomCohortingToolkit. It deploys a Cohorting Data Agent, an OHIF DICOM viewer as a Static Web App, a materialization notebook, and a Power BI report with Direct Lake. This is where you can see imaging studies alongside clinical data.

### Phase 4 — Ontology & Activator (5:45)

**[ARTIFACT: Screenshot or diagram of the ontology entity graph — 9 entities with relationship lines]**

> Phase 4 adds the semantic layer. Fabric IQ Ontology — 9 entity types: Patient, Device, Encounter, Condition, MedicationRequest, Observation, DeviceAssociation, ClinicalAlert, and DeviceTelemetry. This ontology binds to the Data Agents so they understand the schema without you having to repeat it in every instruction prompt.

**[ARTIFACT: Screenshot of the Data Activator email alert (the Fabric email with "URGENT ALERT: Horacio317 Kris249 — SpO2 93.2")]**

> And then Data Activator. A Reflex item sourced from a KQL function, with a Device object, six attributes, and an email rule. When SpO2 drops below a threshold — you get an email. Deployed fully programmatically through the Fabric REST API. No portal clicks.

---

## THE ORCHESTRATOR UI (6:30–8:00)

**[ARTIFACT: Full-screen recording of the Orchestrator UI — showing the Deploy wizard, filling in workspace name, clicking Deploy]**

> You can run all of this from the command line with `Deploy-All.ps1`. But we also built a browser-based deployment wizard.

> It's a React app with Fluent UI v9 — matching the Fabric design language. FastAPI backend on port 7071. You fill in your workspace name, subscription, pick which phases to run, and hit deploy. It streams logs in real time, tracks step durations, shows you phase progress.

**[ARTIFACT: Recording of the Orchestrator showing a deployment in progress — log streaming, phase cards turning green]**

> History page shows past deployments. Teardown page lets you clean everything up. The backend persists state to SQLite — so if you restart the server, your deployment history is still there.

**[ARTIFACT: Quick shot of the Teardown Monitor page with the phase stepper showing workspace deletion in progress]**

> One command to deploy. One click to tear down.

---

## REPO STRUCTURE — THE CLEANUP (8:00–10:30)

> Now let me talk about something less glamorous but honestly just as important — repo hygiene.

**[ARTIFACT: Side-by-side "before and after" comparison of the repo root — left: flat list of 12 .ps1 files; right: organized phase folders]**

> When we started, the repo root had twelve PowerShell scripts sitting flat. `deploy.ps1`, `deploy-fhir.ps1`, `deploy-data-agents.ps1`, `deploy-ontology.ps1`, `storage-access-trusted-workspace.ps1`, `update-agents-inline.ps1`... you get the picture. If you're new to the repo, you have no idea what order these run in or which phase they belong to.

> So we reorganized into phase directories.

**[ARTIFACT: Tree view of the new structure, maybe from VS Code's explorer panel]**

```
├── phase-1/
│   ├── deploy.ps1
│   └── deploy-fhir.ps1
├── phase-2/
│   ├── deploy-data-agents.ps1
│   └── storage-access-trusted-workspace.ps1
├── phase-4/
│   └── deploy-ontology.ps1
├── utilities/
│   ├── update-agents-inline.ps1
│   └── run-kql-scripts.ps1
```

> Entry points stay at root — `Deploy-All.ps1`, `Teardown-All.ps1`, `Start-WebUI.ps1`. Cross-phase scripts like `deploy-fabric-rti.ps1` (which handles both Phase 1 and Phase 2) also stay at root. But everything else is organized by when it runs.

> This took updating about 20 files — every caller in `Deploy-All.ps1`, the scenario manifest in the orchestrator hub, README, SKILL files, doc guides, even the orchestrator activity Python files. And we had to fix the `$ScriptDir` resolution in the moved scripts so their relative paths to `bicep/` and `synthea/` still work.

### State Tracking (9:30)

**[ARTIFACT: Screenshot of the state-tracking/ folder in VS Code with several .deployment-state-*.json files]**

> We also consolidated deployment state. Previously, each deployment dropped a `.deployment-state-{workspace}.json` file in the repo root. If you deployed to five workspaces, you had five dotfiles cluttering root.

> Now those go into `state-tracking/` — gitignored. And the orchestrator also persists resources to SQLite with a new `resources` column. So resources discovered during deployment — workspace IDs, event hub names, storage accounts — are captured in the database, not just in a JSON file that might get deleted.

### AI Planning Artifacts (10:00)

**[ARTIFACT: Screenshot of the .ai/ folder showing OPENSPEC.md, PRD.md, TODO-ITEMS.MD, etc.]**

> Last thing — we moved all the "vibe coding" artifacts into a `.ai/` directory. The OpenSpec, the PRD, the TODO list, the ontology design plan, the theming reference. These are documents that were generated during AI-assisted development — they're useful for context but they're not user-facing docs.

> The `.ai/` convention keeps them version-controlled but out of the way. And the README now links to them under an "AI/planning artifacts" section so new contributors can find them if they want the backstory.

---

## KEY TAKEAWAYS (10:30–11:30)

**[ARTIFACT: Slide with 4-5 bullet takeaways]**

> So what's worth stealing from this repo for your own projects?

> **One.** Fabric can unify real-time and batch healthcare data in a single workspace. KQL for speed, Lakehouses for depth, Data Agents to bridge them.

> **Two.** Fully automated deployment is possible for seven Fabric workloads — including Data Agents, Ontology, and Data Activator — all via the REST API. No portal clicks except for HDS.

> **Three.** Organize deployment scripts by phase, not by alphabet. Future you will thank present you.

> **Four.** Keep your AI planning artifacts — PRDs, specs, TODO lists — in a `.ai/` directory. Version-controlled, discoverable, out of the way.

> **Five.** Persist deployment state in a real database, not just JSON files on disk. It survives restarts and lets your orchestrator UI show deployment history.

---

## OUTRO (11:30–12:00)

**[ARTIFACT: Screen showing the GitHub repo URL, the aka.ms link, and a QR code]**

> The repo is open source. Link in the description. You can deploy the whole thing in about two hours — or just grab the pieces you need.

> If this was useful, hit subscribe. If you have questions, drop them in the comments. And if you build something cool with this — I'd love to hear about it.

> Thanks for watching.

**[ARTIFACT: End card with subscribe button, related video links, GitHub URL overlay]**

---

## ARTIFACT CHECKLIST

| Timestamp | Artifact | Source/Notes |
|-----------|----------|--------------|
| 0:00 | Title card with 30k ft diagram | `docs/images/Simple Diagram.png` |
| 0:40 | GitHub repo page | Browser screenshot |
| 0:45 | "The Problem" slide | Create in PowerPoint/Canva — 4 bullets |
| 1:30 | 30k ft data journey diagram | `docs/images/Simple Diagram.png` — annotate with arrows as you narrate |
| 2:15 | Emulator UI | Screen recording of `localhost:5173` |
| 2:45 | Fabric workspace overview | Screenshot of workspace item list in Fabric portal |
| 3:30 | Phase timeline diagram | Create or use Orchestrator UI stepper screenshot |
| 3:45 | Orchestrator UI Phase 1 | Screen recording |
| 4:00 | Deploy-All.ps1 terminal + Fabric portal split | Screen recording |
| 4:30 | Clinical Alerts Map dashboard | Screenshot from Fabric |
| 4:45 | Patient 360 Data Agent demo | Screen recording of agent chat |
| 5:15 | OHIF DICOM Viewer | Screenshot from deployed SWA |
| 5:45 | Ontology entity graph | Diagram from `.ai/FABRIC-IQ-ONTOLOGY-PLAN.md` or Fabric portal |
| 6:15 | Data Activator email alert | Screenshot of actual email received |
| 6:30 | Orchestrator UI deploy wizard | Screen recording — fill form → deploy |
| 7:15 | Orchestrator logs streaming | Screen recording |
| 7:45 | Teardown Monitor | Screenshot |
| 8:00 | Before/after repo root comparison | VS Code explorer side-by-side |
| 8:30 | New folder structure tree | VS Code explorer or terminal `tree` output |
| 9:30 | state-tracking/ folder | VS Code explorer screenshot |
| 10:00 | .ai/ folder contents | VS Code explorer screenshot |
| 10:30 | Key takeaways slide | Create in PowerPoint/Canva — 5 bullets |
| 11:30 | End card with repo URL + QR | Template |
