# Deployment Orchestrator — FastAPI Backend + React UI

The orchestrator provides a visual deployment experience for the Medical Device FHIR Integration Platform. It consists of a Python FastAPI backend that calls the same PowerShell scripts (`Deploy-All.ps1`, `Teardown-All.ps1`) and a React + Fluent UI frontend.

## Quick Start

> **Prerequisites are required for both the Orchestrator UI and command-line (`Deploy-All.ps1`) deployments.** The setup script detects your OS (Windows, macOS, Linux) and provides platform-specific install commands for anything that's missing.

From the repo root, run the setup script to install all prerequisites:

```bash
# Windows
.\setup-prereqs.ps1

# macOS / Linux
chmod +x setup-prereqs.sh && ./setup-prereqs.sh
```

To check without installing anything: `.\setup-prereqs.ps1 -CheckOnly`

Then start both services:

```bash
# Terminal 1 — Backend (port 7071)
cd orchestrator
.venv\Scripts\Activate.ps1    # Windows
# source .venv/bin/activate    # macOS/Linux
python local_server.py

# Terminal 2 — Frontend (port 5173)
cd orchestrator-ui
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) to access the UI.

## Architecture

- **FastAPI Backend** — REST API that invokes PowerShell deployment scripts, streams logs in real-time, and manages deployment state in SQLite
- **React Frontend** — Fluent UI v9 dashboard with Deploy wizard, Run History, Teardown scanner, and Phase Monitor
- **SQLite Database** — Persistent deployment/teardown history, resource locks, and form history

## Runtime Database Files

The orchestrator uses a local SQLite database under `orchestrator/shared/` at runtime.
You may see sidecar files such as `orchestrator.db-wal` and `orchestrator.db-shm` while
the app is running. These are SQLite write-ahead logging artifacts, are machine-local,
and are not required for end users to build or run from source in a fresh environment.
They should remain gitignored.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/deploy/start` | Start a new deployment |
| GET | `/api/deploy/{instanceId}/status` | Get deployment status |
| POST | `/api/deploy/{instanceId}/resume-hds` | Resume after manual HDS step |
| POST | `/api/deploy/{instanceId}/cancel` | Cancel a running deployment |
| POST | `/api/teardown/start` | Start teardown |
| GET | `/api/deployments` | List deployment history |
| DELETE | `/api/deploy/{instanceId}` | Delete a deployment record |
| POST | `/api/deployments/clear` | Clear all deployment history |
| GET | `/api/deploy/check-existing` | Check for prior deployment by workspace/RG |
| GET | `/api/scan/subscriptions` | List Azure subscriptions |
| POST | `/api/scan/resources/start` | Start incremental teardown resource scan |
| GET | `/api/scan/resources/{scanId}` | Poll scan progress |
| GET | `/api/scan/capacities` | List Fabric capacities |
| GET/POST/DELETE | `/api/locks/{resourceId}` | Manage teardown resource locks |
| GET | `/api/deployment-capacity/{rgName}` | Look up capacity for a resource group |

## UI Pages

| Page | Route | Description |
|------|-------|-------------|
| **Deploy** | `/` | Deployment wizard with naming convention, capacity selection, patient reuse detection |
| **History** | `/history` | Run history with filters (type, name, date range), deployment/teardown badges |
| **Teardown** | `/teardown` | Resource scanner with incremental discovery, paired RG/workspace highlighting, locks |
| **Monitor** | `/monitor/:id` | Real-time phase progress with milestone track, phased log routing, resource verification |
