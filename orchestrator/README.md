# Deployment Orchestrator — Azure Durable Functions (Python v2)

This Azure Functions app orchestrates the end-to-end deployment of the
Medical Device FHIR Integration Platform, replacing the PowerShell-based
`Deploy-All.ps1` with a durable, checkpoint-based workflow.

## Architecture

- **HTTP Triggers** — REST API for the React frontend
- **Orchestrator Functions** — Phase sequencing with checkpointing
- **Activity Functions** — One per deployment phase (Azure infra, FHIR, Fabric RTI, etc.)
- **Entity Functions** — Deployment state tracking

## Local Development

```bash
cd orchestrator
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
func start
```

## First-Time Full Stack Setup (Backend + UI)

Use this once on a fresh clone to ensure all local dependencies are installed before running either app:

```bash
# Backend API dependencies
cd orchestrator
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt

# Frontend UI dependencies
cd ..\orchestrator-ui
npm install
```

Then run:

```bash
# Terminal 1
cd orchestrator
.venv\Scripts\python local_server.py

# Terminal 2
cd orchestrator-ui
npm run dev
```

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
