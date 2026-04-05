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

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/deploy/start` | Start a new deployment |
| GET | `/api/deploy/{instanceId}/status` | Get deployment status |
| POST | `/api/deploy/{instanceId}/resume-hds` | Resume after manual HDS step |
| POST | `/api/deploy/{instanceId}/cancel` | Cancel a running deployment |
| POST | `/api/teardown/start` | Start teardown |
| GET | `/api/deployments` | List deployment history |
