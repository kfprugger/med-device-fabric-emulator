---
name: fabric-ops
description: "Deploy, monitor, teardown, and diagnose the Medical Device FHIR Integration Platform. USE FOR: running Deploy-All.ps1 (any phase), monitoring background deployments, tearing down environments, invoking/polling HDS pipelines, querying Silver/Bronze/Reporting lakehouses, deploying ontologies, rebuilding DICOM viewer index, diagnosing deployment failures. DO NOT USE FOR: general coding questions, writing new features unrelated to deployment ops."
---

# Fabric Ops — Deployment & Operations Skill

## Workspace & Naming Convention

All deployments use a date-suffixed naming pattern:
- **Workspace**: `med-device-rti-hds-MMDD` (e.g., `med-device-rti-hds-0401`)
- **Resource Group**: `rg-med-device-rti-MMDD` (e.g., `rg-med-device-rti-0401`)
- Append `-N` for same-day iterations (e.g., `med-device-rti-hds-0401-2`)

## Deployment Commands

### Full Deploy (Phase 1 → auto-detects HDS → Phase 2+3)
```powershell
cd C:\git\med-device-fabric-emulator
.\Deploy-All.ps1 `
    -FabricWorkspaceName 'med-device-rti-hds-MMDD' `
    -ResourceGroupName 'rg-med-device-rti-MMDD' `
    -Location eastus `
    -AdminSecurityGroup sg-azure-admins `
    -Tags @{SecurityControl='Ignore'}
```

### Phase 2 Only (after HDS manual deploy)
```powershell
.\Deploy-All.ps1 -Phase2 `
    -ResourceGroupName "rg-med-device-rti-MMDD" `
    -Location "eastus" `
    -FabricWorkspaceName "med-device-rti-hds-MMDD" `
    -Tags @{SecurityControl='Ignore'}
```

### Phase 3 Only (Imaging Toolkit)
```powershell
.\Deploy-All.ps1 -Phase3 `
    -FabricWorkspaceName "med-device-rti-hds-MMDD" `
    -ResourceGroupName "rg-med-device-rti-MMDD" `
    -Location "eastus"
```

### Phase 4 Only (Ontology + Agent Binding)
```powershell
.\Deploy-All.ps1 -Phase4 `
    -FabricWorkspaceName "med-device-rti-hds-MMDD" `
    -Location "eastus"
```

### Teardown
```powershell
.\Teardown-All.ps1 `
    -FabricWorkspaceName "med-device-rti-hds-MMDD" `
    -ResourceGroupName "rg-med-device-rti-MMDD" `
    -Force -Wait
```
Omit `-Wait` to teardown async (non-blocking). AHDS RG deletion takes 5-15 min.

### Ontology Deploy
```powershell
.\deploy-ontology.ps1 -FabricWorkspaceName "med-device-rti-hds-MMDD"
```

## Deployment Step Sequence

| Step | Phase | What | Duration |
|------|-------|------|----------|
| 1 | 1 | Fabric Workspace + Identity | ~0.2 min |
| 2 | 1 | Base Azure Infra (Event Hub, ACR, emulator) | ~3-4 min |
| 3 | 1 | FHIR Service + Synthea + Loader | ~17-20 min |
| 3b | 1 | DICOM Service + TCIA Loader | ~12-18 min |
| 4 | 1 | Fabric RTI Phase 1 (Eventhouse, KQL, Eventstream) | ~2 min |
| 5 | — | HDS Guidance (auto-detected or manual) | — |
| 6 | 2 | Phase 2 (KQL shortcuts, enriched alerts) | ~14-15 min |
| 6b | 2 | DICOM Shortcut + HDS Pipelines (Imaging→Clinical→OMOP) | ~40-55 min |
| 7 | 2 | Data Agents (Patient 360 + Clinical Triage) | ~0.1 min |
| 8 | 3 | Phase 3 (Cohorting Agent, DICOM Viewer, PBI Report) | ~9-10 min |
| 9 | 4 | Phase 4 (Clinical pipeline check, Ontology, Agent binding, Activator) | ~4-5 min |

**Total end-to-end**: ~100-110 min (full auto-detection run)

## Monitoring Background Deployments

When running deployments via `run_in_terminal` with `isBackground=true`, save the terminal ID and poll with `get_terminal_output`:

```
Terminal ID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

Key patterns to watch for in output:
- `✓` = step succeeded
- `✗` = step failed (check error message)
- `⚠` = warning (non-fatal, may need attention)
- `toomanyrequests` = Docker Hub rate limit (use MCR base images)
- `409 Conflict` = resource name collision (retry with delay)
- `SharedTokenCacheCredential` = token auth failure (refresh token)

## Querying Lakehouses via SQL

Connect to the Fabric SQL endpoint using the MSSQL extension (profile: `hds-new` or discover via Fabric API).

### Discover SQL Endpoint
```powershell
$t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
if ($t -is [System.Security.SecureString]) { $t = $t | ConvertFrom-SecureString -AsPlainText }
$h = @{ Authorization = "Bearer $t" }
$ws = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $h).value |
    Where-Object { $_.displayName -eq "WORKSPACE_NAME" }
$lh = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($ws.id)/lakehouses" -Headers $h).value |
    Where-Object { $_.displayName -match 'silver' }
$d = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($ws.id)/lakehouses/$($lh.id)" -Headers $h
Write-Host "Server: $($d.properties.sqlEndpointProperties.connectionString)"
```

### Key Verification Queries (Silver)
```sql
SELECT 'Patient' as tbl, COUNT(*) as cnt FROM dbo.Patient
UNION ALL SELECT 'Device', COUNT(*) FROM dbo.Device
UNION ALL SELECT 'Condition', COUNT(*) FROM dbo.Condition
UNION ALL SELECT 'Encounter', COUNT(*) FROM dbo.Encounter
UNION ALL SELECT 'MedicationRequest', COUNT(*) FROM dbo.MedicationRequest
UNION ALL SELECT 'Observation', COUNT(*) FROM dbo.Observation
UNION ALL SELECT 'ImagingStudy', COUNT(*) FROM dbo.ImagingStudy
UNION ALL SELECT 'ImagingMetastore', COUNT(*) FROM dbo.ImagingMetastore
ORDER BY cnt DESC
```

### Expected Row Counts (100 patients)
| Table | Expected |
|-------|----------|
| Observation | ~22K |
| Procedure | ~11K |
| ImagingMetastore | ~4K |
| Encounter | ~3.6K |
| Condition | ~2.6K |
| MedicationRequest | ~1.8K |
| Immunization | ~1.5K |
| Patient | 100 |
| Device | 100 |
| ImagingStudy | ~50-90 |

## Invoking HDS Pipelines Manually

### Discover and Invoke
```powershell
$t = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
if ($t -is [System.Security.SecureString]) { $t = $t | ConvertFrom-SecureString -AsPlainText }
$h = @{ Authorization = "Bearer $t"; "Content-Type" = "application/json" }
$ws = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces" -Headers $h).value |
    Where-Object { $_.displayName -eq "WORKSPACE_NAME" }
$wsId = $ws.id

# List pipelines
$pipelines = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items?type=DataPipeline" -Headers $h).value
$pipelines | Select-Object displayName, id

# Invoke a pipeline
$pipelineId = "PIPELINE_ID"
Invoke-WebRequest -Method POST `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$pipelineId/jobs/Pipeline/instances" `
    -Headers $h -UseBasicParsing
```

### Pipeline Names
| Pipeline | Purpose |
|----------|---------|
| `healthcare1_msft_clinical_data_foundation_ingestion` | FHIR → Bronze → Silver (clinical flattening) |
| `healthcare1_msft_imaging_with_clinical_foundation_ingestion` | DICOM + clinical imaging → Silver |
| `healthcare1_msft_omop_analytics` | Silver → Gold OMOP (must run AFTER imaging + clinical) |

### Pipeline Sequence Rule
Pipelines CANNOT run in parallel. Sequence: **Imaging (wait) → Clinical (wait) → OMOP (fire-and-forget)**

### Poll Pipeline Status
```powershell
$runs = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$wsId/items/$pipelineId/jobs/instances?limit=1" -Headers $h).value
$runs[0].status  # NotStarted | InProgress | Completed | Failed | Cancelled
```

## Docker Base Images

All Dockerfiles use MCR (no Docker Hub rate limits):
| Image | Base |
|-------|------|
| Emulator | `mcr.microsoft.com/cbl-mariner/base/python:3` |
| Synthea | `mcr.microsoft.com/openjdk/jdk:17-ubuntu` |
| FHIR Loader | `mcr.microsoft.com/cbl-mariner/base/python:3` |
| DICOM Loader | `mcr.microsoft.com/cbl-mariner/base/python:3` |
| DICOM Proxy | `mcr.microsoft.com/cbl-mariner/base/python:3` |

**Mariner notes**: Uses `tdnf` (not `apt-get`), needs `ln -sf /usr/bin/python3 /usr/bin/python` symlink, ODBC driver package is `msodbcsql18` with `ENV ACCEPT_EULA=Y`.

## Common Issues & Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| `toomanyrequests` on ACR build | Docker Hub rate limit | Already fixed — all images use MCR |
| `SharedTokenCacheCredential` error | Token expired or wrong scope | Refresh with `Get-AzAccessToken -ResourceUrl` |
| DICOM index = 0 studies | Proxy built before ImagingMetastore populated | Proxy has runtime SQL refresh; also auto-rebuilds in Step 3e |
| 409 Conflict on notebook create | Previous delete still propagating | Retry loop with 10s delay (already implemented) |
| `Condition_` table not found | Wrong table name in diagnostics | Table is `Condition` (no underscore) |
| `libgssapi_krb5.so.2 not found` | Missing Kerberos lib in container | Install `krb5-libs` via tdnf (already fixed) |
| Silver tables appear missing | Fabric Lakehouse Tables API paginated/incomplete | Always verify via SQL endpoint, not REST API |
| PBI report needs OAuth | Direct Lake credential binding | Script attempts auto-bind; falls back to manual |

## Teardown Validation

The teardown script validates both workspace and RG before deleting. If either is not found, it shows fuzzy "Did you mean?" suggestions and prompts for partial teardown. Use `-Force` to skip confirmation.

## Key File Locations

| File | Purpose |
|------|---------|
| `Deploy-All.ps1` | Main orchestrator (all phases) |
| `Teardown-All.ps1` | Cleanup orchestrator |
| `deploy.ps1` | Phase 1 Azure infra |
| `deploy-fhir.ps1` | FHIR + DICOM data pipeline |
| `deploy-fabric-rti.ps1` | Fabric RTI Phase 1 + 2 |
| `deploy-data-agents.ps1` | Data Agents (Patient 360 + Clinical Triage) |
| `deploy-ontology.ps1` | Fabric IQ Ontology |
| `storage-access-trusted-workspace.ps1` | DICOM shortcut + HDS pipeline orchestration |
| `.deployment-state.json` | Deployment state tracking (gitignored) |
