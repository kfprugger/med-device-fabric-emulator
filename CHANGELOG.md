# Changelog

## [Unreleased] — March 26, 2026

### DICOM Loader Fixes
- **Fixed** Python 3.9 compatibility: `str | None` → `Optional[str]`, `tuple[str, str]` → `Tuple[str, str]` in `dicom_retagger.py` and `tcia_client.py`
- **Fixed** `from __future__ import annotations` position in `load_dicom.py` — must be first statement after docstring (was after imports, causing SyntaxError)
- **Fixed** `az acr build` charmap Unicode crash for DICOM loader — added `--no-logs` flag in `deploy-fhir.ps1`

### KQL Deployment
- **Fixed** KQL execution order in `deploy-fabric-rti.ps1` — TelemetryRaw table is now created **before** `fn_AlertHistoryTransform` and the AlertHistory update policy (was created after, causing `General_BadRequest` on fresh deploys)

### Phase 3: Cohorting Toolkit Integration
- **Added** Phase 3 deployment documentation for FabricDicomCohortingToolkit (imaging report, DICOM viewer, cohorting agent)
- **Added** DICOM viewer proxy RBAC requirement — Container App managed identity needs Contributor on Fabric workspace for OneLake file reads
- **Added** OHIF Viewer and TCIA to acknowledgments

### FabricDicomCohortingToolkit
- **Changed** `materialize_reporting.py` — removed all hardcoded workspace/lakehouse GUIDs; now uses `notebookutils.fabric.resolve_workspace_id()` and Fabric REST API to resolve lakehouse IDs by display name
- **Changed** `deploy-notebook.ps1` — auto-discovers OHIF Viewer URL from Azure Static Web App before uploading notebook; patches URL into notebook code at deploy time
- **Changed** Deployment order: DICOM Viewer → Notebook → Report (viewer must deploy first so its URL flows into the reporting data)

## [Unreleased] — March 14-18, 2026

### Deployment Flow
- **Added** Step 1b: Fabric workspace creation early in Phase 1 (before FHIR/DICOM)
- **Fixed** Step 2 to use `-SkipDicom` to prevent duplicate DICOM execution
- **Removed** redundant clinical pipeline trigger — imaging pipeline includes clinical data foundation
- Pipeline sequence: Imaging (includes clinical) → OMOP (was: Clinical → Imaging → OMOP)

### Data Agents
- **Fixed** invalid Observation fewshot query — changed `valueQuantity_value`/`valueQuantity_unit` to `JSON_VALUE(valueQuantity_string, '$.value')`/`JSON_VALUE(valueQuantity_string, '$.unit')`
- **Added** 2 new fewshot examples for full patient summary + demographics by device
- **Added** cross-datasource sample questions (KQL + Lakehouse + DICOM imaging) for both Patient 360 and Clinical Triage agents
- **Fixed** Data Agent portal URL format: `/dataAgents/` → `/aiskills/`

### Deployment Pipeline (Deploy-All.ps1)
- **Added** `-FabricWorkspaceName` as mandatory parameter
- **Added** `-AdminSecurityGroup` as conditionally required (not needed for `-Teardown`/`-Phase2Only`)
- **Changed** `-Location` to mandatory (no default)
- **Changed** `-ResourceGroupName` has default `rg-medtech-rti-fhir` (not mandatory)
- **Added** `-Tags` passthrough to all sub-scripts (`deploy.ps1`, `deploy-fhir.ps1`, `deploy-fabric-rti.ps1`)
- **Added** DICOM shortcut + HDS pipeline step (clinical, imaging, OMOP) in Phase2Only flow
- **Added** pre-populated Phase 2 command in HDS guidance step (auto-fills `-Location`, `-FabricWorkspaceName`, `-Tags` from Phase 1 values)
- **Added** DICOM Data Transformation modality instruction in HDS manual step
- **Fixed** Phase2Only no longer exits early — continues to DICOM shortcuts + Data Agents
- **Fixed** `DeploymentActive` error in `deploy.ps1` — waits 60s and retries
- **Fixed** `RoleAssignmentExists` error in `deploy-fhir.ps1` — treated as non-fatal, falls back to `deployment group show`
- **Fixed** Unicode encoding crash in `az acr build` — added `[Console]::OutputEncoding = UTF8`

### HDS Pipeline Integration
- **Added** OMOP pipeline (`healthcare1_msft_omop_analytics`) as Step 11 in `storage-access-trusted-workspace.ps1`
- **Added** OMOP pipeline parameter to `storage-access-trusted-workspace.ps1`
- Pipeline sequence: Clinical → Imaging → OMOP

### Fabric RTI (deploy-fabric-rti.ps1)
- **Added** `-Tags` parameter — applies tags to Event Hub namespace before enabling SAS auth
- **Added** RBAC propagation wait (60s + verification) after assigning Storage Blob Data Contributor
- **Added** storage access preflight check before shortcut creation
- **Added** 3-attempt retry with 60s wait for Bronze LH shortcut creation
- **Added** Kusto token refresh before KQL external table creation (prevents 401 after long pipeline waits)
- **Added** Workspace identity resolution via Fabric API (`provisionIdentity` → `GET /workspaces/{id}`) with `az ad sp` fallback
- **Added** detailed remediation instructions on shortcut creation failure (SP IDs, portal steps, re-run command)
- **Fixed** `/workspaces/{id}/lakehouses` → `/workspaces/{id}/items?type=Lakehouse` (deprecated endpoint)

### Cleanup (Remove-AllResources.ps1)
- **Added** `-DeleteWorkspace` parameter to delete the Fabric workspace itself
- **Added** Step 2b: deprovision workspace identity + delete Entra app registration
- **Added** `Delete Workspace:` display in teardown banner

### Documentation
- **Updated** README.md: configuration options table with Required column, pre-populated CLI examples, Deploy-All orchestrator section, cleanup section, OMOP in diagrams
- **Updated** PRD.md: OMOP pipeline in artifacts table, data flow diagram, deployment sequence, script descriptions
- **Updated** HDS-SETUP-GUIDE.md: added imaging + OMOP pipelines to artifacts table
- **Updated** all Mermaid diagrams to include OMOP pipeline flow
