# Deployment Evaluation Harness

`deployment_eval_harness.py` validates that a completed med-device-fabric-emulator
deployment's **user-facing surfaces actually work** — not merely that the Fabric
items exist. Run it after any deployment (or wire it into `Deploy-All.ps1` as a
post-deploy gate).

## What it checks

| Category | What "pass" means | Catches |
|----------|-------------------|---------|
| **reports** | Every Power BI semantic model backing a report is queryable via DAX and its visual-backing tables return rows | Blank/unrepaired visuals, Direct Lake data-source connection breaks, empty fact tables |
| **rti** | Every KQL/RTI dashboard's backing Eventhouse tables & functions return data | Empty `TelemetryRaw`/`AlertHistory`, broken `fn_AlertLocationMap`/`fn_PayerOpsWorklist`, unpopulated `PatientLocationDashboard` |
| **agents** | Every Data Agent answers a natural-language test query end to end (thread → message → run → response) | Unpublished/draft agents ("Stage configuration not found"), empty responses, broken datasource bindings |

It auto-resumes the Fabric capacity if paused (Direct Lake, KQL, and agent runs all
fail while the F64 is suspended), and retries transient transport (SSL/network) blips.

## Usage

```bash
# full run (reports + rti + agents)
python3 eval/deployment_eval_harness.py --workspace med-0719

# persist machine-readable results
python3 eval/deployment_eval_harness.py --workspace med-0719 --json-out eval/med-0719-eval.json

# skip a category (agents are the slowest — each run polls up to ~4 min)
python3 eval/deployment_eval_harness.py --workspace med-0719 --skip agents

# do not auto-resume the capacity (fail fast instead)
python3 eval/deployment_eval_harness.py --workspace med-0719 --no-capacity-resume
```

Exit code: `0` = all checks passed · `1` = one or more failures · `2` = setup error
(capacity not Active, workspace not found, auth failure).

Auth uses the local Azure CLI. Default profile is Joey's isolated BrakeKat
(`--azure-config-dir /Users/joey/.azure-isolated/BrakeKat`); tokens are minted for
Fabric, Power BI, and the Eventhouse query endpoint.

## Known Fabric preview manual steps (reported as FAIL with `remediation` hint)

Two artifacts have **no REST publish/refresh API** in the current Fabric preview and
require a one-time portal action after deployment. The harness flags them clearly so
they aren't mistaken for silent breakage:

1. **Data Agents** must be *Published* in the Fabric portal (draft config alone yields
   `Stage configuration not found`). Ref: `docs/phase-7-payer-rti-ops.md`.
2. **Ontology graph models** need `Refresh graph model` in the portal
   (auto-created companion GraphModel is empty → `GraphNotRefreshable`). Ref:
   `phase-4/deploy-ontology.ps1`. Note: agents use the ontology for vocabulary
   grounding and query the Lakehouse/KQL directly, so the graph refresh is **not**
   required for agent functionality.

## Interpreting results

- `reports` FAIL with "Direct Lake data source connection failed" = the semantic
  model can't read its backing warehouse/lakehouse → visuals render blank. This is a
  real deploy defect (fix the Direct Lake binding / warehouse permissions), not a
  preview limitation.
- `agents` FAIL with `remediation: manual-publish` = publish the agent in the portal.
- `rti` FAIL = a genuine data-flow problem (pipeline didn't populate the Eventhouse).
