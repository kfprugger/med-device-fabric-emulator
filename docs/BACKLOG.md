# Backlog

Deferred, non-blocking items to revisit. Tracked here so they aren't lost between deployments.

## Ontology graph models fail `Refresh` (`GraphNotRefreshable`)

**Status:** Deferred — not blocking. Logged 2026-07-20.

**Symptom:** In the Fabric Monitor, the auto-created companion GraphModels for
`ClinicalDeviceOntology` and `DevicePayerOntology` show failed `Refresh` jobs with
`GraphNotRefreshable: Graph doesn't have valid content and cannot be refreshed`.

**Findings:**
- The ontologies themselves deploy correctly (8 entity types, 8 data bindings, 12
  relationships; all bound Silver tables exist and are populated).
- The companion GraphModel items are empty shells (`queryReadiness = None`); the
  graph content is never materialized from the ontology.
- There is no REST API to materialize/publish the graph in the current Fabric IQ
  preview — `RefreshGraph` is the only job type and it fails on the empty graph.
  `phase-4/deploy-ontology.ps1` correctly documents "Refresh graph model" as a
  manual Fabric portal step.
- **Non-blocking:** the Data Agents use the ontology only as a *vocabulary/semantic
  grounding* datasource and query the Lakehouse/KQL directly (see the agent
  instructions in `phase-2/deploy-data-agents.ps1`, e.g. "The ontology is a semantic
  map only; the real assignment/location rows are in the Lakehouse"). Reports and RTI
  dashboards do not use the graph at all.

**To revisit later:** if/when Fabric exposes a graph materialize/publish API (out of
preview), wire an automatic graph refresh into `phase-4/deploy-ontology.ps1` after
ontology creation so the Monitor shows no failed refresh jobs. Until then this is
cosmetic. The `eval/deployment_eval_harness.py` treats it as a known manual step.
