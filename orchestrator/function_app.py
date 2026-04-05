"""Azure Durable Functions app — Deployment Orchestrator.

HTTP triggers expose the REST API for the React frontend.
Orchestrator functions manage phase sequencing with checkpointing.
Activity functions execute individual deployment phases.
"""

import json
import logging
from datetime import timedelta
from typing import Optional

import azure.durable_functions as df
import azure.functions as func

from shared.models import DeploymentConfig, DeploymentState, PhaseResult, PhaseStatus

# ── App setup ──────────────────────────────────────────────────────────

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
logger = logging.getLogger(__name__)

# Standard retry policy for activity functions
RETRY_POLICY = df.RetryOptions(
    first_retry_interval_in_milliseconds=60_000,  # 60s
    max_number_of_attempts=3,
)


# ═══════════════════════════════════════════════════════════════════════
# HTTP TRIGGERS (API Layer)
# ═══════════════════════════════════════════════════════════════════════


@app.route(route="deploy/start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_deployment(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Start a new deployment orchestration.

    Request body: DeploymentConfig JSON.
    Returns: { "instanceId": "...", "statusUrl": "..." }
    """
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    # Validate config
    try:
        config = DeploymentConfig(**body)
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Invalid config: {e}"}),
            status_code=400,
            mimetype="application/json",
        )

    instance_id = await client.start_new(
        "deploy_all_orchestrator",
        client_input=config.model_dump(),
    )

    logger.info("Deployment started: %s", instance_id)

    return func.HttpResponse(
        json.dumps({
            "instanceId": instance_id,
            "statusUrl": f"/api/deploy/{instance_id}/status",
        }),
        status_code=202,
        mimetype="application/json",
    )


@app.route(route="deploy/{instanceId}/status", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_deployment_status(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Get the status of a deployment orchestration."""
    instance_id = req.route_params.get("instanceId", "")
    if not instance_id:
        return func.HttpResponse(
            json.dumps({"error": "instanceId required"}),
            status_code=400,
            mimetype="application/json",
        )

    status = await client.get_status(instance_id, show_history=True, show_history_output=True)

    if not status:
        return func.HttpResponse(
            json.dumps({"error": "Instance not found"}),
            status_code=404,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps({
            "instanceId": instance_id,
            "runtimeStatus": status.runtime_status.value if status.runtime_status else "Unknown",
            "output": status.output,
            "customStatus": status.custom_status,
            "createdTime": str(status.created_time) if status.created_time else None,
            "lastUpdatedTime": str(status.last_updated_time) if status.last_updated_time else None,
        }),
        mimetype="application/json",
    )


@app.route(route="deploy/{instanceId}/resume-hds", methods=["POST"])
@app.durable_client_input(client_name="client")
async def resume_after_hds(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Resume deployment after the manual HDS step.

    The orchestrator pauses at wait_for_external_event("hds_deployed").
    This endpoint raises that event to continue.
    """
    instance_id = req.route_params.get("instanceId", "")
    if not instance_id:
        return func.HttpResponse(
            json.dumps({"error": "instanceId required"}),
            status_code=400,
            mimetype="application/json",
        )

    await client.raise_event(instance_id, "hds_deployed", True)

    logger.info("HDS resume event raised for %s", instance_id)
    return func.HttpResponse(
        json.dumps({"message": "HDS deployment acknowledged. Orchestrator resuming."}),
        mimetype="application/json",
    )


@app.route(route="deploy/{instanceId}/cancel", methods=["POST"])
@app.durable_client_input(client_name="client")
async def cancel_deployment(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Cancel a running deployment."""
    instance_id = req.route_params.get("instanceId", "")
    if not instance_id:
        return func.HttpResponse(
            json.dumps({"error": "instanceId required"}),
            status_code=400,
            mimetype="application/json",
        )

    await client.terminate(instance_id, "Cancelled by user")
    return func.HttpResponse(
        json.dumps({"message": "Deployment cancelled."}),
        mimetype="application/json",
    )


@app.route(route="teardown/start", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_teardown(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Start a teardown orchestration."""
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body"}),
            status_code=400,
            mimetype="application/json",
        )

    instance_id = await client.start_new(
        "teardown_orchestrator",
        client_input=body,
    )

    return func.HttpResponse(
        json.dumps({
            "instanceId": instance_id,
            "statusUrl": f"/api/deploy/{instance_id}/status",
        }),
        status_code=202,
        mimetype="application/json",
    )


@app.route(route="deployments", methods=["GET"])
@app.durable_client_input(client_name="client")
async def list_deployments(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """List recent deployment orchestrations."""
    instances = await client.get_status_all()

    results = []
    for inst in instances:
        results.append({
            "instanceId": inst.instance_id,
            "name": inst.name,
            "runtimeStatus": inst.runtime_status.value if inst.runtime_status else "Unknown",
            "createdTime": str(inst.created_time) if inst.created_time else None,
            "lastUpdatedTime": str(inst.last_updated_time) if inst.last_updated_time else None,
            "customStatus": inst.custom_status,
        })

    return func.HttpResponse(
        json.dumps(results),
        mimetype="application/json",
    )


# ═══════════════════════════════════════════════════════════════════════
# ORCHESTRATOR FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════


@app.orchestration_trigger(context_name="ctx")
def deploy_all_orchestrator(ctx):
    """Main deployment orchestrator — maps to Deploy-All.ps1.

    Phase sequence:
      1. Base Azure Infrastructure (deploy.ps1)
      1b. Fabric Workspace (inline in Deploy-All.ps1)
      2. FHIR Service + Synthea + FHIR Loader (deploy-fhir.ps1)
      2b. DICOM Infrastructure + Loader (deploy-fhir.ps1 -RunDicom)
      3. Fabric RTI Phase 1 (deploy-fabric-rti.ps1)
      -- Human interaction gate: wait for HDS manual deployment --
      4. Fabric RTI Phase 2 (deploy-fabric-rti.ps1 -Phase2)
      4b. HDS Pipeline Triggers (storage-access-trusted-workspace.ps1)
      5. Data Agents (deploy-data-agents.ps1)
      6. Ontology (deploy-ontology.ps1)
    """
    config: dict = ctx.get_input()
    resources: dict = {}
    phases: list[dict] = []

    def update_status(phase_name: str, status: str, detail: str = ""):
        ctx.set_custom_status({
            "currentPhase": phase_name,
            "status": status,
            "detail": detail,
            "completedPhases": len([p for p in phases if p.get("status") == "succeeded"]),
            "totalPhases": 9,
            "resources": resources,
        })

    # ── Phase 1: Base Azure Infrastructure ─────────────────────────
    if not config.get("skip_base_infra"):
        update_status("Phase 1: Base Azure Infrastructure", "running")
        result = yield ctx.call_activity_with_retry(
            "activity_deploy_azure_infra", RETRY_POLICY, config
        )
        resources.update(result.get("resources", {}))
        phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})
    else:
        phases.append({"phase": "Phase 1: Base Azure Infrastructure", "status": "skipped"})

    # ── Phase 1b: Fabric Workspace ─────────────────────────────────
    update_status("Phase 1b: Fabric Workspace", "running")
    result = yield ctx.call_activity_with_retry(
        "activity_provision_workspace", RETRY_POLICY, config
    )
    resources.update(result.get("resources", {}))
    phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})

    # ── Phase 2: FHIR Service + Data Loading ───────────────────────
    if not config.get("skip_fhir"):
        update_status("Phase 2: FHIR Service & Data Loading", "running")
        phase2_input = {"config": config, "resources": resources}
        result = yield ctx.call_activity_with_retry(
            "activity_deploy_fhir", RETRY_POLICY, phase2_input
        )
        resources.update(result.get("resources", {}))
        phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})
    else:
        phases.append({"phase": "Phase 2: FHIR Service & Data Loading", "status": "skipped"})

    # ── Phase 2b: DICOM (can run in parallel with Phase 2 in future) ──
    if not config.get("skip_dicom"):
        update_status("Phase 2b: DICOM Infrastructure & Loading", "running")
        phase2b_input = {"config": config, "resources": resources}
        result = yield ctx.call_activity_with_retry(
            "activity_deploy_dicom", RETRY_POLICY, phase2b_input
        )
        resources.update(result.get("resources", {}))
        phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})
    else:
        phases.append({"phase": "Phase 2b: DICOM", "status": "skipped"})

    # ── Phase 3: Fabric RTI Phase 1 ───────────────────────────────
    if not config.get("skip_fabric"):
        update_status("Phase 3: Fabric RTI Phase 1", "running")
        phase3_input = {"config": config, "resources": resources}
        result = yield ctx.call_activity_with_retry(
            "activity_deploy_fabric_rti", RETRY_POLICY, phase3_input
        )
        resources.update(result.get("resources", {}))
        phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})

    # ── Human Interaction Gate: Wait for HDS ──────────────────────
    update_status("Waiting for HDS Deployment", "waiting_for_input",
                  "Deploy Healthcare Data Solutions (HDS) manually in the Fabric portal, "
                  "install scipy in the environment, then click 'Continue' in the dashboard.")

    # Wait up to 24 hours for the user to deploy HDS and resume
    hds_event = yield ctx.wait_for_external_event("hds_deployed", timedelta(hours=24))

    if not hds_event:
        # Timeout — mark as waiting and return partial results
        phases.append({"phase": "HDS Deployment", "status": "timeout"})
        return {
            "status": "waiting_for_hds",
            "phases": phases,
            "resources": resources,
        }

    phases.append({"phase": "HDS Deployment", "status": "succeeded"})

    # ── Phase 4: Fabric RTI Phase 2 ───────────────────────────────
    update_status("Phase 4: Fabric RTI Phase 2", "running")
    phase4_input = {"config": config, "resources": resources}
    result = yield ctx.call_activity_with_retry(
        "activity_deploy_rti_phase2", RETRY_POLICY, phase4_input
    )
    resources.update(result.get("resources", {}))
    phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})

    # ── Phase 4b: HDS Pipeline Triggers ───────────────────────────
    update_status("Phase 4b: HDS Pipeline Triggers", "running")
    phase4b_input = {"config": config, "resources": resources}
    result = yield ctx.call_activity_with_retry(
        "activity_deploy_hds_pipelines", RETRY_POLICY, phase4b_input
    )
    resources.update(result.get("resources", {}))
    phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})

    # ── Phase 5: Data Agents ──────────────────────────────────────
    update_status("Phase 5: Data Agents", "running")
    phase5_input = {"config": config, "resources": resources}
    result = yield ctx.call_activity_with_retry(
        "activity_deploy_data_agents", RETRY_POLICY, phase5_input
    )
    resources.update(result.get("resources", {}))
    phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})

    # ── Phase 6: Ontology ─────────────────────────────────────────
    update_status("Phase 6: Ontology", "running")
    phase6_input = {"config": config, "resources": resources}
    result = yield ctx.call_activity_with_retry(
        "activity_deploy_ontology", RETRY_POLICY, phase6_input
    )
    resources.update(result.get("resources", {}))
    phases.append({"phase": result["phase"], "status": "succeeded", "duration": result["duration_seconds"]})

    # ── Complete ──────────────────────────────────────────────────
    update_status("Deployment Complete", "succeeded")

    return {
        "status": "succeeded",
        "phases": phases,
        "resources": resources,
    }


@app.orchestration_trigger(context_name="ctx")
def teardown_orchestrator(ctx):
    """Teardown orchestrator — maps to Remove-AllResources.ps1."""
    config = ctx.get_input()

    ctx.set_custom_status({"currentPhase": "Teardown", "status": "running"})

    result = yield ctx.call_activity("activity_teardown", config)

    ctx.set_custom_status({"currentPhase": "Teardown", "status": "succeeded"})

    return result


# ═══════════════════════════════════════════════════════════════════════
# ACTIVITY FUNCTIONS (wrappers calling into activities/ modules)
# ═══════════════════════════════════════════════════════════════════════


@app.activity_trigger(input_name="config")
def activity_deploy_azure_infra(config: dict) -> dict:
    """Phase 1: Base Azure Infrastructure."""
    from activities.deploy_azure_infra import run
    return run(config)


@app.activity_trigger(input_name="config")
def activity_provision_workspace(config: dict) -> dict:
    """Phase 1b: Fabric Workspace."""
    from activities.provision_workspace import run
    return run(config)


@app.activity_trigger(input_name="input_data")
def activity_deploy_fhir(input_data: dict) -> dict:
    """Phase 2: FHIR Service & Data Loading."""
    from activities.deploy_fhir import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="input_data")
def activity_deploy_dicom(input_data: dict) -> dict:
    """Phase 2b: DICOM Infrastructure & Loading."""
    from activities.deploy_dicom import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="input_data")
def activity_deploy_fabric_rti(input_data: dict) -> dict:
    """Phase 3: Fabric RTI Phase 1."""
    from activities.deploy_fabric_rti import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="input_data")
def activity_deploy_rti_phase2(input_data: dict) -> dict:
    """Phase 4: Fabric RTI Phase 2."""
    from activities.deploy_rti_phase2 import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="input_data")
def activity_deploy_hds_pipelines(input_data: dict) -> dict:
    """Phase 4b: HDS Pipeline Triggers."""
    from activities.deploy_hds_pipelines import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="input_data")
def activity_deploy_data_agents(input_data: dict) -> dict:
    """Phase 5: Data Agents."""
    from activities.deploy_data_agents import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="input_data")
def activity_deploy_ontology(input_data: dict) -> dict:
    """Phase 6: Ontology."""
    from activities.deploy_ontology import run
    return run(input_data["config"], input_data["resources"])


@app.activity_trigger(input_name="config")
def activity_teardown(config: dict) -> dict:
    """Teardown all resources."""
    from activities.teardown import run
    return run(config)


