"""Phase 4b: DICOM shortcut + HDS pipeline triggers.

Ports logic from phase-2/storage-access-trusted-workspace.ps1:
- Create Bronze Lakehouse shortcuts (FHIR + DICOM → ADLS Gen2)
- Trigger HDS clinical, imaging, OMOP, and optional CMA pipelines
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# HDS pipeline names. Clinical must run before imaging and OMOP because
# downstream HDS pipelines depend on clinical foundation tables. CMA is optional
# and must not block the HDS deployment result.
CORE_HDS_PIPELINE_NAMES = [
    "healthcare1_msft_clinical_data_foundation_ingestion",
    "healthcare1_msft_imaging_with_clinical_foundation_ingestion",
    "healthcare1_msft_omop_analytics",
]
CMA_PIPELINE_NAME = "healthcare1_msft_cma"
HDS_PIPELINE_NAMES = CORE_HDS_PIPELINE_NAMES
PIPELINE_POLL_SECONDS = 30
PIPELINE_TIMEOUT_SECONDS = 60 * 60


def _is_already_running_error(error: Exception) -> bool:
    message = str(error)
    return any(
        token in message
        for token in ("409", "already running", "TooManyRequestsForJobs")
    )


def _trigger_pipeline(
    fabric: FabricClient,
    workspace_id: str,
    pipeline: dict[str, Any],
    pipeline_name: str,
) -> str:
    try:
        fabric.call(
            "POST",
            f"/workspaces/{workspace_id}/items/{pipeline['id']}/jobs/Pipeline/instances",
        )
        logger.info("Pipeline triggered: %s", pipeline_name)
        return "triggered"
    except Exception as e:
        if _is_already_running_error(e):
            logger.warning(
                "Pipeline already running or recently invoked: %s", pipeline_name
            )
            return "already_running"
        logger.error("Failed to trigger %s: %s", pipeline_name, e)
        return f"error: {e}"


def _wait_for_pipeline_completion(
    fabric: FabricClient,
    workspace_id: str,
    pipeline: dict[str, Any],
    pipeline_name: str,
    timeout_seconds: int = PIPELINE_TIMEOUT_SECONDS,
) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        time.sleep(PIPELINE_POLL_SECONDS)
        try:
            jobs = fabric.call(
                "GET",
                f"/workspaces/{workspace_id}/items/{pipeline['id']}/jobs/instances?limit=1",
            )
        except Exception as e:
            logger.warning("Poll error for %s: %s", pipeline_name, e)
            continue

        latest_job = None
        if isinstance(jobs, dict):
            values = jobs.get("value") or []
            latest_job = values[0] if values else None
        if not latest_job:
            logger.info("Pipeline %s has no job status yet", pipeline_name)
            continue

        status = latest_job.get("status")
        logger.info("Pipeline %s status: %s", pipeline_name, status)
        if status == "Completed":
            return "completed"
        if status in {"Failed", "Cancelled"}:
            return f"failed: {status}"

    return "timeout"


def _run_blocking_pipeline(
    fabric: FabricClient,
    workspace_id: str,
    pipeline: dict[str, Any],
    pipeline_name: str,
) -> str:
    trigger_status = _trigger_pipeline(fabric, workspace_id, pipeline, pipeline_name)
    if trigger_status not in {"triggered", "already_running"}:
        return trigger_status
    completion_status = _wait_for_pipeline_completion(
        fabric, workspace_id, pipeline, pipeline_name
    )
    return completion_status

def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 3: DICOM Shortcut + HDS Pipelines.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        Pipeline trigger results.
    """
    start = time.time()
    fabric = FabricClient(
        config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1")
    )
    workspace_id = resources["fabric_workspace_id"]

    # Find Bronze Lakehouse
    bronze_lh = fabric.find_lakehouse(workspace_id, "bronze")
    bronze_lh_id = bronze_lh["id"] if bronze_lh else ""
    bronze_lh_name = bronze_lh.get("displayName", "") if bronze_lh else ""

    if not bronze_lh_id:
        logger.warning("Bronze Lakehouse not found. Shortcut creation skipped.")

    # Trigger and wait for core HDS pipelines sequentially. Downstream HDS
    # pipelines are skipped unless every prior blocking pipeline completes.
    pipeline_results = {}
    prior_pipeline_completed = True
    for pipeline_name in CORE_HDS_PIPELINE_NAMES:
        if not prior_pipeline_completed:
            pipeline_results[pipeline_name] = "skipped_prerequisites_incomplete"
            logger.warning(
                "Skipping pipeline because a prior HDS pipeline did not complete: %s",
                pipeline_name,
            )
            continue

        pipeline = fabric.find_item(workspace_id, pipeline_name, "DataPipeline")
        if not pipeline:
            pipeline_results[pipeline_name] = "not_found"
            prior_pipeline_completed = False
            logger.warning("Pipeline not found: %s", pipeline_name)
            continue

        logger.info("Triggering pipeline: %s", pipeline_name)
        pipeline_status = _run_blocking_pipeline(fabric, workspace_id, pipeline, pipeline_name)
        pipeline_results[pipeline_name] = pipeline_status
        prior_pipeline_completed = pipeline_status == "completed"

    # CMA is present only when Care Management Analytics was included in the HDS
    # deployment. Treat it as a best-effort follow-up after the final blocking
    # HDS pipeline completes; never block the phase on missing CMA or a trigger failure.
    cma_result = "not_deployed"
    cma_pipeline = fabric.find_item(workspace_id, CMA_PIPELINE_NAME, "DataPipeline")
    if cma_pipeline:
        if pipeline_results.get(CORE_HDS_PIPELINE_NAMES[-1]) == "completed":
            logger.info("Triggering optional CMA pipeline: %s", CMA_PIPELINE_NAME)
            cma_status = _trigger_pipeline(fabric, workspace_id, cma_pipeline, CMA_PIPELINE_NAME)
            if cma_status in {"triggered", "already_running"}:
                cma_result = f"{cma_status}_non_blocking"
            else:
                cma_result = f"warning: {cma_status}"
                logger.warning("Optional CMA pipeline trigger failed: %s", cma_status)
        else:
            cma_result = "skipped_core_pipeline_incomplete"
            logger.warning("Skipping optional CMA pipeline because OMOP did not complete")
    duration = time.time() - start

    return {
        "phase": "Phase 3: DICOM Shortcut + HDS Pipelines",
        "duration_seconds": duration,
        "resources": {
            "bronze_lakehouse_id": bronze_lh_id,
            "bronze_lakehouse_name": bronze_lh_name,
        },
        "pipeline_results": pipeline_results,
        "non_blocking_followups": {
            CMA_PIPELINE_NAME: cma_result,
        },
    }
