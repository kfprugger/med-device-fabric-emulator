"""Phase 4b: DICOM shortcut + HDS pipeline triggers.

Ports logic from phase-2/storage-access-trusted-workspace.ps1:
- Create Bronze Lakehouse shortcuts (FHIR + DICOM → ADLS Gen2)
- Trigger HDS sidecar, clinical, CMA, imaging, and OMOP pipelines in safe order
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# Core HDS pipeline order is intentionally sequential: Clinical → Imaging → OMOP.
# Optional SDoH/claims sidecars are discovered from live Fabric workspace pipeline
# names and launched best-effort before the Clinical wait; they may no-op/fail
# non-blocking if source data is absent. CMA is optional and launches
# after Clinical/Silver readiness, before Imaging and OMOP.
CORE_HDS_PIPELINE_NAMES = [
    "healthcare1_msft_clinical_data_foundation_ingestion",
    "healthcare1_msft_imaging_with_clinical_foundation_ingestion",
    "healthcare1_msft_omop_analytics",
]
CMA_PIPELINE_NAME = "healthcare1_msft_cma"
DEFAULT_SIDECAR_PIPELINE_PATTERNS = (
    "sdoh",
    "socialdeterminant",
    "social_determinant",
    "social determinant",
    "claim",
    "claims",
    "cclf",
)
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

def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [entry.strip() for entry in value.split(",") if entry.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(entry).strip() for entry in value if str(entry).strip()]
    return []


def _pipeline_matches_pattern(display_name: str, pattern: str) -> bool:
    normalized_name = display_name.lower().replace("-", "_")
    normalized_pattern = pattern.lower().replace("-", "_")
    compact_name = normalized_name.replace("_", "").replace(" ", "")
    compact_pattern = normalized_pattern.replace("_", "").replace(" ", "")
    return normalized_pattern in normalized_name or compact_pattern in compact_name


def _resolve_optional_sidecar_pipelines(
    pipelines: list[dict[str, Any]],
    names: list[str],
    patterns: list[str],
    excluded_names: set[str],
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    pipelines_by_name = {
        str(pipeline.get("displayName", "")): pipeline for pipeline in pipelines
    }

    for name in names:
        pipeline = pipelines_by_name.get(name)
        pipeline_id = str(pipeline.get("id", "")) if pipeline else ""
        if pipeline and pipeline_id not in seen_ids:
            resolved.append(pipeline)
            seen_ids.add(pipeline_id)

    for pipeline in pipelines:
        display_name = str(pipeline.get("displayName", ""))
        pipeline_id = str(pipeline.get("id", ""))
        if not display_name or display_name in excluded_names or pipeline_id in seen_ids:
            continue
        if any(_pipeline_matches_pattern(display_name, pattern) for pattern in patterns):
            resolved.append(pipeline)
            seen_ids.add(pipeline_id)

    return resolved


def _trigger_optional_pipeline(
    fabric: FabricClient,
    workspace_id: str,
    pipeline: dict[str, Any] | None,
    pipeline_name: str,
) -> str:
    if not pipeline:
        return "not_deployed"
    status = _trigger_pipeline(fabric, workspace_id, pipeline, pipeline_name)
    if status in {"triggered", "already_running"}:
        return f"{status}_non_blocking"
    logger.warning("Optional pipeline trigger failed for %s: %s", pipeline_name, status)
    return f"warning: {status}"


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

    pipelines = fabric.list_items(workspace_id, "DataPipeline")
    pipelines_by_name = {
        str(pipeline.get("displayName", "")): pipeline for pipeline in pipelines
    }
    excluded_pipeline_names = {*CORE_HDS_PIPELINE_NAMES, CMA_PIPELINE_NAME}
    sidecar_names = _coerce_string_list(config.get("optional_sidecar_pipeline_names"))
    sidecar_patterns = _coerce_string_list(
        config.get("optional_sidecar_pipeline_name_patterns")
    ) or list(DEFAULT_SIDECAR_PIPELINE_PATTERNS)
    sidecar_pipelines = _resolve_optional_sidecar_pipelines(
        pipelines,
        sidecar_names,
        sidecar_patterns,
        excluded_pipeline_names,
    )

    non_blocking_followups: dict[str, str] = {}
    for sidecar_pipeline in sidecar_pipelines:
        sidecar_name = str(sidecar_pipeline.get("displayName", ""))
        logger.info(
            "Triggering optional sidecar pipeline before core HDS wait; source absence is non-blocking: %s",
            sidecar_name,
        )
        non_blocking_followups[sidecar_name] = _trigger_optional_pipeline(
            fabric,
            workspace_id,
            sidecar_pipeline,
            sidecar_name,
        )

    pipeline_results = {}

    clinical_name = CORE_HDS_PIPELINE_NAMES[0]
    clinical_pipeline = pipelines_by_name.get(clinical_name)
    if not clinical_pipeline:
        pipeline_results[clinical_name] = "not_found"
        logger.warning("Pipeline not found: %s", clinical_name)
    else:
        logger.info("Triggering pipeline: %s", clinical_name)
        pipeline_results[clinical_name] = _run_blocking_pipeline(
            fabric, workspace_id, clinical_pipeline, clinical_name
        )

    cma_pipeline = pipelines_by_name.get(CMA_PIPELINE_NAME)
    if pipeline_results.get(clinical_name) == "completed":
        non_blocking_followups[CMA_PIPELINE_NAME] = _trigger_optional_pipeline(
            fabric,
            workspace_id,
            cma_pipeline,
            CMA_PIPELINE_NAME,
        )
    else:
        non_blocking_followups[CMA_PIPELINE_NAME] = "skipped_clinical_incomplete"
        logger.warning(
            "Skipping optional CMA pipeline because Clinical/Silver readiness did not complete"
        )

    prior_pipeline_completed = pipeline_results.get(clinical_name) == "completed"
    for pipeline_name in CORE_HDS_PIPELINE_NAMES[1:]:
        if not prior_pipeline_completed:
            pipeline_results[pipeline_name] = "skipped_prerequisites_incomplete"
            logger.warning(
                "Skipping pipeline because a prior HDS pipeline did not complete: %s",
                pipeline_name,
            )
            continue

        pipeline = pipelines_by_name.get(pipeline_name)
        if not pipeline:
            pipeline_results[pipeline_name] = "not_found"
            prior_pipeline_completed = False
            logger.warning("Pipeline not found: %s", pipeline_name)
            continue

        logger.info("Triggering pipeline: %s", pipeline_name)
        pipeline_status = _run_blocking_pipeline(fabric, workspace_id, pipeline, pipeline_name)
        pipeline_results[pipeline_name] = pipeline_status
        prior_pipeline_completed = pipeline_status == "completed"

    duration = time.time() - start

    return {
        "phase": "Phase 3: DICOM Shortcut + HDS Pipelines",
        "duration_seconds": duration,
        "resources": {
            "bronze_lakehouse_id": bronze_lh_id,
            "bronze_lakehouse_name": bronze_lh_name,
        },
        "pipeline_results": pipeline_results,
        "non_blocking_followups": non_blocking_followups,
    }
