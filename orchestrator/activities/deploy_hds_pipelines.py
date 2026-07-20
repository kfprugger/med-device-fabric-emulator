"""Phase 4b: DICOM shortcut + HDS pipeline triggers.

Ports logic from phase-2/storage-access-trusted-workspace.ps1:
- Create Bronze Lakehouse shortcuts (FHIR + DICOM → ADLS Gen2)
- Trigger HDS sidecar, clinical, CMA, imaging, and OMOP pipelines in safe order
"""

from __future__ import annotations

import base64
import copy
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# Core HDS pipeline order is intentionally sequential: Clinical → Imaging → OMOP.
# Optional SDoH/claims sidecars are discovered from live Fabric workspace pipeline
# names and launched best-effort before the Clinical wait; they may no-op/fail
# non-blocking if source data is absent. CMA launches after Clinical/Silver
# readiness, does not block Imaging/OMOP, then waits and refreshes the exported
# CMA semantic model/report binding as the HDS finalization step.
CORE_HDS_PIPELINE_NAMES = [
    "healthcare1_msft_clinical_data_foundation_ingestion",
    "healthcare1_msft_imaging_with_clinical_foundation_ingestion",
    "healthcare1_msft_omop_analytics",
]
CMA_PIPELINE_NAME = "healthcare1_msft_cma"
CMA_SEMANTIC_MODEL_NAME = "healthcare1_msft_cma_semantic_model"
CMA_REPORT_NAMES = ("healthcare1_msft_cma_report",)
CMA_ARTIFACT_DIR = Path(__file__).resolve().parents[2] / "phase-2" / "cma-report"
# The CMA semantic model TMDL binds every table to this Gold Lakehouse via
# Sql.Database("<server>", CMA_GOLD_LAKEHOUSE). The committed artifact carries a
# stale server from whatever workspace it was last exported from, so the finalize
# step must rewrite it to the target workspace's live SQL analytics endpoint.
CMA_GOLD_LAKEHOUSE = "healthcare1_msft_gold_cma"
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


def _item_definition_format(item_dir: Path) -> str:
    if item_dir.name.endswith(".SemanticModel"):
        return "TMDL"
    if item_dir.name.endswith(".Report"):
        return "PBIR-Legacy"
    raise ValueError(f"Unsupported Fabric item artifact directory: {item_dir}")


def _load_item_definition(item_dir: Path) -> dict[str, Any]:
    if not item_dir.exists():
        raise FileNotFoundError(f"Fabric item artifact directory not found: {item_dir}")

    parts: list[dict[str, str]] = []
    for path in sorted(p for p in item_dir.rglob("*") if p.is_file()):
        rel_path = path.relative_to(item_dir).as_posix()
        parts.append(
            {
                "path": rel_path,
                "payload": base64.b64encode(path.read_bytes()).decode("ascii"),
                "payloadType": "InlineBase64",
            }
        )

    if not parts:
        raise ValueError(f"Fabric item artifact directory is empty: {item_dir}")

    return {"format": _item_definition_format(item_dir), "parts": parts}


def _patch_report_definition_connection(
    definition: dict[str, Any],
    workspace_name: str,
    semantic_model_name: str,
    semantic_model_id: str,
) -> dict[str, Any]:
    patched = copy.deepcopy(definition)
    connection_string = (
        "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
        f"{workspace_name};initial catalog={semantic_model_name};"
        f"integrated security=ClaimsToken;semanticmodelid={semantic_model_id}"
    )

    for part in patched.get("parts", []):
        if part.get("path") != "definition.pbir":
            continue

        payload = base64.b64decode(part["payload"]).decode("utf-8")
        pbir = json.loads(payload)
        pbir.setdefault("datasetReference", {}).setdefault("byConnection", {})[
            "connectionString"
        ] = connection_string
        encoded = json.dumps(pbir, indent=2).encode("utf-8")
        part["payload"] = base64.b64encode(encoded).decode("ascii")
        part["payloadType"] = "InlineBase64"
        return patched

    raise ValueError("Report definition is missing definition.pbir")


def _resolve_gold_cma_sql_endpoint(
    fabric: FabricClient, workspace_id: str
) -> str | None:
    """Return the live SQL analytics endpoint server for the target workspace's
    Gold CMA lakehouse (e.g. '<id>.datawarehouse.fabric.microsoft.com')."""
    lakehouse = fabric.find_lakehouse(workspace_id, CMA_GOLD_LAKEHOUSE)
    if not lakehouse:
        return None
    detail = fabric.call(
        "GET", f"/workspaces/{workspace_id}/lakehouses/{lakehouse['id']}"
    )
    props = (detail or {}).get("properties", {})
    return (props.get("sqlEndpointProperties") or {}).get("connectionString")


def _patch_semantic_model_datasource(
    definition: dict[str, Any], target_server: str
) -> tuple[dict[str, Any], int]:
    """Rewrite every Sql.Database("<server>", "<gold_cma_db>") in the semantic
    model's TMDL parts to point at the target workspace's live SQL endpoint.

    The committed CMA semantic model artifact (which carries the age-group measure
    fix) hardcodes the SQL endpoint server from whatever workspace it was exported
    from. Without this rewrite the overwritten model points at a dead endpoint and
    every visual renders blank ('a connection could not be made to the data source').
    """
    patched = copy.deepcopy(definition)
    # Sql.Database("SERVER", "DB") — rewrite SERVER only when DB is the gold_cma db.
    pattern = re.compile(
        r'(Sql\.Database\(\s*")([^"]+)("\s*,\s*"' + re.escape(CMA_GOLD_LAKEHOUSE) + r'"\s*\))'
    )
    rewrites = 0
    for part in patched.get("parts", []):
        if not part.get("path", "").endswith(".tmdl"):
            continue
        text = base64.b64decode(part["payload"]).decode("utf-8")
        new_text, n = pattern.subn(rf"\g<1>{target_server}\g<3>", text)
        if n:
            part["payload"] = base64.b64encode(new_text.encode("utf-8")).decode("ascii")
            part["payloadType"] = "InlineBase64"
            rewrites += n
    return patched, rewrites


def _finalize_cma_semantic_model(
    fabric: FabricClient,
    workspace_id: str,
    workspace_name: str,
) -> dict[str, str]:
    results: dict[str, str] = {}
    semantic_model = fabric.find_item(
        workspace_id, CMA_SEMANTIC_MODEL_NAME, "SemanticModel"
    )
    if not semantic_model:
        results["semantic_model"] = "not_found"
        logger.warning("CMA semantic model not found: %s", CMA_SEMANTIC_MODEL_NAME)
        return results

    semantic_model_id = str(semantic_model["id"])
    semantic_model_dir = CMA_ARTIFACT_DIR / f"{CMA_SEMANTIC_MODEL_NAME}.SemanticModel"
    semantic_model_definition = _load_item_definition(semantic_model_dir)

    # Repoint the model's Sql.Database source to the target workspace's live Gold CMA
    # SQL endpoint before overwriting; the committed artifact hardcodes a stale server.
    target_server = _resolve_gold_cma_sql_endpoint(fabric, workspace_id)
    if target_server:
        semantic_model_definition, rewrites = _patch_semantic_model_datasource(
            semantic_model_definition, target_server
        )
        if rewrites:
            logger.info(
                "CMA semantic model datasource repointed to %s (%d table bindings rewritten)",
                target_server, rewrites,
            )
            results["semantic_model_datasource"] = f"repointed:{target_server}"
        else:
            logger.warning("CMA semantic model: no Sql.Database bindings matched for rewrite")
            results["semantic_model_datasource"] = "no_match"
    else:
        logger.warning(
            "CMA semantic model: could not resolve live %s SQL endpoint; "
            "overwriting with artifact server unchanged (visuals may be blank)",
            CMA_GOLD_LAKEHOUSE,
        )
        results["semantic_model_datasource"] = "unresolved"

    fabric.update_item_definition(
        workspace_id,
        semantic_model_id,
        {"definition": semantic_model_definition},
    )
    results["semantic_model"] = "overwritten"
    logger.info("CMA semantic model overwritten from exported artifact: %s", semantic_model_id)

    for report_name in CMA_REPORT_NAMES:
        report = fabric.find_item(workspace_id, report_name, "Report")
        if report:
            report_id = str(report["id"])
            report_definition = fabric.get_item_definition(workspace_id, report_id)
            action = "rebound"
        else:
            report_id = ""
            report_dir = CMA_ARTIFACT_DIR / f"{report_name}.Report"
            report_definition = _load_item_definition(report_dir)
            action = "created"

        patched_report_definition = _patch_report_definition_connection(
            report_definition,
            workspace_name,
            CMA_SEMANTIC_MODEL_NAME,
            semantic_model_id,
        )
        if report_id:
            fabric.update_item_definition(
                workspace_id,
                report_id,
                {"definition": patched_report_definition},
            )
        else:
            created = fabric.create_item(
                workspace_id,
                report_name,
                "Report",
                patched_report_definition,
            )
            report_id = str((created or {}).get("id", ""))
        results[f"report:{report_name}"] = action
        logger.info("CMA report %s: %s (%s)", report_name, action, report_id)

    return results


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
    cma_finalization: dict[str, str] = {}

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
    cma_trigger_status = ""
    if pipeline_results.get(clinical_name) == "completed":
        cma_trigger_status = _trigger_optional_pipeline(
            fabric,
            workspace_id,
            cma_pipeline,
            CMA_PIPELINE_NAME,
        )
        non_blocking_followups[CMA_PIPELINE_NAME] = cma_trigger_status
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

    if cma_pipeline and cma_trigger_status in {
        "triggered_non_blocking",
        "already_running_non_blocking",
    }:
        cma_completion = _wait_for_pipeline_completion(
            fabric, workspace_id, cma_pipeline, CMA_PIPELINE_NAME
        )
        cma_finalization["pipeline_completion"] = cma_completion
        if cma_completion == "completed":
            workspace_name = str(
                config.get("fabric_workspace_name")
                or resources.get("fabric_workspace_name")
                or workspace_id
            )
            try:
                cma_finalization.update(
                    _finalize_cma_semantic_model(fabric, workspace_id, workspace_name)
                )
            except Exception as e:
                logger.exception("CMA semantic model finalization failed")
                cma_finalization["semantic_model"] = f"warning: {e}"
        else:
            cma_finalization["semantic_model"] = "skipped_cma_incomplete"

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
        "cma_finalization": cma_finalization,
    }
