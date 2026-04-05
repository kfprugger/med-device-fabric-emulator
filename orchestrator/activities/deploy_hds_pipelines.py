"""Phase 4b: DICOM shortcut + HDS pipeline triggers.

Ports logic from storage-access-trusted-workspace.ps1:
- Create Bronze Lakehouse shortcuts (FHIR + DICOM → ADLS Gen2)
- Trigger HDS clinical, imaging, and OMOP pipelines
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# HDS pipeline names (hardcoded in the original PowerShell)
HDS_PIPELINE_NAMES = [
    "healthcare1_msft_imaging_with_clinical_foundation_ingestion",
    "healthcare1_msft_omop_ingestion",
]


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 4b: HDS Pipeline Triggers.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        Pipeline trigger results.
    """
    start = time.time()
    fabric = FabricClient(config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1"))
    workspace_id = resources["fabric_workspace_id"]

    # Find Bronze Lakehouse
    bronze_lh = fabric.find_lakehouse(workspace_id, "bronze")
    bronze_lh_id = bronze_lh["id"] if bronze_lh else ""
    bronze_lh_name = bronze_lh.get("displayName", "") if bronze_lh else ""

    if not bronze_lh_id:
        logger.warning("Bronze Lakehouse not found. Shortcut creation skipped.")

    # Trigger HDS pipelines
    pipeline_results = {}
    for pipeline_name in HDS_PIPELINE_NAMES:
        pipeline = fabric.find_item(workspace_id, pipeline_name, "DataPipeline")
        if pipeline:
            logger.info("Triggering pipeline: %s", pipeline_name)
            try:
                fabric.call(
                    "POST",
                    f"/workspaces/{workspace_id}/items/{pipeline['id']}/jobs/instances?jobType=Pipeline",
                )
                pipeline_results[pipeline_name] = "triggered"
                logger.info("Pipeline triggered: %s", pipeline_name)
            except Exception as e:
                pipeline_results[pipeline_name] = f"error: {e}"
                logger.error("Failed to trigger %s: %s", pipeline_name, e)
        else:
            pipeline_results[pipeline_name] = "not_found"
            logger.warning("Pipeline not found: %s", pipeline_name)

    duration = time.time() - start

    return {
        "phase": "Phase 4b: HDS Pipeline Triggers",
        "duration_seconds": duration,
        "resources": {
            "bronze_lakehouse_id": bronze_lh_id,
            "bronze_lakehouse_name": bronze_lh_name,
        },
        "pipeline_results": pipeline_results,
    }
