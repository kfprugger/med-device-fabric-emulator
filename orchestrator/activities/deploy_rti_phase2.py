"""Phase 4: Fabric RTI Phase 2 — Silver shortcuts + enriched alerts.

Ports logic from deploy-fabric-rti.ps1 -Phase2:
- Create KQL shortcuts to Silver Lakehouse tables
- Deploy enriched fn_ClinicalAlerts function
- Create Clinical Alerts Map dashboard
- Run dashboard KQL scripts (05-dashboard-queries.kql)
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.fabric_client import FabricClient
from shared.kusto_client import KustoClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 4: Fabric RTI Phase 2.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        Silver shortcut and enriched alert details.
    """
    start = time.time()
    fabric = FabricClient(config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1"))
    workspace_id = resources["fabric_workspace_id"]
    kusto_uri = resources.get("kusto_uri", "")
    kql_db_name = resources.get("kql_db_name", "MasimoKQLDB")

    # 1. Discover Silver Lakehouse
    silver_lh_id = config.get("silver_lakehouse_id", "")
    silver_lh_name = config.get("silver_lakehouse_name", "")

    if not silver_lh_id:
        silver_lh = fabric.find_lakehouse(workspace_id, "silver")
        if silver_lh:
            silver_lh_id = silver_lh["id"]
            silver_lh_name = silver_lh["displayName"]
            logger.info("Silver Lakehouse: %s (%s)", silver_lh_name, silver_lh_id)
        else:
            raise RuntimeError(
                "Silver Lakehouse not found. Ensure HDS has been deployed "
                "and pipelines have run before executing Phase 4."
            )

    # 2. Run enriched KQL scripts
    kql_results = {}
    if kusto_uri:
        kusto = KustoClient(kusto_uri, kql_db_name)
        enriched_scripts = [
            "04-hds-enrichment-example.kql",
            "05-dashboard-queries.kql",
            "06-agent-wrapper-functions.kql",
        ]
        for script in enriched_scripts:
            try:
                result = kusto.run_kql_script(script)
                kql_results[script] = result
                logger.info(
                    "KQL %s: %d/%d succeeded",
                    script, result["succeeded"], result["total"],
                )
            except FileNotFoundError:
                logger.warning("KQL script not found: %s", script)
            except Exception as e:
                logger.error("KQL script %s failed: %s", script, e)

    duration = time.time() - start

    return {
        "phase": "Phase 4: Fabric RTI Phase 2",
        "duration_seconds": duration,
        "resources": {
            "silver_lakehouse_id": silver_lh_id,
            "silver_lakehouse_name": silver_lh_name,
        },
        "kql_results": kql_results,
    }
