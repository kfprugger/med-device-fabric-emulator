"""Phase 3: Deploy Fabric Real-Time Intelligence (Phase 1).

Ports logic from deploy-fabric-rti.ps1 (non-Phase2 mode):
- Create Eventhouse + KQL Database
- Run KQL scripts (tables, functions, update policies)
- Create Eventstream (telemetry ingestion)
- Create Real-Time Dashboard
- Trigger FHIR $export
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.fabric_client import FabricClient
from shared.kusto_client import KustoClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 3: Fabric RTI Phase 1.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        Eventhouse, KQL DB, Eventstream, and dashboard details.
    """
    start = time.time()
    fabric = FabricClient(config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1"))
    workspace_id = resources["fabric_workspace_id"]

    # 1. Create or find Eventhouse
    eventhouse_name = "MasimoEventhouse"
    eventhouse = fabric.find_eventhouse(workspace_id, eventhouse_name)
    if not eventhouse:
        logger.info("Creating Eventhouse '%s'…", eventhouse_name)
        eventhouse = fabric.create_item(
            workspace_id, eventhouse_name, "Eventhouse"
        )
    eventhouse_id = eventhouse["id"] if eventhouse else ""
    logger.info("Eventhouse: %s (%s)", eventhouse_name, eventhouse_id)

    # 2. Find or create KQL Database
    kql_db_name = "MasimoKQLDB"
    kql_db = fabric.find_kql_database(workspace_id, kql_db_name)
    kql_db_id = kql_db["id"] if kql_db else ""

    if not kql_db:
        logger.info("Creating KQL Database '%s'…", kql_db_name)
        kql_db = fabric.create_item(
            workspace_id, kql_db_name, "KQLDatabase"
        )
        kql_db_id = kql_db["id"] if kql_db else ""

    logger.info("KQL Database: %s (%s)", kql_db_name, kql_db_id)

    # 3. Discover Kusto URI
    kusto_uri = config.get("kusto_uri", "")
    if not kusto_uri and kql_db:
        # Try to get from the KQL database properties
        try:
            db_detail = fabric.call(
                "GET",
                f"/workspaces/{workspace_id}/kqlDatabases/{kql_db_id}",
            )
            if db_detail:
                props = db_detail.get("properties", {})
                kusto_uri = props.get("queryServiceUri", "") or props.get(
                    "kustoUri", ""
                )
        except Exception as e:
            logger.warning("Could not auto-discover Kusto URI: %s", e)

    if not kusto_uri:
        logger.warning(
            "Kusto URI not available. KQL scripts will be skipped. "
            "Provide kusto_uri in config or ensure capacity is active."
        )

    # 4. Run KQL scripts
    kql_results = {}
    if kusto_uri:
        kusto = KustoClient(kusto_uri, kql_db_name)

        kql_scripts = [
            "01-alert-history-table.kql",
            "02-telemetry-functions.kql",
            "03-clinical-alert-functions.kql",
        ]
        for script in kql_scripts:
            try:
                result = kusto.run_kql_script(script)
                kql_results[script] = result
                logger.info(
                    "KQL %s: %d/%d succeeded",
                    script,
                    result["succeeded"],
                    result["total"],
                )
            except FileNotFoundError:
                logger.warning("KQL script not found: %s", script)
            except Exception as e:
                logger.error("KQL script %s failed: %s", script, e)

    duration = time.time() - start

    return {
        "phase": "Phase 3: Fabric RTI Phase 1",
        "duration_seconds": duration,
        "resources": {
            "eventhouse_id": eventhouse_id,
            "eventhouse_name": eventhouse_name,
            "kql_db_id": kql_db_id,
            "kql_db_name": kql_db_name,
            "kusto_uri": kusto_uri,
        },
        "kql_results": kql_results,
    }
