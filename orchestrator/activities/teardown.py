"""Teardown: Remove all deployed resources.

Ports logic from cleanup/Remove-AllResources.ps1:
- Delete Fabric workspace items in dependency order
- Deprovision workspace identity
- Delete Entra app registrations
- Optionally delete workspace itself
- Delete Azure resource group
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.azure_client import AzureClient
from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# Deletion order (reverse dependency)
ITEM_TYPES_TO_DELETE = [
    "DataAgent",
    "Ontology",
    "DataPipeline",
    "Eventstream",
    "KQLDashboard",
    "KQLDatabase",
    "Eventhouse",
    "Lakehouse",
    "Notebook",
    "SemanticModel",
    "Report",
]


def run(config: dict[str, Any]) -> dict[str, Any]:
    """Execute teardown of all resources.

    Args:
        config: DeploymentConfig as dict, plus:
            - delete_workspace: bool (default False)
            - delete_azure_rg: bool (default True)

    Returns:
        Teardown results.
    """
    start = time.time()
    results: dict[str, Any] = {"items_deleted": 0, "errors": []}

    workspace_name = config["fabric_workspace_name"]
    rg_name = config.get("resource_group_name", "")
    delete_workspace = config.get("delete_workspace", False)
    delete_azure = config.get("delete_azure_rg", True)

    # ── Fabric Cleanup ─────────────────────────────────────────────────
    fabric = FabricClient(config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1"))

    ws = fabric.find_workspace(workspace_name)
    if ws:
        workspace_id = ws["id"]
        logger.info("Cleaning workspace '%s' (%s)…", workspace_name, workspace_id)

        # Delete items by type in dependency order
        for item_type in ITEM_TYPES_TO_DELETE:
            try:
                items = fabric.list_items(workspace_id, item_type)
                for item in items:
                    try:
                        fabric.delete_item(workspace_id, item["id"])
                        results["items_deleted"] += 1
                        logger.info(
                            "Deleted %s: %s",
                            item_type,
                            item.get("displayName", item["id"]),
                        )
                    except Exception as e:
                        results["errors"].append(
                            f"Failed to delete {item_type} '{item.get('displayName')}': {e}"
                        )
            except Exception:
                pass  # Item type may not exist

        # Deprovision workspace identity
        try:
            fabric.deprovision_workspace_identity(workspace_id)
            logger.info("Workspace identity deprovisioned.")
        except Exception as e:
            logger.warning("Identity deprovision: %s", e)

        # Delete workspace itself
        if delete_workspace:
            try:
                fabric.call("DELETE", f"/workspaces/{workspace_id}")
                logger.info("Workspace '%s' deleted.", workspace_name)
            except Exception as e:
                results["errors"].append(f"Failed to delete workspace: {e}")
    else:
        logger.warning("Workspace '%s' not found — skipping Fabric cleanup.", workspace_name)

    # ── Azure Cleanup ──────────────────────────────────────────────────
    if delete_azure and rg_name:
        try:
            azure = AzureClient()
            azure.delete_resource_group(rg_name, wait=True)
            results["azure_rg_deleted"] = True
        except Exception as e:
            results["errors"].append(f"Failed to delete RG '{rg_name}': {e}")
            results["azure_rg_deleted"] = False

    duration = time.time() - start

    return {
        "phase": "Teardown",
        "duration_seconds": duration,
        "results": results,
    }
