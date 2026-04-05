"""Phase 1b: Provision Fabric workspace and managed identity.

Ports the inline Fabric workspace logic from Deploy-All.ps1:
- Find or create workspace
- Provision workspace managed identity
- Return workspace ID and identity details
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 1b: Fabric Workspace Provisioning.

    Args:
        config: DeploymentConfig as dict.

    Returns:
        Workspace details (id, identity).
    """
    start = time.time()
    fabric = FabricClient(config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1"))
    ws_name = config["fabric_workspace_name"]

    # Find existing workspace or create it
    ws = fabric.find_workspace(ws_name)
    if not ws:
        logger.info("Workspace '%s' not found — creating…", ws_name)
        ws = fabric.call("POST", "/workspaces", {
            "displayName": ws_name,
        })
        if not ws or "id" not in ws:
            raise RuntimeError(
                f"Failed to create workspace '{ws_name}'. "
                "Check Fabric capacity and permissions."
            )
        logger.info("Workspace '%s' created: %s", ws_name, ws["id"])
    else:
        logger.info("Workspace '%s' found: %s", ws_name, ws["id"])

    workspace_id = ws["id"]

    # Ensure capacity is assigned (matches Deploy-All.ps1 logic)
    ws_detail = fabric.call("GET", f"/workspaces/{workspace_id}")
    if ws_detail and not ws_detail.get("capacityId"):
        logger.info("No capacity assigned — searching for active Fabric capacity…")
        caps_result = fabric.call("GET", "/capacities")
        capacities = caps_result.get("value", []) if caps_result else []

        # Prefer F-SKUs (paid) over trial (FT1), exclude PP3
        def sku_priority(cap: dict) -> int:
            sku = cap.get("sku", "")
            if sku.startswith("F") and sku != "FT1":
                return 0  # Paid F-SKU
            if sku == "FT1":
                return 1  # Trial
            return 2  # Other

        active_caps = [
            c for c in capacities
            if c.get("state") == "Active" and c.get("sku") != "PP3"
        ]
        active_caps.sort(key=sku_priority)

        if active_caps:
            cap = active_caps[0]
            logger.info(
                "Assigning capacity: %s (SKU: %s)…",
                cap.get("displayName", ""),
                cap.get("sku", ""),
            )
            fabric.call(
                "POST",
                f"/workspaces/{workspace_id}/assignToCapacity",
                {"capacityId": cap["id"]},
            )
            # Wait for capacity assignment to propagate
            import time as _time
            _time.sleep(5)
            logger.info("Capacity assigned: %s", cap.get("displayName", ""))
        else:
            raise RuntimeError(
                "No active Fabric capacity found. "
                "Start a trial at https://app.fabric.microsoft.com"
            )
    else:
        logger.info("Capacity already assigned")

    # Provision managed identity
    identity = None
    try:
        identity = fabric.provision_workspace_identity(workspace_id)
        if identity:
            logger.info("Workspace identity provisioned: %s", identity)
    except Exception as e:
        logger.warning("Identity provisioning may have already been done: %s", e)

    duration = time.time() - start

    return {
        "phase": "Phase 1b: Fabric Workspace",
        "duration_seconds": duration,
        "resources": {
            "fabric_workspace_id": workspace_id,
            "fabric_workspace_name": ws_name,
            "workspace_identity": str(identity) if identity else "",
        },
    }
