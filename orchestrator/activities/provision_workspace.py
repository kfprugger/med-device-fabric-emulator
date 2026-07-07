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

    # Ensure capacity is assigned to the selected paid Fabric capacity when provided.
    ws_detail = fabric.call("GET", f"/workspaces/{workspace_id}")
    current_capacity_id = ws_detail.get("capacityId") if ws_detail else ""
    selected_capacity_name = (config.get("capacity_name") or "").strip()

    def is_paid_f_sku(cap: dict) -> bool:
        sku = (cap.get("sku") or "").upper()
        return sku.startswith("F") and not sku.startswith("FT") and sku != "PP3"

    def find_selected_capacity(capacities: list[dict]) -> dict:
        matches = [
            cap for cap in capacities
            if cap.get("displayName") == selected_capacity_name or cap.get("name") == selected_capacity_name
        ]
        active_paid_matches = [
            cap for cap in matches
            if cap.get("state") == "Active" and is_paid_f_sku(cap)
        ]
        if not active_paid_matches:
            scope_parts = [
                config.get("capacity_subscription_id", ""),
                config.get("capacity_resource_group", ""),
                selected_capacity_name,
            ]
            scope = "/".join(part for part in scope_parts if part)
            seen = ", ".join(
                f"{cap.get('displayName') or cap.get('name')} [{cap.get('sku')}, {cap.get('state')}]"
                for cap in matches
            ) or "not returned by Fabric"
            raise RuntimeError(
                f"Selected Fabric capacity '{scope}' is not an active paid F-SKU; {seen}. "
                "Trial FT capacities are not supported."
            )
        return active_paid_matches[0]

    target_capacity = None
    if selected_capacity_name or not current_capacity_id:
        if selected_capacity_name:
            logger.info("Resolving selected Fabric capacity '%s'…", selected_capacity_name)
        else:
            logger.info("No capacity assigned — searching for an active paid Fabric capacity…")

        caps_result = fabric.call("GET", "/capacities")
        capacities = caps_result.get("value", []) if caps_result else []

        if selected_capacity_name:
            target_capacity = find_selected_capacity(capacities)
        else:
            paid_caps = [
                cap for cap in capacities
                if cap.get("state") == "Active" and is_paid_f_sku(cap)
            ]
            if not paid_caps:
                raise RuntimeError(
                    "No active paid Fabric F-SKU capacity found. Provision or resume a paid F-SKU (F2+); "
                    "trial FT capacities are not supported."
                )
            target_capacity = paid_caps[0]

    if selected_capacity_name:
        if current_capacity_id != target_capacity.get("id"):
            action = "Reassigning" if current_capacity_id else "Assigning"
            logger.info(
                "%s workspace to selected capacity: %s (SKU: %s)…",
                action,
                target_capacity.get("displayName", ""),
                target_capacity.get("sku", ""),
            )
            fabric.call(
                "POST",
                f"/workspaces/{workspace_id}/assignToCapacity",
                {"capacityId": target_capacity["id"]},
            )
            import time as _time
            _time.sleep(5)
            logger.info("Selected capacity assigned: %s", target_capacity.get("displayName", ""))
        else:
            logger.info("Workspace already assigned to selected capacity: %s", selected_capacity_name)
    elif not current_capacity_id:
        logger.info(
            "Assigning capacity: %s (SKU: %s)…",
            target_capacity.get("displayName", ""),
            target_capacity.get("sku", ""),
        )
        fabric.call(
            "POST",
            f"/workspaces/{workspace_id}/assignToCapacity",
            {"capacityId": target_capacity["id"]},
        )
        import time as _time
        _time.sleep(5)
        logger.info("Capacity assigned: %s", target_capacity.get("displayName", ""))
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
        "phase": "Phase 1: Fabric Workspace",
        "duration_seconds": duration,
        "resources": {
            "fabric_workspace_id": workspace_id,
            "fabric_workspace_name": ws_name,
            "workspace_identity": str(identity) if identity else "",
        },
    }
