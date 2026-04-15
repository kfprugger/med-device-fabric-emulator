"""Phase 1: Deploy base Azure infrastructure.

Ports logic from phase-1/deploy.ps1:
- Ensure resource group
- Deploy infra.bicep (Event Hub, ACR, Storage, Key Vault, Managed Identity)
- Build emulator container image in ACR
- Deploy emulator.bicep (Masimo ACI container with system-assigned identity)
- Assign Event Hubs Data Sender role to emulator MI
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.azure_client import AzureClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 1: Base Azure Infrastructure.

    Args:
        config: DeploymentConfig as dict.

    Returns:
        Resource IDs and names created in this phase.
    """
    start = time.time()
    client = AzureClient()

    rg_name = config["resource_group_name"]
    location = config["location"]
    tags = config.get("tags", {})
    admin_group = config.get("admin_security_group", "")

    # Resolve admin security group object ID
    admin_group_id = ""
    if admin_group:
        try:
            admin_group_id = client.resolve_security_group_id(admin_group)
            logger.info("Admin group '%s' → %s", admin_group, admin_group_id)
        except Exception as e:
            logger.warning("Could not resolve admin group '%s': %s", admin_group, e)

    # 1. Ensure resource group
    client.ensure_resource_group(rg_name, location, tags)

    # 2. Deploy infra.bicep
    infra_outputs = client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="infra",
        template_file="infra.bicep",
        parameters={"adminGroupObjectId": admin_group_id} if admin_group_id else {},
        tags=tags,
    )

    acr_name = infra_outputs.get("acrName", "")
    event_hub_namespace = infra_outputs.get("eventHubNamespace", "")
    event_hub_name = infra_outputs.get("eventHubName", "telemetry-stream")
    storage_account = infra_outputs.get("storageAccountName", "")
    managed_identity_id = infra_outputs.get("managedIdentityId", "")
    managed_identity_client_id = infra_outputs.get("managedIdentityClientId", "")

    # 3. Build emulator container image
    if acr_name:
        import os

        emulator_context = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        )
        try:
            image_uri = client.build_container_image(
                resource_group=rg_name,
                acr_name=acr_name,
                image_name="masimo-emulator",
                image_tag="v1",
                docker_context_path=emulator_context,
            )
            logger.info("Emulator image: %s", image_uri)
        except Exception as e:
            logger.warning("ACR build failed (may already exist): %s", e)

    # 4. Deploy emulator.bicep
    emulator_outputs = client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="emulator",
        template_file="emulator.bicep",
        parameters={
            "acrName": acr_name,
            "eventHubNamespace": event_hub_namespace,
            "eventHubName": event_hub_name,
        },
        tags=tags,
    )

    duration = time.time() - start

    return {
        "phase": "Phase 1: Base Azure Infrastructure",
        "duration_seconds": duration,
        "resources": {
            "resource_group_name": rg_name,
            "acr_name": acr_name,
            "event_hub_namespace": event_hub_namespace,
            "event_hub_name": event_hub_name,
            "storage_account_name": storage_account,
            "managed_identity_id": managed_identity_id,
            "managed_identity_client_id": managed_identity_client_id,
            **{f"emulator_{k}": v for k, v in emulator_outputs.items()},
        },
    }
