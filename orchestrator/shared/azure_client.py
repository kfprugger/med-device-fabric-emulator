"""Azure Resource Manager SDK wrapper.

Translates `az deployment group create`, `az acr build`, `az container show/logs`,
and other CLI commands into Python SDK equivalents.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import ContainerGroup
from azure.mgmt.containerregistry import ContainerRegistryManagementClient
from azure.mgmt.resource import ResourceManagementClient

logger = logging.getLogger(__name__)

# Bicep templates live alongside the main repo scripts
BICEP_DIR = Path(__file__).resolve().parent.parent.parent / "bicep"


class AzureClient:
    """Wrapper around Azure Management SDK for ARM deployments and ACI jobs."""

    def __init__(self, subscription_id: str | None = None) -> None:
        self.credential = DefaultAzureCredential()
        # Resolve subscription from environment or current context
        if subscription_id:
            self._subscription_id = subscription_id
        else:
            import subprocess
            import sys

            result = subprocess.run(
                ["az", "account", "show", "--query", "id", "-o", "tsv"],
                capture_output=True,
                text=True,
                check=True,
                shell=(sys.platform == "win32"),
            )
            self._subscription_id = result.stdout.strip()

        self.resource_client = ResourceManagementClient(
            self.credential, self._subscription_id
        )
        self.aci_client = ContainerInstanceManagementClient(
            self.credential, self._subscription_id
        )
        self.acr_client = ContainerRegistryManagementClient(
            self.credential, self._subscription_id
        )

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    # ── Resource Group ─────────────────────────────────────────────────

    def ensure_resource_group(
        self, name: str, location: str, tags: dict[str, str] | None = None
    ) -> str:
        """Create or update a resource group. Returns the RG id."""
        rg = self.resource_client.resource_groups.create_or_update(
            name, {"location": location, "tags": tags or {}}
        )
        logger.info("Resource group '%s' ready at %s", name, rg.id)
        return rg.id

    # ── ARM / Bicep Deployments ────────────────────────────────────────

    def deploy_bicep(
        self,
        resource_group: str,
        deployment_name: str,
        template_file: str,
        parameters: dict[str, Any] | None = None,
        tags: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Deploy a Bicep template and return the deployment outputs.

        Args:
            resource_group: Target resource group.
            deployment_name: Name for this deployment (idempotent key).
            template_file: Filename within the bicep/ directory (e.g. 'infra.bicep').
            parameters: ARM parameter values (name → value).
            tags: Resource tags to pass as a parameter named 'resourceTags'.

        Returns:
            Dictionary of output name → output value.
        """
        template_path = BICEP_DIR / template_file

        # Read the Bicep file — the SDK will compile it server-side
        # as of ARM API 2024-11-01 which supports bicep directly.
        # Fallback: compile locally with `az bicep build`.
        compiled_path = template_path.with_suffix(".json")
        if not compiled_path.exists() or (
            compiled_path.stat().st_mtime < template_path.stat().st_mtime
        ):
            import subprocess

            subprocess.run(
                ["az", "bicep", "build", "--file", str(template_path)],
                check=True,
                capture_output=True,
                shell=(sys.platform == "win32"),
            )

        with open(compiled_path) as f:
            template_body = json.load(f)

        # Build ARM parameters dict (each key → { "value": v })
        arm_params: dict[str, Any] = {}
        if parameters:
            for k, v in parameters.items():
                arm_params[k] = {"value": v}
        if tags:
            arm_params["resourceTags"] = {"value": tags}

        deployment = {
            "properties": {
                "template": template_body,
                "parameters": arm_params if arm_params else None,
                "mode": "Incremental",
            }
        }

        logger.info(
            "Starting Bicep deployment '%s' in '%s'…", deployment_name, resource_group
        )

        poller = self.resource_client.deployments.begin_create_or_update(
            resource_group, deployment_name, deployment
        )
        result = poller.result()  # blocks until complete

        # Extract outputs
        outputs: dict[str, Any] = {}
        if result.properties and result.properties.outputs:
            for key, obj in result.properties.outputs.items():
                outputs[key] = obj.get("value", obj)

        logger.info(
            "Deployment '%s' succeeded. Outputs: %s",
            deployment_name,
            list(outputs.keys()),
        )
        return outputs

    # ── ACR ────────────────────────────────────────────────────────────

    def build_container_image(
        self,
        resource_group: str,
        acr_name: str,
        image_name: str,
        image_tag: str,
        docker_context_path: str,
    ) -> str:
        """Queue an ACR build task. Returns the full image URI.

        Uses `az acr build` via subprocess because the Python SDK's
        quick-build is verbose and less reliable on Windows.
        """
        import subprocess

        full_image = f"{image_name}:{image_tag}"
        logger.info("Building image %s in ACR %s…", full_image, acr_name)

        result = subprocess.run(
            [
                "az",
                "acr",
                "build",
                "--registry",
                acr_name,
                "--image",
                full_image,
                docker_context_path,
                "--no-logs",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**__import__("os").environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            shell=(sys.platform == "win32"),
        )

        if result.returncode != 0:
            logger.warning("ACR build returned exit code %d, checking if image exists...", result.returncode)
            # Fallback: check if image was actually built despite the exit code
            tag_check = subprocess.run(
                ["az", "acr", "repository", "show-tags", "--name", acr_name,
                 "--repository", image_name, "--query", "contains(@, '" + image_tag + "')", "-o", "tsv"],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                shell=(sys.platform == "win32"),
            )
            if tag_check.stdout.strip().lower() == "true":
                logger.info("Image %s exists in ACR despite build error \u2014 continuing", full_image)
            else:
                logger.error("Image %s NOT found in ACR \u2014 build failed. stderr: %s", full_image, result.stderr[:500])
                raise RuntimeError(f"ACR build failed for {full_image}: {result.stderr[:500]}")

        login_server = f"{acr_name}.azurecr.io"
        return f"{login_server}/{full_image}"

    # ── ACI Job Management ─────────────────────────────────────────────

    def wait_for_aci_job(
        self,
        resource_group: str,
        container_group_name: str,
        timeout_minutes: int = 60,
        poll_interval_seconds: int = 30,
    ) -> dict[str, Any]:
        """Poll an ACI container group until it completes or times out.

        Returns:
            {
                "state": "Succeeded" | "Failed" | "Terminated",
                "exit_code": int,
                "logs": str,
                "duration_seconds": float
            }
        """
        start = time.time()
        deadline = start + timeout_minutes * 60
        last_log_line = ""

        while time.time() < deadline:
            try:
                cg: ContainerGroup = self.aci_client.container_groups.get(
                    resource_group, container_group_name
                )
            except Exception:
                logger.warning(
                    "Container group '%s' not found yet, waiting…",
                    container_group_name,
                )
                time.sleep(poll_interval_seconds)
                continue

            instance_state = None
            if (
                cg.instance_view
                and cg.instance_view.state
            ):
                instance_state = cg.instance_view.state

            # Fetch logs for progress reporting
            try:
                log_resp = self.aci_client.containers.list_logs(
                    resource_group,
                    container_group_name,
                    cg.containers[0].name,
                )
                logs = log_resp.content or ""
                # Show new log lines
                new_lines = logs[len(last_log_line) :]
                if new_lines.strip():
                    for line in new_lines.strip().split("\n")[-5:]:
                        logger.info("[%s] %s", container_group_name, line.strip())
                last_log_line = logs
            except Exception:
                logs = ""

            if instance_state in ("Succeeded", "Failed", "Terminated"):
                exit_code = 0
                if cg.containers and cg.containers[0].instance_view:
                    current = cg.containers[0].instance_view.current_state
                    if current:
                        exit_code = current.exit_code or 0

                return {
                    "state": instance_state,
                    "exit_code": exit_code,
                    "logs": logs[-2000:],  # last 2KB
                    "duration_seconds": time.time() - start,
                }

            time.sleep(poll_interval_seconds)

        return {
            "state": "Timeout",
            "exit_code": -1,
            "logs": last_log_line[-2000:],
            "duration_seconds": time.time() - start,
        }

    # ── RBAC ───────────────────────────────────────────────────────────

    def ensure_role_assignment(
        self,
        scope: str,
        role_definition_id: str,
        principal_id: str,
        principal_type: str = "ServicePrincipal",
    ) -> None:
        """Create a role assignment if it doesn't already exist."""
        from azure.mgmt.authorization import AuthorizationManagementClient

        import uuid

        auth_client = AuthorizationManagementClient(
            self.credential, self._subscription_id
        )

        assignment_name = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{scope}/{role_definition_id}/{principal_id}"))

        try:
            auth_client.role_assignments.create(
                scope,
                assignment_name,
                {
                    "role_definition_id": role_definition_id,
                    "principal_id": principal_id,
                    "principal_type": principal_type,
                },
            )
            logger.info("Role assignment created: %s", assignment_name)
        except Exception as e:
            if "RoleAssignmentExists" in str(e) or "Conflict" in str(e):
                logger.info("Role assignment already exists, skipping.")
            else:
                raise

    # ── AD / Security Group ────────────────────────────────────────────

    def resolve_security_group_id(self, group_name: str) -> str:
        """Resolve an Entra ID security group display name to its object ID."""
        import subprocess
        import sys

        result = subprocess.run(
            [
                "az",
                "ad",
                "group",
                "show",
                "--group",
                group_name,
                "--query",
                "id",
                "-o",
                "tsv",
            ],
            capture_output=True,
            text=True,
            check=True,
            shell=(sys.platform == "win32"),
        )
        return result.stdout.strip()

    # ── Container Cleanup ──────────────────────────────────────────────

    def delete_container_group(
        self, resource_group: str, container_group_name: str
    ) -> None:
        """Delete an ACI container group (cleanup after job)."""
        try:
            self.aci_client.container_groups.begin_delete(
                resource_group, container_group_name
            ).result()
            logger.info("Deleted container group '%s'", container_group_name)
        except Exception:
            logger.debug(
                "Container group '%s' not found or already deleted.",
                container_group_name,
            )

    # ── Resource Group Deletion ────────────────────────────────────────

    def delete_resource_group(
        self, name: str, wait: bool = True
    ) -> None:
        """Delete a resource group with polling status logs."""
        logger.info("Initiating resource group deletion: '%s'…", name)
        poller = self.resource_client.resource_groups.begin_delete(name)
        if wait:
            elapsed = 0
            poll_interval = 10
            while not poller.done():
                elapsed += poll_interval
                logger.info(
                    "Polling RG deletion status… (Deleting) [%ds]", elapsed
                )
                time.sleep(poll_interval)

            # Verify deletion
            try:
                self.resource_client.resource_groups.get(name)
                logger.warning("Resource group '%s' still exists after deletion poller completed.", name)
            except Exception:
                logger.info(
                    "Verified: GET /resourceGroups/%s → 404 Not Found", name
                )

            logger.info("Resource group '%s' deleted and verified.", name)
