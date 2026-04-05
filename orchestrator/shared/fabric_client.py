"""Fabric REST API client.

Replaces the PowerShell Invoke-FabricApi helper with a Python equivalent.
Handles 429 rate-limiting, 202 long-running operations, and token refresh.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


class FabricClient:
    """Wrapper for Fabric REST API calls with retry and LRO support."""

    def __init__(self, api_base: str = FABRIC_API_BASE) -> None:
        self.api_base = api_base
        self.credential = DefaultAzureCredential()
        self._token: str | None = None
        self._token_expires: float = 0

    # ── Auth ───────────────────────────────────────────────────────────

    def _get_token(self) -> str:
        """Get or refresh the Fabric access token."""
        now = time.time()
        if self._token and now < self._token_expires - 60:
            return self._token

        token = self.credential.get_token(f"{FABRIC_RESOURCE}/.default")
        self._token = token.token
        self._token_expires = token.expires_on
        return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # ── Core API call ──────────────────────────────────────────────────

    def call(
        self,
        method: str,
        endpoint: str,
        body: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> dict[str, Any] | list | None:
        """Make a Fabric API call with retry and LRO handling.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            endpoint: API path (e.g. /workspaces/{id}/items).
            body: JSON body for POST/PUT/PATCH.
            max_retries: Max retry attempts for 429/transient errors.

        Returns:
            Parsed JSON response, or None for 204 No Content.
        """
        url = f"{self.api_base}{endpoint}"

        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    json=body if body and method != "GET" else None,
                    timeout=120,
                )

                # Success
                if resp.status_code in (200, 201):
                    return resp.json() if resp.content else None
                if resp.status_code == 204:
                    return None

                # 202 — Long-running operation
                if resp.status_code == 202:
                    return self._poll_lro(resp)

                # 429 — Rate limited
                if resp.status_code == 429 and attempt < max_retries:
                    retry_after = int(
                        resp.headers.get("Retry-After", "30")
                    )
                    logger.warning(
                        "Rate limited (429). Waiting %ds (attempt %d/%d)…",
                        retry_after,
                        attempt,
                        max_retries,
                    )
                    time.sleep(retry_after)
                    continue

                # Other errors
                resp.raise_for_status()

            except requests.exceptions.HTTPError:
                if attempt < max_retries:
                    logger.warning(
                        "HTTP %s on %s (attempt %d/%d): %s",
                        resp.status_code,
                        endpoint,
                        attempt,
                        max_retries,
                        resp.text[:500],
                    )
                    time.sleep(5 * attempt)
                    continue
                raise

        return None

    def _poll_lro(
        self, initial_response: requests.Response, timeout_seconds: int = 600
    ) -> dict[str, Any] | None:
        """Poll a long-running operation until complete."""
        location = initial_response.headers.get("Location")
        operation_id = initial_response.headers.get("x-ms-operation-id")
        retry_after = int(initial_response.headers.get("Retry-After", "5"))

        if not location and not operation_id:
            return initial_response.json() if initial_response.content else None

        poll_url = location or f"{self.api_base}/operations/{operation_id}"
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            time.sleep(retry_after)
            resp = requests.get(
                poll_url, headers=self._headers(), timeout=60
            )

            if resp.status_code == 200:
                data = resp.json() if resp.content else {}
                status = data.get("status", "").lower()
                if status in ("succeeded", "completed"):
                    return data
                if status in ("failed", "cancelled"):
                    raise RuntimeError(
                        f"LRO failed: {data.get('error', data)}"
                    )
                retry_after = int(resp.headers.get("Retry-After", "5"))
            elif resp.status_code == 202:
                retry_after = int(resp.headers.get("Retry-After", "5"))
            else:
                resp.raise_for_status()

        raise TimeoutError(f"LRO timed out after {timeout_seconds}s: {poll_url}")

    # ── Workspace Operations ───────────────────────────────────────────

    def find_workspace(self, name: str) -> dict[str, Any] | None:
        """Find a workspace by display name."""
        result = self.call("GET", "/workspaces")
        if result and "value" in result:
            for ws in result["value"]:
                if ws.get("displayName") == name:
                    return ws
        return None

    def provision_workspace_identity(self, workspace_id: str) -> dict[str, Any] | None:
        """Provision a managed identity for the workspace."""
        return self.call(
            "POST", f"/workspaces/{workspace_id}/provisionIdentity"
        )

    def deprovision_workspace_identity(self, workspace_id: str) -> None:
        """Remove the workspace managed identity."""
        self.call("POST", f"/workspaces/{workspace_id}/deprovisionIdentity")

    # ── Item Discovery ─────────────────────────────────────────────────

    def list_items(
        self, workspace_id: str, item_type: str | None = None
    ) -> list[dict[str, Any]]:
        """List items in a workspace, optionally filtered by type."""
        endpoint = f"/workspaces/{workspace_id}/items"
        if item_type:
            endpoint += f"?type={item_type}"
        result = self.call("GET", endpoint)
        return result.get("value", []) if result else []

    def find_item(
        self, workspace_id: str, display_name: str, item_type: str | None = None
    ) -> dict[str, Any] | None:
        """Find an item by display name (and optional type)."""
        items = self.list_items(workspace_id, item_type)
        for item in items:
            if item.get("displayName") == display_name:
                return item
        return None

    # ── Item CRUD ──────────────────────────────────────────────────────

    def create_item(
        self,
        workspace_id: str,
        display_name: str,
        item_type: str,
        definition: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Create a new item in a workspace."""
        body: dict[str, Any] = {
            "displayName": display_name,
            "type": item_type,
        }
        if definition:
            body["definition"] = definition
        return self.call("POST", f"/workspaces/{workspace_id}/items", body)

    def update_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        definition: dict[str, Any],
        update_metadata: bool = False,
    ) -> dict[str, Any] | None:
        """Update an item's definition."""
        endpoint = f"/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        if update_metadata:
            endpoint += "?updateMetadata=true"
        return self.call("POST", endpoint, definition)

    def delete_item(self, workspace_id: str, item_id: str) -> None:
        """Delete an item."""
        self.call("DELETE", f"/workspaces/{workspace_id}/items/{item_id}")

    # ── Lakehouse ──────────────────────────────────────────────────────

    def find_lakehouse(
        self, workspace_id: str, name_pattern: str
    ) -> dict[str, Any] | None:
        """Find a lakehouse whose name contains name_pattern (case-insensitive)."""
        items = self.list_items(workspace_id, "Lakehouse")
        for item in items:
            if name_pattern.lower() in item.get("displayName", "").lower():
                return item
        return None

    # ── KQL Database / Eventhouse ──────────────────────────────────────

    def find_kql_database(
        self, workspace_id: str, name: str
    ) -> dict[str, Any] | None:
        """Find a KQL database by name."""
        return self.find_item(workspace_id, name, "KQLDatabase")

    def find_eventhouse(
        self, workspace_id: str, name: str
    ) -> dict[str, Any] | None:
        """Find an eventhouse by name."""
        return self.find_item(workspace_id, name, "Eventhouse")

    # ── Data Agents ────────────────────────────────────────────────────

    def create_data_agent(
        self,
        workspace_id: str,
        display_name: str,
        description: str,
        instructions: str,
        datasources: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Create a Data Agent (AI Skill) with datasources."""
        body = {
            "displayName": display_name,
            "description": description,
            "instructions": instructions,
            "datasources": datasources,
        }
        return self.call(
            "POST", f"/workspaces/{workspace_id}/dataAgents", body
        )

    # ── Shortcuts ──────────────────────────────────────────────────────

    def create_adls_shortcut(
        self,
        workspace_id: str,
        item_id: str,
        shortcut_name: str,
        shortcut_path: str,
        connection_id: str,
        adls_location: str,
        adls_subpath: str,
    ) -> dict[str, Any] | None:
        """Create an ADLS Gen2 shortcut in a lakehouse."""
        body = {
            "path": shortcut_path,
            "name": shortcut_name,
            "target": {
                "adlsGen2": {
                    "location": adls_location,
                    "subpath": adls_subpath,
                    "connectionId": connection_id,
                }
            },
        }
        return self.call(
            "POST",
            f"/workspaces/{workspace_id}/items/{item_id}/shortcuts",
            body,
        )

    # ── Connections ────────────────────────────────────────────────────

    def list_connections(self) -> list[dict[str, Any]]:
        """List all connections."""
        result = self.call("GET", "/connections")
        return result.get("value", []) if result else []

    def delete_connection(self, connection_id: str) -> None:
        """Delete a connection."""
        self.call("DELETE", f"/connections/{connection_id}")
