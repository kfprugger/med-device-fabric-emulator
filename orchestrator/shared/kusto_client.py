"""Kusto (KQL) management client.

Translates the PowerShell Invoke-KustoMgmt helper into Python.
Used for creating tables, functions, external tables, and update policies.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import requests
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

KUSTO_RESOURCE = "https://api.kusto.windows.net"

# KQL script files live in the repo
KQL_DIR = Path(__file__).resolve().parent.parent.parent / "fabric-rti" / "kql"


class KustoClient:
    """Execute KQL management commands against an Eventhouse."""

    def __init__(self, kusto_uri: str, database_name: str) -> None:
        self.kusto_uri = kusto_uri.rstrip("/")
        self.database_name = database_name
        self.credential = DefaultAzureCredential()
        self._token: str | None = None
        self._token_expires: float = 0

    def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expires - 60:
            return self._token
        token = self.credential.get_token(f"{KUSTO_RESOURCE}/.default")
        self._token = token.token
        self._token_expires = token.expires_on
        return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def execute_mgmt(self, command: str, label: str = "") -> bool:
        """Execute a KQL management command (.create, .alter, etc.).

        Returns True on success, False on failure.
        Treats 'already exists' as success (idempotent).
        """
        body = {"db": self.database_name, "csl": command}
        display = label or command[:80]

        try:
            resp = requests.post(
                f"{self.kusto_uri}/v1/rest/mgmt",
                headers=self._headers(),
                json=body,
                timeout=120,
            )
            resp.raise_for_status()
            logger.info("✓ %s", display)
            return True
        except requests.exceptions.HTTPError as e:
            error_text = e.response.text if e.response else str(e)
            if "already exists" in error_text.lower():
                logger.info("✓ %s (already exists)", display)
                return True
            logger.error("✗ %s: %s", display, error_text[:500])
            return False

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a KQL query and return rows as dicts."""
        body = {"db": self.database_name, "csl": query}
        resp = requests.post(
            f"{self.kusto_uri}/v1/rest/query",
            headers=self._headers(),
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()

        # Parse Kusto V1 response format
        rows: list[dict[str, Any]] = []
        if "Tables" in data and data["Tables"]:
            table = data["Tables"][0]
            columns = [col["ColumnName"] for col in table.get("Columns", [])]
            for row in table.get("Rows", []):
                rows.append(dict(zip(columns, row)))
        return rows

    def run_kql_script(self, script_filename: str) -> dict[str, Any]:
        """Run all management commands from a .kql file.

        Each command is separated by blank lines. Lines starting with //
        are treated as comments and logged as labels.

        Returns:
            {"total": N, "succeeded": N, "failed": N, "results": [...]}
        """
        script_path = KQL_DIR / script_filename
        if not script_path.exists():
            raise FileNotFoundError(f"KQL script not found: {script_path}")

        content = script_path.read_text(encoding="utf-8")
        commands = self._parse_kql_commands(content)

        results: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0

        for cmd in commands:
            ok = self.execute_mgmt(cmd["command"], cmd.get("label", ""))
            if ok:
                succeeded += 1
            else:
                failed += 1
            results.append(
                {"label": cmd.get("label", ""), "success": ok}
            )

        return {
            "total": len(commands),
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }

    @staticmethod
    def _parse_kql_commands(content: str) -> list[dict[str, str]]:
        """Parse a .kql file into individual commands with optional labels."""
        commands: list[dict[str, str]] = []
        current_lines: list[str] = []
        current_label = ""

        for line in content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("//") and not current_lines:
                current_label = stripped.lstrip("/ ").strip()
                continue
            if stripped == "" and current_lines:
                cmd_text = "\n".join(current_lines).strip()
                if cmd_text:
                    commands.append(
                        {"command": cmd_text, "label": current_label}
                    )
                current_lines = []
                current_label = ""
                continue
            if stripped:
                current_lines.append(line)

        # Final command
        if current_lines:
            cmd_text = "\n".join(current_lines).strip()
            if cmd_text:
                commands.append({"command": cmd_text, "label": current_label})

        return commands
