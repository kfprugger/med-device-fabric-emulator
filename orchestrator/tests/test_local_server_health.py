from __future__ import annotations

import asyncio
import inspect
import os

import importlib
import io
import logging
import sys
import threading
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

_MISSING = object()


class LocalServerHealthTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
        self._saved_modules = {
            name: sys.modules.get(name, _MISSING)
            for name in ("local_server", "shared.database")
        }
        self._saved_shared_database_attr = _MISSING
        shared_pkg = sys.modules.get("shared")
        if shared_pkg is not None and hasattr(shared_pkg, "database"):
            self._saved_shared_database_attr = getattr(shared_pkg, "database")
        self._saved_excepthook = sys.excepthook
        self._saved_threading_excepthook = threading.excepthook
        self.addCleanup(self._cleanup_imports)

        self.local_server = self._import_local_server()

    def _cleanup_imports(self) -> None:
        sys.excepthook = self._saved_excepthook
        threading.excepthook = self._saved_threading_excepthook
        for module_name, module in self._saved_modules.items():
            if module is _MISSING:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = module
        shared_pkg = sys.modules.get("shared")
        if shared_pkg is not None:
            if self._saved_shared_database_attr is _MISSING:
                if getattr(shared_pkg, "database", None) is self._fake_database:
                    delattr(shared_pkg, "database")
            else:
                setattr(shared_pkg, "database", self._saved_shared_database_attr)
        if self._orchestrator_dir in sys.path:
            sys.path.remove(self._orchestrator_dir)

    def _fake_database_module(self) -> types.ModuleType:
        class FakeConnection:
            def execute(self, query: str):
                if query != "SELECT 1":
                    raise AssertionError(f"unexpected liveness query: {query}")
                return self

            def fetchone(self):
                return (1,)

        module = types.ModuleType("shared.database")
        module.save_deployment = lambda *args, **kwargs: None
        module.get_deployment = lambda *args, **kwargs: None
        module.list_deployments = lambda *args, **kwargs: []
        module.delete_deployment = lambda *args, **kwargs: False
        module.get_db = lambda *args, **kwargs: FakeConnection()
        module.clear_all_deployments = lambda *args, **kwargs: 0
        module.mark_stale_as_terminated = lambda *args, **kwargs: None
        module.migrate_from_json = lambda *args, **kwargs: None
        module.get_locks = lambda *args, **kwargs: []
        module.set_lock = lambda *args, **kwargs: None
        module.remove_lock = lambda *args, **kwargs: None
        module.get_form_history = lambda *args, **kwargs: []
        module.add_form_history = lambda *args, **kwargs: None
        module.get_dismissed_teardowns = lambda *args, **kwargs: []
        module.dismiss_teardown = lambda *args, **kwargs: None
        return module

    def _import_local_server(self):
        sys.modules.pop("local_server", None)
        self._fake_database = self._fake_database_module()
        sys.modules["shared.database"] = self._fake_database
        original_path_open = Path.open

        def path_open_without_crash_dump(path: Path, *args, **kwargs):
            if path.name == "backend-crash-dump.log":
                return io.StringIO()
            return original_path_open(path, *args, **kwargs)

        with (
            patch("logging.FileHandler", return_value=logging.NullHandler()),
            patch("pathlib.Path.open", new=path_open_without_crash_dump),
            patch("faulthandler.enable"),
            patch("faulthandler.register"),
            patch("signal.signal"),
            patch("atexit.register"),
        ):
            return importlib.import_module("local_server")

    def _get_route_endpoint(self, path: str):
        for route in self.local_server.app.routes:
            if getattr(route, "path", None) == path and "GET" in getattr(route, "methods", set()):
                return route.endpoint
        self.fail(f"GET {path} route is not registered")

    def _call_get_route(self, path: str, **query):
        endpoint = self._get_route_endpoint(path)
        if inspect.iscoroutinefunction(endpoint):
            return asyncio.run(endpoint(**query))
        return endpoint(**query)

    def _forbid_readiness_checks(self) -> None:
        def forbidden(*args, **kwargs):
            raise AssertionError("liveness endpoints must not call auth or capacity scans")

        async def forbidden_async(*args, **kwargs):
            forbidden(*args, **kwargs)

        self.local_server._get_auth_context_sync = forbidden
        self.local_server._list_capacities_sync = forbidden
        self.local_server.list_capacities = forbidden_async

    def assert_liveness_payload(self, payload: dict[str, object]) -> None:
        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("backend"), "online")
        self.assertEqual(payload.get("database"), "ok")
        checked_at = payload.get("checkedAt")
        self.assertIsInstance(checked_at, str)
        datetime.fromisoformat(checked_at.replace("Z", "+00:00"))

    def test_live_endpoint_is_cheap_and_reports_backend_database_liveness(self) -> None:
        self._forbid_readiness_checks()

        payload = self._call_get_route("/api/live")

        self.assert_liveness_payload(payload)

    def test_health_default_uses_cheap_liveness_contract(self) -> None:
        self._forbid_readiness_checks()

        payload = self._call_get_route("/api/health")

        self.assert_liveness_payload(payload)
        self.assertNotIn("auth", payload)
        self.assertNotIn("capacities", payload)

    def test_health_deep_reports_auth_and_capacity_readiness(self) -> None:
        auth_context = {"ready": True, "user": "operator@example.com", "issues": []}
        capacities = [
            {"name": "cap-active", "state": "Active"},
            {"name": "cap-paused", "state": "Paused"},
        ]

        def fake_auth_context() -> dict[str, object]:
            return auth_context

        def fake_list_capacities_sync(subscription_id: str = "", force: bool = False):
            return capacities

        async def fake_list_capacities(subscription_id: str = "", force: bool = False):
            return capacities

        self.local_server._get_auth_context_sync = fake_auth_context
        self.local_server._list_capacities_sync = fake_list_capacities_sync
        self.local_server.list_capacities = fake_list_capacities

        payload = self._call_get_route("/api/health", deep=True)

        self.assertEqual(payload.get("status"), "ok")
        self.assertEqual(payload.get("backend"), "online")
        self.assertEqual(payload.get("database"), "ok")
        self.assertEqual(payload.get("auth"), auth_context)
        self.assertEqual(
            payload.get("capacities"),
            {"total": 2, "active": 1, "items": capacities},
        )
        self.assertIsInstance(payload.get("checkedAt"), str)

    def test_auth_probe_imports_isolated_az_context_and_aligns_context_fields(self) -> None:
        commands: list[list[str]] = []

        def fake_az_run(args: list[str], **kwargs):
            commands.append(args)
            if args[0] == "az" and args[1] == "version":
                return types.SimpleNamespace(returncode=0, stdout="{}", stderr="")
            if args[0] == "az" and args[1:3] == ["account", "show"]:
                return types.SimpleNamespace(
                    returncode=0,
                    stdout='{"user":"cli-user","subscriptionName":"Production","subscriptionId":"SUB-123","tenantId":"TENANT-456"}',
                    stderr="",
                )
            if args[0] == "pwsh":
                return types.SimpleNamespace(
                    returncode=0,
                    stdout='{"installed":true,"loggedIn":true,"user":"ps-user","subscriptionName":"Production","subscriptionId":"sub-123","tenantId":"tenant-456","error":""}',
                    stderr="",
                )
            raise AssertionError(f"unexpected command: {args!r}")

        with patch.dict(os.environ, {"AZURE_CONFIG_DIR": "/tmp/isolated-azure"}), patch.object(
            self.local_server, "_az_run", side_effect=fake_az_run
        ):
            result = self.local_server._get_auth_context_sync()

        pwsh_command = next(command[-1] for command in commands if command[0] == "pwsh")
        import_path = "Join-Path $env:AZURE_CONFIG_DIR 'azps-context.json'"
        import_command = "Import-AzContext -Path $isolatedContext -ErrorAction Stop | Out-Null"
        get_context = "$ctx = Get-AzContext -ErrorAction Stop"
        self.assertIn(import_path, pwsh_command)
        self.assertIn(import_command, pwsh_command)
        self.assertIn(get_context, pwsh_command)
        self.assertLess(pwsh_command.index(import_command), pwsh_command.index(get_context))
        self.assertTrue(result["ready"])
        self.assertEqual(result["aligned"], {"subscription": True, "tenant": True})

    def test_phase_log_matching_accepts_ui_cards_for_backend_phase_names(self) -> None:
        matching_cases = [
            (
                "RTI enrichment card",
                "Phase 2: Fabric RTI Enrichment (auto)",
                "2c. Active Patient Telemetry: Fabric RTI Enrichment",
            ),
            (
                "HDS deployment gate card",
                "Phase 3: HDS Deployment Detection",
                "2b. Active Patient Telemetry: HDS Deployment Gate",
            ),
            (
                "DICOM shortcut and HDS pipelines card",
                "Phase 3: DICOM Shortcut + HDS Pipelines (auto)",
                "3a. HDS Bridge + Row Gates: DICOM Shortcut + HDS Pipelines",
            ),
        ]

        for name, entry_phase, requested_phase in matching_cases:
            with self.subTest(name=name):
                self.assertTrue(
                    self.local_server._phase_log_matches(entry_phase, requested_phase),
                    f"{entry_phase!r} should match {requested_phase!r}",
                )

    def test_phase_log_matching_rejects_unrelated_backend_phase_names(self) -> None:
        self.assertFalse(
            self.local_server._phase_log_matches(
                "Phase 2: Fabric RTI Enrichment (auto)",
                "2b. Active Patient Telemetry: HDS Deployment Gate",
            )
        )


if __name__ == "__main__":
    unittest.main()
