from __future__ import annotations

import importlib
import sys
import types
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch


_MISSING = object()


class RecordingFabricClient:
    instances: list["RecordingFabricClient"] = []
    workspace: dict[str, Any] | None = None
    created_workspace: dict[str, Any] = {
        "id": "ws-created",
        "displayName": "clinical-fabric-ws",
    }
    workspace_detail: dict[str, Any] = {"id": "ws-created", "capacityId": ""}
    capacities: list[dict[str, Any]] = []
    identity: dict[str, Any] | None = {"principalId": "principal-001"}

    def __init__(self, api_base: str) -> None:
        self.api_base = api_base
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.find_workspace_calls: list[str] = []
        self.identity_calls: list[str] = []
        RecordingFabricClient.instances.append(self)

    @classmethod
    def reset(cls) -> None:
        cls.instances = []
        cls.workspace = None
        cls.created_workspace = {
            "id": "ws-created",
            "displayName": "clinical-fabric-ws",
        }
        cls.workspace_detail = {"id": "ws-created", "capacityId": ""}
        cls.capacities = []
        cls.identity = {"principalId": "principal-001"}

    def find_workspace(self, name: str, max_retries: int = 3) -> dict[str, Any] | None:
        self.find_workspace_calls.append(name)
        return RecordingFabricClient.workspace

    def call(
        self,
        method: str,
        endpoint: str,
        body: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> dict[str, Any] | None:
        self.calls.append((method, endpoint, body))
        if method == "POST" and endpoint == "/workspaces":
            return RecordingFabricClient.created_workspace
        if method == "GET" and endpoint.startswith("/workspaces/"):
            return RecordingFabricClient.workspace_detail
        if method == "GET" and endpoint == "/capacities":
            return {"value": RecordingFabricClient.capacities}
        if method == "POST" and endpoint.endswith("/assignToCapacity"):
            return None
        raise AssertionError(f"Unexpected Fabric API call: {method} {endpoint} {body}")

    def provision_workspace_identity(self, workspace_id: str) -> dict[str, Any] | None:
        self.identity_calls.append(workspace_id)
        return RecordingFabricClient.identity


class ProvisionWorkspaceCapacitySelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        RecordingFabricClient.reset()
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        self._inserted_path = False
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
            self._inserted_path = True

        self._saved_modules = {
            name: sys.modules.get(name, _MISSING)
            for name in (
                "activities.provision_workspace",
                "shared",
                "shared.fabric_client",
            )
        }
        shared_module = sys.modules.get("shared")
        self._saved_shared_fabric_client_attr = (
            getattr(shared_module, "fabric_client", _MISSING)
            if shared_module is not None
            else _MISSING
        )

        sys.modules.pop("activities.provision_workspace", None)
        fake_fabric_client_module = types.ModuleType("shared.fabric_client")
        fake_fabric_client_module.FabricClient = RecordingFabricClient
        sys.modules["shared.fabric_client"] = fake_fabric_client_module
        if shared_module is not None:
            setattr(shared_module, "fabric_client", fake_fabric_client_module)

        self.addCleanup(self._cleanup_imports)
        self.provision_workspace = importlib.import_module("activities.provision_workspace")

    def _cleanup_imports(self) -> None:
        for module_name, module in self._saved_modules.items():
            if module is _MISSING:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = module

        shared_module = sys.modules.get("shared")
        if shared_module is not None:
            if self._saved_shared_fabric_client_attr is _MISSING:
                if hasattr(shared_module, "fabric_client"):
                    delattr(shared_module, "fabric_client")
            else:
                setattr(
                    shared_module,
                    "fabric_client",
                    self._saved_shared_fabric_client_attr,
                )

        if self._inserted_path:
            sys.path.remove(self._orchestrator_dir)

    def base_config(self, **overrides: Any) -> dict[str, Any]:
        config: dict[str, Any] = {
            "fabric_workspace_name": "clinical-fabric-ws",
            "fabric_api_base": "https://fabric.test/v1",
            "capacity_subscription_id": "sub-selected-001",
            "capacity_resource_group": "rg-paid-capacity",
            "capacity_name": "paid-f64-capacity",
        }
        config.update(overrides)
        return config

    def run_activity(self, config: dict[str, Any]) -> dict[str, Any]:
        with patch.object(self.provision_workspace.time, "sleep", return_value=None):
            return self.provision_workspace.run(config)

    def fabric_client(self) -> RecordingFabricClient:
        self.assertEqual(len(RecordingFabricClient.instances), 1)
        return RecordingFabricClient.instances[0]

    def assignment_calls(
        self,
        client: RecordingFabricClient,
    ) -> list[tuple[str, str, dict[str, Any] | None]]:
        return [
            call
            for call in client.calls
            if call[0] == "POST" and call[1].endswith("/assignToCapacity")
        ]

    def test_selected_paid_capacity_is_assigned_when_trial_capacity_is_also_active(self) -> None:
        RecordingFabricClient.workspace = None
        RecordingFabricClient.workspace_detail = {"id": "ws-created", "capacityId": ""}
        RecordingFabricClient.capacities = [
            {
                "id": "trial-ft-id",
                "displayName": "trial-ft-capacity",
                "name": "trial-ft-capacity",
                "sku": "FT64",
                "state": "Active",
            },
            {
                "id": "paid-f64-id",
                "displayName": "paid-f64-capacity",
                "name": "paid-f64-capacity",
                "sku": "F64",
                "state": "Active",
            },
        ]

        result = self.run_activity(self.base_config())

        client = self.fabric_client()
        self.assertEqual(
            result["resources"]["fabric_workspace_id"],
            "ws-created",
        )
        self.assertIn(
            ("POST", "/workspaces", {"displayName": "clinical-fabric-ws"}),
            client.calls,
        )
        self.assertEqual(
            self.assignment_calls(client),
            [
                (
                    "POST",
                    "/workspaces/ws-created/assignToCapacity",
                    {"capacityId": "paid-f64-id"},
                )
            ],
        )
        self.assertNotIn(
            (
                "POST",
                "/workspaces/ws-created/assignToCapacity",
                {"capacityId": "trial-ft-id"},
            ),
            client.calls,
        )

    def test_existing_workspace_on_different_capacity_is_reassigned_to_selected_capacity(self) -> None:
        RecordingFabricClient.workspace = {
            "id": "ws-existing",
            "displayName": "clinical-fabric-ws",
        }
        RecordingFabricClient.workspace_detail = {
            "id": "ws-existing",
            "capacityId": "other-paid-id",
        }
        RecordingFabricClient.capacities = [
            {
                "id": "other-paid-id",
                "displayName": "other-paid-capacity",
                "name": "other-paid-capacity",
                "sku": "F16",
                "state": "Active",
            },
            {
                "id": "paid-f64-id",
                "displayName": "paid-f64-capacity",
                "name": "paid-f64-capacity",
                "sku": "F64",
                "state": "Active",
            },
        ]

        result = self.run_activity(self.base_config())

        client = self.fabric_client()
        self.assertEqual(
            result["resources"]["fabric_workspace_id"],
            "ws-existing",
        )
        self.assertEqual(
            self.assignment_calls(client),
            [
                (
                    "POST",
                    "/workspaces/ws-existing/assignToCapacity",
                    {"capacityId": "paid-f64-id"},
                )
            ],
        )

    def test_selected_trial_or_ppu_capacity_is_rejected_before_assignment(self) -> None:
        cases = [
            {
                "name": "fabric trial",
                "capacity_name": "trial-ft-capacity",
                "sku": "FT64",
            },
            {
                "name": "power bi premium per user",
                "capacity_name": "ppu-capacity",
                "sku": "PP3",
            },
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                RecordingFabricClient.reset()
                RecordingFabricClient.workspace = {
                    "id": "ws-existing",
                    "displayName": "clinical-fabric-ws",
                }
                RecordingFabricClient.workspace_detail = {
                    "id": "ws-existing",
                    "capacityId": "",
                }
                RecordingFabricClient.capacities = [
                    {
                        "id": "unsupported-capacity-id",
                        "displayName": case["capacity_name"],
                        "name": case["capacity_name"],
                        "sku": case["sku"],
                        "state": "Active",
                    },
                    {
                        "id": "paid-f64-id",
                        "displayName": "paid-f64-capacity",
                        "name": "paid-f64-capacity",
                        "sku": "F64",
                        "state": "Active",
                    },
                ]

                with self.assertRaisesRegex(RuntimeError, "not an active paid F-SKU"):
                    self.run_activity(
                        self.base_config(capacity_name=case["capacity_name"])
                    )

                client = self.fabric_client()
                self.assertEqual(self.assignment_calls(client), [])
                self.assertEqual(client.identity_calls, [])


if __name__ == "__main__":
    unittest.main()
