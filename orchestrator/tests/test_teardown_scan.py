from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path


class TeardownScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
        self.addCleanup(self._cleanup_imports)
        self.teardown_scan = importlib.import_module("shared.teardown_scan")

    def _cleanup_imports(self) -> None:
        sys.modules.pop("shared.teardown_scan", None)
        if self._orchestrator_dir in sys.path:
            sys.path.remove(self._orchestrator_dir)

    def test_deployment_history_does_not_create_fabric_teardown_candidates(self) -> None:
        self.assertEqual(
            self.teardown_scan.live_fabric_workspaces_for_teardown(
                [],
                {"med-0526", "med-0701"},
            ),
            [],
        )

    def test_only_live_fabric_workspaces_are_returned(self) -> None:
        live_workspaces = [
            {"displayName": "med-0702", "id": "workspace-live"},
        ]

        self.assertEqual(
            self.teardown_scan.live_fabric_workspaces_for_teardown(
                live_workspaces,
                {"med-0526", "med-0702"},
            ),
            [{"displayName": "med-0702", "id": "workspace-live"}],
        )

    def test_result_is_a_copy_not_the_live_cache_object(self) -> None:
        live_workspace = {"displayName": "med-0702", "id": "workspace-live"}
        result = self.teardown_scan.live_fabric_workspaces_for_teardown([live_workspace], set())

        result[0]["displayName"] = "mutated"

        self.assertEqual(live_workspace["displayName"], "med-0702")


if __name__ == "__main__":
    unittest.main()
