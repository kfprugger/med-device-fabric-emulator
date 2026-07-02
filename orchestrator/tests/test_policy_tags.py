from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path


class PolicyTagTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
        self.addCleanup(self._cleanup_imports)

        self.policy_tags = importlib.import_module("shared.policy_tags")
        self.invoke_powershell = importlib.import_module("activities.invoke_powershell")

    def _cleanup_imports(self) -> None:
        for module_name in (
            "activities.invoke_powershell",
            "shared.policy_tags",
        ):
            sys.modules.pop(module_name, None)
        if self._orchestrator_dir in sys.path:
            sys.path.remove(self._orchestrator_dir)

    def base_deploy_config(self, **overrides: object) -> dict[str, object]:
        config: dict[str, object] = {
            "fabric_workspace_name": "fabric-ws",
            "resource_group_name": "rg-fabric",
            "location": "eastus",
            "patient_count": 25,
        }
        config.update(overrides)
        return config

    def deploy_command(self, config: dict[str, object]) -> str:
        args = self.invoke_powershell._build_deploy_args(config)

        self.assertEqual(args[:4], ["pwsh", "-NoProfile", "-NonInteractive", "-Command"])
        return args[-1]

    def test_normalize_policy_tags_adds_default_to_missing_or_empty_tags(self) -> None:
        for raw_tags in (None, {}):
            with self.subTest(raw_tags=raw_tags):
                self.assertEqual(
                    self.policy_tags.normalize_policy_tags(raw_tags),
                    {"SecurityControl": "Ignore"},
                )

    def test_normalize_policy_tags_preserves_custom_tags_and_forces_security_control_ignore(self) -> None:
        self.assertEqual(
            self.policy_tags.normalize_policy_tags({"Owner": "clinical", "CostCenter": "hds"}),
            {"Owner": "clinical", "CostCenter": "hds", "SecurityControl": "Ignore"},
        )
        self.assertEqual(
            self.policy_tags.normalize_policy_tags({"Owner": "clinical", "SecurityControl": "Audit"}),
            {"Owner": "clinical", "SecurityControl": "Ignore"},
        )


    def test_deploy_all_arg_builder_emits_tags_hashtable_for_normalized_tags(self) -> None:
        normalized_tags = self.policy_tags.normalize_policy_tags({})

        command = self.deploy_command(self.base_deploy_config(tags=normalized_tags))

        self.assertIn("-Tags @{'SecurityControl'='Ignore'}", command)
        self.assertNotIn(" -File ", command)

    def test_deploy_all_arg_builder_normalizes_empty_tags_before_launch_args(self) -> None:
        command = self.deploy_command(self.base_deploy_config(tags={}))

        self.assertIn("-Tags @{'SecurityControl'='Ignore'}", command)
        self.assertIn("'SecurityControl'='Ignore'", command)

    def test_deploy_all_arg_builder_forces_security_control_ignore_for_caller_tags(self) -> None:
        command = self.deploy_command(
            self.base_deploy_config(tags={"Owner": "clinical", "SecurityControl": "Audit"})
        )

        self.assertIn("'Owner'='clinical'", command)
        self.assertIn("'SecurityControl'='Ignore'", command)
        self.assertNotIn("'SecurityControl'='Audit'", command)

    def test_deploy_all_arg_builder_preserves_and_escapes_custom_tags(self) -> None:
        normalized_tags = self.policy_tags.normalize_policy_tags(
            {"Owner": "Joey's Lab", "CostCenter": "hds"}
        )

        command = self.deploy_command(
            self.base_deploy_config(
                fabric_workspace_name="fabric-ws",
                alert_email="ops'team@example.com",
                tags=normalized_tags,
            )
        )

        self.assertIn("-AlertEmail 'ops''team@example.com'", command)
        self.assertIn("'Owner'='Joey''s Lab'", command)
        self.assertIn("'CostCenter'='hds'", command)
        self.assertIn("'SecurityControl'='Ignore'", command)



if __name__ == "__main__":
    unittest.main()
