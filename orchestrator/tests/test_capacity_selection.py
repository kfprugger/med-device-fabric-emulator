from __future__ import annotations

import importlib
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any


class DeployCapacitySelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._repo_root = Path(__file__).resolve().parents[2]
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
        self.addCleanup(self._cleanup_imports)

        self.invoke_powershell = importlib.import_module("activities.invoke_powershell")

    def _cleanup_imports(self) -> None:
        sys.modules.pop("activities.invoke_powershell", None)
        sys.modules.pop("shared.policy_tags", None)
        if self._orchestrator_dir in sys.path:
            sys.path.remove(self._orchestrator_dir)

    def base_deploy_config(self, **overrides: Any) -> dict[str, Any]:
        config: dict[str, Any] = {
            "fabric_workspace_name": "clinical-fabric-ws",
            "resource_group_name": "rg-clinical-deploy",
            "location": "eastus",
            "patient_count": 25,
            "capacity_subscription_id": "sub-selected-001",
            "capacity_resource_group": "rg-paid-capacity",
            "capacity_name": "paid-f64-capacity",
        }
        config.update(overrides)
        return config

    def assert_argument_pair(self, args: list[str], name: str, value: str) -> None:
        self.assertIn(name, args)
        self.assertEqual(args[args.index(name) + 1], value)

    def test_deploy_arg_builder_passes_selected_capacity_in_file_mode_without_tags(self) -> None:
        original_normalizer = self.invoke_powershell.normalize_policy_tags
        self.invoke_powershell.normalize_policy_tags = lambda _tags: {}
        self.addCleanup(setattr, self.invoke_powershell, "normalize_policy_tags", original_normalizer)

        args = self.invoke_powershell._build_deploy_args(self.base_deploy_config(tags={}))

        self.assertEqual(args[:4], ["pwsh", "-NoProfile", "-NonInteractive", "-File"])
        self.assert_argument_pair(args, "-CapacitySubscriptionId", "sub-selected-001")
        self.assert_argument_pair(args, "-CapacityResourceGroup", "rg-paid-capacity")
        self.assert_argument_pair(args, "-CapacityName", "paid-f64-capacity")

    def test_deploy_arg_builder_passes_selected_capacity_in_command_mode_with_tags(self) -> None:
        args = self.invoke_powershell._build_deploy_args(
            self.base_deploy_config(tags={"Owner": "Clinical Ops"})
        )

        self.assertEqual(args[:4], ["pwsh", "-NoProfile", "-NonInteractive", "-Command"])
        command = args[-1]
        self.assertIn("-CapacitySubscriptionId 'sub-selected-001'", command)
        self.assertIn("-CapacityResourceGroup 'rg-paid-capacity'", command)
        self.assertIn("-CapacityName 'paid-f64-capacity'", command)
        self.assertIn("-Tags ", command)
        self.assertNotIn(" -File ", command)

    @unittest.skipIf(shutil.which("pwsh") is None, "PowerShell is required to parse Deploy-All.ps1")
    def test_deploy_all_script_accepts_and_assigns_the_selected_capacity_contract(self) -> None:
        script_path = self._repo_root / "Deploy-All.ps1"
        ast_probe = r'''
$Path = $env:DEPLOY_ALL_PS1
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
$paramNames = @($ast.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
$variableNames = @(
    $ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.VariableExpressionAst]
    }, $true) | ForEach-Object { $_.VariablePath.UserPath } | Sort-Object -Unique
)
$assignCommands = @(
    $ast.FindAll({
        param($node)
        if ($node -isnot [System.Management.Automation.Language.CommandAst]) { return $false }
        if ($node.GetCommandName() -ne 'Invoke-RestMethod') { return $false }
        @($node.CommandElements | Where-Object { $_.Extent.Text -like '*assignToCapacity*' }).Count -gt 0
    }, $true)
)
$assignmentCommandsUsingTargetCapacity = @(
    $assignCommands | Where-Object {
        @($_.FindAll({
            param($node)
            if ($node -isnot [System.Management.Automation.Language.MemberExpressionAst]) { return $false }
            if ($node.Expression -isnot [System.Management.Automation.Language.VariableExpressionAst]) { return $false }
            $node.Expression.VariablePath.UserPath -eq 'targetCap' -and $node.Member.Extent.Text -eq 'id'
        }, $true)).Count -gt 0
    }
)
$assignmentCommandsUsingFormerFallback = @(
    $assignCommands | Where-Object {
        @($_.FindAll({
            param($node)
            if ($node -isnot [System.Management.Automation.Language.MemberExpressionAst]) { return $false }
            if ($node.Expression -isnot [System.Management.Automation.Language.VariableExpressionAst]) { return $false }
            $node.Expression.VariablePath.UserPath -eq 'activeCap' -and $node.Member.Extent.Text -eq 'id'
        }, $true)).Count -gt 0
    }
)
[ordered]@{
    errors = @($errors | ForEach-Object { $_.Message })
    paramNames = $paramNames
    variableNames = $variableNames
    assignToCapacityCommandCount = $assignCommands.Count
    assignmentCommandsUsingTargetCapacity = $assignmentCommandsUsingTargetCapacity.Count
    assignmentCommandsUsingFormerFallback = $assignmentCommandsUsingFormerFallback.Count
} | ConvertTo-Json -Depth 5
'''

        proc = subprocess.run(
            [
                "pwsh",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                ast_probe,
            ],
            check=False,
            text=True,
            capture_output=True,
            env={**os.environ, "DEPLOY_ALL_PS1": str(script_path)},
            timeout=30,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        contract = json.loads(proc.stdout)
        self.assertEqual(contract["errors"], [])
        self.assertGreater(contract["assignToCapacityCommandCount"], 0)
        self.assertEqual(
            contract["assignmentCommandsUsingTargetCapacity"],
            contract["assignToCapacityCommandCount"],
        )
        self.assertEqual(contract["assignmentCommandsUsingFormerFallback"], 0)
        for parameter_name in (
            "CapacitySubscriptionId",
            "CapacityResourceGroup",
            "CapacityName",
        ):
            with self.subTest(parameter_name=parameter_name):
                self.assertIn(parameter_name, contract["paramNames"])
                self.assertIn(parameter_name, contract["variableNames"])


if __name__ == "__main__":
    unittest.main()
