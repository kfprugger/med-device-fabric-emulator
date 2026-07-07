from __future__ import annotations

import importlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


_MISSING = object()


class FakePowerShellProcess:
    def __init__(self, stdout_lines) -> None:
        self.stdout = iter(stdout_lines)
        self.pid = 12345
        self.returncode = 0

    def poll(self):
        return None

    def wait(self) -> int:
        return self.returncode


class HeartbeatAfterFirstLineStdout:
    def __init__(self, first_line: str, trigger_heartbeat) -> None:
        self._first_line = first_line
        self._trigger_heartbeat = trigger_heartbeat
        self._sent_first_line = False
        self._triggered = False

    def __iter__(self):
        return self

    def __next__(self) -> str:
        if not self._sent_first_line:
            self._sent_first_line = True
            return self._first_line
        if not self._triggered:
            self._triggered = True
            self._trigger_heartbeat()
        raise StopIteration


class ControlledHeartbeatEvent:
    def __init__(self) -> None:
        self.wait_calls = 0
        self.set_called = False

    def wait(self, _timeout: float) -> bool:
        self.wait_calls += 1
        return self.wait_calls > 1

    def set(self) -> None:
        self.set_called = True


class InvokePowershellParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        self._inserted_path = False
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
            self._inserted_path = True

        self._saved_modules = {
            name: sys.modules.get(name, _MISSING)
            for name in (
                "activities",
                "activities.invoke_powershell",
                "shared",
                "shared.policy_tags",
            )
        }
        shared_module = sys.modules.get("shared")
        self._saved_shared_policy_tags_attr = (
            getattr(shared_module, "policy_tags", _MISSING)
            if shared_module is not None
            else _MISSING
        )
        sys.modules.pop("activities.invoke_powershell", None)
        self.addCleanup(self._cleanup_imports)

        self.invoke_powershell = importlib.import_module("activities.invoke_powershell")

    def _cleanup_imports(self) -> None:
        for module_name, module in self._saved_modules.items():
            if module is _MISSING:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = module
        shared_module = sys.modules.get("shared")
        if shared_module is not None:
            if self._saved_shared_policy_tags_attr is _MISSING:
                if hasattr(shared_module, "policy_tags"):
                    delattr(shared_module, "policy_tags")
            else:
                setattr(
                    shared_module,
                    "policy_tags",
                    self._saved_shared_policy_tags_attr,
                )
        if self._inserted_path:
            sys.path.remove(self._orchestrator_dir)

    def parse_lines(self, *lines: str) -> list[tuple[str, str, str, str]]:
        events: list[tuple[str, str, str, str]] = []
        process = FakePowerShellProcess([f"{line}\n" for line in lines])

        with patch.object(
            self.invoke_powershell.subprocess,
            "Popen",
            return_value=process,
        ):
            exit_code = self.invoke_powershell._run_powershell(
                ["pwsh", "-NoProfile", "-Command", "Deploy-All.ps1"],
                lambda *event: events.append(event),
            )

        self.assertEqual(exit_code, 0)
        return events

    def test_quiet_running_step_emits_heartbeat_before_next_powershell_line(self) -> None:
        events: list[tuple[str, str, str, str]] = []
        captured_thread_target = {}
        created_events: list[ControlledHeartbeatEvent] = []

        def trigger_heartbeat() -> None:
            target = captured_thread_target.get("target")
            if target is None:
                raise AssertionError("heartbeat thread was not started")
            target()

        process = FakePowerShellProcess(
            HeartbeatAfterFirstLineStdout(
                "|  STEP 2: Fabric RTI Enrichment  |\n",
                trigger_heartbeat,
            )
        )

        class CapturedThread:
            def __init__(self, target, daemon: bool = False) -> None:
                captured_thread_target["target"] = target
                self.daemon = daemon

            def start(self) -> None:
                pass

            def join(self, timeout=None) -> None:
                captured_thread_target["join_timeout"] = timeout

        def event_factory() -> ControlledHeartbeatEvent:
            event = ControlledHeartbeatEvent()
            created_events.append(event)
            return event

        monotonic_values = iter([100.0, 101.0, 132.0])

        def fake_monotonic() -> float:
            return next(monotonic_values)

        with self.assertLogs(self.invoke_powershell.logger, level="INFO") as logs:
            with (
                patch.object(self.invoke_powershell.subprocess, "Popen", return_value=process),
                patch.object(self.invoke_powershell.threading, "Thread", new=CapturedThread),
                patch.object(self.invoke_powershell.threading, "Event", new=event_factory),
                patch.object(self.invoke_powershell.time, "monotonic", new=fake_monotonic),
            ):
                exit_code = self.invoke_powershell._run_powershell(
                    ["pwsh", "-NoProfile", "-Command", "Deploy-All.ps1"],
                    lambda *event: events.append(event),
                )

        self.assertEqual(exit_code, 0)
        self.assertIn(("step_start", "Fabric RTI Enrichment", "", ""), events)
        self.assertEqual(created_events[0].wait_calls, 2)
        self.assertTrue(created_events[0].set_called)
        self.assertIn(
            "Still running Fabric RTI Enrichment — waiting for PowerShell output (31s quiet)",
            "\n".join(logs.output),
        )

    def assert_sidecar_warning_substep(
        self,
        events: list[tuple[str, str, str, str]],
        *,
        sidecar_name: str,
        source_line: str,
        duration: str,
        expected_warning_detail: str,
        expected_substep_status: str,
    ) -> None:
        self.assertIn(
            ("step_warning", sidecar_name, expected_warning_detail, duration),
            events,
        )
        self.assertFalse(
            any(
                event == "step_failed" and step_name == sidecar_name
                for event, step_name, _detail, _duration in events
            ),
            events,
        )

        matching_substeps = [
            event
            for event in events
            if event[0] == "substep_update" and event[1] == sidecar_name
        ]
        self.assertEqual(len(matching_substeps), 1, events)
        _event, _step_name, payload_json, substep_duration = matching_substeps[0]
        self.assertEqual(substep_duration, duration)
        self.assertEqual(
            json.loads(payload_json),
            {"status": expected_substep_status, "detail": source_line},
        )

    def test_sidecar_pipeline_with_display_name_is_hds_substep(self) -> None:
        self.assertTrue(
            self.invoke_powershell.is_hds_pipeline_substep(
                "Sidecar Pipeline: healthcare1_msft_sdoh_ingestion"
            )
        )

    def test_failed_sidecar_result_and_summary_lines_are_non_blocking_substeps(
        self,
    ) -> None:
        sidecar_name = "Sidecar Pipeline: healthcare1_msft_sdoh_ingestion"
        cases = [
            {
                "name": "dash result line",
                "line": f"✗ {sidecar_name} - 0.4 min",
                "duration": "0.4 min",
                "warning_detail": "",
                "substep_status": "warning",
            },
            {
                "name": "status summary row",
                "line": f"✗ {sidecar_name}  Failed: trigger unavailable  12 sec",
                "duration": "12 sec",
                "warning_detail": f"✗ {sidecar_name}  Failed: trigger unavailable  12 sec",
                "substep_status": "failed",
            },
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                line = case["line"]
                events = self.parse_lines(line)

                self.assert_sidecar_warning_substep(
                    events,
                    sidecar_name=sidecar_name,
                    source_line=line,
                    duration=case["duration"],
                    expected_warning_detail=case["warning_detail"],
                    expected_substep_status=case["substep_status"],
                )


if __name__ == "__main__":
    unittest.main()
