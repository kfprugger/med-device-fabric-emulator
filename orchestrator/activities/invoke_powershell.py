"""Activity: Invoke Deploy-All.ps1 directly.

Instead of reimplementing the PowerShell logic in Python, this activity
calls the battle-tested Deploy-All.ps1 script via subprocess and streams
its output as log lines. The PowerShell script handles all edge cases
(workspace creation, capacity assignment, Bicep deployments, FHIR loading,
Fabric RTI, Data Agents, Ontology, Data Activator, etc.).
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import re

logger = logging.getLogger(__name__)

# The Deploy-All.ps1 script lives in the repo root
SCRIPT_DIR = Path(__file__).resolve().parent.parent.parent
DEPLOY_SCRIPT = SCRIPT_DIR / "Deploy-All.ps1"
TEARDOWN_SCRIPT = SCRIPT_DIR / "cleanup" / "Remove-AllResources.ps1"
PREFLIGHT_SCRIPT = SCRIPT_DIR / "Preflight-Check.ps1"

# Regex to parse step markers from PowerShell output
# Main steps from Invoke-Step use pipe-delimited banners: |  STEP N: TITLE  |
# Sub-steps from deploy.ps1 use dashes: --- STEP N: TITLE ---
STEP_BANNER_RE = re.compile(r"^\|\s+STEP\s+(\d+):\s+(.+?)\s*\|$")
# Step results: ✓/✗ (or û/Ã— on Windows encoding) followed by name and duration
# Duration must look like "X.X min", just "—", or be empty (to avoid matching arbitrary error messages)
STEP_RESULT_RE = re.compile(r"[✓✗û]\s+(.+?)\s+[-—]\s+(\d+\.?\d*\s*min|—)")
# Fallback: summary lines use space-padded names with duration at end (no dash separator)
STEP_SUMMARY_RE = re.compile(r"[✓✗û]\s+(.+?)\s{2,}(\d+\.?\d*\s*min|—)")
# Phase transition marker: @@PHASE|<number>|<label>|<stepCount>@@
PHASE_TRANSITION_RE = re.compile(r"@@PHASE\|(\d+)\|(.+?)\|(\d+)@@")

# HDS pipeline sub-step names (failures here are warnings, not hard failures)
HDS_PIPELINE_SUBSTEPS = {"Imaging Pipeline", "Clinical Pipeline", "OMOP Pipeline"}
# Regex to detect HDS pipeline warning/failure lines from PowerShell
HDS_PIPELINE_WARN_RE = re.compile(
    r"⚠.*(?:pipeline|Pipeline).*(?:Failed|FAILED|Cancelled|TIMEOUT|did not complete|SKIPPED)"
    r"|(?:pipeline|Pipeline).*(?:FAILED|Failed|Cancelled)"
    r"|SKIPPING OMOP pipeline",
    re.IGNORECASE,
)

# Callback type for step events
StepCallback = Any


def run_deploy(config: dict[str, Any], step_callback: Any = None, pid_callback: Any = None) -> dict[str, Any]:
    """Run Deploy-All.ps1 with parameters from the config dict.

    Args:
        config: Deployment configuration dict.
        step_callback: Optional callback(event, step_name, detail, duration)
            Events: "step_start", "step_succeeded", "step_failed"
    """
    start = time.time()
    args = _build_deploy_args(config)
    logger.info("Invoking Deploy-All.ps1 with args: %s", " ".join(args[2:]))
    exit_code = _run_powershell(args, step_callback, pid_callback)
    duration = time.time() - start

    if exit_code != 0:
        raise RuntimeError(
            f"Deploy-All.ps1 exited with code {exit_code}. "
            "Check the logs above for details."
        )

    return {
        "phase": "Deploy-All",
        "duration_seconds": duration,
        "exit_code": exit_code,
        "resources": {
            "fabric_workspace_name": config.get("fabric_workspace_name", ""),
            "resource_group_name": config.get("resource_group_name", ""),
        },
    }


def run_teardown(config: dict[str, Any], step_callback: Any = None) -> dict[str, Any]:
    """Run Remove-AllResources.ps1 for teardown."""
    start = time.time()
    args = _build_teardown_args(config)
    logger.info("Invoking Remove-AllResources.ps1 with args: %s", " ".join(args[2:]))
    exit_code = _run_powershell(args, step_callback)
    duration = time.time() - start

    return {
        "phase": "Teardown",
        "duration_seconds": duration,
        "exit_code": exit_code,
        "results": {
            "items_deleted": 0 if exit_code != 0 else -1,
            "errors": [] if exit_code == 0 else [f"Exit code: {exit_code}"],
        },
    }


def run_preflight(config: dict[str, Any]) -> dict[str, Any]:
    """Run Preflight-Check.ps1 and return structured results."""
    args = [
        "pwsh", "-NoProfile", "-NonInteractive", "-File",
        str(PREFLIGHT_SCRIPT),
    ]
    if config.get("fabric_workspace_name"):
        args += ["-FabricWorkspaceName", config["fabric_workspace_name"]]
    if config.get("location"):
        args += ["-Location", config.get("location", "eastus")]
    if config.get("admin_security_group"):
        args += ["-AdminSecurityGroup", config["admin_security_group"]]

    logger.info("Running preflight checks...")

    process = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, cwd=str(SCRIPT_DIR),
        shell=(sys.platform == "win32"),
        encoding="utf-8", errors="replace",
    )

    output_lines = []
    json_lines = []
    in_json = False

    for line in process.stdout:
        line = line.rstrip()
        output_lines.append(line)
        if line:
            logger.info(line)
        if line.strip().startswith("{"):
            in_json = True
        if in_json:
            json_lines.append(line)

    process.wait()

    result = {
        "passed": process.returncode == 0,
        "checks": [],
        "failures": [],
        "warnings": [],
        "output": "\n".join(output_lines),
    }

    if json_lines:
        try:
            parsed = json.loads("\n".join(json_lines))
            result["checks"] = parsed.get("checks", [])
            result["failures"] = parsed.get("failures", [])
            result["warnings"] = parsed.get("warnings", [])
            result["passed"] = parsed.get("passed", process.returncode == 0)
        except Exception:
            pass

    return result


def _build_deploy_args(config: dict[str, Any]) -> list[str]:
    """Build the pwsh command line for Deploy-All.ps1."""
    tags = config.get("tags", {})

    # If tags are present, we must use -Command mode (not -File)
    # because -File can't parse hashtable literals
    if tags:
        params = [
            f"-FabricWorkspaceName '{config['fabric_workspace_name']}'",
            f"-Location '{config.get('location', 'eastus')}'",
        ]
        if config.get("resource_group_name"):
            params.append(f"-ResourceGroupName '{config['resource_group_name']}'")
        if config.get("admin_security_group"):
            params.append(f"-AdminSecurityGroup '{config['admin_security_group']}'")
        if config.get("patient_count"):
            params.append(f"-PatientCount {config['patient_count']}")
        if config.get("alert_email"):
            params.append(f"-AlertEmail '{config['alert_email']}'")
        if config.get("skip_base_infra"):
            params.append("-SkipBaseInfra")
        if config.get("skip_fhir"):
            params.append("-SkipFhir")
        if config.get("skip_dicom"):
            params.append("-SkipDicom")
        if config.get("skip_fabric"):
            params.append("-SkipFabric")
        if config.get("reuse_patients"):
            params.append("-ReusePatients")
        if config.get("skip_synthea"):
            params.append("-SkipSynthea")
        if config.get("skip_device_assoc"):
            params.append("-SkipDeviceAssoc")
        if config.get("skip_fhir_export"):
            params.append("-SkipFhirExport")
        if config.get("skip_rti_phase2"):
            params.append("-SkipRtiPhase2")
        if config.get("skip_hds_pipelines"):
            params.append("-SkipHdsPipelines")
        if config.get("skip_data_agents"):
            params.append("-SkipDataAgents")
        if config.get("skip_imaging"):
            params.append("-SkipImaging")
        if config.get("skip_ontology"):
            params.append("-SkipOntology")
        if config.get("skip_activator"):
            params.append("-SkipActivator")
        if config.get("phase2_only"):
            params.append("-Phase2")
        if config.get("phase3_only"):
            params.append("-Phase3")
        if config.get("phase4_only"):
            params.append("-Phase4")

        tag_str = "@{" + ";".join(f"'{k}'='{v}'" for k, v in tags.items()) + "}"
        params.append(f"-Tags {tag_str}")

        cmd = f"& '{DEPLOY_SCRIPT}' {' '.join(params)}"
        return ["pwsh", "-NoProfile", "-NonInteractive", "-Command", cmd]

    # No tags — use simpler -File mode
    args = [
        "pwsh", "-NoProfile", "-NonInteractive", "-File",
        str(DEPLOY_SCRIPT),
        "-FabricWorkspaceName", config["fabric_workspace_name"],
        "-Location", config.get("location", "eastus"),
    ]

    if config.get("resource_group_name"):
        args += ["-ResourceGroupName", config["resource_group_name"]]
    if config.get("admin_security_group"):
        args += ["-AdminSecurityGroup", config["admin_security_group"]]
    if config.get("patient_count"):
        args += ["-PatientCount", str(config["patient_count"])]
    if config.get("alert_email"):
        args += ["-AlertEmail", config["alert_email"]]

    if config.get("skip_base_infra"):
        args.append("-SkipBaseInfra")
    if config.get("skip_fhir"):
        args.append("-SkipFhir")
    if config.get("skip_dicom"):
        args.append("-SkipDicom")
    if config.get("skip_fabric"):
        args.append("-SkipFabric")
    if config.get("skip_fhir_export"):
        args.append("-SkipFhirExport")
    if config.get("skip_synthea"):
        args.append("-SkipSynthea")
    if config.get("skip_device_assoc"):
        args.append("-SkipDeviceAssoc")
    if config.get("skip_rti_phase2"):
        args.append("-SkipRtiPhase2")
    if config.get("skip_hds_pipelines"):
        args.append("-SkipHdsPipelines")
    if config.get("skip_data_agents"):
        args.append("-SkipDataAgents")
    if config.get("skip_imaging"):
        args.append("-SkipImaging")
    if config.get("skip_ontology"):
        args.append("-SkipOntology")
    if config.get("skip_activator"):
        args.append("-SkipActivator")

    if config.get("phase2_only"):
        args.append("-Phase2")
    if config.get("phase3_only"):
        args.append("-Phase3")
    if config.get("phase4_only"):
        args.append("-Phase4")

    return args


def _build_teardown_args(config: dict[str, Any]) -> list[str]:
    """Build the pwsh command line for Remove-AllResources.ps1."""
    args = [
        "pwsh", "-NoProfile", "-NonInteractive", "-File",
        str(TEARDOWN_SCRIPT),
        "-Force",
    ]

    if config.get("fabric_workspace_name"):
        args += ["-FabricWorkspaceName", config["fabric_workspace_name"]]
    if config.get("resource_group_name"):
        args += ["-ResourceGroupName", config["resource_group_name"]]
    if config.get("delete_workspace"):
        args.append("-DeleteWorkspace")

    return args


def _run_powershell(args: list[str], step_callback: Any = None, pid_callback: Any = None) -> int:
    """Run a PowerShell command, streaming stdout/stderr to the logger.

    Parses step markers from Deploy-All.ps1 output:
    - Phase:  @@PHASE|N|Label|StepCount@@ → phase_transition event
    - Banner: |  STEP N: TITLE  | → step_start event
    - Result: ✓  StepName — X.X min → step_succeeded event
    - Result: ✗  StepName — X.X min → step_failed event

    Returns the exit code.
    """
    logger.info("$ %s", " ".join(args))

    current_phase = 0
    current_phase_label = ""

    process = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(SCRIPT_DIR),
        shell=(sys.platform == "win32"),
        bufsize=1,
        encoding="utf-8",
        errors="replace",
        env={**__import__("os").environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
    )

    if pid_callback:
        pid_callback(process.pid)

    for line in process.stdout:
        line = line.rstrip()
        if not line:
            continue

        # Parse phase transition: @@PHASE|N|Label|StepCount@@
        phase_match = PHASE_TRANSITION_RE.search(line)
        if phase_match:
            current_phase = int(phase_match.group(1))
            current_phase_label = phase_match.group(2)
            step_count = int(phase_match.group(3))
            logger.info("Phase transition → Phase %d: %s (%d steps)", current_phase, current_phase_label, step_count)
            if step_callback:
                step_callback("phase_transition", f"PHASE {current_phase}: {current_phase_label}", str(step_count), "")
            continue

        # Parse step banner: |  STEP N: TITLE  | (main Deploy-All.ps1 steps only)
        banner_match = STEP_BANNER_RE.match(line.strip())
        if banner_match:
            step_num = banner_match.group(1)
            step_title = banner_match.group(2).strip()
            if step_callback:
                step_callback("step_start", step_title, "", "")
            logger.info(line)
            continue

        # Parse step result: ✓/✗/û  StepName - Duration
        result_match = STEP_RESULT_RE.search(line) or STEP_SUMMARY_RE.search(line)
        if result_match:
            step_name = result_match.group(1).strip()
            duration = result_match.group(2).strip()
            # Determine success/failure from the line content
            is_success = "\u2713" in line or "\u00fb" in line  # ✓ or û
            if is_success:
                event = "step_succeeded"
            elif step_name in HDS_PIPELINE_SUBSTEPS:
                # HDS pipeline sub-step failures are warnings, not hard failures
                event = "step_warning"
            else:
                event = "step_failed"
            if step_callback:
                step_callback(event, step_name, "", duration)
            if is_success:
                logger.info(line)
            elif event == "step_warning":
                logger.warning(line)
            else:
                logger.error(line)
            continue

        # Detect HDS pipeline warning/failure lines (e.g. "⚠ Clinical pipeline Failed")
        if HDS_PIPELINE_WARN_RE.search(line):
            logger.warning(line)
            if step_callback:
                step_callback("step_warning", "HDS Pipeline", line.strip(), "")
            continue

        # Detect skipped steps (e.g. ">>  Skipping FHIR / Synthea (--SkipFhir)")
        if "Skipping" in line and ">>" in line:
            # Extract what was skipped for progress tracking
            skip_match = re.search(r"Skipping\s+(.+?)(?:\s+\(|$)", line)
            if skip_match and step_callback:
                skip_name = skip_match.group(1).strip()
                step_callback("step_skipped", skip_name, "", "")
            logger.info(line)
            continue

        # Regular log line
        if any(marker in line for marker in ["✓", "succeeded", "Succeeded", "created", "deployed", "complete"]):
            logger.info(line)
        elif any(marker in line for marker in ["✗", "ERROR", "error", "failed", "Failed", "FAILED"]):
            logger.error(line)
        elif any(marker in line for marker in ["WARNING", "⚠", "Skipping"]):
            logger.warning(line)
        else:
            logger.info(line)

    process.wait()
    logger.info("PowerShell exited with code %d", process.returncode)
    return process.returncode
