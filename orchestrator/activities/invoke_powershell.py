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
import contextvars
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import re
from shared.policy_tags import normalize_policy_tags

logger = logging.getLogger(__name__)

_INSTANCE_ID: contextvars.ContextVar[str] = contextvars.ContextVar("invoke_powershell_instance_id", default="")


class _InstanceCorrelationFilter(logging.Filter):
    """Attach the active deployment id to every activity log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.instance_id = _INSTANCE_ID.get()
        return True


logger.addFilter(_InstanceCorrelationFilter())

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
# HDS Record-Step summary rows include a separate status column before duration.
PIPELINE_KIND_PATTERN = r"Clinical|Imaging|OMOP|CMA"
PIPELINE_SUMMARY_RE = re.compile(
    rf"[✓✗û]\s+((?:{PIPELINE_KIND_PATTERN})\s+Pipeline|Sidecar Pipeline:\s+.+?)\s{{2,}}(.+?)\s{{2,}}(\d+\.?\d*\s*(?:min|sec)|—)",
    re.IGNORECASE,
)
# Phase transition marker: @@PHASE|<number>|<label>|<stepCount>@@
PHASE_TRANSITION_RE = re.compile(r"@@PHASE\|(\d+)\|(.+?)\|(\d+)@@")

# HDS pipeline sub-step names (failures here are warnings, not hard failures)
HDS_PIPELINE_SUBSTEPS = {"Sidecar Pipeline", "Clinical Pipeline", "CMA Pipeline", "Imaging Pipeline", "OMOP Pipeline"}
# Regexes for HDS/Fabric pipeline lifecycle lines emitted by storage-access-trusted-workspace.ps1.
PIPELINE_NAME_RE = re.compile(rf"\b(Sidecar Pipeline:\s+.+?|(?:{PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline)\b", re.IGNORECASE)
PIPELINE_POLL_RE = re.compile(r"\[([\d.]+)\s*min\]\s*Status:\s*(\w+)", re.IGNORECASE)
PIPELINE_STATUS_RE = re.compile(rf"\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline status:\s*(\w+)\s*\(([\d.]+)\s*min elapsed\)", re.IGNORECASE)
PIPELINE_COMPLETED_RE = re.compile(rf"\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline completed\b", re.IGNORECASE)
PIPELINE_FAILED_RE = re.compile(rf"\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline\s+(Failed|Cancelled|Canceled)\b", re.IGNORECASE)
PIPELINE_TIMEOUT_RE = re.compile(rf"\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline did not complete within\s+([\d.]+)\s*min", re.IGNORECASE)
PIPELINE_SKIP_RE = re.compile(rf"(?:SKIPPING|Skipping)\s+({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline|\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline\b.*\bSKIPPED\b", re.IGNORECASE)
PIPELINE_INVOKE_RE = re.compile(rf"\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+pipeline\b.*\b(invoking|invoked|already running|recently invoked|will poll|waiting for .*completion)\b", re.IGNORECASE)
PIPELINE_URL_RE = re.compile(rf"\b({PIPELINE_KIND_PATTERN})(?:\s+\w+)*\s+Pipeline:\s+(https?://\S+)", re.IGNORECASE)



def _ps_single_quoted(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _ps_hashtable_literal(values: dict[str, str]) -> str:
    entries = [f"{_ps_single_quoted(k)}={_ps_single_quoted(v)}" for k, v in values.items()]
    return "@{" + ";".join(entries) + "}"

def _pipeline_substep_name(kind: str) -> str:
    normalized = kind.strip()
    sidecar_match = re.match(r"Sidecar Pipeline:\s*(.+)", normalized, re.IGNORECASE)
    if sidecar_match:
        return f"Sidecar Pipeline: {sidecar_match.group(1).strip()}"
    lowered = normalized.lower()
    if "omop" in lowered:
        return "OMOP Pipeline"
    if "cma" in lowered:
        return "CMA Pipeline"
    if "clinical" in lowered:
        return "Clinical Pipeline"
    if "imaging" in lowered:
        return "Imaging Pipeline"
    return normalized


def is_hds_pipeline_substep(step_name: str) -> bool:
    return step_name in HDS_PIPELINE_SUBSTEPS or step_name.startswith("Sidecar Pipeline:")

def _pipeline_status(raw: str) -> str:
    status = raw.lower()
    if status in {"completed", "succeeded", "success"}:
        return "succeeded"
    if status in {"failed", "cancelled", "canceled"}:
        return "failed"
    if status in {"skipped", "skip"}:
        return "skipped"
    if status in {"notstarted", "not_started", "queued", "pending"}:
        return "pending"
    if status in {"inprogress", "running", "executing"}:
        return "running"
    if status in {"timeout", "timedout", "timed_out"}:
        return "warning"
    return "warning"


def _pipeline_status_from_detail(detail: str) -> str:
    normalized = detail.lower()
    if "skipped" in normalized or "skipping" in normalized:
        return "skipped"
    if "failed" in normalized or "cancelled" in normalized or "canceled" in normalized:
        return "failed"
    if "timeout" in normalized or "did not complete" in normalized or "warn" in normalized:
        return "warning"
    if "completed" in normalized or "succeeded" in normalized or "success" in normalized:
        return "succeeded"
    if "invoked" in normalized or "already running" in normalized:
        return "succeeded"
    return "warning"


def _emit_substep(
    step_callback: Any,
    name: str,
    status: str,
    detail: str,
    duration: str = "",
    *,
    run_id: str = "",
    url: str = "",
) -> None:
    if not step_callback:
        return
    payload = {"status": status, "detail": detail}
    if run_id:
        payload["runId"] = run_id
    if url:
        payload["url"] = url
    step_callback("substep_update", name, json.dumps(payload), duration)

 # Callback type for step events

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
    instance_id = config.get("instance_id", "")
    token = _INSTANCE_ID.set(instance_id)
    try:
        args = _build_deploy_args(config)
        logger.info("Invoking Deploy-All.ps1 with args: %s", " ".join(args[2:]))
        exit_code = _run_powershell(args, step_callback, pid_callback, instance_id=instance_id)
    finally:
        _INSTANCE_ID.reset(token)
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
    if config.get("dicom_toolkit_path"):
        args += ["-DicomToolkitPath", config["dicom_toolkit_path"]]
    if config.get("phase2_only"):
        args.append("-Phase2")
    if config.get("phase3_only"):
        args.append("-Phase3")
    if config.get("phase4_only"):
        args.append("-Phase4")
    if config.get("phase7_only"):
        args.append("-Phase7")
    if config.get("skip_imaging"):
        args.append("-SkipImaging")

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
    tags = normalize_policy_tags(config.get("tags", {}))

    # Tags require a PowerShell hashtable. Use -Command mode and quote every
    # string literal explicitly so policy-tag injection does not make apostrophes
    # in user-supplied names/tags break argument parsing.
    if tags:
        params = [
            f"-FabricWorkspaceName {_ps_single_quoted(config['fabric_workspace_name'])}",
            f"-Location {_ps_single_quoted(config.get('location', 'eastus'))}",
            f"-ExpectedTenantId {_ps_single_quoted(config.get('expected_tenant_id', '8d038e6a-9b7d-4cb8-bbcf-e84dff156478'))}",
            f"-ExpectedSubscriptionId {_ps_single_quoted(config.get('expected_subscription_id', '9bbee190-dc61-4c58-ab47-1275cb04018f'))}",
        ]
        if config.get("resource_group_name"):
            params.append(f"-ResourceGroupName {_ps_single_quoted(config['resource_group_name'])}")
        if config.get("admin_security_group"):
            params.append(f"-AdminSecurityGroup {_ps_single_quoted(config['admin_security_group'])}")
        if config.get("patient_count"):
            params.append(f"-PatientCount {config['patient_count']}")
        if config.get("alert_email"):
            params.append(f"-AlertEmail {_ps_single_quoted(config['alert_email'])}")
        if config.get("payer_ops_email"):
            params.append(f"-PayerOpsEmail {_ps_single_quoted(config['payer_ops_email'])}")
        if config.get("claim_event_rate_per_minute"):
            params.append(f"-ClaimEventRatePerMinute {config['claim_event_rate_per_minute']}")
        if config.get("dicom_toolkit_path"):
            params.append(f"-DicomToolkitPath {_ps_single_quoted(config['dicom_toolkit_path'])}")
        if config.get("capacity_subscription_id"):
            params.append(f"-CapacitySubscriptionId {_ps_single_quoted(config['capacity_subscription_id'])}")
        if config.get("capacity_resource_group"):
            params.append(f"-CapacityResourceGroup {_ps_single_quoted(config['capacity_resource_group'])}")
        if config.get("capacity_name"):
            params.append(f"-CapacityName {_ps_single_quoted(config['capacity_name'])}")
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
        if config.get("source_resource_group"):
            params.append(f"-SourceResourceGroup {_ps_single_quoted(config['source_resource_group'])}")
        if config.get("use_cached_synthea"):
            params.append("-UseCachedSynthea")
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
        if config.get("skip_quality_measures"):
            params.append("-SkipQualityMeasures")
        if config.get("require_bronze_clinical_fhir"):
            params.append("-RequireBronzeClinicalFhir")
        if config.get("require_bronze_imaging_dicom"):
            params.append("-RequireBronzeImagingDicom")
        if config.get("skip_phase7"):
            params.append("-SkipPhase7")
        if config.get("skip_payer_rti"):
            params.append("-SkipPayerRti")
        if config.get("skip_payer_activator"):
            params.append("-SkipPayerActivator")
        if config.get("skip_ops_agent"):
            params.append("-SkipOpsAgent")
        if config.get("skip_graph_agent"):
            params.append("-SkipGraphAgent")
        if config.get("phase2_only"):
            params.append("-Phase2")
        if config.get("phase3_only"):
            params.append("-Phase3")
        if config.get("phase4_only"):
            params.append("-Phase4")
        if config.get("phase7_only"):
            params.append("-Phase7")

        params.append(f"-Tags {_ps_hashtable_literal(tags)}")

        cmd = f"& {_ps_single_quoted(DEPLOY_SCRIPT)} {' '.join(params)}"
        return ["pwsh", "-NoProfile", "-NonInteractive", "-Command", cmd]

    # No tags — use simpler -File mode
    args = [
        "pwsh", "-NoProfile", "-NonInteractive", "-File",
        str(DEPLOY_SCRIPT),
        "-FabricWorkspaceName", config["fabric_workspace_name"],
        "-ExpectedTenantId", config.get("expected_tenant_id", "8d038e6a-9b7d-4cb8-bbcf-e84dff156478"),
        "-ExpectedSubscriptionId", config.get("expected_subscription_id", "9bbee190-dc61-4c58-ab47-1275cb04018f"),
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
    if config.get("payer_ops_email"):
        args += ["-PayerOpsEmail", config["payer_ops_email"]]
    if config.get("claim_event_rate_per_minute"):
        args += ["-ClaimEventRatePerMinute", str(config["claim_event_rate_per_minute"])]
    if config.get("dicom_toolkit_path"):
        args += ["-DicomToolkitPath", config["dicom_toolkit_path"]]
    if config.get("capacity_subscription_id"):
        args += ["-CapacitySubscriptionId", config["capacity_subscription_id"]]
    if config.get("capacity_resource_group"):
        args += ["-CapacityResourceGroup", config["capacity_resource_group"]]
    if config.get("capacity_name"):
        args += ["-CapacityName", config["capacity_name"]]

    if config.get("skip_base_infra"):
        args.append("-SkipBaseInfra")
    if config.get("skip_fhir"):
        args.append("-SkipFhir")
    if config.get("skip_dicom"):
        args.append("-SkipDicom")
    if config.get("skip_fabric"):
        args.append("-SkipFabric")
    if config.get("reuse_patients"):
        args.append("-ReusePatients")
    if config.get("source_resource_group"):
        args += ["-SourceResourceGroup", config["source_resource_group"]]
    if config.get("skip_fhir_export"):
        args.append("-SkipFhirExport")
    if config.get("use_cached_synthea"):
        args.append("-UseCachedSynthea")
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
    if config.get("skip_quality_measures"):
        args.append("-SkipQualityMeasures")
    if config.get("require_bronze_clinical_fhir"):
        args.append("-RequireBronzeClinicalFhir")
    if config.get("require_bronze_imaging_dicom"):
        args.append("-RequireBronzeImagingDicom")
    if config.get("skip_phase7"):
        args.append("-SkipPhase7")
    if config.get("skip_payer_rti"):
        args.append("-SkipPayerRti")
    if config.get("skip_payer_activator"):
        args.append("-SkipPayerActivator")
    if config.get("skip_ops_agent"):
        args.append("-SkipOpsAgent")
    if config.get("skip_graph_agent"):
        args.append("-SkipGraphAgent")

    if config.get("phase2_only"):
        args.append("-Phase2")
    if config.get("phase3_only"):
        args.append("-Phase3")
    if config.get("phase4_only"):
        args.append("-Phase4")
    if config.get("phase7_only"):
        args.append("-Phase7")

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


def _run_powershell(args: list[str], step_callback: Any = None, pid_callback: Any = None, instance_id: str = "") -> int:
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
    current_pipeline_name = ""
    current_step_title = ["PowerShell deployment"]
    last_output_at = [time.monotonic()]
    heartbeat_stop = threading.Event()

    def emit_quiet_heartbeat() -> None:
        token = _INSTANCE_ID.set(instance_id)
        try:
            while not heartbeat_stop.wait(30):
                if process.poll() is not None:
                    return
                quiet_for = time.monotonic() - last_output_at[0]
                if quiet_for < 30:
                    continue
                label = current_step_title[0] or current_phase_label or "PowerShell deployment"
                logger.info("Still running %s — waiting for PowerShell output (%.0fs quiet)", label, quiet_for)
        finally:
            _INSTANCE_ID.reset(token)


    env_dict = {**__import__("os").environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    if instance_id:
        env_dict["ORCHESTRATOR_INSTANCE_ID"] = instance_id

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
        env=env_dict,
    )

    if pid_callback:
        pid_callback(process.pid)

    heartbeat_thread = threading.Thread(target=emit_quiet_heartbeat, daemon=True)
    heartbeat_thread.start()


    for line in process.stdout:
        last_output_at[0] = time.monotonic()
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
            current_step_title[0] = step_title
            if step_callback:
                step_callback("step_start", step_title, "", "")
            logger.info(line)
            continue

        pipeline_match = PIPELINE_NAME_RE.search(line)
        if pipeline_match:
            current_pipeline_name = _pipeline_substep_name(pipeline_match.group(1))

        url_match = PIPELINE_URL_RE.search(line)
        if url_match:
            current_pipeline_name = _pipeline_substep_name(url_match.group(1))
            _emit_substep(step_callback, current_pipeline_name, "running", line.strip(), "", url=url_match.group(2).strip())

        named_status_match = PIPELINE_STATUS_RE.search(line)
        if named_status_match:
            current_pipeline_name = _pipeline_substep_name(named_status_match.group(1))
            raw_status = named_status_match.group(2)
            duration = f"{named_status_match.group(3)} min"
            status = _pipeline_status(raw_status)
            _emit_substep(step_callback, current_pipeline_name, status, line.strip(), duration)
            if status in {"failed", "warning"} and step_callback:
                step_callback("step_warning", current_pipeline_name, line.strip(), duration)

        poll_match = PIPELINE_POLL_RE.search(line)
        if poll_match and current_pipeline_name:
            raw_status = poll_match.group(2)
            status = _pipeline_status(raw_status)
            _emit_substep(step_callback, current_pipeline_name, status, line.strip(), f"{poll_match.group(1)} min")
            if status in {"failed", "warning"} and step_callback:
                step_callback("step_warning", current_pipeline_name, line.strip(), f"{poll_match.group(1)} min")

        completed_match = PIPELINE_COMPLETED_RE.search(line)
        if completed_match:
            current_pipeline_name = _pipeline_substep_name(completed_match.group(1))
            _emit_substep(step_callback, current_pipeline_name, "succeeded", line.strip(), "")

        failed_match = PIPELINE_FAILED_RE.search(line)
        if failed_match:
            current_pipeline_name = _pipeline_substep_name(failed_match.group(1))
            _emit_substep(step_callback, current_pipeline_name, "failed", line.strip(), "")
            if step_callback:
                step_callback("step_warning", current_pipeline_name, line.strip(), "")

        timeout_match = PIPELINE_TIMEOUT_RE.search(line)
        if timeout_match:
            current_pipeline_name = _pipeline_substep_name(timeout_match.group(1))
            duration = f"{timeout_match.group(2)} min"
            _emit_substep(step_callback, current_pipeline_name, "warning", line.strip(), duration)
            if step_callback:
                step_callback("step_warning", current_pipeline_name, line.strip(), duration)

        skip_match = PIPELINE_SKIP_RE.search(line)
        if skip_match:
            skipped_kind = skip_match.group(1) or skip_match.group(2)
            current_pipeline_name = _pipeline_substep_name(skipped_kind)
            _emit_substep(step_callback, current_pipeline_name, "skipped", line.strip(), "")

        invoke_match = PIPELINE_INVOKE_RE.search(line)
        if invoke_match:
            current_pipeline_name = _pipeline_substep_name(invoke_match.group(1))
            _emit_substep(step_callback, current_pipeline_name, "running", line.strip(), "")
        elif current_pipeline_name and any(
            marker in line.lower()
            for marker in ["invoking", "invoked", "waiting for", "already running", "recently invoked", "will poll"]
        ):
            _emit_substep(step_callback, current_pipeline_name, "running", line.strip(), "")

        if current_pipeline_name and ("could not invoke" in line.lower() or "not found" in line.lower()) and "pipeline" in line.lower():
            _emit_substep(step_callback, current_pipeline_name, "warning", line.strip(), "")
            if step_callback:
                step_callback("step_warning", current_pipeline_name, line.strip(), "")

        pipeline_summary_match = PIPELINE_SUMMARY_RE.search(line)
        if pipeline_summary_match:
            step_name = _pipeline_substep_name(pipeline_summary_match.group(1))
            raw_status = pipeline_summary_match.group(2).strip()
            duration = pipeline_summary_match.group(3).strip()
            status = _pipeline_status_from_detail(raw_status)
            _emit_substep(step_callback, step_name, status, line.strip(), duration)
            if status in {"failed", "warning"} and step_callback:
                step_callback("step_warning", step_name, line.strip(), duration)
            logger.info(line) if status in {"succeeded", "skipped"} else logger.warning(line)
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
            elif is_hds_pipeline_substep(step_name):
                # HDS pipeline sub-step failures are warnings, not hard failures
                event = "step_warning"
            else:
                event = "step_failed"
            if step_callback:
                step_callback(event, step_name, "", duration)
            if is_hds_pipeline_substep(step_name):
                _emit_substep(step_callback, step_name, "succeeded" if is_success else _pipeline_status_from_detail(line), line.strip(), duration)
            if is_success:
                logger.info(line)
            elif event == "step_warning":
                logger.warning(line)
            else:
                logger.error(line)
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
        elif re.search(r"(?<![A-Za-z])Failed:\s*0(?!\d)", line):
            logger.info(line)
        elif any(marker in line for marker in ["✗", "ERROR", "error", "failed", "Failed", "FAILED"]):
            logger.error(line)
        elif any(marker in line for marker in ["WARNING", "⚠", "Skipping"]):
            logger.warning(line)
        else:
            logger.info(line)

    process.wait()
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1)
    logger.info("PowerShell exited with code %d", process.returncode)
    return process.returncode
