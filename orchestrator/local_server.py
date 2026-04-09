"""Local development server — lightweight FastAPI replacement for Durable Functions.

Calls the same activity modules directly without the Durable Functions framework.
Used for local testing only. In production, the Durable Functions app handles
orchestration with checkpointing, retries, and human interaction gates.

Usage:
    cd orchestrator
    .venv\\Scripts\\activate
    python local_server.py
"""

import asyncio
import json
import logging
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Add orchestrator to path so activity imports work
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            Path(__file__).parent / "orchestrator.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("local_server")

STATE_FILE = Path(__file__).parent / ".orchestrator-state.json"

# ── Encoding-safe subprocess helper ────────────────────────────────────
# All az CLI calls must use UTF-8 encoding to avoid Windows cp1252 charmap crashes.
_UTF8_ENV = {**__import__("os").environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}

def _az_run(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a subprocess with UTF-8 encoding and Windows shell support."""
    defaults = dict(
        capture_output=True, text=True,
        shell=(sys.platform == "win32"),
        encoding="utf-8", errors="replace",
        env=_UTF8_ENV,
    )
    defaults.update(kwargs)
    return subprocess.run(args, **defaults)

app = FastAPI(title="Med Device Deployment Orchestrator — Local Dev")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Database ───────────────────────────────────────────────────────────

from shared.database import (
    save_deployment, get_deployment, list_deployments as db_list_deployments,
    delete_deployment as db_delete_deployment, clear_all_deployments as db_clear_all,
    mark_stale_as_terminated, migrate_from_json,
    get_locks, set_lock, remove_lock,
    get_form_history, add_form_history,
    get_dismissed_teardowns, dismiss_teardown,
)

# Migrate from old JSON state file if it exists
migrate_from_json(STATE_FILE)
mark_stale_as_terminated()

# In-memory cache for active deployments (for real-time log streaming)
deployments: dict[str, dict] = {}
for dep in db_list_deployments():
    deployments[dep["instanceId"]] = dep
logger.info("Loaded %d deployments from database", len(deployments))

# Track active subprocess PIDs for cancellation
active_processes: dict[str, int] = {}  # instance_id → PID

# Track active teardown scans for incremental UI updates
scan_jobs: dict[str, dict] = {}

# Cache for resource scan results to avoid redundant Azure/Fabric API calls
# Used by both teardown scanner and deployment check-existing endpoint
_scan_cache: dict[str, dict] = {}  # key → {result, timestamp}
_SCAN_CACHE_TTL = 120  # seconds


def _get_cached(key: str) -> dict | None:
    entry = _scan_cache.get(key)
    if entry and (datetime.now(timezone.utc).timestamp() - entry["timestamp"]) < _SCAN_CACHE_TTL:
        return entry["result"]
    return None


def _set_cached(key: str, result: dict):
    _scan_cache[key] = {"result": result, "timestamp": datetime.now(timezone.utc).timestamp()}


def _scan_counts(candidates: list[dict]) -> dict[str, int]:
    return {
        "fabric": sum(1 for candidate in candidates if candidate.get("type") == "fabric"),
        "azure": sum(1 for candidate in candidates if candidate.get("type") == "azure"),
        "spn": sum(1 for candidate in candidates if candidate.get("type") == "spn"),
    }


async def _run_scan_job(scan_id: str, subscription_id: str):
    job = scan_jobs[scan_id]

    def update_status(phase: str, message: str):
        current = scan_jobs.get(scan_id)
        if not current:
            return
        current["phase"] = phase
        current["message"] = message

    def update_candidates(current_candidates: list[dict], phase: str, message: str):
        current = scan_jobs.get(scan_id)
        if not current:
            return
        current["candidates"] = list(current_candidates)
        current["counts"] = _scan_counts(current_candidates)
        current["phase"] = phase
        current["message"] = message

    try:
        candidates = await asyncio.to_thread(
            _scan_resources_sync,
            subscription_id,
            update_candidates,
            update_status,
        )
        job["status"] = "completed"
        job["phase"] = "complete"
        job["message"] = f"Scan complete — {len(candidates)} candidates discovered"
        job["candidates"] = list(candidates)
        job["counts"] = _scan_counts(candidates)
        job["completedAt"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.exception("Scan job %s failed", scan_id)
        job["status"] = "failed"
        job["phase"] = "failed"
        job["message"] = f"Scan failed: {e}"
        job["error"] = str(e)
        job["completedAt"] = datetime.now(timezone.utc).isoformat()


def save_state():
    """Persist current deployment to database."""
    for inst_id, dep in deployments.items():
        save_deployment(inst_id, dep)


class TeardownRequest(BaseModel):
    fabric_workspace_name: str = ""
    resource_group_name: str = ""
    delete_workspace: bool = False
    delete_azure_rg: bool = True


class DeployRequest(BaseModel):
    resource_group_name: str = ""
    location: str = "eastus"
    admin_security_group: str = ""
    fabric_workspace_name: str = ""
    patient_count: int = 100
    tags: dict[str, str] = {}
    skip_base_infra: bool = False
    skip_fhir: bool = False
    skip_dicom: bool = False
    skip_fabric: bool = False
    alert_email: str = ""
    capacity_subscription_id: str = ""
    capacity_resource_group: str = ""
    capacity_name: str = ""
    pause_capacity_after_deploy: bool = False
    reuse_patients: bool = False  # If True, skip Synthea/Loader and reuse existing patients


def now_iso():
    return datetime.now(timezone.utc).isoformat()


@app.post("/api/teardown/start")
async def start_teardown(req: TeardownRequest):
    now_local = datetime.now()
    timestamp = now_local.strftime("%Y%m%d-%H%M%S")
    teardown_mode = "teardownFull" if req.delete_workspace and req.delete_azure_rg else "teardownPartial"
    instance_id = f"{teardown_mode}-{timestamp}"

    teardown_targets = []
    if req.fabric_workspace_name:
        teardown_targets.append(req.fabric_workspace_name)
    if req.resource_group_name:
        teardown_targets.append(req.resource_group_name)
    teardown_display_name = " + ".join(teardown_targets) if teardown_targets else "Teardown"

    deployment = {
        "instanceId": instance_id,
        "name": "teardown_orchestrator",
        "runtimeStatus": "Running",
        "createdTime": now_iso(),
        "lastUpdatedTime": now_iso(),
        "customStatus": {
            "currentPhase": "Starting Teardown",
            "status": "running",
            "detail": "",
            "completedPhases": 0,
            "totalPhases": 4,
            "resources": {},
            "workspaceName": req.fabric_workspace_name,
            "resourceGroupName": req.resource_group_name,
            "runType": "teardown",
            "teardownMode": teardown_mode,
            "displayName": teardown_display_name,
            "logs": [],
        },
        "output": None,
    }
    deployments[instance_id] = deployment
    save_state()

    # Run teardown in background
    asyncio.create_task(_run_teardown(instance_id, req))

    logger.info("Teardown started: %s (workspace=%s, rg=%s)",
                instance_id, req.fabric_workspace_name, req.resource_group_name)
    return {"instanceId": instance_id, "statusUrl": f"/api/deploy/{instance_id}/status"}


async def _run_teardown(instance_id: str, req: TeardownRequest):
    """Run Remove-AllResources.ps1 via subprocess."""
    import logging as _logging

    deployment = deployments[instance_id]
    teardown_logs: list[dict] = []

    class StatusLogHandler(_logging.Handler):
        def emit(self, record: _logging.LogRecord):
            msg = self.format(record)
            teardown_logs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "success" if any(w in msg.lower() for w in ["deleted", "verified", "deprovisioned", "removed", "✓"])
                    else "error" if record.levelno >= _logging.ERROR
                    else "warn" if record.levelno >= _logging.WARNING
                    else "info",
                "message": msg,
            })
            deployment["customStatus"]["logs"] = teardown_logs[-100:]
            deployment["customStatus"]["detail"] = msg
            deployment["lastUpdatedTime"] = now_iso()
            save_state()

    handler = StatusLogHandler()
    handler.setLevel(_logging.INFO)
    handler.setFormatter(_logging.Formatter("%(message)s"))

    _logging.getLogger("activities.invoke_powershell").addHandler(handler)

    try:
        from activities.invoke_powershell import run_teardown

        config = req.model_dump()
        logger.info("Starting teardown for workspace='%s', rg='%s'",
                     req.fabric_workspace_name, req.resource_group_name)

        # Track teardown phases in reverse deployment order
        teardown_phases: list[dict] = []

        def add_teardown_phase(name: str, status: str = "running"):
            teardown_phases.append({"phase": name, "status": status})
            deployment["customStatus"]["currentPhase"] = name
            deployment["output"] = {
                "status": "running",
                "phases": teardown_phases,
                "resources": {},
            }
            save_state()

        def complete_teardown_phase(name: str, duration: float = 0):
            for p in teardown_phases:
                if p["phase"] == name:
                    p["status"] = "succeeded"
                    if duration:
                        p["duration"] = duration
            succeeded = [p for p in teardown_phases if p["status"] == "succeeded"]
            deployment["customStatus"]["completedPhases"] = len(succeeded)
            deployment["output"] = {
                "status": "running",
                "phases": teardown_phases,
                "resources": {},
            }
            save_state()

        # Phase labels map to the log output from Remove-AllResources.ps1
        phase_triggers = {
            "Fabric Workspace Items": "Step 2:",
            "Workspace Identity": "Step 2b:",
            "Delete Workspace": "Step 3:",
            "Azure Resource Group": "Step 1:",
        }
        active_phase: str = ""

        # Override the log handler to detect phase transitions
        original_emit = handler.emit
        def phase_tracking_emit(record):
            nonlocal active_phase
            msg = handler.format(record)

            # Detect phase starts from Remove-AllResources.ps1 output
            if "Step 2: Fabric Workspace Items" in msg or "DEL " in msg:
                if active_phase != "Fabric Workspace Items":
                    active_phase = "Fabric Workspace Items"
                    add_teardown_phase("Fabric Workspace Items")
            elif "Step 2b:" in msg or "deprovision" in msg.lower():
                if active_phase != "Workspace Identity":
                    if active_phase == "Fabric Workspace Items":
                        complete_teardown_phase("Fabric Workspace Items")
                    active_phase = "Workspace Identity"
                    add_teardown_phase("Workspace Identity")
            elif "Step 3:" in msg or "Delete Fabric Workspace" in msg:
                if active_phase != "Delete Workspace":
                    if active_phase == "Workspace Identity":
                        complete_teardown_phase("Workspace Identity")
                    active_phase = "Delete Workspace"
                    add_teardown_phase("Delete Workspace")
            elif "Step 1:" in msg or "Azure Resource Group" in msg:
                if active_phase != "Azure Resource Group":
                    if active_phase == "Delete Workspace":
                        complete_teardown_phase("Delete Workspace")
                    active_phase = "Azure Resource Group"
                    add_teardown_phase("Azure Resource Group")
            elif "Workspace deleted" in msg or "✓ Workspace deleted" in msg:
                if active_phase == "Delete Workspace":
                    complete_teardown_phase("Delete Workspace")
            elif "Azure RG deletion initiated" in msg or "Azure RG deleted" in msg:
                pass  # We'll handle RG completion by polling

            original_emit(record)

        handler.emit = phase_tracking_emit

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_teardown, config)

        exit_code = result.get("exit_code", -1)

        # Mark any remaining running phases as succeeded if script exited cleanly
        if exit_code == 0:
            for p in teardown_phases:
                if p["status"] == "running" and p["phase"] != "Azure Resource Group":
                    p["status"] = "succeeded"

        # If Azure RG was targeted, ensure it doesn't have a phase yet — add one
        if req.resource_group_name:
            rg_phase = next((p for p in teardown_phases if p["phase"] == "Azure Resource Group"), None)
            if not rg_phase:
                add_teardown_phase("Azure Resource Group")

            # Poll until Azure RG is actually deleted (async deletion may take minutes)
            deployment["customStatus"]["detail"] = f"Waiting for Azure RG '{req.resource_group_name}' to be fully deleted..."
            save_state()

            for poll_attempt in range(60):  # Up to ~5 min (5s intervals)
                try:
                    check = _az_run(["az", "group", "exists", "--name", req.resource_group_name])
                    if check.stdout.strip().lower() == "false":
                        complete_teardown_phase("Azure Resource Group")
                        teardown_logs.append({
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "level": "success",
                            "message": f"✓ Azure RG '{req.resource_group_name}' has been fully deleted.",
                        })
                        deployment["customStatus"]["logs"] = teardown_logs[-100:]
                        deployment["customStatus"]["detail"] = f"Azure RG '{req.resource_group_name}' deleted"
                        save_state()
                        break
                    else:
                        if poll_attempt % 6 == 0:  # Log every 30s
                            teardown_logs.append({
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "level": "info",
                                "message": f"Azure RG '{req.resource_group_name}' still deleting... ({(poll_attempt + 1) * 5}s)",
                            })
                            deployment["customStatus"]["logs"] = teardown_logs[-100:]
                            save_state()
                except Exception as poll_err:
                    logger.warning("RG poll error: %s", poll_err)
                await asyncio.sleep(5)
            else:
                # Timed out waiting for RG deletion
                teardown_logs.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "warn",
                    "message": f"Timed out waiting for Azure RG '{req.resource_group_name}' deletion after 5 min. It may still be deleting.",
                })
                deployment["customStatus"]["logs"] = teardown_logs[-100:]
                save_state()

        # Final status
        all_succeeded = all(p["status"] == "succeeded" for p in teardown_phases) if teardown_phases else exit_code == 0
        if all_succeeded:
            deployment["runtimeStatus"] = "Completed"
            deployment["customStatus"]["status"] = "succeeded"
            deployment["customStatus"]["currentPhase"] = "Teardown Complete"
            deployment["customStatus"]["completedPhases"] = len(teardown_phases)
        elif exit_code == 0:
            deployment["runtimeStatus"] = "Completed"
            deployment["customStatus"]["status"] = "succeeded"
            deployment["customStatus"]["currentPhase"] = "Teardown Complete"
            deployment["customStatus"]["completedPhases"] = len([p for p in teardown_phases if p["status"] == "succeeded"])
        else:
            deployment["runtimeStatus"] = "Failed"
            deployment["customStatus"]["status"] = "failed"

        deployment["output"] = {
            "status": "succeeded" if all_succeeded or exit_code == 0 else "failed",
            "phases": teardown_phases if teardown_phases else [{"phase": "Teardown", "status": "succeeded" if exit_code == 0 else "failed", "duration": result.get("duration_seconds", 0)}],
            "resources": result.get("results", {}),
        }
        logger.info("Teardown %s (exit=%d, %.1fs)", instance_id, exit_code, result.get("duration_seconds", 0))

    except Exception as e:
        logger.error("Teardown failed: %s", e, exc_info=True)
        deployment["runtimeStatus"] = "Failed"
        deployment["customStatus"]["status"] = "failed"
        deployment["customStatus"]["detail"] = str(e)
        deployment["output"] = {
            "status": "failed",
            "phases": [{"phase": "Teardown", "status": "failed", "detail": str(e)}],
            "resources": {},
        }
    finally:
        _logging.getLogger("activities.invoke_powershell").removeHandler(handler)
        deployment["lastUpdatedTime"] = now_iso()
        save_state()


@app.post("/api/deploy/preflight")
async def run_preflight(req: DeployRequest):
    """Run prerequisite checks without starting a deployment."""
    from activities.invoke_powershell import run_preflight as _run_preflight
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _run_preflight, req.model_dump())
    status_code = 200 if result["passed"] else 422
    return func_response(result, status_code)


def func_response(data, status_code=200):
    """Helper to return JSON with custom status code."""
    from fastapi.responses import JSONResponse
    return JSONResponse(content=data, status_code=status_code)


@app.post("/api/deploy/start")
async def start_deploy(req: DeployRequest):
    # Build descriptive instance ID: P<milestones>-<datetime>
    # Milestone numbers encode which progress-bar milestones are active:
    #   1 = Infra & Ingestion, 2 = Enrichment & Agents,
    #   3 = Imaging Toolkit,   4 = Ontology & Activator
    now_local = datetime.now()
    timestamp = now_local.strftime("%Y%m%d-%H%M%S")

    # Determine active milestones from config flags
    # From the UI, skip_* flags only skip sub-steps — all 4 milestones remain.
    # Phase-only flags (phase2_only, etc.) would restrict to specific milestones,
    # but the UI doesn't expose these currently.
    milestones = [1, 2, 3, 4]  # Default: all milestones active
    phase_label = "P" + "".join(str(m) for m in milestones)

    instance_id = f"{phase_label}-{timestamp}"
    deployment = {
        "instanceId": instance_id,
        "name": "deploy_all_orchestrator",
        "runtimeStatus": "Running",
        "createdTime": now_iso(),
        "lastUpdatedTime": now_iso(),
        "customStatus": {
            "currentPhase": "Starting",
            "status": "running",
            "detail": "",
            "completedPhases": 0,
            "totalPhases": 12,
            "resources": {},
            "logs": [],
            "workspaceName": req.fabric_workspace_name,
            "resourceGroupName": req.resource_group_name,
            "capacityName": req.capacity_name,
            "capacityResourceGroup": req.capacity_resource_group,
            "capacitySubscriptionId": req.capacity_subscription_id,
            "pauseCapacityAfterDeploy": req.pause_capacity_after_deploy,
            "links": {
                "azurePortal": f"https://portal.azure.com/#@/resource/subscriptions/{req.capacity_subscription_id}/resourceGroups/{req.resource_group_name}" if req.resource_group_name else "",
                "fabricWorkspace": f"https://app.fabric.microsoft.com/groups?experience=fabric-developer&name={req.fabric_workspace_name}" if req.fabric_workspace_name else "",
            },
            "deployConfig": req.model_dump(),
        },
        "output": None,
    }
    deployments[instance_id] = deployment
    save_state()

    # Run deployment in background
    asyncio.create_task(_run_deploy(instance_id, req))

    logger.info("Deployment started: %s (workspace=%s, rg=%s)",
                instance_id, req.fabric_workspace_name, req.resource_group_name)
    return {"instanceId": instance_id, "statusUrl": f"/api/deploy/{instance_id}/status"}


async def _run_deploy(instance_id: str, req: DeployRequest):
    """Run Deploy-All.ps1 via subprocess, streaming output to deployment status."""
    import logging as _logging

    deployment = deployments[instance_id]
    deploy_logs: list[dict] = []
    # Mutable container so the log handler can reference the current phase
    phase_tracker = {"current": 0}

    class StatusLogHandler(_logging.Handler):
        def emit(self, record: _logging.LogRecord):
            msg = self.format(record)
            deploy_logs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "success" if any(w in msg.lower() for w in ["succeeded", "deployed", "created", "completed", "ready", "provisioned", "built", "✓"])
                    else "error" if record.levelno >= _logging.ERROR
                    else "warn" if record.levelno >= _logging.WARNING
                    else "info",
                "message": msg,
                "phase": phase_tracker["current"],
            })
            deployment["customStatus"]["logs"] = deploy_logs[-200:]
            deployment["customStatus"]["detail"] = msg
            deployment["lastUpdatedTime"] = now_iso()
            save_state()

    handler = StatusLogHandler()
    handler.setLevel(_logging.INFO)
    handler.setFormatter(_logging.Formatter("%(message)s"))

    _logging.getLogger("activities.invoke_powershell").addHandler(handler)

    phases: list[dict] = []
    current_major_phase = 0  # Tracks which major phase (1-4) we're in
    current_major_phase_label = ""

    def step_callback(event: str, step_name: str, detail: str, duration: str):
        """Handle step events from PowerShell output parser."""
        nonlocal current_major_phase, current_major_phase_label

        if event == "phase_transition":
            # Major phase transition: "PHASE N: Label"
            import re
            m = re.match(r"PHASE\s+(\d+):\s+(.+)", step_name)
            if m:
                current_major_phase = int(m.group(1))
                current_major_phase_label = m.group(2)
                phase_tracker["current"] = current_major_phase
            deployment["customStatus"]["currentPhase"] = step_name
            deployment["customStatus"]["currentMajorPhase"] = current_major_phase
            deployment["lastUpdatedTime"] = now_iso()
            save_state()
            return

        if event == "step_start":
            # Mark any previously running phase as succeeded (the result line
            # for the previous step may not have been parsed yet)
            for p in phases:
                if p["status"] == "running":
                    p["status"] = "succeeded"
            phases.append({"phase": step_name, "status": "running", "majorPhase": current_major_phase})
            deployment["customStatus"]["currentPhase"] = step_name
            deployment["customStatus"]["status"] = "running"
        elif event == "step_succeeded":
            # Find the last running phase and mark it
            for p in reversed(phases):
                if p["status"] == "running":
                    p["status"] = "succeeded"
                    p["duration"] = duration
                    break
        elif event == "step_failed":
            for p in reversed(phases):
                if p["status"] == "running":
                    p["status"] = "failed"
                    p["detail"] = detail
                    p["duration"] = duration
                    break
        elif event == "step_warning":
            # HDS pipeline sub-step warnings: attach to current phase without failing it
            for p in reversed(phases):
                if p["status"] == "running" or p["status"] == "succeeded":
                    warnings = p.setdefault("warnings", [])
                    msg = f"{step_name}: {detail}" if detail else step_name
                    if msg not in warnings:
                        warnings.append(msg)
                    break

        succeeded = [p for p in phases if p["status"] == "succeeded"]
        deployment["customStatus"]["completedPhases"] = len(succeeded)
        # Always update output.phases for the UI
        deployment["output"] = {
            "status": "running",
            "phases": phases,
            "resources": {"fabric_workspace_name": req.fabric_workspace_name, "resource_group_name": req.resource_group_name},
        }
        # Keep totalPhases at 12 (fixed) — don't shrink to len(phases)
        deployment["lastUpdatedTime"] = now_iso()
        save_state()

    try:
        from activities.invoke_powershell import run_deploy

        config = req.model_dump()
        deploy_start = datetime.now(timezone.utc)
        deployment["customStatus"]["currentPhase"] = "Starting Deploy-All.ps1"
        deployment["customStatus"]["status"] = "running"
        save_state()

        logger.info("Starting Deploy-All.ps1 for workspace='%s', rg='%s'",
                     req.fabric_workspace_name, req.resource_group_name)

        def pid_callback(pid: int):
            active_processes[instance_id] = pid
            logger.info("Deploy subprocess PID: %d", pid)

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_deploy, config, step_callback, pid_callback)

        # Clean up PID tracking
        active_processes.pop(instance_id, None)

        deployment["runtimeStatus"] = "Completed"
        deployment["customStatus"]["status"] = "succeeded"
        deployment["customStatus"]["currentPhase"] = "Deployment Complete"
        completed = [p for p in phases if p["status"] == "succeeded"]
        deployment["customStatus"]["completedPhases"] = len(completed)
        deployment["customStatus"]["resources"] = result.get("resources", {})
        duration = result.get("duration_seconds", (datetime.now(timezone.utc) - deploy_start).total_seconds())
        deployment["customStatus"]["durationSeconds"] = round(duration, 1)
        deployment["output"] = {
            "status": "succeeded",
            "phases": phases,
            "resources": result.get("resources", {}),
        }
        logger.info("Deployment %s completed (%.1fs)", instance_id, duration)

        # Auto-pause Fabric capacity if requested
        if req.pause_capacity_after_deploy and req.capacity_name:
            try:
                _pause_capacity_sync(
                    req.capacity_subscription_id,
                    req.capacity_resource_group,
                    req.capacity_name,
                )
                deployment["customStatus"]["capacityPaused"] = True
                logger.info("Auto-paused capacity '%s' after deployment", req.capacity_name)
            except Exception as e:
                logger.warning("Failed to auto-pause capacity '%s': %s", req.capacity_name, e)
                deployment["customStatus"]["capacityPauseError"] = str(e)

    except Exception as e:
        logger.error("Deployment %s failed: %s", instance_id, e)
        duration = (datetime.now(timezone.utc) - deploy_start).total_seconds()
        deployment["runtimeStatus"] = "Failed"
        deployment["customStatus"]["status"] = "failed"
        deployment["customStatus"]["detail"] = str(e)
        deployment["customStatus"]["durationSeconds"] = round(duration, 1)
        deployment["output"] = {
            "status": "failed",
            "phases": phases if phases else [{"phase": "Deploy-All", "status": "failed", "detail": str(e)}],
            "resources": {},
        }

    finally:
        _logging.getLogger("activities.invoke_powershell").removeHandler(handler)
        deployment["lastUpdatedTime"] = now_iso()
        save_state()


@app.get("/api/deploy/{instance_id}/status")
async def get_status(instance_id: str):
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")
    return deployments[instance_id]


@app.get("/api/deploy/{instance_id}/deployed-resources")
async def get_deployed_resources(instance_id: str):
    """Query Azure & Fabric APIs to list resources that actually exist for this deployment."""
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")

    dep = deployments[instance_id]
    ws_name = dep.get("customStatus", {}).get("workspaceName", "")
    rg_name = dep.get("customStatus", {}).get("resourceGroupName", "")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _get_deployed_resources_sync, ws_name, rg_name)
    return result


def _get_deployed_resources_sync(ws_name: str, rg_name: str) -> dict:
    """Check Azure + Fabric for resources that actually exist."""
    result: dict = {"azure": [], "fabric": [], "workspace": None}

    # ── Azure resources in the resource group ──
    if rg_name:
        try:
            proc = _az_run(
                ["az", "group", "show", "--name", rg_name, "--query", "id", "-o", "tsv"],
            )
            if proc.returncode == 0 and proc.stdout.strip():
                # RG exists — list resources
                res_proc = _az_run(
                    ["az", "resource", "list", "-g", rg_name,
                     "--query", "[].{name:name, type:type, location:location, id:id}",
                     "-o", "json"], check=True,
                )
                resources = json.loads(res_proc.stdout)
                for r in resources:
                    short_type = r["type"].split("/")[-1]
                    result["azure"].append({
                        "name": r["name"],
                        "type": short_type,
                        "fullType": r["type"],
                        "location": r.get("location", ""),
                        "id": r.get("id", ""),
                    })
                logger.info("Found %d Azure resources in RG '%s'", len(resources), rg_name)
        except Exception as e:
            logger.warning("Failed to query Azure RG '%s': %s", rg_name, e)

    # ── Fabric workspace + items ──
    if ws_name:
        try:
            from shared.fabric_client import FabricClient
            fabric = FabricClient()

            ws_result = fabric.call("GET", "/workspaces")
            workspaces = ws_result.get("value", []) if ws_result else []
            ws_match = next((w for w in workspaces if w.get("displayName") == ws_name), None)

            if ws_match:
                ws_id = ws_match["id"]
                result["workspace"] = {
                    "name": ws_name,
                    "id": ws_id,
                    "url": f"https://app.fabric.microsoft.com/groups/{ws_id}",
                }
                items = fabric.list_items(ws_id)
                for item in items:
                    result["fabric"].append({
                        "name": item.get("displayName", ""),
                        "type": item.get("type", "Unknown"),
                        "id": item.get("id", ""),
                    })
                logger.info("Found %d Fabric items in workspace '%s'", len(items), ws_name)
        except Exception as e:
            logger.warning("Failed to query Fabric workspace '%s': %s", ws_name, e)

    return result


@app.post("/api/deploy/{instance_id}/resume-hds")
async def resume_hds(instance_id: str):
    return {"message": "HDS resume acknowledged"}


@app.post("/api/deploy/{instance_id}/cancel")
async def cancel(instance_id: str):
    if instance_id in deployments:
        dep = deployments[instance_id]
        dep["runtimeStatus"] = "Terminated"
        dep["customStatus"]["status"] = "cancelled"
        dep["customStatus"]["detail"] = "Cancelled by user"
        # Compute duration from createdTime
        if dep.get("createdTime"):
            created = datetime.fromisoformat(dep["createdTime"])
            duration = (datetime.now(timezone.utc) - created).total_seconds()
            dep["customStatus"]["durationSeconds"] = round(duration, 1)
        save_state()
        logger.info("Deployment %s cancelled by user", instance_id)

        # Kill the subprocess if it's still running
        pid = active_processes.pop(instance_id, None)
        if pid:
            import signal
            try:
                import os
                if sys.platform == "win32":
                    os.system(f"taskkill /F /T /PID {pid}")
                else:
                    os.kill(pid, signal.SIGTERM)
                logger.info("Killed subprocess PID %d for %s", pid, instance_id)
            except Exception as e:
                logger.warning("Failed to kill PID %d: %s", pid, e)

    return {"message": "Cancelled"}


@app.get("/api/deployments")
async def list_deployments_api():
    return list(deployments.values())


@app.delete("/api/deploy/{instance_id}")
async def delete_deployment_endpoint(instance_id: str):
    """Remove a deployment from history."""
    if instance_id in deployments:
        del deployments[instance_id]
    if db_delete_deployment(instance_id):
        logger.info("Deployment %s removed from history", instance_id)
        return {"message": "Deleted"}
    raise HTTPException(404, "Instance not found")


@app.post("/api/deployments/clear")
async def clear_all_deployments_endpoint():
    """Clear all deployment history."""
    count = db_clear_all()
    deployments.clear()
    logger.info("Cleared %d deployments from history", count)
    return {"message": f"Cleared {count} deployments"}


# ── Lock API (persisted in SQLite) ─────────────────────────────────────

@app.get("/api/locks")
async def get_locks_endpoint():
    return get_locks()


@app.post("/api/locks/{resource_id:path}")
async def set_lock_endpoint(resource_id: str, name: str = "", resource_type: str = ""):
    set_lock(resource_id, name, resource_type)
    return {"message": "Locked"}


@app.delete("/api/locks/{resource_id:path}")
async def remove_lock_endpoint(resource_id: str):
    remove_lock(resource_id)
    return {"message": "Unlocked"}


# ── Form History API ───────────────────────────────────────────────────

@app.get("/api/form-history/{field}")
async def get_form_history_endpoint(field: str):
    return get_form_history(field)


@app.post("/api/form-history/{field}")
async def add_form_history_endpoint(field: str, value: str):
    add_form_history(field, value)
    return {"message": "Saved"}


# ── Dismissed Teardowns API ────────────────────────────────────────────

@app.get("/api/dismissed-teardowns")
async def get_dismissed_endpoint():
    return get_dismissed_teardowns()


@app.post("/api/dismissed-teardowns/{instance_id}")
async def dismiss_teardown_endpoint(instance_id: str):
    dismiss_teardown(instance_id)
    return {"message": "Dismissed"}


@app.get("/api/scan/subscriptions")
async def list_subscriptions():
    """List Azure subscriptions available to the current user."""
    try:
        result = _az_run(
            ["az", "account", "list", "--query", "[].{id:id, name:name, isDefault:isDefault}", "-o", "json"],
            check=True,
        )
        subs = json.loads(result.stdout)
        # Sort so default subscription comes first
        subs.sort(key=lambda s: not s.get("isDefault", False))
        return [{"id": s["id"], "name": s["name"]} for s in subs]
    except Exception as e:
        logger.error("Failed to list subscriptions: %s", e)
        return []


@app.get("/api/scan/resources")
async def scan_resources(subscription_id: str = ""):
    """Scan for teardown candidates across Fabric and Azure."""
    loop = asyncio.get_event_loop()
    candidates = await loop.run_in_executor(None, _scan_resources_sync, subscription_id)
    return candidates


@app.post("/api/scan/resources/start")
async def start_scan_resources(subscription_id: str = ""):
    """Start an incremental teardown scan and return a scan id for polling."""
    scan_id = str(uuid.uuid4())
    scan_jobs[scan_id] = {
        "scanId": scan_id,
        "status": "running",
        "phase": "starting",
        "message": "Starting teardown scan...",
        "subscriptionId": subscription_id,
        "candidates": [],
        "counts": {"fabric": 0, "azure": 0, "spn": 0},
        "startedAt": datetime.now(timezone.utc).isoformat(),
        "completedAt": None,
        "error": "",
    }
    asyncio.create_task(_run_scan_job(scan_id, subscription_id))
    return {"scanId": scan_id}


@app.get("/api/scan/resources/{scan_id}")
async def get_scan_resources(scan_id: str):
    """Get incremental teardown scan state."""
    job = scan_jobs.get(scan_id)
    if not job:
        return {
            "scanId": scan_id,
            "status": "missing",
            "phase": "missing",
            "message": "Scan job not found. It may have expired after a backend restart.",
            "subscriptionId": "",
            "candidates": [],
            "counts": {"fabric": 0, "azure": 0, "spn": 0},
            "startedAt": None,
            "completedAt": datetime.now(timezone.utc).isoformat(),
            "error": "Scan job not found",
        }
    return job


def _scan_resources_sync(subscription_id: str, progress_callback=None, status_callback=None) -> list:
    candidates = []

    def emit_status(phase: str, message: str):
        if status_callback:
            status_callback(phase, message)

    def emit_candidate(candidate: dict, phase: str, message: str):
        existing_index = next(
            (index for index, existing in enumerate(candidates) if existing.get("id") == candidate.get("id")),
            None,
        )
        if existing_index is None:
            candidates.append(candidate)
        else:
            candidates[existing_index] = candidate
        if progress_callback:
            progress_callback(candidates, phase, message)

    # ── Scan Fabric workspaces ─────────────────────────────────────
    try:
        emit_status("fabric", "Scanning Fabric workspaces...")
        from shared.fabric_client import FabricClient
        fabric = FabricClient()

        ws_result = fabric.call("GET", "/workspaces")
        workspaces = ws_result.get("value", []) if ws_result else []

        for ws in workspaces:
            name = ws.get("displayName", "")
            ws_id = ws.get("id", "")

            try:
                items = fabric.list_items(ws_id)
                item_count = len(items)
                item_types: dict[str, int] = {}
                for item in items:
                    t = item.get("type", "Unknown")
                    item_types[t] = item_types.get(t, 0) + 1

                # Criterion 1: HDS deployed
                has_hds = any(i.get("type") == "Healthcaredatasolution" for i in items)

                # Skip workspaces with no HDS at all — they're not our deployments
                if not has_hds:
                    continue

                # Criterion 2: MasimoEventhouse present
                eventhouse_item = next(
                    (i for i in items
                     if i.get("type") == "Eventhouse" and "masimo" in i.get("displayName", "").lower()),
                    None,
                )
                has_eventhouse = eventhouse_item is not None

                # Build artifact list early so the UI can show the workspace immediately
                artifact_list = []
                for t in sorted(item_types.keys()):
                    count = item_types[t]
                    if count > 3:
                        artifact_list.append(f"{t}: (×{count})")
                    else:
                        matching_names = [i.get("displayName", "") for i in items if i.get("type") == t]
                        artifact_list.append(f"{t}: {', '.join(matching_names)}")

                provisional_missing = []
                if not has_eventhouse:
                    provisional_missing.append("MasimoEventhouse")

                provisional_candidate = {
                    "type": "fabric",
                    "name": name,
                    "id": ws_id,
                    "status": "partial",
                    "detail": (
                        f"Discovered workspace — checking fn_ClinicalAlerts ({item_count} Fabric items)"
                        if has_eventhouse
                        else f"Partial deployment — missing: {', '.join(provisional_missing)}"
                    ),
                    "resourceCount": item_count,
                    "expectedCount": item_count,
                    "matchedArtifacts": artifact_list,
                    "qualified": False,
                    "detectedArtifacts": {
                        "hasHDS": has_hds,
                        "hasEventhouse": has_eventhouse,
                        "hasFnClinicalAlerts": False,
                    },
                }
                emit_candidate(provisional_candidate, "fabric", f"Discovered Fabric workspace: {name}")

                # Criterion 3: fn_ClinicalAlerts exists in MasimoKQLDB
                has_fn_clinical_alerts = False
                if has_eventhouse and eventhouse_item:
                    try:
                        kql_db_item = next(
                            (i for i in items if i.get("type") == "KQLDatabase"),
                            None,
                        )
                        if kql_db_item:
                            db_detail = fabric.call(
                                "GET",
                                f"/workspaces/{ws_id}/kqlDatabases/{kql_db_item['id']}",
                            )
                            kusto_uri = ""
                            if db_detail:
                                props = db_detail.get("properties", {})
                                kusto_uri = props.get("queryServiceUri", "") or props.get("kustoUri", "")
                            if kusto_uri:
                                from shared.kusto_client import KustoClient
                                kusto = KustoClient(kusto_uri, kql_db_item.get("displayName", "MasimoKQLDB"))
                                rows = kusto.execute_query(".show functions | where Name == 'fn_ClinicalAlerts'")
                                has_fn_clinical_alerts = len(rows) > 0
                    except Exception as kql_e:
                        logger.warning("Could not check fn_ClinicalAlerts in '%s': %s", name, kql_e)

                # Fully qualified = all 3 criteria met
                qualified = has_hds and has_eventhouse and has_fn_clinical_alerts

                missing = []
                if not has_eventhouse:
                    missing.append("MasimoEventhouse")
                if not has_fn_clinical_alerts:
                    missing.append("fn_ClinicalAlerts")

                if qualified:
                    detail = f"Full deployment — {item_count} Fabric items"
                else:
                    detail = f"Partial deployment — missing: {', '.join(missing)}"

                status = "full" if qualified else "partial"

                candidate = {
                    "type": "fabric",
                    "name": name,
                    "id": ws_id,
                    "status": status,
                    "detail": detail,
                    "resourceCount": item_count,
                    "expectedCount": item_count,
                    "matchedArtifacts": artifact_list,
                    "qualified": qualified,
                    "detectedArtifacts": {
                        "hasHDS": has_hds,
                        "hasEventhouse": has_eventhouse,
                        "hasFnClinicalAlerts": has_fn_clinical_alerts,
                    },
                }
                emit_candidate(candidate, "fabric", f"Discovered Fabric workspace: {name}")
                logger.info(
                    "Workspace '%s' — qualified=%s (HDS=%s, Eventhouse=%s, fn_ClinicalAlerts=%s)",
                    name, qualified, has_hds, has_eventhouse, has_fn_clinical_alerts,
                )
            except Exception as e:
                logger.warning("Failed to scan workspace '%s': %s", name, e)
    except Exception as e:
        logger.error("Fabric scan failed: %s", e)

    # ── Scan Azure resource groups ─────────────────────────────────
    try:
        emit_status("azure", "Scanning Azure resource groups...")
        import subprocess, sys

        sub_arg = ["--subscription", subscription_id] if subscription_id else []
        result = _az_run(
            ["az", "group", "list", "--query",
             "[?starts_with(name, 'rg-med') || starts_with(name, 'rg-medtech')].{name:name, id:id}",
             "-o", "json"] + sub_arg,
            check=True,
        )
        rgs = json.loads(result.stdout)

        for rg in rgs:
            rg_name = rg["name"]
            try:
                res_result = _az_run(
                    ["az", "resource", "list", "-g", rg_name,
                     "--query", "[].{name:name, type:type}", "-o", "json"] + sub_arg,
                    check=True,
                )
                resources = json.loads(res_result.stdout)
                res_count = len(resources)

                artifact_list = [f"{r['type'].split('/')[-1]}: {r['name']}" for r in resources]
                status = "full" if res_count >= 10 else "partial"

                candidate = {
                    "type": "azure",
                    "name": rg_name,
                    "id": rg.get("id", ""),
                    "status": status,
                    "detail": f"{'Full' if status == 'full' else 'Partial'} Azure deployment — {res_count} resources",
                    "resourceCount": res_count,
                    "expectedCount": 12,
                    "matchedArtifacts": artifact_list,
                    "subscription": subscription_id,
                }
                emit_candidate(candidate, "azure", f"Discovered Azure resource group: {rg_name}")
            except Exception as e:
                logger.warning("Failed to scan RG '%s': %s", rg_name, e)
    except Exception as e:
        logger.error("Azure scan failed: %s", e)

    # ── Scan for SPNs matching workspace names ─────────────────────
    emit_status("spn", "Scanning Entra workspace identities...")
    fabric_names = {c["name"] for c in candidates if c["type"] == "fabric"}
    seen_spn_ids: set = set()
    for ws_name in fabric_names:
        try:
            import subprocess, sys
            result = _az_run(
                ["az", "ad", "sp", "list", "--display-name", ws_name,
                 "--query", "[].{appId:appId, displayName:displayName, id:id}", "-o", "json"],
                check=True,
            )
            spns = json.loads(result.stdout)
            for spn in spns:
                spn_id = spn.get("id", "")
                if spn_id in seen_spn_ids:
                    continue  # Deduplicate
                seen_spn_ids.add(spn_id)

                # Check if matching workspace still exists
                ws_exists = spn.get("displayName", "") in fabric_names
                status = "active" if ws_exists else "orphaned"

                candidate = {
                    "type": "spn",
                    "name": spn.get("displayName", ws_name),
                    "id": spn_id,
                    "status": status,
                    "detail": f"Workspace identity SPN ({'workspace exists' if ws_exists else 'workspace deleted'}) — appId: {spn.get('appId', 'unknown')}",
                    "matchedArtifacts": [f"App Registration: {spn.get('displayName', '')} (appId: {spn.get('appId', '')})"],
                }
                emit_candidate(candidate, "spn", f"Discovered Entra identity: {candidate['name']}")
        except Exception:
            pass

    emit_status("complete", f"Scan complete — {len(candidates)} candidates discovered")
    return candidates


# ── Fabric Capacity API ────────────────────────────────────────────────

@app.get("/api/scan/capacities")
async def list_capacities(subscription_id: str = ""):
    """List Fabric capacities in the subscription."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _list_capacities_sync, subscription_id)
    return result


def _list_capacities_sync(subscription_id: str) -> list:
    """Query az fabric capacity list."""
    try:
        sub_arg = ["--subscription", subscription_id] if subscription_id else []
        proc = _az_run(
            ["az", "fabric", "capacity", "list",
             "--query", "[].{name:name, id:id, state:state, sku:sku.name, resourceGroup:resourceGroup, location:location}",
             "-o", "json"] + sub_arg,
            check=True,
        )
        capacities = json.loads(proc.stdout)
        return [
            {
                "name": c["name"],
                "id": c.get("id", ""),
                "state": c.get("state", "Unknown"),
                "sku": c.get("sku", ""),
                "resourceGroup": c.get("resourceGroup", ""),
                "location": c.get("location", ""),
                "subscription": subscription_id,
            }
            for c in capacities
        ]
    except Exception as e:
        logger.warning("Failed to list Fabric capacities: %s", e)
        return []


@app.post("/api/capacity/pause")
async def pause_capacity(subscription_id: str, resource_group: str, name: str):
    """Pause a Fabric capacity."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _pause_capacity_sync, subscription_id, resource_group, name)
    return {"message": f"Capacity '{name}' paused"}


def _pause_capacity_sync(subscription_id: str, resource_group: str, name: str):
    """Suspend a Fabric capacity via az CLI."""
    sub_arg = ["--subscription", subscription_id] if subscription_id else []
    proc = _az_run(
        ["az", "fabric", "capacity", "suspend",
         "--resource-group", resource_group,
         "--capacity-name", name] + sub_arg,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"az fabric capacity suspend failed: {proc.stderr.strip()}")
    logger.info("Paused capacity '%s' in RG '%s'", name, resource_group)


# ── Deployment-to-Capacity mapping lookup ──────────────────────────────

@app.get("/api/deployment-capacity/{rg_name}")
async def get_deployment_capacity(rg_name: str):
    """Look up which Fabric capacity was used for a given resource group."""
    for dep in deployments.values():
        cs = dep.get("customStatus", {})
        if cs.get("resourceGroupName") == rg_name and cs.get("capacityName"):
            return {
                "capacityName": cs["capacityName"],
                "capacityResourceGroup": cs.get("capacityResourceGroup", ""),
                "capacitySubscriptionId": cs.get("capacitySubscriptionId", ""),
                "workspaceName": cs.get("workspaceName", ""),
            }
    return None


@app.get("/api/deploy/check-existing")
async def check_existing_deployment(workspace_name: str = "", resource_group: str = ""):
    """Check if a deployment already exists and return its status + patient count from FHIR."""
    if not workspace_name and not resource_group:
        return None

    # Check deployment history for a successful run with this workspace/RG
    prior_deploy = None
    for dep in deployments.values():
        cs = dep.get("customStatus", {})
        if dep.get("runtimeStatus") != "Completed":
            continue
        if cs.get("status") != "succeeded":
            continue
        if dep.get("name") == "teardown_orchestrator":
            continue
        ws = cs.get("workspaceName", "")
        rg = cs.get("resourceGroupName", "")
        if (workspace_name and ws == workspace_name) or (resource_group and rg == resource_group):
            prior_deploy = dep
            # Don't break — keep looking for the most recent one

    if not prior_deploy:
        return None

    prior_cs = prior_deploy.get("customStatus", {})
    prior_config = prior_cs.get("deployConfig", {})
    result = {
        "found": True,
        "instanceId": prior_deploy.get("instanceId", ""),
        "createdTime": prior_deploy.get("createdTime", ""),
        "workspaceName": prior_cs.get("workspaceName", ""),
        "resourceGroupName": prior_cs.get("resourceGroupName", ""),
        "configuredPatientCount": prior_config.get("patient_count", 0),
        "fhirPatientCount": 0,
        "fhirDeviceCount": 0,
        "emulatorRunning": False,
        "azureRgExists": False,
        "priorConfig": prior_config,
    }

    # Check Azure RG existence
    rg_name = prior_cs.get("resourceGroupName", "")
    if rg_name:
        loop = asyncio.get_event_loop()
        rg_exists = await loop.run_in_executor(None, _check_rg_exists, rg_name)
        result["azureRgExists"] = rg_exists

        if rg_exists:
            # Check emulator status
            emu_state = await loop.run_in_executor(None, _check_emulator_status, rg_name)
            result["emulatorRunning"] = emu_state.get("running", False)
            result["emulatorDeviceCount"] = emu_state.get("deviceCount", 0)

            # Query FHIR for actual patient + device counts, and storage stats
            fhir_counts = await loop.run_in_executor(None, _query_fhir_counts, rg_name)
            result["fhirPatientCount"] = fhir_counts.get("patients", 0)
            result["fhirDeviceCount"] = fhir_counts.get("devices", 0)
            result["exportedFiles"] = fhir_counts.get("exportedFiles", 0)
            result["dicomStudies"] = fhir_counts.get("dicomStudies", 0)

    return result


def _check_rg_exists(rg_name: str) -> bool:
    cached = _get_cached(f"rg_exists:{rg_name}")
    if cached is not None:
        return cached
    try:
        proc = _az_run(["az", "group", "exists", "--name", rg_name])
        result = proc.stdout.strip().lower() == "true"
        _set_cached(f"rg_exists:{rg_name}", result)
        return result
    except Exception:
        return False


def _check_emulator_status(rg_name: str) -> dict:
    cached = _get_cached(f"emulator:{rg_name}")
    if cached is not None:
        return cached
    try:
        proc = _az_run([
            "az", "container", "show",
            "--resource-group", rg_name,
            "--name", "masimo-emulator-grp",
            "--query", "{state:instanceView.state, deviceCount:containers[0].environmentVariables[?name=='DEVICE_COUNT'].value | [0]}",
            "-o", "json",
        ])
        if proc.returncode == 0 and proc.stdout.strip():
            data = json.loads(proc.stdout)
            result = {
                "running": data.get("state") == "Running",
                "deviceCount": int(data.get("deviceCount", 100)),
            }
            _set_cached(f"emulator:{rg_name}", result)
            return result
    except Exception as e:
        logger.warning("Emulator status check failed: %s", e)
    return {"running": False, "deviceCount": 0}


def _query_fhir_counts(rg_name: str) -> dict:
    """Query FHIR service for actual patient and device counts, plus storage stats."""
    result = {"patients": 0, "devices": 0, "exportedFiles": 0, "dicomStudies": 0}
    try:
        # Find FHIR service from the RG using resource list (more reliable)
        proc = _az_run([
            "az", "resource", "list", "-g", rg_name,
            "--resource-type", "Microsoft.HealthcareApis/workspaces/fhirservices",
            "--query", "[0].name", "-o", "tsv",
        ])
        if proc.returncode != 0 or not proc.stdout.strip():
            logger.warning("FHIR resource not found in RG '%s' (exit=%d, out='%s')", rg_name, proc.returncode, proc.stdout[:200])
            return result
        fhir_name = proc.stdout.strip()  # e.g. "hdwsXXX/fhirXXX"
        logger.info("FHIR resource name: '%s'", fhir_name)
        parts = fhir_name.split("/")
        if len(parts) != 2:
            logger.warning("FHIR resource name '%s' doesn't match expected format 'workspace/service'", fhir_name)
            return result
        fhir_url = f"https://{parts[0]}-{parts[1]}.fhir.azurehealthcareapis.com"
        logger.info("FHIR URL: %s", fhir_url)

        # Get FHIR token using the service URL as the resource
        token_proc = _az_run([
            "az", "account", "get-access-token",
            "--resource", fhir_url,
            "--query", "accessToken", "-o", "tsv",
        ])
        if token_proc.returncode != 0 or not token_proc.stdout.strip():
            logger.warning("Failed to get FHIR token (exit=%d, stderr='%s')", token_proc.returncode, token_proc.stderr[:200] if token_proc.stderr else "")
            return result
        token = token_proc.stdout.strip()

        import requests as _requests
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/fhir+json"}

        # Count patients
        try:
            patient_resp = _requests.get(f"{fhir_url}/Patient?_summary=count", headers=headers, timeout=30)
            if patient_resp.ok:
                result["patients"] = patient_resp.json().get("total", 0)
                logger.info("FHIR patients: %d", result["patients"])
            else:
                logger.warning("FHIR Patient query failed: %d %s", patient_resp.status_code, patient_resp.text[:200])
        except Exception as e:
            logger.warning("FHIR Patient query exception: %s", e)

        # Count devices
        try:
            device_resp = _requests.get(f"{fhir_url}/Device?_summary=count", headers=headers, timeout=30)
            if device_resp.ok:
                result["devices"] = device_resp.json().get("total", 0)
                logger.info("FHIR devices: %d", result["devices"])
            else:
                logger.warning("FHIR Device query failed: %d %s", device_resp.status_code, device_resp.text[:200])
        except Exception as e:
            logger.warning("FHIR Device query exception: %s", e)

        # Count FHIR export files and DICOM studies in storage
        try:
            st_proc = _az_run([
                "az", "storage", "account", "list", "-g", rg_name,
                "--query", "[?kind=='StorageV2'].name | [0]", "-o", "tsv",
            ])
            if st_proc.returncode == 0 and st_proc.stdout.strip():
                st_name = st_proc.stdout.strip()
                logger.info("Storage account: %s", st_name)

                # Count fhir-export blobs
                export_proc = _az_run([
                    "az", "storage", "blob", "list",
                    "--container-name", "fhir-export",
                    "--account-name", st_name,
                    "--auth-mode", "login",
                    "--query", "length(@)", "-o", "tsv",
                ])
                if export_proc.returncode == 0 and export_proc.stdout.strip():
                    result["exportedFiles"] = int(export_proc.stdout.strip())

                # Count dicom-output blobs
                dicom_proc = _az_run([
                    "az", "storage", "blob", "list",
                    "--container-name", "dicom-output",
                    "--account-name", st_name,
                    "--auth-mode", "login",
                    "--query", "length(@)", "-o", "tsv",
                ])
                if dicom_proc.returncode == 0 and dicom_proc.stdout.strip():
                    result["dicomStudies"] = int(dicom_proc.stdout.strip())

                logger.info("Storage counts — exported: %d, DICOM: %d", result["exportedFiles"], result["dicomStudies"])
        except Exception as e:
            logger.warning("Storage count query failed: %s", e)

    except Exception as e:
        logger.warning("FHIR count query failed: %s", e)
    return result


if __name__ == "__main__":
    logger.info("Starting local dev server on http://localhost:7071")
    logger.info("This calls real Azure/Fabric APIs using your current az login credentials")
    uvicorn.run(app, host="0.0.0.0", port=7071, log_level="info", timeout_graceful_shutdown=3)
