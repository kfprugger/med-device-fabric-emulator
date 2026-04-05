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


def now_iso():
    return datetime.now(timezone.utc).isoformat()


@app.post("/api/teardown/start")
async def start_teardown(req: TeardownRequest):
    instance_id = f"local-{uuid.uuid4().hex[:8]}"

    deployment = {
        "instanceId": instance_id,
        "name": "teardown_orchestrator",
        "runtimeStatus": "Running",
        "createdTime": now_iso(),
        "lastUpdatedTime": now_iso(),
        "customStatus": {
            "currentPhase": "Teardown",
            "status": "running",
            "detail": "",
            "completedPhases": 0,
            "totalPhases": 1,
            "resources": {},
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

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_teardown, config)

        exit_code = result.get("exit_code", -1)
        if exit_code == 0:
            deployment["runtimeStatus"] = "Completed"
            deployment["customStatus"]["status"] = "succeeded"
            deployment["customStatus"]["currentPhase"] = "Teardown Complete"
            deployment["customStatus"]["completedPhases"] = 1
        else:
            deployment["runtimeStatus"] = "Failed"
            deployment["customStatus"]["status"] = "failed"

        deployment["output"] = {
            "status": "succeeded" if exit_code == 0 else "failed",
            "phases": [{"phase": "Teardown", "status": "succeeded" if exit_code == 0 else "failed", "duration": result.get("duration_seconds", 0)}],
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
    # Build descriptive instance ID: <phase>-<datetime>
    now_local = datetime.now()
    timestamp = now_local.strftime("%Y%m%d-%H%M%S")
    phase_label = "ALLPHASES"
    if req.skip_base_infra and req.skip_fhir and req.skip_dicom:
        phase_label = "FABRIC"
    elif req.skip_base_infra:
        phase_label = "PHASE2+"

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
                "azurePortal": f"https://portal.azure.com/#@/resource/subscriptions//resourceGroups/{req.resource_group_name}" if req.resource_group_name else "",
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
            })
            deployment["customStatus"]["logs"] = deploy_logs[-100:]
            deployment["customStatus"]["detail"] = msg
            deployment["lastUpdatedTime"] = now_iso()
            save_state()

    handler = StatusLogHandler()
    handler.setLevel(_logging.INFO)
    handler.setFormatter(_logging.Formatter("%(message)s"))

    _logging.getLogger("activities.invoke_powershell").addHandler(handler)

    phases: list[dict] = []

    def step_callback(event: str, step_name: str, detail: str, duration: str):
        """Handle step events from PowerShell output parser."""
        if event == "step_start":
            # Mark any previously running phase as succeeded (the result line
            # for the previous step may not have been parsed yet)
            for p in phases:
                if p["status"] == "running":
                    p["status"] = "succeeded"
            phases.append({"phase": step_name, "status": "running"})
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


@app.post("/api/locks/{resource_id}")
async def set_lock_endpoint(resource_id: str, name: str = "", resource_type: str = ""):
    set_lock(resource_id, name, resource_type)
    return {"message": "Locked"}


@app.delete("/api/locks/{resource_id}")
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


def _scan_resources_sync(subscription_id: str) -> list:
    candidates = []

    # ── Scan Fabric workspaces ─────────────────────────────────────
    try:
        from shared.fabric_client import FabricClient
        fabric = FabricClient()

        ws_result = fabric.call("GET", "/workspaces")
        workspaces = ws_result.get("value", []) if ws_result else []

        # Name patterns that indicate a deployment workspace
        name_patterns = ["med-device", "med-ui", "med-device-rti", "healthcare", "hds-", "masimo"]
        # Artifact signatures that confirm our deployment
        deployment_signatures = ["MasimoEventhouse", "MasimoKQLDB", "healthcare1_msft", "MasimoTelemetry"]

        for ws in workspaces:
            name = ws.get("displayName", "")
            ws_id = ws.get("id", "")

            # Quick name check first
            name_match = any(p in name.lower() for p in name_patterns)
            if not name_match:
                continue

            # Count items
            try:
                items = fabric.list_items(ws_id)
                item_count = len(items)
                item_types = {}
                item_names = []
                for item in items:
                    t = item.get("type", "Unknown")
                    item_types[t] = item_types.get(t, 0) + 1
                    item_names.append(item.get("displayName", ""))

                # Must have BOTH HDS and MasimoEventhouse to qualify
                has_hds = any(i.get("type") == "Healthcaredatasolution" for i in items)
                has_eventhouse = any(
                    i.get("type") == "Eventhouse" and "masimo" in i.get("displayName", "").lower()
                    for i in items
                )

                if not (has_hds and has_eventhouse):
                    logger.info(
                        "Workspace '%s' skipped — missing HDS=%s, MasimoEventhouse=%s",
                        name, has_hds, has_eventhouse,
                    )
                    continue

                artifact_list = [f"{t}: {c}" for t, c in sorted(item_types.items())]
                status = "full" if item_count > 20 else "partial"

                candidates.append({
                    "type": "fabric",
                    "name": name,
                    "id": ws_id,
                    "status": status,
                    "detail": f"{'Full' if status == 'full' else 'Partial'} deployment — {item_count} Fabric items",
                    "resourceCount": item_count,
                    "expectedCount": item_count,
                    "matchedArtifacts": artifact_list,
                })
            except Exception as e:
                logger.warning("Failed to scan workspace '%s': %s", name, e)
                candidates.append({
                    "type": "fabric",
                    "name": name,
                    "id": ws_id,
                    "status": "partial",
                    "detail": f"Workspace found but scan failed: {e}",
                    "resourceCount": 0,
                    "expectedCount": 0,
                    "matchedArtifacts": [],
                })
    except Exception as e:
        logger.error("Fabric scan failed: %s", e)

    # ── Scan Azure resource groups ─────────────────────────────────
    try:
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

                candidates.append({
                    "type": "azure",
                    "name": rg_name,
                    "id": rg.get("id", ""),
                    "status": status,
                    "detail": f"{'Full' if status == 'full' else 'Partial'} Azure deployment — {res_count} resources",
                    "resourceCount": res_count,
                    "expectedCount": 12,
                    "matchedArtifacts": artifact_list,
                    "subscription": subscription_id,
                })
            except Exception as e:
                logger.warning("Failed to scan RG '%s': %s", rg_name, e)
    except Exception as e:
        logger.error("Azure scan failed: %s", e)

    # ── Scan for SPNs matching workspace names ─────────────────────
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

                candidates.append({
                    "type": "spn",
                    "name": spn.get("displayName", ws_name),
                    "id": spn_id,
                    "status": status,
                    "detail": f"Workspace identity SPN ({'workspace exists' if ws_exists else 'workspace deleted'}) — appId: {spn.get('appId', 'unknown')}",
                    "matchedArtifacts": [f"App Registration: {spn.get('displayName', '')} (appId: {spn.get('appId', '')})"],
                })
        except Exception:
            pass

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


if __name__ == "__main__":
    logger.info("Starting local dev server on http://localhost:7071")
    logger.info("This calls real Azure/Fabric APIs using your current az login credentials")
    uvicorn.run(app, host="0.0.0.0", port=7071, log_level="info")
