"""Local development server — lightweight FastAPI replacement for Durable Functions.

Calls the same activity modules directly without the Durable Functions framework.
Used for local testing only. In production, the Durable Functions app handles
orchestration with checkpointing, retries, and human interaction gates.

Usage:
    cd orchestrator
    .venv\\Scripts\\activate
    python local_server.py
"""

import atexit
import faulthandler
import os
import signal
import traceback
import asyncio
import json
import logging
import subprocess
import sys
import re
import time
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Add orchestrator to path so activity imports work
sys.path.insert(0, str(Path(__file__).parent))
from shared.policy_tags import normalize_policy_tags
from shared.teardown_scan import live_fabric_workspaces_for_teardown

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

# ── Crash diagnostics — preserve evidence before process exit ──────────
CRASH_DUMP_FILE = Path(__file__).parent / "backend-crash-dump.log"
_CRASH_DUMP_HANDLE = None
_PREVIOUS_SIGNAL_HANDLERS: dict[int, object] = {}


def _log_thread_stacks(reason: str) -> None:
    frames = sys._current_frames()
    for thread in threading.enumerate():
        frame = frames.get(thread.ident)
        if frame is None:
            logger.critical(
                "STACK SNAPSHOT [%s] thread=%s ident=%s unavailable",
                reason,
                thread.name,
                thread.ident,
            )
            continue
        logger.critical(
            "STACK SNAPSHOT [%s] thread=%s ident=%s\n%s",
            reason,
            thread.name,
            thread.ident,
            "".join(traceback.format_stack(frame)),
        )


def _unhandled_exception(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    logger.critical(
        "UNHANDLED EXCEPTION — server crashing",
        exc_info=(exc_type, exc_value, exc_tb),
    )
    _log_thread_stacks("unhandled exception")


def _unhandled_thread_exception(args: threading.ExceptHookArgs) -> None:
    logger.critical(
        "UNHANDLED THREAD EXCEPTION — thread=%s",
        args.thread.name if args.thread else "unknown",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )
    _log_thread_stacks("unhandled thread exception")


def _handle_process_signal(signum, frame):
    try:
        signal_name = signal.Signals(signum).name
    except ValueError:
        signal_name = f"signal {signum}"
    logger.critical("BACKEND RECEIVED %s (%s); dumping stacks before exit", signal_name, signum)
    _log_thread_stacks(signal_name)
    logging.shutdown()

    previous = _PREVIOUS_SIGNAL_HANDLERS.get(signum, signal.SIG_DFL)
    if previous == signal.SIG_IGN:
        return
    if callable(previous):
        previous(signum, frame)
        return
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


def _install_process_crash_diagnostics() -> None:
    global _CRASH_DUMP_HANDLE
    sys.excepthook = _unhandled_exception
    threading.excepthook = _unhandled_thread_exception

    if _CRASH_DUMP_HANDLE is None:
        _CRASH_DUMP_HANDLE = CRASH_DUMP_FILE.open("a", encoding="utf-8", buffering=1)
        _CRASH_DUMP_HANDLE.write(f"\n--- backend crash diagnostics armed pid={os.getpid()} ---\n")
        faulthandler.enable(file=_CRASH_DUMP_HANDLE, all_threads=True)
        sigusr1 = getattr(signal, "SIGUSR1", None)
        if sigusr1 is not None:
            faulthandler.register(sigusr1, file=_CRASH_DUMP_HANDLE, all_threads=True, chain=False)
        atexit.register(_CRASH_DUMP_HANDLE.close)

    handled_signals = tuple(
        sig for sig in (
            getattr(signal, "SIGTERM", None),
            getattr(signal, "SIGINT", None),
            getattr(signal, "SIGHUP", None),
            getattr(signal, "SIGQUIT", None),
        )
        if sig is not None
    )
    for sig in handled_signals:
        if sig not in _PREVIOUS_SIGNAL_HANDLERS:
            _PREVIOUS_SIGNAL_HANDLERS[sig] = signal.getsignal(sig)
            signal.signal(sig, _handle_process_signal)


def _install_asyncio_crash_diagnostics() -> None:
    loop = asyncio.get_running_loop()

    def handle_asyncio_exception(active_loop, context):
        message = context.get("message", "Unhandled asyncio exception")
        exception = context.get("exception")
        if exception is not None:
            logger.critical(
                "UNHANDLED ASYNCIO EXCEPTION — %s",
                message,
                exc_info=(type(exception), exception, exception.__traceback__),
            )
        else:
            logger.critical("UNHANDLED ASYNCIO ERROR — %s; context=%r", message, context)
        _log_thread_stacks("unhandled asyncio exception")

    loop.set_exception_handler(handle_asyncio_exception)
    logger.info("Crash diagnostics armed: faulthandler=%s asyncio_loop=%s", CRASH_DUMP_FILE, id(loop))



def _create_logged_task(coro, *, name: str) -> asyncio.Task:
    """Create a background task that logs exceptions immediately with stack context."""
    task = asyncio.create_task(coro, name=name)

    def log_task_result(completed: asyncio.Task) -> None:
        if completed.cancelled():
            logger.warning("BACKGROUND TASK CANCELLED — %s", completed.get_name())
            return
        try:
            completed.result()
        except BaseException:
            logger.critical("BACKGROUND TASK FAILED — %s", completed.get_name(), exc_info=True)
            _log_thread_stacks(f"background task failed: {completed.get_name()}")

    task.add_done_callback(log_task_result)
    return task

_install_process_crash_diagnostics()

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


# ── Global exception handler — log unhandled route errors ──────────────
from fastapi import Request
from fastapi.responses import JSONResponse


@app.exception_handler(Exception)
async def _global_exception_handler(request: Request, exc: Exception):
    logger.critical(
        "UNHANDLED ROUTE EXCEPTION — %s %s",
        request.method,
        request.url.path,
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    _log_thread_stacks(f"route exception: {request.method} {request.url.path}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error. Check backend logs for details."},
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

CACHE_FILE = Path(__file__).parent / ".orchestrator-cache.json"

# Cache for resource scan results to avoid redundant Azure/Fabric API calls
# Used by both teardown scanner and deployment check-existing endpoint
_scan_cache: dict[str, dict] = {}  # key → {result, timestamp}
_SCAN_CACHE_TTL = 120  # seconds

# General short-lived backend cache for expensive CLI/tooling reads.
_timed_cache: dict[str, dict] = {}  # key → {result, timestamp}
_CACHE_LOCK = threading.RLock()


def _load_persistent_cache():
    global _scan_cache, _timed_cache
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
                _scan_cache = data.get("scan_cache", {})
                _timed_cache = data.get("timed_cache", {})
                logger.info("Loaded persistent cache from %s", CACHE_FILE)
        except Exception as e:
            logger.warning("Failed to load persistent cache: %s", e)
            _scan_cache = {}
            _timed_cache = {}


def _save_persistent_cache():
    try:
        with _CACHE_LOCK:
            temp_file = CACHE_FILE.with_suffix(".tmp")
            payload = {
                "scan_cache": dict(_scan_cache),
                "timed_cache": dict(_timed_cache),
            }
            with open(temp_file, "w") as f:
                json.dump(payload, f, indent=2)
            temp_file.replace(CACHE_FILE)
    except Exception as e:
        logger.warning("Failed to save persistent cache: %s", e)


_load_persistent_cache()

# In-memory cache for active deployments (for real-time log streaming)
deployments: dict[str, dict] = {}
for dep in db_list_deployments():
    deployments[dep["instanceId"]] = dep
logger.info("Loaded %d deployments from database", len(deployments))

def _mark_teardown_phase(phases: list[dict], phase_name: str, status: str) -> None:
    for phase in phases:
        if phase.get("phase") == phase_name:
            phase["status"] = status
            return
    phases.append({"phase": phase_name, "status": status})


def _reconcile_teardown(dep: dict) -> bool:
    """Refresh interrupted teardown state from live Azure/Fabric resources."""
    cs = dep.get("customStatus") or {}
    if cs.get("runType") != "teardown":
        return False
    if dep.get("runtimeStatus") not in {"Running", "Failed", "Terminated"}:
        return False

    rg_name = cs.get("resourceGroupName") or ""
    ws_name = cs.get("workspaceName") or ""
    output = dep.get("output") or {"status": "running", "phases": [], "resources": {}}
    phases = output.setdefault("phases", [])
    logs = cs.setdefault("logs", [])
    changed = False

    def add_log(level: str, message: str) -> None:
        logs.append({"timestamp": datetime.now(timezone.utc).isoformat(), "level": level, "message": message})
        cs["logs"] = logs[-200:]

    if ws_name:
        try:
            from shared.fabric_client import FabricClient
            fabric = FabricClient()
            ws_exists = fabric.find_workspace(ws_name) is not None
            if not ws_exists:
                _mark_teardown_phase(phases, "Workspace Identity", "succeeded")
                _mark_teardown_phase(phases, "Delete Workspace", "succeeded")
                changed = True
        except Exception as ex:
            add_log("warn", f"Workspace reconciliation skipped: {ex}")
            changed = True

    if rg_name:
        exists = _az_run(["az", "group", "exists", "--name", rg_name])
        if exists.stdout.strip().lower() == "false":
            _mark_teardown_phase(phases, "Azure Resource Group", "succeeded")
            dep["runtimeStatus"] = "Completed"
            cs["status"] = "succeeded"
            cs["currentPhase"] = "Teardown Complete"
            cs["cloudStatus"] = "deleted"
            cs["detail"] = f"✓ Azure RG '{rg_name}' fully deleted"
            output["status"] = "succeeded"
            changed = True
        else:
            show = _az_run(["az", "group", "show", "--name", rg_name, "--query", "properties.provisioningState", "-o", "tsv"])
            state = (show.stdout or "").strip() or "Unknown"
            if state.lower() == "deleting":
                _mark_teardown_phase(phases, "Azure Resource Group", "running")
                dep["runtimeStatus"] = "Running"
                cs["status"] = "deleting"
                cs["cloudStatus"] = "deleting"
                cs["currentPhase"] = "Azure Resource Group"
                cs["detail"] = f"Azure RG '{rg_name}' is still deleting in Azure"
                output["status"] = "running"
                changed = True

    completed = sum(1 for phase in phases if phase.get("status") in {"succeeded", "skipped"})
    cs["completedPhases"] = completed
    cs["totalPhases"] = max(cs.get("totalPhases") or 0, len(phases))
    dep["output"] = output
    dep["lastUpdatedTime"] = datetime.now(timezone.utc).isoformat()
    if changed:
        add_log("info", "Teardown state reconciled from live cloud resources")
    return changed


def reconcile_interrupted_teardowns() -> int:
    count = 0
    for dep in deployments.values():
        if _reconcile_teardown(dep):
            count += 1
    if count:
        save_state()
        logger.info("Reconciled %d interrupted teardown(s)", count)
    return count



# Track active subprocess PIDs for cancellation
active_processes: dict[str, int] = {}  # instance_id → PID

# Track active teardown scans for incremental UI updates
scan_jobs: dict[str, dict] = {}


def _get_timed_cached(key: str, ttl_seconds: int):
    entry = _timed_cache.get(key)
    if entry and (datetime.now(timezone.utc).timestamp() - entry["timestamp"]) < ttl_seconds:
        return entry["result"]
    return None



def _get_stale_timed_cached(key: str):
    entry = _timed_cache.get(key)
    return entry.get("result") if entry else None

def _set_timed_cached(key: str, result):
    with _CACHE_LOCK:
        _timed_cache[key] = {"result": result, "timestamp": datetime.now(timezone.utc).timestamp()}
    _save_persistent_cache()


def _get_cached(key: str) -> dict | None:
    entry = _scan_cache.get(key)
    if entry and (datetime.now(timezone.utc).timestamp() - entry["timestamp"]) < _SCAN_CACHE_TTL:
        return entry["result"]
    return None


def _set_cached(key: str, result: dict):
    with _CACHE_LOCK:
        _scan_cache[key] = {"result": result, "timestamp": datetime.now(timezone.utc).timestamp()}
    _save_persistent_cache()


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


@app.on_event("startup")
async def schedule_interrupted_teardown_reconciliation():
    """Reconcile interrupted teardowns after Uvicorn is already accepting requests."""
    _install_asyncio_crash_diagnostics()
    async def run_reconciliation():
        try:
            await asyncio.to_thread(reconcile_interrupted_teardowns)
        except Exception:
            logger.exception("Interrupted teardown reconciliation failed")

    _create_logged_task(run_reconciliation(), name="startup-interrupted-teardown-reconciliation")


def _get_auth_context_sync() -> dict:
    """Inspect local Azure CLI and Az PowerShell auth/tooling context."""
    from concurrent.futures import ThreadPoolExecutor

    def get_cli():
        cli = {
            "installed": False,
            "loggedIn": False,
            "user": "",
            "subscriptionName": "",
            "subscriptionId": "",
            "tenantId": "",
            "error": "",
        }
        try:
            ver = _az_run(["az", "version", "-o", "json"])
            cli["installed"] = ver.returncode == 0
            if ver.returncode == 0:
                acct = _az_run([
                    "az", "account", "show",
                    "--query", "{user:user.name, subscriptionName:name, subscriptionId:id, tenantId:tenantId}",
                    "-o", "json",
                ])
                if acct.returncode == 0 and acct.stdout.strip():
                    data = json.loads(acct.stdout)
                    cli["loggedIn"] = bool(data.get("subscriptionId"))
                    cli["user"] = data.get("user", "") or ""
                    cli["subscriptionName"] = data.get("subscriptionName", "") or ""
                    cli["subscriptionId"] = data.get("subscriptionId", "") or ""
                    cli["tenantId"] = data.get("tenantId", "") or ""
                else:
                    cli["error"] = (acct.stderr or "Not logged in to Azure CLI").strip()[:400]
            else:
                cli["error"] = (ver.stderr or "Azure CLI not installed").strip()[:400]
        except Exception as e:
            cli["error"] = str(e)[:400]
        return cli

    def get_pwsh():
        pwsh = {
            "installed": False,
            "loggedIn": False,
            "user": "",
            "subscriptionName": "",
            "subscriptionId": "",
            "tenantId": "",
            "error": "",
        }
        ps_cmd = (
            "$ErrorActionPreference='Stop'; "
            "if (-not (Get-Module -ListAvailable -Name Az.Accounts)) { "
            "  [PSCustomObject]@{installed=$false;loggedIn=$false;user='';subscriptionName='';subscriptionId='';tenantId='';error='Az.Accounts module not installed'} | ConvertTo-Json -Compress; exit 0 "
            "}; "
            "try { "
            "  $ctx = Get-AzContext -ErrorAction Stop; "
            "  if ($null -eq $ctx -or $null -eq $ctx.Subscription) { throw 'No active Az context' }; "
            "  [PSCustomObject]@{installed=$true;loggedIn=$true;user=$ctx.Account.Id;subscriptionName=$ctx.Subscription.Name;subscriptionId=$ctx.Subscription.Id;tenantId=$ctx.Tenant.Id;error=''} | ConvertTo-Json -Compress "
            "} catch { "
            "  [PSCustomObject]@{installed=$true;loggedIn=$false;user='';subscriptionName='';subscriptionId='';tenantId='';error=$_.Exception.Message} | ConvertTo-Json -Compress "
            "}"
        )
        try:
            ps = _az_run(["pwsh", "-NoProfile", "-NonInteractive", "-Command", ps_cmd])
            if ps.returncode == 0 and ps.stdout.strip():
                data = json.loads(ps.stdout.strip())
                pwsh["installed"] = bool(data.get("installed", False))
                pwsh["loggedIn"] = bool(data.get("loggedIn", False))
                pwsh["user"] = data.get("user", "") or ""
                pwsh["subscriptionName"] = data.get("subscriptionName", "") or ""
                pwsh["subscriptionId"] = data.get("subscriptionId", "") or ""
                pwsh["tenantId"] = data.get("tenantId", "") or ""
                pwsh["error"] = data.get("error", "") or ""
            else:
                pwsh["error"] = (ps.stderr or "Unable to inspect Az PowerShell context").strip()[:400]
        except Exception as e:
            pwsh["error"] = str(e)[:400]
        return pwsh

    with ThreadPoolExecutor(max_workers=2) as executor:
        cli_future = executor.submit(get_cli)
        pwsh_future = executor.submit(get_pwsh)
        cli = cli_future.result()
        pwsh = pwsh_future.result()

    sub_aligned = False
    tenant_aligned = False
    if cli["loggedIn"] and pwsh["loggedIn"]:
        sub_aligned = cli["subscriptionId"].strip().lower() == pwsh["subscriptionId"].strip().lower()
        tenant_aligned = cli["tenantId"].strip().lower() == pwsh["tenantId"].strip().lower()

    issues: list[str] = []
    if not cli["installed"]:
        issues.append("Azure CLI is not installed.")
    elif not cli["loggedIn"]:
        issues.append("Azure CLI is not logged in. Run: az login")
    if not pwsh["installed"]:
        issues.append("Az PowerShell module is not installed. Run: Install-Module Az -Scope CurrentUser")
    elif not pwsh["loggedIn"]:
        issues.append("Azure PowerShell is not logged in. Run: Connect-AzAccount")
    if cli["loggedIn"] and pwsh["loggedIn"] and (not sub_aligned or not tenant_aligned):
        issues.append("Azure CLI and Az PowerShell are using different subscription/tenant contexts.")

    return {
        "ready": len(issues) == 0,
        "cli": cli,
        "pwsh": pwsh,
        "aligned": {
            "subscription": sub_aligned,
            "tenant": tenant_aligned,
        },
        "issues": issues,
    }


class TeardownRequest(BaseModel):
    fabric_workspace_name: str = ""
    resource_group_name: str = ""
    delete_workspace: bool = False
    delete_azure_rg: bool = True


class TeardownBatchRequest(BaseModel):
    jobs: list[TeardownRequest]


class Phase7ContinuationRequest(BaseModel):
    alert_email: str = ""
    payer_ops_email: str = ""
    claim_event_rate_per_minute: int = 60


import re as _re

def _validate_safe_name(v: str, field_name: str) -> str:
    """Reject shell metacharacters in names passed to PowerShell subprocesses."""
    if v and not _re.match(r'^[a-zA-Z0-9_.\-]{0,128}$', v):
        raise ValueError(f"{field_name} contains invalid characters (allowed: a-z, 0-9, -, _, .)")
    return v

class DeployRequest(BaseModel):
    resource_group_name: str = ""
    location: str = "eastus"
    admin_security_group: str = ""
    fabric_workspace_name: str = ""

    @staticmethod
    def _check_name(v: str, info) -> str:
        return _validate_safe_name(v, info.field_name) if v else v

    _v_rg = __import__('pydantic').field_validator('resource_group_name', mode='after')(_check_name)
    _v_ws = __import__('pydantic').field_validator('fabric_workspace_name', mode='after')(_check_name)
    _v_sg = __import__('pydantic').field_validator('admin_security_group', mode='after')(_check_name)
    _v_cap = __import__('pydantic').field_validator('capacity_name', mode='after')(_check_name)
    _v_source_rg = __import__('pydantic').field_validator('source_resource_group', mode='after')(_check_name)
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
    reuse_patients: bool = False
    source_resource_group: str = ""
    use_cached_synthea: bool = False
    # Granular component toggles
    skip_synthea: bool = False
    skip_device_assoc: bool = False
    skip_fhir_export: bool = False
    skip_rti_phase2: bool = False
    skip_hds_pipelines: bool = False
    skip_data_agents: bool = False
    skip_imaging: bool = False
    skip_ontology: bool = False
    skip_activator: bool = False
    skip_quality_measures: bool = False
    require_bronze_clinical_fhir: bool = False
    require_bronze_imaging_dicom: bool = False
    skip_phase7: bool = False
    skip_payer_rti: bool = False
    skip_payer_activator: bool = False
    skip_ops_agent: bool = False
    skip_graph_agent: bool = False
    payer_ops_email: str = ""
    claim_event_rate_per_minute: int = 60
    dicom_toolkit_path: str = ""
    phase7_only: bool = False
    phase2_only: bool = False
    phase3_only: bool = False
    phase4_only: bool = False
    continue_from_instance_id: str = ""


def _ensure_deployment_policy_tags(req: DeployRequest) -> None:
    """Mutate a deploy request so stored config and launched process carry required policy tags."""
    req.tags = normalize_policy_tags(req.tags)


def now_iso():
    return datetime.now(timezone.utc).isoformat()

def _workspace_id_from_url(value: str) -> str:
    if not value:
        return ""
    match = _re.search(r"app\.fabric\.microsoft\.com/groups/([0-9a-fA-F-]{36})", value)
    return match.group(1) if match else ""


def _persisted_workspace_id(ws_name: str) -> str:
    """Recover a Fabric workspace id from persisted run links/logs when live Fabric API is blocked."""
    if not ws_name:
        return ""

    workspace_id_pattern = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

    def extract_workspace_id(value: object) -> str:
        text = str(value or "")
        from_url = _workspace_id_from_url(text)
        if from_url:
            return from_url
        if ws_name not in text:
            return ""
        match = workspace_id_pattern.search(text)
        return match.group(0) if match else ""

    matching = []
    for dep in deployments.values():
        cs = dep.get("customStatus") or {}
        if cs.get("workspaceName") == ws_name:
            matching.append(dep)
    matching.sort(key=lambda dep: dep.get("lastUpdatedTime") or dep.get("createdTime") or "", reverse=True)

    for dep in matching:
        cs = dep.get("customStatus") or {}
        for source in (cs.get("links") or {}, cs.get("resources") or {}):
            if not isinstance(source, dict):
                continue
            for value in source.values():
                workspace_id = extract_workspace_id(value)
                if workspace_id:
                    return workspace_id
        output = dep.get("output") or {}
        resources = output.get("resources") if isinstance(output, dict) else None
        if isinstance(resources, dict):
            for value in resources.values():
                workspace_id = extract_workspace_id(value)
                if workspace_id:
                    return workspace_id

    log_dir = Path(__file__).parent / "logs"
    for dep in matching:
        instance_id = dep.get("instanceId") or dep.get("id") or ""
        log_file = log_dir / f"{instance_id}.jsonl"
        if not log_file.exists():
            continue
        try:
            with log_file.open("r", encoding="utf-8") as f:
                for line in f:
                    workspace_id = extract_workspace_id(line)
                    if workspace_id:
                        return workspace_id
        except Exception as ex:
            logger.debug("Could not scan deployment log %s for workspace id: %s", log_file, ex)

    return ""


def _workspace_state_from_persisted_link(ws_name: str) -> dict | None:
    workspace_id = _persisted_workspace_id(ws_name)
    if not workspace_id:
        return None
    return {
        "name": ws_name,
        "exists": True,
        "id": workspace_id,
        "status": "exists_cached",
        "url": f"https://app.fabric.microsoft.com/groups/{workspace_id}",
        "warning": "Live Fabric workspace query blocked; using persisted deployment link.",
    }

def _cloud_state_sync(ws_name: str = "", rg_name: str = "") -> dict:
    state = {
        "workspace": {"name": ws_name, "exists": None, "id": "", "status": "unknown"},
        "resourceGroup": {"name": rg_name, "exists": None, "provisioningState": "unknown", "status": "unknown"},
        "checkedAt": now_iso(),
    }
    if ws_name:
        try:
            from shared.fabric_client import FabricClient
            fabric = FabricClient()
            ws = fabric.find_workspace(ws_name, max_retries=1)
            state["workspace"].update({"exists": ws is not None, "id": ws.get("id", "") if ws else "", "status": "exists" if ws else "deleted"})
        except Exception as ex:
            persisted = _workspace_state_from_persisted_link(ws_name)
            if persisted:
                state["workspace"].update(persisted)
                logger.info("Fabric workspace '%s' live query blocked; using persisted workspace id %s", ws_name, persisted["id"])
            else:
                state["workspace"].update({"status": "unreachable", "error": str(ex)[:300]})
    if rg_name:
        try:
            exists_proc = _az_run(["az", "group", "exists", "--name", rg_name])
            exists = exists_proc.stdout.strip().lower() == "true"
            state["resourceGroup"]["exists"] = exists
            if exists:
                show = _az_run(["az", "group", "show", "--name", rg_name, "--query", "properties.provisioningState", "-o", "tsv"])
                provisioning = (show.stdout or "").strip() or "Unknown"
                state["resourceGroup"].update({"provisioningState": provisioning, "status": provisioning.lower()})
            else:
                state["resourceGroup"].update({"provisioningState": "Deleted", "status": "deleted"})
        except Exception as ex:
            state["resourceGroup"].update({"status": "unreachable", "error": str(ex)[:300]})
    return state


def _validation_from_resources(resources: dict, is_teardown: bool = False) -> dict:
    azure_count = len(resources.get("azure") or [])
    fabric_count = len(resources.get("fabric") or [])
    workspace_exists = bool(resources.get("workspace"))
    if is_teardown:
        checks = [
            {"name": "Fabric workspace deleted", "status": "pass" if not workspace_exists else "fail", "detail": "Workspace absent" if not workspace_exists else "Workspace still exists"},
            {"name": "Azure resource group empty/deleted", "status": "pass" if azure_count == 0 else "fail", "detail": f"{azure_count} Azure resource(s) found"},
        ]
    else:
        checks = [
            {"name": "Fabric workspace exists", "status": "pass" if workspace_exists else "fail", "detail": "Workspace found" if workspace_exists else "Workspace missing"},
            {"name": "Fabric items discovered", "status": "pass" if fabric_count > 0 else "warning", "detail": f"{fabric_count} Fabric item(s)"},
            {"name": "Azure resources discovered", "status": "pass" if azure_count > 0 else "warning", "detail": f"{azure_count} Azure resource(s)"},
        ]
    return {"passed": all(c["status"] != "fail" for c in checks), "checks": checks, "resources": resources, "checkedAt": now_iso()}


def _reconcile_deployment_completion_from_validation(instance_id: str, dep: dict, validation: dict) -> bool:
    """Persist a completed run state when post-deployment validation has no required failures."""
    if dep.get("runtimeStatus") not in {"Failed", "Terminated"}:
        return False
    cs = dep.get("customStatus") or {}
    if cs.get("runType") == "teardown":
        return False
    if not validation.get("passed"):
        return False

    checks = validation.get("checks") or []
    failed_checks = [check for check in checks if check.get("status") == "fail"]
    if failed_checks:
        return False

    resources = validation.get("resources") or {}
    azure_count = len(resources.get("azure") or [])
    workspace = resources.get("workspace") or {}
    if not workspace:
        return False

    checked_at = validation.get("checkedAt") or now_iso()
    original_status = dep.get("runtimeStatus")
    original_detail = cs.get("detail") or ""
    if not cs.get("validationReconciled"):
        cs["originalRuntimeStatus"] = original_status
        cs["originalFailureDetail"] = original_detail
    cs["validationReconciled"] = True
    cs["validatedAt"] = checked_at
    cs["status"] = "succeeded"
    cs["currentPhase"] = "Deployment Complete"
    cs["detail"] = "Post-deployment validation passed; the prior failed run state was reconciled. Resource discovery warnings remain non-fatal and are preserved in validation details."
    cs["validationSummary"] = {
        "azureResources": azure_count,
        "fabricItems": len(resources.get("fabric") or []),
        "workspaceStatus": workspace.get("status") or "exists",
    }
    total = cs.get("totalPhases") or 0
    if total:
        cs["completedPhases"] = total

    output = dep.get("output") if isinstance(dep.get("output"), dict) else {}
    output["status"] = "succeeded"
    phases = output.get("phases") if isinstance(output.get("phases"), list) else []
    for phase in phases:
        if phase.get("status") == "failed":
            phase["reconciledFrom"] = "failed"
            phase["status"] = "warning"
            if not phase.get("detail"):
                phase["detail"] = original_detail or "Original run step failed before post-deployment validation reconciled the run."
    dep["output"] = output
    dep["runtimeStatus"] = "Completed"
    dep["lastUpdatedTime"] = checked_at
    logger.info("Deployment %s reconciled to Completed from post-deployment validation", instance_id)
    return True


def _normalize_url(raw: str) -> str:
    """Trim common trailing punctuation from captured URLs."""
    return raw.strip().rstrip(",.;)\"]'")


def _extract_deployment_links(message: str) -> dict[str, str]:
    """Extract well-known deployment URLs from log lines."""
    links: dict[str, str] = {}

    report_match = _re.search(r"Report URL:\s*(https?://\S+)", message, flags=_re.IGNORECASE)
    if report_match:
        links["imagingReport"] = _normalize_url(report_match.group(1))

    settings_match = _re.search(r"Settings:\s*(https?://\S+)", message, flags=_re.IGNORECASE)
    if settings_match:
        links["imagingReportSettings"] = _normalize_url(settings_match.group(1))

    viewer_match = _re.search(r"OHIF Viewer(?: \(from Azure\))?\s*:\s*(https?://\S+)", message, flags=_re.IGNORECASE)
    if viewer_match:
        links["ohifViewer"] = _normalize_url(viewer_match.group(1))

    if "azurestaticapps.net" in message.lower() and "ohifViewer" not in links:
        swa_match = _re.search(r"(https?://[^\s]*azurestaticapps\.net\S*)", message, flags=_re.IGNORECASE)
        if swa_match:
            links["ohifViewer"] = _normalize_url(swa_match.group(1))

    return links


@app.post("/api/teardown/start")
async def start_teardown(req: TeardownRequest):
    now_local = datetime.now()
    timestamp = now_local.strftime("%Y%m%d-%H%M%S")
    import random
    suffix = random.randint(1000, 9999)
    teardown_mode = "teardownFull" if (req.delete_workspace and req.delete_azure_rg) else "teardownPartial"
    instance_id = f"{teardown_mode}-{timestamp}-{suffix}"

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
            "cloudStatus": "submitted",
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
    _create_logged_task(_run_teardown(instance_id, req), name=f"teardown:{instance_id}")

    logger.info("Teardown started: %s (workspace=%s, rg=%s)",
                instance_id, req.fabric_workspace_name, req.resource_group_name)
    return {"instanceId": instance_id, "statusUrl": f"/api/deploy/{instance_id}/status"}


@app.post("/api/teardown/batch/start")
async def start_teardown_batch(req: TeardownBatchRequest):
    batch_id = f"teardownBatch-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"
    children = []
    for job in req.jobs:
        result = await start_teardown(job)
        children.append(result["instanceId"])
    display = f"{len(children)} teardown job(s)"
    deployments[batch_id] = {
        "instanceId": batch_id,
        "name": "teardown_batch",
        "runtimeStatus": "Running",
        "createdTime": now_iso(),
        "lastUpdatedTime": now_iso(),
        "customStatus": {
            "currentPhase": "Batch Teardown",
            "status": "running",
            "detail": display,
            "completedPhases": 0,
            "totalPhases": len(children),
            "resources": {},
            "runType": "teardownBatch",
            "displayName": display,
            "childInstanceIds": children,
            "logs": [],
        },
        "output": {"status": "running", "phases": [], "resources": {}},
    }
    save_state()
    return {"batchId": batch_id, "instanceIds": children, "statusUrl": f"/api/teardown/batch/{batch_id}"}


@app.get("/api/teardown/batch/{batch_id}")
async def get_teardown_batch(batch_id: str):
    batch = deployments.get(batch_id)
    if not batch:
        raise HTTPException(404, "Batch not found")
    child_ids = (batch.get("customStatus") or {}).get("childInstanceIds") or []
    children = [deployments[i] for i in child_ids if i in deployments]
    completed = sum(1 for child in children if child.get("runtimeStatus") == "Completed")
    failed = sum(1 for child in children if child.get("runtimeStatus") in {"Failed", "Terminated"})
    running = len(children) - completed - failed
    cs = batch["customStatus"]
    cs["completedPhases"] = completed
    cs["totalPhases"] = len(children)
    if running == 0:
        batch["runtimeStatus"] = "Completed" if failed == 0 else "Failed"
        cs["status"] = "succeeded" if failed == 0 else "failed"
    batch["lastUpdatedTime"] = now_iso()
    save_state()
    return {"batch": batch, "children": children, "summary": {"completed": completed, "failed": failed, "running": running, "total": len(children)}}


async def _run_teardown(instance_id: str, req: TeardownRequest):
    """Fast-path teardown using direct Fabric/Azure APIs.

    Fabric workspace deletion cascades to all items — no need to iterate them
    first. Only the workspace managed identity (SPN) needs a separate
    deprovision call because it survives workspace deletion as an orphaned
    Entra app registration.

    Each call to this function runs as an independent asyncio task, so
    multiple concurrent teardowns proceed in parallel.
    """
    deployment = deployments[instance_id]
    teardown_logs: list[dict] = []
    start = time.time()
    teardown_phases: list[dict] = []

    def log(level: str, message: str):
        teardown_logs.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
        })
        deployment["customStatus"]["logs"] = teardown_logs[-200:]
        deployment["customStatus"]["detail"] = message
        deployment["lastUpdatedTime"] = now_iso()
        logger.info("[%s] %s", instance_id, message)

    def add_phase(name: str):
        teardown_phases.append({"phase": name, "status": "running"})
        deployment["customStatus"]["currentPhase"] = name
        deployment["customStatus"]["totalPhases"] = len(teardown_phases)
        deployment["output"] = {"status": "running", "phases": teardown_phases, "resources": {}}
        save_state()

    def complete_phase(name: str, status: str = "succeeded"):
        for p in teardown_phases:
            if p["phase"] == name and p["status"] == "running":
                p["status"] = status
        succeeded = sum(1 for p in teardown_phases if p["status"] == "succeeded")
        deployment["customStatus"]["completedPhases"] = succeeded
        deployment["output"] = {"status": "running", "phases": teardown_phases, "resources": {}}
        save_state()

    had_error = False

    try:
        log("info", f"Starting teardown (workspace='{req.fabric_workspace_name}', rg='{req.resource_group_name}')")

        # ── Fabric workspace deletion ─────────────────────────────────
        if req.fabric_workspace_name and req.delete_workspace:
            add_phase("Workspace Identity")
            loop = asyncio.get_event_loop()

            def _fabric_delete():
                from shared.fabric_client import FabricClient
                fabric = FabricClient()
                ws = fabric.find_workspace(req.fabric_workspace_name)
                if not ws:
                    return {"found": False}
                ws_id = ws["id"]
                # Deprovision managed identity first — cleans up the Entra SPN
                # that would otherwise be orphaned after workspace deletion.
                identity_ok = True
                identity_err = ""
                try:
                    fabric.deprovision_workspace_identity(ws_id)
                except Exception as ex:
                    identity_ok = False
                    identity_err = str(ex)
                # Delete the workspace — this cascades to all items inside.
                fabric.call("DELETE", f"/workspaces/{ws_id}")
                return {
                    "found": True,
                    "workspace_id": ws_id,
                    "identity_ok": identity_ok,
                    "identity_error": identity_err,
                }

            try:
                result = await loop.run_in_executor(None, _fabric_delete)
                if not result.get("found"):
                    log("warn", f"Workspace '{req.fabric_workspace_name}' not found — skipping Fabric cleanup")
                    complete_phase("Workspace Identity", "skipped")
                    add_phase("Delete Workspace")
                    complete_phase("Delete Workspace", "skipped")
                else:
                    if result["identity_ok"]:
                        log("success", "✓ Workspace managed identity deprovisioned")
                    else:
                        log("warn", f"Identity deprovision skipped/failed: {result['identity_error']}")

                    # Delete matching Entra ID app registrations and service principals to prevent orphans
                    log("info", f"Checking for Entra app registrations matching '{req.fabric_workspace_name}'...")
                    try:
                        proc_apps = _az_run([
                            "az", "ad", "app", "list",
                            "--display-name", req.fabric_workspace_name,
                            "--query", "[].{id:id, appId:appId}",
                            "-o", "json"
                        ])
                        if proc_apps.returncode == 0 and proc_apps.stdout.strip():
                            import json
                            apps = json.loads(proc_apps.stdout)
                            if apps:
                                log("info", f"Found {len(apps)} matching Entra app registration(s)")
                                for app in apps:
                                    app_id = app.get("id")
                                    if app_id:
                                        del_proc = _az_run(["az", "ad", "app", "delete", "--id", app_id])
                                        if del_proc.returncode == 0:
                                            log("success", f"✓ Deleted Entra app registration: {app_id}")
                                        else:
                                            log("warn", f"Could not delete Entra app registration {app_id}: {del_proc.stderr.strip()}")
                            else:
                                log("info", "No matching Entra app registrations found")
                        else:
                            log("info", "No matching Entra app registrations found")
                    except Exception as ex:
                        log("warn", f"Failed to clean up Entra app registrations: {ex}")

                    log("info", f"Checking for Entra service principals matching '{req.fabric_workspace_name}'...")
                    try:
                        proc_sps = _az_run([
                            "az", "ad", "sp", "list",
                            "--display-name", req.fabric_workspace_name,
                            "--query", "[].{id:id, appId:appId}",
                            "-o", "json"
                        ])
                        if proc_sps.returncode == 0 and proc_sps.stdout.strip():
                            import json
                            sps = json.loads(proc_sps.stdout)
                            if sps:
                                log("info", f"Found {len(sps)} matching Entra service principal(s)")
                                for sp in sps:
                                    sp_id = sp.get("id")
                                    if sp_id:
                                        del_proc = _az_run(["az", "ad", "sp", "delete", "--id", sp_id])
                                        if del_proc.returncode == 0:
                                            log("success", f"✓ Deleted Entra service principal: {sp_id}")
                                        else:
                                            log("warn", f"Could not delete Entra service principal {sp_id}: {del_proc.stderr.strip()}")
                            else:
                                log("info", "No matching Entra service principals found")
                        else:
                            log("info", "No matching Entra service principals found")
                    except Exception as ex:
                        log("warn", f"Failed to clean up Entra service principals: {ex}")

                    complete_phase("Workspace Identity")

                    add_phase("Delete Workspace")
                    log("success", f"✓ Workspace '{req.fabric_workspace_name}' deleted (cascades to all items)")
                    complete_phase("Delete Workspace")
            except Exception as e:
                had_error = True
                log("error", f"Fabric teardown failed: {e}")
                complete_phase("Workspace Identity", "failed")

        # ── Azure RG deletion (fire-and-poll) ─────────────────────────
        if req.resource_group_name and req.delete_azure_rg:
            add_phase("Azure Resource Group")
            try:
                # NOTE: _az_run() uses blocking subprocess.run(). Run it in a
                # thread pool so the asyncio event loop is not blocked while
                # multiple concurrent teardowns poll az.
                loop = asyncio.get_event_loop()
                proc = await loop.run_in_executor(None, lambda: _az_run([
                    "az", "group", "delete",
                    "--name", req.resource_group_name,
                    "--yes", "--no-wait",
                ]))
                if proc.returncode != 0:
                    raise RuntimeError(proc.stderr.strip() or "az group delete failed")
                log("info", f"Azure RG deletion initiated for '{req.resource_group_name}' (async)")
                deployment["customStatus"]["cloudStatus"] = "deleting"

                for poll_attempt in range(120):  # up to ~10 min
                    check = await loop.run_in_executor(
                        None,
                        lambda: _az_run(["az", "group", "exists", "--name", req.resource_group_name]),
                    )
                    if check.stdout.strip().lower() == "false":
                        log("success", f"✓ Azure RG '{req.resource_group_name}' fully deleted")
                        deployment["customStatus"]["cloudStatus"] = "deleted"
                        complete_phase("Azure Resource Group")
                        break
                    if poll_attempt % 6 == 0:
                        log("info", f"Azure RG still deleting... ({(poll_attempt + 1) * 5}s)")
                    await asyncio.sleep(5)
                else:
                    log("warn", f"Timed out waiting for RG '{req.resource_group_name}' deletion after 10 min — it may still be deleting")
                    complete_phase("Azure Resource Group", "failed")
                    had_error = True
            except Exception as e:
                had_error = True
                log("error", f"Azure RG teardown failed: {e}")
                complete_phase("Azure Resource Group", "failed")

        duration = time.time() - start

        if not teardown_phases:
            # Nothing was requested
            log("warn", "No teardown targets were specified")
            deployment["runtimeStatus"] = "Completed"
            deployment["customStatus"]["status"] = "succeeded"
            deployment["customStatus"]["cloudStatus"] = "none"
        elif had_error:
            deployment["runtimeStatus"] = "Failed"
            deployment["customStatus"]["status"] = "failed"
            deployment["customStatus"]["cloudStatus"] = "needs_attention"
            deployment["customStatus"]["currentPhase"] = "Teardown Failed"
        else:
            deployment["runtimeStatus"] = "Completed"
            deployment["customStatus"]["status"] = "succeeded"
            deployment["customStatus"]["cloudStatus"] = "deleted"
            deployment["customStatus"]["currentPhase"] = "Teardown Complete"
            deployment["customStatus"]["completedPhases"] = len(teardown_phases)

        deployment["output"] = {
            "status": "succeeded" if not had_error else "failed",
            "phases": teardown_phases or [{"phase": "Teardown", "status": "succeeded", "duration": duration}],
            "resources": {},
        }
        logger.info("Teardown %s finished (had_error=%s, %.1fs)", instance_id, had_error, duration)

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
        # Note: teardown does not attach a per-instance log handler the way
        # _run_deploy does, so there is nothing to remove here. Just persist
        # the final timestamp + state.
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


def _phase_has_blocking_logs(deployment: dict, phase_name: str) -> bool:
    """Return True when a nominally-successful phase logged errors.

    Some PowerShell sub-scripts print recoverable-looking summaries even after
    native tools failed. Auto-resume must not skip those phases, or repair runs
    can bypass incomplete Fabric RTI/Eventstream/KQL work.
    """
    if not phase_name:
        return False

    instance_id = deployment.get("instanceId", "")
    log_file = Path(__file__).parent / "logs" / f"{instance_id}.jsonl"
    if not log_file.exists():
        return False

    target_phase = phase_name.upper()
    blocking_markers = (
        "FAILED TO CREATE CLOUD CONNECTION",
        "SKIPPING EVENTSTREAM TOPOLOGY",
        "ERROR: HEALTHCARE DATA SOLUTIONS NOT FOUND",
        "IMAGE NOT FOUND IN ACR",
    )

    try:
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(entry.get("phase", "")).upper() != target_phase:
                    continue
                message = str(entry.get("message", ""))
                message_upper = message.upper()
                transient_fabric_retry = (
                    "REQUESTDENIEDBYINBOUNDPOLICY" in message_upper
                    and "RETRY" in message_upper
                )
                if transient_fabric_retry:
                    continue
                if entry.get("level") == "error":
                    return True
                ratio = _re.search(r"(?:KQL Deployment|Phase 2 Results):\s*(\d+)\s*/\s*(\d+)\s+succeeded", message, flags=_re.IGNORECASE)
                if ratio and int(ratio.group(1)) < int(ratio.group(2)):
                    return True
                if any(marker in message_upper for marker in blocking_markers):
                    return True
    except OSError as ex:
        logger.warning("Could not inspect deployment log %s: %s", log_file, ex)
    return False


def _reset_continuation_skip_flags(req: DeployRequest) -> None:
    """Clear prior auto-resume skips before re-deriving safe skips from live state."""
    for field in (
        "skip_base_infra",
        "skip_fhir",
        "skip_dicom",
        "skip_fabric",
        "reuse_patients",
        "skip_synthea",
        "skip_device_assoc",
        "skip_fhir_export",
        "skip_rti_phase2",
        "skip_hds_pipelines",
        "skip_data_agents",
        "skip_imaging",
        "skip_ontology",
        "skip_activator",
        "skip_quality_measures",
    ):
        setattr(req, field, False)


def _live_resume_prerequisites(req: DeployRequest, cloud_state: dict) -> dict:
    """Collect live evidence used to decide whether a prior phase can be skipped."""
    evidence: dict = {
        "cloud": cloud_state,
        "azureTypes": set(),
        "azureNames": set(),
        "fabricItems": [],
        "fhirCounts": {"patients": 0, "devices": 0, "exportedFiles": 0, "dicomStudies": 0},
    }
    try:
        resources = _get_deployed_resources_sync(req.fabric_workspace_name, req.resource_group_name)
        evidence["azureTypes"] = {str(r.get("fullType") or r.get("type") or "").lower() for r in resources.get("azure") or []}
        evidence["azureNames"] = {str(r.get("name") or "").lower() for r in resources.get("azure") or []}
        evidence["fabricItems"] = resources.get("fabric") or []
    except Exception as ex:
        logger.warning("Resume prerequisite resource query failed: %s", ex)
    if req.resource_group_name and bool((cloud_state.get("resourceGroup") or {}).get("exists")):
        evidence["fhirCounts"] = _query_fhir_counts(req.resource_group_name)
    return evidence


def _has_azure_type(evidence: dict, expected: str) -> bool:
    expected = expected.lower()
    return any(expected in resource_type for resource_type in evidence.get("azureTypes", set()))


def _fabric_item_matches(evidence: dict, *needles: str) -> bool:
    haystacks = []
    for item in evidence.get("fabricItems") or []:
        haystacks.append(f"{item.get('name', '')} {item.get('type', '')}".lower())
    return any(any(needle.lower() in value for needle in needles) for value in haystacks)
def _fabric_item_type_matches(evidence: dict, item_type: str, *name_needles: str) -> bool:
    item_type = item_type.lower()
    needles = tuple(needle.lower() for needle in name_needles)
    for item in evidence.get("fabricItems") or []:
        current_type = str(item.get("type") or "").lower()
        current_name = str(item.get("name") or "").lower()
        if current_type == item_type and (not needles or any(needle in current_name for needle in needles)):
            return True
    return False



def _phase_live_prerequisites_ok(req: DeployRequest, phase_name: str, evidence: dict) -> tuple[bool, str]:
    phase_name = phase_name.upper()
    cloud = evidence.get("cloud") or {}
    rg_exists = bool((cloud.get("resourceGroup") or {}).get("exists"))
    workspace_exists = bool((cloud.get("workspace") or {}).get("exists")) or bool(evidence.get("fabricItems"))

    if "FABRIC WORKSPACE" in phase_name:
        return (workspace_exists, "Fabric workspace exists" if workspace_exists else "Fabric workspace not verified")

    if "BASE AZURE INFRASTRUCTURE" in phase_name or "SHARED HDS INFRASTRUCTURE" in phase_name:
        required = (
            "microsoft.containerregistry/registries",
            "microsoft.keyvault/vaults",
            "microsoft.storage/storageaccounts",
            "microsoft.healthcareapis/workspaces",
        )
        missing = [resource_type for resource_type in required if not _has_azure_type(evidence, resource_type)]
        if missing:
            return (False, f"Azure prerequisite resources missing: {', '.join(missing)}")
        return (rg_exists, "Azure prerequisite resources exist" if rg_exists else "Resource group not verified")

    if "FHIR SERVICE + SYNTHEA" in phase_name:
        counts = evidence.get("fhirCounts") or {}
        patients = int(counts.get("patients") or 0)
        devices = int(counts.get("devices") or 0)
        exported = int(counts.get("exportedFiles") or 0)
        if patients <= 0 or devices <= 0 or exported <= 0:
            return (False, f"FHIR prerequisites incomplete: patients={patients}, devices={devices}, exportedFiles={exported}")
        return (True, f"FHIR data/export verified: patients={patients}, devices={devices}, exportedFiles={exported}")

    if "DICOM LOADER" in phase_name or "DICOM SERVICE + LOADER" in phase_name:
        dicom_studies = int((evidence.get("fhirCounts") or {}).get("dicomStudies") or 0)
        if dicom_studies <= 0:
            return (False, "DICOM output container has no studies")
        return (True, f"DICOM output verified: studies={dicom_studies}")

    if "FABRIC RTI" in phase_name:
        if not workspace_exists:
            return (False, "Fabric workspace not verified")
        has_eventstream = _fabric_item_type_matches(evidence, "Eventstream", "masimo", "telemetry")
        has_eventhouse = _fabric_item_type_matches(evidence, "Eventhouse", "masimo") or _fabric_item_type_matches(evidence, "KQLDatabase", "masimo")
        has_dashboard = _fabric_item_type_matches(evidence, "Dashboard", "masimo", "telemetry", "clinical", "alerts") or _fabric_item_type_matches(evidence, "KQLDashboard", "masimo", "telemetry", "clinical", "alerts")
        if has_eventstream and has_eventhouse and has_dashboard:
            return (True, "Fabric RTI Eventstream, Eventhouse/KQL, and dashboard items verified")
        missing = []
        if not has_eventstream:
            missing.append("Masimo Eventstream")
        if not has_eventhouse:
            missing.append("Masimo Eventhouse/KQL database")
        if not has_dashboard:
            missing.append("Masimo dashboard")
        return (False, f"Fabric RTI incomplete: missing {', '.join(missing)}")

    return (True, "No extra live prerequisite check required")



def _apply_success_skips_from_deployment(req: DeployRequest, prior_deploy: dict, mode: str = "Auto-resume") -> bool:
    """Skip only phases that safely succeeded in a specific failed deployment."""
    cloud_state = _cloud_state_sync(req.fabric_workspace_name, req.resource_group_name)
    workspace_exists = bool((cloud_state.get("workspace") or {}).get("exists"))
    rg_exists = bool((cloud_state.get("resourceGroup") or {}).get("exists"))
    if not workspace_exists and not rg_exists:
        logger.info(
            "%s disabled for workspace=%s rg=%s because neither target exists in live cloud state",
            mode,
            req.fabric_workspace_name,
            req.resource_group_name,
        )
        return False

    output = prior_deploy.get("output") or {}
    phases = output.get("phases") or []
    if not phases:
        return False


    live_evidence = _live_resume_prerequisites(req, cloud_state)

    applied = False
    # Check each successful phase and enable corresponding skip flags.
    # A phase with error-level logs is not safe to skip: the PowerShell wrapper
    # can otherwise mark a step succeeded after a native sub-command failed.
    for p in phases:
        if p.get("status") != "succeeded":
            continue
        if p.get("warnings") or _phase_has_blocking_logs(prior_deploy, p.get("phase", "")):
            logger.info("%s not skipping phase '%s' from %s because it logged warnings/errors", mode, p.get("phase", ""), prior_deploy.get("instanceId"))
            continue

        phase_name = p.get("phase", "").upper()
        prereq_ok, prereq_detail = _phase_live_prerequisites_ok(req, phase_name, live_evidence)
        if not prereq_ok:
            if "FHIR SERVICE + SYNTHEA" in phase_name:
                counts = live_evidence.get("fhirCounts") or {}
                patients = int(counts.get("patients") or 0)
                devices = int(counts.get("devices") or 0)
                exported = int(counts.get("exportedFiles") or 0)
                if patients > 0 and devices > 0 and exported <= 0:
                    req.reuse_patients = True
                    req.skip_synthea = True
                    req.skip_device_assoc = True
                    req.skip_fhir_export = False
                    applied = True
                    logger.info(
                        "%s preserving existing FHIR data from phase '%s' in %s but rerunning the catch-up FHIR export: patients=%s, devices=%s, exportedFiles=%s",
                        mode,
                        p.get("phase", ""),
                        prior_deploy.get("instanceId"),
                        patients,
                        devices,
                        exported,
                    )
                    continue
            logger.info(
                "%s not skipping phase '%s' from %s because live prerequisite validation failed: %s",
                mode,
                p.get("phase", ""),
                prior_deploy.get("instanceId"),
                prereq_detail,
            )
            continue
        logger.info(
            "%s skipping phase '%s' from %s after live prerequisite validation: %s",
            mode,
            p.get("phase", ""),
            prior_deploy.get("instanceId"),
            prereq_detail,
        )


        if "BASE AZURE INFRASTRUCTURE" in phase_name:
            req.skip_base_infra = True
        elif "FHIR SERVICE + SYNTHEA" in phase_name:
            req.skip_fhir = True
            req.skip_synthea = True
            req.skip_device_assoc = True
            req.reuse_patients = True
            req.skip_fhir_export = True
        elif "DICOM LOADER" in phase_name or "DICOM SERVICE + LOADER" in phase_name:
            req.skip_dicom = True
        elif "FABRIC RTI ENRICHMENT" in phase_name or "FABRIC RTI (AUTO)" in phase_name or "RTI PHASE 2" in phase_name:
            req.skip_rti_phase2 = True
        elif "FABRIC RTI" in phase_name:
            req.skip_fabric = True
        elif "HDS PIPELINES" in phase_name:
            req.skip_hds_pipelines = True
            req.skip_dicom = True
        elif "DATA AGENTS" in phase_name:
            req.skip_data_agents = True
        elif "IMAGING & REPORTING" in phase_name:
            req.skip_imaging = True
        elif "ONTOLOGY" in phase_name:
            req.skip_ontology = True
        elif "DATA ACTIVATOR" in phase_name:
            req.skip_activator = True
        elif "CMS QUALITY" in phase_name:
            req.skip_quality_measures = True
        else:
            continue
        applied = True

    logger.info("%s activated: loaded successful phases from %s. Applied skips: base_infra=%s, fhir=%s, dicom=%s, fabric=%s, rti2=%s, hds=%s, agents=%s, imaging=%s, ontology=%s, activator=%s, quality=%s",
                mode, prior_deploy["instanceId"], req.skip_base_infra, req.skip_fhir, req.skip_dicom, req.skip_fabric, req.skip_rti_phase2, req.skip_hds_pipelines, req.skip_data_agents, req.skip_imaging, req.skip_ontology, req.skip_activator, req.skip_quality_measures)
    return applied


def _apply_live_continuation_skips(req: DeployRequest, prior_deploy: dict, mode: str = "Continue-from-failure") -> bool:
    """Derive safe continuation skips from live cloud state when old run output is missing."""
    cloud_state = _cloud_state_sync(req.fabric_workspace_name, req.resource_group_name)
    workspace_exists = bool((cloud_state.get("workspace") or {}).get("exists"))
    rg_exists = bool((cloud_state.get("resourceGroup") or {}).get("exists"))
    if not workspace_exists and not rg_exists:
        logger.info(
            "%s live fallback disabled for %s because neither workspace nor resource group is verified",
            mode,
            prior_deploy.get("instanceId"),
        )
        return False

    evidence = _live_resume_prerequisites(req, cloud_state)
    applied = False
    if rg_exists:
        required_types = (
            "microsoft.containerregistry/registries",
            "microsoft.keyvault/vaults",
            "microsoft.storage/storageaccounts",
            "microsoft.healthcareapis/workspaces",
        )
        if all(_has_azure_type(evidence, resource_type) for resource_type in required_types):
            req.skip_base_infra = True
            applied = True

    counts = evidence.get("fhirCounts") or {}
    patients = int(counts.get("patients") or 0)
    devices = int(counts.get("devices") or 0)
    exported = int(counts.get("exportedFiles") or 0)
    dicom_studies = int(counts.get("dicomStudies") or 0)

    if patients > 0 and devices > 0:
        req.reuse_patients = True
        req.skip_synthea = True
        req.skip_device_assoc = True
        applied = True
        if exported > 0:
            req.skip_fhir = True
            req.skip_fhir_export = True
        else:
            # Keep -SkipFhir false so Deploy-All runs its catch-up FHIR $export
            # without regenerating Synthea or reloading existing patients.
            req.skip_fhir = False
            req.skip_fhir_export = False

    if dicom_studies > 0:
        req.skip_dicom = True
        applied = True

    logger.info(
        "%s live fallback for %s applied=%s: base_infra=%s, reuse_patients=%s, skip_synthea=%s, skip_fhir=%s, skip_dicom=%s, patients=%s, devices=%s, exportedFiles=%s, dicomStudies=%s",
        mode,
        prior_deploy.get("instanceId"),
        applied,
        req.skip_base_infra,
        req.reuse_patients,
        req.skip_synthea,
        req.skip_fhir,
        req.skip_dicom,
        patients,
        devices,
        exported,
        dicom_studies,
    )
    return applied



def _apply_prior_success_skips(req: DeployRequest):
    """Find the most recent failed deployment with the same workspace or RG,
    and automatically skip all phases that completed successfully in it.
    """
    prior_deploy = None
    # Sort deployments by createdTime to find the most recent one
    for dep in sorted(deployments.values(), key=lambda d: d.get("createdTime", ""), reverse=True):
        if dep.get("runtimeStatus") not in ["Failed", "Terminated"]:
            continue
        cs = dep.get("customStatus", {})
        if cs.get("workspaceName") == req.fabric_workspace_name or cs.get("resourceGroupName") == req.resource_group_name:
            prior_deploy = dep
            break

    if prior_deploy:
        _apply_success_skips_from_deployment(req, prior_deploy)


@app.post("/api/deploy/start")
async def start_deploy(req: DeployRequest):
    # Continue-from-failure uses the exact failed source run. Default starts keep
    # the older auto-resume behavior: skip safe successes from the latest failed
    # deployment with the same workspace or resource group.
    if not req.phase7_only:
        if req.continue_from_instance_id:
            source_deploy = deployments.get(req.continue_from_instance_id)
            if not source_deploy:
                raise HTTPException(404, "Continuation source deployment not found")
            if source_deploy.get("runtimeStatus") not in ["Failed", "Terminated"]:
                raise HTTPException(409, "Continuation source deployment is not failed or terminated")
            _reset_continuation_skip_flags(req)
            _apply_success_skips_from_deployment(req, source_deploy, "Continue-from-failure")
            _apply_live_continuation_skips(req, source_deploy, "Continue-from-failure")
        else:
            _apply_prior_success_skips(req)

    _ensure_deployment_policy_tags(req)

    selected_synth_clinical = not req.skip_hds_pipelines and not req.skip_fhir and not req.skip_synthea
    selected_synth_imaging = not req.skip_hds_pipelines and not req.skip_dicom
    req.require_bronze_clinical_fhir = selected_synth_clinical or req.require_bronze_clinical_fhir
    req.require_bronze_imaging_dicom = selected_synth_imaging or req.require_bronze_imaging_dicom

    # Hard gate on local auth/tooling readiness before launch.
    auth_context = await asyncio.get_event_loop().run_in_executor(None, _get_auth_context_sync)
    if not auth_context.get("ready", False):
        issues = auth_context.get("issues", [])
        return func_response(
            {
                "error": "Deployment blocked: local Azure auth context is not ready.",
                "issues": issues,
                "authContext": auth_context,
            },
            status_code=422,
        )

    # Build descriptive instance ID: P<milestones>-<datetime>
    # Milestone numbers encode which progress-bar milestones are active:
    #   1 = Data Fabric Foundation, 2 = Active Patient Telemetry,
    #   3 = Multimodal Cohorting & Imaging, 4 = Connected Semantic Intelligence,
    #   5 = Bedside Alerting & Action, 6 = CMS Quality & Performance, 7 = Payer RTI & Ops
    now_local = datetime.now()
    timestamp = now_local.strftime("%Y%m%d-%H%M%S")

    # Determine active milestones from config flags
    if req.phase7_only:
        milestones = [7]
    else:
        milestones = [1]  # Milestone 1: Data Fabric Foundation (always active)
        if not req.skip_fabric:
            milestones.append(2)  # Milestone 2: Active Patient Telemetry
        if not (req.skip_dicom and req.skip_imaging and req.skip_hds_pipelines):
            milestones.append(3)  # Milestone 3: Multimodal Cohorting & Imaging
        if not (req.skip_data_agents and req.skip_ontology):
            milestones.append(4)  # Milestone 4: Connected Semantic Intelligence
        if not req.skip_activator:
            milestones.append(5)  # Milestone 5: Bedside Alerting & Action
        if not req.skip_quality_measures:
            milestones.append(6)  # Milestone 6: Population Health & Quality
        if not req.skip_phase7:
            milestones.append(7)  # Milestone 7: Payer RTI & Ops

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
            "totalPhases": 14,
            "resources": {},
            "logs": [],
            "subStepsByPhase": {},
            "workspaceName": req.fabric_workspace_name,
            "resourceGroupName": req.resource_group_name,
            "capacityName": req.capacity_name,
            "capacityResourceGroup": req.capacity_resource_group,
            "capacitySubscriptionId": req.capacity_subscription_id,
            "pauseCapacityAfterDeploy": req.pause_capacity_after_deploy,
            "continuedFrom": req.continue_from_instance_id,
            "links": {
                "azurePortal": f"https://portal.azure.com/#@/resource/subscriptions//resourceGroups/{req.resource_group_name}" if req.resource_group_name else "",
                "fabricWorkspace": f"https://app.fabric.microsoft.com/groups?experience=fabric-developer&name={req.fabric_workspace_name}" if req.fabric_workspace_name else "",
                "imagingReport": "",
                "imagingReportSettings": "",
                "ohifViewer": "",
            },
            "deployConfig": req.model_dump(),
        },
        "output": None,
    }
    deployments[instance_id] = deployment
    save_state()

    # Run deployment in background
    _create_logged_task(_run_deploy(instance_id, req), name=f"deploy:{instance_id}")

    logger.info("Deployment started: %s (workspace=%s, rg=%s)",
                instance_id, req.fabric_workspace_name, req.resource_group_name)
    return {"instanceId": instance_id, "statusUrl": f"/api/deploy/{instance_id}/status"}


@app.post("/api/deploy/{instance_id}/continue-phase7")
async def continue_phase7(instance_id: str, req: Phase7ContinuationRequest | None = None):
    dep = deployments.get(instance_id)
    if not dep:
        raise HTTPException(404, "Instance not found")
    prior_config = ((dep.get("customStatus") or {}).get("deployConfig") or {}).copy()
    if not prior_config:
        raise HTTPException(422, "No deployment configuration is stored for this run")
    prior_config.update({
        "phase7_only": True,
        "skip_phase7": False,
        "skip_payer_rti": False,
        "skip_payer_activator": False,
        "skip_ops_agent": False,
        "skip_graph_agent": False,
    })
    if req:
        if req.alert_email:
            prior_config["alert_email"] = req.alert_email
        if req.payer_ops_email:
            prior_config["payer_ops_email"] = req.payer_ops_email
        prior_config["claim_event_rate_per_minute"] = req.claim_event_rate_per_minute
    return await start_deploy(DeployRequest(**prior_config))

@app.post("/api/deploy/{instance_id}/continue-failed")
async def continue_failed_deployment(instance_id: str):
    dep = deployments.get(instance_id)
    if not dep:
        raise HTTPException(404, "Instance not found")
    if dep.get("runtimeStatus") not in {"Failed", "Terminated"}:
        raise HTTPException(409, "Only failed or terminated deployments can be continued from the last failed step")

    prior_config = ((dep.get("customStatus") or {}).get("deployConfig") or {}).copy()
    if not prior_config:
        raise HTTPException(422, "No deployment configuration is stored for this run")
    prior_config["continue_from_instance_id"] = instance_id
    return await start_deploy(DeployRequest(**prior_config))


@app.get("/api/auth/context")
async def get_auth_context(force: bool = False):
    """Return local Azure CLI + Az PowerShell authentication context.

    PowerShell startup is materially slower than Azure CLI context reads, so the
    default UI path uses a short TTL cache. Pass force=1 from Preflight Refresh
    or deployment start gates when a fresh two-tool validation is required.
    """
    cache_key = "auth_context"
    if not force:
      cached = _get_timed_cached(cache_key, 45)
      if cached is not None:
          return cached
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _get_auth_context_sync)
    _set_timed_cached(cache_key, result)
    return result


@app.get("/api/health")
async def get_health():
    auth = await asyncio.get_event_loop().run_in_executor(None, _get_auth_context_sync)
    capacities = await list_capacities(force=True)
    active_caps = [cap for cap in capacities if cap.get("state") == "Active"] if isinstance(capacities, list) else []
    return {
        "status": "ok" if auth.get("ready") else "warning",
        "backend": "online",
        "database": "ok",
        "auth": auth,
        "capacities": {"total": len(capacities) if isinstance(capacities, list) else 0, "active": len(active_caps), "items": capacities},
        "deployments": len(deployments),
        "checkedAt": now_iso(),
    }


async def _run_deploy(instance_id: str, req: DeployRequest):
    """Run Deploy-All.ps1 via subprocess, streaming output to deployment status."""
    import logging as _logging

    deployment = deployments[instance_id]
    deploy_logs: list[dict] = []

    # Per-deployment log file for on-demand phase log retrieval
    deploy_log_dir = Path(__file__).parent / "logs"
    deploy_log_dir.mkdir(exist_ok=True)
    deploy_log_file = deploy_log_dir / f"{instance_id}.jsonl"
    current_phase_name: list[str] = [""]  # mutable container for closure

    class StatusLogHandler(_logging.Handler):
        def emit(self, record: _logging.LogRecord):
            msg = self.format(record)
            level = ("success" if any(w in msg.lower() for w in ["succeeded", "deployed", "created", "completed", "ready", "provisioned", "built", "✓"])
                    else "error" if record.levelno >= _logging.ERROR
                    else "warn" if record.levelno >= _logging.WARNING
                    else "info")
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "message": msg,
                "phase": current_phase_name[0],
            }
            deploy_logs.append(entry)
            deployment["customStatus"]["logs"] = deploy_logs[-100:]
            deployment["customStatus"]["detail"] = msg
            if "WAITING_FOR_HDS" in msg:
                current_phase_name[0] = "Phase 3: HDS Deployment Detection"
                deployment["customStatus"]["currentPhase"] = current_phase_name[0]
                deployment["customStatus"]["status"] = "waiting_for_input"
                save_state()
            parsed_links = _extract_deployment_links(msg)
            if parsed_links:
                link_map = deployment["customStatus"].setdefault("links", {})
                for key, value in parsed_links.items():
                    link_map[key] = value
                resource_map = deployment["customStatus"].setdefault("resources", {})
                if parsed_links.get("imagingReport"):
                    resource_map["imaging_report_url"] = parsed_links["imagingReport"]
                if parsed_links.get("ohifViewer"):
                    resource_map["ohif_viewer_url"] = parsed_links["ohifViewer"]
            deployment["lastUpdatedTime"] = now_iso()
            # Debounce save_state: only persist every 50th log or on level changes
            if len(deploy_logs) % 50 == 0 or record.levelno >= _logging.WARNING:
                save_state()
            # Append to per-deployment log file (JSONL)
            try:
                with open(deploy_log_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception as e:
                logger.warning("Failed to write deployment log file: %s", e)

    handler = StatusLogHandler()
    handler.setLevel(_logging.INFO)
    handler.setFormatter(_logging.Formatter("%(message)s"))

    _logging.getLogger("activities.invoke_powershell").addHandler(handler)

    phases: list[dict] = []

    def _substep_warning_message(name: str, substep_detail: str) -> str:
        return f"{name}: {substep_detail}" if substep_detail else name

    def _find_phase_for_substeps(phase_name: str) -> dict:
        for phase in reversed(phases):
            if phase.get("phase") == phase_name:
                return phase
        for phase in reversed(phases):
            if phase.get("status") in {"running", "succeeded"}:
                return phase
        phase = {"phase": phase_name or "Deploy-All", "status": "running"}
        phases.append(phase)
        return phase

    def _upsert_substep(step_name: str, detail: str, duration: str) -> None:
        try:
            payload = json.loads(detail) if detail else {}
            if not isinstance(payload, dict):
                payload = {"detail": detail}
        except json.JSONDecodeError:
            payload = {"detail": detail}

        phase_name = current_phase_name[0] or deployment["customStatus"].get("currentPhase") or "Deploy-All"
        status = str(payload.get("status") or "running")
        substep_detail = str(payload.get("detail") or "")
        substep = {
            "name": step_name,
            "status": status,
            "detail": substep_detail,
            "updatedAt": now_iso(),
        }
        if duration:
            substep["duration"] = duration
        if payload.get("runId"):
            substep["runId"] = str(payload["runId"])
        if payload.get("url"):
            substep["url"] = str(payload["url"])

        phase_map = deployment["customStatus"].setdefault("subStepsByPhase", {})
        phase_substeps = phase_map.setdefault(phase_name, [])
        existing = next((item for item in phase_substeps if item.get("name") == step_name), None)
        if existing:
            prior_status = existing.get("status")
            is_terminal = prior_status in {"failed", "warning", "succeeded", "skipped"}
            is_non_terminal_update = status in {"running", "pending"}
            for key, value in substep.items():
                if key == "status" and is_terminal and is_non_terminal_update:
                    continue
                if key == "detail" and prior_status in {"failed", "warning"} and is_non_terminal_update:
                    continue
                if key in {"status", "updatedAt"} or value:
                    existing[key] = value
        else:
            phase_substeps.append(substep)
            existing = substep

        phase = _find_phase_for_substeps(phase_name)
        phase["subSteps"] = phase_substeps

        if existing.get("status") in {"failed", "warning"}:
            warnings = phase.setdefault("warnings", [])
            msg = _substep_warning_message(step_name, str(existing.get("detail") or ""))
            if msg not in warnings:
                warnings.append(msg)


    def _complete_running_substeps_for_phase(phase_name: str, duration: str = "") -> None:
        phase_map = deployment["customStatus"].setdefault("subStepsByPhase", {})
        for substep in phase_map.get(phase_name, []):
            if substep.get("status") == "running":
                substep["status"] = "succeeded"
                if duration and not substep.get("duration"):
                    substep["duration"] = duration
        for phase in phases:
            if phase.get("phase") == phase_name and isinstance(phase.get("subSteps"), list):
                for substep in phase["subSteps"]:
                    if substep.get("status") == "running":
                        substep["status"] = "succeeded"
                        if duration and not substep.get("duration"):
                            substep["duration"] = duration
    def step_callback(event: str, step_name: str, detail: str, duration: str):
        """Handle step events from PowerShell output parser."""
        if event == "step_start":
            # Track current phase for log tagging
            current_phase_name[0] = step_name
            # Mark any previously running phase as succeeded (the result line
            # for the previous step may not have been parsed yet)
            for p in phases:
                if p["status"] == "running":
                    p["status"] = "succeeded"
                    _complete_running_substeps_for_phase(p["phase"])
            phase = {"phase": step_name, "status": "running"}
            existing_substeps = deployment["customStatus"].setdefault("subStepsByPhase", {}).get(step_name)
            if existing_substeps:
                phase["subSteps"] = existing_substeps
            phases.append(phase)
            deployment["customStatus"]["currentPhase"] = step_name
            deployment["customStatus"]["status"] = "running"
        elif event == "step_succeeded":
            # Find the last running phase and mark it
            for p in reversed(phases):
                if p["status"] == "running":
                    p["status"] = "succeeded"
                    p["duration"] = duration
                    _complete_running_substeps_for_phase(p["phase"], duration)
                    break
        elif event == "step_failed":
            for p in reversed(phases):
                if p["status"] == "running":
                    p["status"] = "failed"
                    p["detail"] = detail
                    p["duration"] = duration
                    break
        elif event == "substep_update":
            _upsert_substep(step_name, detail, duration)
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
        # Keep totalPhases fixed; do not shrink to len(phases)
        deployment["lastUpdatedTime"] = now_iso()
        save_state()

    try:
        from activities.invoke_powershell import run_deploy

        config = req.model_dump()
        config["instance_id"] = instance_id
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

        if deployment.get("runtimeStatus") == "Terminated" or deployment.get("customStatus", {}).get("status") == "cancelled":
            logger.info("Deployment %s finished after cancellation; preserving terminated status", instance_id)
            return

        deployment["runtimeStatus"] = "Completed"
        deployment["customStatus"]["status"] = "succeeded"
        deployment["customStatus"]["currentPhase"] = "Deployment Complete"
        completed = [p for p in phases if p["status"] == "succeeded"]
        deployment["customStatus"]["completedPhases"] = len(completed)
        deployment["customStatus"]["resources"] = result.get("resources", {})
        # Compute duration from sum of phase durations (excludes HDS manual wait)
        phase_duration_sum = 0.0
        for p in phases:
            d = p.get("duration")
            if isinstance(d, str) and "min" in d:
                try:
                    phase_duration_sum += float(d.replace("min", "").strip()) * 60
                except ValueError:
                    pass
            elif isinstance(d, (int, float)):
                phase_duration_sum += d
        if phase_duration_sum > 0:
            duration = phase_duration_sum
        else:
            duration = result.get("duration_seconds", (datetime.now(timezone.utc) - deploy_start).total_seconds())
        deployment["customStatus"]["durationSeconds"] = round(duration, 1)
        deployment["output"] = {
            "status": "succeeded",
            "phases": phases,
            "resources": result.get("resources", {}),
        }
        final_links = deployment.get("customStatus", {}).get("links", {})
        if isinstance(final_links, dict):
            if final_links.get("imagingReport"):
                deployment["output"]["resources"]["imaging_report_url"] = final_links["imagingReport"]
            if final_links.get("ohifViewer"):
                deployment["output"]["resources"]["ohif_viewer_url"] = final_links["ohifViewer"]
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
    _backfill_links_from_logs(instance_id, deployments[instance_id])
    _backfill_successful_steps_from_state_tracking(instance_id, deployments[instance_id])
    if _normalize_successful_deployment_progress(instance_id, deployments[instance_id]):
        save_state()
    _normalize_completed_phase_substeps(deployments[instance_id])
    return deployments[instance_id]


@app.get("/api/deploy/{instance_id}/logs")
async def get_phase_logs(instance_id: str, phase: str = ""):
    """Return logs for a specific phase from the per-deployment log file.

    Query params:
      phase — exact phase name to filter (e.g. "PHASE 2: FABRIC RTI")
              If empty, returns all logs.
    """
    log_file = Path(__file__).parent / "logs" / f"{instance_id}.jsonl"
    if not log_file.exists():
        return []

    logs = []
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if not phase or entry.get("phase", "") == phase:
                    logs.append(entry)
    except Exception:
        pass
    return logs


from fastapi.responses import StreamingResponse


@app.get("/api/deploy/{instance_id}/cloud-state")
async def get_deploy_cloud_state(instance_id: str):
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")
    cs = deployments[instance_id].get("customStatus", {})
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _cloud_state_sync, cs.get("workspaceName", ""), cs.get("resourceGroupName", ""))


@app.get("/api/teardown/{instance_id}/cloud-state")
async def get_teardown_cloud_state(instance_id: str):
    return await get_deploy_cloud_state(instance_id)


@app.post("/api/deploy/{instance_id}/validate")
async def validate_deployment(instance_id: str):
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")
    cs = deployments[instance_id].get("customStatus", {})
    loop = asyncio.get_event_loop()
    resources = await loop.run_in_executor(None, _get_deployed_resources_sync, cs.get("workspaceName", ""), cs.get("resourceGroupName", ""))
    validation = _validation_from_resources(resources, is_teardown=False)
    if _reconcile_deployment_completion_from_validation(instance_id, deployments[instance_id], validation):
        save_state()
    return validation


@app.post("/api/teardown/{instance_id}/validate")
async def validate_teardown(instance_id: str):
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")
    cs = deployments[instance_id].get("customStatus", {})
    loop = asyncio.get_event_loop()
    resources = await loop.run_in_executor(None, _get_deployed_resources_sync, cs.get("workspaceName", ""), cs.get("resourceGroupName", ""))
    return _validation_from_resources(resources, is_teardown=True)


@app.get("/api/deploy/{instance_id}/logs/stream")
async def stream_phase_logs(instance_id: str, phase: str = ""):
    """Stream logs for a deployment in real-time using SSE (EventSource)."""
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")

    async def log_generator():
        log_file = Path(__file__).parent / "logs" / f"{instance_id}.jsonl"

        # Wait for log file to be created up to 10 seconds
        for _ in range(20):
            if log_file.exists():
                break
            await asyncio.sleep(0.5)

        if not log_file.exists():
            yield "data: {\"message\": \"Log file not found yet.\", \"level\": \"info\"}\n\n"
            return

        # Keep track of file read position
        position = 0
        while True:
            # Check deployment current status
            deployment = deployments.get(instance_id)
            is_finished = deployment and deployment.get("runtimeStatus") in ["Completed", "Failed", "Terminated"]

            try:
                with open(log_file, "r", encoding="utf-8") as f:
                    f.seek(position)
                    lines = f.readlines()
                    position = f.tell()

                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        if not phase or entry.get("phase", "") == phase:
                            yield f"data: {json.dumps(entry)}\n\n"
                    except Exception:
                        pass
            except Exception as e:
                yield f"data: {json.dumps({'message': f'Error reading logs: {e}', 'level': 'error'})}\n\n"
                break

            if is_finished:
                # One last check for any trailing lines written after our last check
                try:
                    with open(log_file, "r", encoding="utf-8") as f:
                        f.seek(position)
                        lines = f.readlines()
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            if not phase or entry.get("phase", "") == phase:
                                yield f"data: {json.dumps(entry)}\n\n"
                        except Exception:
                            pass
                except Exception:
                    pass
                break

            await asyncio.sleep(0.5)

    return StreamingResponse(log_generator(), media_type="text/event-stream")


def _backfill_links_from_logs(instance_id: str, deployment: dict) -> None:
    """Populate typed links for historical runs by scanning persisted log lines once."""
    custom_status = deployment.get("customStatus", {})
    if not isinstance(custom_status, dict):
        return

    links = custom_status.setdefault("links", {})
    if not isinstance(links, dict):
        return

    # Skip if we've already backfilled or links already exist.
    if custom_status.get("linksBackfilled"):
        return
    if links.get("imagingReport") and links.get("ohifViewer"):
        custom_status["linksBackfilled"] = True
        return

    log_file = Path(__file__).parent / "logs" / f"{instance_id}.jsonl"
    if not log_file.exists():
        return

    try:
        with open(log_file, "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                except Exception:
                    continue
                msg = entry.get("message", "")
                if not isinstance(msg, str):
                    continue
                parsed = _extract_deployment_links(msg)
                if not parsed:
                    continue
                for key, value in parsed.items():
                    links[key] = value

        resources = custom_status.setdefault("resources", {})
        if isinstance(resources, dict):
            if links.get("imagingReport"):
                resources["imaging_report_url"] = links["imagingReport"]
            if links.get("ohifViewer"):
                resources["ohif_viewer_url"] = links["ohifViewer"]

        custom_status["linksBackfilled"] = True
        deployment["lastUpdatedTime"] = now_iso()
        save_state()
    except Exception:
        # Non-fatal: status endpoint should still return deployment details.
        pass


def _parse_state_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None



def _normalize_successful_deployment_progress(instance_id: str, deployment: dict) -> bool:
    """Show successful completed deployment plans as fully complete.

    Deploy-All streams only steps that actually ran. Resumed/repair runs skip
    already-completed components, so persisted completedPhases can be lower than
    totalPhases even though the plan finished successfully. History uses these
    counters directly; normalize successful deployment runs so skipped plan
    components do not look like missed phases.
    """
    if instance_id.lower().startswith("teardown"):
        return False
    if deployment.get("runtimeStatus") != "Completed":
        return False
    custom_status = deployment.get("customStatus")
    if not isinstance(custom_status, dict):
        return False
    output = deployment.get("output") if isinstance(deployment.get("output"), dict) else {}
    if custom_status.get("status") != "succeeded" and output.get("status") != "succeeded":
        return False
    total = custom_status.get("totalPhases")
    if not isinstance(total, int) or total <= 0:
        return False
    if custom_status.get("completedPhases") == total:
        return False
    custom_status["completedPhases"] = total
    deployment["lastUpdatedTime"] = now_iso()
    return True

def _normalize_completed_phase_substeps(deployment: dict) -> None:
    """Do not show stale running substeps under completed phases."""
    output = deployment.get("output")
    custom_status = deployment.get("customStatus", {})
    if not isinstance(output, dict) or not isinstance(custom_status, dict):
        return
    phases = output.get("phases")
    phase_map = custom_status.get("subStepsByPhase")
    if not isinstance(phases, list) or not isinstance(phase_map, dict):
        return
    changed = False
    completed_names = {phase.get("phase") for phase in phases if isinstance(phase, dict) and phase.get("status") in {"succeeded", "skipped"}}
    for phase_name in completed_names:
        substeps = phase_map.get(phase_name)
        if isinstance(substeps, list):
            for substep in substeps:
                if isinstance(substep, dict) and substep.get("status") == "running":
                    substep["status"] = "succeeded"
                    changed = True
    for phase in phases:
        if not isinstance(phase, dict) or phase.get("status") not in {"succeeded", "skipped"}:
            continue
        substeps = phase.get("subSteps")
        if isinstance(substeps, list):
            for substep in substeps:
                if isinstance(substep, dict) and substep.get("status") == "running":
                    substep["status"] = "succeeded"
                    changed = True
    if changed:
        deployment["lastUpdatedTime"] = now_iso()
        save_state()


def _backfill_successful_steps_from_state_tracking(instance_id: str, deployment: dict) -> None:
    """Add successful state-tracking steps that belong to this workspace.

    Deploy-All state tracking is the cross-run ledger for a workspace. A
    resumed or repair run may intentionally skip work that succeeded in an
    earlier attempt, and a targeted repair can run directly from PowerShell
    after orchestration completes. The monitor should show those real completed
    steps as succeeded instead of rendering selected milestones as skipped or
    pending just because the final orchestrated process did not stream them.
    """
    custom_status = deployment.get("customStatus", {})
    if not isinstance(custom_status, dict):
        return
    workspace_name = custom_status.get("workspaceName")
    if not workspace_name:
        return
    output = deployment.get("output")
    if not isinstance(output, dict):
        return
    phases = output.setdefault("phases", [])
    if not isinstance(phases, list):
        return

    state_file = Path(__file__).resolve().parent.parent / "state-tracking" / f".deployment-state-{workspace_name}.json"
    if not state_file.exists():
        return

    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return

    existing_names = {str(phase.get("phase", "")).casefold() for phase in phases if isinstance(phase, dict)}
    added = False
    for phase_entry in state.get("phases") or []:
        if not isinstance(phase_entry, dict):
            continue
        resources = phase_entry.get("resources") if isinstance(phase_entry.get("resources"), dict) else {}
        if resources.get("FabricWorkspaceName") and resources.get("FabricWorkspaceName") != workspace_name:
            continue
        for step in phase_entry.get("steps") or []:
            if not isinstance(step, dict) or not step.get("success"):
                continue
            step_name = str(step.get("name") or "").strip()
            if not step_name or step_name.casefold() in existing_names:
                continue
            phases.append({
                "phase": step_name,
                "status": "succeeded",
                "duration": step.get("duration"),
                "reconciledFrom": "state-tracking",
                "reconciledAt": phase_entry.get("timestamp"),
            })
            existing_names.add(step_name.casefold())
            added = True

    if added:
        completed = sum(1 for phase in phases if isinstance(phase, dict) and phase.get("status") in {"succeeded", "skipped"})
        custom_status["completedPhases"] = completed
        deployment["lastUpdatedTime"] = now_iso()
        save_state()


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


@app.get("/api/deploy/{instance_id}/after-action-report")
async def get_after_action_report(instance_id: str):
    """Compile and return the After Action Security & Resources Report metadata."""
    if instance_id not in deployments:
        raise HTTPException(404, "Instance not found")

    dep = deployments[instance_id]
    custom_status = dep.get("customStatus", {})
    ws_name = custom_status.get("workspaceName", "")
    rg_name = custom_status.get("resourceGroupName", "")
    cfg = custom_status.get("deployConfig", {})
    admin_group = cfg.get("admin_security_group", "") or "sg-msft-hds-dicom-project"

    # Call the existing sync function to get actual resources
    loop = asyncio.get_event_loop()
    resources = await loop.run_in_executor(None, _get_deployed_resources_sync, ws_name, rg_name)

    # Derive appNamePrefix
    app_name_prefix = "masimo"
    if ws_name:
        import re
        sanitized = "".join(c.lower() for c in ws_name if c.isalnum())
        if sanitized and sanitized[0].isdigit():
            sanitized = "m" + sanitized
        sanitized = sanitized[:8]
        while len(sanitized) < 3:
            sanitized += "m"
        if re.match(r"^[a-z][a-z0-9]{2,7}$", sanitized):
            app_name_prefix = sanitized

    # Compile the After Action security schema mappings
    # Map each resource type to its identity strategy and vault secrets
    security_report = {
        "adminGroup": admin_group,
        "keyVaultName": next((r["name"] for r in resources["azure"] if r["type"].lower() == "vaults"), f"{app_name_prefix}-kv"),
        "azurePortalUrl": custom_status.get("links", {}).get("azurePortal", ""),
        "fabricWorkspaceUrl": custom_status.get("links", {}).get("fabricWorkspace", ""),
        "resources": []
    }

    # 1. Map Azure Resources
    for r in resources["azure"]:
        name = r["name"]
        rtype = r["type"].lower()
        identity = "System-Assigned Managed Identity"
        credentials = "None (Entra ID RBAC / Service-to-Service)"
        details = "Service-to-service communication is handled securely via Azure Managed Identity without stored secrets."

        if rtype == "vaults":
            identity = "System-Assigned Managed Identity"
            credentials = "SpnClientId, SpnClientSecret, SpnTenantId, EventHubConnStr (Secure Secrets)"
            details = f"Securely stores connection strings and SPN secrets. Fully governed by RBAC roles assigned to Admin Security Group '{admin_group}'."
        elif rtype == "namespaces" or "eventhub" in rtype:
            identity = "System-Assigned Managed Identity / SAS Rule"
            credentials = "EventHubConnStr (Key Vault Secret)"
            details = "Uses Managed Identity for device emulator stream ingestion. SAS authorization rule fallback is stored in Key Vault."
        elif rtype == "registries":
            identity = "System-Assigned Managed Identity"
            credentials = "None (AcrPull/AcrPush RBAC Roles)"
            details = "Container build and image retrieval utilize Managed Identity, with pushes/pulls secured via RBAC roles."
        elif rtype == "containergroups":
            identity = "System-Assigned Managed Identity"
            credentials = "EventHubConnStr (Key Vault secret reference)"
            details = "Active device emulator container group accesses Event Hub using a System-Assigned Managed Identity."

        security_report["resources"].append({
            "name": name,
            "category": "Azure",
            "type": r["type"],
            "identity": identity,
            "credentialLocation": "Azure Key Vault" if rtype == "vaults" or rtype == "namespaces" else "None",
            "credentialDetails": credentials,
            "accessControlDetails": details
        })

    # 2. Map Fabric Resources
    for r in resources["fabric"]:
        name = r["name"]
        rtype = r["type"].lower()
        identity = "Workspace Identity / Owner Context"
        credentials = "None (Entra ID SSO / Service-to-Service)"
        details = "Microsoft Fabric Workspace Identity allows notebooks, Eventstreams, and KQL databases to interoperate securely without connection strings."

        if rtype == "semanticmodel":
            identity = "Service Principal (SPN) Fixed Identity"
            credentials = "SpnClientSecret (Azure Key Vault Secret)"
            details = "Automated Direct Lake data connections utilize the SPN secrets retrieved from Key Vault to query OneLake securely."
        elif rtype == "reflex":
            identity = "Workspace Identity"
            credentials = "None (Fabric Native Integration)"
            details = "Data Activator alerts operate entirely within the workspace security boundary to route care team notifications."

        security_report["resources"].append({
            "name": name,
            "category": "Fabric",
            "type": r["type"],
            "identity": identity,
            "credentialLocation": "Azure Key Vault (Secret)" if rtype == "semanticmodel" else "Workspace Boundary",
            "credentialDetails": credentials,
            "accessControlDetails": details
        })

    return security_report


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

            ws_result = fabric.call("GET", "/workspaces", max_retries=1)
            workspaces = ws_result.get("value", []) if ws_result else []
            ws_match = next((w for w in workspaces if w.get("displayName") == ws_name), None)

            if ws_match:
                ws_id = ws_match["id"]
                result["workspace"] = {
                    "name": ws_name,
                    "id": ws_id,
                    "url": f"https://app.fabric.microsoft.com/groups/{ws_id}",
                }
                items = fabric.list_items(ws_id, max_retries=1)
                for item in items:
                    result["fabric"].append({
                        "name": item.get("displayName", ""),
                        "type": item.get("type", "Unknown"),
                        "id": item.get("id", ""),
                    })
                logger.info("Found %d Fabric items in workspace '%s'", len(items), ws_name)
        except Exception as e:
            persisted = _workspace_state_from_persisted_link(ws_name)
            if persisted:
                result["workspace"] = {
                    "name": ws_name,
                    "id": persisted["id"],
                    "url": persisted["url"],
                    "status": persisted["status"],
                    "warning": persisted["warning"],
                }
                logger.info("Fabric resources for '%s' live query blocked; using persisted workspace id %s", ws_name, persisted["id"])
            else:
                logger.warning("Failed to query Fabric workspace '%s': %s", ws_name, e)

    return result


@app.post("/api/deploy/{instance_id}/resume-hds")
async def resume_hds(instance_id: str):
    if instance_id in deployments:
        dep = deployments[instance_id]
        dep["customStatus"]["status"] = "running"
        dep["customStatus"]["detail"] = "Resuming deployment..."
        save_state()
        
        # Touch resume flag file in logs directory
        resume_dir = Path(__file__).parent / "logs"
        resume_dir.mkdir(exist_ok=True)
        resume_file = resume_dir / f"{instance_id}.resume"
        try:
            resume_file.touch()
            logger.info("Touched HDS resume file: %s", resume_file)
        except Exception as e:
            logger.warning("Failed to touch HDS resume file: %s", e)
            
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
    changed = False
    for instance_id, deployment in deployments.items():
        if _normalize_successful_deployment_progress(instance_id, deployment):
            changed = True
    if changed:
        save_state()
    return sorted(
        deployments.values(),
        key=lambda dep: dep.get("createdTime") or dep.get("lastUpdatedTime") or "",
        reverse=True,
    )


@app.post("/api/teardown/reconcile")
async def reconcile_teardowns_endpoint():
    count = reconcile_interrupted_teardowns()
    return {"reconciled": count}


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
async def list_subscriptions(force: bool = False):
    """List Azure subscriptions available to the current user."""
    try:
        cache_key = "subscriptions"
        if not force:
            cached = _get_timed_cached(cache_key, 180)
            if cached is not None:
                return cached
        subs = _list_subscriptions_sync(force=force)
        # Sort so default subscription comes first
        subs.sort(key=lambda s: not s.get("isDefault", False))
        result = [{"id": s["id"], "name": s["name"]} for s in subs]
        _set_timed_cached(cache_key, result)
        return result
    except Exception as e:
        logger.error("Failed to list subscriptions: %s", e)
        return []


def _list_subscriptions_sync(force: bool = False) -> list[dict]:
    if not force:
        cached = _get_timed_cached("subscriptions_raw", 180)
        if cached is not None:
            return cached
    result = _az_run(
        ["az", "account", "list", "--query", "[].{id:id, name:name, isDefault:isDefault}", "-o", "json"],
        check=True,
    )
    subs = json.loads(result.stdout)
    subs.sort(key=lambda s: not s.get("isDefault", False))
    _set_timed_cached("subscriptions_raw", subs)
    return subs


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
    _create_logged_task(_run_scan_job(scan_id, subscription_id), name=f"resource-scan:{scan_id}")
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

    # ── Collect previously deployed workspace names from DB ────────
    previously_deployed_ws_names: set[str] = set()
    for dep in deployments.values():
        cs = dep.get("customStatus", {})
        ws_name = cs.get("workspaceName", "")
        if ws_name and cs.get("runType") != "teardown":
            previously_deployed_ws_names.add(ws_name)
    if previously_deployed_ws_names:
        logger.info("Previously deployed workspaces from DB: %s", previously_deployed_ws_names)

    from concurrent.futures import ThreadPoolExecutor

    # Helper for Fabric scanning
    def scan_fabric_workspaces():
        fabric_results = []
        try:
            emit_status("fabric", "Scanning Fabric workspaces...")
            from shared.fabric_client import FabricClient
            fabric = FabricClient()

            live_workspace_query_blocked = False
            stale_workspace_cache_used = False
            try:
                ws_result = fabric.call("GET", "/workspaces", max_retries=1)
                _set_timed_cached("fabric:workspaces", ws_result)
            except Exception as ex:
                live_workspace_query_blocked = True
                logger.warning(
                    "Fabric workspace scan live query failed; not using deployment history or stale workspace cache as teardown candidates: %s",
                    ex,
                )
                ws_result = {"value": []}

            workspaces = live_fabric_workspaces_for_teardown(
                ws_result.get("value", []) if ws_result else [],
                previously_deployed_ws_names,
            )

            def check_workspace(ws):
                name = ws.get("displayName", "")
                ws_id = ws.get("id", "")
                persisted_only = bool(ws.get("_persistedOnly"))
                try:
                    items_cache_key = f"fabric:items:{ws_id}"
                    items = _get_timed_cached(items_cache_key, 120)
                    item_scan_blocked = False
                    stale_items_used = False
                    if items is None:
                        try:
                            if persisted_only or live_workspace_query_blocked:
                                raise RuntimeError("live Fabric item query skipped because workspace enumeration is unavailable")
                            items = fabric.list_items(ws_id, max_retries=1)
                            _set_timed_cached(items_cache_key, items)
                        except Exception as item_ex:
                            items = _get_stale_timed_cached(items_cache_key)
                            if items is not None:
                                stale_items_used = True
                                logger.warning("Fabric item scan for '%s' failed; using stale item cache: %s", name, item_ex)
                            else:
                                item_scan_blocked = True
                                items = []
                                logger.warning("Fabric item scan for '%s' unavailable: %s", name, item_ex)

                    item_count = len(items)
                    item_types: dict[str, int] = {}
                    for item in items:
                        t = item.get("type", "Unknown")
                        item_types[t] = item_types.get(t, 0) + 1

                    has_hds = any(i.get("type") == "Healthcaredatasolution" for i in items)
                    is_previously_deployed = name in previously_deployed_ws_names

                    if not has_hds and not is_previously_deployed:
                        return None

                    eventhouse_item = next(
                        (i for i in items
                         if i.get("type") == "Eventhouse" and "masimo" in i.get("displayName", "").lower()),
                        None,
                    )
                    has_eventhouse = eventhouse_item is not None

                    artifact_list = []
                    for t in sorted(item_types.keys()):
                        count = item_types[t]
                        if count > 3:
                            artifact_list.append(f"{t}: (×{count})")
                        else:
                            matching_names = [i.get("displayName", "") for i in items if i.get("type") == t]
                            artifact_list.append(f"{t}: {', '.join(matching_names)}")

                    scan_notes = []
                    if live_workspace_query_blocked:
                        scan_notes.append("live Fabric workspace API blocked")
                    if stale_workspace_cache_used:
                        scan_notes.append("workspace list from stale cache")
                    if stale_items_used:
                        scan_notes.append("item list from stale cache")
                    if item_scan_blocked:
                        scan_notes.append("item inventory unavailable")
                    if persisted_only:
                        scan_notes.append("workspace recovered from deployment history")
                    if scan_notes:
                        artifact_list.append(f"Scan note: {'; '.join(scan_notes)}")

                    has_fn_clinical_alerts = False
                    if has_eventhouse and eventhouse_item:
                        try:
                            kql_db_item = next(
                                (i for i in items if i.get("type") == "KQLDatabase"),
                                None,
                            )
                            if kql_db_item:
                                kql_cache_key = f"fabric:kqlfn:{ws_id}:{kql_db_item.get('id', '')}"
                                cached_fn_check = _get_timed_cached(kql_cache_key, 120)
                                if cached_fn_check is None:
                                    cached_fn_check = _get_stale_timed_cached(kql_cache_key)
                                if cached_fn_check is not None:
                                    has_fn_clinical_alerts = bool(cached_fn_check)
                                elif not live_workspace_query_blocked:
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
                                        _set_timed_cached(kql_cache_key, has_fn_clinical_alerts)
                        except Exception as kql_e:
                            logger.warning("Could not check fn_ClinicalAlerts in '%s': %s", name, kql_e)

                    detection_qualified = has_hds and has_eventhouse and has_fn_clinical_alerts
                    qualified = detection_qualified or is_previously_deployed

                    missing = []
                    if not has_eventhouse:
                        missing.append("MasimoEventhouse")
                    if not has_fn_clinical_alerts:
                        missing.append("fn_ClinicalAlerts")
                    if not has_hds:
                        missing.append("HDS")

                    if detection_qualified:
                        detail = f"Full deployment — {item_count} Fabric items"
                    elif is_previously_deployed and not detection_qualified:
                        detail = f"Previously deployed workspace — {item_count} Fabric items"
                        if missing and not item_scan_blocked:
                            detail += f" (missing: {', '.join(missing)})"
                        if item_scan_blocked:
                            detail += " (live Fabric inventory unavailable)"
                    else:
                        detail = f"Partial deployment — missing: {', '.join(missing)}"

                    status = "full" if detection_qualified else "partial"

                    return {
                        "type": "fabric",
                        "name": name,
                        "id": ws_id,
                        "status": status,
                        "detail": detail,
                        "resourceCount": item_count,
                        "expectedCount": item_count,
                        "matchedArtifacts": artifact_list,
                        "qualified": qualified,
                        "previouslyDeployed": is_previously_deployed,
                        "detectedArtifacts": {
                            "hasHDS": has_hds,
                            "hasEventhouse": has_eventhouse,
                            "hasFnClinicalAlerts": has_fn_clinical_alerts,
                        },
                    }
                except Exception as ex:
                    logger.warning("Failed to check workspace '%s': %s", name, ex)
                    return None

            with ThreadPoolExecutor(max_workers=6) as ws_executor:
                fabric_results = [r for r in ws_executor.map(check_workspace, workspaces) if r is not None]
        except Exception as e:
            logger.error("Fabric scan failed: %s", e)
        return fabric_results

    # Helper for Azure RG scanning
    def scan_azure_rgs():
        azure_results = []
        try:
            emit_status("azure", "Scanning Azure resource groups with Azure Resource Graph...")
            safe_subscription = subscription_id.replace("'", "''")
            query = (
                "Resources "
                "| where resourceGroup startswith 'rg-med' or resourceGroup startswith 'rg-medtech' "
            )
            if safe_subscription:
                query += f"| where subscriptionId =~ '{safe_subscription}' "
            query += (
                "| summarize resourceCount=count(), artifacts=make_list(pack('name', name, 'type', type), 200) "
                "  by resourceGroup, subscriptionId "
                "| order by resourceGroup asc"
            )

            graph_args = ["az", "graph", "query", "-q", query, "--first", "1000", "-o", "json"]
            if subscription_id:
                graph_args += ["--subscriptions", subscription_id]
            result = _az_run(graph_args, check=True, timeout=30)
            rows = json.loads(result.stdout or "{}").get("data", [])

            for row in rows:
                rg_name = row.get("resourceGroup", "")
                sub_id = row.get("subscriptionId", subscription_id)
                resources = row.get("artifacts", []) or []
                res_count = int(row.get("resourceCount", len(resources)) or 0)
                artifact_list = [f"{str(r.get('type', '')).split('/')[-1]}: {r.get('name', '')}" for r in resources]
                status = "full" if res_count >= 10 else "partial"
                azure_results.append({
                    "type": "azure",
                    "name": rg_name,
                    "id": f"/subscriptions/{sub_id}/resourceGroups/{rg_name}",
                    "status": status,
                    "detail": f"{'Full' if status == 'full' else 'Partial'} Azure deployment — {res_count} resources",
                    "resourceCount": res_count,
                    "expectedCount": 12,
                    "matchedArtifacts": artifact_list,
                    "subscription": sub_id,
                })
        except Exception as e:
            logger.error("Azure Resource Graph scan failed: %s", e)
        return azure_results

    # Run Fabric and Azure scans concurrently!
    with ThreadPoolExecutor(max_workers=2) as main_executor:
        fabric_future = main_executor.submit(scan_fabric_workspaces)
        azure_future = main_executor.submit(scan_azure_rgs)
        fabric_candidates = fabric_future.result()
        azure_candidates = azure_future.result()

    # Emit all results sequentially to keep callback and candidates array clean and thread-safe
    for fc in fabric_candidates:
        emit_candidate(fc, "fabric", f"Discovered Fabric workspace: {fc['name']}")

    for ac in azure_candidates:
        emit_candidate(ac, "azure", f"Discovered Azure resource group: {ac['name']}")

    # ── Scan for SPNs matching workspace names ─────────────────────
    emit_status("spn", "Scanning Entra workspace identities...")
    fabric_names = {c["name"] for c in candidates if c["type"] == "fabric"}
    spn_workspace_names = fabric_names | previously_deployed_ws_names
    seen_spn_ids = set()

    def check_spn(ws_name):
        spn_cache_key = f"spn:{ws_name.lower()}"
        cached = _get_timed_cached(spn_cache_key, 600)
        if cached is not None:
            return cached

        try:
            identities = []
            app_result = _az_run(
                ["az", "ad", "app", "list", "--display-name", ws_name,
                 "--query", "[].{appId:appId, displayName:displayName, id:id}", "-o", "json"],
                check=True,
            )
            for app in json.loads(app_result.stdout or "[]"):
                app["objectType"] = "appRegistration"
                identities.append(app)

            sp_result = _az_run(
                ["az", "ad", "sp", "list", "--display-name", ws_name,
                 "--query", "[].{appId:appId, displayName:displayName, id:id}", "-o", "json"],
                check=True,
            )
            for sp in json.loads(sp_result.stdout or "[]"):
                sp["objectType"] = "servicePrincipal"
                identities.append(sp)

            _set_timed_cached(spn_cache_key, identities)
            return identities
        except Exception as ex:
            stale = _get_stale_timed_cached(spn_cache_key)
            if stale is not None:
                logger.warning("Entra identity scan for '%s' failed; using stale identity cache: %s", ws_name, ex)
                return stale
            logger.warning("Entra identity scan for '%s' failed: %s", ws_name, ex)
            return []

    with ThreadPoolExecutor(max_workers=4) as spn_executor:
        spn_map_results = list(spn_executor.map(check_spn, sorted(spn_workspace_names)))

    for spns in spn_map_results:
        for spn in spns:
            spn_id = spn.get("id", "")
            if spn_id in seen_spn_ids:
                continue
            seen_spn_ids.add(spn_id)

            ws_exists = spn.get("displayName", "") in fabric_names
            status = "active" if ws_exists else "orphaned"

            candidate = {
                "type": "spn",
                "name": spn.get("displayName", "Unknown"),
                "id": spn_id,
                "status": status,
                "detail": f"Workspace identity {('app registration' if spn.get('objectType') == 'appRegistration' else 'service principal')} ({'workspace exists' if ws_exists else 'workspace deleted'}) — appId: {spn.get('appId', 'unknown')}",
                "matchedArtifacts": [f"{('App Registration' if spn.get('objectType') == 'appRegistration' else 'Service Principal')}: {spn.get('displayName', '')} (appId: {spn.get('appId', '')})"],
            }
            emit_candidate(candidate, "spn", f"Discovered Entra identity: {candidate['name']}")

    emit_status("complete", f"Scan complete — {len(candidates)} candidates discovered")
    return candidates


# ── Azure Health Data Services FHIR region validation ──────────────────

FHIR_REGION_CACHE_KEY = "fhir_regions"
FHIR_REGION_PROVIDER_QUERY = "resourceTypes[?resourceType=='workspaces/fhirservices'].locations[]"
FHIR_FALLBACK_REGIONS = [
    "australiaeast", "canadacentral", "centralindia", "eastus", "eastus2",
    "francecentral", "germanywestcentral", "japaneast", "koreacentral",
    "northcentralus", "northeurope", "qatarcentral", "southcentralus",
    "southeastasia", "swedencentral", "switzerlandnorth", "uksouth",
    "westcentralus", "westeurope", "westus2", "westus3",
]


def _normalize_azure_location(location: str) -> str:
    """Convert an Azure display location ("East US") to ARM form ("eastus")."""
    return re.sub(r"\s+", "", location).lower()


@app.get("/api/scan/fhir-regions")
@app.get("/api/scan/ahds-regions")
async def list_fhir_regions(force: bool = False):
    """Return Azure regions where AHDS FHIR services are deployable."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _list_fhir_regions_sync, force)


def _list_fhir_regions_sync(force: bool = False) -> list[str]:
    """Query ARM for AHDS FHIR service locations, with stale/offline fallback."""
    if not force:
        cached = _get_timed_cached(FHIR_REGION_CACHE_KEY, 86400)
        if cached is not None:
            return cached

    try:
        proc = _az_run(
            [
                "az", "provider", "show", "--namespace", "Microsoft.HealthcareApis",
                "--query", FHIR_REGION_PROVIDER_QUERY,
                "-o", "json",
            ],
            check=True,
        )
        regions = json.loads(proc.stdout or "[]")
        result = sorted({_normalize_azure_location(region) for region in regions if region})
        if not result:
            raise ValueError("Azure provider returned no AHDS FHIR regions")
        _set_timed_cached(FHIR_REGION_CACHE_KEY, result)
        return result
    except Exception as e:
        stale = _get_stale_timed_cached(FHIR_REGION_CACHE_KEY)
        if stale:
            logger.warning("Failed to query AHDS FHIR regions; using stale cache: %s", e)
            return stale
        logger.warning("Failed to query AHDS FHIR regions; using fallback list: %s", e)
        return FHIR_FALLBACK_REGIONS.copy()


# ── Fabric Capacity API ────────────────────────────────────────────────

@app.get("/api/scan/capacities")
async def list_capacities(subscription_id: str = "", force: bool = False):
    """List Fabric capacities in the requested or all accessible subscriptions."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _list_capacities_sync, subscription_id, force)
    return result


def _list_capacities_sync(subscription_id: str, force: bool = False) -> list:
    """Query Fabric capacities across all accessible subscriptions or a specific one."""
    cache_key = f"capacities:{subscription_id or 'all'}"
    if not force:
        cached = _get_timed_cached(cache_key, 120)
        if cached is not None:
            return cached
    try:
        subscriptions = _list_subscriptions_sync()
        subscription_names = {sub.get("id", ""): sub.get("name", "") for sub in subscriptions}

        query = (
            "Resources "
            "| where type =~ 'microsoft.fabric/capacities' "
            "| project name, id, resourceGroup, location, subscriptionId, "
            "sku=tostring(sku.name), state=tostring(properties.state)"
        )
        if subscription_id:
            safe_subscription_id = subscription_id.replace("'", "''")
            query += f" | where subscriptionId =~ '{safe_subscription_id}'"

        proc = _az_run(
            ["az", "graph", "query", "-q", query, "--first", "1000", "-o", "json"],
            timeout=30,
        )

        capacities: list[dict] = []
        if proc.returncode == 0:
            graph_result = json.loads(proc.stdout or "{}")
            for capacity in graph_result.get("data", []):
                sub_id = capacity.get("subscriptionId", "")
                capacities.append(
                    {
                        "name": capacity.get("name", ""),
                        "id": capacity.get("id", ""),
                        "state": capacity.get("state", "Unknown") or "Unknown",
                        "sku": capacity.get("sku", ""),
                        "resourceGroup": capacity.get("resourceGroup", ""),
                        "location": capacity.get("location", ""),
                        "subscription": sub_id,
                        "subscriptionName": subscription_names.get(sub_id, sub_id),
                    }
                )
        else:
            logger.warning(
                "Azure Resource Graph capacity query failed, falling back to az fabric capacity list: %s",
                (proc.stderr or "unknown error").strip()[:400],
            )
            fallback_subscriptions = [
                next(
                    (sub for sub in subscriptions if sub.get("id") == subscription_id),
                    {"id": subscription_id, "name": subscription_id, "isDefault": False},
                )
            ] if subscription_id else subscriptions[:12]

            seen_capacity_ids: set[str] = set()
            for sub in fallback_subscriptions:
                sub_id = sub.get("id", "")
                sub_name = sub.get("name", sub_id)
                if not sub_id:
                    continue

                sub_proc = _az_run(
                    [
                        "az", "fabric", "capacity", "list",
                        "--subscription", sub_id,
                        "--query", "[].{name:name, id:id, state:state, sku:sku.name, resourceGroup:resourceGroup, location:location}",
                        "-o", "json",
                    ],
                    timeout=15,
                )
                if sub_proc.returncode != 0:
                    logger.info(
                        "Skipping fallback Fabric capacity scan for subscription '%s' (%s): %s",
                        sub_name,
                        sub_id,
                        (sub_proc.stderr or "access unavailable").strip()[:300],
                    )
                    continue

                sub_capacities = json.loads(sub_proc.stdout or "[]")
                for capacity in sub_capacities:
                    capacity_id = capacity.get("id", "")
                    dedupe_key = capacity_id or f"{sub_id}:{capacity.get('resourceGroup', '')}:{capacity.get('name', '')}"
                    if dedupe_key in seen_capacity_ids:
                        continue
                    seen_capacity_ids.add(dedupe_key)
                    capacities.append(
                        {
                            "name": capacity["name"],
                            "id": capacity_id,
                            "state": capacity.get("state", "Unknown"),
                            "sku": capacity.get("sku", ""),
                            "resourceGroup": capacity.get("resourceGroup", ""),
                            "location": capacity.get("location", ""),
                            "subscription": sub_id,
                            "subscriptionName": sub_name,
                        }
                    )

        capacities.sort(
            key=lambda capacity: (
                capacity.get("state") != "Active",
                capacity.get("subscriptionName", "").lower(),
                capacity.get("name", "").lower(),
            )
        )
        _set_timed_cached(cache_key, capacities)
        return capacities
    except Exception as e:
        logger.warning("Failed to list Fabric capacities across subscriptions: %s", e)
        return []


@app.post("/api/capacity/pause")
async def pause_capacity(subscription_id: str, resource_group: str, name: str):
    """Pause a Fabric capacity."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _pause_capacity_sync, subscription_id, resource_group, name)
    return {"message": f"Capacity '{name}' paused"}


def _pause_capacity_sync(subscription_id: str, resource_group: str, name: str):
    """Suspend a Fabric capacity via az CLI (async — returns immediately)."""
    sub_arg = ["--subscription", subscription_id] if subscription_id else []
    proc = _az_run(
        ["az", "fabric", "capacity", "suspend",
         "--resource-group", resource_group,
         "--capacity-name", name,
         "--no-wait"] + sub_arg,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"az fabric capacity suspend failed: {proc.stderr.strip()}")
    logger.info("Pause initiated for capacity '%s' in RG '%s' (async)", name, resource_group)


@app.post("/api/capacity/resume")
async def resume_capacity(subscription_id: str, resource_group: str, name: str):
    """Resume a paused Fabric capacity."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _resume_capacity_sync, subscription_id, resource_group, name)
    return {"message": f"Capacity '{name}' resumed"}


def _resume_capacity_sync(subscription_id: str, resource_group: str, name: str):
    """Resume a Fabric capacity via az CLI (async — returns immediately)."""
    sub_arg = ["--subscription", subscription_id] if subscription_id else []
    proc = _az_run(
        ["az", "fabric", "capacity", "resume",
         "--resource-group", resource_group,
         "--capacity-name", name,
         "--no-wait"] + sub_arg,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"az fabric capacity resume failed: {proc.stderr.strip()}")
    logger.info("Resume initiated for capacity '%s' in RG '%s' (async)", name, resource_group)


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
async def check_existing_deployment(workspace_name: str = "", resource_group: str = "", deep: bool = False):
    """Check if a deployment already exists.

    The default path is intentionally cheap for keystroke/debounce UI checks and
    only reads local deployment history. Pass deep=1 to run live Azure/FHIR/
    storage inspections before final review/deploy.
    """
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

    if not deep:
        result["liveValidated"] = False
        return result

    # Check Azure RG existence and live resource counts only when explicitly requested.
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
    result["liveValidated"] = True
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
    try:
        uvicorn.run(app, host="0.0.0.0", port=7071, log_level="info", timeout_graceful_shutdown=3)
    except KeyboardInterrupt:
        logger.info("Server stopped by user (Ctrl+C)")
    except SystemExit:
        logger.critical("SERVER EXITED VIA SystemExit — see traceback below", exc_info=True)
        _log_thread_stacks("SystemExit")
        raise
    except BaseException:
        logger.critical("SERVER CRASHED — see traceback below", exc_info=True)
        _log_thread_stacks("BaseException")
        raise
    finally:
        logger.info("Server process exiting")
