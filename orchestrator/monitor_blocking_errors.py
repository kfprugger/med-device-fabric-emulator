#!/usr/bin/env python3
"""Poll the local orchestrator and report deployment blocking errors.

Default behavior watches all currently running deployments every five minutes.
It exits with code 2 as soon as it sees a blocking error so the harness reports
back immediately. It exits 0 when there are no running deployments left.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

BLOCKING_MARKERS = (
    "ERROR",
    "AccessUnauthorized",
    "ALMOperationImportFailed",
    "IncorrectCredentials",
    "RequestDeniedByInboundPolicy",
    "failed with exit code",
    "Traceback (most recent call last)",
)
BLOCKING_WARNING_TERMS = (
    "trial capacities",
    "trial capacity",
    "no fabric capacity",
    "not licensed",
)
TERMINAL_FAILURES = {"failed", "terminated", "cancelled", "canceled"}
TERMINAL_SUCCESS = {"completed", "succeeded"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch_json(url: str, timeout: int = 30) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def normalize_status(value: Any) -> str:
    return str(value or "").strip().lower()


def deployment_name(dep: dict[str, Any]) -> str:
    return str(dep.get("instanceId") or dep.get("id") or "<unknown>")


def custom_status(dep: dict[str, Any]) -> dict[str, Any]:
    status = dep.get("customStatus") or {}
    return status if isinstance(status, dict) else {}


def blocking_messages(dep: dict[str, Any], *, include_warning_logs: bool = False) -> list[str]:
    messages: list[str] = []
    runtime = normalize_status(dep.get("runtimeStatus"))
    cs = custom_status(dep)
    cs_status = normalize_status(cs.get("status"))
    current_phase = cs.get("currentPhase") or "<unknown phase>"
    detail = str(cs.get("detail") or "")

    if runtime in TERMINAL_FAILURES:
        messages.append(f"runtimeStatus={dep.get('runtimeStatus')} at {current_phase}: {detail}".strip())
    if cs_status in TERMINAL_FAILURES:
        messages.append(f"customStatus={cs.get('status')} at {current_phase}: {detail}".strip())

    phases = ((dep.get("output") or {}).get("phases") or []) if isinstance(dep.get("output"), dict) else []
    for phase in phases:
        if not isinstance(phase, dict):
            continue
        phase_status = normalize_status(phase.get("status"))
        if phase_status in TERMINAL_FAILURES:
            messages.append(f"phase failed: {phase.get('phase') or '<unnamed>'}: {phase.get('detail') or ''}".strip())
        for warning in phase.get("warnings") or []:
            warning_text = str(warning)
            if any(term in warning_text.lower() for term in BLOCKING_WARNING_TERMS):
                messages.append(f"phase warning looks blocking: {phase.get('phase') or '<unnamed>'}: {warning_text}")

    logs = cs.get("logs") or []
    for entry in logs:
        if not isinstance(entry, dict):
            continue
        level = normalize_status(entry.get("level"))
        message = str(entry.get("message") or "")
        lower = message.lower()
        is_error = level == "error"
        has_blocking_marker = any(marker in message for marker in BLOCKING_MARKERS)
        is_blocking_warning = include_warning_logs and level == "warn" and any(term in lower for term in BLOCKING_WARNING_TERMS)
        if is_error or is_blocking_warning or has_blocking_marker:
            phase = entry.get("phase") or current_phase
            ts = entry.get("timestamp") or ""
            messages.append(f"log {level} {ts} [{phase}]: {message}")

    return messages


def is_running(dep: dict[str, Any]) -> bool:
    runtime = normalize_status(dep.get("runtimeStatus"))
    cs = custom_status(dep)
    cs_status = normalize_status(cs.get("status"))
    return runtime == "running" or cs_status in {"running", "waiting_for_input", "deleting"}


def is_success_terminal(dep: dict[str, Any]) -> bool:
    runtime = normalize_status(dep.get("runtimeStatus"))
    cs_status = normalize_status(custom_status(dep).get("status"))
    return runtime in TERMINAL_SUCCESS or cs_status in TERMINAL_SUCCESS



def health_blocking_messages(api_base: str) -> list[str]:
    health = fetch_json(f"{api_base.rstrip('/')}/health?monitor={int(time.time())}")
    messages: list[str] = []
    if not isinstance(health, dict):
        return ["health endpoint returned non-object payload"]
    status = normalize_status(health.get("status"))
    if status and status != "ok":
        messages.append(f"backend health status={health.get('status')}: {health.get('auth', {}).get('issues', [])}")
    auth = health.get("auth") if isinstance(health.get("auth"), dict) else {}
    if auth and not auth.get("ready", False):
        messages.append(f"backend auth not ready: {auth.get('issues', [])}")
    return messages


def scan_job_blocking_messages(api_base: str, scan_ids: set[str]) -> list[str]:
    messages: list[str] = []
    for scan_id in sorted(scan_ids):
        job = fetch_json(f"{api_base.rstrip('/')}/scan/resources/{scan_id}")
        status = normalize_status(job.get("status") if isinstance(job, dict) else "")
        if status in {"failed", "missing"}:
            messages.append(f"scan job {scan_id} status={job.get('status')}: {job.get('error') or job.get('message')}")
    return messages

def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor local orchestrator deployments for blocking errors.")
    parser.add_argument("--api-base", default="http://localhost:7071/api")
    parser.add_argument("--deployment-id", action="append", default=[], help="Specific deployment ID to monitor. Repeatable.")
    parser.add_argument("--interval", type=int, default=300, help="Polling interval in seconds. Default: 300.")
    parser.add_argument("--include-warning-logs", action="store_true", help="Treat warning logs matching blocking terms as blockers.")
    parser.add_argument("--scan-id", action="append", default=[], help="Resource scan job ID to monitor for failed/missing status. Repeatable.")
    parser.add_argument("--max-api-failures", type=int, default=3)
    args = parser.parse_args()

    watched = set(args.deployment_id)
    watched_scans = set(args.scan_id)
    api_failures = 0
    print(f"[{utc_now()}] monitor started; interval={args.interval}s; deployments={sorted(watched) or 'all running'}; scans={sorted(watched_scans) or 'none'}", flush=True)

    while True:
        try:
            health_messages = health_blocking_messages(args.api_base)
            scan_messages = scan_job_blocking_messages(args.api_base, watched_scans)
            deployments = fetch_json(f"{args.api_base.rstrip('/')}/deployments")
            api_failures = 0
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            api_failures += 1
            print(f"[{utc_now()}] API check failed ({api_failures}/{args.max_api_failures}): {exc}", flush=True)
            if api_failures >= args.max_api_failures:
                print(f"[{utc_now()}] BLOCKING ERROR: local orchestrator API unavailable after {api_failures} checks", flush=True)
                return 2
            time.sleep(args.interval)
            continue

        if health_messages or scan_messages:
            print(f"[{utc_now()}] BLOCKING ERRORS DETECTED", flush=True)
            for message in health_messages + scan_messages:
                print(f"- {message}", flush=True)
            return 2

        if not isinstance(deployments, list):
            print(f"[{utc_now()}] BLOCKING ERROR: /deployments returned non-list payload", flush=True)
            return 2

        selected = [dep for dep in deployments if isinstance(dep, dict) and (not watched or deployment_name(dep) in watched)]
        scope = selected if watched else [dep for dep in selected if is_running(dep)]
        running = [dep for dep in scope if is_running(dep)]
        blockers: list[tuple[str, list[str]]] = []
        for dep in scope:
            messages = blocking_messages(dep, include_warning_logs=args.include_warning_logs)
            if messages:
                blockers.append((deployment_name(dep), messages))

        if blockers:
            print(f"[{utc_now()}] BLOCKING ERRORS DETECTED", flush=True)
            for dep_id, messages in blockers:
                print(f"\nDeployment: {dep_id}", flush=True)
                for message in messages[-20:]:
                    print(f"- {message}", flush=True)
            return 2

        if watched:
            pending = [dep for dep in selected if not is_success_terminal(dep)]
            if not pending:
                print(f"[{utc_now()}] monitored deployments completed without blocking errors", flush=True)
                return 0
        elif not running:
            print(f"[{utc_now()}] no running deployments; monitor exiting without blocking errors", flush=True)
            return 0

        summary = ", ".join(
            f"{deployment_name(dep)}:{custom_status(dep).get('currentPhase') or dep.get('runtimeStatus')}"
            for dep in running[:5]
        )
        if len(running) > 5:
            summary += f", +{len(running) - 5} more"
        print(f"[{utc_now()}] check OK; running={len(running)}; {summary}", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
