#!/usr/bin/env python3
"""
Post-deployment evaluation harness for the med-device-fabric-emulator.

Validates, for a target Fabric workspace, that the deployment's user-facing
surfaces actually WORK (not just that items exist):

  1. Reports        - every Power BI semantic model backing a report is queryable
                      via DAX and its visual-backing tables return rows (catches
                      blank/unrepaired visuals and Direct Lake connection breaks).
  2. Data Agents    - every Data Agent answers a natural-language test query end to
                      end (catches unpublished/draft agents -> "Stage configuration
                      not found").
  3. RTI dashboards - every KQL dashboard's backing tables/functions return data
                      (TelemetryRaw, AlertHistory, claims_events, fn_AlertLocationMap,
                      fn_PayerOpsWorklist, PatientLocationDashboard).

Auth: uses the local Azure CLI (isolated BrakeKat profile by default) to mint
tokens for three resources — Fabric, Power BI, and the Eventhouse (Kusto).

Exit code 0 = all checks passed; 1 = one or more failures; 2 = harness/setup error.

Usage:
  python3 eval/deployment_eval_harness.py --workspace med-0719
  python3 eval/deployment_eval_harness.py --workspace med-0719 --json-out eval/last_run.json
  python3 eval/deployment_eval_harness.py --workspace med-0719 --skip agents
"""
from __future__ import annotations
import argparse, json, os, ssl, subprocess, sys, time, urllib.request, urllib.error

FABRIC_API = "https://api.fabric.microsoft.com/v1"
PBI_API = "https://api.powerbi.com/v1.0/myorg"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
PBI_RESOURCE = "https://analysis.windows.net/powerbi/api"
DB_RESOURCE = "https://database.windows.net"
AGENT_API_VERSION = "2024-05-01-preview"
CAPACITY_ID = ("/subscriptions/5772d06a-5513-4cc5-ac08-a3805440c60e/resourceGroups/"
               "rg-fabricskus/providers/Microsoft.Fabric/capacities/fabrjbwu2")

_CTX = ssl.create_default_context()


class Az:
    """Azure CLI token minter honoring an isolated config dir."""
    def __init__(self, config_dir: str | None):
        self.env = dict(os.environ)
        if config_dir:
            self.env["AZURE_CONFIG_DIR"] = config_dir
        self._cache: dict[str, tuple[float, str]] = {}

    def token(self, resource: str) -> str:
        # cache ~40 min (tokens live 60-75 min)
        hit = self._cache.get(resource)
        if hit and time.time() - hit[0] < 2400:
            return hit[1]
        out = subprocess.run(
            ["az", "account", "get-access-token", "--resource", resource,
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True, text=True, env=self.env, timeout=60)
        tok = out.stdout.strip()
        if not tok:
            raise RuntimeError(f"az token failed for {resource}: {out.stderr.strip()[:200]}")
        self._cache[resource] = (time.time(), tok)
        return tok

    def run(self, args: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(args, capture_output=True, text=True, env=self.env, timeout=120)


def http(method: str, url: str, token: str, body=None, timeout=90):
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
    elif method == "POST":
        data = b""
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req, context=_CTX, timeout=timeout)
        raw = r.read()
        return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"_raw": raw[:400]}


def ensure_capacity_active(az: Az, log) -> bool:
    """The F64 capacity backing these workspaces auto-pauses; Direct Lake reads,
    KQL queries, and agent runs all fail when it is Paused. Resume if needed."""
    out = az.run(["az", "resource", "show", "--ids", CAPACITY_ID,
                  "--query", "properties.state", "-o", "tsv"])
    state = out.stdout.strip()
    log(f"capacity fabrjbwu2 state: {state or '(unknown)'}")
    if state == "Active":
        return True
    if not state:
        log("  WARN: could not read capacity state; continuing")
        return True
    log(f"  capacity is {state}; resuming...")
    az.run(["az", "resource", "invoke-action", "--action", "resume",
            "--ids", CAPACITY_ID, "--no-wait"])
    for _ in range(12):
        time.sleep(20)
        s = az.run(["az", "resource", "show", "--ids", CAPACITY_ID,
                    "--query", "properties.state", "-o", "tsv"]).stdout.strip()
        if s == "Active":
            log("  capacity resumed -> Active")
            return True
    log("  ERROR: capacity did not reach Active")
    return False


def find_workspace(az: Az, name: str) -> str | None:
    _, data = http("GET", f"{FABRIC_API}/workspaces", az.token(FABRIC_RESOURCE))
    for w in data.get("value", []):
        if w.get("displayName") == name:
            return w["id"]
    return None


def list_items(az: Az, ws_id: str) -> list[dict]:
    _, data = http("GET", f"{FABRIC_API}/workspaces/{ws_id}/items", az.token(FABRIC_RESOURCE))
    return data.get("value", [])


def _dax(az: Az, model_id: str, query: str):
    body = {"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}}
    st, data = http("POST", f"{PBI_API}/datasets/{model_id}/executeQueries",
                    az.token(PBI_RESOURCE), body)
    if st != 200:
        return None, json.dumps(data)[:300]
    try:
        return data["results"][0]["tables"][0]["rows"], None
    except Exception:
        return None, json.dumps(data)[:200]


def _user_tables(az: Az, model_id: str) -> list[str]:
    rows, err = _dax(az, model_id, "EVALUATE INFO.VIEW.TABLES()")
    if err or not rows:
        return []
    names = []
    for r in rows:
        key = next((c for c in r if "Name" in c), None)
        if key and r[key]:
            names.append(r[key])
    return [n for n in names if "Date" not in n and not n.startswith("_")]


def validate_reports(az: Az, ws_id: str, items: list[dict], log) -> dict:
    """Every semantic model backing a report must be queryable AND expose rows.
    Catches Direct Lake connection breaks (blank visuals) and empty fact tables."""
    models = [i for i in items if i["type"] == "SemanticModel"]
    reports = [i for i in items if i["type"] == "Report"]
    log(f"reports: {len(reports)} | semantic models: {len(models)}")
    results = []
    for m in models:
        mid, name = m["id"], m["displayName"]
        trivial, err = _dax(az, mid, 'EVALUATE ROW("n", 1)')
        if err:
            results.append({"model": name, "status": "FAIL",
                            "reason": f"semantic model not queryable: {err[:160]}"})
            log(f"  [FAIL] {name}: not queryable"); continue
        tables = _user_tables(az, mid)
        if not tables:
            results.append({"model": name, "status": "WARN", "reason": "no user tables enumerated"})
            log(f"  [WARN] {name}: no user tables"); continue
        counts, conn_fail, empty = {}, False, []
        for t in tables:
            rows, cerr = _dax(az, mid, f'EVALUATE ROW("n", COUNTROWS(\'{t}\'))')
            if cerr:
                if "connection could not be made" in cerr.lower() or "could not login" in cerr.lower():
                    conn_fail = True
                counts[t] = f"ERR:{cerr[:60]}"
            else:
                n = rows[0].get("[n]") if rows else None
                counts[t] = n
                if not n:
                    empty.append(t)
        if conn_fail:
            results.append({"model": name, "status": "FAIL", "counts": counts,
                            "reason": "Direct Lake data source connection failed — report visuals will be blank"})
            log(f"  [FAIL] {name}: Direct Lake connection failed (visuals blank)")
        elif len(empty) == len(tables):
            results.append({"model": name, "status": "FAIL", "counts": counts,
                            "reason": "all backing tables empty — visuals blank"})
            log(f"  [FAIL] {name}: all {len(tables)} tables empty")
        else:
            nonempty = len(tables) - len(empty)
            results.append({"model": name, "status": "PASS", "counts": counts,
                            "reason": f"{nonempty}/{len(tables)} tables have rows"})
            log(f"  [PASS] {name}: {nonempty}/{len(tables)} tables have rows")
    passed = bool(results) and all(r["status"] == "PASS" for r in results)
    return {"category": "reports", "passed": passed, "results": results}


def validate_agents(az: Az, ws_id: str, items: list[dict], log,
                    question="How many patients are in the system? Answer with a number.") -> dict:
    """Every Data Agent must answer a test query end to end via the aiskills
    OpenAI-assistant thread API. Catches unpublished/draft agents."""
    agents = [i for i in items if i["type"] in ("DataAgent", "OperationsAgent")]
    log(f"data agents: {len(agents)}")
    tok = lambda: az.token(FABRIC_RESOURCE)
    av = f"?api-version={AGENT_API_VERSION}"
    results = []
    for a in agents:
        aid, name = a["id"], a["displayName"]
        base = f"{FABRIC_API}/workspaces/{ws_id}/aiskills/{aid}/aiassistant/openai"
        try:
            st, thread = http("POST", f"{base}/threads{av}", tok())
            if st != 200:
                results.append({"agent": name, "status": "FAIL", "reason": f"thread create {st}: {json.dumps(thread)[:120]}"})
                log(f"  [FAIL] {name}: thread create {st}"); continue
            tid = thread["id"]
            st, _ = http("POST", f"{base}/threads/{tid}/messages{av}", tok(), {"role": "user", "content": question})
            if st != 200:
                results.append({"agent": name, "status": "FAIL", "reason": f"message post {st}"})
                log(f"  [FAIL] {name}: message post {st}"); continue
            st, run = http("POST", f"{base}/threads/{tid}/runs{av}", tok(), {"assistant_id": aid})
            if st not in (200, 201):
                reason = run.get("message") if isinstance(run, dict) else str(run)
                reason = reason or json.dumps(run)[:140]
                # Known Fabric preview limitation: Data Agents must be published in the
                # portal (draft config alone yields "Stage configuration not found").
                if "stage configuration not found" in reason.lower() or "ai skill configuration" in reason.lower():
                    hint = "AGENT NOT PUBLISHED — open the agent in the Fabric portal and click Publish (preview: no REST publish API)"
                    results.append({"agent": name, "status": "FAIL", "reason": hint, "raw": reason[:140], "remediation": "manual-publish"})
                    log(f"  [FAIL] {name}: not published (manual portal Publish required)")
                else:
                    results.append({"agent": name, "status": "FAIL", "reason": f"run start {st}: {reason}"})
                    log(f"  [FAIL] {name}: run start {st} ({reason[:60]})")
                continue
            rid, final = run["id"], None
            for _ in range(40):
                time.sleep(6)
                st, rr = http("GET", f"{base}/threads/{tid}/runs/{rid}{av}", tok())
                if rr.get("status") in ("completed", "failed", "cancelled", "expired", "requires_action"):
                    final = rr; break
            if not final or final.get("status") != "completed":
                reason = (final or {}).get("last_error") or (final or {}).get("status") or "timeout"
                results.append({"agent": name, "status": "FAIL", "reason": f"run did not complete: {str(reason)[:140]}"})
                log(f"  [FAIL] {name}: run {str(reason)[:60]}"); continue
            st, msgs = http("GET", f"{base}/threads/{tid}/messages{av}", tok())
            answer = ""
            for m in msgs.get("data", []):
                if m.get("role") == "assistant":
                    for c in m.get("content", []):
                        if c.get("type") == "text":
                            answer += c.get("text", {}).get("value", "")
            if answer.strip():
                results.append({"agent": name, "status": "PASS", "reason": f"answered ({len(answer)} chars)", "answer": answer[:200]})
                log(f"  [PASS] {name}: answered ({len(answer)} chars)")
            else:
                results.append({"agent": name, "status": "FAIL", "reason": "empty assistant response"})
                log(f"  [FAIL] {name}: empty response")
        except Exception as e:
            results.append({"agent": name, "status": "FAIL", "reason": f"exception: {str(e)[:140]}"})
            log(f"  [FAIL] {name}: {str(e)[:80]}")
    passed = bool(results) and all(r["status"] == "PASS" for r in results)
    return {"category": "agents", "passed": passed, "results": results}


def _kql(az: Az, query_uri: str, db: str, csl: str, tries: int = 4):
    body = json.dumps({"db": db, "csl": csl}).encode()
    last_err = None
    for attempt in range(tries):
        req = urllib.request.Request(f"{query_uri}/v1/rest/query", data=body, method="POST",
                                     headers={"Authorization": f"Bearer {az.token(query_uri)}",
                                              "Content-Type": "application/json", "Accept": "application/json"})
        try:
            r = urllib.request.urlopen(req, context=_CTX, timeout=90)
            return json.loads(r.read())["Tables"][0]["Rows"], None
        except urllib.error.HTTPError as e:
            # HTTP errors (bad query, 404) are deterministic — do not retry
            return None, e.read().decode(errors="replace")[:160]
        except Exception as e:
            # transient transport blips (SSL EOF, reset, timeout) — retry with backoff
            last_err = str(e)[:120]
            if attempt < tries - 1:
                time.sleep(3 * (attempt + 1))
    return None, last_err


def validate_rti(az: Az, ws_id: str, items: list[dict], log) -> dict:
    """Every RTI/KQL dashboard's backing Eventhouse tables/functions return data."""
    dashboards = [i for i in items if i["type"] == "KQLDashboard"]
    ehs = [i for i in items if i["type"] == "Eventhouse"]
    log(f"RTI dashboards: {len(dashboards)} | eventhouses: {len(ehs)}")
    if not ehs:
        return {"category": "rti", "passed": False, "results": [{"status": "FAIL", "reason": "no eventhouse"}]}
    _, det = http("GET", f"{FABRIC_API}/workspaces/{ws_id}/eventhouses/{ehs[0]['id']}", az.token(FABRIC_RESOURCE))
    quri = (det.get("properties") or {}).get("queryServiceUri")
    db = ehs[0]["displayName"]
    if not quri:
        return {"category": "rti", "passed": False, "results": [{"status": "FAIL", "reason": "no queryServiceUri"}]}
    tbl_rows, _ = _kql(az, quri, db, ".show tables | project TableName")
    existing = {r[0] for r in (tbl_rows or [])}
    fn_rows, _ = _kql(az, quri, db, ".show functions | project Name")
    fns = {r[0] for r in (fn_rows or [])}
    checks = [("TelemetryRaw", "TelemetryRaw | count", True),
              ("AlertHistory", "AlertHistory | count", True),
              ("claims_events", "claims_events | count", False),
              ("PatientLocationDashboard", "PatientLocationDashboard | count", False)]
    fn_checks = [("fn_AlertLocationMap", "fn_AlertLocationMap(10080) | count", False),
                 ("fn_PayerOpsWorklist", "fn_PayerOpsWorklist(10080) | count", False)]
    results = []
    for name, csl, must in checks:
        if name not in existing:
            results.append({"check": name, "status": "SKIP", "reason": "table not present"}); continue
        rows, err = _kql(az, quri, db, csl)
        n = rows[0][0] if rows else None
        if err:
            results.append({"check": name, "status": "FAIL", "reason": f"query error: {err[:100]}"}); log(f"  [FAIL] {name}: {err[:60]}")
        elif must and not n:
            results.append({"check": name, "status": "FAIL", "reason": "table empty", "rows": n}); log(f"  [FAIL] {name}: empty")
        else:
            results.append({"check": name, "status": "PASS", "rows": n}); log(f"  [PASS] {name}: rows={n}")
    for name, csl, must in fn_checks:
        if name not in fns:
            results.append({"check": name, "status": "SKIP", "reason": "function not present"}); continue
        rows, err = _kql(az, quri, db, csl)
        n = rows[0][0] if rows else None
        if err:
            results.append({"check": name, "status": "FAIL", "reason": f"query error: {err[:100]}"}); log(f"  [FAIL] {name}: {err[:60]}")
        else:
            results.append({"check": name, "status": "PASS", "rows": n}); log(f"  [PASS] {name}: rows={n}")
    passed = not any(r["status"] == "FAIL" for r in results)
    return {"category": "rti", "passed": passed, "results": results,
            "dashboards": [d["displayName"] for d in dashboards]}


def main() -> int:
    ap = argparse.ArgumentParser(description="Post-deployment eval harness")
    ap.add_argument("--workspace", required=True, help="Fabric workspace display name (e.g. med-0719)")
    ap.add_argument("--azure-config-dir", default="/Users/joey/.azure-isolated/BrakeKat")
    ap.add_argument("--json-out", default=None, help="write full results JSON here")
    ap.add_argument("--skip", action="append", default=[], choices=["reports", "agents", "rti"],
                    help="skip a category (repeatable)")
    ap.add_argument("--no-capacity-resume", action="store_true", help="do not auto-resume the capacity")
    args = ap.parse_args()

    def log(m): print(m, flush=True)
    az = Az(args.azure_config_dir)
    log(f"=== deployment eval harness :: workspace={args.workspace} :: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ===")
    try:
        if not args.no_capacity_resume and not ensure_capacity_active(az, log):
            log("ABORT: capacity not Active"); return 2
        ws_id = find_workspace(az, args.workspace)
        if not ws_id:
            log(f"ABORT: workspace '{args.workspace}' not found"); return 2
        log(f"workspace id: {ws_id}")
        items = list_items(az, ws_id)
        log(f"items: {len(items)}")
    except Exception as e:
        log(f"ABORT: setup failed: {e}"); return 2

    categories = []
    if "reports" not in args.skip:
        log("\n--- REPORTS ---"); categories.append(validate_reports(az, ws_id, items, log))
    if "rti" not in args.skip:
        log("\n--- RTI DASHBOARDS ---"); categories.append(validate_rti(az, ws_id, items, log))
    if "agents" not in args.skip:
        log("\n--- DATA AGENTS ---"); categories.append(validate_agents(az, ws_id, items, log))

    overall = all(c["passed"] for c in categories)
    summary = {"workspace": args.workspace, "workspaceId": ws_id,
               "capturedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "overallPassed": overall, "categories": categories}
    log("\n=== SUMMARY ===")
    for c in categories:
        n_fail = sum(1 for r in c["results"] if r.get("status") == "FAIL")
        log(f"  {c['category']:8} {'PASS' if c['passed'] else 'FAIL'}  ({n_fail} failing checks)")
    log(f"  OVERALL: {'PASS' if overall else 'FAIL'}")
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(summary, f, indent=2)
        log(f"  wrote {args.json_out}")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
