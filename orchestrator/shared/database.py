"""SQLite database for orchestrator state.

Consolidates all state previously spread across:
- .orchestrator-state.json (deployments, teardowns)
- localStorage teardown-locks (resource locks)
- localStorage teardown-dismissed (dismissed teardowns)
- localStorage form-history-* (form field history)

Tables:
- deployments: deployment/teardown instances with status, phases, logs
- locks: teardown resource locks (persisted across sessions)
- form_history: per-field input history for the deploy wizard
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "orchestrator.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_conn: sqlite3.Connection | None = None


def get_db() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = _get_conn()
        _init_tables(_conn)
    return _conn


def _init_tables(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS deployments (
            instance_id TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT 'deploy_all_orchestrator',
            runtime_status TEXT NOT NULL DEFAULT 'Running',
            created_time TEXT NOT NULL,
            last_updated_time TEXT NOT NULL,
            custom_status TEXT NOT NULL DEFAULT '{}',
            output TEXT,
            config TEXT
        );

        CREATE TABLE IF NOT EXISTS locks (
            resource_id TEXT PRIMARY KEY,
            resource_name TEXT,
            resource_type TEXT,
            locked_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS form_history (
            field TEXT NOT NULL,
            value TEXT NOT NULL,
            used_at TEXT NOT NULL,
            PRIMARY KEY (field, value)
        );

        CREATE TABLE IF NOT EXISTS dismissed_teardowns (
            instance_id TEXT PRIMARY KEY,
            dismissed_at TEXT NOT NULL
        );
    """)
    conn.commit()
    logger.info("SQLite database initialized at %s", DB_PATH)


# ── Deployment CRUD ────────────────────────────────────────────────────

def save_deployment(instance_id: str, data: dict[str, Any]):
    db = get_db()
    db.execute("""
        INSERT OR REPLACE INTO deployments
            (instance_id, name, runtime_status, created_time, last_updated_time, custom_status, output, config)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        instance_id,
        data.get("name", ""),
        data.get("runtimeStatus", "Running"),
        data.get("createdTime", datetime.now(timezone.utc).isoformat()),
        data.get("lastUpdatedTime", datetime.now(timezone.utc).isoformat()),
        json.dumps(data.get("customStatus", {}), default=str),
        json.dumps(data.get("output"), default=str) if data.get("output") else None,
        json.dumps(data.get("config", {}), default=str),
    ))
    db.commit()


def get_deployment(instance_id: str) -> dict[str, Any] | None:
    db = get_db()
    row = db.execute(
        "SELECT * FROM deployments WHERE instance_id = ?", (instance_id,)
    ).fetchone()
    if not row:
        return None
    return _row_to_deployment(row)


def list_deployments() -> list[dict[str, Any]]:
    db = get_db()
    rows = db.execute(
        "SELECT * FROM deployments ORDER BY created_time DESC"
    ).fetchall()
    return [_row_to_deployment(r) for r in rows]


def delete_deployment(instance_id: str) -> bool:
    db = get_db()
    cursor = db.execute(
        "DELETE FROM deployments WHERE instance_id = ?", (instance_id,)
    )
    db.commit()
    return cursor.rowcount > 0


def clear_all_deployments() -> int:
    db = get_db()
    cursor = db.execute("DELETE FROM deployments")
    db.commit()
    return cursor.rowcount


def mark_stale_as_terminated():
    """Mark any Running deployments as Terminated (server restart recovery)."""
    db = get_db()
    rows = db.execute(
        "SELECT instance_id, custom_status, output, created_time FROM deployments WHERE runtime_status = 'Running'"
    ).fetchall()
    for row in rows:
        cs = json.loads(row["custom_status"])
        cs["status"] = "terminated"
        cs["detail"] = "Server restarted — deployment was interrupted"
        # Try to compute actual duration from phase data or last update
        output = json.loads(row["output"]) if row["output"] else None
        if output and "phases" in output:
            phase_duration = sum(
                p.get("duration", 0) for p in output["phases"]
                if isinstance(p.get("duration"), (int, float))
            )
            if phase_duration > 0:
                cs["durationSeconds"] = round(phase_duration, 1)
        db.execute("""
            UPDATE deployments
            SET runtime_status = 'Terminated',
                custom_status = ?,
                last_updated_time = ?
            WHERE instance_id = ?
        """, (json.dumps(cs, default=str), datetime.now(timezone.utc).isoformat(), row["instance_id"]))
        logger.warning("Marked stale deployment %s as Terminated", row["instance_id"])
    db.commit()


def _row_to_deployment(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "instanceId": row["instance_id"],
        "name": row["name"],
        "runtimeStatus": row["runtime_status"],
        "createdTime": row["created_time"],
        "lastUpdatedTime": row["last_updated_time"],
        "customStatus": json.loads(row["custom_status"]),
        "output": json.loads(row["output"]) if row["output"] else None,
    }


# ── Locks ──────────────────────────────────────────────────────────────

def get_locks() -> list[str]:
    db = get_db()
    rows = db.execute("SELECT resource_id FROM locks").fetchall()
    return [r["resource_id"] for r in rows]


def set_lock(resource_id: str, name: str = "", resource_type: str = ""):
    db = get_db()
    db.execute("""
        INSERT OR REPLACE INTO locks (resource_id, resource_name, resource_type, locked_at)
        VALUES (?, ?, ?, ?)
    """, (resource_id, name, resource_type, datetime.now(timezone.utc).isoformat()))
    db.commit()


def remove_lock(resource_id: str):
    db = get_db()
    db.execute("DELETE FROM locks WHERE resource_id = ?", (resource_id,))
    db.commit()


def clear_locks():
    db = get_db()
    db.execute("DELETE FROM locks")
    db.commit()


# ── Form History ───────────────────────────────────────────────────────

def get_form_history(field: str, limit: int = 10) -> list[str]:
    db = get_db()
    rows = db.execute(
        "SELECT value FROM form_history WHERE field = ? ORDER BY used_at DESC LIMIT ?",
        (field, limit),
    ).fetchall()
    return [r["value"] for r in rows]


def add_form_history(field: str, value: str):
    if not value.strip():
        return
    db = get_db()
    db.execute("""
        INSERT OR REPLACE INTO form_history (field, value, used_at)
        VALUES (?, ?, ?)
    """, (field, value.strip(), datetime.now(timezone.utc).isoformat()))
    db.commit()


# ── Dismissed Teardowns ────────────────────────────────────────────────

def get_dismissed_teardowns() -> list[str]:
    db = get_db()
    rows = db.execute("SELECT instance_id FROM dismissed_teardowns").fetchall()
    return [r["instance_id"] for r in rows]


def dismiss_teardown(instance_id: str):
    db = get_db()
    db.execute("""
        INSERT OR IGNORE INTO dismissed_teardowns (instance_id, dismissed_at)
        VALUES (?, ?)
    """, (instance_id, datetime.now(timezone.utc).isoformat()))
    db.commit()


# ── Migration from JSON state file ────────────────────────────────────

def migrate_from_json(json_path: Path):
    """One-time migration from .orchestrator-state.json to SQLite."""
    if not json_path.exists():
        return

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        count = 0
        for instance_id, dep in data.items():
            dep["instanceId"] = instance_id
            save_deployment(instance_id, {
                "name": dep.get("name", ""),
                "runtimeStatus": dep.get("runtimeStatus", "Unknown"),
                "createdTime": dep.get("createdTime", ""),
                "lastUpdatedTime": dep.get("lastUpdatedTime", ""),
                "customStatus": dep.get("customStatus", {}),
                "output": dep.get("output"),
            })
            count += 1

        # Rename old file to .bak
        bak_path = json_path.with_suffix(".json.bak")
        json_path.rename(bak_path)
        logger.info("Migrated %d deployments from JSON to SQLite (backup: %s)", count, bak_path)
    except Exception as e:
        logger.warning("JSON migration failed: %s", e)
