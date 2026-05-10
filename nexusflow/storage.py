"""
storage.py — SQLite-based persistence for workflows, executions, and logs.

Design rationale:
  We use SQLite (stdlib, zero-dependency) as the backing store.  Three tables
  track everything:
    - ``workflows``   — serialised graph definitions (the "blueprint")
    - ``executions``  — each run of a workflow (start/end time, overall status)
    - ``task_logs``   — per-node status transitions (for checkpoint/resume)

  Checkpoint / resume works by inspecting the saved state of a previous
  execution: any node that finished successfully is skipped on re-run, and
  failed/pending nodes are retried.  This mirrors how Airflow's "mark success"
  or Prefect's "resume from failure" works.

  We keep the schema simple — no migrations yet.  If the schema evolves,
  bump a version field in the ``_meta`` table.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from nexusflow.graph import Graph, Node, NodeStatus

log = logging.getLogger(__name__)


# ── schema ───────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Version check so future code can migrate.
INSERT OR IGNORE INTO _meta (key, value) VALUES ('schema_version', '1');

CREATE TABLE IF NOT EXISTS workflows (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    graph_json  TEXT NOT NULL,       -- full serialised Graph
    metadata    TEXT NOT NULL DEFAULT '{}',
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS executions (
    id           TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(id),
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending | running | success | failed
    started_at   REAL,
    finished_at  REAL,
    error        TEXT,
    created_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS task_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    node_id      TEXT NOT NULL,
    node_name    TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL,
    detail       TEXT,
    attempt      INTEGER NOT NULL DEFAULT 0,
    timestamp    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_executions_workflow ON executions(workflow_id);
CREATE INDEX IF NOT EXISTS idx_task_logs_execution ON task_logs(execution_id);
"""


# ── helpers ──────────────────────────────────────────────────────────────────

def _now() -> float:
    """Wall-clock seconds (UTC) used for timestamps."""
    return time.time()


# ── SQLiteStore ──────────────────────────────────────────────────────────────

class SQLiteStore:
    """Persists workflows, executions, and task logs to a local SQLite database.

    Thread-safe (uses a per-instance re-entrant lock so higher-level code
    doesn't need to worry about connection sharing).

    Parameters
    ----------
    db_path : str | Path
        Location of the SQLite file.  ``:memory:`` is supported but not
        recommended for production use.

    Examples
    --------
    >>> store = SQLiteStore("nexusflow.db")
    >>> store.save_workflow(graph)
    >>> exec_id = store.create_execution(graph.id)
    >>> store.log_task(exec_id, node_id="abc", status="running")
    >>> store.update_execution(exec_id, status="success")
    """

    def __init__(self, db_path: str | Path = "nexusflow.db") -> None:
        self._db_path = str(db_path)
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self) -> None:
        """Create tables if they don't exist."""
        with self._lock, sqlite3.connect(self._db_path) as conn:
            conn.executescript(_SCHEMA_SQL)
            conn.commit()

    def _conn(self) -> sqlite3.Connection:
        """Return a connection (caller must hold the lock)."""
        return sqlite3.connect(self._db_path)

    # ── workflows ───────────────────────────────────────────────────────

    def save_workflow(self, graph: Graph) -> None:
        """Insert or replace a workflow definition in the database."""
        with self._lock, self._conn() as conn:
            now = _now()
            conn.execute(
                """INSERT OR REPLACE INTO workflows
                   (id, name, graph_json, metadata, created_at, updated_at)
                   VALUES (?, ?, ?, ?, COALESCE(
                       (SELECT created_at FROM workflows WHERE id = ?), ?
                   ), ?)""",
                (
                    graph.id,
                    graph.name,
                    json.dumps(graph.to_dict()),
                    json.dumps(graph.metadata),
                    graph.id,
                    now,
                    now,
                ),
            )
            conn.commit()

    def load_workflow(self, workflow_id: str) -> Optional[Graph]:
        """Load a workflow definition by ID."""
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT graph_json FROM workflows WHERE id = ?",
                (workflow_id,),
            ).fetchone()
        if row is None:
            return None
        data = json.loads(row[0])
        return Graph.from_dict(data)

    def list_workflows(self) -> list[dict]:
        """Return a summary of all stored workflows."""
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """SELECT id, name, created_at, updated_at
                   FROM workflows ORDER BY updated_at DESC"""
            ).fetchall()
        return [
            {
                "id": r[0],
                "name": r[1],
                "created_at": r[2],
                "updated_at": r[3],
            }
            for r in rows
        ]

    def delete_workflow(self, workflow_id: str) -> bool:
        """Remove a workflow and its associated executions/logs."""
        with self._lock, self._conn() as conn:
            # Gather execution IDs so we can remove logs.
            exec_ids = [
                r[0]
                for r in conn.execute(
                    "SELECT id FROM executions WHERE workflow_id = ?",
                    (workflow_id,),
                ).fetchall()
            ]
            for eid in exec_ids:
                conn.execute("DELETE FROM task_logs WHERE execution_id = ?", (eid,))
            conn.execute("DELETE FROM executions WHERE workflow_id = ?", (workflow_id,))
            conn.execute("DELETE FROM workflows WHERE id = ?", (workflow_id,))
            conn.commit()
            return conn.total_changes > 0

    # ── executions ──────────────────────────────────────────────────────

    def create_execution(self, workflow_id: str) -> str:
        """Create a new execution record and return its ID."""
        import uuid
        exec_id = uuid.uuid4().hex[:12]
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO executions (id, workflow_id, status, created_at)
                   VALUES (?, ?, 'pending', ?)""",
                (exec_id, workflow_id, _now()),
            )
            conn.commit()
        return exec_id

    def update_execution(
        self,
        execution_id: str,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        """Update execution status and optionally set finished_at."""
        with self._lock, self._conn() as conn:
            now = _now()
            finished = now if status in ("success", "failed") else None
            conn.execute(
                """UPDATE executions
                   SET status = ?, finished_at = COALESCE(?, finished_at), error = ?
                   WHERE id = ?""",
                (status, finished, error, execution_id),
            )
            conn.commit()

    def get_execution(self, execution_id: str) -> Optional[dict]:
        """Fetch a single execution record."""
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """SELECT id, workflow_id, status, started_at, finished_at, error, created_at
                   FROM executions WHERE id = ?""",
                (execution_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "workflow_id": row[1],
            "status": row[2],
            "started_at": row[3],
            "finished_at": row[4],
            "error": row[5],
            "created_at": row[6],
        }

    def list_executions(self, workflow_id: Optional[str] = None) -> list[dict]:
        """Return executions, optionally filtered by workflow."""
        with self._lock, self._conn() as conn:
            if workflow_id:
                rows = conn.execute(
                    """SELECT id, workflow_id, status, started_at, finished_at, error, created_at
                       FROM executions WHERE workflow_id = ?
                       ORDER BY created_at DESC LIMIT 100""",
                    (workflow_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id, workflow_id, status, started_at, finished_at, error, created_at
                       FROM executions ORDER BY created_at DESC LIMIT 100"""
                ).fetchall()
        return [
            {
                "id": r[0],
                "workflow_id": r[1],
                "status": r[2],
                "started_at": r[3],
                "finished_at": r[4],
                "error": r[5],
                "created_at": r[6],
            }
            for r in rows
        ]

    # ── task logs ────────────────────────────────────────────────────────

    def log_task(
        self,
        execution_id: str,
        node_id: str,
        status: str,
        detail: Optional[str] = None,
        node_name: str = "",
        attempt: int = 0,
    ) -> None:
        """Persist a per-node status transition."""
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO task_logs
                   (execution_id, node_id, node_name, status, detail, attempt, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (execution_id, node_id, node_name, status, detail, attempt, _now()),
            )
            conn.commit()

    def get_task_logs(self, execution_id: str) -> list[dict]:
        """Fetch all task logs for a given execution, ordered by timestamp."""
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                """SELECT id, execution_id, node_id, node_name, status, detail, attempt, timestamp
                   FROM task_logs WHERE execution_id = ?
                   ORDER BY timestamp ASC""",
                (execution_id,),
            ).fetchall()
        return [
            {
                "id": r[0],
                "execution_id": r[1],
                "node_id": r[2],
                "node_name": r[3],
                "status": r[4],
                "detail": r[5],
                "attempt": r[6],
                "timestamp": r[7],
            }
            for r in rows
        ]

    # ── checkpoint / resume ─────────────────────────────────────────────

    def last_execution_status(self, workflow_id: str) -> Optional[dict]:
        """Return the most recent execution for a workflow (for resume logic)."""
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """SELECT id, status FROM executions
                   WHERE workflow_id = ?
                   ORDER BY created_at DESC LIMIT 1""",
                (workflow_id,),
            ).fetchone()
        if row is None:
            return None
        return {"id": row[0], "status": row[1]}

    def resume_state(self, execution_id: str) -> dict[str, NodeStatus]:
        """Build a map of node_id → last known status from a prior execution.

        This is the core of checkpoint/resume: the caller can skip nodes
        that already succeeded and re-run only those that failed or were
        never attempted.
        """
        logs = self.get_task_logs(execution_id)
        # We want the *last* entry per node_id.
        state: dict[str, NodeStatus] = {}
        for log_entry in logs:
            state[log_entry["node_id"]] = NodeStatus(log_entry["status"])
        return state

    # ── lifecycle ───────────────────────────────────────────────────────

    def close(self) -> None:
        """Nothing to clean up (connections are context-managed)."""
        pass


# ── convenience ──────────────────────────────────────────────────────────────

def get_default_store() -> SQLiteStore:
    """Return a store using the default path ``~/.nexusflow/store.db``."""
    path = Path.home() / ".nexusflow" / "store.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return SQLiteStore(str(path))
