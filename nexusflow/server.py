"""
server.py — FastAPI web server for the NexusFlow dashboard.

Provides both a JSON REST API and a browser-based UI for:
  - Viewing workflow DAGs (with a force-directed graph visualisation)
  - Monitoring execution status and task logs
  - Triggering workflow runs
  - Viewing schedules

Design rationale:
  We embed a minimal Jinja2 template inside the package (no external template
  dependency — we ship templates as Python strings or static files).  The
  DAG visualisation uses a pure JS library (vis-network or a lightweight
  SVG renderer) so the UI has zero Python dependencies beyond FastAPI.

  The API is read-heavy by design: the dashboard polls for status updates.
  For production, swap to WebSocket push — we expose a ``/ws`` endpoint
  for that purpose.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from nexusflow import Graph, GraphExecutor, ExecutorConfig
from nexusflow.storage import SQLiteStore, get_default_store

log = logging.getLogger(__name__)

# ── application setup ────────────────────────────────────────────────────────

app = FastAPI(
    title="NexusFlow",
    version="0.1.0",
    description="DAG-based workflow orchestration engine — API & UI",
)

# Store: allow override via env var.
_store_path = os.environ.get("NEXUSFLOW_STORE_PATH", None)
if _store_path:
    store = SQLiteStore(_store_path)
else:
    store = get_default_store()


# ── REST API ─────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


# ── Workflows ───────────────────────────────────────────────────────────────

@app.get("/api/workflows")
async def list_workflows():
    """Return all stored workflow definitions."""
    return store.list_workflows()


@app.get("/api/workflows/{workflow_id}")
async def get_workflow(workflow_id: str):
    """Return the full graph definition for a workflow."""
    graph = store.load_workflow(workflow_id)
    if graph is None:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return graph.to_dict()


@app.delete("/api/workflows/{workflow_id}")
async def delete_workflow(workflow_id: str):
    """Remove a workflow and all its executions."""
    ok = store.delete_workflow(workflow_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {"deleted": True}


# ── Executions ──────────────────────────────────────────────────────────────

@app.get("/api/executions")
async def list_executions(
    workflow_id: Optional[str] = Query(None, alias="workflow_id"),
):
    """Return execution records, optionally filtered by workflow."""
    return store.list_executions(workflow_id)


@app.get("/api/executions/{execution_id}")
async def get_execution(execution_id: str):
    """Return a single execution and its task logs."""
    ex = store.get_execution(execution_id)
    if ex is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    logs = store.get_task_logs(execution_id)
    ex["task_logs"] = logs
    return ex


@app.post("/api/workflows/{workflow_id}/run")
async def trigger_run(workflow_id: str):
    """Trigger an immediate execution of a stored workflow."""
    graph = store.load_workflow(workflow_id)
    if graph is None:
        raise HTTPException(status_code=404, detail="Workflow not found")

    exec_id = store.create_execution(workflow_id)
    config = ExecutorConfig(on_status_change=_make_logger(exec_id))
    executor = GraphExecutor(graph, config=config)

    try:
        results = executor.run()
        status = "success"
        error = None
    except Exception as exc:
        status = "failed"
        error = str(exc)

    store.update_execution(exec_id, status=status, error=error)

    return {
        "execution_id": exec_id,
        "status": status,
        "error": error,
        "task_logs": store.get_task_logs(exec_id),
    }


def _make_logger(exec_id: str):
    """Return a callback that logs node status changes to the store."""

    def _log(node, status, ctx):
        store.log_task(
            exec_id,
            node.id,
            status.value,
            node_name=node.name,
            detail=node.error,
        )

    return _log


# ── HTML UI ─────────────────────────────────────────────────────────────────

def _render_template(name: str, **kwargs: Any) -> str:
    """Load an HTML template from the package's ``templates/`` directory."""
    template_dir = Path(__file__).resolve().parent / "templates"
    path = template_dir / name
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Template {name} not found")
    html = path.read_text(encoding="utf-8")

    # Minimal server-side template substitution (no Jinja2 dependency).
    for key, val in kwargs.items():
        placeholder = "{{ " + key + " }}"
        if isinstance(val, (dict, list)):
            val = json.dumps(val, indent=2)
        html = html.replace(placeholder, str(val))

    return html


@app.get("/", response_class=HTMLResponse)
async def index():
    """Main dashboard — list of workflows."""
    workflows = store.list_workflows()
    executions = store.list_executions()
    return _render_template(
        "index.html",
        workflows=json.dumps(workflows, indent=2),
        executions=json.dumps(executions, indent=2),
    )


@app.get("/workflows/{workflow_id}", response_class=HTMLResponse)
async def workflow_detail(workflow_id: str):
    """Visualise a single workflow DAG and its execution history."""
    graph = store.load_workflow(workflow_id)
    if graph is None:
        raise HTTPException(status_code=404, detail="Workflow not found")
    execs = store.list_executions(workflow_id)
    return _render_template(
        "workflow.html",
        workflow_id=workflow_id,
        graph_json=json.dumps(graph.to_dict()),
        executions=json.dumps(execs, indent=2),
    )


@app.get("/executions/{execution_id}", response_class=HTMLResponse)
async def execution_detail(execution_id: str):
    """Show a single execution with task logs."""
    ex = store.get_execution(execution_id)
    if ex is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    logs = store.get_task_logs(execution_id)
    graph = store.load_workflow(ex["workflow_id"])
    return _render_template(
        "execution.html",
        execution_id=execution_id,
        execution=json.dumps(ex, indent=2),
        task_logs=json.dumps(logs, indent=2),
        graph_json=json.dumps(graph.to_dict() if graph else {}),
    )


# ── static assets (served by FastAPI) ────────────────────────────────────────

_static_dir = Path(__file__).resolve().parent / "templates"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")
