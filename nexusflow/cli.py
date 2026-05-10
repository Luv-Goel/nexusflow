"""
cli.py — Command-line interface for NexusFlow.

Provides two entry points:
  - ``nexusflow run``    — execute a workflow from a Python file
  - ``nexusflow serve``  — start the web UI server
  - ``nexusflow schedule`` — list / add scheduled workflows
  - ``nexusflow list``   — show workflow history from the store

Usage
-----
    # Run a workflow defined in a Python file
    nexusflow run examples/etl_pipeline.py

    # Start the web dashboard
    nexusflow serve --port 8080

    # List recent executions
    nexusflow list --workflow my_pipeline

Design rationale:
  We use Click (not argparse) because it's the de-facto standard for Python
  CLIs: composable subcommands, auto-generated --help, type coercion, and
  parameter validation without boilerplate.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import click

from nexusflow import __version__, Graph, GraphExecutor, ExecutorConfig, SQLiteStore

log = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _load_graph_from_file(path: str) -> Graph:
    """Import a Python file and extract the ``graph`` variable.

    The file should define ``graph = Graph(...)`` at module level.
    """
    abs_path = os.path.abspath(os.path.expanduser(path))
    if not os.path.exists(abs_path):
        raise click.BadParameter(f"File not found: {abs_path}")

    # Derive a module name from the file stem.
    module_name = f"_nexusflow_workflow_{Path(abs_path).stem}"

    spec = importlib.util.spec_from_file_location(module_name, abs_path)
    if spec is None or spec.loader is None:
        raise click.BadParameter(f"Could not load module from {abs_path}")

    module = importlib.util.module_from_spec(spec)
    # Remove cached version if it exists (reload-friendly).
    sys.modules.pop(module_name, None)
    spec.loader.exec_module(module)

    graph = getattr(module, "graph", None)
    if graph is None:
        raise click.BadParameter(
            f"The file {abs_path} must define a top-level `graph` variable "
            "that is an instance of nexusflow.Graph"
        )
    if not isinstance(graph, Graph):
        raise click.BadParameter(
            f"The `graph` variable in {abs_path} is {type(graph).__name__}, "
            "expected nexusflow.Graph"
        )
    return graph


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


# ── CLI group ───────────────────────────────────────────────────────────────

@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__)
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def cli(verbose: bool) -> None:
    """NexusFlow — DAG-based workflow orchestration engine.

    Define workflows as code, run them in parallel, persist state to SQLite,
    schedule with cron, and monitor via the web UI.
    """
    _setup_logging(verbose)


# ── run ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option(
    "--max-workers", "-w", default=0, type=int,
    help="Max parallel workers (0 = auto)",
)
@click.option(
    "--store", "-s", default=None,
    help="Path to SQLite store (default: ~/.nexusflow/store.db)",
)
@click.option(
    "--context", "-c", default=None,
    help="JSON string with extra context passed to the executor",
)
def run(file: str, max_workers: int, store: Optional[str], context: Optional[str]) -> None:
    """Load a workflow from FILE and execute it."""
    graph = _load_graph_from_file(file)

    ctx: dict = {}
    if context:
        try:
            ctx = json.loads(context)
        except json.JSONDecodeError:
            raise click.BadParameter("--context must be valid JSON")

    config = ExecutorConfig(max_workers=max_workers)
    executor = GraphExecutor(graph, config=config, context=ctx)

    click.echo(f"▶  Running workflow: {graph.name} ({len(graph.nodes)} nodes, {len(graph.edges)} edges)")

    if store:
        db = SQLiteStore(store)
    else:
        from nexusflow.storage import get_default_store
        db = get_default_store()

    db.save_workflow(graph)
    exec_id = db.create_execution(graph.id)

    # Wire up logging to the store.
    def _on_change(node, status, ctx):
        db.log_task(exec_id, node.id, status.value, node_name=node.name)

    config.on_status_change = _on_change

    start = time.time()
    try:
        results = executor.run()
        duration = time.time() - start

        # Summarise
        success = sum(1 for n in graph.nodes.values() if n.status.value == "success")
        failed = sum(1 for n in graph.nodes.values() if n.status.value == "failed")
        skipped = sum(1 for n in graph.nodes.values() if n.status.value == "skipped")

        db.update_execution(exec_id, status="success" if not failed else "failed")

        click.echo(f"✔  Done in {duration:.2f}s")
        click.echo(f"   Success: {success}  |  Failed: {failed}  |  Skipped: {skipped}")
        if failed:
            for n in graph.nodes.values():
                if n.status.value in ("failed", "skipped"):
                    click.echo(f"   ✘  {n.name} ({n.id}): {n.error or 'skipped'}")
            sys.exit(1)

    except Exception as exc:
        duration = time.time() - start
        db.update_execution(exec_id, status="failed", error=str(exc))
        click.echo(f"✘  Failed after {duration:.2f}s: {exc}", err=True)
        sys.exit(1)


# ── serve ────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--host", default="127.0.0.1", help="Bind address")
@click.option("--port", "-p", default=8765, type=int, help="Port number")
@click.option("--reload", is_flag=True, help="Auto-reload on code changes (dev)")
@click.option("--store", default=None, help="SQLite store path")
def serve(host: str, port: int, reload: bool, store: Optional[str]) -> None:
    """Start the NexusFlow web dashboard."""
    click.echo(f"▶  Starting NexusFlow server on http://{host}:{port}")

    # We need uvicorn — warn if missing.
    try:
        import uvicorn  # noqa: F401
    except ImportError:
        click.echo(
            "✘  uvicorn is required for the web server.  Install it with:\n"
            "   pip install nexusflow[server]\n"
            "   or\n"
            "   pip install uvicorn",
            err=True,
        )
        sys.exit(1)

    # Pass store path via environment so the server module can pick it up.
    if store:
        os.environ["NEXUSFLOW_STORE_PATH"] = store

    from nexusflow.server import app
    uvicorn.run(app, host=host, port=port, reload=reload)


# ── list ─────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--workflow", "-w", default=None, help="Filter by workflow ID")
@click.option("--store", default=None, help="SQLite store path")
def list(workflow: Optional[str], store: Optional[str]) -> None:
    """List recent workflow executions from the store."""
    if store:
        db = SQLiteStore(store)
    else:
        from nexusflow.storage import get_default_store
        db = get_default_store()

    executions = db.list_executions(workflow)

    if not executions:
        click.echo("No executions found.")
        return

    click.echo(f"{'ID':<14} {'Workflow':<20} {'Status':<10} {'Started':<20} {'Finished':<20}")
    click.echo("─" * 84)
    for ex in executions:
        sid = ex["id"][:12]
        wid = ex["workflow_id"][:12]
        click.echo(
            f"{sid:<14} {wid:<20} {ex['status']:<10} "
            f"{_fmt_time(ex['started_at']):<20} {_fmt_time(ex['finished_at']):<20}"
        )


def _fmt_time(ts: Optional[float]) -> str:
    if ts is None:
        return "—"
    import datetime
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


# ── schedule ─────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--add", "-a", nargs=2, multiple=True,
              metavar="WORKFLOW_ID CRON_EXPR",
              help="Add a schedule (can be used multiple times)")
@click.option("--list-schedules", is_flag=True, help="Show registered schedules")
def schedule(add, list_schedules):
    """Manage scheduled workflows (experimental)."""
    if add:
        from nexusflow.scheduler import CronExpression
        for wf_id, cron_expr in add:
            try:
                cron = CronExpression(cron_expr)
                click.echo(f"✓  {wf_id}: {cron_expr} — next fire: {cron.next_fire()}")
            except ValueError as e:
                click.echo(f"✘  {wf_id}: {e}", err=True)
        return

    if list_schedules:
        click.echo("Use the web UI or database directly to inspect schedules.")
        return

    click.echo("No action specified.  Use --add WORKFLOW_ID CRON_EXPR or --list-schedules.")
    click.echo("Example:  nexusflow schedule --add my_pipeline '*/5 * * * *'")


# ── entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
