"""
nexusflow — A lightweight DAG-based workflow orchestration engine.

NexusFlow lets you define workflows as directed acyclic graphs (DAGs),
execute them in parallel (thread- or process-based), persist state to SQLite,
schedule runs with cron expressions, and monitor everything via a web UI or CLI.

Typical use cases:
  - ETL pipelines (extract → transform → load)
  - Data science workflows (ingest → clean → featurize → train → evaluate)
  - CI/CD task orchestration
  - Multi-stage parallel processing jobs

Exposed public API:
  - graph:       Node, Edge, Graph — define your DAG
  - executor:    GraphExecutor — run it with retries, timeouts, parallelism
  - storage:     SQLiteStore — persist & resume workflows
  - scheduler:   CronScheduler — schedule workflows on cron expressions
  - cli:         nexusflow CLI (via `python -m nexusflow.cli` or `nexusflow`)
  - server:      FastAPI web app + UI (via `nexusflow serve`)
"""

from nexusflow.graph import Node, Edge, Graph
from nexusflow.executor import GraphExecutor, ExecutorConfig
from nexusflow.storage import SQLiteStore

__all__ = [
    "Node",
    "Edge",
    "Graph",
    "GraphExecutor",
    "ExecutorConfig",
    "SQLiteStore",
]

__version__ = "0.1.0"
