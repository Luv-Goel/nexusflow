# ⚡ NexusFlow

**Lightweight DAG-based workflow orchestration engine** — define pipelines as code,
run them in parallel, persist state to SQLite, schedule with cron, and monitor
everything via a web UI or CLI.

```
  ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
  │  Graph   │───▶│   Executor   │───▶│    Store     │───▶│ Scheduler│
  │ (DAG)    │    │ (parallel)   │    │  (SQLite)    │    │ (cron)   │
  └──────────┘    └──────────────┘    └──────────────┘    └──────────┘
       │                 │                  │                  │
       ▼                 ▼                  ▼                  ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                      Web UI (FastAPI) + CLI                      │
  └──────────────────────────────────────────────────────────────────┘
```

## Features

- **DAG-based pipelines** — define workflows as directed acyclic graphs with
  automatic topological sort, cycle detection, and level-based parallel
  execution planning.
- **Parallel execution** — thread pool (default) or process pool backends.
  Nodes at the same dependency level run concurrently.
- **Retry & timeout** — per-node retry count with exponential backoff + full
  jitter, and configurable timeout. Downstream nodes are automatically skipped
  on failure.
- **State persistence** — SQLite-backed storage for workflow definitions,
  execution state, and per-node task logs. Checkpoint/resume lets you retry
  only failed nodes on re-run.
- **Cron scheduling** — register workflows with 5-field cron expressions.
  Built-in background scheduler with 30 s poll granularity.
- **Web dashboard** — FastAPI + HTML/JS UI with live DAG visualisation
  (pure SVG, no canvas/WebGL required), execution monitoring, and run triggers.
- **CLI** — Click-based command-line interface for running workflows, listing
  executions, starting the web server, and managing schedules.
- **Zero deps** — the core engine (`graph`, `executor`, `storage`,
  `scheduler`) imports **nothing outside stdlib**. Only the web server
  requires `fastapi` + `uvicorn`.

## Quick Start

```bash
pip install nexusflow

# Or with web UI support:
pip install nexusflow[server]
```

### 1. Define a workflow

Create a Python file, e.g. `etl_pipeline.py`:

```python
from nexusflow import Graph, Node

def extract():
    return {"data": [1, 2, 3, 4, 5]}

def transform(data, *, scale=2):
    return [x * scale for x in data["data"]]

def load(transformed_data):
    print(f"Loaded: {transformed_data}")
    return {"records": len(transformed_data)}

graph = Graph("etl_pipeline")
graph.add_node(name="extract", fn=extract)
graph.add_node(name="transform", fn=transform, params={"scale": 2})
graph.add_node(name="load", fn=load)
graph.add_edge("extract", "transform")
graph.add_edge("transform", "load")
```

### 2. Run it

```bash
nexusflow run etl_pipeline.py
```

### 3. Start the web UI

```bash
nexusflow serve --port 8765
# Open http://localhost:8765
```

### 4. Schedule it

```python
from nexusflow.scheduler import CronScheduler

sched = CronScheduler(run_callback=lambda wf_id: print(f"Running {wf_id}"))
sched.add("my_etl", "0 */2 * * *")  # every 2 hours
sched.start()
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        User Code (Python)                        │
│  Define Graph → add_node / add_edge                              │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  nexusflow.graph                                                 │
│  ┌─────┐  ┌─────┐  ┌──────────────┐  ┌───────────────────────┐  │
│  │Node │  │Edge │  │ topological  │  │  execution_plan()     │  │
│  │.fn  │  │src→ │  │ _sort()      │  │  {level: [nodes...]}  │  │
│  │.ret │  │tgt  │  │ detect_cycle │  │                       │  │
│  └─────┘  └─────┘  └──────────────┘  └───────────────────────┘  │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  nexusflow.executor                                             │
│  ┌────────────────┐  ┌──────────────┐  ┌─────────────────────┐  │
│  │ Level-by-level │  │ Retry with   │  │ Failure propagation │  │
│  │ parallel exec  │  │ backoff+jit  │  │ → downstream skip   │  │
│  └────────────────┘  └──────────────┘  └─────────────────────┘  │
└──────────┬───────────────────────┬──────────────────────────────┘
           │                       │
           ▼                       ▼
┌──────────────────┐   ┌──────────────────────┐
│  nexusflow.storage│   │  nexusflow.scheduler │
│  ┌──────────────┐ │   │  ┌────────────────┐  │
│  │ SQLite       │ │   │  │ CronExpression │  │
│  │ workflows    │ │   │  │ Poll loop      │  │
│  │ executions   │ │   │  │ Thread         │  │
│  │ task_logs    │ │   │  └────────────────┘  │
│  └──────────────┘ │   └──────────────────────┘
└──────────────────┘
```

## API Reference

### `nexusflow.graph`

| Class / Function | Description |
|---|---|
| `Node(id, name, fn, params, retries, timeout)` | A unit of work. `fn(*upstream_results, **params)` |
| `Edge(source, target)` | Dependency link. |
| `Graph(name)` | DAG container. Methods: `add_node`, `add_edge`, `topological_sort`, `detect_cycles`, `execution_plan`, `upstream_of`, `downstream_of`, `to_dict`, `from_dict`. |
| `TopologicalSortResult` | Contains `order`, `levels`, `has_cycle`, `cycle_nodes`. |

### `nexusflow.executor`

| Class / Function | Description |
|---|---|
| `ExecutorConfig(max_workers, use_processes, retry_backoff_*)` | Execution parameters. |
| `GraphExecutor(graph, config, context)` | Runs a graph. Events: `on_node_start`, `on_node_success`, `on_node_failure`, `on_node_skipped`. |
| `executor.run()` | Execute → returns `{node_id: result}`. |

### `nexusflow.storage`

| Class / Function | Description |
|---|---|
| `SQLiteStore(db_path)` | Persistence layer. Methods: `save_workflow`, `load_workflow`, `list_workflows`, `create_execution`, `update_execution`, `get_execution`, `list_executions`, `log_task`, `get_task_logs`, `resume_state`. |

### `nexusflow.scheduler`

| Class / Function | Description |
|---|---|
| `CronExpression(expr)` | Parse and match cron expressions. Methods: `match(dt)`, `next_fire()`. |
| `CronScheduler(run_callback, poll_interval)` | Background scheduler. Methods: `add`, `remove`, `start`, `stop`. |

### CLI

```
nexusflow run FILE              Execute a workflow
nexusflow serve [--port PORT]   Start web dashboard
nexusflow list [--workflow ID]  List executions
nexusflow schedule --add ID CRON  Add a schedule
```

## Examples

### ETL Pipeline

```python
from nexusflow import Graph

def fetch_api():
    return [{"id": 1, "value": 10}, {"id": 2, "value": 20}]

def clean(raw_data, *, threshold=5):
    return [r for r in raw_data if r["value"] > threshold]

def transform(clean_data):
    return [{"id": r["id"], "score": r["value"] * 2} for r in clean_data]

def load(transformed_data):
    print(f"Inserting {len(transformed_data)} records...")
    return {"inserted": len(transformed_data)}

g = Graph("etl")
g.add_node("fetch", fn=fetch_api)
g.add_node("clean", fn=clean, params={"threshold": 5})
g.add_node("transform", fn=transform)
g.add_node("load", fn=load)
g.add_edge("fetch", "clean")
g.add_edge("clean", "transform")
g.add_edge("transform", "load")
```

### Parallel Branching

```python
g = Graph("parallel_demo")
g.add_node("split", fn=lambda: {"a": 1, "b": 2})
g.add_node("branch_a", fn=lambda split: split["a"] * 10)
g.add_node("branch_b", fn=lambda split: split["b"] * 20)
g.add_node("merge", fn=lambda a, b: a + b)
g.add_edge("split", "branch_a")
g.add_edge("split", "branch_b")
g.add_edge("branch_a", "merge")
g.add_edge("branch_b", "merge")
```

## Development

```bash
git clone https://github.com/yourorg/nexusflow
cd nexusflow
pip install -e ".[dev]"
pytest tests/
```

## License

MIT — see [LICENSE.txt](LICENSE.txt).
