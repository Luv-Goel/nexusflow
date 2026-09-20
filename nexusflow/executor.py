"""
executor.py — Parallel task executor with dependency resolution, retry, and timeout.

Design rationale:
  The executor takes a :class:`~nexusflow.graph.Graph` and runs it according to
  the execution plan computed by the graph's built-in topological sort + level
  assignment.  Nodes at the same level are safe to run concurrently; nodes in
  later levels must wait for all upstream dependencies to finish.

  We offer two execution backends:
    - **thread** (default) — low overhead, good for I/O-bound or Python-bound
      work that releases the GIL (e.g. HTTP calls, NumPy).
    - **process** — sidesteps the GIL for CPU-heavy pure-Python work.

  Retry strategy:
    Exponential backoff with full jitter (base 1 s, cap 60 s) to avoid
    thundering-herd restarts.

  Failure propagation:
    When a node fails (or times out) after exhausting its retries, all
    downstream nodes are marked ``SKIPPED`` with an explanatory message.
    This matches the behaviour of Airflow / Prefect and prevents wasted work.
"""

from __future__ import annotations

import concurrent.futures
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from nexusflow.graph import Graph, Node, NodeStatus

log = logging.getLogger(__name__)


# ── configuration ────────────────────────────────────────────────────────────

@dataclass
class ExecutorConfig:
    """Controls how the executor behaves.

    Parameters
    ----------
    max_workers : int
        Max parallel threads/processes (default: CPU count × 2).
    use_processes : bool
        If True, use ``ProcessPoolExecutor`` instead of ``ThreadPoolExecutor``.
        Only beneficial for CPU-bound pure-Python nodes.
    retry_backoff_base : float
        Base delay (seconds) for exponential backoff.
    retry_backoff_max : float
        Maximum delay (seconds) for exponential backoff.
    on_status_change : Callable | None
        Optional callback fired whenever a node's status changes.  Useful for
        hooking into the web UI or external monitoring.
    """
    max_workers: int = 0  # 0 = auto-detect
    use_processes: bool = False
    retry_backoff_base: float = 1.0
    retry_backoff_max: float = 60.0
    on_status_change: Optional[Callable[[Node, NodeStatus, Optional[dict]], None]] = None


# ── callbacks emitted by the executor ────────────────────────────────────────

class ExecutorEvent:
    """Simple typed event hooks.  Register with ``+=``, deregister with ``-=``."""
    def __init__(self) -> None:
        self._handlers: list[Callable[..., None]] = []

    def __iadd__(self, handler: Callable[..., None]) -> "ExecutorEvent":
        self._handlers.append(handler)
        return self

    def __isub__(self, handler: Callable[..., None]) -> "ExecutorEvent":
        self._handlers.remove(handler)
        return self

    def emit(self, *args: Any, **kwargs: Any) -> None:
        for h in self._handlers:
            try:
                h(*args, **kwargs)
            except Exception:
                log.exception("ExecutorEvent handler raised")


# ── execution helpers ────────────────────────────────────────────────────────

def _retry_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter.

    ``delay = random.uniform(0, min(cap, base * 2 ** attempt))``

    Why full jitter?  It spreads retries across the window and prevents
    coordinated restarts (the "thundering herd" problem).
    """
    return random.uniform(0, min(cap, base * 2 ** attempt))


# ── GraphExecutor ────────────────────────────────────────────────────────────

class GraphExecutor:
    """Runs a :class:`~nexusflow.graph.Graph` with parallel execution.

    Usage
    -----
    >>> g = Graph("my_pipeline")
    >>> # … add nodes & edges …
    >>> ex = GraphExecutor(g)
    >>> results = ex.run()
    >>> print(ex.execution_log)
    """

    def __init__(
        self,
        graph: Graph,
        config: Optional[ExecutorConfig] = None,
        context: Optional[dict] = None,
    ) -> None:
        self.graph = graph
        self.config = config or ExecutorConfig()
        self.context: dict = context or {}

        # Event hooks — external code can subscribe to these.
        self.on_node_start = ExecutorEvent()
        self.on_node_success = ExecutorEvent()
        self.on_node_failure = ExecutorEvent()
        self.on_node_skipped = ExecutorEvent()

        # Execution log: list of (node_id, status, detail)
        self.execution_log: list[dict] = []

        # Thread safety for status changes.
        self._lock = threading.Lock()

    # ── public API ──────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """Execute the graph's execution plan.

        Returns
        -------
        dict[str, Any]
            ``{node_id: result, …}``  Includes results for successful nodes
            only; failed/skipped nodes are absent or ``None``.
        """
        plan = self.graph.execution_plan()
        if not plan:
            log.warning("Graph %s has an empty execution plan", self.graph.name)
        if self.graph.topological_sort().has_cycle:
            raise RuntimeError(
                f"Graph {self.graph.name!r} contains a cycle in nodes: "
                f"{self.graph.detect_cycles()}"
            )

        results: dict[str, Any] = {}

        # Copy nodes so runtime state doesn't mutate the original graph.
        # We work on a local copy to keep the graph reusable.
        nodes = {
            nid: Node(
                id=n.id,
                name=n.name,
                fn=n.fn,
                params=dict(n.params),
                retries=n.retries,
                timeout=n.timeout,
            )
            for nid, n in self.graph._nodes.items()
        }

        # Resolve upstream results for each node — we need to know which
        # node ids come before which in the dependency chain.
        # upstream_map[nid] = list of upstream node ids (in topo order)
        topo_order = self.graph.topological_sort().order
        upstream_order: dict[str, list[str]] = {}
        for nid in topo_order:
            upstream_order[nid] = [e.source for e in self.graph._in_edges[nid]]

        executor_cls = (
            concurrent.futures.ProcessPoolExecutor
            if self.config.use_processes
            else concurrent.futures.ThreadPoolExecutor
        )

        max_workers = self.config.max_workers or (len(topo_order) or 1)
        # Use a shared executor across all levels so we don't pay process
        # spawn overhead per level.
        with executor_cls(max_workers=max_workers) as pool:
            for level in sorted(plan):
                level_nodes = plan[level]
                fut_to_nid: dict[concurrent.futures.Future, str] = {}

                for nid in level_nodes:
                    node = nodes[nid]

                    # ── skip if any upstream failed ────────────────────
                    skip_reason = self._check_skip(node, nodes, results)
                    if skip_reason:
                        self._mark_skipped(node, skip_reason, nodes)
                        continue

                    # Gather upstream results as positional args.
                    args = [
                        results[up_id]
                        for up_id in upstream_order[nid]
                        if up_id in results
                    ]

                    future = pool.submit(
                        self._run_node_with_retries, node, args
                    )
                    fut_to_nid[future] = nid

                # Wait for all futures in this level.
                for future in concurrent.futures.as_completed(fut_to_nid):
                    nid = fut_to_nid[future]
                    node = nodes[nid]
                    try:
                        result = future.result()
                        results[nid] = result
                    except Exception as exc:
                        # The exception was already logged in _run_node_with_retries
                        # but we re-raise here so downstream nodes can see the failure.
                        # Downstream detection is handled by _check_skip above.
                        pass

        # Store final node states back into the graph for reference.
        for nid, node in nodes.items():
            if nid in self.graph._nodes:
                original = self.graph._nodes[nid]
                original.status = node.status
                original.result = node.result
                original.error = node.error
                original.started_at = node.started_at
                original.finished_at = node.finished_at

        return results

    # ── internal helpers ────────────────────────────────────────────────

    def _check_skip(
        self,
        node: Node,
        nodes: dict[str, Node],
        results: dict[str, Any],
    ) -> Optional[str]:
        """Return a skip reason if any upstream node failed/skipped."""
        for up_id in self.graph.upstream_of(node.id):
            up_node = nodes[up_id]
            if up_node.status in (NodeStatus.FAILED, NodeStatus.SKIPPED, NodeStatus.TIMEOUT):
                return f"Upstream node {up_id!r} ({up_node.name}) {up_node.status.value}"
        return None

    def _mark_skipped(self, node: Node, reason: str, nodes: dict[str, Node]) -> None:
        with self._lock:
            node.status = NodeStatus.SKIPPED
            node.error = reason
        self.execution_log.append({
            "node_id": node.id,
            "node_name": node.name,
            "status": NodeStatus.SKIPPED.value,
            "detail": reason,
        })
        self.on_node_skipped.emit(node, reason, self.context)
        if self.config.on_status_change:
            self.config.on_status_change(node, NodeStatus.SKIPPED, self.context)
        log.info("Node %s (%s) SKIPPED: %s", node.id, node.name, reason)

    def _run_node_with_retries(self, node: Node, args: list) -> Any:
        """Execute a single node with retry + timeout logic.

        This method runs in a thread/process-pool worker.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(1 + node.retries):
            try:
                # ── timeout via thread trick ──────────────────────────
                # concurrent.futures doesn't support timeout inside a future
                # directly, so we handle it by wrapping the call in a
                # short-lived future *if* a timeout is set.
                if node.timeout is not None and node.timeout > 0:

                    def _target() -> Any:
                        node.started_at = time.time()
                        self._emit_start(node)
                        if node.fn is None:
                            return None
                        return node.fn(*args, **node.params)

                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as mini_pool:
                        fut = mini_pool.submit(_target)
                        try:
                            result = fut.result(timeout=node.timeout)
                        except concurrent.futures.TimeoutError:
                            node.status = NodeStatus.TIMEOUT
                            node.error = f"Timed out after {node.timeout}s"
                            node.finished_at = time.time()
                            self.execution_log.append({
                                "node_id": node.id,
                                "node_name": node.name,
                                "status": NodeStatus.TIMEOUT.value,
                                "detail": node.error,
                                "attempt": attempt,
                            })
                            self.on_node_failure.emit(node, node.error, self.context)
                            if self.config.on_status_change:
                                self.config.on_status_change(node, NodeStatus.TIMEOUT, self.context)
                            log.error(
                                "Node %s (%s) TIMEOUT after %ss (attempt %d/%d)",
                                node.id, node.name, node.timeout, attempt + 1, node.retries + 1,
                            )
                            raise RuntimeError(node.error)
                        else:
                            node.finished_at = time.time()
                            node.result = result
                            node.status = NodeStatus.SUCCESS
                            self.execution_log.append({
                                "node_id": node.id,
                                "node_name": node.name,
                                "status": NodeStatus.SUCCESS.value,
                                "detail": None,
                                "attempt": attempt,
                            })
                            self.on_node_success.emit(node, result, self.context)
                            if self.config.on_status_change:
                                self.config.on_status_change(node, NodeStatus.SUCCESS, self.context)
                            return result

                else:
                    # No timeout — call directly (avoids thread overhead).
                    node.started_at = time.time()
                    self._emit_start(node)
                    if node.fn is None:
                        node.result = None
                        node.status = NodeStatus.SUCCESS
                        node.finished_at = time.time()
                        self._emit_success(node)
                        return None

                    result = node.fn(*args, **node.params)
                    node.result = result
                    node.status = NodeStatus.SUCCESS
                    node.finished_at = time.time()
                    self._emit_success(node)
                    return result

            except Exception as exc:
                last_exc = exc
                if node.status != NodeStatus.TIMEOUT:
                    node.status = NodeStatus.FAILED
                node.error = str(exc)
                node.finished_at = time.time()
                self.execution_log.append({
                    "node_id": node.id,
                    "node_name": node.name,
                    "status": node.status.value,
                    "detail": str(exc),
                    "attempt": attempt,
                })

                # ── retry? ──────────────────────────────────────────────
                if attempt < node.retries:
                    delay = _retry_delay(
                        attempt,
                        self.config.retry_backoff_base,
                        self.config.retry_backoff_max,
                    )
                    log.warning(
                        "Node %s (%s) failed (attempt %d/%d), retrying in %.2fs: %s",
                        node.id, node.name,
                        attempt + 1, node.retries + 1,
                        delay, exc,
                    )
                    time.sleep(delay)
                    # Reset status so the next attempt can set RUNNING again.
                    node.status = NodeStatus.PENDING
                    node.error = None
                else:
                    # No more retries — propagate failure.
                    log.error(
                        "Node %s (%s) failed after %d attempt(s): %s",
                        node.id, node.name, node.retries + 1, exc,
                    )
                    self.on_node_failure.emit(node, str(exc), self.context)
                    if self.config.on_status_change:
                        self.config.on_status_change(node, NodeStatus.FAILED, self.context)
                    raise  # re-raises last_exc

        # Shouldn't be reached, but just in case:
        raise RuntimeError(f"Node {node.id} failed after {node.retries + 1} attempts")  # type: ignore

    def _emit_start(self, node: Node) -> None:
        with self._lock:
            node.status = NodeStatus.RUNNING
        self.execution_log.append({
            "node_id": node.id,
            "node_name": node.name,
            "status": NodeStatus.RUNNING.value,
            "detail": None,
        })
        self.on_node_start.emit(node, self.context)
        if self.config.on_status_change:
            self.config.on_status_change(node, NodeStatus.RUNNING, self.context)
        log.debug("Node %s (%s) STARTED", node.id, node.name)

    def _emit_success(self, node: Node) -> None:
        self.execution_log.append({
            "node_id": node.id,
            "node_name": node.name,
            "status": NodeStatus.SUCCESS.value,
            "detail": None,
        })
        self.on_node_success.emit(node, node.result, self.context)
        if self.config.on_status_change:
            self.config.on_status_change(node, NodeStatus.SUCCESS, self.context)
        log.info("Node %s (%s) SUCCESS", node.id, node.name)
