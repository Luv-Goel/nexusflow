"""
graph.py — DAG data structure with topological sort, cycle detection, and level assignment.

Design rationale:
  We treat workflows as directed acyclic graphs where each *node* is a unit of
  work (a Python callable + its parameters) and each *edge* encodes a dependency
  ("B depends on A").  The executor later walks these edges to figure out what
  can run in parallel and what must wait.

  Cycle detection is essential: an undetected cycle would deadlock the executor.
  We use Kahn's algorithm (BFS-based) because it gives us both a cycle-free
  guarantee AND a valid topological order in one pass.

  Level assignment builds on the topological order: it groups nodes into "ranks"
  such that all nodes at level N can safely run in parallel after every node
  at level < N has completed.  This is what lets the executor maximise
  parallelism.
"""

from __future__ import annotations

import collections
import enum
import inspect
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_id() -> str:
    """Short, URL-safe unique identifier for nodes & workflows."""
    return uuid.uuid4().hex[:12]


# ── Node status ──────────────────────────────────────────────────────────────

class NodeStatus(enum.Enum):
    """Tracks where a node is in its lifecycle."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"       # upstream failure → downstream skipped
    TIMEOUT = "timeout"


# ── Callback types ───────────────────────────────────────────────────────────
# We keep these as type aliases for readability in the dataclass below.

NodeFn = Callable[..., Any]
"""Signature: f(*upstream_results, **params) -> Any"""

OnStatusChange = Callable[["Node", NodeStatus, Optional[dict]], None]
"""Signature: f(node, new_status, context)"""


# ── Node ─────────────────────────────────────────────────────────────────────

@dataclass
class Node:
    """A single unit of work in a workflow DAG.

    Attributes
    ----------
    id : str
        Unique identifier (auto-generated if not provided).
    name : str
        Human-readable label (defaults to *id*).
    fn : NodeFn | None
        The callable to execute.  Can be ``None`` if the node is a no-op
        (useful as a dependency synchronisation barrier).
    params : dict
        Keyword arguments forwarded to *fn* at execution time.
    retries : int
        How many times to retry on failure (0 = no retry).  Uses exponential
        backoff with jitter.
    timeout : float | None
        Maximum seconds the node is allowed to run.  ``None`` means no limit.
    status : NodeStatus
        Current lifecycle status (managed by the executor).

    Notes
    -----
    The *fn* callable receives **positional** arguments for each upstream
    node's result (in topological order) plus the **keyword** arguments from
    *params*.  This design keeps wiring explicit:
        def merge(upstream_a_result, upstream_b_result, *, threshold=0.5): ...
    """

    id: str = field(default_factory=_make_id)
    name: str = ""
    fn: Optional[NodeFn] = None
    params: dict = field(default_factory=dict)
    retries: int = 0
    timeout: Optional[float] = None

    # ── runtime state (mutated by executor) ──────────────────────────────
    status: NodeStatus = NodeStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.name:
            self.name = self.id

    def __repr__(self) -> str:
        return f"Node(id={self.id!r}, name={self.name!r}, status={self.status.name})"


# ── Edge ─────────────────────────────────────────────────────────────────────

@dataclass
class Edge:
    """A dependency link between two nodes.

    ``source → target`` means *target depends on source*.
    """
    source: str          # source node id
    target: str          # target node id

    def __repr__(self) -> str:
        return f"Edge({self.source!r} -> {self.target!r})"


# ── Topological / cycle-detection result ─────────────────────────────────────

@dataclass
class TopologicalSortResult:
    """Returned by :meth:`Graph.topological_sort`."""
    order: list[str]        # node ids in a valid topological order
    levels: dict[str, int]  # node_id → level number (0-indexed)
    has_cycle: bool
    cycle_nodes: list[str]  # nodes involved if a cycle exists, else empty


# ── Graph ────────────────────────────────────────────────────────────────────

class Graph:
    """A directed acyclic graph of workflow nodes.

    Usage
    -----
    >>> g = Graph("etl_pipeline")
    >>> extract = g.add_node(name="extract", fn=extract_fn)
    >>> transform = g.add_node(name="transform", fn=transform_fn)
    >>> load = g.add_node(name="load", fn=load_fn)
    >>> g.add_edge(extract, transform)
    >>> g.add_edge(transform, load)
    """

    def __init__(self, name: str = "", metadata: Optional[dict] = None) -> None:
        self.id: str = _make_id()
        self.name: str = name or f"workflow-{self.id}"
        self.metadata: dict = metadata or {}

        # Internal adjacency representation.
        # We keep both forward and reverse maps for efficient traversal.
        self._nodes: dict[str, Node] = {}
        self._edges: list[Edge] = []
        self._in_degree: dict[str, int] = {}      # node_id → count of incoming edges
        self._out_edges: dict[str, list[Edge]] = collections.defaultdict(list)
        self._in_edges: dict[str, list[Edge]] = collections.defaultdict(list)

        # Cache for computed properties (invalidated on mutation).
        self._topo_cache: Optional[TopologicalSortResult] = None

    # ── mutation helpers ─────────────────────────────────────────────────

    def add_node(
        self,
        node: Optional[Node] = None,
        *,
        name: str = "",
        fn: Optional[NodeFn] = None,
        params: Optional[dict] = None,
        retries: int = 0,
        timeout: Optional[float] = None,
    ) -> Node:
        """Register a new node in the graph.

        Provide either a pre-built *node* instance, or keyword arguments
        that will be used to construct one.
        """
        if node is not None:
            if node.id in self._nodes:
                raise ValueError(f"Node {node.id!r} already exists in graph {self.name!r}")
            self._nodes[node.id] = node
        else:
            n = Node(
                name=name,
                fn=fn,
                params=params or {},
                retries=retries,
                timeout=timeout,
            )
            self._nodes[n.id] = n
            node = n

        # Initialise degree book-keeping.
        if node.id not in self._in_degree:
            self._in_degree[node.id] = 0

        self._invalidate_cache()
        return node

    def add_edge(self, source: str | Node, target: str | Node) -> Edge:
        """Declare that *target* depends on *source*.

        Parameters
        ----------
        source : str | Node
            Node id or Node instance that must complete first.
        target : str | Node
            Node id or Node instance that depends on *source*.
        """
        src_id = source.id if isinstance(source, Node) else source
        tgt_id = target.id if isinstance(target, Node) else target

        if src_id not in self._nodes:
            raise ValueError(f"Source node {src_id!r} not found in graph")
        if tgt_id not in self._nodes:
            raise ValueError(f"Target node {tgt_id!r} not found in graph")

        # Guard against duplicate edges (silent no-op).
        for e in self._out_edges[src_id]:
            if e.target == tgt_id:
                return e

        edge = Edge(source=src_id, target=tgt_id)
        self._edges.append(edge)
        self._out_edges[src_id].append(edge)
        self._in_edges[tgt_id].append(edge)
        self._in_degree[tgt_id] += 1

        self._invalidate_cache()
        return edge

    def _invalidate_cache(self) -> None:
        self._topo_cache = None

    # ── accessors ────────────────────────────────────────────────────────

    @property
    def nodes(self) -> dict[str, Node]:
        return dict(self._nodes)

    @property
    def edges(self) -> list[Edge]:
        return list(self._edges)

    def get_node(self, node_id: str) -> Node:
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id!r} not found")
        return self._nodes[node_id]

    # ── cycle detection & topological sort ───────────────────────────────

    def topological_sort(self) -> TopologicalSortResult:
        """Compute a topological ordering using Kahn's algorithm.

        Returns
        -------
        TopologicalSortResult
            Contains the ordered node ids, level map, and cycle info.

        Notes
        -----
        We use Kahn's algorithm because:
        1.  It detects cycles natively (if the result doesn't include all nodes).
        2.  It naturally produces a BFS-like ordering that maps well to
            parallel execution levels.
        3.  It runs in O(V + E) time.

        The level assignment groups nodes that can run concurrently:
        level 0 = no dependencies (sources), level 1 = depend only on level 0, etc.
        """
        if self._topo_cache is not None:
            return self._topo_cache

        # Build a mutable in-degree map for the algorithm.
        in_degree = {nid: self._in_degree.get(nid, 0) for nid in self._nodes}
        # Initialise the queue with all nodes that have zero in-degree.
        queue: collections.deque[str] = collections.deque(
            nid for nid, deg in in_degree.items() if deg == 0
        )

        order: list[str] = []
        levels: dict[str, int] = {}

        # BFS layer-by-layer so we can assign levels.
        while queue:
            # All nodes currently in the queue are at the same "frontier" level.
            current_level_nodes = list(queue)
            queue.clear()

            for nid in current_level_nodes:
                order.append(nid)
                # Level = length of the longest path from any source to this node.
                # We compute this iteratively during the BFS.
                parent_levels = [
                    levels.get(e.source, -1) + 1
                    for e in self._in_edges[nid]
                ]
                levels[nid] = max(parent_levels) if parent_levels else 0

                # Decrease in-degree for downstream nodes.
                for edge in self._out_edges[nid]:
                    in_degree[edge.target] -= 1
                    if in_degree[edge.target] == 0:
                        queue.append(edge.target)

        # If we haven't visited every node, there's a cycle.
        has_cycle = len(order) < len(self._nodes)
        cycle_nodes: list[str] = []
        if has_cycle:
            visited = set(order)
            cycle_nodes = [nid for nid in self._nodes if nid not in visited]

        result = TopologicalSortResult(
            order=order,
            levels=levels,
            has_cycle=has_cycle,
            cycle_nodes=cycle_nodes,
        )
        self._topo_cache = result
        return result

    def detect_cycles(self) -> list[str]:
        """Convenience wrapper — returns list of cycle-involved node ids."""
        result = self.topological_sort()
        return result.cycle_nodes if result.has_cycle else []

    # ── level-based execution planning ───────────────────────────────────

    def execution_plan(self) -> dict[int, list[str]]:
        """Group node ids by their execution level.

        Returns
        -------
        dict[int, list[str]]
            level → [node_id, …].  Level 0 runs first, then level 1, etc.
        """
        result = self.topological_sort()
        plan: dict[int, list[str]] = collections.defaultdict(list)
        for nid, lvl in result.levels.items():
            plan[lvl].append(nid)
        return dict(sorted(plan.items()))

    # ── utilities ────────────────────────────────────────────────────────

    def upstream_of(self, node_id: str) -> list[str]:
        """Return all node ids that must run before *node_id*."""
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            nid = stack.pop()
            for edge in self._in_edges[nid]:
                if edge.source not in visited:
                    visited.add(edge.source)
                    stack.append(edge.source)
        return list(visited)

    def downstream_of(self, node_id: str) -> list[str]:
        """Return all node ids that depend on *node_id*."""
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            nid = stack.pop()
            for edge in self._out_edges[nid]:
                if edge.target not in visited:
                    visited.add(edge.target)
                    stack.append(edge.target)
        return list(visited)

    def to_dict(self) -> dict:
        """Serialise the graph to a JSON-safe dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "metadata": self.metadata,
            "nodes": [
                {
                    "id": n.id,
                    "name": n.name,
                    "params": n.params,
                    "retries": n.retries,
                    "timeout": n.timeout,
                    "status": n.status.value,
                }
                for n in self._nodes.values()
            ],
            "edges": [
                {"source": e.source, "target": e.target}
                for e in self._edges
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Graph":
        """Deserialise a graph from a dictionary produced by *to_dict*."""
        g = cls(name=data.get("name", ""), metadata=data.get("metadata", {}))
        g.id = data.get("id", g.id)
        for nd in data.get("nodes", []):
            node = Node(
                id=nd["id"],
                name=nd.get("name", nd["id"]),
                params=nd.get("params", {}),
                retries=nd.get("retries", 0),
                timeout=nd.get("timeout"),
            )
            node.status = NodeStatus(nd.get("status", "pending"))
            g._nodes[node.id] = node
        for ed in data.get("edges", []):
            g.add_edge(ed["source"], ed["target"])
        return g

    def __repr__(self) -> str:
        return (
            f"Graph(id={self.id!r}, name={self.name!r}, "
            f"nodes={len(self._nodes)}, edges={len(self._edges)})"
        )
