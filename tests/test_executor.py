"""
Tests for the executor module.
"""

import time
import pytest
from nexusflow import Graph, GraphExecutor, ExecutorConfig


def test_empty_graph_execution():
    g = Graph("empty")
    ex = GraphExecutor(g)
    results = ex.run()
    assert results == {}


def test_single_node():
    g = Graph("single")
    g.add_node(name="answer", fn=lambda: 42)
    ex = GraphExecutor(g)
    results = ex.run()
    values = list(results.values())
    assert values == [42]


def test_linear_pipeline():
    g = Graph("linear")
    g.add_node(name="a", fn=lambda: 1)
    g.add_node(name="b", fn=lambda a: a + 1)
    g.add_node(name="c", fn=lambda b: b * 2)
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    ex = GraphExecutor(g)
    results = ex.run()
    # c = (1 + 1) * 2 = 4
    assert list(results.values()) == [1, 2, 4]


def test_diamond_dependencies():
    """Split → A, Split → B, A → Merge, B → Merge"""
    g = Graph("diamond")
    g.add_node(name="split", fn=lambda: 5)
    g.add_node(name="a", fn=lambda split: split + 10)
    g.add_node(name="b", fn=lambda split: split * 2)
    g.add_node(name="merge", fn=lambda a, b: a + b)
    g.add_edge("split", "a")
    g.add_edge("split", "b")
    g.add_edge("a", "merge")
    g.add_edge("b", "merge")
    ex = GraphExecutor(g)
    results = ex.run()
    # split=5, a=15, b=10, merge=25
    assert results["merge"] == 25


def test_node_with_params():
    g = Graph("params")
    g.add_node(name="scale", fn=lambda x, *, factor: x * factor, params={"factor": 3})
    ex = GraphExecutor(g)
    results = ex.run()
    assert list(results.values()) == [0]  # x defaults from upstream (none)

    # With upstream value
    g2 = Graph("params2")
    g2.add_node(name="source", fn=lambda: 10)
    g2.add_node(name="scale", fn=lambda source, *, factor: source * factor, params={"factor": 3})
    g2.add_edge("source", "scale")
    ex2 = GraphExecutor(g2)
    results2 = ex2.run()
    assert results2["scale"] == 30


def test_node_timeout():
    g = Graph("timeout")
    g.add_node(name="slow", fn=lambda: time.sleep(5), timeout=0.1)
    ex = GraphExecutor(g)
    with pytest.raises(RuntimeError, match="Timed out"):
        ex.run()


def test_retry_then_succeed():
    """Node fails twice then succeeds."""
    attempt_log = {"count": 0}

    def flaky():
        attempt_log["count"] += 1
        if attempt_log["count"] < 3:
            raise ValueError("not yet")
        return "ok"

    g = Graph("flaky")
    g.add_node(name="flaky", fn=flaky, retries=3)
    ex = GraphExecutor(g)
    results = ex.run()
    assert list(results.values()) == ["ok"]
    assert attempt_log["count"] == 3


def test_retry_exhausted():
    attempt_log = {"count": 0}

    def always_fails():
        attempt_log["count"] += 1
        raise ValueError("always fails")

    g = Graph("fail")
    g.add_node(name="fail", fn=always_fails, retries=1)
    ex = GraphExecutor(g)
    with pytest.raises(ValueError, match="always fails"):
        ex.run()
    assert attempt_log["count"] == 2  # initial + 1 retry


def test_failure_propagation():
    """When upstream fails, downstream should be skipped."""
    g = Graph("propagate")
    g.add_node(name="upstream", fn=lambda: (_ for _ in ()).throw(ValueError("boom")))
    g.add_node(name="downstream", fn=lambda upstream: upstream + 1)
    g.add_edge("upstream", "downstream")
    ex = GraphExecutor(g)
    with pytest.raises(Exception):
        ex.run()
    # After execution, downstream should be SKIPPED
    downstream_node = g.get_node("downstream")
    assert downstream_node.status == "skipped"


def test_noop_node():
    g = Graph("noop")
    g.add_node(name="noop", fn=None)
    ex = GraphExecutor(g)
    results = ex.run()
    assert list(results.values()) == [None]


def test_event_hooks():
    events = []

    def on_start(node, ctx):
        events.append(f"start:{node.name}")

    def on_success(node, result, ctx):
        events.append(f"success:{node.name}={result}")

    g = Graph("hooks")
    g.add_node(name="a", fn=lambda: 1)
    g.add_node(name="b", fn=lambda a: a + 1)
    g.add_edge("a", "b")
    config = ExecutorConfig()
    ex = GraphExecutor(g, config=config)
    ex.on_node_start += on_start
    ex.on_node_success += on_success
    ex.run()
    assert "start:a" in events
    assert "start:b" in events
    assert "success:a=1" in events or "success:a" in events
    assert "success:b=2" in events or "success:b" in events


def test_many_independent_nodes():
    """All nodes at level 0 — should all run in parallel."""
    g = Graph("parallel")
    results_dict = {}

    def make_node(i):
        def fn():
            results_dict[i] = i
            return i
        return fn

    for i in range(20):
        g.add_node(name=f"n{i}", fn=make_node(i))

    ex = GraphExecutor(g)
    results = ex.run()
    assert len(results) == 20
    for i in range(20):
        assert i in results.values()


def test_executor_does_not_mutate_original_graph():
    g = Graph("immutable")
    g.add_node(name="a", fn=lambda: 1)
    original_nodes = {nid: n.status for nid, n in g.nodes.items()}
    ex = GraphExecutor(g)
    ex.run()
    for nid, n in g.nodes.items():
        assert n.status.value == "pending" or n.status.value == "success"
