"""
Tests for the DAG graph module.
"""

import pytest
from nexusflow import Graph, Node, Edge


class TestGraphConstruction:
    def test_empty_graph(self):
        g = Graph("empty")
        assert g.name == "empty"
        assert len(g.nodes) == 0
        assert len(g.edges) == 0

    def test_add_node_with_kwargs(self):
        g = Graph("test")
        fn = lambda x: x + 1
        node = g.add_node(name="add_one", fn=fn, params={"x": 5}, retries=2, timeout=10)
        assert node.name == "add_one"
        assert node.fn is fn
        assert node.params == {"x": 5}
        assert node.retries == 2
        assert node.timeout == 10
        assert node.id in g._nodes

    def test_add_node_with_instance(self):
        g = Graph("test")
        node = Node(name="existing", fn=lambda: 42)
        returned = g.add_node(node=node)
        assert returned is node
        assert node.id in g._nodes

    def test_add_duplicate_node_raises(self):
        g = Graph("test")
        node = Node(name="dup")
        g.add_node(node=node)
        with pytest.raises(ValueError, match="already exists"):
            g.add_node(node=node)

    def test_add_edge(self):
        g = Graph("test")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        edge = g.add_edge(a, b)
        assert edge.source == a.id
        assert edge.target == b.id
        assert len(g.edges) == 1

    def test_add_edge_by_id(self):
        g = Graph("test")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        g.add_edge(a.id, b.id)
        assert len(g.edges) == 1

    def test_add_duplicate_edge_is_noop(self):
        g = Graph("test")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        g.add_edge(a, b)
        g.add_edge(a, b)  # same edge again
        assert len(g.edges) == 1


class TestTopologicalSort:
    def test_linear_dag(self):
        g = Graph("linear")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        c = g.add_node(name="C")
        g.add_edge(a, b)
        g.add_edge(b, c)
        result = g.topological_sort()
        assert result.order == [a.id, b.id, c.id]
        assert result.has_cycle is False
        assert result.levels[a.id] == 0
        assert result.levels[b.id] == 1
        assert result.levels[c.id] == 2

    def test_diamond_dag(self):
        g = Graph("diamond")
        s = g.add_node(name="split")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        m = g.add_node(name="merge")
        g.add_edge(s, a)
        g.add_edge(s, b)
        g.add_edge(a, m)
        g.add_edge(b, m)
        result = g.topological_sort()
        assert result.has_cycle is False
        assert result.order[0] == s.id  # no deps
        # Levels: split=0, A=1, B=1, merge=2
        assert result.levels[s.id] == 0
        assert result.levels[a.id] == 1
        assert result.levels[b.id] == 1
        assert result.levels[m.id] == 2

    def test_cycle_detection(self):
        g = Graph("cycle")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        c = g.add_node(name="C")
        g.add_edge(a, b)
        g.add_edge(b, c)
        g.add_edge(c, a)  # creates cycle
        result = g.topological_sort()
        assert result.has_cycle is True
        assert len(result.cycle_nodes) > 0

    def test_isolated_nodes(self):
        g = Graph("isolated")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        result = g.topological_sort()
        assert result.has_cycle is False
        assert a.id in result.order
        assert b.id in result.order

    def test_execution_plan(self):
        g = Graph("plan")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        c = g.add_node(name="C")
        g.add_edge(a, c)
        g.add_edge(b, c)
        plan = g.execution_plan()
        # A and B are level 0, C is level 1
        assert 0 in plan
        assert 1 in plan
        assert a.id in plan[0]
        assert b.id in plan[0]
        assert c.id in plan[1]


class TestGraphUtilities:
    def test_upstream_of(self):
        g = Graph("upstream")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        c = g.add_node(name="C")
        g.add_edge(a, b)
        g.add_edge(b, c)
        assert g.upstream_of(c.id) == [b.id, a.id] or g.upstream_of(c.id) == [a.id, b.id]
        assert g.upstream_of(a.id) == []

    def test_downstream_of(self):
        g = Graph("downstream")
        a = g.add_node(name="A")
        b = g.add_node(name="B")
        c = g.add_node(name="C")
        g.add_edge(a, b)
        g.add_edge(b, c)
        assert g.downstream_of(a.id) == [b.id, c.id] or g.downstream_of(a.id) == [c.id, b.id]
        assert g.downstream_of(c.id) == []


class TestSerialization:
    def test_roundtrip(self):
        g = Graph("roundtrip")
        g.add_node(name="A", fn=lambda: 1, params={"x": 10})
        g.add_node(name="B", fn=lambda x: x + 1)
        g.add_edge("A", "B")
        data = g.to_dict()
        g2 = Graph.from_dict(data)
        assert g2.name == g.name
        assert g2.id == g.id
        assert len(g2.nodes) == len(g.nodes)
        assert len(g2.edges) == len(g.edges)

    def test_from_dict_restores_status(self):
        g = Graph("status")
        n = g.add_node(name="test")
        n.status = "running"
        data = g.to_dict()
        g2 = Graph.from_dict(data)
        assert g2.get_node(n.id).status == "running"

    def test_to_dict_includes_metadata(self):
        g = Graph("meta", metadata={"env": "prod"})
        d = g.to_dict()
        assert d["metadata"] == {"env": "prod"}
