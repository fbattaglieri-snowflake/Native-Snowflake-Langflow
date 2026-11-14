"""SQL Statement bind values follow the saved edge order, not Langflow's arbitrary set order (2026-11-14)."""
from types import SimpleNamespace

from lfx_snowflake.list_input_order import in_canvas_order


def _raw(source, output, target="sql", field="bind_values"):
    return {"source": source, "target": target,
            "data": {"sourceHandle": {"name": output}, "targetHandle": {"fieldName": field}}}


def _edge(source, output, target="sql", field="bind_values"):
    return SimpleNamespace(source_id=source, target_id=target, target_param=field,
                           source_handle=SimpleNamespace(name=output))


def _component(raw_edges, live_edges):
    graph = SimpleNamespace(_edges=raw_edges)
    return SimpleNamespace(_vertex=SimpleNamespace(id="sql", graph=graph, edges=live_edges))


def test_values_follow_saved_edge_order():
    raw = [_raw("inbox", "message"), _raw("extract", "message"), _raw("decision", "summary")]
    live = [_edge("decision", "summary"), _edge("inbox", "message"), _edge("extract", "message")]
    assert in_canvas_order(_component(raw, live), "bind_values", ["D", "I", "E"]) == ["I", "E", "D"]


def test_other_fields_and_other_targets_are_ignored():
    raw = [_raw("a", "message", field="sql_statement"), _raw("x", "message", target="other"),
           _raw("b", "message"), _raw("a", "message")]
    live = [_edge("a", "message"), _edge("a", "message", field="sql_statement"), _edge("b", "message"),
            _edge("sql", "message", target="next", field="input_value")]
    assert in_canvas_order(_component(raw, live), "bind_values", ["A", "B"]) == ["B", "A"]


def test_two_outputs_of_the_same_node_keep_their_own_position():
    raw = [_raw("n", "summary"), _raw("n", "message")]
    live = [_edge("n", "message"), _edge("n", "summary")]
    assert in_canvas_order(_component(raw, live), "bind_values", ["M", "S"]) == ["S", "M"]


def test_unknown_situations_leave_the_values_untouched():
    raw = [_raw("a", "message"), _raw("b", "message")]
    live = [_edge("b", "message"), _edge("a", "message")]
    assert in_canvas_order(SimpleNamespace(), "bind_values", ["1", "2"]) == ["1", "2"]            # no graph
    assert in_canvas_order(_component(raw, live), "bind_values", ["1", "2", "3"]) == ["1", "2", "3"]  # count differs
    assert in_canvas_order(_component(raw[:1], live), "bind_values", ["1", "2"]) == ["1", "2"]    # edge not saved
    assert in_canvas_order(_component(raw, live), "bind_values", ["1"]) == ["1"]
