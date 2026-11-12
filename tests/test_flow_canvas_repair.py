"""Stored-flow repairs that keep the Langflow 1.13 canvas from dropping edges on load."""

from __future__ import annotations

import copy
import json

from lfx_snowflake.flow_canvas_repair import (
    drop_static_tool_outputs,
    flatten_catalog,
    repair_flow_canvas,
    sync_selected_outputs,
    sync_wired_input_fields,
)


def _handle(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":")).replace('"', "\u0153")


def _out(name: str, types: list[str], *, selected: str | None = None, group: bool = False) -> dict:
    return {"name": name, "types": types, "selected": selected, "group_outputs": group, "method": f"build_{name}"}


def _node(node_id: str, node_type: str, outputs: list[dict], template: dict | None = None, **node_extra) -> dict:
    return {
        "id": node_id,
        "type": "genericNode",
        "data": {"id": node_id, "type": node_type, "node": {"outputs": outputs, "template": template or {}, **node_extra}},
    }


def _edge(src: dict, out: str, out_types: list[str], tgt: dict, field: str, field_type: str, in_types: list[str]) -> dict:
    sh = {"dataType": src["data"]["type"], "id": src["id"], "name": out, "output_types": out_types}
    th = {"fieldName": field, "id": tgt["id"], "inputTypes": in_types, "type": field_type}
    return {
        "id": f"e-{src['id']}-{tgt['id']}-{out}",
        "source": src["id"],
        "target": tgt["id"],
        "sourceHandle": _handle(sh),
        "targetHandle": _handle(th),
        "data": {"sourceHandle": sh, "targetHandle": th},
    }


CHAT_INPUT_TYPES = ["Data", "JSON", "DataFrame", "Table", "Message"]


def _chat_output(node_id: str = "ChatOutput-1", field_type: str = "other") -> dict:
    return _node(
        node_id,
        "ChatOutput",
        [_out("message", ["Message"], selected="Message")],
        {"input_value": {"type": field_type, "input_types": CHAT_INPUT_TYPES, "show": True, "advanced": False}},
    )


def test_trigger_non_first_output_is_pinned():
    trigger = _node(
        "SnowflakeTableChangeTrigger-1",
        "SnowflakeTableChangeTrigger",
        [
            _out("changed_rows", ["Table"], selected="Table"),
            _out("trigger_message_out", ["Message"], selected="Message"),
            _out("stream_metadata", ["JSON"], selected="JSON"),
        ],
    )
    out = _chat_output()
    flow = {"nodes": [trigger, out], "edges": [_edge(trigger, "trigger_message_out", ["Message"], out, "input_value", "other", CHAT_INPUT_TYPES)]}
    assert sync_selected_outputs(flow) == ["SnowflakeTableChangeTrigger-1: selected_output=trigger_message_out"]
    assert trigger["data"]["selected_output"] == "trigger_message_out"
    assert sync_selected_outputs(flow) == []


def test_single_output_and_already_pinned_nodes_are_untouched():
    out = _chat_output()
    chat_in = _node("ChatInput-1", "ChatInput", [_out("message", ["Message"], selected="Message")])
    flow = {"nodes": [chat_in, out], "edges": [_edge(chat_in, "message", ["Message"], out, "input_value", "other", CHAT_INPUT_TYPES)]}
    assert sync_selected_outputs(flow) == []
    assert "selected_output" not in chat_in["data"]


def test_two_wired_dropdown_outputs_are_left_alone():
    node = _node("SQL-1", "SnowflakeSQLStatement", [_out("dataframe", ["Table"], selected="Table"), _out("message", ["Message"])])
    a, b = _chat_output("ChatOutput-a"), _chat_output("ChatOutput-b")
    flow = {
        "nodes": [node, a, b],
        "edges": [
            _edge(node, "dataframe", ["Table"], a, "input_value", "other", CHAT_INPUT_TYPES),
            _edge(node, "message", ["Message"], b, "input_value", "other", CHAT_INPUT_TYPES),
        ],
    }
    assert sync_selected_outputs(flow) == []


def test_decision_grouped_routes_pin_default_output():
    decision = _node(
        "SnowflakeDecision-1",
        "SnowflakeDecision",
        [
            _out("decision_object", ["JSON"], selected="JSON"),
            _out("high_confidence_route", ["JSON"], selected="JSON", group=True),
            _out("low_confidence_route", ["JSON"], selected="JSON", group=True),
            _out("table_decisions", ["Table"], selected="Table"),
        ],
    )
    conv = _node("TypeConverter-1", "TypeConverterComponent", [_out("message_output", ["Message"])])
    flow = {"nodes": [decision, conv], "edges": [_edge(decision, "high_confidence_route", ["JSON"], conv, "input_data", "other", ["JSON"])]}
    assert sync_selected_outputs(flow) == ["SnowflakeDecision-1: selected_output=decision_object"]
    assert decision["data"]["node"]["outputs"][1]["selected"] == "JSON"


def test_wired_hidden_field_is_shown():
    history = _node(
        "MessageHistory-1",
        "MessageHistory",
        [_out("messages", ["Message"]), _out("stored_message", ["Message"])],
        {"message_text": {"type": "str", "input_types": ["Message"], "show": False}},
    )
    chat_in = _node("ChatInput-1", "ChatInput", [_out("message", ["Message"], selected="Message")])
    flow = {"nodes": [chat_in, history], "edges": [_edge(chat_in, "message", ["Message"], history, "message_text", "str", ["Message"])]}
    assert sync_wired_input_fields(flow) == ["MessageHistory-1.message_text: show=True"]
    assert history["data"]["node"]["template"]["message_text"]["show"] is True


def test_stale_field_type_aligned_only_when_edge_matches_catalog():
    agent = _node("Agent-1", "Agent", [_out("response", ["Message"], selected="Message")])
    stale = _chat_output(field_type="str")
    flow = {"nodes": [agent, stale], "edges": [_edge(agent, "response", ["Message"], stale, "input_value", "other", ["Data", "Message"])]}
    catalog = {"ChatOutput": {"template": {"input_value": {"type": "other", "input_types": CHAT_INPUT_TYPES}}}}
    assert sync_wired_input_fields(flow, catalog) == ["ChatOutput-1.input_value: type=other"]
    assert stale["data"]["node"]["template"]["input_value"]["type"] == "other"

    mismatched = _chat_output("ChatOutput-2", field_type="str")
    flow2 = {"nodes": [agent, mismatched], "edges": [_edge(agent, "response", ["Message"], mismatched, "input_value", "str", ["Message"])]}
    assert sync_wired_input_fields(flow2, catalog) == []


def test_static_tool_output_dropped_only_when_safe():
    outputs = [_out("message", ["Message"], selected="Message"), _out("metadata", ["JSON"]), _out("component_as_tool", ["Tool"])]
    catalog = {"SnowflakeCortexSearch": {"outputs": [{"name": "message"}, {"name": "metadata"}]}}

    plain = _node("Search-1", "SnowflakeCortexSearch", copy.deepcopy(outputs))
    assert drop_static_tool_outputs({"nodes": [plain], "edges": []}, catalog) == ["Search-1: removed static component_as_tool output"]
    assert [o["name"] for o in plain["data"]["node"]["outputs"]] == ["message", "metadata"]

    tool_mode = _node("Search-2", "SnowflakeCortexSearch", copy.deepcopy(outputs), tool_mode=True)
    assert drop_static_tool_outputs({"nodes": [tool_mode], "edges": []}, catalog) == []

    wired = _node("Search-3", "SnowflakeCortexSearch", copy.deepcopy(outputs))
    agent = _node("Agent-1", "Agent", [_out("response", ["Message"])], {"tools": {"type": "other", "input_types": ["Tool"]}})
    flow = {"nodes": [wired, agent], "edges": [_edge(wired, "component_as_tool", ["Tool"], agent, "tools", "other", ["Tool"])]}
    assert drop_static_tool_outputs(flow, catalog) == []

    still_declared = {"SnowflakeCortexSearch": {"outputs": [{"name": "message"}, {"name": "component_as_tool"}]}}
    other = _node("Search-4", "SnowflakeCortexSearch", copy.deepcopy(outputs))
    assert drop_static_tool_outputs({"nodes": [other], "edges": []}, still_declared) == []


def test_repair_is_idempotent_and_never_touches_edges():
    trigger = _node(
        "SnowflakeScheduleTrigger-1",
        "SnowflakeScheduleTrigger",
        [_out("trigger_message_out", ["Message"], selected="Message"), _out("execution_metadata", ["JSON"], selected="JSON")],
    )
    out = _chat_output()
    flow = {"nodes": [trigger, out], "edges": [_edge(trigger, "trigger_message_out", ["Message"], out, "input_value", "other", CHAT_INPUT_TYPES)]}
    edges_before = copy.deepcopy(flow["edges"])
    assert repair_flow_canvas(flow, {}) != []
    assert repair_flow_canvas(flow, {}) == []
    assert flow["edges"] == edges_before
    assert repair_flow_canvas(None) == []  # type: ignore[arg-type]


def test_flatten_catalog_keeps_first_definition():
    flat = flatten_catalog({"a": {"X": {"v": 1}}, "b": {"X": {"v": 2}, "Y": {"v": 3}}, "names": []})
    assert flat == {"X": {"v": 1}, "Y": {"v": 3}}
