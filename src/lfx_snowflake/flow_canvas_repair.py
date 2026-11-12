"""Keep stored flow edges valid for the Langflow 1.13 canvas.

On load the canvas re-validates every edge against the nodes (``cleanEdges``) and silently drops the
ones it considers invalid ("Some connections were removed because they were invalid"). Flows built
through the API (rather than wired by hand in the canvas) miss pieces of state the canvas relies on:

* ``selected_output`` on nodes with several non-grouped outputs. Without it the canvas auto-selects the
  FIRST output on mount, clears ``selected`` on the others and drops edges wired from them. Langflow's
  own flow builder fixes this with ``lfx.graph.flow_builder.component.sync_dropdown_selected_outputs``;
  this module applies the same rule, and also pins nodes mixing grouped and non-grouped outputs whose
  edges only use the grouped ones (e.g. the Decision routes).
* ``show`` on wired input fields: a wired field left hidden (a mode-dependent field stored before the
  component's ``update_build_config`` ran) is dropped by the canvas.
* ``type`` / ``input_types`` on wired input fields: when the stored field is older than the installed
  component but the edge already matches the installed one, the stored field is aligned to the catalog.
* a stored static ``component_as_tool`` output on a node that is not in Tool Mode: the canvas would
  treat the node as a tool and hide its ``tool_mode`` inputs (see ``drop_static_tool_outputs``).

All functions mutate ``flow_data`` in place, are idempotent and return a list of change descriptions.
"""

from __future__ import annotations

import json
from typing import Any

HANDLE_QUOTE = "\u0153"
TOOL_OUTPUT_NAME = "component_as_tool"


def parse_handle(handle: Any) -> dict[str, Any]:
    if isinstance(handle, dict):
        return handle
    if not isinstance(handle, str) or "{" not in handle:
        return {}
    try:
        parsed = json.loads(handle.replace(HANDLE_QUOTE, '"'))
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _node_id(node: dict[str, Any]) -> str:
    return str((node.get("data") or {}).get("id") or node.get("id") or "")


def _wired_outputs(flow_data: dict[str, Any]) -> dict[str, set[str]]:
    used: dict[str, set[str]] = {}
    for edge in flow_data.get("edges") or []:
        name = parse_handle((edge.get("data") or {}).get("sourceHandle") or edge.get("sourceHandle")).get("name")
        if edge.get("source") and name:
            used.setdefault(str(edge["source"]), set()).add(str(name))
    return used


def sync_selected_outputs(flow_data: dict[str, Any]) -> list[str]:
    """Pin ``selected_output`` so the canvas does not auto-select another output and drop edges."""
    changes: list[str] = []
    used = _wired_outputs(flow_data)
    for node in flow_data.get("nodes") or []:
        data = node.get("data") or {}
        outputs = (data.get("node") or {}).get("outputs")
        wired = used.get(_node_id(node))
        if not wired or not isinstance(outputs, list) or data.get("selected_output"):
            continue
        non_group = [o for o in outputs if isinstance(o, dict) and not o.get("group_outputs")]
        if len(non_group) <= 1:
            continue
        wired_non_group = [o for o in non_group if o.get("name") in wired]
        if len(wired_non_group) > 1:
            # A single-active dropdown cannot show two wired outputs; leave the node untouched.
            continue
        if wired_non_group:
            target = wired_non_group[0]
        elif all(o.get("name") not in wired for o in non_group) and any(o.get("group_outputs") for o in outputs):
            # Only grouped outputs are wired: pin the default non-grouped output so the canvas keeps
            # the grouped outputs' ``selected`` flags (and their edges) untouched.
            target = next((o for o in non_group if o.get("selected")), non_group[0])
        else:
            continue
        if not target.get("selected"):
            types = target.get("types") or []
            target["selected"] = types[0] if types else None
        data["selected_output"] = target.get("name")
        changes.append(f"{_node_id(node)}: selected_output={target.get('name')}")
    return changes


def _catalog_field(catalog: dict[str, Any] | None, node_type: str, field_name: str) -> dict[str, Any] | None:
    if not catalog:
        return None
    field = ((catalog.get(node_type) or {}).get("template") or {}).get(field_name)
    return field if isinstance(field, dict) else None


def sync_wired_input_fields(flow_data: dict[str, Any], catalog: dict[str, Any] | None = None) -> list[str]:
    """Show wired input fields and align their stored handle metadata with the installed component."""
    changes: list[str] = []
    nodes = {_node_id(n): n for n in flow_data.get("nodes") or []}
    for edge in flow_data.get("edges") or []:
        target = nodes.get(str(edge.get("target") or ""))
        handle = parse_handle((edge.get("data") or {}).get("targetHandle") or edge.get("targetHandle"))
        field_name = handle.get("fieldName")
        if not target or not field_name:
            continue
        data = target.get("data") or {}
        field = ((data.get("node") or {}).get("template") or {}).get(field_name)
        if not isinstance(field, dict):
            continue
        tid = _node_id(target)
        if field.get("show") is False:
            field["show"] = True
            changes.append(f"{tid}.{field_name}: show=True")
        current = _catalog_field(catalog, str(data.get("type") or ""), field_name)
        if not current:
            continue
        # The canvas accepts an edge whose handle ``type`` matches the field and whose types overlap, then
        # rewrites the handle itself; so align the stored field only when the edge already agrees with
        # the installed component on ``type`` and shares at least one input type with it.
        current_types = set(current.get("input_types") or [])
        edge_matches_catalog = handle.get("type") == current.get("type") and (
            not current_types or bool(current_types & set(handle.get("inputTypes") or []))
        )
        for key in ("type", "input_types"):
            if edge_matches_catalog and field.get(key) != current.get(key):
                field[key] = current.get(key)
                changes.append(f"{tid}.{field_name}: {key}={current.get(key)}")
    return changes


def drop_static_tool_outputs(flow_data: dict[str, Any], catalog: dict[str, Any] | None = None) -> list[str]:
    """Remove a stored ``component_as_tool`` output the installed component no longer declares.

    The canvas treats any node whose outputs contain ``component_as_tool`` as being in Tool Mode and hides
    its ``tool_mode`` inputs (and their handles, so edges into them are not drawn). Tool Mode is a
    per-node toggle that adds that output itself, so a stored copy is dropped only when the node is not in
    Tool Mode, the installed component does not declare it and no edge uses it.
    """
    if not catalog:
        return []
    changes: list[str] = []
    used = _wired_outputs(flow_data)
    for node in flow_data.get("nodes") or []:
        data = node.get("data") or {}
        node_cfg = data.get("node") or {}
        outputs = node_cfg.get("outputs")
        current = catalog.get(str(data.get("type") or ""))
        if not isinstance(outputs, list) or not isinstance(current, dict) or node_cfg.get("tool_mode"):
            continue
        if any(o.get("name") == TOOL_OUTPUT_NAME for o in current.get("outputs") or []):
            continue
        if TOOL_OUTPUT_NAME in used.get(_node_id(node), set()):
            continue
        kept = [o for o in outputs if not (isinstance(o, dict) and o.get("name") == TOOL_OUTPUT_NAME)]
        if len(kept) != len(outputs):
            node_cfg["outputs"] = kept
            changes.append(f"{_node_id(node)}: removed static {TOOL_OUTPUT_NAME} output")
    return changes


def repair_flow_canvas(flow_data: dict[str, Any], catalog: dict[str, Any] | None = None) -> list[str]:
    """Apply every canvas repair to a stored flow's ``data``; returns the change descriptions."""
    if not isinstance(flow_data, dict):
        return []
    return (
        drop_static_tool_outputs(flow_data, catalog)
        + sync_wired_input_fields(flow_data, catalog)
        + sync_selected_outputs(flow_data)
    )


def flatten_catalog(all_components: dict[str, Any]) -> dict[str, Any]:
    """Turn the ``GET /api/v1/all`` payload ({category: {type: template}}) into {type: template}."""
    flat: dict[str, Any] = {}
    for components in (all_components or {}).values():
        if isinstance(components, dict):
            for name, template in components.items():
                flat.setdefault(name, template)
    return flat
