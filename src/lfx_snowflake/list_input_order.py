"""Values of a list input in the order their connections were drawn on the canvas.

Langflow keeps the edges of a running graph in a ``set``, so the values of an input that accepts several
connections arrive in an arbitrary order that can change from one run to the next. Positional uses (the ``?``
placeholders of SQL Statement) need a stable order: the one of the saved flow, which is the order the user
connected the nodes in.
"""

from __future__ import annotations

from typing import Any


def _handle_name(handle: Any) -> str:
    if isinstance(handle, dict):
        return str(handle.get("name") or "")
    return str(getattr(handle, "name", "") or "")


def _saved_rank(raw_edges: list[Any], target_id: str, field: str) -> dict[tuple[str, str], int]:
    rank: dict[tuple[str, str], int] = {}
    for edge in raw_edges:
        if not isinstance(edge, dict) or edge.get("target") != target_id:
            continue
        data = edge.get("data") or {}
        if (data.get("targetHandle") or {}).get("fieldName") != field:
            continue
        rank.setdefault((str(edge.get("source")), _handle_name(data.get("sourceHandle"))), len(rank))
    return rank


def in_canvas_order(component: Any, field: str, values: list[Any]) -> list[Any]:
    """Return ``values`` sorted by the saved edge order; unchanged when the order cannot be established."""
    if len(values) < 2:
        return values
    vertex = getattr(component, "_vertex", None)
    graph = getattr(vertex, "graph", None)
    raw_edges = getattr(graph, "_edges", None)
    if vertex is None or not isinstance(raw_edges, list):
        return values
    try:
        # Same iteration Langflow used to fill the list: one source per value, in arrival order.
        arrival = [(str(e.source_id), _handle_name(getattr(e, "source_handle", None)))
                   for e in vertex.edges
                   if e.target_id == vertex.id and getattr(e, "target_param", None) == field]
    except Exception:  # noqa: BLE001 - never fail a run because of ordering
        return values
    rank = _saved_rank(raw_edges, str(vertex.id), field)
    if len(arrival) != len(values) or any(key not in rank for key in arrival):
        return values
    order = sorted(range(len(values)), key=lambda i: rank[arrival[i]])
    return [values[i] for i in order]
