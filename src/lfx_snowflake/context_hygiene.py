"""Flow-context hygiene for contexts saved before v0.8.6.

The pre-v0.8.6 context template injected the bridge's own telemetry tables into ``visible_objects``
even when the user never selected them and they did not exist in the selected schema. Those phantom
entries survive a merge (the merge copies ``visible_objects`` verbatim). ``visible_objects`` must only
carry objects that came from real SHOW results / explicit selection, so phantom telemetry entries are
dropped unless the same object is explicitly present in ``selected_objects`` / ``objects_by_db_schema``.
"""

from __future__ import annotations

from typing import Any

PHANTOM_TELEMETRY_TABLES: frozenset[str] = frozenset(
    {"LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"}
)


def _leaf(name: Any) -> str:
    return str(name or "").strip().strip('"').split(".")[-1].strip('"').upper()


def _explicit_names(ctx: dict[str, Any]) -> set[str]:
    """Upper-cased FQNs/leaf names explicitly selected by the user (not template-injected)."""
    out: set[str] = set()

    def walk(v: Any) -> None:
        if isinstance(v, dict):
            for k, sub in v.items():
                if isinstance(k, str) and k.upper().split(".")[-1] in PHANTOM_TELEMETRY_TABLES:
                    out.add(k.strip().upper())
                walk(sub)
        elif isinstance(v, (list, tuple, set)):
            for item in v:
                walk(item)
        elif isinstance(v, str):
            out.add(v.strip().upper())

    walk(ctx.get("selected_objects"))
    walk(ctx.get("objects_by_db_schema"))
    return out


def drop_phantom_telemetry_objects(ctx: Any) -> list[str]:
    """Remove phantom telemetry tables from ``ctx['visible_objects']`` in place; returns what was dropped."""
    if not isinstance(ctx, dict):
        return []
    vis = ctx.get("visible_objects")
    if not isinstance(vis, dict):
        return []
    explicit = _explicit_names(ctx)
    dropped: list[str] = []
    for key in ("tables", "views"):
        items = vis.get(key)
        if not isinstance(items, list):
            continue
        kept = []
        for item in items:
            name = str(item or "").strip()
            if _leaf(name) in PHANTOM_TELEMETRY_TABLES and name.upper() not in explicit:
                dropped.append(name)
                continue
            kept.append(item)
        vis[key] = kept
    return dropped
