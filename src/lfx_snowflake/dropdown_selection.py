"""One rule for Snowflake dropdowns: a stored selection is never replaced by the first option behind the user's back.

A value that is not in the refreshed list (the role or the flow Context changed) stays selected and visible, so the
run fails with a clear authorization error instead of silently using another object.
"""

from __future__ import annotations

from typing import Any

PARENT_FIELDS = frozenset({
    "database", "schema", "schema_name", "database_schema", "stage_name", "postgres_instance", "resource_type",
    "table_type", "view_type",
})


def is_listed(current: Any, options: list[Any]) -> bool:
    """True when ``current`` is one of ``options``; a fully qualified name matches its short name."""
    cur = str(current or "").strip()
    if not cur:
        return False
    if current in options:
        return True
    short = cur.split(".")[-1].upper()
    return any(str(o).split(".")[-1].upper() == short for o in options)


def keep_or_default(build_config: dict[str, Any], key: str, options: list[Any], field_name: str | None = None) -> None:
    """Default ``key`` only when it is empty or the user just changed a parent field; otherwise keep it."""
    field = build_config.get(key)
    if not isinstance(field, dict):
        return
    current = field.get("value")
    opts = list(options or [])
    if is_listed(current, opts):
        return
    empty = not str(current or "").strip()
    parent_changed = field_name in PARENT_FIELDS and field_name != key
    if opts and (empty or parent_changed):
        field["value"] = opts[0]
    elif not empty:
        field["options"] = [*opts, current]
