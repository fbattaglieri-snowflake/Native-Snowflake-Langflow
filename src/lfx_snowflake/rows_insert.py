"""Write rows into a table by column name.

The write mode of Tables and Iceberg Tables used to run ``INSERT INTO t SELECT PARSE_JSON(?)``: it only fitted
a table with a single VARIANT column, and with nothing to write it inserted a placeholder object. Rows are now
objects keyed by column name; each key becomes a column of one ``INSERT ... SELECT`` over the JSON payload.
"""

from __future__ import annotations

import json
from typing import Any

from lfx_snowflake import sql_safety as _sql_safety

def _unwrap(value: Any) -> Any:
    """Langflow objects and JSON text -> plain Python values."""
    if value is None:
        return None
    if hasattr(value, "to_dict") and hasattr(value, "columns"):  # DataFrame
        return value.to_dict(orient="records")
    if hasattr(value, "data") and isinstance(getattr(value, "data"), (dict, list)):  # Data
        return value.data
    if hasattr(value, "text") and isinstance(getattr(value, "text"), str):  # Message
        value = value.text
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError as exc:
            raise ValueError(f"Rows to write must be JSON (an object or a list of objects): {exc}") from exc
    return value


def normalize_rows(payload: Any) -> list[dict[str, Any]]:
    """Return the rows to insert as a non-empty list of objects, or raise ``ValueError``."""
    rows: list[Any] = []
    for item in payload if isinstance(payload, (list, tuple)) else [payload]:
        item = _unwrap(item)
        if item is None:
            continue
        if isinstance(item, dict):
            # The output of a read node ({"rows": [...], "sql": ...}) carries its rows under "rows".
            nested = item.get("rows")
            is_envelope = isinstance(nested, list) and bool(nested) and all(isinstance(r, dict) for r in nested)
            rows.extend(nested if is_envelope else [item])
        elif isinstance(item, list):
            rows.extend(_unwrap(r) for r in item)
        else:
            rows.append(item)
    if not rows:
        raise ValueError("Nothing to write: connect or type the rows as an object or a list of objects "
                         "keyed by column name.")
    bad = next((r for r in rows if not isinstance(r, dict) or not r), None)
    if bad is not None:
        raise ValueError(f"Each row to write must be a non-empty object keyed by column name; got {str(bad)[:80]!r}.")
    return rows


def build_insert(table_fqn: str, rows: list[dict[str, Any]]) -> tuple[str, dict[str, dict[str, Any]]]:
    """One bound statement inserting ``rows`` into ``table_fqn`` (already validated/quoted by the caller)."""
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    by_upper: dict[str, str] = {}
    for col in columns:
        _sql_safety.validate_identifier(col, "column name")
        if by_upper.setdefault(col.upper(), col) != col:
            raise ValueError(f"Rows use both {by_upper[col.upper()]!r} and {col!r} for the same column.")
    select = ", ".join(f"f.value:{json.dumps(col)}" for col in columns)
    sql = (f"INSERT INTO {table_fqn} ({', '.join(columns)}) SELECT {select} "
           "FROM TABLE(FLATTEN(input => PARSE_JSON(?))) f")
    return sql, _sql_safety.to_sql_bindings([("TEXT", json.dumps(rows, default=str))])
