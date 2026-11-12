"""Tables / Views / Iceberg Tables / Postgres in Tool Mode return real structured tools an Agent can bind."""

from __future__ import annotations

import pytest

from lfx_snowflake.native_category_components.data_source import (
    snowflake_iceberg_tables,
    snowflake_postgres,
    snowflake_tables,
    snowflake_views,
)
from lfx_snowflake.providers.language_models import StructuredTool

CASES = [
    (snowflake_tables, "SnowflakeTables", {"table_name": "SALES_ORDERS"}, "snowflake_table_sales_orders"),
    (snowflake_views, "SnowflakeViews", {"view_name": "SALES_V"}, "snowflake_view_sales_v"),
    (snowflake_iceberg_tables, "SnowflakeIcebergTables", {"table_name": "CATALOG_ICE"}, "snowflake_iceberg_catalog_ice"),
    (snowflake_postgres, "SnowflakePostgres", {"table_name": "events"}, "snowflake_postgres_events"),
]


def _component(module, marker: str):
    cls = next(v for v in vars(module).values() if isinstance(v, type) and getattr(v, "name", None) == marker)
    return cls()


@pytest.mark.parametrize(("module", "marker", "values", "tool_name"), CASES)
def test_tool_mode_returns_a_structured_tool_with_a_filter_argument(module, marker, values, tool_name) -> None:
    comp = _component(module, marker)
    for k, v in values.items():
        setattr(comp, k, v)
    (tool,) = comp.to_toolkit()
    assert isinstance(tool, StructuredTool)
    assert tool.name == tool_name
    assert list(tool.args_schema.model_fields) == ["tool_input"]


def test_tables_tool_passes_the_filter_as_bound_values(monkeypatch) -> None:
    seen: list[tuple] = []

    def fake_exec(stmt, *a, **k):
        seen.append((stmt, k.get("bindings")))
        return {"ok": True, "rows": [{"ORDER_ID": 1005}]}

    monkeypatch.setattr(snowflake_tables, "_flow_component_execute_sql", fake_exec)
    comp = _component(snowflake_tables, "SnowflakeTables")
    comp.database, comp.schema_name, comp.table_name, comp.max_rows = "DB", "SCH", "SALES_ORDERS", 20
    (tool,) = comp.to_toolkit()
    tool.invoke({"tool_input": "STATUS = 'RETURNED'"})
    stmt, bindings = seen[-1]
    assert "WHERE" in stmt and "RETURNED" not in stmt and "?" in stmt
    assert "RETURNED" in str(bindings)
