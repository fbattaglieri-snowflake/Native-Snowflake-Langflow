from __future__ import annotations

from typing import Any

from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MultilineInput, Output
from lfx.schema import Data, DataFrame
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _context_databases() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _context_schemas(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas_by_db = ctx.get("schemas_by_db") or {}
    schemas = list(schemas_by_db.get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC", "INFORMATION_SCHEMA"]


def _context_objects() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    objs: list[str] = []
    for key in ("tables", "views"):
        for item in vis.get(key) or []:
            if item not in objs:
                objs.append(item)
    for fallback in ("LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"):
        if fallback not in objs:
            objs.append(fallback)
    return objs


class SnowflakeDataSourceComponent(Component):
    display_name = "Snowflake"
    description = (
        "Governed Snowflake Data Source (Standard Table, View, Secure View, Materialized View, "
        "Hybrid Table, Dynamic Table, Interactive Table, Iceberg Table, Snowflake Postgres Table) "
        "with hierarchical Engine -> Database -> Schema -> Object Type -> Table/View selection."
    )
    icon = "Snowflake"
    name = "SnowflakeDataSource"

    inputs = [
        DropdownInput(
            name="engine",
            display_name="Engine",
            options=["Snowflake SQL", "Snowflake Postgres"],
            value="Snowflake SQL",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=_context_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_context_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="object_type",
            display_name="Object Type",
            options=[
                "Standard Table",
                "View",
                "Secure View",
                "Materialized View",
                "Hybrid Table",
                "Dynamic Table",
                "Interactive Table",
                "Iceberg Table",
                "Snowflake Postgres Table",
            ],
            value="Standard Table",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="object_name",
            display_name="Table / View Name",
            options=_context_objects(),
            value="LANGFLOW_FLOW_RUNS",
            real_time_refresh=True,
            combobox=True,
        ),
        MultilineInput(
            name="sql_filter",
            display_name="Optional SQL Query or WHERE Filter",
            value="",
            advanced=True,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows",
            value=100,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="DataFrame", name="dataframe", method="read_dataframe"),
        Output(display_name="Metadata", name="metadata", method="read_metadata"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        dbs = _context_databases()
        if "database" in build_config:
            build_config["database"]["options"] = dbs
        selected_db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _context_schemas(selected_db)
        if "object_name" in build_config:
            build_config["object_name"]["options"] = _context_objects()
        return build_config

    def _build_query(self) -> str:
        custom = (self.sql_filter or "").strip()
        if custom.upper().startswith(("SELECT ", "SHOW ")):
            return custom
        fqn = f"{self.database.strip()}.{self.schema_name.strip()}.{self.object_name.strip()}"
        where = f" WHERE {custom}" if custom else ""
        return f"SELECT * FROM {fqn}{where} LIMIT {max(1, int(self.max_rows or 100))}"

    def read_dataframe(self) -> DataFrame:
        res = LIVE_BRIDGE.execute_sql(self._build_query())
        if not res.get("ok"):
            raise ValueError(res.get("message", "Snowflake query failed"))
        return DataFrame(res.get("rows", []))

    def read_metadata(self) -> Data:
        res = LIVE_BRIDGE.execute_sql(self._build_query())
        return Data(
            data={
                "engine": self.engine,
                "database": self.database,
                "schema_name": self.schema_name,
                "object_type": self.object_type,
                "object_name": self.object_name,
                "qualified_name": f"{self.database}.{self.schema_name}.{self.object_name}",
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "row_count": len(res.get("rows", [])),
                "ok": res.get("ok", False),
            }
        )
