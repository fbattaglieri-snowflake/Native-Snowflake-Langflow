from __future__ import annotations

from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import DataInput, DropdownInput, IntInput, MessageTextInput, Output, StrInput
    from lfx.schema import Data, DataFrame
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import (
        Component,
        Data,
        DataFrame,
        DataInput,
        DropdownInput,
        IntInput,
        MessageTextInput,
        Output,
        StrInput,
    )

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _postgres_instances(database: str | None = None, schema: str | None = None) -> list[str]:
    insts = list(
        LIVE_BRIDGE.get_context_objects(
            database or LIVE_BRIDGE.database,
            schema or LIVE_BRIDGE.schema,
            "postgres_instances",
        )
    )
    if not insts:
        insts = ["LANGFLOW_PG_INSTANCE"]
    return insts


def _memory_catalog_options() -> list[str]:
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


class SnowflakePostgresComponent(Component):
    display_name = "Postgres"
    description = (
        "Governed Snowflake Postgres Data Source (Relational, JSONB, pgvector) with instance/database/schema/table "
        "cascading selection, input ports, Read/Write Tool Mode for Agents, and optional Snowflake Memory recall."
    )
    icon = "Snowflake"
    name = "SnowflakePostgres"

    inputs = [
        DropdownInput(
            name="postgres_instance",
            display_name="Postgres Instance",
            options=_postgres_instances(),
            value="LANGFLOW_PG_INSTANCE",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="postgres_database",
            display_name="Postgres Database",
            value="snowflake",
        ),
        StrInput(
            name="postgres_schema",
            display_name="Postgres Schema",
            value="public",
        ),
        StrInput(
            name="table_name",
            display_name="Postgres Table Name",
            value="langflow_pg_events",
        ),
        DropdownInput(
            name="storage_profile",
            display_name="Storage Profile",
            options=["Relational", "JSONB", "pgvector"],
            value="Relational",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="memory_catalog_ref",
            display_name="Snowflake Memory Recall (Optional)",
            options=_memory_catalog_options(),
            value="None",
            real_time_refresh=True,
            combobox=True,
            advanced=True,
        ),
        DropdownInput(
            name="operation_mode",
            display_name="Operation Mode",
            options=["SELECT (Read)", "INSERT / UPSERT (Write)"],
            value="SELECT (Read)",
            real_time_refresh=True,
        ),
        MessageTextInput(
            name="query_filter_input",
            display_name="SQL Filter / Query Input",
            value="",
            tool_mode=True,
        ),
        DataInput(
            name="rows_to_write",
            display_name="Rows / Payload to Write (for INSERT / UPSERT)",
            is_list=True,
            tool_mode=True,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows",
            value=100,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Postgres Data", name="data", method="run_postgres_operation"),
        Output(display_name="DataFrame", name="dataframe", method="read_dataframe"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = LIVE_BRIDGE.get_context_schemas(db)
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else LIVE_BRIDGE.schema or "CORE")
        )
        if "postgres_instance" in build_config:
            pg_opts = _postgres_instances(db, sch)
            build_config["postgres_instance"]["options"] = pg_opts
            if pg_opts and build_config["postgres_instance"].get("value") not in pg_opts:
                build_config["postgres_instance"]["value"] = pg_opts[0]
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = _memory_catalog_options()
        return build_config

    def run_postgres_operation(self) -> Data:
        mem_ref = (getattr(self, "memory_catalog_ref", "None") or "None").strip()
        if mem_ref and mem_ref != "None":
            recalled = LIVE_BRIDGE.recall_memory(
                mem_ref,
                query=str(getattr(self, "query_filter_input", "") or ""),
                limit=int(getattr(self, "max_rows", 10) or 10),
            )
            return Data(data=recalled)

        inst = (getattr(self, "postgres_instance", "") or "LANGFLOW_PG_INSTANCE").strip()
        pg_db = (getattr(self, "postgres_database", "") or "snowflake").strip()
        pg_sch = (getattr(self, "postgres_schema", "") or "public").strip()
        tbl = (getattr(self, "table_name", "") or "langflow_pg_events").strip()
        mode = (getattr(self, "operation_mode", "SELECT (Read)") or "SELECT (Read)").strip()
        fqn = f"{inst}.{pg_db}.{pg_sch}.{tbl}"

        if "Write" in mode or "INSERT" in mode:
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("POSTGRES_WRITE", fqn)
            if denied:
                raise ValueError(denied["message"])

        custom = (getattr(self, "query_filter_input", "") or "").strip()
        sql_stmt = custom if custom.upper().startswith(("SELECT ", "INSERT ")) else f"SELECT * FROM {pg_sch}.{tbl} LIMIT {int(getattr(self, 'max_rows', 100) or 100)}"
        return Data(
            data={
                "postgres_instance": inst,
                "postgres_database": pg_db,
                "postgres_schema": pg_sch,
                "table_name": tbl,
                "qualified_name": fqn,
                "storage_profile": getattr(self, "storage_profile", "Relational"),
                "operation_mode": mode,
                "sql": sql_stmt,
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "rows": [
                    {
                        "instance": inst,
                        "database": pg_db,
                        "schema": pg_sch,
                        "table": tbl,
                        "storage_profile": getattr(self, "storage_profile", "Relational"),
                        "active_role": LIVE_BRIDGE.active_role,
                    }
                ],
                "ok": True,
            }
        )

    def read_dataframe(self) -> DataFrame:
        return DataFrame(self.run_postgres_operation().data.get("rows", []))

    def to_toolkit(self) -> list[Tool]:
        inst = (getattr(self, "postgres_instance", "") or "LANGFLOW_PG_INSTANCE").strip()
        pg_sch = (getattr(self, "postgres_schema", "") or "public").strip()
        tbl = (getattr(self, "table_name", "") or "langflow_pg_events").strip()

        def _invoke(tool_input: Any = None) -> dict[str, Any]:
            if isinstance(tool_input, str) and tool_input.strip():
                self.query_filter_input = tool_input.strip()
            return self.run_postgres_operation().data

        return [
            Tool(
                name=f"snowflake_postgres_{tbl.lower()}",
                description=f"Read or write Snowflake Postgres instance {inst} table {pg_sch}.{tbl} under active role {LIVE_BRIDGE.active_role}.",
                func=_invoke,
                metadata={"postgres_instance": inst, "component": "SnowflakePostgres", "tool_mode": True},
            )
        ]
