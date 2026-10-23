from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MessageTextInput, Output
from lfx.schema import Data, DataFrame

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _view_databases() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _view_schemas(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


def _view_objects(database: str | None = None, schema: str | None = None) -> list[str]:
    objs = list(LIVE_BRIDGE.get_context_objects(database, schema, "views"))
    if not objs:
        objs = ["FLOW_ACTIVITY_VIEW"]
    return objs


def _memory_catalog_options() -> list[str]:
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


class SnowflakeViewsComponent(Component):
    display_name = "Views"
    description = (
        "Governed Snowflake Views Data Source (Standard View, Secure View, Materialized View) "
        "with cascading Database -> Schema -> View selection, input port, Read Tool Mode for Agents, "
        "and optional Snowflake Memory catalog recall."
    )
    icon = "Snowflake"
    name = "SnowflakeViews"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_view_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_view_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="view_subtype",
            display_name="View Type",
            options=["Standard View", "Secure View", "Materialized View"],
            value="Standard View",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="view_name",
            display_name="View Name",
            options=_view_objects("SNOWFLAKE_LANGFLOW", "CORE"),
            value="FLOW_ACTIVITY_VIEW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="memory_catalog_ref",
            display_name="Snowflake Memory Recall (Optional)",
            options=_memory_catalog_options(),
            value="None",
            real_time_refresh=True,
            combobox=True,
        ),
        MessageTextInput(
            name="query_filter_input",
            display_name="SQL Filter / Query Input",
            value="",
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
        Output(display_name="View Data", name="data", method="run_view_query"),
        Output(display_name="DataFrame", name="dataframe", method="read_dataframe"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _view_databases()
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _view_schemas(db)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            cur_sch = build_config["schema_name"].get("value")
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and cur_sch not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        if "view_name" in build_config:
            vw_opts = _view_objects(db, sch)
            build_config["view_name"]["options"] = vw_opts
            if vw_opts and build_config["view_name"].get("value") not in vw_opts:
                build_config["view_name"]["value"] = vw_opts[0]
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = _memory_catalog_options()
        return build_config

    def _build_query(self) -> str:
        db = (getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "") or "CORE").strip()
        vw = (getattr(self, "view_name", "") or "FLOW_ACTIVITY_VIEW").strip()
        fqn = vw if "." in vw else f"{db}.{sch}.{vw}"
        custom = (getattr(self, "query_filter_input", "") or "").strip()
        if custom.upper().startswith(("SELECT ", "SHOW ")):
            return custom
        where = f" WHERE {custom}" if custom else ""
        return f"SELECT * FROM {fqn}{where} LIMIT {max(1, int(getattr(self, 'max_rows', 100) or 100))}"

    def run_view_query(self) -> Data:
        mem_ref = (getattr(self, "memory_catalog_ref", "None") or "None").strip()
        if mem_ref and mem_ref != "None":
            recalled = LIVE_BRIDGE.recall_memory(
                mem_ref,
                query=str(getattr(self, "query_filter_input", "") or ""),
                limit=int(getattr(self, "max_rows", 10) or 10),
            )
            return Data(data=recalled)

        db = (getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "") or "CORE").strip()
        vw = (getattr(self, "view_name", "") or "FLOW_ACTIVITY_VIEW").strip()
        fqn = vw if "." in vw else f"{db}.{sch}.{vw}"
        stmt = self._build_query()
        res = LIVE_BRIDGE.execute_sql(stmt)
        return Data(
            data={
                "view_subtype": getattr(self, "view_subtype", "Standard View"),
                "database": db,
                "schema_name": sch,
                "view_name": vw,
                "qualified_name": fqn,
                "sql": stmt,
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "rows": res.get("rows", []),
                "row_count": len(res.get("rows", [])),
                "ok": res.get("ok", False),
            }
        )

    def read_dataframe(self) -> DataFrame:
        res = LIVE_BRIDGE.execute_sql(self._build_query())
        return DataFrame(res.get("rows", []))

    def to_toolkit(self) -> list[Tool]:
        db = (getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "") or "CORE").strip()
        vw = (getattr(self, "view_name", "") or "FLOW_ACTIVITY_VIEW").strip()
        fqn = vw if "." in vw else f"{db}.{sch}.{vw}"

        def _invoke(tool_input: Any = None) -> dict[str, Any]:
            if isinstance(tool_input, str) and tool_input.strip():
                self.query_filter_input = tool_input.strip()
            return self.run_view_query().data

        return [
            Tool(
                name=f"snowflake_view_{vw.lower()}",
                description=f"Query governed Snowflake view {fqn} ({getattr(self, 'view_subtype', 'Standard View')}) under active role {LIVE_BRIDGE.active_role}.",
                func=_invoke,
                metadata={"qualified_name": fqn, "component": "SnowflakeViews", "tool_mode": True},
            )
        ]
