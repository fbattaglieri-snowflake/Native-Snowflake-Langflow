from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DataInput, DropdownInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import Data, DataFrame

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.components.data_sources import filter_business_data_source_objects
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _resolve_flow_id(component: Any = None, build_config: dict[str, Any] | None = None) -> str | None:
    resolver = getattr(LIVE_BRIDGE, "_resolve_component_flow_id", None) or getattr(
        LIVE_BRIDGE, "resolve_component_flow_id", None
    )
    if callable(resolver):
        res = resolver(component, build_config)
        if res:
            return str(res).strip()
    if isinstance(build_config, dict):
        cfg_fid = build_config.get("_frontend_node_flow_id") or build_config.get("flow_id")
        if isinstance(cfg_fid, dict):
            cfg_fid = cfg_fid.get("value")
        if cfg_fid and str(cfg_fid).strip():
            return str(cfg_fid).strip()
    if component is not None:
        for attr in ("flow_id", "_flow_id"):
            val = getattr(component, attr, None)
            if val and str(val).strip():
                return str(val).strip()
        graph = getattr(component, "graph", None)
        if graph is not None:
            g_fid = getattr(graph, "flow_id", None) or getattr(graph, "_flow_id", None)
            if g_fid and str(g_fid).strip():
                return str(g_fid).strip()
    cur_fn = getattr(LIVE_BRIDGE, "get_current_flow_id", None) or getattr(
        LIVE_BRIDGE, "get_current_snowflake_flow_id", None
    )
    if callable(cur_fn):
        cur = cur_fn()
        if cur and str(cur).strip():
            return str(cur).strip()
    return None


def _iceberg_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _iceberg_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _iceberg_objects(
    database: str | None = None,
    schema: str | None = None,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    objs = list(LIVE_BRIDGE.get_context_objects(database, schema, "iceberg_tables", flow_id=eff_fid))
    objs = filter_business_data_source_objects(
        objs,
        memories_registry=getattr(LIVE_BRIDGE, "memories_registry", None),
    )
    if not objs:
        objs = ["ICEBERG_EVENTS_TBL"]
    return objs


def _memory_catalog_options() -> list[str]:
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


class SnowflakeIcebergTablesComponent(Component):
    display_name = "Iceberg Tables"
    description = (
        "Governed Snowflake Apache Iceberg Tables Data Source (Snowflake-managed & Externally-managed "
        "with EXTERNAL_VOLUME and CATALOG) with cascading Database -> Schema -> Iceberg Table selection, "
        "input ports, and Read/Write Tool Mode for Agents."
    )
    icon = "Snowflake"
    name = "SnowflakeIcebergTables"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_iceberg_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_iceberg_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="iceberg_catalog_mode",
            display_name="Iceberg Catalog Mode",
            options=["Snowflake-managed", "Externally-managed"],
            value="Snowflake-managed",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="table_name",
            display_name="Iceberg Table Name",
            options=_iceberg_objects("SNOWFLAKE_LANGFLOW", "CORE"),
            value="ICEBERG_EVENTS_TBL",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="external_volume",
            display_name="External Volume (optional)",
            value="",
            advanced=True,
        ),
        StrInput(
            name="catalog_integration",
            display_name="Catalog Integration (optional)",
            value="SNOWFLAKE",
            advanced=True,
        ),
        DropdownInput(
            name="operation_mode",
            display_name="Operation Mode",
            options=["SELECT (Read)", "INSERT (Write)"],
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
            display_name="Rows / Payload to Write (for INSERT)",
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
        Output(display_name="Iceberg Data", name="data", method="run_iceberg_operation"),
        Output(display_name="DataFrame", name="dataframe", method="read_dataframe"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _iceberg_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _iceberg_schemas(db, flow_id=flow_id)
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
        ice_opts = _iceberg_objects(db, sch, flow_id=flow_id)
        if "table_name" in build_config:
            build_config["table_name"]["options"] = ice_opts
            if ice_opts and build_config["table_name"].get("value") not in ice_opts:
                build_config["table_name"]["value"] = ice_opts[0]
        build_config["iceberg_table_name"] = {
            "name": "iceberg_table_name",
            "options": ice_opts,
            "value": ice_opts[0] if ice_opts else "",
        }
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = _memory_catalog_options()
        return build_config

    def _build_query(self) -> str:
        db = (getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "") or "CORE").strip()
        tbl = (getattr(self, "table_name", "") or "ICEBERG_EVENTS_TBL").strip()
        fqn = tbl if "." in tbl else f"{db}.{sch}.{tbl}"
        custom = (getattr(self, "query_filter_input", "") or "").strip()
        if custom.upper().startswith(("SELECT ", "SHOW ", "INSERT ")):
            return custom
        where = f" WHERE {custom}" if custom else ""
        return f"SELECT * FROM {fqn}{where} LIMIT {max(1, int(getattr(self, 'max_rows', 100) or 100))}"

    def run_iceberg_operation(self) -> Data:
        db = (getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "") or "CORE").strip()
        tbl = (getattr(self, "table_name", "") or "ICEBERG_EVENTS_TBL").strip()
        fqn = tbl if "." in tbl else f"{db}.{sch}.{tbl}"
        mode = (getattr(self, "operation_mode", "SELECT (Read)") or "SELECT (Read)").strip()

        if "Write" in mode or "INSERT" in mode:
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("ICEBERG_WRITE", fqn)
            if denied:
                raise ValueError(denied["message"])
            payload = getattr(self, "rows_to_write", None) or {"content": getattr(self, "query_filter_input", "")}
            payload_json = json.dumps(payload, default=str).replace("'", "''")
            sql_stmt = f"INSERT INTO {fqn} SELECT '{payload_json}'"
            res = LIVE_BRIDGE.execute_sql(sql_stmt)
            return Data(
                data={
                    "operation_mode": mode,
                    "iceberg_catalog_mode": getattr(self, "iceberg_catalog_mode", "Snowflake-managed"),
                    "qualified_name": fqn,
                    "sql": sql_stmt,
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                    "result": res,
                    "ok": res.get("ok", False),
                }
            )

        stmt = self._build_query()
        res = LIVE_BRIDGE.execute_sql(stmt)
        return Data(
            data={
                "operation_mode": mode,
                "iceberg_catalog_mode": getattr(self, "iceberg_catalog_mode", "Snowflake-managed"),
                "database": db,
                "schema_name": sch,
                "table_name": tbl,
                "qualified_name": fqn,
                "external_volume": getattr(self, "external_volume", "") or None,
                "catalog_integration": getattr(self, "catalog_integration", "SNOWFLAKE"),
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
        tbl = (getattr(self, "table_name", "") or "ICEBERG_EVENTS_TBL").strip()
        fqn = tbl if "." in tbl else f"{db}.{sch}.{tbl}"

        def _invoke(tool_input: Any = None) -> dict[str, Any]:
            if isinstance(tool_input, str) and tool_input.strip():
                self.query_filter_input = tool_input.strip()
            return self.run_iceberg_operation().data

        return [
            Tool(
                name=f"snowflake_iceberg_{tbl.lower()}",
                description=f"Query or insert into governed Apache Iceberg table {fqn} under active role {LIVE_BRIDGE.active_role}.",
                func=_invoke,
                metadata={"qualified_name": fqn, "component": "SnowflakeIcebergTables", "tool_mode": True},
            )
        ]
