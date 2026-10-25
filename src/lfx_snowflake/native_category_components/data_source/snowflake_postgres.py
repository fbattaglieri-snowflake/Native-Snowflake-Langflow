from __future__ import annotations

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


def _postgres_instances(flow_id: str | None = None) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    insts: list[str] = []
    get_pg_fn = getattr(LIVE_BRIDGE, "get_context_postgres_instances", None)
    if callable(get_pg_fn):
        for item in get_pg_fn(flow_id=eff_fid) or []:
            s = str(item).strip()
            if s and s not in insts:
                insts.append(s)

    ctx = (
        LIVE_BRIDGE.get_flow_context(eff_fid)
        if eff_fid and hasattr(LIVE_BRIDGE, "get_flow_context")
        else getattr(LIVE_BRIDGE, "selected_context", {})
    )
    if isinstance(ctx, dict):
        for top_key in ("postgres_instances", "selected_postgres_instances"):
            raw_list = ctx.get(top_key)
            if isinstance(raw_list, list):
                for item in raw_list:
                    s = str(item).strip()
                    if s and s not in insts:
                        insts.append(s)
        for map_key in ("selected_objects", "objects_by_db_schema"):
            obj_map = ctx.get(map_key)
            if isinstance(obj_map, dict):
                for bucket in obj_map.values():
                    if isinstance(bucket, dict):
                        pg_bucket = bucket.get("postgres_instances")
                        if isinstance(pg_bucket, list):
                            for item in pg_bucket:
                                s = str(item).strip()
                                if s and s not in insts:
                                    insts.append(s)
        vis = ctx.get("visible_objects")
        if isinstance(vis, dict) and isinstance(vis.get("postgres_instances"), list) and not insts:
            for item in vis["postgres_instances"]:
                s = str(item).strip()
                if s and s not in insts:
                    insts.append(s)

    if not insts:
        insts = ["LANGFLOW_PG_INSTANCE"]
    return insts


def _postgres_tables(
    postgres_instance: str | None = None,
    postgres_database: str = "snowflake",
    postgres_schema: str = "public",
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    inst = (postgres_instance or "LANGFLOW_PG_INSTANCE").strip()
    pg_db = (postgres_database or "snowflake").strip()
    pg_sch = (postgres_schema or "public").strip()
    tables: list[str] = []
    get_tbl_fn = getattr(LIVE_BRIDGE, "get_context_postgres_tables", None)
    if callable(get_tbl_fn):
        for item in (
            get_tbl_fn(
                postgres_instance=inst,
                postgres_database=pg_db,
                postgres_schema=pg_sch,
                flow_id=eff_fid,
            )
            or []
        ):
            s = str(item).strip()
            if s and s not in tables:
                tables.append(s)

    ctx = (
        LIVE_BRIDGE.get_flow_context(eff_fid)
        if eff_fid and hasattr(LIVE_BRIDGE, "get_flow_context")
        else getattr(LIVE_BRIDGE, "selected_context", {})
    )
    if isinstance(ctx, dict):
        for map_key in ("postgres_tables_by_instance", "selected_postgres_tables"):
            tmap = ctx.get(map_key)
            if isinstance(tmap, dict):
                raw_tbls = tmap.get(inst) or tmap.get(inst.upper())
                if isinstance(raw_tbls, list):
                    for item in raw_tbls:
                        s = str(item).strip()
                        if s and s not in tables:
                            tables.append(s)

    tables = filter_business_data_source_objects(
        tables,
        memories_registry=getattr(LIVE_BRIDGE, "memories_registry", None),
    )
    if not tables:
        tables = ["langflow_pg_events"]
    return tables


def _memory_catalog_options() -> list[str]:
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


class SnowflakePostgresComponent(Component):
    display_name = "Postgres"
    description = (
        "Governed Snowflake Postgres Data Source (Account-Level Instance -> Postgres Database -> Postgres Schema -> "
        "Postgres Table cascading selection, Relational/JSONB/pgvector profiles, input ports, and Read/Write Tool Mode for Agents)."
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
        DropdownInput(
            name="table_name",
            display_name="Postgres Table Name",
            options=_postgres_tables("LANGFLOW_PG_INSTANCE", "snowflake", "public"),
            value="langflow_pg_events",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="storage_profile",
            display_name="Storage Profile",
            options=["Relational", "JSONB", "pgvector"],
            value="Relational",
            real_time_refresh=True,
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
        flow_id = _resolve_flow_id(self, build_config)
        pg_opts = _postgres_instances(flow_id=flow_id)
        if "postgres_instance" in build_config:
            build_config["postgres_instance"]["options"] = pg_opts
            if field_name == "postgres_instance" and isinstance(field_value, str):
                build_config["postgres_instance"]["value"] = field_value
            elif pg_opts and build_config["postgres_instance"].get("value") not in pg_opts:
                build_config["postgres_instance"]["value"] = pg_opts[0]

        inst = (
            field_value
            if field_name == "postgres_instance" and isinstance(field_value, str)
            else build_config.get("postgres_instance", {}).get("value")
            or (pg_opts[0] if pg_opts else "LANGFLOW_PG_INSTANCE")
        )
        pg_db = (
            field_value
            if field_name == "postgres_database" and isinstance(field_value, str)
            else build_config.get("postgres_database", {}).get("value")
            or getattr(self, "postgres_database", "snowflake")
            or "snowflake"
        )
        pg_sch = (
            field_value
            if field_name == "postgres_schema" and isinstance(field_value, str)
            else build_config.get("postgres_schema", {}).get("value")
            or getattr(self, "postgres_schema", "public")
            or "public"
        )
        if "table_name" in build_config:
            tbl_opts = _postgres_tables(inst, pg_db, pg_sch, flow_id=flow_id)
            build_config["table_name"]["options"] = tbl_opts
            if field_name == "table_name" and isinstance(field_value, str):
                build_config["table_name"]["value"] = field_value
            elif tbl_opts and build_config["table_name"].get("value") not in tbl_opts:
                build_config["table_name"]["value"] = tbl_opts[0]
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
