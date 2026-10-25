from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, Output
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake._lfx_compat import Tool
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


def _analyst_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _analyst_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _analyst_sources(
    database: str,
    schema: str,
    source_mode: str,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    if source_mode in ("Stage YAML", "Stage YAML Model"):
        models = list(LIVE_BRIDGE.get_context_objects(database, schema, "semantic_models", flow_id=eff_fid))
        if not models:
            models = [f"@{database}.{schema}.SNOWFLAKE_LANGFLOW_STAGE/semantic_model.yaml"]
        return models
    views = list(LIVE_BRIDGE.get_context_objects(database, schema, "semantic_views", flow_id=eff_fid))
    if not views:
        views = [f"{database}.{schema}.EXAMPLE_SEMANTIC_VIEW"]
    return views


def _memory_catalog_options() -> list[str]:
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


class SnowflakeCortexAnalystComponent(Component):
    display_name = "Cortex Analyst"
    description = (
        "Natural-language-to-SQL via Snowflake Cortex Analyst (/api/v2/cortex/analyst/message) over an existing "
        "Semantic View (SHOW SEMANTIC VIEWS IN SCHEMA) or Stage YAML (LIST @DB.SCHEMA.STAGE) in the active role session, "
        "with Tool Mode for Agents."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAnalyst"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_analyst_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_analyst_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="source_mode",
            display_name="Semantic Source Type",
            options=["Semantic View", "Stage YAML"],
            value="Semantic View",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="semantic_source",
            display_name="Semantic View FQN or @DB.SCHEMA.STAGE/file.yaml",
            options=_analyst_sources("SNOWFLAKE_LANGFLOW", "CORE", "Semantic View"),
            value="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_SEMANTIC_VIEW",
            real_time_refresh=True,
            combobox=True,
        ),
        MessageTextInput(
            name="question",
            display_name="Analytical Question",
            value="Show the latest flow runs by active role",
            tool_mode=True,
        ),
        BoolInput(
            name="execute_generated_sql",
            display_name="Execute Generated SQL in Active Session",
            value=True,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Result Rows",
            value=50,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Analyst Message", name="message", method="ask_analyst"),
        Output(display_name="SQL Result DataFrame", name="dataframe", method="run_analyst_sql"),
        Output(display_name="Analyst Payload & SQL", name="metadata", method="get_analyst_payload"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _analyst_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _analyst_schemas(db, flow_id=flow_id)
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
        mode = (
            field_value
            if field_name == "source_mode" and isinstance(field_value, str)
            else build_config.get("source_mode", {}).get("value") or "Semantic View"
        )
        src_opts = _analyst_sources(db, sch, mode, flow_id=flow_id)
        if "semantic_source" in build_config:
            build_config["semantic_source"]["options"] = src_opts
            if src_opts and build_config["semantic_source"].get("value") not in src_opts:
                build_config["semantic_source"]["value"] = src_opts[0]
        build_config["semantic_view"] = {
            "name": "semantic_view",
            "options": src_opts,
            "value": src_opts[0] if src_opts else "",
        }
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = _memory_catalog_options()
        return build_config

    def _invoke(self) -> dict[str, Any]:
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW"
        sch = getattr(self, "schema_name", "CORE") or "CORE"
        src = (getattr(self, "semantic_source", "") or "").strip()
        mode = getattr(self, "source_mode", "Semantic View") or "Semantic View"
        res = LIVE_BRIDGE.invoke_cortex_analyst(
            database=db,
            schema=sch,
            semantic_source=src,
            source_mode=mode,
            question=getattr(self, "question", "") or "Show the latest flow runs by active role",
            execute_sql=bool(getattr(self, "execute_generated_sql", True)),
            max_rows=int(getattr(self, "max_rows", 50) or 50),
        )
        res["discovery_sql"] = (
            f"SHOW SEMANTIC VIEWS IN SCHEMA {db}.{sch}"
            if mode == "Semantic View"
            else f"LIST @{db}.{sch}.SNOWFLAKE_LANGFLOW_STAGE"
        )
        return res

    def ask_analyst(self) -> Message:
        return Message(text=json.dumps(self._invoke(), indent=2))

    def get_analyst_payload(self) -> Data:
        return Data(data=self._invoke())

    def run_analyst_sql(self) -> DataFrame:
        res = self._invoke()
        return DataFrame(res.get("rows", []))

    def to_toolkit(self) -> list[Tool]:
        src = (getattr(self, "semantic_source", "") or "EXAMPLE_SEMANTIC_VIEW").strip()

        def _tool_fn(tool_input: Any = None) -> dict[str, Any]:
            if isinstance(tool_input, str) and tool_input.strip():
                self.question = tool_input.strip()
            return self._invoke()

        return [
            Tool(
                name="snowflake_cortex_analyst",
                description=f"Ask analytical questions in natural language via Snowflake Cortex Analyst ({src}) under active role {LIVE_BRIDGE.active_role}.",
                func=_tool_fn,
                metadata={"semantic_source": src, "rest_endpoint": "/api/v2/cortex/analyst/message", "tool_mode": True},
            )
        ]
