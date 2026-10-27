from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.components.llm_operations import SnowflakeCortexSearchQueryEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.providers.language_models import (
    CortexSearchToolSchema,
    build_structured_tool,
    sanitize_tool_identifier,
)


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


def _search_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _search_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _search_services(
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    svcs = list(LIVE_BRIDGE.get_context_objects(database, schema, "cortex_search_services", flow_id=eff_fid))
    if not svcs:
        svcs = [f"{database}.{schema}.DOC_SEARCH_SERVICE", "DOC_SEARCH_SERVICE"]
    return svcs


def _memory_catalog_options(flow_id: str | None = None) -> list[str]:
    try:
        return LIVE_BRIDGE.get_memory_catalog_options(include_none=True, flow_id=flow_id)
    except TypeError:
        return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


def _search_rest_endpoint(database: str, schema: str, search_service: str) -> str:
    svc_short = (search_service or "DOC_SEARCH_SERVICE").strip().split(".")[-1]
    return f"/api/v2/databases/{database}/schemas/{schema}/cortex-search-services/{svc_short}:query"


class SnowflakeCortexSearchComponent(Component):
    display_name = "Cortex Search"
    description = (
        "Query an existing Snowflake Cortex Search Service in <Database>.<Schema> "
        "(SHOW CORTEX SEARCH SERVICES IN SCHEMA) with hybrid lexical + vector retrieval, "
        "ATTRIBUTES filters (@eq, @contains, @and, @or, @not), limit, scoring_config, and Tool Mode for Agents."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearch"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_search_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_search_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="search_service",
            display_name="Cortex Search Service",
            options=_search_services("SNOWFLAKE_LANGFLOW", "CORE"),
            value="DOC_SEARCH_SERVICE",
            real_time_refresh=True,
            combobox=True,
        ),
        MessageTextInput(
            name="query",
            display_name="Search Query",
            value="How does Snowflake Langflow govern active roles on SPCS?",
            tool_mode=True,
        ),
        StrInput(
            name="columns_csv",
            display_name="Returned Columns (CSV)",
            value="chunk,relative_path,page_number",
        ),
        MultilineInput(
            name="filter_json",
            display_name="Filter JSON (@eq, @contains, @and, @or, @not on ATTRIBUTES)",
            value="",
            advanced=False,
            tool_mode=True,
        ),
        IntInput(
            name="limit",
            display_name="Max Results (limit)",
            value=5,
        ),
        MultilineInput(
            name="scoring_config_json",
            display_name="Scoring Config JSON (optional)",
            value="",
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Search Results Message", name="message", method="run_search"),
        Output(display_name="Search Metadata & SQL", name="metadata", method="get_search_metadata"),
        Output(display_name="Tool", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _search_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _search_schemas(db, flow_id=flow_id)
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
        if "search_service" in build_config:
            svc_opts = _search_services(db, sch, flow_id=flow_id)
            build_config["search_service"]["options"] = svc_opts
            if svc_opts and build_config["search_service"].get("value") not in svc_opts:
                build_config["search_service"]["value"] = svc_opts[0]
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = _memory_catalog_options(flow_id=flow_id)
        return build_config

    def _build_sql_and_payload(self) -> tuple[str, dict[str, Any]]:
        return SnowflakeCortexSearchQueryEngine.build_search_preview_sql(
            database=getattr(self, "database", "") or "SNOWFLAKE_LANGFLOW",
            schema=getattr(self, "schema_name", "") or "CORE",
            search_service=getattr(self, "search_service", "") or "DOC_SEARCH_SERVICE",
            query=getattr(self, "query", "") or "",
            columns_csv=getattr(self, "columns_csv", "") or "chunk",
            filter_json=getattr(self, "filter_json", "") or "",
            limit=int(getattr(self, "limit", 5) or 5),
            scoring_config_json=getattr(self, "scoring_config_json", "") or "",
        )

    def run_search(self) -> Message:
        mem_ref = (getattr(self, "memory_catalog_ref", "None") or "None").strip()
        if mem_ref and mem_ref != "None":
            recalled = LIVE_BRIDGE.recall_memory(
                mem_ref,
                query=str(getattr(self, "query", "") or ""),
                limit=int(getattr(self, "limit", 5) or 5),
            )
            return Message(text=json.dumps(recalled, indent=2))
        sql_stmt, payload = self._build_sql_and_payload()
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW"
        sch = getattr(self, "schema_name", "CORE") or "CORE"
        svc = getattr(self, "search_service", "DOC_SEARCH_SERVICE") or "DOC_SEARCH_SERVICE"
        rest_ep = _search_rest_endpoint(db, sch, svc)
        res = LIVE_BRIDGE.execute_sql(sql_stmt)
        return Message(
            text=json.dumps(
                {
                    "database": db,
                    "schema_name": sch,
                    "search_service": svc,
                    "rest_endpoint": rest_ep,
                    "active_role": LIVE_BRIDGE.active_role,
                    "request_payload": payload,
                    "sql": sql_stmt,
                    "result": res,
                },
                indent=2,
            )
        )

    def get_search_metadata(self) -> Data:
        mem_ref = (getattr(self, "memory_catalog_ref", "None") or "None").strip()
        if mem_ref and mem_ref != "None":
            recalled = LIVE_BRIDGE.recall_memory(
                mem_ref,
                query=str(getattr(self, "query", "") or ""),
                limit=int(getattr(self, "limit", 5) or 5),
            )
            return Data(data=recalled)
        sql_stmt, payload = self._build_sql_and_payload()
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW"
        sch = getattr(self, "schema_name", "CORE") or "CORE"
        svc = getattr(self, "search_service", "DOC_SEARCH_SERVICE") or "DOC_SEARCH_SERVICE"
        rest_ep = _search_rest_endpoint(db, sch, svc)
        return Data(
            data={
                "database": db,
                "schema_name": sch,
                "search_service": svc,
                "rest_endpoint": rest_ep,
                "discovery_sql": f"SHOW CORTEX SEARCH SERVICES IN SCHEMA {db}.{sch}",
                "search_preview_sql": sql_stmt,
                "request_payload": payload,
                "active_role": LIVE_BRIDGE.active_role,
            }
        )

    def search_results(self) -> Data:
        return self.get_search_metadata()

    def to_toolkit(self) -> list[Tool]:
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW"
        sch = getattr(self, "schema_name", "CORE") or "CORE"
        svc = (getattr(self, "search_service", "DOC_SEARCH_SERVICE") or "DOC_SEARCH_SERVICE").strip()
        svc_short = svc.split(".")[-1] if svc else "DOC_SEARCH_SERVICE"
        rest_ep = _search_rest_endpoint(db, sch, svc)
        tool_name = (
            f"cortex_search_{sanitize_tool_identifier(db)}_"
            f"{sanitize_tool_identifier(sch)}_{sanitize_tool_identifier(svc_short)}"
        )

        def _invoke(query: str = "", limit: int | None = None, **kwargs: Any) -> dict[str, Any]:
            resolved_q = query
            if not resolved_q and isinstance(kwargs.get("tool_input"), str):
                resolved_q = kwargs["tool_input"]
            if isinstance(resolved_q, str) and resolved_q.strip():
                self.query = resolved_q.strip()
            if limit is not None:
                try:
                    self.limit = int(limit)
                except (TypeError, ValueError):
                    pass
            msg = self.run_search()
            raw_text = getattr(msg, "text", "") or ""
            try:
                parsed = json.loads(raw_text)
                if isinstance(parsed, dict):
                    return parsed
                return {"results": parsed}
            except Exception:  # noqa: BLE001
                return {"text": raw_text}

        return [
            build_structured_tool(
                name=tool_name,
                description=(
                    f"Hybrid lexical+vector search over Snowflake Cortex Search Service {db}.{sch}.{svc_short} "
                    f"({rest_ep}) under active role {LIVE_BRIDGE.active_role}."
                ),
                func=_invoke,
                args_schema=CortexSearchToolSchema,
                metadata={
                    "database": db,
                    "schema_name": sch,
                    "search_service": svc,
                    "rest_endpoint": rest_ep,
                    "component": "SnowflakeCortexSearch",
                    "tool_mode": True,
                },
            )
        ]
