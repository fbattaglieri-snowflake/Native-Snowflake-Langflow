from __future__ import annotations

import json
from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
    from lfx.schema import Data
    from lfx.schema.message import Message
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import (
        Component,
        Data,
        DropdownInput,
        IntInput,
        Message,
        MessageTextInput,
        MultilineInput,
        Output,
        StrInput,
    )

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.components.llm_operations import SnowflakeCortexSearchQueryEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _search_databases() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _search_schemas(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


def _search_services(database: str = "SNOWFLAKE_LANGFLOW", schema: str = "CORE") -> list[str]:
    svcs = list(LIVE_BRIDGE.get_context_objects(database, schema, "cortex_search_services"))
    if not svcs:
        svcs = [f"{database}.{schema}.DOC_SEARCH_SERVICE", "DOC_SEARCH_SERVICE"]
    return svcs


def _memory_catalog_options() -> list[str]:
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=True)


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
        DropdownInput(
            name="memory_catalog_ref",
            display_name="Snowflake Memory Recall (Optional)",
            options=_memory_catalog_options(),
            value="None",
            real_time_refresh=True,
            combobox=True,
            advanced=True,
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
        if "database" in build_config:
            build_config["database"]["options"] = _search_databases()
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _search_schemas(db)
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
            svc_opts = _search_services(db, sch)
            build_config["search_service"]["options"] = svc_opts
            if svc_opts and build_config["search_service"].get("value") not in svc_opts:
                build_config["search_service"]["value"] = svc_opts[0]
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = _memory_catalog_options()
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
        res = LIVE_BRIDGE.execute_sql(sql_stmt)
        return Message(
            text=json.dumps(
                {
                    "database": getattr(self, "database", "SNOWFLAKE_LANGFLOW"),
                    "schema_name": getattr(self, "schema_name", "CORE"),
                    "search_service": getattr(self, "search_service", "DOC_SEARCH_SERVICE"),
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
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        sch = getattr(self, "schema_name", "CORE")
        return Data(
            data={
                "database": db,
                "schema_name": sch,
                "search_service": getattr(self, "search_service", "DOC_SEARCH_SERVICE"),
                "discovery_sql": f"SHOW CORTEX SEARCH SERVICES IN SCHEMA {db}.{sch}",
                "search_preview_sql": sql_stmt,
                "request_payload": payload,
                "active_role": LIVE_BRIDGE.active_role,
            }
        )

    def search_results(self) -> Data:
        return self.get_search_metadata()

    def to_toolkit(self) -> list[Tool]:
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        sch = getattr(self, "schema_name", "CORE")
        svc = getattr(self, "search_service", "DOC_SEARCH_SERVICE")

        def _invoke(tool_input: Any = None) -> dict[str, Any]:
            if isinstance(tool_input, str) and tool_input.strip():
                self.query = tool_input.strip()
            return self.get_search_metadata().data

        return [
            Tool(
                name=f"snowflake_cortex_search_{svc.lower().replace('.', '_')}",
                description=f"Hybrid lexical+vector search over Snowflake Cortex Search Service {db}.{sch}.{svc} under active role {LIVE_BRIDGE.active_role}.",
                func=_invoke,
                metadata={"search_service": svc, "component": "SnowflakeCortexSearch", "tool_mode": True},
            )
        ]
