from __future__ import annotations

import json
from typing import Any

from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message
from lfx_snowflake.components.llm_operations import SnowflakeCortexSearchQueryEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _search_databases() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _search_schemas(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


def _search_services(database: str = "SNOWFLAKE_LANGFLOW", schema: str = "CORE") -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    svcs = list(vis.get("cortex_search_services") or [])
    fallback = f"{database}.{schema}.DOC_SEARCH_SERVICE"
    if fallback not in svcs:
        svcs.append(fallback)
    if "DOC_SEARCH_SERVICE" not in svcs:
        svcs.append("DOC_SEARCH_SERVICE")
    return svcs


class SnowflakeCortexSearchComponent(Component):
    display_name = "Cortex Search"
    description = (
        "Query an existing Snowflake Cortex Search Service in <Database>.<Schema> "
        "(SHOW CORTEX SEARCH SERVICES IN SCHEMA) with hybrid lexical + vector retrieval, "
        "ATTRIBUTES filters (@eq, @contains, @and, @or, @not), limit, and scoring_config."
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
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _search_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _search_schemas(db)
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or "CORE"
        )
        if "search_service" in build_config:
            build_config["search_service"]["options"] = _search_services(db, sch)
        return build_config

    def _build_sql_and_payload(self) -> tuple[str, dict[str, Any]]:
        return SnowflakeCortexSearchQueryEngine.build_search_preview_sql(
            database=self.database or "SNOWFLAKE_LANGFLOW",
            schema=self.schema_name or "CORE",
            search_service=self.search_service or "DOC_SEARCH_SERVICE",
            query=self.query or "",
            columns_csv=self.columns_csv or "chunk",
            filter_json=getattr(self, "filter_json", "") or "",
            limit=int(self.limit or 5),
            scoring_config_json=getattr(self, "scoring_config_json", "") or "",
        )

    def run_search(self) -> Message:
        sql_stmt, payload = self._build_sql_and_payload()
        res = LIVE_BRIDGE.execute_sql(sql_stmt)
        return Message(
            text=json.dumps(
                {
                    "database": self.database,
                    "schema_name": self.schema_name,
                    "search_service": self.search_service,
                    "active_role": LIVE_BRIDGE.active_role,
                    "request_payload": payload,
                    "sql": sql_stmt,
                    "result": res,
                },
                indent=2,
            )
        )

    def get_search_metadata(self) -> Data:
        sql_stmt, payload = self._build_sql_and_payload()
        return Data(
            data={
                "database": self.database,
                "schema_name": self.schema_name,
                "search_service": self.search_service,
                "discovery_sql": f"SHOW CORTEX SEARCH SERVICES IN SCHEMA {self.database}.{self.schema_name}",
                "search_preview_sql": sql_stmt,
                "request_payload": payload,
                "active_role": LIVE_BRIDGE.active_role,
            }
        )
