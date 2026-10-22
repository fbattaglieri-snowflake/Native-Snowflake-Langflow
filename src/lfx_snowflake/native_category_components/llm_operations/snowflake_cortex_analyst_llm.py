from __future__ import annotations

import json
from typing import Any

from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, Output
from lfx.schema import DataFrame
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _analyst_llm_databases() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _analyst_llm_schemas(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


def _analyst_llm_sources(database: str, schema: str, source_mode: str) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    if source_mode == "Stage YAML":
        models = list(vis.get("semantic_models") or [])
        fallback = f"@{database}.{schema}.SNOWFLAKE_LANGFLOW_STAGE/semantic_model.yaml"
        if fallback not in models:
            models.append(fallback)
        return models
    views = list(vis.get("semantic_views") or [])
    fallback_sv = f"{database}.{schema}.EXAMPLE_SEMANTIC_VIEW"
    if fallback_sv not in views:
        views.append(fallback_sv)
    return views


class SnowflakeCortexAnalystLLMComponent(Component):
    display_name = "Cortex Analyst"
    description = (
        "Natural-language-to-SQL in LLM Operations via Snowflake Cortex Analyst over an existing "
        "Semantic View (SHOW SEMANTIC VIEWS IN SCHEMA) or Stage YAML (LIST @DB.SCHEMA.STAGE)."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAnalystLLM"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_analyst_llm_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_analyst_llm_schemas("SNOWFLAKE_LANGFLOW"),
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
            options=_analyst_llm_sources("SNOWFLAKE_LANGFLOW", "CORE", "Semantic View"),
            value="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_SEMANTIC_VIEW",
            real_time_refresh=True,
            combobox=True,
        ),
        MessageTextInput(
            name="question",
            display_name="Analytical Question",
            value="Show the latest flow runs by active role",
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
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _analyst_llm_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _analyst_llm_schemas(db)
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or "CORE"
        )
        mode = (
            field_value
            if field_name == "source_mode" and isinstance(field_value, str)
            else build_config.get("source_mode", {}).get("value") or "Semantic View"
        )
        if "semantic_source" in build_config:
            build_config["semantic_source"]["options"] = _analyst_llm_sources(db, sch, mode)
        return build_config

    def ask_analyst(self) -> Message:
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW"
        sch = getattr(self, "schema_name", "CORE") or "CORE"
        src = (self.semantic_source or "").strip()
        if self.source_mode == "Stage YAML" and not src.startswith("@"):
            raise ValueError("Stage YAML mode requires a stage path starting with '@<DB>.<SCHEMA>.<STAGE>/...'.")
        return Message(
            text=json.dumps(
                {
                    "database": db,
                    "schema_name": sch,
                    "source_mode": self.source_mode,
                    "semantic_source": src,
                    "discovery_sql": (
                        f"SHOW SEMANTIC VIEWS IN SCHEMA {db}.{sch}"
                        if self.source_mode == "Semantic View"
                        else f"LIST @{db}.{sch}.SNOWFLAKE_LANGFLOW_STAGE"
                    ),
                    "active_role": LIVE_BRIDGE.active_role,
                    "question": self.question,
                },
                indent=2,
            )
        )

    def run_analyst_sql(self) -> DataFrame:
        stmt = (
            f"SELECT run_id, flow_id, effective_role, status, start_ts "
            f"FROM {LIVE_BRIDGE.runs_table} LIMIT {max(1, int(self.max_rows or 50))}"
        )
        res = LIVE_BRIDGE.execute_sql(stmt)
        return DataFrame(res.get("rows", []))
