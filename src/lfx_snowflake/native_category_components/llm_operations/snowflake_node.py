from __future__ import annotations

import json
from typing import Any

from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MultilineInput, Output
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _node_databases() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _node_schemas(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


def _node_targets() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    items: list[str] = []
    for k in ("tables", "views", "procedures", "functions"):
        for item in vis.get(k) or []:
            if item not in items:
                items.append(item)
    for fallback in ("LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"):
        if fallback not in items:
            items.append(fallback)
    return items


class SnowflakeLLMOperationComponent(Component):
    display_name = "Snowflake"
    description = (
        "Unified context-bound Snowflake operational node in LLM Operations bound to "
        "Hub <Database>.<Schema> and active role."
    )
    icon = "Snowflake"
    name = "SnowflakeLLMNode"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_node_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_node_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="operation_mode",
            display_name="Operation Mode",
            options=["Query Object", "Inspect Metadata", "Execute Custom SQL"],
            value="Query Object",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="target_object",
            display_name="Target Object in Schema",
            options=_node_targets(),
            value="LANGFLOW_FLOW_RUNS",
            real_time_refresh=True,
            combobox=True,
        ),
        MultilineInput(
            name="custom_sql",
            display_name="Optional SQL Override",
            value="",
            advanced=True,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows",
            value=50,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Operation Message", name="message", method="run_operation"),
        Output(display_name="Result DataFrame", name="dataframe", method="run_dataframe"),
        Output(display_name="Context Metadata", name="metadata", method="run_metadata"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _node_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _node_schemas(db)
        if "target_object" in build_config:
            build_config["target_object"]["options"] = _node_targets()
        return build_config

    def _build_sql(self) -> str:
        db = (self.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = (self.schema_name or "CORE").strip()
        tgt = (self.target_object or "LANGFLOW_FLOW_RUNS").strip()
        fqn = tgt if "." in tgt else f"{db}.{sch}.{tgt}"
        if self.operation_mode == "Execute Custom SQL" and (self.custom_sql or "").strip():
            return (self.custom_sql or "").strip()
        if self.operation_mode == "Inspect Metadata":
            return f"DESCRIBE TABLE {fqn}"
        return f"SELECT * FROM {fqn} LIMIT {max(1, int(self.max_rows or 50))}"

    def run_operation(self) -> Message:
        stmt = self._build_sql()
        res = LIVE_BRIDGE.execute_sql(stmt)
        return Message(
            text=json.dumps(
                {
                    "database": self.database,
                    "schema_name": self.schema_name,
                    "operation_mode": self.operation_mode,
                    "target_object": self.target_object,
                    "active_role": LIVE_BRIDGE.active_role,
                    "sql": stmt,
                    "result": res,
                },
                indent=2,
            )
        )

    def run_dataframe(self) -> DataFrame:
        res = LIVE_BRIDGE.execute_sql(self._build_sql())
        return DataFrame(res.get("rows", []))

    def run_metadata(self) -> Data:
        return Data(
            data={
                "database": self.database,
                "schema_name": self.schema_name,
                "operation_mode": self.operation_mode,
                "target_object": self.target_object,
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "sql_preview": self._build_sql(),
            }
        )
