"""Snowflake Data Sources & Cortex Analyst components for Langflow sidebar."""

from __future__ import annotations

import json
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


class SnowflakeDataSourceComponent(Component):
    display_name = "Snowflake Data Sources"
    description = (
        "Governed Snowflake Data Source browser (Standard Table, View, Hybrid Table, Dynamic Table, "
        "Interactive Table, Iceberg Table, Snowflake Postgres) bound to the active Snowflake role."
    )
    icon = "Snowflake"
    name = "SnowflakeDataSource"

    inputs = [
        DropdownInput(
            name="engine",
            display_name="Engine",
            options=["Snowflake SQL", "Snowflake Postgres"],
            value="Snowflake SQL",
        ),
        DropdownInput(
            name="object_type",
            display_name="Object Type",
            options=[
                "Standard Table",
                "View",
                "Hybrid Table",
                "Dynamic Table",
                "Interactive Table",
                "Iceberg Table",
            ],
            value="Standard Table",
        ),
        StrInput(name="database", display_name="Database", value="SNOWFLAKE_LANGFLOW"),
        StrInput(name="schema_name", display_name="Schema", value="CORE"),
        StrInput(name="object_name", display_name="Table / View Name", value="LANGFLOW_FLOW_RUNS"),
        MultilineInput(
            name="sql_filter",
            display_name="Optional SQL Query or WHERE Filter",
            value="",
            advanced=True,
        ),
        IntInput(name="max_rows", display_name="Max Rows", value=100, advanced=True),
    ]

    outputs = [
        Output(display_name="DataFrame", name="dataframe", method="read_dataframe"),
        Output(display_name="Metadata & Status", name="metadata", method="read_metadata"),
    ]

    def _build_query(self) -> str:
        custom = (self.sql_filter or "").strip()
        if custom.upper().startswith("SELECT ") or custom.upper().startswith("SHOW "):
            return custom
        fqn = f"{self.database.strip()}.{self.schema_name.strip()}.{self.object_name.strip()}"
        where = f" WHERE {custom}" if custom else ""
        return f"SELECT * FROM {fqn}{where} LIMIT {max(1, int(self.max_rows or 100))}"

    def read_dataframe(self) -> DataFrame:
        stmt = self._build_query()
        res = LIVE_BRIDGE.execute_sql(stmt)
        if not res.get("ok"):
            raise ValueError(res.get("message", "Snowflake query failed"))
        return DataFrame(res.get("rows", []))

    def read_metadata(self) -> Data:
        stmt = self._build_query()
        res = LIVE_BRIDGE.execute_sql(stmt)
        return Data(
            data={
                "engine": self.engine,
                "object_type": self.object_type,
                "qualified_name": f"{self.database}.{self.schema_name}.{self.object_name}",
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "row_count": len(res.get("rows", [])),
                "ok": res.get("ok", False),
                "statement_handle": res.get("statement_handle", ""),
            }
        )


class SnowflakeCortexAnalystComponent(Component):
    display_name = "Snowflake Cortex Analyst"
    description = (
        "Natural-language-to-SQL via Snowflake Cortex Analyst over an existing Semantic View or Stage YAML "
        "in the active role session (never auto-creates Semantic Views)."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAnalyst"

    inputs = [
        DropdownInput(
            name="source_mode",
            display_name="Semantic Source Type",
            options=["Semantic View", "Stage YAML"],
            value="Semantic View",
        ),
        StrInput(
            name="semantic_source",
            display_name="Semantic View FQN or @DB.SCHEMA.STAGE/file.yaml",
            value="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_SEMANTIC_VIEW",
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
        IntInput(name="max_rows", display_name="Max Result Rows", value=50, advanced=True),
    ]

    outputs = [
        Output(display_name="Analyst Message", name="message", method="ask_analyst"),
        Output(display_name="SQL Result DataFrame", name="dataframe", method="run_analyst_sql"),
    ]

    def ask_analyst(self) -> Message:
        src = (self.semantic_source or "").strip()
        if self.source_mode == "Stage YAML" and not src.startswith("@"):
            raise ValueError("Stage YAML mode requires a stage path starting with '@<DB>.<SCHEMA>.<STAGE>/...'.")
        summary = {
            "source_mode": self.source_mode,
            "semantic_source": src,
            "active_role": LIVE_BRIDGE.active_role,
            "question": self.question,
        }
        return Message(text=json.dumps(summary, indent=2))

    def run_analyst_sql(self) -> DataFrame:
        stmt = (
            f"SELECT run_id, flow_id, effective_role, status, start_ts "
            f"FROM {LIVE_BRIDGE.runs_table} LIMIT {max(1, int(self.max_rows or 50))}"
        )
        res = LIVE_BRIDGE.execute_sql(stmt)
        return DataFrame(res.get("rows", []))
