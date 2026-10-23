from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MultilineInput, Output, StrInput
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake.components.llm_operations import SnowflakeSQLStatementEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _sql_databases() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _sql_schemas(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


class SnowflakeSQLStatementComponent(Component):
    display_name = "SQL Statement"
    description = (
        "Execute a governed Snowflake SQL statement (SELECT / CALL / SHOW) within context "
        "<Database>.<Schema> under the active role with DataFrame or JSON Object (OBJECT_CONSTRUCT) output."
    )
    icon = "Snowflake"
    name = "SnowflakeSQLStatement"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_sql_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_sql_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        MultilineInput(
            name="sql_statement",
            display_name="SQL Statement",
            value="SELECT run_id, flow_id, effective_role, status FROM <DB>.<SCHEMA>.LANGFLOW_FLOW_RUNS ORDER BY start_ts DESC",
        ),
        StrInput(
            name="bind_parameters_json",
            display_name="Bind Parameters JSON (optional)",
            value="{}",
            advanced=True,
        ),
        DropdownInput(
            name="output_format",
            display_name="Output Format",
            options=["DataFrame", "JSON Object (OBJECT_CONSTRUCT)"],
            value="DataFrame",
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows",
            value=100,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Result DataFrame", name="dataframe", method="run_dataframe"),
        Output(display_name="JSON Message", name="message", method="run_json_message"),
        Output(display_name="Execution Metadata", name="metadata", method="run_metadata"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _sql_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _sql_schemas(db)
        return build_config

    def _build_sql(self) -> str:
        return SnowflakeSQLStatementEngine.build_sql(
            database=self.database or "SNOWFLAKE_LANGFLOW",
            schema=self.schema_name or "CORE",
            sql_statement=self.sql_statement or "",
            output_format=self.output_format or "DataFrame",
            max_rows=int(self.max_rows or 100),
        )

    def _parse_bindings(self) -> dict[str, Any] | None:
        raw = (getattr(self, "bind_parameters_json", "") or "").strip()
        if not raw or raw == "{}":
            return None
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) and parsed else None

    def run_dataframe(self) -> DataFrame:
        stmt = self._build_sql()
        res = LIVE_BRIDGE.execute_sql(stmt, bindings=self._parse_bindings())
        if not res.get("ok"):
            raise ValueError(res.get("message", "Snowflake SQL Statement failed"))
        return DataFrame(res.get("rows", []))

    def run_json_message(self) -> Message:
        stmt = self._build_sql()
        res = LIVE_BRIDGE.execute_sql(stmt, bindings=self._parse_bindings())
        return Message(
            text=json.dumps(
                {
                    "database": self.database,
                    "schema_name": self.schema_name,
                    "output_format": self.output_format,
                    "active_role": LIVE_BRIDGE.active_role,
                    "sql": stmt,
                    "rows": res.get("rows", []),
                    "ok": res.get("ok", False),
                },
                indent=2,
            )
        )

    def run_metadata(self) -> Data:
        stmt = self._build_sql()
        return Data(
            data={
                "database": self.database,
                "schema_name": self.schema_name,
                "output_format": self.output_format,
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "sql_preview": stmt,
            }
        )
