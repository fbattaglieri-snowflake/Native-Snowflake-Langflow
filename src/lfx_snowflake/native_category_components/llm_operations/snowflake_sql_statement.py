from __future__ import annotations

import json

from lfx_snowflake import sql_safety as _sql_safety
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MultilineInput, Output, StrInput
from lfx.schema import Data, DataFrame
from lfx.schema.message import Message

from lfx_snowflake.components.llm_operations import SnowflakeSQLStatementEngine
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.flow_context_guard import flow_component_sql as _flow_component_sql
from lfx_snowflake.flow_context_guard import raise_if_flow_context_denied as _raise_if_flow_context_denied


def _flow_component_execute_sql(*args, **kwargs):
    """BUG-CTX-03: this component's SQL is checked against the flow's `[ ❄️ Context ]`."""
    with _flow_component_sql():
        # E2E T1: a guard refusal surfaces as a component error carrying AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT.
        return _raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(*args, **kwargs))


def _sql_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _sql_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def normalize_bindings(parsed: Any) -> dict[str, dict[str, Any]] | None:
    """BUG-LLMOP-012: typed SQL API bindings ``{"1": {"type", "value"}}``.

    Accepts a list of values (``[1, "a"]``), a positional dict (``{"1": 1}``) or an already typed dict
    (``{"1": {"type": "FIXED", "value": "1"}}``); every entry goes through ``sql_safety.binding_for``."""
    if parsed in (None, [], {}):
        return None
    if isinstance(parsed, list):
        items = [(str(i + 1), v) for i, v in enumerate(parsed)]
    elif isinstance(parsed, dict):
        items = list(parsed.items())
        keys = [str(k) for k, _ in items]
        if not all(k.isdigit() and int(k) >= 1 for k in keys) or sorted(int(k) for k in keys) != list(
            range(1, len(keys) + 1)
        ):
            raise ValueError('Bind Parameter keys must be positions "1".."n" matching the ? placeholders.')
    else:
        raise ValueError("Bind Parameters must be a JSON array or object.")
    out: dict[str, dict[str, Any]] = {}
    for key, val in items:
        if isinstance(val, dict) and set(val) <= {"type", "value"} and "value" in val:
            out[str(key)] = _sql_safety.binding_for(val["value"], val.get("type"))
        else:
            out[str(key)] = _sql_safety.binding_for(val)
    return out


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
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _sql_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _sql_schemas(db, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            cur_sch = build_config["schema_name"].get("value")
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and cur_sch not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
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
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("Bind Parameters must be a JSON array or object.") from exc
        return normalize_bindings(parsed)

    def run_dataframe(self) -> DataFrame:
        stmt = self._build_sql()
        res = _flow_component_execute_sql(stmt, bindings=self._parse_bindings())
        if not res.get("ok"):
            raise ValueError(res.get("message", "Snowflake SQL Statement failed"))
        return DataFrame(res.get("rows", []))

    def run_json_message(self) -> Message:
        stmt = self._build_sql()
        res = _flow_component_execute_sql(stmt, bindings=self._parse_bindings())
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
