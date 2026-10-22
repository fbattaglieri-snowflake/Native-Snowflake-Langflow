from __future__ import annotations

import json
from typing import Any

from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import DataFrame
from lfx.schema.message import Message
from lfx_snowflake.components.llm_operations import SnowflakeFunctionEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_REPLACE_FUNC_OPTION = "+ Create / Replace Function..."


def _func_databases() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _func_schemas(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


def _func_options() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    funcs = list(vis.get("functions") or [])
    if CREATE_REPLACE_FUNC_OPTION not in funcs:
        funcs.insert(0, CREATE_REPLACE_FUNC_OPTION)
    return funcs


class SnowflakeFunctionComponent(Component):
    display_name = "Snowflake Function (UDF / UDTF)"
    description = (
        "Author, preview, register, and invoke Snowflake Scalar UDFs and Tabular UDTFs "
        "(SQL or Python 3.12 with optional Stage IMPORTS) in <Database>.<Schema>."
    )
    icon = "Snowflake"
    name = "SnowflakeFunctionUDF"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_func_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_func_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="function_selector",
            display_name="Function in Schema",
            options=_func_options(),
            value=CREATE_REPLACE_FUNC_OPTION,
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="function_name",
            display_name="Function Name",
            value="NORMALIZE_TEXT_UDF",
        ),
        DropdownInput(
            name="function_kind",
            display_name="Function Kind",
            options=["Scalar UDF", "Tabular UDTF"],
            value="Scalar UDF",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="language",
            display_name="Language",
            options=["SQL", "PYTHON"],
            value="SQL",
            real_time_refresh=True,
        ),
        StrInput(
            name="signature_args",
            display_name="Signature Arguments",
            value="x VARCHAR",
        ),
        StrInput(
            name="return_type",
            display_name="Return Type (VARCHAR, VARIANT, or TABLE(...))",
            value="VARCHAR",
        ),
        StrInput(
            name="stage_imports",
            display_name="Stage IMPORTS (optional CSV, e.g. @DB.SCHEMA.STAGE/helper.py)",
            value="",
            advanced=True,
        ),
        MultilineInput(
            name="body",
            display_name="Function Body (SQL expression or Python handler)",
            value="UPPER(TRIM(x))",
        ),
        DropdownInput(
            name="action_mode",
            display_name="Action Mode",
            options=["Preview DDL", "Invoke Existing", "Create & Invoke"],
            value="Preview DDL",
        ),
        StrInput(
            name="call_args",
            display_name="Invocation Arguments",
            value="'snowflake langflow'",
        ),
        BoolInput(
            name="return_as_json",
            display_name="Return as JSON Object",
            value=False,
        ),
    ]
    outputs = [
        Output(display_name="Scalar / JSON Result", name="result", method="run_function"),
        Output(display_name="Table DataFrame", name="dataframe", method="run_function_dataframe"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _func_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _func_schemas(db)
        if "function_selector" in build_config:
            build_config["function_selector"]["options"] = _func_options()
        return build_config

    def _effective_func_name(self) -> str:
        sel = (self.function_selector or "").strip()
        if sel and sel != CREATE_REPLACE_FUNC_OPTION:
            return sel.split("(")[0].strip().split(".")[-1]
        return (self.function_name or "NORMALIZE_TEXT_UDF").strip()

    def build_ddl(self) -> str:
        return SnowflakeFunctionEngine.build_function_ddl(
            database=self.database or "SNOWFLAKE_LANGFLOW",
            schema=self.schema_name or "CORE",
            function_name=self._effective_func_name(),
            function_kind=self.function_kind or "Scalar UDF",
            language=self.language or "SQL",
            signature_args=self.signature_args or "x VARCHAR",
            return_type=self.return_type or "VARCHAR",
            stage_imports=getattr(self, "stage_imports", "") or "",
            body=self.body or "",
        )

    def build_invocation_sql(self) -> str:
        return SnowflakeFunctionEngine.build_invocation_sql(
            database=self.database or "SNOWFLAKE_LANGFLOW",
            schema=self.schema_name or "CORE",
            function_name=self._effective_func_name(),
            function_kind=self.function_kind or "Scalar UDF",
            call_args=self.call_args or "'sample'",
            return_as_json=bool(getattr(self, "return_as_json", False)),
        )

    def run_function(self) -> Message:
        db = (self.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = (self.schema_name or "CORE").strip()
        nm = self._effective_func_name()
        ddl = self.build_ddl()
        inv_sql = self.build_invocation_sql()
        mode = (self.action_mode or "Preview DDL").strip()

        if mode == "Preview DDL":
            return Message(
                text=json.dumps(
                    {
                        "action_mode": mode,
                        "function_fqn": f"{db}.{sch}.{nm}",
                        "function_kind": self.function_kind,
                        "active_role": LIVE_BRIDGE.active_role,
                        "ddl_preview": ddl,
                        "invocation_sql": inv_sql,
                    },
                    indent=2,
                )
            )

        if mode == "Create & Invoke":
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_FUNCTION", f"{db}.{sch}.{nm}")
            if denied:
                raise ValueError(denied["message"])
            LIVE_BRIDGE.execute_sql(ddl)

        res = LIVE_BRIDGE.execute_sql(inv_sql)
        return Message(
            text=json.dumps(
                {
                    "action_mode": mode,
                    "function_fqn": f"{db}.{sch}.{nm}",
                    "function_kind": self.function_kind,
                    "active_role": LIVE_BRIDGE.active_role,
                    "invocation_sql": inv_sql,
                    "result": res,
                },
                indent=2,
            )
        )

    def run_function_dataframe(self) -> DataFrame:
        inv_sql = self.build_invocation_sql()
        res = LIVE_BRIDGE.execute_sql(inv_sql)
        return DataFrame(res.get("rows", []))
