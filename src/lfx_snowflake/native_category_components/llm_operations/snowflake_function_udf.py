from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import DataFrame
from lfx.schema.message import Message

from lfx_snowflake.components.llm_operations import SnowflakeFunctionEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import (
    _ddl_succeeded,
    _register_created_object,
    _resolve_flow_id,
)


CREATE_REPLACE_FUNC_OPTION = "+ Create / Replace Function..."
DEFAULT_SQL_FUNC_BODY = "UPPER(TRIM(x))"
DEFAULT_PYTHON_FUNC_BODY = "def compute(x):\n    return (x or '').strip().upper()"


def _func_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _func_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _func_options(
    database: str | None = None,
    schema: str | None = None,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    funcs = list(LIVE_BRIDGE.get_context_objects(database, schema, "functions", flow_id=eff_fid))
    if CREATE_REPLACE_FUNC_OPTION not in funcs:
        funcs.insert(0, CREATE_REPLACE_FUNC_OPTION)
    return funcs


class SnowflakeFunctionComponent(Component):
    display_name = "Function (UDF / UDTF)"
    description = (
        "Author, preview, register, and invoke Snowflake Scalar UDFs and Tabular UDTFs "
        "(SQL or Python 3.12 with optional Stage IMPORTS) in <Database>.<Schema> with dynamic code editor visibility."
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
            name="action_mode",
            display_name="Action Mode",
            options=["Preview DDL", "Execute Existing", "Invoke Existing", "CREATE OR REPLACE", "Create & Invoke"],
            value="Preview DDL",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="function_selector",
            display_name="Function in Schema",
            options=_func_options("SNOWFLAKE_LANGFLOW", "CORE"),
            value=CREATE_REPLACE_FUNC_OPTION,
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="function_name",
            display_name="Function Name",
            value="NORMALIZE_TEXT_UDF",
            show=True,
        ),
        DropdownInput(
            name="function_kind",
            display_name="Function Kind",
            options=["Scalar UDF", "Tabular UDTF"],
            value="Scalar UDF",
            real_time_refresh=True,
            show=True,
        ),
        DropdownInput(
            name="language",
            display_name="Language",
            options=["SQL", "PYTHON"],
            value="SQL",
            real_time_refresh=True,
            show=True,
        ),
        StrInput(
            name="signature_args",
            display_name="Signature Arguments",
            value="x VARCHAR",
            show=True,
        ),
        MultilineInput(
            name="body",
            display_name="Function Code Body (SQL expression or Python def compute(...):)",
            value=DEFAULT_SQL_FUNC_BODY,
            real_time_refresh=False,
            advanced=False,
            show=True,
        ),
        StrInput(
            name="return_type",
            display_name="Return Type (VARCHAR, VARIANT, or TABLE(...))",
            value="VARCHAR",
            show=True,
        ),
        StrInput(
            name="stage_imports",
            display_name="Stage IMPORTS (optional CSV, e.g. @DB.SCHEMA.STAGE/helper.py)",
            value="",
            advanced=False,
            show=True,
        ),
        StrInput(
            name="call_args",
            display_name="Invocation Arguments",
            value="'snowflake langflow'",
            show=True,
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
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _func_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        )
        schemas = _func_schemas(db, flow_id=flow_id)
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
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else getattr(self, "schema_name", "CORE"))
        )
        if "function_selector" in build_config:
            build_config["function_selector"]["options"] = _func_options(db, sch, flow_id=flow_id)

        mode = (
            field_value
            if field_name == "action_mode" and isinstance(field_value, str)
            else build_config.get("action_mode", {}).get("value") or getattr(self, "action_mode", "Preview DDL")
        ).strip()
        fn_sel = (
            field_value
            if field_name == "function_selector" and isinstance(field_value, str)
            else build_config.get("function_selector", {}).get("value") or getattr(self, "function_selector", CREATE_REPLACE_FUNC_OPTION)
        ).strip()
        lang = (
            field_value
            if field_name == "language" and isinstance(field_value, str)
            else build_config.get("language", {}).get("value") or getattr(self, "language", "SQL")
        ).strip().upper()

        is_existing_call = mode in ("Execute Existing", "Invoke Existing") or (
            bool(fn_sel) and fn_sel != CREATE_REPLACE_FUNC_OPTION and mode not in ("CREATE OR REPLACE", "Create & Invoke", "Preview DDL")
        )
        if field_name == "function_selector" and fn_sel != CREATE_REPLACE_FUNC_OPTION:
            is_existing_call = True

        authoring_fields = (
            "function_name",
            "function_kind",
            "language",
            "signature_args",
            "return_type",
            "stage_imports",
            "body",
        )
        for fname in authoring_fields:
            if fname in build_config:
                build_config[fname]["show"] = not is_existing_call
        if "call_args" in build_config:
            build_config["call_args"]["show"] = True

        if field_name == "language" and "body" in build_config:
            cur_body = (build_config["body"].get("value") or "").strip()
            if lang == "PYTHON" and (not cur_body or cur_body == DEFAULT_SQL_FUNC_BODY):
                build_config["body"]["value"] = DEFAULT_PYTHON_FUNC_BODY
            elif lang == "SQL" and (not cur_body or cur_body == DEFAULT_PYTHON_FUNC_BODY or "def compute(" in cur_body):
                build_config["body"]["value"] = DEFAULT_SQL_FUNC_BODY

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

        if mode in ("Create & Invoke", "CREATE OR REPLACE"):
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_FUNCTION", f"{db}.{sch}.{nm}")
            if denied:
                raise ValueError(denied["message"])
            ddl_res = LIVE_BRIDGE.execute_sql(ddl)
            if _ddl_succeeded(ddl_res):
                _register_created_object(
                    database=db,
                    schema=sch,
                    category="functions",
                    object_name=nm,
                    flow_id=_resolve_flow_id(self),
                )

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
