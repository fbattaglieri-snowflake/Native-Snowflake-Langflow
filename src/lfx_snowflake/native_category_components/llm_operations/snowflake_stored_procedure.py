from __future__ import annotations

import json
import uuid
from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import DataInput, DropdownInput, MultilineInput, Output, StrInput
    from lfx.schema import Data
    from lfx.schema.message import Message
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import (
        Component,
        Data,
        DataInput,
        DropdownInput,
        Message,
        MultilineInput,
        Output,
        StrInput,
    )

from lfx_snowflake.components.llm_operations import StoredProcedureEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_REPLACE_PROC_OPTION = "+ Create / Replace Procedure..."
DEFAULT_SQL_PROC_BODY = "BEGIN\n  RETURN OBJECT_CONSTRUCT('status', 'ok', 'role', CURRENT_ROLE());\nEND;"
DEFAULT_PYTHON_PROC_BODY = (
    "def run(session):\n"
    "    row = session.sql(\"SELECT CURRENT_ROLE() AS role\").collect()[0]\n"
    "    return {\"status\": \"ok\", \"role\": row[\"ROLE\"]}"
)


def _proc_databases() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _proc_schemas(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


def _proc_options(database: str | None = None, schema: str | None = None) -> list[str]:
    procs = list(LIVE_BRIDGE.get_context_objects(database, schema, "procedures"))
    if CREATE_REPLACE_PROC_OPTION not in procs:
        procs.insert(0, CREATE_REPLACE_PROC_OPTION)
    return procs


class SnowflakeStoredProcedureComponent(Component):
    display_name = "Stored Procedure"
    description = (
        "Author, preview, register, and invoke Snowflake Stored Procedures (SQL BEGIN...END or Python 3.12 Snowpark) "
        "in <Database>.<Schema> with dynamic code editor visibility, EXECUTE AS CALLER default, and graphical Child Procedure chaining."
    )
    icon = "Snowflake"
    name = "SnowflakeStoredProcedure"

    inputs = [
        DropdownInput(
            name="database",
            display_name="Database",
            options=_proc_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_proc_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="action_mode",
            display_name="Action Mode",
            options=["Preview DDL", "CALL Existing", "Execute Existing", "CREATE OR REPLACE", "Create & Execute"],
            value="Preview DDL",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="procedure_selector",
            display_name="Procedure in Schema",
            options=_proc_options("SNOWFLAKE_LANGFLOW", "CORE"),
            value=CREATE_REPLACE_PROC_OPTION,
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="procedure_name",
            display_name="Procedure Name",
            value="ORCHESTRATE_WORKFLOW_STEP",
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
            value="",
            show=True,
        ),
        MultilineInput(
            name="body",
            display_name="Procedure Code Body (SQL BEGIN...END; or Python def run(session, ...):)",
            value=DEFAULT_SQL_PROC_BODY,
            real_time_refresh=False,
            advanced=False,
            show=True,
        ),
        DropdownInput(
            name="return_type",
            display_name="Return Type",
            options=["VARIANT", "VARCHAR", "TABLE(status VARCHAR, details VARIANT)"],
            value="VARIANT",
            combobox=True,
            show=True,
        ),
        DropdownInput(
            name="execute_as",
            display_name="Execution Rights",
            options=["EXECUTE AS CALLER", "EXECUTE AS OWNER"],
            value="EXECUTE AS CALLER",
            show=True,
        ),
        StrInput(
            name="stage_imports",
            display_name="Stage IMPORTS (optional CSV for Python)",
            value="",
            advanced=False,
            show=True,
        ),
        StrInput(
            name="call_args",
            display_name="Call Arguments (for CALL / Execute)",
            value="",
            advanced=False,
            show=True,
        ),
        DataInput(
            name="child_procedures",
            display_name="Child Procedures (Graphical Chain)",
            is_list=True,
        ),
    ]
    outputs = [
        Output(
            display_name="Execution Result",
            name="result",
            method="run_procedure",
        ),
        Output(
            display_name="Procedure Reference (To Parent)",
            name="proc_ref",
            method="as_child_reference",
        ),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _proc_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _proc_schemas(db)
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or getattr(self, "schema_name", "CORE")
        )
        if "procedure_selector" in build_config:
            build_config["procedure_selector"]["options"] = _proc_options(db, sch)

        mode = (
            field_value
            if field_name == "action_mode" and isinstance(field_value, str)
            else build_config.get("action_mode", {}).get("value") or getattr(self, "action_mode", "Preview DDL")
        ).strip()
        proc_sel = (
            field_value
            if field_name == "procedure_selector" and isinstance(field_value, str)
            else build_config.get("procedure_selector", {}).get("value") or getattr(self, "procedure_selector", CREATE_REPLACE_PROC_OPTION)
        ).strip()
        lang = (
            field_value
            if field_name == "language" and isinstance(field_value, str)
            else build_config.get("language", {}).get("value") or getattr(self, "language", "SQL")
        ).strip().upper()

        is_existing_call = mode in ("CALL Existing", "Execute Existing") or (
            bool(proc_sel) and proc_sel != CREATE_REPLACE_PROC_OPTION and mode not in ("CREATE OR REPLACE", "Create & Execute", "Preview DDL")
        )
        if field_name == "procedure_selector" and proc_sel != CREATE_REPLACE_PROC_OPTION:
            is_existing_call = True

        authoring_fields = (
            "procedure_name",
            "language",
            "signature_args",
            "return_type",
            "execute_as",
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
            if lang == "PYTHON" and (not cur_body or cur_body == DEFAULT_SQL_PROC_BODY or "RETURN OBJECT_CONSTRUCT" in cur_body):
                build_config["body"]["value"] = DEFAULT_PYTHON_PROC_BODY
            elif lang == "SQL" and (not cur_body or cur_body == DEFAULT_PYTHON_PROC_BODY or "def run(" in cur_body):
                build_config["body"]["value"] = DEFAULT_SQL_PROC_BODY

        return build_config

    def _effective_proc_name(self) -> str:
        sel = (self.procedure_selector or "").strip()
        if sel and sel != CREATE_REPLACE_PROC_OPTION:
            return sel.split("(")[0].strip().split(".")[-1]
        return (self.procedure_name or "ORCHESTRATE_WORKFLOW_STEP").strip()

    def build_ddl(self) -> str:
        return StoredProcedureEngine.build_procedure_ddl(
            database=self.database or "SNOWFLAKE_LANGFLOW",
            schema=self.schema_name or "CORE",
            procedure_name=self._effective_proc_name(),
            signature_args=self.signature_args or "",
            return_type=self.return_type or "VARIANT",
            language=self.language or "SQL",
            execute_as=self.execute_as or "EXECUTE AS CALLER",
            body=self.body or "",
            child_procedures=getattr(self, "child_procedures", None),
        )

    def build_call_sql(self) -> str:
        return StoredProcedureEngine.build_call_sql(
            database=self.database or "SNOWFLAKE_LANGFLOW",
            schema=self.schema_name or "CORE",
            procedure_name=self._effective_proc_name(),
            call_args=getattr(self, "call_args", "") or "",
        )

    def as_child_reference(self) -> Data:
        db = (self.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = (self.schema_name or "CORE").strip()
        nm = self._effective_proc_name()
        fqn = f"{db}.{sch}.{nm}"
        return Data(
            data={
                "procedure_fqn": fqn,
                "database": db,
                "schema_name": sch,
                "procedure_name": nm,
                "language": self.language,
                "execute_as": self.execute_as,
                "call_sql": self.build_call_sql(),
                "ddl_preview": self.build_ddl(),
                "span_id": f"sp-child-{uuid.uuid4().hex[:10]}",
            }
        )

    def run_procedure(self) -> Message:
        db = (self.database or "SNOWFLAKE_LANGFLOW").strip()
        sch = (self.schema_name or "CORE").strip()
        nm = self._effective_proc_name()
        ddl = self.build_ddl()
        call_sql = self.build_call_sql()
        children = StoredProcedureEngine.extract_child_calls(getattr(self, "child_procedures", None))
        mode = (self.action_mode or "Preview DDL").strip()

        if mode == "Preview DDL":
            return Message(
                text=json.dumps(
                    {
                        "action_mode": mode,
                        "procedure_fqn": f"{db}.{sch}.{nm}",
                        "active_role": LIVE_BRIDGE.active_role,
                        "child_procedures": [c.get("procedure_fqn") for c in children],
                        "ddl_preview": ddl,
                        "call_sql": call_sql,
                    },
                    indent=2,
                )
            )

        if mode in ("Create & Execute", "CREATE OR REPLACE"):
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_PROCEDURE", f"{db}.{sch}.{nm}")
            if denied:
                raise ValueError(denied["message"])
            LIVE_BRIDGE.execute_sql(ddl)

        exec_res = LIVE_BRIDGE.execute_sql(call_sql)
        return Message(
            text=json.dumps(
                {
                    "action_mode": mode,
                    "procedure_fqn": f"{db}.{sch}.{nm}",
                    "active_role": LIVE_BRIDGE.active_role,
                    "child_procedures": [c.get("procedure_fqn") for c in children],
                    "ddl": ddl if mode in ("Create & Execute", "CREATE OR REPLACE") else None,
                    "call_sql": call_sql,
                    "result": exec_res,
                },
                indent=2,
            )
        )
