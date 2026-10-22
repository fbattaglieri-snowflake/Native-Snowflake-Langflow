from __future__ import annotations

import json
import uuid
from typing import Any

from lfx.custom import Component
from lfx.io import DataInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message
from lfx_snowflake.components.llm_operations import StoredProcedureEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_REPLACE_PROC_OPTION = "+ Create / Replace Procedure..."


def _proc_databases() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _proc_schemas(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


def _proc_options() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    procs = list(vis.get("procedures") or [])
    if CREATE_REPLACE_PROC_OPTION not in procs:
        procs.insert(0, CREATE_REPLACE_PROC_OPTION)
    return procs


class SnowflakeStoredProcedureComponent(Component):
    display_name = "Snowflake Stored Procedure"
    description = (
        "Author, preview, register, and invoke Snowflake Stored Procedures (SQL BEGIN...END or Python 3.12 Snowpark) "
        "in <Database>.<Schema> with EXECUTE AS CALLER default and graphical Child Procedure chaining."
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
            name="procedure_selector",
            display_name="Procedure in Schema",
            options=_proc_options(),
            value=CREATE_REPLACE_PROC_OPTION,
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="procedure_name",
            display_name="Procedure Name",
            value="ORCHESTRATE_WORKFLOW_STEP",
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
            value="",
        ),
        DropdownInput(
            name="return_type",
            display_name="Return Type",
            options=["VARIANT", "VARCHAR", "TABLE(status VARCHAR, details VARIANT)"],
            value="VARIANT",
            combobox=True,
        ),
        DropdownInput(
            name="execute_as",
            display_name="Execution Rights",
            options=["EXECUTE AS CALLER", "EXECUTE AS OWNER"],
            value="EXECUTE AS CALLER",
        ),
        MultilineInput(
            name="body",
            display_name="Procedure Body (SQL statements or Python def run(session):)",
            value="RETURN OBJECT_CONSTRUCT('status', 'ok', 'role', CURRENT_ROLE());",
        ),
        DropdownInput(
            name="action_mode",
            display_name="Action Mode",
            options=["Preview DDL", "Execute Existing", "Create & Execute"],
            value="Preview DDL",
        ),
        StrInput(
            name="call_args",
            display_name="Call Arguments (for Execute)",
            value="",
            advanced=True,
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
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _proc_schemas(db)
        if "procedure_selector" in build_config:
            build_config["procedure_selector"]["options"] = _proc_options()
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

        if mode == "Create & Execute":
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
                    "ddl": ddl if mode == "Create & Execute" else None,
                    "call_sql": call_sql,
                    "result": exec_res,
                },
                indent=2,
            )
        )
