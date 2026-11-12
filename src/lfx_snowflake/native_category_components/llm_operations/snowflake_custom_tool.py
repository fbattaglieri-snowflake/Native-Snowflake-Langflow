from __future__ import annotations

from lfx_snowflake import sql_safety as _sql_safety

import json
import re
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, HandleInput, IntInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.llm_operations import (
    CUSTOM_TOOL_SORT_OPTIONS,
    CUSTOM_TOOL_WAREHOUSE_MODES,
    SUPPORTED_CUSTOM_TOOL_RESOURCE_TYPES,
    SnowflakeCustomToolEngine,
)
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.providers.language_models import (
    build_structured_tool,
)
from lfx_snowflake.flow_context_guard import flow_component_sql as _flow_component_sql
from lfx_snowflake.flow_context_guard import raise_if_flow_context_denied as _raise_if_flow_context_denied


def _flow_component_execute_sql(*args, **kwargs):
    """BUG-CTX-03: this component's SQL is checked against the flow's `[ ❄️ Context ]`."""
    with _flow_component_sql():
        # E2E T1: a guard refusal surfaces as a component error carrying AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT.
        return _raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(*args, **kwargs))


# BUG-LLMOP-005: no hard-coded routine; the identifier comes from the selected object.
DEFAULT_CUSTOM_TOOL_IDENTIFIER = ""
DEFAULT_PARAMETERS_JSON = "[]"

_JSON_TO_PY: dict[str, Any] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


def build_args_schema(tool_name: str, parameters: list[dict[str, Any]]) -> Any:
    """BUG-LLMOP-013: pydantic model of the routine's real parameters for StructuredTool."""
    from pydantic import Field, create_model

    fields: dict[str, Any] = {}
    for p in parameters:
        name = str(p.get("name") or "").strip()  # Snowflake's parameter name, case preserved
        if not name.isidentifier() or name.startswith("_") or name.lower() in {f.lower() for f in fields}:
            raise ValueError(f"Parameter name '{name}' cannot be exposed as a tool argument.")
        py_t = _JSON_TO_PY.get(str(p.get("type") or "string").lower(), str)
        desc = str(p.get("description") or p.get("sql_type") or "")
        if p.get("required", True):
            fields[name] = (py_t, Field(..., description=desc))
        else:
            fields[name] = (py_t | None, Field(None, description=desc))
    model_name = re.sub(r"\W", "_", tool_name or "custom_tool") + "_args"
    return create_model(model_name, **fields)


def _custom_tool_db_schemas(flow_id: str | None = None) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    options: list[str] = ["Database All"]
    dbs = list(LIVE_BRIDGE.get_context_databases(flow_id=eff_fid))
    for db in dbs:
        for sch in LIVE_BRIDGE.get_context_schemas(db, flow_id=eff_fid):
            pair = f"{db}.{sch}"
            if pair not in options:
                options.append(pair)
    if "SNOWFLAKE_LANGFLOW.CORE" not in options:
        options.append("SNOWFLAKE_LANGFLOW.CORE")
    return options


def _custom_tool_warehouses(flow_id: str | None = None) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    fn = getattr(LIVE_BRIDGE, "get_context_warehouses", None) or getattr(LIVE_BRIDGE, "list_warehouses", None)
    if callable(fn):
        res = fn(flow_id=eff_fid) if "flow_id" in getattr(fn, "__code__", ()).co_varnames else fn()
        if isinstance(res, list) and res:
            return [str(w) for w in res]
    ctx = (
        LIVE_BRIDGE.get_flow_context(eff_fid)
        if eff_fid and hasattr(LIVE_BRIDGE, "get_flow_context")
        else getattr(LIVE_BRIDGE, "selected_context", None)
    )
    if isinstance(ctx, dict):
        whs = ctx.get("warehouses") or []
        if isinstance(whs, list) and whs:
            return [str(w) for w in whs]
    default_wh = getattr(LIVE_BRIDGE, "warehouse", None) or "COMPUTE_WH"
    return [str(default_wh), "COMPUTE_WH"] if str(default_wh) != "COMPUTE_WH" else ["COMPUTE_WH"]


def _custom_tool_identifiers(
    resource_type: str = "procedure",
    database_schema: str = "Database All",
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    res_type = (resource_type or "procedure").strip().lower()
    db_sch = (database_schema or "Database All").strip()
    bridge_fn = getattr(LIVE_BRIDGE, "list_custom_tool_identifiers", None)
    if callable(bridge_fn):
        items = bridge_fn(
            resource_type=res_type,
            database_schema=db_sch,
            flow_id=eff_fid,
        )
        if isinstance(items, list) and items:
            return [str(x) for x in items]

    target_pairs: list[tuple[str, str]] = []
    if db_sch and db_sch != "Database All" and "." in db_sch:
        db_part, sch_part = db_sch.split(".", 1)
        target_pairs.append((db_part.strip(), sch_part.strip()))
    else:
        for pair in _custom_tool_db_schemas(flow_id=eff_fid):
            if pair != "Database All" and "." in pair:
                d_p, s_p = pair.split(".", 1)
                target_pairs.append((d_p.strip(), s_p.strip()))

    category = "procedures" if res_type == "procedure" else "functions"
    identifiers: list[str] = []
    for db, sch in target_pairs:
        for obj in LIVE_BRIDGE.get_context_objects(db, sch, category, flow_id=eff_fid):
            obj_s = str(obj).strip()
            if not obj_s:
                continue
            if res_type == "table_function" and "UDTF" not in obj_s.upper() and "TABLE" not in obj_s.upper():
                continue
            if res_type == "function" and "UDTF" in obj_s.upper():
                continue
            fqn = obj_s if "." in obj_s.split("(")[0] else f"{db}.{sch}.{obj_s}"
            if "(" not in fqn:
                fqn = f"{fqn}(VARCHAR)"
            if fqn not in identifiers:
                identifiers.append(fqn)

    return identifiers


def _describe_custom_tool_params(
    resource_type: str,
    identifier: str,
    database_schema: str = "Database All",
    flow_id: str | None = None,
) -> list[dict[str, Any]]:
    eff_fid = flow_id or _resolve_flow_id()
    bridge_fn = getattr(LIVE_BRIDGE, "describe_custom_tool_signature", None)
    if callable(bridge_fn):
        desc = bridge_fn(
            resource_type=resource_type,
            identifier=identifier,
            database_schema=database_schema,
            flow_id=eff_fid,
        )
        if isinstance(desc, dict) and isinstance(desc.get("parameters"), list):
            return list(desc["parameters"])
        if isinstance(desc, list):
            return list(desc)
    return SnowflakeCustomToolEngine.parse_signature_parameters(identifier)


class SnowflakeCustomToolComponent(Component):
    display_name = "Custom Tool"
    description = (
        "Unified Snowflake Custom Tool mirroring Snowsight 'Add custom tool' 1:1 for Stored Procedures "
        "('procedure'), Scalar Functions ('function'), and Tabular Functions ('table_function' / UDTF) "
        "with signature auto-discovery, parameter schema sorting/editing, Warehouse & Query Timeout "
        "governance, and Cortex Agent generic ToolSpec + ToolResource output."
    )
    icon = "Snowflake"
    name = "SnowflakeCustomTool"

    inputs = [
        HandleInput(
            name="snowflake_context",
            display_name="Snowflake Context (Optional Override)",
            input_types=["Data"],
            required=False,
            advanced=True,
        ),
        DropdownInput(
            name="resource_type",
            display_name="Resource type",
            options=list(SUPPORTED_CUSTOM_TOOL_RESOURCE_TYPES),
            value="procedure",
            real_time_refresh=True,
            info="Select 'procedure' (Stored Procedure), 'function' (Scalar UDF), or 'table_function' (Tabular UDTF).",
        ),
        DropdownInput(
            name="database_schema",
            display_name="Database & Schema",
            options=_custom_tool_db_schemas(),
            value="Database All",
            real_time_refresh=True,
            combobox=True,
            info="Filter custom tools by <DATABASE>.<SCHEMA> from the active [ ❄️ Context ] or 'Database All'.",
        ),
        DropdownInput(
            name="custom_tool_identifier",
            display_name="Custom tool identifier",
            options=_custom_tool_identifiers("procedure", "Database All"),
            value=DEFAULT_CUSTOM_TOOL_IDENTIFIER,
            real_time_refresh=True,
            combobox=True,
            info="Select the existing Snowflake procedure, scalar function, or table function with its signature.",
        ),
        StrInput(
            name="tool_name",
            display_name="Name",
            value="send_email_all",
            info="Enter tool name (1–64 characters, matching ^[A-Za-z0-9_-]{1,64}$).",
        ),
        DropdownInput(
            name="warehouse_mode",
            display_name="Warehouse",
            options=list(CUSTOM_TOOL_WAREHOUSE_MODES),
            value="User's default",
            real_time_refresh=True,
            info="Warehouse to execute the SQL query ('User's default' or 'Custom').",
        ),
        DropdownInput(
            name="custom_warehouse",
            display_name="Select Warehouse",
            options=_custom_tool_warehouses(),
            value="COMPUTE_WH",
            show=False,
            combobox=True,
            info="Warehouse used when Warehouse is set to 'Custom'.",
        ),
        IntInput(
            name="query_timeout",
            display_name="Query timeout",
            value=60,
            info="Maximum time in seconds for query execution (1..600s, max 600s).",
        ),
        MultilineInput(
            name="tool_description",
            display_name="Description",
            value="Send an email notification using the governed Snowflake procedure.",
            info="Will be used during orchestration to guide how the agent uses this tool.",
        ),
        DropdownInput(
            name="parameters_sort_by",
            display_name="Sort by:",
            options=list(CUSTOM_TOOL_SORT_OPTIONS),
            value="Latest added",
            real_time_refresh=True,
            info="Configure the parameters your agent will use when triggering this procedure / function / table_function.",
        ),
        MultilineInput(
            name="parameters_json",
            display_name="Parameters (Auto-Discovered & Editable Cards JSON)",
            value=DEFAULT_PARAMETERS_JSON,
            real_time_refresh=True,
            info=(
                "Configure parameters ({name, sql_type, type, description, required}). "
                "Supports string, number, integer, boolean, array (object is unsupported by Snowflake Cortex)."
            ),
        ),
        StrInput(
            name="invocation_arguments",
            display_name="Invocation Arguments JSON (for Direct Execution)",
            value='{"body": "Deployment succeeded", "recipient_email": "team@example.com", "subject": "Status"}',
            advanced=False,
        ),
    ]

    outputs = [
        Output(
            display_name="Custom Tool",
            name="tool",
            method="build_tool",
        ),
        Output(
            display_name="Execution Result",
            name="result",
            method="run_custom_tool",
        ),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        res_type = (
            field_value
            if field_name == "resource_type" and isinstance(field_value, str)
            else build_config.get("resource_type", {}).get("value") or getattr(self, "resource_type", "procedure")
        ).strip()
        db_sch = (
            field_value
            if field_name == "database_schema" and isinstance(field_value, str)
            else build_config.get("database_schema", {}).get("value") or getattr(self, "database_schema", "Database All")
        ).strip()

        if "database_schema" in build_config:
            build_config["database_schema"]["options"] = _custom_tool_db_schemas(flow_id=flow_id)
            if field_name == "database_schema" and isinstance(field_value, str):
                build_config["database_schema"]["value"] = field_value

        id_options = _custom_tool_identifiers(res_type, db_sch, flow_id=flow_id)
        if "custom_tool_identifier" in build_config:
            build_config["custom_tool_identifier"]["options"] = id_options
            cur_id = build_config["custom_tool_identifier"].get("value")
            if field_name == "custom_tool_identifier" and isinstance(field_value, str):
                build_config["custom_tool_identifier"]["value"] = field_value
            elif id_options and (field_name in ("resource_type", "database_schema") or cur_id not in id_options):
                build_config["custom_tool_identifier"]["value"] = id_options[0]

        selected_id = (
            field_value
            if field_name == "custom_tool_identifier" and isinstance(field_value, str)
            else build_config.get("custom_tool_identifier", {}).get("value")
            or getattr(self, "custom_tool_identifier", DEFAULT_CUSTOM_TOOL_IDENTIFIER)
        ).strip()

        if field_name in ("resource_type", "database_schema", "custom_tool_identifier"):
            derived_name = SnowflakeCustomToolEngine.derive_tool_name(selected_id)
            if "tool_name" in build_config:
                build_config["tool_name"]["value"] = derived_name
            discovered_params = _describe_custom_tool_params(
                res_type, selected_id, database_schema=db_sch, flow_id=flow_id
            )
            sort_mode = (
                build_config.get("parameters_sort_by", {}).get("value")
                or getattr(self, "parameters_sort_by", "Latest added")
            )
            sorted_params = SnowflakeCustomToolEngine.sort_parameters(discovered_params, sort_by=sort_mode)
            if "parameters_json" in build_config:
                build_config["parameters_json"]["value"] = json.dumps(sorted_params, indent=2)

        if field_name == "parameters_sort_by" and "parameters_json" in build_config:
            raw_json = build_config["parameters_json"].get("value") or "[]"
            parsed_list = json.loads(raw_json) if isinstance(raw_json, str) and raw_json.strip() else []
            resorted = SnowflakeCustomToolEngine.sort_parameters(parsed_list, sort_by=str(field_value))
            build_config["parameters_json"]["value"] = json.dumps(resorted, indent=2)

        wh_mode = (
            field_value
            if field_name == "warehouse_mode" and isinstance(field_value, str)
            else build_config.get("warehouse_mode", {}).get("value")
            or getattr(self, "warehouse_mode", "User's default")
        ).strip()
        if "custom_warehouse" in build_config:
            build_config["custom_warehouse"]["options"] = _custom_tool_warehouses(flow_id=flow_id)
            build_config["custom_warehouse"]["show"] = wh_mode == "Custom"

        if field_name == "query_timeout" and "query_timeout" in build_config:
            raw_t = int(field_value) if field_value is not None and str(field_value).strip() else 60
            if raw_t > 600:
                build_config["query_timeout"]["value"] = 600
            elif raw_t < 1:
                build_config["query_timeout"]["value"] = 1

        if "parameters_sort_by" in build_config:
            build_config["parameters_sort_by"]["info"] = (
                f"Configure the parameters your agent will use when triggering this {res_type}"
            )

        return build_config

    def _parsed_parameters(self) -> list[dict[str, Any]]:
        raw = getattr(self, "parameters_json", None)
        # BUG-LLMOP-005: the default "[]" (or blank) means "not configured" -> DESCRIBE the selected routine.
        if isinstance(raw, str) and raw.strip() in ("", "[]"):
            raw = None
        if isinstance(raw, list) and not raw:
            raw = None
        if isinstance(raw, list):
            return SnowflakeCustomToolEngine.sort_parameters(
                raw, sort_by=getattr(self, "parameters_sort_by", "Latest added")
            )
        if isinstance(raw, str) and raw.strip():
            loaded = json.loads(raw)
            if isinstance(loaded, list):
                return SnowflakeCustomToolEngine.sort_parameters(
                    loaded, sort_by=getattr(self, "parameters_sort_by", "Latest added")
                )
        ident = (getattr(self, "custom_tool_identifier", "") or "").strip()
        if not ident:
            return []
        res_t = getattr(self, "resource_type", "procedure") or "procedure"
        db_sch = getattr(self, "database_schema", "Database All") or "Database All"
        params = _describe_custom_tool_params(res_t, ident, database_schema=db_sch, flow_id=_resolve_flow_id(self))
        return SnowflakeCustomToolEngine.sort_parameters(
            params, sort_by=getattr(self, "parameters_sort_by", "Latest added")
        )

    def add_parameter(
        self,
        name: str,
        param_type: str = "string",
        description: str = "",
        required: bool = True,
        sql_type: str | None = None,
    ) -> list[dict[str, Any]]:
        updated = SnowflakeCustomToolEngine.add_parameter(
            self._parsed_parameters(),
            name=name,
            param_type=param_type,
            description=description,
            required=required,
            sql_type=sql_type,
        )
        self.parameters_json = json.dumps(updated, indent=2)
        return updated

    def delete_parameter(self, name_or_index: str | int) -> list[dict[str, Any]]:
        updated = SnowflakeCustomToolEngine.delete_parameter(self._parsed_parameters(), name_or_index)
        self.parameters_json = json.dumps(updated, indent=2)
        return updated

    def build_tool_payload(self) -> dict[str, Any]:
        db_sch = (getattr(self, "database_schema", "Database All") or "Database All").strip()
        default_pair = db_sch if db_sch != "Database All" and "." in db_sch else "SNOWFLAKE_LANGFLOW.CORE"
        ident = (
            getattr(self, "custom_tool_identifier", DEFAULT_CUSTOM_TOOL_IDENTIFIER) or DEFAULT_CUSTOM_TOOL_IDENTIFIER
        ).strip()
        return SnowflakeCustomToolEngine.build_cortex_agent_tool_payload(
            resource_type=getattr(self, "resource_type", "procedure") or "procedure",
            custom_tool_identifier=ident,
            tool_name=getattr(self, "tool_name", "") or "",
            tool_description=getattr(self, "tool_description", "") or "",
            warehouse_mode=getattr(self, "warehouse_mode", "User's default") or "User's default",
            custom_warehouse=getattr(self, "custom_warehouse", "") or "",
            query_timeout=getattr(self, "query_timeout", 60),
            parameters=self._parsed_parameters(),
            parameters_sort_by=getattr(self, "parameters_sort_by", "Latest added") or "Latest added",
            default_db_schema=default_pair,
        )

    def build_invocation_sql(self, arguments_override: Any = None) -> str:
        return self.build_invocation_request(arguments_override=arguments_override)[0]

    def build_invocation_request(self, arguments_override: Any = None) -> tuple[str, dict[str, Any] | None]:
        db_sch = (getattr(self, "database_schema", "Database All") or "Database All").strip()
        default_pair = db_sch if db_sch != "Database All" and "." in db_sch else "SNOWFLAKE_LANGFLOW.CORE"
        ident = (
            getattr(self, "custom_tool_identifier", DEFAULT_CUSTOM_TOOL_IDENTIFIER) or DEFAULT_CUSTOM_TOOL_IDENTIFIER
        ).strip()
        args = arguments_override if arguments_override is not None else getattr(self, "invocation_arguments", None)
        return SnowflakeCustomToolEngine.build_invocation_request(
            resource_type=getattr(self, "resource_type", "procedure") or "procedure",
            custom_tool_identifier=ident,
            arguments=args,
            parameters=self._parsed_parameters(),
            default_db_schema=default_pair,
        )

    def _execute_tool(self, arguments_override: Any = None) -> dict[str, Any]:
        payload = self.build_tool_payload()
        sql_stmt, sql_bindings = self.build_invocation_request(arguments_override=arguments_override)
        t_name = payload["tool_spec"]["name"]
        res_cfg = payload["tool_resources"][t_name]
        exec_env = res_cfg["execution_environment"]
        timeout_s = exec_env["query_timeout"]
        wh = exec_env.get("warehouse")
        if wh:
            wh = _sql_safety.validate_identifier(wh, "warehouse")
        timeout_s = _sql_safety.bounded_int(timeout_s, 60, 1, 600)

        # BUG-LLMOP-007: every SQL API request is its own session, so warehouse and timeout travel in the
        # /api/v2/statements body (the bridge polls the statement handle when timeout > 60s).
        exec_res = _flow_component_execute_sql(sql_stmt, bindings=sql_bindings, timeout=timeout_s, warehouse=wh or None)

        return {
            "tool_name": t_name,
            "resource_type": res_cfg["type"],
            "identifier": res_cfg["identifier"],
            "execution_environment": exec_env,
            "request_overrides": {"warehouse": wh or None, "timeout": timeout_s},
            "invocation_sql": sql_stmt,
            "invocation_bindings": sql_bindings,
            "tool_spec": payload["tool_spec"],
            "tool_resources": payload["tool_resources"],
            "active_role": LIVE_BRIDGE.active_role,
            "result": exec_res,
        }

    def _execute_tool_or_raise(self, arguments_override: Any = None) -> dict[str, Any]:
        """Tool Mode: a failed routine call is a tool error for the Agent, never a normal tool result."""
        res = self._execute_tool(arguments_override=arguments_override)
        outcome = res.get("result")
        if isinstance(outcome, dict) and outcome.get("ok") is False:
            code = outcome.get("error_code") or "CUSTOM_TOOL_ERROR"
            raise ValueError(f"{code}: {str(outcome.get('message') or 'routine call failed')[:500]}")
        return res

    def build_tool(self) -> Data:
        payload = self.build_tool_payload()
        t_name = payload["tool_spec"]["name"]
        t_desc = payload["tool_spec"]["description"]
        data_obj = Data(data=payload)
        setattr(data_obj, "name", t_name)
        setattr(data_obj, "description", t_desc)
        setattr(data_obj, "tool_spec", payload["tool_spec"])
        setattr(data_obj, "tool_resources", payload["tool_resources"])
        setattr(data_obj, "metadata", payload)
        setattr(data_obj, "invoke", lambda tool_input=None, **kw: self._execute_tool(tool_input or kw))
        setattr(data_obj, "run", getattr(data_obj, "invoke"))
        return data_obj

    def to_toolkit(self) -> list[Any]:
        payload = self.build_tool_payload()
        t_name = payload["tool_spec"]["name"]
        t_desc = payload["tool_spec"]["description"]

        return [
            build_structured_tool(
                name=t_name,
                description=t_desc,
                func=lambda **kwargs: self._execute_tool_or_raise(
                    arguments_override={k: v for k, v in kwargs.items() if v is not None}
                ),
                args_schema=build_args_schema(t_name, self._parsed_parameters()),
                metadata=payload,
            )
        ]

    def run_custom_tool(self) -> Message:
        res = self._execute_tool()
        return Message(text=json.dumps(res, indent=2))
