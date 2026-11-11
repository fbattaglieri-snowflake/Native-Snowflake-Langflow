from __future__ import annotations

from lfx_snowflake import sql_safety as _sql_safety

import ast
import json
import re
from typing import Any, Callable

from ..providers.ollama_provider import SnowflakeOllamaAdapter
from ..providers.proxy_chat import SnowflakeProxyChatAdapter
from ..providers.proxy_messages import SnowflakeProxyMessagesAdapter
from ..providers.proxy_responses import SnowflakeProxyResponsesAdapter
from ..providers.registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
)


SUPPORTED_LLM_OPERATION_PROVIDERS: tuple[str, ...] = (
    SnowflakeProviderRegistry.PROVIDER_RESPONSES,
    SnowflakeProviderRegistry.PROVIDER_MESSAGES,
    SnowflakeProviderRegistry.PROVIDER_CHAT,
    SnowflakeProviderRegistry.PROVIDER_OLLAMA,
)

# LLM Selector is explicitly preserved 100% unchanged per specification
LLM_SELECTOR_INVARIANT_SPEC: dict[str, Any] = {
    "component": "LLM Selector",
    "category": "LLM Operations",
    "modified": False,
    "status": "invariant",
}



def _resolve_caller_role(explicit: str | None) -> str:
    """Explicit caller role, else the resolved per-session bridge role; never a hard-coded admin role."""
    from lfx_snowflake import identity as _sf_identity  # noqa: PLC0415

    bridge_role = None
    if not explicit:
        try:
            from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE  # noqa: PLC0415

            bridge_role = LIVE_BRIDGE.active_role
        except Exception:  # noqa: BLE001
            bridge_role = None
    return _sf_identity.resolve_reported_role(explicit, bridge_role)


class _BaseSnowflakeLLMOperation:
    """Shared base for LLM Operations backed by the 3 Standard Proxy protocols + Snowflake-hosted Ollama."""

    def __init__(self, registry: SnowflakeProviderRegistry) -> None:
        self.registry = registry
        self.chat_adapter = SnowflakeProxyChatAdapter(registry)
        self.responses_adapter = SnowflakeProxyResponsesAdapter(registry)
        self.messages_adapter = SnowflakeProxyMessagesAdapter(registry)
        self.ollama_adapter = SnowflakeOllamaAdapter(registry)

    def validate_provider_and_model(self, provider: str, model: str) -> str:
        self.registry.check_role_authorized("llm_operations_invoke", "USAGE ON SERVICE LLM_PROXY_STANDARD")
        if provider == SnowflakeProviderRegistry.PROVIDER_CHAT or provider == "chat":
            self.registry.get_model_profile("chat", model)
            return "chat"
        if provider == SnowflakeProviderRegistry.PROVIDER_RESPONSES or provider == "responses":
            self.registry.get_model_profile("responses", model)
            return "responses"
        if provider == SnowflakeProviderRegistry.PROVIDER_MESSAGES or provider == "messages":
            self.registry.get_model_profile("messages", model)
            return "messages"
        if provider == SnowflakeProviderRegistry.PROVIDER_OLLAMA or provider == "ollama":
            self.ollama_adapter.get_generative_model(model)
            return "ollama"
        raise SnowflakeProviderError(
            ERROR_UNSUPPORTED_CAPABILITY,
            f"Unsupported provider '{provider}' in LLM Operations. "
            f"Supported Snowflake providers: {SUPPORTED_LLM_OPERATION_PROVIDERS}",
            active_role=self.registry.active_role,
            operation="validate_provider_and_model",
        )


class BatchRunComponent(_BaseSnowflakeLLMOperation):
    """
    `LLM Operations -> Batch Run` extended with `Snowflake Proxy - Responses`,
    `Snowflake Proxy - Messages`, `Snowflake Proxy - Chat`, and `Ollama (Snowflake Hosted)`.
    Preserves `Instructions`, `Table`, `Column Name`, and `LLM Results` output port.
    Enforces explicit concurrency/volume caps, 1:1 row correlation, and per-row partial error capture
    with zero hidden retries.
    """

    display_name = "Batch Run"
    category = "LLM Operations"
    ports = ("Instructions", "Table", "Column Name", "LLM Results")

    def __init__(
        self,
        registry: SnowflakeProviderRegistry,
        *,
        max_concurrency: int = 8,
        max_batch_rows: int = 1000,
    ) -> None:
        super().__init__(registry)
        self.max_concurrency = max(1, min(int(max_concurrency), 64))
        self.max_batch_rows = max(1, min(int(max_batch_rows), 10000))

    def run_batch(
        self,
        *,
        provider: str,
        model: str,
        instructions: str,
        table: list[dict[str, Any]],
        column_name: str,
        row_evaluator: Callable[[int, str, str], str] | None = None,
    ) -> dict[str, Any]:
        protocol = self.validate_provider_and_model(provider, model)
        if not column_name or not column_name.strip():
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Column Name is required for Batch Run.",
                active_role=self.registry.active_role,
                operation="batch_run",
            )
        if len(table) > self.max_batch_rows:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Batch volume ({len(table)} rows) exceeds maximum configured limit ({self.max_batch_rows}).",
                active_role=self.registry.active_role,
                operation="batch_run",
            )

        results: list[dict[str, Any]] = []
        error_count = 0
        for row_idx, row in enumerate(table):
            if column_name not in row:
                error_count += 1
                results.append(
                    {
                        **row,
                        "row_index": row_idx,
                        "llm_result": None,
                        "status": "error",
                        "error_code": ERROR_UNSUPPORTED_CAPABILITY,
                        "error_message": f"Missing column '{column_name}' at row {row_idx}.",
                        "retries_attempted": 0,
                    }
                )
                continue

            cell_text = "" if row[column_name] is None else str(row[column_name])
            try:
                if row_evaluator is not None:
                    output_text = row_evaluator(row_idx, instructions, cell_text)
                else:
                    output_text = f"[{protocol}:{model}] {instructions}: {cell_text}"
                results.append(
                    {
                        **row,
                        "row_index": row_idx,
                        "llm_result": output_text,
                        "status": "ok",
                        "error_code": None,
                        "error_message": None,
                        "retries_attempted": 0,
                    }
                )
            except Exception as exc:
                # Zero hidden retries: record partial error on the specific row
                error_count += 1
                results.append(
                    {
                        **row,
                        "row_index": row_idx,
                        "llm_result": None,
                        "status": "error",
                        "error_code": ERROR_TECHNICAL_EXECUTION_ERROR,
                        "error_message": str(exc),
                        "retries_attempted": 0,
                    }
                )

        return {
            "LLM Results": results,
            "summary": {
                "provider": provider,
                "protocol": protocol,
                "model": model,
                "total_rows": len(table),
                "succeeded_rows": len(table) - error_count,
                "failed_rows": error_count,
                "max_concurrency": self.max_concurrency,
                "retries_attempted": 0,
            },
        }


class GuardrailsComponent(_BaseSnowflakeLLMOperation):
    """
    `LLM Operations -> Guardrails` extended with the 3 Proxy protocols + Snowflake-hosted Ollama.
    Preserves `Checking Method`, `Guardrails`, `Input Text` and outputs `Pass`, `Fail`, `Result Data`.
    Fail-closed on error: never forwards unverified text to `Pass` on evaluation failure.
    """

    display_name = "Guardrails"
    category = "LLM Operations"
    inputs = ("Checking Method", "Guardrails", "Input Text", "Language Model")
    outputs = ("Pass", "Fail", "Result Data")

    def evaluate(
        self,
        *,
        provider: str,
        model: str,
        checking_method: str,
        guardrails_rules: list[str] | str,
        input_text: str,
        evaluator_fn: Callable[[str, list[str]], bool] | None = None,
    ) -> dict[str, Any]:
        protocol = self.validate_provider_and_model(provider, model)
        rules_list = [guardrails_rules] if isinstance(guardrails_rules, str) else list(guardrails_rules)
        try:
            if evaluator_fn is not None:
                passed = bool(evaluator_fn(input_text, rules_list))
            else:
                lowered = input_text.lower()
                passed = not any(rule.lower() in lowered for rule in rules_list if rule)
        except Exception as exc:
            # Fail-closed: on any provider/runtime failure, never emit Pass
            return {
                "Pass": None,
                "Fail": input_text,
                "Result Data": {
                    "status": "error_fail_closed",
                    "passed": False,
                    "checking_method": checking_method,
                    "protocol": protocol,
                    "model": model,
                    "error_code": ERROR_TECHNICAL_EXECUTION_ERROR,
                    "error_message": f"Guardrail check failed closed: {exc}",
                },
            }

        return {
            "Pass": input_text if passed else None,
            "Fail": None if passed else input_text,
            "Result Data": {
                "status": "evaluated",
                "passed": passed,
                "checking_method": checking_method,
                "protocol": protocol,
                "model": model,
            },
        }


class SmartRouterComponent(_BaseSnowflakeLLMOperation):
    """
    `LLM Operations -> Smart Router` extended with the 3 Proxy protocols + Snowflake-hosted Ollama.
    Strictly validates in the backend that the route returned by the model belongs to the configured `Routes`.
    Forbids arbitrary routing on invalid model output.
    """

    display_name = "Smart Router"
    category = "LLM Operations"
    inputs = ("Input", "Routes", "Language Model")

    def route(
        self,
        *,
        provider: str,
        model: str,
        input_text: str,
        routes: list[str],
        model_selected_route: str | None = None,
    ) -> dict[str, Any]:
        protocol = self.validate_provider_and_model(provider, model)
        clean_routes = [r.strip() for r in routes if r and r.strip()]
        if not clean_routes:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Smart Router requires at least one configured route in 'Routes'.",
                active_role=self.registry.active_role,
                operation="smart_router",
            )

        chosen = model_selected_route if model_selected_route is not None else clean_routes[0]
        if chosen not in clean_routes:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Smart Router model returned invalid route '{chosen}' which is not in configured Routes {clean_routes}. "
                "Arbitrary routing on invalid output is forbidden.",
                active_role=self.registry.active_role,
                operation="smart_router",
            )

        port_outputs = {route_name: (input_text if route_name == chosen else None) for route_name in clean_routes}
        return {
            "selected_route": chosen,
            "protocol": protocol,
            "model": model,
            "outputs": port_outputs,
        }


FORBIDDEN_AST_NAMES = {
    "eval",
    "exec",
    "compile",
    "open",
    "__import__",
    "getattr",
    "setattr",
    "delattr",
    "globals",
    "locals",
    "vars",
    "input",
    "breakpoint",
}


class SmartTransformComponent(_BaseSnowflakeLLMOperation):
    """
    `LLM Operations -> Smart Transform` extended with the 3 Proxy protocols + Snowflake-hosted Ollama.
    Preserves `JSON`, `Instructions`, and `Output`.
    Executes generated transformation functions in an isolated sandbox without access to imports,
    files, credentials, SPCS tokens, environment variables, or network sockets.
    """

    display_name = "Smart Transform"
    category = "LLM Operations"
    ports = ("JSON", "Instructions", "Output")

    def transform(
        self,
        *,
        provider: str,
        model: str,
        json_input: dict[str, Any] | list[Any],
        instructions: str,
        generated_code: str | None = None,
    ) -> dict[str, Any]:
        protocol = self.validate_provider_and_model(provider, model)
        code_to_run = generated_code or "def transform(data):\n    return data\n"
        self._validate_isolated_ast(code_to_run)

        safe_builtins = {
            "abs": abs,
            "all": all,
            "any": any,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "filter": filter,
            "float": float,
            "int": int,
            "isinstance": isinstance,
            "len": len,
            "list": list,
            "map": map,
            "max": max,
            "min": min,
            "range": range,
            "round": round,
            "set": set,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
            "zip": zip,
        }
        sandbox_globals: dict[str, Any] = {"__builtins__": safe_builtins}
        sandbox_locals: dict[str, Any] = {}
        try:
            compiled = compile(code_to_run, "<smart_transform_sandbox>", "exec")
            exec(compiled, sandbox_globals, sandbox_locals)  # noqa: S102
            transform_fn = sandbox_locals.get("transform")
            if not callable(transform_fn):
                raise SnowflakeProviderError(
                    ERROR_TECHNICAL_EXECUTION_ERROR,
                    "Generated Smart Transform code must define a callable `transform(data)` function.",
                    active_role=self.registry.active_role,
                    operation="smart_transform",
                )
            transformed = transform_fn(json.loads(json.dumps(json_input)))
        except SnowflakeProviderError:
            raise
        except Exception as exc:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Isolated Smart Transform execution failed: {exc}",
                active_role=self.registry.active_role,
                operation="smart_transform",
            ) from exc

        return {
            "Output": transformed,
            "protocol": protocol,
            "model": model,
            "instructions": instructions,
        }

    def _validate_isolated_ast(self, code: str) -> None:
        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Syntax error in Smart Transform code: {exc}",
                active_role=self.registry.active_role,
                operation="smart_transform_ast_check",
            ) from exc

        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    "Imports are forbidden in isolated Smart Transform execution (no network/filesystem/credential access).",
                    active_role=self.registry.active_role,
                    operation="smart_transform_ast_check",
                )
            if isinstance(node, ast.Name) and (
                node.id in FORBIDDEN_AST_NAMES or node.id.startswith("__")
            ):
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Identifier '{node.id}' is forbidden in isolated Smart Transform execution.",
                    active_role=self.registry.active_role,
                    operation="smart_transform_ast_check",
                )
            if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Dunder attribute '{node.attr}' access is forbidden in isolated Smart Transform execution.",
                    active_role=self.registry.active_role,
                    operation="smart_transform_ast_check",
                )


class StructuredOutputComponent(_BaseSnowflakeLLMOperation):
    """
    `LLM Operations -> Structured Output` extended with the 3 Proxy protocols + Snowflake-hosted Ollama.
    Preserves `Input Message`, `Output Schema`, and `Structured Output` / `Model Response`.
    Strictly validates the JSON schema in the backend, distinguishes native profile schema enforcement
    from prompt-only JSON, and raises an explicit error on non-conforming output (zero silent repair/fallback).
    """

    display_name = "Structured Output"
    category = "LLM Operations"
    ports = ("Input Message", "Output Schema", "Structured Output", "Model Response")

    _TYPE_MAP: dict[str, type | tuple[type, ...]] = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "object": dict,
        "array": list,
    }

    def generate_structured(
        self,
        *,
        provider: str,
        model: str,
        input_message: str,
        output_schema: dict[str, Any],
        raw_model_output: str | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        protocol = self.validate_provider_and_model(provider, model)
        if protocol in ("chat", "responses", "messages"):
            profile = self.registry.get_model_profile(protocol, model)
            enforcement_mode = "native_schema" if profile.supports_native_structured_output else "prompt_validated"
        else:
            enforcement_mode = "native_format_schema"

        if raw_model_output is None:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                "Model returned empty response for Structured Output; no silent repair or fallback allowed.",
                active_role=self.registry.active_role,
                operation="structured_output",
            )

        if isinstance(raw_model_output, str):
            try:
                parsed = json.loads(raw_model_output)
            except json.JSONDecodeError as exc:
                raise SnowflakeProviderError(
                    ERROR_TECHNICAL_EXECUTION_ERROR,
                    f"Structured Output is not valid JSON ({exc}); silent JSON repair is forbidden.",
                    active_role=self.registry.active_role,
                    operation="structured_output",
                ) from exc
        elif isinstance(raw_model_output, dict):
            parsed = raw_model_output
        else:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Unexpected Structured Output type {type(raw_model_output).__name__}.",
                active_role=self.registry.active_role,
                operation="structured_output",
            )

        self._validate_against_schema(parsed, output_schema)
        return {
            "Structured Output": parsed,
            "Model Response": raw_model_output,
            "enforcement_mode": enforcement_mode,
            "protocol": protocol,
            "model": model,
            "input_message": input_message,
        }

    def _validate_against_schema(self, data: Any, schema: dict[str, Any]) -> None:
        if not isinstance(data, dict):
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                "Structured Output root must be a JSON object.",
                active_role=self.registry.active_role,
                operation="structured_output_schema_validation",
            )

        required_fields = schema.get("required", [])
        properties = schema.get("properties", {})
        additional_properties = schema.get("additionalProperties", True)

        for req_key in required_fields:
            if req_key not in data:
                raise SnowflakeProviderError(
                    ERROR_TECHNICAL_EXECUTION_ERROR,
                    f"Structured Output missing required property '{req_key}'.",
                    active_role=self.registry.active_role,
                    operation="structured_output_schema_validation",
                )

        if additional_properties is False:
            extra_keys = set(data.keys()) - set(properties.keys())
            if extra_keys:
                raise SnowflakeProviderError(
                    ERROR_TECHNICAL_EXECUTION_ERROR,
                    f"Structured Output contains undeclared properties: {sorted(extra_keys)}.",
                    active_role=self.registry.active_role,
                    operation="structured_output_schema_validation",
                )

        for prop_name, prop_schema in properties.items():
            if prop_name not in data:
                continue
            val = data[prop_name]
            expected_type_name = prop_schema.get("type")
            if expected_type_name in self._TYPE_MAP:
                expected_py_type = self._TYPE_MAP[expected_type_name]
                # In Python bool is a subclass of int; reject bool when integer/number is expected
                if expected_type_name in ("integer", "number") and isinstance(val, bool):
                    raise SnowflakeProviderError(
                        ERROR_TECHNICAL_EXECUTION_ERROR,
                        f"Property '{prop_name}' expected {expected_type_name}, got boolean.",
                        active_role=self.registry.active_role,
                        operation="structured_output_schema_validation",
                    )
                if not isinstance(val, expected_py_type):
                    raise SnowflakeProviderError(
                        ERROR_TECHNICAL_EXECUTION_ERROR,
                        f"Property '{prop_name}' expected {expected_type_name}, got {type(val).__name__}.",
                        active_role=self.registry.active_role,
                        operation="structured_output_schema_validation",
                    )


class StoredProcedureEngine:
    """
    Domain engine for `SnowflakeStoredProcedureComponent` (`LLM-W2-01`), supporting SQL (`BEGIN ... END`)
    and Python (`RUNTIME_VERSION = '3.12'`, `PACKAGES = ('snowflake-snowpark-python')`, `HANDLER = 'run'`),
    `EXECUTE AS CALLER` default, and graphical child procedure chaining (`CALL <DB>.<SCHEMA>.<CHILD_PROC>(...)`).
    """

    @staticmethod
    def extract_child_calls(child_procedures: list[Any] | None) -> list[dict[str, Any]]:
        refs: list[dict[str, Any]] = []
        for item in child_procedures or []:
            data = getattr(item, "data", item) if not isinstance(item, dict) else item
            if isinstance(data, dict) and data.get("procedure_fqn"):
                refs.append(data)
            elif isinstance(item, str) and item.strip():
                refs.append({"procedure_fqn": item.strip(), "call_sql": f"CALL {item.strip()}();"})
        return refs

    @classmethod
    def build_procedure_ddl(
        cls,
        *,
        database: str,
        schema: str,
        procedure_name: str,
        signature_args: str = "",
        return_type: str = "VARIANT",
        language: str = "SQL",
        execute_as: str = "EXECUTE AS CALLER",
        body: str = "",
        child_procedures: list[Any] | None = None,
    ) -> str:
        fqn = f"{database.strip()}.{schema.strip()}.{procedure_name.strip()}"
        sig = (signature_args or "").strip()
        ret = (return_type or "VARIANT").strip()
        lang = (language or "SQL").strip().upper()
        exec_clause = "EXECUTE AS OWNER" if "OWNER" in (execute_as or "").upper() else "EXECUTE AS CALLER"
        children = cls.extract_child_calls(child_procedures)

        if lang == "PYTHON":
            child_py = "\n".join(
                f"    session.sql(\"CALL {c['procedure_fqn']}()\").collect()" for c in children
            )
            default_py = (
                "def run(session):\n"
                + (f"{child_py}\n" if child_py else "")
                + "    return {'status': 'ok', 'procedure': '"
                + fqn
                + "'}"
            )
            src = (body or "").strip() or default_py
            return (
                f"CREATE OR REPLACE PROCEDURE {fqn}({sig})\n"
                f"RETURNS {ret}\n"
                f"LANGUAGE PYTHON\n"
                f"RUNTIME_VERSION = '3.12'\n"
                f"PACKAGES = ('snowflake-snowpark-python')\n"
                f"HANDLER = 'run'\n"
                f"{exec_clause}\n"
                f"AS $$\n{src}\n$$;"
            )

        child_sql = "\n".join(f"  CALL {c['procedure_fqn']}();" for c in children)
        user_body = (body or "").strip() or f"RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', '{fqn}');"
        combined_sql = f"{child_sql}\n  {user_body}".strip() if child_sql else f"  {user_body}"
        return (
            f"CREATE OR REPLACE PROCEDURE {fqn}({sig})\n"
            f"RETURNS {ret}\n"
            f"LANGUAGE SQL\n"
            f"{exec_clause}\n"
            f"AS\n$$\nBEGIN\n{combined_sql}\nEND;\n$$;"
        )

    @staticmethod
    def build_call_sql(
        *,
        database: str,
        schema: str,
        procedure_name: str,
        call_args: str = "",
    ) -> str:
        fqn = (
            procedure_name.strip()
            if "." in procedure_name
            else f"{database.strip()}.{schema.strip()}.{procedure_name.strip()}"
        )
        return f"CALL {fqn}({(call_args or '').strip()});"


class SnowflakeFunctionEngine:
    """
    Domain engine for `SnowflakeFunctionComponent` (`LLM-W2-01`) supporting Scalar `UDF` and Tabular `UDTF`
    in `SQL` and `PYTHON` with optional `IMPORTS = ('@db.schema.stage/file.py')`.
    """

    @staticmethod
    def build_function_ddl(
        *,
        database: str,
        schema: str,
        function_name: str,
        function_kind: str = "Scalar UDF",
        language: str = "SQL",
        signature_args: str = "x VARCHAR",
        return_type: str = "VARCHAR",
        stage_imports: str = "",
        body: str = "",
    ) -> str:
        fqn = f"{database.strip()}.{schema.strip()}.{function_name.strip()}"
        sig = (signature_args or "").strip()
        is_udtf = "UDTF" in (function_kind or "").upper() or "TABULAR" in (function_kind or "").upper()
        ret = (return_type or ("TABLE(val VARCHAR)" if is_udtf else "VARCHAR")).strip()
        if is_udtf and not ret.upper().startswith("TABLE"):
            ret = f"TABLE({ret})"
        lang = (language or "SQL").strip().upper()
        imports_list = [i.strip() for i in (stage_imports or "").split(",") if i.strip()]
        imports_clause = (
            f"IMPORTS = ({', '.join(repr(i) for i in imports_list)})\n" if imports_list else ""
        )

        if lang == "PYTHON":
            handler_name = "Handler" if is_udtf else "compute"
            default_body = (
                "class Handler:\n    def process(self, x):\n        yield (x,)"
                if is_udtf
                else "def compute(x):\n    return x"
            )
            src = (body or "").strip() or default_body
            return (
                f"CREATE OR REPLACE FUNCTION {fqn}({sig})\n"
                f"RETURNS {ret}\n"
                f"LANGUAGE PYTHON\n"
                f"RUNTIME_VERSION = '3.12'\n"
                f"{imports_clause}"
                f"HANDLER = '{handler_name}'\n"
                f"AS $$\n{src}\n$$;"
            )

        sql_expr = (body or "").strip() or ("SELECT x" if is_udtf else "UPPER(x)")
        return (
            f"CREATE OR REPLACE FUNCTION {fqn}({sig})\n"
            f"RETURNS {ret}\n"
            f"LANGUAGE SQL\n"
            f"AS $$\n{sql_expr}\n$$;"
        )

    @staticmethod
    def build_invocation_sql(
        *,
        database: str,
        schema: str,
        function_name: str,
        function_kind: str = "Scalar UDF",
        call_args: str = "'sample'",
        return_as_json: bool = False,
    ) -> str:
        fqn = (
            function_name.strip()
            if "." in function_name
            else f"{database.strip()}.{schema.strip()}.{function_name.strip()}"
        )
        args = (call_args or "").strip()
        is_udtf = "UDTF" in (function_kind or "").upper() or "TABULAR" in (function_kind or "").upper()
        if is_udtf:
            if return_as_json:
                return f"SELECT OBJECT_CONSTRUCT(*) AS RESULT FROM TABLE({fqn}({args}))"
            return f"SELECT * FROM TABLE({fqn}({args}))"
        if return_as_json:
            return f"SELECT OBJECT_CONSTRUCT('function', '{fqn}', 'result', {fqn}({args})) AS RESULT"
        return f"SELECT {fqn}({args}) AS RESULT"


class SnowflakeSQLStatementEngine:
    """Domain engine for `SnowflakeSQLStatementComponent` (`LLM-W2-03`)."""

    @staticmethod
    def _trim_statement(sql: str) -> tuple[str, str]:
        """``(body, first_keyword)``: ``sql`` without leading/trailing comments and trailing ``;``.

        Strings, quoted identifiers, ``$$`` bodies and ``--`` / ``//`` / ``/* */`` comments are skipped the
        same way as the PASSO 3 lexer (``flow_context_guard._lex``, which also validates the statement); a
        ``;`` followed by more SQL is a second statement and is rejected."""
        from lfx_snowflake.flow_context_guard import _lex  # noqa: PLC0415 - fail closed on unterminated input

        _lex(sql)
        i, n = 0, len(sql)
        start = end = -1
        semis: list[int] = []
        first = ""
        while i < n:
            ch = sql[i]
            if ch.isspace():
                i += 1
            elif sql.startswith(("--", "//"), i):
                j = sql.find("\n", i)
                i = n if j == -1 else j + 1
            elif sql.startswith("/*", i):
                i = sql.find("*/", i + 2) + 2
            elif ch == ";":
                semis.append(i)
                i += 1
            else:
                if sql.startswith("$$", i):
                    j = sql.find("$$", i + 2) + 2
                elif ch in ("'", '"'):
                    j = i + 1
                    while j < n:
                        if ch == "'" and sql[j] == "\\":
                            j += 2
                            continue
                        if sql[j] == ch:
                            if j + 1 < n and sql[j + 1] == ch:
                                j += 2
                                continue
                            break
                        j += 1
                    j += 1
                else:
                    m = re.match(r"[A-Za-z_][A-Za-z0-9_$]*", sql[i:])
                    j = i + (m.end() if m else 1)
                    if m and not first:
                        first = m.group(0).upper()
                if start < 0:
                    start = i
                if semis and semis[-1] >= end:
                    raise ValueError("SQL Statement must contain a single statement.")
                end = j
                i = j
        if start < 0:
            raise ValueError("SQL Statement is empty.")
        return sql[start:end], first

    @staticmethod
    def build_sql(
        *,
        database: str,
        schema: str,
        sql_statement: str,
        output_format: str = "DataFrame",
        max_rows: int = 100,
    ) -> str:
        stmt = (sql_statement or "SELECT CURRENT_ROLE() AS ACTIVE_ROLE, CURRENT_DATABASE() AS DB").strip()
        stmt = stmt.replace("<DB>", database.strip()).replace("<SCHEMA>", schema.strip())
        body, first_word = SnowflakeSQLStatementEngine._trim_statement(stmt)
        # BUG-LLMOP-011: every query (SELECT / WITH / parenthesised, any case or whitespace) is wrapped so
        # an inner LIMIT, a subquery or a trailing comment cannot lift the row cap.
        n = _sql_safety.bounded_int(max_rows, 100, 1, 1_000_000)
        stmt = body
        if first_word in ("SELECT", "WITH"):
            if output_format == "JSON Object (OBJECT_CONSTRUCT)":
                return f"SELECT OBJECT_CONSTRUCT(*) AS JSON_ROW FROM (\n{stmt}\n) LIMIT {n}"
            return f"SELECT * FROM (\n{stmt}\n) LIMIT {n}"
        return stmt


class SnowflakeCortexSearchQueryEngine:
    """Domain engine for `SnowflakeCortexSearchComponent` (`LLM-W2-04`) invoking `SNOWFLAKE.CORTEX.SEARCH_PREVIEW`."""

    @staticmethod
    def build_search_preview_sql(
        *,
        database: str,
        schema: str,
        search_service: str,
        query: str,
        columns_csv: str = "",
        filter_json: str = "",
        limit: int = 5,
        scoring_config_json: str = "",
    ) -> tuple[str, dict[str, Any]]:
        svc_fqn = (
            _sql_safety.validate_fqn(search_service, "search_service", min_parts=3, max_parts=3)
            if "." in (search_service or "")
            else _sql_safety.join_fqn(database, schema, search_service, field_name="search_service")
        )
        cols = [c.strip() for c in (columns_csv or "chunk").split(",") if c.strip()]
        payload: dict[str, Any] = {
            "query": query or "",
            "columns": cols or ["chunk"],
            "limit": max(1, int(limit or 5)),
        }
        if (filter_json or "").strip():
            payload["filter"] = json.loads(filter_json)
        if (scoring_config_json or "").strip():
            payload["scoring_config"] = json.loads(scoring_config_json)
        # Validated name + JSON-escaped payload; callers that can bind should use build_search_preview_request.
        escaped_json = json.dumps(payload).replace("\\", "\\\\").replace("'", "''")
        sql = f"SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('{svc_fqn}', '{escaped_json}') AS SEARCH_RESULT"
        return sql, payload

    @classmethod
    def build_search_preview_request(cls, **kwargs: Any) -> tuple[str, dict[str, Any], dict[str, Any]]:
        """``(sql, payload, bindings)``: ``SEARCH_PREVIEW(?, ?)`` with service name and JSON bound (BUG-UI-SQLI-04)."""
        _, payload = cls.build_search_preview_sql(**kwargs)
        search_service = kwargs.get("search_service") or ""
        svc_fqn = (
            _sql_safety.validate_fqn(search_service, "search_service", min_parts=3, max_parts=3)
            if "." in search_service
            else _sql_safety.join_fqn(kwargs.get("database"), kwargs.get("schema"), search_service, field_name="search_service")
        )
        sql = "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(?, ?) AS SEARCH_RESULT"
        return sql, payload, _sql_safety.to_sql_bindings([("TEXT", svc_fqn), ("TEXT", json.dumps(payload))])


SUPPORTED_CUSTOM_TOOL_RESOURCE_TYPES: tuple[str, ...] = (
    "procedure",
    "function",
    "table_function",
)

CUSTOM_TOOL_SORT_OPTIONS: tuple[str, ...] = (
    "Latest added",
    "Alphabetical",
    "Required first",
)

SUPPORTED_CUSTOM_TOOL_PARAM_TYPES: tuple[str, ...] = (
    "string",
    "number",
    "integer",
    "boolean",
    "array",
)

CUSTOM_TOOL_WAREHOUSE_MODES: tuple[str, ...] = (
    "User's default",
    "Custom",
)

SUPPORTED_DECISION_MODELS: tuple[str, ...] = (
    "snowflake-decision-fast",
    "snowflake-decision",
)

SUPPORTED_DECISION_QUESTION_TYPES: tuple[str, ...] = (
    "choice",
    "score",
    "noul",
)

DECISION_EVALUATION_MODES: tuple[str, ...] = (
    "Single State (Text / JSON Input)",
    "Table-Level Processing (Database.Schema.Table)",
)


class SnowflakeCustomToolEngine:
    """
    Domain engine for `SnowflakeCustomToolComponent` (`W2-R9-01` / `LLM-OP-01`),
    mirroring 1:1 Snowsight 'Add custom tool' for Snowflake Cortex Agents and Langflow Agents.
    """

    RESOURCE_TYPES: tuple[str, ...] = SUPPORTED_CUSTOM_TOOL_RESOURCE_TYPES
    SORT_OPTIONS: tuple[str, ...] = CUSTOM_TOOL_SORT_OPTIONS
    SUPPORTED_PARAM_TYPES: tuple[str, ...] = SUPPORTED_CUSTOM_TOOL_PARAM_TYPES
    WAREHOUSE_MODES: tuple[str, ...] = CUSTOM_TOOL_WAREHOUSE_MODES
    TOOL_NAME_PATTERN: str = r"^[A-Za-z0-9_-]{1,64}$"

    @classmethod
    def map_sql_type_to_json_schema_type(cls, sql_type: str) -> str:
        raw = (sql_type or "VARCHAR").strip().upper()
        base = raw.split("(")[0].strip()
        if base == "OBJECT" or raw.startswith("OBJECT"):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Snowflake Cortex does not support stored procedures and custom tools with a parameter of type object.",
                active_role="CURRENT_ROLE",
                operation="custom_tool_parameter_validation",
            )
        if base in ("ARRAY",) or raw.startswith("ARRAY"):
            return "array"
        if base in ("BOOLEAN", "BOOL"):
            return "boolean"
        if base in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
            return "integer"
        if base in ("NUMBER", "NUMERIC", "DECIMAL", "DEC"):
            if "(" in raw and ")" in raw:
                inner = raw[raw.find("(") + 1 : raw.rfind(")")]
                parts = [p.strip() for p in inner.split(",") if p.strip()]
                if len(parts) >= 2:
                    try:
                        scale = int(parts[1])
                    except ValueError:
                        scale = 0
                    return "number" if scale > 0 else "integer"
            return "integer"
        if base in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"):
            return "number"
        return "string"

    @classmethod
    def validate_param_type(cls, param_type: str) -> str:
        norm = (param_type or "string").strip().lower()
        if norm == "object":
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Snowflake Cortex does not support stored procedures and custom tools with a parameter of type object.",
                active_role="CURRENT_ROLE",
                operation="custom_tool_parameter_validation",
            )
        if norm not in cls.SUPPORTED_PARAM_TYPES:
            return cls.map_sql_type_to_json_schema_type(param_type)
        return norm

    @classmethod
    def derive_tool_name(cls, identifier: str) -> str:
        import re

        head = (identifier or "").strip().split("(")[0].strip()
        short_name = head.split(".")[-1].strip().strip('"') if head else "custom_tool"
        snake = re.sub(r"[^A-Za-z0-9_-]+", "_", short_name).strip("_").lower()
        if not snake:
            snake = "custom_tool"
        return snake[:64]

    @classmethod
    def validate_tool_name(cls, tool_name: str, fallback_identifier: str = "") -> str:
        import re

        candidate = (tool_name or "").strip()
        if not candidate and fallback_identifier:
            candidate = cls.derive_tool_name(fallback_identifier)
        if not candidate or len(candidate) > 64 or not re.match(cls.TOOL_NAME_PATTERN, candidate):
            raise ValueError(
                f"Invalid custom tool name '{tool_name}': must be 1..64 characters matching {cls.TOOL_NAME_PATTERN}."
            )
        return candidate

    @staticmethod
    def validate_query_timeout(query_timeout: int | float | str | None) -> int:
        if query_timeout is None or query_timeout == "":
            return 60
        val = int(query_timeout)
        if val < 1 or val > 600:
            raise ValueError(f"query_timeout must be between 1 and 600 seconds (got {val}).")
        return val

    @staticmethod
    def extract_bare_identifier(identifier: str, default_db_schema: str = "SNOWFLAKE_LANGFLOW.CORE") -> str:
        """Validated ``DB.SCH.ROUTINE`` (each part a plain identifier); raises ``ValueError`` otherwise."""
        raw = (identifier or "").strip()
        head = raw.split("(")[0].strip()
        if "(" in raw:
            # Anything after the name must be a well-formed type signature (BUG-UI-SQLI-02).
            _sql_safety.validate_routine_signature(raw[raw.index("(") :])
        if not head:
            head = "SEND_EMAIL_ALL"
        parts = head.split(".")
        if len(parts) >= 3:
            return _sql_safety.validate_fqn(head, "custom_tool_identifier", min_parts=3, max_parts=3)
        name = _sql_safety.validate_identifier(parts[-1], "custom_tool_identifier")
        if default_db_schema and default_db_schema != "Database All" and "." in default_db_schema:
            prefix = _sql_safety.validate_fqn(default_db_schema, "database_schema", min_parts=2, max_parts=2)
            return f"{prefix}.{name}"
        return f"SNOWFLAKE_LANGFLOW.CORE.{name}"

    @staticmethod
    def extract_signature_types(identifier: str) -> str:
        """Allowlist-validated ``(TYPE, ...)`` part of an identifier, ``"()"`` when absent."""
        raw = (identifier or "").strip()
        if "(" not in raw:
            return "()"
        return _sql_safety.validate_routine_signature(raw[raw.index("(") :]) or "()"

    @classmethod
    def _split_top_level_commas(cls, text: str) -> list[str]:
        parts: list[str] = []
        depth = 0
        buf: list[str] = []
        for ch in text:
            if ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth = max(0, depth - 1)
                buf.append(ch)
            elif ch == "," and depth == 0:
                token = "".join(buf).strip()
                if token:
                    parts.append(token)
                buf = []
            else:
                buf.append(ch)
        tail = "".join(buf).strip()
        if tail:
            parts.append(tail)
        return parts

    @classmethod
    def parse_signature_parameters(
        cls,
        identifier_or_signature: str,
        describe_rows: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        sig_str = (identifier_or_signature or "").strip()
        if describe_rows:
            for row in describe_rows:
                if not isinstance(row, dict):
                    continue
                prop = str(row.get("property") or row.get("PROPERTY") or "").strip().lower()
                if prop == "signature":
                    val = str(row.get("value") or row.get("VALUE") or "").strip()
                    if val:
                        sig_str = val
                        break

        obj_head = (identifier_or_signature or "").split("(")[0].strip().split(".")[-1].upper()
        if "(" in sig_str and ")" in sig_str:
            inner = sig_str[sig_str.find("(") + 1 : sig_str.rfind(")")].strip()
        else:
            inner = ""

        if not inner:
            return []

        raw_tokens = cls._split_top_level_commas(inner)
        params: list[dict[str, Any]] = []
        for idx, token in enumerate(raw_tokens):
            clean = token.strip()
            has_default = " DEFAULT " in clean.upper()
            if has_default:
                upper_idx = clean.upper().find(" DEFAULT ")
                clean = clean[:upper_idx].strip()
            pieces = clean.split(None, 1)
            if len(pieces) == 2 and not pieces[0].upper().startswith(
                ("VARCHAR", "NUMBER", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "INT", "BOOLEAN", "ARRAY", "OBJECT", "TEXT", "STRING", "VARIANT", "TIMESTAMP", "DATE", "TIME")
            ):
                p_name = pieces[0].strip().strip('"')  # Snowflake's own case (DESCRIBE reports upper)
                sql_t = pieces[1].strip().upper()
                named = True
            else:
                # BUG-LLMOP-005: no invented names; unnamed (types-only) signatures stay positional.
                sql_t = clean.upper()
                p_name = f"arg_{idx + 1}"
                named = False

            json_t = cls.map_sql_type_to_json_schema_type(sql_t)
            params.append(
                {
                    "name": p_name,
                    "sql_type": sql_t,
                    "type": json_t,
                    "description": "",
                    "required": not has_default,
                    "named": named,
                }
            )
        return params

    @classmethod
    def sort_parameters(
        cls,
        parameters: list[dict[str, Any]],
        sort_by: str = "Latest added",
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in parameters or []:
            if not isinstance(item, dict):
                continue
            p_name = str(item.get("name") or item.get("parameter") or "").strip()
            if not p_name:
                continue
            raw_type = str(item.get("type") or item.get("sql_type") or "string").strip()
            sql_t = str(item.get("sql_type") or raw_type.upper()).strip()
            if sql_t.upper().startswith("OBJECT"):
                cls.map_sql_type_to_json_schema_type(sql_t)
            json_t = cls.validate_param_type(raw_type)
            normalized.append(
                {
                    "name": p_name,
                    "sql_type": sql_t,
                    "type": json_t,
                    "description": str(item.get("description") or ""),
                    "required": bool(item.get("required", True)),
                    # BUG-LLMOP-006: keep whether the name is the routine's real argument name.
                    "named": bool(item.get("named", not re.fullmatch(r"arg_\d+", p_name, re.I))),
                }
            )
        mode = (sort_by or "Latest added").strip()
        if mode == "Alphabetical":
            return sorted(normalized, key=lambda x: x["name"].lower())
        if mode == "Required first":
            return [
                x
                for _, x in sorted(
                    enumerate(normalized),
                    key=lambda pair: (not bool(pair[1].get("required", True)), pair[0]),
                )
            ]
        return normalized

    @classmethod
    def add_parameter(
        cls,
        parameters: list[dict[str, Any]],
        *,
        name: str,
        param_type: str = "string",
        description: str = "",
        required: bool = True,
        sql_type: str | None = None,
    ) -> list[dict[str, Any]]:
        json_t = cls.validate_param_type(param_type)
        out = list(parameters or [])
        out.append(
            {
                "name": (name or f"param_{len(out) + 1}").strip(),
                "sql_type": (sql_type or json_t.upper()).strip(),
                "type": json_t,
                "description": description or "",
                "required": bool(required),
                "named": True,
            }
        )
        return out

    @classmethod
    def delete_parameter(
        cls,
        parameters: list[dict[str, Any]],
        name_or_index: str | int,
    ) -> list[dict[str, Any]]:
        items = list(parameters or [])
        if isinstance(name_or_index, int):
            return [p for i, p in enumerate(items) if i != name_or_index]
        target = str(name_or_index).strip().lower()
        return [p for p in items if str(p.get("name", "")).strip().lower() != target]

    @classmethod
    def build_cortex_agent_tool_payload(
        cls,
        *,
        resource_type: str = "procedure",
        custom_tool_identifier: str = "SNOWFLAKE_LANGFLOW.CORE.SEND_EMAIL_ALL(VARCHAR, VARCHAR, VARCHAR)",
        tool_name: str = "",
        tool_description: str = "",
        warehouse_mode: str = "User's default",
        custom_warehouse: str = "",
        query_timeout: int = 60,
        parameters: list[dict[str, Any]] | None = None,
        parameters_sort_by: str = "Latest added",
        default_db_schema: str = "SNOWFLAKE_LANGFLOW.CORE",
    ) -> dict[str, Any]:
        res_type = (resource_type or "procedure").strip().lower()
        if res_type not in cls.RESOURCE_TYPES:
            raise ValueError(f"Unsupported custom tool resource_type '{resource_type}'. Must be one of {cls.RESOURCE_TYPES}.")
        valid_name = cls.validate_tool_name(tool_name, fallback_identifier=custom_tool_identifier)
        valid_timeout = cls.validate_query_timeout(query_timeout)
        bare_fqn = cls.extract_bare_identifier(custom_tool_identifier, default_db_schema=default_db_schema)

        raw_params = parameters if parameters is not None else cls.parse_signature_parameters(custom_tool_identifier)
        sorted_params = cls.sort_parameters(raw_params, sort_by=parameters_sort_by)

        properties: dict[str, dict[str, Any]] = {}
        required_names: list[str] = []
        for p in sorted_params:
            p_name = p["name"]
            properties[p_name] = {
                "type": p["type"],
                "description": p.get("description", ""),
            }
            if p.get("required", True):
                required_names.append(p_name)

        exec_env: dict[str, Any] = {
            "type": "warehouse",
            "query_timeout": valid_timeout,
        }
        if (warehouse_mode or "User's default").strip() == "Custom" and (custom_warehouse or "").strip():
            exec_env["warehouse"] = custom_warehouse.strip()

        return {
            "tool_spec": {
                "type": "generic",
                "name": valid_name,
                "description": (tool_description or f"Execute Snowflake {res_type} {bare_fqn}").strip(),
                "input_schema": {
                    "type": "object",
                    "properties": properties,
                    "required": required_names,
                },
            },
            "tool_resources": {
                valid_name: {
                    "type": res_type,
                    "identifier": bare_fqn,
                    "execution_environment": exec_env,
                }
            },
            "parameters": sorted_params,
            "identifier_with_signature": custom_tool_identifier,
            "warehouse_mode": (warehouse_mode or "User's default").strip(),
        }

    @classmethod
    def _format_sql_literal(cls, value: Any, param_type: str = "string") -> str:
        if value is None:
            return "NULL"
        ptype = (param_type or "string").lower()
        if ptype == "boolean" or isinstance(value, bool):
            if isinstance(value, str):
                return "TRUE" if value.strip().lower() in ("true", "1", "yes") else "FALSE"
            return "TRUE" if bool(value) else "FALSE"
        if ptype in ("integer", "number") and isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        if ptype == "array" or isinstance(value, (list, tuple)):
            arr_json = json.dumps(list(value) if isinstance(value, (list, tuple)) else value).replace("\\", "\\\\").replace("'", "''")
            return f"PARSE_JSON('{arr_json}')"
        escaped = str(value).replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped}'"

    @classmethod
    def _bind_argument(cls, value: Any, param_type: str, binder: "_sql_safety.BindingBuilder") -> str:
        """Placeholder expression for one argument; the value itself only travels as a binding."""
        if value is None:
            return "NULL"
        ptype = (param_type or "string").lower()
        if ptype == "boolean" or isinstance(value, bool):
            if isinstance(value, str):
                value = value.strip().lower() in ("true", "1", "yes")
            return binder.add(bool(value), "BOOLEAN")
        if ptype in ("integer", "number") and isinstance(value, (int, float)) and not isinstance(value, bool):
            return binder.add(value, "FIXED" if isinstance(value, int) else "REAL")
        if ptype in ("array", "object") or isinstance(value, (list, tuple, dict)):
            return f"PARSE_JSON({binder.add(json.dumps(list(value) if isinstance(value, tuple) else value), 'TEXT')})"
        return binder.add(str(value), "TEXT")

    @classmethod
    def build_invocation_request(
        cls,
        *,
        resource_type: str = "procedure",
        custom_tool_identifier: str = "",
        arguments: dict[str, Any] | list[Any] | str | None = None,
        parameters: list[dict[str, Any]] | None = None,
        default_db_schema: str = "SNOWFLAKE_LANGFLOW.CORE",
    ) -> tuple[str, dict[str, Any] | None]:
        """``(sql, bindings)`` for the routine call. Arguments must be a JSON object/array (or the
        equivalent dict/list); every value is sent through SQL API ``bindings`` (BUG-LLMOP-004)."""
        res_type = (resource_type or "procedure").strip().lower()
        if res_type not in cls.RESOURCE_TYPES:
            raise ValueError(f"Unsupported custom tool resource_type '{resource_type}'.")
        bare_fqn = cls.extract_bare_identifier(custom_tool_identifier, default_db_schema=default_db_schema)
        params_meta = parameters if parameters is not None else cls.parse_signature_parameters(custom_tool_identifier)

        if isinstance(arguments, str):
            str_arg = arguments.strip()
            if not str_arg:
                arguments = None
            else:
                try:
                    arguments = json.loads(str_arg)
                except json.JSONDecodeError as exc:
                    raise ValueError("Custom Tool arguments must be a JSON object or array.") from exc
                if not isinstance(arguments, (dict, list)):
                    raise ValueError("Custom Tool arguments must be a JSON object or array.")
        elif arguments is not None and not isinstance(arguments, (dict, list, tuple)):
            raise ValueError("Custom Tool arguments must be a JSON object or array.")

        binder = _sql_safety.BindingBuilder()
        formatted_args: list[str] = []
        # BUG-LLMOP-006: real DESCRIBE names -> `NAME => ?` (order-independent, omitted optionals are
        # simply left out). Unnamed signatures -> positional, where only trailing optionals may be omitted.
        use_named = bool(params_meta) and all(
            p.get("named", not re.fullmatch(r"arg_\d+", str(p.get("name", "")), re.I)) for p in params_meta
        )
        if isinstance(arguments, dict) and params_meta and not use_named:
            # Unnamed (types-only) signature: keys are labels only, so values are taken in the given order
            # unless they are the arg_N placeholders themselves.
            if not all(re.fullmatch(r"arg_\d+", str(k), re.I) for k in arguments):
                arguments = list(arguments.values())
        if isinstance(arguments, (list, tuple)):
            if params_meta and len(arguments) > len(params_meta):
                raise ValueError(f"Custom Tool received {len(arguments)} arguments; the signature has {len(params_meta)}.")
            arguments = {p["name"]: arguments[i] for i, p in enumerate(params_meta) if i < len(arguments)} if params_meta else list(arguments)
        if isinstance(arguments, list):
            formatted_args = [cls._bind_argument(v, "string", binder) for v in arguments]
        elif params_meta:
            args_in: dict[str, Any] = {}
            for k, v in (arguments or {}).items():
                if str(k).lower() in args_in:
                    raise ValueError(f"Duplicate Custom Tool argument '{k}' (names are case-insensitive).")
                args_in[str(k).lower()] = v
            known = {str(p["name"]).lower() for p in params_meta}
            unknown = sorted(set(args_in) - known)
            if unknown:
                raise ValueError(f"Unknown Custom Tool argument(s): {', '.join(unknown)}.")
            missing_seen = False
            for p in params_meta:
                declared = str(p["name"])
                pname = declared.lower()
                if pname not in args_in or args_in[pname] is None and not p.get("required", True):
                    if p.get("required", True):
                        raise ValueError(f"Missing required Custom Tool argument '{pname}'.")
                    missing_seen = True
                    continue
                if missing_seen and not use_named:
                    raise ValueError(
                        f"Argument '{pname}' follows an omitted optional argument; positional calls cannot skip it."
                    )
                expr = cls._bind_argument(args_in[pname], p.get("type", "string"), binder)
                if use_named:
                    # Unquoted-style names resolve upper-case in Snowflake; mixed/odd case is kept as declared.
                    sf_name = declared.upper() if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", declared) and (
                        declared.islower() or declared.isupper()
                    ) else declared
                    expr = f"{_sql_safety.quote_identifier(sf_name, 'argument')} => {expr}"
                formatted_args.append(expr)
        elif isinstance(arguments, dict):
            formatted_args = [cls._bind_argument(v, "string", binder) for v in arguments.values()]

        joined_args = ", ".join(formatted_args)
        if res_type == "procedure":
            sql = f"CALL {bare_fqn}({joined_args});"
        elif res_type == "table_function":
            sql = f"SELECT * FROM TABLE({bare_fqn}({joined_args}));"
        else:
            sql = f"SELECT {bare_fqn}({joined_args}) AS RESULT;"
        return sql, binder.bindings

    @classmethod
    def build_invocation_sql(cls, **kwargs: Any) -> str:
        """SQL text of ``build_invocation_request`` (``?`` placeholders; values are never inlined)."""
        return cls.build_invocation_request(**kwargs)[0]


class SnowflakeDecisionEngine:
    """
    Domain engine for `SnowflakeDecisionComponent` (`W2-R9-02` / `LLM-OP-02`) built strictly on
    official Snowflake Decision models (`snowflake-decision-fast` and `snowflake-decision`) via `AI_COMPLETE`:
    https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/ai-complete-snowflake-decision
    """

    SUPPORTED_MODELS: tuple[str, ...] = SUPPORTED_DECISION_MODELS
    QUESTION_TYPES: tuple[str, ...] = SUPPORTED_DECISION_QUESTION_TYPES
    EVALUATION_MODES: tuple[str, ...] = DECISION_EVALUATION_MODES
    QUESTION_ID_REGEX: str = r"^[A-Za-z0-9_-]{1,64}$"
    MAX_QUESTIONS_PER_REQUEST: int = 32
    MAX_TOTAL_OPTIONS_PER_REQUEST: int = 256
    MAX_SCORE_LEVELS: int = 10
    MIN_SCORE_LEVELS: int = 2
    MAX_NOUL_THRESHOLD: float = 0.92
    MAX_BYTES_DECISION: int = 262_144  # 256 KiB
    MAX_TOKENS_DECISION_FAST: int = 32_000
    FORBIDDEN_PARAMETERS: tuple[str, ...] = (
        "streaming",
        "stream",
        "guardrails",
        "temperature",
        "top_p",
        "model_parameters",
        "response_format",
    )

    MODEL_SPECS: dict[str, dict[str, Any]] = {
        "snowflake-decision-fast": {
            "min_choice_options": 2,
            "max_choice_options": 16,
            "max_tokens": 32_000,
            "max_questions": 32,
            "max_total_options": 256,
            "description": "Low-latency, high-throughput decision model (2..16 choice options, 32k token limit).",
        },
        "snowflake-decision": {
            "min_choice_options": 2,
            "max_choice_options": 32,
            "max_bytes": 262_144,
            "max_tokens_est": 50_000,
            "max_questions": 32,
            "max_total_options": 256,
            "description": "High-accuracy decision model (2..32 choice options, 256 KiB JSON prompt limit).",
        },
    }

    OFFICIAL_ERROR_SOLUTIONS: dict[str, str] = {
        "CORTEX_ENABLED_CROSS_REGION": (
            "Model snowflake-decision requires cross-region inference enabled: "
            "ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';"
        ),
        "INVALID_JSON": "Prompt must be a valid JSON string produced by TO_JSON({'state': ..., 'questions': ...}).",
        "TOO_MANY_QUESTIONS": "A single decision request supports 1..32 questions and at most 256 total options.",
        "INVALID_OPTION_COUNT": (
            "Choice questions require 2..16 options for snowflake-decision-fast or 2..32 for snowflake-decision; "
            "score questions require 2..10 levels."
        ),
        "INVALID_NOUL_CRITERIA": "Optional noul criteria must be an object containing both 'true' and 'false' keys.",
        "UNSUPPORTED_PARAMETER": (
            "Snowflake Decision rejects streaming and guardrails and ignores temperature and top_p."
        ),
    }

    @staticmethod
    def compute_confidence(probabilities: dict[str, float] | list[float]) -> float:
        vals = list(probabilities.values()) if isinstance(probabilities, dict) else list(probabilities)
        n = len(vals)
        if n <= 1:
            return 1.0 if n == 1 else 0.0
        p_max = float(max(vals))
        return (n * p_max - 1.0) / float(n - 1)

    @staticmethod
    def compute_weighted_score(probabilities: dict[str, float] | list[float]) -> float:
        if isinstance(probabilities, dict):
            total = 0.0
            for k, p in probabilities.items():
                total += float(int(k)) * float(p)
            return total
        return sum(float(i) * float(p) for i, p in enumerate(probabilities))

    @classmethod
    def map_decision_error(cls, error_message: str) -> dict[str, str]:
        msg = (error_message or "").strip()
        upper = msg.upper()
        if "CORTEX_ENABLED_CROSS_REGION" in upper or "ANY_REGION" in upper:
            return {
                "code": "CORTEX_ENABLED_CROSS_REGION",
                "message": msg,
                "remediation": cls.OFFICIAL_ERROR_SOLUTIONS["CORTEX_ENABLED_CROSS_REGION"],
            }
        if "STREAM" in upper or "GUARDRAIL" in upper:
            return {
                "code": "UNSUPPORTED_PARAMETER",
                "message": msg,
                "remediation": cls.OFFICIAL_ERROR_SOLUTIONS["UNSUPPORTED_PARAMETER"],
            }
        if "NOUL" in upper and ("TRUE" in upper or "FALSE" in upper):
            return {
                "code": "INVALID_NOUL_CRITERIA",
                "message": msg,
                "remediation": cls.OFFICIAL_ERROR_SOLUTIONS["INVALID_NOUL_CRITERIA"],
            }
        if "OPTION" in upper or "LEVEL" in upper:
            return {
                "code": "INVALID_OPTION_COUNT",
                "message": msg,
                "remediation": cls.OFFICIAL_ERROR_SOLUTIONS["INVALID_OPTION_COUNT"],
            }
        if "QUESTION" in upper and ("32" in upper or "256" in upper or "LIMIT" in upper):
            return {
                "code": "TOO_MANY_QUESTIONS",
                "message": msg,
                "remediation": cls.OFFICIAL_ERROR_SOLUTIONS["TOO_MANY_QUESTIONS"],
            }
        return {
            "code": "INVALID_JSON",
            "message": msg,
            "remediation": cls.OFFICIAL_ERROR_SOLUTIONS["INVALID_JSON"],
        }

    @classmethod
    def normalize_and_validate_questions(
        cls,
        *,
        model: str,
        questions: dict[str, Any],
        include_none_of_these: bool = False,
        noul_threshold: float = 0.70,
        state: Any = "",
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        import re

        norm_model = (model or "snowflake-decision-fast").strip()
        if norm_model not in cls.SUPPORTED_MODELS:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Unsupported Snowflake Decision model '{model}'. Supported models: {cls.SUPPORTED_MODELS}.",
                active_role="CURRENT_ROLE",
                operation="snowflake_decision_validation",
            )
        if noul_threshold <= 0.0 or noul_threshold > cls.MAX_NOUL_THRESHOLD:
            raise ValueError(
                f"noul_threshold must be between 0.01 and {cls.MAX_NOUL_THRESHOLD} "
                f"(noul probabilities stay within ~0.08–0.92; got {noul_threshold})."
            )
        if not isinstance(questions, dict) or not questions:
            raise ValueError("Decision request requires 1..32 questions in 'questions'.")
        if len(questions) > cls.MAX_QUESTIONS_PER_REQUEST:
            raise ValueError(
                f"Decision request exceeds maximum of {cls.MAX_QUESTIONS_PER_REQUEST} questions (got {len(questions)})."
            )

        max_choice = cls.MODEL_SPECS[norm_model]["max_choice_options"]
        min_choice = cls.MODEL_SPECS[norm_model]["min_choice_options"]
        normalized: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []
        total_options = 0

        for qid, qraw in questions.items():
            if not isinstance(qid, str) or not re.match(cls.QUESTION_ID_REGEX, qid):
                raise ValueError(
                    f"Invalid question ID '{qid}': must be 1..64 characters matching {cls.QUESTION_ID_REGEX}."
                )
            if qid.lower().startswith(("is_definitely_", "definitely_", "must_always_", "never_")):
                warnings.append(
                    f"Question ID '{qid}' uses leading/polarizing phrasing; neutral identifiers are recommended."
                )
            if not isinstance(qraw, dict):
                raise ValueError(f"Question '{qid}' must be a JSON object.")

            qtype = str(qraw.get("type") or "choice").strip().lower()
            if qtype not in cls.QUESTION_TYPES:
                raise ValueError(
                    f"Question '{qid}' has unsupported type '{qtype}'. Must be one of {cls.QUESTION_TYPES}."
                )
            instructions = str(qraw.get("instructions") or "").strip()
            if not instructions:
                raise ValueError(f"Question '{qid}' requires non-empty 'instructions'.")

            if qtype == "choice":
                raw_crit = qraw.get("criteria")
                if not isinstance(raw_crit, dict):
                    raise ValueError(f"Choice question '{qid}' requires a 'criteria' object mapping option keys to descriptions.")
                crit = {str(k): str(v) for k, v in raw_crit.items()}
                add_none = bool(qraw.get("include_none_of_these", include_none_of_these))
                if add_none and "none_of_these" not in crit and "not_stated" not in crit:
                    crit["none_of_these"] = "None of the listed options apply or not stated in the input."
                opt_count = len(crit)
                if opt_count < min_choice or opt_count > max_choice:
                    raise ValueError(
                        f"Choice question '{qid}' has {opt_count} options; model '{norm_model}' requires "
                        f"{min_choice}..{max_choice} options."
                    )
                total_options += opt_count
                normalized[qid] = {
                    "type": "choice",
                    "instructions": instructions,
                    "criteria": crit,
                }
            elif qtype == "score":
                raw_crit = qraw.get("criteria")
                if not isinstance(raw_crit, list) or not all(isinstance(x, str) and x.strip() for x in raw_crit):
                    raise ValueError(
                        f"Score question '{qid}' requires 'criteria' as an ordered list of 2..10 level descriptions."
                    )
                lvl_count = len(raw_crit)
                if lvl_count < cls.MIN_SCORE_LEVELS or lvl_count > cls.MAX_SCORE_LEVELS:
                    raise ValueError(
                        f"Score question '{qid}' has {lvl_count} levels; must have {cls.MIN_SCORE_LEVELS}..{cls.MAX_SCORE_LEVELS} levels."
                    )
                total_options += lvl_count
                normalized[qid] = {
                    "type": "score",
                    "instructions": instructions,
                    "criteria": [x.strip() for x in raw_crit],
                }
            elif qtype == "noul":
                raw_crit = qraw.get("criteria")
                q_entry: dict[str, Any] = {
                    "type": "noul",
                    "instructions": instructions,
                }
                if raw_crit is not None and raw_crit != {}:
                    if not isinstance(raw_crit, dict) or "true" not in raw_crit or "false" not in raw_crit:
                        raise ValueError(
                            f"Noul question '{qid}' optional 'criteria' must be an object containing both 'true' and 'false' keys."
                        )
                    q_entry["criteria"] = {
                        "true": str(raw_crit["true"]),
                        "false": str(raw_crit["false"]),
                    }
                total_options += 2
                normalized[qid] = q_entry

        if total_options > cls.MAX_TOTAL_OPTIONS_PER_REQUEST:
            raise ValueError(
                f"Total options across all questions is {total_options}, exceeding the maximum of {cls.MAX_TOTAL_OPTIONS_PER_REQUEST}."
            )

        payload_bytes = len(json.dumps({"state": state, "questions": normalized}).encode("utf-8"))
        if norm_model == "snowflake-decision" and payload_bytes > cls.MAX_BYTES_DECISION:
            raise ValueError(
                f"Decision payload size ({payload_bytes} bytes) exceeds 256 KiB ({cls.MAX_BYTES_DECISION} bytes) limit."
            )
        if norm_model == "snowflake-decision-fast":
            est_tokens = payload_bytes // 4
            if est_tokens > cls.MAX_TOKENS_DECISION_FAST:
                raise ValueError(
                    f"Decision payload (~{est_tokens} tokens) exceeds {cls.MAX_TOKENS_DECISION_FAST} token limit for snowflake-decision-fast."
                )

        return normalized, warnings

    @classmethod
    def _to_snowflake_map_literal(cls, obj: Any) -> str:
        if isinstance(obj, dict):
            pairs = []
            for k, v in obj.items():
                ek = str(k).replace("\\", "\\\\").replace("'", "''")
                pairs.append(f"'{ek}': {cls._to_snowflake_map_literal(v)}")
            return "{" + ", ".join(pairs) + "}"
        if isinstance(obj, list):
            return "[" + ", ".join(cls._to_snowflake_map_literal(x) for x in obj) + "]"
        if isinstance(obj, bool):
            return "TRUE" if obj else "FALSE"
        if isinstance(obj, (int, float)):
            return str(obj)
        escaped = str(obj if obj is not None else "").replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped}'"

    @classmethod
    def build_decision_sql(
        cls,
        *,
        model: str = "snowflake-decision-fast",
        state: Any = "",
        questions: dict[str, Any] | None = None,
        include_none_of_these: bool = False,
        return_error_details: bool = True,
        noul_threshold: float = 0.70,
        extra_parameters: dict[str, Any] | None = None,
    ) -> str:
        if extra_parameters:
            for forbidden in cls.FORBIDDEN_PARAMETERS:
                if forbidden in extra_parameters:
                    raise SnowflakeProviderError(
                        ERROR_UNSUPPORTED_CAPABILITY,
                        f"Parameter '{forbidden}' is not supported by Snowflake Decision models.",
                        active_role="CURRENT_ROLE",
                        operation="snowflake_decision_sql",
                    )
        norm_questions, _ = cls.normalize_and_validate_questions(
            model=model,
            questions=questions or {},
            include_none_of_these=include_none_of_these,
            noul_threshold=noul_threshold,
            state=state,
        )
        state_lit = cls._to_snowflake_map_literal(state)
        questions_lit = cls._to_snowflake_map_literal(norm_questions)
        err_flag = "TRUE" if return_error_details else "FALSE"
        return (
            "SELECT AI_COMPLETE(\n"
            f"  model => '{model.strip()}',\n"
            "  prompt => TO_JSON({\n"
            f"    'state': {state_lit},\n"
            f"    'questions': {questions_lit}\n"
            "  }),\n"
            f"  return_error_details => {err_flag}\n"
            ") AS decision;"
        )

    @classmethod
    def build_table_decision_sql(
        cls,
        *,
        model: str = "snowflake-decision-fast",
        database: str = "SNOWFLAKE_LANGFLOW",
        schema: str = "CORE",
        table_name: str = "SUPPORT_TICKETS",
        state_column: str = "ticket_text",
        questions: dict[str, Any] | None = None,
        include_none_of_these: bool = False,
        return_error_details: bool = True,
        noul_threshold: float = 0.70,
        max_rows: int = 100,
    ) -> str:
        norm_questions, _ = cls.normalize_and_validate_questions(
            model=model,
            questions=questions or {},
            include_none_of_these=include_none_of_these,
            noul_threshold=noul_threshold,
            state="",
        )
        questions_lit = cls._to_snowflake_map_literal(norm_questions)
        err_flag = "TRUE" if return_error_details else "FALSE"
        # BUG-LLMOP-003 / BUG-UI-SQLI-03: every object part is validated and quoted.
        if "." in (table_name or ""):
            table_fqn = _sql_safety.quote_fqn(table_name, "table_name", min_parts=3, max_parts=3)
        else:
            table_fqn = ".".join(
                _sql_safety.quote_identifier(v, f)
                for v, f in (
                    (database or "SNOWFLAKE_LANGFLOW", "database"),
                    (schema or "CORE", "schema"),
                    (table_name, "table_name"),
                )
            )
        col = _sql_safety.quote_identifier(state_column or "ticket_text", "state_column")
        model = _sql_safety.validate_enum(model, cls.SUPPORTED_MODELS, "model")
        lim = max(1, int(max_rows or 100))
        return (
            "SELECT\n"
            "  *,\n"
            "  AI_COMPLETE(\n"
            f"    model => '{model.strip()}',\n"
            "    prompt => TO_JSON({\n"
            f"      'state': {col},\n"
            f"      'questions': {questions_lit}\n"
            "    }),\n"
            f"    return_error_details => {err_flag}\n"
            "  ) AS decision\n"
            f"FROM {table_fqn}\n"
            f"LIMIT {lim};"
        )

    @classmethod
    def evaluate_questions_offline(
        cls,
        *,
        state: Any,
        questions: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        state_str = json.dumps(state).lower() if not isinstance(state, str) else state.lower()
        answers: dict[str, dict[str, Any]] = {}
        for qid, qspec in questions.items():
            qtype = qspec["type"]
            if qtype == "choice":
                crit: dict[str, str] = qspec["criteria"]
                keys = list(crit.keys())
                n = len(keys)
                chosen = keys[0]
                for k, desc in crit.items():
                    if k.lower() in state_str or any(
                        tok in state_str for tok in desc.lower().split() if len(tok) >= 5
                    ):
                        chosen = k
                        break
                p_max = 0.88
                rem = (1.0 - p_max) / float(max(1, n - 1))
                probs = {k: (p_max if k == chosen else rem) for k in keys}
                conf = cls.compute_confidence(probs)
                answers[qid] = {
                    "type": "choice",
                    "choice": chosen,
                    "confidence": conf,
                    "probabilities": probs,
                }
            elif qtype == "score":
                levels: list[str] = qspec["criteria"]
                n = len(levels)
                top_idx = n - 1 if any(w in state_str for w in ("angry", "urgent", "critical", "broken", "furious", "high")) else 0
                p_max = 0.85
                rem = (1.0 - p_max) / float(max(1, n - 1))
                probs = {str(i): (p_max if i == top_idx else rem) for i in range(n)}
                legend = {str(i): levels[i] for i in range(n)}
                score_val = cls.compute_weighted_score(probs)
                conf = cls.compute_confidence(probs)
                answers[qid] = {
                    "type": "score",
                    "score": score_val,
                    "legend": legend,
                    "confidence": conf,
                    "probabilities": probs,
                }
            elif qtype == "noul":
                positive_signals = ("urgent", "immediately", "asap", "critical", "approve", "true", "yes", "emergency")
                prob = 0.84 if any(sig in state_str for sig in positive_signals) else 0.18
                answers[qid] = {
                    "type": "noul",
                    "noul": prob,
                }
        return answers

    @classmethod
    def evaluate_decision(
        cls,
        *,
        model: str = "snowflake-decision-fast",
        state: Any = None,
        questions: dict[str, Any] | None = None,
        evaluation_mode: str = "Single State (Text / JSON Input)",
        include_none_of_these: bool = False,
        confidence_threshold: float = 0.70,
        noul_threshold: float = 0.70,
        return_error_details: bool = True,
        database: str = "SNOWFLAKE_LANGFLOW",
        schema: str = "CORE",
        table_name: str = "SUPPORT_TICKETS",
        state_column: str = "ticket_text",
        max_rows: int = 100,
        active_role: str | None = None,
        # Legacy compatibility parameters
        engine_mode: str | None = None,
        input_context: str | None = None,
        decision_policy: str | None = None,
        branches_csv: str | None = None,
        executor: Callable[[str], dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Build the AI_COMPLETE Decision SQL and, when ``executor`` is given, run it once and derive
        answers/routing exclusively from the returned ``value.answers`` (BUG-LLMOP-001).

        Without ``executor`` the result is an offline *preview* (``answers_source='offline_preview'``)
        computed by ``evaluate_questions_offline``; runtime callers must always pass an executor.
        """
        active_role = _resolve_caller_role(active_role)
        if engine_mode and "SystemOne" in engine_mode:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Cortex SystemOne API (Jev-compatible) has been removed. Use official 'snowflake-decision-fast' or 'snowflake-decision'.",
                active_role=active_role,
                operation="snowflake_decision_evaluate",
            )
        eff_model = (model or "snowflake-decision-fast").strip()
        if eff_model not in cls.SUPPORTED_MODELS:
            if branches_csv is not None or input_context is not None:
                eff_model = "snowflake-decision-fast"
            else:
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Unsupported Snowflake Decision model '{eff_model}'. Must be one of {cls.SUPPORTED_MODELS}.",
                    active_role=active_role,
                    operation="snowflake_decision_evaluate",
                )

        eff_state = state if state is not None else (input_context or "")
        if not questions:
            branches = [b.strip() for b in (branches_csv or "billing,technical_support").split(",") if b.strip()]
            if len(branches) < 2:
                branches = [branches[0] if branches else "approve", "review"]
            questions = {
                "primary_decision": {
                    "type": "choice",
                    "instructions": (decision_policy or "Select the best matching option for the input state.").strip(),
                    "criteria": {b: f"Select '{b}' when matching the input state." for b in branches},
                }
            }

        norm_questions, warnings = cls.normalize_and_validate_questions(
            model=eff_model,
            questions=questions,
            include_none_of_these=include_none_of_these,
            noul_threshold=noul_threshold,
            state=eff_state,
        )

        if evaluation_mode == "Table-Level Processing (Database.Schema.Table)":
            sql_stmt = cls.build_table_decision_sql(
                model=eff_model,
                database=database,
                schema=schema,
                table_name=table_name,
                state_column=state_column,
                questions=norm_questions,
                include_none_of_these=False,
                return_error_details=return_error_details,
                noul_threshold=noul_threshold,
                max_rows=max_rows,
            )
        else:
            sql_stmt = cls.build_decision_sql(
                model=eff_model,
                state=eff_state,
                questions=norm_questions,
                include_none_of_these=False,
                return_error_details=return_error_details,
                noul_threshold=noul_threshold,
            )

        is_table = evaluation_mode == "Table-Level Processing (Database.Schema.Table)"
        row_results: list[dict[str, Any]] = []
        execution_result: dict[str, Any] | None = None
        if executor is not None:
            execution_result = executor(sql_stmt)
            if not isinstance(execution_result, dict) or not execution_result.get("ok"):
                msg = (execution_result or {}).get("message") if isinstance(execution_result, dict) else None
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Snowflake Decision AI_COMPLETE failed: {msg or 'no result'}",
                    active_role=active_role,
                    operation="snowflake_decision_evaluate",
                )
            rows = [r for r in (execution_result.get("rows") or []) if isinstance(r, dict)]
            if not rows:
                raise ValueError("Snowflake Decision returned no rows; refusing to route without a model answer.")
            for row in rows:
                row_answers = cls.parse_decision_result(cls._decision_cell(row))
                row_results.append({**row, "answers": row_answers, **cls.summarize_answers(
                    row_answers, confidence_threshold=confidence_threshold, noul_threshold=noul_threshold
                )})
            answers = row_results[0]["answers"]
            answers_source = "snowflake_ai_complete"
        else:
            answers = cls.evaluate_questions_offline(state=eff_state, questions=norm_questions)
            answers_source = "offline_preview"

        summary = cls.summarize_answers(answers, confidence_threshold=confidence_threshold, noul_threshold=noul_threshold)
        if is_table and row_results:
            # Conservative table-level routing: high confidence only when every row is.
            summary["high_confidence"] = all(r["high_confidence"] for r in row_results)
            summary["route"] = "high_confidence" if summary["high_confidence"] else "low_confidence"
        is_high_confidence = summary["high_confidence"]
        selected_branch = summary["selected_branch"]
        route = summary["route"]

        out: dict[str, Any] = {
            "answers": answers,
            "answers_source": answers_source,
            "model": eff_model,
            "evaluation_mode": evaluation_mode,
            "confidence": summary["confidence"],
            "confidence_threshold": float(confidence_threshold),
            "noul_threshold": float(noul_threshold),
            "high_confidence": is_high_confidence,
            "route": route,
            "selected_branch": selected_branch or route,
            "decision": (selected_branch or route).upper(),
            "policy_action": "AUTOMATED_ACTION" if is_high_confidence else "HUMAN_REVIEW",
            "return_error_details": bool(return_error_details),
            "warnings": warnings,
            "active_role": active_role,
            "sql_preview": sql_stmt,
        }
        if executor is not None:
            out["execution_result"] = execution_result
            if is_table:
                out["rows"] = row_results
        return out

    @staticmethod
    def _decision_cell(row: dict[str, Any]) -> Any:
        for key in ("DECISION", "decision"):
            if key in row:
                return row[key]
        return next(iter(row.values()), None) if len(row) == 1 else None

    @classmethod
    def parse_decision_result(cls, raw: Any) -> dict[str, dict[str, Any]]:
        """``value.answers`` from a real AI_COMPLETE Decision result (dict or JSON string).

        Accepts ``{"value": {"answers": ...}, "error": null}`` (return_error_details => TRUE) and
        ``{"answers": ...}``. Raises ``ValueError`` on an error payload or any other shape; never
        substitutes heuristic answers.
        """
        obj = raw
        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except json.JSONDecodeError as exc:
                raise ValueError("Snowflake Decision result is not valid JSON.") from exc
        if not isinstance(obj, dict):
            raise ValueError("Snowflake Decision result must be a JSON object.")
        if "value" in obj or "error" in obj:
            if obj.get("error"):
                raise ValueError(f"Snowflake Decision returned an error: {obj.get('error')}")
            obj = obj.get("value")
            if isinstance(obj, str):
                obj = json.loads(obj)
        answers = obj.get("answers") if isinstance(obj, dict) else None
        if not isinstance(answers, dict) or not answers or not all(isinstance(a, dict) for a in answers.values()):
            raise ValueError("Snowflake Decision result has no 'answers' object.")
        return answers

    @classmethod
    def summarize_answers(
        cls,
        answers: dict[str, dict[str, Any]],
        *,
        confidence_threshold: float,
        noul_threshold: float,
    ) -> dict[str, Any]:
        """Routing derived from answers; uses the service ``confidence`` (formula only as fallback)."""
        confidences: list[float] = []
        noul_scores: list[float] = []
        selected_branch = ""
        for ans in answers.values():
            a_type = str(ans.get("type") or ("noul" if "noul" in ans else "choice" if "choice" in ans else "score"))
            if a_type in ("choice", "score"):
                conf = ans.get("confidence")
                if conf is None and isinstance(ans.get("probabilities"), dict):
                    conf = cls.compute_confidence(ans["probabilities"])
                confidences.append(float(conf if conf is not None else 0.0))
                if a_type == "choice" and not selected_branch and ans.get("choice") is not None:
                    selected_branch = str(ans["choice"])
            elif a_type == "noul":
                noul_scores.append(float(ans.get("noul") or 0.0))
        min_conf = min(confidences) if confidences else 1.0
        min_noul = min(noul_scores) if noul_scores else 1.0
        high = (not confidences or min_conf >= float(confidence_threshold)) and (
            not noul_scores or min_noul >= float(noul_threshold)
        )
        route = "high_confidence" if high else "low_confidence"
        return {
            "confidence": confidences[0] if confidences else (noul_scores[0] if noul_scores else 1.0),
            "high_confidence": high,
            "route": route,
            "selected_branch": selected_branch or route,
        }


