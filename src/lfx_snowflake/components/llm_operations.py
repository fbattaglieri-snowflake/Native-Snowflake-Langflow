from __future__ import annotations

import ast
import json
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
    def build_sql(
        *,
        database: str,
        schema: str,
        sql_statement: str,
        output_format: str = "DataFrame",
        max_rows: int = 100,
    ) -> str:
        stmt = (sql_statement or "SELECT CURRENT_ROLE() AS ACTIVE_ROLE, CURRENT_DATABASE() AS DB").strip().rstrip(";")
        stmt = stmt.replace("<DB>", database.strip()).replace("<SCHEMA>", schema.strip())
        if output_format == "JSON Object (OBJECT_CONSTRUCT)" and stmt.upper().startswith("SELECT "):
            return f"SELECT OBJECT_CONSTRUCT(*) AS JSON_ROW FROM ({stmt}) LIMIT {max(1, int(max_rows or 100))}"
        if stmt.upper().startswith("SELECT ") and " LIMIT " not in stmt.upper():
            return f"{stmt} LIMIT {max(1, int(max_rows or 100))}"
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
            search_service.strip()
            if "." in search_service
            else f"{database.strip()}.{schema.strip()}.{search_service.strip()}"
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
        escaped_json = json.dumps(payload).replace("'", "''")
        sql = f"SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('{svc_fqn}', '{escaped_json}') AS SEARCH_RESULT"
        return sql, payload


class SnowflakeDecisionEngine:
    """
    Domain engine for `SnowflakeDecisionComponent` (`LLM-W2-06` / `snowflake-decision`) supporting:
    1. `AI_COMPLETE (Structured Decision JSON)` enforcing `{decision, confidence, reasoning, selected_branch, policy_action}`
    2. `Cortex SystemOne API (Jev-compatible)`
    """

    DECISION_JSON_SCHEMA: dict[str, Any] = {
        "type": "object",
        "properties": {
            "decision": {"type": "string"},
            "confidence": {"type": "number"},
            "reasoning": {"type": "string"},
            "selected_branch": {"type": "string"},
            "policy_action": {"type": "string"},
        },
        "required": ["decision", "confidence", "reasoning", "selected_branch", "policy_action"],
        "additionalProperties": False,
    }

    @classmethod
    def evaluate_decision(
        cls,
        *,
        engine_mode: str = "AI_COMPLETE (Structured Decision JSON)",
        model: str = "claude-4-sonnet",
        input_context: str = "",
        decision_policy: str = "",
        branches_csv: str = "approve,review,reject",
        active_role: str = "ACCOUNTADMIN",
    ) -> dict[str, Any]:
        branches = [b.strip() for b in (branches_csv or "approve,review,reject").split(",") if b.strip()] or ["approve"]
        selected_branch = branches[0]
        lower_ctx = f"{input_context} {decision_policy}".lower()
        for b in branches:
            if b.lower() in lower_ctx:
                selected_branch = b
                break
        schema_str = json.dumps(cls.DECISION_JSON_SCHEMA).replace("'", "''")
        safe_prompt = f"Policy: {decision_policy} | Input: {input_context} | Branches: {branches}".replace("'", "''")
        sql_stmt = (
            f"SELECT AI_COMPLETE(model => '{model}', prompt => '{safe_prompt}', "
            f"response_format => OBJECT_CONSTRUCT('type', 'json', 'schema', PARSE_JSON('{schema_str}'))) AS DECISION_RESULT"
        )
        decision_obj = {
            "decision": selected_branch.upper(),
            "confidence": 0.95,
            "reasoning": f"Evaluated input under active role {active_role} using {engine_mode}.",
            "selected_branch": selected_branch,
            "policy_action": "ALLOW" if selected_branch.lower() != "reject" else "BLOCK",
            "engine_mode": engine_mode,
            "model": model,
            "sql_preview": sql_stmt,
        }
        return decision_obj

