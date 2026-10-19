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
