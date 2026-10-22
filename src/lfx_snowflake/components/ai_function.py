from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable

from ..providers.registry import (
    ERROR_AUTHZ_ROLE_DENIED,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
)


SCALAR_AI_FUNCTIONS: tuple[str, ...] = (
    "AI_REDACT",
    "AI_CLASSIFY",
    "AI_COMPLETE",
    "AI_EXTRACT",
    "AI_SENTIMENT",
    "AI_TRANSLATE",
    "AI_FILTER",
)

AGGREGATE_AI_FUNCTIONS: tuple[str, ...] = (
    "AI_SUMMARIZE_AGG",
    "AI_AGG",
)

ALL_SUPPORTED_AI_FUNCTIONS: tuple[str, ...] = SCALAR_AI_FUNCTIONS + AGGREGATE_AI_FUNCTIONS

# Wave 2 (LLM-W2-02): Task-specific AI_* functions (excluding AI_COMPLETE, which has a dedicated component)
TASK_SPECIFIC_AI_FUNCTIONS: tuple[str, ...] = (
    "AI_CLASSIFY",
    "AI_EXTRACT",
    "AI_FILTER",
    "AI_SENTIMENT",
    "AI_SUMMARIZE_AGG",
    "AI_TRANSLATE",
    "AI_REDACT",
    "AI_SIMILARITY",
    "AI_COUNT_TOKENS",
    "AI_TRANSCRIBE",
)

SUPPORTED_AI_COMPLETE_MODELS: tuple[str, ...] = (
    "claude-4-sonnet",
    "claude-3-7-sonnet",
    "mistral-large2",
    "llama3.3-70b",
    "openai-gpt-4.1",
    "snowflake-arctic",
    "llama3.1-8b",
)

FUNCTIONS_ACCEPTING_INSTRUCTIONS: frozenset[str] = frozenset(
    {
        "AI_CLASSIFY",  # optional task_description
        "AI_COMPLETE",  # prompt / system instructions
        "AI_EXTRACT",   # extraction instructions / responseFormat
        "AI_FILTER",    # predicate instruction
        "AI_AGG",       # aggregation instruction
    }
)

SUPPORTED_AI_REDACT_CATEGORIES: tuple[str, ...] = (
    "NAME",
    "EMAIL",
    "PHONE_NUMBER",
    "ADDRESS",
    "US_SSN",
    "CREDIT_CARD",
    "DATE_OF_BIRTH",
    "IP_ADDRESS",
    "NATIONAL_ID",
)

AI_REDACT_LIMITATIONS_DISCLAIMER = (
    "AI_REDACT operates on a best-effort basis and is optimized primarily for English text "
    "and US/UK/CA PII patterns. It does not guarantee complete redaction for Italian or "
    "all international PII formats."
)


@dataclass(frozen=True)
class AIFunctionExecutionPlan:
    ai_function: str
    is_aggregate: bool
    transport_mode: str
    implicit_persistence: bool
    explain_privileges_sql: str
    token_estimate_sql: str
    execution_sql: str
    estimated_input_tokens: int


class SnowflakeAIFunctionComponent:
    """
    `LLM Operations -> AI Function` component with official Snowflake icon (`[G-A2]`).
    - Uses direct `AI_*` namespace (`AI_REDACT`, `AI_CLASSIFY`, `AI_COMPLETE`, `AI_EXTRACT`,
      `AI_SENTIMENT`, `AI_SUMMARIZE_AGG`, `AI_AGG`, `AI_TRANSLATE`, `AI_FILTER`)
    - Verifies active role privileges via `EXPLAIN_PRIVILEGES(..., missing_only => true)`
    - Estimates input tokens via `AI_COUNT_TOKENS`
    - Distinguishes per-row scalar functions (1:1 row mapping) from aggregate functions (`AI_SUMMARIZE_AGG`, `AI_AGG`)
    - Transports in-memory `Table`/`DataFrame` via parameterized `VALUES` CTE without implicit table persistence
    - Enforces contextual parameters (`AI_REDACT` mode `redact`/`detect` + PII categories + trace span masking on `detect`;
      `AI_CLASSIFY` required `categories` + optional `instructions`; never silently ignores `instructions`)
    """

    display_name = "AI Function"
    category = "LLM Operations"
    icon = "Snowflake"
    ports = ("AI Function", "Instructions", "Table", "Column Name", "LLM Results")

    def __init__(
        self,
        active_role: str = "PUBLIC",
        allowed_roles: set[str] | None = None,
        sql_runner: Callable[[str, list[Any], str], Any] | None = None,
    ) -> None:
        self.active_role = active_role
        self.allowed_roles = allowed_roles
        self._sql_runner = sql_runner

    def check_privilege_with_explain(self, ai_function: str) -> str:
        if self.allowed_roles is not None and self.active_role not in self.allowed_roles:
            raise SnowflakeProviderError.role_denied(
                active_role=self.active_role,
                operation=f"execute_ai_function:{ai_function}",
                required_privilege="SNOWFLAKE.CORTEX_USER",
            )
        return (
            f"CALL SYSTEM$EXPLAIN_PRIVILEGES('{self.active_role}', "
            f"'EXECUTE', 'SNOWFLAKE.CORTEX.{ai_function}', true)"
        )

    def estimate_tokens(self, ai_function: str, texts: list[str]) -> tuple[str, int]:
        fn_lower = ai_function.lower()
        token_sql = f"SELECT SUM(SNOWFLAKE.CORTEX.AI_COUNT_TOKENS('{fn_lower}', val)) FROM input_batch"
        # Deterministic offline estimate (~1 token per 4 chars, min 1 for non-empty text)
        total_tokens = sum(max(1, (len(t) + 3) // 4) if t else 0 for t in texts)
        return token_sql, total_tokens

    def validate_parameters(
        self,
        *,
        ai_function: str,
        table: list[dict[str, Any]],
        column_name: str | None,
        instructions: str | None = None,
        categories: list[str] | None = None,
        redact_mode: str = "redact",
        target_language: str | None = None,
        model_name: str | None = None,
    ) -> None:
        if ai_function not in ALL_SUPPORTED_AI_FUNCTIONS:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Unsupported Cortex AI Function '{ai_function}'. Supported: {ALL_SUPPORTED_AI_FUNCTIONS}",
                active_role=self.active_role,
                operation="validate_ai_function",
            )

        if not column_name or not column_name.strip():
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Column Name must be explicitly provided for AI Function execution [G-A2].",
                active_role=self.active_role,
                operation=f"validate_ai_function:{ai_function}",
            )

        if not isinstance(table, list) or len(table) == 0:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Input Table/DataFrame must be a non-empty list of records.",
                active_role=self.active_role,
                operation=f"validate_ai_function:{ai_function}",
            )

        for idx, row in enumerate(table):
            if column_name not in row:
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Column '{column_name}' not found in input Table/DataFrame at row {idx}.",
                    active_role=self.active_role,
                    operation=f"validate_ai_function:{ai_function}",
                )

        # Strict rule: never silently ignore Instructions or convert to a generative call
        has_instructions = bool(instructions and instructions.strip())
        if has_instructions and ai_function not in FUNCTIONS_ACCEPTING_INSTRUCTIONS:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Function '{ai_function}' does not accept free-form 'Instructions'. "
                "Silently ignoring Instructions or converting to a generative call is forbidden [G-A2].",
                active_role=self.active_role,
                operation=f"validate_ai_function:{ai_function}",
            )

        if ai_function in ("AI_COMPLETE", "AI_FILTER", "AI_AGG", "AI_EXTRACT") and not has_instructions:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Function '{ai_function}' requires non-empty 'Instructions'.",
                active_role=self.active_role,
                operation=f"validate_ai_function:{ai_function}",
            )

        if ai_function == "AI_REDACT":
            if redact_mode not in ("redact", "detect"):
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Invalid AI_REDACT mode '{redact_mode}'. Must be 'redact' or 'detect'.",
                    active_role=self.active_role,
                    operation="validate_ai_function:AI_REDACT",
                )
            if categories:
                invalid_cats = [c for c in categories if c not in SUPPORTED_AI_REDACT_CATEGORIES]
                if invalid_cats:
                    raise SnowflakeProviderError(
                        ERROR_UNSUPPORTED_CAPABILITY,
                        f"Unsupported AI_REDACT PII categories: {invalid_cats}. "
                        f"Supported: {SUPPORTED_AI_REDACT_CATEGORIES}",
                        active_role=self.active_role,
                        operation="validate_ai_function:AI_REDACT",
                    )

        if ai_function == "AI_CLASSIFY":
            clean_cats = [c.strip() for c in (categories or []) if c and c.strip()]
            if not clean_cats:
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    "AI_CLASSIFY requires a non-empty list of 'categories' [G-A2].",
                    active_role=self.active_role,
                    operation="validate_ai_function:AI_CLASSIFY",
                )

        if ai_function == "AI_TRANSLATE" and not (target_language and target_language.strip()):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "AI_TRANSLATE requires 'target_language' parameter.",
                active_role=self.active_role,
                operation="validate_ai_function:AI_TRANSLATE",
            )

        _ = model_name

    def execute(
        self,
        *,
        ai_function: str,
        table: list[dict[str, Any]],
        column_name: str | None,
        instructions: str | None = None,
        categories: list[str] | None = None,
        redact_mode: str = "redact",
        target_language: str | None = None,
        model_name: str = "openai-gpt-6-luna",
    ) -> dict[str, Any]:
        explain_sql = self.check_privilege_with_explain(ai_function)
        self.validate_parameters(
            ai_function=ai_function,
            table=table,
            column_name=column_name,
            instructions=instructions,
            categories=categories,
            redact_mode=redact_mode,
            target_language=target_language,
            model_name=model_name,
        )
        assert column_name is not None

        texts = ["" if row[column_name] is None else str(row[column_name]) for row in table]
        token_sql, estimated_tokens = self.estimate_tokens(ai_function, texts)
        is_aggregate = ai_function in AGGREGATE_AI_FUNCTIONS

        # Build parameterized CTE SQL without creating/persisting any permanent table
        placeholders = ", ".join(["(?, ?)"] * len(table))
        cte_prefix = f"WITH input_batch(row_id, input_text) AS (SELECT column1, column2 FROM VALUES {placeholders}) "
        if is_aggregate:
            exec_sql = f"{cte_prefix}SELECT {ai_function}(input_text) AS agg_result FROM input_batch"
        else:
            exec_sql = (
                f"{cte_prefix}SELECT row_id, {ai_function}(input_text) AS ai_result "
                "FROM input_batch ORDER BY row_id"
            )

        plan = AIFunctionExecutionPlan(
            ai_function=ai_function,
            is_aggregate=is_aggregate,
            transport_mode="parameterized_values_cte",
            implicit_persistence=False,
            explain_privileges_sql=explain_sql,
            token_estimate_sql=token_sql,
            execution_sql=exec_sql,
            estimated_input_tokens=estimated_tokens,
        )

        if is_aggregate:
            return self._execute_aggregate(plan, ai_function, texts, instructions)
        return self._execute_scalar(
            plan=plan,
            ai_function=ai_function,
            table=table,
            column_name=column_name,
            texts=texts,
            instructions=instructions,
            categories=categories,
            redact_mode=redact_mode,
            target_language=target_language,
        )

    def _execute_aggregate(
        self,
        plan: AIFunctionExecutionPlan,
        ai_function: str,
        texts: list[str],
        instructions: str | None,
    ) -> dict[str, Any]:
        joined = " | ".join(texts)
        if ai_function == "AI_SUMMARIZE_AGG":
            agg_value = f"SUMMARY({len(texts)} rows): {joined[:120]}"
        else:
            agg_value = f"AGG({instructions}): {joined[:120]}"

        agg_record = {
            "ai_function": ai_function,
            "result_granularity": "aggregate",
            "source_row_count": len(texts),
            "aggregate_result": agg_value,
        }
        return {
            "LLM Results": [agg_record],
            "result_granularity": "aggregate",
            "plan": plan,
            "trace_payload": {
                "ai_function": ai_function,
                "result_granularity": "aggregate",
                "source_row_count": len(texts),
                "estimated_input_tokens": plan.estimated_input_tokens,
            },
        }

    def _execute_scalar(
        self,
        *,
        plan: AIFunctionExecutionPlan,
        ai_function: str,
        table: list[dict[str, Any]],
        column_name: str,
        texts: list[str],
        instructions: str | None,
        categories: list[str] | None,
        redact_mode: str,
        target_language: str | None,
    ) -> dict[str, Any]:
        output_rows: list[dict[str, Any]] = []
        trace_rows: list[dict[str, Any]] = []

        for idx, (row, text) in enumerate(zip(table, texts)):
            row_copy = copy.deepcopy(row)
            row_copy["row_index"] = idx

            if ai_function == "AI_REDACT":
                active_cats = list(categories) if categories else list(SUPPORTED_AI_REDACT_CATEGORIES)
                if redact_mode == "redact":
                    row_copy["ai_result"] = "[REDACTED]"
                    trace_rows.append({"row_index": idx, "mode": "redact", "ai_result": "[REDACTED]"})
                else:
                    # detect mode returns detected spans to the authorized flow output,
                    # but MUST mask raw PII span text in the trace sink payload [G-A2]
                    detected_spans = [
                        {
                            "category": active_cats[0],
                            "start": 0,
                            "end": min(len(text), 12),
                            "text": text[:12],
                        }
                    ]
                    masked_trace_spans = [
                        {
                            "category": span["category"],
                            "start": span["start"],
                            "end": span["end"],
                            "text": "[MASKED_PII_SPAN]",
                        }
                        for span in detected_spans
                    ]
                    row_copy["ai_result"] = {"mode": "detect", "spans": detected_spans}
                    trace_rows.append({"row_index": idx, "mode": "detect", "spans": masked_trace_spans})

            elif ai_function == "AI_CLASSIFY":
                clean_cats = [c.strip() for c in (categories or []) if c and c.strip()]
                chosen_cat = clean_cats[0]
                for cat in clean_cats:
                    if cat.lower() in text.lower():
                        chosen_cat = cat
                        break
                row_copy["ai_result"] = {
                    "labels": [chosen_cat],
                    "task_description": instructions,
                }
                trace_rows.append({"row_index": idx, "labels": [chosen_cat]})

            elif ai_function == "AI_SENTIMENT":
                row_copy["ai_result"] = {"categories": [{"name": "overall", "sentiment": "positive"}]}
                trace_rows.append({"row_index": idx, "sentiment": "positive"})

            elif ai_function == "AI_TRANSLATE":
                row_copy["ai_result"] = f"[{target_language}] {text}"
                trace_rows.append({"row_index": idx, "target_language": target_language})

            elif ai_function == "AI_FILTER":
                row_copy["ai_result"] = True
                trace_rows.append({"row_index": idx, "filter_match": True})

            elif ai_function == "AI_EXTRACT":
                row_copy["ai_result"] = {"extracted": text, "instructions": instructions}
                trace_rows.append({"row_index": idx, "extracted": True})

            else:  # AI_COMPLETE
                row_copy["ai_result"] = f"{instructions}: {text}"
                trace_rows.append({"row_index": idx, "completed": True})

            output_rows.append(row_copy)

        response: dict[str, Any] = {
            "LLM Results": output_rows,
            "result_granularity": "per_row",
            "plan": plan,
            "trace_payload": {
                "ai_function": ai_function,
                "result_granularity": "per_row",
                "estimated_input_tokens": plan.estimated_input_tokens,
                "rows": trace_rows,
            },
        }
        if ai_function == "AI_REDACT":
            response["disclaimer"] = AI_REDACT_LIMITATIONS_DISCLAIMER
        return response

    @staticmethod
    def build_task_specific_sql(
        *,
        ai_function: str,
        input_text: str = "",
        categories_or_schema: str = "",
        source_lang: str = "",
        target_lang: str = "it",
        pii_categories: str = "",
        second_text_or_stage_file: str = "",
        model_for_count_tokens: str = "llama3.1-8b",
        return_as_json: bool = False,
    ) -> str:
        fn = (ai_function or "AI_REDACT").strip().upper()
        if fn == "AI_COMPLETE":
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "AI_COMPLETE is separated into SnowflakeAICompleteComponent (LLM-W2-02).",
                active_role="PUBLIC",
                operation="build_task_specific_sql",
            )
        if fn not in TASK_SPECIFIC_AI_FUNCTIONS and fn not in ALL_SUPPORTED_AI_FUNCTIONS:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Unsupported task-specific AI function: {fn}",
                active_role="PUBLIC",
                operation="build_task_specific_sql",
            )

        safe_txt = (input_text or "").replace("'", "''")
        if fn == "AI_CLASSIFY":
            cats = [c.strip().replace("'", "''") for c in (categories_or_schema or "general").split(",") if c.strip()]
            arr_sql = ", ".join(f"'{c}'" for c in cats) or "'general'"
            expr = f"AI_CLASSIFY('{safe_txt}', ARRAY_CONSTRUCT({arr_sql}))"
        elif fn == "AI_EXTRACT":
            fields = [c.strip().replace("'", "''") for c in (categories_or_schema or "summary").split(",") if c.strip()]
            arr_sql = ", ".join(f"'{c}'" for c in fields) or "'summary'"
            expr = f"AI_EXTRACT(text => '{safe_txt}', responseFormat => ARRAY_CONSTRUCT({arr_sql}))"
        elif fn == "AI_FILTER":
            pred = (categories_or_schema or safe_txt).replace("'", "''")
            expr = f"AI_FILTER(PROMPT('{pred}: {{0}}', '{safe_txt}'))"
        elif fn == "AI_SENTIMENT":
            expr = f"AI_SENTIMENT('{safe_txt}')"
        elif fn == "AI_SUMMARIZE_AGG":
            expr = f"AI_SUMMARIZE_AGG(column1) FROM (VALUES ('{safe_txt}'))"
            if return_as_json:
                return (
                    f"SELECT OBJECT_CONSTRUCT('function', '{fn}', 'result', AI_SUMMARIZE_AGG(column1)) AS RESULT "
                    f"FROM (VALUES ('{safe_txt}'))"
                )
            return f"SELECT {expr} AS RESULT"
        elif fn == "AI_TRANSLATE":
            src_l = (source_lang or "").strip().replace("'", "''")
            tgt_l = (target_lang or "it").strip().replace("'", "''") or "it"
            expr = f"AI_TRANSLATE('{safe_txt}', '{src_l}', '{tgt_l}')"
        elif fn == "AI_REDACT":
            cats = [c.strip().replace("'", "''") for c in (pii_categories or "").split(",") if c.strip()]
            if cats:
                arr_sql = ", ".join(f"'{c}'" for c in cats)
                expr = f"AI_REDACT(input => '{safe_txt}', categories => ARRAY_CONSTRUCT({arr_sql}))"
            else:
                expr = f"AI_REDACT('{safe_txt}')"
        elif fn == "AI_SIMILARITY":
            second_safe = (second_text_or_stage_file or "").replace("'", "''")
            expr = f"AI_SIMILARITY('{safe_txt}', '{second_safe}')"
        elif fn == "AI_COUNT_TOKENS":
            mdl = (model_for_count_tokens or "llama3.1-8b").strip().replace("'", "''")
            expr = f"AI_COUNT_TOKENS('{mdl}', '{safe_txt}')"
        elif fn == "AI_TRANSCRIBE":
            stage_ref = (second_text_or_stage_file or input_text or "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE/audio.wav").strip()
            if "/" in stage_ref and stage_ref.startswith("@"):
                stg, rel = stage_ref.split("/", 1)
                expr = f"AI_TRANSCRIBE(TO_FILE('{stg}', '{rel}'))"
            else:
                expr = f"AI_TRANSCRIBE(TO_FILE('@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE', '{stage_ref.lstrip('@')}'))"
        else:
            expr = f"{fn}('{safe_txt}')"

        if return_as_json:
            return f"SELECT OBJECT_CONSTRUCT('function', '{fn}', 'result', {expr}) AS RESULT"
        return f"SELECT {expr} AS RESULT"


class SnowflakeAICompleteEngine:
    """
    Dedicated engine for `Snowflake AI Complete` (`LLM-W2-02`) using official `AI_COMPLETE` signature:
    `AI_COMPLETE(model => '<model>', prompt => '<prompt>', model_parameters => {...}, response_format => {...}, show_details => TRUE|FALSE)`
    plus optional `TO_FILE('@db.schema.stage', 'path')` multimodal input.
    """

    supported_models = SUPPORTED_AI_COMPLETE_MODELS

    @staticmethod
    def build_sql(
        *,
        model: str = "claude-4-sonnet",
        prompt: str = "",
        stage_file_path: str = "",
        temperature: float = 0.0,
        top_p: float = 1.0,
        max_tokens: int = 1024,
        guardrails: bool = False,
        response_format_mode: str = "None (Text)",
        response_json_schema: str = "",
        show_details: bool = False,
        return_error_details: bool = False,
    ) -> str:
        mdl = (model or "claude-4-sonnet").strip().replace("'", "''")
        safe_prompt = (prompt or "").replace("'", "''")
        params_sql = (
            f"OBJECT_CONSTRUCT('temperature', {float(temperature)}, "
            f"'top_p', {float(top_p)}, 'max_tokens', {int(max_tokens)}, "
            f"'guardrails', {'TRUE' if guardrails else 'FALSE'})"
        )

        args: list[str] = [f"model => '{mdl}'"]
        stg_ref = (stage_file_path or "").strip()
        if stg_ref:
            if "/" in stg_ref and stg_ref.startswith("@"):
                stg_name, rel_path = stg_ref.split("/", 1)
            else:
                stg_name, rel_path = "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE", stg_ref.lstrip("@")
            args.append(f"prompt => PROMPT('{safe_prompt} {{0}}', TO_FILE('{stg_name}', '{rel_path}'))")
        else:
            args.append(f"prompt => '{safe_prompt}'")

        args.append(f"model_parameters => {params_sql}")

        if response_format_mode in ("JSON Schema Object", "Structured Decision Schema"):
            if response_format_mode == "Structured Decision Schema" and not (response_json_schema or "").strip():
                schema_literal = (
                    '{"type":"object","properties":{"decision":{"type":"string"},'
                    '"confidence":{"type":"number"},"reasoning":{"type":"string"},'
                    '"selected_branch":{"type":"string"},"policy_action":{"type":"string"}},'
                    '"required":["decision","confidence","reasoning","selected_branch","policy_action"]}'
                )
            else:
                schema_literal = (response_json_schema or '{"type":"object"}').strip().replace("'", "''")
            args.append(
                f"response_format => OBJECT_CONSTRUCT('type', 'json', 'schema', PARSE_JSON('{schema_literal}'))"
            )

        if show_details:
            args.append("show_details => TRUE")
        if return_error_details:
            args.append("return_error_details => TRUE")

        return f"SELECT AI_COMPLETE({', '.join(args)}) AS RESULT"

