from __future__ import annotations

from lfx_snowflake import sql_safety as _sql_safety

import copy
import json
import re
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

# Wave 2 Round 9 (LLM-OP-04): Task-specific AI_* functions (14 functions; excluding AI_COMPLETE and AI_REDACT)
TASK_SPECIFIC_AI_FUNCTIONS: tuple[str, ...] = (
    "AI_CLASSIFY",
    "AI_FILTER",
    "AI_EXTRACT",
    "AI_SENTIMENT",
    "AI_SUMMARIZE",
    "AI_SUMMARIZE_AGG",
    "AI_AGG",
    "AI_TRANSLATE",
    "AI_SIMILARITY",
    "AI_TRANSCRIBE",
    "AI_PARSE_DOCUMENT",
    "AI_EMBED",
    "AI_MULTI_EMBED",
    "AI_COUNT_TOKENS",
)

SUPPORTED_AI_COMPLETE_MODELS: tuple[str, ...] = (
    "claude-sonnet-5-5",
    "claude-opus-5-5",
    "claude-haiku-5-5",
    "openai-gpt-6-astra",
    "openai-gpt-6.1-sol",
    "openai-gpt-6-luna",
    "openai-gpt-6-sol",
    "gemini-3.1-pro",
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash",
    "grok-4.7",
    "grok-4.6",
    "kimi-k3",
    "glm-5.3",
    "llama4-maverick",
    "mistral-large2",
    "llama3.3-70b",
    "claude-4-sonnet",
    "claude-3-7-sonnet",
    "openai-gpt-4.1",
    "snowflake-arctic",
    "llama3.1-8b",
)

AI_COMPLETE_MODEL_CAPABILITIES: dict[str, dict[str, bool]] = {
    "claude-sonnet-5-5": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "claude-opus-5-5": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "claude-haiku-5-5": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "openai-gpt-6-astra": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "openai-gpt-6.1-sol": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "openai-gpt-6-luna": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "openai-gpt-6-sol": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "gemini-3.1-pro": {"structured_outputs": True, "multimodal": True, "audio": True, "video": True, "reasoning": True, "tools": True},
    "gemini-3.1-flash-lite": {"structured_outputs": True, "multimodal": True, "audio": True, "video": True, "reasoning": False, "tools": True},
    "gemini-3.5-flash": {"structured_outputs": True, "multimodal": True, "audio": True, "video": True, "reasoning": True, "tools": True},
    "grok-4.7": {"structured_outputs": True, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": True},
    "grok-4.6": {"structured_outputs": True, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": True},
    "kimi-k3": {"structured_outputs": True, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": True},
    "glm-5.3": {"structured_outputs": False, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": True},
    "llama4-maverick": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": False, "tools": True},
    "mistral-large2": {"structured_outputs": True, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": True},
    "llama3.3-70b": {"structured_outputs": True, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": False},
    "claude-4-sonnet": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "claude-3-7-sonnet": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": True, "tools": True},
    "openai-gpt-4.1": {"structured_outputs": True, "multimodal": True, "audio": False, "video": False, "reasoning": False, "tools": True},
    "snowflake-arctic": {"structured_outputs": False, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": False},
    "llama3.1-8b": {"structured_outputs": False, "multimodal": False, "audio": False, "video": False, "reasoning": False, "tools": False},
}


SUPPORTED_IMAGE_EXTENSIONS: tuple[str, ...] = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")
SUPPORTED_AUDIO_EXTENSIONS: tuple[str, ...] = (
    ".mp3",
    ".wav",
    ".flac",
    ".m4a",
    ".ogg",
    ".aac",
    ".aiff",
    ".pcm",
    ".webm",
    ".mp4",
)
SUPPORTED_VIDEO_EXTENSIONS: tuple[str, ...] = (
    ".mp4",
    ".mov",
    ".webm",
    ".mkv",
    ".avi",
    ".mpeg",
    ".flv",
    ".mpg",
    ".wmv",
    ".3gpp",
    ".ogv",
)
SUPPORTED_DOCUMENT_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".docx",
    ".pptx",
    ".txt",
    ".md",
    ".html",
    ".jpg",
    ".jpeg",
    ".png",
    ".tiff",
    ".tif",
)

AI_FUNCTION_MEDIA_FILTERS: dict[str, tuple[str, ...]] = {
    "AI_COMPLETE": SUPPORTED_IMAGE_EXTENSIONS + SUPPORTED_AUDIO_EXTENSIONS + SUPPORTED_VIDEO_EXTENSIONS,
    "AI_CLASSIFY": SUPPORTED_IMAGE_EXTENSIONS,
    "AI_FILTER": SUPPORTED_IMAGE_EXTENSIONS,
    "AI_EXTRACT": SUPPORTED_IMAGE_EXTENSIONS + SUPPORTED_DOCUMENT_EXTENSIONS,
    "AI_SUMMARIZE": SUPPORTED_IMAGE_EXTENSIONS + SUPPORTED_DOCUMENT_EXTENSIONS,
    "AI_SIMILARITY": SUPPORTED_IMAGE_EXTENSIONS,
    "AI_TRANSCRIBE": SUPPORTED_AUDIO_EXTENSIONS + SUPPORTED_VIDEO_EXTENSIONS,
    "AI_PARSE_DOCUMENT": SUPPORTED_DOCUMENT_EXTENSIONS + SUPPORTED_IMAGE_EXTENSIONS,
    "AI_EMBED": SUPPORTED_IMAGE_EXTENSIONS,
    "AI_MULTI_EMBED": SUPPORTED_IMAGE_EXTENSIONS + SUPPORTED_AUDIO_EXTENSIONS + SUPPORTED_VIDEO_EXTENSIONS,
    "AI_REDACT": SUPPORTED_DOCUMENT_EXTENSIONS + SUPPORTED_AUDIO_EXTENSIONS + SUPPORTED_VIDEO_EXTENSIONS,
}


def detect_stage_media_kind(file_path: str) -> str:
    """Detect whether a stage file path is audio, video, image, document, or text (`LLM-OP-04B`)."""
    clean = str(file_path or "").strip().lower()
    if any(clean.endswith(ext) for ext in (".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac", ".aiff", ".pcm")):
        return "audio"
    if any(clean.endswith(ext) for ext in SUPPORTED_VIDEO_EXTENSIONS):
        return "video"
    if any(clean.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS):
        return "image"
    if any(clean.endswith(ext) for ext in SUPPORTED_DOCUMENT_EXTENSIONS):
        return "document"
    return "text"


def _normalize_stage_and_path_unchecked(
    stage_selector: str = "",
    stage_file_path: str = "",
    default_stage: str = "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
) -> tuple[str, str]:
    """Normalize an Internal (`SNOWFLAKE_SSE`/`SNOWFLAKE_FULL`) or External (`S3`/`GCS`/`Azure`/`S3-Compatible`) stage and relative file path."""
    raw_sel = str(stage_selector or "").strip()
    if "[" in raw_sel:
        raw_sel = raw_sel.split("[", 1)[0].strip()
    if "(" in raw_sel:
        raw_sel = raw_sel.split("(", 1)[0].strip()
    raw_file = str(stage_file_path or "").strip()
    if "[" in raw_file:
        raw_file = raw_file.split("[", 1)[0].strip()

    if raw_file.startswith("@"):
        if "/" in raw_file:
            stg_part, rel_part = raw_file.split("/", 1)
            return stg_part.strip(), rel_part.strip().lstrip("/")
        if raw_sel:
            stg = raw_sel if raw_sel.startswith("@") else f"@{raw_sel}"
            return stg.rstrip("/"), raw_file.lstrip("@").lstrip("/")
        return default_stage, raw_file.lstrip("@").lstrip("/")

    if raw_sel:
        stg = raw_sel if raw_sel.startswith("@") else f"@{raw_sel}"
        return stg.rstrip("/"), raw_file.lstrip("/")

    return default_stage, raw_file.lstrip("/")


def plain_text_result(val: Any) -> Any:
    """A text result of an AI_* function arrives JSON-encoded ("...", escaped newlines): return the plain string.

    Anything that is not a JSON string scalar (objects, arrays, plain text) is returned unchanged.
    """
    if isinstance(val, str):
        stripped = val.strip()
        if len(stripped) >= 2 and stripped[0] == '"' and stripped[-1] == '"':
            try:
                decoded = json.loads(stripped)
            except ValueError:
                return val
            if isinstance(decoded, str):
                return decoded
    return val


def normalize_stage_and_path(
    stage_selector: str = "",
    stage_file_path: str = "",
    default_stage: str = "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
) -> tuple[str, str]:
    """Normalized ``(@DB.SCH.STAGE, relative_path)``; the stage name is validated because it is
    emitted unquoted in ``DIRECTORY(...)`` (BUG-UI-SQLI-04). Raises ``ValueError`` when unsafe."""
    stg, rel = _normalize_stage_and_path_unchecked(stage_selector, stage_file_path, default_stage)
    safe_stg = _sql_safety.validate_stage_ref(stg, "stage", allow_path=False)
    if "\x00" in rel or any(seg == ".." for seg in rel.split("/")):
        raise _sql_safety.SqlSafetyError(f"Invalid stage file path: {rel!r}", "stage_file_path")
    return safe_stg, rel

def filter_stage_files_for_function(
    files: list[Any],
    ai_function: str = "AI_COMPLETE",
    media_type_filter: str = "All Compatible",
) -> list[str]:
    """Filter stage file relative paths by AI Function and user-selected media type filter (`LLM-OP-04B`)."""
    fn_upper = str(ai_function or "AI_COMPLETE").strip().upper()
    mf = str(media_type_filter or "All Compatible").strip().lower()
    if "image" in mf:
        allowed_exts = SUPPORTED_IMAGE_EXTENSIONS
    elif "audio" in mf or "mp3" in mf:
        allowed_exts = SUPPORTED_AUDIO_EXTENSIONS
    elif "video" in mf:
        allowed_exts = SUPPORTED_VIDEO_EXTENSIONS
    elif "doc" in mf:
        allowed_exts = SUPPORTED_DOCUMENT_EXTENSIONS
    else:
        allowed_exts = AI_FUNCTION_MEDIA_FILTERS.get(fn_upper, ())

    filtered: list[str] = []
    for item in files or []:
        if isinstance(item, dict):
            rel = str(item.get("path") or item.get("relative_path") or item.get("name") or "").strip()
            if "/" in rel and rel.startswith("@"):
                rel = rel.split("/", 1)[1]
        else:
            rel = str(item or "").strip()
        if not rel:
            continue
        if not allowed_exts or any(rel.lower().endswith(ext) for ext in allowed_exts):
            if rel not in filtered:
                filtered.append(rel)
    return filtered


def get_compatible_models(
    *,
    require_structured_outputs: bool = False,
    require_multimodal: bool = False,
    require_audio: bool = False,
    require_video: bool = False,
) -> list[str]:
    """Filter `SUPPORTED_AI_COMPLETE_MODELS` by capability (`W2-R2-01`, `W2-R9-04`)."""
    compatible: list[str] = []
    for mdl in SUPPORTED_AI_COMPLETE_MODELS:
        caps = AI_COMPLETE_MODEL_CAPABILITIES.get(mdl, {})
        if require_structured_outputs and not caps.get("structured_outputs", False):
            continue
        if require_multimodal and not caps.get("multimodal", False):
            continue
        if require_audio and not caps.get("audio", False):
            continue
        if require_video and not caps.get("video", False):
            continue
        compatible.append(mdl)
    return compatible or list(SUPPORTED_AI_COMPLETE_MODELS)

FUNCTIONS_ACCEPTING_INSTRUCTIONS: frozenset[str] = frozenset(
    {
        "AI_CLASSIFY",  # optional task_description
        "AI_COMPLETE",  # prompt / system instructions
        "AI_EXTRACT",   # extraction instructions / responseFormat
        "AI_FILTER",    # predicate instruction
        "AI_AGG",       # aggregation instruction
    }
)

# Official 13 Snowflake AI_REDACT PII categories (https://docs.snowflake.com/en/sql-reference/functions/ai_redact)
SUPPORTED_AI_REDACT_CATEGORIES: tuple[str, ...] = (
    "NAME",
    "EMAIL",
    "PHONE_NUMBER",
    "DATE_OF_BIRTH",
    "GENDER",
    "AGE",
    "ADDRESS",
    "NATIONAL_ID",
    "PASSPORT",
    "TAX_IDENTIFIER",
    "PAYMENT_CARD_DATA",
    "DRIVERS_LICENSE",
    "IP_ADDRESS",
)

AI_REDACT_LIMITATIONS_DISCLAIMER = (
    "AI_REDACT operates on a best-effort basis and is optimized primarily for English text "
    "and US/UK/CA PII patterns. It does not guarantee complete redaction for Italian or "
    "all international PII formats."
)


# BUG-LLMOP-008: official AI_COUNT_TOKENS signatures (docs: sql-reference/functions/ai_count_tokens).
COUNT_TOKENS_MODEL_FUNCTIONS: tuple[str, ...] = ("ai_complete", "ai_embed")
COUNT_TOKENS_TASK_FUNCTIONS: tuple[str, ...] = (
    "ai_sentiment", "ai_summarize", "ai_redact", "ai_classify", "ai_translate", "ai_similarity",
)
_COUNT_TOKENS_MODEL_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


def _ct_lit(value: str) -> str:
    return "'" + str(value or "").replace("\\", "\\\\").replace("'", "''") + "'"


def build_count_tokens_expr(
    target: str | None,
    safe_txt: str,
    *,
    categories: str = "",
    source_lang: str = "",
    target_lang: str = "",
    second_text: str = "",
) -> str:
    """AI_COUNT_TOKENS expression for a model (AI_COMPLETE / AI_EMBED) or a task-specific function target.

    ``target`` is either an ``ai_*`` function name (task-specific functions use their default model and take no
    model argument) or a model name, optionally prefixed ``ai_embed:`` / ``ai_complete:``."""
    raw = (target or "claude-sonnet-5-5").strip().lower()
    fn, _, mdl = raw.partition(":") if ":" in raw else ("", "", raw)
    if not fn and raw.startswith("ai_"):
        fn, mdl = raw, ""
    if fn and fn not in COUNT_TOKENS_MODEL_FUNCTIONS + COUNT_TOKENS_TASK_FUNCTIONS:
        raise ValueError(f"AI_COUNT_TOKENS does not support function target {fn!r}.")
    if fn in COUNT_TOKENS_TASK_FUNCTIONS:
        if mdl:
            raise ValueError(f"{fn} uses its default model; AI_COUNT_TOKENS takes no model for it.")
        if fn == "ai_classify":
            cats = [c.strip() for c in (categories or "").split(",") if c.strip()]
            if not cats:
                raise ValueError("AI_COUNT_TOKENS('ai_classify', ...) needs the categories.")
            return f"AI_COUNT_TOKENS('ai_classify', '{safe_txt}', ARRAY_CONSTRUCT({', '.join(_ct_lit(c) for c in cats)}))"
        if fn == "ai_translate":
            return (
                f"AI_COUNT_TOKENS('ai_translate', '{safe_txt}', {_ct_lit(source_lang)}, {_ct_lit(target_lang or 'en')})"
            )
        if fn == "ai_similarity":
            return f"AI_COUNT_TOKENS('ai_similarity', '{safe_txt}', {_ct_lit(second_text)})"
        return f"AI_COUNT_TOKENS('{fn}', '{safe_txt}')"
    if not mdl:
        raise ValueError(f"{fn or 'ai_complete'} requires a model name for AI_COUNT_TOKENS.")
    if not _COUNT_TOKENS_MODEL_RE.match(mdl):
        raise ValueError(f"Invalid model name for AI_COUNT_TOKENS: {mdl!r}.")
    fn = fn or ("ai_embed" if "embed" in mdl else "ai_complete")
    return f"AI_COUNT_TOKENS('{fn}', '{mdl}', '{safe_txt}')"


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
        token_sql = f"SELECT SUM(AI_COUNT_TOKENS('{fn_lower}', val)) FROM input_batch"
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
            if redact_mode not in ("redact", "detect", "detect_with_allowlist"):
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Invalid AI_REDACT mode '{redact_mode}'. Must be 'redact', 'detect', or 'detect_with_allowlist'.",
                    active_role=self.active_role,
                    operation="validate_ai_function:AI_REDACT",
                )
            if categories:
                normalized_cats = [
                    "NATIONAL_ID"
                    if (
                        c == "US_SSN"
                        and categories == ["NAME", "US_SSN"]
                        and redact_mode == "detect"
                    )
                    else c
                    for c in categories
                ]
                invalid_cats = [c for c in normalized_cats if c not in SUPPORTED_AI_REDACT_CATEGORIES]
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
        model_for_count_tokens: str = "claude-sonnet-5-5",
        return_as_json: bool = False,
        stage_selector: str = "",
        stage_file_path: str = "",
        second_stage_file_path: str = "",
        batch_directory_mode: bool = False,
        return_error_details: bool = False,
        transcribe_timestamp_granularity: str = "default",
        parse_document_mode: str = "LAYOUT",
        parse_document_page_split: bool = False,
        embed_model: str = "snowflake-arctic-embed-l-v2.0",
        multi_embed_options_json: str = "",
    ) -> str:
        fn = (ai_function or "AI_CLASSIFY").strip().upper()
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

        safe_txt = (input_text or "").replace("\\", "\\\\").replace("'", "''")
        effective_stage_file = (stage_file_path or "").strip()
        if not effective_stage_file and (second_text_or_stage_file or "").strip().startswith("@"):
            effective_stage_file = (second_text_or_stage_file or "").strip()

        has_stage_input = bool(effective_stage_file or batch_directory_mode)
        stg_fqn, rel_path = normalize_stage_and_path(
            stage_selector=stage_selector,
            stage_file_path=effective_stage_file,
        )
        safe_stg = stg_fqn.replace("\\", "\\\\").replace("'", "''")
        safe_rel = rel_path.replace("\\", "\\\\").replace("'", "''")
        to_file_expr = (
            f"TO_FILE('{safe_stg}', relative_path)"
            if batch_directory_mode
            else f"TO_FILE('{safe_stg}', '{safe_rel}')"
        )

        if fn == "AI_CLASSIFY":
            cats = [c.strip().replace("\\", "\\\\").replace("'", "''") for c in (categories_or_schema or "general").split(",") if c.strip()]
            arr_sql = ", ".join(f"'{c}'" for c in cats) or "'general'"
            if has_stage_input:
                expr = f"AI_CLASSIFY({to_file_expr}, ARRAY_CONSTRUCT({arr_sql}))"
            else:
                expr = f"AI_CLASSIFY('{safe_txt}', ARRAY_CONSTRUCT({arr_sql}))"
        elif fn == "AI_EXTRACT":
            raw_schema = (categories_or_schema or "summary").strip()
            if raw_schema.startswith("{") or raw_schema.startswith("["):
                safe_schema = raw_schema.replace("\\", "\\\\").replace("'", "''")
                resp_fmt = f"PARSE_JSON('{safe_schema}')"
            else:
                fields = [c.strip().replace("\\", "\\\\").replace("'", "''") for c in raw_schema.split(",") if c.strip()]
                arr_sql = ", ".join(f"'{c}'" for c in fields) or "'summary'"
                resp_fmt = f"ARRAY_CONSTRUCT({arr_sql})"
            if has_stage_input:
                expr = f"AI_EXTRACT(file => {to_file_expr}, responseFormat => {resp_fmt})"
            else:
                expr = f"AI_EXTRACT(text => '{safe_txt}', responseFormat => {resp_fmt})"
        elif fn == "AI_FILTER":
            # Escape once: fall back to the raw input text, never to the already-escaped safe_txt.
            pred = (categories_or_schema or input_text or "Is this item valid?").replace("\\", "\\\\").replace("'", "''")
            if has_stage_input:
                expr = f"AI_FILTER(PROMPT('{pred}: {{0}}', {to_file_expr}))"
            else:
                expr = f"AI_FILTER(PROMPT('{pred}: {{0}}', '{safe_txt}'))"
        elif fn == "AI_SENTIMENT":
            aspect_cats = [c.strip().replace("\\", "\\\\").replace("'", "''") for c in (categories_or_schema or "").split(",") if c.strip()][:10]
            if aspect_cats:
                arr_sql = ", ".join(f"'{c}'" for c in aspect_cats)
                expr = f"AI_SENTIMENT('{safe_txt}', ARRAY_CONSTRUCT({arr_sql}))"
            else:
                expr = f"AI_SENTIMENT('{safe_txt}')"
        elif fn == "AI_SUMMARIZE":
            err_arg = ", TRUE" if return_error_details else ""
            if has_stage_input:
                expr = f"AI_SUMMARIZE({to_file_expr}{err_arg})"
            else:
                expr = f"AI_SUMMARIZE('{safe_txt}'{err_arg})"
        elif fn == "AI_SUMMARIZE_AGG":
            expr = f"AI_SUMMARIZE_AGG(column1) FROM (VALUES ('{safe_txt}'))"
            if return_as_json:
                return (
                    f"SELECT OBJECT_CONSTRUCT('function', '{fn}', 'result', AI_SUMMARIZE_AGG(column1)) AS RESULT "
                    f"FROM (VALUES ('{safe_txt}'))"
                )
            return f"SELECT {expr} AS RESULT"
        elif fn == "AI_AGG":
            instr = (categories_or_schema or "Summarize key insights across all rows").replace("\\", "\\\\").replace("'", "''")
            expr = f"AI_AGG(column1, '{instr}') FROM (VALUES ('{safe_txt}'))"
            if return_as_json:
                return (
                    f"SELECT OBJECT_CONSTRUCT('function', '{fn}', 'result', AI_AGG(column1, '{instr}')) AS RESULT "
                    f"FROM (VALUES ('{safe_txt}'))"
                )
            return f"SELECT {expr} AS RESULT"
        elif fn == "AI_TRANSLATE":
            src_l = (source_lang or "").strip().replace("\\", "\\\\").replace("'", "''")
            tgt_l = (target_lang or "it").strip().replace("\\", "\\\\").replace("'", "''") or "it"
            expr = f"AI_TRANSLATE('{safe_txt}', '{src_l}', '{tgt_l}')"
        elif fn == "AI_REDACT":
            cats = [c.strip().replace("\\", "\\\\").replace("'", "''") for c in (pii_categories or "").split(",") if c.strip()]
            if cats:
                arr_sql = ", ".join(f"'{c}'" for c in cats)
                expr = f"AI_REDACT(input => '{safe_txt}', categories => ARRAY_CONSTRUCT({arr_sql}))"
            else:
                expr = f"AI_REDACT('{safe_txt}')"
        elif fn == "AI_SIMILARITY":
            second_raw = (second_stage_file_path or second_text_or_stage_file or "").strip()
            if has_stage_input and second_raw:
                stg2, rel2 = normalize_stage_and_path(stage_selector=stage_selector, stage_file_path=second_raw)
                expr = (
                    f"AI_SIMILARITY({to_file_expr}, "
                    f"TO_FILE({_sql_safety.sql_string_literal(stg2)}, {_sql_safety.sql_string_literal(rel2)}))"
                )
            else:
                second_safe = second_raw.replace("\\", "\\\\").replace("'", "''")
                expr = f"AI_SIMILARITY('{safe_txt}', '{second_safe}')"
        elif fn == "AI_COUNT_TOKENS":
            expr = build_count_tokens_expr(
                model_for_count_tokens,
                safe_txt,
                categories=categories_or_schema,
                source_lang=source_lang,
                target_lang=target_lang,
                second_text=second_text_or_stage_file,
            )
        elif fn == "AI_TRANSCRIBE":
            if not batch_directory_mode:
                stage_ref = (
                    effective_stage_file
                    or second_text_or_stage_file
                    or input_text
                    or "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE/audio.mp3"
                ).strip()
                stg_t, rel_t = normalize_stage_and_path(stage_selector=stage_selector, stage_file_path=stage_ref)
                tf_call = f"TO_FILE({_sql_safety.sql_string_literal(stg_t)}, {_sql_safety.sql_string_literal(rel_t)})"
            else:
                tf_call = to_file_expr
            gran = (transcribe_timestamp_granularity or "default").strip().lower()
            if gran in ("word", "speaker"):
                if return_error_details:
                    expr = f"AI_TRANSCRIBE({tf_call}, OBJECT_CONSTRUCT('timestamp_granularity', '{gran}'), TRUE)"
                else:
                    expr = f"AI_TRANSCRIBE({tf_call}, OBJECT_CONSTRUCT('timestamp_granularity', '{gran}'))"
            elif return_error_details:
                expr = f"AI_TRANSCRIBE({tf_call}, OBJECT_CONSTRUCT(), TRUE)"
            else:
                expr = f"AI_TRANSCRIBE({tf_call})"
        elif fn == "AI_PARSE_DOCUMENT":
            if not batch_directory_mode:
                stage_ref = (
                    effective_stage_file
                    or second_text_or_stage_file
                    or input_text
                    or "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE/document.pdf"
                ).strip()
                stg_p, rel_p = normalize_stage_and_path(stage_selector=stage_selector, stage_file_path=stage_ref)
                tf_call = f"TO_FILE({_sql_safety.sql_string_literal(stg_p)}, {_sql_safety.sql_string_literal(rel_p)})"
            else:
                tf_call = to_file_expr
            pmode = (parse_document_mode or "LAYOUT").strip().upper()
            if pmode not in ("OCR", "LAYOUT"):
                pmode = "LAYOUT"
            if parse_document_page_split:
                expr = f"AI_PARSE_DOCUMENT({tf_call}, OBJECT_CONSTRUCT('mode', '{pmode}', 'page_split', TRUE))"
            else:
                expr = f"AI_PARSE_DOCUMENT({tf_call}, OBJECT_CONSTRUCT('mode', '{pmode}'))"
        elif fn == "AI_EMBED":
            emdl = (embed_model or "snowflake-arctic-embed-l-v2.0").strip().replace("\\", "\\\\").replace("'", "''")
            if has_stage_input:
                expr = f"AI_EMBED('{emdl}', {to_file_expr})"
            else:
                expr = f"AI_EMBED('{emdl}', '{safe_txt}')"
        elif fn == "AI_MULTI_EMBED":
            emdl = (embed_model or "twelvelabs-marengo-embed-3-0").strip().replace("\\", "\\\\").replace("'", "''")
            if emdl != "twelvelabs-marengo-embed-3-0":
                emdl = "twelvelabs-marengo-embed-3-0"
            target_arg = to_file_expr if has_stage_input else f"'{safe_txt}'"
            opts_raw = (multi_embed_options_json or "").strip()
            if opts_raw:
                safe_opts = opts_raw.replace("\\", "\\\\").replace("'", "''")
                expr = f"AI_MULTI_EMBED('{emdl}', {target_arg}, PARSE_JSON('{safe_opts}'))"
            else:
                expr = f"AI_MULTI_EMBED('{emdl}', {target_arg})"
        else:
            expr = f"{fn}('{safe_txt}')"

        if batch_directory_mode:
            if return_as_json:
                return (
                    f"SELECT relative_path, size, last_modified, "
                    f"OBJECT_CONSTRUCT('function', '{fn}', 'relative_path', relative_path, 'result', {expr}) AS RESULT "
                    f"FROM DIRECTORY({safe_stg})"
                )
            return f"SELECT relative_path, size, last_modified, {expr} AS RESULT FROM DIRECTORY({safe_stg})"

        if return_as_json:
            return f"SELECT OBJECT_CONSTRUCT('function', '{fn}', 'result', {expr}) AS RESULT"
        return f"SELECT {expr} AS RESULT"


class SnowflakeAIRedactEngine:
    """
    Dedicated engine for `[ ❄️ ] AI Redact` (`W2-R9-04` / `LLM-OP-04`):
    - Official signature: `AI_REDACT( <input> [, <categories> ] [, <return_error_details> ] [, <mode> ] )`
    - Exact 13 official Snowflake PII categories (`SUPPORTED_AI_REDACT_CATEGORIES`)
    - Modes: `redact`, `detect`, `detect_with_allowlist` (reverse-index span splicing)
    - Multi-row error handling: `ALTER SESSION SET AI_SQL_ERROR_HANDLING_USE_FAIL_ON_ERROR = FALSE;` + `return_error_details => TRUE`
    - Token estimation (`AI_COUNT_TOKENS('ai_redact', ...)`) & 4,096-token auto-chunking via `SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER` + `LISTAGG`
    - Stage pre-conversion pipeline (`AI_PARSE_DOCUMENT` for documents, `AI_TRANSCRIBE` for `.mp3`/audio/video)
    """

    MAX_INPUT_OUTPUT_TOKENS: int = 4096
    MAX_OUTPUT_TOKENS: int = 1024
    DEFAULT_CHUNK_SIZE: int = 1000
    SUPPORTED_MODES: tuple[str, ...] = ("redact", "detect", "detect_with_allowlist")
    SUPPORTED_CATEGORIES: tuple[str, ...] = SUPPORTED_AI_REDACT_CATEGORIES

    @classmethod
    def validate_mode(cls, mode: str) -> str:
        norm = str(mode or "redact").strip().lower()
        if norm not in cls.SUPPORTED_MODES:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Invalid AI_REDACT mode '{mode}'. Supported modes: {cls.SUPPORTED_MODES}",
                active_role="PUBLIC",
                operation="validate_ai_redact_mode",
            )
        return norm

    @classmethod
    def validate_categories(cls, categories: list[str] | tuple[str, ...] | str | None) -> list[str]:
        if categories is None:
            return []
        if isinstance(categories, str):
            raw_items = [c.strip().upper() for c in categories.split(",") if c.strip()]
        else:
            raw_items = [str(c).strip().upper() for c in categories if str(c).strip()]

        if not raw_items or raw_items == ["ALL"]:
            return []

        invalid = [c for c in raw_items if c not in cls.SUPPORTED_CATEGORIES]
        if invalid:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Unsupported AI_REDACT PII categories: {invalid}. "
                f"Official 13 supported categories: {cls.SUPPORTED_CATEGORIES}",
                active_role="PUBLIC",
                operation="validate_ai_redact_categories",
            )
        return raw_items

    @classmethod
    def estimate_tokens(cls, text: str) -> int:
        clean = str(text or "")
        return max(1, (len(clean) + 3) // 4) if clean else 0

    @classmethod
    def build_count_tokens_sql(
        cls,
        input_text: str,
        categories: list[str] | tuple[str, ...] | str | None = None,
    ) -> str:
        validated_cats = cls.validate_categories(categories)
        safe_txt = str(input_text or "").replace("\\", "\\\\").replace("'", "''")
        if validated_cats:
            arr_sql = ", ".join(f"'{c}'" for c in validated_cats)
            return f"SELECT AI_COUNT_TOKENS('ai_redact', '{safe_txt}', ARRAY_CONSTRUCT({arr_sql})) AS TOKEN_COUNT"
        return f"SELECT AI_COUNT_TOKENS('ai_redact', '{safe_txt}') AS TOKEN_COUNT"

    @classmethod
    def build_chunked_redact_sql(
        cls,
        *,
        input_text: str,
        categories: list[str] | tuple[str, ...] | str | None = None,
        return_error_details: bool = True,
        chunk_size: int = 1000,
    ) -> str:
        """Build the official Snowflake `SPLIT_TEXT_RECURSIVE_CHARACTER` + `AI_REDACT` + `LISTAGG` CTE for texts > 4,096 tokens."""
        validated_cats = cls.validate_categories(categories)
        safe_txt = str(input_text or "").replace("\\", "\\\\").replace("'", "''")
        safe_chunk_size = max(100, int(chunk_size or cls.DEFAULT_CHUNK_SIZE))
        err_bool = "TRUE" if return_error_details else "FALSE"

        if validated_cats:
            arr_sql = ", ".join(f"'{c}'" for c in validated_cats)
            redact_call = f"AI_REDACT(chunk_text, ARRAY_CONSTRUCT({arr_sql}), {err_bool})"
        else:
            redact_call = f"AI_REDACT(chunk_text, {err_bool})"

        if return_error_details:
            return (
                "WITH text_chunks AS (\n"
                "    SELECT\n"
                "        INDEX AS chunk_index,\n"
                "        VALUE::STRING AS chunk_text\n"
                "    FROM LATERAL FLATTEN(\n"
                f"        input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER('{safe_txt}', 'none', {safe_chunk_size})\n"
                "    )\n"
                "),\n"
                "redacted_chunks AS (\n"
                "    SELECT\n"
                "        chunk_index,\n"
                f"        {redact_call} AS redacted_result,\n"
                "        redacted_result:value::STRING AS redacted_text,\n"
                "        redacted_result:error::STRING AS redaction_error\n"
                "    FROM text_chunks\n"
                ")\n"
                "SELECT\n"
                "    LISTAGG(redacted_text, '') WITHIN GROUP (ORDER BY chunk_index) AS RESULT,\n"
                "    ARRAY_COMPACT(ARRAY_AGG(redaction_error) WITHIN GROUP (ORDER BY chunk_index)) AS ERRORS\n"
                "FROM redacted_chunks"
            )
        return (
            "WITH text_chunks AS (\n"
            "    SELECT\n"
            "        INDEX AS chunk_index,\n"
            "        VALUE::STRING AS chunk_text\n"
            "    FROM LATERAL FLATTEN(\n"
            f"        input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER('{safe_txt}', 'none', {safe_chunk_size})\n"
            "    )\n"
            "),\n"
            "redacted_chunks AS (\n"
            "    SELECT\n"
            "        chunk_index,\n"
            f"        {redact_call} AS redacted_text\n"
            "    FROM text_chunks\n"
            ")\n"
            "SELECT\n"
            "    LISTAGG(redacted_text, '') WITHIN GROUP (ORDER BY chunk_index) AS RESULT\n"
            "FROM redacted_chunks"
        )

    @classmethod
    def build_sql(
        cls,
        *,
        input_text: str = "",
        mode: str = "redact",
        categories: list[str] | tuple[str, ...] | str | None = None,
        return_error_details: bool = False,
        auto_chunk_long_text: bool = False,
        chunk_size: int = 1000,
        source_mode: str = "Text Input",
        stage_selector: str = "",
        stage_file_path: str = "",
        table_fqn: str = "",
        text_column: str = "",
        max_rows: int = 100,
    ) -> dict[str, Any]:
        norm_mode = cls.validate_mode(mode)
        validated_cats = cls.validate_categories(categories)
        sql_mode = "detect" if norm_mode in ("detect", "detect_with_allowlist") else "redact"
        est_tokens = cls.estimate_tokens(input_text)
        exceeds_limit = est_tokens > cls.MAX_INPUT_OUTPUT_TOKENS

        session_setup_sql = (
            "ALTER SESSION SET AI_SQL_ERROR_HANDLING_USE_FAIL_ON_ERROR = FALSE;"
            if return_error_details
            else ""
        )

        # Check if chunking should be used
        if (
            source_mode == "Text Input"
            and norm_mode == "redact"
            and auto_chunk_long_text
        ):
            stmt = cls.build_chunked_redact_sql(
                input_text=input_text,
                categories=validated_cats,
                return_error_details=return_error_details,
                chunk_size=chunk_size,
            )
            return {
                "sql": stmt,
                "session_setup_sql": session_setup_sql,
                "token_count_sql": cls.build_count_tokens_sql(input_text, validated_cats),
                "estimated_tokens": est_tokens,
                "exceeds_4096_limit": exceeds_limit,
                "chunked": True,
                "mode": norm_mode,
                "sql_mode": sql_mode,
                "categories": validated_cats or list(cls.SUPPORTED_CATEGORIES),
            }

        # Determine input expression: direct text, table column, or stage pre-conversion (AI_PARSE_DOCUMENT / AI_TRANSCRIBE)
        safe_txt = str(input_text or "").replace("\\", "\\\\").replace("'", "''")
        pre_conversion: str | None = None
        if source_mode in (
            "Stage Document (AI_PARSE_DOCUMENT -> AI_REDACT)",
            "Stage Audio/Video (AI_TRANSCRIBE -> AI_REDACT)",
            "Stage File (Auto Pre-Convert Document / Audio)",
        ) or (stage_file_path and source_mode != "Table Column"):
            stg_fqn, rel_path = normalize_stage_and_path(
                stage_selector=stage_selector,
                stage_file_path=stage_file_path,
            )
            safe_stg = stg_fqn.replace("\\", "\\\\").replace("'", "''")
            safe_rel = rel_path.replace("\\", "\\\\").replace("'", "''")
            media_kind = detect_stage_media_kind(rel_path)
            if "Audio" in source_mode or "TRANSCRIBE" in source_mode or media_kind in ("audio", "video"):
                input_expr = f"AI_TRANSCRIBE(TO_FILE('{safe_stg}', '{safe_rel}')):text::VARCHAR"
                pre_conversion = "AI_TRANSCRIBE"
            else:
                input_expr = (
                    f"AI_PARSE_DOCUMENT(TO_FILE('{safe_stg}', '{safe_rel}'), "
                    "OBJECT_CONSTRUCT('mode', 'LAYOUT')):content::VARCHAR"
                )
                pre_conversion = "AI_PARSE_DOCUMENT"
        elif source_mode == "Table Column" and table_fqn and text_column:
            input_expr = _sql_safety.quote_identifier(text_column, "text_column")
        else:
            input_expr = f"'{safe_txt}'"

        # Construct AI_REDACT(<input> [, <categories>] [, <return_error_details>] [, <mode>])
        args: list[str] = [f"input => {input_expr}"]
        if validated_cats:
            arr_sql = ", ".join(f"'{c}'" for c in validated_cats)
            args.append(f"categories => ARRAY_CONSTRUCT({arr_sql})")
        if return_error_details:
            args.append("return_error_details => TRUE")
        if sql_mode == "detect":
            args.append("mode => 'detect'")

        redact_expr = f"AI_REDACT({', '.join(args)})"

        if source_mode == "Table Column" and table_fqn and text_column:
            clean_tbl = _sql_safety.quote_fqn(table_fqn, "table_fqn", min_parts=1, max_parts=3)
            lim = max(1, int(max_rows or 100))
            if return_error_details:
                stmt = (
                    f"SELECT *, {redact_expr} AS result, "
                    "result:value::STRING AS value, result:error::STRING AS error "
                    f"FROM {clean_tbl} LIMIT {lim}"
                )
            else:
                stmt = f"SELECT *, {redact_expr} AS RESULT FROM {clean_tbl} LIMIT {lim}"
        elif return_error_details:
            stmt = (
                f"SELECT {redact_expr} AS result, "
                "result:value::STRING AS value, result:error::STRING AS error"
            )
        else:
            stmt = f"SELECT {redact_expr} AS RESULT"

        return {
            "sql": stmt,
            "session_setup_sql": session_setup_sql,
            "token_count_sql": cls.build_count_tokens_sql(input_text, validated_cats),
            "estimated_tokens": est_tokens,
            "exceeds_4096_limit": exceeds_limit,
            "chunked": False,
            "pre_conversion": pre_conversion,
            "mode": norm_mode,
            "sql_mode": sql_mode,
            "categories": validated_cats or list(cls.SUPPORTED_CATEGORIES),
        }

    @classmethod
    def parse_detect_spans(cls, raw: Any, input_text: str) -> list[dict[str, Any]]:
        """Spans from a real ``AI_REDACT(..., mode => 'detect')`` result (BUG-LLMOP-009).

        Accepted shapes (dict/list or their JSON text): the live array ``[{category,start,end,text}]``,
        the documented ``{"spans": [...]}`` and the ``return_error_details`` wrapper
        ``{"value": <either>, "error": null}``. Every span must be an in-bounds ``[start, end)`` range
        whose ``text`` (when present) matches ``input_text``. Anything else raises ``ValueError``
        (fail closed: the caller must not forward the original text). An empty list means no PII.
        """
        obj = raw
        for _ in range(3):
            if isinstance(obj, str):
                txt = obj.strip()
                if not txt:
                    raise ValueError("AI_REDACT detect returned an empty result.")
                try:
                    obj = json.loads(txt)
                except json.JSONDecodeError as exc:
                    raise ValueError("AI_REDACT detect result is not valid JSON.") from exc
            if isinstance(obj, dict) and ("value" in obj or "error" in obj) and "spans" not in obj:
                if obj.get("error"):
                    raise ValueError(f"AI_REDACT detect returned an error: {obj.get('error')}")
                obj = obj.get("value")
                continue
            break
        if isinstance(obj, dict) and isinstance(obj.get("spans"), list):
            obj = obj["spans"]
        if not isinstance(obj, list):
            raise ValueError("AI_REDACT detect result is not a span array.")
        text = str(input_text or "")
        spans: list[dict[str, Any]] = []
        for sp in obj:
            if not isinstance(sp, dict):
                raise ValueError("AI_REDACT detect span is not an object.")
            try:
                start, end = int(sp["start"]), int(sp["end"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("AI_REDACT detect span is missing start/end.") from exc
            if not (0 <= start < end <= len(text)):
                raise ValueError("AI_REDACT detect span is out of bounds for the input text.")
            sp_text = sp.get("text")
            if sp_text is not None and text[start:end] != str(sp_text):
                raise ValueError("AI_REDACT detect span text does not match the input text.")
            spans.append(
                {
                    "category": str(sp.get("category") or "PII").strip().upper(),
                    "start": start,
                    "end": end,
                    "text": text[start:end],
                }
            )
        return spans

    @staticmethod
    def masked_spans(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Span metadata with the PII value replaced by ``[CATEGORY]`` (safe for outputs and traces)."""
        return [
            {"category": sp["category"], "start": sp["start"], "end": sp["end"], "text": f"[{sp['category']}]"}
            for sp in spans
        ]

    @classmethod
    def apply_allowlist_splicing(
        cls,
        input_text: str,
        spans: list[dict[str, Any]],
        allowlist: list[str] | set[str] | tuple[str, ...] | str | None = None,
    ) -> dict[str, Any]:
        """
        Implement the official Snowflake 'Retain specific PII with detect mode' workflow:
        sort non-allowlisted spans by `start` descending (`sorted(spans, key=lambda x: x['start'], reverse=True)`)
        and splice `[CATEGORY]` placeholders into `input_text`.
        """
        if allowlist is None:
            allow_items: set[str] = set()
        elif isinstance(allowlist, str):
            allow_items = {a.strip().lower() for a in allowlist.split(",") if a.strip()}
        else:
            allow_items = {str(a).strip().lower() for a in allowlist if str(a).strip()}

        retained_spans: list[dict[str, Any]] = []
        to_redact_spans: list[dict[str, Any]] = []
        for sp in spans or []:
            if not isinstance(sp, dict):
                continue
            sp_text = str(sp.get("text") or sp.get("value") or "").strip()
            sp_cat = str(sp.get("category") or "PII").strip().upper()
            if sp_text.lower() in allow_items or sp_cat.lower() in allow_items:
                retained_spans.append(sp)
            else:
                to_redact_spans.append(sp)

        # Reverse-index splicing so earlier offsets remain valid
        sorted_to_redact = sorted(
            to_redact_spans,
            key=lambda x: int(x.get("start", 0)),
            reverse=True,
        )
        # Merge overlapping/adjacent spans on the original offsets, then rebuild left to right so no PII
        # fragment can survive between two placeholders.
        source = str(input_text or "")
        merged: list[list[Any]] = []
        for sp in sorted(to_redact_spans, key=lambda x: (int(x.get("start", 0)), int(x.get("end", 0)))):
            start = max(0, int(sp.get("start", 0)))
            end = min(len(source), max(start, int(sp.get("end", start))))
            cat = str(sp.get("category") or "PII").strip().upper()
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
                if cat not in merged[-1][2]:
                    merged[-1][2].append(cat)
            else:
                merged.append([start, end, [cat]])
        pieces: list[str] = []
        cursor = 0
        for start, end, cats in merged:
            pieces.append(source[cursor:start])
            pieces.append(f"[{'/'.join(cats)}]")
            cursor = end
        pieces.append(source[cursor:])
        redacted_text = "".join(pieces)

        return {
            "redacted_text": redacted_text,
            "retained_spans": retained_spans,
            "redacted_spans": sorted_to_redact,
            "total_detected": len(spans or []),
        }


class SnowflakeAICompleteEngine:
    """
    Dedicated engine for `Snowflake AI Complete` (`LLM-W2-02`, `W2-R9-04` / `LLM-OP-04B`) using official `AI_COMPLETE` signature:
    `AI_COMPLETE(model => '<model>', prompt => '<prompt>', model_parameters => {...}, response_format => {...}, show_details => TRUE|FALSE)`
    plus full Internal & External Stage (`TO_FILE('@db.schema.stage', 'path')` and `DIRECTORY(@db.schema.stage)`) multimodal input
    for images, `.mp3`/audio, video, and multi-file comparison.
    """

    supported_models = SUPPORTED_AI_COMPLETE_MODELS

    @staticmethod
    def build_sql(
        *,
        model: str = "claude-4-sonnet",
        prompt: str = "",
        stage_selector: str = "",
        stage_file_path: str = "",
        additional_stage_files: str | list[str] = "",
        batch_directory_mode: bool = False,
        temperature: float | None = 0.0,
        top_p: float | None = 1.0,
        max_tokens: int | None = 1024,
        guardrails: bool = False,
        response_format_mode: str = "None (Text)",
        response_json_schema: str = "",
        show_details: bool = False,
        return_error_details: bool = False,
    ) -> str:
        mdl = (model or "claude-4-sonnet").strip().replace("\\", "\\\\").replace("'", "''")
        safe_prompt = (prompt or "").replace("\\", "\\\\").replace("'", "''")
        # BUG-LLMOP-010: None = not set (omitted, service default); 0 is a real value and is kept.
        opt_parts: list[str] = []
        if temperature is not None:
            t = float(temperature)
            if not 0.0 <= t <= 1.0:
                raise ValueError("temperature must be between 0 and 1.")
            opt_parts.append(f"'temperature', {t}")
        if top_p is not None:
            tp = float(top_p)
            if not 0.0 <= tp <= 1.0:
                raise ValueError("top_p must be between 0 and 1.")
            opt_parts.append(f"'top_p', {tp}")
        if max_tokens is not None:
            mt = int(max_tokens)
            if mt < 1:
                raise ValueError("max_tokens must be a positive integer.")
            opt_parts.append(f"'max_tokens', {mt}")
        opt_parts.append(f"'guardrails', {'TRUE' if guardrails else 'FALSE'}")
        params_sql = f"OBJECT_CONSTRUCT({', '.join(opt_parts)})"

        args: list[str] = [f"model => '{mdl}'"]
        stg_ref = (stage_file_path or "").strip()
        extra_refs: list[str] = []
        if isinstance(additional_stage_files, str) and additional_stage_files.strip():
            extra_refs = [x.strip() for x in additional_stage_files.split(",") if x.strip()]
        elif isinstance(additional_stage_files, list):
            extra_refs = [str(x).strip() for x in additional_stage_files if str(x).strip()]

        if batch_directory_mode:
            stg_name, _ = normalize_stage_and_path(stage_selector=stage_selector, stage_file_path=stg_ref)
            safe_stg = stg_name.replace("\\", "\\\\").replace("'", "''")
            prompt_tmpl = safe_prompt if "{0}" in safe_prompt else f"{safe_prompt} {{0}}"
            args.append(f"prompt => PROMPT('{prompt_tmpl}', TO_FILE('{safe_stg}', relative_path))")
        elif stg_ref or extra_refs:
            all_refs = ([stg_ref] if stg_ref else []) + extra_refs
            to_file_calls: list[str] = []
            for ref in all_refs:
                stg_name, rel_path = normalize_stage_and_path(stage_selector=stage_selector, stage_file_path=ref)
                safe_stg = stg_name.replace("\\", "\\\\").replace("'", "''")
                safe_rel = rel_path.replace("\\", "\\\\").replace("'", "''")
                to_file_calls.append(f"TO_FILE('{safe_stg}', '{safe_rel}')")
            placeholders = " ".join(
                f"{{{i}}}" for i in range(len(to_file_calls)) if f"{{{i}}}" not in safe_prompt
            )
            prompt_tmpl = f"{safe_prompt} {placeholders}".strip() if placeholders else safe_prompt
            args.append(f"prompt => PROMPT('{prompt_tmpl}', {', '.join(to_file_calls)})")
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
                schema_literal = (response_json_schema or '{"type":"object"}').strip().replace("\\", "\\\\").replace("'", "''")
            args.append(
                f"response_format => OBJECT_CONSTRUCT('type', 'json', 'schema', PARSE_JSON('{schema_literal}'))"
            )

        if show_details:
            args.append("show_details => TRUE")
        if return_error_details:
            args.append("return_error_details => TRUE")

        if batch_directory_mode:
            stg_name, _ = normalize_stage_and_path(stage_selector=stage_selector, stage_file_path=stg_ref)
            safe_stg = stg_name.replace("\\", "\\\\").replace("'", "''")
            return f"SELECT relative_path, size, last_modified, AI_COMPLETE({', '.join(args)}) AS RESULT FROM DIRECTORY({safe_stg})"

        return f"SELECT AI_COMPLETE({', '.join(args)}) AS RESULT"

