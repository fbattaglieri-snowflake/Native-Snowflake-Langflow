from __future__ import annotations

import json
from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import BoolInput, DropdownInput, MessageTextInput, Output, StrInput
    from lfx.schema import Data
    from lfx.schema.message import Message
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import (
        BoolInput,
        Component,
        Data,
        DropdownInput,
        Message,
        MessageTextInput,
        Output,
        StrInput,
    )

from lfx_snowflake.components.ai_function import (
    TASK_SPECIFIC_AI_FUNCTIONS,
    SnowflakeAIFunctionComponent as DomainAIFunctionEngine,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.observability.redaction import mask_ai_redact_detect_spans


AI_FUNCTION_VISIBLE_FIELDS: dict[str, tuple[str, ...]] = {
    "AI_SENTIMENT": ("input_text", "categories_or_schema", "return_as_json", "estimate_tokens"),
    "AI_TRANSLATE": ("input_text", "source_lang", "target_lang", "return_as_json", "estimate_tokens"),
    "AI_REDACT": ("input_text", "redact_mode", "pii_categories", "return_as_json", "estimate_tokens"),
    "AI_CLASSIFY": ("input_text", "categories_or_schema", "second_text_or_stage_file", "return_as_json", "estimate_tokens"),
    "AI_EXTRACT": ("input_text", "categories_or_schema", "second_text_or_stage_file", "return_as_json", "estimate_tokens"),
    "AI_FILTER": ("input_text", "categories_or_schema", "second_text_or_stage_file", "return_as_json", "estimate_tokens"),
    "AI_SUMMARIZE_AGG": ("input_text", "return_as_json", "estimate_tokens"),
    "AI_SIMILARITY": ("input_text", "second_text_or_stage_file", "return_as_json", "estimate_tokens"),
    "AI_COUNT_TOKENS": ("input_text", "model_for_count_tokens", "return_as_json"),
    "AI_TRANSCRIBE": ("second_text_or_stage_file", "return_as_json"),
}


class SnowflakeAIFunctionComponent(Component):
    display_name = "AI Function"
    description = (
        "Task-specific Snowflake AI_* Function execution (AI_CLASSIFY, AI_EXTRACT, AI_FILTER, "
        "AI_SENTIMENT, AI_SUMMARIZE_AGG, AI_TRANSLATE, AI_REDACT, AI_SIMILARITY, AI_COUNT_TOKENS, AI_TRANSCRIBE) "
        "with dynamic per-function field adaptation and Return as JSON Object (OBJECT_CONSTRUCT) toggle."
    )
    icon = "Snowflake"
    name = "SnowflakeAIFunction"

    inputs = [
        DropdownInput(
            name="ai_function",
            display_name="Task-Specific AI Function",
            options=list(TASK_SPECIFIC_AI_FUNCTIONS),
            value="AI_REDACT",
            real_time_refresh=True,
        ),
        MessageTextInput(
            name="input_text",
            display_name="Input Text",
            value="Contact Mario Rossi at mario.rossi@example.com or +39 02 1234567.",
            show=True,
        ),
        StrInput(
            name="categories_or_schema",
            display_name="Categories / Aspect Categories / Extraction Schema / Filter Predicate",
            value="NAME,EMAIL,PHONE_NUMBER",
            show=False,
        ),
        StrInput(
            name="source_lang",
            display_name="Source Language (for AI_TRANSLATE, empty = auto)",
            value="",
            advanced=False,
            show=False,
        ),
        StrInput(
            name="target_lang",
            display_name="Target Language (for AI_TRANSLATE)",
            value="it",
            advanced=False,
            show=False,
        ),
        DropdownInput(
            name="redact_mode",
            display_name="AI_REDACT Mode",
            options=["redact", "detect"],
            value="redact",
            show=True,
        ),
        StrInput(
            name="pii_categories",
            display_name="PII Categories CSV (for AI_REDACT)",
            value="NAME,EMAIL,PHONE_NUMBER",
            show=True,
        ),
        StrInput(
            name="second_text_or_stage_file",
            display_name="Second Text (AI_SIMILARITY) or @Stage/File (AI_TRANSCRIBE / Multimodal)",
            value="",
            show=False,
        ),
        StrInput(
            name="model_for_count_tokens",
            display_name="Model / Function Target for AI_COUNT_TOKENS",
            value="claude-sonnet-5-5",
            advanced=False,
            show=False,
        ),
        BoolInput(
            name="return_as_json",
            display_name="Return as JSON Object (OBJECT_CONSTRUCT)",
            value=False,
            show=True,
        ),
        BoolInput(
            name="estimate_tokens",
            display_name="Estimate Tokens via AI_COUNT_TOKENS",
            value=True,
            advanced=True,
            show=True,
        ),
    ]
    outputs = [
        Output(display_name="Result Message", name="message", method="run_ai_function"),
        Output(display_name="Telemetry & Token Metadata", name="telemetry", method="run_telemetry"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        selected_fn = (
            field_value
            if field_name == "ai_function" and isinstance(field_value, str)
            else build_config.get("ai_function", {}).get("value")
            or getattr(self, "ai_function", "AI_REDACT")
        ).strip().upper()
        visible = set(AI_FUNCTION_VISIBLE_FIELDS.get(selected_fn, ("input_text", "return_as_json")))
        managed_fields = (
            "input_text",
            "categories_or_schema",
            "source_lang",
            "target_lang",
            "redact_mode",
            "pii_categories",
            "second_text_or_stage_file",
            "model_for_count_tokens",
            "return_as_json",
            "estimate_tokens",
        )
        for fname in managed_fields:
            if fname in build_config:
                build_config[fname]["show"] = fname in visible
        return build_config

    def _build_sql(self) -> str:
        cats_or_schema = (
            getattr(self, "categories_or_schema", None)
            or getattr(self, "categories_csv", None)
            or getattr(self, "instructions", "")
            or ""
        )
        return DomainAIFunctionEngine.build_task_specific_sql(
            ai_function=self.ai_function or "AI_REDACT",
            input_text=self.input_text or "",
            categories_or_schema=cats_or_schema,
            source_lang=getattr(self, "source_lang", "") or "",
            target_lang=getattr(self, "target_lang", None) or getattr(self, "instructions", "it") or "it",
            pii_categories=getattr(self, "pii_categories", "") or "",
            second_text_or_stage_file=getattr(self, "second_text_or_stage_file", "") or "",
            model_for_count_tokens=getattr(self, "model_for_count_tokens", "claude-sonnet-5-5") or "claude-sonnet-5-5",
            return_as_json=bool(getattr(self, "return_as_json", False)),
        )

    def run_ai_function(self) -> Message:
        stmt = self._build_sql()
        res = LIVE_BRIDGE.execute_sql(stmt)
        if not res.get("ok"):
            raise ValueError(res.get("message", f"{self.ai_function} execution failed"))
        rows = res.get("rows", [])
        raw_val = rows[0].get("RESULT") if rows else ""
        if self.ai_function == "AI_REDACT" and getattr(self, "redact_mode", "redact") == "detect":
            safe_trace = mask_ai_redact_detect_spans(
                {
                    "input_text": self.input_text or "",
                    "spans": [{"start": 0, "end": min(len(self.input_text or ""), 12), "category": "PII"}],
                },
                force_detect_context=True,
            )
            return Message(text=json.dumps({"mode": "detect", "result": raw_val, "trace_safe_preview": safe_trace}))
        return Message(text=str(raw_val) if not isinstance(raw_val, (dict, list)) else json.dumps(raw_val))

    def run_telemetry(self) -> Data:
        return Data(
            data={
                "ai_function": self.ai_function,
                "namespace": "DIRECT_AI_*",
                "active_role": LIVE_BRIDGE.active_role,
                "return_as_json": bool(getattr(self, "return_as_json", False)),
                "sql_preview": self._build_sql(),
                "redact_mode": getattr(self, "redact_mode", "redact"),
            }
        )
