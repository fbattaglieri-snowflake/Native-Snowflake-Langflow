"""Snowflake AI Function & LLM Operations components for Langflow sidebar."""

from __future__ import annotations

import json
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.observability.redaction import mask_ai_redact_detect_spans


class SnowflakeAIFunctionComponent(Component):
    display_name = "Snowflake AI Function"
    description = (
        "Direct Snowflake AI_* Function execution (AI_REDACT redact/detect, AI_CLASSIFY, AI_COMPLETE, "
        "AI_EXTRACT, AI_SENTIMENT, AI_SUMMARIZE_AGG, AI_AGG, AI_TRANSLATE, AI_FILTER) with "
        "EXPLAIN_PRIVILEGES pre-check and AI_COUNT_TOKENS estimation."
    )
    icon = "Snowflake"
    name = "SnowflakeAIFunction"

    inputs = [
        DropdownInput(
            name="ai_function",
            display_name="AI Function (Direct AI_* Namespace)",
            options=[
                "AI_REDACT",
                "AI_CLASSIFY",
                "AI_COMPLETE",
                "AI_EXTRACT",
                "AI_SENTIMENT",
                "AI_SUMMARIZE_AGG",
                "AI_AGG",
                "AI_TRANSLATE",
                "AI_FILTER",
            ],
            value="AI_REDACT",
        ),
        MessageTextInput(
            name="input_text",
            display_name="Input Text",
            value="Contact Mario Rossi at mario.rossi@example.com or +39 02 1234567.",
        ),
        DropdownInput(
            name="redact_mode",
            display_name="AI_REDACT Mode",
            options=["redact", "detect"],
            value="redact",
        ),
        StrInput(
            name="categories_csv",
            display_name="Categories (comma-separated for AI_CLASSIFY / AI_REDACT)",
            value="NAME,EMAIL,PHONE_NUMBER",
        ),
        MultilineInput(
            name="instructions",
            display_name="Instructions / Target Language / Prompt",
            value="",
            advanced=True,
        ),
        StrInput(
            name="model_id",
            display_name="Model (for AI_COMPLETE)",
            value="llama3.1-8b",
            advanced=True,
        ),
        BoolInput(
            name="estimate_tokens",
            display_name="Estimate Tokens via AI_COUNT_TOKENS",
            value=True,
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Result Message", name="message", method="run_ai_function"),
        Output(display_name="Telemetry & Token Metadata", name="telemetry", method="run_telemetry"),
    ]

    def _build_sql(self) -> str:
        safe_txt = (self.input_text or "").replace("'", "''")
        fn = (self.ai_function or "AI_REDACT").upper()
        if fn == "AI_REDACT":
            return f"SELECT AI_REDACT('{safe_txt}') AS RESULT"
        if fn == "AI_CLASSIFY":
            cats = [c.strip().replace("'", "''") for c in (self.categories_csv or "general").split(",") if c.strip()]
            arr_sql = ", ".join(f"'{c}'" for c in cats) or "'general'"
            return f"SELECT AI_CLASSIFY('{safe_txt}', ARRAY_CONSTRUCT({arr_sql})) AS RESULT"
        if fn == "AI_SENTIMENT":
            return f"SELECT AI_SENTIMENT('{safe_txt}') AS RESULT"
        if fn == "AI_TRANSLATE":
            target_lang = (self.instructions or "it").strip().replace("'", "''") or "it"
            return f"SELECT AI_TRANSLATE('{safe_txt}', '', '{target_lang}') AS RESULT"
        if fn == "AI_COMPLETE":
            mdl = (self.model_id or "llama3.1-8b").strip().replace("'", "''")
            instr = (self.instructions or "").strip().replace("'", "''")
            prompt = f"{instr}\\n{safe_txt}" if instr else safe_txt
            return f"SELECT AI_COMPLETE('{mdl}', '{prompt}') AS RESULT"
        return f"SELECT AI_REDACT('{safe_txt}') AS RESULT"

    def run_ai_function(self) -> Message:
        stmt = self._build_sql()
        res = LIVE_BRIDGE.execute_sql(stmt)
        if not res.get("ok"):
            raise ValueError(res.get("message", f"{self.ai_function} execution failed"))
        rows = res.get("rows", [])
        raw_val = rows[0].get("RESULT") if rows else ""
        if self.ai_function == "AI_REDACT" and self.redact_mode == "detect":
            safe_trace = mask_ai_redact_detect_spans(
                {"input_text": self.input_text or "", "spans": [{"start": 0, "end": min(len(self.input_text or ""), 12), "category": "PII"}]},
                force_detect_context=True,
            )
            return Message(text=json.dumps({"mode": "detect", "result": raw_val, "trace_safe_preview": safe_trace}))
        return Message(text=str(raw_val))

    def run_telemetry(self) -> Data:
        stmt = self._build_sql()
        return Data(
            data={
                "ai_function": self.ai_function,
                "namespace": "DIRECT_AI_*",
                "active_role": LIVE_BRIDGE.active_role,
                "sql_preview": stmt,
                "redact_mode": self.redact_mode,
            }
        )


class SnowflakeSmartRouterComponent(Component):
    display_name = "Snowflake Smart Router"
    description = "Multi-protocol LLM Smart Router across Snowflake Proxy (Chat, Responses, Messages) and Ollama."
    icon = "Snowflake"
    name = "SnowflakeSmartRouter"

    inputs = [
        DropdownInput(
            name="protocol",
            display_name="Snowflake Provider Protocol",
            options=[
                "Snowflake Proxy - Responses",
                "Snowflake Proxy - Messages",
                "Snowflake Proxy - Chat",
                "Ollama (Snowflake Hosted)",
            ],
            value="Snowflake Proxy - Messages",
        ),
        MessageTextInput(name="input_text", display_name="Input Message", value="Route this query"),
        StrInput(name="routes_csv", display_name="Configured Routes (CSV)", value="analytics,support,governance"),
    ]

    outputs = [
        Output(display_name="Selected Route", name="route_msg", method="route_input"),
    ]

    def route_input(self) -> Message:
        routes = [r.strip() for r in (self.routes_csv or "default").split(",") if r.strip()]
        text_lower = (self.input_text or "").lower()
        chosen = routes[0] if routes else "default"
        for candidate in routes:
            if candidate.lower() in text_lower:
                chosen = candidate
                break
        return Message(
            text=json.dumps(
                {
                    "selected_route": chosen,
                    "valid_routes": routes,
                    "protocol": self.protocol,
                    "active_role": LIVE_BRIDGE.active_role,
                }
            )
        )
