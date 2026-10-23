from __future__ import annotations

import json
from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import BoolInput, DropdownInput, FloatInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
    from lfx.schema import Data
    from lfx.schema.message import Message
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import (
        BoolInput,
        Component,
        Data,
        DropdownInput,
        FloatInput,
        IntInput,
        Message,
        MessageTextInput,
        MultilineInput,
        Output,
        StrInput,
    )

from lfx_snowflake.components.ai_function import (
    SUPPORTED_AI_COMPLETE_MODELS,
    SnowflakeAICompleteEngine,
    get_compatible_models,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


class SnowflakeAICompleteComponent(Component):
    display_name = "AI Complete"
    description = (
        "Dedicated Snowflake AI_COMPLETE generative component with capability-filtered model catalog, "
        "model_parameters (temperature, top_p, max_tokens, guardrails), structured JSON Schema response_format, "
        "optional multimodal TO_FILE('@db.schema.stage', 'path'), show_details, and return_error_details."
    )
    icon = "Snowflake"
    name = "SnowflakeAIComplete"

    inputs = [
        DropdownInput(
            name="model",
            display_name="Model",
            options=list(SUPPORTED_AI_COMPLETE_MODELS),
            value="claude-sonnet-5-5",
            real_time_refresh=True,
            combobox=True,
        ),
        MessageTextInput(
            name="prompt",
            display_name="Prompt",
            value="Summarize the operational status of this workflow.",
        ),
        StrInput(
            name="stage_file_path",
            display_name="Optional Stage File (@DB.SCHEMA.STAGE/path for TO_FILE)",
            value="",
            real_time_refresh=True,
            advanced=False,
        ),
        FloatInput(
            name="temperature",
            display_name="Temperature",
            value=0.0,
        ),
        FloatInput(
            name="top_p",
            display_name="Top P",
            value=1.0,
            advanced=True,
        ),
        IntInput(
            name="max_tokens",
            display_name="Max Tokens",
            value=1024,
        ),
        BoolInput(
            name="guardrails",
            display_name="Enable Cortex Guardrails",
            value=False,
        ),
        DropdownInput(
            name="response_format_mode",
            display_name="Response Format Mode",
            options=["None (Text)", "JSON Schema Object", "Structured Decision Schema"],
            value="None (Text)",
            real_time_refresh=True,
        ),
        MultilineInput(
            name="response_json_schema",
            display_name="Response JSON Schema (when JSON Schema mode selected)",
            value='{"type": "object", "properties": {"summary": {"type": "string"}}, "required": ["summary"]}',
            advanced=False,
            show=False,
        ),
        BoolInput(
            name="show_details",
            display_name="Show Details (usage, model, finish_reason)",
            value=False,
            advanced=True,
        ),
        BoolInput(
            name="return_error_details",
            display_name="Return Error Details",
            value=False,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="AI Complete Output", name="message", method="run_ai_complete"),
        Output(display_name="Structured Details & SQL", name="details", method="get_details"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        fmt_mode = (
            field_value
            if field_name == "response_format_mode" and isinstance(field_value, str)
            else build_config.get("response_format_mode", {}).get("value")
            or getattr(self, "response_format_mode", "None (Text)")
        ).strip()
        stg_file = (
            field_value
            if field_name == "stage_file_path" and isinstance(field_value, str)
            else build_config.get("stage_file_path", {}).get("value")
            or getattr(self, "stage_file_path", "")
        ).strip()

        req_structured = fmt_mode not in ("None (Text)", "Text (Default)", "")
        req_multimodal = bool(stg_file)
        compatible = get_compatible_models(
            require_structured_outputs=req_structured,
            require_multimodal=req_multimodal,
        )
        if "model" in build_config:
            build_config["model"]["options"] = compatible
            cur_val = build_config["model"].get("value")
            if cur_val and cur_val not in compatible and compatible:
                build_config["model"]["value"] = compatible[0]
        if "response_json_schema" in build_config:
            build_config["response_json_schema"]["show"] = fmt_mode == "JSON Schema Object"
        return build_config

    def _build_sql(self) -> str:
        return SnowflakeAICompleteEngine.build_sql(
            model=self.model or "claude-sonnet-5-5",
            prompt=self.prompt or "",
            stage_file_path=getattr(self, "stage_file_path", "") or "",
            temperature=float(getattr(self, "temperature", 0.0) or 0.0),
            top_p=float(getattr(self, "top_p", 1.0) or 1.0),
            max_tokens=int(getattr(self, "max_tokens", 1024) or 1024),
            guardrails=bool(getattr(self, "guardrails", False)),
            response_format_mode=getattr(self, "response_format_mode", "None (Text)") or "None (Text)",
            response_json_schema=getattr(self, "response_json_schema", "") or "",
            show_details=bool(getattr(self, "show_details", False)),
            return_error_details=bool(getattr(self, "return_error_details", False)),
        )

    def run_ai_complete(self) -> Message:
        stmt = self._build_sql()
        res = LIVE_BRIDGE.execute_sql(stmt)
        if not res.get("ok"):
            if getattr(self, "return_error_details", False):
                return Message(text=json.dumps({"error": res, "sql_preview": stmt}, indent=2))
            raise ValueError(res.get("message", "AI_COMPLETE execution failed"))
        rows = res.get("rows", [])
        val = rows[0].get("RESULT") if rows else ""
        return Message(text=str(val) if not isinstance(val, (dict, list)) else json.dumps(val, indent=2))

    def get_details(self) -> Data:
        return Data(
            data={
                "function": "AI_COMPLETE",
                "model": self.model,
                "response_format_mode": self.response_format_mode,
                "show_details": bool(self.show_details),
                "return_error_details": bool(self.return_error_details),
                "active_role": LIVE_BRIDGE.active_role,
                "sql_preview": self._build_sql(),
            }
        )
