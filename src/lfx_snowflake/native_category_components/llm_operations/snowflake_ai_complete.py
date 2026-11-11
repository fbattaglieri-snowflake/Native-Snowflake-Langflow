from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import (
    BoolInput,
    DropdownInput,
    FloatInput,
    HandleInput,
    IntInput,
    MessageTextInput,
    MultilineInput,
    Output,
    StrInput,
)
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.ai_function import (
    SUPPORTED_AI_COMPLETE_MODELS,
    SnowflakeAICompleteEngine,
    detect_stage_media_kind,
    filter_stage_files_for_function,
    get_compatible_models,
)
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.flow_context_guard import flow_component_sql as _flow_component_sql
from lfx_snowflake.flow_context_guard import raise_if_flow_context_denied as _raise_if_flow_context_denied


def _flow_component_execute_sql(*args, **kwargs):
    """BUG-CTX-03: this component's SQL is checked against the flow's `[ ❄️ Context ]`."""
    with _flow_component_sql():
        # E2E T1: a guard refusal surfaces as a component error carrying AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT.
        return _raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(*args, **kwargs))


def _optional_number(value: Any, cast: Any) -> Any:
    """BUG-LLMOP-010: ``None``/blank means "not set"; ``0`` is a real value."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return cast(value)


class SnowflakeAICompleteComponent(Component):
    display_name = "AI Complete"
    description = (
        "Dedicated Snowflake AI_COMPLETE generative component with capability-filtered model catalog "
        "(including gemini-3.1-pro, gemini-3.1-flash-lite, gemini-3.5-flash for .mp3/audio and video), "
        "model_parameters (temperature, top_p, max_tokens, guardrails), structured JSON Schema response_format, "
        "full Internal & External Stage multimodal TO_FILE('@db.schema.stage', 'path') + DIRECTORY(@stage) batch mode, "
        "multi-file comparison, show_details, and return_error_details."
    )
    icon = "Snowflake"
    name = "SnowflakeAIComplete"

    inputs = [
        HandleInput(
            name="snowflake_context",
            display_name="Snowflake Context",
            info="Optional connection from [ ❄️ Context ] / Operations node.",
            input_types=["Data"],
            required=False,
        ),
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
        DropdownInput(
            name="stage_selector",
            display_name="Stage / Bucket (@Database.Schema.Stage)",
            info="Internal Stage (SNOWFLAKE_SSE / SNOWFLAKE_FULL) or External Stage (S3 / GCS / Azure / S3-Compatible).",
            options=["@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"],
            value="@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="media_type_filter",
            display_name="Stage Media Type Filter",
            options=[
                "All Compatible",
                "Images (.jpg/.png/.webp/.gif/.bmp)",
                "Audio (.mp3/.wav/.flac/.m4a/.ogg)",
                "Video (.mp4/.mov/.webm/.mkv/.avi)",
            ],
            value="All Compatible",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="stage_file_path",
            display_name="Stage Media File (Relative Path or @DB.SCHEMA.STAGE/path for TO_FILE)",
            options=[],
            value="",
            real_time_refresh=True,
            combobox=True,
            advanced=False,
        ),
        StrInput(
            name="additional_stage_files",
            display_name="Additional Stage Files CSV (Multi-File PROMPT('{0} ... {1}', TO_FILE(...), TO_FILE(...)))",
            value="",
            advanced=True,
        ),
        BoolInput(
            name="batch_directory_mode",
            display_name="Batch Directory Table (DIRECTORY(@stage))",
            info="Evaluate AI_COMPLETE across all compatible files in the Internal or External Stage via DIRECTORY(@stage).",
            value=False,
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
        flow_id = _resolve_flow_id(self, build_config)
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
        mfilter = (
            field_value
            if field_name == "media_type_filter" and isinstance(field_value, str)
            else build_config.get("media_type_filter", {}).get("value")
            or getattr(self, "media_type_filter", "All Compatible")
        ).strip()

        stg_resp = LIVE_BRIDGE.list_stages_for_context(flow_id=flow_id)
        stg_items = stg_resp.get("stages") or []
        stg_opts = [
            str(s.get("stage_fqn") or f"@{s.get('fqn')}")
            for s in stg_items
            if isinstance(s, dict) and (s.get("stage_fqn") or s.get("fqn"))
        ]
        if not stg_opts:
            stg_opts = ["@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"]
        if "stage_selector" in build_config:
            build_config["stage_selector"]["options"] = stg_opts
            cur_stg = build_config["stage_selector"].get("value") or stg_opts[0]
        else:
            cur_stg = stg_opts[0]
        if field_name == "stage_selector" and isinstance(field_value, str) and field_value.strip():
            cur_stg = field_value.strip()

        files_resp = LIVE_BRIDGE.list_stage_files(cur_stg)
        file_opts = filter_stage_files_for_function(
            files_resp.get("files") or [],
            ai_function="AI_COMPLETE",
            media_type_filter=mfilter,
        )
        if "stage_file_path" in build_config:
            build_config["stage_file_path"]["options"] = file_opts

        media_kind = detect_stage_media_kind(stg_file) if stg_file else ""
        req_audio = media_kind == "audio" or "audio" in mfilter.lower()
        req_video = media_kind == "video" or "video" in mfilter.lower()
        req_structured = fmt_mode not in ("None (Text)", "Text (Default)", "")
        req_multimodal = bool(stg_file) or req_audio or req_video
        compatible = get_compatible_models(
            require_structured_outputs=req_structured,
            require_multimodal=req_multimodal,
            require_audio=req_audio,
            require_video=req_video,
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
            stage_selector=getattr(self, "stage_selector", "") or "",
            stage_file_path=getattr(self, "stage_file_path", "") or "",
            additional_stage_files=getattr(self, "additional_stage_files", "") or "",
            batch_directory_mode=bool(getattr(self, "batch_directory_mode", False)),
            temperature=_optional_number(getattr(self, "temperature", None), float),
            top_p=_optional_number(getattr(self, "top_p", None), float),
            max_tokens=_optional_number(getattr(self, "max_tokens", None), int),
            guardrails=bool(getattr(self, "guardrails", False)),
            response_format_mode=getattr(self, "response_format_mode", "None (Text)") or "None (Text)",
            response_json_schema=getattr(self, "response_json_schema", "") or "",
            show_details=bool(getattr(self, "show_details", False)),
            return_error_details=bool(getattr(self, "return_error_details", False)),
        )

    def run_ai_complete(self) -> Message:
        stmt = self._build_sql()
        res = _flow_component_execute_sql(stmt)
        if not res.get("ok"):
            if getattr(self, "return_error_details", False):
                return Message(text=json.dumps({"error": res, "sql_preview": stmt}, indent=2))
            raise ValueError(res.get("message", "AI_COMPLETE execution failed"))
        rows = res.get("rows", [])
        if bool(getattr(self, "batch_directory_mode", False)):
            return Message(text=json.dumps({"rows": rows, "count": len(rows)}, indent=2))
        val = rows[0].get("RESULT") if rows else ""
        return Message(text=str(val) if not isinstance(val, (dict, list)) else json.dumps(val, indent=2))

    def get_details(self) -> Data:
        return Data(
            data={
                "function": "AI_COMPLETE",
                "model": self.model,
                "stage_selector": getattr(self, "stage_selector", "") or "",
                "stage_file_path": getattr(self, "stage_file_path", "") or "",
                "batch_directory_mode": bool(getattr(self, "batch_directory_mode", False)),
                "response_format_mode": self.response_format_mode,
                "show_details": bool(self.show_details),
                "return_error_details": bool(self.return_error_details),
                "active_role": LIVE_BRIDGE.active_role,
                "sql_preview": self._build_sql(),
            }
        )
