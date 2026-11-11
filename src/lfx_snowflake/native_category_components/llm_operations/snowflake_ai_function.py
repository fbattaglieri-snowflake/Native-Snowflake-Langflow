from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, HandleInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.ai_function import (
    TASK_SPECIFIC_AI_FUNCTIONS,
    SnowflakeAIFunctionComponent as DomainAIFunctionEngine,
    filter_stage_files_for_function,
)
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.flow_context_guard import flow_component_sql as _flow_component_sql


def _flow_component_execute_sql(*args, **kwargs):
    """BUG-CTX-03: this component's SQL is checked against the flow's `[ ❄️ Context ]`."""
    with _flow_component_sql():
        return LIVE_BRIDGE.execute_sql(*args, **kwargs)


AI_FUNCTION_VISIBLE_FIELDS: dict[str, tuple[str, ...]] = {
    "AI_CLASSIFY": (
        "input_text",
        "categories_or_schema",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "second_text_or_stage_file",
        "batch_directory_mode",
        "return_as_json",
        "estimate_tokens",
    ),
    "AI_FILTER": (
        "input_text",
        "categories_or_schema",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "second_text_or_stage_file",
        "batch_directory_mode",
        "return_as_json",
        "estimate_tokens",
    ),
    "AI_EXTRACT": (
        "input_text",
        "categories_or_schema",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "second_text_or_stage_file",
        "batch_directory_mode",
        "return_as_json",
        "estimate_tokens",
    ),
    "AI_SENTIMENT": ("input_text", "categories_or_schema", "return_as_json", "estimate_tokens"),
    "AI_SUMMARIZE": (
        "input_text",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "batch_directory_mode",
        "return_error_details",
        "return_as_json",
        "estimate_tokens",
    ),
    "AI_SUMMARIZE_AGG": ("input_text", "return_as_json", "estimate_tokens"),
    "AI_AGG": ("input_text", "categories_or_schema", "return_as_json", "estimate_tokens"),
    "AI_TRANSLATE": ("input_text", "source_lang", "target_lang", "return_as_json", "estimate_tokens"),
    "AI_SIMILARITY": (
        "input_text",
        "second_text_or_stage_file",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "second_stage_file_path",
        "return_as_json",
        "estimate_tokens",
    ),
    "AI_TRANSCRIBE": (
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "second_text_or_stage_file",
        "batch_directory_mode",
        "transcribe_timestamp_granularity",
        "return_error_details",
        "return_as_json",
    ),
    "AI_PARSE_DOCUMENT": (
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "second_text_or_stage_file",
        "batch_directory_mode",
        "parse_document_mode",
        "parse_document_page_split",
        "return_as_json",
    ),
    "AI_EMBED": (
        "input_text",
        "embed_model",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "batch_directory_mode",
        "return_as_json",
    ),
    "AI_MULTI_EMBED": (
        "input_text",
        "embed_model",
        "stage_selector",
        "media_type_filter",
        "stage_file_path",
        "batch_directory_mode",
        "multi_embed_options_json",
        "return_as_json",
    ),
    "AI_COUNT_TOKENS": ("input_text", "model_for_count_tokens", "return_as_json"),
}


class SnowflakeAIFunctionComponent(Component):
    display_name = "AI Function"
    description = (
        "Task-specific Snowflake Cortex AI_* Function execution across all 14 functions "
        "(AI_CLASSIFY, AI_FILTER, AI_EXTRACT, AI_SENTIMENT, AI_SUMMARIZE, AI_SUMMARIZE_AGG, AI_AGG, "
        "AI_TRANSLATE, AI_SIMILARITY, AI_TRANSCRIBE, AI_PARSE_DOCUMENT, AI_EMBED, AI_MULTI_EMBED, AI_COUNT_TOKENS) "
        "with full Internal & External Stage multimodal support (TO_FILE & DIRECTORY(@stage) for images, .mp3/audio, video, and documents)."
    )
    icon = "Snowflake"
    name = "SnowflakeAIFunction"

    inputs = [
        HandleInput(
            name="snowflake_context",
            display_name="Snowflake Context",
            info="Optional connection from [ ❄️ Context ] / Operations node.",
            input_types=["Data"],
            required=False,
        ),
        DropdownInput(
            name="ai_function",
            display_name="Task-Specific AI Function",
            options=list(TASK_SPECIFIC_AI_FUNCTIONS),
            value="AI_CLASSIFY",
            real_time_refresh=True,
        ),
        MessageTextInput(
            name="input_text",
            display_name="Input Text",
            value="Evaluate customer feedback or document content.",
            show=True,
        ),
        StrInput(
            name="categories_or_schema",
            display_name="Categories / Aspect Categories / Extraction Schema / Filter Predicate / Instruction",
            value="billing,support,technical",
            show=True,
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
            name="stage_selector",
            display_name="Stage / Bucket (@Database.Schema.Stage)",
            info="Internal Stage (SNOWFLAKE_SSE / SNOWFLAKE_FULL) or External Stage (S3 / GCS / Azure / S3-Compatible).",
            options=["@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"],
            value="@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
            show=True,
        ),
        DropdownInput(
            name="media_type_filter",
            display_name="Stage Media Type Filter",
            options=[
                "All Compatible",
                "Images (.jpg/.png/.webp/.gif/.bmp)",
                "Audio (.mp3/.wav/.flac/.m4a/.ogg)",
                "Video (.mp4/.mov/.webm/.mkv/.avi)",
                "Documents (.pdf/.docx/.pptx)",
            ],
            value="All Compatible",
            real_time_refresh=True,
            show=True,
        ),
        DropdownInput(
            name="stage_file_path",
            display_name="Stage Media File (Relative Path)",
            info="Select a file from the Internal or External Stage for TO_FILE('@stage', 'path').",
            options=[],
            value="",
            real_time_refresh=True,
            combobox=True,
            show=True,
        ),
        DropdownInput(
            name="second_stage_file_path",
            display_name="Second Stage Media File (for AI_SIMILARITY Multi-File Comparison)",
            options=[],
            value="",
            real_time_refresh=True,
            combobox=True,
            show=False,
        ),
        StrInput(
            name="second_text_or_stage_file",
            display_name="Second Text (AI_SIMILARITY) or @Stage/File (AI_TRANSCRIBE / Multimodal)",
            value="",
            show=True,
        ),
        BoolInput(
            name="batch_directory_mode",
            display_name="Batch Directory Table (DIRECTORY(@stage))",
            info="Process all compatible files in the Internal or External Stage in a single query via DIRECTORY(@stage).",
            value=False,
            show=True,
        ),
        DropdownInput(
            name="transcribe_timestamp_granularity",
            display_name="AI_TRANSCRIBE Timestamp Granularity",
            options=["default", "word", "speaker"],
            value="default",
            show=False,
        ),
        DropdownInput(
            name="parse_document_mode",
            display_name="AI_PARSE_DOCUMENT Mode",
            options=["LAYOUT", "OCR"],
            value="LAYOUT",
            show=False,
        ),
        BoolInput(
            name="parse_document_page_split",
            display_name="AI_PARSE_DOCUMENT Page Split",
            value=False,
            show=False,
        ),
        DropdownInput(
            name="embed_model",
            display_name="Embedding Model (for AI_EMBED / AI_MULTI_EMBED)",
            options=[
                "snowflake-arctic-embed-l-v2.0",
                "snowflake-arctic-embed-m-v2.0",
                "voyage-multimodal-3",
                "twelvelabs-marengo-embed-3-0",
            ],
            value="snowflake-arctic-embed-l-v2.0",
            combobox=True,
            show=False,
        ),
        MultilineInput(
            name="multi_embed_options_json",
            display_name="AI_MULTI_EMBED Options JSON (start_sec, end_sec, embedding_options, embedding_scope, embedding_type)",
            value='{"embedding_options": ["visual", "audio", "transcription"], "embedding_scope": ["clip", "asset"]}',
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
            name="return_error_details",
            display_name="Return Error Details",
            value=False,
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
        flow_id = _resolve_flow_id(self, build_config)
        selected_fn = (
            field_value
            if field_name == "ai_function" and isinstance(field_value, str)
            else build_config.get("ai_function", {}).get("value")
            or getattr(self, "ai_function", "AI_CLASSIFY")
        ).strip().upper()
        visible = set(AI_FUNCTION_VISIBLE_FIELDS.get(selected_fn, ("input_text", "return_as_json")))
        managed_fields = (
            "input_text",
            "categories_or_schema",
            "source_lang",
            "target_lang",
            "stage_selector",
            "media_type_filter",
            "stage_file_path",
            "second_stage_file_path",
            "second_text_or_stage_file",
            "batch_directory_mode",
            "transcribe_timestamp_granularity",
            "parse_document_mode",
            "parse_document_page_split",
            "embed_model",
            "multi_embed_options_json",
            "model_for_count_tokens",
            "return_error_details",
            "return_as_json",
            "estimate_tokens",
        )
        for fname in managed_fields:
            if fname in build_config:
                build_config[fname]["show"] = fname in visible

        if selected_fn == "AI_MULTI_EMBED" and "embed_model" in build_config:
            build_config["embed_model"]["value"] = "twelvelabs-marengo-embed-3-0"

        if "stage_selector" in visible:
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

            mfilter = (
                field_value
                if field_name == "media_type_filter" and isinstance(field_value, str)
                else build_config.get("media_type_filter", {}).get("value")
                or getattr(self, "media_type_filter", "All Compatible")
            )
            files_resp = LIVE_BRIDGE.list_stage_files(cur_stg)
            file_opts = filter_stage_files_for_function(
                files_resp.get("files") or [],
                ai_function=selected_fn,
                media_type_filter=str(mfilter),
            )
            if "stage_file_path" in build_config:
                build_config["stage_file_path"]["options"] = file_opts
            if "second_stage_file_path" in build_config:
                build_config["second_stage_file_path"]["options"] = file_opts

        return build_config

    def _build_sql(self) -> str:
        cats_or_schema = (
            getattr(self, "categories_or_schema", None)
            or getattr(self, "categories_csv", None)
            or getattr(self, "instructions", "")
            or ""
        )
        return DomainAIFunctionEngine.build_task_specific_sql(
            ai_function=self.ai_function or "AI_CLASSIFY",
            input_text=self.input_text or "",
            categories_or_schema=cats_or_schema,
            source_lang=getattr(self, "source_lang", "") or "",
            target_lang=getattr(self, "target_lang", None) or getattr(self, "instructions", "it") or "it",
            pii_categories=getattr(self, "pii_categories", "") or "",
            second_text_or_stage_file=getattr(self, "second_text_or_stage_file", "") or "",
            model_for_count_tokens=getattr(self, "model_for_count_tokens", "claude-sonnet-5-5") or "claude-sonnet-5-5",
            return_as_json=bool(getattr(self, "return_as_json", False)),
            stage_selector=getattr(self, "stage_selector", "") or "",
            stage_file_path=getattr(self, "stage_file_path", "") or "",
            second_stage_file_path=getattr(self, "second_stage_file_path", "") or "",
            batch_directory_mode=bool(getattr(self, "batch_directory_mode", False)),
            return_error_details=bool(getattr(self, "return_error_details", False)),
            transcribe_timestamp_granularity=getattr(self, "transcribe_timestamp_granularity", "default") or "default",
            parse_document_mode=getattr(self, "parse_document_mode", "LAYOUT") or "LAYOUT",
            parse_document_page_split=bool(getattr(self, "parse_document_page_split", False)),
            embed_model=getattr(self, "embed_model", "snowflake-arctic-embed-l-v2.0") or "snowflake-arctic-embed-l-v2.0",
            multi_embed_options_json=getattr(self, "multi_embed_options_json", "") or "",
        )

    def run_ai_function(self) -> Message:
        stmt = self._build_sql()
        res = _flow_component_execute_sql(stmt)
        if not res.get("ok"):
            raise ValueError(res.get("message", f"{self.ai_function} execution failed"))
        rows = res.get("rows", [])
        if bool(getattr(self, "batch_directory_mode", False)):
            return Message(text=json.dumps({"rows": rows, "count": len(rows)}, indent=2))
        raw_val = rows[0].get("RESULT") if rows else ""
        return Message(text=str(raw_val) if not isinstance(raw_val, (dict, list)) else json.dumps(raw_val))

    def run_telemetry(self) -> Data:
        return Data(
            data={
                "ai_function": self.ai_function,
                "namespace": "DIRECT_AI_*",
                "active_role": LIVE_BRIDGE.active_role,
                "return_as_json": bool(getattr(self, "return_as_json", False)),
                "batch_directory_mode": bool(getattr(self, "batch_directory_mode", False)),
                "stage_selector": getattr(self, "stage_selector", "") or "",
                "stage_file_path": getattr(self, "stage_file_path", "") or "",
                "sql_preview": self._build_sql(),
            }
        )
