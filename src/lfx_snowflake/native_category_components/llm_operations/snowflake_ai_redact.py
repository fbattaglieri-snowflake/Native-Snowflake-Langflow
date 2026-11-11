from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, HandleInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.ai_function import (
    AI_REDACT_LIMITATIONS_DISCLAIMER,
    SUPPORTED_AI_REDACT_CATEGORIES,
    SnowflakeAIRedactEngine,
    filter_stage_files_for_function,
)
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.observability.redaction import mask_ai_redact_detect_spans
from lfx_snowflake.flow_context_guard import flow_component_sql as _flow_component_sql


def _flow_component_execute_sql(*args, **kwargs):
    """BUG-CTX-03: this component's SQL is checked against the flow's `[ ❄️ Context ]`."""
    with _flow_component_sql():
        return LIVE_BRIDGE.execute_sql(*args, **kwargs)


class SnowflakeAIRedactComponent(Component):
    display_name = "AI Redact"
    description = (
        "Dedicated Snowflake Cortex AI_REDACT component supporting all 13 official PII categories, "
        "'redact', 'detect', and 'detect_with_allowlist' (selective reverse-index span splicing) modes, "
        "multi-row error handling (AI_SQL_ERROR_HANDLING_USE_FAIL_ON_ERROR = FALSE + return_error_details => TRUE), "
        "4,096-token limit auto-chunking via SPLIT_TEXT_RECURSIVE_CHARACTER, and stage pre-conversion "
        "(AI_PARSE_DOCUMENT / AI_TRANSCRIBE)."
    )
    icon = "Snowflake"
    name = "SnowflakeAIRedact"

    inputs = [
        HandleInput(
            name="snowflake_context",
            display_name="Snowflake Context",
            info="Optional connection from [ ❄️ Context ] / Operations node.",
            input_types=["Data"],
            required=False,
        ),
        DropdownInput(
            name="source_mode",
            display_name="Input Source Mode",
            options=[
                "Text Input",
                "Table Column",
                "Stage Document (AI_PARSE_DOCUMENT -> AI_REDACT)",
                "Stage Audio/Video (AI_TRANSCRIBE -> AI_REDACT)",
            ],
            value="Text Input",
            real_time_refresh=True,
        ),
        MessageTextInput(
            name="input_text",
            display_name="Input Text",
            value="Contact Mario Rossi at mario.rossi@example.com or +39 02 1234567.",
            show=True,
        ),
        DropdownInput(
            name="redact_mode",
            display_name="AI_REDACT Mode",
            options=["redact", "detect", "detect_with_allowlist"],
            value="redact",
            real_time_refresh=True,
        ),
        StrInput(
            name="pii_categories",
            display_name="PII Categories CSV (ALL or subset of 13 official categories)",
            info=(
                "Official 13 Snowflake PII categories: "
                + ", ".join(SUPPORTED_AI_REDACT_CATEGORIES)
                + ". Leave empty or 'ALL' to redact all 13 categories."
            ),
            value="ALL",
        ),
        StrInput(
            name="allowlist_values",
            display_name="Allowlist Values CSV (Retained PII in detect_with_allowlist mode)",
            info="Comma-separated PII strings or categories to retain while redacting all other detected spans.",
            value="",
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
            show=False,
        ),
        DropdownInput(
            name="stage_file_path",
            display_name="Stage Media File (Relative Path)",
            options=[],
            value="",
            real_time_refresh=True,
            combobox=True,
            show=False,
        ),
        DropdownInput(
            name="table_fqn",
            display_name="Database.Schema.Table",
            options=["SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_AUDIT_LOG"],
            value="",
            real_time_refresh=True,
            combobox=True,
            show=False,
        ),
        StrInput(
            name="text_column",
            display_name="Text Column Name",
            value="",
            show=False,
        ),
        IntInput(
            name="max_rows",
            display_name="Max Rows (Table Column Mode)",
            value=100,
            show=False,
        ),
        BoolInput(
            name="return_error_details",
            display_name="Return Error Details (AI_SQL_ERROR_HANDLING_USE_FAIL_ON_ERROR = FALSE)",
            value=True,
        ),
        BoolInput(
            name="auto_chunk_long_text",
            display_name="Auto-Chunk Long Text (>4,096 tokens via SPLIT_TEXT_RECURSIVE_CHARACTER)",
            value=False,
        ),
        IntInput(
            name="chunk_size",
            display_name="Chunk Size (Characters for SPLIT_TEXT_RECURSIVE_CHARACTER)",
            value=1000,
            advanced=True,
        ),
        BoolInput(
            name="estimate_tokens",
            display_name="Estimate Tokens via AI_COUNT_TOKENS('ai_redact', ...)",
            value=True,
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Redacted / Detected Output", name="message", method="run_ai_redact"),
        Output(display_name="Telemetry & Redaction Metadata", name="telemetry", method="run_telemetry"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        src_mode = (
            field_value
            if field_name == "source_mode" and isinstance(field_value, str)
            else build_config.get("source_mode", {}).get("value")
            or getattr(self, "source_mode", "Text Input")
        ).strip()
        rmode = (
            field_value
            if field_name == "redact_mode" and isinstance(field_value, str)
            else build_config.get("redact_mode", {}).get("value")
            or getattr(self, "redact_mode", "redact")
        ).strip().lower()

        is_stage = src_mode.startswith("Stage ")
        is_table = src_mode == "Table Column"
        if "input_text" in build_config:
            build_config["input_text"]["show"] = src_mode == "Text Input"
        if "allowlist_values" in build_config:
            build_config["allowlist_values"]["show"] = rmode == "detect_with_allowlist"
        if "stage_selector" in build_config:
            build_config["stage_selector"]["show"] = is_stage
        if "stage_file_path" in build_config:
            build_config["stage_file_path"]["show"] = is_stage
        if "table_fqn" in build_config:
            build_config["table_fqn"]["show"] = is_table
        if "text_column" in build_config:
            build_config["text_column"]["show"] = is_table
        if "max_rows" in build_config:
            build_config["max_rows"]["show"] = is_table

        if is_stage:
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
            media_filter = "Audio" if "Audio" in src_mode else "Documents"
            file_opts = filter_stage_files_for_function(
                files_resp.get("files") or [],
                ai_function="AI_REDACT",
                media_type_filter=media_filter,
            )
            if "stage_file_path" in build_config:
                build_config["stage_file_path"]["options"] = file_opts

        return build_config

    def _build_plan(self) -> dict[str, Any]:
        return SnowflakeAIRedactEngine.build_sql(
            input_text=getattr(self, "input_text", "") or "",
            mode=getattr(self, "redact_mode", "redact") or "redact",
            categories=getattr(self, "pii_categories", "ALL"),
            return_error_details=bool(getattr(self, "return_error_details", True)),
            auto_chunk_long_text=bool(getattr(self, "auto_chunk_long_text", False)),
            chunk_size=int(getattr(self, "chunk_size", 1000) or 1000),
            source_mode=getattr(self, "source_mode", "Text Input") or "Text Input",
            stage_selector=getattr(self, "stage_selector", "") or "",
            stage_file_path=getattr(self, "stage_file_path", "") or "",
            table_fqn=getattr(self, "table_fqn", "") or "",
            text_column=getattr(self, "text_column", "") or "",
            max_rows=int(getattr(self, "max_rows", 100) or 100),
        )

    def _build_sql(self) -> str:
        return str(self._build_plan()["sql"])

    def run_ai_redact(self) -> Message:
        plan = self._build_plan()
        # PASSO3: `session_setup_sql` (ALTER SESSION) is reported, not executed: every SQL API request is its
        # own session, so it never affected the next statement, and ALTER SESSION is not a flow-context statement.

        stmt = str(plan["sql"])
        res = _flow_component_execute_sql(stmt)
        if not res.get("ok"):
            if bool(getattr(self, "return_error_details", True)):
                return Message(
                    text=json.dumps(
                        {
                            "value": None,
                            "error": res.get("message") or "AI_REDACT execution failed",
                        },
                        indent=2,
                    )
                )
            raise ValueError(res.get("message", "AI_REDACT execution failed"))

        rows = res.get("rows", [])
        first_row = rows[0] if rows and isinstance(rows[0], dict) else {}
        raw_val = (
            first_row.get("RESULT")
            if "RESULT" in first_row
            else first_row.get("value")
            if "value" in first_row
            else first_row.get("VALUE")
            if "VALUE" in first_row
            else first_row.get("result")
            if "result" in first_row
            else ""
        )
        raw_err = first_row.get("error") or first_row.get("ERROR") or first_row.get("ERRORS")

        mode = str(plan.get("mode") or "redact")
        txt_in = str(getattr(self, "input_text", "") or "")

        if mode in ("detect", "detect_with_allowlist"):
            # BUG-LLMOP-009: parse the real detect output; fail closed (never forward the original text,
            # never invent spans). The original-text spans stay local; outputs carry masked values only.
            if raw_err:
                raise ValueError(f"AI_REDACT {mode} failed: {raw_err}")
            try:
                spans = SnowflakeAIRedactEngine.parse_detect_spans(raw_val, txt_in)
            except ValueError as exc:
                raise ValueError(f"AI_REDACT {mode} result could not be parsed safely: {exc}") from exc
            spliced = SnowflakeAIRedactEngine.apply_allowlist_splicing(
                input_text=txt_in,
                spans=spans,
                allowlist=(getattr(self, "allowlist_values", "") or "") if mode == "detect_with_allowlist" else "",
            )
            masked = SnowflakeAIRedactEngine.masked_spans(sorted(spliced["redacted_spans"], key=lambda sp: sp["start"]))
            safe_trace = mask_ai_redact_detect_spans(
                {"input_text": spliced["redacted_text"], "spans": masked},
                force_detect_context=True,
            )
            payload: dict[str, Any] = {
                "mode": mode,
                "redacted_text": spliced["redacted_text"],
                "spans": masked,
                "detected_spans_count": len(spans),
                "redacted_spans_count": len(spliced["redacted_spans"]),
                "error": None,
                "trace_safe_preview": safe_trace,
            }
            if mode == "detect_with_allowlist":
                # Allowlisted values were explicitly approved by the user and stay in clear text.
                payload["retained_spans"] = spliced["retained_spans"]
            return Message(text=json.dumps(payload, indent=2))

        if isinstance(raw_val, (dict, list)):
            return Message(text=json.dumps(raw_val, indent=2))
        return Message(text=str(raw_val or ""))

    def run_telemetry(self) -> Data:
        plan = self._build_plan()
        return Data(
            data={
                "ai_function": "AI_REDACT",
                "namespace": "DIRECT_AI_REDACT",
                "active_role": LIVE_BRIDGE.active_role,
                "mode": plan["mode"],
                "sql_mode": plan["sql_mode"],
                "supported_categories": list(SUPPORTED_AI_REDACT_CATEGORIES),
                "selected_categories": plan["categories"],
                "estimated_tokens": plan["estimated_tokens"],
                "max_input_output_tokens": SnowflakeAIRedactEngine.MAX_INPUT_OUTPUT_TOKENS,
                "max_output_tokens": SnowflakeAIRedactEngine.MAX_OUTPUT_TOKENS,
                "exceeds_4096_limit": plan["exceeds_4096_limit"],
                "chunked": plan["chunked"],
                "pre_conversion": plan.get("pre_conversion"),
                "session_setup_sql": plan["session_setup_sql"],
                "token_count_sql": plan["token_count_sql"],
                "sql_preview": plan["sql"],
                "disclaimer": AI_REDACT_LIMITATIONS_DISCLAIMER,
            }
        )
