from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.field_typing import Tool
from lfx.io import BoolInput, DropdownInput, FloatInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.knowledge_and_memories import (
    DYNAMIC_MEMORY_BASE_TYPE_SPECS,
    SnowflakeMemoryBaseComponent,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE, resolve_canonical_memory_type


def _memory_base_options() -> list[str]:
    opts = list(LIVE_BRIDGE.get_memory_catalog_options(include_none=False))
    defaults = [
        "MEM_1_GRAPH_CHECKPOINTER",
        "MEM_2_CHAT_WINDOW",
        "MEM_3_SESSION_SUMMARY",
        "MEM_4_USER_ENTITY_PROFILE",
        "MEM_5_EPISODIC_FEWSHOT",
    ]
    for d in defaults:
        if d not in opts:
            opts.append(d)
    return opts or ["SNOWFLAKE_KB_MEM"]


class SnowflakeMemoryBaseNativeComponent(Component):
    display_name = "Memory Base"
    description = (
        "Connect to any Snowflake Memory (#1 Graph State & Checkpointer, #2 Buffer / Window Memory, "
        "#3 Summary Memory, #4 User Profile & Entity Memory, #5 Episodic Memory) created in Memories "
        "on Snowflake SQL (Database ▸ Schema ▸ Hybrid/Standard/Dynamic/Interactive Table) or "
        "Account-Level Snowflake Postgres (:5432), dynamically adapting fields and output ports."
    )
    icon = "Snowflake"
    name = "SnowflakeMemoryBase"

    inputs = [
        DropdownInput(
            name="memory_base",
            display_name="Memory Base",
            options=_memory_base_options(),
            value=_memory_base_options()[0],
            real_time_refresh=True,
            refresh_button=True,
            combobox=True,
            info="Select a Snowflake Memory (#1..#5) created from the Memories page.",
        ),
        StrInput(
            name="canonical_memory_type",
            display_name="Detected Memory Type (#1..#5)",
            value="episodic_memory",
            advanced=True,
        ),
        StrInput(
            name="storage_target_summary",
            display_name="Storage Target Summary",
            value="",
            advanced=True,
        ),
        StrInput(
            name="thread_id",
            display_name="Thread ID (#1 Checkpointer)",
            value="default",
            show=False,
        ),
        StrInput(
            name="checkpoint_ns",
            display_name="Checkpoint Namespace (#1 Checkpointer)",
            value="default",
            show=False,
        ),
        IntInput(
            name="ttl_hours",
            display_name="TTL Hours (#1 Checkpointer)",
            value=24,
            show=False,
            advanced=True,
        ),
        StrInput(
            name="session_id",
            display_name="Session ID (#2 Window / #3 Summary)",
            value="default",
            show=False,
        ),
        IntInput(
            name="window_k",
            display_name="Window Size K (#2 Buffer / Window)",
            value=8,
            show=False,
        ),
        StrInput(
            name="summarizer_model",
            display_name="Summarizer Model (#3 Summary)",
            value="openai-gpt-6-luna",
            show=False,
        ),
        IntInput(
            name="max_summary_tokens",
            display_name="Max Summary Tokens (#3 Summary)",
            value=512,
            show=False,
            advanced=True,
        ),
        StrInput(
            name="user_id",
            display_name="User ID (#4 Profile & Entity)",
            value="default",
            show=False,
        ),
        StrInput(
            name="entity_namespace",
            display_name="Entity Namespace (#4 Profile & Entity)",
            value="default",
            show=False,
        ),
        StrInput(
            name="topic",
            display_name="Episode Topic (#5 Episodic)",
            value="financial_reconciliation",
            show=True,
        ),
        FloatInput(
            name="outcome_score_threshold",
            display_name="Min Outcome Score (#5 Episodic)",
            value=0.7,
            show=True,
        ),
        IntInput(
            name="few_shot_k",
            display_name="Few-Shot Top-K (#5 Episodic)",
            value=3,
            show=True,
        ),
        MessageTextInput(
            name="search_query",
            display_name="Search Query",
            value="",
            tool_mode=True,
            show=True,
        ),
        BoolInput(
            name="filter_by_session",
            display_name="Filter by Session",
            value=False,
            real_time_refresh=True,
            show=True,
        ),
        BoolInput(
            name="tool_mode",
            display_name="Tool Mode (Expose to Agent Tools)",
            value=True,
            real_time_refresh=True,
            show=True,
        ),
        StrInput(
            name="project_id",
            display_name="Project / Flow Scope ID",
            value="default",
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Results", name="results", method="recall_memory_data"),
        Output(display_name="Checkpoint State", name="checkpoint_state", method="get_checkpoint_state"),
        Output(display_name="Messages / Chat History", name="chat_history", method="get_chat_history"),
        Output(display_name="Running Summary", name="running_summary", method="get_running_summary"),
        Output(display_name="Profile Context", name="profile_context", method="get_profile_context"),
        Output(display_name="Few-Shot Context", name="few_shot_context", method="get_few_shot_context"),
        Output(display_name="Toolset", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def _build_helper(self, memory_name_override: str | None = None) -> SnowflakeMemoryBaseComponent:
        mem_name = (
            memory_name_override
            or getattr(self, "memory_base", None)
            or "SNOWFLAKE_KB_MEM"
        )
        short_name = str(mem_name).split(" (")[0].strip()
        raw_type = getattr(self, "canonical_memory_type", None)
        entry = SnowflakeMemoryBaseComponent._resolve_catalog_entry(short_name)
        canonical_type = (
            resolve_canonical_memory_type(
                str(entry.get("canonical_memory_type") or entry.get("agent_memory_type"))
            )
            if entry
            else resolve_canonical_memory_type(str(raw_type or "episodic_memory"))
        )
        return SnowflakeMemoryBaseComponent(
            None,
            short_name,
            canonical_memory_type=canonical_type,
            filter_by_session=getattr(self, "filter_by_session", None),
            tool_mode=getattr(self, "tool_mode", None),
            thread_id=str(getattr(self, "thread_id", "default") or "default"),
            checkpoint_ns=str(getattr(self, "checkpoint_ns", "default") or "default"),
            ttl_hours=int(getattr(self, "ttl_hours", 24) or 24),
            session_id=str(getattr(self, "session_id", "default") or "default"),
            window_k=int(getattr(self, "window_k", 8) or 8),
            summarizer_model=str(getattr(self, "summarizer_model", "openai-gpt-6-luna") or "openai-gpt-6-luna"),
            max_summary_tokens=int(getattr(self, "max_summary_tokens", 512) or 512),
            user_id=str(getattr(self, "user_id", "default") or "default"),
            entity_namespace=str(getattr(self, "entity_namespace", "default") or "default"),
            topic=str(getattr(self, "topic", "general") or "general"),
            outcome_score_threshold=float(
                getattr(self, "outcome_score_threshold", 0.7)
                if getattr(self, "outcome_score_threshold", None) is not None
                else 0.7
            ),
            few_shot_k=int(getattr(self, "few_shot_k", 3) or 3),
        )

    def get_dynamic_port_and_field_spec(self, memory_name: str | None = None) -> dict[str, Any]:
        helper = self._build_helper(memory_name)
        return helper.get_dynamic_port_and_field_spec(memory_name)

    def update_build_config(
        self,
        build_config: dict[str, Any],
        field_value: Any = None,
        field_name: str | None = None,
    ) -> dict[str, Any]:
        selected = (
            str(field_value)
            if field_name in ("memory_base", "memory_name", "memory_catalog_ref") and isinstance(field_value, str)
            else str((build_config.get("memory_base") or {}).get("value") or getattr(self, "memory_base", ""))
        )
        helper = self._build_helper(selected)
        updated = helper.update_build_config(build_config, field_value, field_name)
        if "memory_base" in updated and isinstance(updated["memory_base"], dict):
            updated["memory_base"]["options"] = _memory_base_options()
            if field_name == "memory_base" and isinstance(field_value, str) and field_value.strip():
                updated["memory_base"]["value"] = field_value
        return updated

    def _execute_recall(self) -> dict[str, Any]:
        helper = self._build_helper()
        eff_session = str(getattr(self, "session_id", "default") or "default")
        return helper.search_memory(
            str(getattr(self, "search_query", "") or ""),
            user_id=str(getattr(self, "user_id", "default") or "default"),
            project_id=str(getattr(self, "project_id", "default") or "default"),
            session_id=eff_session,
            thread_id=str(getattr(self, "thread_id", eff_session) or eff_session),
            entity_namespace=str(getattr(self, "entity_namespace", "default") or "default"),
            topic=str(getattr(self, "topic", "") or getattr(self, "search_query", "") or "general"),
            top_k=int(getattr(self, "few_shot_k", 3) or 3),
        )

    def recall_memory_data(self) -> Data:
        return Data(data=self._execute_recall())

    def get_checkpoint_state(self) -> Data:
        payload = self._execute_recall()
        payload["output_port"] = "checkpoint_state"
        return Data(data=payload)

    def get_chat_history(self) -> Message:
        payload = self._execute_recall()
        rows = payload.get("rows") or []
        text = json.dumps(
            {
                "output_port": "chat_history",
                "memory_ref": payload.get("memory_ref"),
                "canonical_memory_type": payload.get("canonical_memory_type"),
                "session_id": getattr(self, "session_id", "default"),
                "window_k": int(getattr(self, "window_k", 8) or 8),
                "messages": rows,
                "recall_sql": payload.get("recall_sql"),
                "active_role": LIVE_BRIDGE.active_role,
            }
        )
        return Message(text=text)

    def get_running_summary(self) -> Message:
        payload = self._execute_recall()
        rows = payload.get("rows") or []
        summary_text = ""
        if rows and isinstance(rows[0], dict):
            summary_text = str(rows[0].get("running_summary") or rows[0].get("RUNNING_SUMMARY") or "")
        if not summary_text:
            summary_text = (
                f"[Summary Memory ({payload.get('memory_ref')}) — Session {getattr(self, 'session_id', 'default')}]: "
                f"Prior turns summarized via {getattr(self, 'summarizer_model', 'openai-gpt-6-luna')}."
            )
        return Message(text=summary_text)

    def get_profile_context(self) -> Data:
        payload = self._execute_recall()
        payload["output_port"] = "profile_context"
        payload["filter_by_session"] = False
        return Data(data=payload)

    def get_few_shot_context(self) -> Data:
        payload = self._execute_recall()
        payload["output_port"] = "few_shot_context"
        payload["filter_by_session"] = False
        payload["outcome_score_threshold"] = float(getattr(self, "outcome_score_threshold", 0.7) or 0.7)
        payload["few_shot_k"] = int(getattr(self, "few_shot_k", 3) or 3)
        return Data(data=payload)

    def to_toolkit(self) -> Tool:
        helper = self._build_helper()
        descriptor = helper.build_tool()

        def _invoke_tool(tool_input: Any = None, **_kwargs: Any) -> dict[str, Any]:
            q = ""
            if isinstance(tool_input, dict):
                q = str(tool_input.get("query") or tool_input.get("topic") or "")
            elif isinstance(tool_input, str):
                q = tool_input
            return helper.search_memory(
                q or str(getattr(self, "search_query", "") or ""),
                user_id=str(getattr(self, "user_id", "default") or "default"),
                project_id=str(getattr(self, "project_id", "default") or "default"),
                session_id=str(getattr(self, "session_id", "default") or "default"),
                topic=q or str(getattr(self, "topic", "general") or "general"),
                top_k=int(getattr(self, "few_shot_k", 3) or 3),
            )

        return Tool(
            name=descriptor["tool_name"],
            description=(
                f"Recall governed Snowflake memory '{helper.memory_name}' "
                f"({DYNAMIC_MEMORY_BASE_TYPE_SPECS[helper.canonical_memory_type]['label']}) "
                f"from {helper.table_fqn} under active role {LIVE_BRIDGE.active_role}."
            ),
            func=_invoke_tool,
            metadata=descriptor,
        )
