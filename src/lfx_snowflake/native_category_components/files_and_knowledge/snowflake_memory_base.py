from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.field_typing import BaseChatMessageHistory, Memory, Tool
from lfx.io import BoolInput, DropdownInput, FloatInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.knowledge_and_memories import (
    DYNAMIC_MEMORY_BASE_TYPE_SPECS,
    SnowflakeMemoryBaseComponent,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE, resolve_canonical_memory_type


def _memory_base_options(flow_id: str | None = None) -> list[str]:
    opts = list(LIVE_BRIDGE.get_memory_catalog_options(include_none=False, flow_id=flow_id))
    defaults = [
        "MEM_1_GRAPH_CHECKPOINTER",
        "MEM_2_CHAT_WINDOW",
        "MEM_3_SESSION_SUMMARY",
        "MEM_4_USER_ENTITY_PROFILE",
        "MEM_5_EPISODIC_FEWSHOT",
    ]
    reg_by_name = {
        str(m.get("name") or "").strip().upper(): m
        for m in getattr(LIVE_BRIDGE, "memories_registry", [])
        if isinstance(m, dict) and m.get("name")
    }
    for d in defaults:
        if d in opts:
            continue
        reg_entry = reg_by_name.get(d.upper())
        if reg_entry is not None:
            if LIVE_BRIDGE.is_memory_visible_in_context(reg_entry, flow_id=flow_id):
                opts.append(d)
        elif not reg_by_name:
            opts.append(d)
    return opts or ["SNOWFLAKE_KB_MEM"]


def _memory_options(flow_id: str | None = None) -> list[str]:
    return _memory_base_options(flow_id=flow_id)


def _register_chat_message_history_adapter():
    class SnowflakeChatMessageHistoryAdapter:
        """LangChain `BaseChatMessageHistory` & Langflow `Memory` port adapter (`W2-R4-09`).

        Wraps `SnowflakeMemoryBaseNativeComponent` so it can either operate as a single
        standalone memory node or plug into `SnowflakeMessageHistoryNativeComponent`'s
        orange `External Memory` (`memory`) handle.
        """

        def __init__(self, component: SnowflakeMemoryBaseNativeComponent) -> None:
            self.component = component
            self._local_messages: list[dict[str, Any]] = []

        @property
        def helper(self) -> SnowflakeMemoryBaseComponent:
            return self.component._build_helper()

        @property
        def memory_name(self) -> str:
            return self.helper.memory_name

        @property
        def canonical_memory_type(self) -> str:
            return self.helper.canonical_memory_type

        @property
        def storage_family(self) -> str:
            return self.helper.storage_family

        @property
        def table_fqn(self) -> str:
            return self.helper.table_fqn

        @property
        def session_id(self) -> str:
            return str(getattr(self.component, "session_id", None) or self.helper.session_id or "default")

        @property
        def window_k(self) -> int:
            return int(getattr(self.component, "window_k", None) or self.helper.window_k or 8)

        @property
        def user_id(self) -> str:
            return str(getattr(self.component, "user_id", None) or self.helper.user_id or "default")

        @property
        def project_id(self) -> str:
            return str(getattr(self.component, "project_id", "default") or "default")

        @property
        def filter_by_session(self) -> bool:
            return bool(self.helper.filter_by_session)

        def _find_bridge_entry(self) -> dict[str, Any] | None:
            finder = getattr(LIVE_BRIDGE, "_find_memory_entry", None)
            if callable(finder):
                return finder(self.memory_name)
            for entry in getattr(LIVE_BRIDGE, "memories_registry", []):
                if str(entry.get("name") or "").strip().upper() == self.memory_name.upper():
                    return entry
            return None

        def add_message(
            self,
            message: Any,
            *,
            session_id: str | None = None,
            sender: str | None = None,
            sender_name: str | None = None,
            user_id: str | None = None,
            project_id: str | None = None,
            message_id: str | None = None,
            metadata: dict[str, Any] | None = None,
        ) -> Message:
            eff_session = str(session_id or self.session_id or "default").strip()
            eff_user = str(user_id or self.user_id or "default").strip()
            eff_project = str(project_id or self.project_id or "default").strip()

            raw_text = ""
            inferred_sender = sender
            inferred_sender_name = sender_name
            if isinstance(message, Message):
                raw_text = str(message.text or "")
                msg_data = getattr(message, "data", {}) or {}
                inferred_sender = inferred_sender or msg_data.get("sender") or getattr(message, "sender", None)
                inferred_sender_name = (
                    inferred_sender_name or msg_data.get("sender_name") or getattr(message, "sender_name", None)
                )
                if not session_id and (msg_data.get("session_id") or getattr(message, "session_id", None)):
                    eff_session = str(msg_data.get("session_id") or getattr(message, "session_id"))
            elif isinstance(message, dict):
                raw_text = str(message.get("text") or message.get("message") or message.get("content") or "")
                inferred_sender = inferred_sender or message.get("sender")
                inferred_sender_name = inferred_sender_name or message.get("sender_name")
                if not session_id and message.get("session_id"):
                    eff_session = str(message["session_id"])
            elif hasattr(message, "content"):
                raw_text = str(getattr(message, "content", "") or "")
                msg_type = str(getattr(message, "type", "") or "").lower()
                if not inferred_sender:
                    inferred_sender = "AI" if msg_type in ("ai", "assistant") else "User"
            else:
                raw_text = str(message or "")

            eff_sender = str(inferred_sender or "User")
            eff_sender_name = str(inferred_sender_name or eff_sender)
            now_iso = datetime.now(timezone.utc).isoformat()
            seq_no = len(self._local_messages) + 1
            eff_msg_id = str(message_id or f"msg-{eff_session}-{seq_no}")

            record: dict[str, Any] = {
                "id": eff_msg_id,
                "message_id": eff_msg_id,
                "session_id": eff_session,
                "user_id": eff_user,
                "project_id": eff_project,
                "sender": eff_sender,
                "sender_name": eff_sender_name,
                "text": raw_text,
                "message_text": raw_text,
                "topic": f"{eff_sender_name}: {raw_text[:80]}",
                "status": "Ingested",
                "timestamp": now_iso,
                "updated": now_iso,
                "memory_base": self.memory_name,
                "canonical_memory_type": self.canonical_memory_type,
                "storage_family": self.storage_family,
                "table_fqn": self.table_fqn,
                "active_role": LIVE_BRIDGE.active_role,
                **(metadata or {}),
            }
            self._local_messages.append(record)

            self.helper.store_memory(
                raw_text,
                user_id=eff_user,
                project_id=eff_project,
                session_id=eff_session,
                metadata=record,
            )

            entry = self._find_bridge_entry()
            if entry is not None:
                stored_list = list(entry.get("stored_records") or [])
                stored_list.insert(0, dict(record))
                entry["stored_records"] = stored_list

            return Message(
                text=raw_text,
                id=eff_msg_id,
                message_id=eff_msg_id,
                sender=eff_sender,
                sender_name=eff_sender_name,
                session_id=eff_session,
                timestamp=now_iso,
                memory_base=self.memory_name,
                canonical_memory_type=self.canonical_memory_type,
                storage_family=self.storage_family,
                table_fqn=self.table_fqn,
                active_role=LIVE_BRIDGE.active_role,
            )

        def add_messages(self, messages: list[Any], **kwargs: Any) -> list[Message]:
            return [self.add_message(m, **kwargs) for m in messages]

        def add_user_message(self, message: str | Any, **kwargs: Any) -> Message:
            kwargs.setdefault("sender", "User")
            kwargs.setdefault("sender_name", "User")
            return self.add_message(message, **kwargs)

        def add_ai_message(self, message: str | Any, **kwargs: Any) -> Message:
            kwargs.setdefault("sender", "AI")
            kwargs.setdefault("sender_name", "AI")
            return self.add_message(message, **kwargs)

        def get_message_dicts(
            self,
            *,
            session_id: str | None = None,
            limit: int | None = None,
            user_id: str | None = None,
            project_id: str | None = None,
        ) -> list[dict[str, Any]]:
            eff_session = str(session_id or self.session_id or "default").strip()
            eff_limit = int(limit if limit is not None else self.window_k or 8)
            filter_sess = self.filter_by_session or bool(session_id)

            matched: list[dict[str, Any]] = []
            seen_ids: set[str] = set()

            for rec in self._local_messages:
                if filter_sess and str(rec.get("session_id") or "") != eff_session:
                    continue
                rid = str(rec.get("message_id") or rec.get("id") or "")
                if rid and rid in seen_ids:
                    continue
                if rid:
                    seen_ids.add(rid)
                matched.append(dict(rec))

            entry = self._find_bridge_entry()
            if entry is not None:
                for rec in reversed(list(entry.get("stored_records") or [])):
                    if filter_sess and str(rec.get("session_id") or "") != eff_session:
                        continue
                    rid = str(rec.get("message_id") or rec.get("id") or "")
                    if rid and rid in seen_ids:
                        continue
                    text_val = str(rec.get("text") or rec.get("message_text") or rec.get("topic") or "")
                    norm_rec = {
                        "id": rid or f"rec-{len(matched) + 1}",
                        "message_id": rid or f"rec-{len(matched) + 1}",
                        "session_id": str(rec.get("session_id") or eff_session),
                        "user_id": str(rec.get("user_id") or user_id or self.user_id),
                        "project_id": str(rec.get("project_id") or project_id or self.project_id),
                        "sender": str(rec.get("sender") or "User"),
                        "sender_name": str(rec.get("sender_name") or rec.get("sender") or "User"),
                        "text": text_val,
                        "message_text": text_val,
                        "timestamp": str(rec.get("timestamp") or rec.get("updated") or ""),
                        "memory_base": self.memory_name,
                        "canonical_memory_type": self.canonical_memory_type,
                        "storage_family": self.storage_family,
                        "table_fqn": self.table_fqn,
                        "active_role": LIVE_BRIDGE.active_role,
                    }
                    if rid:
                        seen_ids.add(rid)
                    matched.append(norm_rec)

            if eff_limit > 0 and len(matched) > eff_limit:
                matched = matched[-eff_limit:]
            return matched

        def get_messages(
            self,
            *,
            session_id: str | None = None,
            limit: int | None = None,
            user_id: str | None = None,
            project_id: str | None = None,
        ) -> list[Message]:
            rows = self.get_message_dicts(
                session_id=session_id,
                limit=limit,
                user_id=user_id,
                project_id=project_id,
            )
            return [Message(**{k: v for k, v in r.items() if k != "text"}, text=str(r.get("text") or "")) for r in rows]

        @property
        def messages(self) -> list[Message]:
            return self.get_messages()

        def clear(self, *, session_id: str | None = None) -> None:
            if session_id:
                self._local_messages = [
                    r for r in self._local_messages if str(r.get("session_id") or "") != str(session_id)
                ]
            else:
                self._local_messages.clear()
            entry = self._find_bridge_entry()
            if entry is not None and isinstance(entry.get("stored_records"), list):
                if session_id:
                    entry["stored_records"] = [
                        r for r in entry["stored_records"] if str(r.get("session_id") or "") != str(session_id)
                    ]
                else:
                    entry["stored_records"] = []

        def to_data(self) -> Data:
            return Data(
                data={
                    "memory_name": self.memory_name,
                    "canonical_memory_type": self.canonical_memory_type,
                    "storage_family": self.storage_family,
                    "table_fqn": self.table_fqn,
                    "session_id": self.session_id,
                    "window_k": self.window_k,
                    "active_role": LIVE_BRIDGE.active_role,
                    "messages": self.get_message_dicts(),
                }
            )

    return SnowflakeChatMessageHistoryAdapter


SnowflakeChatMessageHistoryAdapter = _register_chat_message_history_adapter()


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
        Output(display_name="Memory", name="memory", method="build_memory", types=["Memory", "BaseChatMessageHistory"]),
        Output(display_name="Results", name="results", method="recall_memory_data"),
        Output(display_name="Checkpoint State", name="checkpoint_state", method="get_checkpoint_state"),
        Output(display_name="Messages / Chat History", name="chat_history", method="get_chat_history"),
        Output(display_name="Running Summary", name="running_summary", method="get_running_summary"),
        Output(display_name="Profile Context", name="profile_context", method="get_profile_context"),
        Output(display_name="Few-Shot Context", name="few_shot_context", method="get_few_shot_context"),
        Output(display_name="Toolset", name="component_as_tool", method="to_toolkit", types=["Tool"]),
    ]

    def __init__(self, **kwargs: Any) -> None:
        if "memory_name" in kwargs and "memory_base" not in kwargs:
            kwargs["memory_base"] = kwargs["memory_name"]
        super().__init__(**kwargs)

    @property
    def memory_name(self) -> str:
        return str(getattr(self, "memory_base", "SNOWFLAKE_KB_MEM") or "SNOWFLAKE_KB_MEM").split(" (")[0].strip()

    @memory_name.setter
    def memory_name(self, value: str) -> None:
        self.memory_base = str(value or "SNOWFLAKE_KB_MEM").strip()

    def build_memory(self) -> SnowflakeChatMessageHistoryAdapter:
        """Return an External Memory adapter (`Memory` / `BaseChatMessageHistory`) for 2-node or 1-node canvas wiring (`W2-R4-09`)."""
        adapter = getattr(self, "_memory_adapter", None)
        if not isinstance(adapter, SnowflakeChatMessageHistoryAdapter):
            adapter = SnowflakeChatMessageHistoryAdapter(self)
            self._memory_adapter = adapter
        return adapter

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

    def to_build_config(self) -> dict[str, dict[str, Any]]:
        cfg: dict[str, dict[str, Any]] = {}
        for inp in getattr(self, "inputs", []):
            inp_name = getattr(inp, "name", None) or (inp.get("name") if isinstance(inp, dict) else None)
            if inp_name:
                if isinstance(inp, dict):
                    cfg[inp_name] = dict(inp)
                elif hasattr(inp, "to_dict"):
                    cfg[inp_name] = inp.to_dict()
                else:
                    cfg[inp_name] = {
                        "name": inp_name,
                        "value": getattr(inp, "value", None),
                        "show": getattr(inp, "show", True),
                    }
        return cfg

    def get_dynamic_port_and_field_spec(self, memory_name: str | None = None) -> dict[str, Any]:
        helper = self._build_helper(memory_name)
        return helper.get_dynamic_port_and_field_spec(memory_name)

    def update_build_config(
        self,
        build_config: dict[str, Any],
        field_value: Any = None,
        field_name: str | None = None,
    ) -> dict[str, Any]:
        flow_id = (
            getattr(self, "flow_id", None)
            or (build_config.get("_flow_id") or {}).get("value")
            if isinstance(build_config.get("_flow_id"), dict)
            else getattr(self, "flow_id", None)
        )
        selected = (
            str(field_value)
            if field_name in ("memory_base", "memory_name", "memory_catalog_ref") and isinstance(field_value, str)
            else str((build_config.get("memory_base") or {}).get("value") or getattr(self, "memory_base", ""))
        )
        helper = self._build_helper(selected)
        updated = helper.update_build_config(build_config, field_value, field_name)
        opts = _memory_base_options(flow_id=flow_id)
        for mem_key in ("memory_base", "memory_name"):
            if mem_key in updated and isinstance(updated[mem_key], dict):
                updated[mem_key]["options"] = opts
                if field_name == mem_key and isinstance(field_value, str) and field_value.strip():
                    updated[mem_key]["value"] = field_value
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
        rows = list(payload.get("rows") or [])
        if not rows:
            rows = self.build_memory().get_message_dicts(
                session_id=str(getattr(self, "session_id", "default") or "default"),
                limit=int(getattr(self, "window_k", 8) or 8),
            )
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
