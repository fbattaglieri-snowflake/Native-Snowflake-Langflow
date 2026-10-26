from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import (
    BoolInput,
    DropdownInput,
    HandleInput,
    IntInput,
    MessageInput,
    Output,
    StrInput,
    TabInput,
)
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.message_history import (
    InMemoryMessageHistoryBackend,
    MessageHistoryBackendType,
    MessageHistoryStorageConfig,
    SnowflakeMessageHistoryComponent,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE, resolve_canonical_memory_type
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_memory_base import (
    SnowflakeChatMessageHistoryAdapter,
    SnowflakeMemoryBaseNativeComponent,
)

_SHARED_HISTORY_BACKENDS: dict[str, InMemoryMessageHistoryBackend] = {}


def _buffer_window_memory_options() -> list[str]:
    opts: list[str] = []
    for entry in getattr(LIVE_BRIDGE, "memories_catalog", []):
        c_type = resolve_canonical_memory_type(
            str(entry.get("canonical_memory_type") or entry.get("agent_memory_type") or "")
        )
        nm = str(entry.get("name") or "").strip()
        if c_type == "buffer_window_memory" and nm and nm not in opts:
            opts.append(nm)
    for entry in getattr(LIVE_BRIDGE, "memories_catalog", []):
        nm = str(entry.get("name") or "").strip()
        if nm and nm not in opts:
            opts.append(nm)
    for fallback in ("MEM_2_CHAT_WINDOW", "CHAT_WINDOW_HISTORY", "CHAT_MESSAGE_HISTORY"):
        if fallback not in opts:
            opts.append(fallback)
    return opts


class SnowflakeMessageHistoryNativeComponent(Component):
    display_name = "Message History"
    description = (
        "Store or retrieve #2 Short-Term Buffer / Window Memory chat turns on Account-Level "
        "Snowflake Postgres (:5432, public.chat_message_history) or Snowflake Hybrid / Standard "
        "Tables (<DB>.<SCHEMA>) with window_k sliding window, session_id isolation, External Memory "
        "delegation, and ADR-003 signed replay envelope branching."
    )
    icon = "Snowflake"
    name = "SnowflakeMessageHistory"

    inputs = [
        TabInput(
            name="mode",
            display_name="Mode",
            options=["Retrieve", "Store"],
            value="Retrieve",
            real_time_refresh=True,
            info="Select Retrieve to load governed conversation turns or Store to persist a new message turn.",
        ),
        HandleInput(
            name="memory",
            display_name="External Memory",
            input_types=["Memory", "BaseChatMessageHistory"],
            value=None,
            info=(
                "Connect a Snowflake Memory Base component (or compatible external Memory) "
                "to route Retrieve/Store operations through its governed Snowflake backend."
            ),
        ),
        MessageInput(
            name="message_text",
            display_name="Message",
            input_types=["Message"],
            value="",
            show=False,
            advanced=False,
            info="The chat Message to store when Mode is set to Store.",
        ),
        DropdownInput(
            name="memory_base",
            display_name="Window Memory (#2 from Memories)",
            options=_buffer_window_memory_options(),
            value=_buffer_window_memory_options()[0],
            real_time_refresh=True,
            refresh_button=True,
            combobox=True,
        ),
        DropdownInput(
            name="storage_family",
            display_name="Storage Backend Family",
            options=[
                "snowflake_postgres",
                "snowflake_sql",
            ],
            value="snowflake_postgres",
            real_time_refresh=True,
        ),
        StrInput(
            name="postgres_instance",
            display_name="Postgres Instance (:5432)",
            value="LANGFLOW_PG_INSTANCE",
            show=True,
        ),
        StrInput(
            name="pg_table",
            display_name="Postgres Table (schema.table)",
            value="public.chat_message_history",
            show=True,
        ),
        StrInput(
            name="database",
            display_name="Database (for Hybrid / Standard Table)",
            value="SNOWFLAKE_LANGFLOW",
            show=False,
        ),
        StrInput(
            name="schema_name",
            display_name="Schema (for Hybrid / Standard Table)",
            value="CORE",
            show=False,
        ),
        StrInput(
            name="table_name",
            display_name="Table Name (for Hybrid / Standard Table)",
            value="CHAT_WINDOW_HISTORY",
            show=False,
        ),
        StrInput(
            name="session_id",
            display_name="Session ID",
            value="fin-reconcile-session-001",
            show=True,
        ),
        IntInput(
            name="window_k",
            display_name="Window Size K (Last K Verbatim Turns)",
            value=8,
            show=True,
        ),
        BoolInput(
            name="filter_by_session",
            display_name="Filter by Session",
            value=True,
            show=True,
        ),
        StrInput(
            name="user_id",
            display_name="User ID",
            value="fin_analyst_01",
            advanced=True,
        ),
        StrInput(
            name="project_id",
            display_name="Project / Flow ID",
            value="snowflake_full_memory_enterprise_agent",
            advanced=True,
        ),
        StrInput(
            name="sender",
            display_name="Sender (User | AI)",
            value="User",
            advanced=True,
        ),
        StrInput(
            name="sender_name",
            display_name="Sender Name",
            value="User",
            advanced=True,
        ),
        StrInput(
            name="turn_id",
            display_name="Turn / Message ID",
            value="",
            show=False,
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Chat History", name="messages", method="retrieve_chat_history", types=["Message"]),
        Output(display_name="Table Records", name="table", method="retrieve_history_table", types=["Data", "DataFrame"]),
        Output(display_name="Replay State", name="replay_state", method="retrieve_replay_state", types=["Data"]),
        Output(display_name="Stored Message", name="stored_message", method="store_message", types=["Message"]),
    ]

    def __init__(self, **kwargs: Any) -> None:
        if "message" in kwargs and "message_text" not in kwargs:
            kwargs["message_text"] = kwargs["message"]
        if "k_window" in kwargs and "window_k" not in kwargs:
            kwargs["window_k"] = kwargs["k_window"]
        super().__init__(**kwargs)

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

    def update_build_config(
        self,
        build_config: dict[str, Any],
        field_value: Any = None,
        field_name: str | None = None,
    ) -> dict[str, Any]:
        if "memory_base" in build_config and isinstance(build_config["memory_base"], dict):
            build_config["memory_base"]["options"] = _buffer_window_memory_options()
            if field_name == "memory_base" and isinstance(field_value, str) and field_value.strip():
                build_config["memory_base"]["value"] = field_value

        active_mode = (
            str(field_value)
            if field_name == "mode" and isinstance(field_value, str)
            else str((build_config.get("mode") or {}).get("value") or getattr(self, "mode", "Retrieve") or "Retrieve")
        ).strip()
        if active_mode not in ("Retrieve", "Store"):
            active_mode = "Retrieve"
        if "mode" in build_config and isinstance(build_config["mode"], dict):
            build_config["mode"]["value"] = active_mode

        is_store = active_mode == "Store"
        if "window_k" in build_config and isinstance(build_config["window_k"], dict):
            build_config["window_k"]["show"] = not is_store
        for store_field in ("message_text", "sender", "sender_name", "turn_id"):
            if store_field in build_config and isinstance(build_config[store_field], dict):
                build_config[store_field]["show"] = is_store
                if store_field == "message_text":
                    build_config[store_field]["advanced"] = not is_store

        sel_mem = (
            str(field_value)
            if field_name == "memory_base" and isinstance(field_value, str)
            else str((build_config.get("memory_base") or {}).get("value") or getattr(self, "memory_base", ""))
        ).split(" (")[0].strip()

        matched: dict[str, Any] | None = None
        for entry in getattr(LIVE_BRIDGE, "memories_catalog", []):
            if sel_mem.upper() in {
                str(entry.get("name") or "").strip().upper(),
                str(entry.get("table_fqn") or "").strip().upper(),
            }:
                matched = entry
                break

        family = (
            str(field_value)
            if field_name == "storage_family" and isinstance(field_value, str)
            else str(
                (matched or {}).get("storage_family")
                or (
                    "snowflake_postgres"
                    if "postgres" in str((matched or {}).get("vector_database") or "").lower()
                    or ":5432" in str((matched or {}).get("table_fqn") or "")
                    else (build_config.get("storage_family") or {}).get("value") or "snowflake_postgres"
                )
            )
        )
        if "storage_family" in build_config and isinstance(build_config["storage_family"], dict):
            build_config["storage_family"]["value"] = family

        is_pg = family == "snowflake_postgres"
        for f_pg in ("postgres_instance", "pg_table"):
            if f_pg in build_config and isinstance(build_config[f_pg], dict):
                build_config[f_pg]["show"] = is_pg
        for f_sql in ("database", "schema_name", "table_name"):
            if f_sql in build_config and isinstance(build_config[f_sql], dict):
                build_config[f_sql]["show"] = not is_pg

        ext_connected = bool(
            (field_name == "memory" and field_value)
            or (build_config.get("memory") or {}).get("value")
            or getattr(self, "memory", None)
        )
        if ext_connected:
            for manual_field in (
                "storage_family",
                "postgres_instance",
                "pg_table",
                "database",
                "schema_name",
                "table_name",
            ):
                if manual_field in build_config and isinstance(build_config[manual_field], dict):
                    build_config[manual_field]["required"] = False

        if matched is not None:
            if "window_k" in build_config and isinstance(build_config["window_k"], dict) and matched.get("window_k"):
                build_config["window_k"]["value"] = int(matched["window_k"])
            if "session_id" in build_config and isinstance(build_config["session_id"], dict) and matched.get("session_id"):
                build_config["session_id"]["value"] = str(matched["session_id"])
        if "filter_by_session" in build_config and isinstance(build_config["filter_by_session"], dict):
            build_config["filter_by_session"]["value"] = True
        return build_config

    def update_outputs(
        self,
        frontend_node: dict[str, Any],
        field_name: str,
        field_value: Any,
    ) -> dict[str, Any]:
        """Dynamically adjust output ports when `mode` switches between `Retrieve` and `Store` (`W2-R4-09`)."""
        if field_name == "mode":
            active_mode = str(field_value or "Retrieve").strip()
            if active_mode == "Store":
                frontend_node["outputs"] = [
                    Output(
                        display_name="Stored Message",
                        name="stored_message",
                        method="store_message",
                        types=["Message"],
                    ).to_dict(),
                    Output(
                        display_name="Table Records",
                        name="table",
                        method="retrieve_history_table",
                        types=["Data", "DataFrame"],
                    ).to_dict(),
                    Output(
                        display_name="Replay State",
                        name="replay_state",
                        method="retrieve_replay_state",
                        types=["Data"],
                    ).to_dict(),
                ]
            else:
                frontend_node["outputs"] = [
                    Output(
                        display_name="Chat History",
                        name="messages",
                        method="retrieve_chat_history",
                        types=["Message"],
                    ).to_dict(),
                    Output(
                        display_name="Table Records",
                        name="table",
                        method="retrieve_history_table",
                        types=["Data", "DataFrame"],
                    ).to_dict(),
                    Output(
                        display_name="Replay State",
                        name="replay_state",
                        method="retrieve_replay_state",
                        types=["Data"],
                    ).to_dict(),
                ]
        return frontend_node

    def _resolve_external_memory(self) -> SnowflakeChatMessageHistoryAdapter | Any | None:
        ext = getattr(self, "memory", None)
        if ext is None or ext == "":
            return None
        if isinstance(ext, SnowflakeMemoryBaseNativeComponent):
            return ext.build_memory()
        if isinstance(ext, SnowflakeChatMessageHistoryAdapter):
            return ext
        if hasattr(ext, "build_memory") and callable(ext.build_memory):
            return ext.build_memory()
        if hasattr(ext, "add_message") or hasattr(ext, "get_messages") or hasattr(ext, "messages"):
            return ext
        return None

    def _build_domain_component(self, *, storage_family_override: str | None = None) -> SnowflakeMessageHistoryComponent:
        family = str(
            storage_family_override or getattr(self, "storage_family", "snowflake_postgres") or "snowflake_postgres"
        ).strip().lower()
        is_pg = family == "snowflake_postgres"
        role = LIVE_BRIDGE.active_role
        pg_role = f"pg_{role.lower()}"
        backend = _SHARED_HISTORY_BACKENDS.get(role)
        if backend is None:
            backend = InMemoryMessageHistoryBackend(
                active_role=role,
                snowflake_privileges={role: {("SELECT", "*"), ("INSERT", "*"), ("USAGE", "*")}},
                postgres_privileges={pg_role: {("SELECT", "*"), ("INSERT", "*")}},
            )
            _SHARED_HISTORY_BACKENDS[role] = backend
        else:
            backend.active_role = role
            backend.snowflake_privileges.setdefault(role, {("SELECT", "*"), ("INSERT", "*"), ("USAGE", "*")})
            backend.postgres_privileges.setdefault(pg_role, {("SELECT", "*"), ("INSERT", "*")})
        db = str(getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = str(getattr(self, "schema_name", "CORE") or "CORE").strip().upper()
        tbl = str(getattr(self, "table_name", "CHAT_WINDOW_HISTORY") or "CHAT_WINDOW_HISTORY").strip().upper()
        pg_inst = str(getattr(self, "postgres_instance", "LANGFLOW_PG_INSTANCE") or "LANGFLOW_PG_INSTANCE").strip()
        config = MessageHistoryStorageConfig(
            backend_type=(
                MessageHistoryBackendType.SNOWFLAKE_POSTGRES
                if is_pg
                else MessageHistoryBackendType.SNOWFLAKE_STANDARD_TABLE
            ),
            database=db,
            schema=sch,
            table_name=tbl,
            postgres_instance=pg_inst if is_pg else None,
            postgres_role=pg_role if is_pg else None,
        )
        return SnowflakeMessageHistoryComponent(backend, config)

    def _extract_input_message(self) -> tuple[str, str | None, str | None, str | None]:
        raw = getattr(self, "message_text", None)
        if raw is None or raw == "":
            raw = getattr(self, "message", None)
        if isinstance(raw, Message):
            m_data = getattr(raw, "data", {}) or {}
            return (
                str(raw.text or ""),
                m_data.get("sender") or getattr(raw, "sender", None),
                m_data.get("sender_name") or getattr(raw, "sender_name", None),
                m_data.get("session_id") or getattr(raw, "session_id", None),
            )
        if isinstance(raw, dict):
            return (
                str(raw.get("text") or raw.get("message") or raw.get("content") or ""),
                raw.get("sender"),
                raw.get("sender_name"),
                raw.get("session_id"),
            )
        return (str(raw or ""), None, None, None)

    def _run_history(self, *, mode_override: str | None = None) -> dict[str, Any]:
        explicit = getattr(self, "_explicit_kwargs", set())
        ext_mem = self._resolve_external_memory()

        mem_ref = str(getattr(self, "memory_base", "MEM_2_CHAT_WINDOW") or "MEM_2_CHAT_WINDOW").split(" (")[0].strip()
        storage_family = str(getattr(self, "storage_family", "snowflake_postgres") or "snowflake_postgres")
        sess_id = str(getattr(self, "session_id", "fin-reconcile-session-001") or "fin-reconcile-session-001").strip()
        win_k = int(getattr(self, "window_k", 8) or 8)
        user_id = str(getattr(self, "user_id", "fin_analyst_01") or "fin_analyst_01")
        project_id = str(getattr(self, "project_id", "snowflake_full_memory_enterprise_agent"))

        if ext_mem is not None:
            if "memory_base" not in explicit and getattr(ext_mem, "memory_name", None):
                mem_ref = str(ext_mem.memory_name)
            if "storage_family" not in explicit and getattr(ext_mem, "storage_family", None):
                storage_family = str(ext_mem.storage_family)
            if "session_id" not in explicit and getattr(ext_mem, "session_id", None) not in (None, "", "default"):
                sess_id = str(ext_mem.session_id)
            if "window_k" not in explicit and getattr(ext_mem, "window_k", None):
                win_k = int(ext_mem.window_k)
            if "user_id" not in explicit and getattr(ext_mem, "user_id", None) not in (None, "", "default"):
                user_id = str(ext_mem.user_id)
            if "project_id" not in explicit and getattr(ext_mem, "project_id", None) not in (None, "", "default"):
                project_id = str(ext_mem.project_id)

        msg_str, msg_sender, msg_sender_name, msg_sess_id = self._extract_input_message()
        if "session_id" not in explicit and msg_sess_id:
            sess_id = str(msg_sess_id)
        sender = str(
            getattr(self, "sender", None)
            if "sender" in explicit
            else (msg_sender or getattr(self, "sender", "User") or "User")
        )
        sender_name = str(
            getattr(self, "sender_name", None)
            if "sender_name" in explicit
            else (msg_sender_name or getattr(self, "sender_name", sender) or sender)
        )
        turn_id = str(getattr(self, "turn_id", "") or "").strip()

        mode = str(mode_override or getattr(self, "mode", "Retrieve") or "Retrieve").strip()
        comp = self._build_domain_component(storage_family_override=storage_family)

        ext_messages: list[dict[str, Any]] = []
        if mode == "Store" and msg_str:
            existing_rows = comp.retrieve_messages(
                user_id=user_id,
                project_id=project_id,
                session_id=sess_id,
                limit=max(win_k, 100),
            ).get("Messages", [])
            eff_msg_id = turn_id or f"msg-{sess_id}-{len(existing_rows) + 1}"
            stored = comp.store_message(
                message_id=eff_msg_id,
                user_id=user_id,
                project_id=project_id,
                session_id=sess_id,
                sender=sender,
                sender_name=sender_name,
                visible_text=msg_str,
            )
            if ext_mem is not None and hasattr(ext_mem, "add_message"):
                ext_mem.add_message(
                    msg_str,
                    session_id=sess_id,
                    sender=sender,
                    sender_name=sender_name,
                    user_id=user_id,
                    project_id=project_id,
                    message_id=eff_msg_id,
                )
        else:
            stored = comp.retrieve_messages(
                user_id=user_id,
                project_id=project_id,
                session_id=sess_id,
                limit=win_k,
            )

        if ext_mem is not None:
            if hasattr(ext_mem, "get_message_dicts"):
                ext_messages = ext_mem.get_message_dicts(
                    session_id=sess_id,
                    limit=win_k,
                    user_id=user_id,
                    project_id=project_id,
                )
            elif hasattr(ext_mem, "get_messages"):
                raw_list = ext_mem.get_messages(session_id=sess_id, limit=win_k)
                ext_messages = [
                    dict(m.data) if hasattr(m, "data") and isinstance(m.data, dict) else {"text": getattr(m, "text", str(m))}
                    for m in raw_list
                ]
            elif hasattr(ext_mem, "messages"):
                raw_list = list(getattr(ext_mem, "messages") or [])
                ext_messages = [
                    dict(m.data) if hasattr(m, "data") and isinstance(m.data, dict) else {"text": getattr(m, "text", str(m))}
                    for m in raw_list[-win_k:]
                ]

        recalled = LIVE_BRIDGE.recall_memory(
            mem_ref,
            session_id=sess_id,
            user_id=user_id,
            limit=win_k,
        )
        domain_msgs = list(stored.get("Messages") or stored.get("messages") or [])
        recalled_rows = list(recalled.get("rows") or [])
        resolved_messages = ext_messages or domain_msgs or recalled_rows

        return {
            "memory_base": mem_ref,
            "canonical_memory_type": getattr(ext_mem, "canonical_memory_type", "buffer_window_memory")
            if ext_mem is not None
            else "buffer_window_memory",
            "mode": mode,
            "storage_family": storage_family,
            "postgres_instance": getattr(self, "postgres_instance", "LANGFLOW_PG_INSTANCE"),
            "pg_table": getattr(self, "pg_table", "public.chat_message_history"),
            "session_id": sess_id,
            "window_k": win_k,
            "filter_by_session": True,
            "enable_embedding": False,
            "external_memory_connected": ext_mem is not None,
            "external_memory_name": getattr(ext_mem, "memory_name", None) if ext_mem is not None else None,
            "stored_text": msg_str if mode == "Store" else "",
            "messages": resolved_messages,
            "recall_sql": recalled.get("recall_sql"),
            "replay_state": stored.get("signed_replay_envelopes") or stored.get("replay_state", []),
            "active_role": LIVE_BRIDGE.active_role,
        }

    def retrieve_chat_history(self) -> Message:
        payload = self._run_history()
        return Message(text=json.dumps(payload), **{k: v for k, v in payload.items() if k != "text"})

    def retrieve_messages(self) -> Message:
        return self.retrieve_chat_history()

    def retrieve_history_table(self) -> Data:
        return Data(data=self._run_history())

    def retrieve_table(self) -> Data:
        return self.retrieve_history_table()

    def retrieve_replay_state(self) -> Data:
        payload = self._run_history()
        return Data(
            data={
                "memory_base": payload["memory_base"],
                "session_id": payload["session_id"],
                "replay_state": payload["replay_state"],
                "active_role": payload["active_role"],
            }
        )

    def build_replay_state(self) -> Data:
        return self.retrieve_replay_state()

    def store_message(self) -> Message:
        payload = self._run_history(mode_override="Store")
        return Message(text=json.dumps(payload), **{k: v for k, v in payload.items() if k != "text"})

