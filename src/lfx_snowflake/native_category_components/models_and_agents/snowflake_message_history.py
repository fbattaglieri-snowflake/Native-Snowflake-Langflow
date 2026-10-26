from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.components.message_history import (
    InMemoryMessageHistoryBackend,
    MessageHistoryBackendType,
    MessageHistoryStorageConfig,
    SnowflakeMessageHistoryComponent,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE, resolve_canonical_memory_type


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
        "Tables (<DB>.<SCHEMA>) with window_k sliding window, session_id isolation, and ADR-003 "
        "signed replay envelope branching."
    )
    icon = "Snowflake"
    name = "SnowflakeMessageHistory"

    inputs = [
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
            name="mode",
            display_name="Mode",
            options=["Retrieve", "Store"],
            value="Retrieve",
            real_time_refresh=True,
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
        MessageTextInput(
            name="message_text",
            display_name="Message Text (for Store mode)",
            value="",
            advanced=True,
        ),
    ]

    outputs = [
        Output(display_name="Chat History", name="messages", method="retrieve_chat_history"),
        Output(display_name="Table Records", name="table", method="retrieve_history_table"),
        Output(display_name="Replay State", name="replay_state", method="retrieve_replay_state"),
    ]

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

        if matched is not None:
            if "window_k" in build_config and isinstance(build_config["window_k"], dict) and matched.get("window_k"):
                build_config["window_k"]["value"] = int(matched["window_k"])
            if "session_id" in build_config and isinstance(build_config["session_id"], dict) and matched.get("session_id"):
                build_config["session_id"]["value"] = str(matched["session_id"])
        if "filter_by_session" in build_config and isinstance(build_config["filter_by_session"], dict):
            build_config["filter_by_session"]["value"] = True
        return build_config

    def _build_domain_component(self) -> SnowflakeMessageHistoryComponent:
        family = str(getattr(self, "storage_family", "snowflake_postgres") or "snowflake_postgres").strip().lower()
        is_pg = family == "snowflake_postgres"
        role = LIVE_BRIDGE.active_role
        pg_role = f"pg_{role.lower()}"
        backend = InMemoryMessageHistoryBackend(
            active_role=role,
            snowflake_privileges={role: {("SELECT", "*"), ("INSERT", "*"), ("USAGE", "*")}},
            postgres_privileges={pg_role: {("SELECT", "*"), ("INSERT", "*")}},
        )
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

    def _run_history(self) -> dict[str, Any]:
        mem_ref = str(getattr(self, "memory_base", "MEM_2_CHAT_WINDOW") or "MEM_2_CHAT_WINDOW").split(" (")[0].strip()
        sess_id = str(getattr(self, "session_id", "fin-reconcile-session-001") or "fin-reconcile-session-001").strip()
        win_k = int(getattr(self, "window_k", 8) or 8)
        mode = str(getattr(self, "mode", "Retrieve") or "Retrieve").strip()
        comp = self._build_domain_component()

        if mode == "Store" and getattr(self, "message_text", ""):
            stored = comp.store_message(
                message_id=f"msg-{sess_id}-1",
                user_id=str(getattr(self, "user_id", "fin_analyst_01") or "fin_analyst_01"),
                project_id=str(getattr(self, "project_id", "snowflake_full_memory_enterprise_agent")),
                session_id=sess_id,
                sender=str(getattr(self, "sender", "User") or "User"),
                sender_name=str(getattr(self, "sender_name", "User") or "User"),
                visible_text=str(getattr(self, "message_text", "")),
            )
        else:
            stored = comp.retrieve_messages(
                user_id=str(getattr(self, "user_id", "fin_analyst_01") or "fin_analyst_01"),
                project_id=str(getattr(self, "project_id", "snowflake_full_memory_enterprise_agent")),
                session_id=sess_id,
                limit=win_k,
            )

        recalled = LIVE_BRIDGE.recall_memory(
            mem_ref,
            session_id=sess_id,
            user_id=str(getattr(self, "user_id", "fin_analyst_01") or "fin_analyst_01"),
            limit=win_k,
        )
        return {
            "memory_base": mem_ref,
            "canonical_memory_type": "buffer_window_memory",
            "mode": mode,
            "storage_family": getattr(self, "storage_family", "snowflake_postgres"),
            "postgres_instance": getattr(self, "postgres_instance", "LANGFLOW_PG_INSTANCE"),
            "pg_table": getattr(self, "pg_table", "public.chat_message_history"),
            "session_id": sess_id,
            "window_k": win_k,
            "filter_by_session": True,
            "enable_embedding": False,
            "messages": recalled.get("rows") or stored.get("messages", []),
            "recall_sql": recalled.get("recall_sql"),
            "replay_state": stored.get("replay_state", []),
            "active_role": LIVE_BRIDGE.active_role,
        }

    def retrieve_chat_history(self) -> Message:
        payload = self._run_history()
        return Message(text=json.dumps(payload))

    def retrieve_history_table(self) -> Data:
        return Data(data=self._run_history())

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
