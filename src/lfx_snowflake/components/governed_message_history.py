"""Governed Message History (BUG-MEM-MH-01).

Messages are keyed by ``(flow_id, session_id)`` exactly like Langflow's native message table, and every
write is mirrored to the governed ``LANGFLOW_MESSAGES`` table (``LANGFLOW_MESSAGES_TABLE``, default
``<DB>.<SCHEMA>.LANGFLOW_MESSAGES``) protected by ``LANGFLOW_MEMORY_RAP`` (``owner_user`` defaults to
``CURRENT_USER()`` server-side, never taken from the payload). No in-RAM per-role store, no demo defaults.

The governed table is a bridge-owned governance object: SQL is issued as bridge-internal statements
(not inside ``flow_component_sql()``), in the caller session (``mode="caller"``) so the RAP applies.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from lfx_snowflake import sql_safety

MAX_WINDOW_K = 1000


class MessageHistoryError(RuntimeError):
    """Raised when a governed Message History operation cannot be completed (never silently faked)."""

    def __init__(self, message: str, *, error_code: str = "MESSAGE_HISTORY_ERROR", http_status: int = 500) -> None:
        super().__init__(f"{error_code}: {message}")
        self.error_code = error_code
        self.http_status = http_status


def _native_memory_api() -> tuple[Any, Any] | None:
    """Langflow native message persistence (``store_message`` / ``get_messages``) when installed."""
    for mod_name in ("lfx.memory", "langflow.memory"):
        try:
            mod = __import__(mod_name, fromlist=["store_message", "get_messages"])
        except Exception:  # noqa: BLE001
            continue
        store = getattr(mod, "store_message", None)
        get = getattr(mod, "get_messages", None)
        if callable(store) and callable(get):
            return store, get
    return None


def _row_value(row: Any, key: str, idx: int) -> Any:
    if isinstance(row, dict):
        return row.get(key.upper(), row.get(key))
    if isinstance(row, (list, tuple)) and len(row) > idx:
        return row[idx]
    return None


class GovernedMessageHistory:
    """(flow_id, session_id) message history mirrored to the governed ``LANGFLOW_MESSAGES`` table."""

    def __init__(self, bridge: Any, *, flow_id: str, session_id: str, native_api: Any = "auto") -> None:
        flow_id = str(flow_id or "").strip()
        session_id = str(session_id or "").strip()
        if not flow_id:
            raise MessageHistoryError(
                "Message History requires the flow_id of the running flow.", error_code="MISSING_FLOW_ID", http_status=422
            )
        if not session_id:
            raise MessageHistoryError(
                "Message History requires a session_id (graph.session_id or an explicit Session ID).",
                error_code="MISSING_SESSION_ID",
                http_status=422,
            )
        if len(flow_id) > 64 or len(session_id) > 256:
            raise MessageHistoryError("flow_id/session_id exceed the LANGFLOW_MESSAGES column sizes.", http_status=422)
        self.bridge = bridge
        self.flow_id = flow_id
        self.session_id = session_id
        self._native = _native_memory_api() if native_api == "auto" else native_api
        self.table_fqn = sql_safety.validate_fqn(
            bridge._configured_messages_table(), "LANGFLOW_MESSAGES_TABLE", min_parts=3, max_parts=3
        )
        self.last_sql: list[str] = []

    # ------------------------------------------------------------------ SQL
    def _exec(self, sql: str, values: list[tuple[str, Any]]) -> dict[str, Any]:
        self.last_sql.append(sql)
        res = self.bridge.execute_sql(sql, bindings=sql_safety.to_sql_bindings(values), mode="caller")
        if not isinstance(res, dict):
            raise MessageHistoryError("Unexpected response from Snowflake for LANGFLOW_MESSAGES.")
        if res.get("mode") == "offline":
            raise MessageHistoryError(
                "Governed LANGFLOW_MESSAGES unavailable: no Snowflake session (statement not executed).",
                error_code="NOT_CONFIGURED",
                http_status=503,
            )
        if res.get("ok") is False or res.get("error"):
            raise MessageHistoryError(
                f"LANGFLOW_MESSAGES statement failed: {res.get('message') or res.get('error')}",
                error_code=str(res.get("error_code") or "SNOWFLAKE_ERROR"),
                http_status=int(res.get("status_code") or 502),
            )
        return res

    def insert_sql(self) -> str:
        return (
            f"INSERT INTO {sql_safety.quote_fqn(self.table_fqn, min_parts=3)} "
            "(message_id, flow_id, session_id, sender, sender_name, text, created_at) "
            "SELECT ?, ?, ?, ?, ?, ?, TO_TIMESTAMP_TZ(?)"
        )

    def select_sql(self, window_k: int) -> str:
        k = sql_safety.bounded_int(window_k, 8, 1, MAX_WINDOW_K)
        return (
            "SELECT message_id, sender, sender_name, text, created_at, owner_user FROM ("
            f"SELECT * FROM {sql_safety.quote_fqn(self.table_fqn, min_parts=3)} "
            "WHERE flow_id = ? AND session_id = ? "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY replicated_at DESC) = 1"
            ") latest WHERE latest.deleted_in_ui_at IS NULL "
            f"ORDER BY created_at DESC, message_id DESC LIMIT {k}"
        )

    # ------------------------------------------------------------------ API
    def store(self, text: str, *, sender: str = "User", sender_name: str | None = None, message_id: str | None = None) -> dict[str, Any]:
        msg_id = str(message_id or uuid.uuid4().hex)[:64]
        created_at = datetime.now(timezone.utc).isoformat()
        sender = str(sender or "User")[:32]
        sender_name = str(sender_name or sender)[:256]
        native_stored = False
        if self._native is not None:
            native_id = self._store_native(text, sender=sender, sender_name=sender_name)
            native_stored = True
            if native_id and not message_id:
                msg_id = str(native_id)[:64]  # same id as the native row: the LF_MEM mirror MERGE dedupes on it
        self._exec(
            self.insert_sql(),
            [
                ("TEXT", msg_id),
                ("TEXT", self.flow_id),
                ("TEXT", self.session_id),
                ("TEXT", sender),
                ("TEXT", sender_name),
                ("TEXT", str(text or "")),
                ("TEXT", created_at),
            ],
        )
        return {"message_id": msg_id, "created_at": created_at, "native_stored": native_stored}

    def retrieve(self, window_k: int = 8) -> list[dict[str, Any]]:
        res = self._exec(self.select_sql(window_k), [("TEXT", self.flow_id), ("TEXT", self.session_id)])
        out: list[dict[str, Any]] = []
        for row in res.get("rows") or []:
            out.append(
                {
                    "message_id": _row_value(row, "message_id", 0),
                    "sender": _row_value(row, "sender", 1),
                    "sender_name": _row_value(row, "sender_name", 2),
                    "text": _row_value(row, "text", 3),
                    "created_at": str(_row_value(row, "created_at", 4) or ""),
                    "owner_user": _row_value(row, "owner_user", 5),
                    "flow_id": self.flow_id,
                    "session_id": self.session_id,
                }
            )
        out.reverse()  # chronological order: oldest of the window first
        return out

    def _store_native(self, text: str, *, sender: str, sender_name: str) -> str | None:
        store, _get = self._native
        try:
            from lfx.schema.message import Message  # noqa: PLC0415

            stored = store(
                Message(text=str(text or ""), sender=sender, sender_name=sender_name, session_id=self.session_id),
                flow_id=self.flow_id,
            )
            first = stored[0] if isinstance(stored, list) and stored else None
            return str(getattr(first, "id", "") or "") or None
        except Exception as exc:  # noqa: BLE001
            raise MessageHistoryError(f"Langflow native message persistence failed: {exc}") from exc
