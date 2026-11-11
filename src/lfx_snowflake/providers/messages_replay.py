"""BUG-PROXY-A4-01: cross-turn replay of Anthropic Messages tool loops (thinking + tool_use + tool_result).

Langflow keeps only the final assistant text in chat history, so on turn N+1 the model no longer sees the
``thinking`` blocks (with their ``signature``), ``tool_use`` and ``tool_result`` of turn N and denies having
called tools. This module records, per conversation scope, the exact message sequence of each completed
tool loop and splices it back in place of the plain-text assistant message on later turns.

Anthropic Messages contract honoured here:
  * signed ``thinking`` / ``redacted_thinking`` blocks are replayed byte-for-byte (never edited) and only when
    the current request has extended thinking enabled; otherwise they are dropped as a whole block;
  * every ``tool_use`` is followed by its matching ``tool_result`` (ToolMessage) in the replayed sequence; when
    the current request has no tools bound, the loop is replaced by a structured text summary instead
    (``tool_use`` without ``tools`` would be rejected).
Messages are duck-typed (``.type`` in human/ai/tool, ``.content``, ``.tool_calls``) so LangChain is optional.
"""

from __future__ import annotations

import contextvars
import copy
import hashlib
import json
import threading
import time
from collections import OrderedDict
from typing import Any, Iterable

CURRENT_LLM_SESSION: contextvars.ContextVar[str | None] = contextvars.ContextVar("sflf_llm_session_id", default=None)
_THINKING_TYPES = {"thinking", "redacted_thinking"}
_SUMMARY_MAX_CHARS = 2000


def _mtype(msg: Any) -> str:
    return str(getattr(msg, "type", "") or (msg.get("type") if isinstance(msg, dict) else "") or "")


def _content(msg: Any) -> Any:
    return getattr(msg, "content", None) if not isinstance(msg, dict) else msg.get("content")


def _tool_calls(msg: Any) -> list:
    tc = getattr(msg, "tool_calls", None) if not isinstance(msg, dict) else msg.get("tool_calls")
    return list(tc or [])


def message_text(msg: Any) -> str:
    content = _content(msg)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(b.get("text") or "") if isinstance(b, dict) and b.get("type") == "text" else (b if isinstance(b, str) else "")
            for b in content
        )
    return ""


def _is_plain_ai(msg: Any) -> bool:
    if _mtype(msg) not in ("ai", "AIMessageChunk") or _tool_calls(msg):
        return False
    content = _content(msg)
    if isinstance(content, str):
        return True
    return isinstance(content, list) and all(
        isinstance(b, str) or (isinstance(b, dict) and b.get("type") == "text") for b in content
    )


def _has_tool_calls(seq: list[Any]) -> bool:
    return any(_mtype(m) == "tool" or _tool_calls(m) for m in seq)


def _has_loop(seq: list[Any]) -> bool:
    return any(_mtype(m) == "tool" or _tool_calls(m) for m in seq) or any(
        isinstance(b, dict) and b.get("type") in _THINKING_TYPES
        for m in seq
        if isinstance(_content(m), list)
        for b in _content(m)
    )


def current_session_id() -> str | None:
    """Conversation session: explicit CURRENT_LLM_SESSION > bound trace session > current flow run record."""
    sid = CURRENT_LLM_SESSION.get()
    if sid:
        return str(sid)
    try:
        from lfx_snowflake.traceability import CURRENT_SESSION_ID  # noqa: PLC0415

        sid = CURRENT_SESSION_ID.get()
        if sid:
            return str(sid)
        from lfx_snowflake.telemetry import CURRENT_RUN_RECORD  # noqa: PLC0415

        rec = CURRENT_RUN_RECORD.get()
        if rec is not None and rec.session_id:
            return str(rec.session_id)
    except Exception:  # noqa: BLE001
        return None
    return None


def _scope(session_id: str | None = None) -> str | None:
    """Isolation scope ``user|flow|session``; ``None`` (no replay at all) when no session is known."""
    session = str(session_id) if session_id else current_session_id()
    if not session:
        return None
    parts: list[str] = []
    try:
        from lfx_snowflake import identity as _ident

        parts.append(str(_ident.CURRENT_IDENTITY.get().sf_user or ""))
    except Exception:  # noqa: BLE001
        parts.append("")
    try:
        from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

        parts.append(str(CURRENT_SNOWFLAKE_FLOW_ID.get() or ""))
    except Exception:  # noqa: BLE001
        parts.append("")
    parts.append(session)
    return "|".join(parts)


def _key(scope: str, human_text: str, ai_text: str) -> str:
    return hashlib.sha256(json.dumps([scope, human_text, ai_text]).encode()).hexdigest()


class MessagesReplayStore:
    """Bounded, TTL'd, thread-safe store of completed tool loops."""

    def __init__(self, max_entries: int = 512, ttl_seconds: float = 6 * 3600.0) -> None:
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self._data: OrderedDict[str, tuple[float, list[Any]]] = OrderedDict()
        self._lock = threading.Lock()

    def put(self, key: str, seq: list[Any]) -> None:
        with self._lock:
            self._data[key] = (time.monotonic(), copy.deepcopy(seq))
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)

    def get(self, key: str) -> list[Any] | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if time.monotonic() - entry[0] > self.ttl_seconds:
                self._data.pop(key, None)
                return None
            self._data.move_to_end(key)
            return copy.deepcopy(entry[1])

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


STORE = MessagesReplayStore()


def record_turn(messages: Iterable[Any], output: Any, *, store: MessagesReplayStore = STORE, scope: str | None = None) -> bool:
    """Record ``[AI(tool_use…), Tool…, …, output]`` after the last human message when the loop is complete."""
    msgs = list(messages)
    if output is None or _tool_calls(output):
        return False
    last_h = max((i for i, m in enumerate(msgs) if _mtype(m) == "human"), default=-1)
    if last_h < 0:
        return False
    seq = [*msgs[last_h + 1 :], output]
    if not _has_loop(seq):
        return False
    sc = scope if scope is not None else _scope()
    if not sc:  # no session -> never store (tool results must not be shared across sessions)
        return False
    store.put(_key(sc, message_text(msgs[last_h]), message_text(output)), seq)
    return True


def _strip_thinking(msg: Any) -> Any:
    content = _content(msg)
    if not isinstance(content, list):
        return msg
    kept = [b for b in content if not (isinstance(b, dict) and b.get("type") in _THINKING_TYPES)]
    if isinstance(msg, dict):
        return {**msg, "content": kept}
    try:
        return msg.model_copy(update={"content": kept})
    except AttributeError:
        clone = copy.copy(msg)
        clone.content = kept
        return clone


def _summarize(seq: list[Any], final_msg: Any) -> Any:
    lines: list[str] = []
    for m in seq[:-1]:
        for tc in _tool_calls(m):
            name = tc.get("name") if isinstance(tc, dict) else getattr(tc, "name", "")
            args = tc.get("args") if isinstance(tc, dict) else getattr(tc, "args", {})
            lines.append(f"- tool_use {name}({json.dumps(args, default=str)[:400]})")
        if _mtype(m) == "tool":
            lines.append(f"  tool_result: {message_text(m)[:400]}")
    summary = ("[Tool calls made in this turn]\n" + "\n".join(lines))[:_SUMMARY_MAX_CHARS]
    text = f"{summary}\n\n{message_text(final_msg)}"
    if isinstance(final_msg, dict):
        return {**final_msg, "content": text}
    try:
        return final_msg.model_copy(update={"content": text})
    except AttributeError:
        clone = copy.copy(final_msg)
        clone.content = text
        return clone


def rehydrate(
    messages: Iterable[Any],
    *,
    thinking_enabled: bool,
    tools_bound: bool,
    store: MessagesReplayStore = STORE,
    scope: str | None = None,
) -> list[Any]:
    """Replace prior plain-text assistant turns with their recorded tool loop (see module docstring)."""
    msgs = list(messages)
    sc = scope if scope is not None else _scope()
    if not sc:
        return msgs
    out: list[Any] = []
    human_text: str | None = None
    for m in msgs:
        t = _mtype(m)
        if t == "human":
            human_text = message_text(m)
        elif human_text is not None and _is_plain_ai(m):
            seq = store.get(_key(sc, human_text, message_text(m)))
            if seq:
                if _has_tool_calls(seq) and not tools_bound:
                    # tool_use without a tools definition is rejected: keep the facts as a text summary.
                    out.append(_summarize(seq, seq[-1]))
                else:
                    # Same order as recorded: assistant(thinking, tool_use) -> tool_result(s) -> ... -> final.
                    out.extend(seq if thinking_enabled else [_strip_thinking(x) for x in seq])
                human_text = None
                continue
        out.append(m)
    return out
