from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, FloatInput, MessageTextInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message
from pydantic import BaseModel, Field

from lfx_snowflake import memories_typed as _typed
from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.context_helpers import _resolve_flow_id
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.providers.language_models import build_structured_tool

OP_RECALL = "Recall"
OP_SAVE = "Save"
OP_CLEANUP = "Clean up"
NO_MEMORY = "None"

KIND_LABELS = {
    _typed.CHECKPOINTER: "Checkpointer",
    _typed.SUMMARY: "Summary",
    _typed.PROFILE: "Profile",
    _typed.EPISODIC: "Episodic",
}


def typed_memory_options(flow_id: str | None = None) -> list[str]:
    """Typed memories of the flow `[ ❄️ Context ]`, read back from Snowflake."""
    opts = [NO_MEMORY]
    try:
        LIVE_BRIDGE.discover_typed_memories(flow_id)
        entries = LIVE_BRIDGE.get_memories_for_flow(flow_id=flow_id)
    except Exception:  # noqa: BLE001
        return opts
    for entry in entries:
        kind = str(entry.get("canonical_memory_type") or "")
        if isinstance(entry.get("lf_mem"), dict) or kind not in _typed.WRITABLE_KINDS:
            continue
        label = f"{entry.get('name')} ({KIND_LABELS[kind]} -> {entry.get('table_fqn')})"
        if label not in opts:
            opts.append(label)
    return opts


def _row_get(row: Any, key: str) -> Any:
    if not isinstance(row, dict):
        return None
    for cand in (key, key.upper(), key.lower()):
        if cand in row:
            return row[cand]
    return None


def format_recall(kind: str, rows: list[Any]) -> str:
    """Plain-text view of recalled rows, ready to be placed in a prompt."""
    if not rows:
        return ""
    if kind == _typed.CHECKPOINTER:
        state = _row_get(rows[0], "state_blob")
        return state if isinstance(state, str) else json.dumps(state, default=str)
    if kind == _typed.SUMMARY:
        return str(_row_get(rows[0], "running_summary") or "")
    if kind == _typed.PROFILE:
        profile = _row_get(rows[0], "profile_json")
        return profile if isinstance(profile, str) else json.dumps(profile, default=str)
    lines = []
    for row in rows:
        text = _row_get(row, "episode_text") or _row_get(row, "text_chunk") or ""
        topic = _row_get(row, "topic")
        score = _row_get(row, "outcome_score")
        head = " ".join(p for p in (f"[{topic}]" if topic else "", f"(outcome {score})" if score is not None else "") if p)
        lines.append(f"- {head} {text}".replace("-  ", "- ").strip())
    return "\n".join(lines)


class _RecallArgs(BaseModel):
    query: str = Field(default="", description="What to look for in the memory; leave empty for the most relevant recent records.")
    topic: str = Field(default="", description="Optional topic to narrow the search.")


class _SaveArgs(BaseModel):
    content: str = Field(description="The text or JSON to store in the memory.")
    topic: str = Field(default="", description="Short topic label for the record (episodic memories).")
    outcome_score: float = Field(
        default=1.0, description="How well it went, from 0.0 (failed) to 1.0 (worked); episodic memories only."
    )
    tool_used: str = Field(default="", description="Tool or approach that was used, if any (episodic memories).")


class SnowflakeAgentMemoryComponent(Component):
    display_name = "Agent Memory"
    description = (
        "Read and write a Snowflake memory created from the Memories page: checkpoint the state of a run, keep a "
        "running summary, store a user profile, or save and recall past episodes. The memory is a table in the "
        "flow context; in Tool Mode an agent decides when to save and when to recall."
    )
    icon = "Snowflake"
    name = "SnowflakeAgentMemory"

    inputs = [
        DropdownInput(
            name="memory",
            display_name="Memory",
            options=[NO_MEMORY],
            value=NO_MEMORY,
            real_time_refresh=True,
            refresh_button=True,
            combobox=True,
            info="A typed memory created from Memories ▸ + Snowflake in a schema of this flow's context.",
        ),
        DropdownInput(
            name="operation",
            display_name="Operation",
            options=[OP_RECALL, OP_SAVE, OP_CLEANUP],
            value=OP_RECALL,
            info="Save stores Content and passes it on unchanged through the Text output.",
        ),
        MessageTextInput(
            name="content",
            display_name="Content",
            value="",
            tool_mode=True,
            info="State, summary, facts or episode to save (text or JSON).",
        ),
        MessageTextInput(
            name="query",
            display_name="Recall Query",
            value="",
            tool_mode=True,
            info="What to look for when recalling episodes.",
        ),
        MessageTextInput(
            name="key",
            display_name="Key",
            value="",
            info="Thread (checkpointer), session (summary), user (profile) or owner of the episodes (episodic). "
            "Empty uses the run session; for episodic memories empty means shared by all sessions.",
        ),
        MessageTextInput(name="topic", display_name="Topic", value="", info="Topic of the episode (episodic)."),
        FloatInput(
            name="outcome_score",
            display_name="Outcome Score",
            value=1.0,
            info="0.0 = failed, 1.0 = worked (episodic). Cleanup keeps the best episodes.",
        ),
        StrInput(name="tool_used", display_name="Tool Used", value="", advanced=True),
        StrInput(
            name="namespace",
            display_name="Namespace",
            value="default",
            advanced=True,
            info="Checkpoint namespace or profile entity namespace.",
        ),
    ]
    outputs = [
        Output(display_name="Result", name="result", method="run_memory"),
        Output(display_name="Text", name="text", method="run_text"),
    ]

    def update_build_config(self, build_config: dict[str, Any], field_value: Any = None, field_name: str | None = None) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if isinstance(build_config.get("memory"), dict):
            build_config["memory"]["options"] = typed_memory_options(flow_id)
        return build_config

    def _flow_id(self) -> str | None:
        return _resolve_flow_id(self)

    def _kind(self) -> str:
        entry, _err = LIVE_BRIDGE._resolve_typed_memory(str(getattr(self, "memory", "") or ""), self._flow_id())
        return str((entry or {}).get("canonical_memory_type") or "")

    def _key(self) -> str:
        key = self._text("key")
        if key:
            return key
        if self._kind() == _typed.EPISODIC:
            # Episodes are meant to be found again in later sessions: without a key they are shared.
            return "default"
        graph = getattr(self, "graph", None)
        return str(getattr(graph, "session_id", "") or "default")

    def _text(self, name: str) -> str:
        val = getattr(self, name, "")
        return str(getattr(val, "text", val) or "").strip()

    def _recall(self, *, query: str = "", topic: str = "") -> dict[str, Any]:
        key = self._key()
        res = LIVE_BRIDGE.recall_memory(
            str(getattr(self, "memory", "") or ""),
            query=query,
            session_id=key,
            thread_id=key,
            user_id=key,
            entity_namespace=str(getattr(self, "namespace", "default") or "default"),
            topic=topic,
            flow_id=self._flow_id(),
        )
        res["text"] = format_recall(str(res.get("canonical_memory_type") or ""), list(res.get("rows") or []))
        return res

    def _save(self, *, content: Any, topic: str = "", outcome_score: float = 1.0, tool_used: str = "") -> dict[str, Any]:
        key = self._key()
        ns = str(getattr(self, "namespace", "default") or "default")
        graph = getattr(self, "graph", None)
        return LIVE_BRIDGE.write_memory(
            str(getattr(self, "memory", "") or ""),
            content=content,
            flow_id=self._flow_id(),
            thread_id=key,
            checkpoint_ns=ns,
            session_id=key,
            user_id=key,
            entity_namespace=ns,
            topic=topic or "general",
            tool_used=tool_used,
            outcome_score=outcome_score,
            metadata={"flow_id": self._flow_id(), "run_id": str(getattr(graph, "run_id", "") or "")},
        )

    def _execute_once(self) -> dict[str, Any]:
        """Both outputs share one execution, so a Save is written once per run."""
        cached = getattr(self, "_memory_result", None)
        if isinstance(cached, dict):
            return cached
        op = str(getattr(self, "operation", OP_RECALL) or OP_RECALL)
        if op == OP_SAVE:
            res = self._save(
                content=self._text("content"),
                topic=self._text("topic"),
                outcome_score=float(getattr(self, "outcome_score", 1.0) if getattr(self, "outcome_score", None) is not None else 1.0),
                tool_used=str(getattr(self, "tool_used", "") or ""),
            )
        elif op == OP_CLEANUP:
            res = LIVE_BRIDGE.cleanup_memory(str(getattr(self, "memory", "") or ""), flow_id=self._flow_id())
        else:
            res = self._recall(query=self._text("query"), topic=self._text("topic"))
        if not res.get("ok"):
            raise ValueError(f"{res.get('error_code') or 'MEMORY_ERROR'}: {res.get('message') or 'The memory operation failed.'}")
        self._memory_result = res
        return res

    def _pre_run_setup(self):
        self._memory_result = None

    def run_memory(self) -> Data:
        return Data(data=self._execute_once())

    def run_text(self) -> Message:
        res = self._execute_once()
        op = str(getattr(self, "operation", OP_RECALL) or OP_RECALL)
        if op == OP_SAVE:
            return Message(text=self._text("content"))
        if op == OP_CLEANUP:
            return Message(text=f"Cleanup removed {res.get('rows_deleted')} record(s) from {res.get('table_fqn')}.")
        return Message(text=str(res.get("text") or ""))

    def to_toolkit(self) -> list[Tool]:
        mem_ref = str(getattr(self, "memory", "") or "").split(" (")[0].strip()
        entry, _err = LIVE_BRIDGE._resolve_typed_memory(mem_ref, self._flow_id())
        entry = entry or {}
        kind = str(entry.get("canonical_memory_type") or _typed.EPISODIC)
        label = KIND_LABELS.get(kind, "Memory").lower()
        slug = "".join(c if c.isalnum() else "_" for c in mem_ref.lower()).strip("_") or "memory"
        instructions = str(entry.get("capture_instructions") or _typed.DEFAULT_CAPTURE_INSTRUCTIONS.get(kind, ""))
        meta = {"component": "SnowflakeAgentMemory", "memory": mem_ref, "canonical_memory_type": kind, "tool_mode": True}

        def _recall_tool(query: str = "", topic: str = "", **_: Any) -> dict[str, Any]:
            res = self._recall(query=query, topic=topic)
            if not res.get("ok"):
                return {"ok": False, "error": res.get("message") or res.get("error_code")}
            return {"ok": True, "records": res.get("text") or "No matching records."}

        def _save_tool(content: str = "", topic: str = "", outcome_score: float = 1.0, tool_used: str = "", **_: Any) -> dict[str, Any]:
            res = self._save(content=content, topic=topic, outcome_score=outcome_score, tool_used=tool_used)
            if not res.get("ok"):
                return {"ok": False, "error": res.get("message") or res.get("error_code")}
            return {"ok": True, "stored_in": res.get("table_fqn")}

        return [
            build_structured_tool(
                name=f"recall_{slug}",
                description=f"Recall records from the {label} memory '{mem_ref}'. Use it before answering when past records may help.",
                func=_recall_tool,
                args_schema=_RecallArgs,
                metadata=meta,
            ),
            build_structured_tool(
                name=f"save_{slug}",
                description=f"Save a record in the {label} memory '{mem_ref}'. {instructions}".strip(),
                func=_save_tool,
                args_schema=_SaveArgs,
                metadata=meta,
            ),
        ]
