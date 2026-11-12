"""Governed ingestion into ``LF_MEM_<NAME>`` Memory Bases (BUG-MEM-PROV-01 / BUG-MEM-MB-01 auto-capture).

Langflow 1.13 auto-capture calls ``langflow.services.memory_base.ingestion.on_flow_output`` after every flow run;
for each attached Memory Base with ``auto_capture`` it evaluates the ``threshold`` and starts an ingestion job.
For Snowflake ``LF_MEM`` Memory Bases (``memory_base.kb_name`` marker) that per-base step is redirected here:

1. the run's native messages for ``(flow_id, session_id)`` are mirrored (idempotent ``MERGE`` on ``message_id``)
   into the governed ``LANGFLOW_MESSAGES`` table (``owner_user`` = ``CURRENT_USER()``, RAP-protected);
2. the session's messages newer than the ingestion cursor are counted; below ``threshold`` nothing happens;
3. optional preprocessing runs ``AI_COMPLETE(<model literal>, ?)``; an exact kill-phrase answer (``NO_INGEST``)
   writes a tombstone row that only advances the cursor;
4. otherwise the chunks are inserted with ``AI_EMBED(<model literal>, ?)``, ``session_id``, ``owner_user``,
   ``job_id`` and ``ingested_at``; ``metadata.source_last_created_at`` is the cursor used by ``pending_count``.

Work runs off the turn's critical path on a batched background writer (A4's ``TelemetryWriter`` pattern: queue,
one daemon thread, ``flush`` and an ``atexit`` drain). Each job runs in the submitting turn's context
(identity + flow ContextVars) and in ``flow_component_sql()``, so the flow ``[ ❄️ Context ]`` must contain the
LF_MEM table. Failures (``AI_EMBED``/``AI_COMPLETE`` errors, offline) are logged and recorded in
:meth:`LfMemIngestionPipeline.status`; no vector is ever fabricated.
"""

from __future__ import annotations

import contextvars
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from lfx_snowflake import sql_safety
from lfx_snowflake.components.ai_function import plain_text_result
from lfx_snowflake.memories_lf_mem import (
    iso_timestamp,
    DEFAULT_KILL_PHRASE,
    MAX_INGEST_BATCH,
    LfMemError,
    LfMemSpec,
    build_preprocessing_request,
    format_batch,
    contains_kill_token,
    is_kill_phrase,
    threshold_reached,
)

_LOG = logging.getLogger("lfx_snowflake.memories.ingestion")


def _row(row: Any, key: str, idx: int) -> Any:
    if isinstance(row, dict):
        return row.get(key.upper(), row.get(key))
    if isinstance(row, (list, tuple)) and len(row) > idx:
        return row[idx]
    return None


class LfMemIngestionPipeline:
    """Threshold / kill-phrase / AI_EMBED ingestion for LF_MEM Memory Bases, executed on a background writer."""

    def __init__(self, bridge: Any = None, *, background: bool = True) -> None:
        self._bridge = bridge
        self._lock = threading.Lock()
        self._status: dict[str, dict[str, Any]] = {}
        self._writer = None
        if background:
            from lfx_snowflake.telemetry import TelemetryWriter  # noqa: PLC0415

            self._writer = TelemetryWriter(lambda job, _binds, _tag: self.process(job), batch_size=20, interval_s=0.25)

    @property
    def bridge(self) -> Any:
        if self._bridge is None:
            from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE  # noqa: PLC0415 - import cycle

            self._bridge = LIVE_BRIDGE
        return self._bridge

    # ------------------------------------------------------------------ API
    def submit_turn(
        self,
        spec: LfMemSpec,
        *,
        flow_id: str,
        session_id: str,
        messages: list[dict[str, Any]],
        threshold: int,
        preprocessing: bool = False,
        preproc_model: str | None = None,
        preproc_instructions: str | None = None,
        kill_phrase: str | None = None,
    ) -> str:
        """Queue one run's capture for this Memory Base; returns the job_id (status via :meth:`status`)."""
        job_id = uuid.uuid4().hex
        job = {
            "job_id": job_id,
            "spec": spec,
            "flow_id": str(flow_id),
            "session_id": str(session_id),
            "messages": list(messages or []),
            "threshold": max(1, int(threshold or 1)),
            "preprocessing": bool(preprocessing),
            "preproc_model": preproc_model,
            "preproc_instructions": preproc_instructions,
            "kill_phrase": (kill_phrase or DEFAULT_KILL_PHRASE).strip() or DEFAULT_KILL_PHRASE,
        }
        self._set_status(spec, job_id=job_id, state="QUEUED", session_id=job["session_id"])
        ctx = contextvars.copy_context()  # identity (caller's rights) + flow ContextVars of this turn
        if self._writer is None:
            ctx.run(self.process, job)
        else:
            self._writer.submit(job, None, None, ctx=ctx)  # type: ignore[arg-type]
        return job_id

    def flush(self, timeout: float | None = None) -> bool:
        return True if self._writer is None else self._writer.flush(timeout)

    def status(self, memory_base_id: str) -> dict[str, Any] | None:
        with self._lock:
            st = self._status.get(str(memory_base_id))
            return dict(st) if st else None

    # ------------------------------------------------------------------ worker
    def _set_status(self, spec: LfMemSpec, **values: Any) -> None:
        with self._lock:
            st = self._status.setdefault(spec.memory_base_id, {"memory_base_id": spec.memory_base_id})
            st.update(values)
            st["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _exec(self, sql: str, bindings: dict[str, Any], *, guarded: bool, flow_id: str) -> dict[str, Any]:
        from lfx_snowflake import flow_context_guard as _guard  # noqa: PLC0415
        from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID  # noqa: PLC0415

        token = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_id)
        try:
            if guarded:
                with _guard.flow_component_sql():
                    res = self.bridge.execute_sql(sql, bindings=bindings, mode="caller")
            else:
                res = self.bridge.execute_sql(sql, bindings=bindings, mode="caller")
        finally:
            CURRENT_SNOWFLAKE_FLOW_ID.reset(token)
        if not isinstance(res, dict) or res.get("mode") == "offline" or res.get("ok") is False:
            err = res if isinstance(res, dict) else {}
            raise LfMemError(
                str(err.get("message") or "Snowflake statement failed."),
                error_code=str(err.get("error_code") or ("NOT_CONFIGURED" if err.get("mode") == "offline" else "SNOWFLAKE_ERROR")),
                http_status=int(err.get("status_code") or 502),
            )
        return res

    def _mirror_messages(self, messages_fqn: str, flow_id: str, session_id: str, messages: list[dict[str, Any]]) -> int:
        """Idempotent copy of the run's native messages into governed LANGFLOW_MESSAGES (bridge-internal SQL)."""
        rows = [m for m in messages if str(m.get("text") or "").strip() and m.get("message_id")]
        if not rows:
            return 0
        tuples, values = [], []
        for m in rows[:MAX_INGEST_BATCH]:
            tuples.append("SELECT ? AS message_id, ? AS flow_id, ? AS session_id, ? AS sender, ? AS sender_name, ? AS text, TO_TIMESTAMP_TZ(?) AS created_at")
            values += [
                ("TEXT", str(m["message_id"])[:64]), ("TEXT", flow_id), ("TEXT", session_id),
                ("TEXT", str(m.get("sender") or "User")[:32]), ("TEXT", str(m.get("sender_name") or m.get("sender") or "")[:256]),
                ("TEXT", str(m.get("text") or "")), ("TEXT", iso_timestamp(m.get("created_at"))),
            ]
        target = sql_safety.quote_fqn(messages_fqn, "LANGFLOW_MESSAGES_TABLE", min_parts=3, max_parts=3)
        sql = (
            f"MERGE INTO {target} t USING ({' UNION ALL '.join(tuples)}) s ON t.message_id = s.message_id "
            "WHEN NOT MATCHED THEN INSERT (message_id, flow_id, session_id, sender, sender_name, text, created_at) "
            "VALUES (s.message_id, s.flow_id, s.session_id, s.sender, s.sender_name, s.text, s.created_at)"
        )
        self._exec(sql, sql_safety.to_sql_bindings(values), guarded=False, flow_id=flow_id)
        return len(tuples)

    def process(self, job: dict[str, Any]) -> dict[str, Any]:
        spec: LfMemSpec = job["spec"]
        fid, sid, job_id = job["flow_id"], job["session_id"], job["job_id"]
        try:
            messages_fqn = self.bridge._configured_messages_table()
            self._mirror_messages(messages_fqn, fid, sid, job["messages"])
            turn = [m for m in job["messages"] if m.get("message_id") and str(m.get("text") or "").strip()]
            if any(contains_kill_token(m.get("text"), job["kill_phrase"]) for m in turn):
                # Kill-only turn: consume the WHOLE turn (user message + reply) with one tombstone.
                meta = {
                    "source_last_created_at": max(iso_timestamp(m.get("created_at")) for m in turn),
                    "source_message_ids": [str(m["message_id"])[:64] for m in turn],
                }
                ssql, sb = spec.build_skip_request(flow_id=fid, session_id=sid, job_id=job_id, metadata=meta)
                self._exec(ssql, sb, guarded=True, flow_id=fid)
                return self._done(spec, job_id, "SKIPPED_KILL_PHRASE", sid, pending_count=0, skipped=len(turn))
            csql, cb = spec.build_cursor_request(flow_id=fid, session_id=sid)
            crow = (self._exec(csql, cb, guarded=True, flow_id=fid).get("rows") or [None])[0]
            cursor_ts = _row(crow, "cursor_ts", 0) if crow is not None else None
            # Governed LANGFLOW_MESSAGES is bridge-owned (guard exemption is append-only): read outside the flow
            # guard, owner-scoped by LANGFLOW_MEMORY_RAP in the caller session.
            sql, binds = LfMemSpec.build_pending_messages_request(
                messages_fqn, flow_id=fid, session_id=sid, cursor_ts=iso_timestamp(cursor_ts, default_now=False)
            )
            pending = [
                {"message_id": _row(r, "message_id", 0), "sender": _row(r, "sender", 1), "sender_name": _row(r, "sender_name", 2),
                 "text": _row(r, "text", 3), "created_at": iso_timestamp(_row(r, "created_at", 4))}
                for r in (self._exec(sql, binds, guarded=False, flow_id=fid).get("rows") or [])
            ]
            if not threshold_reached(len(pending), job["threshold"]):
                return self._done(spec, job_id, "PENDING", sid, pending_count=len(pending))
            cursor = max(m["created_at"] for m in pending)
            meta = {"source_last_created_at": cursor, "source_message_ids": [m["message_id"] for m in pending]}
            if job["preprocessing"]:
                psql, pb = build_preprocessing_request(
                    str(job["preproc_model"] or ""), str(job["preproc_instructions"] or ""), format_batch(pending), job["kill_phrase"]
                )
                rows = self._exec(psql, pb, guarded=True, flow_id=fid).get("rows") or []
                # AI_COMPLETE returns its text JSON-encoded ("...", escaped newlines): memories store plain text.
                response = str(plain_text_result(_row(rows[0], "response", 0)) or "") if rows else ""
                if not response.strip():
                    raise LfMemError("AI_COMPLETE preprocessing returned no output.", error_code="PREPROCESSING_EMPTY")
                if is_kill_phrase(response, job["kill_phrase"]):
                    ssql, sb = spec.build_skip_request(flow_id=fid, session_id=sid, job_id=job_id, metadata=meta)
                    self._exec(ssql, sb, guarded=True, flow_id=fid)
                    return self._done(spec, job_id, "SKIPPED_KILL_PHRASE", sid, pending_count=0, skipped=len(pending))
                chunks = [{"text": response.strip(), "session_id": sid, "metadata": meta}]
            else:
                chunks = [
                    {"text": m["text"], "session_id": sid, "sender": m["sender"], "sender_name": m["sender_name"],
                     "metadata": {**meta, "source_message_id": m["message_id"]}}
                    for m in pending
                ]
            isql, ib, receipt = spec.build_ingest_request(chunks, flow_id=fid, job_id=job_id, kill_phrase=job["kill_phrase"])
            if isql is None:
                ssql, sb = spec.build_skip_request(flow_id=fid, session_id=sid, job_id=job_id, metadata=meta)
                self._exec(ssql, sb, guarded=True, flow_id=fid)
                return self._done(spec, job_id, "SKIPPED_KILL_PHRASE", sid, pending_count=0, skipped=len(pending))
            self._exec(isql, ib, guarded=True, flow_id=fid)
            return self._done(
                spec, job_id, "INGESTED", sid, pending_count=0, ingested=len(receipt["ingested_chunk_ids"]),
                last_sync_at=datetime.now(timezone.utc).isoformat(),
            )
        except (LfMemError, sql_safety.SqlSafetyError, ValueError) as exc:
            _LOG.error("LF_MEM ingestion failed memory_base=%s job=%s: %s", spec.memory_base_id, job_id, exc)
            return self._done(spec, job_id, "FAILED", sid, error=str(exc), error_code=getattr(exc, "error_code", "INGESTION_ERROR"))

    def _done(self, spec: LfMemSpec, job_id: str, state: str, session_id: str, **values: Any) -> dict[str, Any]:
        out = {"job_id": job_id, "state": state, "session_id": session_id, "error": None, "error_code": None, **values}
        self._set_status(spec, **out)
        if state != "FAILED":
            _LOG.info("LF_MEM ingestion memory_base=%s job=%s state=%s", spec.memory_base_id, job_id, state)
        return out


PIPELINE = LfMemIngestionPipeline()


# ---------------------------------------------------------------------- native auto-capture hook
def _message_dict(msg: Any) -> dict[str, Any]:
    ts = getattr(msg, "timestamp", None)
    return {
        "message_id": str(getattr(msg, "id", "") or ""),
        "sender": getattr(msg, "sender", None),
        "sender_name": getattr(msg, "sender_name", None),
        "text": getattr(msg, "text", None),
        "created_at": iso_timestamp(ts),
    }


def _as_uuid(value: Any) -> uuid.UUID | None:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


async def _owns_flow(mb: Any) -> bool:
    """Our owner check replacing upstream ``resolve_owned_memory_flow`` for LF_MEM bases (fail closed).

    Upstream rejects every UUIDv5 flow id as "virtual" (``is_virtual_flow_id``), but the bridge's
    deterministic flow ids are uuid5, so existing flows raised ``MemoryBaseFlowNotFoundError``.
    Here: the stored Flow row must exist with ``id == mb.flow_id`` and ``user_id == mb.user_id``.
    """
    flow_id, user_id = _as_uuid(getattr(mb, "flow_id", None)), _as_uuid(getattr(mb, "user_id", None))
    if flow_id is None or user_id is None:
        return False
    try:
        from langflow.services.database.models.flow.model import Flow  # noqa: PLC0415
        from langflow.services.deps import session_scope  # noqa: PLC0415
        from sqlmodel import select  # noqa: PLC0415

        async with session_scope() as db:
            row = (await db.exec(select(Flow.id).where(Flow.id == flow_id).where(Flow.user_id == user_id))).first()
    except Exception:  # noqa: BLE001 - fail closed on any lookup failure
        _LOG.exception("LF_MEM auto-capture owner check failed memory_base=%s", getattr(mb, "id", None))
        return False
    return row is not None


async def capture_snowflake_memory_base(
    mb: Any, session_id: str, *, pipeline: LfMemIngestionPipeline | None = None, owner_check: Any = None
) -> str | None:
    """Per-Memory-Base auto-capture for LF_MEM bases; None when ``mb`` is not a Snowflake Memory Base."""
    spec = LfMemSpec.from_kb_name(getattr(mb, "kb_name", None))
    if spec is None:
        return None
    if not await (owner_check or _owns_flow)(mb):
        _LOG.warning("LF_MEM auto-capture refused memory_base=%s: flow not owned by memory base owner", getattr(mb, "id", None))
        return None
    from langflow.memory import aget_messages  # noqa: PLC0415

    msgs = await aget_messages(session_id=session_id, flow_id=mb.flow_id, order="ASC", user_id=mb.user_id)
    return (pipeline or PIPELINE).submit_turn(
        spec,
        flow_id=str(mb.flow_id),
        session_id=str(session_id),
        messages=[_message_dict(m) for m in msgs],
        threshold=int(getattr(mb, "threshold", 1) or 1),
        preprocessing=bool(getattr(mb, "preprocessing", False)),
        preproc_model=getattr(mb, "preproc_model", None),
        preproc_instructions=getattr(mb, "preproc_instructions", None),
        kill_phrase=getattr(mb, "preproc_kill_phrase", None),
    )


def install_snowflake_auto_capture(ingestion_module: Any) -> bool:
    """Redirect ``ingestion._maybe_trigger`` for LF_MEM Memory Bases; other bases keep the native path."""
    if getattr(ingestion_module, "_snowflake_lf_mem_capture", False):
        return False
    native_maybe_trigger = ingestion_module._maybe_trigger

    async def _maybe_trigger(*, mb, session_id, job_id, get_or_create_session):  # type: ignore[no-untyped-def]
        if LfMemSpec.from_kb_name(getattr(mb, "kb_name", None)) is not None:
            try:
                await capture_snowflake_memory_base(mb, session_id)
            except Exception:  # noqa: BLE001 - never fall through to the native resolver for LF_MEM bases
                _LOG.exception("LF_MEM auto-capture failed memory_base=%s", getattr(mb, "id", None))
            return None
        return await native_maybe_trigger(mb=mb, session_id=session_id, job_id=job_id, get_or_create_session=get_or_create_session)

    ingestion_module._maybe_trigger = _maybe_trigger
    ingestion_module._snowflake_lf_mem_capture = True
    return True
