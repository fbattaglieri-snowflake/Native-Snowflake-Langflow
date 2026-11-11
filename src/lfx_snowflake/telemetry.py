"""Unified flow-run telemetry (BUG-LLMOP-A4-01, BUG-UI-TRC-01, BUG-MCP-02) and the async telemetry writer.

* ``RunRegistry``: one record per flow execution with a unique ``run_id`` and ``trace_id`` (UUIDs), the canonical
  hyphenated ``flow_id``, the real ``session_id``, ``request_id``, ``cortex_thread_id``, ``a2a_task_id``,
  ``start_ts``/``end_ts`` and ``status``. Every sink (query_tag, FLOW_RUNS, FLOW_SPANS, FLOW_ACTIVITY, AUDIT_LOG)
  takes its ids from here, so they join on ``run_id`` / ``trace_id``.
* ``TelemetryWriter``: telemetry DML leaves the turn's critical path. Requests are queued and drained in
  batches by one daemon thread; ``flush()`` and an ``atexit`` drain make sure queued rows are written on
  shutdown (bounded by ``shutdown_timeout``).
"""

from __future__ import annotations

import atexit
import queue
import threading
import time
import uuid
from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from lfx_snowflake.traceability import canonical_flow_id

CURRENT_RUN_RECORD: ContextVar["RunRecord | None"] = ContextVar("sflf_current_run_record", default=None)
_RUN_NAMESPACE = uuid.UUID("6f1c3a8e-58a4-4d0b-9a7e-1b2c3d4e5f60")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def derived_run_ids(flow_id: Any, discriminator: Any) -> tuple[str, str]:
    """Deterministic (run_id, trace_id) for rows replicated without a live run record (e.g. after restart)."""
    base = f"{canonical_flow_id(flow_id) or ''}:{discriminator}"
    return str(uuid.uuid5(_RUN_NAMESPACE, "run:" + base)), str(uuid.uuid5(_RUN_NAMESPACE, "trace:" + base))


@dataclass
class RunRecord:
    run_id: str
    trace_id: str
    flow_id: str | None
    request_id: str | None = None
    session_id: str | None = None
    cortex_thread_id: str | None = None
    cortex_parent_message_id: str | None = None
    a2a_task_id: str | None = None
    job_id: str | None = None
    start_ts: datetime = field(default_factory=_now)
    end_ts: datetime | None = None
    status: str = "RUNNING"
    components: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["start_ts"] = self.start_ts.isoformat()
        d["end_ts"] = self.end_ts.isoformat() if self.end_ts else None
        return d


class RunRegistry:
    """Bounded in-memory registry of recent flow executions (thread-safe)."""

    def __init__(self, max_runs: int = 2000, match_grace_s: float = 120.0) -> None:
        self._runs: OrderedDict[str, RunRecord] = OrderedDict()
        self._lock = threading.Lock()
        self.max_runs = max_runs
        self.match_grace_s = match_grace_s
        # T10: explicit row -> run linkage stamped while the execution runs (contextvar), so concurrent runs
        # of the same flow are never resolved by time window alone.
        self._row_runs: OrderedDict[str, str] = OrderedDict()
        self._job_runs: OrderedDict[str, str] = OrderedDict()

    def start(
        self,
        *,
        flow_id: Any,
        run_id: str | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
    ) -> RunRecord:
        rec = RunRecord(
            run_id=run_id or str(uuid.uuid4()),
            trace_id=str(uuid.uuid4()),
            flow_id=canonical_flow_id(flow_id),
            request_id=request_id,
            session_id=(str(session_id).strip()[:250] or None) if session_id else None,
        )
        with self._lock:
            self._runs[rec.run_id] = rec
            while len(self._runs) > self.max_runs:
                self._runs.popitem(last=False)
        return rec

    def get(self, run_id: str | None) -> RunRecord | None:
        if not run_id:
            return None
        with self._lock:
            return self._runs.get(run_id)

    def end(self, run_id: str | None, status: str) -> RunRecord | None:
        rec = self.get(run_id)
        if rec is not None and rec.end_ts is None:
            rec.end_ts = _now()
            rec.status = str(status or "SUCCESS").upper()[:32]
        return rec

    def _remember(self, store: OrderedDict[str, str], key: str, run_id: str) -> None:
        with self._lock:
            store[key] = run_id
            store.move_to_end(key)
            while len(store) > self.max_runs * 50:
                store.popitem(last=False)

    def bind_row(self, *, flow_id: Any, vertex_id: Any, ts: Any, row_id: Any = None, run_id: str | None = None) -> RunRecord | None:
        """Stamp a Langflow transaction/vertex_build row with the run that produced it (current run by default)."""
        rec = self.get(run_id) if run_id else CURRENT_RUN_RECORD.get()
        if rec is None:
            return None
        fp = row_fingerprint(flow_id, vertex_id, ts)
        if fp:
            self._remember(self._row_runs, fp, rec.run_id)
        if row_id:
            self._remember(self._row_runs, f"id:{row_id}", rec.run_id)
        return rec

    def link_job(self, run_id: str | None, job_id: Any) -> None:
        rec = self.get(run_id)
        if rec is None or not job_id:
            return
        rec.job_id = rec.job_id or str(job_id)[:64]
        self._remember(self._job_runs, str(job_id), rec.run_id)

    def by_row(self, *, flow_id: Any, vertex_id: Any, ts: Any, row_id: Any = None) -> RunRecord | None:
        with self._lock:
            rid = (self._row_runs.get(f"id:{row_id}") if row_id else None) or self._row_runs.get(
                row_fingerprint(flow_id, vertex_id, ts) or ""
            )
        return self.get(rid)

    def by_job(self, job_id: Any) -> RunRecord | None:
        if not job_id:
            return None
        with self._lock:
            rid = self._job_runs.get(str(job_id))
        return self.get(rid)

    def candidates(self, flow_id: Any, ts: Any) -> list[RunRecord]:
        """Runs of ``flow_id`` whose window ([start - 1 s, end + grace]) contains ``ts``."""
        fid = canonical_flow_id(flow_id)
        when = _parse_ts(ts)
        if when is None:
            return []
        with self._lock:
            runs = [r for r in self._runs.values() if r.flow_id == fid]
        t = when.timestamp()
        return [r for r in runs if r.start_ts.timestamp() - 1.0 <= t <= (r.end_ts or _now()).timestamp() + self.match_grace_s]

    def resolve(
        self,
        *,
        flow_id: Any,
        vertex_id: Any,
        ts: Any,
        row_id: Any = None,
        job_id: Any = None,
        session_id: Any = None,
    ) -> tuple[RunRecord | None, bool]:
        """(run, ambiguous). Explicit linkage first (row stamp, job id), then a *unique* active run.

        ``ambiguous`` is True when several runs could own the row and nothing explicit picks one: the caller
        must not invent a run for it (no synthetic RUN row); it can retry later or leave ``run_id`` NULL.
        """
        rec = self.by_row(flow_id=flow_id, vertex_id=vertex_id, ts=ts, row_id=row_id) or self.by_job(job_id)
        if rec is not None:
            return rec, False
        active = self.candidates(flow_id, ts)
        if session_id:
            active = [r for r in active if r.session_id == str(session_id)]
        if len(active) == 1:
            return active[0], False
        return None, len(active) > 1

    def match(self, flow_id: Any, ts: Any, session_id: Any = None) -> RunRecord | None:
        """The single run of ``flow_id`` active at ``ts`` ([start - 1 s, end + grace]), else ``None``.

        Concurrent runs of the same flow are never collapsed: when several runs are active at ``ts`` the
        row's ``session_id`` must pick exactly one, otherwise the match is ambiguous and ``None`` is returned
        (the caller then derives an id unique to that row). A row without a timestamp never matches.
        """
        fid = canonical_flow_id(flow_id)
        when = _parse_ts(ts)
        if when is None:
            return None
        with self._lock:
            runs = [r for r in self._runs.values() if r.flow_id == fid]
        t = when.timestamp()
        active = [
            r
            for r in runs
            if r.start_ts.timestamp() - 1.0 <= t <= (r.end_ts or _now()).timestamp() + self.match_grace_s
        ]
        if session_id:
            active = [r for r in active if r.session_id == str(session_id)] or active
        return active[0] if len(active) == 1 else None

def row_fingerprint(flow_id: Any, vertex_id: Any, ts: Any) -> str | None:
    when = _parse_ts(ts)
    if when is None or not vertex_id:
        return None
    return f"{canonical_flow_id(flow_id) or ''}|{vertex_id}|{when.astimezone(timezone.utc).isoformat(timespec='milliseconds')}"


def utc_iso(ts: Any) -> str | None:
    """T10: every telemetry timestamp as ISO-8601 UTC with an explicit ``+00:00`` offset (naive = UTC)."""
    when = _parse_ts(ts)
    return when.astimezone(timezone.utc).isoformat() if when is not None else None


def _parse_ts(ts: Any) -> datetime | None:
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if not ts:
        return None
    try:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def annotate_current_run(**values: Any) -> None:
    """Attach real ids (session_id, cortex_thread_id, cortex_parent_message_id, a2a_task_id) to the current run."""
    rec = CURRENT_RUN_RECORD.get()
    if rec is None:
        return
    for key, value in values.items():
        if value is None or value == "" or not hasattr(rec, key):
            continue
        if key == "components":
            continue
        if getattr(rec, key) in (None, ""):
            setattr(rec, key, str(value)[:250])


RUNS = RunRegistry()


class TelemetryWriter:
    """Queue + one daemon thread draining batches; synchronous when ``enabled`` is False (tests/local)."""

    def __init__(
        self,
        execute: Callable[[str, dict[str, Any] | None, str | None], Any],
        *,
        batch_size: int = 50,
        interval_s: float = 0.5,
        max_queue: int = 10000,
        shutdown_timeout: float = 10.0,
    ) -> None:
        self._execute = execute
        self.batch_size = batch_size
        self.interval_s = interval_s
        self.shutdown_timeout = shutdown_timeout
        self._q: queue.Queue[tuple[str, dict[str, Any] | None, str | None, Any]] = queue.Queue(maxsize=max_queue)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._stopped = False
        self.written = 0
        self.failed = 0
        atexit.register(self.close)

    def submit(self, sql: str, bindings: dict[str, Any] | None = None, query_tag: str | None = None, *, ctx: Any = None) -> None:
        if self._stopped:
            self._run_one((sql, bindings, query_tag, ctx))
            return
        self._ensure_thread()
        try:
            self._q.put_nowait((sql, bindings, query_tag, ctx))
        except queue.Full:
            # Back-pressure: never drop a row; write it inline instead.
            self._run_one((sql, bindings, query_tag, ctx))

    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._loop, name="sflf-telemetry", daemon=True)
                self._thread.start()

    def _run_one(self, item: tuple[str, dict[str, Any] | None, str | None, Any]) -> None:
        sql, binds, tag, ctx = item
        try:
            if ctx is not None:
                ctx.run(self._execute, sql, binds, tag)
            else:
                self._execute(sql, binds, tag)
            self.written += 1
        except Exception:  # noqa: BLE001 - telemetry must never break the caller
            self.failed += 1

    def _drain(self, max_items: int) -> int:
        n = 0
        while n < max_items:
            try:
                item = self._q.get_nowait()
            except queue.Empty:
                break
            try:
                self._run_one(item)
            finally:
                self._q.task_done()
            n += 1
        return n

    def _loop(self) -> None:
        while not self._stopped:
            try:
                item = self._q.get(timeout=self.interval_s)
            except queue.Empty:
                continue
            try:
                self._run_one(item)
            finally:
                self._q.task_done()
            self._drain(self.batch_size - 1)

    def pending(self) -> int:
        return self._q.qsize()

    def flush(self, timeout: float | None = None) -> bool:
        """Block until queued rows are written (or ``timeout``); drains inline if the worker is gone."""
        deadline = time.monotonic() + (self.shutdown_timeout if timeout is None else timeout)
        while self._q.unfinished_tasks and time.monotonic() < deadline:
            if self._thread is None or not self._thread.is_alive():
                self._drain(self.batch_size)
            else:
                time.sleep(0.01)
        return not self._q.unfinished_tasks

    def close(self) -> None:
        self.flush()
        self._stopped = True
        self._drain(10**9)


# --- T10: stamp Langflow's own transaction / vertex_build rows with the run that wrote them ----------------
_LANGFLOW_ROW_WRITERS = (
    ("langflow.services.database.models.transactions.crud", "log_transaction"),
    ("langflow.services.database.models.vertex_builds.crud", "log_vertex_build"),
)
_STAMPED_ATTR = "__sflf_run_stamped__"


def _stamp_row(obj: Any) -> None:
    try:
        ts = getattr(obj, "timestamp", None)
        RUNS.bind_row(
            flow_id=getattr(obj, "flow_id", None),
            vertex_id=getattr(obj, "vertex_id", None) or getattr(obj, "id", None),
            ts=ts,
            row_id=getattr(obj, "build_id", None) or (getattr(obj, "id", None) if hasattr(obj, "vertex_id") else None),
        )
        rec = CURRENT_RUN_RECORD.get()
        job = getattr(obj, "job_id", None)
        if rec is not None and job:
            RUNS.link_job(rec.run_id, job)
    except Exception:  # noqa: BLE001 - telemetry never breaks a build
        pass


def wrap_row_writer(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a Langflow ``log_transaction`` / ``log_vertex_build`` (sync or async) to stamp the current run."""
    import functools
    import inspect

    if getattr(fn, _STAMPED_ATTR, False):
        return fn

    def _row_arg(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        for v in list(args[1:]) + list(kwargs.values()):
            if hasattr(v, "flow_id"):
                return v
        return None

    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def _async(*args: Any, **kwargs: Any) -> Any:
            result = await fn(*args, **kwargs)
            _stamp_row(result if hasattr(result, "flow_id") else _row_arg(args, kwargs))
            return result

        wrapped: Callable[..., Any] = _async
    else:

        @functools.wraps(fn)
        def _sync(*args: Any, **kwargs: Any) -> Any:
            result = fn(*args, **kwargs)
            _stamp_row(result if hasattr(result, "flow_id") else _row_arg(args, kwargs))
            return result

        wrapped = _sync
    setattr(wrapped, _STAMPED_ATTR, True)
    return wrapped


def install_langflow_row_stamping() -> int:
    """Idempotently wrap Langflow's row writers, including aliases already imported by other modules."""
    import importlib
    import sys

    patched = 0
    for mod_name, attr in _LANGFLOW_ROW_WRITERS:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:  # noqa: BLE001 - Langflow not installed / different version
            continue
        original = getattr(mod, attr, None)
        if original is None or getattr(original, _STAMPED_ATTR, False):
            continue
        wrapped = wrap_row_writer(original)
        for other in list(sys.modules.values()):
            try:
                for name, value in list(vars(other).items()):
                    if value is original:
                        setattr(other, name, wrapped)
                        patched += 1
            except Exception:  # noqa: BLE001
                continue
    return patched
