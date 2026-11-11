"""v0.8.6 E2E rerun-2 T10: Langflow 1.13 telemetry-writer stamping, pending rows never dropped, no future timestamps."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
import time
import types
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from lfx_snowflake import telemetry as tm


def _make_db(path) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE \"transaction\" (id TEXT, timestamp TEXT, vertex_id TEXT, target_id TEXT, inputs TEXT, "
                 "outputs TEXT, status TEXT, error TEXT, flow_id TEXT)")
    conn.execute("CREATE TABLE vertex_build (build_id TEXT, id TEXT, timestamp TEXT, flow_id TEXT, data TEXT, "
                 "artifacts TEXT, params TEXT, valid INTEGER)")
    conn.commit()
    conn.close()


def _ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")


def _insert_tx(path, *, row_id: str, vertex: str, ts: datetime, flow_hex: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute('INSERT INTO "transaction" VALUES (?,?,?,?,?,?,?,?,?)',
                 (row_id, _ts(ts), vertex, None, "{}", "{}", "success", None, flow_hex))
    conn.commit()
    conn.close()


class _FakeTelemetryWriterService:
    """Langflow 1.13 ``TelemetryWriterService``: sync ``enqueue_*`` taking ``model_dump(mode="python")`` dicts
    (UUID objects, aware datetimes) in the request context; rows reach SQLite later from a background flush."""

    def __init__(self, path) -> None:
        self.path = path
        self.buffer: list[tuple[str, dict[str, Any]]] = []

    def enqueue_transaction(self, payload: dict[str, Any]) -> bool:
        self.buffer.append(("tx", payload))
        return True

    def enqueue_vertex_build(self, payload: dict[str, Any]) -> bool:
        self.buffer.append(("vb", payload))
        return True

    def flush(self) -> None:  # runs with no run contextvar, like the real background task
        conn = sqlite3.connect(self.path)
        for kind, p in self.buffer:
            if kind == "tx":
                conn.execute('INSERT INTO "transaction" VALUES (?,?,?,?,?,?,?,?,?)',
                             (p["id"].hex, _ts(p["timestamp"]), p["vertex_id"], None, "{}", "{}", "success", None, p["flow_id"].hex))
            else:
                conn.execute("INSERT INTO vertex_build VALUES (?,?,?,?,?,?,?,?)",
                             (p["build_id"].hex, p["id"], _ts(p["timestamp"]), p["flow_id"].hex, "{}", "{}", "{}", 1))
        conn.commit()
        conn.close()
        self.buffer.clear()


@pytest.fixture
def bridge(tmp_path, monkeypatch: pytest.MonkeyPatch):
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    written: list[tuple[str, dict[str, Any]]] = []
    orig_merge = LIVE_BRIDGE._merge_insert_request

    def capture(table, key_col, key_value, columns):
        written.append((table, {c: v for c, v, _ in columns}))
        return orig_merge(table, key_col, key_value, columns)

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    monkeypatch.setattr(LIVE_BRIDGE, "_merge_insert_request", capture)
    orig_batch = LIVE_BRIDGE._merge_insert_batch_request

    def capture_batch(table, key_col, rows):
        written.extend((table, {c: v for c, v, _ in cols}) for _k, cols in rows)
        return orig_batch(table, key_col, rows)

    monkeypatch.setattr(LIVE_BRIDGE, "_merge_insert_batch_request", capture_batch)
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", lambda sql, **kw: {"ok": True, "data": []})
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_tx_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_vertex_build_ids", set())
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_since", {})
    _make_db(tmp_path / "langflow.db")
    LIVE_BRIDGE._t10_written = written
    yield LIVE_BRIDGE
    del LIVE_BRIDGE._t10_written


def _spans(bridge, flow_uuid: str) -> list[dict[str, Any]]:
    return [c for t, c in bridge._t10_written if t == bridge.spans_table and c.get("flow_id") == flow_uuid]


def test_telemetry_writer_service_rows_are_stamped_under_concurrency(tmp_path, bridge) -> None:
    flow = uuid.uuid4()
    svc = _FakeTelemetryWriterService(tmp_path / "langflow.db")
    enqueue_tx = tm.wrap_row_writer(_FakeTelemetryWriterService.enqueue_transaction)
    enqueue_vb = tm.wrap_row_writer(_FakeTelemetryWriterService.enqueue_vertex_build)
    runs = [tm.RUNS.start(flow_id=flow.hex) for _ in range(5)]
    base = datetime.now(timezone.utc) - timedelta(seconds=1)

    async def execute(rec: tm.RunRecord) -> None:
        token = tm.CURRENT_RUN_RECORD.set(rec)
        try:
            for i, vertex in enumerate(("ChatInput-1", "LLM-2", "ChatOutput-3")):
                ts = base + timedelta(milliseconds=10 * i)  # identical across runs: time windows cannot decide
                enqueue_vb(svc, {"build_id": uuid.uuid4(), "id": vertex, "timestamp": ts, "flow_id": flow, "job_id": None})
                enqueue_tx(svc, {"id": uuid.uuid4(), "vertex_id": vertex, "timestamp": ts, "flow_id": flow})
                await asyncio.sleep(0)
        finally:
            tm.CURRENT_RUN_RECORD.reset(token)

    async def main() -> None:
        await asyncio.gather(*(execute(r) for r in runs))

    asyncio.run(main())
    svc.flush()
    for r in runs:
        tm.RUNS.end(r.run_id, "SUCCESS")
    bridge.sync_langflow_traces_to_snowflake(limit=25)
    spans = _spans(bridge, str(flow))
    ids = {r.run_id for r in runs}
    assert len(spans) == 30
    assert {rid: sum(s["run_id"] == rid for s in spans) for rid in ids} == {rid: 6 for rid in ids}


def test_ambiguous_rows_are_written_with_null_run_after_grace_even_out_of_window(tmp_path, bridge, monkeypatch) -> None:
    monkeypatch.setattr(tm.RUNS, "match_grace_s", 0.2)
    db = tmp_path / "langflow.db"
    flow = uuid.uuid4()
    runs = [tm.RUNS.start(flow_id=flow.hex) for _ in range(2)]  # two live runs, rows unstamped -> ambiguous
    now = datetime.now(timezone.utc)
    amb_ids = [uuid.uuid4().hex for _ in range(3)]
    for i, rid in enumerate(amb_ids):
        _insert_tx(db, row_id=rid, vertex=f"V-{i}", ts=now, flow_hex=flow.hex)

    bridge.sync_langflow_traces_to_snowflake(limit=25)
    assert _spans(bridge, str(flow)) == []  # held inside the grace window, not marked synced

    # 300 newer rows of another flow push the pending rows out of the newest-N fetch window.
    noise = uuid.uuid4()
    for i in range(300):
        _insert_tx(db, row_id=uuid.uuid4().hex, vertex=f"N-{i}", ts=now + timedelta(milliseconds=i + 1), flow_hex=noise.hex)
    time.sleep(0.3)
    bridge.sync_langflow_traces_to_snowflake(limit=25)
    spans = _spans(bridge, str(flow))
    assert sorted(s["span_id"] for s in spans) == sorted(amb_ids)  # never dropped
    assert all(s["run_id"] is None for s in spans)  # never attributed to a guessed run
    assert not bridge.__dict__["_telemetry_pending_since"]
    for r in runs:
        tm.RUNS.end(r.run_id, "SUCCESS")


def test_future_timestamps_are_clamped_and_never_pend_forever(tmp_path, bridge, monkeypatch) -> None:
    monkeypatch.setattr(tm.RUNS, "match_grace_s", 0.1)
    db = tmp_path / "langflow.db"
    flow = uuid.uuid4()
    future = datetime.now(timezone.utc) + timedelta(days=3)
    _insert_tx(db, row_id=uuid.uuid4().hex, vertex="Solo-1", ts=future, flow_hex=flow.hex)  # no live run: derived
    bridge.sync_langflow_traces_to_snowflake(limit=25)
    after = datetime.now(timezone.utc)
    rows = [c for t, c in bridge._t10_written if c.get("flow_id") == str(flow)]
    stamps = [c[k] for c in rows for k in ("start_ts", "end_ts") if c.get(k)]
    assert stamps and all(datetime.fromisoformat(v) <= after for v in stamps)

    assert tm.utc_iso_clamped(None)  # missing -> now, never NULL
    assert tm.utc_iso_clamped("2001-01-01T00:00:00") == "2001-01-01T00:00:00+00:00"

    # A future-dated ambiguous row is released after the grace window measured from first sighting.
    key = "tx:future"
    assert bridge._telemetry_row_pending(key, future) is True
    time.sleep(0.15)
    assert bridge._telemetry_row_pending(key, future) is False


def test_install_patches_langflow_113_writer_service_and_logs(monkeypatch, caplog) -> None:
    mod = types.ModuleType("langflow.services.telemetry_writer.service")

    class TelemetryWriterService:
        def enqueue_transaction(self, payload):
            return True

        def enqueue_vertex_build(self, payload):
            return True

    mod.TelemetryWriterService = TelemetryWriterService
    for name in ("langflow", "langflow.services", "langflow.services.telemetry_writer"):
        monkeypatch.setitem(sys.modules, name, sys.modules.get(name) or types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "langflow.services.telemetry_writer.service", mod)
    monkeypatch.setattr(tm, "_INSTALL_LOGGED", set())
    with caplog.at_level(logging.INFO, logger="lfx_snowflake.telemetry"):
        assert tm.install_langflow_row_stamping() == 2
        assert tm.install_langflow_row_stamping() == 0  # idempotent
    assert getattr(TelemetryWriterService.enqueue_transaction, tm._STAMPED_ATTR)
    msgs = [r.getMessage() for r in caplog.records if "row stamping" in r.getMessage()]
    assert msgs[0].startswith("sflf telemetry row stamping: patched=2 active_writers=2")
    assert len(msgs) == 2  # the no-op install logs its state once, not once per request

    rec = tm.RUNS.start(flow_id=uuid.uuid4().hex)
    token = tm.CURRENT_RUN_RECORD.set(rec)
    try:
        row_id = uuid.uuid4()
        TelemetryWriterService().enqueue_transaction(
            {"id": row_id, "vertex_id": "V", "timestamp": datetime.now(timezone.utc), "flow_id": uuid.UUID(rec.flow_id)}
        )
    finally:
        tm.CURRENT_RUN_RECORD.reset(token)
    assert tm.RUNS.by_row(flow_id=rec.flow_id, vertex_id="x", ts=None, row_id=row_id.hex) is rec
