"""v0.8.6 rerun-4: spans of a finished execution land on its RUN close, written as batched multi-row MERGEs."""

from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from lfx_snowflake import telemetry as tm
from tests.test_v086_t10_t7_telemetry_and_responses_replay import _make_db, _Row, _writer

VERTICES = ("ChatInput-1", "Prompt-2", "LLM-3", "Parser-4", "ChatOutput-5")


@pytest.fixture
def bridge(tmp_path, monkeypatch: pytest.MonkeyPatch):
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_sql(sql, **kw):
        calls.append((sql, dict(kw.get("bindings") or {})))
        return {"ok": True, "data": []}

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", fake_sql)
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_tx_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_vertex_build_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_activity_ids", set())
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_since", {})
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_close_sync_flows", set())
    LIVE_BRIDGE._r4_calls = calls
    yield LIVE_BRIDGE
    del LIVE_BRIDGE._r4_calls


def _run_stress(tmp_path, n_exec: int) -> tuple[str, list[tm.RunRecord]]:
    db = tmp_path / "langflow.db"
    flow_hex = uuid.uuid4().hex
    base = datetime.now(timezone.utc).replace(tzinfo=None)
    _make_db(db, flow_hex, n_exec, base)
    log_tx, log_vb = _writer(db)
    runs = [tm.RUNS.start(flow_id=flow_hex, session_id=f"s{i % 5}") for i in range(n_exec)]

    async def execute(rec: tm.RunRecord) -> None:
        token = tm.CURRENT_RUN_RECORD.set(rec)
        try:
            for i, vertex in enumerate(VERTICES):
                ts = base + timedelta(milliseconds=10 * i)  # identical across executions on purpose
                await log_vb(None, _Row(build_id=uuid.uuid4().hex, id=vertex, timestamp=ts, flow_id=flow_hex))
                await log_tx(None, _Row(id=uuid.uuid4().hex, vertex_id=vertex, timestamp=ts, flow_id=flow_hex))
                await asyncio.sleep(0)
        finally:
            tm.CURRENT_RUN_RECORD.reset(token)

    async def main() -> None:
        await asyncio.gather(*(asyncio.create_task(execute(r)) for r in runs))

    asyncio.run(main())
    return flow_hex, runs


def _span_merges(bridge) -> list[tuple[str, dict[str, Any]]]:
    return [(q, b) for q, b in bridge._r4_calls if f"MERGE INTO {bridge.spans_table}" in q]


def test_25_executions_spans_land_on_close_batched(tmp_path, bridge) -> None:
    flow_hex, runs = _run_stress(tmp_path, 25)
    flow_uuid = str(uuid.UUID(flow_hex))
    rows: list[dict[str, Any]] = []
    orig_batch = bridge._merge_insert_batch_request

    def capture(table, key_col, batch):
        if table == bridge.spans_table:
            rows.extend({c: v for c, v, _ in cols} for _k, cols in batch)
        return orig_batch(table, key_col, batch)

    bridge._merge_insert_batch_request = capture  # type: ignore[method-assign]
    try:
        for r in runs:  # one close-triggered sync per execution (worst case: no debounce coalescing)
            bridge.record_flow_run(tm.RUNS.end(r.run_id, "SUCCESS"))
            bridge.trigger_close_span_sync(r.flow_id, delay_s=0)
    finally:
        del bridge._merge_insert_batch_request

    spans = [s for s in rows if s["flow_id"] == flow_uuid]
    real = {r.run_id for r in runs}
    assert len(spans) == 25 * len(VERTICES) * 2  # tx + vertex_build per vertex; nothing pending or dropped
    assert len({s["span_id"] for s in spans}) == len(spans), "each span written exactly once"
    assert all(s["run_id"] in real for s in spans), "every span joins its RUN"
    assert all(sum(s["run_id"] == rid for s in spans) == 2 * len(VERTICES) for rid in real)

    merges = _span_merges(bridge)
    # 250 rows -> ceil(250/50)=5 span MERGEs (was 250 single-row MERGEs); later syncs write nothing new.
    assert len(merges) <= 6, len(merges)
    for sql, binds in merges:
        assert sql.count("?") == len(binds), "every placeholder bound"
        assert all(v.get("value") is not None for v in binds.values()), "NULL is a literal, never a null binding"
        assert "UNION ALL" in sql or sql.count("SELECT ") == 1


def test_close_triggers_coalesce_into_one_debounced_sync(bridge, monkeypatch: pytest.MonkeyPatch) -> None:
    fired: list[set[str]] = []
    done = threading.Event()

    def fake_sync(limit=25, *, replicate_sqlite=True, flow_ids=None):
        fired.append(set(bridge.__dict__["_close_sync_flows"]))
        done.set()
        return {"ok": True}

    monkeypatch.setattr(bridge, "sync_langflow_traces_to_snowflake", fake_sync)
    monkeypatch.setattr(bridge, "CLOSE_SYNC_DEBOUNCE_S", 0.2)
    monkeypatch.setitem(bridge.__dict__, "_close_sync_timer_armed", False)
    for i in range(25):
        bridge.trigger_close_span_sync(f"flow-{i % 5}")
    assert done.wait(5.0)
    assert len(fired) == 1 and fired[0] == {f"flow-{i}" for i in range(5)}


def test_rejected_chunk_is_retried_not_dropped(tmp_path, bridge, monkeypatch: pytest.MonkeyPatch) -> None:
    flow_hex, runs = _run_stress(tmp_path, 2)
    for r in runs:
        bridge.record_flow_run(tm.RUNS.end(r.run_id, "SUCCESS"))
    ok = bridge.execute_sql
    monkeypatch.setattr(
        bridge, "execute_sql",
        lambda sql, **kw: {"ok": False} if f"MERGE INTO {bridge.spans_table}" in sql else ok(sql, **kw),
    )
    first = bridge.sync_langflow_traces_to_snowflake(limit=25, flow_ids=[flow_hex])
    assert first["replicated_spans"] == 0
    monkeypatch.setattr(bridge, "execute_sql", ok)
    second = bridge.sync_langflow_traces_to_snowflake(limit=25, flow_ids=[flow_hex])
    assert second["replicated_spans"] == 2 * 2 * len(VERTICES)


def test_batch_merge_keeps_null_literal_and_dedupes_keys() -> None:
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    cols = lambda k, rid: [("span_id", k, "TEXT"), ("run_id", rid, "TEXT"), ("redacted_inputs", "{}", "JSON")]  # noqa: E731
    sql, binds = LiveSnowflakeBridge._merge_insert_batch_request(
        "DB.S.SPANS", "span_id", [("a", cols("a", None)), ("b", cols("b", "r1")), ("a", cols("a", "x"))]
    )
    assert sql.count("UNION ALL") == 1 and "NULL AS run_id" in sql and "PARSE_JSON(?)" in sql
    assert sql.count("?") == len(binds) == 7  # (key, span_id, json) x2 + run_id of "b"
    assert all(v["value"] is not None for v in binds.values())


def test_spans_written_after_close_land_via_followup_syncs(tmp_path, bridge, monkeypatch: pytest.MonkeyPatch) -> None:
    """Round-3 repro: Langflow writes an API run's rows 5-25 s AFTER its close (time scaled 1/50 here).

    The debounced close sync sees nothing; the coalesced +10 s / +30 s follow-ups must still land every span
    within 60 s, with at most one follow-up chain and a bounded number of syncs.
    """
    import sqlite3
    import time

    scale = 1 / 50
    monkeypatch.setattr(bridge, "CLOSE_SYNC_DEBOUNCE_S", 3.0 * scale)
    monkeypatch.setattr(bridge, "CLOSE_FOLLOWUP_DELAYS_S", (10.0 * scale, 30.0 * scale))
    monkeypatch.setitem(bridge.__dict__, "_close_sync_timer_armed", False)
    monkeypatch.setitem(bridge.__dict__, "_close_followup_armed", False)
    monkeypatch.setitem(bridge.__dict__, "_close_followup_flows", set())
    monkeypatch.setitem(bridge.__dict__, "_close_sync_since", {})
    db = tmp_path / "langflow.db"
    flow_hex = uuid.uuid4().hex
    flow_uuid = str(uuid.UUID(flow_hex))
    _make_db(db, flow_hex, 1, datetime.now(timezone.utc))
    syncs: list[float] = []
    orig_sync = bridge.sync_langflow_traces_to_snowflake

    def counting_sync(*a, **kw):
        syncs.append(time.monotonic())
        return orig_sync(*a, **kw)

    monkeypatch.setattr(bridge, "sync_langflow_traces_to_snowflake", counting_sync)
    rec = tm.RUNS.start(flow_id=flow_hex, session_id="s-late")
    run_ts = rec.start_ts.replace(tzinfo=None)
    bridge.record_flow_run(tm.RUNS.end(rec.run_id, "SUCCESS"))
    t_close = time.monotonic()
    for _ in range(5):  # several closes coalesce into one debounced sync + one follow-up chain
        bridge.trigger_close_span_sync(flow_hex, since=rec.start_ts)

    def late_writer() -> None:
        for i, vertex in enumerate(VERTICES):
            time.sleep((5.0 + 5.0 * i) * scale - (time.monotonic() - t_close) if i else 5.0 * scale)
            conn = sqlite3.connect(db)
            ts = (run_ts + timedelta(milliseconds=10 * i)).strftime("%Y-%m-%d %H:%M:%S.%f")
            conn.execute('INSERT INTO "transaction" VALUES (?,?,?,?,?,?,?,?,?)',
                         (uuid.uuid4().hex, ts, vertex, None, "{}", "{}", "success", None, flow_hex))
            conn.execute("INSERT INTO vertex_build VALUES (?,?,?,?,?,?,?,?)",
                         (uuid.uuid4().hex, vertex, ts, flow_hex, "{}", "{}", "{}", 1))
            conn.commit()
            conn.close()

    w = threading.Thread(target=late_writer, daemon=True)
    w.start()
    w.join(5.0)
    rows: list[dict[str, Any]] = []
    deadline = t_close + 60.0 * scale
    while time.monotonic() < deadline + 0.5:
        rows = [b for q, b in _span_merges(bridge) if flow_uuid in {v.get("value") for v in b.values()}]
        if not bridge.__dict__.get("_close_followup_armed") and time.monotonic() > t_close + 31.0 * scale:
            break
        time.sleep(0.02)
    landed = sum(sum(1 for v in b.values() if v.get("value") == flow_uuid) for _q, b in _span_merges(bridge))
    assert landed == 2 * len(VERTICES), landed
    assert syncs and syncs[-1] - t_close <= 60.0 * scale, "all spans land within 60 s of close"
    assert len(syncs) <= 3, "1 close sync + at most 2 follow-ups"
    assert not bridge.__dict__.get("_close_followup_armed")
