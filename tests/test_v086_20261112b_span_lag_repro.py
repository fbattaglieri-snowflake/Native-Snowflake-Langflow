"""v0.8.6-20261112b: end-to-end reproduction of the live T10 span lag (5 sessions x 5 turns, one flow).

Live evidence (2026-11-12 T10): every span was linked to its run (no NULL run_id, so not the match grace), but the
spans landed in 50-row MERGE bursts at +90 s / +145 s / +290 s after the last close. Query history showed each sync
re-MERGEing hundreds of rows already in Snowflake (rows_inserted=0, ~1.3 s per statement, serialized under the sync
lock): after a restart the in-memory synced sets are empty and the targeted per-flow read returns up to 2000
historical rows of the flow. A fake Snowflake with a per-statement cost clock reproduces that and proves <= 60 s.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from lfx_snowflake import telemetry as tm
from tests.test_v086_t10_t7_telemetry_and_responses_replay import _make_db, _Row, _writer

VERTICES = ("ChatInput-1", "Prompt-2", "LLM-3", "Parser-4", "ChatOutput-5")
MERGE_COST_S = 1.3
PROBE_COST_S = 0.3


class _FakeSnowflake:
    """Keeps the spans/activity keys that exist and advances a fake clock per statement."""

    def __init__(self, bridge) -> None:
        self.bridge = bridge
        self.clock = 0.0
        self.keys: dict[str, set[str]] = {bridge.spans_table: set(), bridge.activity_table: set()}
        self.landed_at: dict[str, float] = {}

    def __call__(self, sql: str, **kw: Any) -> dict[str, Any]:
        binds = kw.get("bindings") or {}
        vals = [str(v.get("value") if isinstance(v, dict) else v) for v in binds.values()]
        for table, keys in self.keys.items():
            if sql.startswith(f"MERGE INTO {table} "):
                self.clock += MERGE_COST_S
                # each UNION branch starts with "SELECT ? AS _k": its key is the binding at that '?' position
                pos = [sql[: m.start()].count("?") for m in re.finditer(r"SELECT \? AS _k", sql)]
                for k in (vals[p] for p in pos):
                    if k not in keys:
                        keys.add(k)
                        if table == self.bridge.spans_table:
                            self.landed_at[k] = self.clock
                return {"ok": True, "data": []}
            if sql.startswith("SELECT ") and f" FROM {table} WHERE " in sql:
                self.clock += PROBE_COST_S
                return {"ok": True, "data": [[v] for v in vals if v in keys]}
        return {"ok": True, "data": []}


@pytest.fixture
def bridge(tmp_path, monkeypatch: pytest.MonkeyPatch):
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_tx_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_vertex_build_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_activity_ids", set())
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_since", {})
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_flow", {})
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_close_sync_flows", set())
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_span_backlog_hint", 0)
    monkeypatch.setattr(LIVE_BRIDGE, "_arm_pending_expiry_sync", lambda: None)
    monkeypatch.setattr(LIVE_BRIDGE, "flush_telemetry", lambda *a, **k: True)
    fake = _FakeSnowflake(LIVE_BRIDGE)
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", fake)
    LIVE_BRIDGE._repro_fake = fake
    yield LIVE_BRIDGE
    del LIVE_BRIDGE._repro_fake


def _seed_history(db, flow_hex: str, bridge, n: int, base: datetime) -> None:
    """``n`` older executions of the same flow, replicated by a previous process, then a restart (sets empty)."""
    conn = sqlite3.connect(db)
    for i in range(n):
        ts = (base - timedelta(hours=1, seconds=i)).strftime("%Y-%m-%d %H:%M:%S.%f")
        conn.execute('INSERT INTO "transaction" VALUES (?,?,?,?,?,?,?,?,?)', (uuid.uuid4().hex, ts, "LLM-3", None, "{}", "{}", "success", None, flow_hex))
        conn.execute("INSERT INTO vertex_build VALUES (?,?,?,?,?,?,?,?)", (uuid.uuid4().hex, "LLM-3", ts, flow_hex, "{}", "{}", "{}", 1))
    conn.commit()
    conn.close()
    bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True, flow_ids=[str(uuid.UUID(flow_hex))])
    for attr in ("_synced_tx_ids", "_synced_vertex_build_ids", "_synced_activity_ids"):
        getattr(bridge, attr).clear()
    bridge._repro_fake.clock = 0.0
    bridge._repro_fake.landed_at.clear()


def _t10(tmp_path, bridge, *, history: int) -> tuple[set[str], float]:
    db = tmp_path / "langflow.db"
    flow_hex = uuid.uuid4().hex
    flow_uuid = str(uuid.UUID(flow_hex))
    base = datetime.now(timezone.utc).replace(tzinfo=None)
    _make_db(db, flow_hex, 25, base)
    fake: _FakeSnowflake = bridge._repro_fake
    _seed_history(db, flow_hex, bridge, history, base)
    log_tx, log_vb = _writer(db)
    new_keys: set[str] = set()

    async def turn(rec: tm.RunRecord, t: int) -> None:
        token = tm.CURRENT_RUN_RECORD.set(rec)
        try:
            for i, vertex in enumerate(VERTICES):
                ts = base + timedelta(seconds=5 * t, milliseconds=10 * i)  # interleaved across the 5 sessions
                b, x = uuid.uuid4().hex, uuid.uuid4().hex
                await log_vb(None, _Row(build_id=b, id=vertex, timestamp=ts, flow_id=flow_hex))
                await log_tx(None, _Row(id=x, vertex_id=vertex, timestamp=ts, flow_id=flow_hex))
                new_keys.update({x, f"vb-{b}"})
                await asyncio.sleep(0)
        finally:
            tm.CURRENT_RUN_RECORD.reset(token)

    def run_turn(t: int) -> None:
        runs = [tm.RUNS.start(flow_id=flow_hex, session_id=f"e2e-t10-{s}") for s in range(5)]

        async def main() -> None:
            await asyncio.gather(*(turn(r, t) for r in runs))

        asyncio.run(main())
        for r in runs:  # the middleware closes each RUN, then arms the close-triggered sync
            tm.RUNS.end(r.run_id, "SUCCESS")
            bridge.__dict__.setdefault("_close_sync_flows", set()).add(flow_uuid)
            bridge._span_backlog_hint = int(bridge.__dict__.get("_span_backlog_hint", 0)) + 50

    # Live: the first close-triggered sync starts after turn 1 and the remaining 4 turns (~60 s) finish
    # while it runs; their spans can only land on a later sync.
    run_turn(0)
    first_sync_at = fake.clock
    bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
    for t in range(1, 5):
        run_turn(t)
    last_close = first_sync_at + 60.0
    fake.clock = max(fake.clock, last_close)
    while fake.clock - last_close < 600 and not new_keys <= set(fake.landed_at):
        bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
        fake.clock += 15.0
    landed = [fake.landed_at[k] - last_close for k in new_keys if k in fake.landed_at]
    return new_keys - set(fake.landed_at), max(landed, default=1e9)


def test_t10_after_restart_with_history_all_spans_within_60s(tmp_path, bridge) -> None:
    missing, worst = _t10(tmp_path, bridge, history=2000)
    assert not missing
    assert worst <= 60.0, worst


def test_existing_rows_are_not_remerged(tmp_path, bridge) -> None:
    _t10(tmp_path, bridge, history=2000)
    fake: _FakeSnowflake = bridge._repro_fake
    before = fake.clock
    bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
    assert fake.clock - before < 5.0  # nothing left to write: no MERGE statements at all
