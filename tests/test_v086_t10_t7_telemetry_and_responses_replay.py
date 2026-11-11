"""v0.8.6 E2E rerun fixes: T10 span->run linkage under concurrency + UTC timestamps, T7 Responses tool memory."""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from lfx_snowflake import telemetry as tm
from lfx_snowflake.providers import messages_replay as mr
from lfx_snowflake.providers import proxy_responses as pr


class _Row:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


def _make_db(path, flow_hex: str, runs: int, base: datetime) -> list[tuple[str, str]]:
    """Langflow-shaped transaction + vertex_build rows: ``runs`` overlapping executions, same flow, same vertices,
    identical timestamps across runs (worst case for any time-window heuristic). Returns (kind, row_id) list."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE \"transaction\" (id TEXT, timestamp TEXT, vertex_id TEXT, target_id TEXT, inputs TEXT, "
                 "outputs TEXT, status TEXT, error TEXT, flow_id TEXT)")
    conn.execute("CREATE TABLE vertex_build (build_id TEXT, id TEXT, timestamp TEXT, flow_id TEXT, data TEXT, "
                 "artifacts TEXT, params TEXT, valid INTEGER)")
    conn.commit()
    conn.close()
    return []


def _writer(path):
    """Fake Langflow ``log_transaction`` / ``log_vertex_build`` (async, like Langflow) that inserts the row."""

    async def log_transaction(db, tx):
        conn = sqlite3.connect(path)
        conn.execute('INSERT INTO "transaction" VALUES (?,?,?,?,?,?,?,?,?)',
                     (tx.id, tx.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"), tx.vertex_id, None, "{}", "{}", "success", None, tx.flow_id))
        conn.commit()
        conn.close()
        return tx

    async def log_vertex_build(db, vb):
        conn = sqlite3.connect(path)
        conn.execute("INSERT INTO vertex_build VALUES (?,?,?,?,?,?,?,?)",
                     (vb.build_id, vb.id, vb.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f"), vb.flow_id, "{}", "{}", "{}", 1))
        conn.commit()
        conn.close()
        return vb

    return tm.wrap_row_writer(log_transaction), tm.wrap_row_writer(log_vertex_build)


def test_five_concurrent_runs_every_span_joins_exactly_one_run(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    db = tmp_path / "langflow.db"
    flow_hex = uuid.uuid4().hex
    flow_uuid = str(uuid.UUID(flow_hex))
    base = datetime.now(timezone.utc).replace(tzinfo=None)
    _make_db(db, flow_hex, 5, base)
    log_tx, log_vb = _writer(db)

    # 5 overlapping executions of the same flow; each build task inherits its run via the contextvar.
    runs = [tm.RUNS.start(flow_id=flow_hex, session_id=None) for _ in range(5)]

    async def execute(rec: tm.RunRecord) -> None:
        token = tm.CURRENT_RUN_RECORD.set(rec)
        try:
            await asyncio.sleep(0)
            for i, vertex in enumerate(("ChatInput-1", "LLM-2", "ChatOutput-3")):
                ts = base + timedelta(milliseconds=10 * i)  # identical across runs on purpose
                await log_vb(None, _Row(build_id=uuid.uuid4().hex, id=vertex, timestamp=ts, flow_id=flow_hex))
                await log_tx(None, _Row(id=uuid.uuid4().hex, vertex_id=vertex, timestamp=ts, flow_id=flow_hex))
                await asyncio.sleep(0)
        finally:
            tm.CURRENT_RUN_RECORD.reset(token)

    async def main() -> None:
        await asyncio.gather(*(asyncio.create_task(execute(r)) for r in runs))

    asyncio.run(main())

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
    for r in runs:
        LIVE_BRIDGE.record_flow_run(tm.RUNS.end(r.run_id, "SUCCESS"))
    LIVE_BRIDGE.sync_langflow_traces_to_snowflake(limit=25)

    spans = [cols for table, cols in written if table == LIVE_BRIDGE.spans_table and cols.get("flow_id") == flow_uuid]
    run_rows = [cols for table, cols in written if table == LIVE_BRIDGE.runs_table and cols.get("flow_id") == flow_uuid]
    real_ids = {r.run_id for r in runs}
    assert len(spans) == 30  # 15 tx + 15 vertex_build rows, all replicated, none deferred
    assert all(s["run_id"] in real_ids for s in spans), "every span joins exactly one real run"
    per_run = {rid: sum(1 for s in spans if s["run_id"] == rid) for rid in real_ids}
    assert all(n == 6 for n in per_run.values()), per_run
    # One RUN row per execution, no synthetic per-row runs.
    assert {r["run_id"] for r in run_rows} == real_ids and len(run_rows) == 5


def test_ambiguous_rows_never_get_a_synthetic_run() -> None:
    reg = tm.RunRegistry()
    fid = uuid.uuid4().hex
    a, b = reg.start(flow_id=fid), reg.start(flow_id=fid)
    run, ambiguous = reg.resolve(flow_id=fid, vertex_id="v", ts=a.start_ts)
    assert run is None and ambiguous
    reg.bind_row(flow_id=fid, vertex_id="v", ts=a.start_ts, run_id=b.run_id)
    assert reg.resolve(flow_id=fid, vertex_id="v", ts=a.start_ts) == (b, False)
    reg.link_job(a.run_id, "job-1")
    assert reg.resolve(flow_id=fid, vertex_id="w", ts=a.start_ts, job_id="job-1") == (a, False)
    # No live runs at all: not ambiguous (caller may derive one id per job).
    assert tm.RunRegistry().resolve(flow_id=fid, vertex_id="v", ts=a.start_ts) == (None, False)


def test_telemetry_timestamps_are_uniform_utc() -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    assert tm.utc_iso("2026-11-10 12:00:00.5") == "2026-11-10T12:00:00.500000+00:00"
    assert tm.utc_iso("2026-11-10T04:00:00-08:00") == "2026-11-10T12:00:00+00:00"
    assert tm.utc_iso(datetime(2026, 11, 10, 12, tzinfo=timezone.utc)) == "2026-11-10T12:00:00+00:00"
    sql, binds = LIVE_BRIDGE._merge_insert_request(
        "T", "k", "1", [("a", "2026-11-10T04:00:00-08:00", "TS"), ("b", None, "NOW")]
    )
    assert "CURRENT_TIMESTAMP()))" in sql and sql.count("CONVERT_TIMEZONE('UTC',") == 2
    assert " CURRENT_TIMESTAMP()," not in sql.replace("(CURRENT_TIMESTAMP()", "")
    vals = [b["value"] for b in binds.values()]
    assert "2026-11-10T12:00:00+00:00" in vals


# ------------------------------------------------------------------ T7 Responses 3-turn tool memory
def _user(t: str) -> dict:
    return {"type": "message", "role": "user", "content": [{"type": "input_text", "text": t}]}


def _asst(t: str) -> dict:
    return {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": t}]}


def _adapter(outputs: list[list[dict]], seen: list[dict]):
    from lfx_snowflake.providers.registry import SnowflakeProviderRegistry

    reg = SnowflakeProviderRegistry(proxy_base_url="http://llm-proxy-standard.local:8080", active_role="ROLE_ANALYST")
    it = iter(outputs)

    def transport(url, payload, headers):
        seen.append(payload)
        return {"output": next(it)}

    return pr.SnowflakeProxyResponsesAdapter(reg, transport=transport)


def test_responses_three_turn_tool_memory_session_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    pr.RESPONSES_REPLAY.clear()
    seen: list[dict] = []
    fc = {"type": "function_call", "call_id": "call_0", "name": "calc", "arguments": '{"e":"17*23"}'}
    fco = {"type": "function_call_output", "call_id": "call_0", "output": "391"}
    adapter = _adapter([[fc], [_asst("391")], [_asst("I used calc")], [_asst("17*23")], [_asst("no tools")]], seen)
    tools = [{"type": "function", "name": "calc"}]
    h1 = _user("17*23?")
    # Turn 1: tool loop (function_call -> function_call_output -> final text).
    adapter.invoke("openai-gpt-6-astra", [h1], tools=tools, session_id="s-A")
    adapter.invoke("openai-gpt-6-astra", [h1, fc, fco], tools=tools, session_id="s-A")
    # Turn 2: Langflow history only has the final text; the tool loop must be re-injected.
    adapter.invoke("openai-gpt-6-astra", [h1, _asst("391"), _user("what tool did you use?")], tools=tools, session_id="s-A")
    types = [i.get("type") for i in seen[-1]["input"]]
    assert types == ["message", "function_call", "function_call_output", "message", "message"]
    assert seen[-1]["input"][1]["call_id"] == seen[-1]["input"][2]["call_id"]
    # Turn 3: still remembered (turn-2 had no tools, so it is kept as-is).
    adapter.invoke("openai-gpt-6-astra", [h1, _asst("391"), _user("what tool did you use?"), _asst("I used calc"), _user("and the args?")],
                   tools=tools, session_id="s-A")
    assert [i.get("type") for i in seen[-1]["input"]].count("function_call") == 1
    # Other session: never sees s-A's tool outputs.
    adapter.invoke("openai-gpt-6-astra", [h1, _asst("391"), _user("what tool?")], tools=tools, session_id="s-B")
    assert "function_call" not in [i.get("type") for i in seen[-1]["input"]]


def test_responses_replay_without_tools_is_text_summary_and_no_session_no_replay() -> None:
    pr.RESPONSES_REPLAY.clear()
    sc = "u|f|s1"
    h1 = _user("17*23?")
    fc = {"type": "function_call", "call_id": "c1", "name": "calc", "arguments": "{}"}
    fco = {"type": "function_call_output", "call_id": "c1", "output": "391"}
    assert pr.record_responses_turn([h1, fc, fco], [_asst("391")], scope=sc)
    out = pr.rehydrate_responses_history([h1, _asst("391"), _user("?")], tools_bound=False, scope=sc)
    assert [i["type"] for i in out] == ["message", "message", "message"]
    assert "function_call calc" in out[1]["content"][0]["text"]
    # Incomplete loops are never recorded.
    assert not pr.record_responses_turn([h1, fc], [_asst("x")], scope=sc)
    assert pr.record_responses_turn([h1, fc, fco], [_asst("391")], scope="") is False


def test_protected_responses_uses_replay_hooks() -> None:
    from lfx_snowflake import standard_langflow as sl

    assert issubclass(sl.ProtectedResponses, sl._SflfReplayMixin)
    assert issubclass(sl.ProtectedAnthropic, sl._SflfReplayMixin)
    assert sl.ProtectedResponses._sflf_thinking_on(object()) is True
