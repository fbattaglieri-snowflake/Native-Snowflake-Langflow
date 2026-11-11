"""v0.8.6 rerun-3: one RUN per execution joined by its spans, no silent RUN/AUDIT write failures, ROLE_SWITCH audit.

Live evidence: every bridge statement is a multi-statement SQL API request (``USE SECONDARY ROLES ...;`` prefix)
and there a ``{"value": null}`` binding is treated as unset (002049 "Bind variable ? not set"), so every RUN MERGE
(NULL cortex/a2a ids) and ALLOWED audit insert (NULL error_code) was rejected while the writer counted it written.
The fake ``execute_sql`` below enforces the same rule.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from lfx_snowflake import telemetry as tm
from lfx_snowflake import traceability as tr


def _sql_api(sql: str, bindings: dict[str, Any] | None) -> dict[str, Any]:
    """SQL API emulation: placeholders must match bindings and no binding may carry a null value."""
    binds = bindings or {}
    if any(v.get("value") is None for v in binds.values()) or sql.count("?") != len(binds):
        return {"ok": False, "status_code": 422, "error_code": "SNOWFLAKE_ERROR", "code": "002049"}
    return {"ok": True, "data": []}


def _make_db(path) -> None:
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE "transaction" (id TEXT, timestamp TEXT, vertex_id TEXT, target_id TEXT, inputs TEXT, '
                 "outputs TEXT, status TEXT, error TEXT, flow_id TEXT)")
    conn.execute("CREATE TABLE vertex_build (build_id TEXT, id TEXT, timestamp TEXT, flow_id TEXT, data TEXT, "
                 "artifacts TEXT, params TEXT, valid INTEGER)")
    conn.commit()
    conn.close()


def _ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")


class _FakeTelemetryWriterService:
    """Langflow 1.13 ``TelemetryWriterService``: payloads enqueued in the request, flushed to SQLite later."""

    def __init__(self, path) -> None:
        self.path = path
        self.buffer: list[tuple[str, dict[str, Any]]] = []

    def enqueue_transaction(self, payload: dict[str, Any]) -> bool:
        self.buffer.append(("tx", payload))
        return True

    def enqueue_vertex_build(self, payload: dict[str, Any]) -> bool:
        self.buffer.append(("vb", payload))
        return True

    def flush(self) -> None:
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

    executed: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []

    def fake_execute(sql, **kw):
        res = _sql_api(sql, kw.get("bindings"))
        executed.append((sql, kw.get("bindings"), res))
        return res

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", fake_execute)
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_tx_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_vertex_build_ids", set())
    monkeypatch.setattr(LIVE_BRIDGE, "_synced_derived_run_ids", set())
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_since", {})
    _make_db(tmp_path / "langflow.db")
    LIVE_BRIDGE._rr3_executed = executed
    yield LIVE_BRIDGE
    del LIVE_BRIDGE._rr3_executed


def _ok_rows(bridge, table: str) -> list[dict[str, Any]]:
    """Columns of successful MERGEs into ``table``, rebuilt from SQL + bindings (only bound values matter)."""
    out = []
    for sql, binds, res in bridge._rr3_executed:
        if res["ok"] and sql.startswith(f"MERGE INTO {table} "):
            out.append({"sql": sql, "binds": [b["value"] for b in (binds or {}).values()]})
    return out


# --- null bindings ---------------------------------------------------------------------------------------------


def test_merge_insert_request_emits_null_literals_never_null_bindings(bridge) -> None:
    rec = tm.RUNS.start(flow_id=uuid.uuid4().hex)
    tm.RUNS.end(rec.run_id, "SUCCESS")
    sql, binds = bridge._build_run_merge_request(rec)  # cortex/a2a ids are NULL for a plain build
    assert all(v["value"] is not None for v in binds.values())
    assert sql.count("?") == len(binds)
    assert ", NULL" in sql
    assert _sql_api(sql, binds)["ok"] is True

    sql2, binds2 = bridge._merge_insert_request("T", "k", "1", [("a", None, "TS"), ("b", None, "JSON"), ("c", 0, "REAL")])
    assert "TRY_TO_TIMESTAMP_TZ" not in sql2 and "PARSE_JSON(NULL)" not in sql2
    assert sql2.count("?") == len(binds2) == 2  # key + REAL 0 (falsy, not NULL)


def test_audit_insert_emits_null_literals_and_keeps_details_binding(bridge) -> None:
    stmt, binds = tr.build_audit_insert(
        "DB.S.LANGFLOW_AUDIT_LOG", event_id="aud-1", authenticated_user="U", effective_role="R",
        execution_mode="SERVICE", context_epoch=1, action="ROLE_SWITCH", outcome="ALLOWED", error_code=None,
        target_object="R", flow_id=None, run_id=None, details={"new_role": "R"},
    )
    assert _sql_api(stmt, binds)["ok"] is True
    assert stmt.count("NULL") >= 3
    # record_audit_event overwrites binding len-1 with the redacted details JSON: it must be the details slot.
    assert '"new_role"' in binds[str(len(binds) - 1)]["value"]
    assert binds[str(len(binds))]["value"] == "aud-1"


# --- writer: failures are counted and logged ------------------------------------------------------------------


def test_writer_counts_rejected_results_as_failed_and_logs_warning(caplog) -> None:
    w = tm.TelemetryWriter(lambda sql, b, t: _sql_api(sql, b))
    w._stopped = True  # synchronous
    with caplog.at_level(logging.WARNING, logger="lfx_snowflake.telemetry"):
        w.submit("MERGE INTO DB.S.LANGFLOW_FLOW_RUNS t USING (SELECT ? AS run_id) s", {"1": {"type": "TEXT", "value": None}})
        w.submit("MERGE INTO DB.S.LANGFLOW_FLOW_RUNS t USING (SELECT ? AS run_id) s", {"1": {"type": "TEXT", "value": "x"}})
    assert (w.written, w.failed) == (1, 1)
    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("sink=LANGFLOW_FLOW_RUNS" in m and "sql_code=002049" in m for m in msgs)
    w.close()


def test_writer_background_thread_writes_run_within_seconds() -> None:
    seen: list[str] = []
    w = tm.TelemetryWriter(lambda sql, b, t: seen.append(sql) or {"ok": True})
    w.submit("MERGE INTO X.Y.LANGFLOW_FLOW_RUNS t", None)
    assert w.flush(3.0) and seen and w.written == 1
    w.close()


# --- one RUN per execution -------------------------------------------------------------------------------------


def test_derived_execution_groups_rows_of_one_execution_in_any_order() -> None:
    reg = tm.RunRegistry()
    flow = uuid.uuid4().hex
    t0 = datetime.now(timezone.utc) - timedelta(minutes=10)
    exec_a = [t0 + timedelta(milliseconds=300 * i) for i in range(8)]
    exec_b = [t0 + timedelta(seconds=30, milliseconds=300 * i) for i in range(8)]
    order = list(reversed(exec_b + exec_a))  # sync reads newest first
    ids = [reg.derived_execution(flow_id=flow, ts=ts)[0] for ts in order]
    assert len(set(ids)) == 2
    assert len({reg.derived_execution(flow_id=flow, ts=ts)[0] for ts in exec_a}) == 1  # stable on re-sync
    assert reg.derived_execution(flow_id=flow, ts=t0, job_id="J1") == tm.derived_run_ids(flow, "job:J1")


def test_unlinked_rows_of_one_execution_produce_one_derived_run(tmp_path, bridge) -> None:
    db = tmp_path / "langflow.db"
    flow = uuid.uuid4()
    t0 = datetime.now(timezone.utc) - timedelta(minutes=5)  # no live run record (e.g. after a restart)
    conn = sqlite3.connect(db)
    for i in range(6):
        conn.execute('INSERT INTO "transaction" VALUES (?,?,?,?,?,?,?,?,?)',
                     (uuid.uuid4().hex, _ts(t0 + timedelta(milliseconds=200 * i)), f"V-{i}", None, "{}", "{}", "success", None, flow.hex))
    conn.commit()
    conn.close()
    bridge.sync_langflow_traces_to_snowflake(limit=25)
    runs = _ok_rows(bridge, bridge.runs_table)
    spans = _ok_rows(bridge, bridge.spans_table)
    assert len(spans) == 6
    assert len(runs) == 1  # was 6: one derived RUN per transaction row
    run_id = runs[0]["binds"][0]
    assert all(s["binds"][3] == run_id for s in spans)  # [key, event_id, span_id, run_id, ...]


# --- end to end: build request -> 1.13 writer -> sync -> RUN + joined spans ----------------------------------


def test_build_request_writes_one_run_row_joined_by_all_spans(tmp_path, bridge) -> None:
    from lfx_snowflake import traceability as _trace

    flow = uuid.uuid4()
    svc = _FakeTelemetryWriterService(tmp_path / "langflow.db")
    enqueue_tx = tm.wrap_row_writer(_FakeTelemetryWriterService.enqueue_transaction)
    enqueue_vb = tm.wrap_row_writer(_FakeTelemetryWriterService.enqueue_vertex_build)

    # What the request middleware does for POST /api/v1/build/<flow>/flow.
    path = f"/api/v1/build/{flow}/flow"
    run_id = _trace.run_id_for_request(path, None)
    assert run_id
    rec = tm.RUNS.start(flow_id=str(flow), run_id=run_id, request_id=str(uuid.uuid4()), session_id="sess-rr3")
    token = tm.CURRENT_RUN_RECORD.set(rec)
    try:
        base = datetime.now(timezone.utc) - timedelta(seconds=1)
        for i, vertex in enumerate(("ChatInput-a", "Prompt-b", "LLM-c", "ChatOutput-d")):
            ts = base + timedelta(milliseconds=25 * i)
            enqueue_vb(svc, {"build_id": uuid.uuid4(), "id": vertex, "timestamp": ts, "flow_id": flow, "job_id": None})
            enqueue_tx(svc, {"id": uuid.uuid4(), "vertex_id": vertex, "timestamp": ts, "flow_id": flow})
    finally:
        tm.CURRENT_RUN_RECORD.reset(token)
    bridge.record_flow_run(tm.RUNS.end(rec.run_id, "SUCCESS"))  # middleware finally-block
    svc.flush()  # Langflow's background flush, no run contextvar

    bridge.sync_langflow_traces_to_snowflake(limit=25)

    runs = _ok_rows(bridge, bridge.runs_table)
    assert len(runs) == 1, "exactly one RUN row per execution"
    assert runs[0]["binds"][0] == run_id  # MERGE key == the execution run_id
    spans = _ok_rows(bridge, bridge.spans_table)
    assert len(spans) == 8
    assert {s["binds"][3] for s in spans} == {run_id}  # every span joins the RUN row
    failed = [sql[:40] for sql, _b, res in bridge._rr3_executed if not res["ok"] and "LANGFLOW_" in sql]
    assert failed == []


# --- ROLE_SWITCH audit -----------------------------------------------------------------------------------------


@pytest.fixture
def audits(bridge, monkeypatch):
    events: list[dict[str, Any]] = []
    orig = bridge.record_audit_event

    def capture(**kw):
        events.append(kw)
        return orig(**kw)

    monkeypatch.setattr(bridge, "record_audit_event", capture)
    return events


def _role_switch_rows(bridge) -> list[str]:
    return [sql for sql, _b, res in bridge._rr3_executed if res["ok"] and "AUDIT" in sql.upper() and sql.startswith("INSERT")]


@pytest.mark.parametrize("channel", ["REST", "MCP"])
def test_role_switch_denied_is_audited_and_written(bridge, audits, monkeypatch, channel) -> None:
    monkeypatch.setattr(bridge, "available_roles", ["PUBLIC"])
    res = bridge.switch_role("NOT_GRANTED_ROLE", "flow-x", channel=channel)
    assert res["ok"] is False
    ev = [e for e in audits if e["action"] == "ROLE_SWITCH"]
    assert ev and ev[-1]["outcome"] == "DENIED"
    d = ev[-1]["details"]
    assert d["channel"] == channel and d["new_role"] == "NOT_GRANTED_ROLE" and "previous_role" in d and "user" in d
    assert _role_switch_rows(bridge), "denied ROLE_SWITCH audit row must reach Snowflake"


@pytest.mark.parametrize("callers_rights", [False, True])
def test_role_switch_allowed_is_audited_in_both_modes(bridge, audits, monkeypatch, callers_rights) -> None:
    from lfx_snowflake import identity as _id

    monkeypatch.setattr(bridge, "available_roles", ["ANALYST_RR3"])
    monkeypatch.setattr(_id, "callers_rights_enabled", lambda: callers_rights)
    monkeypatch.setattr(bridge, "compute_role_effective_context", lambda *a, **k: {
        "retained_objects_count": 0, "hidden_by_role_count": 0, "stored_context": {},
        "effective_context": {}, "inaccessible_objects": []})
    monkeypatch.setattr(bridge, "sync_flow_node_templates_sqlite", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "reconcile_triggers_with_flow_context", lambda *a, **k: {"ok": True})
    res = bridge.switch_role("ANALYST_RR3", "flow-y", channel="MCP")
    assert res["ok"] is True
    ev = [e for e in audits if e["action"] == "ROLE_SWITCH"]
    assert ev and ev[-1]["outcome"] == "ALLOWED" and ev[-1]["details"]["new_role"] == "ANALYST_RR3"
    assert ev[-1]["details"]["channel"] == "MCP"
    assert _role_switch_rows(bridge)
