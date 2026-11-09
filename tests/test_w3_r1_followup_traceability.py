"""W3-R1 follow-up: request id reaches the client; audit rows carry effective mode/role and a TRACE_ID."""

from __future__ import annotations

import asyncio
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from lfx_snowflake import identity as ident
from lfx_snowflake import traceability as tr

ROOT = Path(__file__).resolve().parents[1]
RID = "11111111-2222-4333-8444-555555555555"


# --- Request id propagation (pure ASGI, no FastAPI needed) ---------------------------------------


def _call(app, headers=None):
    scope = {"type": "http", "method": "GET", "path": "/x", "headers": headers or []}
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(msg):
        sent.append(msg)

    asyncio.run(tr.RequestIdASGIMiddleware(app)(scope, receive, send))
    start = next(m for m in sent if m["type"] == "http.response.start")
    hdrs = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], hdrs, body


def _app(status, ctype, chunks):
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", ctype)]})
        for i, c in enumerate(chunks):
            await send({"type": "http.response.body", "body": c, "more_body": i < len(chunks) - 1})

    return app


def _assert_ids(hdrs, rid=None):
    rid = rid or hdrs["x-request-id"]
    uuid.UUID(rid)
    assert hdrs["x-request-id"] == rid
    assert hdrs[tr.REQUEST_ID_FALLBACK_HEADER.lower()] == rid
    return rid


def test_request_id_on_json_success_and_incoming_id_reused():
    status, hdrs, _ = _call(_app(200, b"application/json", [b'{"a":1}']), [(b"x-request-id", RID.encode())])
    assert status == 200
    _assert_ids(hdrs, RID)


def test_request_id_on_streaming_and_static_responses():
    _, hdrs, body = _call(_app(200, b"text/event-stream", [b"data: 1\n\n", b"data: 2\n\n"]))
    _assert_ids(hdrs)
    assert body == b"data: 1\n\ndata: 2\n\n"
    _, hdrs, body = _call(_app(200, b"text/html", [b"<html></html>"]))
    _assert_ids(hdrs)


def test_request_id_in_json_error_envelope_with_correct_length():
    _, hdrs, body = _call(_app(404, b"application/json", [b'{"detail":', b'"Not Found"}']))
    rid = _assert_ids(hdrs)
    payload = json.loads(body)
    assert payload == {"detail": "Not Found", "request_id": rid}
    assert int(hdrs["content-length"]) == len(body)


def test_unhandled_exception_returns_500_envelope_with_request_id():
    async def boom(scope, receive, send):
        raise RuntimeError("secret internals")

    status, hdrs, body = _call(boom, [(b"x-request-id", RID.encode())])
    assert status == 500
    _assert_ids(hdrs, RID)
    assert json.loads(body) == {"detail": "Internal Server Error", "request_id": RID}
    assert b"secret internals" not in body


def test_invalid_incoming_request_id_is_replaced():
    _, hdrs, _ = _call(_app(200, b"text/plain", [b"ok"]), [(b"x-request-id", b"<script>")])
    assert _assert_ids(hdrs) != "<script>"


def test_bridge_registers_asgi_middleware_and_fallback_header():
    src = (ROOT / "src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert "app.add_middleware(_trace.RequestIdASGIMiddleware)" in src
    assert "_trace.REQUEST_ID_FALLBACK_HEADER" in src
    assert ".get(_trace.REQUEST_ID_SCOPE_KEY)" in src


# --- Audit row: EXECUTION_MODE / EFFECTIVE_ROLE / TRACE_ID ---------------------------------------


@pytest.fixture
def bridge(monkeypatch, tmp_path):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    monkeypatch.setenv("LANGFLOW_DATABASE_URL", f"sqlite:///{tmp_path / 'lf.db'}")
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    b = LiveSnowflakeBridge()
    b.sent = []  # type: ignore[attr-defined]

    def fake_sql(stmt, *a, **k):
        b.sent.append((stmt, k))
        if "CURRENT_ROLE()" in stmt:
            return {"ok": True, "rows": [{"RL": "SVC_DEFAULT_ROLE"}]}
        return {"ok": True, "rows": []}

    monkeypatch.setattr(b, "execute_sql", fake_sql)
    monkeypatch.setattr(b, "_has_spcs_token", lambda: False)
    return b


def _audit_values(b):
    stmt, k = next((s, k) for s, k in b.sent if s.startswith("INSERT INTO"))
    vals = [v["value"] for _, v in sorted(k["bindings"].items(), key=lambda kv: int(kv[0]))]
    # bindings order: event_id, user, effective_role, execution_mode, epoch, action, outcome, err, target,
    # flow_id, run_id, trace_id, details, event_id
    return {"effective_role": vals[2], "execution_mode": vals[3], "run_id": vals[10], "trace_id": vals[11]}


def test_audit_execution_mode_is_service_when_flag_off_even_on_caller_routes(bridge, monkeypatch):
    monkeypatch.delenv(ident.CALLERS_RIGHTS_ENV, raising=False)
    tok = ident.bind_request_mode(ident.MODE_CALLER)
    try:
        bridge.record_audit_event(action="STAGE_FILE_READ", outcome="SUCCESS")
    finally:
        ident.reset_request_mode(tok)
    assert _audit_values(bridge)["execution_mode"] == "SERVICE"


def test_audit_execution_mode_caller_when_flag_on_survives_background(bridge, monkeypatch):
    monkeypatch.setenv(ident.CALLERS_RIGHTS_ENV, "1")
    tok = ident.bind_request_mode(ident.MODE_CALLER)
    try:
        with ThreadPoolExecutor(1) as ex:
            ident.submit_background(ex, bridge.record_audit_event, action="STAGE_FILE_READ", outcome="SUCCESS").result()
    finally:
        ident.reset_request_mode(tok)
    vals = _audit_values(bridge)
    assert vals["execution_mode"] == "INTERACTIVE_CALLER"
    # Caller mode without an explicit role: unknown is recorded as NULL, never invented.
    assert vals["effective_role"] is None


def test_audit_effective_role_resolved_once_from_service_token(bridge, monkeypatch):
    monkeypatch.delenv(ident.CALLERS_RIGHTS_ENV, raising=False)
    bridge.active_role = ""
    bridge.record_audit_event(action="STAGE_FILE_READ", outcome="SUCCESS")
    assert _audit_values(bridge)["effective_role"] == "SVC_DEFAULT_ROLE"
    bridge.record_audit_event(action="STAGE_FILE_READ", outcome="SUCCESS", target_object="other")
    assert sum("CURRENT_ROLE()" in s for s, _ in bridge.sent) == 1


def test_audit_effective_role_is_role_sent_when_set(bridge, monkeypatch):
    monkeypatch.delenv(ident.CALLERS_RIGHTS_ENV, raising=False)
    bridge.active_role = "ANALYST_ROLE"
    bridge.record_audit_event(action="STAGE_FILE_READ", outcome="SUCCESS")
    assert _audit_values(bridge)["effective_role"] == "ANALYST_ROLE"
    assert not any("CURRENT_ROLE()" in s for s, _ in bridge.sent)


def test_audit_trace_id_links_request_id_and_run_id(bridge):
    with tr.trace_scope(request_id=RID):
        bridge.record_audit_event(action="STAGE_FILE_READ", outcome="SUCCESS")
    assert _audit_values(bridge)["trace_id"] == RID
    bridge.sent.clear()
    bridge.record_audit_event(action="STAGE_FILE_READ", outcome="SUCCESS", run_id="run-9")
    vals = _audit_values(bridge)
    assert vals["run_id"] == "run-9" and vals["trace_id"] == "run-9"


def test_no_hardcoded_interactive_caller_in_bridge_inserts():
    src = (ROOT / "src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert "'INTERACTIVE_CALLER'" not in src
    assert "ExecutionMode.INTERACTIVE_CALLER.value" not in src
