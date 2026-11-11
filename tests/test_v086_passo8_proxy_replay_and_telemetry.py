"""PASSO 8 items 2-3: BUG-PROXY-A4-01 (Messages replay) and unified telemetry (BUG-LLMOP-A4-01, BUG-UI-TRC-01, BUG-MCP-02)."""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest

from lfx_snowflake import telemetry as tm
from lfx_snowflake import traceability as tr
from lfx_snowflake.providers import messages_replay as mr


@dataclass
class Msg:
    type: str
    content: Any
    tool_calls: list = field(default_factory=list)
    tool_call_id: str | None = None


def _turn1() -> tuple[list[Msg], Msg]:
    """Turn 1 of an Agent tool loop on /messages: thinking(signed) + tool_use -> tool_result -> final text."""
    sys = Msg("system", "You are helpful")
    human = Msg("human", "What is 17*23?")
    ai_tool = Msg(
        "ai",
        [
            {"type": "thinking", "thinking": "need calculator", "signature": "SIG-abc=="},
            {"type": "tool_use", "id": "toolu_1", "name": "calculator", "input": {"expr": "17*23"}},
        ],
        tool_calls=[{"id": "toolu_1", "name": "calculator", "args": {"expr": "17*23"}}],
    )
    tool = Msg("tool", "391", tool_call_id="toolu_1")
    final = Msg("ai", [{"type": "thinking", "thinking": "done", "signature": "SIG-def=="}, {"type": "text", "text": "17*23 = 391"}])
    return [sys, human, ai_tool, tool], final


# --------------------------------------------------------------------- BUG-PROXY-A4-01
def test_replay_reinjects_signed_thinking_tool_use_and_tool_result() -> None:
    store = mr.MessagesReplayStore()
    msgs, final = _turn1()
    assert mr.record_turn(msgs, final, store=store, scope="s1")
    # Turn 2 as Langflow sends it: history keeps only the final text.
    turn2 = [msgs[0], msgs[1], Msg("ai", "17*23 = 391"), Msg("human", "Which tool did you call?")]
    out = mr.rehydrate(turn2, thinking_enabled=True, tools_bound=True, store=store, scope="s1")
    types = [m.type for m in out]
    assert types == ["system", "human", "ai", "tool", "ai", "human"]
    assert out[2].content[0] == {"type": "thinking", "thinking": "need calculator", "signature": "SIG-abc=="}
    assert out[2].tool_calls[0]["id"] == out[3].tool_call_id == "toolu_1"  # tool_use / tool_result pairing
    assert out[4].content[-1]["text"] == "17*23 = 391"


def test_replay_drops_thinking_when_disabled_and_summarizes_without_tools() -> None:
    store = mr.MessagesReplayStore()
    msgs, final = _turn1()
    mr.record_turn(msgs, final, store=store, scope="s1")
    turn2 = [msgs[1], Msg("ai", "17*23 = 391"), Msg("human", "again?")]
    no_think = mr.rehydrate(turn2, thinking_enabled=False, tools_bound=True, store=store, scope="s1")
    assert all(
        not (isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking"))
        for m in no_think
        if isinstance(m.content, list)
        for b in m.content
    )
    assert no_think[1].tool_calls and no_think[2].type == "tool"
    summ = mr.rehydrate(turn2, thinking_enabled=True, tools_bound=False, store=store, scope="s1")
    assert [m.type for m in summ] == ["human", "ai", "human"]
    assert "tool_use calculator" in summ[1].content and "tool_result: 391" in summ[1].content
    assert summ[1].content.endswith("17*23 = 391")


def test_replay_scope_isolation_and_no_loop_not_recorded() -> None:
    store = mr.MessagesReplayStore()
    msgs, final = _turn1()
    mr.record_turn(msgs, final, store=store, scope="userA")
    turn2 = [msgs[1], Msg("ai", "17*23 = 391")]
    assert mr.rehydrate(turn2, thinking_enabled=True, tools_bound=True, store=store, scope="userB") == turn2
    assert not mr.record_turn([Msg("human", "hi")], Msg("ai", "hello"), store=store, scope="userA")
    # Unfinished loop (output still has tool_calls) is never recorded.
    assert not mr.record_turn(msgs, msgs[2], store=store, scope="userA")


def test_protected_anthropic_hooks_rehydrate_and_record(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake import standard_langflow as sl

    mr.STORE.clear()
    seen: list[list[Msg]] = []

    class _Result:
        def __init__(self, message: Msg) -> None:
            self.generations = [type("G", (), {"message": message})()]

    class _FakeBase:
        def _generate(self, messages, stop=None, run_manager=None, **kwargs):
            seen.append(list(messages))
            last = messages[-1]
            if last.type == "human" and "17" in str(last.content):
                return _Result(Msg("ai", [{"type": "tool_use", "id": "toolu_9", "name": "calc", "input": {}}],
                                   tool_calls=[{"id": "toolu_9", "name": "calc", "args": {}}]))
            if last.type == "tool":
                return _Result(Msg("ai", [{"type": "thinking", "thinking": "t", "signature": "S1"}, {"type": "text", "text": "391"}]))
            return _Result(Msg("ai", "ok"))

    class _P(sl.ProtectedAnthropic, _FakeBase):
        pass

    llm = _P()
    llm.thinking = {"type": "enabled", "budget_tokens": 1024}
    llm._sflf_session_id = "sess-hook"
    h1 = Msg("human", "17*23?")
    r1 = llm._generate([h1], tools=[{"name": "calc"}])
    ai1 = r1.generations[0].message
    llm._generate([h1, ai1, Msg("tool", "391", tool_call_id="toolu_9")], tools=[{"name": "calc"}])
    llm._generate([h1, Msg("ai", "391"), Msg("human", "what tool?")], tools=[{"name": "calc"}])
    assert [m.type for m in seen[-1]] == ["human", "ai", "tool", "ai", "human"]
    assert seen[-1][3].content[0]["signature"] == "S1"
    # Another session of the same user/flow never sees those tool results.
    other = _P()
    other.thinking = llm.thinking
    other._sflf_session_id = "sess-other"
    other._generate([h1, Msg("ai", "391"), Msg("human", "what tool?")], tools=[{"name": "calc"}])
    assert [m.type for m in seen[-1]] == ["human", "ai", "human"]
    # No session at all -> no replay and nothing recorded.
    anon = _P()
    anon.thinking = llm.thinking
    anon._generate([h1, Msg("ai", "391"), Msg("human", "what tool?")], tools=[{"name": "calc"}])
    assert [m.type for m in seen[-1]] == ["human", "ai", "human"]


# --------------------------------------------------------------------- telemetry ids
def test_canonical_flow_id_and_query_tag_shape() -> None:
    hexid = uuid.uuid4().hex
    assert tr.canonical_flow_id(hexid) == str(uuid.UUID(hexid))
    assert tr.canonical_flow_id(hexid.upper()) == str(uuid.UUID(hexid))
    assert tr.canonical_flow_id("custom-key") == "custom-key"
    tag = json.loads(tr.build_query_tag(flow_id=hexid, run_id="r1"))
    assert tuple(tag) == tr.QUERY_TAG_KEYS
    assert tag["flow_id"] == str(uuid.UUID(hexid)) and tag["component"] == tr.DEFAULT_COMPONENT


def test_query_tag_uses_run_record_and_component_from_stack() -> None:
    rec = tm.RUNS.start(flow_id=uuid.uuid4().hex, session_id="sess-1", request_id="req-1")
    token = tm.CURRENT_RUN_RECORD.set(rec)

    class SnowflakeDemoComponent:
        display_name = "Demo Component"

        def run(self) -> str:
            return tr.build_query_tag(run_id=rec.run_id)

    try:
        tag = json.loads(SnowflakeDemoComponent().run())
    finally:
        tm.CURRENT_RUN_RECORD.reset(token)
    assert tag["flow_id"] == rec.flow_id and "-" in tag["flow_id"]
    assert tag["trace_id"] == rec.trace_id and tag["session_id"] == "sess-1"
    assert tag["component"] == "Demo Component"


def test_run_registry_unique_ids_and_match() -> None:
    reg = tm.RunRegistry()
    fid = uuid.uuid4().hex
    a, b = reg.start(flow_id=fid), reg.start(flow_id=fid)
    assert a.run_id != b.run_id and a.trace_id != b.trace_id and a.flow_id == str(uuid.UUID(fid))
    reg.end(a.run_id, "success")
    assert a.status == "SUCCESS" and a.end_ts is not None
    assert reg.match(fid, a.start_ts.isoformat()) is None  # a and b overlap: ambiguous, never collapsed
    assert tm.derived_run_ids(fid, "2026-11-11T10:00") == tm.derived_run_ids(str(uuid.UUID(fid)), "2026-11-11T10:00")


def test_annotate_current_run() -> None:
    rec = tm.RUNS.start(flow_id="f")
    token = tm.CURRENT_RUN_RECORD.set(rec)
    try:
        tm.annotate_current_run(session_id="s", cortex_thread_id=123, a2a_task_id="task-1")
        tm.annotate_current_run(session_id="other")
    finally:
        tm.CURRENT_RUN_RECORD.reset(token)
    assert (rec.session_id, rec.cortex_thread_id, rec.a2a_task_id) == ("s", "123", "task-1")


def test_audit_insert_carries_request_id_and_canonical_flow() -> None:
    hexid = uuid.uuid4().hex
    with tr.trace_scope(request_id="11111111-2222-3333-4444-555555555555"):
        stmt, binds = tr.build_audit_insert(
            "DB.SCH.LANGFLOW_AUDIT_LOG", event_id="e1", authenticated_user="u", effective_role="R",
            execution_mode="SERVICE", context_epoch=1, action="A", outcome="SUCCESS", error_code=None,
            target_object=None, flow_id=hexid, run_id="r", details={},
        )
    values = [b["value"] for b in binds.values()]
    assert str(uuid.UUID(hexid)) in values
    assert any(v and "11111111-2222-3333-4444-555555555555" in v for v in values)


def test_bridge_record_flow_run_binds_real_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", lambda sql, **kw: calls.append((sql, kw)) or {"ok": True})
    rec = tm.RUNS.start(flow_id=uuid.uuid4().hex, session_id="sess-9", request_id="req-9")
    rec.cortex_thread_id, rec.a2a_task_id = "42", "task-9"
    LIVE_BRIDGE.record_flow_run(tm.RUNS.end(rec.run_id, "SUCCESS"))
    sql, kw = calls[-1]
    vals = [b["value"] for b in kw["bindings"].values()]
    for needle in (rec.run_id, rec.trace_id, rec.flow_id, "sess-9", "42", "task-9", "SUCCESS"):
        assert needle in vals
    assert "TRY_TO_TIMESTAMP_TZ(?)" in sql
    assert json.loads(kw["query_tag"])["run_id"] == rec.run_id


def test_replication_has_no_fabricated_ids() -> None:
    import pathlib

    src = (pathlib.Path(tr.__file__).parent / "fastapi_live_bridge.py").read_text()
    assert 'f"trc-{flow_id[:16]}"' not in src and 'f"run-{flow_id[:16]}"' not in src


# --------------------------------------------------------------------- async writer
def test_writer_is_async_and_flushes_without_loss() -> None:
    gate = threading.Event()
    written: list[str] = []

    def _exec(sql, binds, tag):
        gate.wait(2)
        written.append(sql)

    w = tm.TelemetryWriter(_exec, interval_s=0.01)
    t0 = time.monotonic()
    for i in range(20):
        w.submit(f"S{i}")
    assert time.monotonic() - t0 < 0.5  # caller never waits on DML
    gate.set()
    assert w.flush(5)
    assert sorted(written, key=lambda x: int(x[1:])) == [f"S{i}" for i in range(20)]


def test_writer_close_drains_and_backpressure_writes_inline() -> None:
    written: list[str] = []
    w = tm.TelemetryWriter(lambda s, b, t: written.append(s), max_queue=1, interval_s=5)
    for i in range(5):
        w.submit(f"S{i}")
    w.close()
    assert len(written) == 5
    w.submit("after-close")
    assert written[-1] == "after-close"


def test_writer_survives_execute_errors() -> None:
    def _boom(sql, binds, tag):
        raise RuntimeError("x")

    w = tm.TelemetryWriter(_boom, interval_s=0.01)
    w.submit("S")
    assert w.flush(2) and w.failed == 1


def test_replay_requires_session_and_uses_bound_session(monkeypatch: pytest.MonkeyPatch) -> None:
    store = mr.MessagesReplayStore()
    msgs, final = _turn1()
    assert mr._scope() is None
    assert not mr.record_turn(msgs, final, store=store)
    with tr.trace_scope(session_id="sess-A"):
        assert mr._scope().endswith("|sess-A")
        assert mr.record_turn(msgs, final, store=store)
        turn2 = [msgs[1], Msg("ai", "17*23 = 391")]
        assert len(mr.rehydrate(turn2, thinking_enabled=True, tools_bound=True, store=store)) == 4
    with tr.trace_scope(session_id="sess-B"):
        assert mr.rehydrate(turn2, thinking_enabled=True, tools_bound=True, store=store) == turn2
    token = mr.CURRENT_LLM_SESSION.set("sess-A")
    try:
        assert len(mr.rehydrate(turn2, thinking_enabled=True, tools_bound=True, store=store)) == 4
    finally:
        mr.CURRENT_LLM_SESSION.reset(token)


def test_thinking_only_turn_is_never_summarized_as_tool_calls() -> None:
    store = mr.MessagesReplayStore()
    human = Msg("human", "hi")
    final = Msg("ai", [{"type": "thinking", "thinking": "t", "signature": "S9"}, {"type": "text", "text": "hello"}])
    assert mr.record_turn([human], final, store=store, scope="s")
    hist = [human, Msg("ai", "hello"), Msg("human", "next")]
    out = mr.rehydrate(hist, thinking_enabled=True, tools_bound=False, store=store, scope="s")
    assert out[1].content[0]["signature"] == "S9"
    assert "[Tool calls made in this turn]" not in json.dumps([m.content for m in out], default=str)
    plain = mr.rehydrate(hist, thinking_enabled=False, tools_bound=False, store=store, scope="s")
    assert plain[1].content == [{"type": "text", "text": "hello"}]


def test_session_id_from_run_and_build_bodies() -> None:
    import asyncio

    from lfx_snowflake.fastapi_live_bridge import _session_id_from_body

    class _Req:
        def __init__(self, body: Any, method: str = "POST", ctype: str = "application/json") -> None:
            self._b = json.dumps(body).encode()
            self.method = method
            self.headers = {"content-type": ctype, "content-length": str(len(self._b))}

        async def body(self) -> bytes:
            return self._b

    run = asyncio.run
    assert run(_session_id_from_body(_Req({"session_id": "s-run", "input_value": "x"}))) == "s-run"
    assert run(_session_id_from_body(_Req({"inputs": {"input_value": "x", "session": "s-build"}}))) == "s-build"
    assert run(_session_id_from_body(_Req({"x": 1}))) is None
    assert run(_session_id_from_body(_Req({"session_id": "s"}, method="GET"))) is None
    assert run(_session_id_from_body(_Req({"session_id": "s"}, ctype="text/plain"))) is None


def test_match_never_collapses_concurrent_runs() -> None:
    reg = tm.RunRegistry()
    fid = uuid.uuid4().hex
    a = reg.start(flow_id=fid, session_id="s1")
    b = reg.start(flow_id=fid, session_id="s2")
    now = a.start_ts.isoformat()
    assert reg.match(fid, now) is None  # two active runs, no session: ambiguous
    assert reg.match(fid, now, "s2") is b and reg.match(fid, now, "s1") is a
    assert reg.match(fid, None) is None
    solo = tm.RunRegistry()
    c = solo.start(flow_id=fid)
    assert solo.match(fid, c.start_ts.isoformat()) is c


def test_record_flow_run_counts_and_sync_flushes(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", lambda sql, **kw: {"ok": True})
    before = LIVE_BRIDGE.synced_runs_count
    rec = tm.RUNS.start(flow_id=uuid.uuid4().hex, session_id="s")
    LIVE_BRIDGE.record_flow_run(tm.RUNS.end(rec.run_id, "SUCCESS"))
    assert LIVE_BRIDGE.synced_runs_count == before + 1
    flushed: list[Any] = []
    monkeypatch.setattr(LIVE_BRIDGE, "flush_telemetry", lambda timeout=None: flushed.append(timeout) or True)
    LIVE_BRIDGE.sync_langflow_traces_to_snowflake(replicate_sqlite=False)
    assert len(flushed) == 2


class _SRResp:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code, self._b = status, body
        self.content = b"x"

    def json(self) -> dict:
        return self._b


def test_t2_denied_user_statement_100132_never_retried_without_prefix() -> None:
    """E2E rerun-2/T2: 100132 on statement 2 (denied SELECT) must surface, never retry, never leak."""
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    for status_code in (422, 200):
        posted: list[str] = []

        class _Client:
            def post(self, url, headers=None, json=None, timeout=None):
                posted.append(json["statement"])
                if json["statement"].upper().startswith("USE SECONDARY ROLES"):
                    return _SRResp(status_code if status_code == 422 else 422, {
                        "code": "100132",
                        "message": "SQL compilation error: Object 'SECRET_T' does not exist or not authorized.",
                        "statementHandle": "h2",
                        "statementHandles": ["h1", "h2"],
                    })
                return _SRResp(200, {"data": [["TOP-SECRET"]]})

            def get(self, *a, **k):
                raise AssertionError("no child fetch on error")

        status, body = LiveSnowflakeBridge._post_sql_statements(
            _Client(), "u", {}, {"statement": "USE SECONDARY ROLES NONE;\nSELECT * FROM SECRET_T"}, 5.0
        )
        assert status == 422 and len(posted) == 1
        assert "TOP-SECRET" not in repr(body) and "not authorized" in body["message"]

    # Same error without handles: message is not the USE rejection -> surfaced, no retry.
    posted.clear()

    class _NoHandles:
        def post(self, url, headers=None, json=None, timeout=None):
            posted.append(json["statement"])
            return _SRResp(422, {"code": "100132", "message": "Object does not exist or not authorized."})

    status, _ = LiveSnowflakeBridge._post_sql_statements(_NoHandles(), "u", {}, {"statement": "USE SECONDARY ROLES ALL;\nSELECT 2"}, 5.0)
    assert status == 422 and len(posted) == 1


def test_secondary_roles_prefix_rejection_is_403_and_user_sql_never_sent() -> None:
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    cases = [
        {"code": "100132", "message": "USE ROLE not allowed in SYSTEM$MULTISTMT"},
        {"code": "391911", "message": "Command not supported by SQL API: USE"},
        {"code": "100132", "message": "x", "statementHandle": "h1", "statementHandles": ["h1", "h2"]},
    ]
    for err in cases:
        posted: list[str] = []

        class _Client:
            def post(self, url, headers=None, json=None, timeout=None):
                posted.append(json["statement"])
                if not json["statement"].upper().startswith("USE SECONDARY ROLES"):
                    raise AssertionError("user SQL sent without prefix")
                return _SRResp(422, dict(err))

            def get(self, *a, **k):
                raise AssertionError("no polling expected")

        status, body = LiveSnowflakeBridge._post_sql_statements(
            _Client(), "u", {}, {"statement": "USE SECONDARY ROLES NONE;\nSELECT 1"}, 5.0
        )
        assert status == 403 and body["error_code"] == "AUTHZ_ROLE_DENIED" and len(posted) == 1


def test_secondary_roles_success_and_unprefixed_paths_unchanged() -> None:
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    class _Ok:
        def post(self, url, headers=None, json=None, timeout=None):
            return _SRResp(200, {"statementHandles": ["h1", "h2"]})

        def get(self, url, headers=None, timeout=None):
            assert url.endswith("/h2")
            return _SRResp(200, {"data": [["1"]]})

    status, body = LiveSnowflakeBridge._post_sql_statements(_Ok(), "u", {}, {"statement": "USE SECONDARY ROLES NONE;\nSELECT 1"}, 5.0)
    assert status == 200 and body == {"data": [["1"]]}
    posted: list[str] = []

    class _Always422:
        def post(self, url, headers=None, json=None, timeout=None):
            posted.append(json["statement"])
            return _SRResp(422, {"code": "100132", "message": "USE ROLE not allowed in SYSTEM$MULTISTMT"})

    status, _ = LiveSnowflakeBridge._post_sql_statements(_Always422(), "u", {}, {"statement": "SELECT 3"}, 5.0)
    assert status == 422 and posted == ["SELECT 3"]  # no prefix -> passthrough, no retry
