"""W3-R1-14/15 traceability tests (TR-01, TR-02, TR-06, TR-08)."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest

from lfx_snowflake import traceability as tr

ROOT = Path(__file__).resolve().parents[1]


def test_tr02_query_tag_shape_and_contextvars():
    with tr.trace_scope(run_id="run-1", request_id="req-1", component="MemoryRecall", memory_base_id="mb-1"):
        tag = json.loads(tr.build_query_tag(flow_id="flow-1"))
    assert tuple(tag) == tr.QUERY_TAG_KEYS
    assert tag["app"] == "snowflake-langflow"
    assert tag["run_id"] == "run-1" and tag["request_id"] == "req-1" and tag["memory_base_id"] == "mb-1"
    # Scope is reset: no leak to the next request.
    after = json.loads(tr.build_query_tag(flow_id="f"))
    assert after["run_id"] is None and after["request_id"] is None


def test_tr02_query_tag_length_bounded():
    tag = tr.build_query_tag(flow_id="x" * 5000, run_id="r" * 5000)
    assert len(tag) <= tr.QUERY_TAG_MAX_LEN
    json.loads(tag)


def test_tr01_deterministic_event_id_is_idempotent():
    a = tr.deterministic_event_id("STAGE_FILE_REMOVE", "@DB.S.ST/a.txt", "run", "req")
    assert a == tr.deterministic_event_id("STAGE_FILE_REMOVE", "@DB.S.ST/a.txt", "run", "req")
    assert a != tr.deterministic_event_id("STAGE_FILE_REMOVE", "@DB.S.ST/b.txt", "run", "req")


def test_tr01_emit_outcome_comes_only_from_real_result():
    calls = []
    writer = lambda **kw: calls.append(kw)  # noqa: E731
    with pytest.raises(ValueError):
        tr.emit(writer, tr.AuditAction.STAGE_FILE_REMOVE, {})
    tr.emit(writer, "STAGE_FILE_REMOVE", {"ok": False, "mode": "offline"}, target_object="@A.B.C/x")
    tr.emit(writer, "STAGE_FILE_REMOVE", {"ok": True, "statement_handle": "qid"}, target_object="@A.B.C/x")
    assert calls[0]["outcome"] == "FAILURE" and calls[0]["error_code"] == "NOT_CONFIGURED"
    assert calls[1]["outcome"] == "SUCCESS" and calls[1]["details"]["query_id"] == "qid"
    assert assert_event_id(calls[1]["event_id"])


def assert_event_id(v: str) -> bool:
    return bool(re.fullmatch(r"aud-[0-9a-f]{32}", v))


@pytest.mark.parametrize("action", sorted(tr.DEPRECATED_ACTIONS) + ["MADE_UP"])
def test_tr01_deprecated_and_unknown_actions_rejected(action):
    with pytest.raises(ValueError):
        tr.emit(lambda **kw: None, action, {"ok": True})


def test_tr01_details_never_carry_text():
    clean = tr.sanitize_details({"text": "secret prompt", "message": "m", "size": 10, "md5": "abc"})
    assert clean == {"size": 10, "md5": "abc"}


def test_tr01_audit_insert_uses_bindings_and_is_idempotent():
    stmt, binds = tr.build_audit_insert(
        "DB.CORE.LANGFLOW_AUDIT_LOG", event_id="aud-1", authenticated_user="U", effective_role="R",
        execution_mode="INTERACTIVE_CALLER", context_epoch=1, action="MEMORY_RECALL", outcome="SUCCESS",
        error_code=None, target_object="o'; DROP TABLE x;--", flow_id="f", run_id="r", details={"top_k": 5},
    )
    assert "DROP TABLE" not in stmt and stmt.startswith("INSERT INTO DB.CORE.LANGFLOW_AUDIT_LOG") and "WHERE NOT EXISTS" in stmt
    assert stmt.count("?") == len(binds) == 14
    with pytest.raises(ValueError):
        tr.build_audit_insert("x; DROP", event_id="e", authenticated_user=None, effective_role=None,
                              execution_mode="m", context_epoch=0, action="a", outcome="o", error_code=None,
                              target_object=None, flow_id=None, run_id=None, details=None)


def test_tr08_json_logs_allowlist(caplog):
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, "audit", None, None)
    rec.sflf = {"event": "audit", "run_id": "r", "outcome": "SUCCESS", "text": "should not appear"}
    out = json.loads(tr.JsonLogFormatter().format(rec))
    assert out["event"] == "audit" and out["run_id"] == "r" and "text" not in out


def test_tr06_masking_sql_template_rules():
    sql = (ROOT / "sql" / "04_memories_and_files.sql").read_text()
    code = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    assert "ACCOUNTADMIN" not in code
    assert "TRIM(UPPER(CURRENT_USER()))" in sql
    for col in ("INPUT_TEXT", "OUTPUT_TEXT", "ERROR_MESSAGE", "INPUT_PAYLOAD", "OUTPUT_PAYLOAD",
                "NODE_SPANS_JSON", "FLOW_CONTEXT_JSON", "input_payload", "output_payload",
                "redacted_inputs", "redacted_outputs"):
        assert f"MODIFY COLUMN {col}\n" in sql, col
    # B.3: dedup in subquery, deleted filter outside.
    view = sql.split("V_LANGFLOW_MESSAGES AS", 1)[1].split(";", 1)[0]
    assert view.index("QUALIFY") < view.index("deleted_in_ui_at IS NULL")
    assert "PARSE_JSON(v.$8)" not in sql.replace("TRY_PARSE_JSON", "")
    assert "IS_ROLE_IN_SESSION(effective_role_col)" in sql
    assert "DELETE ON TABLE <DATABASE>.<SCHEMA>.LANGFLOW_MESSAGES" not in sql


# --- Bridge wiring (TR-01/TR-02) -------------------------------------------------------------


@pytest.fixture
def bridge(monkeypatch, tmp_path):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    monkeypatch.setenv("LANGFLOW_DATABASE_URL", f"sqlite:///{tmp_path / 'lf.db'}")
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    b = LiveSnowflakeBridge()
    b.audits = []  # type: ignore[attr-defined]
    monkeypatch.setattr(b, "record_audit_event", lambda **kw: b.audits.append(kw) or kw.get("event_id"))
    monkeypatch.setattr(b, "check_write_privilege_or_deny", lambda *a, **k: None)
    return b


class _Resp:
    status_code = 200
    content = b"{}"

    def json(self):
        return {"data": [], "resultSetMetaData": {"rowType": []}, "statementHandle": "qh"}


def test_tr02_execute_sql_payload_has_json_query_tag(bridge, monkeypatch):
    sent = {}

    class _Client:
        def post(self, url, headers=None, json=None, timeout=None):
            sent.update(json)
            return _Resp()

    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: True)
    monkeypatch.setattr(bridge, "_get_spcs_headers", lambda: {})
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    bridge._http_client = _Client()
    with tr.trace_scope(run_id="run-9", request_id="req-9"):
        bridge.execute_sql("SELECT 1")
    tag = json.loads(sent["parameters"]["query_tag"])
    assert tag["app"] == "snowflake-langflow" and tag["run_id"] == "run-9" and tag["request_id"] == "req-9"


def test_tr01_stage_remove_audited_with_real_outcome(bridge, monkeypatch):
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": False, "error_code": "SQL_ERROR"})
    res = bridge.delete_stage_file("@DB.SCH.ST", "a.txt")
    assert res["ok"] is False
    ev = [a for a in bridge.audits if a["action"] == "STAGE_FILE_REMOVE"]
    assert ev and ev[0]["outcome"] == "FAILURE" and ev[0]["error_code"] == "SQL_ERROR"
    assert "text" not in ev[0]["details"]


def test_tr01_stage_upload_rejection_never_audited_as_success(bridge, monkeypatch):
    monkeypatch.setattr(bridge, "_stage_type", lambda fqn: "INTERNAL")
    res = bridge.upload_stage_file(stage_fqn="@DB.SCH.ST", file_name="a.txt", content_text="x")
    assert res["ok"] is False
    ev = [a for a in bridge.audits if a["action"] == "STAGE_FILE_UPLOAD"]
    assert ev and ev[0]["outcome"] == "FAILURE"


def test_tr01_no_deprecated_audit_action_written_in_source():
    src = (ROOT / "src" / "lfx_snowflake" / "fastapi_live_bridge.py").read_text()
    for name in tr.DEPRECATED_ACTIONS:
        assert f'action="{name}"' not in src, name


# --- W3-R1 Phase 1 fix round: 14a / 14b / 14d / 15 ---------------------------------------------


def test_14a_request_id_from_header_only_accepts_uuid():
    good = "123e4567-e89b-12d3-a456-426614174000"
    assert tr.request_id_from_header(good) == good
    minted = tr.request_id_from_header("'; DROP TABLE x;--")
    assert tr.valid_uuid_or_none(minted) == minted and minted != good
    assert tr.valid_uuid_or_none(tr.request_id_from_header(None))


def test_14a_run_id_only_for_flow_run_routes():
    assert tr.run_id_for_request("/api/v1/flows/abc") is None
    rid = tr.run_id_for_request("/api/v1/build/abc/flow")
    assert tr.valid_uuid_or_none(rid) == rid
    given = "123e4567-e89b-12d3-a456-426614174000"
    assert tr.run_id_for_request("/api/v1/run/abc", given) == given
    assert tr.run_id_for_request("/api/v1/run/abc", "not-a-uuid") != "not-a-uuid"


def test_14a_middleware_binds_and_resets_trace_scope():
    src = (ROOT / "src" / "lfx_snowflake" / "fastapi_live_bridge.py").read_text()
    mw = src.split("async def snowflake_flow_context_and_trace_middleware", 1)[1].split("@sf_router", 1)[0]
    assert "_trace.trace_scope(request_id=request_id, run_id=run_id)" in mw
    assert "request_id_from_header" in mw and "run_id_for_request" in mw
    assert tr.CURRENT_REQUEST_ID.get() is None and tr.CURRENT_RUN_ID.get() is None


def test_14a_cortex_analyst_uses_shared_query_tag():
    src = (ROOT / "src" / "lfx_snowflake" / "components" / "cortex_analyst.py").read_text()
    assert 'build_query_tag(component="cortex_analyst", run_id=run_id)' in src
    assert '"role": self.data_sources.active_role,\n                }' not in src


def test_14b_record_audit_event_enforces_sanitize_details(monkeypatch, tmp_path):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    monkeypatch.setenv("LANGFLOW_DATABASE_URL", f"sqlite:///{tmp_path / 'lf.db'}")
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    b = LiveSnowflakeBridge()
    sent = []
    monkeypatch.setattr(b, "execute_sql", lambda stmt, *a, **k: sent.append((stmt, k)) or {"ok": True})
    b.record_audit_event(
        action="STAGE_FILE_READ", outcome="SUCCESS", target_object="@A.B.C/x",
        details={"message": "secret prompt text", "ddl": "CREATE ...", "size": 3, "stage": "@A.B.C"},
    )
    import time as _t

    deadline = _t.time() + 5
    while not sent and _t.time() < deadline:
        _t.sleep(0.05)
    blob = json.dumps([k.get("bindings") for _, k in sent])
    assert "secret prompt text" not in blob and "CREATE ..." not in blob
    assert '\\"size\\": 3' in blob


def test_14b_recall_emits_memory_recall_with_real_outcome(bridge, monkeypatch):
    cat = [{"name": "M1", "table_fqn": "DB.SCH.M1", "memory_id": "mb-1",
            "agent_memory_type": "structured_long_term", "enable_embedding": False}]
    monkeypatch.setattr(type(bridge), "memories_catalog", property(lambda self: cat))
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": False, "error_code": "SQL_ERROR"})
    out = bridge.recall_memory("M1", query="hello")
    assert out["ok"] is False
    ev = [a for a in bridge.audits if a["action"] == "MEMORY_RECALL"]
    assert len(ev) == 1 and ev[0]["outcome"] == "FAILURE" and ev[0]["error_code"] == "SQL_ERROR"
    assert "hello" not in json.dumps(ev[0])
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": [{"x": 1}], "statement_handle": "q1"})
    bridge.recall_memory("M1", query="hello")
    ok = [a for a in bridge.audits if a["action"] == "MEMORY_RECALL"][-1]
    assert ok["outcome"] == "SUCCESS" and ok["details"]["n_results"] == 1 and ok["details"]["query_id"] == "q1"


def test_14b_recall_not_found_is_not_audited_as_success(bridge, monkeypatch):
    monkeypatch.setattr(type(bridge), "memories_catalog", property(lambda self: []))
    bridge.recall_memory("NOPE")
    assert not [a for a in bridge.audits if a.get("outcome") == "SUCCESS"]


def test_14b_patch_memory_emits_update_after_real_persistence(bridge, monkeypatch):
    entry = {"name": "M1", "table_fqn": "DB.SCH.M1", "memory_id": "mb-1"}
    monkeypatch.setattr(bridge, "_find_memory_entry", lambda mid: entry)
    monkeypatch.setattr(bridge, "_to_langflow_memory_base_read", lambda e: {"name": e["name"]})

    def boom(*a, **k):
        raise RuntimeError("disk")

    monkeypatch.setattr(bridge, "_persist_memory_in_langflow_sqlite", boom)
    res = bridge.patch_native_memory("mb-1", {"threshold": 5})
    assert res["ok"] is False and res["error_code"] == "PERSISTENCE_ERROR"
    assert bridge.audits[-1]["action"] == "MEMORY_BASE_UPDATE" and bridge.audits[-1]["outcome"] == "FAILURE"
    monkeypatch.setattr(bridge, "_persist_memory_in_langflow_sqlite", lambda *a, **k: None)
    assert bridge.patch_native_memory("mb-1", {"threshold": 6})["ok"] is True
    assert bridge.audits[-1]["outcome"] == "SUCCESS" and bridge.audits[-1]["details"]["threshold"] == 6


def test_14b_stage_file_read_audited_with_real_outcome():
    src = (ROOT / "src" / "lfx_snowflake" / "native_category_components" / "files_and_knowledge"
           / "snowflake_canonical_fk.py").read_text()
    body = src.split("def _read_impl", 1)[1].split("def read_stage_file_data", 1)[0]
    i_emit, i_raise = body.index("AuditAction.STAGE_FILE_READ"), body.index("raise ValueError")
    assert body.index("LIVE_BRIDGE.execute_sql") < i_emit < i_raise
    assert '"text"' not in body[i_emit:i_raise]


def test_14d_lifecycle_logs_are_allowlisted(caplog):
    logger = tr.get_trace_logger()
    records = []

    class _H(logging.Handler):
        def emit(self, record):
            records.append(tr.JsonLogFormatter().format(record))

    h = _H()
    logger.addHandler(h)
    try:
        with tr.trace_scope(request_id="req-9", run_id="run-9"):
            tr.log_event("flow_run_end", outcome="SUCCESS", duration_ms=12, prompt="secret", flow_id="f1")
    finally:
        logger.removeHandler(h)
    out = json.loads(records[-1])
    assert out["event"] == "flow_run_end" and out["request_id"] == "req-9" and out["run_id"] == "run-9"
    assert out["duration_ms"] == 12 and "prompt" not in out and "secret" not in records[-1]


def test_14d_startup_and_middleware_emit_lifecycle_events():
    src = (ROOT / "src" / "lfx_snowflake" / "fastapi_live_bridge.py").read_text()
    reg = src.split("def register_snowflake_fastapi_routes", 1)[1][:600]
    assert "_trace.get_trace_logger()" in reg and '"bridge_startup"' in reg
    for ev in ("request_start", "request_end", "flow_run_start", "flow_run_end"):
        assert f'"{ev}"' in src


def test_15_caller_warehouse_grant_present():
    sql = (ROOT / "sql" / "04_memories_and_files.sql").read_text()
    assert "GRANT CALLER USAGE ON WAREHOUSE <WAREHOUSE> TO ROLE <SERVICE_OWNER_ROLE>;" in sql
