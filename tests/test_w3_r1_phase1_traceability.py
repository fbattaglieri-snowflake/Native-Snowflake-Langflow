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


def test_tr01_audit_insert_uses_bindings_and_merge():
    stmt, binds = tr.build_audit_insert(
        "DB.CORE.LANGFLOW_AUDIT_LOG", event_id="aud-1", authenticated_user="U", effective_role="R",
        execution_mode="INTERACTIVE_CALLER", context_epoch=1, action="MEMORY_RECALL", outcome="SUCCESS",
        error_code=None, target_object="o'; DROP TABLE x;--", flow_id="f", run_id="r", details={"top_k": 5},
    )
    assert "DROP TABLE" not in stmt and stmt.startswith("MERGE INTO DB.CORE.LANGFLOW_AUDIT_LOG")
    assert stmt.count("?") == len(binds) == 13
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
