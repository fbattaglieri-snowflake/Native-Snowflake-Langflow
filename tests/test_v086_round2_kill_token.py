import re
import uuid

from lfx_snowflake.memories_lf_mem import LfMemSpec, kill_token_sql, _normalize_kill_token


def _spec():
    return LfMemSpec(name="FACTS", memory_base_id=str(uuid.uuid4()), database="DB", memory_schema="S",
                     embedding_model="snowflake-arctic-embed-m-v1.5")


def _py_twin(text):
    return re.sub(r"[^A-Za-z0-9_]", "", text or "").strip("_").upper() == "NO_INGEST"


def test_sql_twin_matches_python_normalizer_on_variants():
    for t in [" NO_INGEST", "NO_INGEST", "'NO_INGEST'", "```\nNO_INGEST\n```", "no_ingest.", "\n NO_INGEST\n"]:
        assert _normalize_kill_token(t) == "no_ingest" and _py_twin(t)
    assert not _py_twin("please NO_INGEST this")
    assert "REGEXP_REPLACE" in kill_token_sql("x.text") and "'NO_INGEST'" in kill_token_sql("x.text")


def test_pending_and_recall_exclude_kill_token_text():
    s = _spec()
    sql, _ = s.build_pending_request("DB.S.LANGFLOW_MESSAGES", flow_id="f", owner_user=None)
    assert "NOT " + kill_token_sql("m.text") in sql
    sql, _ = s.build_pending_messages_request("DB.S.LANGFLOW_MESSAGES", flow_id="f", session_id="s", cursor_ts=None)
    assert "NOT " + kill_token_sql("m.text") in sql
    sql, _ = s.build_recall_request("q", flow_id="f", session_id=None, filter_by_session=False)
    assert "latest.deleted_at IS NULL AND NOT " + kill_token_sql("latest.text") in sql


def test_recall_rows_drop_legacy_kill_chunks():
    rows = [{"CHUNK_ID": "a", "TEXT": " NO_INGEST", "SCORE": 0.9}, {"CHUNK_ID": "b", "TEXT": "real fact", "SCORE": 0.5}]
    out = LfMemSpec.recall_rows_to_results(rows)
    assert [r["content"] for r in out] == ["real fact"]
