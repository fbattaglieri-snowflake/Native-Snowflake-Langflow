"""Round-3: recall kill filter is NULL-safe; a kill-only turn (user NO_INGEST + reply ' NO_INGEST') is consumed whole."""

from __future__ import annotations

from lfx_snowflake.memories_lf_mem import (
    LfMemSpec, contains_kill_token, kill_contains_sql, kill_token_sql, kill_turn_boundary_sql,
)
from lfx_snowflake.memories_lf_mem_ingest import LfMemIngestionPipeline
from test_v086_passo7_ingestion import FakeSnowflake, MB_ID, _spec

MSGS = "SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_MESSAGES"


def test_recall_kill_filter_is_null_safe():
    sql, _ = _spec().build_recall_request("q", flow_id="f", session_id=None, filter_by_session=False, top_k=5)
    assert f"NOT {kill_token_sql('latest.text')}" in sql
    # NOT(NULL) would drop every row: both kill predicates coalesce to FALSE.
    assert kill_token_sql("x").startswith("COALESCE(") and kill_token_sql("x").endswith(", FALSE)")
    assert kill_contains_sql("x").startswith("COALESCE(") and kill_contains_sql("x").endswith(", FALSE)")


def test_contains_kill_token_semantics():
    assert contains_kill_token("NO_INGEST foo non memorizzare")
    assert contains_kill_token(" NO_INGEST") and contains_kill_token("`no_ingest`.")
    assert not contains_kill_token("NO_INGESTION is a word") and not contains_kill_token(None)
    assert not contains_kill_token("Preferisco l'italiano")


def test_pending_sql_excludes_messages_up_to_latest_kill_message():
    for sql, _ in (
        _spec().build_pending_request(MSGS, flow_id="f", owner_user=None),
        LfMemSpec.build_pending_messages_request(MSGS, flow_id="f", session_id="s", cursor_ts=None),
    ):
        assert kill_turn_boundary_sql('"SNOWFLAKE_LANGFLOW"."CORE"."LANGFLOW_MESSAGES"', "m.flow_id", "m.session_id") in sql


def _turn(*texts, base=0):
    return [{"message_id": f"m{base + i}", "sender": "User" if i % 2 == 0 else "Machine", "sender_name": "U",
             "text": t, "created_at": f"2026-11-12 10:00:{base + i:02d}.000000 UTC"} for i, t in enumerate(texts)]


def test_kill_only_turn_writes_one_tombstone_covering_user_and_reply():
    sf = FakeSnowflake()
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_turn("NO_INGEST foo non memorizzare", " NO_INGEST"), threshold=1)
    st = pipe.status(MB_ID)
    assert st["state"] == "SKIPPED_KILL_PHRASE" and st["pending_count"] == 0
    assert len(sf.chunks) == 1 and sf.chunks[0]["deleted"] is True  # only the tombstone, never a live memory
    skips = [c for c in sf.calls if c["sql"].startswith("INSERT INTO") and "deleted_at) SELECT" in c["sql"]]
    assert len(skips) == 1 and sf.chunks[0]["metadata"]["source_message_ids"] == ["m0", "m1"]


def test_normal_turn_still_ingests():
    sf = FakeSnowflake()
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_turn("Preferisco l'italiano", "Ok!"), threshold=1)
    assert pipe.status(MB_ID)["state"] != "SKIPPED_KILL_PHRASE"
