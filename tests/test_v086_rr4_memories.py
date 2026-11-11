"""Rerun-4: kill-phrase normalization and owner-identity consistency between LF_MEM writes and stats."""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from lfx_snowflake.memories_lf_mem import OWNER_MATCH_SQL, LfMemSpec, is_kill_phrase

MB_ID = "0b2f5a1e-6f6a-4c55-9a5b-1b2a3c4d5e6f"
ROOT = Path(__file__).resolve().parents[1]


def _spec():
    return LfMemSpec(memory_base_id=MB_ID, name="facts", database="SNOWFLAKE_LANGFLOW", memory_schema="MEMORIES",
                     embedding_model="snowflake-arctic-embed-l-v2.0", governance_schema="CORE")


KILL = ['" NO_INGEST"', "NO_INGEST", " no_ingest ", "NO_INGEST.", "NO_INGEST!", "'NO_INGEST'", "`NO_INGEST`",
        "```NO_INGEST```", "```\nNO_INGEST\n```", "```text\nNO_INGEST\n```", "**NO_INGEST**", "\u201cNO_INGEST\u201d",
        "\u2018NO_INGEST\u2019", '"NO_INGEST."', "\n\n  NO_INGEST  \n", "> NO_INGEST", "Nothing to store.\nNO_INGEST", "(NO_INGEST)"]
NOT_KILL = ["The user said NO_INGEST should not apply to allergies.", "NO_INGEST the logs", "NO INGEST",
            "User prefers Italian. NO_INGEST", "NO_INGESTED", "", "   "]


@pytest.mark.parametrize("reply", KILL)
def test_kill_variants(reply):
    assert is_kill_phrase(reply)


@pytest.mark.parametrize("reply", NOT_KILL)
def test_not_kill(reply):
    assert not is_kill_phrase(reply)


def test_custom_kill_phrase_normalized():
    assert is_kill_phrase("`skip.`", " SKIP ") and not is_kill_phrase("skip this", "SKIP")


@pytest.mark.parametrize("reply", KILL)
def test_kill_chunk_never_stored(reply):
    sql, _b, receipt = _spec().build_ingest_request([{"text": reply, "session_id": "s"}], flow_id="f")
    assert sql is None and receipt["ingested_chunk_ids"] == []


def test_real_sentence_chunk_stored():
    sql, _b, receipt = _spec().build_ingest_request([{"text": NOT_KILL[0], "session_id": "s"}], flow_id="f")
    assert sql is not None and len(receipt["ingested_chunk_ids"]) == 1


def test_owner_filter_matches_writer_and_rap():
    spec = _spec()
    ingest, _b, _r = spec.build_ingest_request([{"text": "hello", "session_id": "s"}], flow_id="f")
    assert "CURRENT_USER()" in ingest  # writer identity = caller session CURRENT_USER()
    rap = (ROOT / "sql/04_memories_and_files.sql").read_text()
    m = re.search(r"LANGFLOW_MEMORY_RAP\s+AS \(owner_user VARCHAR\) RETURNS BOOLEAN ->\s*\((.*?)\)\s*;", rap, re.S)
    assert m and OWNER_MATCH_SQL in m.group(1)
    for sql, b in (spec.build_session_stats_request(flow_id="f", owner_user="alice@corp"),
                   spec.build_pending_request("DB.CORE.LANGFLOW_MESSAGES", flow_id="f", owner_user="alice@corp"),
                   spec.build_messages_request(flow_id="f", owner_user="alice@corp", session_id=None, limit=5, offset=0)):
        assert "TRIM(UPPER(CURRENT_USER()))" in sql and "owner_user = ?" not in sql
        assert "alice@corp" not in str(b)  # no header-string binding that could mismatch CURRENT_USER()


def test_stats_and_ingest_same_caller_mode(monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
    from lfx_snowflake import memories_lf_mem_ingest as ing

    modes = []
    bridge = LiveSnowflakeBridge.__new__(LiveSnowflakeBridge)
    bridge.execute_sql = lambda sql, bindings=None, mode=None, **k: (modes.append(mode) or {"ok": True, "rows": []})
    bridge._configured_messages_table = lambda: "DB.CORE.LANGFLOW_MESSAGES"
    s = _spec()
    entry = {"flow_id": "f", "lf_mem": s.to_dict()}
    out = bridge.governed_memory_stats(entry)
    assert out["status"] == "OK" and modes == ["caller", "caller"]
    pipe = ing.LfMemIngestionPipeline.__new__(ing.LfMemIngestionPipeline)
    pipe._bridge = bridge
    pipe._exec("SELECT 1", {}, guarded=False, flow_id="f")
    assert modes[-1] == "caller"
