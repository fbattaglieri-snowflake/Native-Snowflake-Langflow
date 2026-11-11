"""PASSO 7 follow-up: LF_MEM auto-capture ingestion (threshold, NO_INGEST, AI_EMBED, cursor, status, background writer)."""

from __future__ import annotations

import asyncio
import sys
import types
import uuid

import pytest

from lfx_snowflake import flow_context_guard as guard
from lfx_snowflake.memories_lf_mem import LfMemSpec, aggregate_memory_stats
from lfx_snowflake.memories_lf_mem_ingest import (
    LfMemIngestionPipeline,
    capture_snowflake_memory_base,
    install_snowflake_auto_capture,
)

MB_ID = "0b2f5a1e-6f6a-4c55-9a5b-1b2a3c4d5e6f"
MSG = "SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_MESSAGES"


def _spec():
    return LfMemSpec(name="facts", memory_base_id=MB_ID, database="SNOWFLAKE_LANGFLOW", memory_schema="MEMORIES",
                     embedding_model="snowflake-arctic-embed-l-v2.0", governance_schema="CORE")


def _vals(b):
    return [x["value"] for _, x in sorted((b or {}).items(), key=lambda kv: int(kv[0]))]


class FakeSnowflake:
    """Emulates the governed tables: MERGE into LANGFLOW_MESSAGES, cursor read, pending read, LF_MEM inserts."""

    def __init__(self, *, complete_response="User prefers answers in Italian.", embed_error=False):
        self.messages: dict[str, dict] = {}
        self.chunks: list[dict] = []
        self.calls: list[dict] = []
        self.complete_response = complete_response
        self.embed_error = embed_error

    def _configured_messages_table(self):
        return MSG

    def execute_sql(self, sql, *, bindings=None, mode=None, **_):
        vals = _vals(bindings)
        self.calls.append({"sql": sql, "vals": vals, "mode": mode, "guarded": guard.FLOW_COMPONENT_SQL.get()})
        if sql.startswith("MERGE INTO"):
            for i in range(0, len(vals), 7):
                mid, fid, sid, snd, sname, text, ts = vals[i:i + 7]
                self.messages.setdefault(mid, {"message_id": mid, "flow_id": fid, "session_id": sid, "sender": snd,
                                               "sender_name": sname, "text": text, "created_at": ts})
            return {"ok": True, "rows": []}
        if sql.startswith("SELECT TO_VARCHAR(MAX("):
            mb, fid, sid = vals
            cur = [c["metadata"]["source_last_created_at"] for c in self.chunks
                   if c["memory_base_id"] == mb and c["flow_id"] == fid and c["session_id"] == sid]
            return {"ok": True, "rows": [{"CURSOR_TS": max(cur) if cur else None}]}
        if sql.startswith("SELECT m.message_id"):
            fid, sid, cursor = vals
            rows = sorted((m for m in self.messages.values() if m["flow_id"] == fid and m["session_id"] == sid
                           and m["created_at"] > cursor), key=lambda m: m["created_at"])
            return {"ok": True, "rows": [{k.upper(): v for k, v in m.items()} for m in rows]}
        if sql.startswith("SELECT AI_COMPLETE("):
            return {"ok": True, "rows": [{"RESPONSE": self.complete_response}]}
        if sql.startswith("INSERT INTO") and "deleted_at)" in sql:
            cid, mb, fid, sid, text, meta, job = vals
            import json

            self.chunks.append({"chunk_id": cid, "memory_base_id": mb, "flow_id": fid, "session_id": sid, "text": text,
                                "metadata": json.loads(meta), "job_id": job, "deleted": True})
            return {"ok": True, "rows": []}
        if sql.startswith("INSERT INTO"):
            if self.embed_error:
                return {"ok": False, "error_code": "SNOWFLAKE_ERROR", "message": "AI_EMBED: invalid model", "status_code": 422}
            import json

            for i in range(0, len(vals), 10):
                cid, mb, fid, sid, snd, sname, text, meta, text2, job = vals[i:i + 10]
                assert text == text2
                self.chunks.append({"chunk_id": cid, "memory_base_id": mb, "flow_id": fid, "session_id": sid,
                                    "text": text, "metadata": json.loads(meta), "job_id": job, "deleted": False})
            return {"ok": True, "rows": []}
        raise AssertionError(sql)


def _msgs(*texts, start=0):
    return [{"message_id": f"m{start + i}", "sender": "User", "sender_name": "U", "text": t,
             "created_at": f"2026-11-11T09:00:{start + i:02d}.000000+00:00"} for i, t in enumerate(texts)]


def test_threshold_then_ingest_with_ai_embed_bindings_and_cursor():
    sf = FakeSnowflake()
    pipe = LfMemIngestionPipeline(sf, background=False)
    spec = _spec()
    r1 = pipe.submit_turn(spec, flow_id="flow-1", session_id="s1", messages=_msgs("Preferisco l'italiano"), threshold=2)
    st = pipe.status(MB_ID)
    assert st["job_id"] == r1 and st["state"] == "PENDING" and st["pending_count"] == 1 and sf.chunks == []
    pipe.submit_turn(spec, flow_id="flow-1", session_id="s1", messages=_msgs("Preferisco l'italiano", "e il formato EUR"), threshold=2)
    st = pipe.status(MB_ID)
    assert st["state"] == "INGESTED" and st["ingested"] == 2 and st["last_sync_at"]
    assert [c["text"] for c in sf.chunks] == ["Preferisco l'italiano", "e il formato EUR"]
    assert all(c["session_id"] == "s1" and c["job_id"] == st["job_id"] for c in sf.chunks)
    assert sf.chunks[-1]["metadata"]["source_last_created_at"] == "2026-11-11T09:00:01.000000+00:00"
    ins = [c for c in sf.calls if c["sql"].startswith("INSERT INTO")][0]
    assert "AI_EMBED('snowflake-arctic-embed-l-v2.0', ?)" in ins["sql"] and "l'italiano" not in ins["sql"]
    assert "CURRENT_USER(), CURRENT_ROLE()" in ins["sql"]
    # user LF_MEM table SQL runs under the flow guard; governed messages SQL does not; all caller mode
    for c in sf.calls:
        assert c["mode"] == "caller"
        assert c["guarded"] is ((f'"{spec.table_name}"' in c["sql"] and "LANGFLOW_MESSAGES" not in c["sql"]) or c["sql"].startswith("SELECT AI_COMPLETE"))
    # cursor: the same messages are not ingested twice
    pipe.submit_turn(spec, flow_id="flow-1", session_id="s1", messages=_msgs("Preferisco l'italiano", "e il formato EUR"), threshold=1)
    assert pipe.status(MB_ID)["state"] == "PENDING" and len(sf.chunks) == 2


def test_preprocessing_kill_phrase_writes_tombstone_and_advances_cursor():
    sf = FakeSnowflake(complete_response="  NO_INGEST \n")
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_msgs("ciao"), threshold=1, preprocessing=True,
                     preproc_model="mistral-large2", preproc_instructions="Extract facts", kill_phrase="NO_INGEST")
    st = pipe.status(MB_ID)
    assert st["state"] == "SKIPPED_KILL_PHRASE" and st["skipped"] == 1
    assert len(sf.chunks) == 1 and sf.chunks[0]["deleted"] is True and "AI_EMBED" not in sf.calls[-1]["sql"]
    comp = [c for c in sf.calls if c["sql"].startswith("SELECT AI_COMPLETE")][0]
    assert comp["sql"] == "SELECT AI_COMPLETE('mistral-large2', ?) AS RESPONSE" and "ciao" in comp["vals"][0]
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_msgs("ciao"), threshold=1, preprocessing=True,
                     preproc_model="mistral-large2", preproc_instructions="Extract facts")
    assert pipe.status(MB_ID)["state"] == "PENDING"  # cursor advanced past the skipped batch


def test_preprocessing_extracted_context_is_one_chunk():
    sf = FakeSnowflake(complete_response="L'utente preferisce l'italiano.")
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_msgs("a", "b"), threshold=2, preprocessing=True,
                     preproc_model="mistral-large2", preproc_instructions="facts")
    assert [c["text"] for c in sf.chunks] == ["L'utente preferisce l'italiano."]
    assert sf.chunks[0]["metadata"]["source_message_ids"] == ["m0", "m1"]


@pytest.mark.parametrize("model", ["mistral-large2'); DROP TABLE x; --", "Mistral Large", ""])
def test_t12_preprocessing_model_injection_rejected(model):
    sf = FakeSnowflake()
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_msgs("x"), threshold=1, preprocessing=True,
                     preproc_model=model, preproc_instructions="facts")
    st = pipe.status(MB_ID)
    assert st["state"] == "FAILED" and st["error_code"] == "INVALID_PREPROC_MODEL"
    assert not any(c["sql"].startswith("SELECT AI_COMPLETE") for c in sf.calls) and sf.chunks == []


def test_ai_embed_failure_is_loud_and_never_fakes():
    sf = FakeSnowflake(embed_error=True)
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_msgs("x"), threshold=1)
    st = pipe.status(MB_ID)
    assert st["state"] == "FAILED" and "AI_EMBED" in st["error"] and sf.chunks == []

    class Offline(FakeSnowflake):
        def execute_sql(self, *a, **k):
            return {"ok": False, "mode": "offline"}

    pipe2 = LfMemIngestionPipeline(Offline(), background=False)
    pipe2.submit_turn(_spec(), flow_id="f", session_id="s", messages=_msgs("x"), threshold=1)
    assert pipe2.status(MB_ID)["error_code"] == "NOT_CONFIGURED"


def test_background_writer_runs_off_turn_and_drains_with_context():
    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

    sf = FakeSnowflake()
    pipe = LfMemIngestionPipeline(sf, background=True)
    token = CURRENT_SNOWFLAKE_FLOW_ID.set("turn-flow")
    try:
        pipe.submit_turn(_spec(), flow_id="flow-bg", session_id="s", messages=_msgs("x", "y"), threshold=2)
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(token)
    assert pipe.flush(timeout=10) is True
    assert pipe.status(MB_ID)["state"] == "INGESTED" and len(sf.chunks) == 2


def test_native_auto_capture_hook_routes_only_lf_mem(monkeypatch):
    ingestion = types.ModuleType("fake_ingestion")
    native_calls = []

    async def native_maybe_trigger(*, mb, session_id, job_id, get_or_create_session):
        native_calls.append(mb.kb_name)

    ingestion._maybe_trigger = native_maybe_trigger
    assert install_snowflake_auto_capture(ingestion) is True and install_snowflake_auto_capture(ingestion) is False

    fake_memory = types.ModuleType("langflow.memory")
    seen = {}

    async def aget_messages(**kw):
        seen.update(kw)
        from types import SimpleNamespace
        from datetime import datetime, timezone

        return [SimpleNamespace(id=uuid.UUID(int=1), sender="User", sender_name="U", text="Preferisco l'italiano",
                                timestamp=datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc))]

    fake_memory.aget_messages = aget_messages
    monkeypatch.setitem(sys.modules, "langflow", types.ModuleType("langflow"))
    monkeypatch.setitem(sys.modules, "langflow.memory", fake_memory)

    sf = FakeSnowflake()
    import lfx_snowflake.memories_lf_mem_ingest as ing

    monkeypatch.setattr(ing, "PIPELINE", LfMemIngestionPipeline(sf, background=False))
    flow_id, user_id = uuid.uuid4(), uuid.uuid4()

    async def owns(mb):  # emulates the stored Flow(id, user_id) row
        return mb.flow_id == flow_id and mb.user_id == user_id

    monkeypatch.setattr(ing, "_owns_flow", owns)
    from types import SimpleNamespace

    sf_mb = SimpleNamespace(kb_name=_spec().to_kb_name(), flow_id=flow_id, user_id=user_id, threshold=1,
                            preprocessing=False, preproc_model=None, preproc_instructions=None, preproc_kill_phrase="NO_INGEST")
    other = SimpleNamespace(kb_name="chroma_kb", flow_id=flow_id, user_id=user_id, threshold=1)
    asyncio.run(ingestion._maybe_trigger(mb=sf_mb, session_id="s1", job_id=None, get_or_create_session=None))
    asyncio.run(ingestion._maybe_trigger(mb=other, session_id="s1", job_id=None, get_or_create_session=None))
    assert native_calls == ["chroma_kb"]
    assert seen == {"session_id": "s1", "flow_id": flow_id, "order": "ASC", "user_id": user_id}
    assert ing.PIPELINE.status(MB_ID)["state"] == "INGESTED"
    assert sf.chunks[0]["text"] == "Preferisco l'italiano" and sf.chunks[0]["flow_id"] == str(flow_id)
    assert asyncio.run(capture_snowflake_memory_base(other, "s1")) is None


def test_bridge_recall_lf_mem_returns_score(monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID, LiveSnowflakeBridge

    bridge = LiveSnowflakeBridge()
    entry = {"name": "E2E_MB_A3_FACTS", "table_fqn": _spec().table_fqn, "lf_mem": _spec().to_dict(),
             "flow_id": "flow-1", "filter_by_session": False}
    monkeypatch.setattr(type(bridge), "memories_catalog", property(lambda self: [entry]))
    seen = {}

    def fake_exec(sql, *, bindings=None, mode=None, **_):
        seen.update(sql=sql, vals=_vals(bindings), mode=mode, guarded=guard.FLOW_COMPONENT_SQL.get(),
                    flow=CURRENT_SNOWFLAKE_FLOW_ID.get())
        return {"ok": True, "rows": [{"CHUNK_ID": "c", "TEXT": "italiano", "METADATA": "{}", "SESSION_ID": "s",
                                      "SENDER": "User", "SENDER_NAME": "U", "JOB_ID": "j", "INGESTED_AT": "t", "SCORE": 0.7}]}

    monkeypatch.setattr(bridge, "execute_sql", fake_exec)
    res = bridge.recall_memory("E2E_MB_A3_FACTS", query="che lingua preferisce?")
    assert res["ok"] is True and res["rows"][0]["_score"] == pytest.approx(0.7) and res["rows"][0]["content"] == "italiano"
    assert seen["mode"] == "caller" and seen["guarded"] is True and seen["flow"] == "flow-1"
    assert seen["vals"] == ["che lingua preferisce?", MB_ID, "flow-1"]  # Facts & preferences: cross-session
    entry["filter_by_session"] = True
    bridge.recall_memory("E2E_MB_A3_FACTS", query="q", session_id="s9")
    assert seen["vals"][-1] == "s9" and "session_id = ?" in seen["sql"]


def test_pending_count_uses_ingestion_cursor():
    sql, b = _spec().build_pending_request(MSG, flow_id="f", owner_user="ALICE")
    assert "metadata:source_last_created_at" in sql and _vals(b) == ["f", "ALICE", MB_ID, "f", "ALICE"]
    assert aggregate_memory_stats([], [{"SESSION_ID": "s", "PENDING_COUNT": 2}])["pending_count"] == 2


def test_create_applies_preset_filter_by_session(tmp_path, monkeypatch):
    import sqlite3

    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    user, flow = uuid.uuid4(), uuid.uuid4()
    db = tmp_path / "langflow.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE user (id TEXT PRIMARY KEY); CREATE TABLE flow (id TEXT PRIMARY KEY, user_id TEXT);"
        "CREATE TABLE memory_base (id TEXT PRIMARY KEY, name TEXT, flow_id TEXT, user_id TEXT, threshold INTEGER,"
        " auto_capture INTEGER, embedding_model TEXT, preprocessing INTEGER, preproc_model TEXT,"
        " preproc_instructions TEXT, preproc_kill_phrase TEXT, kb_name TEXT, created_at TEXT);"
    )
    conn.execute("INSERT INTO user VALUES (?)", (user.hex,))
    conn.execute("INSERT INTO flow VALUES (?, ?)", (flow.hex, user.hex))
    conn.commit()
    conn.close()
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": []})
    base = {"embedding_model": "snowflake-arctic-embed-l-v2.0", "database": "SNOWFLAKE_LANGFLOW", "memory_schema": "MEMORIES"}
    s = bridge.create_native_memory({**base, "name": "sess", "preset": "Session summary"}, flow_id=str(flow), user_id=str(user))
    f = bridge.create_native_memory({**base, "name": "facts", "preset": "Facts & preferences"}, flow_id=str(flow), user_id=str(user))
    assert s["memory"]["filter_by_session"] is True and f["memory"]["filter_by_session"] is False
    bad = bridge.create_native_memory({**base, "name": "x", "preset": "Invented"}, flow_id=str(flow), user_id=str(user))
    assert bad["error_code"] == "UNKNOWN_PRESET"


def test_installer_patches_native_auto_capture():
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "scripts" / "install_live_hooks_in_container.py").read_text()
    assert "_patch_upstream_memory_base_auto_capture()" in src
    assert "install_snowflake_auto_capture as _sf_capture" in src and '"langflow" / "services" / "memory_base" / "ingestion.py"' in src
