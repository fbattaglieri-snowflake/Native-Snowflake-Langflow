"""PASSO 7 (v0.8.6): native Memory Bases + Snowflake LF_MEM DB Provider, governed Message History, AI_EMBED catalog."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid

import pytest

from lfx_snowflake import flow_context_guard as guard
from lfx_snowflake import sql_safety
from lfx_snowflake.memories_lf_mem import (
    MEMORY_PRESETS,
    LfMemError,
    LfMemSpec,
    aggregate_memory_stats,
    is_kill_phrase,
    threshold_reached,
)

MB_ID = "0b2f5a1e-6f6a-4c55-9a5b-1b2a3c4d5e6f"
EXPECTED_AI_EMBED = {
    "snowflake-arctic-embed-m-v1.5": 768,
    "snowflake-arctic-embed-m": 768,
    "e5-base-v2": 768,
    "snowflake-arctic-embed-l-v2.0": 1024,
    "snowflake-arctic-embed-l-v2.0-8k": 1024,
    "multilingual-e5-large": 1024,
    "voyage-multilingual-2": 1024,
    "nv-embed-qa-4": 1024,
}


def _spec(**kw):
    base = dict(name="facts", memory_base_id=MB_ID, database="SNOWFLAKE_LANGFLOW", memory_schema="MEMORIES",
                embedding_model="snowflake-arctic-embed-l-v2.0", governance_schema="CORE")
    base.update(kw)
    return LfMemSpec(**base)


def _vals(bindings):
    return [b["value"] for _, b in sorted(bindings.items(), key=lambda kv: int(kv[0]))]


# --------------------------------------------------------------------------- BUG-MEM-EMB-01/02
def test_single_ai_embed_catalog_source_of_truth():
    from lfx_snowflake.components.knowledge_and_memories import DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG
    from lfx_snowflake.fastapi_live_bridge import EXTERNAL_EMBEDDING_PROVIDERS, OFFICIAL_SNOWFLAKE_CORTEX_AI_EMBED_MODELS
    from lfx_snowflake.providers.embeddings import (
        DEFAULT_CORTEX_EMBEDDING_MODELS,
        OFFICIAL_AI_EMBED_MODEL_DIMENSIONS,
        OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS,
    )

    assert sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS == EXPECTED_AI_EMBED
    assert OFFICIAL_AI_EMBED_MODEL_DIMENSIONS == EXPECTED_AI_EMBED
    assert OFFICIAL_SNOWFLAKE_CORTEX_AI_EMBED_MODELS == EXPECTED_AI_EMBED
    assert list(OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS) == list(EXPECTED_AI_EMBED)
    assert {m.model_id: m.dimension for m in DEFAULT_CORTEX_EMBEDDING_MODELS} == EXPECTED_AI_EMBED
    assert {k: v.dimension for k, v in DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG.items()} == EXPECTED_AI_EMBED
    cortex = EXTERNAL_EMBEDDING_PROVIDERS["snowflake_cortex"]
    ai_embed = {m["model_id"]: m["dimension"] for m in cortex["models"] if m["function"] == "AI_EMBED"}
    assert ai_embed == EXPECTED_AI_EMBED
    for bad in ("nomic-embed-text", "twelvelabs-marengo-embed-3-0"):
        assert bad not in sql_safety.AI_EMBED_MODELS and bad not in ai_embed and bad not in DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG
    # Marengo only through AI_MULTI_EMBED
    assert sql_safety.AI_MULTI_EMBED_MODELS == frozenset({"twelvelabs-marengo-embed-3-0"})
    assert [m["function"] for m in cortex["models"] if m["model_id"] == "twelvelabs-marengo-embed-3-0"] == ["AI_MULTI_EMBED"]
    with pytest.raises(sql_safety.SqlSafetyError):
        sql_safety.validate_embed_model("twelvelabs-marengo-embed-3-0")
    with pytest.raises(sql_safety.SqlSafetyError):
        sql_safety.validate_embed_model("nomic-embed-text")


# --------------------------------------------------------------------------- BUG-MEM-PROV-01: LF_MEM contract
def test_lf_mem_ddl_matches_sql_04_contract():
    spec = _spec()
    ddl = spec.build_ddl()
    suffix = spec.memory_base_id.replace("-", "")[:8].upper()
    assert spec.table_fqn == f"SNOWFLAKE_LANGFLOW.MEMORIES.LF_MEM_FACTS_{suffix}"
    assert ddl.startswith(f'CREATE TABLE IF NOT EXISTS "SNOWFLAKE_LANGFLOW"."MEMORIES"."LF_MEM_FACTS_{suffix}" (')
    for col in ("chunk_id VARCHAR(64) NOT NULL", "memory_base_id VARCHAR(64) NOT NULL", "flow_id VARCHAR(64) NOT NULL",
                "session_id VARCHAR(256)", "embedding VECTOR(FLOAT, 1024)",
                "owner_user VARCHAR(256) NOT NULL DEFAULT CURRENT_USER()",
                "owner_role VARCHAR(256) NOT NULL DEFAULT CURRENT_ROLE()", "job_id VARCHAR(64)",
                "ingested_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()", "deleted_at TIMESTAMP_TZ"):
        assert col in ddl
    assert ddl.endswith('WITH ROW ACCESS POLICY "SNOWFLAKE_LANGFLOW"."CORE"."LANGFLOW_MEMORY_RAP" ON (owner_user)')
    assert '"dimensions":1024' in ddl and f'"memory_base_id":"{MB_ID}"' in ddl
    assert "VECTOR(FLOAT, 768)" in _spec(embedding_model="e5-base-v2").build_ddl()


def test_lf_mem_ingest_binds_text_and_honours_no_ingest():
    spec = _spec()
    secret = "O'Reilly said: DROP TABLE x; --"
    sql, bindings, receipt = spec.build_ingest_request(
        [{"text": secret, "session_id": "s1", "sender": "User"}, {"text": "  no_ingest  "}, {"text": ""}],
        flow_id="flow-1", job_id="job-7",
    )
    assert secret not in sql and "DROP" not in sql
    assert "AI_EMBED('snowflake-arctic-embed-l-v2.0', ?)" in sql
    assert "CURRENT_USER(), CURRENT_ROLE()" in sql  # owner from the session, never the payload
    vals = _vals(bindings)
    assert vals[2] == "flow-1" and vals[3] == "s1" and vals[6] == secret and vals[8] == secret and vals[9] == "job-7"
    assert sql.count("?") == len(vals) == 10
    assert len(receipt["ingested_chunk_ids"]) == 1 and len(receipt["skipped_chunk_ids"]) == 2
    only_kill, _b, rec = spec.build_ingest_request([{"text": "NO_INGEST"}], flow_id="flow-1")
    assert only_kill is None and rec["skipped_chunk_ids"]
    with pytest.raises(LfMemError, match="MISSING_FLOW_ID"):
        spec.build_ingest_request([{"text": "x"}], flow_id="")


def test_kill_phrase_and_threshold_semantics_match_langflow():
    assert is_kill_phrase("NO_INGEST") and is_kill_phrase("\n no_ingest \n") and is_kill_phrase("blah\nNO_INGEST")
    assert not is_kill_phrase("NO_INGEST_PLEASE") and not is_kill_phrase("")
    assert is_kill_phrase("SKIP", "SKIP")
    assert not threshold_reached(4, 5) and threshold_reached(5, 5) and threshold_reached(1, 0)


@pytest.mark.parametrize("preset", ["Session summary", "Facts & preferences", "Task outcomes"])
def test_recall_with_score_for_all_presets(preset):
    cfg = MEMORY_PRESETS[preset]
    assert cfg["preproc_instructions"].strip()
    spec = _spec()
    sql, bindings = spec.build_recall_request(
        "what currency?", flow_id="flow-1", session_id="s1", filter_by_session=cfg["filter_by_session"], top_k=3
    )
    vals = _vals(bindings)
    assert "VECTOR_COSINE_SIMILARITY(embedding, AI_EMBED('snowflake-arctic-embed-l-v2.0', ?)) AS score" in sql
    assert "deleted_at IS NULL" in sql and "QUALIFY ROW_NUMBER() OVER (PARTITION BY chunk_id" in sql
    assert sql.endswith("ORDER BY score DESC LIMIT 3")
    assert vals[:3] == ["what currency?", MB_ID, "flow-1"]
    if cfg["filter_by_session"]:
        assert "AND session_id = ?" in sql and vals[3] == "s1"
    else:
        assert "session_id = ?" not in sql and len(vals) == 3
    rows = [{"CHUNK_ID": "c1", "TEXT": "EUR", "METADATA": '{"source":"chat"}', "SESSION_ID": "s1", "SENDER": "User",
             "SENDER_NAME": "U", "JOB_ID": "j1", "INGESTED_AT": "2026-11-11", "SCORE": 0.81}]
    out = LfMemSpec.recall_rows_to_results(rows)
    assert out[0]["content"] == "EUR" and out[0]["_score"] == pytest.approx(0.81) and out[0]["source"] == "chat"


def test_presets_filter_by_session_defaults():
    assert MEMORY_PRESETS["Session summary"]["filter_by_session"] is True
    assert MEMORY_PRESETS["Facts & preferences"]["filter_by_session"] is False
    assert set(MEMORY_PRESETS) == {"Session summary", "Facts & preferences", "Task outcomes"}


def test_filter_by_session_requires_session():
    with pytest.raises(LfMemError, match="MISSING_SESSION_ID"):
        _spec().build_recall_request("q", flow_id="f", session_id=None, filter_by_session=True)


@pytest.mark.parametrize(
    "kw",
    [
        {"name": "x; DROP TABLE t"},
        {"name": 'a"b'},
        {"name": ""},
        {"database": "DB.X"},
        {"memory_schema": "S; --"},
        {"embedding_model": "nomic-embed-text"},
        {"embedding_model": "x'), 1); DROP TABLE t; --"},
        {"memory_base_id": "not-a-uuid"},
    ],
)
def test_t12_adversarial_lf_mem_inputs_rejected(kw):
    with pytest.raises((LfMemError, sql_safety.SqlSafetyError, ValueError)):
        _spec(**kw)


def test_flow_context_guard_requires_lf_mem_in_context():
    spec = _spec()
    in_ctx = {"selected_objects": {"SNOWFLAKE_LANGFLOW.MEMORIES": {"memories": [spec.table_name]}}}
    ingest, _b, _r = spec.build_ingest_request([{"text": "hello"}], flow_id="f")
    recall, _rb = spec.build_recall_request("q", flow_id="f", session_id=None, filter_by_session=False)
    stats, _sb = spec.build_session_stats_request(flow_id="f", owner_user="ALICE")
    for stmt in (ingest, recall, stats):
        assert guard.check_statement(stmt, in_ctx, "SNOWFLAKE_LANGFLOW", "CORE")[0] is True
        allowed, _why, offending = guard.check_statement(stmt, {}, "SNOWFLAKE_LANGFLOW", "CORE")
        assert allowed is False and offending == [spec.table_fqn]


def test_kb_name_marker_roundtrip():
    spec = _spec()
    assert LfMemSpec.from_kb_name(spec.to_kb_name()) == spec
    assert LfMemSpec.from_kb_name("my_chroma_kb") is None


# --------------------------------------------------------------------------- BUG-MEM-API-01: governed stats
def test_stats_requests_bind_flow_and_owner_and_aggregate():
    spec = _spec()
    sql, b = spec.build_session_stats_request(flow_id="flow-1", owner_user="ALICE")
    assert _vals(b) == [MB_ID, "flow-1"] and "TRIM(UPPER(owner_user)) = TRIM(UPPER(CURRENT_USER()))" in sql
    assert "ALICE" not in sql and "owner_user = ?" not in sql
    psql, pb = spec.build_pending_request("SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_MESSAGES", flow_id="flow-1", owner_user="ALICE")
    assert _vals(pb) == ["flow-1", MB_ID, "flow-1"] and psql.count("?") == 3
    agg = aggregate_memory_stats(
        [{"SESSION_ID": "s1", "TOTAL_PROCESSED": 4, "LAST_SYNC_AT": "2026-11-10 10:00", "LAST_JOB_ID": "j9"}],
        [{"SESSION_ID": "s1", "PENDING_COUNT": 2}, {"SESSION_ID": "s2", "PENDING_COUNT": 3}],
    )
    assert agg["total_processed"] == 4 and agg["pending_count"] == 5 and agg["last_sync_at"] == "2026-11-10 10:00"
    assert {s["session_id"]: s["pending_count"] for s in agg["sessions"]} == {"s1": 2, "s2": 3}
    assert aggregate_memory_stats([], [])["last_sync_at"] is None


def test_memory_read_uses_governed_stats_and_user_id(monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    bridge = LiveSnowflakeBridge()
    calls = []

    def fake_exec(sql, *, bindings=None, mode=None, **_):
        calls.append((sql, _vals(bindings or {}), mode))
        if "pending_count" in sql:
            return {"ok": True, "rows": [{"SESSION_ID": "s1", "PENDING_COUNT": 1}]}
        return {"ok": True, "rows": [{"SESSION_ID": "s1", "TOTAL_PROCESSED": 7, "LAST_SYNC_AT": "2026-11-11T09:00:00Z", "LAST_JOB_ID": "j1"}]}

    monkeypatch.setattr(bridge, "execute_sql", fake_exec)
    entry = {"name": "facts", "flow_id": "flow-1", "lf_mem": _spec().to_dict(), "langflow_user_id": "u-1",
             "table_fqn": _spec().table_fqn, "enable_embedding": True}
    read = bridge._to_langflow_memory_base_read(entry)
    assert read["user_id"] == "u-1"
    assert read["total_processed"] == 7 and read["pending_count"] == 1 and read["last_sync_at"] == "2026-11-11T09:00:00Z"
    assert read["stats_status"] == "OK" and read["sessions_count"] == 1
    assert all(mode == "caller" for _s, _v, mode in calls) and all("flow-1" in v for _s, v, _m in calls)
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": False, "mode": "offline", "rows": []})
    offline = bridge._to_langflow_memory_base_read(entry)
    assert offline["total_processed"] is None and offline["stats_status"] == "NOT_CONFIGURED"


# --------------------------------------------------------------------------- BUG-MEM-MB-01: native registration
def _langflow_db(tmp_path, user_hex, flow_hex, flow_owner_hex=None):
    db = tmp_path / "langflow.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE user (id TEXT PRIMARY KEY, username TEXT, is_superuser INTEGER);"
        "CREATE TABLE flow (id TEXT PRIMARY KEY, name TEXT, user_id TEXT, updated_at TEXT);"
        "CREATE TABLE memory_base (id TEXT PRIMARY KEY, name TEXT, flow_id TEXT, user_id TEXT, threshold INTEGER,"
        " auto_capture INTEGER, embedding_model TEXT, preprocessing INTEGER, preproc_model TEXT,"
        " preproc_instructions TEXT, preproc_kill_phrase TEXT, kb_name TEXT, created_at TEXT);"
    )
    su = uuid.uuid4().hex
    conn.execute("INSERT INTO user VALUES (?, 'admin', 1)", (su,))  # a superuser that must NOT be picked
    conn.execute("INSERT INTO user VALUES (?, 'alice', 0)", (user_hex,))
    conn.execute("INSERT INTO flow VALUES (?, 'f', ?, '2026-11-11')", (flow_hex, flow_owner_hex or user_hex))
    conn.execute("INSERT INTO flow VALUES (?, 'newer', ?, '2027-01-01')", (uuid.uuid4().hex, su))
    conn.commit()
    conn.close()
    return db


def test_native_memory_base_row_uses_caller_user_and_explicit_flow(tmp_path, monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge, MemoryBasePersistenceError

    user, flow = uuid.uuid4(), uuid.uuid4()
    db = _langflow_db(tmp_path, user.hex, flow.hex)
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    entry = {"name": "facts", "lf_mem": _spec().to_dict(), "threshold": 4, "auto_capture": True,
             "llm_preprocessing": {"enabled": True, "kill_phrase": "NO_INGEST"}}
    bridge._persist_memory_in_langflow_sqlite(entry, flow_id=str(flow), user_id=str(user))
    row = sqlite3.connect(db).execute("SELECT name, flow_id, user_id, threshold, kb_name, preprocessing, embedding_model FROM memory_base").fetchone()
    assert row[:4] == ("facts", flow.hex, user.hex, 4)
    assert LfMemSpec.from_kb_name(row[4]) == _spec() and row[5] == 1 and row[6] == "snowflake-arctic-embed-l-v2.0"
    assert entry["flow_id"] == str(flow) and entry["langflow_user_id"] == str(user)

    with pytest.raises(MemoryBasePersistenceError) as no_user:
        bridge._persist_memory_in_langflow_sqlite({"name": "x"}, flow_id=str(flow), user_id=None)
    assert no_user.value.http_status == 401
    with pytest.raises(MemoryBasePersistenceError) as no_flow:
        bridge._persist_memory_in_langflow_sqlite({"name": "x"}, flow_id=None, user_id=str(user))
    assert no_flow.value.http_status == 422
    with pytest.raises(MemoryBasePersistenceError) as unknown_flow:
        bridge._persist_memory_in_langflow_sqlite({"name": "x"}, flow_id=str(uuid.uuid4()), user_id=str(user))
    assert unknown_flow.value.http_status == 404


def test_flow_of_another_user_is_rejected(tmp_path, monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge, MemoryBasePersistenceError

    user, flow = uuid.uuid4(), uuid.uuid4()
    _langflow_db(tmp_path, user.hex, flow.hex, flow_owner_hex=uuid.uuid4().hex)
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    with pytest.raises(MemoryBasePersistenceError) as exc:
        LiveSnowflakeBridge()._persist_memory_in_langflow_sqlite({"name": "x"}, flow_id=str(flow), user_id=str(user))
    assert exc.value.http_status == 403


def test_create_native_memory_lf_mem_end_to_end(tmp_path, monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    user, flow = uuid.uuid4(), uuid.uuid4()
    db = _langflow_db(tmp_path, user.hex, flow.hex)
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    body = {"name": "Facts prefs", "embedding_model": "snowflake-arctic-embed-l-v2.0", "threshold": 3,
            "database": "SNOWFLAKE_LANGFLOW", "memory_schema": "MEMORIES"}

    assert bridge.create_native_memory(dict(body), user_id=str(user))["error_code"] == "MISSING_FLOW_ID"
    assert bridge.create_native_memory(dict(body), flow_id=str(flow))["status_code"] == 401
    bad = bridge.create_native_memory({**body, "embedding_model": "nomic-embed-text"}, flow_id=str(flow), user_id=str(user))
    assert bad["status_code"] == 422 and bad["error_code"] == "UNSUPPORTED_EMBEDDING_MODEL"

    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": False, "mode": "offline", "rows": []})
    off = bridge.create_native_memory(dict(body), flow_id=str(flow), user_id=str(user))
    assert off["ok"] is False and off["error_code"] == "NOT_CONFIGURED"
    assert sqlite3.connect(db).execute("SELECT COUNT(*) FROM memory_base").fetchone()[0] == 0

    ddls = []
    monkeypatch.setattr(bridge, "execute_sql", lambda sql, **k: ddls.append((sql, k.get("mode"))) or {"ok": True, "rows": []})
    res = bridge.create_native_memory(dict(body), flow_id=str(flow), user_id=str(user))
    assert res["ok"] is True and res["status_code"] == 201
    assert ddls[0][0].startswith('CREATE TABLE IF NOT EXISTS "SNOWFLAKE_LANGFLOW"."MEMORIES"."LF_MEM_FACTS_PREFS_')
    assert ddls[0][1] == "caller"
    row = sqlite3.connect(db).execute("SELECT name, flow_id, user_id, kb_name, threshold FROM memory_base").fetchone()
    assert row[0] == "Facts prefs" and row[1] == flow.hex and row[2] == user.hex and row[4] == 3
    spec = LfMemSpec.from_kb_name(row[3])
    assert spec.table_fqn == "SNOWFLAKE_LANGFLOW.MEMORIES.LF_MEM_FACTS_PREFS_" + spec.memory_base_id.replace("-", "")[:8].upper()
    ctx = bridge.get_flow_context(bridge._normalize_flow_id(str(flow)), create=False)
    recall_sql, _rb = spec.build_recall_request("q", flow_id=str(flow), session_id=None, filter_by_session=False)
    assert guard.check_statement(recall_sql, ctx, "SNOWFLAKE_LANGFLOW", "CORE")[0] is True

    new_flow = uuid.uuid4()
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO flow VALUES (?, 'f2', ?, '2026-11-11')", (new_flow.hex, user.hex))
    conn.commit()
    conn.close()
    monkeypatch.setattr(bridge, "check_write_privilege_or_deny", lambda *a, **k: None)  # role check covered elsewhere
    patched = bridge.patch_native_memory(res["id"], {"flow_id": str(new_flow), "threshold": 9}, user_id=str(user))
    assert patched["ok"] is True
    row2 = sqlite3.connect(db).execute("SELECT flow_id, threshold FROM memory_base").fetchone()
    assert row2 == (new_flow.hex, 9)


# --------------------------------------------------------------------------- native MemoryBaseComponent hook
class _FakeNativeMemoryBase:
    """Shape of Langflow 1.13.2 MemoryBaseComponent's two resolution hooks."""

    memory_base = "facts"

    async def _build_backend(self, owner, owner_username, kb_name):
        return ("native", kb_name)

    async def _additional_model_provider_policy_ids(self, purpose, parameters=None):
        return ("openai",)


def test_native_memory_base_hook_routes_lf_mem_to_snowflake(monkeypatch):
    from types import SimpleNamespace

    from lfx_snowflake import native_memory_base_hook as hook

    cls = type("MB", (_FakeNativeMemoryBase,), {})
    assert hook.install_snowflake_memory_base_hook(cls) is True
    assert hook.install_snowflake_memory_base_hook(cls) is False  # idempotent
    comp = cls()
    comp.graph = SimpleNamespace(flow_id="flow-1", session_id="s1")
    spec = _spec()

    assert asyncio.run(comp._build_backend(None, "u", "chroma_kb")) == ("native", "chroma_kb")
    backend = asyncio.run(comp._build_backend(None, "u", spec.to_kb_name()))
    assert isinstance(backend, hook.SnowflakeLfMemBackend) and backend.spec == spec

    seen = {}

    class _Bridge:
        def execute_sql(self, sql, *, bindings=None, mode=None, **_):
            from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

            seen.update(sql=sql, vals=_vals(bindings), mode=mode, guarded=guard.FLOW_COMPONENT_SQL.get(),
                        flow=CURRENT_SNOWFLAKE_FLOW_ID.get())
            return {"ok": True, "rows": [{"CHUNK_ID": "c1", "TEXT": "prefers EUR", "METADATA": "{}", "SESSION_ID": "s1",
                                          "SENDER": "User", "SENDER_NAME": "U", "JOB_ID": "j", "INGESTED_AT": "t", "SCORE": 0.9}]}

    backend.bridge = _Bridge()
    results = asyncio.run(backend.similarity_search("currency", k=2, filter={"session_id": "s1"}, with_scores=True))
    assert results[0][0].page_content == "prefers EUR" and results[0][1] == pytest.approx(0.9)
    assert seen["mode"] == "caller" and seen["guarded"] is True and seen["flow"] == "flow-1"
    assert seen["vals"] == ["currency", MB_ID, "flow-1", "s1"]
    cross = asyncio.run(backend.similarity_search("currency", k=2, filter=None))
    assert len(cross) == 1 and seen["vals"] == ["currency", MB_ID, "flow-1"]

    async def fake_kb(component, selected):
        return spec.to_kb_name() if selected == "facts" else "chroma_kb"

    monkeypatch.setattr(hook, "_selected_kb_name", fake_kb)
    assert asyncio.run(comp._additional_model_provider_policy_ids("use", {"memory_base": "facts"})) == ()
    assert asyncio.run(comp._additional_model_provider_policy_ids("use", {"memory_base": "other"})) == ("openai",)


def test_backend_failure_raises_not_empty(monkeypatch):
    from lfx_snowflake import native_memory_base_hook as hook

    backend = hook.SnowflakeLfMemBackend(_spec(), flow_id="flow-1", bridge=type("B", (), {
        "execute_sql": lambda self, *a, **k: {"ok": False, "mode": "offline", "error_code": "NOT_CONFIGURED"}})())
    with pytest.raises(LfMemError, match="NOT_CONFIGURED"):
        backend.search_sync("q", k=1, filter=None)


def test_installer_patches_upstream_memory_retrieval():
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "scripts" / "install_live_hooks_in_container.py").read_text()
    assert "_patch_upstream_memory_base_component()" in src
    assert "install_snowflake_memory_base_hook(MemoryBaseComponent)" in src.replace("_sf_mb_hook(MemoryBaseComponent)", "install_snowflake_memory_base_hook(MemoryBaseComponent)")


def test_no_superuser_or_recent_flow_fallback_in_persistence():
    from pathlib import Path

    import lfx_snowflake.fastapi_live_bridge as br

    body = Path(br.__file__).read_text().split("def _persist_memory_in_langflow_sqlite", 1)[1].split("\n    def ", 1)[0]
    assert "is_superuser" not in body and "ORDER BY updated_at" not in body and "507000000000" not in body
    assert "except Exception:  # noqa: BLE001\n            pass" not in body


# --------------------------------------------------------------------------- BUG-MEM-MH-01
def _history(fake, flow="flow-A", session="sess-1"):
    from lfx_snowflake.components.governed_message_history import GovernedMessageHistory

    return GovernedMessageHistory(fake, flow_id=flow, session_id=session, native_api=None)


def test_governed_history_keyed_by_flow_session_and_owner():
    from _test_fake_messages import FakeGovernedMessagesBridge

    fake = FakeGovernedMessagesBridge(object())
    h = _history(fake)
    for i in range(5):
        h.store(f"m{i}", sender="User" if i % 2 == 0 else "AI")
    _history(fake, flow="flow-B").store("other flow")
    _history(fake, session="sess-2").store("other session")
    assert [m["text"] for m in h.retrieve(3)] == ["m2", "m3", "m4"]
    fake.current_user = "BOB"
    assert h.retrieve(10) == []  # RAP: another Snowflake user sees nothing
    sql, bindings, mode = fake.calls[0]
    assert sql.startswith('INSERT INTO "SNOWFLAKE_LANGFLOW"."CORE"."LANGFLOW_MESSAGES"') and mode == "caller"
    assert "m0" not in sql and "owner_user" not in sql.split("SELECT")[0]


def test_governed_history_fails_closed_and_requires_ids():
    from lfx_snowflake.components.governed_message_history import GovernedMessageHistory, MessageHistoryError

    class _Offline:
        def _configured_messages_table(self):
            return "SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_MESSAGES"

        def execute_sql(self, *a, **k):
            return {"ok": False, "mode": "offline"}

    with pytest.raises(MessageHistoryError, match="NOT_CONFIGURED"):
        GovernedMessageHistory(_Offline(), flow_id="f", session_id="s", native_api=None).retrieve()
    with pytest.raises(MessageHistoryError, match="MISSING_FLOW_ID"):
        GovernedMessageHistory(_Offline(), flow_id="", session_id="s", native_api=None)
    with pytest.raises(MessageHistoryError, match="MISSING_SESSION_ID"):
        GovernedMessageHistory(_Offline(), flow_id="f", session_id="", native_api=None)


def test_native_message_history_component_uses_graph_ids_and_no_demo_defaults(monkeypatch):
    from types import SimpleNamespace

    from _test_fake_messages import FakeGovernedMessagesBridge

    import lfx_snowflake.native_category_components.models_and_agents.snowflake_message_history as mh

    assert not hasattr(mh, "_SHARED_HISTORY_BACKENDS")
    defaults = {i.name: getattr(i, "value", None) for i in mh.SnowflakeMessageHistoryNativeComponent.inputs}
    assert defaults["session_id"] == "" and defaults["user_id"] == "" and defaults["project_id"] == ""
    src = open(mh.__file__).read()
    assert "fin-reconcile-session-001" not in src and "fin_analyst_01" not in src

    fake = FakeGovernedMessagesBridge(mh.LIVE_BRIDGE)
    monkeypatch.setattr(mh, "LIVE_BRIDGE", fake)
    graph = SimpleNamespace(flow_id="flow-graph", session_id="graph-sess")
    for i in range(4):
        comp = mh.SnowflakeMessageHistoryNativeComponent(mode="Store", message_text=f"turn {i}", window_k=3)
        comp.graph = graph
        stored = json.loads(comp.store_message().text)
    assert stored["flow_id"] == "flow-graph" and stored["session_id"] == "graph-sess"
    assert [m["text"] for m in stored["messages"]] == ["turn 1", "turn 2", "turn 3"]  # Store returns window_k
    other = mh.SnowflakeMessageHistoryNativeComponent(mode="Retrieve", window_k=10)
    other.graph = SimpleNamespace(flow_id="flow-other", session_id="graph-sess")
    assert json.loads(other.retrieve_chat_history().text)["messages"] == []


def test_domain_store_returns_session_window():
    from lfx_snowflake.components.message_history import (
        InMemoryMessageHistoryBackend,
        MessageHistoryBackendType,
        MessageHistoryStorageConfig,
        SnowflakeMessageHistoryComponent,
    )

    backend = InMemoryMessageHistoryBackend(active_role="R", snowflake_privileges={"R": {("INSERT", "*"), ("SELECT", "*")}})
    comp = SnowflakeMessageHistoryComponent(
        backend, MessageHistoryStorageConfig(MessageHistoryBackendType.SNOWFLAKE_STANDARD_TABLE, "DB", "SCH")
    )
    for i in range(4):
        out = comp.store_message(message_id=f"m{i}", user_id="u", project_id="p", session_id="s", sender="User",
                                 sender_name="U", visible_text=f"t{i}", window_k=2)
    msgs = out.get("Messages") or out.get("messages")
    assert [m.get("visible_text") or m.get("text") for m in msgs] == ["t2", "t3"]
