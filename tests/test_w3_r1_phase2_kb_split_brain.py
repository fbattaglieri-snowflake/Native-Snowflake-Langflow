"""A8 Phase C VETO: Snowflake memories must never live in native `knowledge_base` (split brain)."""
import json
import sqlite3
from pathlib import Path

from lfx_snowflake import fastapi_live_bridge as bridge


def _db(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE knowledge_base (id TEXT PRIMARY KEY, name TEXT, backend_config TEXT)")
    conn.executemany("INSERT INTO knowledge_base VALUES (?,?,?)", rows)
    return conn


ROWS = [
    ("1", "SF_MEM", json.dumps({"mode": "snowflake", "table_fqn": "DB.S.T"})),
    ("2", "LOCAL_KB", json.dumps({"path": "/x"})),
    ("3", "NULL_CFG", None),
    ("4", "BAD_CFG", "{not json"),
]


def test_cleanup_removes_only_snowflake_rows_and_is_idempotent():
    conn = _db(ROWS)
    assert bridge.purge_snowflake_rows_from_native_knowledge_base(conn) == 1
    assert bridge.purge_snowflake_rows_from_native_knowledge_base(conn) == 0
    assert {r[0] for r in conn.execute("SELECT name FROM knowledge_base")} == {"LOCAL_KB", "NULL_CFG", "BAD_CFG"}


def test_cleanup_without_table_is_noop():
    assert bridge.purge_snowflake_rows_from_native_knowledge_base(sqlite3.connect(":memory:")) == 0


def test_snowflake_names_detection():
    assert bridge.snowflake_kb_names_in_native_table(_db(ROWS)) == {"SF_MEM", "1"}


def test_native_kb_target_names_routes():
    f = bridge.native_kb_target_names
    assert f("/api/v1/flows/x", "GET", None) is None
    assert f("/api/v1/knowledge_bases", "GET", None) == []
    assert f("/api/v1/knowledge_bases/connectors", "GET", None) == []
    for sub in ("", "/ingest", "/chunks", "/runs", "/runs/r1", "/cancel"):
        assert f(f"/api/v1/knowledge_bases/SF%5FMEM{sub}", "POST", None) == ["SF_MEM"]
    assert f("/api/v1/knowledge_bases", "DELETE", b'{"kb_names":["A","SF_MEM"]}') == ["A", "SF_MEM"]
    assert f("/api/v1/knowledge_bases/", "DELETE", b"garbage") == []


def test_registration_no_longer_writes_knowledge_base():
    src = Path(bridge.__file__).read_text()
    body = src.split("def _persist_memory_in_langflow_sqlite", 1)[1].split("\n    def ", 1)[0]
    assert '"knowledge_base",' not in body
    assert "UPDATE knowledge_base" not in body
    assert "purge_snowflake_rows_from_native_knowledge_base(conn)" in body


def test_guard_wired_fail_closed_with_409():
    src = Path(bridge.__file__).read_text()
    g = src.split("async def _guard_native_kb_request", 1)[1].split("\n        async def ", 1)[0]
    assert "status_code=409" in g and "SNOWFLAKE_KB_NOT_NATIVE" in g and "request_id" in g
    assert 'outcome="DENIED"' in g
    assert "native_kb_guard_decision(" in g
    assert '"/api/v1/knowledge-base-storage"))' in src


def test_guard_decision_behaviour():
    d = bridge.native_kb_guard_decision
    sf = {"SF_MEM", "ABCDEF01"}
    assert d("/api/v1/knowledge_bases/SF_MEM/ingest", "POST", None, sf) == "SF_MEM"
    assert d("/api/v1/knowledge_bases/sf_mem/chunks", "GET", None, sf) == "sf_mem"
    assert d("/api/v1/knowledge_bases/LOCAL_KB/ingest", "POST", None, sf) is None
    assert d("/api/v1/knowledge_bases", "DELETE", b'{"kb_names":["LOCAL","SF_MEM"]}', sf) == "SF_MEM"
    assert d("/api/v1/knowledge_bases", "DELETE", b'{"kb_names":["LOCAL"]}', sf) is None
    assert d("/api/v1/knowledge_bases", "GET", None, sf) is None
    # fail closed when the lookup failed
    assert d("/api/v1/knowledge_bases/LOCAL_KB", "DELETE", None, None) == "LOCAL_KB"
    assert d("/api/v1/flows/x", "GET", None, None) is None


def test_kb_storage_routes_guarded():
    d = bridge.native_kb_guard_decision
    sf = {"ABCDEF01"}
    assert d("/api/v1/knowledge-base-storage/pending-cleanup/abcd-ef01/retry", "POST", None, sf) == "abcdef01"
    assert d("/api/v1/knowledge-base-storage/attention/ABCDEF01/detach", "POST", None, sf) == "ABCDEF01"
    assert d("/api/v1/knowledge-base-storage/attention/OTHER/detach", "POST", None, sf) is None
    for p in ("/status", "/inventory", "/pending-cleanup", "/migrations", "/migrations/m1/retry"):
        assert d("/api/v1/knowledge-base-storage" + p, "GET", None, None) is None


def test_memory_delete_never_deletes_knowledge_base_by_name():
    src = Path(bridge.__file__).read_text()
    assert "DELETE FROM knowledge_base WHERE name" not in src


def test_cleanup_purges_legacy_seed_memory_kbs_and_storage_migrations():
    """Legacy 1.12 seed memories (`AGENT_MEMORY_STORE`, `MEM_1..5`) created via native POST /api/v1/memories
    had `backend_type='chroma'` and `backend_config=NULL`. On 1.13.1 alembic fences them into
    `storage_state='needs_attention'` with `error_code='legacy_source_missing'`, triggering the
    'Upgrading your data / Storage upgrade needs attention' modal. Verify they and their migration
    ledger rows are purged while legitimate local KBs and migrations stay untouched.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE knowledge_base ("
        "id TEXT PRIMARY KEY, name TEXT, user_id TEXT, backend_type TEXT, backend_config TEXT, "
        "source_types TEXT, storage_state TEXT, active_migration_id TEXT)"
    )
    conn.execute(
        "CREATE TABLE memory_base ("
        "id TEXT PRIMARY KEY, name TEXT, user_id TEXT, kb_name TEXT, preproc_model TEXT, "
        "preproc_instructions TEXT, embedding_model TEXT)"
    )
    conn.execute(
        "CREATE TABLE knowledge_base_storage_migration ("
        "id TEXT PRIMARY KEY, kb_id TEXT, phase TEXT, error_code TEXT)"
    )
    seed_rows = [
        ("a1cccf54-90d1-428f-a4e5-d702de82696f", "mem_a1cccf54", "AGENT_MEMORY_STORE", "15e03f01-20eb-4406-9d2d-83a349de478b"),
        ("4241e722-5e10-4f19-8d9d-1fb86c76c9e1", "mem_4241e722", "MEM_5_EPISODIC_FEWSHOT", "2d9d0df4-835d-434c-878c-6cdf277a06ab"),
        ("1d924ca0-624c-4bc0-a989-492e941f084a", "mem_1d924ca0", "MEM_4_USER_ENTITY_PROFILE", "e54af021-8cd1-4fca-a972-388b7fedb258"),
        ("eb8f94e1-9e21-4707-9eb1-53a5a42bc8a6", "mem_eb8f94e1", "MEM_3_SESSION_SUMMARY", "310bcf00-807b-42c0-910b-261ca0ddd95a"),
        ("9542f0a0-0644-44a6-ac08-0bcce7cd3184", "mem_9542f0a0", "MEM_2_CHAT_WINDOW", "adea5178-dac4-4771-b285-46dbf377455b"),
        ("2c9b1305-95b2-4ff3-bb9f-eb32a804540e", "mem_2c9b1305", "MEM_1_GRAPH_CHECKPOINTER", "09906b9d-6935-4a40-aada-673a6e394f10"),
    ]
    for kb_id, kb_name, mem_name, mig_id in seed_rows:
        conn.execute(
            "INSERT INTO knowledge_base VALUES (?,?,?,?,?,?,?,?)",
            (kb_id, kb_name, "u1", "chroma", None, "[]", "needs_attention", mig_id),
        )
        conn.execute(
            "INSERT INTO memory_base VALUES (?,?,?,?,?,?,?)",
            (f"mb_{kb_id[:8]}", mem_name, "u1", kb_name, "snowflake-sql", "Store: SNOWFLAKE_LANGFLOW.CORE.T", "No Embedding"),
        )
        conn.execute(
            "INSERT INTO knowledge_base_storage_migration VALUES (?,?,?,?)",
            (mig_id, kb_id.replace("-", ""), "needs_attention", "legacy_source_missing"),
        )
    # Legitimate local KB and its migration must survive
    conn.execute(
        "INSERT INTO knowledge_base VALUES (?,?,?,?,?,?,?,?)",
        ("local-kb-id-001", "USER_LOCAL_KB", "u1", "sqlite", json.dumps({"path": "/tmp/kb"}), "[]", "ready", "mig-local-001"),
    )
    conn.execute(
        "INSERT INTO knowledge_base_storage_migration VALUES (?,?,?,?)",
        ("mig-local-001", "local-kb-id-001", "complete", None),
    )
    conn.commit()

    purged = bridge.purge_snowflake_rows_from_native_knowledge_base(conn)
    assert purged == 6
    assert bridge.purge_snowflake_rows_from_native_knowledge_base(conn) == 0
    assert [r[0] for r in conn.execute("SELECT name FROM knowledge_base").fetchall()] == ["USER_LOCAL_KB"]
    assert [r[0] for r in conn.execute("SELECT name FROM memory_base").fetchall()] == []
    assert [r[0] for r in conn.execute("SELECT id FROM knowledge_base_storage_migration").fetchall()] == ["mig-local-001"]

