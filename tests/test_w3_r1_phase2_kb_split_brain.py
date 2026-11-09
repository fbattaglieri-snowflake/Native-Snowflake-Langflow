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
