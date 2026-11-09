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
    assert bridge.snowflake_kb_names_in_native_table(_db(ROWS)) == {"SF_MEM"}


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
    # lookup failure (None) blocks every targeted name -> fail closed
    assert "sf_names is None or" in g
    assert 'path.startswith("/api/v1/knowledge_bases")' in src
