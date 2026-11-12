"""A Memory Base lives in the configured DB.SCHEMA memory location even when the flow works on another database."""

from __future__ import annotations

import sqlite3
import uuid

from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge


def test_memory_database_comes_from_the_configured_memory_schema(tmp_path, monkeypatch) -> None:
    user, flow = uuid.uuid4(), uuid.uuid4()
    conn = sqlite3.connect(tmp_path / "langflow.db")
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
    monkeypatch.setenv("LANGFLOW_MEMORY_SCHEMA", "APP_DB.MEMORIES")
    monkeypatch.setenv("LANGFLOW_GOVERNANCE_SCHEMA", "APP_DB.MEMORIES")
    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "execute_sql", lambda *_a, **_k: {"ok": True, "rows": []})
    # The flow context points at a business database, not at the application database.
    monkeypatch.setattr(bridge, "get_flow_context", lambda *_a, **_k: {"database": "BUSINESS_DB", "schema": "DATA"})

    res = bridge.create_native_memory(
        {"name": "episodes", "preset": "Task outcomes", "embedding_model": "snowflake-arctic-embed-l-v2.0"},
        flow_id=str(flow), user_id=str(user),
    )

    assert res["ok"], res
    assert res["memory"]["table_fqn"].startswith("APP_DB.MEMORIES.LF_MEM_EPISODES_")
