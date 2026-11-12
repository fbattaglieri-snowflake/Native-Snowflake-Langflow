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


def test_duplicate_name_is_refused_before_any_ddl_and_default_gate_model(tmp_path, monkeypatch) -> None:
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
    bridge = LiveSnowflakeBridge()
    statements: list[str] = []

    def fake_execute(sql, *_a, **_k):
        statements.append(sql)
        return {"ok": True, "rows": []}

    monkeypatch.setattr(bridge, "execute_sql", fake_execute)
    body = {"name": "episodes", "preset": "Task outcomes", "preprocessing": True, "database": "APP_DB",
            "memory_schema": "MEMORIES", "embedding_model": "snowflake-arctic-embed-l-v2.0"}

    first = bridge.create_native_memory(dict(body), flow_id=str(flow), user_id=str(user))
    assert first["ok"] and first["memory"]["llm_preprocessing"]["model"] == "claude-haiku-5-5"
    created = len(statements)

    second = bridge.create_native_memory(dict(body), flow_id=str(flow), user_id=str(user))
    assert second["status_code"] == 409 and second["error_code"] == "MEMORY_BASE_NAME_TAKEN"
    assert len(statements) == created  # no CREATE TABLE for the refused duplicate


def test_delete_evaluates_the_role_of_the_owning_flow(monkeypatch) -> None:
    from lfx_snowflake import fastapi_live_bridge as mod

    bridge = LiveSnowflakeBridge()
    seen: list[str] = []
    monkeypatch.setattr(bridge, "_find_memory_entry", lambda _t: {"name": "M", "flow_id": "flow-1", "table_fqn": "A.B.C"})
    monkeypatch.setattr(bridge, "_delete_memory", lambda _t: seen.append(mod.CURRENT_SNOWFLAKE_FLOW_ID.get()) or {"ok": True})

    assert bridge.delete_memory("M") == {"ok": True}
    assert seen == ["flow-1"] and not mod.CURRENT_SNOWFLAKE_FLOW_ID.get()
