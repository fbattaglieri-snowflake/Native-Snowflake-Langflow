"""A6 VETO (PASSO 7 part 2): LF_MEM Memory Bases isolated per flow, owner and session; hydrated after restart."""

from __future__ import annotations

import sqlite3
import types
import uuid

import pytest

from lfx_snowflake import identity as sf_identity
from lfx_snowflake.memories_lf_mem import LfMemSpec

BODY = {"name": "Shared name", "embedding_model": "snowflake-arctic-embed-l-v2.0", "threshold": 2,
        "database": "SNOWFLAKE_LANGFLOW", "memory_schema": "MEMORIES"}


@pytest.fixture
def world(tmp_path, monkeypatch):
    """langflow.db with alice (flows A1, A2) and bob (flow B1); a bridge whose DDL calls succeed."""
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    alice, bob = uuid.uuid4(), uuid.uuid4()
    a1, a2, b1 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    db = tmp_path / "langflow.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE user (id TEXT PRIMARY KEY, username TEXT, is_superuser INTEGER);"
        "CREATE TABLE flow (id TEXT PRIMARY KEY, name TEXT, user_id TEXT, updated_at TEXT);"
        "CREATE TABLE memory_base (id TEXT PRIMARY KEY, name TEXT, flow_id TEXT, user_id TEXT, threshold INTEGER,"
        " auto_capture INTEGER, embedding_model TEXT, preprocessing INTEGER, preproc_model TEXT,"
        " preproc_instructions TEXT, preproc_kill_phrase TEXT, kb_name TEXT, created_at TEXT,"
        " UNIQUE(user_id, name));"
    )
    conn.execute("INSERT INTO user VALUES (?, 'alice', 0)", (alice.hex,))
    conn.execute("INSERT INTO user VALUES (?, 'bob', 0)", (bob.hex,))
    for f, u in ((a1, alice), (a2, alice), (b1, bob)):
        conn.execute("INSERT INTO flow VALUES (?, 'f', ?, '2026-11-11')", (f.hex, u.hex))
    conn.commit()
    conn.close()
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    calls: list[dict] = []

    def fake_sql(sql, bindings=None, mode=None, **_k):
        calls.append({"sql": sql, "bindings": bindings or {}, "mode": mode})
        return {"ok": True, "rows": []}

    monkeypatch.setattr(bridge, "execute_sql", fake_sql)
    return types.SimpleNamespace(bridge=bridge, db=db, alice=str(alice), bob=str(bob), a1=str(a1), a2=str(a2),
                                 b1=str(b1), calls=calls, tmp_path=tmp_path)


def _names(page):
    return sorted((i["name"], i["flow_id"]) for i in page["items"])


def test_item1_list_filters_strictly_by_flow_and_owner(world):
    w = world
    ra = w.bridge.create_native_memory(dict(BODY), flow_id=w.a1, user_id=w.alice)
    rb = w.bridge.create_native_memory(dict(BODY), flow_id=w.b1, user_id=w.bob)
    assert ra["ok"] and rb["ok"] and ra["id"] != rb["id"]
    a_page = w.bridge.list_native_memories(flow_id=w.a1, user_id=w.alice)
    assert [i["id"] for i in a_page["items"]] == [ra["id"]]          # same name, other flow: not merged
    assert w.bridge.list_native_memories(flow_id=w.b1, user_id=w.bob)["items"][0]["id"] == rb["id"]
    assert w.bridge.list_native_memories(flow_id=w.a2, user_id=w.alice)["items"] == []   # other flow of alice
    assert w.bridge.list_native_memories(flow_id=w.a1, user_id=w.bob)["items"] == []     # bob cannot see alice's
    assert w.bridge.list_native_memories(flow_id=w.a1, user_id=None)["items"] == []      # anonymous: nothing


def test_item5_same_name_two_flows_no_overwrite_and_distinct_tables(world):
    w = world
    ra = w.bridge.create_native_memory(dict(BODY), flow_id=w.a1, user_id=w.alice)
    rb = w.bridge.create_native_memory(dict(BODY), flow_id=w.b1, user_id=w.bob)
    rows = sqlite3.connect(w.db).execute("SELECT id, flow_id, user_id, kb_name FROM memory_base").fetchall()
    assert len(rows) == 2 and {r[1] for r in rows} == {uuid.UUID(w.a1).hex, uuid.UUID(w.b1).hex}
    tables = {LfMemSpec.from_kb_name(r[3]).table_fqn for r in rows}
    assert len(tables) == 2
    ids = {m["memory_id"] for m in w.bridge.memories_registry if isinstance(m.get("lf_mem"), dict)}
    assert {ra["id"], rb["id"]} <= ids
    # the same user re-using the name in another flow must not hijack the first row (native UNIQUE(user_id, name))
    dup = w.bridge.create_native_memory(dict(BODY), flow_id=w.a2, user_id=w.alice)
    assert dup["ok"] is False and dup["status_code"] == 409
    still = sqlite3.connect(w.db).execute(
        "SELECT flow_id FROM memory_base WHERE user_id = ?", (uuid.UUID(w.alice).hex,)).fetchall()
    assert still == [(uuid.UUID(w.a1).hex,)]


def test_item3_registry_hydrated_from_sqlite_after_restart(world, monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    w = world
    ra = w.bridge.create_native_memory({**BODY, "preset": "Session summary"}, flow_id=w.a1, user_id=w.alice)
    fresh = LiveSnowflakeBridge()          # simulated restart: empty RAM registry
    assert not any(isinstance(m.get("lf_mem"), dict) for m in fresh.memories_registry)
    page = fresh.list_native_memories(flow_id=w.a1, user_id=w.alice)
    assert [i["id"] for i in page["items"]] == [ra["id"]]
    entry = fresh._find_memory_entry(ra["id"])
    assert entry["flow_id"] == fresh._normalize_flow_id(w.a1) and entry["filter_by_session"] is True
    assert entry["lf_mem"]["filter_by_session"] is True
    assert fresh.list_native_memories(flow_id=w.b1, user_id=w.bob)["items"] == []


def test_item2_messages_read_governed_table_bound_by_flow_owner_session(world, monkeypatch):
    w = world
    ra = w.bridge.create_native_memory(dict(BODY), flow_id=w.a1, user_id=w.alice)
    entry = w.bridge._find_memory_entry(ra["id"])
    entry["stored_records"] = [{"text": "RAM ONLY - must never be returned", "session_id": "s1"}]
    monkeypatch.setattr(sf_identity, "current_identity", lambda: types.SimpleNamespace(sf_user="ALICE_SF"))
    rows = [{"CHUNK_ID": "c1", "SESSION_ID": "s1", "SENDER": "User", "SENDER_NAME": "alice", "TEXT": "governed",
             "JOB_ID": "j1", "INGESTED_AT": "2026-11-11T09:00:00", "TOTAL": 1}]
    seen = []
    monkeypatch.setattr(w.bridge, "execute_sql",
                        lambda sql, bindings=None, mode=None, **k: seen.append((sql, bindings, mode)) or {"ok": True, "rows": rows})
    page = w.bridge.list_native_memory_messages(ra["id"], session_id="s1", user_id=w.alice)
    assert [i["text"] for i in page["items"]] == ["governed"] and page["total"] == 1
    sql, bindings, mode = seen[0]
    spec = LfMemSpec(**entry["lf_mem"])
    assert mode == "caller" and spec.quoted_fqn in sql and "?" in sql
    vals = [b["value"] for b in bindings.values()]
    assert ra["id"] in vals and w.bridge._normalize_flow_id(w.a1) in vals and "s1" in vals and "TRIM(UPPER(CURRENT_USER()))" in sql  # rr4: owner = writer identity
    assert "governed" not in sql and "ALICE_SF" not in sql            # values bound, never inlined
    # another user / no caller identity: fail closed, no SQL
    seen.clear()
    assert w.bridge.list_native_memory_messages(ra["id"], user_id=w.bob)["items"] == [] and seen == []
    monkeypatch.setattr(sf_identity, "current_identity", lambda: types.SimpleNamespace(sf_user=""))
    denied = w.bridge.list_native_memory_messages(ra["id"], user_id=w.alice)
    assert denied["ok"] is False and denied["error_code"] == "NO_CALLER_IDENTITY" and seen == []
    # backend failure is surfaced, never the RAM records
    monkeypatch.setattr(sf_identity, "current_identity", lambda: types.SimpleNamespace(sf_user="ALICE_SF"))
    monkeypatch.setattr(w.bridge, "execute_sql", lambda *a, **k: {"ok": False, "error_code": "SNOWFLAKE_ERROR"})
    err = w.bridge.list_native_memory_messages(ra["id"], user_id=w.alice)
    assert err["ok"] is False and err["items"] == []


@pytest.mark.parametrize("sess", [None, "", "default", "DEFAULT", "  "])
def test_item4_recall_fails_closed_without_session(world, sess):
    w = world
    ra = w.bridge.create_native_memory({**BODY, "preset": "Session summary"}, flow_id=w.a1, user_id=w.alice)
    entry = w.bridge._find_memory_entry(ra["id"])
    w.calls.clear()
    res = w.bridge._recall_lf_mem(entry, query="q", session_id=sess, limit=5)
    assert res["ok"] is False and res["error_code"] == "MISSING_SESSION_ID" and res["rows"] == [] and w.calls == []


def test_item4_two_sessions_recall_binds_its_own_session(world):
    w = world
    ra = w.bridge.create_native_memory({**BODY, "preset": "Session summary"}, flow_id=w.a1, user_id=w.alice)
    entry = w.bridge._find_memory_entry(ra["id"])
    for sess in ("s1", "s2"):
        w.calls.clear()
        res = w.bridge._recall_lf_mem(entry, query="q", session_id=sess, limit=5)
        assert res["ok"] is True and res["filter_by_session"] is True
        vals = [b["value"] for b in w.calls[0]["bindings"].values()]
        assert sess in vals and ({"s1", "s2"} - {sess}).isdisjoint(vals)
        assert "session_id = ?" in w.calls[0]["sql"]
