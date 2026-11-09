"""W3-R1 Phase 2: knowledge_base writes stay compatible with Langflow 1.12.5 and 1.13.2 schemas."""

from __future__ import annotations

import sqlite3
import sys
import types

import pytest

from lfx_snowflake import fastapi_live_bridge as bridge

BASE_COLS = (
    "id TEXT PRIMARY KEY, name TEXT, user_id TEXT, model_selection TEXT, chunk_size INT, chunk_overlap INT, "
    "separator TEXT, column_config TEXT, backend_type TEXT NOT NULL DEFAULT 'sqlite', backend_config TEXT, "
    "chunks INT, words INT, characters INT, size_bytes INT, source_types TEXT, status TEXT, failure_reason TEXT, "
    "created_at TEXT, updated_at TEXT"
)
V113_EXTRA = (
    ", storage_generation INT NOT NULL DEFAULT 1, storage_state TEXT NOT NULL DEFAULT 'ready', "
    "active_migration_id TEXT"
)


def _row(name: str = "MEM", backend: str = "sqlite") -> dict:
    return {
        "id": "abc", "name": name, "user_id": "u", "model_selection": "{}", "chunk_size": 1000,
        "chunk_overlap": 200, "separator": None, "column_config": "[]", "backend_type": backend,
        "backend_config": "{}", "chunks": 0, "words": 0, "characters": 0, "size_bytes": 0,
        "source_types": "[]", "status": "ready", "failure_reason": None, "created_at": "t0", "updated_at": "t0",
    }


@pytest.mark.parametrize("extra", ["", V113_EXTRA])
def test_upsert_works_on_both_schemas(extra: str) -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE knowledge_base ({BASE_COLS}{extra})")
    cur = conn.cursor()
    bridge.upsert_sqlite_row_preserving_columns(cur, "knowledge_base", _row(), insert_only_columns=("created_at",))
    assert cur.execute("SELECT name FROM knowledge_base").fetchone()[0] == "MEM"


def test_upsert_preserves_unowned_storage_columns() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE knowledge_base ({BASE_COLS}{V113_EXTRA})")
    cur = conn.cursor()
    bridge.upsert_sqlite_row_preserving_columns(cur, "knowledge_base", _row())
    cur.execute(
        "UPDATE knowledge_base SET storage_generation = 3, storage_state = 'migrating', active_migration_id = 'm1', chunks = 7"
    )
    new = _row(name="MEM2")
    new["created_at"] = "t1"
    bridge.upsert_sqlite_row_preserving_columns(
        cur, "knowledge_base", new, insert_only_columns=("created_at", "chunks")
    )
    got = cur.execute(
        "SELECT name, storage_generation, storage_state, active_migration_id, created_at, chunks FROM knowledge_base"
    ).fetchone()
    assert got == ("MEM2", 3, "migrating", "m1", "t0", 7)


def test_upsert_rejects_bad_table_name() -> None:
    cur = sqlite3.connect(":memory:").cursor()
    with pytest.raises(ValueError):
        bridge.upsert_sqlite_row_preserving_columns(cur, "kb; DROP TABLE x", {"id": 1})


def test_native_backend_type_never_label(monkeypatch: pytest.MonkeyPatch) -> None:
    from enum import Enum

    class BackendType(str, Enum):
        CHROMA = "chroma"
        SQLITE = "sqlite"
        POSTGRES = "postgres"

    mod = types.ModuleType("lfx.base.knowledge_bases.backends.base")
    mod.BackendType = BackendType
    monkeypatch.setitem(sys.modules, "lfx.base.knowledge_bases.backends.base", mod)
    for key in bridge.SNOWFLAKE_VECTOR_STORE_BACKENDS:
        val = bridge.native_kb_backend_type_value(key)
        assert BackendType(val) == BackendType.SQLITE


def test_resolve_snowflake_backend_key() -> None:
    assert bridge.resolve_snowflake_backend_key({"vector_database": "Hybrid Table"}) == "snowflake_hybrid"
    assert bridge.resolve_snowflake_backend_key({"backend_type": "Snowflake · X (A.B.C)"}) == "snowflake_table"
    assert bridge.resolve_snowflake_backend_key({"backend_type": "snowflake_iceberg"}) == "snowflake_iceberg"


def test_creation_allowed_backends_frozenset_extended(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = types.ModuleType("langflow.schema.knowledge_base")
    schema._CREATION_ALLOWED_BACKENDS = frozenset({"sqlite"})
    lf = sys.modules.get("langflow") or types.ModuleType("langflow")
    lf_schema = sys.modules.get("langflow.schema") or types.ModuleType("langflow.schema")
    monkeypatch.setitem(sys.modules, "langflow", lf)
    monkeypatch.setitem(sys.modules, "langflow.schema", lf_schema)
    monkeypatch.setitem(sys.modules, "langflow.schema.knowledge_base", schema)
    monkeypatch.setattr(lf, "schema", lf_schema, raising=False)
    monkeypatch.setattr(lf_schema, "knowledge_base", schema, raising=False)
    bridge.register_snowflake_vector_store_backends()
    allowed = schema._CREATION_ALLOWED_BACKENDS
    assert isinstance(allowed, frozenset)
    assert "sqlite" in allowed and "snowflake_table" in allowed


def test_bridge_no_longer_uses_insert_or_replace_for_knowledge_base() -> None:
    import inspect

    src = inspect.getsource(bridge)
    assert "INSERT OR REPLACE INTO knowledge_base" not in src
