"""Typed Snowflake memories are real tables that flows read and write; stage and volume creation report real outcomes."""

from __future__ import annotations

import json
from typing import Any

import pytest

from lfx_snowflake import memories_typed as typed
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

FQN = "SNOWFLAKE_LANGFLOW.CORE.MEM_OPS_EPISODES"


def _values(bindings: dict[str, dict[str, Any]] | None) -> list[str]:
    return [bindings[k]["value"] for k in sorted(bindings or {}, key=int)]


class _FakeSnowflake:
    """Records statements and answers SHOW TABLES from the comments of the tables created so far."""

    def __init__(self, *, fail_on: tuple[str, ...] = ()) -> None:
        self.statements: list[str] = []
        self.bindings: list[dict[str, Any] | None] = []
        self.tables: dict[str, str] = {}
        self.fail_on = fail_on

    def __call__(self, statement: str, *, bindings: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
        self.statements.append(statement)
        self.bindings.append(bindings)
        if any(token in statement for token in self.fail_on):
            return {"ok": False, "error_code": "SQL_ERROR", "message": "not allowed", "statement_handle": None}
        if statement.startswith("CREATE") and " TABLE " in statement and "COMMENT = '" in statement:
            name = statement.split("IF NOT EXISTS ")[1].split(" ")[0].split(".")[-1]
            self.tables[name] = statement.split("COMMENT = '")[1].rsplit("'", 1)[0].replace("''", "'").replace("\\\\", "\\")
        if statement.startswith("DROP TABLE"):
            self.tables.pop(statement.split(".")[-1], None)
        if statement.startswith("SHOW TABLES"):
            return {"ok": True, "rows": [{"name": n, "comment": c, "is_hybrid": "N"} for n, c in self.tables.items()]}
        return {"ok": True, "rows": [], "statement_handle": "01b-test"}


@pytest.fixture
def bridge(monkeypatch: pytest.MonkeyPatch) -> LiveSnowflakeBridge:
    b = LiveSnowflakeBridge()
    b.memories_registry.clear()
    b.database, b.schema = "SNOWFLAKE_LANGFLOW", "CORE"
    b.selected_context.update(
        {"database": "SNOWFLAKE_LANGFLOW", "schema": "CORE", "databases": ["SNOWFLAKE_LANGFLOW"],
         "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]}}
    )
    monkeypatch.setattr(b, "check_write_privilege_or_deny", lambda *a, **k: None)
    monkeypatch.setattr(b, "record_audit_event", lambda **k: "aud-1")
    return b


def _create(bridge: LiveSnowflakeBridge, **extra: Any) -> dict[str, Any]:
    return bridge.create_or_register_memory(
        {"name": "OPS_EPISODES", "agent_memory_type": "episodic_memory", "database": "SNOWFLAKE_LANGFLOW",
         "schema": "CORE", "table_type": "STANDARD", **extra}
    )


def test_comment_round_trip_and_foreign_comments_are_ignored() -> None:
    meta = {"name": "OPS", "canonical_memory_type": typed.EPISODIC, "capture_instructions": "Save it's outcome",
            "max_rows": 50, "managed": True}
    parsed = typed.parse_comment(typed.build_comment(meta))
    assert parsed and parsed["capture_instructions"] == "Save it's outcome" and parsed["max_rows"] == 50
    assert typed.parse_comment("plain business comment") is None
    assert typed.parse_comment('{"owner": "finance"}') is None
    assert typed.parse_comment(json.dumps({typed.MARKER: 1, "canonical_memory_type": "buffer_window_memory"})) is None


def test_create_provisions_table_with_metadata_and_cleanup_task(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake()
    monkeypatch.setattr(bridge, "execute_sql", sf)
    res = _create(bridge, capture_instructions="Save resolved incidents only.", retention_days=30, max_rows=200,
                  warehouse="COMPUTE_WH")
    assert res["ok"] is True and res["memory"]["snowflake_created"] is True
    create, task, resume = sf.statements[:3]
    assert create.startswith(f"CREATE TABLE IF NOT EXISTS {FQN} (")
    assert "DATA_RETENTION_TIME_IN_DAYS" not in create
    assert typed.parse_comment(sf.tables["MEM_OPS_EPISODES"])["capture_instructions"] == "Save resolved incidents only."
    assert task.startswith(f"CREATE OR REPLACE TASK {FQN}_CLEANUP WAREHOUSE = COMPUTE_WH SCHEDULE = 'USING CRON 0 3 * * * UTC' AS DELETE FROM {FQN}")
    assert "DATEADD(day, -30" in task and "r.rn > 200" in task
    assert " IN (SELECT" not in task  # Snowflake rejects IN (subquery) combined with OR
    assert resume == f"ALTER TASK {FQN}_CLEANUP RESUME"
    assert res["cleanup"]["ok"] is True and "warning" not in res


def test_failed_cleanup_task_is_reported_not_hidden(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "execute_sql", _FakeSnowflake(fail_on=("CREATE OR REPLACE TASK",)))
    res = _create(bridge, max_rows=10)
    assert res["ok"] is True
    assert res["cleanup"]["ok"] is False and "not allowed" in res["warning"]


def test_hybrid_failure_is_an_error_without_silent_standard_fallback(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake(fail_on=("CREATE HYBRID TABLE",))
    monkeypatch.setattr(bridge, "execute_sql", sf)
    res = bridge.create_or_register_memory(
        {"name": "RUN_STATE", "agent_memory_type": "graph_state_checkpointer", "database": "SNOWFLAKE_LANGFLOW",
         "schema": "CORE", "table_type": "HYBRID"}
    )
    assert res["ok"] is False and "Standard table" in res["message"]
    assert not any(s.startswith("CREATE TABLE") for s in sf.statements)
    assert bridge.memories_registry == []


def test_memories_are_rediscovered_from_snowflake_after_restart(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake()
    monkeypatch.setattr(bridge, "execute_sql", sf)
    _create(bridge, capture_instructions="Save resolved incidents only.")
    bridge.memories_registry.clear()  # process restart: the in-memory registry is gone
    assert bridge.discover_typed_memories(force=True) == 1
    entry = bridge.memories_registry[0]
    assert entry["name"] == "OPS_EPISODES" and entry["table_fqn"] == FQN
    assert entry["canonical_memory_type"] == typed.EPISODIC
    assert entry["capture_instructions"] == "Save resolved incidents only."
    assert bridge.discover_typed_memories(force=True) == 0  # no duplicates


def test_write_episode_and_checkpoint_statements(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake()
    monkeypatch.setattr(bridge, "execute_sql", sf)
    _create(bridge)
    bridge.create_or_register_memory(
        {"name": "RUN_STATE", "agent_memory_type": "graph_state_checkpointer", "database": "SNOWFLAKE_LANGFLOW",
         "schema": "CORE", "table_type": "STANDARD", "ttl_hours": 6}
    )
    ep = bridge.write_memory("OPS_EPISODES", content="Refund approved after manual review.", topic="refunds",
                             outcome_score=0.9, tool_used="OPERATIONS_AGENT", user_id="u1")
    assert ep["ok"] is True and ep["stored"] is True and ep["table_fqn"] == FQN
    assert sf.statements[-1].startswith(f"INSERT INTO {FQN} (chunk_id, user_id, memory_name, topic")
    vals = _values(sf.bindings[-1])
    assert "Refund approved after manual review." in vals and "refunds" in vals and "0.9" in vals
    assert "Refund approved" not in sf.statements[-1]  # content travels as a bind value

    ck = bridge.write_memory("RUN_STATE", content="plain text state", thread_id="t-1")
    assert ck["ok"] is True
    assert "COALESCE((SELECT MAX(step_id)" in sf.statements[-1] and "PARSE_JSON(?)" in sf.statements[-1]
    ck_vals = _values(sf.bindings[-1])
    assert json.dumps({"value": "plain text state"}) in ck_vals and "6" in ck_vals

    empty = bridge.write_memory("OPS_EPISODES", content="  ")
    assert empty["ok"] is False and empty["error_code"] == "INVALID_MEMORY_WRITE"
    missing = bridge.write_memory("NOT_THERE", content="x")
    assert missing["ok"] is False and missing["status_code"] == 404


def test_write_outside_flow_context_is_denied(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake()
    monkeypatch.setattr(bridge, "execute_sql", sf)
    _create(bridge)
    bridge.selected_context.update({"databases": ["OTHER_DB"], "schemas_by_db": {"OTHER_DB": ["PUBLIC"]}})
    before = len(sf.statements)
    res = bridge.write_memory("OPS_EPISODES", content="x")
    assert res["ok"] is False and res["error_code"] == "AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT"
    assert len(sf.statements) == before


def test_episodic_recall_uses_keywords_and_score_threshold() -> None:
    sql, bindings = typed.build_episodic_recall(FQN, query="How did we handle the refund dispute?", min_score=0.7, limit=3)
    assert "episode_text ILIKE ?" in sql and "outcome_score >= ?" in sql and sql.endswith("LIMIT 3")
    vals = _values(bindings)
    assert "%dispute%" in vals and "%refund%" in vals and "0.7" in vals
    assert "refund" not in sql
    recent_sql, recent_bindings = typed.build_episodic_recall(FQN)
    assert " WHERE " not in recent_sql and recent_bindings == {}


def test_cleanup_runs_the_retention_statement_on_demand(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake()
    monkeypatch.setattr(bridge, "execute_sql", sf)
    _create(bridge, max_rows=100)
    res = bridge.cleanup_memory("OPS_EPISODES")
    assert res["ok"] is True and res["statement"] == sf.statements[-1]
    assert sf.statements[-1].startswith(f"DELETE FROM {FQN} t USING (SELECT chunk_id, ROW_NUMBER() OVER")
    assert sf.statements[-1].endswith("WHERE t.chunk_id = r.chunk_id AND (r.rn > 100)")
    bridge.create_or_register_memory(
        {"name": "NOTES", "agent_memory_type": "summary_memory", "database": "SNOWFLAKE_LANGFLOW", "schema": "CORE",
         "table_type": "STANDARD"}
    )
    assert bridge.cleanup_memory("NOTES")["error_code"] == "NO_CLEANUP_POLICY"


def test_delete_drops_task_and_table_and_keeps_registration_when_drop_fails(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake()
    monkeypatch.setattr(bridge, "execute_sql", sf)
    _create(bridge, max_rows=10)
    sf.fail_on = ("DROP TABLE",)
    failed = bridge.delete_memory("OPS_EPISODES")
    assert failed["ok"] is False and len(bridge.memories_registry) == 1
    sf.fail_on = ()
    done = bridge.delete_memory("OPS_EPISODES")
    assert done["ok"] is True and bridge.memories_registry == []
    assert done["snowflake_statements"] == [f"DROP TASK IF EXISTS {FQN}_CLEANUP", f"DROP TABLE IF EXISTS {FQN}"]


def test_bound_table_is_validated_and_never_dropped(bridge, monkeypatch) -> None:
    sf = _FakeSnowflake(fail_on=("SELECT chunk_id",))
    monkeypatch.setattr(bridge, "execute_sql", sf)
    body = {"name": "LEGACY", "agent_memory_type": "episodic_memory", "storage_mode": "bind_existing",
            "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.PAST_CASES"}
    bad = bridge.create_or_register_memory(dict(body))
    assert bad["ok"] is False and "must exist and expose the columns" in bad["message"]
    assert bridge.memories_registry == []
    sf.fail_on = ()
    ok = bridge.create_or_register_memory(dict(body))
    assert ok["ok"] is True and ok["memory"]["table_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.PAST_CASES"
    assert any(s.startswith("COMMENT ON TABLE SNOWFLAKE_LANGFLOW.CORE.PAST_CASES IS '{") for s in sf.statements)
    bridge.delete_memory("LEGACY")
    assert not any(s.startswith("DROP TABLE") for s in sf.statements)
    assert sf.statements[-1] == "COMMENT ON TABLE SNOWFLAKE_LANGFLOW.CORE.PAST_CASES IS ''"


def test_failed_stage_and_volume_ddl_register_nothing(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "can_create_stage", lambda: True)
    monkeypatch.setattr(bridge, "can_create_external_volume", lambda: True)
    monkeypatch.setattr(bridge, "execute_sql", _FakeSnowflake(fail_on=("CREATE STAGE", "CREATE EXTERNAL VOLUME")))
    stage = bridge.create_stage_governed({"database": "SNOWFLAKE_LANGFLOW", "schema": "CORE", "name": "DOCS"})
    assert stage["ok"] is False and stage["status_code"] == 422 and stage["message"] == "not allowed"
    assert not any(s.get("name") == "DOCS" for s in bridge.stages_registry)
    vol = bridge.create_external_volume_governed(
        {"name": "ICE_VOL", "storage_provider": "S3", "storage_base_url": "s3://bucket/path/",
         "storage_aws_role_arn": "arn:aws:iam::000000000000:role/example"}
    )
    assert vol["ok"] is False and vol["status_code"] == 422 and vol["message"] == "not allowed"
    assert not any(v.get("name") == "ICE_VOL" for v in bridge.external_volumes_registry)


def test_checkpointer_cleanup_keeps_last_steps_and_expires_by_ttl() -> None:
    stmt = typed.build_cleanup_statement(typed.CHECKPOINTER, "D.S.MEM_RUN", {"keep_last_steps": 2})
    assert stmt.startswith("DELETE FROM D.S.MEM_RUN t USING (SELECT thread_id, checkpoint_ns, step_id, ROW_NUMBER() OVER")
    assert "PARTITION BY thread_id, checkpoint_ns ORDER BY step_id DESC" in stmt
    assert stmt.endswith("(t.created_at < DATEADD(hour, -COALESCE(t.ttl_hours, 24), CURRENT_TIMESTAMP()) OR r.rn > 2)")
    ttl_only = typed.build_cleanup_statement(typed.CHECKPOINTER, "D.S.MEM_RUN", {})
    assert ttl_only == (
        "DELETE FROM D.S.MEM_RUN t WHERE t.created_at < DATEADD(hour, -COALESCE(t.ttl_hours, 24), CURRENT_TIMESTAMP())"
    )
