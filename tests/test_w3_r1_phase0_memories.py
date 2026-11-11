"""Wave 3 Round 1 Phase 0: memory truth tests (AC-01, AC-11) for W3-R1-01/02/05.

Clean state must expose no seeded or invented memory data, and no memory path may report
success for a Snowflake operation it did not execute.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from lfx_snowflake.components.knowledge_and_memories import SnowflakeMemoryBaseComponent
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_memory_base import (
    SnowflakeMemoryBaseNativeComponent,
    _memory_base_options,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_message_history import (
    _buffer_window_memory_options,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src" / "lfx_snowflake"
JS_PATCH_PATH = REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js"

FORBIDDEN_SYNTHETIC_TOKENS: tuple[str, ...] = (
    "margin_q4_reconciliation",
    "ebitda_variance_bridge",
    "trivial_greeting_turn",
    "SNOWFLAKE_KB_MEM",
    "MEM_1_GRAPH_CHECKPOINTER",
    "MEM_2_CHAT_WINDOW",
    "MEM_3_SESSION_SUMMARY",
    "MEM_4_USER_ENTITY_PROFILE",
    "MEM_5_EPISODIC_FEWSHOT",
    "sf-job-",
)

# Files whose memory paths must not carry invented defaults. The explicit, opt-in showcase
# bootstrap (showcase_full_memory_agent.py / bootstrap_full_memory_showcase_flow) is out of scope.
STATIC_NO_DEFAULT_FILES: tuple[Path, ...] = (
    SRC / "native_category_components" / "files_and_knowledge" / "snowflake_memory_base.py",
    SRC / "native_category_components" / "models_and_agents" / "snowflake_message_history.py",
    SRC / "components" / "knowledge_and_memories.py",
    SRC / "components" / "utilities_a2a.py",
    JS_PATCH_PATH,
)


@pytest.fixture
def clean_bridge(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> LiveSnowflakeBridge:
    """Fresh bridge on an empty persist dir, offline (no service token), wired into the memory components."""
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: False)
    for mod in (
        "lfx_snowflake.fastapi_live_bridge",
        "lfx_snowflake.native_category_components.files_and_knowledge.snowflake_memory_base",
        "lfx_snowflake.native_category_components.models_and_agents.snowflake_message_history",
        "lfx_snowflake.components.knowledge_and_memories",
    ):
        monkeypatch.setattr(f"{mod}.LIVE_BRIDGE", bridge, raising=False)
    return bridge


def _register(bridge: LiveSnowflakeBridge, **overrides: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": "11111111-2222-3333-4444-555555555555",
        "memory_id": "11111111-2222-3333-4444-555555555555",
        "name": "REAL_TEST_MEM",
        "database": "SNOWFLAKE_LANGFLOW",
        "schema": "CORE",
        "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.REAL_TEST_MEM",
        "agent_memory_type": "episodic_session",
        "canonical_memory_type": "buffer_window_memory",
        "enable_embedding": False,
        "stored_records": [],
    }
    entry.update(overrides)
    bridge.memories_registry.append(entry)
    return entry


# ============================================================================
# AC-01: clean state is empty; no synthetic tokens; null instead of invented values
# ============================================================================
class TestAC01CleanStateNoSyntheticMemories:
    def test_fresh_bridge_has_no_seeded_memories(self, clean_bridge: LiveSnowflakeBridge) -> None:
        assert clean_bridge.memories_registry == []
        assert (clean_bridge.selected_context.get("visible_objects") or {}).get("memories") == []
        page = clean_bridge.list_native_memories(flow_id="clean-flow-001")
        assert page["items"] == [] and page["total"] == 0
        assert clean_bridge.get_memories_for_flow(flow_id="clean-flow-001") == []
        assert clean_bridge.get_memory_catalog_options(include_none=False) == []
        assert clean_bridge.get_memory_catalog_options(include_none=True) == ["None"]

    def test_no_forbidden_tokens_in_clean_payloads(self, clean_bridge: LiveSnowflakeBridge) -> None:
        payload = json.dumps(
            {
                "native": clean_bridge.list_native_memories(flow_id="clean-flow-001"),
                "flow": clean_bridge.get_memories_for_flow(flow_id="clean-flow-001"),
                "catalog": clean_bridge.get_memory_catalog_options(include_none=True),
                "mb_options": _memory_base_options(flow_id="clean-flow-001"),
                "mh_options": _buffer_window_memory_options(flow_id="clean-flow-001"),
                "context": clean_bridge.selected_context,
            },
            default=str,
        )
        for token in FORBIDDEN_SYNTHETIC_TOKENS:
            assert token not in payload, token

    @pytest.mark.parametrize("path", STATIC_NO_DEFAULT_FILES, ids=lambda p: p.name)
    def test_no_invented_memory_defaults_in_source(self, path: Path) -> None:
        text = path.read_text(encoding="utf-8")
        for token in ("SNOWFLAKE_KB_MEM", "margin_q4_reconciliation", "ebitda_variance_bridge", "sf-job-"):
            assert token not in text, f"{token} in {path.name}"

    def test_bridge_has_no_startup_seeding_or_demo_records(self) -> None:
        text = (SRC / "fastapi_live_bridge.py").read_text(encoding="utf-8")
        for token in (
            "_seed_default_showcase_memories",
            "_default_stored_records_for_memory",
            "SNOWFLAKE_KB_MEM",
            "margin_q4_reconciliation",
            "trivial_greeting_turn",
            "sf-job-",
        ):
            assert token not in text, token

    def test_recall_unknown_memory_returns_not_found_without_fabrication(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []
        monkeypatch.setattr(clean_bridge, "execute_sql", lambda s, **kw: calls.append(s) or {"ok": True, "rows": []})
        res = clean_bridge.recall_memory("NON_EXISTENT_MEM_XYZ", query="x")
        assert res["ok"] is False and res["recalled"] is False
        assert res["error_code"] == "MEMORY_NOT_FOUND_OR_LEGACY"
        assert res["rows"] == [] and res["query_id"] is None
        assert "MEM_NON_EXISTENT_MEM_XYZ" not in json.dumps(res)
        assert calls == []

    def test_recall_rejects_invalid_fqn_without_executing(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []
        monkeypatch.setattr(clean_bridge, "execute_sql", lambda s, **kw: calls.append(s) or {"ok": True, "rows": []})
        _register(clean_bridge, name="BAD_FQN_MEM", table_fqn="DB.SCH.T WHERE 1=1; DROP TABLE X")
        res = clean_bridge.recall_memory("BAD_FQN_MEM", query="q")
        assert res["ok"] is False and res["error_code"] == "INVALID_IDENTIFIER"
        assert calls == []

    def test_recall_binds_values_instead_of_interpolating(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[tuple[str, Any]] = []

        def _ok(statement: str, **kwargs: Any) -> dict[str, Any]:
            seen.append((statement, kwargs.get("bindings")))
            return {"ok": True, "rows": [{"sender": "User"}], "statement_handle": "01b2-real-qid"}

        monkeypatch.setattr(clean_bridge, "execute_sql", _ok)
        _register(clean_bridge)
        evil = "s1' OR '1'='1"
        res = clean_bridge.recall_memory("REAL_TEST_MEM", session_id=evil)
        assert res["ok"] is True and res["query_id"] == "01b2-real-qid"
        sql, binds = seen[0]
        assert evil not in sql and "?" in sql
        assert any(b["value"] == evil for b in binds.values())

    def test_recall_offline_fails_closed(self, clean_bridge: LiveSnowflakeBridge) -> None:
        _register(clean_bridge)
        res = clean_bridge.recall_memory("REAL_TEST_MEM", session_id="s1")
        assert res["ok"] is False and res["recalled"] is False
        assert res["error_code"] == "NOT_CONFIGURED"
        assert res["rows"] == [] and res["query_id"] is None

    def test_running_summary_is_empty_when_none_exists(self, clean_bridge: LiveSnowflakeBridge) -> None:
        comp = SnowflakeMemoryBaseNativeComponent(memory_base="NONE_EXISTENT")
        assert (comp.get_running_summary().text or "") == ""

    def test_native_messages_and_sessions_use_null_not_invented_values(
        self, clean_bridge: LiveSnowflakeBridge
    ) -> None:
        entry = _register(
            clean_bridge,
            stored_records=[
                {"session_id": "sess_real_1", "text": "real user message", "outcome_score": 0.96},
            ],
        )
        read_obj = clean_bridge._to_langflow_memory_base_read(entry)
        assert read_obj["embedding_model"] is None
        assert read_obj["last_generated_at"] is None
        assert read_obj["created_at"] is None

        item = clean_bridge.list_native_memory_messages("REAL_TEST_MEM")["items"][0]
        assert item["job_id"] is None
        assert item["timestamp"] is None
        assert item["ingested_at"] is None

        sess = clean_bridge.list_native_memory_sessions("REAL_TEST_MEM")["items"][0]
        assert sess["last_sync_at"] is None

    def test_empty_memory_has_no_default_session_or_messages(self, clean_bridge: LiveSnowflakeBridge) -> None:
        _register(clean_bridge)
        assert clean_bridge.list_native_memory_sessions("REAL_TEST_MEM")["items"] == []
        assert clean_bridge.list_native_memory_messages("REAL_TEST_MEM")["items"] == []
        assert clean_bridge.get_memory_detail("REAL_TEST_MEM")["stored_records"] == []

    def test_frontend_patch_refetches_external_volumes_and_has_no_kb_mem_default(self) -> None:
        js = JS_PATCH_PATH.read_text(encoding="utf-8")
        assert "SNOWFLAKE_KB_MEM" not in js
        assert "_stagesVolumesState.external_volumes.push(" not in js


# ============================================================================
# AC-11: no memory path claims success for an operation it did not execute
# ============================================================================
class TestAC11NoFakeMemorySuccess:
    def test_store_memory_returns_not_configured(self, clean_bridge: LiveSnowflakeBridge) -> None:
        res = SnowflakeMemoryBaseComponent(None, "TEST_MEM").store_memory("remember this", user_id="u1", session_id="s1")
        assert res["ok"] is False
        assert res["error_code"] == "NOT_CONFIGURED"
        assert res["ingested_count"] == 0

    def test_chat_history_adapter_does_not_succeed_in_ram(self, clean_bridge: LiveSnowflakeBridge) -> None:
        entry = _register(clean_bridge, name="TEST_MEM", table_fqn="SNOWFLAKE_LANGFLOW.CORE.TEST_MEM")
        adapter = SnowflakeMemoryBaseNativeComponent(memory_base="TEST_MEM").build_memory()
        with pytest.raises(RuntimeError, match="NOT_CONFIGURED"):
            adapter.add_user_message("test message")
        assert adapter._local_messages == []
        assert entry["stored_records"] == []

    def test_auto_capture_route_and_function_removed(self) -> None:
        text = (SRC / "fastapi_live_bridge.py").read_text(encoding="utf-8")
        assert '"/memories/auto-capture"' not in text
        assert "auto_capture_flow_run_memories" not in text

    def test_reload_ingest_turn_does_not_fake_ingestion(self, clean_bridge: LiveSnowflakeBridge) -> None:
        entry = _register(clean_bridge)
        res = clean_bridge.reload_memory_detail("REAL_TEST_MEM", {"ingest_turn": {"text": "hello world"}})
        assert res["ok"] is False and res["error_code"] == "NOT_CONFIGURED"
        assert entry["stored_records"] == []

    def test_execute_sql_offline_registers_nothing(self, clean_bridge: LiveSnowflakeBridge) -> None:
        before = json.dumps(clean_bridge.selected_context, sort_keys=True, default=str)
        res = clean_bridge.execute_sql("CREATE TABLE SNOWFLAKE_LANGFLOW.CORE.PHANTOM_T (ID INT)")
        assert res["ok"] is False and res["mode"] == "offline"
        assert res["error_code"] == "NOT_CONFIGURED"
        assert res["statement_handle"] is None
        assert json.dumps(clean_bridge.selected_context, sort_keys=True, default=str) == before

    def test_failed_memory_ddl_is_not_registered_nor_audited_as_allowed(
        self, clean_bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        audits: list[dict[str, Any]] = []
        monkeypatch.setattr(clean_bridge, "check_write_privilege_or_deny", lambda *a, **kw: None)
        monkeypatch.setattr(clean_bridge, "record_audit_event", lambda **kw: audits.append(kw) or "aud-1")
        monkeypatch.setattr(
            clean_bridge,
            "execute_sql",
            lambda *a, **kw: {"ok": False, "error_code": "SQL_ERROR", "message": "denied", "statement_handle": None},
        )
        res = clean_bridge.create_or_register_memory(
            {
                "name": "FAILING_MEM",
                "agent_memory_type": "buffer_window_memory",
                "database": "SNOWFLAKE_LANGFLOW",
                "schema": "CORE",
                "create_new_physical_store": True,
            }
        )
        assert res["ok"] is False
        assert clean_bridge.memories_registry == []
        create_audits = [a for a in audits if a.get("action") == "MEMORY_BASE_CREATE"]  # §7.4 (was MEMORY_CREATE_PHYSICAL)
        assert create_audits and all(a["outcome"] not in ("ALLOWED", "SUCCESS") for a in create_audits)
