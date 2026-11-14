"""Regression of 2026-11-14: parallel memory nodes after a restart, and a stored Cortex Agent kept on refresh."""

from __future__ import annotations

from typing import Any

import pytest

from lfx_snowflake import memories_typed as typed
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
from lfx_snowflake.native_category_components.models_and_agents import snowflake_cortex_agent as agent_mod

DB, SCH = "SNOWFLAKE_LANGFLOW", "CORE"


@pytest.fixture
def bridge(monkeypatch: pytest.MonkeyPatch) -> LiveSnowflakeBridge:
    b = LiveSnowflakeBridge()
    b.memories_registry.clear()
    b.database, b.schema = DB, SCH
    b.selected_context.update({"database": DB, "schema": SCH, "databases": [DB], "schemas_by_db": {DB: [SCH]}})
    monkeypatch.setattr(b, "is_memory_visible_in_context", lambda *a, **k: True)
    return b


def _entry(name: str) -> dict[str, Any]:
    meta = {"name": name, "canonical_memory_type": typed.EPISODIC, "managed": True}
    return typed.entry_from_table(DB, SCH, f"{typed.TABLE_PREFIX}{name}", meta, is_hybrid=False)


def _another_node_registers_first(bridge: LiveSnowflakeBridge, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Discovery finds nothing new for this caller because a parallel node appended the entry meanwhile."""

    def discover(flow_id: str | None = None, *, force: bool = False) -> int:
        bridge.memories_registry.append(_entry(name))
        return 0

    monkeypatch.setattr(bridge, "discover_typed_memories", discover)


def test_write_resolves_a_memory_registered_by_a_parallel_node(bridge, monkeypatch) -> None:
    _another_node_registers_first(bridge, monkeypatch, "TOUR_PROFILE")
    entry, err = bridge._resolve_typed_memory("TOUR_PROFILE", "flow-1")
    assert err is None and entry and entry["name"] == "TOUR_PROFILE"


def test_recall_resolves_a_memory_registered_by_a_parallel_node(bridge, monkeypatch) -> None:
    _another_node_registers_first(bridge, monkeypatch, "TOUR_PROFILE")
    monkeypatch.setattr(bridge, "get_memories_for_flow", lambda flow_id=None: list(bridge.memories_registry))
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": []})
    res = bridge.recall_memory("TOUR_PROFILE", flow_id="flow-1")
    assert res.get("error_code") != "MEMORY_NOT_FOUND_OR_LEGACY"


def _config(agent: str, schema: str = "AGENTS") -> dict[str, Any]:
    return {"database": {"value": "SALES_DB", "options": []}, "schema_name": {"value": schema, "options": []},
            "cortex_agent": {"value": agent, "options": []}}


@pytest.fixture
def component(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(agent_mod, "_resolve_flow_id", lambda *a, **k: "flow-1")
    monkeypatch.setattr(agent_mod, "_agent_databases", lambda flow_id=None: ["SALES_DB"])
    monkeypatch.setattr(agent_mod, "_agent_schemas", lambda db, flow_id=None: ["AGENTS", "OTHER"])
    monkeypatch.setattr(agent_mod, "_cortex_agents", lambda db, sch, flow_id=None: ["FIRST_AGENT", "SALES_AI"])
    return agent_mod.SnowflakeCortexAgentComponent()


def test_fully_qualified_stored_agent_is_kept_on_refresh(component) -> None:
    out = component.update_build_config(_config("SALES_DB.AGENTS.SALES_AI"), "x", "user_message")
    assert out["cortex_agent"]["value"] == "SALES_DB.AGENTS.SALES_AI"


def test_agent_no_longer_listed_is_kept_and_shown_not_swapped(component) -> None:
    out = component.update_build_config(_config("REVOKED_AGENT"), "x", "user_message")
    assert out["cortex_agent"]["value"] == "REVOKED_AGENT"
    assert "REVOKED_AGENT" in out["cortex_agent"]["options"]


def test_schema_no_longer_listed_is_kept(component) -> None:
    out = component.update_build_config(_config("SALES_AI", schema="HIDDEN"), "x", "user_message")
    assert out["schema_name"]["value"] == "HIDDEN"


def test_empty_agent_and_changed_schema_still_get_a_default(component) -> None:
    assert component.update_build_config(_config(""), "x", "user_message")["cortex_agent"]["value"] == "FIRST_AGENT"
    out = component.update_build_config(_config("ONLY_IN_OLD_SCHEMA"), "OTHER", "schema_name")
    assert out["schema_name"]["value"] == "OTHER" and out["cortex_agent"]["value"] == "FIRST_AGENT"
    kept = component.update_build_config(_config("SALES_AI"), "OTHER", "schema_name")
    assert kept["cortex_agent"]["value"] == "SALES_AI"
