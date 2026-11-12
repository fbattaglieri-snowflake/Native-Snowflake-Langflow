"""Saving a flow [Context] keeps the LF_MEM table of every Memory Base attached to that flow."""

from __future__ import annotations

import uuid

from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
from lfx_snowflake.memories_lf_mem import LfMemSpec

FLOW, OTHER_FLOW = str(uuid.uuid4()), str(uuid.uuid4())


def _spec(name: str) -> LfMemSpec:
    return LfMemSpec(
        name=name, memory_base_id=str(uuid.uuid4()), database="EXAMPLE_DB", memory_schema="MEMORY",
        embedding_model="snowflake-arctic-embed-m-v1.5", filter_by_session=False,
    )


def _bridge(tmp_path, monkeypatch, specs: dict[str, LfMemSpec]) -> LiveSnowflakeBridge:
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "sync_flow_node_templates_sqlite", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "reconcile_triggers_with_flow_context", lambda *a, **k: [])
    bridge.memories_registry = [
        {"name": spec.name, "flow_id": flow_id, "lf_mem": spec.to_dict()} for flow_id, spec in specs.items()
    ]
    return bridge


def test_context_save_keeps_the_attached_memory_table(tmp_path, monkeypatch) -> None:
    mine, other = _spec("FACTS"), _spec("OTHER")
    bridge = _bridge(tmp_path, monkeypatch, {FLOW: mine, OTHER_FLOW: other})
    selection = {"EXAMPLE_DB.SALES": {"tables": ["ORDERS"]}}
    bridge.update_selected_context(
        {"database": "EXAMPLE_DB", "schema": "SALES", "databases": ["EXAMPLE_DB"],
         "schemas_by_db": {"EXAMPLE_DB": ["SALES"]}, "objects_by_db_schema": selection, "selected_objects": selection},
        flow_id=FLOW,
    )
    ctx = bridge.get_flow_context(FLOW, create=False)
    assert ctx["selected_objects"]["EXAMPLE_DB.MEMORY"]["memories"] == [mine.table_name]
    assert ctx["objects_by_db_schema"]["EXAMPLE_DB.MEMORY"]["memories"] == [mine.table_name]
    assert "MEMORY" in ctx["schemas_by_db"]["EXAMPLE_DB"]
    assert other.table_name not in str(ctx["selected_objects"])
    assert ctx["selected_objects"]["EXAMPLE_DB.SALES"]["tables"] == ["ORDERS"]
