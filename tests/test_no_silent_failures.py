"""No node hides a Snowflake failure or invents a result (2026-11-15).

Before: a refused statement left the node green with zero rows; Cortex Analyst ran a placeholder query on the
flow-runs table when its call failed or returned no SQL; Knowledge returned an invented row when nothing matched.
"""
from __future__ import annotations

import pathlib
import re
from typing import Any

import pytest

from lfx_snowflake import fastapi_live_bridge as flb
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
from lfx_snowflake.native_category_components.files_and_knowledge import snowflake_canonical_fk as knowledge_mod

COMPONENTS = pathlib.Path(__file__).parent.parent / "src" / "lfx_snowflake" / "native_category_components"

# Calls allowed to continue after a failure, each with the reason.
ALLOWED_UNCHECKED = {
    ("processing/split_text.py", "res = _flow_component_execute_sql(sql_stmt)"): "falls back to the local splitter",
    ("files_and_knowledge/snowflake_cortex_search_service_creator.py", 'show_res = _flow_component_execute_sql(status_sql["show_sql"])'):
        "inspect status: raises only when both lookups fail",
    ("files_and_knowledge/snowflake_cortex_search_service_creator.py", 'desc_res = _flow_component_execute_sql(status_sql["describe_sql"])'):
        "inspect status: raises only when both lookups fail",
    ("files_and_knowledge/snowflake_cortex_search_stream_task.py", "hist_res = _flow_component_execute_sql(history_sql)"):
        "task history is informational",
    ("files_and_knowledge/snowflake_canonical_fk.py", "res = _flow_component_execute_sql(sql_stmt)"):
        "Read File: audited first, then raises on failure a few lines below",
    ("llm_operations/snowflake_custom_tool.py",
     "exec_res = _flow_component_execute_sql(sql_stmt, bindings=sql_bindings, timeout=timeout_s, warehouse=wh or None)"):
        "_execute_tool returns the raw result; every caller goes through _execute_tool_or_raise",
}
HANDLED_NEARBY = re.compile(r'if not \w+\.get\("ok"\)|\.get\("ok"\) is False|_or_raise|require_ok|not res\.get\("ok"\)')


def test_every_component_statement_is_checked() -> None:
    unchecked = []
    for path in sorted(COMPONENTS.glob("*/*.py")):
        lines = path.read_text().splitlines()
        rel = str(path.relative_to(COMPONENTS))
        for i, line in enumerate(lines):
            if "_flow_component_execute_sql(" not in line or line.lstrip().startswith(("def ", "#")):
                continue
            window = " ".join(lines[max(0, i - 1): i + 14])
            if (rel, line.strip()) in ALLOWED_UNCHECKED or HANDLED_NEARBY.search(window):
                continue
            unchecked.append(f"{rel}:{i + 1}: {line.strip()}")
    assert unchecked == []


# ------------------------------------------------------------------ Cortex Analyst
class _Resp:
    def __init__(self, status: int, body: dict[str, Any]):
        self.status_code, self._body, self.content, self.text = status, body, b"x", str(body)

    def json(self) -> dict[str, Any]:
        return self._body


def _analyst(monkeypatch, response: _Resp | Exception, sql_result: dict[str, Any] | None = None):
    bridge = LiveSnowflakeBridge()
    executed: list[str] = []

    class _Client:
        def __init__(self, *a: Any, **k: Any) -> None: ...
        def __enter__(self) -> "_Client":
            return self
        def __exit__(self, *a: Any) -> None: ...
        def post(self, *a: Any, **k: Any) -> _Resp:
            if isinstance(response, Exception):
                raise response
            return response

    class _Httpx:
        Client = _Client

    def _execute(sql: str, *a: Any, **k: Any) -> dict[str, Any]:
        executed.append(sql)
        return sql_result or {"ok": True, "rows": [{"N": 1}]}

    monkeypatch.setattr(flb, "httpx", _Httpx)
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: True)
    monkeypatch.setattr(bridge, "_get_spcs_headers", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "execute_sql", _execute)
    res = bridge.invoke_cortex_analyst(database="DB", schema="SCH", semantic_source="SV", question="q")
    return res, executed


def test_analyst_refused_request_fails_and_runs_no_sql(monkeypatch) -> None:
    res, executed = _analyst(monkeypatch, _Resp(404, {"message": "Semantic view DB.SCH.SV does not exist"}))
    assert res["ok"] is False and res["error_code"] == "CORTEX_ANALYST_ERROR" and res["status_code"] == 404
    assert "does not exist" in res["message"] and res["rows"] == [] and executed == []


def test_analyst_unreachable_fails_and_runs_no_sql(monkeypatch) -> None:
    res, executed = _analyst(monkeypatch, RuntimeError("connection reset"))
    assert res["ok"] is False and res["error_code"] == "CORTEX_ANALYST_UNREACHABLE" and executed == []


def test_analyst_answer_without_sql_returns_no_rows_and_no_placeholder(monkeypatch) -> None:
    body = {"message": {"content": [{"type": "text", "text": "I cannot answer that with this data."},
                                    {"type": "suggestions", "suggestions": ["How many tickets are open?"]}]}}
    res, executed = _analyst(monkeypatch, _Resp(200, body))
    assert res["ok"] is True and res["answered_with_sql"] is False
    assert res["rows"] == [] and res["generated_sql"] == "" and executed == []
    assert res["interpretation"] == "I cannot answer that with this data."
    assert res["suggestions"] == ["How many tickets are open?"]


def test_analyst_generated_sql_that_fails_is_an_error(monkeypatch) -> None:
    body = {"message": {"content": [{"type": "sql", "statement": "SELECT X FROM T"}]}}
    res, executed = _analyst(monkeypatch, _Resp(200, body), {"ok": False, "message": "invalid identifier 'X'", "error_code": "SQL_ERROR"})
    assert executed == ["SELECT X FROM T"]
    assert res["ok"] is False and res["error_code"] == "SQL_ERROR" and "invalid identifier" in res["message"]


def test_analyst_without_token_is_not_configured(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: False)
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: pytest.fail("no SQL may run"))
    res = bridge.invoke_cortex_analyst(database="DB", schema="SCH", semantic_source="SV", question="q")
    assert res["ok"] is False and res["error_code"] == "NOT_CONFIGURED" and res["rows"] == []


def test_no_placeholder_statement_left_in_the_analyst_code() -> None:
    src = (COMPONENTS.parent / "fastapi_live_bridge.py").read_text()
    start = src.index("    def invoke_cortex_analyst(")
    body = src[start: src.index("    def _ensure_cortex_agent_threads_sqlite_table", start)]
    assert "runs_table" not in body and "interpreted question" not in body


# ------------------------------------------------------------------ Knowledge
def _knowledge(monkeypatch, result: dict[str, Any], **fields: Any):
    calls: list[str] = []

    def _execute(sql: str, *a: Any, **k: Any) -> dict[str, Any]:
        calls.append(sql)
        return result

    monkeypatch.setattr(knowledge_mod, "_flow_component_execute_sql", _execute)
    monkeypatch.setattr(knowledge_mod, "_register_component_object", lambda *a, **k: None)
    comp = knowledge_mod.SnowflakeKnowledgeComponent(
        database="DB", schema_name="SCH", knowledge_base="KB", embedding_model="snowflake-arctic-embed-m-v1.5", **fields)
    return comp, calls


def test_knowledge_retrieve_with_no_match_returns_no_rows(monkeypatch) -> None:
    comp, _ = _knowledge(monkeypatch, {"ok": True, "rows": []}, action="Retrieve", search_query="anything")
    assert comp.as_data().data["rows"] == []


def test_knowledge_retrieve_failure_raises(monkeypatch) -> None:
    comp, _ = _knowledge(monkeypatch, {"ok": False, "message": "Table 'KB' does not exist"}, action="Retrieve", search_query="q")
    with pytest.raises(ValueError, match="Knowledge retrieve failed: Table 'KB' does not exist"):
        comp.as_data()


def test_knowledge_ingest_failure_raises_instead_of_reporting_a_count(monkeypatch) -> None:
    comp, _ = _knowledge(monkeypatch, {"ok": False, "message": "Insufficient privileges"}, action="Ingest", input_data="a chunk")
    with pytest.raises(ValueError, match="Knowledge ingest .*Insufficient privileges"):
        comp.as_data()


def test_knowledge_ingest_without_text_stores_nothing(monkeypatch) -> None:
    comp, calls = _knowledge(monkeypatch, {"ok": True, "rows": []}, action="Ingest", input_data="")
    with pytest.raises(ValueError, match="no text received"):
        comp.as_data()
    assert calls == []


def test_custom_tool_never_calls_the_unchecked_executor_directly() -> None:
    src = (COMPONENTS / "llm_operations" / "snowflake_custom_tool.py").read_text()
    direct = [l.strip() for l in src.splitlines() if "self._execute_tool(" in l and "def " not in l]
    assert direct == ["res = self._execute_tool(arguments_override=arguments_override)"]  # inside _execute_tool_or_raise


def test_postgres_node_refuses_instead_of_returning_a_descriptor_row() -> None:
    from lfx_snowflake.native_category_components.data_source.snowflake_postgres import SnowflakePostgresComponent

    comp = SnowflakePostgresComponent(postgres_instance="PG", table_name="t", operation_mode="SELECT (Read)")
    with pytest.raises(ValueError, match="not available in this version. Nothing was read from PG.snowflake.public.t"):
        comp.run_postgres_operation()
    with pytest.raises(ValueError):
        comp.read_dataframe()
    # A write is refused too (by the privilege check or by the same message): never an "ok": true result.
    writer = SnowflakePostgresComponent(postgres_instance="PG", table_name="t", operation_mode="INSERT / MERGE (Write)")
    with pytest.raises(ValueError):
        writer.run_postgres_operation()
