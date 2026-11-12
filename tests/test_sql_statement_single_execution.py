"""SQL Statement: the statement runs once per run even when several output ports are used."""

from __future__ import annotations

from lfx_snowflake.native_category_components.llm_operations import snowflake_sql_statement as mod


def _component(monkeypatch):
    calls = []

    def fake_execute(sql, *, bindings=None, **_):
        calls.append(sql)
        return {"ok": True, "rows": [{"N": len(calls)}]}

    monkeypatch.setattr(mod, "_flow_component_execute_sql", fake_execute)
    comp = mod.SnowflakeSQLStatementComponent()
    comp.sql_statement = "CALL P(?)"
    comp.bind_parameters_json = '["x"]'
    return comp, calls


def test_two_output_ports_share_one_execution(monkeypatch) -> None:
    comp, calls = _component(monkeypatch)
    comp._pre_run_setup()
    comp.run_dataframe()
    message = comp.run_json_message()
    assert len(calls) == 1
    assert '"N": 1' in message.text


def test_next_run_executes_again(monkeypatch) -> None:
    comp, calls = _component(monkeypatch)
    comp._pre_run_setup()
    comp.run_json_message()
    comp._pre_run_setup()
    comp.run_json_message()
    assert len(calls) == 2
