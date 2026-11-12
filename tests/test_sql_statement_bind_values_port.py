"""SQL Statement: upstream values connected to Bind Values are bound to the ? placeholders, never inlined."""

from __future__ import annotations

from lfx_snowflake.native_category_components.llm_operations import snowflake_sql_statement as mod

Data, Message = mod.Data, mod.Message

HOSTILE = "it's a \"quoted\" value; DROP TABLE T; -- $$"


def _component(**attrs):
    comp = mod.SnowflakeSQLStatementComponent()
    for key, value in attrs.items():
        setattr(comp, key, value)
    return comp


def test_upstream_values_follow_the_static_bindings() -> None:
    comp = _component(bind_parameters_json='["first", 2]', bind_values=[Message(text=HOSTILE), Data(data={"k": 1})])
    assert comp._parse_bindings() == {
        "1": {"type": "TEXT", "value": "first"},
        "2": {"type": "FIXED", "value": "2"},
        "3": {"type": "TEXT", "value": HOSTILE},
        "4": {"type": "TEXT", "value": '{"k": 1}'},
    }


def test_single_upstream_message_and_no_values() -> None:
    assert _component(bind_parameters_json="{}", bind_values=Message(text="x"))._parse_bindings() == {
        "1": {"type": "TEXT", "value": "x"}
    }
    assert _component(bind_parameters_json="{}", bind_values=None)._parse_bindings() is None
    assert _component(bind_parameters_json="{}", bind_values="")._parse_bindings() is None


def test_bound_value_never_reaches_the_sql_text(monkeypatch) -> None:
    seen = {}

    def fake_execute(sql, *, bindings=None, **_):
        seen.update(sql=sql, bindings=bindings)
        return {"ok": True, "rows": [{"N": 1}]}

    monkeypatch.setattr(mod, "_flow_component_execute_sql", fake_execute)
    comp = _component(
        database="EXAMPLE_DB", schema_name="SALES", sql_statement="CALL <DB>.<SCHEMA>.SAVE_NOTE(?)",
        bind_parameters_json="{}", bind_values=[Message(text=HOSTILE)], output_format="DataFrame", max_rows=10,
    )
    comp.run_dataframe()
    assert seen["sql"] == "CALL EXAMPLE_DB.SALES.SAVE_NOTE(?)" and HOSTILE not in seen["sql"]
    assert seen["bindings"] == {"1": {"type": "TEXT", "value": HOSTILE}}
