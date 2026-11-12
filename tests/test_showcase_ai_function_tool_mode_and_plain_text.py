"""AI Function Tool Mode and plain-text results of AI Complete / AI Redact / AI Function."""

from __future__ import annotations

import pytest

from lfx_snowflake.components.ai_function import plain_text_result
from lfx_snowflake.native_category_components.llm_operations import snowflake_ai_complete as complete_mod
from lfx_snowflake.native_category_components.llm_operations import snowflake_ai_function as function_mod
from lfx_snowflake.native_category_components.llm_operations import snowflake_ai_redact as redact_mod


def _rows(cell):
    return lambda *a, **k: {"ok": True, "rows": [{"RESULT": cell}]}


# --- plain_text_result ------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('"line 1\\nline \\"2\\""', 'line 1\nline "2"'),
        ('""', ""),
        ("already plain", "already plain"),
        ('{"labels": ["billing"]}', '{"labels": ["billing"]}'),
        ('"unterminated', '"unterminated'),
        ({"value": "x"}, {"value": "x"}),
        (None, None),
    ],
)
def test_plain_text_result_only_decodes_json_string_scalars(raw, expected) -> None:
    assert plain_text_result(raw) == expected


# --- AI Complete ------------------------------------------------------------------------------------
def _complete(**values):
    comp = complete_mod.SnowflakeAICompleteComponent()
    comp.model = "claude-sonnet-5-5"
    comp.prompt = "Summarise the case."
    for name, value in values.items():
        setattr(comp, name, value)
    return comp


def test_ai_complete_text_result_is_plain_text(monkeypatch) -> None:
    monkeypatch.setattr(complete_mod, "_flow_component_execute_sql", _rows('"- order 1005\\n- refund"'))
    assert _complete().run_ai_complete().text == "- order 1005\n- refund"


def test_ai_complete_json_object_result_is_kept(monkeypatch) -> None:
    monkeypatch.setattr(complete_mod, "_flow_component_execute_sql", _rows('{"invoice": "4711"}'))
    assert _complete(response_format_mode="JSON Schema Object").run_ai_complete().text == '{"invoice": "4711"}'


@pytest.mark.parametrize("cell", ['""', "", None, '"  "'])
def test_ai_complete_empty_completion_is_an_error(monkeypatch, cell) -> None:
    monkeypatch.setattr(complete_mod, "_flow_component_execute_sql", _rows(cell))
    with pytest.raises(ValueError, match="empty completion"):
        _complete(max_tokens=400).run_ai_complete()


# --- AI Redact --------------------------------------------------------------------------------------
def test_ai_redact_text_result_is_plain_text(monkeypatch) -> None:
    monkeypatch.setattr(redact_mod, "_flow_component_execute_sql", _rows('"Hi, I am [NAME] ([EMAIL])."'))
    comp = redact_mod.SnowflakeAIRedactComponent()
    comp.redact_mode = "redact"
    comp.source_mode = "Text Input"
    comp.input_text = "Hi, I am Maria Rossi (maria.rossi@example.com)."
    comp.return_error_details = False
    assert comp.run_ai_redact().text == "Hi, I am [NAME] ([EMAIL])."


# --- AI Function Tool Mode --------------------------------------------------------------------------
def _function(ai_function: str = "AI_CLASSIFY", categories: str = "refund,shipping"):
    comp = function_mod.SnowflakeAIFunctionComponent()
    comp.ai_function = ai_function
    comp.categories_or_schema = categories
    comp.return_as_json = True
    return comp


def test_ai_function_input_text_is_the_tool_mode_field() -> None:
    tool_fields = [i.name for i in function_mod.SnowflakeAIFunctionComponent.inputs if getattr(i, "tool_mode", False)]
    assert tool_fields == ["input_text"]


def test_ai_function_toolkit_runs_the_configured_function_on_the_llm_text(monkeypatch) -> None:
    seen: list[str] = []

    def fake_exec(stmt, *a, **k):
        seen.append(stmt)
        return {"ok": True, "rows": [{"RESULT": '{"labels": ["refund"]}'}]}

    monkeypatch.setattr(function_mod, "_flow_component_execute_sql", fake_exec)
    (tool,) = _function().to_toolkit()
    assert tool.name == "snowflake_ai_classify"
    assert "AI_CLASSIFY" in tool.description and "refund,shipping" in tool.description
    assert list(tool.args_schema.model_fields) == ["input_text"]
    out = tool.invoke({"input_text": "Please refund order 1005"})
    assert out == '{"labels": ["refund"]}'
    assert len(seen) == 1 and "AI_CLASSIFY" in seen[0] and "Please refund order 1005" in seen[0]


def test_ai_function_tool_name_follows_the_selected_function() -> None:
    (tool,) = _function("AI_SENTIMENT", "").to_toolkit()
    assert tool.name == "snowflake_ai_sentiment"


def test_ai_function_tool_refuses_an_empty_input(monkeypatch) -> None:
    monkeypatch.setattr(function_mod, "_flow_component_execute_sql", _rows("x"))
    (tool,) = _function().to_toolkit()
    with pytest.raises(ValueError, match="without input_text"):
        tool.invoke({"input_text": "  "})


def test_ai_function_text_result_is_plain_text(monkeypatch) -> None:
    monkeypatch.setattr(function_mod, "_flow_component_execute_sql", _rows('"Bonjour"'))
    comp = _function("AI_TRANSLATE", "")
    comp.return_as_json = False
    comp.input_text = "Hello"
    comp.target_lang = "fr"
    assert comp.run_ai_function().text == "Bonjour"
