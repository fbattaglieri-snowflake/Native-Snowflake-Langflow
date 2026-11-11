"""v0.8.6 PASSO 4: BUG-LLMOP-009 (AI Redact detect fail-closed) and BUG-LLMOP-001 (Decision from real AI_COMPLETE).

Payloads below are the exact shapes returned live by Snowflake on 2026-11-11.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

import lfx_snowflake.native_category_components.llm_operations.snowflake_ai_redact as redact_mod
import lfx_snowflake.native_category_components.llm_operations.snowflake_decision as dec_mod
from lfx_snowflake.components.ai_function import SnowflakeAIRedactEngine
from lfx_snowflake.components.llm_operations import SnowflakeDecisionEngine
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_redact import SnowflakeAIRedactComponent
from lfx_snowflake.native_category_components.llm_operations.snowflake_decision import SnowflakeDecisionComponent

TEXT_1 = "My name is John Smith and my email is john.smith@example.com"
# Live: AI_REDACT(input => TEXT_1, mode => 'detect') -> bare array (docs describe {"spans": [...]}).
LIVE_DETECT_ARRAY = (
    '[{"category":"NAME","end":21,"start":11,"text":"John Smith"},'
    '{"category":"EMAIL","end":60,"start":38,"text":"john.smith@example.com"}]'
)
TEXT_2 = "Call Mike at (415) 450-0973"
# Live: ... return_error_details => TRUE, mode => 'detect' -> {"value": [...], "error": null}.
LIVE_DETECT_WRAPPED = (
    '{"value":[{"category":"NAME","end":9,"start":5,"text":"Mike"},'
    '{"category":"PHONE_NUMBER","end":27,"start":13,"text":"(415) 450-0973"}],"error":null}'
)
PII = ("John Smith", "john.smith@example.com", "Mike", "(415) 450-0973", "450-0973")

# Live: AI_COMPLETE(model => 'snowflake-decision-fast', ..., return_error_details => TRUE).
LIVE_DECISION = json.dumps(
    {
        "value": {
            "answers": {
                "dept": {
                    "choice": "billing",
                    "confidence": 0.9690965356634291,
                    "probabilities": {"billing": 0.9968273177907793, "tech": 0.0031726822092206968},
                    "type": "choice",
                },
                "refund": {"noul": 0.904650538956921, "type": "noul"},
                "sev": {
                    "confidence": 0.22716801406333043,
                    "legend": {"0": "low", "1": "medium", "2": "high"},
                    "probabilities": {"0": 0.08718948485636767, "1": 0.2685625203030672, "2": 0.644247994840565},
                    "score": 1.5570585099841974,
                    "type": "score",
                },
            }
        },
        "error": None,
    }
)
QUESTIONS = {
    "dept": {"type": "choice", "instructions": "Which team?", "criteria": {"billing": "invoices", "tech": "bugs"}},
    "sev": {"type": "score", "instructions": "How severe?", "criteria": ["low", "medium", "high"]},
    "refund": {"type": "noul", "instructions": "Is a refund required?"},
}


# --------------------------------------------------------------------------- BUG-LLMOP-009
@pytest.mark.parametrize(
    "raw,text",
    [
        (LIVE_DETECT_ARRAY, TEXT_1),
        (json.loads(LIVE_DETECT_ARRAY), TEXT_1),
        ({"spans": json.loads(LIVE_DETECT_ARRAY)}, TEXT_1),  # documented shape
        (json.dumps({"spans": json.loads(LIVE_DETECT_ARRAY)}), TEXT_1),
        (LIVE_DETECT_WRAPPED, TEXT_2),
        (json.loads(LIVE_DETECT_WRAPPED), TEXT_2),
    ],
)
def test_bug_llmop_009_parses_all_real_detect_shapes(raw: Any, text: str) -> None:
    spans = SnowflakeAIRedactEngine.parse_detect_spans(raw, text)
    assert len(spans) == 2
    for sp in spans:
        assert text[sp["start"] : sp["end"]] == sp["text"]


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "not json",
        "{}",
        '{"value": null, "error": "AI_REDACT failed"}',
        '[{"category":"NAME","start":0}]',
        '[{"category":"NAME","start":5,"end":999,"text":"x"}]',
        '[{"category":"NAME","start":0,"end":4,"text":"Nope"}]',
        "[1, 2]",
        None,
    ],
)
def test_bug_llmop_009_parser_fails_closed(raw: Any) -> None:
    with pytest.raises(ValueError):
        SnowflakeAIRedactEngine.parse_detect_spans(raw, TEXT_2)


def test_bug_llmop_009_empty_array_means_no_pii_and_no_fabricated_span() -> None:
    assert SnowflakeAIRedactEngine.parse_detect_spans("[]", "nothing here") == []


def test_allowlist_splicing_merges_overlapping_spans() -> None:
    out = SnowflakeAIRedactEngine.apply_allowlist_splicing(
        "abcdefgh", [{"category": "A", "start": 1, "end": 5}, {"category": "B", "start": 3, "end": 7}], ""
    )
    assert out["redacted_text"] == "a[A/B]h"


def _redact_component(monkeypatch, mode: str, text: str, cell: Any, **extra: Any) -> SnowflakeAIRedactComponent:
    monkeypatch.setattr(redact_mod, "_flow_component_execute_sql", lambda *a, **k: {"ok": True, "rows": [{"RESULT": cell}]})
    comp = SnowflakeAIRedactComponent()
    comp.input_text = text
    comp.redact_mode = mode
    comp.return_error_details = False
    for k, v in extra.items():
        setattr(comp, k, v)
    return comp


@pytest.mark.parametrize("mode", ["detect", "detect_with_allowlist"])
def test_bug_llmop_009_component_never_forwards_pii(monkeypatch, mode: str) -> None:
    comp = _redact_component(monkeypatch, mode, TEXT_1, LIVE_DETECT_ARRAY)
    out = comp.run_ai_redact().text
    body = json.loads(out)
    for value in PII:
        assert value not in out
    assert body["redacted_text"] == "My name is [NAME] and my email is [EMAIL]"
    assert body["spans"] == [
        {"category": "NAME", "start": 11, "end": 21, "text": "[NAME]"},
        {"category": "EMAIL", "start": 38, "end": 60, "text": "[EMAIL]"},
    ]


def test_bug_llmop_009_allowlist_keeps_only_approved_values(monkeypatch) -> None:
    comp = _redact_component(monkeypatch, "detect_with_allowlist", TEXT_2, LIVE_DETECT_WRAPPED, allowlist_values="Mike")
    body = json.loads(comp.run_ai_redact().text)
    assert body["redacted_text"] == "Call Mike at [PHONE_NUMBER]"
    assert "450-0973" not in json.dumps(body)
    assert [s["text"] for s in body["retained_spans"]] == ["Mike"]


@pytest.mark.parametrize("mode", ["detect", "detect_with_allowlist"])
@pytest.mark.parametrize("cell", ["garbage", '{"spans": "x"}', '[{"category":"NAME","start":0,"end":2,"text":"zz"}]'])
def test_bug_llmop_009_component_fails_closed(monkeypatch, mode: str, cell: str) -> None:
    comp = _redact_component(monkeypatch, mode, TEXT_1, cell)
    with pytest.raises(ValueError) as exc:
        comp.run_ai_redact()
    for value in PII:
        assert value not in str(exc.value)


def test_bug_llmop_009_execution_error_payload_has_no_input_text(monkeypatch) -> None:
    monkeypatch.setattr(redact_mod, "_flow_component_execute_sql", lambda *a, **k: {"ok": False, "message": "boom"})
    comp = SnowflakeAIRedactComponent()
    comp.input_text = TEXT_1
    comp.redact_mode = "detect"
    comp.return_error_details = True
    out = comp.run_ai_redact().text
    assert "John Smith" not in out and "sql_preview" not in out


# --------------------------------------------------------------------------- BUG-LLMOP-001
def test_bug_llmop_001_parse_real_ai_complete_answers() -> None:
    answers = SnowflakeDecisionEngine.parse_decision_result(LIVE_DECISION)
    assert answers["dept"]["choice"] == "billing"
    assert SnowflakeDecisionEngine.parse_decision_result(json.loads(LIVE_DECISION)["value"]) == answers
    for bad in ('{"value": null, "error": "model failed"}', "{}", "nope", '{"value": {"answers": {}}}'):
        with pytest.raises(ValueError):
            SnowflakeDecisionEngine.parse_decision_result(bad)


def test_bug_llmop_001_routing_uses_service_confidence() -> None:
    res = SnowflakeDecisionEngine.evaluate_decision(
        state="double charged",
        questions=QUESTIONS,
        executor=lambda sql: {"ok": True, "rows": [{"DECISION": LIVE_DECISION}]},
    )
    assert res["answers_source"] == "snowflake_ai_complete"
    assert res["selected_branch"] == "billing"
    assert res["answers"]["sev"]["confidence"] == pytest.approx(0.22716801406333043)
    # Service confidence 0.227 < 0.70 -> low confidence (the formula would give 0.466; irrelevant here).
    assert res["high_confidence"] is False and res["route"] == "low_confidence"


def test_bug_llmop_001_execution_failure_never_falls_back_to_heuristics() -> None:
    for bad in ({"ok": False, "message": "denied"}, {"ok": True, "rows": []}, {"ok": True, "rows": [{"DECISION": "{}"}]}):
        with pytest.raises(Exception):
            SnowflakeDecisionEngine.evaluate_decision(state="x", questions=QUESTIONS, executor=lambda sql, b=bad: b)


def _decision_component(monkeypatch, cell: str = LIVE_DECISION) -> tuple[SnowflakeDecisionComponent, list[str]]:
    calls: list[str] = []

    def _exec(sql: str, **_k: Any) -> dict[str, Any]:
        calls.append(sql)
        return {"ok": True, "rows": [{"DECISION": cell}]}

    monkeypatch.setattr(dec_mod, "_flow_component_execute_sql", _exec)
    comp = SnowflakeDecisionComponent()
    comp.state_input = "Customer was double charged on the invoice and is furious"
    comp.questions_json = json.dumps(QUESTIONS)
    return comp, calls


def test_bug_llmop_001_all_ports_share_one_execution(monkeypatch) -> None:
    comp, calls = _decision_component(monkeypatch)
    main = comp.run_decision().data
    high = comp.route_high_confidence().data
    low = comp.route_low_confidence().data
    branch = comp.get_selected_branch().text
    audit = comp.get_audit_record().data
    blob = json.loads(comp.get_decision_json().text)
    assert len(calls) == 1
    assert main["answers"] == audit["answers"] == blob["answers"]
    assert branch == main["selected_branch"]
    assert high["active"] is not low["active"]


def test_bug_llmop_001_cache_refreshes_on_input_change_and_reset(monkeypatch) -> None:
    comp, calls = _decision_component(monkeypatch)
    comp.run_decision()
    comp.state_input = "Different ticket"
    comp.run_decision()
    assert len(calls) == 2
    comp._reset_decision_cache()
    comp.run_decision()
    assert len(calls) == 3


def test_bug_llmop_001_component_fails_closed(monkeypatch) -> None:
    comp, _ = _decision_component(monkeypatch, cell='{"value": null, "error": "quota"}')
    with pytest.raises(ValueError):
        comp.route_high_confidence()


def test_bug_llmop_001_runtime_path_does_not_call_offline_heuristic(monkeypatch) -> None:
    def _boom(*a: Any, **k: Any) -> Any:
        raise AssertionError("evaluate_questions_offline must not run at runtime")

    monkeypatch.setattr(SnowflakeDecisionEngine, "evaluate_questions_offline", classmethod(_boom))
    comp, _ = _decision_component(monkeypatch)
    assert comp.run_decision().data["selected_branch"] == "billing"
