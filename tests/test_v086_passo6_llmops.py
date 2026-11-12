"""v0.8.6 PASSO 6 — Custom Tool (LLMOP-005/006/007/013), Decision routing (002/003), LLMOP-008/010/011/012."""

from __future__ import annotations

import json
from typing import Any

import pytest

from lfx_snowflake import flow_context_guard as g
from lfx_snowflake.components.ai_function import SnowflakeAICompleteEngine, SnowflakeAIFunctionComponent
from lfx_snowflake.components.llm_operations import SnowflakeCustomToolEngine, SnowflakeSQLStatementEngine
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
from lfx_snowflake.native_category_components.llm_operations import snowflake_custom_tool as ct_mod
from lfx_snowflake.native_category_components.llm_operations import snowflake_decision as dec_mod
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_complete import _optional_number
from lfx_snowflake.native_category_components.llm_operations.snowflake_sql_statement import normalize_bindings

# Live DESCRIBE output shape (LF_E2E_TEST_20261110.E2E_A5_PROC).
DESCRIBE_ROWS = [
    {"property": "signature", "value": "(NAME VARCHAR, QTY NUMBER, FLAG BOOLEAN DEFAULT FALSE)"},
    {"property": "returns", "value": "VARCHAR"},
]
IDENT = "DB.SCH.E2E_A5_PROC(VARCHAR, NUMBER, BOOLEAN)"


def _params() -> list[dict[str, Any]]:
    return SnowflakeCustomToolEngine.parse_signature_parameters(IDENT, describe_rows=DESCRIBE_ROWS)


# --- BUG-LLMOP-005: real names from DESCRIBE, no SEND_EMAIL_ALL -------------------------------------
def test_005_names_and_types_come_from_describe() -> None:
    params = _params()
    assert [(p["name"], p["sql_type"], p["required"], p["named"]) for p in params] == [
        ("NAME", "VARCHAR", True, True),
        ("QTY", "NUMBER", True, True),
        ("FLAG", "BOOLEAN", False, True),
    ]


def test_005_unnamed_signature_stays_positional_without_invented_names() -> None:
    params = SnowflakeCustomToolEngine.parse_signature_parameters("DB.SCH.SEND_EMAIL_ALL(VARCHAR, VARCHAR, VARCHAR)")
    assert [p["name"] for p in params] == ["arg_1", "arg_2", "arg_3"]
    assert not any(p["named"] for p in params)


def test_005_no_send_email_all_fallback() -> None:
    assert ct_mod.DEFAULT_CUSTOM_TOOL_IDENTIFIER == ""
    comp = ct_mod.SnowflakeCustomToolComponent()
    comp.custom_tool_identifier = ""
    comp.parameters_json = ""
    assert comp._parsed_parameters() == []
    desc = LiveSnowflakeBridge().describe_custom_tool_signature(resource_type="procedure", identifier="")
    assert desc["ok"] is False and desc["error_code"] == "MISSING_IDENTIFIER"


# --- BUG-LLMOP-006: named syntax, optional parameters, bindings ------------------------------------
def test_006_named_args_order_independent_and_optional_skipped() -> None:
    sql, binds = SnowflakeCustomToolEngine.build_invocation_request(
        resource_type="procedure", custom_tool_identifier=IDENT, arguments={"qty": 2, "name": "x"}, parameters=_params()
    )
    assert sql == 'CALL DB.SCH.E2E_A5_PROC("NAME" => ?, "QTY" => ?::NUMBER);'
    assert binds == {"1": {"type": "TEXT", "value": "x"}, "2": {"type": "FIXED", "value": "2"}}


def test_006_named_function_and_udtf_shapes() -> None:
    params = [
        {"name": "x", "type": "integer", "required": True, "named": True},
        {"name": "y", "type": "number", "required": True, "named": True},
    ]
    sql, _ = SnowflakeCustomToolEngine.build_invocation_request(
        resource_type="function", custom_tool_identifier="DB.SCH.F(NUMBER, FLOAT)", arguments={"y": 1.5, "x": 2},
        parameters=params,
    )
    assert sql == 'SELECT DB.SCH.F("X" => ?::NUMBER, "Y" => ?::FLOAT) AS RESULT;'
    sql, _ = SnowflakeCustomToolEngine.build_invocation_request(
        resource_type="table_function", custom_tool_identifier="DB.SCH.U(NUMBER)", arguments=[3],
        parameters=[{"name": "n", "type": "integer", "named": True}],
    )
    assert sql == 'SELECT * FROM TABLE(DB.SCH.U("N" => ?::NUMBER));'


def test_006_missing_required_and_unknown_args_raise() -> None:
    with pytest.raises(ValueError, match="Missing required"):
        SnowflakeCustomToolEngine.build_invocation_request(
            custom_tool_identifier=IDENT, arguments={"name": "x"}, parameters=_params()
        )
    with pytest.raises(ValueError, match="Unknown"):
        SnowflakeCustomToolEngine.build_invocation_request(
            custom_tool_identifier=IDENT, arguments={"name": "x", "qty": 1, "evil": 1}, parameters=_params()
        )


def test_006_positional_cannot_skip_a_middle_optional() -> None:
    params = [
        {"name": "arg_1", "type": "string", "required": True, "named": False},
        {"name": "arg_2", "type": "string", "required": False, "named": False},
        {"name": "arg_3", "type": "string", "required": False, "named": False},
    ]
    sql, binds = SnowflakeCustomToolEngine.build_invocation_request(
        custom_tool_identifier="DB.SCH.P(VARCHAR, VARCHAR, VARCHAR)", arguments={"arg_1": "a"}, parameters=params
    )
    assert sql == "CALL DB.SCH.P(?);" and len(binds) == 1
    with pytest.raises(ValueError, match="positional"):
        SnowflakeCustomToolEngine.build_invocation_request(
            custom_tool_identifier="DB.SCH.P(VARCHAR, VARCHAR, VARCHAR)",
            arguments={"arg_1": "a", "arg_3": "c"},
            parameters=params,
        )


def test_006_injection_payload_only_travels_as_binding() -> None:
    sql, binds = SnowflakeCustomToolEngine.build_invocation_request(
        custom_tool_identifier=IDENT, arguments={"name": "x'); DROP TABLE T; --", "qty": 1}, parameters=_params()
    )
    assert "DROP" not in sql and binds["1"]["value"] == "x'); DROP TABLE T; --"


# --- BUG-LLMOP-013: dynamic args_schema -------------------------------------------------------------
def test_013_args_schema_reflects_real_parameters() -> None:
    model = ct_mod.build_args_schema("e2e_tool", _params())
    schema = model.model_json_schema()
    assert set(schema["properties"]) == {"NAME", "QTY", "FLAG"}
    assert set(schema["required"]) == {"NAME", "QTY"}
    assert model(NAME="a", QTY=2).FLAG is None


def test_013_to_toolkit_passes_schema_and_drops_omitted_optionals(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_exec(stmt, **kw):
        captured.update(stmt=stmt, **kw)
        return {"ok": True, "rows": []}

    monkeypatch.setattr(ct_mod, "_flow_component_execute_sql", fake_exec)
    comp = ct_mod.SnowflakeCustomToolComponent()
    comp.resource_type = "procedure"
    comp.custom_tool_identifier = IDENT
    comp.parameters_json = json.dumps(_params())
    comp.warehouse_mode = "Custom"
    comp.custom_warehouse = "COMPUTE_WH"
    comp.query_timeout = 300
    (tool,) = comp.to_toolkit()
    assert set(tool.args_schema.model_json_schema()["properties"]) == {"NAME", "QTY", "FLAG"}
    tool.invoke({"NAME": "a", "QTY": 2})
    assert captured["stmt"] == 'CALL DB.SCH.E2E_A5_PROC("NAME" => ?, "QTY" => ?::NUMBER);'
    # BUG-LLMOP-007: warehouse + timeout are request-level, no USE / ALTER SESSION
    assert captured["warehouse"] == "COMPUTE_WH" and captured["timeout"] == 300


def test_013_unexposable_parameter_name_rejected() -> None:
    with pytest.raises(ValueError):
        ct_mod.build_args_schema("t", [{"name": "bad name", "type": "string"}])


# --- BUG-LLMOP-007: SQL API body + async polling ----------------------------------------------------
class _Resp:
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status_code, self._body, self.content = status, body, b"x"

    def json(self) -> dict[str, Any]:
        return self._body


class _Client:
    def __init__(self, posts: list[_Resp], gets: list[_Resp]) -> None:
        self.posts, self.gets, self.sent, self.polled = posts, gets, [], []

    def post(self, url, headers=None, json=None, timeout=None):
        self.sent.append(json)
        return self.posts.pop(0)

    def get(self, url, headers=None, timeout=None):
        self.polled.append(url)
        return self.gets.pop(0)


def test_007_long_timeout_goes_async_and_polls_handle(monkeypatch) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    client = _Client(
        [_Resp(202, {"code": "333334", "statementHandle": "h1"})],
        [_Resp(202, {"code": "333334", "statementHandle": "h1"}), _Resp(200, {"data": [["ok"]]})],
    )
    status, body = LiveSnowflakeBridge._post_sql_statements(client, "u", {}, {"statement": "S"}, 65.0, max_wait=300)
    assert status == 200 and body == {"data": [["ok"]]}
    assert client.sent[0]["async"] is True and client.polled == ["u/h1", "u/h1"]


def test_007_short_timeout_stays_sync() -> None:
    client = _Client([_Resp(200, {"data": []})], [])
    status, _ = LiveSnowflakeBridge._post_sql_statements(client, "u", {}, {"statement": "S"}, 65.0, max_wait=60)
    assert status == 200 and "async" not in client.sent[0]


def test_007_polling_deadline_returns_408(monkeypatch) -> None:
    clock = iter(range(0, 10_000, 100))
    monkeypatch.setattr("time.monotonic", lambda: next(clock))
    monkeypatch.setattr("time.sleep", lambda s: None)
    pending = _Resp(202, {"statementHandle": "h"})
    client = _Client([pending], [pending] * 50)
    status, body = LiveSnowflakeBridge._post_sql_statements(client, "u", {}, {"statement": "S"}, 65.0, max_wait=120)
    assert status == 408 and body["statementHandle"] == "h"


# --- BUG-LLMOP-002/003: Decision If-Else routing ----------------------------------------------------
@pytest.mark.parametrize("high,stopped", [(True, "low_confidence_route"), (False, "high_confidence_route")])
def test_002_003_only_active_branch_runs(high: bool, stopped: str) -> None:
    comp = dec_mod.SnowflakeDecisionComponent()
    res = {"high_confidence": high, "confidence": 0.9 if high else 0.2, "route": "x"}
    calls: list[str] = []
    comp._compute = lambda: res  # PASSO 4 cache: one AI_COMPLETE per build
    comp.stop = calls.append
    comp._vertex = object()
    comp.route_high_confidence()
    comp.route_low_confidence()
    assert calls == [stopped, stopped]


def test_002_standalone_build_without_graph_does_not_stop() -> None:
    comp = dec_mod.SnowflakeDecisionComponent()
    comp._compute = lambda: {"high_confidence": True, "confidence": 0.95, "route": "high_confidence"}
    comp.stop = lambda name: (_ for _ in ()).throw(AssertionError("no graph"))
    assert comp.route_high_confidence().data["active"] is True


# --- BUG-LLMOP-008 ---------------------------------------------------------------------------------
def test_008_count_tokens_uses_function_model_text_signature() -> None:
    sql = SnowflakeAIFunctionComponent.build_task_specific_sql(ai_function="AI_COUNT_TOKENS", input_text="hello world")
    assert "AI_COUNT_TOKENS('ai_complete', 'claude-sonnet-5-5', 'hello world')" in sql


# --- BUG-LLMOP-010 ---------------------------------------------------------------------------------
def test_010_zero_is_kept_none_is_omitted() -> None:
    assert _optional_number(0, float) == 0.0 and _optional_number(None, int) is None and _optional_number("", int) is None
    sql = SnowflakeAICompleteEngine.build_sql(model="m", prompt="p", temperature=0.0, top_p=0.0, max_tokens=None)
    assert "'top_p', 0.0" in sql and "max_tokens" not in sql
    with pytest.raises(ValueError):
        SnowflakeAICompleteEngine.build_sql(model="m", prompt="p", max_tokens=0)


# --- BUG-LLMOP-011 ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "stmt",
    [
        "SELECT * FROM T LIMIT 1000000",
        "with t as (select 1) select * from t",
        "\n  select\n * from T;",
        "(SELECT * FROM T) UNION ALL (SELECT * FROM T)",
        "-- note\nSELECT * FROM T",
        "SELECT * FROM T -- trailing comment",
        "SELECT * FROM (SELECT * FROM T LIMIT 99999)",
    ],
)
def test_011_row_cap_cannot_be_bypassed(stmt: str) -> None:
    sql = SnowflakeSQLStatementEngine.build_sql(database="D", schema="S", sql_statement=stmt, max_rows=10)
    assert sql.startswith("SELECT * FROM (\n") and sql.endswith("\n) LIMIT 10")
    js = SnowflakeSQLStatementEngine.build_sql(
        database="D", schema="S", sql_statement=stmt, output_format="JSON Object (OBJECT_CONSTRUCT)", max_rows=10
    )
    assert js.startswith("SELECT OBJECT_CONSTRUCT(*) AS JSON_ROW FROM (\n") and js.endswith("\n) LIMIT 10")


def test_011_composes_with_flow_context_guard() -> None:
    ctx = {
        "databases": ["D"],
        "schemas_by_db": {"D": ["S"]},
        "full_databases": [],
        "full_schemas": [],
        "selected_objects": {"D.S": {"tables": ["T"]}},
    }
    ok = SnowflakeSQLStatementEngine.build_sql(database="D", schema="S", sql_statement="WITH x AS (SELECT * FROM D.S.T) SELECT * FROM x")
    bad = SnowflakeSQLStatementEngine.build_sql(database="D", schema="S", sql_statement="SELECT * FROM D.S.OTHER")
    assert g.check_statement(ok, ctx, "D", "S")[0] is True
    assert g.check_statement(bad, ctx, "D", "S")[0] is False


# --- BUG-LLMOP-012 ---------------------------------------------------------------------------------
def test_012_bindings_normalized_from_list_and_dicts() -> None:
    assert normalize_bindings([1, "a", True, None, 1.5]) == {
        "1": {"type": "FIXED", "value": "1"},
        "2": {"type": "TEXT", "value": "a"},
        "3": {"type": "BOOLEAN", "value": "true"},
        "4": {"type": "TEXT", "value": None},
        "5": {"type": "REAL", "value": "1.5"},
    }
    assert normalize_bindings({"1": "x", "2": 3}) == {"1": {"type": "TEXT", "value": "x"}, "2": {"type": "FIXED", "value": "3"}}
    assert normalize_bindings({"1": {"type": "FIXED", "value": "7"}}) == {"1": {"type": "FIXED", "value": "7"}}
    assert normalize_bindings([]) is None


@pytest.mark.parametrize("bad", [{"name": "x"}, {"1": 1, "3": 2}, {"1": {"type": "EVIL", "value": "1"}}, "s"])
def test_012_bad_bindings_rejected(bad: Any) -> None:
    with pytest.raises(ValueError):
        normalize_bindings(bad)


# ===================== A6 veto regressions (PASSO 6 items 1, 2, 5) =====================
def test_veto1_default_empty_parameters_json_triggers_describe(monkeypatch) -> None:
    seen: dict[str, Any] = {}

    def fake_describe(res_t, ident, database_schema="Database All", flow_id=None):
        seen["ident"] = ident
        return SnowflakeCustomToolEngine.parse_signature_parameters(ident, describe_rows=DESCRIBE_ROWS)

    monkeypatch.setattr(ct_mod, "_describe_custom_tool_params", fake_describe)
    for raw in ("[]", "", [], None):
        seen.clear()
        comp = ct_mod.SnowflakeCustomToolComponent()
        comp.custom_tool_identifier = IDENT
        comp.parameters_json = raw
        assert [p["name"] for p in comp._parsed_parameters()] == ["NAME", "QTY", "FLAG"]
        assert seen["ident"] == IDENT


def test_veto2_sort_keeps_named_and_case() -> None:
    for mode in ("Latest added", "Alphabetical", "Required first"):
        out = SnowflakeCustomToolEngine.sort_parameters(_params(), sort_by=mode)
        assert {p["name"] for p in out} == {"NAME", "QTY", "FLAG"} and all(p["named"] for p in out)
    unnamed = SnowflakeCustomToolEngine.sort_parameters(
        SnowflakeCustomToolEngine.parse_signature_parameters("DB.S.U(NUMBER)")
    )
    assert unnamed[0]["named"] is False


def test_veto2_dict_args_case_insensitive_named_syntax() -> None:
    params = SnowflakeCustomToolEngine.sort_parameters(_params())
    for args in ({"NAME": "a", "QTY": 1}, {"name": "a", "qty": 1}, {"Name": "a", "Qty": 1}):
        sql, _ = SnowflakeCustomToolEngine.build_invocation_request(
            custom_tool_identifier=IDENT, arguments=args, parameters=params
        )
        assert sql == 'CALL DB.SCH.E2E_A5_PROC("NAME" => ?, "QTY" => ?::NUMBER);'
    with pytest.raises(ValueError, match="Duplicate"):
        SnowflakeCustomToolEngine.build_invocation_request(
            custom_tool_identifier=IDENT, arguments={"NAME": "a", "name": "b", "QTY": 1}, parameters=params
        )


def test_veto2_positional_fallback_accepts_dict_in_order() -> None:
    # T9: unnamed signature, dict keyed by the real (unknown to us) names -> values in given order.
    sql, binds = SnowflakeCustomToolEngine.build_invocation_request(
        resource_type="table_function", custom_tool_identifier="DB.S.E2E_A5_UDTF(NUMBER)", arguments={"N": 2}
    )
    assert sql == "SELECT * FROM TABLE(DB.S.E2E_A5_UDTF(?::NUMBER));" and binds == {"1": {"type": "FIXED", "value": "2"}}


def test_veto2_args_schema_keeps_uppercase_names() -> None:
    model = ct_mod.build_args_schema("t", SnowflakeCustomToolEngine.sort_parameters(_params()))
    assert set(model.model_json_schema()["properties"]) == {"NAME", "QTY", "FLAG"}
    assert model(NAME="a", QTY=1).FLAG is None


def test_veto3_multi_statement_child_handle_202_is_polled(monkeypatch) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    client = _Client(
        [_Resp(200, {"statementHandles": ["h-use", "h-user"]})],
        [_Resp(202, {"code": "333334"}), _Resp(202, {"code": "333334", "statementHandle": "h-user"}),
         _Resp(200, {"data": [["done"]]})],
    )
    status, body = LiveSnowflakeBridge._post_sql_statements(client, "u", {}, {"statement": "S"}, 65.0, max_wait=300)
    assert status == 200 and body == {"data": [["done"]]}
    assert client.polled == ["u/h-user", "u/h-user", "u/h-user"]


def test_veto3_async_parent_then_child() -> None:
    client = _Client(
        [_Resp(202, {"statementHandle": "parent"})],
        [_Resp(200, {"statementHandles": ["c1", "c2"]}), _Resp(200, {"data": [["x"]]})],
    )
    import time as _t

    orig = _t.sleep
    _t.sleep = lambda s: None
    try:
        status, body = LiveSnowflakeBridge._post_sql_statements(client, "u", {}, {"statement": "S"}, 65.0, max_wait=120)
    finally:
        _t.sleep = orig
    assert status == 200 and body == {"data": [["x"]]} and client.polled == ["u/parent", "u/c2"]


@pytest.mark.parametrize(
    "target,kw,expected",
    [
        ("claude-sonnet-5-5", {}, "AI_COUNT_TOKENS('ai_complete', 'claude-sonnet-5-5', 'hi')"),
        ("ai_sentiment", {}, "AI_COUNT_TOKENS('ai_sentiment', 'hi')"),
        ("ai_redact", {}, "AI_COUNT_TOKENS('ai_redact', 'hi')"),
        ("ai_summarize", {}, "AI_COUNT_TOKENS('ai_summarize', 'hi')"),
        ("ai_classify", {"categories": "a,b"}, "AI_COUNT_TOKENS('ai_classify', 'hi', ARRAY_CONSTRUCT('a', 'b'))"),
        ("ai_translate", {"source_lang": "en", "target_lang": "it"}, "AI_COUNT_TOKENS('ai_translate', 'hi', 'en', 'it')"),
        ("ai_similarity", {"second_text": "yo"}, "AI_COUNT_TOKENS('ai_similarity', 'hi', 'yo')"),
        ("snowflake-arctic-embed-l-v2.0", {}, "AI_COUNT_TOKENS('ai_embed', 'snowflake-arctic-embed-l-v2.0', 'hi')"),
    ],
)
def test_veto4_count_tokens_official_signatures(target: str, kw: dict, expected: str) -> None:
    from lfx_snowflake.components.ai_function import build_count_tokens_expr

    assert build_count_tokens_expr(target, "hi", **kw) == expected


@pytest.mark.parametrize("bad", ["ai_bogus", "ai_sentiment:some-model", "ai_complete", "x'); DROP", "ai_classify"])
def test_veto4_count_tokens_rejects_bad_targets(bad: str) -> None:
    from lfx_snowflake.components.ai_function import build_count_tokens_expr

    with pytest.raises(ValueError):
        build_count_tokens_expr(bad, "hi")


def test_veto4_count_tokens_defaults_agree() -> None:
    import inspect

    from lfx_snowflake.native_category_components.llm_operations import snowflake_ai_function as aif

    sig = inspect.signature(SnowflakeAIFunctionComponent.build_task_specific_sql)
    native_default = next(i.value for i in aif.SnowflakeAIFunctionComponent.inputs if i.name == "model_for_count_tokens")
    assert sig.parameters["model_for_count_tokens"].default == native_default


def test_veto5_decision_routes_have_separate_ports() -> None:
    outs = {o.name: o for o in dec_mod.SnowflakeDecisionComponent.outputs}
    assert outs["high_confidence_route"].group_outputs is True
    assert outs["low_confidence_route"].group_outputs is True


@pytest.mark.parametrize(
    "stmt,body",
    [
        ("SELECT 1; -- c", "SELECT 1"),
        ("SELECT 1 -- c\n;", "SELECT 1"),
        ("/*x*/ (select 1);;", "(select 1)"),
        ("SELECT 'a;--b' AS x; /* end */", "SELECT 'a;--b' AS x"),
    ],
)
def test_veto5_semicolon_and_comment_trim(stmt: str, body: str) -> None:
    assert SnowflakeSQLStatementEngine.build_sql(database="D", schema="S", sql_statement=stmt, max_rows=3) == (
        f"SELECT * FROM (\n{body}\n) LIMIT 3"
    )


@pytest.mark.parametrize("stmt", ["SELECT 1; DROP TABLE T", "SELECT 1;DROP TABLE T", "SELECT 'x", "/* open"])
def test_veto5_multi_or_unterminated_rejected(stmt: str) -> None:
    with pytest.raises(ValueError):
        SnowflakeSQLStatementEngine.build_sql(database="D", schema="S", sql_statement=stmt)


def test_veto_reentry_warehouse_and_timeout_reach_sql_api_body(monkeypatch) -> None:
    """BUG-LLMOP-007 regression: the PASSO 3 flow-context re-entry must keep warehouse + timeout."""
    from lfx_snowflake import fastapi_live_bridge as fb

    bridge = fb.LIVE_BRIDGE
    sent: dict[str, Any] = {}

    def fake_post(client, url, headers, payload, timeout, max_wait=0.0):
        sent.update(payload=payload, max_wait=max_wait)
        return 200, {"data": [], "resultSetMetaData": {"rowType": []}}

    monkeypatch.setattr(bridge, "_flow_context_denial", lambda stmt: None)
    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: True)
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    monkeypatch.setattr(bridge, "_get_spcs_headers", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "_secondary_roles_guard", lambda *a, **k: None)
    monkeypatch.setattr(type(bridge), "_post_sql_statements", staticmethod(fake_post))
    monkeypatch.setattr(bridge, "_http_client", object(), raising=False)

    res = ct_mod._flow_component_execute_sql("SELECT 1", timeout=300, warehouse="TOOL_WH")
    assert "payload" in sent, res
    assert sent["payload"]["warehouse"] == "TOOL_WH"
    assert sent["payload"]["timeout"] == 300 and sent["max_wait"] == 300.0
