"""v0.8.6 PASSO 3 regressions: BUG-CTX-03, BUG-A2A-03, BUG-CTX-01, BUG-CTX-02, BUG-CTX-PH-01, BUG-CTX-STG-01."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lfx_snowflake import fastapi_live_bridge as flb
from lfx_snowflake import flow_context_guard as g

REPO = Path(__file__).resolve().parents[1]
FLOW_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
FLOW_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def _ctx(db: str, sch: str, tables: list[str]) -> dict:
    return {
        "databases": [db],
        "schemas_by_db": {db: [sch]},
        "full_databases": [],
        "full_schemas": [],
        "selected_objects": {f"{db}.{sch}": {"tables": tables, "cortex_agents": ["AGENT_A"], "stages": ["STG_A"]}},
    }


@pytest.fixture()
def bridge(monkeypatch: pytest.MonkeyPatch):
    b = flb.LiveSnowflakeBridge()
    b.database, b.schema = "E2E_DB", "E2E_SCH"
    b.contexts_by_flow_id = {
        FLOW_A: _ctx("E2E_DB", "E2E_SCH", ["E2E_A2_T1A_ITEMS"]),
        FLOW_B: _ctx("E2E_DB", "E2E_SCH", ["E2E_A2_T1B_ITEMS"]),
    }
    monkeypatch.setattr(b, "_load_flow_contexts_sqlite", lambda: None)
    monkeypatch.setattr(b, "_save_flow_context_sqlite", lambda *a, **k: pytest.fail("must not persist"))
    monkeypatch.setattr(b, "record_audit_event", lambda **kw: "audit")
    return b


def _in_flow(fid: str):
    return flb.CURRENT_SNOWFLAKE_FLOW_ID.set(fid)


# --------------------------------------------------------------------------- BUG-CTX-03


@pytest.mark.parametrize(
    "stmt, refs",
    [
        ("SELECT 1", []),
        ("SELECT * FROM T1 a, X.T2 b JOIN D.S.T3 ON 1=1", ["DB.SCH.T1", "DB.X.T2", "D.S.T3"]),
        ("WITH c AS (SELECT * FROM T3) SELECT * FROM c, LATERAL FLATTEN(input => c.v)", ["DB.SCH.T3"]),
        ("SELECT * FROM (SELECT * FROM T4) q WHERE x IN (SELECT y FROM T5)", ["DB.SCH.T4", "DB.SCH.T5"]),
        ("INSERT INTO T6 SELECT * FROM T7", ["DB.SCH.T6", "DB.SCH.T7"]),
        ("UPDATE T8 SET a = 1 FROM T9", ["DB.SCH.T8", "DB.SCH.T9"]),
        ("MERGE INTO T10 USING T11 s ON 1=1 WHEN MATCHED THEN DELETE", ["DB.SCH.T10", "DB.SCH.T11"]),
        ("SELECT 'FROM T_FAKE' AS s /* FROM T_HIDDEN */ -- FROM T_X", []),
        ("LIST @DB.SCH.STG/path", ["DB.SCH.STG"]),
        ("CALL P1('x')", ["DB.SCH.P1"]),
        ("SELECT a FROM T1; SELECT b FROM O.S.T", ["DB.SCH.T1", "O.S.T"]),
    ],
)
def test_ctx03_extracts_referenced_objects(stmt, refs) -> None:
    assert [r.fqn for r in g.extract_object_refs(stmt, "DB", "SCH")] == refs


@pytest.mark.parametrize(
    "stmt",
    [
        "SELECT * FROM IDENTIFIER(?)",
        "EXECUTE IMMEDIATE 'SELECT 1'",
        "CREATE DATABASE X",
        "CREATE ROLE R",
        "CREATE WAREHOUSE W",
        "ALTER ACCOUNT SET X = 1",
        "SELECT $$x$$ AS body",
        "SELECT * FROM T WHERE x = SNOWFLAKE.CORTEX.SEARCH_PREVIEW(?, 'q')",
        "GRANT SELECT ON T TO ROLE R",
        "BEGIN SELECT 1; END",
        "SELECT 'unterminated",
        "SHOW TABLES IN ACCOUNT",
        "SELECT * FROM @~/f",
    ],
)
def test_ctx03_unverifiable_statements_fail_closed(stmt) -> None:
    allowed, _reason, _ = g.check_statement(stmt, _ctx("DB", "SCH", ["T"]), "DB", "SCH")
    assert allowed is False


def test_ctx03_flow_a_cannot_read_flow_b_table(bridge, monkeypatch) -> None:
    sent: list[str] = []
    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: False)
    original = flb.LiveSnowflakeBridge.execute_sql

    def _spy(self, statement, **kw):
        if not g.FLOW_COMPONENT_SQL.get():
            sent.append(statement)
            return {"ok": True, "rows": []}
        return original(self, statement, **kw)

    monkeypatch.setattr(flb.LiveSnowflakeBridge, "execute_sql", _spy)
    tok = _in_flow(FLOW_A)
    try:
        with g.flow_component_sql():
            denied = bridge.execute_sql("SELECT * FROM E2E_DB.E2E_SCH.E2E_A2_T1B_ITEMS")
            allowed = bridge.execute_sql("SELECT * FROM E2E_A2_T1A_ITEMS LIMIT 5")
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)
    assert denied["ok"] is False and denied["status_code"] == 403
    assert denied["error_code"] == "AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT"
    assert denied["objects"] == ["E2E_DB.E2E_SCH.E2E_A2_T1B_ITEMS"]
    assert allowed["ok"] is True
    assert sent == ["SELECT * FROM E2E_A2_T1A_ITEMS LIMIT 5"]  # the denied statement never reached Snowflake


def test_ctx03_bridge_internal_sql_is_not_enforced(bridge) -> None:
    tok = _in_flow(FLOW_A)
    try:
        assert bridge._flow_context_denial("SELECT * FROM E2E_DB.E2E_SCH.E2E_A2_T1B_ITEMS") is not None
        # Without the component scope (audit/trace writes, catalog discovery) nothing is checked.
        assert g.FLOW_COMPONENT_SQL.get() is False
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)
    assert bridge._flow_context_denial("SELECT * FROM ANY.WHERE.T") is None  # no flow id -> not enforced


def test_ctx03_wildcard_schema_and_database_selections(bridge) -> None:
    ctx = {"databases": ["D"], "schemas_by_db": {"D": ["S"]}, "full_schemas": ["D.S"], "full_databases": ["W"]}
    assert g.check_statement("SELECT * FROM D.S.ANY_TABLE", ctx, "D", "S")[0]
    assert g.check_statement("SELECT * FROM W.ANY.T", ctx, "D", "S")[0]
    assert not g.check_statement("SELECT * FROM D.OTHER.T", ctx, "D", "S")[0]
    assert g.check_statement("SHOW TABLES IN SCHEMA D.S", ctx, "D", "S")[0]


def test_ctx03_components_use_the_flow_component_scope() -> None:
    for rel in (
        "native_category_components/llm_operations/snowflake_sql_statement.py",
        "native_category_components/data_source/snowflake_tables.py",
        "native_category_components/data_source/snowflake_views.py",
        "native_category_components/data_source/snowflake_iceberg_tables.py",
    ):
        text = (REPO / "src/lfx_snowflake" / rel).read_text(encoding="utf-8")
        bare = text.replace("return LIVE_BRIDGE.execute_sql(*args, **kwargs)", "").replace(_RAISING_WRAPPER_CALL, "")
        assert "LIVE_BRIDGE.execute_sql(" not in bare, rel
        assert "with _flow_component_sql():" in text, rel


# --------------------------------------------------------------------------- BUG-A2A-03


def test_a2a03_no_hardcoded_role_denylist() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert '("PUBLIC", "ANONYMOUS", "DENIED_ROLE")' not in src


@pytest.mark.parametrize(
    "desc, status, code",
    [
        ({"ok": False, "status_code": 404, "error_code": "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"}, 404, "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"),
        ({"ok": False, "status_code": 403, "error_code": "AUTHZ_ROLE_DENIED"}, 403, "AUTHZ_ROLE_DENIED"),
        ({"ok": True, "rows": []}, 404, "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"),
        ({"ok": False, "status_code": 500, "error_code": "TECHNICAL_EXECUTION_ERROR"}, 404, "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"),
    ],
)
def test_a2a03_describe_failure_never_yields_a_card(bridge, monkeypatch, desc, status, code) -> None:
    seen: list[dict] = []
    monkeypatch.setattr(bridge, "execute_sql", lambda st, **kw: seen.append({"st": st, **kw}) or desc)
    bridge.active_role = "DEV_ROLE"
    card = bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")
    assert card["ok"] is False and card["status_code"] == status and card["error_code"] == code
    assert "protocolVersion" not in card
    assert seen[0]["st"] == 'DESCRIBE AGENT "E2E_DB"."E2E_SCH"."AGENT_A"' and seen[0]["role"] == "DEV_ROLE"
    rpc = bridge.handle_cortex_agent_a2a_jsonrpc("E2E_DB", "E2E_SCH", "AGENT_A", {"id": 1, "method": "message/send"})
    assert rpc["error"]["data"]["status_code"] == status


def test_a2a03_real_describe_success_returns_card_even_for_public(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "execute_sql", lambda st, **kw: {"ok": True, "rows": [{"comment": "Real agent"}]})
    bridge.active_role = "PUBLIC"  # the role name alone no longer decides; Snowflake does
    card = bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")
    assert card["name"] == "E2E_DB.E2E_SCH.AGENT_A" and card["description"] == "Real agent"


def test_a2a03_agent_outside_flow_context_is_403_without_sql(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: pytest.fail("no SQL for out-of-context agent"))
    card = bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "OTHER_AGENT", flow_id=FLOW_A)
    assert card["status_code"] == 403 and card["error_code"] == "AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT"
    res = bridge.invoke_cortex_agent(
        database="E2E_DB", schema="E2E_SCH", agent_name="OTHER_AGENT", user_message="hi", flow_id=FLOW_A
    )
    assert res["ok"] is False and res["status_code"] == 403


# --------------------------------------------------------------------------- BUG-CTX-01


def test_ctx01_unknown_flow_read_has_no_side_effects(bridge) -> None:
    ghost = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    ctx = bridge.get_flow_context(ghost, create=False)
    assert ctx["databases"] == [] and ghost not in bridge.contexts_by_flow_id
    created = bridge.get_flow_context(ghost)  # default path caches in memory, never persists (fixture fails on save)
    assert created["databases"] == []


def test_ctx01_context_route_is_read_only_and_scoped() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    start = src.index('@sf_router.get("/context")')
    route = src[start : src.index("@sf_router", start + 10)]
    assert "set_active_flow" not in route
    assert "get_flow_context(req_flow_id, create=False)" in route
    assert '"contexts_by_flow_id"' not in route
    assert "is_context_read" in src  # middleware does not re-target the active flow on GET /context


def test_ctx01_get_flow_context_never_saves_fresh_rows() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    body = src[src.index("    def get_flow_context(") : src.index("    def set_active_flow(")]
    assert "_save_flow_context_sqlite" not in body


# --------------------------------------------------------------------------- BUG-CTX-02 / BUG-CTX-PH-01


def test_ctx02_new_flow_context_is_empty(bridge) -> None:
    tpl = bridge._default_context_template()
    assert tpl["databases"] == [] and tpl["schemas_by_db"] == {}
    assert all(v == [] for v in tpl["visible_objects"].values())
    assert tpl["selected_objects"] == {}


def test_ctx_ph01_no_phantom_trace_tables_injected() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert '["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"]' not in src
    assert 'self.selected_context.get("visible_objects", {}).get("functions"' not in src


def test_ctx_ph01_catalog_buckets_only_from_show(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": []})
    bridge.selected_context["visible_objects"]["tables"] = ["PHANTOM_TABLE"]
    disc = bridge.discover_catalog(
        database="E2E_DB", schema="E2E_SCH", databases=["E2E_DB"], schemas_by_db={"E2E_DB": ["E2E_SCH"]},
        force_refresh=True,
    )
    bucket = (disc.get("objects_by_db_schema") or {}).get("E2E_DB.E2E_SCH") or {}
    assert all(v == [] for v in bucket.values()), bucket


# --------------------------------------------------------------------------- BUG-CTX-STG-01


def test_ctx_stg01_only_internal_stages(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": []})
    monkeypatch.setattr(bridge, "is_stage_visible_in_context", lambda *a, **k: True)
    monkeypatch.setattr(bridge, "list_external_volumes_for_context", lambda **k: {"external_volumes": []})
    bridge.stages_registry = [
        {"name": "INT_STG", "database": "E2E_DB", "schema": "E2E_SCH", "type": "INTERNAL"},
        {"name": "EXT_STG", "database": "E2E_DB", "schema": "E2E_SCH", "type": "EXTERNAL", "url": "s3://b/"},
        {"name": "UNKNOWN_STG", "database": "E2E_DB", "schema": "E2E_SCH"},
    ]
    names = [s["name"] for s in bridge.list_stages_for_context(flow_id=FLOW_A)["stages"]]
    assert names == ["INT_STG"]
    assert json.dumps(names)


# --------------------------------------------------------------------------- PASSO 3 follow-up: full coverage


@pytest.mark.parametrize(
    "stmt, refs",
    [
        ("CREATE OR REPLACE TABLE D.S.NEW_T AS SELECT * FROM D.S.SRC", ["D.S:schema", "D.S.SRC:object"]),
        ("CREATE STREAM IF NOT EXISTS D.S.STRM ON TABLE D.S.SRC", ["D.S:schema", "D.S.SRC:object"]),
        ("CREATE STREAM D.S.DIRSTRM ON STAGE D.S.STG", ["D.S:schema", "D.S.STG:stage"]),
        ("ALTER STAGE D.S.STG SET DIRECTORY = (ENABLE = TRUE)", ["D.S.STG:stage"]),
        ("ALTER TASK D.S.TSK RESUME", ["D.S:schema"]),
        ("EXECUTE TASK D.S.TSK", ["D.S:schema"]),
        ("DROP TABLE IF EXISTS D.S.OLD", ["D.S.OLD:object"]),
        (  # A6 item 2: SQL UDF bodies are analyzed like any other SQL
            "CREATE OR REPLACE FUNCTION D.S.F(x VARCHAR) RETURNS VARCHAR AS $$ SELECT MAX(V) FROM D.S.SRC $$",
            ["D.S:schema", "D.S.SRC:object"],
        ),
        (
            "CREATE OR REPLACE CORTEX SEARCH SERVICE D.S.SVC ON TXT WAREHOUSE = WH TARGET_LAG = '1 hour' AS (SELECT TXT FROM D.S.CHUNKS)",
            ["D.S:schema", "D.S.CHUNKS:object"],
        ),
        ("SELECT AI_PARSE_DOCUMENT(TO_FILE('@D.S.STG', 'a.pdf'))", ["D.S.STG:stage"]),
        ("SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('D.S.SVC', '{}')", ["D.S.SVC:object"]),
        ("SELECT BUILD_SCOPED_FILE_URL(@D.S.STG, 'a.pdf')", ["D.S.STG:stage"]),
        ("REMOVE @D.S.STG/a.txt", ["D.S.STG:stage"]),
        ("COPY INTO D.S.T FROM @D.S.STG", ["D.S.T:object", "D.S.STG:stage"]),
        ("TRUNCATE TABLE D.S.T", ["D.S.T:object"]),
        ("SELECT 'contact@example.com' AS email", []),
    ],
)
def test_ctx03_followup_ddl_and_literal_references(stmt, refs) -> None:
    got = [f"{r.fqn}:{r.kind}" for r in g.extract_object_refs(stmt, "D", "S")]
    assert got == refs


def test_ctx03_followup_ddl_authorization(bridge) -> None:
    ctx = _ctx("D", "S", ["SRC"])
    assert g.check_statement("CREATE TABLE D.S.NEW_T AS SELECT * FROM D.S.SRC", ctx, "D", "S")[0]
    assert not g.check_statement("CREATE TABLE D.OTHER.NEW_T AS SELECT 1", ctx, "D", "S")[0]
    assert not g.check_statement("CREATE TABLE D.S.NEW_T AS SELECT * FROM D.S.NOT_SELECTED", ctx, "D", "S")[0]
    assert not g.check_statement("DROP TABLE D.S.NOT_SELECTED", ctx, "D", "S")[0]
    assert g.check_statement("DROP TABLE D.S.SRC", ctx, "D", "S")[0]
    assert g.check_statement("LIST @D.S.STG_A", ctx, "D", "S")[0]
    assert not g.check_statement("LIST @D.S.OTHER_STG", ctx, "D", "S")[0]
    assert g.check_statement("CALL D.S.PROC_A()", {**ctx, "selected_objects": {"D.S": {"procedures": ["PROC_A"]}}}, "D", "S")[0]
    assert not g.check_statement("CALL D.S.PROC_B()", {**ctx, "selected_objects": {"D.S": {"procedures": ["PROC_A"]}}}, "D", "S")[0]


def test_ctx03_followup_governance_exemptions_are_explicit(bridge) -> None:
    exempt = bridge._flow_guard_exempt_fqns()
    assert {t.upper() for t in (bridge.runs_table, bridge.spans_table, bridge.audit_table, bridge.activity_table)} <= exempt
    ctx = _ctx("E2E_DB", "E2E_SCH", [])
    stmt = f"INSERT INTO {bridge.audit_table} SELECT 1"
    assert not g.check_statement(stmt, ctx, "E2E_DB", "E2E_SCH")[0]
    assert g.check_statement(stmt, ctx, "E2E_DB", "E2E_SCH", exempt)[0]
    assert not g.check_statement("SELECT * FROM E2E_DB.E2E_SCH.USER_TABLE", ctx, "E2E_DB", "E2E_SCH", exempt)[0]


ALL_COMPONENT_FILES = (
    "native_category_components/data_source/snowflake_cortex_search.py",
    "native_category_components/data_source/snowflake_tables.py",
    "native_category_components/data_source/snowflake_views.py",
    "native_category_components/data_source/snowflake_iceberg_tables.py",
    "native_category_components/files_and_knowledge/snowflake_canonical_fk.py",
    "native_category_components/files_and_knowledge/snowflake_cortex_search_parse_document.py",
    "native_category_components/files_and_knowledge/snowflake_cortex_search_service_creator.py",
    "native_category_components/files_and_knowledge/snowflake_cortex_search_split_text.py",
    "native_category_components/files_and_knowledge/snowflake_cortex_search_stream_task.py",
    "native_category_components/files_and_knowledge/snowflake_stage_files.py",
    "native_category_components/llm_operations/snowflake_ai_complete.py",
    "native_category_components/llm_operations/snowflake_ai_function.py",
    "native_category_components/llm_operations/snowflake_ai_redact.py",
    "native_category_components/llm_operations/snowflake_custom_tool.py",
    "native_category_components/llm_operations/snowflake_decision.py",
    "native_category_components/llm_operations/snowflake_node.py",
    "native_category_components/llm_operations/snowflake_sql_statement.py",
    "native_category_components/processing/split_text.py",
    "native_category_components/triggers/snowflake_alert_trigger.py",
    "native_category_components/triggers/snowflake_stage_file_trigger.py",
    "native_category_components/triggers/snowflake_table_change_trigger.py",
)


# E2E T1: the scoped wrapper also turns a guard refusal into a component error.
_RAISING_WRAPPER_CALL = "return _raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(*args, **kwargs))"


@pytest.mark.parametrize("rel", ALL_COMPONENT_FILES)
def test_ctx03_followup_every_component_sql_path_is_scoped(rel) -> None:
    text = (REPO / "src/lfx_snowflake" / rel).read_text(encoding="utf-8")
    bare = text.replace("return LIVE_BRIDGE.execute_sql(*args, **kwargs)", "").replace(_RAISING_WRAPPER_CALL, "")
    assert "LIVE_BRIDGE.execute_sql(" not in bare, rel
    assert "with _flow_component_sql():" in text, rel


def test_ctx03_followup_no_unscoped_component_sql_anywhere() -> None:
    root = REPO / "src/lfx_snowflake/native_category_components"
    offenders = []
    for path in root.rglob("*.py"):
        text = (
            path.read_text(encoding="utf-8")
            .replace("return LIVE_BRIDGE.execute_sql(*args, **kwargs)", "")
            .replace(_RAISING_WRAPPER_CALL, "")
        )
        if "LIVE_BRIDGE.execute_sql(" in text:
            offenders.append(str(path.relative_to(root)))
    assert offenders == []


def test_ctx03_followup_cortex_analyst_semantic_source_in_context(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "_cortex_rest_post", lambda *a, **k: pytest.fail("no REST call"), raising=False)
    res = bridge.invoke_cortex_analyst(
        database="E2E_DB", schema="E2E_SCH", semantic_source="OTHER_VIEW", question="q", flow_id=FLOW_A
    )
    assert res["ok"] is False and res["status_code"] == 403
    assert res["error_code"] == "AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT"
    yaml_res = bridge.invoke_cortex_analyst(
        database="E2E_DB", schema="E2E_SCH", semantic_source="@E2E_DB.E2E_SCH.OTHER_STG/m.yaml",
        source_mode="Stage YAML", question="q", flow_id=FLOW_A,
    )
    assert yaml_res["status_code"] == 403


def test_ctx03_followup_trigger_hook_sets_flow_scope(bridge) -> None:
    seen = {}

    def _probe():
        seen["fid"] = flb.CURRENT_SNOWFLAKE_FLOW_ID.get()
        seen["scoped"] = g.FLOW_COMPONENT_SQL.get()
        return "done"

    assert g.run_in_flow_scope(FLOW_A, _probe) == "done"
    assert seen == {"fid": FLOW_A, "scoped": True}
    assert flb.CURRENT_SNOWFLAKE_FLOW_ID.get() is None and g.FLOW_COMPONENT_SQL.get() is False


def test_a2a03_describe_agent_ttl_cache(bridge, monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(bridge, "execute_sql", lambda st, **kw: calls.append(st) or {"ok": True, "rows": [{"comment": "x"}]})
    bridge.active_role = "DEV_ROLE"
    for _ in range(3):
        assert bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")["name"] == "E2E_DB.E2E_SCH.AGENT_A"
    assert len(calls) == 1  # cached per (role, context_epoch, fqn)
    bridge.context_epoch = bridge.context_epoch + 1
    bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")
    assert len(calls) == 2
    bridge.active_role = "OTHER_ROLE"
    bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")
    assert len(calls) == 3
    # failures are never cached
    monkeypatch.setattr(bridge, "execute_sql", lambda st, **kw: calls.append(st) or {"ok": False, "status_code": 404})
    bridge.active_role = "THIRD_ROLE"
    bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")
    bridge.get_cortex_agent_a2a_card("E2E_DB", "E2E_SCH", "AGENT_A")
    assert len(calls) == 5
