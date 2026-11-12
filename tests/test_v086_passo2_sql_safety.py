"""v0.8.6 PASSO 2: shared SQL safety helper (`lfx_snowflake.sql_safety`) and per-bug regressions.

Covers BUG-A2A-04, BUG-UI-SQLI-01..05, BUG-LLMOP-003/004, BUG-MEM-RCL-01 and the denied-probe TTL cache.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from lfx_snowflake import sql_safety as ss
from lfx_snowflake.components.ai_function import SnowflakeAICompleteEngine, SnowflakeAIRedactEngine, normalize_stage_and_path
from lfx_snowflake.components.files_and_stage import SnowflakeComponentError, validate_snowflake_identifier
from lfx_snowflake.components.llm_operations import (
    SnowflakeCortexSearchQueryEngine,
    SnowflakeCustomToolEngine,
    SnowflakeDecisionEngine,
)
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

INJECTIONS = [
    "X; SELECT 1 --",
    "T WHERE 1=1 UNION SELECT 1",
    "A' OR '1'='1",
    'A" OR 1=1 --',
    "A)--",
    "A\\",
    "",
    "1ABC",
    "A B",
    "ТABLE",  # Cyrillic
]

_QUESTIONS = {"q1": {"type": "choice", "instructions": "pick", "criteria": {"a": "opt a", "b": "opt b"}}}


class _Recorder:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.rows = rows or []

    def __call__(self, statement: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((statement, kwargs))
        return {"ok": True, "rows": list(self.rows), "columns": []}


# --------------------------------------------------------------------------- helper API
@pytest.mark.parametrize("bad", INJECTIONS)
def test_identifier_helpers_reject_injection(bad: str) -> None:
    with pytest.raises(ss.SqlSafetyError):
        ss.validate_identifier(bad)
    with pytest.raises(ss.SqlSafetyError):
        ss.quote_fqn(f"DB.SCH.{bad}")
    with pytest.raises(SnowflakeComponentError):
        validate_snowflake_identifier(bad, "x")


def test_identifier_helpers_accept_and_quote() -> None:
    assert ss.validate_identifier("my_tbl$1") == "MY_TBL$1"
    assert ss.quote_identifier("col") == '"COL"'
    assert ss.quote_fqn("db.sch.t") == '"DB"."SCH"."T"'
    assert ss.join_fqn("db", "sch", "t") == "DB.SCH.T"
    assert ss.validate_stage_ref("@db.sch.stg") == "@DB.SCH.STG"
    assert ss.validate_stage_ref("@db.sch.stg/a/b-1.pdf", allow_path=True) == "@DB.SCH.STG/a/b-1.pdf"
    for bad in ("@db.sch.stg) UNION SELECT 1", "@~", "@%T", "@db.sch.stg/../x"):
        with pytest.raises(ss.SqlSafetyError):
            ss.validate_stage_ref(bad, allow_path=True)
    with pytest.raises(ss.SqlSafetyError):
        ss.validate_stage_ref("@db.sch.stg/p")  # path not allowed by default


def test_enum_and_literal_helpers() -> None:
    assert ss.validate_enum("incremental", ("AUTO", "FULL", "INCREMENTAL")) == "INCREMENTAL"
    with pytest.raises(ss.SqlSafetyError):
        ss.validate_enum("FULL; DROP", ("AUTO", "FULL"))
    assert ss.sql_string_literal("a'b\\") == "'a''b\\\\'"


def test_bindings_conversion_uses_documented_types() -> None:
    b = ss.to_sql_bindings(["x", 3, 1.5, True, None, {"k": 1}, ("TEXT", 7)])
    assert b == {
        "1": {"type": "TEXT", "value": "x"},
        "2": {"type": "FIXED", "value": "3"},
        "3": {"type": "REAL", "value": "1.5"},
        "4": {"type": "BOOLEAN", "value": "true"},
        "5": {"type": "TEXT", "value": None},
        "6": {"type": "TEXT", "value": '{"k": 1}'},
        "7": {"type": "TEXT", "value": "7"},
    }
    assert all(e["type"] in ss.SQL_API_BINDING_TYPES for e in b.values())
    with pytest.raises(ss.SqlSafetyError):
        ss.binding_for("x", "VARCHAR")


def test_routine_signature_allowlist() -> None:
    assert ss.validate_routine_signature("(varchar, NUMBER(38, 0))") == "(VARCHAR, NUMBER(38, 0))"
    assert ss.validate_routine_signature("(PAYLOAD OBJECT)") == "(OBJECT)"
    for bad in ("(VARCHAR); DROP TABLE X", "(EVIL)", "(VARCHAR", "(VARCHAR) --"):
        with pytest.raises(ss.SqlSafetyError):
            ss.validate_routine_signature(bad)


# --------------------------------------------------------------------------- BUG-A2A-04
def test_bug_a2a_04_describe_agent_rejects_injected_path_params(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    for kwargs in (
        {"database": "SNOWFLAKE_INTELLIGENCE", "schema": "AGENTS", "agent_name": "X; SELECT 1 --"},
        {"database": "DB; DROP", "schema": "AGENTS", "agent_name": "A"},
        {"database": "DB", "schema": "S'--", "agent_name": "A"},
    ):
        card = bridge.get_cortex_agent_a2a_card(**kwargs, role="SYSADMIN")
        assert card["ok"] is False and card["status_code"] == 400
    assert rec.calls == []


# --------------------------------------------------------------------------- BUG-UI-SQLI-01
def _tables_component(**attrs: Any):
    from lfx_snowflake.native_category_components.data_source.snowflake_tables import SnowflakeTablesComponent

    comp = SnowflakeTablesComponent.__new__(SnowflakeTablesComponent)
    for k, v in {"database": "DB", "schema_name": "SCH", "table_name": "T", "max_rows": 10, **attrs}.items():
        object.__setattr__(comp, k, v)
    return comp


@pytest.mark.parametrize(
    "free_sql",
    [
        "SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS",
        "SHOW USERS",
        "INSERT INTO X VALUES (1)",
        "MERGE INTO X USING Y ON 1=1 WHEN MATCHED THEN DELETE",
        "1=1 OR 1=1",
        "A = (SELECT 1)",
        "A = 'x'; DROP TABLE T",
    ],
)
def test_bug_ui_sqli_01_filter_never_executes_free_sql(free_sql: str) -> None:
    with pytest.raises(ss.SqlSafetyError):
        _tables_component(query_filter_input=free_sql)._build_request()


def test_bug_ui_sqli_01_structured_filter_is_bound() -> None:
    sql, binds = _tables_component(query_filter_input="STATUS = 'O''K' AND N >= 3")._build_request()
    assert sql == 'SELECT * FROM "DB"."SCH"."T" WHERE "STATUS" = ? AND "N" >= ? LIMIT 10'
    assert binds == {"1": {"type": "TEXT", "value": "O'K"}, "2": {"type": "FIXED", "value": "3"}}
    sql, binds = _tables_component(query_filter_input='{"ID": [1, 2]}')._build_request()
    assert '"ID" IN (?, ?)' in sql and len(binds) == 2
    with pytest.raises(ss.SqlSafetyError):
        _tables_component(table_name="T; DROP TABLE X")._build_request()


def test_bug_ui_sqli_01_views_and_iceberg_share_the_guard() -> None:
    from lfx_snowflake.native_category_components.data_source.snowflake_iceberg_tables import SnowflakeIcebergTablesComponent
    from lfx_snowflake.native_category_components.data_source.snowflake_views import SnowflakeViewsComponent

    for cls, name_attr in ((SnowflakeViewsComponent, "view_name"), (SnowflakeIcebergTablesComponent, "table_name")):
        comp = cls.__new__(cls)
        for k, v in {"database": "DB", "schema_name": "SCH", name_attr: "OBJ", "max_rows": 5}.items():
            object.__setattr__(comp, k, v)
        object.__setattr__(comp, "query_filter_input", "SELECT 1")
        with pytest.raises(ss.SqlSafetyError):
            comp._build_request()
        object.__setattr__(comp, "query_filter_input", "")
        assert comp._build_request() == ('SELECT * FROM "DB"."SCH"."OBJ" LIMIT 5', None)


# --------------------------------------------------------------------------- BUG-UI-SQLI-02 / BUG-LLMOP-004
def test_bug_ui_sqli_02_routine_identifier_and_signature_validated(monkeypatch) -> None:
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeCustomToolEngine.extract_bare_identifier("DB.SCH.FN; DROP TABLE X; --(VARCHAR)")
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeCustomToolEngine.extract_bare_identifier("DB.SCH.FN(VARCHAR); DROP TABLE X")
    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    monkeypatch.setattr(bridge, "_is_execute_sql_mocked", lambda: True)
    res = bridge.describe_custom_tool_signature(resource_type="function", identifier="DB.SCH.FN(VARCHAR) ; DROP TABLE X")
    assert res["ok"] is False and res["status_code"] == 400
    assert rec.calls == []
    bridge.describe_custom_tool_signature(resource_type="function", identifier="db.sch.fn(varchar, number)")
    assert rec.calls[0][0] == "DESCRIBE FUNCTION DB.SCH.FN(VARCHAR, NUMBER);"


def test_bug_llmop_004_arguments_are_json_only_and_bound() -> None:
    for raw in ("1); DROP TABLE USERS; --", "(SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.USERS), 1", "42"):
        with pytest.raises(ValueError):
            SnowflakeCustomToolEngine.build_invocation_request(
                resource_type="function", custom_tool_identifier="DB.SCH.FN(VARCHAR)", arguments=raw
            )
    sql, binds = SnowflakeCustomToolEngine.build_invocation_request(
        resource_type="procedure",
        custom_tool_identifier="DB.SCH.P(VARCHAR, NUMBER, BOOLEAN, ARRAY)",
        arguments=json.dumps(["O'Brien'); DROP TABLE X; --", 3, True, [1, 2]]),
        parameters=[
            {"name": "a", "type": "string"},
            {"name": "b", "type": "integer"},
            {"name": "c", "type": "boolean"},
            {"name": "d", "type": "array"},
        ],
    )
    assert sql == 'CALL DB.SCH.P("A" => ?, "B" => ?::NUMBER, "C" => ?::BOOLEAN, "D" => PARSE_JSON(?));'
    assert "DROP" not in sql
    assert binds == {
        "1": {"type": "TEXT", "value": "O'Brien'); DROP TABLE X; --"},
        "2": {"type": "FIXED", "value": "3"},
        "3": {"type": "TEXT", "value": "true"},
        "4": {"type": "TEXT", "value": "[1, 2]"},
    }


# --------------------------------------------------------------------------- BUG-LLMOP-003 / BUG-UI-SQLI-03
def test_bug_llmop_003_table_decision_identifiers_validated() -> None:
    for kwargs in (
        {"table_name": "T WHERE 1=1 UNION SELECT 1"},
        {"state_column": "ticket_text}), (SELECT 1) --"},
        {"database": "D; DROP"},
    ):
        with pytest.raises(ss.SqlSafetyError):
            SnowflakeDecisionEngine.build_table_decision_sql(questions=_QUESTIONS, **kwargs)
    sql = SnowflakeDecisionEngine.build_table_decision_sql(
        database="db", schema="sch", table_name="tickets", state_column="txt", questions=_QUESTIONS
    )
    assert 'FROM "DB"."SCH"."TICKETS"' in sql and "'state': \"TXT\"" in sql


# --------------------------------------------------------------------------- BUG-UI-SQLI-04
def test_bug_ui_sqli_04_redact_complete_and_search_validated() -> None:
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeAIRedactEngine.build_sql(source_mode="Table Column", table_fqn="DB.SCH.T; DROP TABLE X", text_column="c")
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeAIRedactEngine.build_sql(source_mode="Table Column", table_fqn="DB.SCH.T", text_column="c) FROM X --")
    plan = SnowflakeAIRedactEngine.build_sql(source_mode="Table Column", table_fqn="db.sch.t", text_column="body")
    assert 'input => "BODY"' in plan["sql"] and 'FROM "DB"."SCH"."T"' in plan["sql"]
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeAICompleteEngine.build_sql(
            model="claude-sonnet-5-5", prompt="p", stage_selector="@DB.SCH.STG) UNION SELECT 1 --", batch_directory_mode=True
        )
    with pytest.raises(ss.SqlSafetyError):
        normalize_stage_and_path("@DB.SCH.STG", "../../etc/passwd")
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeCortexSearchQueryEngine.build_search_preview_sql(
            database="DB", schema="SCH", search_service="SVC', '{}'); DROP TABLE X; --", query="q"
        )
    sql, payload, binds = SnowflakeCortexSearchQueryEngine.build_search_preview_request(
        database="db", schema="sch", search_service="svc", query="it's \\ fine"
    )
    # v0.8.6 E2E: service name must be a validated literal (SEARCH_PREVIEW + flow-context guard); JSON stays bound.
    assert sql == "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('DB.SCH.SVC', ?) AS SEARCH_RESULT"
    assert list(binds) == ["1"]
    assert json.loads(binds["1"]["value"])["query"] == "it's \\ fine" == payload["query"]


# --------------------------------------------------------------------------- BUG-UI-SQLI-05
def test_bug_ui_sqli_05_stage_and_volume_ddl_validated(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    monkeypatch.setattr(bridge, "can_create_stage", lambda: True)
    monkeypatch.setattr(bridge, "can_create_external_volume", lambda: True)
    monkeypatch.setattr(bridge, "check_write_privilege_or_deny", lambda *a, **k: None)
    monkeypatch.setattr(bridge, "record_audit_event", lambda *a, **k: "evt")
    for body in (
        {"name": "S", "database": "DB; DROP TABLE X"},
        {"name": "S", "schema": "SCH--"},
        {"name": "S", "stage_type": "EXTERNAL", "url": "s3://b/", "storage_integration": "I; DROP"},
        {"name": "S", "stage_type": "EXTERNAL", "url": "s3://b/", "encryption": "AWS_SSE_S3') --"},
        {"name": "S", "cloud_provider": "S3' OR 1=1"},
        {"name": "S", "file_format": "FF'); DROP TABLE X; --"},
    ):
        res = bridge.create_stage_governed(body, flow_id=None)
        assert res["ok"] is False and res["status_code"] == 400, body
    for body in ({"name": "V", "storage_provider": "S3' --"}, {"name": "V", "encryption": "X') --"}):
        res = bridge.create_external_volume_governed(body)
        assert res["ok"] is False and res["status_code"] == 400, body
    assert rec.calls == []


def test_bug_ui_sqli_05_memory_ddl_validated(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    monkeypatch.setattr(bridge, "check_write_privilege_or_deny", lambda *a, **k: None)
    for extra in (
        {"table_fqn": "DB.SCH.T; DROP TABLE X"},
        {"database": "DB'--"},
        {"primary_key": "(chunk_id) ; DROP TABLE X"},
    ):
        body = {"name": "MEMX", "agent_memory_type": "episodic_memory", "dry_run": True, **extra}
        res = bridge.create_or_register_memory(body)
        assert res["ok"] is False and res.get("status_code") in (400, 422), (extra, res)
    assert not any("DROP" in sql for sql, _ in rec.calls)


def test_bug_ui_sqli_05_telemetry_merge_uses_bindings(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    evil = "x'); DROP TABLE LANGFLOW_FLOW_RUNS; --\\"
    sql, binds = bridge._build_activity_merge_request(
        {"activity_id": evil, "flow_id": evil, "input_text": evil, "error_message": evil, "flow_context_json": {"k": evil}}
    )
    assert evil not in sql and "DROP" not in sql
    assert sql.startswith(f"MERGE INTO {bridge.activity_table} t USING (SELECT ? AS activity_id)")
    assert sql.count("?") == len(binds)
    assert binds["1"]["value"] == evil
    bridge.record_trigger_execution_telemetry(flow_id=evil, run_id="run-1", trace_id=evil, trigger_type="SCHEDULED_RUN")
    merges = [(s, k) for s, k in rec.calls if s.startswith("MERGE INTO")]
    assert len(merges) == 3
    for stmt, kw in merges:
        assert "DROP" not in stmt and kw.get("bindings")


# --------------------------------------------------------------------------- BUG-MEM-RCL-01
def test_bug_mem_rcl_01_ai_embed_model_is_allowlisted_literal(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    base = {
        "name": "E2E_MB_FACTS",
        "table_fqn": "DB.SCH.LF_MEM_FACTS",
        "agent_memory_type": "semantic_vector",
        "canonical_memory_type": "vector_store_retriever_memory",
        "enable_embedding": True,
        "embedding_provider": "snowflake_cortex",
    }
    monkeypatch.setattr(bridge, "get_memories_for_flow", lambda **k: [{**base, "embedding_model": "snowflake-arctic-embed-l-v2.0"}])
    bridge.recall_memory("E2E_MB_FACTS", query="what's up")
    sql, kw = next((c for c in rec.calls if "AI_EMBED" in c[0]), ("", {}))
    assert "AI_EMBED('snowflake-arctic-embed-l-v2.0', ?)" in sql
    assert kw["bindings"] == {"1": {"type": "TEXT", "value": "what's up"}}

    rec.calls.clear()
    monkeypatch.setattr(bridge, "get_memories_for_flow", lambda **k: [{**base, "embedding_model": "e5-base-v2', ?) --"}])
    res = bridge.recall_memory("E2E_MB_FACTS", query="q")
    assert res["ok"] is False and res["error_code"] == "NOT_CONFIGURED"
    assert not any("AI_EMBED" in c[0] for c in rec.calls)


def test_bug_mem_rcl_01_embedding_provider_binds_text() -> None:
    from lfx_snowflake.providers.embeddings import SnowflakeCortexEmbeddings

    emb = SnowflakeCortexEmbeddings.__new__(SnowflakeCortexEmbeddings)
    emb.model = "snowflake-arctic-embed-m-v1.5"
    sql, binds = emb.build_ai_embed_request("a'b")
    assert sql == "SELECT AI_EMBED('snowflake-arctic-embed-m-v1.5', ?) AS EMBEDDING"
    assert binds == {"1": {"type": "TEXT", "value": "a'b"}}
    emb.model = "x'); DROP"
    with pytest.raises(ss.SqlSafetyError):
        emb.build_ai_embed_request("t")


# --------------------------------------------------------------------------- privilege probe TTL
def test_privilege_probe_failure_is_cached_briefly(monkeypatch) -> None:
    bridge = LiveSnowflakeBridge()
    calls: list[str] = []

    def _fail(statement: str, **kwargs: Any) -> dict[str, Any]:
        calls.append(statement)
        return {"ok": False, "rows": [], "message": "EXPLAIN_PRIVILEGES failed"}

    monkeypatch.setattr(bridge, "execute_sql", _fail)
    monkeypatch.setattr(bridge, "active_role", "SYSADMIN", raising=False)
    privs = bridge.get_role_privileges()
    assert not any(v for v in privs.values() if isinstance(v, bool))
    assert len(calls) == 1


# --------------------------------------------------------------------------- backslash-escape hardening
def test_soft_delete_ids_and_store_predicates_escape_backslash(monkeypatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import SnowflakeVectorStoreBackend

    bridge = LiveSnowflakeBridge()
    rec = _Recorder()
    monkeypatch.setattr(bridge, "execute_sql", rec)
    evil = "a\\' OR 1=1 --"
    bridge.soft_delete_flow_activity([evil])
    updates = [s for s, _ in rec.calls if s.startswith("UPDATE")]
    assert updates and all("'a\\\\'' OR 1=1 --'" in u for u in updates)

    store = SnowflakeVectorStoreBackend(
        database="DB", schema="SCH", table_name="KB", enable_embedding=False, bridge=bridge
    )
    sql = store.build_search_sql("q", session_id=evil, where_clause="TOPIC = 'x' AND SCORE >= 0.5")
    assert "SESSION_ID = 'a\\\\'' OR 1=1 --'" in sql
    assert "(\"TOPIC\" = 'x' AND \"SCORE\" >= 0.5)" in sql
    with pytest.raises(ss.SqlSafetyError):
        store.build_search_sql("q", where_clause="1=1) UNION SELECT * FROM SECRETS --")
    with pytest.raises(ss.SqlSafetyError):
        SnowflakeVectorStoreBackend(table_fqn="DB.SCH.T; DROP TABLE X", enable_embedding=False, bridge=bridge)


# --------------------------------------------------------------------------- A6 PASSO2 veto fixes
def test_veto1_date_time_values_bind_as_text_iso8601() -> None:
    import datetime as dt

    assert ss.binding_for(dt.date(2026, 1, 2)) == {"type": "TEXT", "value": "2026-01-02"}
    assert ss.binding_for(dt.datetime(2026, 1, 2, 3, 4, 5)) == {"type": "TEXT", "value": "2026-01-02T03:04:05"}
    assert ss.binding_for(dt.datetime(2026, 1, 2, 3, 4, tzinfo=dt.timezone.utc))["type"] == "TEXT"
    # DATE/TIMESTAMP_* SQL API bindings take epoch values (ISO strings -> 422 / 002086).
    for typ in ("DATE", "TIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"):
        with pytest.raises(ss.SqlSafetyError):
            ss.binding_for(dt.date(2026, 1, 2), typ)
    assert "DECFLOAT" in ss.SQL_API_BINDING_TYPES
    assert ss.binding_for("1.5", "DECFLOAT") == {"type": "DECFLOAT", "value": "1.5"}


def test_veto2_ai_filter_escapes_input_once() -> None:
    from lfx_snowflake.components.ai_function import SnowflakeAIFunctionComponent

    sql = SnowflakeAIFunctionComponent.build_task_specific_sql(ai_function="AI_FILTER", input_text="it's \\ ok")
    sql = sql if isinstance(sql, str) else str(sql)
    assert "PROMPT('it''s \\\\ ok: {0}'" in sql
    assert "''''" not in sql and "\\\\\\\\" not in sql


def test_veto2_provisioner_truncates_before_escaping() -> None:
    import inspect

    from lfx_snowflake.governance import provisioner

    src = inspect.getsource(provisioner)
    assert 'comment[:256].replace("\\\\", "\\\\\\\\").replace("\'", "\'\'")' in src
    assert '.replace("\'", "\'\'")[:256]' not in src
    # A 255-char prefix followed by a quote must never leave a dangling unescaped quote.
    comment = "x" * 255 + "'"
    safe = comment[:256].replace("\\", "\\\\").replace("'", "''")
    assert safe.endswith("''")


def test_veto3_parse_document_extensions_and_stream_task_enums_validated() -> None:
    from lfx_snowflake.components.files_and_stage import (
        CortexSearchParseDocumentBuilder,
        CortexSearchParseDocumentSpec,
        CortexSearchStreamTaskSpec,
    )

    spec = CortexSearchParseDocumentSpec(
        database="DB", schema="SCH", stage_name="STG", file_extensions_filter=(".pdf", "x' OR 1=1 --")
    )
    with pytest.raises(SnowflakeComponentError):
        CortexSearchParseDocumentBuilder(spec).build_parse_sql()
    ok = CortexSearchParseDocumentSpec(database="DB", schema="SCH", stage_name="STG", file_extensions_filter=(".pdf", "md"))
    assert "LIKE '%.md'" in CortexSearchParseDocumentBuilder(ok).build_parse_sql()

    base = dict(
        database="DB", schema="SCH", stage_name="STG", stream_name="S", task_name="T",
        parsed_table="P", chunks_table="C", warehouse="WH",
    )
    for bad in ({"parse_mode": "LAYOUT'}); DROP TABLE X; --"}, {"split_format": "markdown', 1); --"}):
        with pytest.raises(SnowflakeComponentError):
            CortexSearchStreamTaskSpec(**base, **bad)
    assert CortexSearchStreamTaskSpec(**base, parse_mode="ocr").parse_mode == "OCR"


def test_veto3_cortex_analyst_generated_sql_must_be_read_only(monkeypatch) -> None:
    from lfx_snowflake import fastapi_live_bridge as flb

    bridge = LiveSnowflakeBridge()
    rec = _Recorder(rows=[{"N": 1}])
    statement = {"sql": "DROP TABLE SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_FLOW_RUNS"}

    class _Resp:
        status_code = 200
        content = b"{}"

        def json(self) -> dict[str, Any]:
            return {"message": {"content": [{"type": "sql", "statement": statement["sql"]}]}}

    class _Client:
        def __init__(self, *a: Any, **k: Any) -> None: ...
        def __enter__(self) -> "_Client":
            return self
        def __exit__(self, *a: Any) -> None: ...
        def post(self, *a: Any, **k: Any) -> _Resp:
            return _Resp()

    class _Httpx:
        Client = _Client

    monkeypatch.setattr(flb, "httpx", _Httpx)
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    monkeypatch.setattr(bridge, "_has_spcs_token", lambda: True)
    monkeypatch.setattr(bridge, "_get_spcs_headers", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "execute_sql", rec)

    res = bridge.invoke_cortex_analyst(database="DB", schema="SCH", semantic_source="SV", question="q")
    assert res["generated_sql_blocked"] is True and res["rows"] == []
    assert not any("DROP" in s for s, _ in rec.calls)

    statement["sql"] = "SELECT 1 AS N"
    res = bridge.invoke_cortex_analyst(database="DB", schema="SCH", semantic_source="SV", question="q")
    assert res["generated_sql_blocked"] is False and res["rows"] == [{"N": 1}]
