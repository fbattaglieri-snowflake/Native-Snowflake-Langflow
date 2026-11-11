"""v0.8.6 PASSO 3 — A6 veto regressions: every bypass vector must be denied, legitimate SQL allowed."""

from __future__ import annotations

import pytest

from lfx_snowflake import flow_context_guard as g


def _ctx(**extra) -> dict:
    sel = {"tables": ["T", "SRC", "CHUNKS"], "stages": ["STG"], "functions": ["MY_UDF"], "procedures": ["MY_PROC"]}
    sel.update(extra)
    return {
        "databases": ["D"],
        "schemas_by_db": {"D": ["S"]},
        "full_databases": [],
        "full_schemas": [],
        "selected_objects": {"D.S": sel},
    }


def _allowed(stmt: str, exempt=frozenset()) -> bool:
    return g.check_statement(stmt, _ctx(), "D", "S", exempt)[0]


# Each statement touches SECRET_T / an out-of-context object, or is unverifiable -> must be denied.
BYPASSES = [
    # (1a) quoted identifiers hiding comment / literal markers
    'SELECT 1 AS "col --" FROM SECRET_T',
    'SELECT 1 AS "/*" FROM SECRET_T WHERE "*/" = 1',
    "SELECT 1 AS \"x'FROM\" FROM SECRET_T WHERE \"x'\" = 1",
    'SELECT 1 AS "a""b --" FROM SECRET_T',
    "SELECT 'it''s -- not a comment' AS x FROM SECRET_T",
    "SELECT 'a\\' FROM T' AS x FROM SECRET_T",
    # (1b) INSERT INTO t(cols)
    "INSERT INTO SECRET_T(col1) VALUES (1)",
    "INSERT INTO SECRET_T (col1) SELECT 1",
    "INSERT OVERWRITE INTO OTHER_DB.SCH.SECRET_T(c) SELECT * FROM T",
    "MERGE INTO SECRET_T t USING T s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v",
    "MERGE INTO T t USING SECRET_T s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
    "UPDATE SECRET_T SET a = 1",
    "DELETE FROM SECRET_T",
    "DELETE FROM T USING SECRET_T s WHERE T.id = s.id",
    # (1c) TABLE(<literal/bind/udtf>), RESULT_SCAN, UDF calls
    "SELECT * FROM TABLE('OTHER_DB.SCH.SECRET_T')",
    "SELECT * FROM TABLE(?)",
    "SELECT * FROM TABLE($1)",
    "SELECT * FROM TABLE(:t)",
    "SELECT * FROM TABLE(OTHER_DB.SCH.SECRET_UDTF(1))",
    "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
    "SELECT * FROM TABLE(RESULT_SCAN('01b2c3'))",
    "SELECT OTHER_DB.SCH.SECRET_UDF(1)",
    "SELECT SECRET_UDF(1)",
    'SELECT "secret_udf"(1)',
    "SELECT * FROM T WHERE x = D.S.NOT_MY_UDF(x)",
    "SELECT * FROM IDENTIFIER('SECRET_T')",
    "SELECT * FROM IDENTIFIER(?)",
    "SELECT GET_DDL('TABLE', 'OTHER_DB.SCH.SECRET_T')",
    "SELECT GET_DDL('TABLE', ?)",
    "SELECT SYSTEM$CLUSTERING_INFORMATION('SECRET_T')",
    "SELECT TO_FILE(?, 'a.pdf')",
    "SELECT AI_PARSE_DOCUMENT(TO_FILE('@OTHER_DB.SCH.STG', 'a.pdf'))",
    "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(?, '{}')",
    "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('OTHER_DB.SCH.SVC', '{}')",
    "SELECT OTHER_DB.SCH.SEQ.NEXTVAL",
    # (1d) comma joins after subqueries / table functions / stages, parenthesized joins
    "SELECT * FROM (SELECT 1) q, SECRET_T",
    "SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[1]'))) f, SECRET_T",
    "SELECT * FROM T, LATERAL FLATTEN(INPUT => T.v) f, SECRET_T",
    "SELECT * FROM @D.S.STG s, SECRET_T",
    "SELECT * FROM T SAMPLE (10), SECRET_T",
    "SELECT * FROM T AT(OFFSET => -60), SECRET_T",
    "SELECT * FROM T CHANGES(INFORMATION => DEFAULT) AT(OFFSET => -60), SECRET_T",
    "SELECT * FROM (T, SECRET_T)",
    "SELECT * FROM (T JOIN SECRET_T ON 1 = 1)",
    "SELECT * FROM T a JOIN (SECRET_T b JOIN T c ON 1 = 1) ON 1 = 1",
    "SELECT * FROM T LEFT OUTER JOIN SECRET_T s USING (id)",
    "SELECT * FROM T NATURAL JOIN SECRET_T",
    "SELECT * FROM T ASOF JOIN SECRET_T MATCH_CONDITION(T.ts >= SECRET_T.ts)",
    "SELECT * FROM T WHERE EXISTS (SELECT 1 FROM SECRET_T)",
    "SELECT (SELECT MAX(x) FROM SECRET_T) AS m",
    "SELECT * FROM T UNION ALL SELECT * FROM SECRET_T",
    # (1e) CTE spoofing
    "SELECT * FROM T MATCH_RECOGNIZE (ORDER BY x PATTERN (SECRET_T) DEFINE SECRET_T AS (1 = 1)) JOIN SECRET_T ON 1 = 1",
    "WITH SECRET_T AS (SELECT * FROM SECRET_T) SELECT * FROM SECRET_T",
    "SELECT * FROM (WITH SECRET_T AS (SELECT 1) SELECT * FROM SECRET_T) a, SECRET_T",
    "WITH c AS (SELECT 1) SELECT * FROM c JOIN OTHER_DB.SCH.C ON 1 = 1",
    # (1f) SHOW / DESCRIBE scope
    "SHOW USERS",
    "SHOW ROLES",
    "SHOW TABLES",
    "SHOW GRANTS ON TABLE OTHER_DB.SCH.SECRET_T",
    "SHOW GRANTS TO ROLE ACCOUNTADMIN",
    "SHOW TABLES IN ACCOUNT",
    "SHOW TABLES IN SCHEMA D.OTHER",
    "SHOW COLUMNS IN TABLE SECRET_T",
    "SHOW COLUMNS IN TABLE OTHER_DB.SCH.T",
    "DESCRIBE TABLE SECRET_T",
    "DESCRIBE USER ADMIN",
    "DESCRIBE RESULT LAST_QUERY_ID()",
    # statement kinds outside the allowlist
    "USE DATABASE OTHER_DB",
    "USE ROLE ACCOUNTADMIN",
    "USE WAREHOUSE WH",
    "ALTER SESSION SET QUERY_TAG = 'x'",
    "EXECUTE IMMEDIATE 'SELECT * FROM SECRET_T'",
    "EXECUTE IMMEDIATE $$ SELECT * FROM SECRET_T $$",
    "CALL OTHER_DB.SCH.SECRET_PROC()",
    "CALL SYSTEM$SEND_EMAIL('i', 'a@b.c', 's', 'b')",
    "BEGIN SELECT * FROM SECRET_T; END",
    "GRANT SELECT ON TABLE T TO ROLE PUBLIC",
    "PUT file:///tmp/x @D.S.STG",
    "SET v = (SELECT MAX(x) FROM SECRET_T)",
    "SELECT * FROM T; SELECT * FROM SECRET_T",
    "SELECT * FROM T WHERE 1 = 1 ; DROP TABLE D.S.SRC2",
    # (2a) routine bodies
    "CREATE OR REPLACE FUNCTION D.S.LEAK() RETURNS VARCHAR AS $$ SELECT secret FROM OTHER_DB.OTHER_SCH.T $$",
    "CREATE OR REPLACE FUNCTION D.S.LEAK() RETURNS VARCHAR AS 'SELECT secret FROM OTHER_DB.OTHER_SCH.T'",
    "CREATE OR REPLACE FUNCTION D.S.LEAK() RETURNS TABLE (x VARCHAR) AS $$ SELECT secret FROM SECRET_T $$",
    "CREATE FUNCTION D.S.F() RETURNS VARCHAR LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='h' AS $$\ndef h(): return 1\n$$",
    "CREATE FUNCTION D.S.F() RETURNS VARCHAR LANGUAGE JAVASCRIPT AS $$ return 1 $$",
    "CREATE FUNCTION D.S.F() RETURNS VARCHAR LANGUAGE JAVA HANDLER='A.b' AS 'class A {}'",
    "CREATE PROCEDURE D.S.P() RETURNS VARCHAR LANGUAGE SQL AS $$ BEGIN RETURN (SELECT 1 FROM SECRET_T); END $$",
    "CREATE OR REPLACE AGENT D.S.A FROM SPECIFICATION $$ tools: [] $$",
    "CREATE TABLE D.S.X AS SELECT * FROM SECRET_T",
    "CREATE TABLE D.S.X LIKE OTHER_DB.SCH.SECRET_T",
    "CREATE TABLE D.S.X CLONE SECRET_T",
    "CREATE TABLE D.S.X (id INT REFERENCES OTHER_DB.SCH.SECRET_T (id))",
    "CREATE VIEW D.S.V AS SELECT * FROM SECRET_T",
    "CREATE TABLE D.OTHER.X (id INT)",
    "CREATE STREAM D.S.STR ON TABLE SECRET_T",
    "CREATE STAGE D.S.EXT URL = 's3://b/' STORAGE_INTEGRATION = INTEG",
    "CREATE TABLE D.S.X (id INT) WITH ROW ACCESS POLICY D.S.P ON (id)",
    "CREATE SEMANTIC VIEW D.S.SV TABLES (s AS OTHER_DB.SCH.SECRET_T PRIMARY KEY (id))",
    "CREATE TASK D.S.TSK AS SELECT * FROM SECRET_T",
    "CREATE TASK D.S.TSK AS BEGIN INSERT INTO D.S.T SELECT * FROM SECRET_T; END",
    "CREATE TASK D.S.TSK AS BEGIN EXECUTE IMMEDIATE 'x'; END",
    "ALTER TABLE D.S.T RENAME TO OTHER_DB.SCH.T",
    "ALTER TABLE D.S.T SWAP WITH SECRET_T",
    "DROP TABLE SECRET_T",
    "TRUNCATE TABLE SECRET_T",
    "COPY INTO SECRET_T FROM @D.S.STG",
    "COPY INTO @D.S.STG FROM (SELECT * FROM SECRET_T)",
    "COPY INTO D.S.T FROM @OTHER_DB.SCH.STG",
    "LIST @~",
    "SELECT * FROM @%SECRET_T",
    "REMOVE @OTHER_DB.SCH.STG",
    "SELECT $$x$$",
    "SELECT * FROM T WHERE x = 'unterminated",
    'SELECT "unterminated FROM T',
]


@pytest.mark.parametrize("stmt", BYPASSES)
def test_a6_bypass_vectors_are_denied(stmt: str) -> None:
    assert _allowed(stmt) is False, stmt


LEGIT = [
    "SELECT 1",
    "SELECT * FROM T",
    'SELECT 1 AS "col --" FROM T',
    'SELECT 1 AS "/*" FROM T WHERE "*/" = 1',
    "SELECT 'FROM SECRET_T' AS s /* FROM SECRET_T */ -- FROM SECRET_T\nFROM T",
    "SELECT EXTRACT(YEAR FROM ts) FROM T",
    "SELECT v:a.b::VARCHAR, ARRAY_SIZE(v:items) FROM T",
    "SELECT * FROM T a, SRC b JOIN CHUNKS c ON a.id = c.id LEFT JOIN SRC d ON LEFT(d.s, 2) = 'x'",
    "SELECT * FROM (T JOIN SRC ON 1 = 1)",
    "SELECT * FROM T, LATERAL FLATTEN(INPUT => T.v) f, SRC",
    "SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[1]'))) f, T",
    "SELECT * FROM TABLE(GENERATOR(ROWCOUNT => 10)), T",
    "SELECT * FROM T SAMPLE (10) s",
    "SELECT * FROM T AT(OFFSET => -60) a",
    "WITH c(x) AS (SELECT 1) SELECT * FROM c",
    "WITH a AS (SELECT * FROM T), b AS (SELECT * FROM a) SELECT * FROM b JOIN SRC ON 1 = 1",
    "SELECT * FROM T WHERE id IN (SELECT id FROM SRC) QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) = 1",
    "INSERT INTO T(col1, col2) SELECT ?, AI_EMBED('snowflake-arctic-embed-l-v2.0', ?)",
    "INSERT INTO D.S.CHUNKS (CONTENT, EMBEDDING) SELECT ?, SNOWFLAKE.CORTEX.AI_EMBED('e5-base-v2', ?)",
    "MERGE INTO T t USING SRC s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
    "UPDATE T SET a = 1 FROM SRC WHERE T.id = SRC.id",
    "DELETE FROM T USING SRC s WHERE T.id = s.id",
    "SELECT MY_UDF(1), D.S.MY_UDF(2)",
    "CALL MY_PROC('x')",
    "SELECT AI_PARSE_DOCUMENT(TO_FILE('@D.S.STG', 'a.pdf'), {'mode': 'LAYOUT'})",
    "SELECT BUILD_SCOPED_FILE_URL(@D.S.STG, 'a.pdf')",
    "SELECT * FROM DIRECTORY(@D.S.STG)",
    "SELECT $1, METADATA$FILENAME FROM @D.S.STG (FILE_FORMAT => 'D.S.FF')",
    "COPY INTO D.S.T FROM @D.S.STG FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) ON_ERROR = CONTINUE",
    "LIST @D.S.STG",
    "SHOW TABLES IN SCHEMA D.S",
    "SHOW TABLES LIKE 'X%' IN SCHEMA S",
    "SHOW CORTEX SEARCH SERVICES IN SCHEMA D.S",
    "SHOW COLUMNS IN TABLE T",
    "SHOW TABLES IN D.S",
    "DESCRIBE TABLE T",
    "DESC STAGE D.S.STG",
    "SELECT 'contact@example.com' AS email",
    "CREATE TABLE IF NOT EXISTS D.S.NEW_T (DOC_ID VARCHAR DEFAULT UUID_STRING(), E VECTOR(FLOAT, 768), C TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())",
    "CREATE TABLE D.S.NEW_T AS SELECT * FROM SRC",
    "CREATE OR REPLACE FUNCTION D.S.F(x VARCHAR) RETURNS VARCHAR AS $$ UPPER(x) $$",
    "CREATE OR REPLACE FUNCTION D.S.F(x VARCHAR) RETURNS VARCHAR LANGUAGE SQL AS 'SELECT MAX(V) FROM D.S.SRC'",
    "CREATE OR REPLACE STREAM D.S.STR ON STAGE D.S.STG",
    "CREATE OR REPLACE STREAM D.S.STR ON TABLE D.S.T",
    "CREATE TABLE D.S.X (a INT); INSERT INTO D.S.X SELECT 1; SELECT * FROM D.S.X",
    "SELECT 'a\\' FROM SECRET_T' AS x FROM T",
]


@pytest.mark.parametrize("stmt", LEGIT)
def test_legitimate_component_sql_is_allowed(stmt: str) -> None:
    allowed, reason, offending = g.check_statement(stmt, _ctx(), "D", "S")
    assert allowed, (stmt, reason, offending)


def test_cte_shadows_only_inside_its_with_scope() -> None:
    refs = [r.fqn for r in g.extract_object_refs("SELECT * FROM (WITH X AS (SELECT 1) SELECT * FROM X) a, X", "D", "S")]
    assert refs == ["D.S.X"]


def test_unqualified_names_need_flow_defaults() -> None:
    with pytest.raises(g.UnverifiableStatement):
        g.extract_object_refs("SELECT * FROM T", "", "")


def test_governance_tables_are_append_only_exemptions() -> None:
    exempt = frozenset({"D.GOV.LANGFLOW_AUDIT_LOG"})
    assert _allowed("INSERT INTO D.GOV.LANGFLOW_AUDIT_LOG SELECT 1", exempt)
    for stmt in (
        "SELECT * FROM D.GOV.LANGFLOW_AUDIT_LOG",
        "DELETE FROM D.GOV.LANGFLOW_AUDIT_LOG",
        "UPDATE D.GOV.LANGFLOW_AUDIT_LOG SET x = 1",
        "TRUNCATE TABLE D.GOV.LANGFLOW_AUDIT_LOG",
        "DROP TABLE D.GOV.LANGFLOW_AUDIT_LOG",
        "INSERT INTO D.GOV.LANGFLOW_AUDIT_LOG SELECT * FROM SECRET_T",
    ):
        assert not _allowed(stmt, exempt), stmt


def test_task_block_with_batch_local_temp_table_is_allowed() -> None:
    from lfx_snowflake.components.files_and_stage import CortexSearchStreamTaskBuilder, CortexSearchStreamTaskSpec
    import inspect

    params = inspect.signature(CortexSearchStreamTaskSpec).parameters
    kwargs = {k: v for k, v in {
        "database": "D", "schema": "S", "stage_name": "STG", "stream_name": "DOC_STREAM", "task_name": "DOC_INGEST_TASK",
        "warehouse": "WH", "parsed_table": "PARSED", "chunks_table": "CHUNKS",
    }.items() if k in params}
    try:
        spec = CortexSearchStreamTaskSpec(**kwargs)
    except TypeError:
        pytest.skip("spec signature differs")
    ddl = CortexSearchStreamTaskBuilder(spec).build_task_ddl()
    ctx = _ctx(tables=["PARSED", "CHUNKS"], streams=["DOC_STREAM"])
    allowed, reason, offending = g.check_statement(ddl, ctx, "D", "S")
    assert allowed, (reason, offending)
    stolen = ddl.replace(f"FROM {spec.stream_fqn};", f"FROM {spec.stream_fqn}, D.S.SECRET_T;")
    assert not g.check_statement(stolen, ctx, "D", "S")[0]


# --------------------------------------------------------------------------- items 2b / 3 / 4 (bridge + components)

from pathlib import Path  # noqa: E402

from lfx_snowflake import fastapi_live_bridge as flb  # noqa: E402

FLOW = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
SRC = Path(__file__).resolve().parents[1] / "src" / "lfx_snowflake"


@pytest.fixture()
def bridge(monkeypatch: pytest.MonkeyPatch):
    b = flb.LiveSnowflakeBridge()
    b.database, b.schema = "D", "S"
    b.contexts_by_flow_id = {FLOW: _ctx()}
    monkeypatch.setattr(b, "_load_flow_contexts_sqlite", lambda: None)
    monkeypatch.setattr(b, "_save_flow_context_sqlite", lambda *a, **k: None)
    monkeypatch.setattr(b, "record_audit_event", lambda **kw: "audit")
    return b


def test_exempt_fqns_are_exact_and_use_configured_messages_table(bridge, monkeypatch) -> None:
    monkeypatch.setenv("LANGFLOW_MESSAGES_TABLE", "GOV.HIST.MY_MESSAGES")
    exempt = bridge._flow_guard_exempt_fqns()
    assert "GOV.HIST.MY_MESSAGES" in exempt
    assert "D.S.LANGFLOW_MESSAGES" not in exempt
    assert all(len(f.split(".")) == 3 and "*" not in f for f in exempt)
    monkeypatch.setenv("LANGFLOW_MESSAGES_TABLE", "MSGS")
    assert "D.S.MSGS" in bridge._flow_guard_exempt_fqns()


def test_flow_component_cannot_read_or_drop_governance_tables(bridge) -> None:
    tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW)
    try:
        for stmt in (f"SELECT * FROM {bridge.audit_table}", f"DELETE FROM {bridge.runs_table}", f"DROP TABLE {bridge.spans_table}"):
            denied = bridge._flow_context_denial(stmt)
            assert denied is not None and denied["status_code"] == 403, stmt
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)


def test_unqualified_names_fail_closed_when_flow_default_differs_from_session(bridge) -> None:
    bridge.contexts_by_flow_id[FLOW] = {**_ctx(), "database": "OTHER", "schema": "SCH"}
    tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW)
    try:
        assert bridge._flow_context_denial("SELECT * FROM T") is not None
        assert bridge._flow_context_denial("SELECT * FROM D.S.T") is None
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)


@pytest.mark.parametrize("pseudo", ["default", "a2a-bridge", None])
def test_a2a_without_flow_is_rbac_only(bridge, monkeypatch, pseudo) -> None:
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": [{"name": "AG"}]})
    denied, desc = bridge._cortex_agent_access("X", "Y", "AG", "R", pseudo)
    assert denied is None and desc["ok"]


def test_a2a_with_real_flow_checks_context(bridge, monkeypatch) -> None:
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": [{"name": "AG"}]})
    denied, _ = bridge._cortex_agent_access("X", "Y", "AG", "R", FLOW)
    assert denied is not None and denied["error_code"] == g.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT


def test_a2a_component_maps_flow_context_error_code() -> None:
    from lfx_snowflake.components import a2a_and_cortex_agent as a2a
    from lfx_snowflake.components.files_and_stage import ErrorCode

    assert a2a._taxonomy_code(g.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT) is ErrorCode.AUTHZ_ROLE_DENIED
    assert a2a._taxonomy_code("WHATEVER") is ErrorCode.TECHNICAL_EXECUTION_ERROR
    assert a2a._taxonomy_code("OBJECT_NOT_FOUND_OR_UNAUTHORIZED") is ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED


def test_no_pseudo_flow_ids_passed_to_invoke() -> None:
    for rel in ("native_category_components/models_and_agents/snowflake_cortex_agent.py",):
        assert '_resolve_flow_id(self) or "default"' not in (SRC / rel).read_text()
    installer = (SRC.parents[1] / "scripts" / "install_live_hooks_in_container.py").read_text()
    assert '_resolve_flow_id(self) or "default"' not in installer


def test_register_component_object_requires_in_context_schema(bridge, monkeypatch) -> None:
    monkeypatch.setattr(flb, "LIVE_BRIDGE", bridge)
    tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW)
    try:
        assert g.register_component_object("D.OTHER.STR", "streams") is False
        assert g.register_component_object("D.S.STR", "streams") is True
        ctx = bridge.get_flow_context(FLOW, create=False)
        assert g.check_statement("SELECT * FROM D.S.STR", ctx, "D", "S")[0]
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)


def test_create_stream_is_auto_registered(bridge, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(bridge, "register_object_in_context", lambda *a, **k: calls.append((a, k)))
    bridge._auto_register_ddl_object_if_create("CREATE OR REPLACE STREAM D.S.DOC_STREAM ON STAGE D.S.STG", flow_id=FLOW)
    assert calls and calls[0][0][2] == "streams"


def test_components_do_not_issue_session_statements() -> None:
    for rel in ("native_category_components/llm_operations/snowflake_ai_redact.py", "native_category_components/llm_operations/snowflake_custom_tool.py"):
        text = (SRC / rel).read_text()
        assert '_flow_component_execute_sql(f"USE ' not in text
        assert "_flow_component_execute_sql(f\"ALTER SESSION" not in text
        assert "_flow_component_execute_sql(setup_sql)" not in text
