"""PASSO 3 follow-up: Split Text LangChain mode runs on a BRIDGE-OWNED fixed-code Python UDF."""

from __future__ import annotations

import pytest

from lfx_snowflake import bridge_routines as br
from lfx_snowflake import fastapi_live_bridge as flb
from lfx_snowflake import flow_context_guard as g
from lfx_snowflake.components.files_and_stage import CortexSearchSplitTextBuilder, CortexSearchSplitTextSpec

FLOW = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
# Pinned: changing the handler code requires bumping LANGCHAIN_SPLITTER_UDF_NAME (versioned, IF NOT EXISTS).
PINNED = {"LF_LANGCHAIN_SPLIT_TEXT_V1": "d2266e390f942a78aa1eec1cbd24b824a0d8ffe5acc2fc577b861737571fbfb0"}


def _ctx() -> dict:
    return {
        "databases": ["D"],
        "schemas_by_db": {"D": ["S"]},
        "selected_objects": {"D.S": {"tables": ["PARSED_DOCS", "DOC_CHUNKS"]}},
    }


@pytest.fixture()
def bridge(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv(br.ROUTINES_SCHEMA_ENV, raising=False)
    b = flb.LiveSnowflakeBridge()
    b.database, b.schema = "D", "S"
    b.contexts_by_flow_id = {FLOW: _ctx()}
    monkeypatch.setattr(b, "_load_flow_contexts_sqlite", lambda: None)
    monkeypatch.setattr(b, "_save_flow_context_sqlite", lambda *a, **k: None)
    monkeypatch.setattr(b, "record_audit_event", lambda **kw: "audit")
    monkeypatch.setattr(flb, "LIVE_BRIDGE", b)
    return b


def test_handler_code_is_versioned_and_pinned() -> None:
    assert br.LANGCHAIN_SPLITTER_UDF_NAME in PINNED
    assert PINNED[br.LANGCHAIN_SPLITTER_UDF_NAME] == br.handler_sha256()


def test_ddl_is_fixed_idempotent_and_only_fqn_varies(bridge) -> None:
    fqn = br.langchain_splitter_udf_fqn(bridge)
    assert fqn == "D.S.LF_LANGCHAIN_SPLIT_TEXT_V1"
    ddl = br.build_langchain_splitter_udf_ddl(fqn)
    assert ddl.startswith(f"CREATE FUNCTION IF NOT EXISTS {fqn}(")
    assert "OR REPLACE" not in ddl
    assert br.LANGCHAIN_SPLITTER_HANDLER in ddl
    assert "PACKAGES = ('langchain-text-splitters==1.1.2')" in ddl
    other = br.build_langchain_splitter_udf_ddl("X.Y." + br.LANGCHAIN_SPLITTER_UDF_NAME)
    assert ddl.replace(fqn, "X.Y." + br.LANGCHAIN_SPLITTER_UDF_NAME) == other


@pytest.mark.parametrize("bad", ["D.S.F; DROP TABLE T", "D.S", "D.S.F$$", "'x'", "D.S.\"f\""])
def test_ddl_rejects_non_plain_fqn(bad) -> None:
    with pytest.raises(Exception):
        br.build_langchain_splitter_udf_ddl(bad)


@pytest.mark.parametrize("bad", ["ONLYDB", "A.B.C", "A;B.C", "A.B C"])
def test_configured_schema_is_validated(monkeypatch, bridge, bad) -> None:
    monkeypatch.setenv(br.ROUTINES_SCHEMA_ENV, bad)
    with pytest.raises(br.BridgeRoutineError):
        br.langchain_splitter_udf_fqn(bridge)
    assert br.bridge_routine_fqns(bridge) == frozenset()  # misconfigured -> nothing exempt


def test_configured_schema_is_used(monkeypatch, bridge) -> None:
    monkeypatch.setenv(br.ROUTINES_SCHEMA_ENV, "GOV_DB.ROUTINES")
    assert br.bridge_routine_fqns(bridge) == frozenset({"GOV_DB.ROUTINES.LF_LANGCHAIN_SPLIT_TEXT_V1"})


def _handler():
    ns: dict = {}
    pytest.importorskip("langchain_text_splitters")
    exec(compile(br.LANGCHAIN_SPLITTER_HANDLER, "handler", "exec"), ns)  # noqa: S102 - our own fixed code
    return ns["split_fn"]


def test_handler_validates_arguments() -> None:
    fn = _handler()
    assert fn("", 500, 50) == []
    assert fn("a " * 400, 200, 20)
    for size, ovl in ((50, 0), (200, 200), (200, -1), (None, 1), (10**6, 1)):
        with pytest.raises(ValueError):
            fn("abc", size, ovl)


def test_guard_allows_calling_the_exact_bridge_udf_only(bridge) -> None:
    spec = CortexSearchSplitTextSpec(database="D", schema="S", splitter_mode="PYTHON_UDF_LANGCHAIN")
    insert_sql = CortexSearchSplitTextBuilder(spec).build_split_insert_sql()
    assert "D.S.LF_LANGCHAIN_SPLIT_TEXT_V1(p.page_content, 1800, 300)" in insert_sql
    tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW)
    try:
        assert bridge._flow_context_denial(insert_sql) is None
        for stmt in (
            # any user routine creation stays denied, including re-creating / replacing the bridge UDF
            CortexSearchSplitTextBuilder(spec).build_langchain_python_udf_ddl(),
            "CREATE OR REPLACE FUNCTION D.S.LF_LANGCHAIN_SPLIT_TEXT_V1(x VARCHAR) RETURNS VARCHAR AS $$ x $$",
            "DROP FUNCTION D.S.LF_LANGCHAIN_SPLIT_TEXT_V1(VARCHAR, NUMBER, NUMBER)",
            "ALTER FUNCTION D.S.LF_LANGCHAIN_SPLIT_TEXT_V1(VARCHAR, NUMBER, NUMBER) SET SECURE",
            "SELECT D.S.LF_LANGCHAIN_SPLIT_TEXT_V2('x', 200, 0)",  # other versions are not exempt
            "SELECT D.S.LANGCHAIN_SPLIT_TEXT_UDF('x', 200, 0)",  # old user-schema UDF name
            "SELECT D.S.LF_LANGCHAIN_SPLIT_TEXT_V1('x', 200, 0) FROM D.S.SECRET_T",
        ):
            assert bridge._flow_context_denial(stmt) is not None, stmt
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)


def test_guard_never_accepts_wildcard_bridge_routines() -> None:
    assert not g.check_statement("SELECT D.S.ANY_UDF(1)", _ctx(), "D", "S", bridge_routine_fqns={"D.S.*"})[0]
    assert g.check_statement("SELECT D.S.ANY_UDF(1)", _ctx(), "D", "S", bridge_routine_fqns={"D.S.ANY_UDF"})[0]


def test_provisioning_uses_service_role_outside_flow_scope(bridge, monkeypatch) -> None:
    seen: list = []

    def _exec(stmt, **kw):
        seen.append((stmt, kw, g.FLOW_COMPONENT_SQL.get(), flb.CURRENT_SNOWFLAKE_FLOW_ID.get()))
        return {"ok": True, "rows": []}

    monkeypatch.setattr(bridge, "execute_sql", _exec)
    monkeypatch.setattr(bridge, "_service_role", lambda: "LF_SERVICE")
    tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW)
    try:
        with g.flow_component_sql():
            out = br.provision_bridge_routines(bridge)
    finally:
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)
    stmt, kw, scoped, fid = seen[0]
    assert stmt.startswith("CREATE FUNCTION IF NOT EXISTS D.S.LF_LANGCHAIN_SPLIT_TEXT_V1(")
    assert kw["role"] == "LF_SERVICE" and kw["mode"] == "service"
    assert scoped is False and fid is None
    assert out["ok"] and out["routines"][0]["fqn"] == "D.S.LF_LANGCHAIN_SPLIT_TEXT_V1"


@pytest.mark.parametrize(
    "res",
    [
        {"ok": False, "status_code": 403, "message": "denied"},
        {"ok": False, "status_code": 422, "message": "003001 (42501): SQL access control error: Insufficient privileges"},
    ],
)
def test_provisioning_fails_clearly_without_create_function(bridge, monkeypatch, res) -> None:
    calls: list = []
    monkeypatch.setattr(bridge, "execute_sql", lambda stmt, **kw: calls.append(stmt) or res)
    monkeypatch.setattr(bridge, "_service_role", lambda: "LF_SERVICE")
    with pytest.raises(br.BridgeRoutineError, match="CREATE FUNCTION on schema D.S"):
        br.provision_bridge_routines(bridge)
    assert len(calls) == 1  # no fallback statement of any kind


def test_component_never_creates_routines_and_requires_provisioned_udf(bridge, monkeypatch) -> None:
    from lfx_snowflake.native_category_components.files_and_knowledge import (
        snowflake_cortex_search_split_text as mod,
    )

    src = open(mod.__file__).read()
    assert "_flow_component_execute_sql(udf_ddl)" not in src
    monkeypatch.setattr(bridge, "execute_sql", lambda stmt, **kw: {"ok": False, "message": "does not exist"})
    with pytest.raises(br.BridgeRoutineError, match="provision_bridge_routines"):
        br.require_langchain_splitter(bridge)
    monkeypatch.setattr(bridge, "execute_sql", lambda stmt, **kw: {"ok": True, "rows": [{"property": "language"}]})
    assert br.require_langchain_splitter(bridge) == "D.S.LF_LANGCHAIN_SPLIT_TEXT_V1"


def test_provision_script_dry_run_prints_ddl(bridge, capsys) -> None:
    import importlib.util
    from pathlib import Path

    p = Path(__file__).resolve().parents[1] / "scripts" / "provision_bridge_routines.py"
    spec = importlib.util.spec_from_file_location("provision_bridge_routines", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.main([]) == 0
    assert "CREATE FUNCTION IF NOT EXISTS D.S.LF_LANGCHAIN_SPLIT_TEXT_V1(" in capsys.readouterr().out
