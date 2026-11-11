"""Rerun-3: null SQL API bindings are inlined as NULL centrally in execute_sql (multi-statement requests)."""

from __future__ import annotations

import pytest

from lfx_snowflake import fastapi_live_bridge as flb
from lfx_snowflake import identity as idm
from lfx_snowflake import sql_safety as ss
from tests.test_v086_passo1_security_perimeter import _SqlApi, sql_bridge  # noqa: F401  (fixture)

T = lambda v: {"type": "TEXT", "value": v}  # noqa: E731
N = {"type": "TEXT", "value": None}


def test_mixed_null_and_non_null_renumbered() -> None:
    sql, b = ss.inline_null_bindings("INSERT INTO t VALUES (?, ?, ?, ?)", {"1": T("a"), "2": N, "3": T("c"), "4": N})
    assert sql == "INSERT INTO t VALUES (?, NULL::VARCHAR, ?, NULL::VARCHAR)"
    assert b == {"1": T("a"), "2": T("c")}


def test_all_null_drops_bindings_and_typed_cast() -> None:
    sql, b = ss.inline_null_bindings("SELECT ?::VARCHAR AS a, PARSE_JSON(?) AS j", {"1": {"type": "FIXED", "value": None}, "2": None})
    assert sql == "SELECT NULL::NUMBER::VARCHAR AS a, PARSE_JSON(NULL::VARCHAR) AS j" and b is None


def test_question_marks_in_strings_comments_identifiers_dollar_untouched() -> None:
    src = (
        "SELECT '?', 'it''s ?', 'a\\'?', \"col?\", $$ ? $$, ? -- ? here\n"
        "/* ? */ FROM t // ?\nWHERE x = ?"
    )
    sql, b = ss.inline_null_bindings(src, {"1": N, "2": T("v")})
    assert sql == src.replace("$$, ? --", "$$, NULL::VARCHAR --")
    assert b == {"1": T("v")}


def test_colon_positional_with_reuse_and_renumber() -> None:
    sql, b = ss.inline_null_bindings(
        "SELECT :1, :2::NUMBER, v:3, :3, :1 FROM t WHERE c = :3", {"1": N, "2": T("5"), "3": T("z")}
    )
    # v:3 is a semi-structured path, not a bind.
    assert sql == "SELECT NULL::VARCHAR, :1::NUMBER, v:3, :2, NULL::VARCHAR FROM t WHERE c = :2"
    assert b == {"1": T("5"), "2": T("z")}


def test_no_null_or_unsafe_shapes_unchanged() -> None:
    s = "SELECT ?, ?"
    assert ss.inline_null_bindings(s, {"1": T("a"), "2": T("b")}) == (s, {"1": T("a"), "2": T("b")})
    for bad_sql, bad_b in [
        ("SELECT ?", {"1": N, "2": T("x")}),  # count mismatch
        ("SELECT ?, :1", {"1": N, "2": T("x")}),  # mixed styles
        ("SELECT 'open ?", {"1": N}),  # unlexable
        ("SELECT ?", {"a": N}),  # non-positional key
    ]:
        assert ss.inline_null_bindings(bad_sql, bad_b) == (bad_sql, bad_b)


def test_non_null_value_never_interpolated() -> None:
    evil = "x'); DROP TABLE t; --"
    sql, b = ss.inline_null_bindings("SELECT ?, ?", {"1": T(evil), "2": N})
    assert evil not in sql and b == {"1": T(evil)}


def test_ident_adjacency_padding() -> None:
    sql, _ = ss.inline_null_bindings("SELECT 1 WHERE a=?AND b", {"1": N})
    assert sql == "SELECT 1 WHERE a=NULL::VARCHAR AND b"


def test_execute_sql_two_statement_prefix_case(sql_bridge, monkeypatch) -> None:  # noqa: F811
    monkeypatch.delenv(idm.SECONDARY_ROLES_MODE_ENV, raising=False)
    api = _SqlApi(admin_in_session=False)
    sql_bridge._http_client = api
    res = sql_bridge.execute_sql(
        "MERGE INTO M t USING (SELECT ? AS ID) s ON t.ID = s.ID WHEN NOT MATCHED THEN INSERT (ID, A) VALUES (?, ?)",
        bindings={"1": T("k"), "2": T("k"), "3": N},
    )
    assert res["ok"] is True
    user = api.posts[-1]
    assert user["statement"].startswith("USE SECONDARY ROLES NONE;\n")
    assert user["statement"].endswith("VALUES (?, NULL::VARCHAR)")
    assert user["bindings"] == {"1": T("k"), "2": T("k")}
    assert all(v.get("value") is not None for p in api.posts for v in (p.get("bindings") or {}).values())


def test_builder_level_fix_compatible() -> None:
    sql, b = flb.LiveSnowflakeBridge._merge_insert_request("T", "ID", "k", [("A", None, "TEXT"), ("B", "x", "TEXT")])
    assert ss.inline_null_bindings(sql, b) == (sql, b)  # no nulls left -> no double handling
