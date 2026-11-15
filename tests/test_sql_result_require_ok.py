"""Data nodes stop on a failed Snowflake statement instead of returning an empty green result (2026-11-15)."""
import pathlib

import pytest

from lfx_snowflake.sql_result import require_ok

COMPONENTS = pathlib.Path(__file__).parent.parent / "src" / "lfx_snowflake" / "native_category_components"


def test_failed_result_raises_with_snowflake_message_and_code():
    with pytest.raises(ValueError, match=r"Snowflake Tables failed: invalid identifier 'X' \[SQL_ERROR\]"):
        require_ok({"ok": False, "message": "invalid identifier 'X'", "error_code": "SQL_ERROR"}, "Snowflake Tables")
    with pytest.raises(ValueError, match="no error message returned"):
        require_ok({"ok": False}, "Stage Files")


def test_successful_or_untyped_results_pass_through():
    ok = {"ok": True, "rows": []}
    assert require_ok(ok, "x") is ok
    assert require_ok({"rows": [1]}, "x") == {"rows": [1]}
    assert require_ok(None, "x") is None


@pytest.mark.parametrize("path", ["data_source/snowflake_tables.py", "data_source/snowflake_views.py",
                                  "data_source/snowflake_iceberg_tables.py", "llm_operations/snowflake_sql_statement.py"])
def test_data_nodes_check_every_statement(path):
    src = (COMPONENTS / path).read_text()
    unchecked = [l.strip() for l in src.splitlines()
                 if l.strip().startswith("res = _flow_component_execute_sql(")]
    assert not unchecked, unchecked
    assert "require_ok(" in src
