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


def test_multi_statement_wrapper_text_is_removed():
    from lfx_snowflake.sql_result import statement_error

    raw = ('JavaScript execution error: Uncaught Execution of multiple statements failed on statement "SELECT * FROM (\n'
           'SELECT NO_SUCH..." (at line 2, position 0).\nSQL compilation error: error line 2 at position 7\n'
           "invalid identifier 'NO_SUCH_COLUMN' in SYSTEM$MULTISTMT at '    throw `Execution of multiple statements "
           "failed`' position 4\nstackstrace: \nSYSTEM$MULTISTMT line: 10")
    assert statement_error(raw) == "SQL compilation error: error line 2 at position 7 invalid identifier 'NO_SUCH_COLUMN'"
    assert statement_error("Object does not exist") == "Object does not exist"
    with pytest.raises(ValueError, match=r"^SQL Statement failed: SQL compilation error: .*NO_SUCH_COLUMN' \[TECHNICAL_EXECUTION_ERROR\]$"):
        require_ok({"ok": False, "message": raw, "error_code": "TECHNICAL_EXECUTION_ERROR"}, "SQL Statement")
