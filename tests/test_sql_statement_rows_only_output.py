"""SQL Statement 'Rows Only' output: rows without the SQL/role envelope and without the JSON_ROW wrapper."""
from lfx_snowflake.native_category_components.llm_operations.snowflake_sql_statement import (
    SnowflakeSQLStatementComponent,
    rows_only,
)


def test_json_row_wrapper_is_opened():
    rows = [{"JSON_ROW": '{\n  "SITE": "MILANO_PLANT",\n  "N": 2\n}'}, {"JSON_ROW": '{"SITE": "TORINO_PLANT", "N": 1}'}]
    assert rows_only(rows) == [{"SITE": "MILANO_PLANT", "N": 2}, {"SITE": "TORINO_PLANT", "N": 1}]


def test_call_returning_variant_is_opened():
    assert rows_only([{"CLAIM": '{"claimed": true, "site": "MILANO_PLANT"}'}]) == [{"claimed": True, "site": "MILANO_PLANT"}]


def test_plain_rows_are_kept():
    rows = [{"A": 1, "B": "x"}, {"ONLY": "plain text"}, {"ONLY": "{not json"}, {"N": 3}]
    assert rows_only(rows) == rows
    assert rows_only(None) == []


def test_component_exposes_the_output():
    names = [o.name for o in SnowflakeSQLStatementComponent.outputs]
    assert names == ["dataframe", "message", "metadata", "rows"]
