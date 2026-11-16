"""Tables / Iceberg Tables write mode inserts rows by column name (2026-11-16).

Before: ``INSERT INTO t SELECT PARSE_JSON(?)`` only fitted a table with one VARIANT column, and an empty or
malformed input was written as a placeholder object.
"""
from __future__ import annotations

import json

import pytest

from lfx_snowflake.rows_insert import build_insert, normalize_rows


class _Data:
    def __init__(self, data):
        self.data = data


class Message:
    """Like Langflow's Message: a Data whose ``data`` is the envelope and whose payload is ``text``."""

    def __init__(self, text):
        self.text = text
        self.data = {"text": text, "sender": "User", "session_id": "s"}


_Message = Message


def test_rows_come_from_objects_lists_json_text_and_read_node_output() -> None:
    assert normalize_rows({"A": 1}) == [{"A": 1}]
    assert normalize_rows('[{"A": 1}, {"A": 2}]') == [{"A": 1}, {"A": 2}]
    assert normalize_rows([_Data({"A": 1}), _Message('{"A": 2}')]) == [{"A": 1}, {"A": 2}]
    assert normalize_rows(_Data({"rows": [{"A": 1}, {"A": 2}], "sql": "SELECT ...", "ok": True})) == [{"A": 1}, {"A": 2}]


@pytest.mark.parametrize("payload", [None, "", [], [""], "{not json", '"just text"', "[1, 2]", [{}]])
def test_nothing_or_malformed_input_is_refused_not_written(payload) -> None:
    with pytest.raises(ValueError):
        normalize_rows(payload)


def test_insert_names_every_column_and_binds_the_payload() -> None:
    sql, bindings = build_insert("DB.SCH.T", [{"ID": 1, "name": "O'Brien"}, {"ID": 2, "NOTE": None}])
    assert sql == ('INSERT INTO DB.SCH.T (ID, name, NOTE) SELECT f.value:"ID", f.value:"name", f.value:"NOTE" '
                   "FROM TABLE(FLATTEN(input => PARSE_JSON(?))) f")
    assert json.loads(bindings["1"]["value"]) == [{"ID": 1, "name": "O'Brien"}, {"ID": 2, "NOTE": None}]
    assert "O'Brien" not in sql


@pytest.mark.parametrize("column", ["A; DROP TABLE T", 'A"', "A B", ""])
def test_column_names_cannot_carry_sql(column) -> None:
    with pytest.raises(ValueError):
        build_insert("DB.SCH.T", [{column: 1}])


def test_same_column_in_two_spellings_is_refused() -> None:
    with pytest.raises(ValueError):
        build_insert("DB.SCH.T", [{"id": 1}, {"ID": 2}])
