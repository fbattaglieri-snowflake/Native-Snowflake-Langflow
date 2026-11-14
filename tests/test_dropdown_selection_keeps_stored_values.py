"""A stored dropdown selection is never replaced by the first option unless the user changed a parent field."""

from __future__ import annotations

import pathlib
import re

from lfx_snowflake import dropdown_selection as sel

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "lfx_snowflake"


def _cfg(value: str) -> dict:
    return {"table_name": {"value": value, "options": []}}


def test_listed_value_is_untouched_and_fqn_matches_short_name() -> None:
    cfg = _cfg("DB.SCH.ORDERS")
    sel.keep_or_default(cfg, "table_name", ["CUSTOMERS", "ORDERS"], "user_message")
    assert cfg["table_name"]["value"] == "DB.SCH.ORDERS"


def test_unlisted_value_is_kept_and_shown() -> None:
    cfg = _cfg("PAYROLL")
    sel.keep_or_default(cfg, "table_name", ["CUSTOMERS", "ORDERS"], None)
    assert cfg["table_name"] == {"value": "PAYROLL", "options": ["CUSTOMERS", "ORDERS", "PAYROLL"]}


def test_empty_value_and_parent_change_get_the_first_option() -> None:
    empty = _cfg("")
    sel.keep_or_default(empty, "table_name", ["CUSTOMERS"], None)
    assert empty["table_name"]["value"] == "CUSTOMERS"
    moved = _cfg("PAYROLL")
    sel.keep_or_default(moved, "table_name", ["CUSTOMERS"], "schema_name")
    assert moved["table_name"]["value"] == "CUSTOMERS"


def test_no_component_assigns_the_first_option_on_a_membership_test() -> None:
    pattern = re.compile(r"not in (\w+)(?:\)|:)[^\n]*\n[^\n]*\[\"value\"\] = \1\[0\]")
    allowed = {
        "snowflake_ai_complete.py",  # model list depends on the selected media type, not on role or Context
        "snowflake_table_change_trigger.py",  # defaults only when empty or when database/schema just changed
        "snowflake_stage_file_trigger.py",  # same
    }
    offenders = [p.name for p in SRC.rglob("*.py") if p.name not in allowed and pattern.search(p.read_text())]
    assert offenders == []
