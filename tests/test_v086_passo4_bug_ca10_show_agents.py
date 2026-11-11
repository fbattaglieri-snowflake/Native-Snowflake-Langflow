"""BUG-CA-10: official SHOW AGENTS syntax, quoted identifiers, no phantom EXAMPLE_AGENT."""

from __future__ import annotations

import inspect
import pathlib

from lfx_snowflake import fastapi_live_bridge as flb
from lfx_snowflake.native_category_components.models_and_agents import snowflake_cortex_agent as sca

SRC = pathlib.Path(flb.__file__).parent


def test_show_agents_sql_quoted():
    assert flb._show_agents_sql("snowflake_langflow", "core") == 'SHOW AGENTS IN SCHEMA "SNOWFLAKE_LANGFLOW"."CORE"'


def test_show_agents_sql_rejects_injection():
    assert flb._show_agents_sql("DB; DROP TABLE X", "CORE") is None
    assert flb._show_agents_sql("DB", 'CORE"--') is None


def test_no_invalid_show_cortex_agents_in_sources():
    for path in (SRC / "fastapi_live_bridge.py", pathlib.Path(sca.__file__)):
        assert "SHOW CORTEX AGENTS" not in path.read_text()


def test_cortex_agents_empty_when_none(monkeypatch):
    monkeypatch.setattr(sca.LIVE_BRIDGE, "get_context_objects", lambda *a, **k: [])
    assert sca._cortex_agents("DB", "SCH", flow_id="f") == []


def test_no_example_agent_fallback_in_catalog():
    src = inspect.getsource(flb)
    assert 'fallback_fqn = f"{default_db}.{default_sch}.EXAMPLE_AGENT"' not in src
