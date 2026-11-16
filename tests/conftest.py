"""Shared pytest setup.

Langflow is not installed in the unit-test environment, so the `/api/v1/snowflake/*` auth guard
(BUG-A2A-06) has no native credential to check. The legacy route tests opt into the test-only bypass,
which is ignored whenever Langflow's auth utilities are importable.
"""

import os

os.environ.setdefault("SNOWFLAKE_LANGFLOW_AUTH_TEST_BYPASS", "1")


import pytest  # noqa: E402

# PASSO1/A6: `can_create_*` now read Snowflake's EXPLAIN_PRIVILEGES verdicts and fail closed offline. Legacy suites were
# written against the old role-name heuristics, so they get an emulated grant catalogue that mirrors a
# typical account. Production code never sees this; v0.8.6 security tests exercise the real fail-closed path.
_ADMIN_ROLES = {"ACCOUNTADMIN", "SYSADMIN", "SNOWFLAKE_LANGFLOW_ADMIN_ROLE", "SNOWFLAKE_LANGFLOW_ADMIN"}
_READ_ONLY_ROLES = {
    "", "PUBLIC", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE", "VIEWER_ROLE", "ROLE_VIEWER",
    "READONLY_ROLE", "UNAUTHORIZED_ROLE", "NO_DDL_ROLE",
}


def _emulated_probe(role: str) -> dict[str, bool]:
    """Offline stand-in for Snowflake's EXPLAIN_PRIVILEGES verdicts (tests only)."""
    writer = role not in _READ_ONLY_ROLES
    admin = role in _ADMIN_ROLES
    return {
        "DATABASE": admin,
        "SCHEMA": writer,
        "OBJECT": writer,
        "STAGE": writer,
        "EXTERNAL_VOLUME": admin,
    }


@pytest.fixture(autouse=True)
def _emulated_snowflake_grants(request, monkeypatch):
    if "v086" in request.node.nodeid:
        yield
        return
    from lfx_snowflake import fastapi_live_bridge as flb

    original = flb.LiveSnowflakeBridge._privilege_probe

    def _probe(self):
        if self._has_spcs_token():
            return original(self)
        return _emulated_probe(str(self.active_role or "").strip().upper())

    monkeypatch.setattr(flb.LiveSnowflakeBridge, "_privilege_probe", _probe)
    yield


def scripted_cortex_rest_transport():
    """Stand-in for the Cortex REST API in offline tests: real threads ids, an echo answer per run."""
    import re as _re

    state = {"threads": 0}

    def _transport(method: str, path: str, payload):
        if method == "POST" and path == "/api/v2/cortex/threads":
            state["threads"] += 1
            return {"thread_id": state["threads"]}
        if method == "POST" and path.startswith("/api/v2/cortex/threads/"):
            return {}
        if method == "GET" and path.startswith("/api/v2/cortex/threads"):
            return [] if "?" in path or path.endswith("/threads") else None
        m = _re.match(r"/api/v2/databases/([^/]+)/schemas/([^/]+)/agents/([^/:]+):run$", path)
        if method == "POST" and m:
            prompt = payload["messages"][-1]["content"][0]["text"]
            return {"text": f"Cortex Agent {'.'.join(m.groups())} response to: {prompt}"}
        raise AssertionError(f"Unexpected Cortex REST call: {method} {path}")

    return _transport


@pytest.fixture(autouse=True)
def _scripted_cortex_agent(request, monkeypatch):
    """The bridge never invents an agent answer; the agent tests script the REST API instead."""
    if any(k in request.node.nodeid for k in ("test_models_and_agents_w2_r6", "passo6_cortex_agent_and_a2a")):
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

        monkeypatch.setattr(LIVE_BRIDGE, "_cortex_rest_transport_fn", scripted_cortex_rest_transport())
    yield
