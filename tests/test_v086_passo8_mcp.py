"""v0.8.6 PASSO 8 / MCP: BUG-MCP-04 (native MCP Tools kept), BUG-MCP-05 (in-container project server), BUG-MCP-01."""

from __future__ import annotations

import ast
import asyncio
import importlib
import re
from pathlib import Path

import pytest

from lfx_snowflake import mcp_project_servers as mps

REPO = Path(__file__).resolve().parents[1]
PID = "819286b8-4326-401d-8796-bddcfff0e5c3"
LOCAL = f"http://localhost:7860/api/v1/mcp/project/{PID}/streamable"


# --------------------------------------------------------------------------- BUG-MCP-04


def _installer() -> str:
    return (REPO / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")


def _tuple_or_set_containing(src: str, marker: str) -> list[list[str]]:
    """String elements of every literal tuple/set/list in the installer that contains `marker`."""
    out = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, (ast.Tuple, ast.Set, ast.List)):
            vals = [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
            if marker in vals:
                out.append(vals)
    return out


def test_mcp04_installer_never_removes_native_mcp_tools() -> None:
    src = _installer()
    assert '("models_and_agents", "MCPTools")' not in src  # not a stale index entry
    for group in _tuple_or_set_containing(src, "SnowflakeCortexAgentA2A"):  # every "duplicates to drop" list
        assert "MCPTools" not in group
    canonical = [g for g in _tuple_or_set_containing(src, "Prompt Template") if "MessageHistory" in g]
    assert canonical and all("MCPTools" in g for g in canonical)  # canonical set + presence + exact-list asserts
    assert "Expected strictly 8 canonical non-legacy components in models_and_agents" in src


# --------------------------------------------------------------------------- BUG-MCP-05: config + proxy discovery


def test_mcp05_ssrf_allowlist_has_loopback_with_placeholders_only() -> None:
    spec = (REPO / "service-spec.template.yaml").read_text(encoding="utf-8")
    m = re.search(r'LANGFLOW_SSRF_ALLOWED_HOSTS: "([^"]+)"', spec)
    assert m and m.group(1) == "<LLM_PROXY_HOST>,<OLLAMA_HOST>,localhost,127.0.0.1"
    for df in ("Dockerfile", "Dockerfile.release"):
        assert 'LANGFLOW_SSRF_ALLOWED_HOSTS="localhost,127.0.0.1"' in (REPO / df).read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "allowed, proxy, ollama",
    [
        ("localhost,127.0.0.1,llm-proxy.svc:8080,ollama.svc", "http://llm-proxy.svc:8080", "http://ollama.svc:11434/v1"),
        ("127.0.0.1:7860,[::1],::1,http://localhost:7860,proxy.internal,my-ollama", "http://proxy.internal:8080", "http://my-ollama:11434/v1"),
        ("localhost,127.0.0.1", "http://<PROXY_SERVICE>.<DNS_NAMESPACE>:8080", "http://<OLLAMA_SERVICE>.<DNS_NAMESPACE>:11434/v1"),
    ],
)
def test_mcp05_proxy_and_ollama_discovery_ignore_loopback(monkeypatch, allowed, proxy, ollama) -> None:
    for key in ("SNOWFLAKE_STANDARD_PROXY_ORIGIN", "LLM_PROXY_BASE_URL", "SNOWFLAKE_PROXY_BASE_URL",
                "SNOWFLAKE_OLLAMA_ORIGIN", "OLLAMA_BASE_URL", "SNOWFLAKE_OLLAMA_BASE_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("LANGFLOW_SSRF_ALLOWED_HOSTS", allowed)
    sl = importlib.import_module("lfx_snowflake.standard_langflow")
    assert sl._resolve_proxy_origin() == proxy
    assert sl._resolve_ollama_origin() == ollama


# --------------------------------------------------------------------------- BUG-MCP-05: direct streamable registration


def test_mcp05_native_uvx_project_entry_becomes_direct_http_keeping_api_key() -> None:
    native = {
        "command": "uvx",
        "args": ["--with", "mcp~=1.28", "mcp-proxy", "--transport", "streamablehttp", "--headers", "x-api-key", "sk-test", LOCAL],
    }
    assert mps.to_direct_streamable(native) == {"url": LOCAL, "headers": {"x-api-key": "sk-test"}}
    no_auth = {"command": "uvx", "args": ["mcp-proxy", "--transport", "streamablehttp", LOCAL.replace("localhost", "127.0.0.1")]}
    assert mps.to_direct_streamable(no_auth) == {"url": LOCAL.replace("localhost", "127.0.0.1")}


@pytest.mark.parametrize(
    "cfg",
    [
        {"url": LOCAL, "headers": {"x-api-key": "k"}},  # already direct
        {"command": "uvx", "args": ["mcp-proxy", "--transport", "streamablehttp", f"https://remote.example.com/api/v1/mcp/project/{PID}/streamable"]},
        {"command": "uvx", "args": ["mcp-proxy", "--transport", "sse", LOCAL]},
        {"command": "uvx", "args": ["mcp-proxy", "--allow-origin", "*", LOCAL]},
        {"command": "uvx", "args": ["mcp-proxy", LOCAL, "http://localhost:7860/other"]},
        {"command": "uvx", "args": ["mcp-proxy", "http://localhost:7860/api/v1/mcp/streamable"]},
        {"command": "uvx", "args": ["mcp-proxy", f"http://user:pw@localhost:7860/api/v1/mcp/project/{PID}/streamable"]},
        {"command": "npx", "args": ["some-server"]},
        {"command": "uvx", "args": ["mcp-server-fetch"]},
    ],
)
def test_mcp05_only_loopback_project_proxy_entries_are_rewritten(cfg) -> None:
    assert mps.to_direct_streamable(cfg) is None


def test_mcp05_lfx_client_treats_url_config_as_streamable_http_under_ssrf_policy() -> None:
    # Langflow 1.13.1 lfx.base.mcp.util.update_tools: mode = "Stdio" if "command" else "Streamable_HTTP" if "url";
    # HTTP mode validates the URL against LANGFLOW_SSRF_ALLOWED_HOSTS. Direct entries carry no `command`.
    direct = mps.to_direct_streamable({"command": "uvx", "args": ["mcp-proxy", LOCAL]})
    assert "command" not in direct and direct["url"] == LOCAL


class _Row:
    def __init__(self, config):
        self.config, self.transport, self.version, self.updated_at = config, "stdio", 3, None


def test_mcp05_reconcile_rewrites_rows_and_bumps_version(monkeypatch) -> None:
    import sys
    import types

    rows = [
        _Row({"enc": {"command": "uvx", "args": ["mcp-proxy", "--headers", "x-api-key", "k1", LOCAL]}}),
        _Row({"enc": {"command": "npx", "args": ["x"]}}),
    ]
    added = []

    class _Session:
        async def exec(self, _stmt):
            return types.SimpleNamespace(all=lambda: rows)

        def add(self, row):
            added.append(row)

    class _Scope:
        async def __aenter__(self):
            return _Session()

        async def __aexit__(self, *a):
            return False

    fake = {
        "langflow.services.auth.mcp_encryption": types.SimpleNamespace(
            decrypt_mcp_config=lambda c: c["enc"], encrypt_mcp_config=lambda c: {"enc": c}
        ),
        "langflow.services.database.models.mcp_server.model": types.SimpleNamespace(MCPServer=object),
        "langflow.services.deps": types.SimpleNamespace(session_scope=_Scope),
        "sqlmodel": types.SimpleNamespace(select=lambda m: m),
    }
    for name, mod in fake.items():
        monkeypatch.setitem(sys.modules, name, mod)
    assert asyncio.run(mps.reconcile_project_mcp_servers()) == 1
    assert added == [rows[0]]
    assert rows[0].config == {"enc": {"url": LOCAL, "headers": {"x-api-key": "k1"}}}
    assert rows[0].transport == "streamable-http" and rows[0].version == 4
    assert rows[1].config["enc"]["command"] == "npx"


def test_mcp05_middleware_reconciles_on_server_list_and_after_project_create(monkeypatch) -> None:
    calls = []

    async def _rec():
        calls.append(1)
        return 0

    monkeypatch.setattr(mps, "reconcile_project_mcp_servers", _rec)
    seen = []

    async def _app(scope, receive, send):
        seen.append(scope["path"])

    mw = mps.ProjectMCPServerReconcileASGIMiddleware(_app)

    async def _go():
        await mw({"type": "http", "path": "/api/v1/flows/", "method": "GET"}, None, None)  # first request: startup pass
        await mw({"type": "http", "path": "/api/v1/flows/", "method": "GET"}, None, None)  # throttled
        await mw({"type": "http", "path": "/api/v2/mcp/servers", "method": "GET"}, None, None)  # forced before list
        await mw({"type": "http", "path": "/api/v1/projects/", "method": "POST"}, None, None)  # forced after create
        await mw({"type": "websocket", "path": "/ws"}, None, None)

    asyncio.run(_go())
    assert len(calls) == 3 and len(seen) == 5


def test_mcp05_reconcile_failure_never_breaks_requests(monkeypatch) -> None:
    async def _boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(mps, "reconcile_project_mcp_servers", _boom)
    hits = []

    async def _app(scope, receive, send):
        hits.append(1)

    asyncio.run(mps.ProjectMCPServerReconcileASGIMiddleware(_app)({"type": "http", "path": "/api/v2/mcp/servers", "method": "GET"}, None, None))
    assert hits == [1]


def test_mcp05_middleware_registered_and_no_loopback_auth_bypass() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert "app.add_middleware(ProjectMCPServerReconcileASGIMiddleware)" in src
    mod = (REPO / "src/lfx_snowflake/mcp_project_servers.py").read_text(encoding="utf-8")
    # the rewrite keeps the project's x-api-key header; nothing in it relaxes auth for loopback callers
    assert "x-api-key" in mod and "auth" not in re.sub(r"(?s)\"\"\".*?\"\"\"|#.*", "", mod).lower()


# --------------------------------------------------------------------------- BUG-MCP-01


def _mcp_call(client, name: str, arguments: dict) -> dict:
    r = client.post(
        "/api/v1/snowflake/mcp/streamable",
        json={"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": name, "arguments": arguments}},
    )
    assert r.status_code == 200
    return r.json()["result"]


def test_mcp01_denied_tool_call_is_error_and_success_is_not(tmp_path, monkeypatch) -> None:
    from tests.test_triggers_w2_r8 import _make_bridge_and_client

    bridge, client, executed = _make_bridge_and_client(tmp_path, monkeypatch)
    monkeypatch.setattr(bridge, "sync_superuser_credentials", lambda: None, raising=False)
    denied = _mcp_call(client, "snowflake_query_sql", {"statement": "DELETE FROM T"})
    assert denied["isError"] is True and "AUTHZ_ROLE_DENIED" in denied["content"][0]["text"]
    ok = _mcp_call(client, "snowflake_query_sql", {"statement": "SELECT 1"})
    assert ok["isError"] is False
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": False, "error_code": "SQL_ERROR"})
    assert _mcp_call(client, "snowflake_query_sql", {"statement": "SELECT 1"})["isError"] is True
