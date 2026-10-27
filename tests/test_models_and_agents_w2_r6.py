"""Wave 2 Round 6 (`MA-W2-R6-01..MA-W2-R6-06`) test suite for Unified `Models & Agents` Architecture."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import types
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from pydantic import BaseModel

from lfx_snowflake import standard_langflow
from lfx_snowflake.components.a2a_and_cortex_agent import (
    A2AAgentCard,
    ServeAsA2AAgentRegistry,
    SnowflakeA2AAgentComponent,
)
from lfx_snowflake.components.files_and_stage import ErrorCode, SnowflakeComponentError
from lfx_snowflake.fastapi_live_bridge import (
    LIVE_BRIDGE,
    LiveSnowflakeBridge,
    register_snowflake_fastapi_routes,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_analyst import (
    SnowflakeCortexAnalystComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_search import (
    SnowflakeCortexSearchComponent,
)
from lfx_snowflake.native_category_components.models_and_agents import (
    SnowflakeCortexAgentA2AComponent,
    SnowflakeCortexAgentComponent,
    SnowflakeMessageHistoryNativeComponent,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_cortex_agent import (
    START_FRESH_THREAD_LABEL,
    THREAD_CONTINUITY_MODES,
)
from lfx_snowflake.providers.language_models import (
    PROVIDER_PRIVATE_OLLAMA,
    PROVIDER_PROXY_CHAT,
    PROVIDER_PROXY_MESSAGES,
    PROVIDER_PROXY_RESPONSES,
    SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS,
    CortexAgentToolSchema,
    CortexAnalystToolSchema,
    CortexSearchToolSchema,
    StructuredTool,
    get_snowflake_live_language_model_catalog,
    register_snowflake_language_model_providers,
    sanitize_tool_identifier,
    update_snowflake_language_model_build_config,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_client() -> Any:
    try:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        register_snowflake_fastapi_routes(app)
        return TestClient(app)
    except (ModuleNotFoundError, ImportError):
        pass

    class _StubJSONResponse:
        def __init__(self, status_code: int = 200, content: Any = None) -> None:
            self.status_code = int(status_code)
            self.content = content

        def json(self) -> Any:
            return self.content

    class _StubRequest:
        def __init__(
            self,
            method: str,
            url: str,
            json_body: Any = None,
            params: dict[str, str] | None = None,
        ) -> None:
            if params:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{urlencode(params)}"
            parsed = urlsplit(url)
            self.method = method.upper()
            self.url = types.SimpleNamespace(path=parsed.path)
            self.query_params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            self.headers: dict[str, str] = {}
            self._json_body = json_body

        async def json(self) -> Any:
            if self._json_body is None:
                raise ValueError("No JSON body")
            return self._json_body

    class _StubAPIRouter:
        def __init__(self, prefix: str = "", tags: list[str] | None = None) -> None:
            self.prefix = prefix.rstrip("/")
            self.tags = tags or []
            self.routes: list[tuple[str, str, Any]] = []

        def _register(self, method: str, path: str):
            full_path = f"{self.prefix}{path}"

            def _decorator(fn: Any) -> Any:
                self.routes.append((method.upper(), full_path, fn))
                return fn

            return _decorator

        def get(self, path: str, **_kwargs: Any):
            return self._register("GET", path)

        def post(self, path: str, **_kwargs: Any):
            return self._register("POST", path)

        def put(self, path: str, **_kwargs: Any):
            return self._register("PUT", path)

        def patch(self, path: str, **_kwargs: Any):
            return self._register("PATCH", path)

        def delete(self, path: str, **_kwargs: Any):
            return self._register("DELETE", path)

    import sys

    fastapi_mod = types.ModuleType("fastapi")
    fastapi_mod.APIRouter = _StubAPIRouter  # type: ignore[attr-defined]
    fastapi_mod.Request = _StubRequest  # type: ignore[attr-defined]
    resp_mod = types.ModuleType("fastapi.responses")
    resp_mod.JSONResponse = _StubJSONResponse  # type: ignore[attr-defined]
    sys.modules.setdefault("fastapi", fastapi_mod)
    sys.modules.setdefault("fastapi.responses", resp_mod)

    class _StubApp:
        def __init__(self) -> None:
            self.routers: list[_StubAPIRouter] = []

        def include_router(self, router: _StubAPIRouter) -> None:
            self.routers.append(router)

    app = _StubApp()
    register_snowflake_fastapi_routes(app)

    class _FallbackTestClient:
        def _dispatch(
            self,
            method: str,
            url: str,
            json_body: Any = None,
            params: dict[str, str] | None = None,
        ) -> _StubJSONResponse:
            import inspect
            import re as _re

            req = _StubRequest(method, url, json_body=json_body, params=params)
            req_path = req.url.path
            for router in app.routers:
                for r_method, r_path, fn in router.routes:
                    if r_method != method.upper():
                        continue
                    pattern = "^" + _re.sub(r"\{([a-zA-Z0-9_]+)(?::path)?\}", r"(?P<\1>[^/]+)", r_path) + "$"
                    m = _re.match(pattern, req_path)
                    if m:
                        sig = inspect.signature(fn)
                        call_kwargs: dict[str, Any] = {}
                        for p_name in sig.parameters:
                            if p_name == "request":
                                call_kwargs["request"] = req
                            elif p_name in m.groupdict():
                                call_kwargs[p_name] = m.groupdict()[p_name]
                            elif p_name in req.query_params:
                                val = req.query_params[p_name]
                                call_kwargs[p_name] = int(val) if val.isdigit() else val
                        return asyncio.run(fn(**call_kwargs))
            raise AssertionError(f"Route not found: {method} {req_path}")

        def get(self, url: str, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("GET", url, params=params)

        def post(self, url: str, json: Any = None, params: dict[str, str] | None = None) -> _StubJSONResponse:  # noqa: A002
            return self._dispatch("POST", url, json_body=json, params=params)

    return _FallbackTestClient()


# ---------------------------------------------------------------------------
# MA-W2-R6-01: ❄️ Cortex Agent & 2-Level Snowflake Threads API Auto-Recovery
# ---------------------------------------------------------------------------


def test_ma_w2_r6_01_cortex_agent_component_schema_and_no_a2a_inputs() -> None:
    """Verify SnowflakeCortexAgentComponent removes A2A inputs and exposes Resume Thread, 3 Continuity Modes, and 2 Outputs."""
    assert SnowflakeCortexAgentComponent.name == "SnowflakeCortexAgent"
    assert SnowflakeCortexAgentComponent.display_name == "Cortex Agent"
    assert SnowflakeCortexAgentComponent.icon == "Snowflake"
    assert SnowflakeCortexAgentA2AComponent is SnowflakeCortexAgentComponent

    input_by_name = {inp.name: inp for inp in SnowflakeCortexAgentComponent.inputs}
    assert "mode" not in input_by_name
    assert "a2a_endpoint" not in input_by_name

    # Required cascading & thread continuity inputs
    assert list(input_by_name)[:3] == ["database", "schema_name", "cortex_agent"]
    assert "resume_thread" in input_by_name
    assert input_by_name["resume_thread"].value is True
    assert "thread_continuity_mode" in input_by_name
    assert input_by_name["thread_continuity_mode"].options == list(THREAD_CONTINUITY_MODES)
    assert len(THREAD_CONTINUITY_MODES) == 3
    assert "conversation_thread" in input_by_name
    assert input_by_name["conversation_thread"].refresh_button is True
    assert input_by_name["conversation_thread"].value == START_FRESH_THREAD_LABEL
    assert "user_message" in input_by_name
    assert input_by_name["user_message"].tool_mode is True
    assert "session_id" in input_by_name
    assert input_by_name["session_id"].advanced is True
    assert "tool_choice" in input_by_name
    assert input_by_name["tool_choice"].options == ["auto", "required", "none"]

    output_by_name = {out.name: out for out in SnowflakeCortexAgentComponent.outputs}
    assert set(output_by_name) == {"response", "structured_response"}
    assert output_by_name["response"].method == "invoke_agent"
    assert output_by_name["structured_response"].method == "invoke_agent_structured"


def test_ma_w2_r6_01_threads_api_sqlite_persistence_and_cold_start_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify 2-level thread tracking in SQLite `snowflake_cortex_agent_threads` and cold-start recovery from Threads API."""
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    bridge.active_role = "ACCOUNTADMIN"

    transport_calls: list[tuple[str, str, Any]] = []
    remote_threads_store: dict[int, dict[str, Any]] = {}
    seq = {"tid": 900, "mid": 5000}

    def _mock_cortex_rest_transport(method: str, path: str, payload: Any) -> Any:
        transport_calls.append((method, path, payload))
        if method == "POST" and path == "/api/v2/cortex/threads":
            seq["tid"] += 1
            tid = seq["tid"]
            remote_threads_store[tid] = {
                "metadata": {
                    "thread_id": tid,
                    "thread_name": f"Thread #{tid}",
                    "origin_application": (payload or {}).get("origin_application", "langflow"),
                    "created_on": "2026-10-27T10:00:00Z",
                    "updated_on": "2026-10-27T10:00:00Z",
                },
                "messages": [],
            }
            return {"thread_id": tid}
        if method == "POST" and path.startswith("/api/v2/cortex/threads/"):
            tid = int(path.rsplit("/", 1)[-1])
            if tid in remote_threads_store and isinstance(payload, dict) and payload.get("thread_name"):
                remote_threads_store[tid]["metadata"]["thread_name"] = payload["thread_name"]
            return {"status": "updated"}
        if method == "GET" and path.startswith("/api/v2/cortex/threads?"):
            return [v["metadata"] for v in remote_threads_store.values()]
        if method == "GET" and path.startswith("/api/v2/cortex/threads/"):
            tid_part = path.split("/api/v2/cortex/threads/", 1)[1].split("?", 1)[0]
            tid = int(tid_part)
            return remote_threads_store.get(tid, {"metadata": {"thread_id": tid}, "messages": []})
        if method == "POST" and path.endswith(":run"):
            seq["mid"] += 1
            u_mid = seq["mid"]
            seq["mid"] += 1
            a_mid = seq["mid"]
            tid = int((payload or {}).get("thread_id") or 0)
            p_mid = int((payload or {}).get("parent_message_id") or 0)
            prompt_txt = payload["messages"][0]["content"][0]["text"]
            if tid in remote_threads_store:
                # Snowflake Threads API returns messages newest-first (descending)
                remote_threads_store[tid]["messages"].insert(
                    0,
                    {
                        "message_id": u_mid,
                        "parent_id": p_mid,
                        "role": "user",
                        "created_on": "2026-10-27T10:01:00Z",
                        "message_payload": json.dumps({"content": [{"type": "text", "text": prompt_txt}]}),
                    },
                )
                remote_threads_store[tid]["messages"].insert(
                    0,
                    {
                        "message_id": a_mid,
                        "parent_id": u_mid,
                        "role": "assistant",
                        "created_on": "2026-10-27T10:01:02Z",
                        "message_payload": json.dumps({"content": [{"type": "text", "text": f"Answer for {prompt_txt}"}]}),
                    },
                )
            return (
                f"event: metadata\n"
                f'data: {{"metadata": {{"role": "user", "message_id": {u_mid}}}}}\n\n'
                f"event: response.thinking.delta\n"
                f'data: {{"text": "Analyzing SQL metrics..."}}\n\n'
                f"event: response.tool_use\n"
                f'data: {{"name": "cortex_analyst", "type": "cortex_analyst_text_to_sql"}}\n\n'
                f"event: response.tool_result\n"
                f'data: {{"content": [{{"json": {{"sql": "SELECT COUNT(*) FROM ORDERS", "result_set": {{"rows": [[42]]}}}}}}]}}\n\n'
                f"event: metadata\n"
                f'data: {{"metadata": {{"role": "assistant", "message_id": {a_mid}}}}}\n\n'
                f"event: response.text.delta\n"
                f'data: {{"text": "Answer for {prompt_txt}"}}\n\n'
            )
        raise AssertionError(f"Unexpected call: {method} {path}")

    bridge._cortex_rest_transport_fn = _mock_cortex_rest_transport

    # Turn 1: creates a new thread and sends parent_message_id = 0
    turn1 = bridge.invoke_cortex_agent(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        agent_name="SALES_COPILOT",
        user_message="How many orders today?",
        flow_id="flow-r6-01",
        session_id="sess-r6-01",
        resume_thread=True,
        thread_continuity_mode="Resume Latest Thread of this Agent",
    )
    assert turn1["ok"] is True
    assert turn1["thread_id"] == 901
    assert turn1["parent_message_id"] == 0
    assert turn1["user_message_id"] == 5001
    assert turn1["assistant_message_id"] == 5002
    assert turn1["next_parent_message_id"] == 5002
    assert turn1["sql_queries"] == ["SELECT COUNT(*) FROM ORDERS"]
    assert turn1["thinking"] == "Analyzing SQL metrics..."
    assert "Thread #901 · Parent Msg #0 -> New Msg #5002" in turn1["thread_indicator"]

    # Verify SQLite table `snowflake_cortex_agent_threads` was created and populated
    sqlite_db = tmp_path / "langflow.db"
    assert sqlite_db.exists()
    conn = sqlite3.connect(str(sqlite_db))
    row = conn.execute(
        "SELECT thread_id, last_user_message_id, last_assistant_message_id FROM snowflake_cortex_agent_threads WHERE agent_fqn = ?",
        ("SNOWFLAKE_LANGFLOW.CORE.SALES_COPILOT",),
    ).fetchone()
    msg_rows = conn.execute("SELECT sender, text FROM message").fetchall()
    conn.close()
    assert row == (901, 5001, 5002)
    assert len(msg_rows) >= 2

    # Simulate SPCS container restart / cold-start: wipe local SQLite and in-memory pointer cache
    conn = sqlite3.connect(str(sqlite_db))
    conn.execute("DELETE FROM snowflake_cortex_agent_threads")
    conn.commit()
    conn.close()
    bridge._cortex_agent_threads_mem.clear()

    # Turn 2 after cold start: must recover thread 901 and last_assistant_message_id=5002 from Snowflake Threads API!
    turn2 = bridge.invoke_cortex_agent(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        agent_name="SALES_COPILOT",
        user_message="Break it down by region",
        flow_id="flow-r6-01",
        session_id="sess-r6-01",
        resume_thread=True,
        thread_continuity_mode="Resume Latest Thread of this Agent",
    )
    assert turn2["ok"] is True
    assert turn2["thread_id"] == 901
    assert turn2["parent_message_id"] == 5002
    assert turn2["user_message_id"] == 5003
    assert turn2["assistant_message_id"] == 5004
    assert turn2["next_parent_message_id"] == 5004

    # Stateless mode (resume_thread=False): omits thread_id and parent_message_id in request_payload
    stateless_turn = bridge.invoke_cortex_agent(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        agent_name="SALES_COPILOT",
        user_message="Stateless ping",
        flow_id="flow-r6-01",
        session_id="sess-r6-01",
        resume_thread=False,
    )
    assert stateless_turn["ok"] is True
    assert stateless_turn["thread_id"] is None
    assert "thread_id" not in stateless_turn["request_payload"]
    assert "parent_message_id" not in stateless_turn["request_payload"]


def test_ma_w2_r6_01_cortex_agent_component_execution_and_role_governance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SnowflakeCortexAgentComponent invoke_agent(), invoke_agent_structured(), and PUBLIC role denial."""
    monkeypatch.setattr(LIVE_BRIDGE, "active_role", "ACCOUNTADMIN")
    comp = SnowflakeCortexAgentComponent()
    comp.database = "SNOWFLAKE_LANGFLOW"
    comp.schema_name = "CORE"
    comp.cortex_agent = "FINANCE_AGENT"
    comp.resume_thread = True
    comp.thread_continuity_mode = "One Thread per Playground Session (Session ID)"
    comp.conversation_thread = START_FRESH_THREAD_LABEL
    comp.user_message = "Check Q3 margin"
    comp.session_id = "sess-fin-01"
    comp.tool_choice = "auto"

    msg_out = comp.invoke_agent()
    parsed = json.loads(msg_out.text)
    assert parsed["ok"] is True
    assert parsed["agent_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.FINANCE_AGENT"
    assert parsed["rest_endpoint"] == "/api/v2/databases/SNOWFLAKE_LANGFLOW/schemas/CORE/agents/FINANCE_AGENT:run"
    assert parsed["thread_id"] is not None

    data_out = comp.invoke_agent_structured()
    assert isinstance(data_out.data, dict)
    assert data_out.data["agent_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.FINANCE_AGENT"

    # Denied role check
    denied = LIVE_BRIDGE.invoke_cortex_agent(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        agent_name="FINANCE_AGENT",
        user_message="Should be blocked",
        role="PUBLIC",
    )
    assert denied["ok"] is False
    assert denied["status_code"] == 403
    assert denied["error_code"] == ErrorCode.AUTHZ_ROLE_DENIED.value


# ---------------------------------------------------------------------------
# MA-W2-R6-02: Native Language Model & Agent Provider Unification & Live /models
# ---------------------------------------------------------------------------


def test_ma_w2_r6_02_live_models_ttl_cache_and_force_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify `catalog_with_metadata` 15s TTL cache, `force_refresh=True` bypass, and live metadata reasoning detection."""
    import contextlib

    standard_langflow.clear_catalog_cache()
    call_counter = {"chat": 0}

    @contextlib.contextmanager
    def _fake_discovery_transport(protocol: str):
        class _FakeResp:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, Any]:
                if protocol == "chat":
                    call_counter["chat"] += 1
                    n = call_counter["chat"]
                    return {
                        "data": [
                            {
                                "id": f"openai-gpt-6-dynamic-v{n}",
                                "metadata": {
                                    "release_status": "qualified",
                                    "expires_at": "2099-12-31T23:59:59Z",
                                    "capabilities": ["chat", "streaming", "tool_calling", "reasoning"],
                                    "reasoning": {"supported": True, "required": True},
                                },
                            },
                            {
                                "id": "grok-4.7",
                                "metadata": {
                                    "release_status": "qualified",
                                    "expires_at": "2099-12-31T23:59:59Z",
                                    "capabilities": ["chat", "streaming", "tool_calling"],
                                    "reasoning": False,
                                },
                            },
                        ]
                    }
                return {"data": []}

        class _FakeClient:
            def get(self, *_args: Any, **_kwargs: Any) -> _FakeResp:
                return _FakeResp()

        yield _FakeClient()

    monkeypatch.setattr(standard_langflow, "discovery_transport", _fake_discovery_transport)

    first = standard_langflow.catalog_with_metadata("chat", force_refresh=False)
    assert "openai-gpt-6-dynamic-v1" in first
    assert call_counter["chat"] == 1

    # Second call within 15s TTL uses cache without hitting transport
    second = standard_langflow.catalog_with_metadata("chat", force_refresh=False)
    assert "openai-gpt-6-dynamic-v1" in second
    assert call_counter["chat"] == 1

    # force_refresh=True bypasses cache immediately and picks up newly published v2 model
    third = standard_langflow.catalog_with_metadata("chat", force_refresh=True)
    assert "openai-gpt-6-dynamic-v2" in third
    assert call_counter["chat"] == 2

    # Verify newly published model with `capabilities: ["reasoning"]` is recognized by resolve_reasoning_options()
    supports_r, req_r = standard_langflow.model_supports_reasoning("chat", "openai-gpt-6-dynamic-v2")
    assert supports_r is True
    assert req_r is True
    r_opts = standard_langflow.resolve_reasoning_options("chat", "openai-gpt-6-dynamic-v2")
    assert r_opts["extra_body"] == {"reasoning_effort": "none"}


def test_ma_w2_r6_02_four_snowflake_providers_and_table_3_2_dynamic_build_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the 4 Snowflake providers, Table 3.2 dynamic reasoning/thinking rules, and saved-model non-replacement."""
    assert SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS == (
        PROVIDER_PROXY_CHAT,
        PROVIDER_PROXY_RESPONSES,
        PROVIDER_PROXY_MESSAGES,
        PROVIDER_PRIVATE_OLLAMA,
    )

    standard_langflow.clear_catalog_cache()
    monkeypatch.setattr(standard_langflow, "discovery_transport", None)

    catalog = get_snowflake_live_language_model_catalog(force_refresh=True)
    providers_found = {opt["provider"] for opt in catalog}
    assert set(SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS).issubset(providers_found)

    # 1. Snowflake Proxy - Chat (/chat/v1) with reasoning-required model (openai-gpt-6-luna)
    build_cfg: dict[str, Any] = {
        "model": {
            "options": [],
            "value": [{"name": "openai-gpt-6-luna", "provider": PROVIDER_PROXY_CHAT}],
        },
        "api_key": {"show": True, "required": True, "value": "secret"},
        "temperature": {"show": True, "value": 0.1},
        "max_tokens": {"show": True, "value": 2048},
    }
    update_snowflake_language_model_build_config(build_cfg, field_name="model", force_refresh=False)
    assert build_cfg["api_key"]["show"] is False
    assert build_cfg["reasoning_effort"]["show"] is True
    assert build_cfg["reasoning_effort"]["value"] == "none"
    assert build_cfg["extended_thinking"]["show"] is False
    assert build_cfg["thinking_budget_tokens"]["show"] is False

    # 2. Snowflake Proxy - Chat (/chat/v1) with non-reasoning model (grok-4.7)
    build_cfg["model"]["value"] = [{"name": "grok-4.7", "provider": PROVIDER_PROXY_CHAT}]
    update_snowflake_language_model_build_config(build_cfg, field_name="model", force_refresh=False)
    assert build_cfg["reasoning_effort"]["show"] is False
    assert build_cfg["extended_thinking"]["show"] is False

    # 3. Snowflake Proxy - Responses (/responses/v1)
    build_cfg["model"]["value"] = [{"name": "openai-gpt-6-astra", "provider": PROVIDER_PROXY_RESPONSES}]
    update_snowflake_language_model_build_config(build_cfg, field_name="model", force_refresh=False)
    assert build_cfg["reasoning_effort"]["show"] is True
    assert build_cfg["reasoning_effort"]["options"] == ["low", "medium", "high"]
    assert build_cfg["reasoning_effort"]["value"] == "medium"
    assert build_cfg["extended_thinking"]["show"] is False

    # 4. Snowflake Proxy - Messages (/messages) with Extended Thinking toggled ON
    build_cfg["model"]["value"] = [{"name": "claude-sonnet-5-5", "provider": PROVIDER_PROXY_MESSAGES}]
    update_snowflake_language_model_build_config(build_cfg, field_name="model", force_refresh=False)
    assert build_cfg["reasoning_effort"]["show"] is False
    assert build_cfg["extended_thinking"]["show"] is True
    assert build_cfg["extended_thinking"]["value"] == "disabled"
    assert build_cfg["thinking_budget_tokens"]["show"] is False

    update_snowflake_language_model_build_config(
        build_cfg, field_value="enabled", field_name="extended_thinking", force_refresh=False
    )
    assert build_cfg["extended_thinking"]["value"] == "enabled"
    assert build_cfg["thinking_budget_tokens"]["show"] is True

    # 5. Snowflake Private - Ollama
    build_cfg["model"]["value"] = [{"name": "llama3.1:8b", "provider": PROVIDER_PRIVATE_OLLAMA}]
    update_snowflake_language_model_build_config(build_cfg, field_name="model", force_refresh=False)
    assert build_cfg["reasoning_effort"]["show"] is False
    assert build_cfg["extended_thinking"]["show"] is False
    assert build_cfg["thinking_budget_tokens"]["show"] is False

    # 6. Saved-model non-replacement safeguard when model is retired/absent from live catalog
    build_cfg["model"]["value"] = [{"name": "retired-gpt-model-2025", "provider": PROVIDER_PROXY_CHAT}]
    update_snowflake_language_model_build_config(build_cfg, field_name="model", force_refresh=False)
    assert build_cfg["model"]["value"][0]["name"] == "retired-gpt-model-2025"
    assert (
        "Saved model 'retired-gpt-model-2025' is no longer in the live published catalog; choose a model explicitly"
        in build_cfg["model"]["info"]
    )

    reg_status = register_snowflake_language_model_providers()
    assert reg_status["providers"] == list(SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS)


# ---------------------------------------------------------------------------
# MA-W2-R6-03: Multi-Tool is_list=True on Agent & Real StructuredTool in Cortex Analyst / Search / Agent
# ---------------------------------------------------------------------------


def test_ma_w2_r6_03_structured_tools_for_cortex_analyst_search_and_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Cortex Analyst, Cortex Search, and Cortex Agent return real StructuredTool instances with Pydantic schemas."""
    monkeypatch.setattr(LIVE_BRIDGE, "active_role", "ACCOUNTADMIN")

    ca = SnowflakeCortexAnalystComponent()
    ca.database = "SNOWFLAKE_LANGFLOW"
    ca.schema_name = "CORE"
    ca.source_mode = "Semantic View"
    ca.semantic_source = "SALES_SEMANTIC_VIEW"
    ca.question = "Default analyst question"

    cs = SnowflakeCortexSearchComponent()
    cs.database = "SNOWFLAKE_LANGFLOW"
    cs.schema_name = "CORE"
    cs.search_service = "DOCS_SEARCH_SVC"
    cs.query = "Default search query"
    cs.limit = 4

    ag = SnowflakeCortexAgentComponent()
    ag.database = "SNOWFLAKE_LANGFLOW"
    ag.schema_name = "CORE"
    ag.cortex_agent = "OPS_COPILOT"
    ag.user_message = "Default agent prompt"

    combined_tools = [*ca.to_toolkit(), *cs.to_toolkit(), *ag.to_toolkit()]
    assert len(combined_tools) == 3

    tool_names = [t.name for t in combined_tools]
    assert tool_names == [
        "cortex_analyst_snowflake_langflow_core_sales_semantic_view",
        "cortex_search_snowflake_langflow_core_docs_search_svc",
        "cortex_agent_snowflake_langflow_core_ops_copilot",
    ]
    assert len(set(tool_names)) == 3

    for t in combined_tools:
        assert isinstance(t, StructuredTool)
        assert issubclass(t.args_schema, BaseModel)
        assert t.metadata.get("tool_mode") is True
        assert t.metadata.get("rest_endpoint")

    assert combined_tools[0].args_schema is CortexAnalystToolSchema
    assert combined_tools[1].args_schema is CortexSearchToolSchema
    assert combined_tools[2].args_schema is CortexAgentToolSchema

    # Verify Cortex Analyst tool execution with dict & string
    ca_res_dict = combined_tools[0].invoke({"question": "What was Q4 revenue?"})
    assert ca_res_dict["question"] == "What was Q4 revenue?"
    assert ca_res_dict["rest_endpoint"] == "/api/v2/cortex/analyst/message"

    # Verify Cortex Search tool invokes run_search() (NOT get_search_metadata()) and updates query & limit
    cs_res = combined_tools[1].invoke({"query": "Snowflake dynamic tables lag", "limit": 3})
    assert cs_res["request_payload"]["query"] == "Snowflake dynamic tables lag"
    assert cs_res["request_payload"]["limit"] == 3
    assert cs_res["rest_endpoint"] == "/api/v2/databases/SNOWFLAKE_LANGFLOW/schemas/CORE/cortex-search-services/DOCS_SEARCH_SVC:query"
    assert "result" in cs_res
    assert "search_preview_sql" not in cs_res

    cs_res_str = combined_tools[1].invoke("Iceberg catalog integration")
    assert cs_res_str["request_payload"]["query"] == "Iceberg catalog integration"

    # Verify Cortex Agent tool execution
    ag_res = combined_tools[2].invoke({"user_message": "Summarize pipeline SLA"})
    assert ag_res["user_message"] == "Summarize pipeline SLA"
    assert ag_res["rest_endpoint"] == "/api/v2/databases/SNOWFLAKE_LANGFLOW/schemas/CORE/agents/OPS_COPILOT:run"
    assert sanitize_tool_identifier("@DB.SCH.MODEL.yaml") == "db_sch_model"


# ---------------------------------------------------------------------------
# MA-W2-R6-04: Models & Agents Sidebar Cleanup & Message History Deduplication
# ---------------------------------------------------------------------------


def test_ma_w2_r6_04_models_and_agents_7_canonical_sidebar_components() -> None:
    """Verify install_live_hooks_in_container.py enforces the exact 7 canonical Models & Agents components."""
    hook_script = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    assert '"A2AAgent"' in hook_script
    assert '"Agent"' in hook_script
    assert '"SnowflakeCortexAgent"' in hook_script
    assert '"EmbeddingModel"' in hook_script
    assert '"LanguageModelComponent"' in hook_script
    assert '"MessageHistory"' in hook_script
    assert '"Prompt Template"' in hook_script
    assert "register_snowflake_language_model_providers()" in hook_script

    for removed_dup in (
        "StandardChat",
        "StandardResponses",
        "StandardMessages",
        "PrivateOllama",
        "SnowflakeCortexAgentA2A",
        "Memory",
        "SnowflakeMessageHistory",
        "MCPTools",
    ):
        assert f'"{removed_dup}"' in hook_script

    # Ensure backward-compatible Python class name on SnowflakeMessageHistoryNativeComponent is preserved
    assert SnowflakeMessageHistoryNativeComponent.name == "SnowflakeMessageHistory"
    assert SnowflakeMessageHistoryNativeComponent.display_name == "Message History"
    assert SnowflakeMessageHistoryNativeComponent.icon == "Snowflake"


# ---------------------------------------------------------------------------
# MA-W2-R6-05: LANGFLOW_A2A_ENABLED=true, A2A Agent Internal/External & A2A v0.3.0 Server Bridge
# ---------------------------------------------------------------------------


def test_ma_w2_r6_05_a2a_enabled_and_internal_external_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify LANGFLOW_A2A_ENABLED=true and SnowflakeA2AAgentComponent Internal/External modes + anti-recursion."""
    dockerfile_text = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "LANGFLOW_A2A_ENABLED=true" in dockerfile_text

    monkeypatch.setattr(LIVE_BRIDGE, "active_role", "ACCOUNTADMIN")
    registry = ServeAsA2AAgentRegistry(
        role_privileges={
            "ACCOUNTADMIN": {
                ("EDIT_FLOW", "flow-alpha"),
                ("PUBLISH_A2A", "flow-alpha"),
                ("INVOKE_A2A", "flow-alpha"),
            }
        },
    )
    registry.register_flow_topology(
        flow_id="flow-alpha",
        owner_role="ACCOUNTADMIN",
        has_chat_input=True,
        has_chat_output=True,
    )
    registry.save_card_metadata(
        flow_id="flow-alpha",
        active_role="ACCOUNTADMIN",
        card=A2AAgentCard(flow_id="flow-alpha", name="Order Triage Flow", description="Triages orders"),
    )
    registry.publish_flow(flow_id="flow-alpha", active_role="ACCOUNTADMIN")

    a2a_comp = SnowflakeA2AAgentComponent(a2a_registry=registry, max_depth=3)
    internal_opts = a2a_comp.list_internal_agents()
    assert any(opt.startswith("[Langflow Flow] Order Triage Flow (flow-alpha)") for opt in internal_opts)
    assert any(opt.startswith("[Cortex Agent] ") for opt in internal_opts)

    # 1. Internal invocation of [Langflow Flow]
    flow_res = a2a_comp.invoke_internal(
        selected_agent="[Langflow Flow] Order Triage Flow (flow-alpha)",
        message="Check order #123",
        caller_effective_role="ACCOUNTADMIN",
    )
    assert flow_res["flow_id"] == "flow-alpha"
    assert flow_res["effective_role"] == "ACCOUNTADMIN"

    # 2. Internal anti-recursion guard (max_depth=3 or cycle)
    with pytest.raises(SnowflakeComponentError) as exc_info:
        a2a_comp.invoke_internal(
            selected_agent="[Langflow Flow] Order Triage Flow (flow-alpha)",
            message="Loop",
            caller_effective_role="ACCOUNTADMIN",
            call_chain=("flow-1", "flow-2", "flow-3"),
        )
    assert exc_info.value.http_status == 400
    assert "max_depth=3" in str(exc_info.value)

    # 3. Internal invocation of [Cortex Agent] <DB>.<SCHEMA>.<NAME>
    ca_internal_res = a2a_comp.invoke_internal(
        selected_agent="[Cortex Agent] SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_AGENT",
        message="Summarize KPIs",
        caller_effective_role="ACCOUNTADMIN",
        session_id="sess-a2a-int",
    )
    assert ca_internal_res["ok"] is True
    assert ca_internal_res["agent_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_AGENT"

    # 4. External Quick Fill & invocation (A2A Bridge URL + Direct :run URL + Flow URL)
    qf_map = a2a_comp.list_quick_fill_endpoints(base_url="http://127.0.0.1:7860")
    assert any("/api/v1/snowflake/a2a/cortex-agents/" in url for url in qf_map.values())

    ext_bridge_res = a2a_comp.invoke_external(
        agent_url="http://127.0.0.1:7860/api/v1/snowflake/a2a/cortex-agents/SNOWFLAKE_LANGFLOW/CORE/EXAMPLE_AGENT",
        message="Bridge ping",
        caller_effective_role="ACCOUNTADMIN",
        session_id="445566",
    )
    assert ext_bridge_res["ok"] is True
    assert ext_bridge_res["protocol"] == "A2A/0.3.0"
    assert ext_bridge_res["task"]["status"]["state"] == "completed"

    ext_run_res = a2a_comp.invoke_external(
        agent_url="/api/v2/databases/SNOWFLAKE_LANGFLOW/schemas/CORE/agents/EXAMPLE_AGENT:run",
        message="Direct REST :run ping",
        caller_effective_role="ACCOUNTADMIN",
    )
    assert ext_run_res["ok"] is True
    assert ext_run_res["agent_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_AGENT"


def test_ma_w2_r6_05_fastapi_a2a_v030_server_bridge_and_threads_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify FastAPI routes for A2A v0.3.0 Agent Card, JSON-RPC (`message/send`, `tasks/get`, `tasks/cancel`), and Threads."""
    monkeypatch.setattr(LIVE_BRIDGE, "active_role", "ACCOUNTADMIN")
    client = _make_client()

    # 1. GET /.well-known/agent-card.json
    card_resp = client.get(
        "/api/v1/snowflake/a2a/cortex-agents/SNOWFLAKE_LANGFLOW/CORE/REVENUE_AGENT/.well-known/agent-card.json"
    )
    assert card_resp.status_code == 200
    card = card_resp.json()
    assert card["name"] == "SNOWFLAKE_LANGFLOW.CORE.REVENUE_AGENT"
    assert card["protocolVersion"] == "0.3.0"
    assert card["preferredTransport"] == "JSONRPC"
    assert card["url"].endswith("/api/v1/snowflake/a2a/cortex-agents/SNOWFLAKE_LANGFLOW/CORE/REVENUE_AGENT/jsonrpc")
    assert card["metadata"]["describe_sql"] == "DESCRIBE AGENT SNOWFLAKE_LANGFLOW.CORE.REVENUE_AGENT"

    # 2. POST /jsonrpc (message/send)
    send_resp = client.post(
        "/api/v1/snowflake/a2a/cortex-agents/SNOWFLAKE_LANGFLOW/CORE/REVENUE_AGENT/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "id": "rpc-101",
            "method": "message/send",
            "params": {
                "id": "task-rev-101",
                "message": {
                    "role": "user",
                    "parts": [{"kind": "text", "text": "Compute Q3 ARR"}],
                },
            },
        },
    )
    assert send_resp.status_code == 200
    rpc_out = send_resp.json()
    assert rpc_out["jsonrpc"] == "2.0"
    assert rpc_out["id"] == "rpc-101"
    assert rpc_out["result"]["id"] == "task-rev-101"
    assert rpc_out["result"]["status"]["state"] == "completed"
    assert rpc_out["result"]["metadata"]["agent_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.REVENUE_AGENT"

    # 3. POST /jsonrpc (tasks/get)
    get_resp = client.post(
        "/api/v1/snowflake/a2a/cortex-agents/SNOWFLAKE_LANGFLOW/CORE/REVENUE_AGENT/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "id": "rpc-102",
            "method": "tasks/get",
            "params": {"id": "task-rev-101"},
        },
    )
    assert get_resp.status_code == 200
    assert get_resp.json()["result"]["id"] == "task-rev-101"

    # 4. POST /jsonrpc (tasks/cancel)
    cancel_resp = client.post(
        "/api/v1/snowflake/a2a/cortex-agents/SNOWFLAKE_LANGFLOW/CORE/REVENUE_AGENT/jsonrpc",
        json={
            "jsonrpc": "2.0",
            "id": "rpc-103",
            "method": "tasks/cancel",
            "params": {"id": "task-rev-101"},
        },
    )
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["result"]["status"]["state"] == "canceled"

    # 5. GET /api/v1/snowflake/cortex/threads & /api/v1/snowflake/language-models
    threads_resp = client.get("/api/v1/snowflake/cortex/threads")
    assert threads_resp.status_code == 200
    assert threads_resp.json()["ok"] is True

    lm_resp = client.get("/api/v1/snowflake/language-models")
    assert lm_resp.status_code == 200
    assert lm_resp.json()["ok"] is True
    assert lm_resp.json()["providers"] == list(SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS)
