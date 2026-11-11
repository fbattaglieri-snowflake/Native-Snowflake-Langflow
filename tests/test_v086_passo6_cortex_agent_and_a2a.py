"""PASSO 6 items 3-4: BUG-CA-11/12/13 (Cortex Agent) and BUG-A2A-01/05 (A2A Agent + JSON-RPC bridge)."""

from __future__ import annotations

import ast
import pathlib
from typing import Any

import pytest

from lfx_snowflake import a2a_call_chain as chain
from lfx_snowflake.components.a2a_and_cortex_agent import SnowflakeA2AAgentComponent
from lfx_snowflake.components.files_and_stage import SnowflakeComponentError
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.native_category_components.models_and_agents.snowflake_cortex_agent import (
    START_FRESH_THREAD_LABEL,
    SnowflakeCortexAgentComponent,
)

REPO = pathlib.Path(__file__).resolve().parents[1]


@pytest.fixture
def admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Offline DESCRIBE AGENT emulation (same as test_models_and_agents_w2_r6.py); grants are out of scope here."""
    from lfx_snowflake import fastapi_live_bridge as _flb

    monkeypatch.setattr(LIVE_BRIDGE, "active_role", "ACCOUNTADMIN")
    monkeypatch.setattr(_flb.LiveSnowflakeBridge, "_cortex_agent_access", lambda self, *a, **k: (None, {"ok": True, "rows": [{}]}))


def _component(mode: str) -> SnowflakeCortexAgentComponent:
    comp = SnowflakeCortexAgentComponent()
    comp.database = "SNOWFLAKE_LANGFLOW"
    comp.schema_name = "CORE"
    comp.cortex_agent = "FINANCE_AGENT"
    comp.resume_thread = True
    comp.thread_continuity_mode = mode
    comp.conversation_thread = START_FRESH_THREAD_LABEL
    comp.session_id = "sess-p6-ca11"
    comp.tool_choice = "auto"
    return comp


# --------------------------------------------------------------------- BUG-CA-11
@pytest.mark.parametrize(
    "mode",
    ["One Thread per Playground Session (Session ID)", "Resume Latest Thread of this Agent"],
)
def test_ca11_continuity_mode_does_not_force_new_thread_each_turn(admin: None, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    created: list[int] = []
    real_create = LIVE_BRIDGE.create_cortex_thread

    def _counting_create(**kwargs: Any) -> dict[str, Any]:
        res = real_create(**kwargs)
        created.append(int(res["thread_id"]))
        return res

    monkeypatch.setattr(LIVE_BRIDGE, "create_cortex_thread", _counting_create)
    tids = set()
    for turn in range(3):
        comp = _component(mode)
        comp.session_id = f"sess-p6-{mode[:4]}"
        comp.user_message = f"turn {turn}"
        tids.add(comp.invoke_agent_structured().data["thread_id"])
    assert len(created) <= 1
    assert len(tids) == 1


def test_ca11_select_mode_start_fresh_still_creates(admin: None, monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[int] = []
    real_create = LIVE_BRIDGE.create_cortex_thread
    monkeypatch.setattr(
        LIVE_BRIDGE, "create_cortex_thread", lambda **kw: created.append(1) or real_create(**kw)
    )
    for turn in range(2):
        comp = _component("Select Specific Thread / Start Fresh Thread")
        comp.user_message = f"fresh {turn}"
        comp.invoke_agent_structured()
    assert len(created) == 2


# --------------------------------------------------------------------- BUG-CA-12
def test_ca12_clean_message_and_single_execution(admin: None, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}
    real = LIVE_BRIDGE.invoke_cortex_agent

    def _counting(**kwargs: Any) -> dict[str, Any]:
        calls["n"] += 1
        return real(**kwargs)

    monkeypatch.setattr(LIVE_BRIDGE, "invoke_cortex_agent", _counting)
    comp = _component("One Thread per Playground Session (Session ID)")
    comp.user_message = "Margin?"
    msg = comp.invoke_agent()
    data = comp.invoke_agent_structured()
    assert calls["n"] == 1
    assert msg.text == str(data.data.get("text") or "")
    assert not msg.text.lstrip().startswith("{")
    comp.user_message = "Another question"
    comp.invoke_agent()
    assert calls["n"] == 2


def test_ca12_cache_never_returns_previous_turn(admin: None, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}
    real = LIVE_BRIDGE.invoke_cortex_agent

    def _counting(**kwargs: Any) -> dict[str, Any]:
        calls["n"] += 1
        return real(**kwargs)

    monkeypatch.setattr(LIVE_BRIDGE, "invoke_cortex_agent", _counting)
    comp = _component("One Thread per Playground Session (Session ID)")
    comp.user_message = "same prompt"
    comp.invoke_agent()
    comp.invoke_agent()  # same port again = a new turn
    assert calls["n"] == 2
    comp.invoke_agent_structured()  # other port of the latest turn: reused
    assert calls["n"] == 2
    comp.thread_continuity_mode = "Resume Latest Thread of this Agent"
    comp.invoke_agent_structured()  # config changed -> fresh turn
    assert calls["n"] == 3
    monkeypatch.setattr(LIVE_BRIDGE, "context_epoch", int(LIVE_BRIDGE.context_epoch or 0) + 1)
    comp.invoke_agent()  # role/context epoch changed -> fresh turn
    assert calls["n"] == 4


# --------------------------------------------------------------------- BUG-CA-13
def test_ca13_error_raises(admin: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        LIVE_BRIDGE,
        "invoke_cortex_agent",
        lambda **kw: {"ok": False, "error_code": "TECHNICAL_EXECUTION_ERROR", "message": "399518 tool missing"},
    )
    comp = _component("Resume Latest Thread of this Agent")
    comp.user_message = "x"
    with pytest.raises(ValueError, match="399518"):
        comp.invoke_agent()


# --------------------------------------------------------------------- call chain helper
def test_call_chain_helpers() -> None:
    assert chain.parse_call_chain("a, b,,bad value,c") == ("a", "b", "c")
    assert chain.extend_call_chain(("a",), "b", 3) == ("a", "b")
    with pytest.raises(chain.A2ARecursionError):
        chain.extend_call_chain(("a", "b"), "a", 5)
    with pytest.raises(chain.A2ARecursionError):
        chain.extend_call_chain(("a", "b", "c"), "d", 3)
    assert chain.clamp_max_depth(999) == chain.HARD_MAX_DEPTH
    with chain.bind_call_chain(("x",), 2):
        assert chain.inbound_call_chain() == ("x",)
        assert chain.effective_max_depth(10) == 2
    assert chain.inbound_call_chain() == ()


def test_call_chain_asgi_middleware_binds_headers() -> None:
    import asyncio

    seen: list[tuple[Any, ...]] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append((chain.inbound_call_chain(), chain.effective_max_depth(9)))

    mw = chain.CallChainASGIMiddleware(app)
    with_headers = {"type": "http", "headers": [(b"x-snowflake-a2a-call-chain", b"flow-a,flow-b"), (b"x-snowflake-a2a-max-depth", b"4")]}
    no_headers = {"type": "http", "headers": []}

    async def _run() -> None:
        await mw(with_headers, None, None)
        await mw(no_headers, None, None)

    asyncio.run(_run())
    assert seen == [(("flow-a", "flow-b"), 4), ((), 9)]
    assert chain.inbound_call_chain() == ()


def test_middleware_does_not_inherit_ambient_context() -> None:
    import asyncio

    seen: list[tuple[Any, ...]] = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append((chain.inbound_call_chain(), chain.effective_max_depth(9)))

    with chain.bind_call_chain(("leaked",), 2):
        asyncio.run(chain.CallChainASGIMiddleware(app)({"type": "http", "headers": []}, None, None))
        assert chain.inbound_call_chain() == ("leaked",)
    assert seen == [((), 9)]


def test_bind_call_chain_never_widens_parent_depth() -> None:
    with chain.bind_call_chain(("a",), 2):
        with chain.bind_call_chain(("a", "b"), 10):
            assert chain.effective_max_depth(10) == 2
        with chain.bind_call_chain(("a",)):
            assert chain.effective_max_depth(10) == 2
        with chain.bind_call_chain(("a",), 1):
            assert chain.effective_max_depth(10) == 1
    assert chain.effective_max_depth(5) == 5


@pytest.mark.parametrize(
    "url,hop",
    [
        ("https://h.example:8443/api/v1/snowflake/a2a/cortex-agents/db/sch/agent_x", "DB.SCH.AGENT_X"),
        ("https://h.example/api/v1/snowflake/a2a/cortex-agents/DB/SCH/A/jsonrpc", "DB.SCH.A"),
        ("/api/v2/databases/d/schemas/s/agents/a:run", "D.S.A"),
        ("http://x/api/v1/a2a/0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdef"),
        ("https://h.example/api/v1/a2a/flows/my-flow/.well-known/agent-card.json", "my-flow"),
        ("https://Agents.Example.com/team/a/", "https://agents.example.com:443/team/a"),
        ("http://agents.example.com:8080/team/b?x=1", "http://agents.example.com:8080/team/b"),
    ],
)
def test_hop_from_url_precise(url: str, hop: str) -> None:
    assert chain.hop_from_url(url) == hop


def test_hop_from_url_distinguishes_paths_and_bridge_agents() -> None:
    a = chain.hop_from_url("https://h/api/v1/snowflake/a2a/cortex-agents/D/S/A1")
    b = chain.hop_from_url("https://h/api/v1/snowflake/a2a/cortex-agents/D/S/A2")
    assert a != b and "cortex-agents" not in a
    assert chain.hop_from_url("https://h/agent/one") != chain.hop_from_url("https://h/agent/two")
    assert chain.extend_call_chain((), chain.hop_from_url("https://h.example/x"), 3) == ("https://h.example:443/x",)


# --------------------------------------------------------------------- BUG-A2A-01
def test_a2a01_external_loop_back_to_caller_blocked(admin: None) -> None:
    comp = SnowflakeA2AAgentComponent(max_depth=3)
    with pytest.raises(SnowflakeComponentError, match="recursion"):
        comp.invoke_external(
            agent_url="http://127.0.0.1:7860/api/v1/a2a/flow-a",
            message="loop",
            flow_id="flow-b",
            call_chain=("flow-a",),
            caller_effective_role="ACCOUNTADMIN",
        )


def test_a2a01_internal_respects_inbound_chain_and_depth(admin: None) -> None:
    comp = SnowflakeA2AAgentComponent(max_depth=3)
    with chain.bind_call_chain(("flow-a", "flow-b"), 3):
        with pytest.raises(SnowflakeComponentError):
            comp.invoke_internal(selected_agent="[Langflow Flow] A (flow-a)", message="m", flow_id="flow-b", caller_effective_role="ACCOUNTADMIN")
        with pytest.raises(SnowflakeComponentError):
            comp.invoke_internal(selected_agent="[Langflow Flow] C (flow-c)", message="m", flow_id="flow-x", caller_effective_role="ACCOUNTADMIN")
    ok = comp.invoke_internal(selected_agent="[Langflow Flow] C (flow-c)", message="m", flow_id="flow-b", caller_effective_role="ACCOUNTADMIN")
    assert ok["call_chain"] == ["flow-b", "flow-c"]


def test_a2a01_bridge_metadata_cycle_blocked(admin: None) -> None:
    res = LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc(
        "SNOWFLAKE_LANGFLOW",
        "CORE",
        "REVENUE_AGENT",
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "message/send",
            "params": {
                "message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]},
                "metadata": {chain.CALL_CHAIN_METADATA_KEY: ["flow-a", "SNOWFLAKE_LANGFLOW.CORE.REVENUE_AGENT"]},
            },
        },
    )
    assert res["error"]["code"] == -32000
    assert res["error"]["data"]["error_code"] == "A2A_RECURSION_BLOCKED"


# --------------------------------------------------------------------- BUG-A2A-05
def _send(task_id: str) -> dict[str, Any]:
    return LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc(
        "SNOWFLAKE_LANGFLOW",
        "CORE",
        "REVENUE_AGENT",
        {
            "jsonrpc": "2.0",
            "id": "s",
            "method": "message/send",
            "params": {"message": {"role": "user", "taskId": task_id, "parts": [{"kind": "text", "text": "hi"}]}},
        },
    )


def _rpc(method: str, task_id: str) -> dict[str, Any]:
    return LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc(
        "SNOWFLAKE_LANGFLOW", "CORE", "REVENUE_AGENT", {"jsonrpc": "2.0", "id": "r", "method": method, "params": {"id": task_id}}
    )


def test_a2a05_deepcopy_not_found_and_not_cancelable(admin: None) -> None:
    sent = _send("task-p6-05")
    assert sent["result"]["status"]["state"] == "completed"
    assert sent["result"]["metadata"][chain.CALL_CHAIN_METADATA_KEY][-1] == "SNOWFLAKE_LANGFLOW.CORE.REVENUE_AGENT"
    sent["result"]["status"]["state"] = "tampered"
    got = _rpc("tasks/get", "task-p6-05")
    assert got["result"]["status"]["state"] == "completed"
    got["result"]["status"]["state"] = "tampered"
    assert _rpc("tasks/get", "task-p6-05")["result"]["status"]["state"] == "completed"

    cancel = _rpc("tasks/cancel", "task-p6-05")
    assert cancel["error"]["code"] == -32002
    assert _rpc("tasks/get", "task-p6-05")["result"]["status"]["state"] == "completed"

    assert _rpc("tasks/cancel", "no-such-task")["error"]["code"] == -32001
    assert _rpc("tasks/get", "no-such-task")["error"]["code"] == -32001
    assert _rpc("tasks/get", "987654321012345")["error"]["code"] == -32001  # unknown numeric thread


def test_a2a05_cancel_non_terminal_task(admin: None) -> None:
    LIVE_BRIDGE._a2a_cortex_tasks_mem["task-p6-working"] = {
        "task": {"id": "task-p6-working", "kind": "task", "status": {"state": "working"}},
        "agent_fqn": "SNOWFLAKE_LANGFLOW.CORE.REVENUE_AGENT",
    }
    res = _rpc("tasks/cancel", "task-p6-working")
    assert res["result"]["status"]["state"] == "canceled"
    assert _rpc("tasks/cancel", "task-p6-working")["error"]["code"] == -32002


# --------------------------------------------------------------------- installer injected code
def test_installer_a2a_block_v2() -> None:
    src = (REPO / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    ast.parse(src)
    start = src.index("a2a_patch_block = '''") + len("a2a_patch_block = '''")
    block = src[start : src.index("\n'''", start)].replace("\\\\", "\\")
    ast.parse("class _X:\n" + block)
    for needle in (
        "SFLF_A2A_PATCH_V2",
        "_sflf_selected_agent",
        "bind_call_chain(next_chain, depth)",
        "outbound_headers(",
        "outbound_metadata(",
        'build_config["agent_name_selected"]["combobox"] = True',
    ):
        assert needle in block, needle
    assert "apply_delimited_class_block(a2a_txt, a2a_patch_block, *A2A_BLOCK_MARKERS)" in src
    assert 'a2a_txt.find("\\n\\n    async def _populate_internal_agents(")' not in src
    assert "hop_from_url(raw_url)" in block


# --------------------------------------------------------------------- installer delimited block (A6 veto 1)
_UPSTREAM = """class A2AAgentComponent(Component):
    async def update_build_config(self, build_config, field_value, field_name=None):
        await self._populate_internal_agents(build_config)
        return build_config

    async def _populate_internal_agents(self, build_config: dotdict) -> None:
        \"\"\"Fill the Internal dropdown with A2A agents published in the caller's folder.\"\"\"
        return None

    def _selected_agent_flow_id(self):
        return None

    async def _run_target_isolated(self, graph, flow_id: str):
        return None

    @staticmethod
    def _reply_from_run(run_outputs: list) -> str:
        return ""

    async def _call_external_agent(self):
        return None
"""


def _installer():
    import importlib.util

    spec = importlib.util.spec_from_file_location("_inst", REPO / "scripts" / "install_live_hooks_in_container.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_installer_block_preserves_upstream_and_is_idempotent() -> None:
    inst = _installer()
    block = "    def _sflf_selected_agent(self) -> str:\n        return ''\n\n    def _populate_quick_fill_endpoints(self, bc):\n        return {}\n"
    once = inst.apply_delimited_class_block(_UPSTREAM, block, *inst.A2A_BLOCK_MARKERS)
    twice = inst.apply_delimited_class_block(once, block, *inst.A2A_BLOCK_MARKERS)
    assert once == twice
    for name in ("_selected_agent_flow_id", "_run_target_isolated", "_reply_from_run", "_call_external_agent"):
        assert f"def {name}(" in once
    assert once.count(inst.A2A_BLOCK_MARKERS[0]) == 1 and once.count(inst.A2A_BLOCK_MARKERS[1]) == 1
    assert once.startswith(_UPSTREAM.rstrip("\n"))
    ast.parse(once)
    # Updated block replaces exactly the delimited span.
    updated = inst.apply_delimited_class_block(once, block.replace("return {}", "return {'v': 2}"), *inst.A2A_BLOCK_MARKERS)
    assert "{'v': 2}" in updated and updated.count("_populate_quick_fill_endpoints") == 1
    assert updated.startswith(_UPSTREAM.rstrip("\n"))


@pytest.mark.parametrize("legacy_head", [
    "    # SFLF_A2A_PATCH_V2 (BUG-A2A-01 call_chain/max_depth, agent_name_selected UI+API)\n",
    "    async def _populate_internal_agents(self, build_config: dotdict) -> None:\n"
    "        \"\"\"Fill the Internal dropdown with both published Langflow A2A flows and Cortex Agents.\"\"\"\n"
    "        return None\n",
])
def test_installer_upgrades_legacy_block_without_touching_upstream(legacy_head: str) -> None:
    inst = _installer()
    legacy = _UPSTREAM.rstrip("\n") + "\n\n" + legacy_head + "\n    def _populate_quick_fill_endpoints(self, bc):\n        return {}\n"
    out = inst.apply_delimited_class_block(legacy, "    def _sflf_x(self):\n        return 1\n", *inst.A2A_BLOCK_MARKERS)
    assert out.startswith(_UPSTREAM.rstrip("\n"))
    assert "_populate_quick_fill_endpoints" not in out and "def _sflf_x(" in out
    assert "Fill the Internal dropdown with A2A agents published" in out  # upstream method kept
    ast.parse(out)


def test_installer_refuses_unterminated_marker() -> None:
    inst = _installer()
    with pytest.raises(RuntimeError):
        inst.apply_delimited_class_block(_UPSTREAM + inst.A2A_BLOCK_MARKERS[0] + "    x = 1\n", "    y = 2\n", *inst.A2A_BLOCK_MARKERS)
