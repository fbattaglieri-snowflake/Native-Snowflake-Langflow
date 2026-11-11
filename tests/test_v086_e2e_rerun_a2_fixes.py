"""v0.8.6 E2E rerun fixes (A2): caller-mode role (T2), guard refusal as component error (T1), stale flows."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from lfx_snowflake import identity as idm

ROOT = Path(__file__).resolve().parents[1]
FLOW = "11111111-2222-3333-4444-555555555555"


def _capture_payload(monkeypatch, tmp_path, flb):
    token_file = tmp_path / "token"
    token_file.write_text("svc")
    monkeypatch.setattr(flb, "SPCS_TOKEN_PATH", token_file)
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    sent: list[tuple[dict, dict]] = []

    class _Resp:
        status_code = 200
        content = b"{}"

        def json(self):
            return {"data": [], "resultSetMetaData": {"rowType": []}}

    class _Client:
        def post(self, url, headers=None, json=None, timeout=None):
            sent.append((headers, json))
            return _Resp()

    monkeypatch.setattr(flb.LIVE_BRIDGE, "_http_client", _Client())
    return sent


@pytest.fixture
def caller_bridge(monkeypatch, tmp_path):
    from lfx_snowflake import fastapi_live_bridge as flb

    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")
    monkeypatch.setattr(flb.LIVE_BRIDGE, "_role_sessions", {})
    monkeypatch.setitem(flb.LIVE_BRIDGE.__dict__, "_base_active_role", "SVC_ROLE")
    sent = _capture_payload(monkeypatch, tmp_path, flb)
    return flb, sent


def _as_alice_in_flow(flb):
    return (
        idm.bind_identity(idm.RequestIdentity(sf_user="ALICE", caller_token="ctok")),
        idm.bind_request_mode(idm.MODE_CALLER),
        flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW),
    )


def _reset(flb, toks):
    flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(toks[2])
    idm.reset_request_mode(toks[1])
    idm.reset_identity(toks[0])


# --- T2: caller's rights must send the selected role, not the token default (ACCOUNTADMIN) ----------------


def test_t2_viewer_selected_caller_mode_sends_viewer_role(caller_bridge, monkeypatch) -> None:
    """Repro T2: VIEWER selected via switch_role, caller mode, a table only ACCOUNTADMIN can read.

    Before the fix the body had no ``role`` so Snowflake used the token default role (ACCOUNTADMIN) while
    the response reported VIEWER. Now the body carries role=VIEWER, so Snowflake itself denies the read.
    """
    flb, sent = caller_bridge
    toks = _as_alice_in_flow(flb)
    try:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(flb.LIVE_BRIDGE, "available_roles", ["VIEWER", "ACCOUNTADMIN"])
            mp.setattr(flb.LIVE_BRIDGE, "execute_sql", lambda *a, **k: {"ok": True, "rows": [{}]})
            res = flb.LIVE_BRIDGE.switch_role("VIEWER", flow_id=FLOW)
        assert res["ok"] is True and res["active_role"] == "VIEWER"
        out = flb.LIVE_BRIDGE.execute_sql("SELECT * FROM ADMIN_DB.SECRET.ONLY_ACCOUNTADMIN_TABLE")
    finally:
        _reset(flb, toks)
    body = sent[-1][1]
    assert body["role"] == "VIEWER"
    assert out["effective_role"] == "VIEWER"
    assert sent[-1][0]["Authorization"] == "Bearer svc.ctok"  # still the caller's token
    assert body["statement"].startswith("USE SECONDARY ROLES NONE;")  # primary role alone authorizes


def test_t2_selection_is_per_user_and_never_the_service_role(caller_bridge) -> None:
    flb, sent = caller_bridge
    toks = _as_alice_in_flow(flb)
    try:
        flb.LIVE_BRIDGE._set_session_role("VIEWER", FLOW)
    finally:
        _reset(flb, toks)
    bob = (
        idm.bind_identity(idm.RequestIdentity(sf_user="BOB", caller_token="btok")),
        idm.bind_request_mode(idm.MODE_CALLER),
        flb.CURRENT_SNOWFLAKE_FLOW_ID.set(FLOW),
    )
    try:
        flb.LIVE_BRIDGE.execute_sql("SELECT 1")  # BOB selected nothing: token default, never SVC_ROLE/VIEWER
    finally:
        _reset(flb, bob)
    assert "role" not in sent[-1][1]


def test_t2_explicit_role_wins_over_selection(caller_bridge) -> None:
    flb, sent = caller_bridge
    toks = _as_alice_in_flow(flb)
    try:
        flb.LIVE_BRIDGE._set_session_role("VIEWER", FLOW)
        flb.LIVE_BRIDGE.execute_sql("SELECT 1", role="ANALYST")
        flb.LIVE_BRIDGE.execute_sql("SELECT 2", role="")  # explicit token default
    finally:
        _reset(flb, toks)
    assert sent[0][1]["role"] == "ANALYST"
    assert "role" not in sent[1][1]


# --- T1: flow-context refusal surfaces as a component error ------------------------------------------------


def test_t1_guard_refusal_raises_component_error() -> None:
    from lfx_snowflake import flow_context_guard as g

    denial = {"ok": False, "error_code": g.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT, "message": "DB.S.T is outside the flow context."}
    with pytest.raises(g.FlowContextDenied) as exc:
        g.raise_if_flow_context_denied(denial)
    assert "AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT" in str(exc.value) and "DB.S.T" in str(exc.value)
    assert exc.value.error_code == g.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT
    other = {"ok": False, "error_code": "SNOWFLAKE_ERROR"}
    assert g.raise_if_flow_context_denied(other) is other


def test_t1_component_wrapper_raises_on_guard_refusal(monkeypatch) -> None:
    from lfx_snowflake import flow_context_guard as g
    from lfx_snowflake.native_category_components.data_source import snowflake_views as mod

    denial = {"ok": False, "error_code": g.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT, "message": "outside"}
    monkeypatch.setattr(mod.LIVE_BRIDGE, "execute_sql", lambda *a, **k: denial)
    with pytest.raises(g.FlowContextDenied):
        mod._flow_component_execute_sql("SELECT * FROM X.Y.Z")


def test_t1_every_native_component_wrapper_raises() -> None:
    files = [p for p in (ROOT / "src/lfx_snowflake/native_category_components").rglob("*.py") if "_flow_component_sql()" in p.read_text()]
    assert files
    for path in files:
        assert "_raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(" in path.read_text(), path


# --- Stale stored flows -------------------------------------------------------------------------------------


def _views_source() -> str:
    return (ROOT / "src/lfx_snowflake/native_category_components/data_source/snowflake_views.py").read_text()


def _stale_views_source() -> str:
    return _views_source().replace("_raise_if_flow_context_denied(LIVE_BRIDGE.execute_sql(", "(LIVE_BRIDGE.execute_sql(")


def test_stale_code_detected_and_current_code_untouched() -> None:
    from lfx_snowflake import stale_flow_guard as sg

    assert sg.outdated_classes(_views_source()) == ([], None)
    stale, current = sg.outdated_classes(_stale_views_source())
    assert stale == ["SnowflakeViewsComponent"] and current == sg.installed_component_sources()["SnowflakeViewsComponent"]
    assert sg.outdated_classes("class MyOwnComponent:\n    pass\n") == ([], None)  # not a Snowflake component


def test_stale_code_upgraded_by_default_and_refused_in_strict_mode(monkeypatch) -> None:
    from lfx_snowflake import stale_flow_guard as sg

    monkeypatch.delenv(sg.POLICY_ENV, raising=False)
    assert sg.resolve_component_code(_stale_views_source()) == sg.installed_component_sources()["SnowflakeViewsComponent"]
    monkeypatch.setenv(sg.POLICY_ENV, "refuse")
    with pytest.raises(sg.StaleComponentCode) as exc:
        sg.resolve_component_code(_stale_views_source())
    assert exc.value.error_code == "COMPONENT_CODE_OUTDATED" and "SnowflakeViewsComponent" in str(exc.value)


def test_guard_wraps_lfx_eval_bindings(monkeypatch) -> None:
    import sys
    import types

    from lfx_snowflake import stale_flow_guard as sg

    seen: list[str] = []

    def fake_eval(code):
        seen.append(code)
        return code

    pkg = types.ModuleType("lfx.custom.eval")
    pkg.eval_custom_component_code = fake_eval
    loading = types.ModuleType("lfx.interface.initialize.loading")
    loading.eval_custom_component_code = fake_eval
    monkeypatch.setitem(sys.modules, "lfx.custom.eval", pkg)
    monkeypatch.setitem(sys.modules, "lfx.interface.initialize.loading", loading)
    monkeypatch.delenv(sg.POLICY_ENV, raising=False)
    assert sg.install_stale_component_guard() == 2
    assert sg.install_stale_component_guard() == 0  # idempotent
    loading.eval_custom_component_code(_stale_views_source())
    assert seen[-1] == sg.installed_component_sources()["SnowflakeViewsComponent"]


def test_refresh_flow_data_rewrites_only_stale_snowflake_code() -> None:
    from lfx_snowflake import stale_flow_guard as sg

    data = {
        "nodes": [
            {"data": {"id": "views-1", "node": {"template": {"code": {"value": _stale_views_source()}, "database": {"value": "DB"}}}}},
            {"data": {"id": "views-2", "node": {"template": {"code": {"value": _views_source()}}}}},
            {"data": {"id": "custom-1", "node": {"template": {"code": {"value": "class X:\n    pass\n"}}}}},
        ]
    }
    assert sg.refresh_flow_data(data) == ["views-1"]
    tpl = data["nodes"][0]["data"]["node"]["template"]
    assert tpl["code"]["value"] == sg.installed_component_sources()["SnowflakeViewsComponent"]
    assert tpl["database"]["value"] == "DB"
    assert sg.refresh_flow_data(data) == []


def test_startup_installs_guard_and_admin_script_exists() -> None:
    src = (ROOT / "src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert "install_stale_component_guard()" in src
    script = ROOT / "scripts/refresh_stored_flow_component_code.py"
    ast.parse(script.read_text())
    assert "refresh_flow_data" in script.read_text() and "--apply" in script.read_text()


# --- Cortex Agent discovery label ----------------------------------------------------------------------------


def test_cortex_agent_discovery_sql_label() -> None:
    from lfx_snowflake.fastapi_live_bridge import _show_agents_sql

    assert _show_agents_sql("DB", "SCH").startswith("SHOW AGENTS IN SCHEMA ")
    for path in [*(ROOT / "src").rglob("*.py"), ROOT / "docs/CAPABILITY_MATRIX.md"]:
        assert "SHOW CORTEX AGENTS" not in path.read_text(), path


# --- A6 veto on stale_flow_guard: bypasses, full index, patched upstream modules ------------------------------


@pytest.fixture
def fresh_index(monkeypatch):
    from lfx_snowflake import stale_flow_guard as sg

    sg.installed_component_sources.cache_clear()
    monkeypatch.delenv(sg.POLICY_ENV, raising=False)
    yield sg
    sg.installed_component_sources.cache_clear()


def test_renamed_copy_of_snowflake_component_is_refused(fresh_index) -> None:
    sg = fresh_index
    renamed = _stale_views_source().replace("class SnowflakeViewsComponent(", "class MyViews(")
    with pytest.raises(sg.UnregisteredSnowflakeComponentCode) as exc:
        sg.resolve_component_code(renamed)
    assert exc.value.error_code == "UNREGISTERED_SNOWFLAKE_COMPONENT_CODE" and "MyViews" in str(exc.value)


@pytest.mark.parametrize(
    "code",
    [
        "from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE\n"
        "from lfx.custom import Component\n\n"
        "class Sneaky(Component):\n    def run(self):\n        return LIVE_BRIDGE.execute_sql('SELECT * FROM A.B.C')\n",
        "import lfx_snowflake.fastapi_live_bridge as b\n\nclass Sneaky:\n    pass\n",
        "import importlib\n\nclass Sneaky:\n    b = importlib.import_module('lfx_snowflake.fastapi_live_bridge')\n",
        "class Sneaky:\n    b = __import__('lfx_snowflake')\n",
    ],
)
def test_custom_component_using_lfx_snowflake_is_refused_under_any_policy(fresh_index, monkeypatch, code) -> None:
    sg = fresh_index
    for pol in ("upgrade", "refuse"):
        monkeypatch.setenv(sg.POLICY_ENV, pol)
        with pytest.raises(sg.UnregisteredSnowflakeComponentCode):
            sg.resolve_component_code(code)


def test_plain_custom_component_untouched(fresh_index, monkeypatch) -> None:
    sg = fresh_index
    code = "from lfx.custom import Component\n\nclass MyComponent(Component):\n    display_name = 'Mine'\n"
    assert sg.resolve_component_code(code) is code
    monkeypatch.setenv(sg.POLICY_ENV, "refuse")
    assert sg.resolve_component_code(code) is code


def test_triggers_and_standard_proxies_are_indexed(fresh_index) -> None:
    index = fresh_index.installed_component_sources()
    for name in (
        "SnowflakeAlertTrigger",
        "SnowflakeScheduleTrigger",
        "SnowflakeStageFileTrigger",
        "SnowflakeTableChangeTrigger",
        "StandardChat",
        "StandardMessages",
        "StandardResponses",
        "PrivateOllama",
    ):
        assert name in index, name


def test_patched_upstream_modules_indexed_from_installed_lfx(fresh_index, monkeypatch, tmp_path) -> None:
    sg = fresh_index
    classes = {
        "models_and_agents/language_model.py": "LanguageModelComponent",
        "models_and_agents/agent.py": "AgentComponent",
        "models_and_agents/a2a_agent.py": "A2AAgentComponent",
        "files_and_knowledge/memory_retrieval.py": "MemoryBaseComponent",
        "files_and_knowledge/filesystem.py": "FileSystemComponent",
        "files_and_knowledge/knowledge.py": "KnowledgeComponent",
        "files_and_knowledge/file.py": "FileComponent",
        "files_and_knowledge/save_file.py": "SaveToFileComponent",
    }
    assert set(classes) == set(sg.PATCHED_UPSTREAM_MODULES)
    for rel, cls in classes.items():
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(f"from lfx.custom import Component\nimport lfx_snowflake\n\nclass {cls}(Component):\n    patched = True\n")
    monkeypatch.setattr(sg, "_lfx_components_dir", lambda: tmp_path)
    index = sg.installed_component_sources()
    for rel, cls in classes.items():
        assert index[cls] == (tmp_path / rel).read_text()
    # An old (unpatched) stored AgentComponent is built from the installed patched module ...
    old = "from lfx.custom import Component\n\nclass AgentComponent(Component):\n    patched = False\n"
    assert sg.resolve_component_code(old) == (tmp_path / "models_and_agents/agent.py").read_text()
    # ... and the current patched source (which imports lfx_snowflake) is not refused.
    current = (tmp_path / "models_and_agents/a2a_agent.py").read_text()
    assert sg.resolve_component_code(current) is current
