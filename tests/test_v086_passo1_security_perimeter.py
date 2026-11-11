"""v0.8.6 PASSO 1 regressions: BUG-A2A-06, BUG-CA-01, BUG-A2A-02, BUG-CA-02, BUG-MCP-03."""

from __future__ import annotations

import asyncio
import json
import types
from pathlib import Path

import pytest

from lfx_snowflake import fastapi_live_bridge as flb
from lfx_snowflake import identity as idm
from lfx_snowflake.components import a2a_and_cortex_agent as a2a
from lfx_snowflake.components.files_and_stage import SnowflakeComponentError

REPO = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- BUG-A2A-06


class _HTTPException(Exception):
    def __init__(self, status_code: int, detail: str = "") -> None:
        super().__init__(detail)
        self.status_code = status_code


@pytest.fixture()
def guard_env(monkeypatch: pytest.MonkeyPatch):
    fastapi_stub = types.ModuleType("fastapi")
    fastapi_stub.Depends = lambda dependency=None: dependency  # type: ignore[attr-defined]
    fastapi_stub.HTTPException = _HTTPException  # type: ignore[attr-defined]
    fastapi_stub.Request = object  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "fastapi", fastapi_stub)
    monkeypatch.setattr(flb, "_running_behind_spcs_ingress", lambda: False)
    return monkeypatch


def _req(path: str, headers: dict[str, str] | None = None):
    return types.SimpleNamespace(url=types.SimpleNamespace(path=path), headers=headers or {})


def _run(guard, request, user=None):
    params = guard.__code__.co_varnames[: guard.__code__.co_argcount]
    return asyncio.run(guard(request, user) if "user" in params else guard(request))


def test_a2a06_router_requires_native_langflow_user(guard_env) -> None:
    guard = flb._build_snowflake_route_guard(native_dependency=lambda: None)
    with pytest.raises(_HTTPException) as exc:
        _run(guard, _req("/api/v1/snowflake/context"), user=None)
    assert exc.value.status_code == 401
    inactive = types.SimpleNamespace(is_active=False)
    with pytest.raises(_HTTPException):
        _run(guard, _req("/api/v1/snowflake/role"), user=inactive)
    assert _run(guard, _req("/api/v1/snowflake/context"), user=types.SimpleNamespace(is_active=True)) is None


def test_a2a06_only_agent_card_is_public(guard_env) -> None:
    guard = flb._build_snowflake_route_guard(native_dependency=lambda: None)
    card = "/api/v1/snowflake/a2a/cortex-agents/DB/SCH/AG/.well-known/agent-card.json"
    assert _run(guard, _req(card), user=None) is None
    guard_env.setenv(flb.AGENT_CARD_PUBLIC_ENV, "0")
    with pytest.raises(_HTTPException):
        _run(guard, _req(card), user=None)


def test_a2a06_spcs_requires_ingress_identity(guard_env) -> None:
    guard_env.setattr(flb, "_running_behind_spcs_ingress", lambda: True)
    guard = flb._build_snowflake_route_guard(native_dependency=lambda: None)
    user = types.SimpleNamespace(is_active=True)
    with pytest.raises(_HTTPException) as exc:
        _run(guard, _req("/api/v1/snowflake/context"), user=user)
    assert exc.value.status_code == 403
    ok = _req("/api/v1/snowflake/context", {idm.CALLER_USER_HEADER: "ALICE"})
    assert _run(guard, ok, user=user) is None


def test_a2a06_fails_closed_without_langflow_unless_test_bypass(guard_env) -> None:
    guard_env.setattr(flb, "_native_auth_dependency", lambda: None)
    guard_env.delenv("SNOWFLAKE_LANGFLOW_AUTH_TEST_BYPASS", raising=False)
    guard = flb._build_snowflake_route_guard()
    with pytest.raises(_HTTPException):
        _run(guard, _req("/api/v1/snowflake/mcp/streamable"))


def test_a2a06_router_is_mounted_with_guard_dependency() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert "dependencies=[Depends(_build_snowflake_route_guard())]" in src


# --------------------------------------------------------------------------- BUG-CA-01


class _Resp:
    def __init__(self, body: dict, status: int = 200) -> None:
        self._body = body
        self.status_code = status
        self.content = json.dumps(body).encode()

    def json(self) -> dict:
        return self._body


class _SqlApi:
    """Fake SQL API honouring multi-statement handles; `admin_in_session` drives the guard verdict."""

    def __init__(self, admin_in_session: bool | None) -> None:
        self.admin_in_session = admin_in_session
        self.posts: list[dict] = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.posts.append(json)
        return _Resp({"statementHandles": ["h-use", f"h-{len(self.posts)}"]})

    def get(self, url, headers=None, timeout=None):
        last = self.posts[-1]["statement"]
        if "IS_ROLE_IN_SESSION" in last:
            if self.admin_in_session is None:
                return _Resp({"message": "boom"}, 500)
            return _Resp({"resultSetMetaData": {"rowType": [{"name": "A"}]}, "data": [[str(self.admin_in_session).lower()]]})
        return _Resp({"resultSetMetaData": {"rowType": [{"name": "X"}]}, "data": [["1"]], "statementHandle": "h"})


@pytest.fixture()
def sql_bridge(monkeypatch: pytest.MonkeyPatch, tmp_path):
    token = tmp_path / "token"
    token.write_text("svc")
    monkeypatch.setattr(flb, "SPCS_TOKEN_PATH", token)
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    bridge = flb.LiveSnowflakeBridge.__new__(flb.LiveSnowflakeBridge)
    bridge.__dict__.update(
        {
            "_base_active_role": "DEV_ROLE",
            "_base_context_epoch": 1,
            "_role_sessions": {},
            "_secondary_guard_cache": {},
            "database": "DB",
            "schema": "SCH",
            "warehouse": None,
            "secondary_roles_mode": "NONE",
        }
    )
    import threading

    bridge._role_sessions_lock = threading.RLock()
    monkeypatch.setattr(bridge, "_auto_register_ddl_object_if_create", lambda _s: None, raising=False)
    return bridge


def test_ca01_every_execution_pins_secondary_roles_none(sql_bridge, monkeypatch) -> None:
    monkeypatch.delenv(idm.SECONDARY_ROLES_MODE_ENV, raising=False)
    api = _SqlApi(admin_in_session=False)
    sql_bridge._http_client = api
    res = sql_bridge.execute_sql("SELECT 1;")
    assert res["ok"] is True and res["rows"] == [{"X": "1"}]
    assert all(p["statement"].startswith("USE SECONDARY ROLES NONE;\n") for p in api.posts)
    assert all(p["parameters"]["MULTI_STATEMENT_COUNT"] == "0" for p in api.posts)
    assert api.posts[-1]["statement"].endswith("SELECT 1")


def test_ca01_honours_secondary_roles_mode_env(sql_bridge, monkeypatch) -> None:
    monkeypatch.setenv(idm.SECONDARY_ROLES_MODE_ENV, "all")
    api = _SqlApi(admin_in_session=False)
    sql_bridge._http_client = api
    sql_bridge.execute_sql("SELECT 1")
    assert api.posts[-1]["statement"].startswith("USE SECONDARY ROLES ALL;")
    monkeypatch.setenv(idm.SECONDARY_ROLES_MODE_ENV, "bogus")
    assert idm.secondary_roles_mode() == "NONE"


@pytest.mark.parametrize("admin_in_session", [True, None])
def test_ca01_fails_closed_when_accountadmin_in_session(sql_bridge, admin_in_session) -> None:
    api = _SqlApi(admin_in_session=admin_in_session)
    sql_bridge._http_client = api
    res = sql_bridge.execute_sql("DELETE FROM T")
    assert res["ok"] is False and res["status_code"] == 403
    assert res["error_code"] == "AUTHZ_ROLE_DENIED"
    assert not any("DELETE FROM T" in p["statement"] for p in api.posts)


# --------------------------------------------------------------------------- BUG-A2A-02


def test_a2a02_no_accountadmin_literal_fallback() -> None:
    for rel in ("src/lfx_snowflake/components/a2a_and_cortex_agent.py", "scripts/install_live_hooks_in_container.py"):
        text = (REPO / rel).read_text(encoding="utf-8")
        assert 'or "ACCOUNTADMIN"' not in text, rel
        assert "'ACCOUNTADMIN'" not in text, rel


def test_a2a02_missing_or_privileged_fallback_role_is_denied(monkeypatch) -> None:
    monkeypatch.delenv(idm.CALLERS_RIGHTS_ENV, raising=False)
    for service_role in ("", "ACCOUNTADMIN", "bad-role;drop"):
        with pytest.raises(SnowflakeComponentError) as exc:
            a2a._resolve_a2a_role(None, None, service_role)
        assert exc.value.http_status == 403 and exc.value.code.value == "AUTHZ_ROLE_DENIED"
    assert a2a._resolve_a2a_role(None, None, "dev_role") == "DEV_ROLE"
    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")
    assert a2a._resolve_a2a_role(None, None, "") == ""


def test_a2a02_verified_privileged_session_role_is_honoured(monkeypatch) -> None:
    monkeypatch.delenv(idm.CALLERS_RIGHTS_ENV, raising=False)
    # Session roles were grant-checked at switch time, so ACCOUNTADMIN/SYSADMIN are legitimate here.
    assert a2a._resolve_a2a_role(None, "ACCOUNTADMIN", "VIEWER_ROLE") == "ACCOUNTADMIN"
    assert a2a._resolve_a2a_role(None, "SYSADMIN", "") == "SYSADMIN"
    assert a2a._resolve_a2a_role("dev_role", "ACCOUNTADMIN", "") == "DEV_ROLE"
    with pytest.raises(SnowflakeComponentError):
        a2a._resolve_a2a_role('X" ; DROP', None, "")


def test_a2a02_installer_injected_component_resolves_role() -> None:
    text = (REPO / "scripts/install_live_hooks_in_container.py").read_text(encoding="utf-8")
    assert text.count("resolve_a2a_session_role()") == 3
    assert "ACCOUNTADMIN" not in text


# --------------------------------------------------------------------------- BUG-CA-02 / BUG-MCP-03


@pytest.fixture()
def role_bridge(monkeypatch: pytest.MonkeyPatch):
    bridge = flb.LiveSnowflakeBridge()
    bridge.available_roles = ["DEV_ROLE", "VIEWER_ROLE", "ACCOUNTADMIN"]
    monkeypatch.delenv(idm.CALLERS_RIGHTS_ENV, raising=False)
    monkeypatch.setattr(bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": []})
    monkeypatch.setattr(bridge, "compute_role_effective_context", lambda *a, **k: {
        "stored_context": {}, "effective_context": {}, "inaccessible_objects": [],
        "retained_objects_count": 0, "hidden_by_role_count": 0,
    })
    monkeypatch.setattr(bridge, "sync_flow_node_templates_sqlite", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "reconcile_triggers_with_flow_context", lambda *a, **k: {})
    monkeypatch.setattr(bridge, "record_audit_event", lambda *a, **k: None)
    bridge.active_role = "VIEWER_ROLE"
    return bridge


def _as_user(user: str):
    return idm.bind_identity(idm.RequestIdentity(sf_user=user))


def test_ca02_role_switch_is_scoped_per_user_and_flow(role_bridge) -> None:
    tok = _as_user("ALICE")
    try:
        res = role_bridge.switch_role("DEV_ROLE", flow_id="flow-a")
        assert res["ok"] is True and res["active_role"] == "DEV_ROLE"
        f_tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set("flow-a")
        assert role_bridge.active_role == "DEV_ROLE"
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(f_tok)
        f_tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set("flow-b")
        assert role_bridge.active_role == "VIEWER_ROLE"
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(f_tok)
    finally:
        idm.reset_identity(tok)
    tok = _as_user("BOB")
    try:
        f_tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set("flow-a")
        assert role_bridge.active_role == "VIEWER_ROLE"
        assert role_bridge.context_epoch == 1
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(f_tok)
    finally:
        idm.reset_identity(tok)
    assert role_bridge.service_active_role == "VIEWER_ROLE"


def test_ca02_epoch_tracked_per_session(role_bridge) -> None:
    tok = _as_user("ALICE")
    try:
        first = role_bridge.switch_role("DEV_ROLE", flow_id="flow-a")["context_epoch"]
        second = role_bridge.switch_role("VIEWER_ROLE", flow_id="flow-a")["context_epoch"]
        assert second == first + 1
    finally:
        idm.reset_identity(tok)
    assert role_bridge.context_epoch == 1


def test_mcp03_rest_and_mcp_share_selectability_policy(role_bridge, monkeypatch) -> None:
    for mode in ("0", "1"):
        monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, mode)
        res = role_bridge.switch_role("ACCOUNTADMIN")
        assert res["ok"] is False and res["status_code"] == 403
        assert res["error_code"] == "AUTHZ_ROLE_DENIED"
    assert idm.is_selectable_role("ACCOUNTADMIN", role_bridge.available_roles) is False
    assert role_bridge.switch_role("NOT_GRANTED")["status_code"] == 403
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert "LIVE_BRIDGE.switch_role, requested_role, mcp_flow_id" in src


def test_ca01_user_multi_statement_sql_still_works(sql_bridge) -> None:
    class _MultiApi(_SqlApi):
        def post(self, url, headers=None, json=None, timeout=None):
            self.posts.append(json)
            n = json["statement"].count(";") + 1
            return _Resp({"statementHandles": [f"h{i}" for i in range(n)]})

        def get(self, url, headers=None, timeout=None):
            self.fetched = url
            return super().get(url, headers=headers, timeout=timeout)

    api = _MultiApi(admin_in_session=False)
    sql_bridge._http_client = api
    res = sql_bridge.execute_sql("CREATE TEMP TABLE T (X INT); SELECT X FROM T")
    assert res["ok"] is True and res["rows"] == [{"X": "1"}]
    sent = api.posts[-1]["statement"]
    assert sent == "USE SECONDARY ROLES NONE;\nCREATE TEMP TABLE T (X INT); SELECT X FROM T"
    assert api.fetched.endswith("/h2")  # last statement handle


# --------------------------------------------------------------------------- privileged roles (lead decision)


def _grant_sql(granted_role: str | None):
    def _exec(statement, *, role=None, mode=None, **_k):
        if "CURRENT_ROLE()" not in statement:
            return {"ok": False, "rows": []}
        assert mode == idm.MODE_CALLER  # grant is verified under the caller's identity
        if granted_role and role == granted_role:
            return {"ok": True, "rows": [{"VERIFIED_ROLE": role}]}
        return {"ok": False, "status_code": 403, "error_code": "AUTHZ_ROLE_DENIED", "rows": []}

    return _exec


@pytest.mark.parametrize("role", ["ACCOUNTADMIN", "SYSADMIN"])
def test_privileged_role_selectable_only_when_caller_granted(role_bridge, monkeypatch, role) -> None:
    monkeypatch.setattr(role_bridge, "execute_sql", _grant_sql(role))
    tok = _as_user("ALICE")
    try:
        res = role_bridge.switch_role(role, flow_id="flow-a")
        assert res["ok"] is True and res["active_role"] == role
    finally:
        idm.reset_identity(tok)
    assert role_bridge.service_active_role == "VIEWER_ROLE"


@pytest.mark.parametrize("role", ["ACCOUNTADMIN", "SECURITYADMIN"])
def test_privileged_role_not_granted_is_denied(role_bridge, monkeypatch, role) -> None:
    monkeypatch.setattr(role_bridge, "execute_sql", _grant_sql(None))
    res = role_bridge.switch_role(role)
    assert res["ok"] is False and res["status_code"] == 403 and res["error_code"] == "AUTHZ_ROLE_DENIED"
    # Wrong CURRENT_ROLE() echoed back is not a grant either.
    monkeypatch.setattr(role_bridge, "execute_sql", lambda *a, **k: {"ok": True, "rows": [{"VERIFIED_ROLE": "PUBLIC"}]})
    assert role_bridge.switch_role(role)["status_code"] == 403


def test_mcp_switch_role_delegates_to_bridge_grant_policy() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    start = src.index('elif tool_name == "snowflake_switch_role":')
    block = src[start : start + 1500]
    assert "is_selectable_role" not in block
    assert "if not requested_role:" in block


def test_residual_admin_defaults_resolve_to_caller_role(monkeypatch) -> None:
    from lfx_snowflake.components import llm_operations as lo
    from lfx_snowflake.governance import provisioner as pv
    from lfx_snowflake import context_helpers as ch

    for mod in (lo, pv):
        text = Path(mod.__file__).read_text(encoding="utf-8")
        assert 'active_role: str = "ACCOUNTADMIN"' not in text
    assert "ACCOUNTADMIN" not in Path(ch.__file__).read_text(encoding="utf-8")

    monkeypatch.setattr(flb.LIVE_BRIDGE, "active_role", "DEV_ROLE")
    assert lo._resolve_caller_role(None) == "DEV_ROLE"
    assert pv._resolve_caller_role(None) == "DEV_ROLE"
    assert lo._resolve_caller_role("viewer_role") == "VIEWER_ROLE"
    monkeypatch.setattr(flb.LIVE_BRIDGE, "active_role", "")
    assert pv._resolve_caller_role(None) == ""  # no fallback to an admin role


def test_context_helpers_fail_closed_without_privilege_source(monkeypatch) -> None:
    from lfx_snowflake import context_helpers as ch

    assert ch._can_create_database({"active_role": "ACCOUNTADMIN_LOOKALIKE"}) is False
    assert ch._can_create_schema({"role": "SOME_ROLE"}) is False
    assert ch._can_create_database({"role_privileges": {"can_create_database": True}}) is True
    fake = types.SimpleNamespace(active_role="DEV_ROLE")
    monkeypatch.setattr(ch, "LIVE_BRIDGE", fake)
    assert ch._can_create_database() is False and ch._can_create_schema() is False
    fake.role_privileges = {"can_create_database": True, "can_create_schema": False}
    assert ch._can_create_database() is True and ch._can_create_schema() is False


# --------------------------------------------------------------------------- A6 veto follow-ups


@pytest.mark.parametrize(
    "row, mode, ok",
    [
        (["true", "ACCOUNTADMIN", '{"roles":"","value":""}'], "NONE", True),  # empty requested role, admin default
        (["false", "DEV_ROLE", '{"roles":"","value":""}'], "NONE", True),
        (["true", "DEV_ROLE", '{"roles":"","value":""}'], "NONE", False),
        (["false", "DEV_ROLE", '{"roles":"ANALYST","value":"ANALYST"}'], "NONE", False),  # secondary leaked
        (["true", "DEV_ROLE", '{"roles":"ACCOUNTADMIN","value":"ALL"}'], "ALL", False),
        (["false", "DEV_ROLE", "not-json"], "NONE", False),
    ],
)
def test_a6_1_guard_judges_actual_primary_role(row, mode, ok) -> None:
    assert flb.LiveSnowflakeBridge._secondary_roles_verdict(row, mode) is ok


def test_a6_1_empty_role_caller_mode_runs_under_default_role(sql_bridge, monkeypatch) -> None:
    class _AdminDefault(_SqlApi):
        def get(self, url, headers=None, timeout=None):
            if "IS_ROLE_IN_SESSION" in self.posts[-1]["statement"]:
                return _Resp({"data": [["true", "ACCOUNTADMIN", '{"roles":"","value":""}']]})
            return super().get(url, headers=headers, timeout=timeout)

    sql_bridge.__dict__["_base_active_role"] = ""
    api = _AdminDefault(admin_in_session=True)
    sql_bridge._http_client = api
    res = sql_bridge.execute_sql("SELECT 1")
    assert res["ok"] is True
    assert "role" not in api.posts[-1]
    assert api.posts[-1]["statement"].startswith("USE SECONDARY ROLES NONE;")


def test_a6_1_service_role_never_uses_user_session(role_bridge, monkeypatch) -> None:
    monkeypatch.delenv(idm.SERVICE_ROLE_ENV, raising=False)
    role_bridge.switch_role("DEV_ROLE")
    assert role_bridge.active_role == "DEV_ROLE"
    assert role_bridge._service_role() == "VIEWER_ROLE"


def test_a6_1_refresh_roles_writes_base_not_session(role_bridge, monkeypatch) -> None:
    role_bridge.switch_role("DEV_ROLE")
    role_bridge._roles_discovered = False
    monkeypatch.setattr(
        role_bridge,
        "execute_sql",
        lambda *a, **k: {"ok": True, "rows": [{"USR": "SVC", "RL": "SVC_ROLE", "AVAIL": "[]"}]},
    )
    role_bridge.refresh_roles_from_snowflake(force=True)
    assert role_bridge.service_active_role == "SVC_ROLE"
    assert role_bridge.active_role == "DEV_ROLE"


def test_a6_2_service_trigger_requires_service_function_identity(guard_env) -> None:
    guard = flb._build_snowflake_route_guard(native_dependency=lambda: None)
    path = "/api/v1/snowflake/service-trigger"
    sf_headers = {"sf-external-function-current-query-id": "q1", "sf-external-function-name": "DB.SCH.RUN_LANGFLOW_FLOW"}
    assert _run(guard, _req(path, dict(sf_headers)), user=None) is None
    for bad in (
        {},
        {**sf_headers, "sf-external-function-name": "OTHER_FN"},
        {**sf_headers, idm.CALLER_USER_HEADER: "ALICE"},  # public ingress, not a service function
    ):
        with pytest.raises(_HTTPException):
            _run(guard, _req(path, bad), user=None)
    with pytest.raises(_HTTPException):
        _run(guard, _req("/api/v1/snowflake/role", dict(sf_headers)), user=None)
    guard_env.setenv(flb.SERVICE_TRIGGER_SECRET_ENV, "s3cret")
    with pytest.raises(_HTTPException):
        _run(guard, _req(path, dict(sf_headers)), user=None)
    assert _run(guard, _req(path, {flb.SERVICE_TRIGGER_SECRET_HEADER: "s3cret"}), user=None) is None


def test_a6_3_user_wide_switch_invalidates_flow_scoped_entries(role_bridge) -> None:
    role_bridge.active_flow_id = "global-flow"
    tok = _as_user("ALICE")
    try:
        role_bridge.switch_role("DEV_ROLE", flow_id="flow-a")
        res = role_bridge.switch_role("VIEWER_ROLE")  # user-wide
        assert res["flow_id"] is None  # never the process-global active flow
        f_tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set("flow-a")
        assert role_bridge.active_role == "VIEWER_ROLE"
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(f_tok)
        assert not any(k == ("ALICE", "flow-a") for k in role_bridge._role_sessions)
    finally:
        idm.reset_identity(tok)


def test_a6_3_flow_scoped_switch_beats_user_wide(role_bridge) -> None:
    tok = _as_user("ALICE")
    try:
        role_bridge.switch_role("VIEWER_ROLE")
        role_bridge.switch_role("DEV_ROLE", flow_id="flow-a")
        f_tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set("flow-a")
        assert role_bridge.active_role == "DEV_ROLE"
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(f_tok)
        f_tok = flb.CURRENT_SNOWFLAKE_FLOW_ID.set("flow-b")
        assert role_bridge.active_role == "VIEWER_ROLE"
        flb.CURRENT_SNOWFLAKE_FLOW_ID.reset(f_tok)
    finally:
        idm.reset_identity(tok)


@pytest.mark.parametrize("bad", ['DEV_ROLE"; DROP', "role with space", "1ROLE", "A" * 300, ""])
def test_a6_5_invalid_role_identifiers_rejected(role_bridge, bad) -> None:
    assert idm.validate_role_identifier(bad) is None
    assert idm.is_selectable_role(bad, [bad]) is False
    res = role_bridge.switch_role(bad)
    assert res["status_code"] == 403 and res["error_code"] == "AUTHZ_ROLE_DENIED"
    with pytest.raises(ValueError):
        idm.quote_role_identifier(bad)


def test_a6_5_execute_sql_rejects_invalid_role_and_secondary_mode(sql_bridge) -> None:
    api = _SqlApi(admin_in_session=False)
    sql_bridge._http_client = api
    res = sql_bridge.execute_sql("SELECT 1", role='X"; DROP ROLE Y; --')
    assert res["status_code"] == 403 and api.posts == []
    assert flb.LiveSnowflakeBridge._with_secondary_roles("SELECT 1", "ALL; DROP") == "USE SECONDARY ROLES NONE;\nSELECT 1"


def test_a6_mcp_switch_role_schema_has_optional_flow_id() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    block = src[src.index('"name": "snowflake_switch_role"'):][:700]
    assert '"flow_id"' in block and '"required": ["target_role"]' in block


# --------------------------------------------------------------------------- A6 re-review follow-ups


def _probe_row(**authorized: bool) -> dict:
    missing = '{"allOf":[{"privilege":"X","objectType":"ACCOUNT","objectName":"A"}]}'
    return {k: ('{"authorized" : true}' if v else missing) for k, v in authorized.items()}


def test_a6r_1_privileges_come_from_one_explain_privileges_query(role_bridge, monkeypatch) -> None:
    role_bridge.database, role_bridge.schema = "DB1", "SCH1"
    calls: list[dict] = []

    def _exec(statement, **kw):
        calls.append({"statement": statement, **kw})
        # Mirrors a live DEV role: no CREATE SCHEMA on the database, CREATE TABLE/STAGE on the schema.
        return {"ok": True, "rows": [_probe_row(DATABASE=False, SCHEMA=False, OBJECT=True, STAGE=True, EXTERNAL_VOLUME=False)]}

    monkeypatch.setattr(role_bridge, "execute_sql", _exec)
    role_bridge.__dict__.pop("_privilege_probe_cache", None)
    role_bridge.active_role = "DEV_ROLE"
    privs = role_bridge.get_role_privileges()
    assert privs["can_create_table"] is True and privs["can_create_object"] is True
    assert privs["can_create_stage"] is True
    assert privs["can_create_schema"] is False and privs["can_create_database"] is False
    assert role_bridge.can_create_object() and role_bridge.can_create_stage()
    assert len(calls) == 1  # single cached query for all probes
    stmt = calls[0]["statement"]
    assert stmt.count("EXPLAIN_PRIVILEGES(") == 5
    assert "for_role => 'DEV_ROLE'" in stmt and "missing_only => TRUE" in stmt
    assert "CREATE TABLE DB1.SCH1.LF_PRIV_PROBE" in stmt and "SHOW GRANTS" not in stmt
    role_bridge.context_epoch = role_bridge.context_epoch + 1  # new epoch -> re-probe once
    role_bridge.can_create_object()
    assert len(calls) == 2


def test_a6r_1_privilege_probe_fails_closed(role_bridge, monkeypatch) -> None:
    role_bridge.database, role_bridge.schema = "DB1", "SCH1"
    role_bridge.__dict__.pop("_privilege_probe_cache", None)
    seen: list[str] = []
    monkeypatch.setattr(role_bridge, "execute_sql", lambda st, **k: seen.append(st) or {"ok": False})
    role_bridge.active_role = "ACCOUNTADMIN"
    assert role_bridge.can_create_database() is False  # a role name proves nothing
    role_bridge.active_role = ""
    assert role_bridge.can_create_object() is False
    role_bridge.database = 'DB"; DROP'
    role_bridge.active_role = "DEV_ROLE"
    assert role_bridge.can_create_object() is False
    assert len(seen) == 1  # empty role and invalid database never reach Snowflake
    monkeypatch.setattr(role_bridge, "execute_sql", lambda st, **k: {"ok": True, "rows": [{"OBJECT": "garbage"}]})
    role_bridge.database = "DB1"
    role_bridge.__dict__.pop("_privilege_probe_cache", None)
    assert role_bridge.can_create_object() is False


def test_a6r_2_auto_login_injection_scoped_to_spcs_browser_sessions(monkeypatch) -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert src.count("(not _is_protected_path(clean_path) or _is_spcs_browser_session(request))") == 2
    browser = {idm.CALLER_USER_HEADER: "ALICE", "sec-fetch-site": "same-origin"}
    monkeypatch.setattr(flb, "_running_behind_spcs_ingress", lambda: True)
    monkeypatch.setenv("LANGFLOW_AUTO_LOGIN", "true")
    assert flb._is_spcs_browser_session(types.SimpleNamespace(headers=browser))
    assert not flb._is_spcs_browser_session(types.SimpleNamespace(headers={"sec-fetch-site": "same-origin"}))
    assert not flb._is_spcs_browser_session(types.SimpleNamespace(headers={**browser, "sec-fetch-site": "cross-site"}))
    monkeypatch.setenv("LANGFLOW_AUTO_LOGIN", "false")
    assert not flb._is_spcs_browser_session(types.SimpleNamespace(headers=browser))
    monkeypatch.setenv("LANGFLOW_AUTO_LOGIN", "true")
    monkeypatch.setattr(flb, "_running_behind_spcs_ingress", lambda: False)
    assert not flb._is_spcs_browser_session(types.SimpleNamespace(headers=browser))


def test_a6r_2_session_token_injection_never_overrides_credentials() -> None:
    scope = {"headers": [(b"cookie", b"access_token_lf=real")]}
    assert flb._inject_session_token(scope, "cached") is False
    scope = {"headers": [(b"x-api-key", b"k")]}
    assert flb._inject_session_token(scope, "cached") is False
    scope = {"headers": []}
    assert flb._inject_session_token(scope, "cached") is True
    assert (b"authorization", b"Bearer cached") in scope["headers"]


@pytest.mark.parametrize(
    "path",
    ["/api/v2/traces", "/api/v2/traces/abc", "/api/v1/memories", "/api/v1/memories/m1/messages",
     "/api/v1/memories/m1", "/api/v1/monitor/builds", "/api/v1/monitor/transactions/t1"],
)
def test_a6r_2_intercepted_paths_cover_sub_paths(path) -> None:
    assert flb._is_middleware_intercepted_path("GET", path)
    assert flb._is_protected_path(path)
    assert not flb._is_middleware_intercepted_path("GET", "/api/v1/flows")


def test_a6r_2_intercepted_requests_are_validated_natively(monkeypatch) -> None:
    monkeypatch.setattr(flb, "_running_behind_spcs_ingress", lambda: False)
    valid = types.SimpleNamespace(is_active=True)

    async def _resolver(req):
        token = (req.headers.get("authorization") or "").removeprefix("Bearer ")
        return valid if token == "good" else None

    monkeypatch.setattr(flb, "_NATIVE_USER_RESOLVER", _resolver)
    ok = types.SimpleNamespace(headers={"authorization": "Bearer good"}, cookies={})
    bogus = types.SimpleNamespace(headers={"authorization": "Bearer bogus"}, cookies={})
    assert asyncio.run(flb._authorize_intercepted_request(ok)) is True
    assert asyncio.run(flb._authorize_intercepted_request(bogus)) is False  # presence is not enough
    monkeypatch.setattr(flb, "_running_behind_spcs_ingress", lambda: True)
    assert asyncio.run(flb._authorize_intercepted_request(ok)) is False  # SPCS also needs ingress identity
    ok.headers[idm.CALLER_USER_HEADER] = "ALICE"
    assert asyncio.run(flb._authorize_intercepted_request(ok)) is True


def test_a6r_2_companion_routers_guarded() -> None:
    src = (REPO / "src/lfx_snowflake/fastapi_live_bridge.py").read_text(encoding="utf-8")
    assert src.count("dependencies=[Depends(_build_snowflake_route_guard())]") == 3
    assert "_has_request_credentials" not in src


def test_a6r_3_user_wide_switch_does_not_reconcile_all_flows(role_bridge, monkeypatch) -> None:
    calls: list = []
    monkeypatch.setattr(role_bridge, "reconcile_triggers_with_flow_context", lambda fid=None: calls.append(fid) or {})
    res = role_bridge.switch_role("DEV_ROLE")
    assert calls == [] and res["reconciled_triggers"]["skipped"] == "user_wide_switch"
    role_bridge.switch_role("DEV_ROLE", flow_id="flow-a")
    assert calls == ["flow-a"]


def test_a6r_3_service_queries_never_inherit_session_role(role_bridge, monkeypatch) -> None:
    monkeypatch.delenv(idm.SERVICE_ROLE_ENV, raising=False)
    role_bridge.__dict__["_base_active_role"] = ""
    role_bridge.switch_role("DEV_ROLE")
    sent: list[dict] = []

    def _exec(statement, **kw):
        sent.append({"statement": statement, **kw})
        if "CURRENT_AVAILABLE_ROLES" in statement:
            return {"ok": True, "rows": [{"USR": "SVC", "RL": "SVC_ROLE", "AVAIL": "[]"}]}
        return {"ok": True, "rows": [{"RL": "SVC_ROLE"}]}

    monkeypatch.setattr(role_bridge, "execute_sql", _exec)
    role_bridge.__dict__.pop("_service_default_role_cache", None)
    role_bridge.__dict__["_service_default_role_retry_at"] = 0.0
    assert role_bridge._service_default_role() == "SVC_ROLE"
    role_bridge._roles_discovered = False
    role_bridge.refresh_roles_from_snowflake(force=True)
    assert [c["role"] for c in sent] == ["", ""]  # explicit "" = token default, never DEV_ROLE
    assert role_bridge.active_role == "DEV_ROLE"


def test_a6r_3_execute_sql_empty_role_sends_no_role(sql_bridge) -> None:
    sql_bridge.__dict__["_role_sessions"] = {("", None): {"role": "DEV_ROLE", "epoch": 2}}
    api = _SqlApi(admin_in_session=False)
    sql_bridge._http_client = api
    sql_bridge.execute_sql("SELECT 1", role="")
    assert "role" not in api.posts[-1]
    sql_bridge.execute_sql("SELECT 1")
    assert api.posts[-1]["role"] == "DEV_ROLE"
