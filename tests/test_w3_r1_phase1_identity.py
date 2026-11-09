"""W3-R1-10 identity / caller's rights (AC-04..AC-08, TR-01..TR-08 where applicable)."""

from __future__ import annotations

import asyncio
import contextvars

import pytest

from lfx_snowflake import identity as idm


def _hdr(**kw: str) -> dict[str, str]:
    return {k.replace("_", "-"): v for k, v in kw.items()}


def test_flag_defaults_off_and_token_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(idm.CALLERS_RIGHTS_ENV, raising=False)
    ident = idm.identity_from_headers({idm.CALLER_TOKEN_HEADER: "tok", idm.CALLER_USER_HEADER: "ALICE"})
    assert ident.caller_token is None and ident.sf_user == "ALICE"
    assert idm.resolve_mode(idm.MODE_CALLER) == idm.MODE_SERVICE
    assert idm.authorization_value("svc", idm.MODE_CALLER) == "Bearer svc"


def test_caller_mode_fails_closed_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")
    tok = idm.bind_identity(idm.identity_from_headers({idm.CALLER_USER_HEADER: "ALICE"}))
    try:
        with pytest.raises(idm.CallerIdentityMissing):
            idm.authorization_value("svc", idm.MODE_CALLER)
    finally:
        idm.reset_identity(tok)


def test_caller_mode_builds_combined_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")
    tok = idm.bind_identity(idm.identity_from_headers({idm.CALLER_TOKEN_HEADER: "ctok"}))
    try:
        assert idm.authorization_value("svc", idm.MODE_CALLER) == "Bearer svc.ctok"
        assert "ctok" not in repr(idm.current_identity())
    finally:
        idm.reset_identity(tok)


def test_identity_is_isolated_per_request() -> None:
    async def handler(user: str) -> str | None:
        tok = idm.bind_identity(idm.RequestIdentity(sf_user=user))
        try:
            await asyncio.sleep(0.01)
            return idm.current_identity().sf_user
        finally:
            idm.reset_identity(tok)

    async def main() -> list[str | None]:
        return await asyncio.gather(handler("ALICE"), handler("BOB"))

    assert asyncio.run(main()) == ["ALICE", "BOB"]
    assert contextvars.copy_context().run(idm.current_identity).sf_user is None


def test_service_role_never_privileged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(idm.SERVICE_ROLE_ENV, "ACCOUNTADMIN")
    assert idm.service_role() is None
    monkeypatch.setenv(idm.SERVICE_ROLE_ENV, "snowflake_langflow_owner_role")
    assert idm.service_role() == "SNOWFLAKE_LANGFLOW_OWNER_ROLE"


@pytest.mark.parametrize(
    "sql",
    ["SELECT 1", "with a as (select 1) select * from a", "SHOW TABLES", "select 'drop table x'"],
)
def test_read_only_guard_accepts(sql: str) -> None:
    assert idm.is_read_only_statement(sql)


@pytest.mark.parametrize(
    "sql",
    ["DROP TABLE x", "SELECT 1; DROP TABLE x", "CREATE TABLE t AS SELECT 1", "", "CALL p()",
     "WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a", "/* x */ GRANT ROLE r TO USER u"],
)
def test_read_only_guard_rejects(sql: str) -> None:
    assert not idm.is_read_only_statement(sql)


def test_selectable_role_blocks_privileged() -> None:
    assert not idm.is_selectable_role("ACCOUNTADMIN", ["ACCOUNTADMIN", "PUBLIC"])
    assert idm.is_selectable_role("public", ["PUBLIC"])
    assert not idm.is_selectable_role("X", ["PUBLIC"])


def test_mcp_tools_have_no_admin_default_and_guard_writes() -> None:
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert 'args.get("target_role") or "ACCOUNTADMIN"' not in src
    assert "_sf_identity.is_read_only_statement(mcp_statement)" in src


def test_bridge_user_is_per_request_not_global() -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    base = LIVE_BRIDGE.authenticated_user
    tok = idm.bind_identity(idm.RequestIdentity(sf_user="ALICE"))
    try:
        assert LIVE_BRIDGE.authenticated_user == "ALICE"
    finally:
        idm.reset_identity(tok)
    assert LIVE_BRIDGE.authenticated_user == base


def test_bridge_never_writes_header_user_globally() -> None:
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert "LIVE_BRIDGE.authenticated_user =" not in src
    assert "_sf_identity.bind_identity(_sf_identity.identity_from_headers(request.headers))" in src


def test_execute_sql_caller_mode_fails_closed_without_token(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from lfx_snowflake import fastapi_live_bridge as flb

    token_file = tmp_path / "token"
    token_file.write_text("svc")
    monkeypatch.setattr(flb, "SPCS_TOKEN_PATH", token_file)
    monkeypatch.setenv("SNOWFLAKE_HOST", "example.invalid")
    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")

    class _NoNetwork:
        def post(self, *a, **k):  # pragma: no cover - must never be reached
            raise AssertionError("no request may be sent without a caller token")

    monkeypatch.setattr(flb.LIVE_BRIDGE, "_http_client", _NoNetwork())
    res = flb.LIVE_BRIDGE.execute_sql("SELECT 1", mode=idm.MODE_CALLER)
    assert res["ok"] is False and res["status_code"] == 401
    assert res["error_code"] == idm.CallerIdentityMissing.error_code


def test_no_hardcoded_admin_role_in_background_writes() -> None:
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert 'role="ACCOUNTADMIN"' not in src
    assert "return _sf_identity.service_role() or self.active_role" in src


def test_switch_role_rejects_privileged_role_under_callers_rights(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")

    res = LIVE_BRIDGE.switch_role("ACCOUNTADMIN")
    assert res["ok"] is False and res["status_code"] == 403


# ---- W3-R1 Phase 1 fix round (10a..10e) -------------------------------------------------------

@pytest.mark.parametrize(
    "stmt",
    [
        "SELECT '/*'; DROP TABLE x; SELECT '*/'",
        "WITH c AS (SELECT '/*') INSERT INTO t SELECT '*/'",
        "SELECT 1; SELECT 2",
        "CALL my_proc()",
        "EXECUTE IMMEDIATE 'SELECT 1'",
        "WITH a AS (SELECT 1) UPDATE t SET x = 1",
        "WITH a AS (SELECT 1) DELETE FROM t",
        "WITH a AS (SELECT 1) MERGE INTO t USING a ON 1=1 WHEN MATCHED THEN DELETE",
        "SELECT \"x\"; DROP TABLE y",
        "SELECT $$ ; $$; DROP TABLE y",
        "SELECT 'unterminated",
        "SHOW TABLES; DROP TABLE x",
        "-- SELECT\nDROP TABLE x",
    ],
)
def test_read_only_guard_rejects_bypasses(stmt: str) -> None:
    assert idm.is_read_only_statement(stmt) is False


@pytest.mark.parametrize(
    "stmt",
    ["SELECT 1", "SELECT 'a;b /* c' FROM t;", "SELECT 1 -- ; DROP", "WITH a AS (SELECT 1) SELECT * FROM a", "SHOW ROLES"],
)
def test_read_only_guard_accepts_reads(stmt: str) -> None:
    assert idm.is_read_only_statement(stmt) is True


def test_mcp_denials_are_audited_as_authz_role_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake import fastapi_live_bridge as flb

    seen: list[dict] = []
    monkeypatch.setattr(flb.LIVE_BRIDGE, "record_audit_event", lambda **kw: seen.append(kw) or "id")
    flb._emit_mcp_denial("snowflake_query_sql", {"ok": False, "status_code": 403, "error_code": "AUTHZ_ROLE_DENIED"}, None)
    assert seen and seen[0]["action"] == "AUTHZ_ROLE_DENIED" and seen[0]["outcome"] == "DENIED"
    src = flb.__file__ and open(flb.__file__).read()
    assert src.count("_emit_mcp_denial(\"snowflake_") == 2


def test_executor_submissions_propagate_identity_and_force_service_for_background() -> None:
    from concurrent.futures import ThreadPoolExecutor

    ex = ThreadPoolExecutor(max_workers=1)
    tok = idm.bind_identity(idm.RequestIdentity(sf_user="ALICE"))
    mtok = idm.bind_request_mode(idm.MODE_CALLER)
    try:
        seen = idm.submit_with_context(ex, lambda: (idm.current_identity().sf_user, idm.current_request_mode())).result()
        bg = idm.submit_background(ex, lambda: (idm.origin_user(), idm.current_request_mode())).result()
        raw = ex.submit(lambda: idm.current_identity().sf_user).result()
    finally:
        idm.reset_request_mode(mtok)
        idm.reset_identity(tok)
        ex.shutdown()
    assert seen == ("ALICE", idm.MODE_CALLER)
    assert bg == ("ALICE", idm.MODE_SERVICE)
    assert raw is None  # proves plain submit would have lost the context


def test_bridge_never_submits_to_executors_without_context() -> None:
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert "_AUDIT_EXECUTOR.submit(" not in src and "_CATALOG_EXECUTOR.submit(" not in src


def test_refresh_roles_does_not_mutate_service_user_or_inject_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    prev = (LIVE_BRIDGE._service_user, list(LIVE_BRIDGE.available_roles), LIVE_BRIDGE._roles_discovered)
    calls: list[dict] = []

    def fake_sql(stmt, **kw):
        calls.append(kw)
        return {"ok": True, "rows": [{"USR": "ALICE", "RL": "DEV", "AVAIL": '["DEV","PUBLIC"]'}]}

    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", fake_sql)
    try:
        LIVE_BRIDGE.refresh_roles_from_snowflake(force=True)
        assert LIVE_BRIDGE._service_user == prev[0]
        assert "ACCOUNTADMIN" not in LIVE_BRIDGE.available_roles
        assert calls[0]["mode"] == idm.MODE_SERVICE
    finally:
        LIVE_BRIDGE._service_user, LIVE_BRIDGE.available_roles, LIVE_BRIDGE._roles_discovered = prev


@pytest.mark.parametrize(
    ("path", "mode"),
    [
        ("/api/v1/snowflake/memories", idm.MODE_CALLER),
        ("/api/v1/snowflake/memories/recall", idm.MODE_CALLER),
        ("/api/v1/snowflake/stages/files", idm.MODE_CALLER),
        ("/api/v1/snowflake/catalog", idm.MODE_CALLER),
        ("/api/v1/snowflake/context", idm.MODE_CALLER),
        ("/api/v1/snowflake/cortex-agents/D/S/A/run", idm.MODE_CALLER),
        ("/api/v1/snowflake/analyst/message", idm.MODE_CALLER),
        ("/api/v1/flows/abc", idm.MODE_SERVICE),
        ("/api/v1/snowflake/memoriesx", idm.MODE_SERVICE),
    ],
)
def test_user_facing_routes_request_caller_mode(path: str, mode: str) -> None:
    assert idm.mode_for_path(path) == mode


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


def test_flag_off_user_route_mode_keeps_service_behaviour(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from lfx_snowflake import fastapi_live_bridge as flb

    monkeypatch.delenv(idm.CALLERS_RIGHTS_ENV, raising=False)
    sent = _capture_payload(monkeypatch, tmp_path, flb)
    mtok = idm.bind_request_mode(idm.MODE_CALLER)
    try:
        flb.LIVE_BRIDGE.execute_sql("SELECT 1")
    finally:
        idm.reset_request_mode(mtok)
    assert sent[0][0]["Authorization"] == "Bearer svc"


def test_flag_on_user_route_uses_caller_token(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from lfx_snowflake import fastapi_live_bridge as flb

    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")
    sent = _capture_payload(monkeypatch, tmp_path, flb)
    tok = idm.bind_identity(idm.RequestIdentity(sf_user="ALICE", caller_token="ctok"))
    mtok = idm.bind_request_mode(idm.MODE_CALLER)
    try:
        flb.LIVE_BRIDGE.execute_sql("SELECT 1")
        flb.LIVE_BRIDGE.execute_sql("SELECT 2", mode=idm.MODE_SERVICE)
    finally:
        idm.reset_request_mode(mtok)
        idm.reset_identity(tok)
    assert sent[0][0]["Authorization"] == "Bearer svc.ctok"
    assert sent[1][0]["Authorization"] == "Bearer svc"


def test_no_role_sent_when_no_role_configured(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from lfx_snowflake import fastapi_live_bridge as flb

    monkeypatch.delenv(idm.CALLERS_RIGHTS_ENV, raising=False)
    monkeypatch.delenv(idm.SERVICE_ROLE_ENV, raising=False)
    sent = _capture_payload(monkeypatch, tmp_path, flb)
    monkeypatch.setattr(flb.LIVE_BRIDGE, "active_role", "")
    flb.LIVE_BRIDGE.execute_sql("SELECT 1", role=flb.LIVE_BRIDGE._service_role(), mode=idm.MODE_SERVICE)
    assert "role" not in sent[0][1]


def test_no_accountadmin_default_or_fallback_in_bridge() -> None:
    from pathlib import Path

    src = Path(__file__).resolve().parents[1].joinpath("src/lfx_snowflake/fastapi_live_bridge.py").read_text()
    assert 'or "ACCOUNTADMIN"' not in src
    assert '"SNOWFLAKE_DEFAULT_ROLE", "ACCOUNTADMIN"' not in src


def test_switch_role_is_display_only_under_callers_rights(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    monkeypatch.setenv(idm.CALLERS_RIGHTS_ENV, "1")
    monkeypatch.setattr(LIVE_BRIDGE, "available_roles", ["DEV_ROLE", "PUBLIC"])
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", lambda *a, **k: {"ok": True, "rows": [{}]})
    before = (LIVE_BRIDGE.active_role, LIVE_BRIDGE.context_epoch)
    res = LIVE_BRIDGE.switch_role("DEV_ROLE")
    assert res["ok"] is True and res["display_only"] is True and res["active_role"] == "DEV_ROLE"
    assert (LIVE_BRIDGE.active_role, LIVE_BRIDGE.context_epoch) == before


def test_background_audit_records_origin_user(monkeypatch: pytest.MonkeyPatch) -> None:
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    captured: list[dict] = []
    monkeypatch.setattr(LIVE_BRIDGE, "_has_spcs_token", lambda: False)
    monkeypatch.setattr(LIVE_BRIDGE, "execute_sql", lambda stmt, **kw: captured.append(kw) or {"ok": True})
    tok = idm.bind_identity(idm.RequestIdentity(sf_user="ALICE"))
    try:
        LIVE_BRIDGE.record_audit_event(action="STAGE_DIRECTORY_REFRESH", outcome="SUCCESS", target_object="@S")
    finally:
        idm.reset_identity(tok)
    details_json = captured[0]["bindings"][str(len(captured[0]["bindings"]) - 1)]["value"]
    assert '"origin_user": "ALICE"' in details_json
    assert captured[0]["mode"] == idm.MODE_SERVICE
