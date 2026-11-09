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
