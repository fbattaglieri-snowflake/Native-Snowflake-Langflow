"""Per-request Snowflake identity and connection-mode selection (W3-R1-10b/10c).

Identity is carried in a ``ContextVar`` and never in process-global state, so
concurrent requests cannot observe each other's user, token or roles.

Two execution modes exist:

* ``service`` - the SPCS service OAuth token; the role comes from
  ``SNOWFLAKE_LANGFLOW_SERVICE_ROLE`` (never a hard-coded admin role).
* ``caller``  - restricted caller's rights: ``Bearer <service>.<caller>`` built
  from the ingress header ``Sf-Context-Current-User-Token``. Only active when
  ``SNOWFLAKE_LANGFLOW_CALLERS_RIGHTS=1``; fails closed when the token is missing.

The plaintext ``Sf-Context-Current-User`` header is kept for display and
attribution only; it is never used for authorization decisions.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token, copy_context
from typing import Any
from dataclasses import dataclass, field

CALLERS_RIGHTS_ENV = "SNOWFLAKE_LANGFLOW_CALLERS_RIGHTS"
SERVICE_ROLE_ENV = "SNOWFLAKE_LANGFLOW_SERVICE_ROLE"
CALLER_TOKEN_HEADER = "Sf-Context-Current-User-Token"
CALLER_USER_HEADER = "Sf-Context-Current-User"

MODE_CALLER = "caller"
MODE_SERVICE = "service"

# Roles that may never be selected through tools or the UI selector.
PRIVILEGED_ROLES = frozenset({"ACCOUNTADMIN", "SECURITYADMIN", "ORGADMIN", "USERADMIN", "SYSADMIN"})


class CallerIdentityMissing(PermissionError):
    """Raised when caller mode is required but no caller token is bound to the request."""

    error_code = "AUTH_CALLER_TOKEN_MISSING"


@dataclass(frozen=True)
class RequestIdentity:
    sf_user: str | None = None
    caller_token: str | None = field(default=None, repr=False)
    display_role: str | None = None

    @property
    def has_caller_token(self) -> bool:
        return bool(self.caller_token)


_EMPTY = RequestIdentity()
CURRENT_IDENTITY: ContextVar[RequestIdentity] = ContextVar("snowflake_request_identity", default=_EMPTY)


def callers_rights_enabled() -> bool:
    return os.getenv(CALLERS_RIGHTS_ENV, "0").strip().lower() in {"1", "true", "yes", "on"}


def service_role() -> str | None:
    """Role for background/service writes. ``None`` means the service user's default role."""
    role = (os.getenv(SERVICE_ROLE_ENV) or "").strip().upper()
    if not role or role in PRIVILEGED_ROLES:
        return None
    return role


def _header(headers: Mapping[str, str], name: str) -> str | None:
    getter = getattr(headers, "get", None)
    if getter is None:
        return None
    value = getter(name) or getter(name.lower())
    value = str(value).strip() if value else ""
    return value or None


def identity_from_headers(headers: Mapping[str, str]) -> RequestIdentity:
    """Build the request identity from ingress headers only.

    The caller token is retained only when the caller's-rights flag is ON.
    """
    token = _header(headers, CALLER_TOKEN_HEADER) if callers_rights_enabled() else None
    return RequestIdentity(sf_user=_header(headers, CALLER_USER_HEADER), caller_token=token)


def current_identity() -> RequestIdentity:
    return CURRENT_IDENTITY.get()


def bind_identity(identity: RequestIdentity) -> Token[RequestIdentity]:
    return CURRENT_IDENTITY.set(identity)


def reset_identity(token: Token[RequestIdentity]) -> None:
    CURRENT_IDENTITY.reset(token)


def resolve_mode(requested: str | None = None) -> str:
    """Return the effective mode. Caller mode is only reachable when the flag is ON."""
    if (requested or MODE_SERVICE) == MODE_CALLER and callers_rights_enabled():
        return MODE_CALLER
    return MODE_SERVICE


def authorization_value(service_token: str, mode: str) -> str:
    """Bearer value for the SQL API / Cortex REST. Fails closed in caller mode without token."""
    if resolve_mode(mode) == MODE_CALLER:
        ident = current_identity()
        if not ident.has_caller_token:
            raise CallerIdentityMissing("Caller mode requires Sf-Context-Current-User-Token; nothing was executed.")
        return f"Bearer {service_token}.{ident.caller_token}"
    return f"Bearer {service_token}"


_READ_ONLY_LEADS = frozenset({"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "LIST", "LS"})
_METADATA_LEADS = frozenset({"SHOW", "DESCRIBE", "DESC", "LIST", "LS"})
_WRITE_WORDS = frozenset(
    {
        "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE",
        "COPY", "PUT", "GET", "REMOVE", "RM", "CALL", "EXECUTE", "IMMEDIATE", "USE", "UNDROP", "SET",
        "UNSET", "BEGIN", "COMMIT", "ROLLBACK", "DECLARE", "LET",
    }
)
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")


class _UnterminatedToken(ValueError):
    pass


def _scrub_sql(statement: str) -> str:
    """Single left-to-right pass replacing literals, quoted identifiers and comments.

    Whichever construct opens first wins, so ``'/*'`` is a string (not a comment
    start) and ``-- '`` is a comment (not a string start). Unterminated tokens raise.
    """
    out: list[str] = []
    i, n = 0, len(statement)
    while i < n:
        ch = statement[i]
        two = statement[i : i + 2]
        if two == "--" or two == "//":
            j = statement.find("\n", i)
            i = n if j < 0 else j
            out.append(" ")
        elif two == "/*":
            j = statement.find("*/", i + 2)
            if j < 0:
                raise _UnterminatedToken("comment")
            i = j + 2
            out.append(" ")
        elif two == "$$":
            j = statement.find("$$", i + 2)
            if j < 0:
                raise _UnterminatedToken("dollar")
            i = j + 2
            out.append(" '' ")
        elif ch in "'\"":
            j = i + 1
            while True:
                if j >= n:
                    raise _UnterminatedToken("quote")
                if ch == "'" and statement[j] == "\\":
                    j += 2
                    continue
                if statement[j] == ch:
                    if j + 1 < n and statement[j + 1] == ch:
                        j += 2
                        continue
                    break
                j += 1
            i = j + 1
            out.append(" '' " if ch == "'" else " _qid ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def is_read_only_statement(statement: str) -> bool:
    """Conservative single-statement read-only check (fails closed on doubt).

    Rejects multi-statement input, CALL / EXECUTE IMMEDIATE, scripting blocks and
    any write keyword anywhere outside literals (e.g. ``WITH ... INSERT``).
    """
    try:
        scrubbed = _scrub_sql(statement or "").strip()
    except _UnterminatedToken:
        return False
    scrubbed = scrubbed.rstrip().rstrip(";").strip()
    if not scrubbed or ";" in scrubbed:
        return False
    words = [w.upper() for w in _WORD.findall(scrubbed)]
    if not words or words[0] not in _READ_ONLY_LEADS:
        return False
    if words[0] in _METADATA_LEADS:
        return True
    return not any(w in _WRITE_WORDS for w in words)


def is_selectable_role(role: str | None, available: list[str] | None) -> bool:
    clean = (role or "").strip().upper()
    if not clean or clean in PRIVILEGED_ROLES:
        return False
    return clean in {str(r).strip().upper() for r in (available or [])}


# W3-R1-10c: execution mode requested for the current request. User-facing routes bind
# ``caller``; everything else (and every background job) stays ``service``. ``resolve_mode``
# still downgrades to service when the caller's-rights flag is OFF.
CURRENT_REQUEST_MODE: ContextVar[str] = ContextVar("snowflake_request_mode", default=MODE_SERVICE)

USER_FACING_PATH_MARKERS: tuple[str, ...] = (
    "/memories",
    "/stages",
    "/catalog",
    "/context",
    "/cortex",
    "/cortex-agents",
    "/analyst",
    "/a2a/cortex-agents",
)


def mode_for_path(path: str | None) -> str:
    clean = str(path or "")
    for marker in USER_FACING_PATH_MARKERS:
        if f"{marker}/" in f"{clean}/":
            return MODE_CALLER
    return MODE_SERVICE


def current_request_mode() -> str:
    return CURRENT_REQUEST_MODE.get()


def bind_request_mode(mode: str) -> Token[str]:
    return CURRENT_REQUEST_MODE.set(mode if mode in (MODE_CALLER, MODE_SERVICE) else MODE_SERVICE)


def reset_request_mode(token: Token[str]) -> None:
    CURRENT_REQUEST_MODE.reset(token)


def submit_with_context(executor: Any, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Submit ``fn`` with a copy of the caller's contextvars (identity, mode, trace ids)."""
    ctx = copy_context()
    return executor.submit(ctx.run, fn, *args, **kwargs)


def _run_as_service(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    token = CURRENT_REQUEST_MODE.set(MODE_SERVICE)
    try:
        return fn(*args, **kwargs)
    finally:
        CURRENT_REQUEST_MODE.reset(token)


def submit_background(executor: Any, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Background job: keeps identity/trace context (for origin_user) but always runs in service mode."""
    ctx = copy_context()
    return executor.submit(ctx.run, _run_as_service, fn, *args, **kwargs)


def origin_user() -> str | None:
    return current_identity().sf_user
