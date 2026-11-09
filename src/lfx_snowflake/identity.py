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
from collections.abc import Mapping
from contextvars import ContextVar, Token
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


_READ_ONLY_LEAD = re.compile(r"^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN|LIST|LS)\b", re.IGNORECASE)
_COMMENTS = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
_STRINGS = re.compile(r"'(?:[^'\\]|\\.|'')*'|\$\$.*?\$\$", re.DOTALL)
_WRITE_WORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COPY|PUT|REMOVE|RM|CALL|EXECUTE|USE|UNDROP|SET|UNSET|BEGIN|COMMIT|ROLLBACK)\b",
    re.IGNORECASE,
)


def is_read_only_statement(statement: str) -> bool:
    """Conservative single-statement read-only check (fails closed on doubt)."""
    sql = _COMMENTS.sub(" ", statement or "")
    scrubbed = _STRINGS.sub("''", sql).strip().rstrip(";").strip()
    if not scrubbed or ";" in scrubbed:
        return False
    if not _READ_ONLY_LEAD.match(scrubbed):
        return False
    lead = scrubbed.split(None, 1)[0].upper()
    if lead in {"SHOW", "DESCRIBE", "DESC", "LIST", "LS"}:
        return True
    return _WRITE_WORDS.search(scrubbed) is None


def is_selectable_role(role: str | None, available: list[str] | None) -> bool:
    clean = (role or "").strip().upper()
    if not clean or clean in PRIVILEGED_ROLES:
        return False
    return clean in {str(r).strip().upper() for r in (available or [])}
