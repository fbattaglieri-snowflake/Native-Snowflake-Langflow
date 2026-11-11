"""Bridge-owned Snowflake routines (PASSO 3 follow-up: Split Text LangChain mode).

Flow components may never create routines (``flow_context_guard`` rejects every user ``CREATE FUNCTION``
with a Python/JS/Java body). Features that need Python therefore use routines that the BRIDGE owns:

* fixed code that lives here — nothing user-controlled is ever interpolated into the body; runtime values
  (text, chunk size, overlap) are UDF ARGUMENTS and are validated inside the handler;
* a versioned name (``..._V<n>``) created with ``CREATE FUNCTION IF NOT EXISTS`` (idempotent; a code change
  bumps the version instead of replacing a function other flows may be calling);
* a configured bridge-owned schema (``LANGFLOW_BRIDGE_ROUTINES_SCHEMA`` = ``DB.SCHEMA``, default the bridge's
  own database/schema);
* created only through :func:`provision_bridge_routines` (deploy-time ``scripts/provision_bridge_routines.py``
  or an admin action) under the SERVICE role, failing loudly when it lacks ``CREATE FUNCTION``.

Flow components only call these functions; the guard allows exactly :func:`bridge_routine_fqns`.
"""

from __future__ import annotations

import hashlib
import os
from typing import Any

from lfx_snowflake import sql_safety as _sql_safety

ROUTINES_SCHEMA_ENV = "LANGFLOW_BRIDGE_ROUTINES_SCHEMA"

LANGCHAIN_SPLITTER_UDF_NAME = "LF_LANGCHAIN_SPLIT_TEXT_V1"
LANGCHAIN_SPLITTER_SIGNATURE = "(VARCHAR, NUMBER, NUMBER)"
# Snowflake Anaconda channel (verified via SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES: 1.1.2 for runtime 3.12).
LANGCHAIN_SPLITTER_PACKAGE = "langchain-text-splitters==1.1.2"
LANGCHAIN_SPLITTER_RUNTIME = "3.12"
SPLIT_MIN_CHUNK_SIZE = 100
SPLIT_MAX_CHUNK_SIZE = 100_000

# Fixed handler code. Changing it REQUIRES bumping LANGCHAIN_SPLITTER_UDF_NAME (tests pin the hash).
LANGCHAIN_SPLITTER_HANDLER = f'''from langchain_text_splitters import RecursiveCharacterTextSplitter


def split_fn(text_val, chunk_sz, chunk_ovlp):
    if not text_val:
        return []
    if chunk_sz is None or chunk_ovlp is None:
        raise ValueError("chunk_sz and chunk_ovlp are required")
    size = int(chunk_sz)
    overlap = int(chunk_ovlp)
    if size < {SPLIT_MIN_CHUNK_SIZE} or size > {SPLIT_MAX_CHUNK_SIZE}:
        raise ValueError("chunk_sz must be between {SPLIT_MIN_CHUNK_SIZE} and {SPLIT_MAX_CHUNK_SIZE}")
    if overlap < 0 or overlap >= size:
        raise ValueError("chunk_ovlp must satisfy 0 <= chunk_ovlp < chunk_sz")
    splitter = RecursiveCharacterTextSplitter(chunk_size=size, chunk_overlap=overlap)
    return splitter.split_text(str(text_val))
'''


class BridgeRoutineError(RuntimeError):
    """A bridge-owned routine is not configured, not provisioned, or cannot be provisioned."""


def handler_sha256() -> str:
    return hashlib.sha256(LANGCHAIN_SPLITTER_HANDLER.encode("utf-8")).hexdigest()


def _bridge(bridge: Any = None) -> Any:
    if bridge is not None:
        return bridge
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE  # lazy: import cycle

    return LIVE_BRIDGE


def routines_schema(bridge: Any = None) -> tuple[str, str]:
    """(DATABASE, SCHEMA) that holds bridge-owned routines; validated plain identifiers."""
    raw = (os.getenv(ROUTINES_SCHEMA_ENV) or "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(".")]
        if len(parts) != 2:
            raise BridgeRoutineError(f"{ROUTINES_SCHEMA_ENV} must be '<DATABASE>.<SCHEMA>', got {raw!r}.")
        db, sch = parts
    else:
        b = _bridge(bridge)
        db, sch = str(getattr(b, "database", "") or ""), str(getattr(b, "schema", "") or "")
    try:
        return (
            _sql_safety.validate_identifier(db, "bridge routines database"),
            _sql_safety.validate_identifier(sch, "bridge routines schema"),
        )
    except _sql_safety.SqlSafetyError as exc:
        raise BridgeRoutineError(f"Invalid bridge routines schema: {exc}") from exc


def langchain_splitter_udf_fqn(bridge: Any = None) -> str:
    db, sch = routines_schema(bridge)
    return f"{db}.{sch}.{LANGCHAIN_SPLITTER_UDF_NAME}"


def bridge_routine_fqns(bridge: Any = None) -> frozenset[str]:
    """Exact FQNs of bridge-owned routines flow components may CALL / SELECT (never a wildcard)."""
    try:
        return frozenset({langchain_splitter_udf_fqn(bridge)})
    except BridgeRoutineError:
        return frozenset()  # misconfigured -> nothing is exempt (fail closed)


def build_langchain_splitter_udf_ddl(udf_fqn: str) -> str:
    """Idempotent DDL for the fixed-code LangChain splitter (only the validated FQN varies)."""
    fqn = _sql_safety.validate_fqn(udf_fqn, "bridge routine", min_parts=3, max_parts=3)
    if "$" in fqn:  # `$$` would open a dollar-quoted body before AS; bridge routine names never need `$`
        raise BridgeRoutineError(f"Bridge routine name must not contain '$': {fqn!r}")
    return (
        f"CREATE FUNCTION IF NOT EXISTS {fqn}(text_val VARCHAR, chunk_sz NUMBER, chunk_ovlp NUMBER)\n"
        f"RETURNS ARRAY\n"
        f"LANGUAGE PYTHON\n"
        f"RUNTIME_VERSION = '{LANGCHAIN_SPLITTER_RUNTIME}'\n"
        f"PACKAGES = ('{LANGCHAIN_SPLITTER_PACKAGE}')\n"
        f"HANDLER = 'split_fn'\n"
        f"COMMENT = 'lfx_snowflake bridge-owned routine; handler sha256={handler_sha256()}'\n"
        f"AS $$\n{LANGCHAIN_SPLITTER_HANDLER}$$"
    )


def _is_privilege_failure(res: dict[str, Any]) -> bool:
    text = str(res.get("message") or "").lower()
    return (
        int(res.get("status_code") or 0) == 403
        or str(res.get("error_code") or "").startswith("AUTHZ")
        or "insufficient privileges" in text
        or "003001" in text
        or "does not exist or not authorized" in text
    )


def provision_bridge_routines(bridge: Any = None) -> dict[str, Any]:
    """Create every bridge-owned routine (idempotent) under the SERVICE role. Never under a user/flow scope.

    Raises :class:`BridgeRoutineError` when the service role cannot create the function; there is no fallback.
    """
    from lfx_snowflake import identity as _sf_identity
    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID
    from lfx_snowflake.flow_context_guard import FLOW_COMPONENT_SQL

    b = _bridge(bridge)
    fqn = langchain_splitter_udf_fqn(b)
    ddl = build_langchain_splitter_udf_ddl(fqn)
    role = b._service_role()
    # Bridge-internal provisioning: explicitly outside any flow / component scope.
    scope_tok, flow_tok = FLOW_COMPONENT_SQL.set(False), CURRENT_SNOWFLAKE_FLOW_ID.set(None)
    try:
        res = b.execute_sql(ddl, role=role, mode=_sf_identity.MODE_SERVICE, timeout=120)
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(flow_tok)
        FLOW_COMPONENT_SQL.reset(scope_tok)
    if not res.get("ok"):
        db, sch = fqn.split(".")[:2]
        if _is_privilege_failure(res):
            raise BridgeRoutineError(
                f"Service role {role or '(default)'} cannot create bridge routine {fqn}: it needs USAGE on "
                f"database {db}, USAGE and CREATE FUNCTION on schema {db}.{sch}. Grant them (or set "
                f"{ROUTINES_SCHEMA_ENV} to a schema the service role owns) and re-run provisioning. "
                f"Snowflake said: {res.get('message')}"
            )
        raise BridgeRoutineError(f"Provisioning bridge routine {fqn} failed: {res.get('message')}")
    return {
        "ok": True,
        "routines": [{"fqn": fqn, "signature": LANGCHAIN_SPLITTER_SIGNATURE, "handler_sha256": handler_sha256()}],
        "service_role": role,
        "next_steps": (
            f"Grant USAGE ON FUNCTION {fqn}{LANGCHAIN_SPLITTER_SIGNATURE} (and USAGE on {fqn.rsplit('.', 1)[0]}) "
            "to the roles that run flows."
        ),
    }


def require_langchain_splitter(bridge: Any = None) -> str:
    """FQN of the provisioned splitter, or :class:`BridgeRoutineError` with how to provision it."""
    b = _bridge(bridge)
    fqn = langchain_splitter_udf_fqn(b)
    from lfx_snowflake.flow_context_guard import FLOW_COMPONENT_SQL

    tok = FLOW_COMPONENT_SQL.set(False)  # metadata lookup of a bridge object, not component SQL
    try:
        res = b.execute_sql(f"DESCRIBE FUNCTION {fqn}{LANGCHAIN_SPLITTER_SIGNATURE}", timeout=15)
    finally:
        FLOW_COMPONENT_SQL.reset(tok)
    if not (res.get("ok") and res.get("rows")):
        raise BridgeRoutineError(
            f"LangChain splitter {fqn} is not provisioned or not usable by the active role. An administrator must "
            "run scripts/provision_bridge_routines.py (service role) and grant USAGE on the function."
        )
    return fqn
