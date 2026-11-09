"""Traceability primitives for W3-R1-14 (TR-01, TR-02, TR-08).

- ``build_query_tag``: JSON ``parameters.query_tag`` for every SQL API call (TR-02, handover §7.2).
- ``AuditAction`` / ``AuditOutcome``: the §7.4 audit vocabulary; deprecated actions stay readable
  for history but ``emit`` refuses to write them.
- ``deterministic_event_id``: idempotent event id so a retry never duplicates an audit row.
- ``build_audit_insert``: parameterised INSERT (SQL API ``bindings``), never string interpolation.
- ``emit``: writes an audit event only for a *real* result (a SQL API response dict); there is
  no default/assumed outcome (fail closed, A2<->A5 interface).
- ``JsonLogFormatter`` / ``log_event``: structured container logs with an allowlist of fields and
  no content (TR-08).

Per-request correlation (run_id, request_id, component, memory_base_id) lives in ContextVars, never
in globals, so concurrent requests cannot leak values into each other.
"""

from __future__ import annotations

import enum
import hashlib
import json
import logging
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

APP_NAME = "snowflake-langflow"
QUERY_TAG_MAX_LEN = 2000  # Snowflake QUERY_TAG limit
QUERY_TAG_KEYS = ("app", "v", "flow_id", "run_id", "component", "memory_base_id", "request_id")

CURRENT_RUN_ID: ContextVar[str | None] = ContextVar("sflf_current_run_id", default=None)
CURRENT_REQUEST_ID: ContextVar[str | None] = ContextVar("sflf_current_request_id", default=None)
CURRENT_COMPONENT: ContextVar[str | None] = ContextVar("sflf_current_component", default=None)
CURRENT_MEMORY_BASE_ID: ContextVar[str | None] = ContextVar("sflf_current_memory_base_id", default=None)

_CORRELATION_VARS: dict[str, ContextVar[str | None]] = {
    "run_id": CURRENT_RUN_ID,
    "request_id": CURRENT_REQUEST_ID,
    "component": CURRENT_COMPONENT,
    "memory_base_id": CURRENT_MEMORY_BASE_ID,
}


def _version() -> str:
    try:
        from lfx_snowflake import __version__

        return str(__version__)
    except Exception:  # noqa: BLE001
        return "unknown"


@contextmanager
def trace_scope(**values: str | None) -> Iterator[None]:
    """Bind correlation values (run_id, request_id, component, memory_base_id) for the current context."""
    tokens = []
    for key, value in values.items():
        var = _CORRELATION_VARS.get(key)
        if var is None:
            raise KeyError(f"unknown trace scope key: {key}")
        tokens.append((var, var.set(None if value is None else str(value))))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def _clip(value: Any, limit: int = 256) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[:limit] if text else None


def build_query_tag(
    *,
    flow_id: str | None = None,
    run_id: str | None = None,
    component: str | None = None,
    memory_base_id: str | None = None,
    request_id: str | None = None,
) -> str:
    """Return the §7.2 JSON query tag. Explicit arguments win over ContextVar values.

    All seven keys are always present (``null`` when unknown) so QUERY_HISTORY can be joined to
    LANGFLOW_FLOW_RUNS by ``run_id`` with a stable shape.
    """
    if flow_id is None:
        try:
            from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

            flow_id = CURRENT_SNOWFLAKE_FLOW_ID.get()
        except Exception:  # noqa: BLE001
            flow_id = None
    tag = {
        "app": APP_NAME,
        "v": _version(),
        "flow_id": _clip(flow_id),
        "run_id": _clip(run_id if run_id is not None else CURRENT_RUN_ID.get()),
        "component": _clip(component if component is not None else CURRENT_COMPONENT.get(), 128),
        "memory_base_id": _clip(memory_base_id if memory_base_id is not None else CURRENT_MEMORY_BASE_ID.get()),
        "request_id": _clip(request_id if request_id is not None else CURRENT_REQUEST_ID.get()),
    }
    out = json.dumps(tag, separators=(",", ":"), ensure_ascii=True)
    if len(out) > QUERY_TAG_MAX_LEN:  # only reachable with pathological ids; keep it valid JSON
        tag.update(component=_clip(tag["component"], 32), flow_id=_clip(tag["flow_id"], 64))
        out = json.dumps(tag, separators=(",", ":"), ensure_ascii=True)
    return out


class AuditAction(str, enum.Enum):
    """§7.4 audit vocabulary."""

    MEMORY_BASE_CREATE = "MEMORY_BASE_CREATE"
    MEMORY_BASE_UPDATE = "MEMORY_BASE_UPDATE"
    MEMORY_BASE_DELETE = "MEMORY_BASE_DELETE"
    MEMORY_INGEST_BATCH = "MEMORY_INGEST_BATCH"
    MEMORY_INGEST_FAILED = "MEMORY_INGEST_FAILED"
    MEMORY_RECALL = "MEMORY_RECALL"
    KNOWLEDGE_BASE_CREATE = "KNOWLEDGE_BASE_CREATE"
    KNOWLEDGE_BASE_INGEST = "KNOWLEDGE_BASE_INGEST"
    KNOWLEDGE_BASE_DELETE = "KNOWLEDGE_BASE_DELETE"
    STAGE_FILE_UPLOAD = "STAGE_FILE_UPLOAD"
    STAGE_FILE_REMOVE = "STAGE_FILE_REMOVE"
    STAGE_FILE_READ = "STAGE_FILE_READ"
    STAGE_DIRECTORY_REFRESH = "STAGE_DIRECTORY_REFRESH"
    RETENTION_PURGE = "RETENTION_PURGE"
    AUTHZ_ROLE_DENIED = "AUTHZ_ROLE_DENIED"
    # Existing actions kept unchanged (§7.4).
    STAGE_CREATE = "STAGE_CREATE"
    UPDATE_CONTEXT = "UPDATE_CONTEXT"
    OBJECT_REMOVED_FROM_CONTEXT = "OBJECT_REMOVED_FROM_CONTEXT"


DEPRECATED_ACTIONS: frozenset[str] = frozenset({"MEMORY_CREATE_PHYSICAL", "MEMORY_USE_EXISTING", "MEMORY_CREATE_OR_BIND"})


class AuditOutcome(str, enum.Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    DENIED = "DENIED"


# Details are metadata only: ids, counts, sizes, hashes (TR-01). Free text is never recorded.
_DETAIL_ALLOWLIST: frozenset[str] = frozenset(
    {
        "memory_base_id", "knowledge_base_id", "name", "table_fqn", "embedding_model", "threshold",
        "job_id", "messages_read", "chunks_written", "skipped_kill_phrase", "duration_ms",
        "top_k", "n_results", "session_filter", "stage", "relative_path", "size", "md5",
        "rows", "query_id", "required_privilege", "object", "status_code", "origin_user",
        "execution_mode",
    }
)
_DETAIL_MAX_STR = 512


def sanitize_details(details: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only allowlisted, scalar metadata keys; drop everything else (no text content)."""
    clean: dict[str, Any] = {}
    for key, value in (details or {}).items():
        if key not in _DETAIL_ALLOWLIST:
            continue
        if value is None or isinstance(value, (bool, int, float)):
            clean[key] = value
        else:
            clean[key] = str(value)[:_DETAIL_MAX_STR]
    return clean


def deterministic_event_id(
    action: str, target_object: str | None, run_id: str | None, request_id: str | None, discriminator: str | None = None
) -> str:
    """sha256 over (action, target, run_id, request_id, discriminator): a retry yields the same id."""
    raw = "\x1f".join(str(p or "") for p in (action, target_object, run_id, request_id, discriminator))
    return "aud-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


_FQN_PART = r'(?:[A-Za-z_][A-Za-z0-9_$]{0,254}|"[^"]{1,255}")'
_FQN_RE = re.compile(rf"^{_FQN_PART}(?:\.{_FQN_PART}){{0,2}}$")


def build_audit_insert(
    audit_table: str,
    *,
    event_id: str,
    authenticated_user: str | None,
    effective_role: str | None,
    execution_mode: str,
    context_epoch: int,
    action: str,
    outcome: str,
    error_code: str | None,
    target_object: str | None,
    flow_id: str | None,
    run_id: str | None,
    details: Mapping[str, Any] | None,
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Return (statement, SQL API bindings). Idempotent: MERGE on event_id, so retries do not duplicate."""
    if not _FQN_RE.match(audit_table or ""):
        raise ValueError("invalid audit table identifier")
    details_json = json.dumps(sanitize_details(details), separators=(",", ":"))
    stmt = (
        f"MERGE INTO {audit_table} t USING (SELECT ? AS event_id) s ON t.event_id = s.event_id "
        "WHEN NOT MATCHED THEN INSERT (event_id, event_ts, authenticated_user, effective_role, execution_mode, "
        "context_epoch, action, outcome, error_code, target_object, project_id, flow_id, run_id, trace_id, "
        "redacted_details) VALUES (s.event_id, CURRENT_TIMESTAMP(), ?, ?, ?, ?, ?, ?, ?, ?, 'default', ?, ?, ?, "
        "TRY_PARSE_JSON(?))"
    )
    values: list[tuple[str, Any]] = [
        ("TEXT", event_id),
        ("TEXT", authenticated_user),
        ("TEXT", effective_role),
        ("TEXT", execution_mode),
        ("FIXED", int(context_epoch or 0)),
        ("TEXT", action),
        ("TEXT", outcome),
        ("TEXT", error_code),
        ("TEXT", target_object),
        ("TEXT", flow_id),
        ("TEXT", run_id),
        ("TEXT", run_id),
        ("TEXT", details_json),
    ]
    bindings = {str(i + 1): {"type": t, "value": None if v is None else str(v)} for i, (t, v) in enumerate(values)}
    return stmt, bindings


def outcome_from_result(result: Mapping[str, Any]) -> tuple[AuditOutcome, str | None]:
    """Derive outcome strictly from a real SQL API result dict. Raises if no result is given."""
    if not isinstance(result, Mapping) or "ok" not in result:
        raise ValueError("audit emit requires a real execution result with an 'ok' field")
    if result.get("ok") is True:
        return AuditOutcome.SUCCESS, None
    code = result.get("error_code") or ("NOT_CONFIGURED" if result.get("mode") == "offline" else "SNOWFLAKE_ERROR")
    if int(result.get("status_code") or 0) == 403 or code == "AUTHZ_ROLE_DENIED":
        return AuditOutcome.DENIED, str(code)
    return AuditOutcome.FAILURE, str(code)


def emit(
    writer: Callable[..., Any],
    action: AuditAction | str,
    result: Mapping[str, Any],
    *,
    target_object: str | None = None,
    details: Mapping[str, Any] | None = None,
    flow_id: str | None = None,
    run_id: str | None = None,
    discriminator: str | None = None,
) -> str:
    """Emit one §7.4 audit event for an operation that has *already* produced ``result``.

    ``writer`` is the bridge's ``record_audit_event`` (keyword API). Returns the event id.
    Deprecated or unknown actions are rejected so they can never be written again.
    """
    name = action.value if isinstance(action, AuditAction) else str(action)
    if name in DEPRECATED_ACTIONS:
        raise ValueError(f"deprecated audit action: {name}")
    AuditAction(name)  # raises ValueError for anything outside the vocabulary
    outcome, error_code = outcome_from_result(result)
    eff_run = run_id if run_id is not None else CURRENT_RUN_ID.get()
    det = dict(details or {})
    if result.get("statement_handle") and "query_id" not in det:
        det["query_id"] = result.get("statement_handle")
    event_id = deterministic_event_id(name, target_object, eff_run, CURRENT_REQUEST_ID.get(), discriminator)
    writer(
        action=name,
        outcome=outcome.value,
        error_code=error_code,
        target_object=target_object,
        details=sanitize_details(det),
        flow_id=flow_id,
        run_id=eff_run,
        event_id=event_id,
    )
    log_event("audit", action=name, outcome=outcome.value, flow_id=flow_id, run_id=eff_run, error_code=error_code)
    return event_id


LOG_FIELDS: tuple[str, ...] = (
    "event", "flow_id", "run_id", "request_id", "component", "action", "outcome", "error_code", "duration_ms",
    "n_results",
)


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line; only allowlisted fields, never message content (TR-08)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
        }
        fields = getattr(record, "sflf", None) or {}
        for key in LOG_FIELDS:
            if key in fields and fields[key] is not None:
                value = fields[key]
                payload[key] = value if isinstance(value, (bool, int, float)) else str(value)[:256]
        payload.setdefault("event", record.getMessage()[:64])
        return json.dumps(payload, separators=(",", ":"))


LOGGER_NAME = "lfx_snowflake.trace"


def get_trace_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not any(isinstance(h.formatter, JsonLogFormatter) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(event: str, **fields: Any) -> None:
    fields.setdefault("request_id", CURRENT_REQUEST_ID.get())
    fields.setdefault("run_id", CURRENT_RUN_ID.get())
    fields.setdefault("component", CURRENT_COMPONENT.get())
    fields["event"] = event
    get_trace_logger().info(event, extra={"sflf": fields})
