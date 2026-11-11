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
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

APP_NAME = "snowflake-langflow"
QUERY_TAG_MAX_LEN = 2000  # Snowflake QUERY_TAG limit
QUERY_TAG_KEYS = (
    "app", "v", "flow_id", "run_id", "trace_id", "session_id", "component", "memory_base_id", "request_id",
)
DEFAULT_COMPONENT = "bridge"

CURRENT_RUN_ID: ContextVar[str | None] = ContextVar("sflf_current_run_id", default=None)
CURRENT_REQUEST_ID: ContextVar[str | None] = ContextVar("sflf_current_request_id", default=None)
CURRENT_COMPONENT: ContextVar[str | None] = ContextVar("sflf_current_component", default=None)
CURRENT_MEMORY_BASE_ID: ContextVar[str | None] = ContextVar("sflf_current_memory_base_id", default=None)
CURRENT_TRACE_ID: ContextVar[str | None] = ContextVar("sflf_current_trace_id", default=None)
CURRENT_SESSION_ID: ContextVar[str | None] = ContextVar("sflf_current_session_id", default=None)

_CORRELATION_VARS: dict[str, ContextVar[str | None]] = {
    "run_id": CURRENT_RUN_ID,
    "request_id": CURRENT_REQUEST_ID,
    "component": CURRENT_COMPONENT,
    "memory_base_id": CURRENT_MEMORY_BASE_ID,
    "trace_id": CURRENT_TRACE_ID,
    "session_id": CURRENT_SESSION_ID,
}


_HEX32_RE = re.compile(r"^[0-9a-fA-F]{32}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def canonical_flow_id(value: Any) -> str | None:
    """BUG-MCP-02: hyphenated lower-case UUID (8-4-4-4-12) for UUID-like flow ids; other keys unchanged."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "undefined"}:
        return None
    if _HEX32_RE.match(text) or _UUID_RE.match(text):
        return str(uuid.UUID(text))
    return text


def _component_from_stack(max_depth: int = 40) -> str | None:
    """Name of the nearest Langflow component on the call stack (``display_name``/``name``), if any."""
    import sys  # noqa: PLC0415

    frame = sys._getframe(2)
    for _ in range(max_depth):
        if frame is None:
            return None
        obj = frame.f_locals.get("self")
        if obj is not None and not isinstance(obj, type):
            cls = type(obj)
            mod = getattr(cls, "__module__", "") or ""
            if ("components" in mod or "component" in cls.__name__.lower()) and "fastapi_live_bridge" not in mod:
                name = getattr(obj, "display_name", None) or getattr(cls, "display_name", None) or cls.__name__
                if isinstance(name, str) and name:
                    return name
        frame = frame.f_back
    return None


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


def valid_uuid_or_none(value: Any) -> str | None:
    """Return the canonical form of ``value`` if it is a UUID, else None (untrusted header input)."""
    if not value:
        return None
    try:
        return str(uuid.UUID(str(value).strip()[:64]))
    except (ValueError, AttributeError, TypeError):
        return None


def request_id_from_header(value: Any) -> str:
    """Use an incoming ``X-Request-Id`` only when it is a valid UUID; otherwise mint a new one."""
    return valid_uuid_or_none(value) or str(uuid.uuid4())


FLOW_RUN_PATH_SEGMENTS: tuple[str, ...] = ("/api/v1/build/", "/api/v1/run/", "/api/v1/webhook/")


def is_flow_run_path(path: str) -> bool:
    return any(seg in (path or "") for seg in FLOW_RUN_PATH_SEGMENTS)


def run_id_for_request(path: str, header_value: Any = None) -> str | None:
    """Flow run id for flow execution routes: incoming ``X-Run-Id`` if a valid UUID, else a new UUID.

    Non-run routes get ``None`` (no run exists), never a placeholder.
    """
    if not is_flow_run_path(path):
        return None
    return valid_uuid_or_none(header_value) or str(uuid.uuid4())


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
    eff_run_id = run_id if run_id is not None else CURRENT_RUN_ID.get()
    run_rec = None
    try:
        from lfx_snowflake.telemetry import CURRENT_RUN_RECORD, RUNS  # noqa: PLC0415

        run_rec = CURRENT_RUN_RECORD.get() or RUNS.get(eff_run_id)
    except Exception:  # noqa: BLE001
        run_rec = None
    if flow_id is None and run_rec is not None:
        flow_id = run_rec.flow_id
    # BUG-UI-TRC-01: a component name is always present (explicit > bound > nearest component > "bridge").
    eff_component = component if component is not None else CURRENT_COMPONENT.get()
    if not eff_component:
        eff_component = _component_from_stack() or DEFAULT_COMPONENT
    tag = {
        "app": APP_NAME,
        "v": _version(),
        "flow_id": _clip(canonical_flow_id(flow_id)),
        "run_id": _clip(eff_run_id),
        "trace_id": _clip(CURRENT_TRACE_ID.get() or (run_rec.trace_id if run_rec is not None else None)),
        "session_id": _clip(CURRENT_SESSION_ID.get() or (run_rec.session_id if run_rec is not None else None)),
        "component": _clip(eff_component, 128),
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
        # Identifier / count metadata used by context, role and flow audit events (no free text).
        "flow_id", "database", "schema", "databases", "schemas_by_db", "full_databases", "full_schemas",
        "postgres_instances", "selected_pairs_count", "added_objects", "removed_objects", "removed_databases",
        "removed_schemas", "context_epoch", "previous_role", "new_role", "requested_role", "available_roles",
        "role", "agent_fqn",
        # BUG-UI-TRC-01 correlation ids (no content).
        "request_id", "session_id", "trace_id", "cortex_thread_id", "a2a_task_id", "retained_objects_count", "hidden_by_role_count", "trigger_type", "path", "method",
        "durable_snowflake_audit_preserved", "source", "object_kind", "hierarchy_path",
        # Rerun-3 ROLE_SWITCH audit: who switched, through which channel, with what result (identifiers only).
        "user", "channel", "outcome", "error_code", "display_only",
    }
)
# Never allowlisted: message, reason, ddl, sql, text, content, prompt, trigger_payload, result, ...
_DETAIL_MAX_STR = 512
_DETAIL_MAX_ITEMS = 200


def _sanitize_value(value: Any, depth: int = 0) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:_DETAIL_MAX_STR]
    if depth >= 2:
        return None
    if isinstance(value, Mapping):
        return {
            str(k)[:_DETAIL_MAX_STR]: _sanitize_value(v, depth + 1)
            for k, v in list(value.items())[:_DETAIL_MAX_ITEMS]
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize_value(v, depth + 1) for v in list(value)[:_DETAIL_MAX_ITEMS]]
    return str(value)[:_DETAIL_MAX_STR]


def sanitize_details(details: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep only allowlisted metadata keys (ids, names, counts); drop everything else (no text content).

    Values are scalars, or bounded lists/maps of scalars (identifier lists such as removed objects).
    """
    clean: dict[str, Any] = {}
    for key, value in (details or {}).items():
        if key not in _DETAIL_ALLOWLIST:
            continue
        clean[key] = _sanitize_value(value)
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
    trace_id: str | None = None,
    request_id: str | None = None,
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Return (statement, SQL API bindings). Idempotent on event_id (NOT EXISTS), so retries do not duplicate.

    TRACE_ID links the row to its request: explicit ``trace_id``, else the run id, else the bound request id.
    """
    trace_id = trace_id or CURRENT_TRACE_ID.get() or run_id or CURRENT_REQUEST_ID.get()
    flow_id = canonical_flow_id(flow_id)
    if not _FQN_RE.match(audit_table or ""):
        raise ValueError("invalid audit table identifier")
    clean_details = sanitize_details(details)
    # BUG-UI-TRC-01: the audit table has no REQUEST_ID column; request_id travels in REDACTED_DETAILS.
    req_id = request_id or CURRENT_REQUEST_ID.get()
    if req_id:
        clean_details.setdefault("request_id", str(req_id)[:64])
    details_json = json.dumps(clean_details, separators=(",", ":"))
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
        ("TEXT", trace_id),
        ("TEXT", details_json),
        ("TEXT", event_id),
    ]
    # Rerun-3: NULL values are SQL literals, never ``{"value": null}`` bindings. Every bridge statement is sent
    # as a multi-statement request (``USE SECONDARY ROLES ...;`` prefix) and the SQL API treats a null-valued
    # binding there as unset (002049 "Bind variable ? not set"), so ALLOWED events (no error_code) were lost.
    ph: list[str] = []
    bindings: dict[str, dict[str, Any]] = {}
    for t, v in values:
        if v is None:
            ph.append("NULL")
        else:
            bindings[str(len(bindings) + 1)] = {"type": t, "value": str(v)}
            ph.append("?")
    stmt = (
        f"INSERT INTO {audit_table} (event_id, event_ts, authenticated_user, effective_role, execution_mode, "
        "context_epoch, action, outcome, error_code, target_object, project_id, flow_id, run_id, trace_id, "
        f"redacted_details) SELECT {ph[0]}, CURRENT_TIMESTAMP(), {', '.join(ph[1:9])}, 'default', "
        f"{', '.join(ph[9:12])}, TRY_PARSE_JSON({ph[12]}) "
        f"WHERE NOT EXISTS (SELECT 1 FROM {audit_table} WHERE event_id = {ph[13]})"
    )
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


# --- Request id propagation to the client (W3-R1-14 follow-up) -----------------------------------

REQUEST_ID_HEADER = "X-Request-Id"
# Fallback header: some ingresses (e.g. SPCS public endpoints) drop X-Request-Id from responses.
REQUEST_ID_FALLBACK_HEADER = "X-Snowflake-Langflow-Request-Id"
REQUEST_ID_SCOPE_KEY = "sflf_request_id"
_ERROR_BODY_MAX_BYTES = 256 * 1024


def _scope_header(scope: Mapping[str, Any], name: str) -> str | None:
    target = name.lower().encode("latin-1")
    for key, value in scope.get("headers") or []:
        if key.lower() == target:
            return value.decode("latin-1", errors="ignore")
    return None


def request_id_for_scope(scope: Mapping[str, Any]) -> str:
    """Request id already assigned to this ASGI scope, else derived from the incoming header."""
    existing = scope.get(REQUEST_ID_SCOPE_KEY) if isinstance(scope, Mapping) else None
    return existing or request_id_from_header(_scope_header(scope, REQUEST_ID_HEADER))


def _with_request_id_headers(headers: list[Any], request_id: str, *, drop_length: bool = False) -> list[Any]:
    names = {REQUEST_ID_HEADER.lower().encode(), REQUEST_ID_FALLBACK_HEADER.lower().encode()}
    if drop_length:
        names.add(b"content-length")
    kept = [(k, v) for k, v in headers if k.lower() not in names]
    rid = request_id.encode("latin-1")
    return [*kept, (REQUEST_ID_HEADER.lower().encode(), rid), (REQUEST_ID_FALLBACK_HEADER.lower().encode(), rid)]


def _inject_request_id(body: bytes, request_id: str) -> bytes:
    try:
        payload = json.loads(body or b"null")
    except (ValueError, UnicodeDecodeError):
        return body
    if not isinstance(payload, dict):
        return body
    payload.setdefault("request_id", request_id)
    return json.dumps(payload).encode("utf-8")


class RequestIdASGIMiddleware:
    """Pure ASGI middleware: every HTTP response carries the request id (headers + JSON error body).

    Wraps ``send`` rather than ``call_next`` so streaming, static files, error responses and
    unhandled exceptions are all covered. Request/response content is never logged.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        request_id = request_id_for_scope(scope)
        scope[REQUEST_ID_SCOPE_KEY] = request_id
        state: dict[str, Any] = {"started": False, "start": None, "buffer": None}

        async def _send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                state["started"] = True
                status = int(message.get("status", 200))
                headers = list(message.get("headers") or [])
                ctype = next((v for k, v in headers if k.lower() == b"content-type"), b"")
                if status >= 400 and ctype.split(b";")[0].strip() == b"application/json":
                    # Buffer the (small) JSON error body so request_id can be added to the envelope.
                    state["start"] = {**message, "headers": headers}
                    state["buffer"] = bytearray()
                    return
                await send({**message, "headers": _with_request_id_headers(headers, request_id)})
                return
            if message["type"] == "http.response.body" and state["buffer"] is not None:
                state["buffer"].extend(message.get("body", b""))
                more = message.get("more_body", False)
                if more and len(state["buffer"]) <= _ERROR_BODY_MAX_BYTES:
                    return
                body = bytes(state["buffer"])
                if not more:
                    body = _inject_request_id(body, request_id)
                start = state["start"]
                headers = _with_request_id_headers(start["headers"], request_id, drop_length=True)
                if not more:
                    headers.append((b"content-length", str(len(body)).encode()))
                state["buffer"] = None
                await send({**start, "headers": headers})
                await send({"type": "http.response.body", "body": body, "more_body": more})
                return
            await send(message)

        try:
            await self.app(scope, receive, _send)
        except Exception:
            if state["started"]:
                raise
            log_event("request_unhandled_error", request_id=request_id, error_code="HTTP_500")
            body = json.dumps({"detail": "Internal Server Error", "request_id": request_id}).encode("utf-8")
            headers = _with_request_id_headers([(b"content-type", b"application/json")], request_id)
            headers.append((b"content-length", str(len(body)).encode()))
            await send({"type": "http.response.start", "status": 500, "headers": headers})
            await send({"type": "http.response.body", "body": body})


EXECUTION_MODE_CALLER = "INTERACTIVE_CALLER"
EXECUTION_MODE_SERVICE = "SERVICE"


def audit_execution_mode(effective_mode: str | None) -> str:
    """EXECUTION_MODE column value for an *effective* identity mode (``caller``/``service``)."""
    return EXECUTION_MODE_CALLER if (effective_mode or "").lower() == "caller" else EXECUTION_MODE_SERVICE
