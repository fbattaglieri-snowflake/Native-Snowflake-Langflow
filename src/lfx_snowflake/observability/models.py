"""Data models and canonical DDL identifiers for SnowflakeLangflow observability.

Implements ADR-007, REQ-15, [G-A1], and [G-A2]:
- Plane 1: SPCS container structured JSON logs + OTLP spans/metrics to Event Table
- Plane 2: Langflow Flow Activity UI + idempotent SQL MERGE replica into
  <DB>.<SCHEMA>.LANGFLOW_FLOW_RUNS and <DB>.<SCHEMA>.LANGFLOW_FLOW_SPANS
- Plane 3: Durable transactional governance audit outbox into
  <DB>.<SCHEMA>.LANGFLOW_AUDIT_LOG
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import re
from typing import Any
import uuid


CANONICAL_FLOW_ACTIVITY_TABLE = "LANGFLOW_FLOW_ACTIVITY"
CANONICAL_FLOW_RUNS_TABLE = "LANGFLOW_FLOW_RUNS"
CANONICAL_FLOW_SPANS_TABLE = "LANGFLOW_FLOW_SPANS"
CANONICAL_AUDIT_LOG_TABLE = "LANGFLOW_AUDIT_LOG"
CANONICAL_ROW_ACCESS_POLICY = "LANGFLOW_TRACE_RAP"
CANONICAL_ACTIVE_FLOW_ACTIVITY_VIEW = "V_LANGFLOW_ACTIVE_FLOW_ACTIVITY"
CANONICAL_ACTIVE_FLOW_RUNS_VIEW = "V_LANGFLOW_ACTIVE_FLOW_RUNS"

FLOW_ACTIVITY_GOVERNED_COLUMNS: tuple[str, ...] = (
    "ACTIVITY_ID",
    "TRACE_ID",
    "EVENT_TYPE",
    "FLOW_ID",
    "FLOW_NAME",
    "PROJECT_ID",
    "PROJECT_NAME",
    "SESSION_ID",
    "RUN_TIMESTAMP",
    "STATUS",
    "LATENCY_MS",
    "INPUT_TEXT",
    "OUTPUT_TEXT",
    "ERROR_MESSAGE",
    "PROMPT_TOKENS",
    "COMPLETION_TOKENS",
    "TOTAL_TOKENS",
    "USER_IDENTITY",
    "ACTIVE_ROLE",
    "CONTEXT_EPOCH",
    "FLOW_CONTEXT_JSON",
    "INPUT_PAYLOAD",
    "OUTPUT_PAYLOAD",
    "NODE_SPANS_JSON",
    "SNOWFLAKE_QUERY_IDS",
    "DELETED_IN_UI_AT",
    "PURGED_FOR_RETENTION_AT",
    "REPLICATED_AT",
)

_IDENTIFIER_RE = re.compile(r"^(?:<[A-Z0-9_]+>|[A-Za-z_][A-Za-z0-9_$]*)$")


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def validate_identifier_or_placeholder(identifier: str, field_name: str = "identifier") -> str:
    """Validate a Snowflake SQL identifier or parametric template placeholder."""
    cleaned = (identifier or "").strip()
    if not cleaned or not _IDENTIFIER_RE.match(cleaned):
        raise ValueError(f"Invalid Snowflake identifier or placeholder for {field_name}: {identifier!r}")
    return cleaned


class ObservabilityPlane(str, Enum):
    """Three distinct observability planes defined by ADR-007."""

    SPCS_INFRASTRUCTURE_OTLP = "SPCS_INFRASTRUCTURE_OTLP"
    FLOW_ACTIVITY_TRACES = "FLOW_ACTIVITY_TRACES"
    GOVERNANCE_AUDIT_OUTBOX = "GOVERNANCE_AUDIT_OUTBOX"


class ExecutionMode(str, Enum):
    """Dual identity execution mode (ADR-003)."""

    INTERACTIVE = "INTERACTIVE"
    AUTOMATION = "AUTOMATION"


class ErrorTaxonomyCode(str, Enum):
    """Four mandatory error/denial codes (ADR-004)."""

    AUTHZ_ROLE_DENIED = "AUTHZ_ROLE_DENIED"
    OBJECT_NOT_FOUND_OR_UNAUTHORIZED = "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"
    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"
    TECHNICAL_EXECUTION_ERROR = "TECHNICAL_EXECUTION_ERROR"


class FlowActivityEventType(str, Enum):
    """Canonical event types stored in LANGFLOW_FLOW_ACTIVITY (CTX-ARCH-06)."""

    FLOW_RUN = "FLOW_RUN"
    NODE_BUILD = "NODE_BUILD"
    CONTEXT_CHANGE = "CONTEXT_CHANGE"
    GOVERNANCE_AUDIT = "GOVERNANCE_AUDIT"


@dataclass(frozen=True)
class ObservabilityTableConfig:
    """Parametric configuration for canonical Snowflake observability tables ([G-A1], CTX-ARCH-06)."""

    database: str = "<DATABASE>"
    schema: str = "<SCHEMA>"

    def __post_init__(self) -> None:
        validate_identifier_or_placeholder(self.database, "database")
        validate_identifier_or_placeholder(self.schema, "schema")

    @property
    def prefix(self) -> str:
        return f"{self.database}.{self.schema}"

    @property
    def flow_activity_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_FLOW_ACTIVITY_TABLE}"

    @property
    def active_flow_activity_view_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_ACTIVE_FLOW_ACTIVITY_VIEW}"

    @property
    def active_flow_runs_view_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_ACTIVE_FLOW_RUNS_VIEW}"

    @property
    def flow_runs_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_FLOW_RUNS_TABLE}"

    @property
    def flow_spans_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_FLOW_SPANS_TABLE}"

    @property
    def audit_log_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_AUDIT_LOG_TABLE}"

    @property
    def row_access_policy_fqn(self) -> str:
        return f"{self.prefix}.{CANONICAL_ROW_ACCESS_POLICY}"


@dataclass
class SpcsStructuredLogEvent:
    """Plane 1: SPCS container JSON stdout/stderr and OTLP event representation."""

    message: str
    severity: str = "INFO"
    service_name: str = "snowflake-langflow"
    plane: ObservabilityPlane = ObservabilityPlane.SPCS_INFRASTRUCTURE_OTLP
    timestamp: str = field(default_factory=utc_now_iso)
    trace_id: str | None = None
    span_id: str | None = None
    run_id: str | None = None
    flow_id: str | None = None
    effective_role: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_otlp_json(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "severity_text": self.severity,
            "body": self.message,
            "resource": {
                "service.name": self.service_name,
                "observability.plane": self.plane.value,
            },
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "attributes": {
                "langflow.run_id": self.run_id,
                "langflow.flow_id": self.flow_id,
                "snowflake.effective_role": self.effective_role,
                **self.attributes,
            },
        }


@dataclass
class FlowRunRecord:
    """Plane 2: Flow execution run record for Flow Activity UI and LANGFLOW_FLOW_RUNS replica."""

    run_id: str
    trace_id: str
    project_id: str
    flow_id: str
    flow_version_hash: str
    session_id: str
    authenticated_user: str
    effective_role: str
    flow_name: str = "Flow"
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    execution_mode: ExecutionMode = ExecutionMode.INTERACTIVE
    start_ts: str = field(default_factory=utc_now_iso)
    end_ts: str | None = None
    latency_ms: float | None = None
    status: str = "RUNNING"
    error_category: str | None = None
    error_message: str | None = None
    input_payload: dict[str, Any] = field(default_factory=dict)
    output_payload: dict[str, Any] = field(default_factory=dict)
    # Token metrics: MUST stay None (SQL NULL) when unmeasured, never fabricated as 0
    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    snowflake_query_ids: list[str] = field(default_factory=list)
    proxy_request_ids: list[str] = field(default_factory=list)
    proxy_profile_revision: str | None = None
    cortex_thread_id: str | None = None
    cortex_parent_message_id: str | None = None
    a2a_task_id: str | None = None
    deleted_in_ui_at: str | None = None

    @property
    def total_tokens(self) -> int | None:
        """Return sum of measured token counters, or None if no token counter was measured.

        Distinguishes NULL (unmeasured) from 0 (explicitly measured zero tokens).
        """
        measured = [
            t for t in (self.input_tokens, self.output_tokens, self.reasoning_tokens) if t is not None
        ]
        if not measured:
            return None
        return sum(measured)

    def to_flow_activity_ui_row(self) -> dict[str, Any]:
        """Format record for Langflow Flow Activity -> Runs table in UI."""
        return {
            "Run": self.run_id,
            "Flow Name": self.flow_name,
            "Trace ID": self.trace_id,
            "Session ID": self.session_id,
            "Timestamp": self.start_ts,
            "Input": self.input_payload,
            "Output": self.output_payload,
            "Token": self.total_tokens,
            "Input Tokens": self.input_tokens,
            "Output Tokens": self.output_tokens,
            "Reasoning Tokens": self.reasoning_tokens,
            "Latency": self.latency_ms,
            "Status": self.status,
            "Effective Role": self.effective_role,
            "Error Category": self.error_category,
            "Deleted In UI At": self.deleted_in_ui_at,
        }

    def to_snowflake_row(self) -> dict[str, Any]:
        """Format record for <DB>.<SCHEMA>.LANGFLOW_FLOW_RUNS."""
        return {
            "event_id": self.event_id,
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "project_id": self.project_id,
            "flow_id": self.flow_id,
            "flow_name": self.flow_name,
            "flow_version_hash": self.flow_version_hash,
            "session_id": self.session_id,
            "authenticated_user": self.authenticated_user,
            "effective_role": self.effective_role,
            "execution_mode": (
                self.execution_mode.value
                if isinstance(self.execution_mode, ExecutionMode)
                else str(self.execution_mode)
            ),
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "latency_ms": self.latency_ms,
            "status": self.status,
            "error_category": self.error_category,
            "error_message": self.error_message,
            "input_payload": self.input_payload,
            "output_payload": self.output_payload,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "total_tokens": self.total_tokens,
            "snowflake_query_ids": list(dict.fromkeys(self.snowflake_query_ids)),
            "proxy_request_ids": list(dict.fromkeys(self.proxy_request_ids)),
            "proxy_profile_revision": self.proxy_profile_revision,
            "cortex_thread_id": self.cortex_thread_id,
            "cortex_parent_message_id": self.cortex_parent_message_id,
            "a2a_task_id": self.a2a_task_id,
            "deleted_in_ui_at": self.deleted_in_ui_at,
        }


@dataclass
class FlowSpanRecord:
    """Plane 2: Component/vertex span record for <DB>.<SCHEMA>.LANGFLOW_FLOW_SPANS."""

    span_id: str
    run_id: str
    trace_id: str
    project_id: str
    flow_id: str
    flow_version_hash: str
    session_id: str
    component_id: str
    component_name: str
    component_type: str
    authenticated_user: str
    effective_role: str
    parent_span_id: str | None = None
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    execution_mode: ExecutionMode = ExecutionMode.INTERACTIVE
    start_ts: str = field(default_factory=utc_now_iso)
    end_ts: str | None = None
    latency_ms: float | None = None
    status: str = "SUCCESS"
    error_category: str | None = None
    error_message: str | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    snowflake_query_ids: list[str] = field(default_factory=list)
    proxy_request_ids: list[str] = field(default_factory=list)
    proxy_profile_revision: str | None = None
    cortex_thread_id: str | None = None
    cortex_parent_message_id: str | None = None
    a2a_task_id: str | None = None
    deleted_in_ui_at: str | None = None

    @property
    def total_tokens(self) -> int | None:
        measured = [
            t for t in (self.input_tokens, self.output_tokens, self.reasoning_tokens) if t is not None
        ]
        if not measured:
            return None
        return sum(measured)

    def to_snowflake_row(self) -> dict[str, Any]:
        """Format record for <DB>.<SCHEMA>.LANGFLOW_FLOW_SPANS."""
        return {
            "event_id": self.event_id,
            "span_id": self.span_id,
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "parent_span_id": self.parent_span_id,
            "project_id": self.project_id,
            "flow_id": self.flow_id,
            "flow_version_hash": self.flow_version_hash,
            "session_id": self.session_id,
            "component_id": self.component_id,
            "component_name": self.component_name,
            "component_type": self.component_type,
            "authenticated_user": self.authenticated_user,
            "effective_role": self.effective_role,
            "execution_mode": (
                self.execution_mode.value
                if isinstance(self.execution_mode, ExecutionMode)
                else str(self.execution_mode)
            ),
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "latency_ms": self.latency_ms,
            "status": self.status,
            "error_category": self.error_category,
            "error_message": self.error_message,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "total_tokens": self.total_tokens,
            "snowflake_query_ids": list(dict.fromkeys(self.snowflake_query_ids)),
            "proxy_request_ids": list(dict.fromkeys(self.proxy_request_ids)),
            "proxy_profile_revision": self.proxy_profile_revision,
            "cortex_thread_id": self.cortex_thread_id,
            "cortex_parent_message_id": self.cortex_parent_message_id,
            "a2a_task_id": self.a2a_task_id,
            "deleted_in_ui_at": self.deleted_in_ui_at,
        }


@dataclass
class AuditEventRecord:
    """Plane 3: Governance audit record for <DB>.<SCHEMA>.LANGFLOW_AUDIT_LOG."""

    action_category: str
    operation: str
    authenticated_user: str
    effective_role: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=utc_now_iso)
    execution_mode: ExecutionMode = ExecutionMode.INTERACTIVE
    target_object_fqn: str | None = None
    project_id: str | None = None
    flow_id: str | None = None
    run_id: str | None = None
    trace_id: str | None = None
    session_id: str | None = None
    outcome: str = "SUCCESS"
    error_code: str | None = None
    required_privilege: str | None = None
    denial_message: str | None = None
    snowflake_query_ids: list[str] = field(default_factory=list)
    proxy_request_ids: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def to_snowflake_row(self) -> dict[str, Any]:
        """Format record for <DB>.<SCHEMA>.LANGFLOW_AUDIT_LOG."""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "action_category": self.action_category,
            "operation": self.operation,
            "authenticated_user": self.authenticated_user,
            "effective_role": self.effective_role,
            "execution_mode": (
                self.execution_mode.value
                if isinstance(self.execution_mode, ExecutionMode)
                else str(self.execution_mode)
            ),
            "target_object_fqn": self.target_object_fqn,
            "project_id": self.project_id,
            "flow_id": self.flow_id,
            "run_id": self.run_id,
            "trace_id": self.trace_id,
            "session_id": self.session_id,
            "outcome": self.outcome,
            "error_code": self.error_code,
            "required_privilege": self.required_privilege,
            "denial_message": self.denial_message,
            "snowflake_query_ids": list(dict.fromkeys(self.snowflake_query_ids)),
            "proxy_request_ids": list(dict.fromkeys(self.proxy_request_ids)),
            "details": self.details,
        }


@dataclass
class FlowActivityRecord:
    """Unified Flow Activity record for <DB>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY (CTX-ARCH-06)."""

    activity_id: str
    flow_id: str
    user_identity: str
    active_role: str
    trace_id: str | None = None
    event_type: str = FlowActivityEventType.FLOW_RUN.value
    flow_name: str | None = "Flow"
    project_id: str | None = None
    project_name: str | None = None
    session_id: str | None = None
    run_timestamp: str = field(default_factory=utc_now_iso)
    status: str = "SUCCESS"
    latency_ms: float | None = None
    input_text: str | None = None
    output_text: str | None = None
    error_message: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    context_epoch: int | None = None
    flow_context_json: dict[str, Any] = field(default_factory=dict)
    input_payload: dict[str, Any] = field(default_factory=dict)
    output_payload: dict[str, Any] = field(default_factory=dict)
    node_spans_json: list[dict[str, Any]] | dict[str, Any] = field(default_factory=list)
    snowflake_query_ids: list[str] = field(default_factory=list)
    deleted_in_ui_at: str | None = None
    purged_for_retention_at: str | None = None
    replicated_at: str = field(default_factory=utc_now_iso)

    @classmethod
    def from_flow_run(
        cls,
        run: FlowRunRecord,
        *,
        project_name: str | None = None,
        context_epoch: int | None = None,
        flow_context_json: dict[str, Any] | None = None,
        node_spans_json: list[dict[str, Any]] | dict[str, Any] | None = None,
    ) -> FlowActivityRecord:
        """Create a unified FlowActivityRecord from a FlowRunRecord."""
        in_text = None
        if isinstance(run.input_payload, dict):
            in_text = str(
                run.input_payload.get("input_value")
                or run.input_payload.get("text")
                or run.input_payload.get("value")
                or (run.input_payload if run.input_payload else "")
            )
        out_text = None
        if isinstance(run.output_payload, dict):
            out_text = str(
                run.output_payload.get("output_value")
                or run.output_payload.get("text")
                or run.output_payload.get("value")
                or (run.output_payload if run.output_payload else "")
            )
        p_tokens = int(run.input_tokens or 0)
        c_tokens = int(run.output_tokens or 0)
        r_tokens = int(run.reasoning_tokens or 0)
        tot_tokens = int(run.total_tokens if run.total_tokens is not None else (p_tokens + c_tokens + r_tokens))
        return cls(
            activity_id=run.run_id,
            trace_id=run.trace_id,
            event_type=FlowActivityEventType.FLOW_RUN.value,
            flow_id=run.flow_id,
            flow_name=run.flow_name,
            project_id=run.project_id,
            project_name=project_name or run.project_id,
            session_id=run.session_id,
            run_timestamp=run.start_ts,
            status=run.status,
            latency_ms=run.latency_ms,
            input_text=in_text,
            output_text=out_text,
            error_message=run.error_message,
            prompt_tokens=p_tokens,
            completion_tokens=c_tokens,
            total_tokens=tot_tokens,
            user_identity=run.authenticated_user,
            active_role=run.effective_role,
            context_epoch=context_epoch,
            flow_context_json=dict(flow_context_json or {}),
            input_payload=dict(run.input_payload or {}),
            output_payload=dict(run.output_payload or {}),
            node_spans_json=list(node_spans_json) if isinstance(node_spans_json, list) else (node_spans_json or []),
            snowflake_query_ids=list(dict.fromkeys(run.snowflake_query_ids)),
            deleted_in_ui_at=run.deleted_in_ui_at,
        )

    def to_snowflake_row(self) -> dict[str, Any]:
        """Format record with all 25+ governed columns for <DB>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY."""
        ev_type = (
            self.event_type.value
            if isinstance(self.event_type, FlowActivityEventType)
            else str(self.event_type or FlowActivityEventType.FLOW_RUN.value)
        )
        return {
            "activity_id": self.activity_id,
            "trace_id": self.trace_id or self.activity_id,
            "event_type": ev_type,
            "flow_id": self.flow_id,
            "flow_name": self.flow_name,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "session_id": self.session_id,
            "run_timestamp": self.run_timestamp,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "input_text": self.input_text,
            "output_text": self.output_text,
            "error_message": self.error_message,
            "prompt_tokens": int(self.prompt_tokens or 0),
            "completion_tokens": int(self.completion_tokens or 0),
            "total_tokens": int(
                self.total_tokens
                if self.total_tokens is not None
                else (int(self.prompt_tokens or 0) + int(self.completion_tokens or 0))
            ),
            "user_identity": self.user_identity,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "flow_context_json": self.flow_context_json,
            "input_payload": self.input_payload,
            "output_payload": self.output_payload,
            "node_spans_json": self.node_spans_json,
            "snowflake_query_ids": list(dict.fromkeys(self.snowflake_query_ids)),
            "deleted_in_ui_at": self.deleted_in_ui_at,
            "purged_for_retention_at": self.purged_for_retention_at,
            "replicated_at": self.replicated_at,
        }

    def to_v2_trace_item(self) -> dict[str, Any]:
        """Format record for Langflow v1.12.5 GET /api/v2/traces (Flow Activity -> Runs)."""
        return {
            "id": self.activity_id,
            "trace_id": self.trace_id or self.activity_id,
            "name": self.flow_name or self.flow_id,
            "flow_id": self.flow_id,
            "flow_name": self.flow_name or self.flow_id,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "session_id": self.session_id or self.flow_id,
            "timestamp": self.run_timestamp,
            "start_time": self.run_timestamp,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "total_latency_ms": self.latency_ms,
            "input": self.input_payload if self.input_payload else (self.input_text or ""),
            "output": self.output_payload if self.output_payload else (self.output_text or ""),
            "input_text": self.input_text,
            "output_text": self.output_text,
            "error_message": self.error_message,
            "prompt_tokens": int(self.prompt_tokens or 0),
            "completion_tokens": int(self.completion_tokens or 0),
            "total_tokens": int(self.total_tokens or 0),
            "user_identity": self.user_identity,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "flow_context_json": self.flow_context_json,
            "spans": self.node_spans_json if isinstance(self.node_spans_json, list) else [self.node_spans_json],
            "deleted_in_ui_at": self.deleted_in_ui_at,
        }

