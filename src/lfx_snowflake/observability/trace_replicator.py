"""SnowflakeTraceReplicator and three-plane observability service for SnowflakeLangflow.

Implements ADR-007, REQ-15, [G-A1], and [G-A2]:
- Plane 1: Container structured JSON logs + OTLP spans/metrics to SPCS Event Table.
- Plane 2: Langflow `Flow Activity` UI state (`Runs`, session grouping, search/filter,
  columns `Run`, `Trace ID`, `Timestamp`, `Input`, `Output`, `Token`, `Latency`, `Status`)
  synchronized via asynchronous idempotent `MERGE INTO` into
  `<DB>.<SCHEMA>.LANGFLOW_FLOW_RUNS` and `<DB>.<SCHEMA>.LANGFLOW_FLOW_SPANS`.
- Plane 3: Durable governance audit log in `<DB>.<SCHEMA>.LANGFLOW_AUDIT_LOG` via
  `TransactionalOutbox`, recording object creations, role switches, A2A lifecycle,
  trace access/deletions, and every `AUTHZ_ROLE_DENIED` denial.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from typing import Any, Callable

from .models import (
    AuditEventRecord,
    CANONICAL_AUDIT_LOG_TABLE,
    CANONICAL_FLOW_RUNS_TABLE,
    CANONICAL_FLOW_SPANS_TABLE,
    ErrorTaxonomyCode,
    ExecutionMode,
    FlowRunRecord,
    FlowSpanRecord,
    ObservabilityTableConfig,
    SpcsStructuredLogEvent,
    utc_now_iso,
)
from .outbox import OutboxFlushSummary, TransactionalOutbox
from .redaction import redact_payload_for_sink


class TraceAuthorizationError(PermissionError):
    """Explicit HTTP 403 AUTHZ_ROLE_DENIED error for trace/audit operations (ADR-004)."""

    def __init__(
        self,
        *,
        active_role: str,
        operation: str,
        required_privilege: str | None = None,
    ) -> None:
        self.error_code = ErrorTaxonomyCode.AUTHZ_ROLE_DENIED.value
        self.status_code = 403
        self.active_role = active_role
        self.operation = operation
        self.required_privilege = required_privilege
        self.message = f"Il ruolo attivo ({active_role}) non consente questa operazione ({operation})"
        super().__init__(self.message)

    def to_response_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "error_code": self.error_code,
            "status_code": self.status_code,
            "active_role": self.active_role,
            "operation": self.operation,
            "message": self.message,
        }
        if self.required_privilege:
            payload["required_privilege"] = self.required_privilege
        return payload


def generate_canonical_observability_ddl(
    table_config: ObservabilityTableConfig | None = None,
) -> str:
    """Generate canonical account-agnostic Snowflake DDL for the 3 observability tables ([G-A1])."""
    cfg = table_config or ObservabilityTableConfig()
    return f"""-- Canonical SnowflakeLangflow Observability & Governance Audit DDL ([G-A1], ADR-007)
CREATE TABLE IF NOT EXISTS {cfg.flow_runs_fqn} (
    event_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL PRIMARY KEY,
    trace_id VARCHAR NOT NULL,
    project_id VARCHAR NOT NULL,
    flow_id VARCHAR NOT NULL,
    flow_name VARCHAR,
    flow_version_hash VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    authenticated_user VARCHAR NOT NULL,
    effective_role VARCHAR NOT NULL,
    execution_mode VARCHAR NOT NULL,
    start_ts TIMESTAMP_TZ NOT NULL,
    end_ts TIMESTAMP_TZ,
    latency_ms FLOAT,
    status VARCHAR NOT NULL,
    error_category VARCHAR,
    error_message VARCHAR,
    input_payload VARIANT,
    output_payload VARIANT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    reasoning_tokens INTEGER,
    total_tokens INTEGER,
    snowflake_query_ids ARRAY,
    proxy_request_ids ARRAY,
    proxy_profile_revision VARCHAR,
    cortex_thread_id VARCHAR,
    cortex_parent_message_id VARCHAR,
    a2a_task_id VARCHAR,
    deleted_in_ui_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS {cfg.flow_spans_fqn} (
    event_id VARCHAR NOT NULL,
    span_id VARCHAR NOT NULL PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    trace_id VARCHAR NOT NULL,
    parent_span_id VARCHAR,
    project_id VARCHAR NOT NULL,
    flow_id VARCHAR NOT NULL,
    flow_version_hash VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    component_id VARCHAR NOT NULL,
    component_name VARCHAR NOT NULL,
    component_type VARCHAR NOT NULL,
    authenticated_user VARCHAR NOT NULL,
    effective_role VARCHAR NOT NULL,
    execution_mode VARCHAR NOT NULL,
    start_ts TIMESTAMP_TZ NOT NULL,
    end_ts TIMESTAMP_TZ,
    latency_ms FLOAT,
    status VARCHAR NOT NULL,
    error_category VARCHAR,
    error_message VARCHAR,
    inputs VARIANT,
    outputs VARIANT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    reasoning_tokens INTEGER,
    total_tokens INTEGER,
    snowflake_query_ids ARRAY,
    proxy_request_ids ARRAY,
    proxy_profile_revision VARCHAR,
    cortex_thread_id VARCHAR,
    cortex_parent_message_id VARCHAR,
    a2a_task_id VARCHAR,
    deleted_in_ui_at TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS {cfg.audit_log_fqn} (
    event_id VARCHAR NOT NULL PRIMARY KEY,
    timestamp TIMESTAMP_TZ NOT NULL,
    action_category VARCHAR NOT NULL,
    operation VARCHAR NOT NULL,
    authenticated_user VARCHAR NOT NULL,
    effective_role VARCHAR NOT NULL,
    execution_mode VARCHAR NOT NULL,
    target_object_fqn VARCHAR,
    project_id VARCHAR,
    flow_id VARCHAR,
    run_id VARCHAR,
    trace_id VARCHAR,
    session_id VARCHAR,
    outcome VARCHAR NOT NULL,
    error_code VARCHAR,
    required_privilege VARCHAR,
    denial_message VARCHAR,
    snowflake_query_ids ARRAY,
    proxy_request_ids ARRAY,
    details VARIANT
);

CREATE ROW ACCESS POLICY IF NOT EXISTS {cfg.row_access_policy_fqn}
AS (row_effective_role VARCHAR, row_authenticated_user VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() = row_effective_role
    OR CURRENT_USER() = row_authenticated_user;

ALTER TABLE {cfg.flow_runs_fqn}
    ADD ROW ACCESS POLICY {cfg.row_access_policy_fqn} ON (effective_role, authenticated_user);

ALTER TABLE {cfg.flow_spans_fqn}
    ADD ROW ACCESS POLICY {cfg.row_access_policy_fqn} ON (effective_role, authenticated_user);
"""


class SnowflakeReplicaStore:
    """Idempotent MERGE target representing the Snowflake SQL tables (`LANGFLOW_FLOW_RUNS`, `LANGFLOW_FLOW_SPANS`, `LANGFLOW_AUDIT_LOG`).

    Supports an optional external `sql_executor(merge_sql, payload)` while maintaining
    exact MERGE state keyed by (`run_id`, `span_id`, `event_id`) for verification and parity checks.
    """

    def __init__(
        self,
        table_config: ObservabilityTableConfig | None = None,
        sql_executor: Callable[[str, dict[str, Any]], Any] | None = None,
    ) -> None:
        self.table_config = table_config or ObservabilityTableConfig()
        self.sql_executor = sql_executor
        self.runs_table: dict[str, dict[str, Any]] = {}
        self.spans_table: dict[str, dict[str, Any]] = {}
        self.audit_table: dict[str, dict[str, Any]] = {}
        self.executed_merge_statements: list[tuple[str, str, str]] = []
        self._Transient_failure_remaining: int = 0

    def simulate_transient_failures(self, count: int) -> None:
        """Configure the sink to fail the next `count` write attempts with a transient error."""
        self._Transient_failure_remaining = max(0, count)

    def write_merged_row(
        self,
        table_kind: str,
        idempotency_key: str,
        payload: dict[str, Any],
        merge_sql: str,
    ) -> None:
        """Apply idempotent MERGE semantics for a single outbox row."""
        if self._Transient_failure_remaining > 0:
            self._Transient_failure_remaining -= 1
            raise ConnectionError("Simulated transient Snowflake sink network timeout.")

        if self.sql_executor is not None:
            self.sql_executor(merge_sql, payload)

        self.executed_merge_statements.append((table_kind, idempotency_key, merge_sql))
        row_copy = copy.deepcopy(payload)

        if table_kind == CANONICAL_FLOW_RUNS_TABLE:
            existing = self.runs_table.get(idempotency_key)
            if existing is None:
                self.runs_table[idempotency_key] = row_copy
            else:
                merged = dict(existing)
                terminal_statuses = {"SUCCESS", "ERROR", "FAILED", "CANCELLED"}
                existing_terminal = existing.get("status") in terminal_statuses
                incoming_running = row_copy.get("status") == "RUNNING"
                for k, v in row_copy.items():
                    if k in {
                        "end_ts",
                        "latency_ms",
                        "input_tokens",
                        "output_tokens",
                        "reasoning_tokens",
                        "total_tokens",
                        "error_category",
                        "error_message",
                        "cortex_thread_id",
                        "cortex_parent_message_id",
                        "a2a_task_id",
                        "deleted_in_ui_at",
                    }:
                        merged[k] = v if v is not None else existing.get(k)
                    elif k == "status" and existing_terminal and incoming_running:
                        merged[k] = existing.get(k)
                    elif k == "output_payload" and existing_terminal and incoming_running and not v:
                        merged[k] = existing.get(k)
                    elif k in {"snowflake_query_ids", "proxy_request_ids"}:
                        merged[k] = list(dict.fromkeys([*(existing.get(k) or []), *(v or [])]))
                    else:
                        merged[k] = v
                self.runs_table[idempotency_key] = merged
            return

        if table_kind == CANONICAL_FLOW_SPANS_TABLE:
            existing = self.spans_table.get(idempotency_key)
            if existing is None:
                self.spans_table[idempotency_key] = row_copy
            else:
                merged = dict(existing)
                for k, v in row_copy.items():
                    if k in {
                        "end_ts",
                        "latency_ms",
                        "input_tokens",
                        "output_tokens",
                        "reasoning_tokens",
                        "total_tokens",
                        "deleted_in_ui_at",
                    }:
                        merged[k] = v if v is not None else existing.get(k)
                    else:
                        merged[k] = v
                self.spans_table[idempotency_key] = merged
            return

        if table_kind == CANONICAL_AUDIT_LOG_TABLE:
            # Audit log is append-only / WHEN NOT MATCHED THEN INSERT on event_id
            if idempotency_key not in self.audit_table:
                self.audit_table[idempotency_key] = row_copy
            return

        raise ValueError(f"Unknown table_kind: {table_kind!r}")


class SnowflakeTraceReplicator:
    """Coordinates Plane 1 (SPCS OTLP), Plane 2 (Flow Activity UI + SQL MERGE), and Plane 3 (Audit Outbox)."""

    def __init__(
        self,
        *,
        table_config: ObservabilityTableConfig | None = None,
        outbox: TransactionalOutbox | None = None,
        replica_store: SnowflakeReplicaStore | None = None,
        authorized_trace_roles: set[str] | None = None,
        admin_audit_roles: set[str] | None = None,
    ) -> None:
        self.table_config = table_config or ObservabilityTableConfig()
        self.outbox = outbox or TransactionalOutbox(table_config=self.table_config)
        self.replica_store = replica_store or SnowflakeReplicaStore(table_config=self.table_config)
        self.authorized_trace_roles = (
            set(authorized_trace_roles)
            if authorized_trace_roles is not None
            else {"ROLE_ANALYST", "ROLE_DEVELOPER", "ROLE_ADMIN", "ROLE_DATA_ENGINEER"}
        )
        self.admin_audit_roles = (
            set(admin_audit_roles) if admin_audit_roles is not None else {"ROLE_ADMIN"}
        )
        # Plane 1: SPCS structured JSON / OTLP stream
        self.spcs_logs: list[dict[str, Any]] = []
        # Plane 2: Local Flow Activity UI state
        self._ui_runs: dict[str, FlowRunRecord] = {}
        self._ui_spans: dict[str, FlowSpanRecord] = {}

    # ------------------------------------------------------------------
    # Plane 1: SPCS Container JSON & OTLP Event Table Telemetry
    # ------------------------------------------------------------------
    def emit_spcs_log(
        self,
        message: str,
        *,
        severity: str = "INFO",
        trace_id: str | None = None,
        span_id: str | None = None,
        run_id: str | None = None,
        flow_id: str | None = None,
        effective_role: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Emit a structured JSON/OTLP log event for the SPCS Event Table (Plane 1)."""
        redacted_msg = redact_payload_for_sink(message)
        redacted_attrs = redact_payload_for_sink(attributes or {})
        evt = SpcsStructuredLogEvent(
            message=str(redacted_msg),
            severity=severity,
            trace_id=trace_id,
            span_id=span_id,
            run_id=run_id,
            flow_id=flow_id,
            effective_role=effective_role,
            attributes=redacted_attrs if isinstance(redacted_attrs, dict) else {},
        )
        otlp_payload = evt.to_otlp_json()
        self.spcs_logs.append(otlp_payload)
        return otlp_payload

    # ------------------------------------------------------------------
    # Plane 2: Flow Activity UI & Asynchronous Idempotent SQL Replica
    # ------------------------------------------------------------------
    def start_flow_run(
        self,
        run: FlowRunRecord,
        *,
        context: dict[str, Any] | None = None,
    ) -> FlowRunRecord:
        """Register the start of a flow run in Flow Activity UI and enqueue for LANGFLOW_FLOW_RUNS."""
        if self.outbox.is_purged_by_retention(CANONICAL_FLOW_RUNS_TABLE, run.run_id):
            return run

        redacted_input = redact_payload_for_sink(run.input_payload, context=context)
        redacted_output = redact_payload_for_sink(run.output_payload, context=context)
        redacted_err = (
            redact_payload_for_sink(run.error_message, context=context)
            if run.error_message
            else None
        )

        sanitized_run = replace(
            run,
            input_payload=redacted_input if isinstance(redacted_input, dict) else {"value": redacted_input},
            output_payload=redacted_output if isinstance(redacted_output, dict) else {"value": redacted_output},
            error_message=str(redacted_err) if redacted_err is not None else None,
        )
        self._ui_runs[sanitized_run.run_id] = sanitized_run
        self.outbox.enqueue_flow_run(sanitized_run, context=context)
        return sanitized_run

    def end_flow_run(
        self,
        run_id: str,
        *,
        status: str = "SUCCESS",
        output_payload: dict[str, Any] | None = None,
        end_ts: str | None = None,
        latency_ms: float | None = None,
        error_category: str | None = None,
        error_message: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        reasoning_tokens: int | None = None,
        snowflake_query_ids: list[str] | None = None,
        proxy_request_ids: list[str] | None = None,
        proxy_profile_revision: str | None = None,
        cortex_thread_id: str | None = None,
        cortex_parent_message_id: str | None = None,
        a2a_task_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> FlowRunRecord | None:
        """Complete an existing flow run, preserving NULL vs 0 tokens and correlation IDs."""
        if self.outbox.is_purged_by_retention(CANONICAL_FLOW_RUNS_TABLE, run_id):
            return None

        existing = self._ui_runs.get(run_id)
        if existing is None:
            raise KeyError(f"FlowRunRecord {run_id!r} not found.")

        redacted_output = redact_payload_for_sink(
            output_payload if output_payload is not None else existing.output_payload,
            context=context,
        )
        redacted_err = (
            redact_payload_for_sink(error_message, context=context)
            if error_message is not None
            else existing.error_message
        )

        merged_qids = list(
            dict.fromkeys([*existing.snowflake_query_ids, *(snowflake_query_ids or [])])
        )
        merged_pids = list(
            dict.fromkeys([*existing.proxy_request_ids, *(proxy_request_ids or [])])
        )

        updated = replace(
            existing,
            status=status,
            end_ts=end_ts or utc_now_iso(),
            latency_ms=latency_ms if latency_ms is not None else existing.latency_ms,
            error_category=error_category if error_category is not None else existing.error_category,
            error_message=str(redacted_err) if redacted_err is not None else None,
            output_payload=redacted_output if isinstance(redacted_output, dict) else {"value": redacted_output},
            input_tokens=input_tokens if input_tokens is not None else existing.input_tokens,
            output_tokens=output_tokens if output_tokens is not None else existing.output_tokens,
            reasoning_tokens=reasoning_tokens if reasoning_tokens is not None else existing.reasoning_tokens,
            snowflake_query_ids=merged_qids,
            proxy_request_ids=merged_pids,
            proxy_profile_revision=proxy_profile_revision or existing.proxy_profile_revision,
            cortex_thread_id=cortex_thread_id or existing.cortex_thread_id,
            cortex_parent_message_id=cortex_parent_message_id or existing.cortex_parent_message_id,
            a2a_task_id=a2a_task_id or existing.a2a_task_id,
        )
        self._ui_runs[run_id] = updated
        self.outbox.enqueue_flow_run(updated, context=context)
        return updated

    def record_flow_span(
        self,
        span: FlowSpanRecord,
        *,
        context: dict[str, Any] | None = None,
    ) -> FlowSpanRecord | None:
        """Record a component/vertex execution span and enqueue for LANGFLOW_FLOW_SPANS."""
        if (
            self.outbox.is_purged_by_retention(CANONICAL_FLOW_SPANS_TABLE, span.span_id)
            or self.outbox.is_purged_by_retention(CANONICAL_FLOW_RUNS_TABLE, span.run_id)
        ):
            return None

        redacted_inputs = redact_payload_for_sink(span.inputs, context=context)
        redacted_outputs = redact_payload_for_sink(span.outputs, context=context)
        redacted_err = (
            redact_payload_for_sink(span.error_message, context=context)
            if span.error_message
            else None
        )

        sanitized_span = replace(
            span,
            inputs=redacted_inputs if isinstance(redacted_inputs, dict) else {"value": redacted_inputs},
            outputs=redacted_outputs if isinstance(redacted_outputs, dict) else {"value": redacted_outputs},
            error_message=str(redacted_err) if redacted_err is not None else None,
        )
        self._ui_spans[sanitized_span.span_id] = sanitized_span
        self.outbox.enqueue_flow_span(sanitized_span, context=context)
        return sanitized_span

    def sync_to_snowflake(self) -> OutboxFlushSummary:
        """Flush pending outbox events to the Snowflake SQL replica using idempotent MERGE."""
        return self.outbox.flush_pending(self.replica_store.write_merged_row)

    # ------------------------------------------------------------------
    # Authorization Enforcement on UI & SQL Trace / Audit Access
    # ------------------------------------------------------------------
    def _verify_role_for_trace_operation(
        self,
        *,
        caller_role: str,
        caller_user: str,
        operation: str,
        required_privilege: str,
        project_id: str | None = None,
        run_id: str | None = None,
    ) -> None:
        if not caller_role or caller_role not in self.authorized_trace_roles:
            self.outbox.record_authz_denial(
                operation=operation,
                authenticated_user=caller_user,
                effective_role=caller_role or "NONE",
                required_privilege=required_privilege,
                target_object_fqn=self.table_config.flow_runs_fqn,
                project_id=project_id,
                run_id=run_id,
            )
            raise TraceAuthorizationError(
                active_role=caller_role or "NONE",
                operation=operation,
                required_privilege=required_privilege,
            )

    def _can_role_see_row(
        self,
        *,
        caller_role: str,
        caller_user: str,
        row_role: str,
        row_user: str,
        project_id: str | None = None,
        row_project_id: str | None = None,
    ) -> bool:
        if project_id is not None and row_project_id != project_id:
            return False
        if caller_role in self.admin_audit_roles:
            return True
        return row_role == caller_role and (not caller_user or row_user == caller_user)

    def list_flow_activity_runs(
        self,
        *,
        caller_role: str,
        caller_user: str,
        project_id: str | None = None,
        session_id: str | None = None,
        status: str | None = None,
        search_query: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """Return Langflow Flow Activity UI rows filtered by active role and search criteria."""
        self._verify_role_for_trace_operation(
            caller_role=caller_role,
            caller_user=caller_user,
            operation="READ_FLOW_ACTIVITY_UI",
            required_privilege=f"SELECT ON {self.table_config.flow_runs_fqn}",
            project_id=project_id,
        )

        ui_rows: list[dict[str, Any]] = []
        for run in self._ui_runs.values():
            if not include_deleted and run.deleted_in_ui_at is not None:
                continue
            if not self._can_role_see_row(
                caller_role=caller_role,
                caller_user=caller_user,
                row_role=run.effective_role,
                row_user=run.authenticated_user,
                project_id=project_id,
                row_project_id=run.project_id,
            ):
                continue
            if session_id is not None and run.session_id != session_id:
                continue
            if status is not None and run.status != status:
                continue
            if search_query:
                q = search_query.lower()
                haystack = f"{run.run_id} {run.trace_id} {run.flow_name} {run.session_id} {run.status}".lower()
                if q not in haystack:
                    continue
            ui_rows.append(run.to_flow_activity_ui_row())

        return ui_rows

    def group_flow_activity_by_session(
        self,
        *,
        caller_role: str,
        caller_user: str,
        project_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Group Flow Activity UI runs by `session_id` (preserving Langflow session view)."""
        rows = self.list_flow_activity_runs(
            caller_role=caller_role,
            caller_user=caller_user,
            project_id=project_id,
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            sid = str(row["Session ID"])
            grouped.setdefault(sid, []).append(row)
        return grouped

    def export_flow_activity(
        self,
        *,
        caller_role: str,
        caller_user: str,
        project_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Export Flow Activity runs with role verification and audit logging."""
        self._verify_role_for_trace_operation(
            caller_role=caller_role,
            caller_user=caller_user,
            operation="EXPORT_FLOW_ACTIVITY_TRACES",
            required_privilege=f"SELECT ON {self.table_config.flow_runs_fqn}",
            project_id=project_id,
        )
        rows = self.list_flow_activity_runs(
            caller_role=caller_role,
            caller_user=caller_user,
            project_id=project_id,
        )
        self.outbox.enqueue_audit_event(
            AuditEventRecord(
                action_category="TRACE_ACCESS",
                operation="EXPORT_FLOW_ACTIVITY_TRACES",
                authenticated_user=caller_user,
                effective_role=caller_role,
                target_object_fqn=self.table_config.flow_runs_fqn,
                project_id=project_id,
                outcome="SUCCESS",
                details={"exported_run_count": len(rows)},
            )
        )
        return rows

    def query_snowflake_flow_runs(
        self,
        *,
        caller_role: str,
        caller_user: str,
        project_id: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """Query replicated `<DB>.<SCHEMA>.LANGFLOW_FLOW_RUNS` enforcing the same RBAC/RAP boundary."""
        self._verify_role_for_trace_operation(
            caller_role=caller_role,
            caller_user=caller_user,
            operation="QUERY_SNOWFLAKE_FLOW_RUNS_SQL",
            required_privilege=f"SELECT ON {self.table_config.flow_runs_fqn}",
            project_id=project_id,
        )
        results: list[dict[str, Any]] = []
        for row in self.replica_store.runs_table.values():
            if not include_deleted and row.get("deleted_in_ui_at") is not None:
                continue
            if not self._can_role_see_row(
                caller_role=caller_role,
                caller_user=caller_user,
                row_role=str(row.get("effective_role", "")),
                row_user=str(row.get("authenticated_user", "")),
                project_id=project_id,
                row_project_id=row.get("project_id"),
            ):
                continue
            results.append(copy.deepcopy(row))
        return results

    def query_snowflake_flow_spans(
        self,
        *,
        caller_role: str,
        caller_user: str,
        run_id: str | None = None,
        project_id: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """Query replicated `<DB>.<SCHEMA>.LANGFLOW_FLOW_SPANS` enforcing the same RBAC/RAP boundary."""
        self._verify_role_for_trace_operation(
            caller_role=caller_role,
            caller_user=caller_user,
            operation="QUERY_SNOWFLAKE_FLOW_SPANS_SQL",
            required_privilege=f"SELECT ON {self.table_config.flow_spans_fqn}",
            project_id=project_id,
            run_id=run_id,
        )
        results: list[dict[str, Any]] = []
        for row in self.replica_store.spans_table.values():
            if not include_deleted and row.get("deleted_in_ui_at") is not None:
                continue
            if run_id is not None and row.get("run_id") != run_id:
                continue
            if not self._can_role_see_row(
                caller_role=caller_role,
                caller_user=caller_user,
                row_role=str(row.get("effective_role", "")),
                row_user=str(row.get("authenticated_user", "")),
                project_id=project_id,
                row_project_id=row.get("project_id"),
            ):
                continue
            results.append(copy.deepcopy(row))
        return results

    def verify_ui_sql_parity(
        self,
        *,
        caller_role: str,
        caller_user: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        """Verify field-by-field parity between Flow Activity UI and Snowflake SQL replica."""
        ui_rows = self.list_flow_activity_runs(
            caller_role=caller_role,
            caller_user=caller_user,
            project_id=project_id,
        )
        sql_rows = self.query_snowflake_flow_runs(
            caller_role=caller_role,
            caller_user=caller_user,
            project_id=project_id,
        )
        ui_by_run = {r["Run"]: r for r in ui_rows}
        sql_by_run = {r["run_id"]: r for r in sql_rows}

        mismatches: list[str] = []
        if set(ui_by_run.keys()) != set(sql_by_run.keys()):
            mismatches.append(
                f"Run ID set mismatch: ui={sorted(ui_by_run.keys())} vs sql={sorted(sql_by_run.keys())}"
            )
        else:
            for rid, urow in ui_by_run.items():
                srow = sql_by_run[rid]
                if urow["Trace ID"] != srow["trace_id"]:
                    mismatches.append(f"{rid}: Trace ID mismatch")
                if urow["Timestamp"] != srow["start_ts"]:
                    mismatches.append(f"{rid}: Timestamp mismatch")
                if urow["Input"] != srow["input_payload"]:
                    mismatches.append(f"{rid}: Input mismatch")
                if urow["Output"] != srow["output_payload"]:
                    mismatches.append(f"{rid}: Output mismatch")
                if urow["Token"] != srow["total_tokens"]:
                    mismatches.append(f"{rid}: Token mismatch ({urow['Token']} vs {srow['total_tokens']})")
                if urow["Latency"] != srow["latency_ms"]:
                    mismatches.append(f"{rid}: Latency mismatch")
                if urow["Status"] != srow["status"]:
                    mismatches.append(f"{rid}: Status mismatch")

        return {
            "in_parity": len(mismatches) == 0,
            "ui_count": len(ui_rows),
            "sql_count": len(sql_rows),
            "mismatches": mismatches,
        }

    # ------------------------------------------------------------------
    # Soft-Delete in UI (`deleted_in_ui_at`) & Retention Anti-Resurrection
    # ------------------------------------------------------------------
    def delete_run_in_ui(
        self,
        run_id: str,
        *,
        caller_role: str,
        caller_user: str,
    ) -> FlowRunRecord:
        """Soft-delete a run from Flow Activity UI (`deleted_in_ui_at`) without deleting LANGFLOW_AUDIT_LOG."""
        self._verify_role_for_trace_operation(
            caller_role=caller_role,
            caller_user=caller_user,
            operation="DELETE_FLOW_ACTIVITY_RUN_UI",
            required_privilege=f"DELETE ON {self.table_config.flow_runs_fqn}",
            run_id=run_id,
        )
        run = self._ui_runs.get(run_id)
        if run is None:
            raise KeyError(f"FlowRunRecord {run_id!r} not found.")

        if not self._can_role_see_row(
            caller_role=caller_role,
            caller_user=caller_user,
            row_role=run.effective_role,
            row_user=run.authenticated_user,
        ):
            self.outbox.record_authz_denial(
                operation="DELETE_FLOW_ACTIVITY_RUN_UI",
                authenticated_user=caller_user,
                effective_role=caller_role,
                required_privilege="OWNERSHIP OR MATCHING EFFECTIVE_ROLE",
                target_object_fqn=self.table_config.flow_runs_fqn,
                run_id=run_id,
            )
            raise TraceAuthorizationError(
                active_role=caller_role,
                operation="DELETE_FLOW_ACTIVITY_RUN_UI",
                required_privilege="OWNERSHIP OR MATCHING EFFECTIVE_ROLE",
            )

        deleted_ts = utc_now_iso()
        soft_deleted_run = replace(run, deleted_in_ui_at=deleted_ts)
        self._ui_runs[run_id] = soft_deleted_run
        self.outbox.enqueue_flow_run(soft_deleted_run)

        for span_id, span in list(self._ui_spans.items()):
            if span.run_id == run_id:
                soft_deleted_span = replace(span, deleted_in_ui_at=deleted_ts)
                self._ui_spans[span_id] = soft_deleted_span
                self.outbox.enqueue_flow_span(soft_deleted_span)

        # Record UI soft-delete in governance audit log (which is NEVER deleted)
        self.outbox.enqueue_audit_event(
            AuditEventRecord(
                action_category="TRACE_ACCESS",
                operation="SOFT_DELETE_FLOW_RUN_UI",
                authenticated_user=caller_user,
                effective_role=caller_role,
                target_object_fqn=self.table_config.flow_runs_fqn,
                project_id=run.project_id,
                flow_id=run.flow_id,
                run_id=run.run_id,
                trace_id=run.trace_id,
                session_id=run.session_id,
                outcome="SUCCESS",
                details={"deleted_in_ui_at": deleted_ts},
            )
        )
        return soft_deleted_run

    def purge_runs_for_retention(self, run_ids: list[str]) -> dict[str, int]:
        """Permanently purge runs and their spans for retention and register anti-resurrection tombstones.

        `LANGFLOW_AUDIT_LOG` is never modified or deleted by retention purge of flow runs.
        """
        span_ids_to_purge = [
            sid
            for sid, sp in self._ui_spans.items()
            if sp.run_id in set(run_ids)
        ] + [
            sid
            for sid, sp_row in self.replica_store.spans_table.items()
            if sp_row.get("run_id") in set(run_ids)
        ]
        unique_span_ids = list(dict.fromkeys(span_ids_to_purge))

        purged_runs = self.outbox.register_retention_purge(
            CANONICAL_FLOW_RUNS_TABLE, run_ids, reason="RETENTION_POLICY"
        )
        purged_spans = self.outbox.register_retention_purge(
            CANONICAL_FLOW_SPANS_TABLE, unique_span_ids, reason="RETENTION_POLICY"
        )

        for rid in run_ids:
            self._ui_runs.pop(rid, None)
            self.replica_store.runs_table.pop(rid, None)
        for sid in unique_span_ids:
            self._ui_spans.pop(sid, None)
            self.replica_store.spans_table.pop(sid, None)

        return {"purged_runs": purged_runs, "purged_spans": purged_spans}
