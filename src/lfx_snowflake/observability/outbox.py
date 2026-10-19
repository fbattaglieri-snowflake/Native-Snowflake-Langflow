"""Durable Transactional Outbox for SnowflakeLangflow traces and governance audit.

Implements ADR-007, REQ-15, [G-A1], and [G-A2]:
- Local SQLite durable queue (`outbox_events` + `purged_tombstones`) that survives
  container restarts before asynchronous delivery to Snowflake.
- Pre-sink deterministic redaction applied BEFORE writing to local SQLite and
  before transmitting to Snowflake.
- Fail-closed intent logging for privileged operations with side effects: if the
  intent cannot be durably recorded in the outbox, the operation is blocked.
- Golden Rule: outbox retries ONLY retransmit the already-captured telemetry/audit
  payload; they NEVER re-execute flows, tools, SQL queries, or LLM inferences.
- Anti-resurrection filter: records purged by retention policy cannot be
  re-inserted by delayed outbox retries, while `LANGFLOW_AUDIT_LOG` remains
  immutable against UI soft-deletes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import json
import sqlite3
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
    utc_now_iso,
)
from .redaction import redact_payload_for_sink


class AuditOutboxWriteError(RuntimeError):
    """Raised when a privileged intent cannot be persisted to the durable outbox (fail-closed)."""


class OutboxStatus(str, Enum):
    """Delivery lifecycle state for an outbox entry."""

    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    SKIPPED_PURGED = "SKIPPED_PURGED"


@dataclass
class OutboxEntry:
    """Single serialized record stored in the local durable outbox."""

    outbox_id: str
    table_kind: str
    idempotency_key: str
    payload: dict[str, Any]
    status: OutboxStatus
    attempt_count: int = 0
    last_error: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass
class OutboxFlushSummary:
    """Result summary of an outbox flush cycle."""

    processed: int = 0
    delivered: int = 0
    failed: int = 0
    skipped_purged: int = 0
    errors: list[str] = field(default_factory=list)


def build_idempotent_merge_sql(
    table_config: ObservabilityTableConfig,
    table_kind: str,
) -> str:
    """Build canonical idempotent Snowflake MERGE INTO statement ([G-A1])."""
    if table_kind == CANONICAL_FLOW_RUNS_TABLE:
        fqn = table_config.flow_runs_fqn
        return (
            f"MERGE INTO {fqn} AS target "
            "USING (SELECT "
            ":event_id AS event_id, "
            ":run_id AS run_id, "
            ":trace_id AS trace_id, "
            ":project_id AS project_id, "
            ":flow_id AS flow_id, "
            ":flow_name AS flow_name, "
            ":flow_version_hash AS flow_version_hash, "
            ":session_id AS session_id, "
            ":authenticated_user AS authenticated_user, "
            ":effective_role AS effective_role, "
            ":execution_mode AS execution_mode, "
            ":start_ts AS start_ts, "
            ":end_ts AS end_ts, "
            ":latency_ms AS latency_ms, "
            ":status AS status, "
            ":error_category AS error_category, "
            ":error_message AS error_message, "
            "PARSE_JSON(:input_payload) AS input_payload, "
            "PARSE_JSON(:output_payload) AS output_payload, "
            ":input_tokens AS input_tokens, "
            ":output_tokens AS output_tokens, "
            ":reasoning_tokens AS reasoning_tokens, "
            ":total_tokens AS total_tokens, "
            "PARSE_JSON(:snowflake_query_ids) AS snowflake_query_ids, "
            "PARSE_JSON(:proxy_request_ids) AS proxy_request_ids, "
            ":proxy_profile_revision AS proxy_profile_revision, "
            ":cortex_thread_id AS cortex_thread_id, "
            ":cortex_parent_message_id AS cortex_parent_message_id, "
            ":a2a_task_id AS a2a_task_id, "
            ":deleted_in_ui_at AS deleted_in_ui_at"
            ") AS source "
            "ON target.run_id = source.run_id "
            "WHEN MATCHED THEN UPDATE SET "
            "target.end_ts = COALESCE(source.end_ts, target.end_ts), "
            "target.latency_ms = COALESCE(source.latency_ms, target.latency_ms), "
            "target.status = source.status, "
            "target.error_category = source.error_category, "
            "target.error_message = source.error_message, "
            "target.output_payload = source.output_payload, "
            "target.input_tokens = COALESCE(source.input_tokens, target.input_tokens), "
            "target.output_tokens = COALESCE(source.output_tokens, target.output_tokens), "
            "target.reasoning_tokens = COALESCE(source.reasoning_tokens, target.reasoning_tokens), "
            "target.total_tokens = COALESCE(source.total_tokens, target.total_tokens), "
            "target.snowflake_query_ids = source.snowflake_query_ids, "
            "target.proxy_request_ids = source.proxy_request_ids, "
            "target.cortex_thread_id = COALESCE(source.cortex_thread_id, target.cortex_thread_id), "
            "target.cortex_parent_message_id = COALESCE(source.cortex_parent_message_id, target.cortex_parent_message_id), "
            "target.a2a_task_id = COALESCE(source.a2a_task_id, target.a2a_task_id), "
            "target.deleted_in_ui_at = COALESCE(source.deleted_in_ui_at, target.deleted_in_ui_at) "
            "WHEN NOT MATCHED THEN INSERT ("
            "event_id, run_id, trace_id, project_id, flow_id, flow_name, flow_version_hash, "
            "session_id, authenticated_user, effective_role, execution_mode, start_ts, end_ts, "
            "latency_ms, status, error_category, error_message, input_payload, output_payload, "
            "input_tokens, output_tokens, reasoning_tokens, total_tokens, snowflake_query_ids, "
            "proxy_request_ids, proxy_profile_revision, cortex_thread_id, cortex_parent_message_id, "
            "a2a_task_id, deleted_in_ui_at"
            ") VALUES ("
            "source.event_id, source.run_id, source.trace_id, source.project_id, source.flow_id, "
            "source.flow_name, source.flow_version_hash, source.session_id, source.authenticated_user, "
            "source.effective_role, source.execution_mode, source.start_ts, source.end_ts, "
            "source.latency_ms, source.status, source.error_category, source.error_message, "
            "source.input_payload, source.output_payload, source.input_tokens, source.output_tokens, "
            "source.reasoning_tokens, source.total_tokens, source.snowflake_query_ids, "
            "source.proxy_request_ids, source.proxy_profile_revision, source.cortex_thread_id, "
            "source.cortex_parent_message_id, source.a2a_task_id, source.deleted_in_ui_at"
            ")"
        )

    if table_kind == CANONICAL_FLOW_SPANS_TABLE:
        fqn = table_config.flow_spans_fqn
        return (
            f"MERGE INTO {fqn} AS target "
            "USING (SELECT "
            ":event_id AS event_id, "
            ":span_id AS span_id, "
            ":run_id AS run_id, "
            ":trace_id AS trace_id, "
            ":parent_span_id AS parent_span_id, "
            ":project_id AS project_id, "
            ":flow_id AS flow_id, "
            ":flow_version_hash AS flow_version_hash, "
            ":session_id AS session_id, "
            ":component_id AS component_id, "
            ":component_name AS component_name, "
            ":component_type AS component_type, "
            ":authenticated_user AS authenticated_user, "
            ":effective_role AS effective_role, "
            ":execution_mode AS execution_mode, "
            ":start_ts AS start_ts, "
            ":end_ts AS end_ts, "
            ":latency_ms AS latency_ms, "
            ":status AS status, "
            ":error_category AS error_category, "
            ":error_message AS error_message, "
            "PARSE_JSON(:inputs) AS inputs, "
            "PARSE_JSON(:outputs) AS outputs, "
            ":input_tokens AS input_tokens, "
            ":output_tokens AS output_tokens, "
            ":reasoning_tokens AS reasoning_tokens, "
            ":total_tokens AS total_tokens, "
            "PARSE_JSON(:snowflake_query_ids) AS snowflake_query_ids, "
            "PARSE_JSON(:proxy_request_ids) AS proxy_request_ids, "
            ":proxy_profile_revision AS proxy_profile_revision, "
            ":cortex_thread_id AS cortex_thread_id, "
            ":cortex_parent_message_id AS cortex_parent_message_id, "
            ":a2a_task_id AS a2a_task_id, "
            ":deleted_in_ui_at AS deleted_in_ui_at"
            ") AS source "
            "ON target.span_id = source.span_id "
            "WHEN MATCHED THEN UPDATE SET "
            "target.end_ts = COALESCE(source.end_ts, target.end_ts), "
            "target.latency_ms = COALESCE(source.latency_ms, target.latency_ms), "
            "target.status = source.status, "
            "target.error_category = source.error_category, "
            "target.error_message = source.error_message, "
            "target.outputs = source.outputs, "
            "target.input_tokens = COALESCE(source.input_tokens, target.input_tokens), "
            "target.output_tokens = COALESCE(source.output_tokens, target.output_tokens), "
            "target.reasoning_tokens = COALESCE(source.reasoning_tokens, target.reasoning_tokens), "
            "target.total_tokens = COALESCE(source.total_tokens, target.total_tokens), "
            "target.snowflake_query_ids = source.snowflake_query_ids, "
            "target.proxy_request_ids = source.proxy_request_ids, "
            "target.deleted_in_ui_at = COALESCE(source.deleted_in_ui_at, target.deleted_in_ui_at) "
            "WHEN NOT MATCHED THEN INSERT ("
            "event_id, span_id, run_id, trace_id, parent_span_id, project_id, flow_id, "
            "flow_version_hash, session_id, component_id, component_name, component_type, "
            "authenticated_user, effective_role, execution_mode, start_ts, end_ts, latency_ms, "
            "status, error_category, error_message, inputs, outputs, input_tokens, output_tokens, "
            "reasoning_tokens, total_tokens, snowflake_query_ids, proxy_request_ids, "
            "proxy_profile_revision, cortex_thread_id, cortex_parent_message_id, a2a_task_id, "
            "deleted_in_ui_at"
            ") VALUES ("
            "source.event_id, source.span_id, source.run_id, source.trace_id, source.parent_span_id, "
            "source.project_id, source.flow_id, source.flow_version_hash, source.session_id, "
            "source.component_id, source.component_name, source.component_type, "
            "source.authenticated_user, source.effective_role, source.execution_mode, "
            "source.start_ts, source.end_ts, source.latency_ms, source.status, "
            "source.error_category, source.error_message, source.inputs, source.outputs, "
            "source.input_tokens, source.output_tokens, source.reasoning_tokens, "
            "source.total_tokens, source.snowflake_query_ids, source.proxy_request_ids, "
            "source.proxy_profile_revision, source.cortex_thread_id, "
            "source.cortex_parent_message_id, source.a2a_task_id, source.deleted_in_ui_at"
            ")"
        )

    if table_kind == CANONICAL_AUDIT_LOG_TABLE:
        fqn = table_config.audit_log_fqn
        return (
            f"MERGE INTO {fqn} AS target "
            "USING (SELECT "
            ":event_id AS event_id, "
            ":timestamp AS timestamp, "
            ":action_category AS action_category, "
            ":operation AS operation, "
            ":authenticated_user AS authenticated_user, "
            ":effective_role AS effective_role, "
            ":execution_mode AS execution_mode, "
            ":target_object_fqn AS target_object_fqn, "
            ":project_id AS project_id, "
            ":flow_id AS flow_id, "
            ":run_id AS run_id, "
            ":trace_id AS trace_id, "
            ":session_id AS session_id, "
            ":outcome AS outcome, "
            ":error_code AS error_code, "
            ":required_privilege AS required_privilege, "
            ":denial_message AS denial_message, "
            "PARSE_JSON(:snowflake_query_ids) AS snowflake_query_ids, "
            "PARSE_JSON(:proxy_request_ids) AS proxy_request_ids, "
            "PARSE_JSON(:details) AS details"
            ") AS source "
            "ON target.event_id = source.event_id "
            "WHEN NOT MATCHED THEN INSERT ("
            "event_id, timestamp, action_category, operation, authenticated_user, "
            "effective_role, execution_mode, target_object_fqn, project_id, flow_id, "
            "run_id, trace_id, session_id, outcome, error_code, required_privilege, "
            "denial_message, snowflake_query_ids, proxy_request_ids, details"
            ") VALUES ("
            "source.event_id, source.timestamp, source.action_category, source.operation, "
            "source.authenticated_user, source.effective_role, source.execution_mode, "
            "source.target_object_fqn, source.project_id, source.flow_id, source.run_id, "
            "source.trace_id, source.session_id, source.outcome, source.error_code, "
            "source.required_privilege, source.denial_message, source.snowflake_query_ids, "
            "source.proxy_request_ids, source.details"
            ")"
        )

    raise ValueError(f"Unsupported canonical observability table_kind: {table_kind!r}")


class TransactionalOutbox:
    """SQLite-backed durable transactional outbox for traces and audit records."""

    def __init__(
        self,
        db_path: str = ":memory:",
        table_config: ObservabilityTableConfig | None = None,
    ) -> None:
        self.db_path = db_path
        self.table_config = table_config or ObservabilityTableConfig()
        self._storage_fault_simulated = False
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox_events (
                    outbox_id TEXT PRIMARY KEY,
                    table_kind TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_status_created
                ON outbox_events (status, created_at)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_outbox_table_idem
                ON outbox_events (table_kind, idempotency_key)
                """
                )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS purged_tombstones (
                    table_kind TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    purged_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    PRIMARY KEY (table_kind, idempotency_key)
                )
                """
            )

    def simulate_storage_fault(self, faulted: bool = True) -> None:
        """Enable/disable simulated outbox storage failure for testing fail-closed behavior."""
        self._storage_fault_simulated = faulted

    def is_purged_by_retention(self, table_kind: str, idempotency_key: str) -> bool:
        """Check whether a trace run or span was permanently purged by retention policy."""
        if table_kind == CANONICAL_AUDIT_LOG_TABLE:
            return False
        cur = self._conn.execute(
            "SELECT 1 FROM purged_tombstones WHERE table_kind = ? AND idempotency_key = ?",
            (table_kind, idempotency_key),
        )
        return cur.fetchone() is not None

    def register_retention_purge(
        self,
        table_kind: str,
        idempotency_keys: list[str],
        reason: str = "RETENTION_POLICY",
    ) -> int:
        """Record anti-resurrection tombstones so delayed outbox workers never re-insert purged rows.

        Audit log (`LANGFLOW_AUDIT_LOG`) is explicitly forbidden from retention purge via trace UI.
        """
        if table_kind == CANONICAL_AUDIT_LOG_TABLE:
            raise PermissionError("LANGFLOW_AUDIT_LOG cannot be purged via trace retention tombstone.")

        now = utc_now_iso()
        count = 0
        with self._conn:
            for key in idempotency_keys:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO purged_tombstones (table_kind, idempotency_key, purged_at, reason)
                    VALUES (?, ?, ?, ?)
                    """,
                    (table_kind, key, now, reason),
                )
                self._conn.execute(
                    """
                    UPDATE outbox_events
                    SET status = ?, updated_at = ?
                    WHERE table_kind = ? AND idempotency_key = ? AND status != ?
                    """,
                    (
                        OutboxStatus.SKIPPED_PURGED.value,
                        now,
                        table_kind,
                        key,
                        OutboxStatus.DELIVERED.value,
                    ),
                )
                count += 1
        return count

    def _enqueue_raw(
        self,
        *,
        outbox_id: str,
        table_kind: str,
        idempotency_key: str,
        raw_payload: dict[str, Any],
        context: dict[str, Any] | None = None,
        fail_closed: bool = False,
    ) -> OutboxEntry:
        if self._storage_fault_simulated:
            if fail_closed:
                raise AuditOutboxWriteError(
                    "Durable audit outbox unavailable; blocking privileged operation (fail-closed)."
                )
            raise RuntimeError("Outbox storage unavailable.")

        # Deterministic pre-sink redaction BEFORE writing to local SQLite
        redacted_payload = redact_payload_for_sink(raw_payload, context=context)
        now = utc_now_iso()

        initial_status = OutboxStatus.PENDING
        if self.is_purged_by_retention(table_kind, idempotency_key):
            initial_status = OutboxStatus.SKIPPED_PURGED

        try:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO outbox_events (
                        outbox_id, table_kind, idempotency_key, payload_json,
                        status, attempt_count, last_error, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?)
                    ON CONFLICT(outbox_id) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        status = CASE
                            WHEN outbox_events.status = 'SKIPPED_PURGED' THEN 'SKIPPED_PURGED'
                            ELSE excluded.status
                        END,
                        updated_at = excluded.updated_at
                    """,
                    (
                        outbox_id,
                        table_kind,
                        idempotency_key,
                        json.dumps(redacted_payload, sort_keys=True),
                        initial_status.value,
                        now,
                        now,
                    ),
                )
        except sqlite3.Error as exc:
            if fail_closed:
                raise AuditOutboxWriteError(
                    f"Failed to persist audit record to durable outbox: {exc}"
                ) from exc
            raise

        return OutboxEntry(
            outbox_id=outbox_id,
            table_kind=table_kind,
            idempotency_key=idempotency_key,
            payload=redacted_payload,
            status=initial_status,
            attempt_count=0,
            last_error=None,
            created_at=now,
            updated_at=now,
        )

    def enqueue_flow_run(
        self,
        run: FlowRunRecord,
        *,
        context: dict[str, Any] | None = None,
    ) -> OutboxEntry:
        """Enqueue a FlowRunRecord for idempotent replication to LANGFLOW_FLOW_RUNS."""
        row = run.to_snowflake_row()
        outbox_id = f"run:{run.run_id}:{run.status}:{run.deleted_in_ui_at or 'active'}"
        return self._enqueue_raw(
            outbox_id=outbox_id,
            table_kind=CANONICAL_FLOW_RUNS_TABLE,
            idempotency_key=run.run_id,
            raw_payload=row,
            context=context,
            fail_closed=False,
        )

    def enqueue_flow_span(
        self,
        span: FlowSpanRecord,
        *,
        context: dict[str, Any] | None = None,
    ) -> OutboxEntry:
        """Enqueue a FlowSpanRecord for idempotent replication to LANGFLOW_FLOW_SPANS."""
        row = span.to_snowflake_row()
        outbox_id = f"span:{span.span_id}:{span.status}:{span.deleted_in_ui_at or 'active'}"
        return self._enqueue_raw(
            outbox_id=outbox_id,
            table_kind=CANONICAL_FLOW_SPANS_TABLE,
            idempotency_key=span.span_id,
            raw_payload=row,
            context=context,
            fail_closed=False,
        )

    def enqueue_audit_event(
        self,
        audit: AuditEventRecord,
        *,
        require_durable_for_side_effect: bool = False,
        context: dict[str, Any] | None = None,
    ) -> OutboxEntry:
        """Enqueue an AuditEventRecord for idempotent replication to LANGFLOW_AUDIT_LOG."""
        row = audit.to_snowflake_row()
        outbox_id = f"audit:{audit.event_id}"
        return self._enqueue_raw(
            outbox_id=outbox_id,
            table_kind=CANONICAL_AUDIT_LOG_TABLE,
            idempotency_key=audit.event_id,
            raw_payload=row,
            context=context,
            fail_closed=require_durable_for_side_effect,
        )

    def record_authz_denial(
        self,
        *,
        operation: str,
        authenticated_user: str,
        effective_role: str,
        required_privilege: str | None = None,
        target_object_fqn: str | None = None,
        project_id: str | None = None,
        flow_id: str | None = None,
        run_id: str | None = None,
        trace_id: str | None = None,
        session_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> OutboxEntry:
        """Record an explicit AUTHZ_ROLE_DENIED event in LANGFLOW_AUDIT_LOG."""
        denial_msg = f"Il ruolo attivo ({effective_role}) non consente questa operazione ({operation})"
        record = AuditEventRecord(
            action_category="AUTHZ_DENIAL",
            operation=operation,
            authenticated_user=authenticated_user,
            effective_role=effective_role,
            execution_mode=ExecutionMode.INTERACTIVE,
            target_object_fqn=target_object_fqn,
            project_id=project_id,
            flow_id=flow_id,
            run_id=run_id,
            trace_id=trace_id,
            session_id=session_id,
            outcome="DENIED",
            error_code=ErrorTaxonomyCode.AUTHZ_ROLE_DENIED.value,
            required_privilege=required_privilege,
            denial_message=denial_msg,
            details=details or {},
        )
        return self.enqueue_audit_event(record, require_durable_for_side_effect=True)

    def execute_privileged_with_audit_intent(
        self,
        *,
        action_category: str,
        operation: str,
        authenticated_user: str,
        effective_role: str,
        target_object_fqn: str | None,
        side_effect_fn: Callable[[], Any],
        required_privilege: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> Any:
        """Execute a privileged side-effect operation only after durable intent recording.

        If the outbox cannot record the intent, `AuditOutboxWriteError` is raised and
        `side_effect_fn` is NEVER executed (fail-closed).
        """
        intent_record = AuditEventRecord(
            action_category=action_category,
            operation=operation,
            authenticated_user=authenticated_user,
            effective_role=effective_role,
            target_object_fqn=target_object_fqn,
            outcome="INTENT",
            required_privilege=required_privilege,
            details=details or {},
        )
        # Fail-closed gate before any side effect runs
        self.enqueue_audit_event(intent_record, require_durable_for_side_effect=True)

        try:
            result = side_effect_fn()
        except Exception as exc:
            err_code = getattr(exc, "error_code", ErrorTaxonomyCode.TECHNICAL_EXECUTION_ERROR.value)
            outcome = (
                "DENIED"
                if err_code == ErrorTaxonomyCode.AUTHZ_ROLE_DENIED.value
                else "ERROR"
            )
            failure_record = AuditEventRecord(
                action_category=action_category,
                operation=operation,
                authenticated_user=authenticated_user,
                effective_role=effective_role,
                target_object_fqn=target_object_fqn,
                outcome=outcome,
                error_code=str(err_code),
                required_privilege=required_privilege,
                denial_message=str(exc),
                details=details or {},
            )
            self.enqueue_audit_event(failure_record, require_durable_for_side_effect=False)
            raise

        success_record = AuditEventRecord(
            action_category=action_category,
            operation=operation,
            authenticated_user=authenticated_user,
            effective_role=effective_role,
            target_object_fqn=target_object_fqn,
            outcome="SUCCESS",
            required_privilege=required_privilege,
            details=details or {},
        )
        self.enqueue_audit_event(success_record, require_durable_for_side_effect=False)
        return result

    def list_entries(self, *, status: OutboxStatus | None = None, table_kind: str | None = None) -> list[OutboxEntry]:
        """Inspect entries in the local durable outbox."""
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        if table_kind is not None:
            clauses.append("table_kind = ?")
            params.append(table_kind)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cur = self._conn.execute(
            f"SELECT * FROM outbox_events {where_sql} ORDER BY created_at ASC, outbox_id ASC",
            tuple(params),
        )
        entries: list[OutboxEntry] = []
        for row in cur.fetchall():
            entries.append(
                OutboxEntry(
                    outbox_id=row["outbox_id"],
                    table_kind=row["table_kind"],
                    idempotency_key=row["idempotency_key"],
                    payload=json.loads(row["payload_json"]),
                    status=OutboxStatus(row["status"]),
                    attempt_count=int(row["attempt_count"]),
                    last_error=row["last_error"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            )
        return entries

    def flush_pending(
        self,
        sink_writer: Callable[[str, str, dict[str, Any], str], None],
    ) -> OutboxFlushSummary:
        """Deliver pending/failed outbox entries to Snowflake using idempotent MERGE SQL.

        Golden Rule (ADR-007): This method ONLY reads serialized JSON payloads from
        `outbox_events` and invokes `sink_writer`. It never accepts or re-executes any
        flow, tool, SQL query, or LLM inference callable.
        """
        summary = OutboxFlushSummary()
        cur = self._conn.execute(
            """
            SELECT * FROM outbox_events
            WHERE status IN (?, ?)
            ORDER BY created_at ASC, outbox_id ASC
            """,
            (OutboxStatus.PENDING.value, OutboxStatus.FAILED.value),
        )
        rows = cur.fetchall()

        for row in rows:
            summary.processed += 1
            outbox_id = row["outbox_id"]
            table_kind = row["table_kind"]
            idempotency_key = row["idempotency_key"]
            payload = json.loads(row["payload_json"])
            attempts = int(row["attempt_count"]) + 1
            now = utc_now_iso()

            # Anti-resurrection check before writing to Snowflake
            if self.is_purged_by_retention(table_kind, idempotency_key):
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE outbox_events
                        SET status = ?, updated_at = ?
                        WHERE outbox_id = ?
                        """,
                        (OutboxStatus.SKIPPED_PURGED.value, now, outbox_id),
                    )
                summary.skipped_purged += 1
                continue

            merge_sql = build_idempotent_merge_sql(self.table_config, table_kind)
            try:
                sink_writer(table_kind, idempotency_key, payload, merge_sql)
            except Exception as exc:
                err_text = str(exc)
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE outbox_events
                        SET status = ?, attempt_count = ?, last_error = ?, updated_at = ?
                        WHERE outbox_id = ?
                        """,
                        (OutboxStatus.FAILED.value, attempts, err_text, now, outbox_id),
                    )
                summary.failed += 1
                summary.errors.append(err_text)
                continue

            with self._conn:
                self._conn.execute(
                    """
                    UPDATE outbox_events
                    SET status = ?, attempt_count = ?, last_error = NULL, updated_at = ?
                    WHERE outbox_id = ?
                    """,
                    (OutboxStatus.DELIVERED.value, attempts, now, outbox_id),
                )
            summary.delivered += 1

        return summary
