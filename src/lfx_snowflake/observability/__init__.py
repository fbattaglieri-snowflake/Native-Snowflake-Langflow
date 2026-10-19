"""SnowflakeLangflow three-plane observability, Flow Activity SQL replication, and audit outbox."""

from .models import (
    AuditEventRecord,
    CANONICAL_AUDIT_LOG_TABLE,
    CANONICAL_FLOW_RUNS_TABLE,
    CANONICAL_FLOW_SPANS_TABLE,
    CANONICAL_ROW_ACCESS_POLICY,
    ErrorTaxonomyCode,
    ExecutionMode,
    FlowRunRecord,
    FlowSpanRecord,
    ObservabilityPlane,
    ObservabilityTableConfig,
    SpcsStructuredLogEvent,
    utc_now_iso,
    validate_identifier_or_placeholder,
)
from .outbox import (
    AuditOutboxWriteError,
    OutboxEntry,
    OutboxFlushSummary,
    OutboxStatus,
    TransactionalOutbox,
    build_idempotent_merge_sql,
)
from .redaction import (
    REDACTED_ACCESS_ROLES,
    REDACTED_HEADER,
    REDACTED_REPLAY_STATE,
    REDACTED_SECRET,
    mask_ai_redact_detect_spans,
    redact_payload_for_sink,
    redact_text_secrets,
)
from .trace_replicator import (
    SnowflakeReplicaStore,
    SnowflakeTraceReplicator,
    TraceAuthorizationError,
    generate_canonical_observability_ddl,
)

# Shared contract aliases
redact_sensitive_payload = redact_payload_for_sink
DurableAuditOutbox = TransactionalOutbox

__all__ = [
    "AuditEventRecord",
    "AuditOutboxWriteError",
    "CANONICAL_AUDIT_LOG_TABLE",
    "CANONICAL_FLOW_RUNS_TABLE",
    "CANONICAL_FLOW_SPANS_TABLE",
    "CANONICAL_ROW_ACCESS_POLICY",
    "DurableAuditOutbox",
    "ErrorTaxonomyCode",
    "ExecutionMode",
    "FlowRunRecord",
    "FlowSpanRecord",
    "ObservabilityPlane",
    "ObservabilityTableConfig",
    "OutboxEntry",
    "OutboxFlushSummary",
    "OutboxStatus",
    "REDACTED_ACCESS_ROLES",
    "REDACTED_HEADER",
    "REDACTED_REPLAY_STATE",
    "REDACTED_SECRET",
    "SnowflakeReplicaStore",
    "SnowflakeTraceReplicator",
    "SpcsStructuredLogEvent",
    "TraceAuthorizationError",
    "TransactionalOutbox",
    "build_idempotent_merge_sql",
    "generate_canonical_observability_ddl",
    "mask_ai_redact_detect_spans",
    "redact_payload_for_sink",
    "redact_sensitive_payload",
    "redact_text_secrets",
    "utc_now_iso",
    "validate_identifier_or_placeholder",
]
