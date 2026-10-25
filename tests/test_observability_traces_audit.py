"""Comprehensive unit and contract tests for Role 5: Observability, Flow Activity SQL Replication, and Durable Audit Outbox."""

from __future__ import annotations

import pytest

from lfx_snowflake.observability import (
    AuditEventRecord,
    AuditOutboxWriteError,
    CANONICAL_ACTIVE_FLOW_ACTIVITY_VIEW,
    CANONICAL_AUDIT_LOG_TABLE,
    CANONICAL_FLOW_ACTIVITY_TABLE,
    CANONICAL_FLOW_RUNS_TABLE,
    CANONICAL_FLOW_SPANS_TABLE,
    CANONICAL_ROW_ACCESS_POLICY,
    ErrorTaxonomyCode,
    ExecutionMode,
    FLOW_ACTIVITY_GOVERNED_COLUMNS,
    FlowActivityEventType,
    FlowActivityRecord,
    FlowRunRecord,
    FlowSpanRecord,
    ObservabilityPlane,
    ObservabilityTableConfig,
    OutboxStatus,
    REDACTED_ACCESS_ROLES,
    REDACTED_HEADER,
    REDACTED_REPLAY_STATE,
    REDACTED_SECRET,
    SnowflakeTraceReplicator,
    TraceAuthorizationError,
    TransactionalOutbox,
    build_flow_activity_merge_sql,
    build_flow_activity_soft_delete_sql,
    build_idempotent_merge_sql,
    generate_canonical_observability_ddl,
    mask_ai_redact_detect_spans,
    redact_payload_for_sink,
    redact_text_secrets,
)


def test_canonical_table_names_and_ddl_generation() -> None:
    """Verify [G-A1] canonical table names and account-agnostic DDL generation."""
    cfg = ObservabilityTableConfig(database="<DATABASE>", schema="<SCHEMA>")
    assert cfg.flow_runs_fqn == "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS"
    assert cfg.flow_spans_fqn == "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS"
    assert cfg.audit_log_fqn == "<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG"
    assert cfg.row_access_policy_fqn == f"<DATABASE>.<SCHEMA>.{CANONICAL_ROW_ACCESS_POLICY}"

    ddl = generate_canonical_observability_ddl(cfg)
    assert "CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS" in ddl
    assert "CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS" in ddl
    assert "CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG" in ddl
    assert "CREATE ROW ACCESS POLICY IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP" in ddl

    for kind in (CANONICAL_FLOW_RUNS_TABLE, CANONICAL_FLOW_SPANS_TABLE, CANONICAL_AUDIT_LOG_TABLE):
        merge_sql = build_idempotent_merge_sql(cfg, kind)
        assert f"MERGE INTO <DATABASE>.<SCHEMA>.{kind}" in merge_sql


def test_plane_1_spcs_structured_otlp_logs_with_redaction() -> None:
    """Verify Plane 1 SPCS OTLP log formatting and secret scrubbing."""
    replicator = SnowflakeTraceReplicator()
    fake_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9." + "eyJzdWIiOiIxMjM0NTY3ODkwIn0." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    log_evt = replicator.emit_spcs_log(
        f"Connecting with Bearer {fake_jwt}",
        severity="INFO",
        trace_id="trace-001",
        span_id="span-001",
        run_id="run-001",
        flow_id="flow-001",
        effective_role="ROLE_ANALYST",
        attributes={"Authorization": f"Bearer {fake_jwt}", "normal_key": "ok"},
    )
    assert log_evt["resource"]["observability.plane"] == ObservabilityPlane.SPCS_INFRASTRUCTURE_OTLP.value
    assert fake_jwt not in log_evt["body"]
    assert REDACTED_SECRET in log_evt["body"]
    assert log_evt["attributes"]["Authorization"] == REDACTED_HEADER
    assert log_evt["attributes"]["normal_key"] == "ok"


def test_null_vs_zero_token_accounting_and_correlation_keys() -> None:
    """Verify NULL (None) vs 0 token distinction and end-to-end correlation keys (ADR-007)."""
    unmeasured_run = FlowRunRecord(
        run_id="run-unmeasured",
        trace_id="trace-u",
        project_id="proj-1",
        flow_id="flow-1",
        flow_version_hash="sha256:abc",
        session_id="sess-1",
        authenticated_user="USER_A",
        effective_role="ROLE_ANALYST",
        input_tokens=None,
        output_tokens=None,
        reasoning_tokens=None,
    )
    assert unmeasured_run.total_tokens is None
    assert unmeasured_run.to_snowflake_row()["total_tokens"] is None
    assert unmeasured_run.to_flow_activity_ui_row()["Token"] is None

    zero_run = FlowRunRecord(
        run_id="run-zero",
        trace_id="trace-z",
        project_id="proj-1",
        flow_id="flow-1",
        flow_version_hash="sha256:abc",
        session_id="sess-1",
        authenticated_user="USER_A",
        effective_role="ROLE_ANALYST",
        input_tokens=0,
        output_tokens=0,
        reasoning_tokens=None,
    )
    assert zero_run.total_tokens == 0

    measured_run = FlowRunRecord(
        run_id="run-measured",
        trace_id="trace-m",
        project_id="proj-1",
        flow_id="flow-1",
        flow_version_hash="sha256:abc",
        session_id="sess-1",
        authenticated_user="USER_A",
        effective_role="ROLE_ANALYST",
        input_tokens=120,
        output_tokens=80,
        reasoning_tokens=45,
        snowflake_query_ids=["01b2-query-1", "01b2-query-1", "01b2-query-2"],
        proxy_request_ids=["req-chat-1"],
        proxy_profile_revision="rev-20261016",
        cortex_thread_id="thread-99",
        cortex_parent_message_id="msg-42",
        a2a_task_id="a2a-77",
    )
    row = measured_run.to_snowflake_row()
    assert row["total_tokens"] == 245
    assert row["snowflake_query_ids"] == ["01b2-query-1", "01b2-query-2"]
    assert row["proxy_request_ids"] == ["req-chat-1"]
    assert row["cortex_thread_id"] == "thread-99"
    assert row["cortex_parent_message_id"] == "msg-42"
    assert row["a2a_task_id"] == "a2a-77"


def test_deterministic_redaction_and_ai_redact_detect_mode_masking() -> None:
    """Verify deterministic redaction of secrets, replay signatures, access_roles, and AI_REDACT detect spans ([G-A2])."""
    raw_payload = {
        "headers": {"Authorization": "Bearer secret-token-value-1234567890", "X-Api-Key": "secret-key"},
        "password": "my-db-password",
        "access_roles": [{"role": "snowflake_admin", "password": "cleartext-pg-password"}],
        "reasoning": {"encrypted_content": "opaque-encrypted-blob-xyz"},
        "thinking": {"type": "thinking", "thinking": "chain", "signature": "sig-anthropic-123"},
        "redacted_thinking": {"type": "redacted_thinking", "data": "opaque-redacted-data"},
        "function": "AI_REDACT",
        "mode": "detect",
        "input_text": "Customer Mario Rossi (CF RSSMRA80A01H501U) called +39 02 1234567.",
        "detected_spans": [
            {"category": "NAME", "text": "Mario Rossi", "start": 9, "end": 20, "confidence": 0.99},
            {"category": "NATIONAL_ID", "value": "RSSMRA80A01H501U", "start": 25, "end": 41},
        ],
    }

    sanitized = redact_payload_for_sink(raw_payload)
    assert sanitized["headers"]["Authorization"] == REDACTED_HEADER
    assert sanitized["headers"]["X-Api-Key"] == REDACTED_HEADER
    assert sanitized["password"] == REDACTED_SECRET
    assert sanitized["access_roles"] == REDACTED_ACCESS_ROLES
    assert sanitized["reasoning"]["encrypted_content"] == REDACTED_REPLAY_STATE
    assert sanitized["thinking"]["signature"] == REDACTED_REPLAY_STATE
    assert sanitized["redacted_thinking"]["data"] == REDACTED_REPLAY_STATE

    # Verify [G-A2]: raw PII inside AI_REDACT detect spans and companion text is masked
    spans = sanitized["detected_spans"]
    assert spans[0]["text"] == "[REDACTED_PII:NAME]"
    assert spans[0]["confidence"] == 0.99
    assert spans[1]["value"] == "[REDACTED_PII:NATIONAL_ID]"
    assert "Mario Rossi" not in sanitized["input_text"]
    assert "RSSMRA80A01H501U" not in sanitized["input_text"]

    # Also test standalone helper mask_ai_redact_detect_spans and redact_text_secrets
    direct_masked = mask_ai_redact_detect_spans(raw_payload)
    assert direct_masked["detected_spans"][0]["text"] == "[REDACTED_PII:NAME]"
    assert REDACTED_SECRET in redact_text_secrets("postgres://admin:supersecret@localhost:5432/db")


def test_flow_activity_ui_and_snowflake_sql_parity_with_outbox_retry() -> None:
    """Verify Flow Activity UI parity with LANGFLOW_FLOW_RUNS/SPANS and Golden Rule on outbox retry."""
    replicator = SnowflakeTraceReplicator()
    side_effect_executions = 0

    def run_flow_Step() -> dict[str, str]:
        nonlocal side_effect_executions
        side_effect_executions += 1
        return {"answer": "42"}

    # Execute actual flow logic once
    output_data = run_flow_Step()
    assert side_effect_executions == 1

    run = FlowRunRecord(
        run_id="run-100",
        trace_id="trace-100",
        project_id="proj-alpha",
        flow_id="flow-alpha",
        flow_name="Customer Analytics Flow",
        flow_version_hash="v1-hash",
        session_id="sess-alpha",
        authenticated_user="ALICE",
        effective_role="ROLE_ANALYST",
        input_payload={"question": "What is the total?", "api_key": "should-be-redacted"},
    )
    replicator.start_flow_run(run)

    span = FlowSpanRecord(
        span_id="span-100-1",
        run_id="run-100",
        trace_id="trace-100",
        project_id="proj-alpha",
        flow_id="flow-alpha",
        flow_version_hash="v1-hash",
        session_id="sess-alpha",
        component_id="SnowflakeProxyResponses-1",
        component_name="Snowflake Proxy - Responses",
        component_type="LanguageModel",
        authenticated_user="ALICE",
        effective_role="ROLE_ANALYST",
        inputs={"prompt": "Calculate"},
        outputs={"result": output_data, "encrypted_content": "secret-replay-state"},
        input_tokens=50,
        output_tokens=20,
        reasoning_tokens=30,
        proxy_request_ids=["proxy-req-001"],
    )
    replicator.record_flow_span(span)

    replicator.end_flow_run(
        "run-100",
        status="SUCCESS",
        output_payload=output_data,
        latency_ms=145.5,
        input_tokens=50,
        output_tokens=20,
        reasoning_tokens=30,
        snowflake_query_ids=["01b3-sql-99"],
        proxy_request_ids=["proxy-req-001"],
        cortex_thread_id="ctx-thread-1",
        cortex_parent_message_id="ctx-msg-2",
    )

    # Simulate transient Snowflake network failure on first sync attempt
    replicator.replica_store.simulate_transient_failures(1)
    first_flush = replicator.sync_to_snowflake()
    assert first_flush.failed == 1
    assert first_flush.delivered == 2
    # Golden Rule: outbox retry NEVER re-executes the flow/tool callable
    assert side_effect_executions == 1

    # Second sync retries the failed entry and converges idempotently
    second_flush = replicator.sync_to_snowflake()
    assert second_flush.failed == 0
    assert second_flush.delivered == 1
    assert side_effect_executions == 1

    parity = replicator.verify_ui_sql_parity(
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
        project_id="proj-alpha",
    )
    assert parity["in_parity"] is True
    assert parity["ui_count"] == 1
    assert parity["sql_count"] == 1

    grouped = replicator.group_flow_activity_by_session(
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
        project_id="proj-alpha",
    )
    assert "sess-alpha" in grouped
    assert grouped["sess-alpha"][0]["Token"] == 100

    spans_sql = replicator.query_snowflake_flow_spans(
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
        run_id="run-100",
    )
    assert len(spans_sql) == 1
    assert spans_sql[0]["outputs"]["encrypted_content"] == REDACTED_REPLAY_STATE


def test_rbac_enforcement_and_fail_closed_audit_intent() -> None:
    """Verify explicit HTTP 403 AUTHZ_ROLE_DENIED on trace access and fail-closed privileged audit intent."""
    replicator = SnowflakeTraceReplicator()
    run = FlowRunRecord(
        run_id="run-sec-1",
        trace_id="trace-sec-1",
        project_id="proj-sec",
        flow_id="flow-sec",
        flow_version_hash="h1",
        session_id="sess-sec",
        authenticated_user="ALICE",
        effective_role="ROLE_ANALYST",
    )
    replicator.start_flow_run(run)
    replicator.sync_to_snowflake()

    # Unauthorized role attempting to read UI or SQL traces gets HTTP 403 AUTHZ_ROLE_DENIED
    with pytest.raises(TraceAuthorizationError) as exc_info:
        replicator.list_flow_activity_runs(
            caller_role="PUBLIC_UNPRIVILEGED",
            caller_user="BOB",
        )
    err = exc_info.value
    assert err.status_code == 403
    assert err.error_code == ErrorTaxonomyCode.AUTHZ_ROLE_DENIED.value
    assert "Il ruolo attivo (PUBLIC_UNPRIVILEGED) non consente questa operazione" in err.message

    replicator.sync_to_snowflake()
    denial_audits = [
        a
        for a in replicator.replica_store.audit_table.values()
        if a["error_code"] == ErrorTaxonomyCode.AUTHZ_ROLE_DENIED.value
    ]
    assert len(denial_audits) >= 1

    # Test fail-closed when outbox storage fails before a privileged side effect
    outbox = TransactionalOutbox()
    outbox.simulate_storage_fault(True)
    executed = False

    def privileged_ddl() -> str:
        nonlocal executed
        executed = True
        return "CREATED"

    with pytest.raises(AuditOutboxWriteError):
        outbox.execute_privileged_with_audit_intent(
            action_category="OBJECT_PROVISIONING",
            operation="CREATE_POSTGRES_INSTANCE",
            authenticated_user="ADMIN_USER",
            effective_role="ROLE_ADMIN",
            target_object_fqn="<POSTGRES_INSTANCE>",
            side_effect_fn=privileged_ddl,
        )
    assert executed is False


def test_ui_soft_delete_and_retention_anti_resurrection() -> None:
    """Verify UI soft-delete preserves LANGFLOW_AUDIT_LOG and retention purge prevents resurrection."""
    replicator = SnowflakeTraceReplicator()
    run = FlowRunRecord(
        run_id="run-del-1",
        trace_id="trace-del-1",
        project_id="proj-del",
        flow_id="flow-del",
        flow_version_hash="h1",
        session_id="sess-del",
        authenticated_user="ALICE",
        effective_role="ROLE_ANALYST",
    )
    replicator.start_flow_run(run)
    replicator.sync_to_snowflake()

    # Soft-delete in UI
    soft_deleted = replicator.delete_run_in_ui(
        "run-del-1",
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
    )
    assert soft_deleted.deleted_in_ui_at is not None
    replicator.sync_to_snowflake()

    # Active UI/SQL queries exclude soft-deleted run by default, but audit log keeps the deletion record
    assert (
        len(
            replicator.list_flow_activity_runs(
                caller_role="ROLE_ANALYST",
                caller_user="ALICE",
            )
        )
        == 0
    )
    assert (
        len(
            replicator.query_snowflake_flow_runs(
                caller_role="ROLE_ANALYST",
                caller_user="ALICE",
                include_deleted=True,
            )
        )
        == 1
    )
    audit_ops = [a["operation"] for a in replicator.replica_store.audit_table.values()]
    assert "SOFT_DELETE_FLOW_RUN_UI" in audit_ops

    # Retention purge registers anti-resurrection tombstone without touching LANGFLOW_AUDIT_LOG
    audit_count_before = len(replicator.replica_store.audit_table)
    purge_summary = replicator.purge_runs_for_retention(["run-del-1"])
    assert purge_summary["purged_runs"] == 1
    assert len(replicator.replica_store.audit_table) == audit_count_before

    # Attempting to re-enqueue or flush a purged run is blocked by the anti-resurrection filter
    entry = replicator.outbox.enqueue_flow_run(run)
    assert entry.status == OutboxStatus.SKIPPED_PURGED
    flush_after = replicator.sync_to_snowflake()
    assert "run-del-1" not in replicator.replica_store.runs_table
    assert flush_after.delivered == 0


def test_unified_langflow_flow_activity_ddl_merge_and_soft_delete() -> None:
    """Verify CTX-ARCH-06 & CTX-ARCH-07 #5: Unified LANGFLOW_FLOW_ACTIVITY table, 25+ governed columns, MERGE, and UI soft-delete."""
    cfg = ObservabilityTableConfig(database="<DATABASE>", schema="<SCHEMA>")
    assert cfg.flow_activity_fqn == f"<DATABASE>.<SCHEMA>.{CANONICAL_FLOW_ACTIVITY_TABLE}"
    assert cfg.active_flow_activity_view_fqn == f"<DATABASE>.<SCHEMA>.{CANONICAL_ACTIVE_FLOW_ACTIVITY_VIEW}"
    assert len(FLOW_ACTIVITY_GOVERNED_COLUMNS) >= 25

    ddl = generate_canonical_observability_ddl(cfg)
    assert "CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY" in ddl
    assert "CREATE OR REPLACE SECURE VIEW <DATABASE>.<SCHEMA>.V_LANGFLOW_ACTIVE_FLOW_ACTIVITY" in ddl
    for col in (
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
    ):
        assert col in ddl
        assert col in FLOW_ACTIVITY_GOVERNED_COLUMNS

    merge_sql = build_flow_activity_merge_sql(cfg)
    assert "MERGE INTO <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY" in merge_sql
    assert "deleted_in_ui_at is null" in merge_sql.lower()

    soft_delete_sql = build_flow_activity_soft_delete_sql(cfg)
    assert "UPDATE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY" in soft_delete_sql
    assert "SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP()" in soft_delete_sql

    replicator = SnowflakeTraceReplicator(table_config=cfg)
    act = FlowActivityRecord(
        activity_id="act-flow-run-1",
        trace_id="trace-flow-run-1",
        event_type=FlowActivityEventType.FLOW_RUN.value,
        flow_id="flow-ctx-01",
        flow_name="Customer Analytics Flow",
        project_id="proj-01",
        project_name="Retail Project",
        session_id="sess-01",
        user_identity="ALICE",
        active_role="ROLE_ANALYST",
        context_epoch=3,
        flow_context_json={"databases": ["<DATABASE>"], "schemas_by_db": {"<DATABASE>": ["<SCHEMA>"]}},
        input_text="Analyze Q4 orders",
        output_text="Total orders: 1,240",
        prompt_tokens=40,
        completion_tokens=20,
        total_tokens=60,
        latency_ms=88.5,
        node_spans_json=[{"node_id": "SnowflakeTables-1", "status": "SUCCESS"}],
        snowflake_query_ids=["01c9-query-01"],
    )
    replicator.upsert_flow_activity(act)
    replicator.sync_to_snowflake()

    active_rows = replicator.query_snowflake_flow_activity(
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
        flow_id="flow-ctx-01",
    )
    assert len(active_rows) == 1
    assert active_rows[0]["activity_id"] == "act-flow-run-1"
    assert active_rows[0]["flow_context_json"]["databases"] == ["<DATABASE>"]

    # Soft-delete in UI sets DELETED_IN_UI_AT without deleting the row from Snowflake
    sd_res = replicator.soft_delete_flow_activity_in_ui(
        "act-flow-run-1",
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
    )
    assert sd_res["deleted_in_ui_at"] is not None
    assert "SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP()" in sd_res["soft_delete_sql"]

    # Hidden from active UI query, retained in Snowflake when include_deleted=True
    assert (
        len(
            replicator.query_snowflake_flow_activity(
                caller_role="ROLE_ANALYST",
                caller_user="ALICE",
                flow_id="flow-ctx-01",
                include_deleted=False,
            )
        )
        == 0
    )
    retained_rows = replicator.query_snowflake_flow_activity(
        caller_role="ROLE_ANALYST",
        caller_user="ALICE",
        flow_id="flow-ctx-01",
        include_deleted=True,
    )
    assert len(retained_rows) == 1
    assert retained_rows[0]["deleted_in_ui_at"] is not None

