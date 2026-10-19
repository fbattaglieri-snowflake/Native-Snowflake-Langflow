-- =============================================================================
-- SnowflakeLangflow — 02_traces_and_audit_tables.sql
-- Canonical Observability & Durable Audit DDL (ADR-007, REQ-15, [G-A1], [G-A2])
-- Tables:
--   1. <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS
--   2. <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS
--   3. <DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG
-- =============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS (
    event_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL,
    project_id VARCHAR(128) NOT NULL,
    flow_id VARCHAR(128) NOT NULL,
    flow_version_hash VARCHAR(128),
    session_id VARCHAR(128),
    authenticated_user VARCHAR(256) NOT NULL,
    effective_role VARCHAR(256) NOT NULL,
    execution_mode VARCHAR(32) NOT NULL,
    start_ts TIMESTAMP_TZ NOT NULL,
    end_ts TIMESTAMP_TZ,
    latency_ms NUMBER(18, 2),
    status VARCHAR(64) NOT NULL,
    error_category VARCHAR(64),
    input_payload VARIANT,
    output_payload VARIANT,
    input_tokens NUMBER(18, 0),
    output_tokens NUMBER(18, 0),
    reasoning_tokens NUMBER(18, 0),
    snowflake_query_ids ARRAY,
    proxy_request_ids ARRAY,
    proxy_profile_revision VARCHAR(128),
    cortex_thread_id VARCHAR(128),
    cortex_parent_message_id VARCHAR(128),
    a2a_task_id VARCHAR(128),
    deleted_in_ui_at TIMESTAMP_TZ,
    purged_for_retention_at TIMESTAMP_TZ,
    replicated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_langflow_flow_runs PRIMARY KEY (run_id)
);

CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS (
    event_id VARCHAR(64) NOT NULL,
    span_id VARCHAR(128) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL,
    parent_span_id VARCHAR(128),
    project_id VARCHAR(128) NOT NULL,
    flow_id VARCHAR(128) NOT NULL,
    component_id VARCHAR(128),
    component_name VARCHAR(256),
    authenticated_user VARCHAR(256) NOT NULL,
    effective_role VARCHAR(256) NOT NULL,
    execution_mode VARCHAR(32) NOT NULL,
    start_ts TIMESTAMP_TZ NOT NULL,
    end_ts TIMESTAMP_TZ,
    latency_ms NUMBER(18, 2),
    status VARCHAR(64) NOT NULL,
    error_category VARCHAR(64),
    redacted_inputs VARIANT,
    redacted_outputs VARIANT,
    input_tokens NUMBER(18, 0),
    output_tokens NUMBER(18, 0),
    reasoning_tokens NUMBER(18, 0),
    snowflake_query_ids ARRAY,
    proxy_request_ids ARRAY,
    deleted_in_ui_at TIMESTAMP_TZ,
    purged_for_retention_at TIMESTAMP_TZ,
    replicated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_langflow_flow_spans PRIMARY KEY (span_id)
);

CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG (
    event_id VARCHAR(64) NOT NULL,
    event_ts TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    authenticated_user VARCHAR(256) NOT NULL,
    effective_role VARCHAR(256) NOT NULL,
    execution_mode VARCHAR(32) NOT NULL,
    context_epoch NUMBER(18, 0) NOT NULL,
    action VARCHAR(128) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    error_code VARCHAR(64),
    target_object VARCHAR(512),
    project_id VARCHAR(128),
    flow_id VARCHAR(128),
    run_id VARCHAR(128),
    trace_id VARCHAR(128),
    redacted_details VARIANT,
    CONSTRAINT pk_langflow_audit_log PRIMARY KEY (event_id)
);

-- Row Access Policy so SQL queries respect the active role / admin governance boundaries
CREATE ROW ACCESS POLICY IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP
AS (effective_role_col VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('<LANGFLOW_ADMIN_ROLE>', effective_role_col);

ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS
    ADD ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP ON (effective_role);

ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS
    ADD ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP ON (effective_role);

-- Active Flow Activity View (excludes soft-deleted and retention-purged records)
CREATE OR REPLACE SECURE VIEW <DATABASE>.<SCHEMA>.V_LANGFLOW_ACTIVE_FLOW_RUNS AS
SELECT *
FROM <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS
WHERE deleted_in_ui_at IS NULL
  AND purged_for_retention_at IS NULL;
