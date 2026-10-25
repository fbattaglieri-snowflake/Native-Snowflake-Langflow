-- =============================================================================
-- SnowflakeLangflow — 02_traces_and_audit_tables.sql
-- Canonical Observability & Durable Audit DDL (ADR-007, REQ-15, [G-A1], [G-A2], CTX-ARCH-06)
-- Tables:
--   0. <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY (Canonical Unified Real-Time Table)
--   1. <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS
--   2. <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS
--   3. <DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG
-- =============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY (
    ACTIVITY_ID VARCHAR(128) NOT NULL,
    TRACE_ID VARCHAR(128),
    EVENT_TYPE VARCHAR(32) NOT NULL,
    FLOW_ID VARCHAR(128) NOT NULL,
    FLOW_NAME VARCHAR(256),
    PROJECT_ID VARCHAR(128),
    PROJECT_NAME VARCHAR(256),
    SESSION_ID VARCHAR(256),
    RUN_TIMESTAMP TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    STATUS VARCHAR(32) NOT NULL,
    LATENCY_MS FLOAT,
    INPUT_TEXT VARCHAR,
    OUTPUT_TEXT VARCHAR,
    ERROR_MESSAGE VARCHAR,
    PROMPT_TOKENS INTEGER DEFAULT 0,
    COMPLETION_TOKENS INTEGER DEFAULT 0,
    TOTAL_TOKENS INTEGER DEFAULT 0,
    USER_IDENTITY VARCHAR(256) NOT NULL,
    ACTIVE_ROLE VARCHAR(128) NOT NULL,
    CONTEXT_EPOCH INTEGER,
    FLOW_CONTEXT_JSON VARIANT,
    INPUT_PAYLOAD VARIANT,
    OUTPUT_PAYLOAD VARIANT,
    NODE_SPANS_JSON VARIANT,
    SNOWFLAKE_QUERY_IDS ARRAY,
    DELETED_IN_UI_AT TIMESTAMP_TZ,
    PURGED_FOR_RETENTION_AT TIMESTAMP_TZ,
    REPLICATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ACTIVITY_ID)
);

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

ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY
    ADD ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP ON (ACTIVE_ROLE);

ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS
    ADD ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP ON (effective_role);

ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS
    ADD ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP ON (effective_role);

-- Active Flow Activity Views (excludes soft-deleted and retention-purged records)
CREATE OR REPLACE SECURE VIEW <DATABASE>.<SCHEMA>.V_LANGFLOW_ACTIVE_FLOW_ACTIVITY AS
SELECT *
FROM <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY
WHERE DELETED_IN_UI_AT IS NULL
  AND PURGED_FOR_RETENTION_AT IS NULL;

CREATE OR REPLACE SECURE VIEW <DATABASE>.<SCHEMA>.V_LANGFLOW_ACTIVE_FLOW_RUNS AS
SELECT *
FROM <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS
WHERE deleted_in_ui_at IS NULL
  AND purged_for_retention_at IS NULL;
