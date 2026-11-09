-- 04_memories_and_files.sql — W3-R1-13/15/20/24 templates (handover Appendix B, A8-corrected).
-- TEMPLATE ONLY: not executed by the application or by CI. Replace every <PLACEHOLDER>,
-- compile in a scratch schema first, and re-check syntax against current Snowflake docs.
-- Applies A8 verdicts: B.2 normalisation, B.3 tombstone fix (VETO lifted), B.4/B.6 owner
-- defaults and TRY_PARSE_JSON, B.7 chunk dedup, B.8 coverage + IS_ROLE_IN_SESSION,
-- B.10 caller grants, D-04(b) (ingestion runs in the caller session, MANAGED ACCESS schema).
-- Invariant: <LANGFLOW_MEMORY_AUDITOR_ROLE> and <LANGFLOW_ADMIN_ROLE> are NEVER granted to
-- <SERVICE_OWNER_ROLE>, and ACCOUNTADMIN is never used as the admin role.

-- ---------------------------------------------------------------------------
-- 0. Memory schema with managed access (D-04: table owners cannot drop the RAP)
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS <DATABASE>.<MEMORY_SCHEMA> WITH MANAGED ACCESS
  COMMENT = 'Snowflake Langflow memory bases (managed access)';

-- B.1 Tag
CREATE TAG IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_OBJECT_KIND
  ALLOWED_VALUES 'chat_messages', 'memory_base', 'knowledge_base'
  COMMENT = 'Object managed by Snowflake Langflow';

-- B.2 Row access policy: private memories per user (case/space normalised)
CREATE ROW ACCESS POLICY IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_MEMORY_RAP
  AS (owner_user VARCHAR) RETURNS BOOLEAN ->
    (owner_user IS NOT NULL AND TRIM(UPPER(owner_user)) = TRIM(UPPER(CURRENT_USER())))
    OR IS_ROLE_IN_SESSION('<LANGFLOW_MEMORY_AUDITOR_ROLE>');

-- B.3 Chat messages (durable copy of the Langflow message table), owner defaults from session
CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_MESSAGES (
    message_id        VARCHAR(64)  NOT NULL,
    flow_id           VARCHAR(64)  NOT NULL,
    session_id        VARCHAR(256) NOT NULL,
    context_id        VARCHAR(256),
    sender            VARCHAR(32)  NOT NULL,
    sender_name       VARCHAR(256),
    text              VARCHAR,
    content_blocks    VARIANT,
    files             ARRAY,
    owner_user        VARCHAR(256) NOT NULL DEFAULT CURRENT_USER(),
    owner_role        VARCHAR(256) NOT NULL DEFAULT CURRENT_ROLE(),
    created_at        TIMESTAMP_TZ NOT NULL,
    replicated_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    deleted_in_ui_at  TIMESTAMP_TZ,
    CONSTRAINT pk_langflow_messages PRIMARY KEY (message_id)
)
COMMENT = 'Snowflake Langflow chat messages (durable copy of the Langflow message table)'
WITH ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_MEMORY_RAP ON (owner_user)
WITH TAG (<DATABASE>.<SCHEMA>.LANGFLOW_OBJECT_KIND = 'chat_messages');

-- Dedup FIRST (latest version per message_id, tombstones included), THEN drop deleted rows.
-- Filtering deleted_in_ui_at in the same SELECT as QUALIFY would resurrect deleted messages.
CREATE OR REPLACE SECURE VIEW <DATABASE>.<SCHEMA>.V_LANGFLOW_MESSAGES AS
SELECT * FROM (
    SELECT * FROM <DATABASE>.<SCHEMA>.LANGFLOW_MESSAGES
    QUALIFY ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY replicated_at DESC) = 1
) latest
WHERE latest.deleted_in_ui_at IS NULL;

-- B.4 Memory base table (one per base). <NAME> must match ^[A-Za-z0-9_]{1,200}$, <UUID> canonical,
-- <MODEL> from the embedding allowlist, <DIM> an integer (768 or 1024). Created by the provisioner
-- inside the MANAGED ACCESS schema so ownership stays with the schema governance role.
CREATE TABLE IF NOT EXISTS <DATABASE>.<MEMORY_SCHEMA>.LF_MEM_<NAME> (
    chunk_id        VARCHAR(64)  NOT NULL,
    memory_base_id  VARCHAR(64)  NOT NULL,
    flow_id         VARCHAR(64)  NOT NULL,
    session_id      VARCHAR(256),
    sender          VARCHAR(32),
    sender_name     VARCHAR(256),
    text            VARCHAR      NOT NULL,
    metadata        VARIANT,
    embedding       VECTOR(FLOAT, <DIM>),
    owner_user      VARCHAR(256) NOT NULL DEFAULT CURRENT_USER(),
    owner_role      VARCHAR(256) NOT NULL DEFAULT CURRENT_ROLE(),
    job_id          VARCHAR(64),
    ingested_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    deleted_at      TIMESTAMP_TZ,
    CONSTRAINT pk_lf_mem_<NAME> PRIMARY KEY (chunk_id)
)
COMMENT = '{"langflow":"memory_base","memory_base_id":"<UUID>","embedding_model":"<MODEL>","dimensions":<DIM>}'
WITH ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_MEMORY_RAP ON (owner_user)
WITH TAG (<DATABASE>.<SCHEMA>.LANGFLOW_OBJECT_KIND = 'memory_base');

-- B.5 Knowledge base: same columns as B.4 (owner_user NOT NULL DEFAULT CURRENT_USER()), no RAP
-- (visibility by grants), tag 'knowledge_base'.

-- B.6 Batch ingestion in the caller session (D-04 b): owner taken from the session, never the payload.
-- The table FQN is validated in Python (regex per segment) even though IDENTIFIER(?) is used.
INSERT INTO IDENTIFIER(?) (chunk_id, memory_base_id, flow_id, session_id, sender, sender_name,
                           text, metadata, embedding, owner_user, owner_role, job_id)
SELECT v.$1, v.$2, v.$3, v.$4, v.$5, v.$6, v.$7, TRY_PARSE_JSON(v.$8),
       AI_EMBED('<ALLOWLISTED_MODEL>', v.$7), CURRENT_USER(), CURRENT_ROLE(), v.$9
FROM VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) AS v;   -- one tuple per chunk

-- B.7 Recall in the caller session: dedup chunk_id (append-only retries) BEFORE ranking/LIMIT.
SELECT chunk_id, text, metadata, session_id, sender, sender_name, ingested_at,
       VECTOR_COSINE_SIMILARITY(embedding, AI_EMBED('<ALLOWLISTED_MODEL>', ?)) AS score
FROM (
    SELECT * FROM IDENTIFIER(?)
    WHERE memory_base_id = ?
      AND flow_id = ?
      AND (? IS NULL OR session_id = ?)          -- "Filter by Session": bind NULL to disable
    QUALIFY ROW_NUMBER() OVER (PARTITION BY chunk_id ORDER BY ingested_at DESC) = 1
) latest
WHERE latest.deleted_at IS NULL
ORDER BY score DESC
LIMIT 5;                                          -- top_k validated as int in Python if LIMIT cannot bind (U-06)

-- ---------------------------------------------------------------------------
-- B.8 / W3-R1-15 Trace content visible only to owner and auditor (D-07 a)
-- ---------------------------------------------------------------------------
CREATE MASKING POLICY IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_TEXT_MASK
  AS (content VARCHAR, owner_user VARCHAR) RETURNS VARCHAR ->
    CASE WHEN (owner_user IS NOT NULL AND TRIM(UPPER(owner_user)) = TRIM(UPPER(CURRENT_USER())))
              OR IS_ROLE_IN_SESSION('<LANGFLOW_MEMORY_AUDITOR_ROLE>')
         THEN content ELSE '[owner only]' END;

CREATE MASKING POLICY IF NOT EXISTS <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK
  AS (content VARIANT, owner_user VARCHAR) RETURNS VARIANT ->
    CASE WHEN (owner_user IS NOT NULL AND TRIM(UPPER(owner_user)) = TRIM(UPPER(CURRENT_USER())))
              OR IS_ROLE_IN_SESSION('<LANGFLOW_MEMORY_AUDITOR_ROLE>')
         THEN content ELSE NULL END;

-- LANGFLOW_FLOW_ACTIVITY (owner column USER_IDENTITY); ERROR_MESSAGE and FLOW_CONTEXT_JSON included (A8).
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN INPUT_TEXT
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_TEXT_MASK USING (INPUT_TEXT, USER_IDENTITY);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN OUTPUT_TEXT
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_TEXT_MASK USING (OUTPUT_TEXT, USER_IDENTITY);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN ERROR_MESSAGE
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_TEXT_MASK USING (ERROR_MESSAGE, USER_IDENTITY);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN INPUT_PAYLOAD
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (INPUT_PAYLOAD, USER_IDENTITY);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN OUTPUT_PAYLOAD
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (OUTPUT_PAYLOAD, USER_IDENTITY);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN NODE_SPANS_JSON
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (NODE_SPANS_JSON, USER_IDENTITY);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY MODIFY COLUMN FLOW_CONTEXT_JSON
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (FLOW_CONTEXT_JSON, USER_IDENTITY);

-- LANGFLOW_FLOW_RUNS (owner column authenticated_user)
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS MODIFY COLUMN input_payload
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (input_payload, authenticated_user);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS MODIFY COLUMN output_payload
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (output_payload, authenticated_user);

-- LANGFLOW_FLOW_SPANS (owner column authenticated_user)
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS MODIFY COLUMN redacted_inputs
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (redacted_inputs, authenticated_user);
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS MODIFY COLUMN redacted_outputs
  SET MASKING POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_VARIANT_MASK USING (redacted_outputs, authenticated_user);

-- TR-03: memory/knowledge bases used by a run/span
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS ADD COLUMN IF NOT EXISTS memory_refs ARRAY;
ALTER TABLE <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS ADD COLUMN IF NOT EXISTS memory_refs ARRAY;

-- LANGFLOW_TRACE_RAP: metadata by role, secondary-role aware, dedicated admin role (not ACCOUNTADMIN).
-- A policy attached to tables cannot be replaced in place; use ALTER ... SET BODY.
ALTER ROW ACCESS POLICY <DATABASE>.<SCHEMA>.LANGFLOW_TRACE_RAP SET BODY ->
    IS_ROLE_IN_SESSION(effective_role_col) OR IS_ROLE_IN_SESSION('<LANGFLOW_ADMIN_ROLE>');

-- ---------------------------------------------------------------------------
-- B.10 Restricted caller's rights grants (syntax to re-verify on GRANT CALLER docs, U-08)
-- ---------------------------------------------------------------------------
GRANT CALLER USAGE ON DATABASE <DATABASE> TO ROLE <SERVICE_OWNER_ROLE>;
GRANT CALLER USAGE ON SCHEMA <DATABASE>.<SCHEMA> TO ROLE <SERVICE_OWNER_ROLE>;
GRANT CALLER USAGE ON WAREHOUSE <WAREHOUSE> TO ROLE <SERVICE_OWNER_ROLE>;  -- caller queries need warehouse usage (A8)
GRANT CALLER SELECT, INSERT ON TABLE <DATABASE>.<SCHEMA>.LANGFLOW_MESSAGES TO ROLE <SERVICE_OWNER_ROLE>;  -- no DELETE
GRANT CALLER SELECT ON VIEW <DATABASE>.<SCHEMA>.V_LANGFLOW_MESSAGES TO ROLE <SERVICE_OWNER_ROLE>;
GRANT CALLER USAGE, CREATE TABLE ON SCHEMA <DATABASE>.<MEMORY_SCHEMA> TO ROLE <SERVICE_OWNER_ROLE>;
GRANT INHERITED CALLER SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA <DATABASE>.<MEMORY_SCHEMA> TO ROLE <SERVICE_OWNER_ROLE>;
-- Owner's-rights grants needed by background jobs (no content access beyond the RAP):
GRANT USAGE ON DATABASE <DATABASE> TO ROLE <SERVICE_OWNER_ROLE>;
GRANT USAGE ON SCHEMA <DATABASE>.<MEMORY_SCHEMA> TO ROLE <SERVICE_OWNER_ROLE>;
GRANT USAGE ON WAREHOUSE <WAREHOUSE> TO ROLE <SERVICE_OWNER_ROLE>;
