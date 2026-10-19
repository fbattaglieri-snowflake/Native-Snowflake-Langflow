-- =============================================================================
-- SnowflakeLangflow — 03_spcs_service_template.sql
-- Parametric SPCS Image Repository, Stage, Snowflake Postgres & Service DDL
-- Tested Cloud Environment: AWS (Snowflake on AWS)
-- =============================================================================

-- 1. Create Image Repository for linux/amd64 container images
CREATE IMAGE REPOSITORY IF NOT EXISTS <DATABASE>.<SCHEMA>.<IMAGE_REPOSITORY>;

-- 2. Create Internal Named Stage for specs, flow imports, and encrypted SQLite backups
CREATE STAGE IF NOT EXISTS <DATABASE>.<SCHEMA>.<STAGE_NAME>
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- 3. Optional Production Transactional Backend: Dedicated Snowflake Postgres Instance (ADR-002)
-- Requires CREATE POSTGRES INSTANCE ON ACCOUNT privilege
CREATE POSTGRES INSTANCE IF NOT EXISTS <POSTGRES_INSTANCE>
    COMPUTE_FAMILY = 'STANDARD_M'
    STORAGE_SIZE_GB = 10
    AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE
    COMMENT = 'Dedicated SnowflakeLangflow transactional ORM & pgvector instance';

-- 4. Create SPCS Service from parametric specification
CREATE SERVICE IF NOT EXISTS <DATABASE>.<SCHEMA>.<SERVICE_NAME>
    IN COMPUTE POOL <COMPUTE_POOL>
    FROM @<DATABASE>.<SCHEMA>.<STAGE_NAME>
    SPECIFICATION_FILE = 'service-spec.template.yaml'
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1
    AUTO_RESUME = TRUE
    EXTERNAL_ACCESS_INTEGRATIONS = (<EXTERNAL_ACCESS_INTEGRATION>);

-- 5. Grant endpoint access to authorized roles (never public unauthenticated access)
GRANT SERVICE ROLE <DATABASE>.<SCHEMA>.<SERVICE_NAME>!ALL_ENDPOINTS_USAGE
    TO ROLE IDENTIFIER('<LANGFLOW_DEVELOPER_ROLE>');
GRANT SERVICE ROLE <DATABASE>.<SCHEMA>.<SERVICE_NAME>!ALL_ENDPOINTS_USAGE
    TO ROLE IDENTIFIER('<LANGFLOW_VIEWER_ROLE>');
