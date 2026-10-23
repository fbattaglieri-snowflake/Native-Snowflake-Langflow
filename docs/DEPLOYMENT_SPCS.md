# Deploying SnowflakeLangflow on Snowpark Container Services (SPCS)

**Tested Cloud Environment: AWS — Snowflake on AWS**

This guide describes how to deploy SnowflakeLangflow on **Snowflake on AWS** using Snowpark Container Services (SPCS), Snowflake Postgres (or block storage for single-replica development), and the Standard LLM Proxy. All templates use account-agnostic placeholders.

---

## 1. Prerequisites & Required Placeholders

Replace the following placeholders with your target environment values during deployment:

- `<SNOWFLAKE_ACCOUNT>` — Snowflake account identifier on AWS
- `<DATABASE>` — Database hosting Langflow metadata, stages, and trace/audit tables
- `<SCHEMA>` — Schema hosting the SPCS service, image repository, and tables
- `<COMPUTE_POOL>` — SPCS Compute Pool (`CPU_X64_S` / `CPU_X64_M` or larger)
- `<IMAGE_REPOSITORY>` — Image repository name inside `<DATABASE>.<SCHEMA>`
- `<STAGE_NAME>` — Internal Named Stage for specs, flow exports, and backups
- `<POSTGRES_INSTANCE>` — Snowflake Postgres instance for transactional ORM persistence and `pgvector`
- `<LLM_PROXY_BASE_URL>` — Internal SPCS DNS or ingress URL of the Standard LLM Proxy (`Chat`, `Responses`, `Messages`)
- `<OLLAMA_BASE_URL>` — Internal SPCS DNS or ingress URL of the Snowflake-hosted Ollama service

If your AWS region uses cross-region Cortex inference, enable:

```sql
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

---

## 2. Transactional ORM Persistence (`ADR-002`)

Langflow's internal transactional ORM (`SQLModel`/`SQLAlchemy`/`Alembic`) requires ACID transactional locking and **must never** be placed on a Snowflake Stage (which causes SQLite WAL/lock `disk I/O error`) or substituted with analytical columnar tables. Choose one of the two supported topologies:

1. **Option A — Production / Multi-Replica (Recommended): Snowflake Postgres (`<POSTGRES_INSTANCE>`)**
   ```sql
   CREATE POSTGRES INSTANCE <POSTGRES_INSTANCE>
     COMPUTE_FAMILY = 'STANDARD_M'
     STORAGE_SIZE_GB = 10
     AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE
     NETWORK_POLICY = <POSTGRES_INGRESS_POLICY>;
   ```
   Connect Langflow with `LANGFLOW_DATABASE_URL=postgresql+psycopg://<USER>:<TOKEN>@<POSTGRES_HOST>:5432/langflow?sslmode=verify-ca&connect_timeout=10`.
2. **Option B — Single-Replica Development: SPCS Block Storage Volume (with SQLite WAL & Writable `UV_CACHE_DIR`)**
   Mount a dedicated SPCS `block` volume (`>= 1 GiB`, e.g., `10Gi` in `service-spec.template.yaml`) at `/persist` (`LANGFLOW_PERSIST_DIR=/persist`, `LANGFLOW_CONFIG_DIR=/persist/config`, `LANGFLOW_DATABASE_URL=sqlite:////persist/langflow.db`). At startup and on every SQLite connection (`ensure_sqlite_wal_and_uv_cache()` / `_open_sqlite_conn()`), SnowflakeLangflow enforces `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=30000; PRAGMA temp_store=MEMORY;` and sets `UV_CACHE_DIR=/tmp/uv-cache` (`XDG_CACHE_HOME=/tmp/.cache`, `SQLITE_TMPDIR=/tmp`) to prevent `sqlite3.OperationalError: database is locked` and container cache permission errors. Run periodic quiesced backups (`PRAGMA integrity_check` + SHA-256 verification) to `@<DATABASE>.<SCHEMA>.<STAGE_NAME>/backups/`.

---

## 3. Governance RBAC & Trace/Audit Tables (`sql/01_governance_rbac.sql`, `sql/02_traces_and_audit_tables.sql`)

Execute the parametric SQL scripts in `sql/` to provision:

- `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS` — Synchronized `Flow Activity` run summaries with Row Access Policy filtering by `effective_role` and `project_id`.
- `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS` — Granular node/component execution spans with `snowflake_query_ids`, `proxy_request_ids`, `cortex_thread_id`, `cortex_parent_message_id`, and `a2a_task_id`.
- `<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG` — Durable append-only governance audit log recording every object creation, role change, A2A publication/revocation, and `AUTHZ_ROLE_DENIED` event.

---

## 4. Building & Pushing the `linux/amd64` Container Image

SPCS requires `linux/amd64` images running as a non-root user (`UID 1000`):

```bash
docker build --platform linux/amd64 \
  -t <SNOWFLAKE_ACCOUNT>.registry.snowflakecomputing.com/<DATABASE>/<SCHEMA>/<IMAGE_REPOSITORY>/snowflake-langflow:0.1.0 .

docker push <SNOWFLAKE_ACCOUNT>.registry.snowflakecomputing.com/<DATABASE>/<SCHEMA>/<IMAGE_REPOSITORY>/snowflake-langflow:0.1.0
```

---

## 5. Creating the SPCS Service (`sql/03_spcs_service_template.sql`)

Render `service-spec.template.yaml` and create the service on `<COMPUTE_POOL>`:

```sql
CREATE SERVICE <DATABASE>.<SCHEMA>.SNOWFLAKE_LANGFLOW_SERVICE
  IN COMPUTE_POOL <COMPUTE_POOL>
  FROM SPECIFICATION_FILE = '@<DATABASE>.<SCHEMA>.<STAGE_NAME>/service-spec.yaml'
  EXTERNAL_ACCESS_INTEGRATIONS = (<POSTGRES_EAI>, <GIT_EAI>);

GRANT SERVICE ROLE <DATABASE>.<SCHEMA>.SNOWFLAKE_LANGFLOW_SERVICE!ALL_ENDPOINTS_USAGE
  TO ROLE <AUTHORIZED_CALLER_ROLE>;
```
