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
- `<LLM_PROXY_HOST>`, `<OLLAMA_HOST>` — Host (`name[:port]`) of the two services above, for `LANGFLOW_SSRF_ALLOWED_HOSTS`. Keep `localhost,127.0.0.1` in that list: the `MCP Tools` component reaches the in-container Project MCP Server over loopback (still authenticated with the project's `x-api-key`).

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

### Request correlation and audit attribution

- Every HTTP response (JSON, streaming, static assets, errors, unhandled exceptions) carries the request id in
  both `X-Request-Id` and `X-Snowflake-Langflow-Request-Id`. Some ingress layers strip `X-Request-Id` from
  responses; clients should read either header. JSON error bodies (status >= 400) also include a
  `request_id` field, which survives any header filtering. An incoming `X-Request-Id` is reused only if it
  is a valid UUID.
- `LANGFLOW_AUDIT_LOG.EXECUTION_MODE` records the *effective* mode: `INTERACTIVE_CALLER` only when
  `SNOWFLAKE_LANGFLOW_CALLERS_RIGHTS` is ON and the request ran with caller's rights, otherwise `SERVICE`.
  Background writes keep the mode of the request that scheduled them.
- `EFFECTIVE_ROLE` is the role sent to the SQL API. When no role is sent in service mode, the service
  token's default role is resolved once with `SELECT CURRENT_ROLE()` and cached. In caller mode without an
  explicit role it is `NULL` (unknown, never guessed).
- `TRACE_ID` is the flow `run_id` when one exists, otherwise the request id, so an audit row can be joined
  to container logs, `QUERY_TAG` (`request_id`/`run_id` keys) and `LANGFLOW_FLOW_RUNS`.

---

## 4. Building & Pushing the `linux/amd64` Container Image

SPCS requires `linux/amd64` images running as a non-root user (`UID 1000`):

```bash
docker build --platform linux/amd64 \
  -t <SNOWFLAKE_ACCOUNT>.registry.snowflakecomputing.com/<DATABASE>/<SCHEMA>/<IMAGE_REPOSITORY>/snowflake-langflow:<VERSION> .

docker push <SNOWFLAKE_ACCOUNT>.registry.snowflakecomputing.com/<DATABASE>/<SCHEMA>/<IMAGE_REPOSITORY>/snowflake-langflow:<VERSION>
```


### 4.1 Release build on the upstream Langflow image (`Dockerfile.release`)

`Dockerfile.release` is the release image. It starts from the official Langflow image and runs
`scripts/install_live_hooks_in_container.py` once against its site-packages. The Langflow version is
recorded only in the `LANGFLOW_BASE` build argument. Do not stack a release on a previous release digest.

```bash
# Resolve and pin the upstream digest (recommended for releases)
docker pull --platform linux/amd64 langflowai/langflow:1.13.1
docker inspect --format '{{index .RepoDigests 0}}' langflowai/langflow:1.13.1

docker build --platform linux/amd64 -f Dockerfile.release \
  --build-arg LANGFLOW_BASE=langflowai/langflow:1.13.1@sha256:643d78228cd148be70e52a860dd3456c834a070c98c81b354f713908d40949f8 \
  -t <SNOWFLAKE_ACCOUNT>.registry.snowflakecomputing.com/<DATABASE>/<SCHEMA>/<IMAGE_REPOSITORY>/snowflake-langflow:<VERSION> .

docker push <SNOWFLAKE_ACCOUNT>.registry.snowflakecomputing.com/<DATABASE>/<SCHEMA>/<IMAGE_REPOSITORY>/snowflake-langflow:<VERSION>
```

- The build fails if the installer or the import smoke check fails.
- `.dockerignore` keeps `.git`, caches, tests, Markdown notes, local databases and key files out of the build context.
- Runtime user, workdir and command match upstream (`user`, `/app`, `langflow run`).
- Before building, check that the installer still matches the new base: extract the image site-packages and run `python3 scripts/install_live_hooks_in_container.py --report` against it. The report lists every hook point; any missing or ambiguous match fails the build (fail closed).
- The image entrypoint is `scripts/langflow-persist-entrypoint.sh`. It points the core Langflow database and config at the `/persist` block volume before starting `langflow run`, so flows and users survive restarts and image upgrades. Without it the core database lands on ephemeral container storage.
- Upgrading the base runs the upstream alembic migrations on the persisted SQLite database on first start, and they cannot be undone. Rehearse the migration locally on a copy of `/persist/langflow.db`, snapshot the `/persist` block volume, and test on a canary service before you switch traffic. Rollback means restoring the snapshot together with the previous image. See [`UPSTREAM_SYNC.md`](UPSTREAM_SYNC.md).

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

> [!IMPORTANT]
> **Deploy gotcha — pinned image digest.** If you build the new spec from the output of `DESCRIBE SERVICE`, the `image:` line contains a resolved `@sha256:<DIGEST>`. Remove that digest (keep the tag, e.g. `.../snowflake-langflow:<VERSION>`) before `ALTER SERVICE ... FROM SPECIFICATION`. Otherwise the service is updated but the old image keeps running. Confirm the new digest with `SHOW SERVICE CONTAINERS IN SERVICE <DATABASE>.<SCHEMA>.SNOWFLAKE_LANGFLOW_SERVICE`.

---

## 6. v0.8.6 Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `LANGFLOW_SECONDARY_ROLES_MODE` | `NONE` | Secondary roles applied to every SQL session. Any value other than `ALL` means `NONE`. If `USE SECONDARY ROLES` fails, the request fails closed. |
| `LANGFLOW_MEMORY_SCHEMA` | flow `[Context]` schema | `<SCHEMA>` or `<DATABASE>.<SCHEMA>` for governed `LF_MEM_*` memory objects. |
| `LANGFLOW_GOVERNANCE_SCHEMA` | bridge schema | `<SCHEMA>` or `<DATABASE>.<SCHEMA>` for governance/audit objects used by memories. |
| `LANGFLOW_BRIDGE_ROUTINES_SCHEMA` | bridge database/schema | `<DATABASE>.<SCHEMA>` holding bridge-owned routines (Split Text UDF). |
| `LANGFLOW_MESSAGES_TABLE` | unset | Fully qualified governed Message History table (`<DATABASE>.<SCHEMA>.<TABLE>`). |
| `SNOWFLAKE_LANGFLOW_SERVICE_TRIGGER_SECRET` | unset | Shared secret that authenticates Snowflake-side trigger calls into the service (recommended; pass it from the service function). Store it in a Snowflake secret, never in the spec. |
| `SNOWFLAKE_LANGFLOW_STALE_COMPONENT_POLICY` | `upgrade` | Stored flow component code older than the installed component: `upgrade` rebuilds it from the installed code; `refuse` fails with `COMPONENT_CODE_OUTDATED`. |
| `SNOWFLAKE_LANGFLOW_PUBLIC_AGENT_CARD` | `1` | Whether the A2A agent card (`/.well-known/agent-card.json`) is public. All other `/api/v1/snowflake/*` routes always require Langflow authentication. |
| `LANGFLOW_SSRF_ALLOWED_HOSTS` | — | Must include `localhost,127.0.0.1` so `MCP Tools` can reach the project MCP server over the direct streamable transport. |

---

## 7. Provisioning Bridge-Owned Routines (Split Text UDF)

The Split Text component (LangChain mode) calls a fixed-code UDF owned by the bridge. Flow components never create routines; they only invoke the configured FQN.

```bash
export LANGFLOW_BRIDGE_ROUTINES_SCHEMA=<DATABASE>.<SCHEMA>
python3 scripts/provision_bridge_routines.py           # dry run: prints the DDL
python3 scripts/provision_bridge_routines.py --apply   # creates the UDF under the service role
```

The service role needs `USAGE` on the database and schema and `CREATE FUNCTION` on the schema. The script exits with an error when they are missing (no fallback). Then grant use to the roles that run flows:

```sql
GRANT USAGE ON DATABASE <DATABASE> TO ROLE <FLOW_ROLE>;
GRANT USAGE ON SCHEMA <DATABASE>.<SCHEMA> TO ROLE <FLOW_ROLE>;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA <DATABASE>.<SCHEMA> TO ROLE <FLOW_ROLE>;
```

## 8. Post-Deployment Verification

After `ALTER SERVICE <DATABASE>.<SCHEMA>.<SERVICE_NAME> FROM SPECIFICATION ...`:

1. `SHOW SERVICE CONTAINERS IN SERVICE <DATABASE>.<SCHEMA>.<SERVICE_NAME>` reports `READY`, the new image tag and `restart_count = 0`.
2. Run `scripts/refresh_stored_flow_component_code.py` so stored flows pick up the new component code.
3. Run one flow per category (data source, LLM operation, Cortex Agent, Memory Base, trigger) and one `tools/call` on the Project MCP Server.
4. Other services in the account are not touched by the rollout; compare their `spec_digest` and `updated_on` before and after.
