# Upstream Synchronization & Fork Maintenance Guide (`ADR-001`)

**Upstream Repository:** `https://github.com/langflow-ai/langflow.git` (Read-Only Remote `upstream`)  
**Baseline Tag:** `v1.12.5` (MIT License)  
**Protected Primary Branch:** `snowflake/main`

---

## 1. Branching Strategy

| Branch Pattern | Purpose |
|---|---|
| `snowflake/main` | Protected main branch containing verified SnowflakeLangflow releases |
| `feat/<area>` | Scoped feature branches (e.g., `feat/governance-r0`, `feat/proxy-adapters-r1`) |
| `sync/upstream-<version>` | Dedicated staging branch for merging a new upstream Langflow tag (e.g., `sync/upstream-v1.12.5`) |

---

## 2. Strict Isolation: `lfx_snowflake` Bundle vs Tracked Core Patches

To minimize merge conflicts across upstream upgrades, 95%+ of Snowflake-specific logic resides inside the self-contained package `src/lfx_snowflake/`:

- `src/lfx_snowflake/governance/` — `SnowflakeAuthzService`, `SnowflakeObjectProvisioner`, role-bound session pool, 4-code error taxonomy, and `EXPLAIN_PRIVILEGES` pre-checks.
- `src/lfx_snowflake/observability/` — `SnowflakeTraceReplicator` (`<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS`, `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS`), `SnowflakeAuditOutbox` (`<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG`), deterministic pre-sink secret/PII redactor.
- `src/lfx_snowflake/providers/` — Native adapters for `Snowflake Proxy - Chat`, `Snowflake Proxy - Responses`, `Snowflake Proxy - Messages`, `Ollama (Snowflake Hosted)`, and Snowflake `Embedding Model`.
- `src/lfx_snowflake/components/` — `Data Sources` + `Cortex Analyst`, `LLM Operations` + `AI Function`, `Files & Knowledge` + `Create Memory`, `Message History`, `Python Interpreter` (Snowflake PyPI), `ToolGuard / Policies` (ALTK), and `A2A Agent` / `Cortex Agent`.

### Tracked Core Patch Registry (`frontend_patches/`)

Only six narrow hook points touch core Langflow files:

1. **Topbar & Profile Menu Branding (`REQ-01`, `[G-A6]`):** Official black Snowflake logo in topbar (home + flow), removal of GitHub/Discord/X links, addition of `Snowflake Documentation` (`https://docs.snowflake.com/`) with color Snowflake logo.
2. **Active Role Selector & Backend Authz Middleware (`REQ-02`):** Topbar role switcher and FastAPI middleware enforcing active role permissions and `context_epoch`.
3. **Flow Import Dialogs (`REQ-03`):** `Upload from computer`, `Add from stage` (`@<DATABASE>.<SCHEMA>.<STAGE_NAME>`), and `Add from Git`.
4. **Settings & Memories Panels (`REQ-04`, `REQ-10`):** `Model Providers`, `DB Providers`, `Messages` (3 protocols), `Global Variables`, and `Create Memory` dialog.
5. **Tracer & Audit Outbox Hook (`REQ-15`):** Registration of `SnowflakeTraceReplicator` inside `services/tracing/service.py`.
6. **A2A Server & Client Hook (`REQ-14`):** `Serve as an A2A agent` SPCS caller authentication and `Cortex Agent Run API` adapter.

Unchanged upstream categories (`Input & Output`, `Prompt Template`, `Processing`, `Flow Control` with `[Beta]` badges on `Listen`, `Notify`, `Run Flow`, `LLM Selector`, `Calculator`, `Current Date`) are never modified.

---

## 3. Step-by-Step Upgrade Procedure (`scripts/sync_upstream.sh`)

1. **Run Dry-Run Verification:**
   ```bash
   DRY_RUN=true bash scripts/sync_upstream.sh v1.12.5
   ```
2. **Execute Upstream Merge on `sync/upstream-<version>`:**
   ```bash
   DRY_RUN=false bash scripts/sync_upstream.sh v1.12.5
   ```
3. **Verify Non-Regression & Sanitization Gate:**
   ```bash
   pytest
   python3 scripts/verify_public_repo_sanitization.py
   ```
4. **Database & Code Rollback Plan:**
   Before promoting a synchronized release to `<COMPUTE_POOL>`, execute `python3 scripts/backup_restore_lifecycle.py` to create a verified quiesced backup (or Snowflake Postgres point-in-time fork of `<POSTGRES_INSTANCE>`) so both container image and ORM database can be rolled back atomically if post-upgrade checks fail.
