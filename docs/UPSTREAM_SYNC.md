# Upstream Synchronization & Fork Maintenance Guide (`ADR-001`)

> **Disclaimer & Ownership:** This repository is a personal, independent fork developed by **Francesco Battaglieri** ([@fbattaglieri-snowflake](https://github.com/fbattaglieri-snowflake)) based on the upstream open-source project [langflow-ai/langflow](https://github.com/langflow-ai/langflow) (`v1.12.5`, MIT License). **It is NOT an official project or product of Snowflake Inc. nor of Langflow / DataStax**, and it is **currently under active testing and validation (Work in Progress / Experimental Testing)**.

**Upstream Repository:** `https://github.com/langflow-ai/langflow.git` (Read-Only Remote `upstream`)  
**Baseline Tag:** `v1.12.5` (MIT License)  
**Protected Primary Branch:** `snowflake/main`

---

## 0. Mandatory Upstream Update Rules (`RULE-SYNC-01` .. `RULE-SYNC-07`)

| Rule ID | Rule Name | Enforcement |
|---|---|---|
| `RULE-SYNC-01` | **Read-Only Upstream Remote** | `upstream` (`https://github.com/langflow-ai/langflow.git`) is read-only. Never push branches or tags to `upstream`. |
| `RULE-SYNC-02` | **Stable Release Tag Pinning** | Synchronize only against official release tags (e.g., `v1.12.5`), never untagged `main` commits. |
| `RULE-SYNC-03` | **Dedicated Staging Branch** | Run every upstream merge on `sync/upstream-<version>` before merging into `snowflake/main`. |
| `RULE-SYNC-04` | **Bundle Isolation & 6 Hook Points** | Keep Snowflake code inside `src/lfx_snowflake/` and the 6 tracked core hooks; keep invariant categories untouched. |
| `RULE-SYNC-05` | **Blocking Sanitization Gate** | `python3 scripts/verify_public_repo_sanitization.py` must report 0 secrets and 0 denylist matches before commit/merge. |
| `RULE-SYNC-06` | **Full Test & Protocol Qualification** | `pytest` must pass 100% across all governance, observability, 3 Proxy protocols (`Chat`, `Responses`, `Messages`), Ollama, and component tests. |
| `RULE-SYNC-07` | **Quiesced Backup & Atomic Rollback** | Run `python3 scripts/backup_restore_lifecycle.py` (or `<POSTGRES_INSTANCE>` point-in-time fork) before promoting to `<COMPUTE_POOL>`. |

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
- `src/lfx_snowflake/components/` — `Data Sources` + `Cortex Analyst`, `LLM Operations` + `AI Complete` / `AI Function` / `Stored Procedure` / `UDF-UDTF` / `SQL Statement` / `Cortex Search` / `Snowflake Decision`, `Files & Knowledge` + `Create Memory` + 4-stage `Cortex Search` indexing pipeline (`AI_PARSE_DOCUMENT`, `SPLIT_TEXT_RECURSIVE_CHARACTER`, `CREATE CORTEX SEARCH SERVICE`, `Stream + Task`), `Message History`, `Python Interpreter` (Snowflake PyPI), `ToolGuard / Policies` (ALTK), and `A2A Agent` / `Cortex Agent`.
- `src/lfx_snowflake/native_category_components/` & `scripts/install_live_hooks_in_container.py` — Native Langflow sidebar component classes and build-time `component_index.json` / `index.html` / `snowflake-ui-integration.js` installer (`UI-W2-01`, `HUB-W2-01..05`, `W2-R3-01..07`, `DS-W2-01`, `PROV-W2-01`, `MA-W2-01`, `LLM-W2-01..06`, `FK-W2-01..04`, including Snowsight hierarchical tree selector, 5 LangChain/Langflow Memory Types with optional embeddings, and SPCS SQLite WAL / `UV_CACHE_DIR` runtime hardening).

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
