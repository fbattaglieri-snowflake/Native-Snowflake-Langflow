# SnowflakeLangflow Capability Matrix (`REQ-01..REQ-17`, `ADR-001..ADR-007`, `G-A1..G-A6`)

**Tested Cloud Environment: AWS — Snowflake on AWS**  
**Upstream Baseline:** `langflow-ai/langflow` `v1.12.5` (MIT License)

---

## 1. Qualification Matrix (`Q1..Q4`)

| Qualification ID | Area | Architectural Resolution & Enforcement Contract |
|---|---|---|
| **Q1** | Real Active Role Switching under SPCS | Discovers roles via `SELECT CURRENT_AVAILABLE_ROLES()`. Maintains role-segregated session pool `(user_principal, active_role)` verified via `SELECT CURRENT_ROLE()`. Increments `context_epoch` and invalidates all caches on switch/revocation. Prohibits `SECONDARY ROLES ALL` and silent fallback to service owner. |
| **Q2** | `Python Interpreter` + Snowflake Artifact Repository PyPI | Integrates with `snowflake.snowpark.pypi_shared_repository` requiring database role `SNOWFLAKE.PYPI_REPOSITORY_USER`. Executes in an isolated environment with declared package versions and zero silent fallback to public PyPI or ambient credentials. |
| **Q3** | `ToolGuard / Policies` (ALTK Beta) | Extends the `Language Model` selector in ALTK `Policies` with `Snowflake Proxy - Responses`, `Snowflake Proxy - Messages`, `Snowflake Proxy - Chat`, and `Ollama (Snowflake Hosted)` for both `Generate` and `Guard` modes. Enforces fail-closed execution when `Enabled = true`. Leaves all other `Discover more components` bundles unchanged (`[G-A6]`). |
| **Q4** | `A2A` Client & Server + `Cortex Agent Run API` | Client supports internal Langflow A2A endpoints and `Cortex Agent Run API` (`/api/v2/databases/<DATABASE>/schemas/<SCHEMA>/agents/<AGENT>:run` and `/api/v2/cortex/agent:run`) preserving `thread_id` and `parent_message_id` (from last successful assistant `message_id`). Server requires `Chat Input` + `Chat Output`, separates `Save` vs `Publish`, authenticates caller on SPCS without inheriting flow owner privileges, and guards against recursive loops. |

---

## 2. Requirements & Refinements Matrix (`REQ-01..REQ-17`)

| ID | Requirement | Module / Component | Verification Gate |
|---|---|---|---|
| **REQ-01** | Preserved Langflow UI & Official Snowflake Branding | `frontend_patches/`, `lfx_snowflake/bundle_manifest.py` | Official black Snowflake logo in topbar (home + flow), GitHub/Discord/X removed, `Snowflake Documentation` (`https://docs.snowflake.com/`) with color logo in profile menu (`[G-A6]`). |
| **REQ-02** | Real Role Selector & Explicit Denials Everywhere | `lfx_snowflake/governance/` | 4-code taxonomy (`AUTHZ_ROLE_DENIED` HTTP 403 *"Il ruolo attivo (`<ROLE>`) non consente questa operazione"*, `OBJECT_NOT_FOUND_OR_UNAUTHORIZED`, `UNSUPPORTED_CAPABILITY`, `TECHNICAL_EXECUTION_ERROR`), `EXPLAIN_PRIVILEGES(..., missing_only => true)`. |
| **REQ-03** | Flow Import under `New Flow` | `frontend_patches/`, `lfx_snowflake/components/files_knowledge.py` | `Upload from computer` (secret redaction, no code execution), `Add from stage` (`@<DATABASE>.<SCHEMA>.<STAGE_NAME>`), `Add from Git` (commit provenance). |
| **REQ-04** | Extended `Settings` (`Model Providers`, `DB Providers`, `Messages`, `Global Variables`) | `frontend_patches/`, `lfx_snowflake/providers/registry.py` | 3 Proxy protocols + Ollama, 4 vector backends, multi-protocol message edit/delete with signature invalidation, read-only `SNOWFLAKE_ACTIVE_ROLE`. |
| **REQ-05** | Structured `Data Sources` + `Cortex Analyst` in Unified Session (`[G-A5]`) | `lfx_snowflake/components/data_sources.py` | Separates visibility, `SELECT`, and semantic readiness (`Semantic View` / YAML). No auto Semantic View creation, no Analyst-on-Postgres, no cross-engine implicit joins, bounded SQL results across turns. |
| **REQ-06** | 3 Native Proxy Standard Protocols + `Ollama` | `lfx_snowflake/providers/registry.py` | `/chat/v1`, `/responses/v1` (`ordered-items`, `store=False`, `include=["reasoning.encrypted_content"]`), `/messages` (inference `/messages/v1/messages`, `thinking.signature`, `redacted_thinking`), `<OLLAMA_BASE_URL>`, `call_0..call_N` deduplication, 0 silent retries. |
| **REQ-07** | Snowflake `Embedding Model` | `lfx_snowflake/providers/registry.py` | Separate catalog (`/api/v2/cortex/inference:embed` / `AI_EMBED` + verified Ollama embedders) with `(model_id, dimension, space_fingerprint)` guard. |
| **REQ-08** | `Message History` on Snowflake Postgres & Standard Tables | `lfx_snowflake/components/utilities_a2a.py` | Store/Retrieve (`Messages`/`Table`), separating visible UI messages, signed protocol replay state, and Cortex Agent thread references. |
| **REQ-09** | `LLM Operations` + Native `AI Function` (`[G-A2]`) | `lfx_snowflake/components/llm_operations.py` | `Batch Run`, `Guardrails`, `Smart Router`, `Smart Transform`, `Structured Output` (`LLM Selector` unchanged) + `AI Function` (`AI_REDACT`, `AI_CLASSIFY`, `AI_COMPLETE`, `AI_EXTRACT`, `AI_SENTIMENT`, `AI_SUMMARIZE_AGG`, `AI_AGG`, `AI_TRANSLATE`, `AI_FILTER`) with `EXPLAIN_PRIVILEGES`, `AI_COUNT_TOKENS`, row vs aggregate semantics, and `AI_REDACT` `detect` span masking. |
| **REQ-10** | `Files & Knowledge` & `Memories / Create Memory` (`[G-A3]`, `[G-A4]`) | `lfx_snowflake/components/files_knowledge.py` | `File System` (stage creation), `Read File`, `Write File` (safe atomic `Append`, internal vs external stages), `Knowledge` & `Create Memory` (optional `LLM Preprocessing`, separated `CREATE` vs use permissions, cross-session isolation) across 4 vector backends. |
| **REQ-11** | Unchanged Core Categories | `lfx_snowflake/bundle_manifest.py`, `frontend_patches/` | `Input & Output`, `Prompt Template`, `Processing`, `Flow Control` (with `[Beta]` badges on `Listen`, `Notify`, `Run Flow`), `LLM Selector`, `Calculator`, `Current Date` 100% unchanged. |
| **REQ-12** | `Utilities`: `Python Interpreter` with Snowflake PyPI | `lfx_snowflake/components/utilities_a2a.py` | Uses `snowflake.snowpark.pypi_shared_repository` (`SNOWFLAKE.PYPI_REPOSITORY_USER`) with zero fallback to public PyPI. |
| **REQ-13** | `ToolGuard / Policies` (ALTK Beta, `[G-A6]`) | `lfx_snowflake/components/utilities_a2a.py` | 3 Proxy protocols + Ollama in `Policies`, fail-closed on error, other `Discover more components` bundles untouched. |
| **REQ-14** | `A2A` End-to-End (`A2A Agent` + `Cortex Agent` & `Serve as an A2A agent`) | `lfx_snowflake/components/utilities_a2a.py` | Client + Server A2A + Cortex Agent Threads adapter, caller RBAC enforcement, recursion guard. |
| **REQ-15** | `Flow Activity` SQL Trace Replication & Audit Outbox (`[G-A1]`) | `lfx_snowflake/observability/` | Canonical tables `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS`, `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS`, `<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG`, idempotent `MERGE INTO`, pre-sink redaction, no side-effect re-execution on retry, UI soft-delete (`deleted_in_ui_at`) with anti-resurrection filter. |
| **REQ-16** | Governed Snowflake Object Provisioning | `lfx_snowflake/governance/` | Provisioning with DDL preview, cost/impact notice, explicit confirmation, active-role ownership, and audit logging for `<POSTGRES_INSTANCE>`, `pgvector`, Databases, Schemas, `Standard`/`Hybrid`/`Iceberg`/`Interactive`/`Dynamic` Tables, and Stages. |
| **REQ-17** | 100% Account-Agnostic Public Repo + AWS Attestation | `scripts/verify_public_repo_sanitization.py` | Automated pre-commit/CI scanner verifying zero secrets, zero denylist matches, parametric placeholders, and explicit AWS test attestation. |
