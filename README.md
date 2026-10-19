# SnowflakeLangflow (`lfx_snowflake`)

> ### ⚠️ Disclaimer Importante / Important Notice (Unofficial Personal Fork — Active Testing)
>
> - **Sviluppo Personale / Personal Development:** Questo repository è uno sviluppo personale e indipendente di **Francesco Battaglieri** ([@fbattaglieri-snowflake](https://github.com/fbattaglieri-snowflake)).  
>   *This repository is an independent personal development project by **Francesco Battaglieri** ([@fbattaglieri-snowflake](https://github.com/fbattaglieri-snowflake)).*
> - **Non Ufficiale / Not Official:** Questo progetto **NON è un prodotto, rilascio o repository ufficiale di Snowflake Inc.**, **né tantomeno è un progetto ufficiale di Langflow / DataStax**. Le opinioni, scelte architetturali e modifiche qui contenute appartengono esclusivamente all'autore e non impegnano né rappresentano Snowflake Inc. o Langflow.  
>   *This project is **NOT an official product, release, or repository of Snowflake Inc.**, **nor is it an official project of Langflow / DataStax**. It is not officially supported, endorsed, or warranted by Snowflake Inc. or Langflow.*
> - **Natura di Fork / Open-Source Fork:** Questo repository è un **fork pubblico** del progetto open-source upstream [langflow-ai/langflow](https://github.com/langflow-ai/langflow) (`v1.12.5` baseline, licenza MIT), esteso con il pacchetto isolato `lfx_snowflake` per sperimentare l'esecuzione nativa in Snowpark Container Services (SPCS) e l'integrazione con i servizi Snowflake.  
>   *This repository is a **public fork** of the upstream open-source project [langflow-ai/langflow](https://github.com/langflow-ai/langflow) (`v1.12.5` baseline, MIT License).*
> - **Stato del Progetto — Ancora Oggetto di Test / Under Active Testing:** Questa soluzione è **attualmente oggetto di test, qualifica e validazione attiva (Work in Progress / Experimental Testing)**. API, componenti, schemi SQL e configurazioni possono subire modifiche senza preavviso; non utilizzare in ambienti di produzione senza una propria validazione indipendente.  
>   *This solution is **currently under active testing and validation (Work in Progress / Experimental)**. Do not deploy to production environments without independent qualification and security review.*

Snowflake-native extension bundle, governance layer, and Snowpark Container Services (SPCS) runtime integration for [Langflow](https://github.com/langflow-ai/langflow) (`v1.12.5` baseline, MIT License).

SnowflakeLangflow preserves the exact Langflow user interface, component layout, typography, themes, and developer ergonomics while adding native Snowflake authentication, real-time active role switching (`CURRENT_ROLE()`), fail-closed backend authorization (`AUTHZ_ROLE_DENIED`), governed Snowflake object provisioning, synchronized `Flow Activity` SQL trace replication, and multi-protocol LLM execution.

---

## Tested Cloud Environment: AWS — Snowflake on AWS

**Tested Cloud Environment: AWS — Snowflake on AWS**

This solution has been architected, developed, and verified on **Snowflake on AWS** (Amazon Web Services) using Snowpark Container Services (SPCS), Snowflake Postgres, Snowflake Cortex AI Functions (`AI_*`), Cortex Analyst, Cortex Search, and Cortex Agent Run APIs. Where cross-region Cortex model inference is required on AWS deployments, configure:

```sql
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

All configuration files, SQL scripts, and container specifications in this repository are **100% account-agnostic** and parameterized via placeholders (`<SNOWFLAKE_ACCOUNT>`, `<DATABASE>`, `<SCHEMA>`, `<COMPUTE_POOL>`, `<IMAGE_REPOSITORY>`, `<STAGE_NAME>`, `<POSTGRES_INSTANCE>`, `<LLM_PROXY_BASE_URL>`, `<OLLAMA_BASE_URL>`).

---

## Key Capabilities (`REQ-01..REQ-17`)

1. **Preserved Langflow UI & Official Snowflake Branding (`REQ-01`)**
   - Unmodified Langflow React/Tailwind/Radix design system, spacing, themes, and component categories.
   - Official black Snowflake logo in topbar (home and flow editor), removal of GitHub/Discord/X links, and `Snowflake Documentation` (`https://docs.snowflake.com/`) entry with color Snowflake icon in the profile menu.
2. **Real Active Role Selector & Universal Fail-Closed Authorization (`REQ-02`, `ADR-003`, `ADR-004`)**
   - Discovers granted roles via `CURRENT_AVAILABLE_ROLES()`, binds per-role Snowflake sessions verified with `SELECT CURRENT_ROLE()`, increments `context_epoch` on role switch, invalidates caches immediately, and never falls back to service-owner roles or `SECONDARY ROLES ALL`.
   - Standardized 4-code error taxonomy: `AUTHZ_ROLE_DENIED` (HTTP 403: *"Il ruolo attivo (`<ROLE>`) non consente questa operazione"*), `OBJECT_NOT_FOUND_OR_UNAUTHORIZED`, `UNSUPPORTED_CAPABILITY`, and `TECHNICAL_EXECUTION_ERROR`.
   - Zero-credit SQL/AI Function privilege pre-checks via `EXPLAIN_PRIVILEGES(..., missing_only => true)`.
3. **Governed Flow Imports (`REQ-03`)**
   - `New Flow` supports `Upload from computer` (with automatic secret redaction and no custom code auto-execution), `Add from stage` (`@<DATABASE>.<SCHEMA>.<STAGE_NAME>`), and `Add from Git` (with commit provenance tracking).
4. **Extended Settings & Memories (`REQ-04`, `REQ-10`)**
   - `Model Providers`: `Snowflake Proxy - Responses`, `Snowflake Proxy - Messages`, `Snowflake Proxy - Chat`, and `Ollama (Snowflake Hosted)`.
   - `DB Providers`: `Standard Table (VECTOR)`, `Hybrid Table (VECTOR)`, `Cortex Search`, and `Snowflake Postgres (pgvector)`.
   - `Messages`: multi-protocol history inspection, editing, and deletion across `Chat`, `Responses`, and `Messages` with cryptographic signature/encrypted reasoning block invalidation handling.
   - `Global Variables`: read-only backend-bound `SNOWFLAKE_ACTIVE_ROLE` and role-filtered defaults.
5. **Structured Data Sources & Cortex Analyst in Unified Sessions (`REQ-05`, `[G-A5]`)**
   - Hierarchical browser (`<DATABASE> -> <SCHEMA> -> <OBJECT>`) for `Standard Table`, `View`, `Hybrid Table`, `Dynamic Table`, `Interactive Table`, and `Iceberg Table`, plus a distinct `Snowflake Postgres` adapter.
   - Separates catalog visibility, `SELECT` permission, and `Cortex Analyst` semantic readiness (`Semantic View` or validated YAML). Never auto-creates Semantic Views or implicit cross-engine joins.
6. **Three Native Proxy Standard Protocols + Ollama & Dedicated Embeddings (`REQ-06`, `REQ-07`, `ADR-006`)**
   - `Snowflake Proxy - Chat` (`<LLM_PROXY_BASE_URL>/chat/v1`), `Snowflake Proxy - Responses` (`<LLM_PROXY_BASE_URL>/responses/v1` with `ordered-items`, `store=False`, `include=["reasoning.encrypted_content"]`), `Snowflake Proxy - Messages` (`<LLM_PROXY_BASE_URL>/messages` base, `/messages/v1/messages` inference, preserving `thinking.signature` and `redacted_thinking`), and `Ollama` (`<OLLAMA_BASE_URL>`).
   - Separate `Embedding Model` catalog (`/api/v2/cortex/inference:embed` / `AI_EMBED` + verified Ollama embedders) with `(model_id, dimension, space_fingerprint)` compatibility guard.
7. **LLM Operations & Native `AI Function` Component (`REQ-09`, `[G-A2]`)**
   - Extends `Batch Run`, `Guardrails`, `Smart Router`, `Smart Transform`, and `Structured Output` (`LLM Selector` unchanged).
   - Adds `AI Function` (`AI_REDACT`, `AI_CLASSIFY`, `AI_COMPLETE`, `AI_EXTRACT`, `AI_SENTIMENT`, `AI_SUMMARIZE_AGG`, `AI_AGG`, `AI_TRANSLATE`, `AI_FILTER`) using direct `AI_*` namespace, `AI_COUNT_TOKENS` estimation, row-level vs aggregate semantics, and automatic PII span masking in `AI_REDACT` `detect` mode.
8. **Files, Knowledge, Vector Memories, Message History, Python Interpreter PyPI, ToolGuard & A2A (`REQ-08..REQ-14`)**
   - Safe `Write File` `Append` semantics on `@<DATABASE>.<SCHEMA>.<STAGE_NAME>`, `Create Memory` with optional `LLM Preprocessing` and separated `CREATE` vs usage permissions (`[G-A3]`, `[G-A4]`), `Python Interpreter` integrated with `snowflake.snowpark.pypi_shared_repository` (`SNOWFLAKE.PYPI_REPOSITORY_USER`), ALTK `ToolGuard / Policies` fail-closed (`[G-A6]`), and `A2A Agent` client/server + `Cortex Agent Run API` (`thread_id`, `parent_message_id`).
9. **Synchronized `Flow Activity` SQL Traces & Durable Audit Outbox (`REQ-15`, `ADR-007`, `[G-A1]`)**
   - Replicates `Flow Activity` runs and spans idempotently into `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS` and `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS`, and writes governance events/denials to `<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG`.
10. **Governed Snowflake Object Provisioning (`REQ-16`, `ADR-005`)**
    - Creates Snowflake Postgres instances (`<POSTGRES_INSTANCE>`) and internal `pgvector` tables, databases, schemas, `Standard`/`Hybrid`/`Iceberg`/`Interactive`/`Dynamic` tables, and internal/external stages with dry-run DDL preview, explicit user confirmation, active-role ownership, and durable audit logging.

---

## Parametric Placeholders Reference

| Placeholder | Description |
|---|---|
| `<SNOWFLAKE_ACCOUNT>` | Target Snowflake account identifier |
| `<DATABASE>` | Target Snowflake database for Langflow objects and telemetry |
| `<SCHEMA>` | Target Snowflake schema for tables, stages, and services |
| `<COMPUTE_POOL>` | SPCS Compute Pool hosting the Langflow container service |
| `<IMAGE_REPOSITORY>` | SPCS Image Repository FQN (`<DATABASE>.<SCHEMA>.<IMAGE_REPOSITORY>`) |
| `<STAGE_NAME>` | Internal or External Named Stage (`<DATABASE>.<SCHEMA>.<STAGE_NAME>`) |
| `<POSTGRES_INSTANCE>` | Snowflake Postgres instance name for ORM persistence / `pgvector` |
| `<LLM_PROXY_BASE_URL>` | Base URL of the Standard LLM Proxy service (`Chat`, `Responses`, `Messages`) |
| `<OLLAMA_BASE_URL>` | Base URL of the Snowflake-hosted Ollama service |

---

## Upstream Update & Synchronization Rules (`langflow-ai/langflow`)

Because this repository is a fork of [`langflow-ai/langflow`](https://github.com/langflow-ai/langflow), all updates from upstream must follow the mandatory governance rules documented in [`docs/UPSTREAM_SYNC.md`](docs/UPSTREAM_SYNC.md) and enforced by [`scripts/sync_upstream.sh`](scripts/sync_upstream.sh):

1. **`RULE-SYNC-01` — Read-Only Upstream Remote:** `upstream` (`https://github.com/langflow-ai/langflow.git`) is strictly read-only; all fork commits and tags are pushed exclusively to `origin` (`snowflake/main` or scoped `feat/*` / `sync/upstream-*` branches).
2. **`RULE-SYNC-02` — Stable Release Tag Pinning Only:** Never merge arbitrary untagged commits from upstream `main`. Always synchronize against an explicit upstream release tag (e.g., `v1.12.5` → next stable tag).
3. **`RULE-SYNC-03` — Dedicated Staging Branch (`sync/upstream-<tag>`):** Every upstream update is staged on `sync/upstream-<tag>` via `bash scripts/sync_upstream.sh <tag>` before any fast-forward/merge into `snowflake/main`.
4. **`RULE-SYNC-04` — Strict `lfx_snowflake` Bundle Isolation & 6 Tracked Hook Points:** Keep 95%+ of Snowflake logic inside `src/lfx_snowflake/`, `frontend_patches/`, `sql/`, and `docs/`. Verify that the 6 tracked core hook points and invariant categories (`Input & Output`, `Prompt Template`, `Processing`, `Flow Control`, `LLM Selector`, `Calculator`, `Current Date`) remain intact.
5. **`RULE-SYNC-05` — Mandatory Pre-Commit & CI Sanitization Gate:** `python3 scripts/verify_public_repo_sanitization.py` must pass with **0 secrets** and **0 denylist matches** across all working-tree files and Git commit history before pushing.
6. **`RULE-SYNC-06` — Full Regression & Multi-Protocol Qualification Gate:** The full `pytest` suite (`tests/`) covering RBAC role switching, 3 Proxy Standard protocols (`Chat`, `Responses`, `Messages`), Ollama, AI Functions, and SQL trace replication must pass 100%.
7. **`RULE-SYNC-07` — Pre-Upgrade Quiesced Backup & Atomic Rollback:** Before deploying an updated image to `<COMPUTE_POOL>`, run `python3 scripts/backup_restore_lifecycle.py` (or create a point-in-time fork of `<POSTGRES_INSTANCE>`) so both container image and ORM schema can be rolled back atomically.

---

## Verification & Sanitization Gate

Run the automated sanitization gate and test suite before any commit or push:

```bash
python3 scripts/verify_public_repo_sanitization.py
pytest
```

