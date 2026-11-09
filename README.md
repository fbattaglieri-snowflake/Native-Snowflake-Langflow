<div align="center">

# Native Snowflake Langflow

**Snowflake-native governance, Cortex AI, and SPCS integration for Langflow.**

[![License: MIT](https://img.shields.io/badge/License-MIT-success?style=for-the-badge)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-SPCS-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://docs.snowflake.com/)
[![Langflow](https://img.shields.io/badge/Langflow-v1.12.5_baseline-7E57C2?style=for-the-badge)](https://github.com/langflow-ai/langflow)

</div>

> [!WARNING]
> **Unofficial personal fork, under active testing.** This repository ([`fbattaglieri-snowflake/Native-Snowflake-Langflow`](https://github.com/fbattaglieri-snowflake/Native-Snowflake-Langflow)) is an independent project by **Francesco Battaglieri** ([@fbattaglieri-snowflake](https://github.com/fbattaglieri-snowflake)). It is **not** an official product of Snowflake Inc. or of Langflow / DataStax, and it is not supported or warranted by either. APIs, components, SQL schemas, and configuration may change without notice. Do not deploy to production without your own qualification and security review.

> [!TIP]
> Public fork of [langflow-ai/langflow](https://github.com/langflow-ai/langflow) (`v1.12.5` baseline) released under the **MIT License**. Snowflake extensions live in the isolated `lfx_snowflake` package.

> [!NOTE]
> **W3-R1 redesign in progress.** Memories, Knowledge & Files, caller identity, and traceability are being reworked. Phase 0 changes exist only on a development branch and are **not deployed**. See [Status & Roadmap](#status--roadmap).

---

## At a Glance: Why Native Snowflake Langflow?

**Native Snowflake Langflow** keeps 100% of the standard Langflow visual experience while turning every workflow into a **first-class, role-governed Snowflake application** running inside **Snowpark Container Services (SPCS)**.

| Pillar | What It Does | Where in the UI | Status |
|---|---|---|---|
| **1. Role-scoped Snowflake access** | Queries run through a service session scoped to the role selected in the topbar. True caller's-rights execution (per-user identity) is not live yet. | **`❄️ Role: [ <ROLE> ▾ ]`** in the topbar | In progress (caller's rights: W3-R1 Phase 1) |
| **2. Per-Flow `[ ❄️ Context ]`** | Each flow defines its own visibility scope (databases, schemas, tables, views, stages, routines, Cortex Agents, Snowflake Postgres). | **`[ ❄️ Context ]`** in the flow editor | Live |
| **3. Native Cortex AI & Multimodal** | Visual nodes for Cortex Agent, Search, Analyst, Custom Tool, Decision, AI Redact, AI Complete, and 14 `AI_*` functions over stages. | **`[ ❄️ ]`** components in the sidebar | Live |
| **4. Memories, Knowledge & Files** | Agent memories, knowledge bases, and stage files backed by real Snowflake objects only. Internal and external stages are listed from live Snowflake metadata; uploads go to internal stages only; External Volumes move to a separate configuration panel. | **`Memories`**, **`Stages & Volumes`** | Under redesign (W3-R1) — Planned |
| **5. Snowflake Triggers** | `TASK`, `STREAM` (table/stage), and `ALERT` triggers that run Langflow flows from Snowflake data changes. | **`Triggers`** sidebar category | Live |
| **6. SQL Observability** | Flow runs, spans, and audit events are replicated to Snowflake SQL tables. Per-user attribution and query tags are being completed. | **`Flow Activity`**, `<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY` | Live (attribution: In progress) |

---

## Status & Roadmap

| Area | Status | Notes |
|---|---|---|
| `[ ❄️ Context ]`, Cortex AI nodes, 5 Triggers | Live | Behaviour unchanged by W3-R1 |
| Memories pages and Memory Base | Planned | Demo rows and in-memory backends are being removed; only real Snowflake objects will be shown |
| Stages & Volumes | In progress | Internal and external stages from live metadata; upload to internal stages only; External Volumes in a separate panel |
| Message History on Snowflake | In progress | Current backend is in-memory per role |
| Caller's-rights identity | Planned | W3-R1 Phase 1 |
| Traceability (query tags, audit) | In progress | W3-R1 |

---

## 1. End-to-End Architecture

```mermaid
flowchart TB
    subgraph Browser["1. Langflow Visual Studio UI (React + Tailwind)"]
        Topbar["Topbar Controls<br/>• ❄️ Role Selector (CURRENT_ROLE)<br/>• [ ❄️ Context ] Per-Flow Scope"]
        LeftRail["Left Navigation Rail<br/>• Flows & Agents<br/>• Memories (#1..#5)<br/>• Stages & Volumes (Bucket)<br/>• Traces (Flow Activity)"]
        Canvas["Workflow Canvas & Sidebar Categories<br/>• Input & Output (Invariant)<br/>• Triggers (5)<br/>• Data Sources (6)<br/>• Models & Agents (7)<br/>• LLM Operations (5 Native + 7 [ ❄️ ])<br/>• Files & Knowledge (5)"]
    end

    subgraph SPCS["2. Snowpark Container Services (SPCS Runtime — lfx_snowflake)"]
        Auth["Zero-Prompt SPCS Auto-Login<br/>& Per-Role Session Pool"]
        Guard["Fail-Closed RBAC Guard<br/>EXPLAIN_PRIVILEGES + Context Cone"]
        Bridge["LiveSnowflakeBridge<br/>Catalog Discovery • SQL Execution • Multimodal Stage I/O"]
        Persistence["Durable Block Volume (/persist)<br/>SQLite WAL (langflow.db) + Real-Time SQL Replicator"]
    end

    subgraph Snowflake["3. Snowflake Data Cloud (<SNOWFLAKE_ACCOUNT> on AWS)"]
        Cortex["Snowflake Cortex AI<br/>• Cortex Agents & Threads API<br/>• Cortex Search & Cortex Analyst<br/>• AI_COMPLETE (Decision & Multimodal)<br/>• AI_REDACT & 14 AI_* Functions"]
        Storage["Governed Storage & Compute<br/>• Standard / Hybrid / Dynamic / Iceberg Tables<br/>• Internal Stages (SNOWFLAKE_SSE) & External Buckets (S3/GCS/Azure)<br/>• Stored Procedures, UDFs & UDTFs<br/>• Snowflake Postgres (:5432 / pgvector)"]
        Observability["Unified SQL Telemetry<br/>• <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY<br/>• LANGFLOW_FLOW_RUNS / SPANS / AUDIT_LOG"]
    end

    Topbar & LeftRail & Canvas --> Auth --> Guard --> Bridge
    Bridge --> Cortex & Storage
    Bridge --> Persistence --> Observability
```

---

## 2. How Role Governance & Per-Flow `[ ❄️ Context ]` Work

Every workflow has its own **isolated context cone**. Selecting databases, schemas, or objects in **Flow A** never pollutes **Flow B**, and switching the active role in the topbar dynamically computes the intersection between the flow's context and the role's live privileges.

```mermaid
sequenceDiagram
    autonumber
    actor User as User in Browser
    participant UI as Topbar & Canvas Nodes
    participant Bridge as LiveSnowflakeBridge (SPCS)
    participant SF as Snowflake (<SNOWFLAKE_ACCOUNT>)

    User->>UI: 1. Select Active Role (❄️ Role)
    UI->>Bridge: POST /api/v1/snowflake/context (role)
    Bridge->>SF: USE ROLE <ROLE>; SELECT CURRENT_ROLE();
    SF-->>Bridge: Active role verified (context_epoch++)

    User->>UI: 2. Open [ ❄️ Context ] inside /flow/<flow_id>
    UI->>Bridge: Browse Databases ▸ Schemas ▸ Objects + Postgres
    User->>UI: Check allowed Databases / Schemas / Stages / Routines
    UI->>Bridge: Save isolated context for <flow_id>
    Bridge-->>UI: Instantly updates all Canvas Node dropdowns (< 5 ms)

    User->>UI: 3. Run Workflow Node (SQL / AI_* / Custom Tool)
    UI->>Bridge: Execute node with X-Snowflake-Flow-Id
    Bridge->>SF: Verify EXPLAIN_PRIVILEGES & execute under CURRENT_ROLE()
    SF-->>Bridge: Result + Query ID
    Bridge->>SF: Async MERGE INTO <DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY
    Bridge-->>UI: Return result to Canvas / Playground
```

---

## 3. Visual Guide to the Sidebar Categories

All Snowflake components are cleanly integrated into the native Langflow sidebar categories (`1 Component class = 1 Python file`), with zero redundant bottom bundles.

```text
+---------------------------------------------------------------------------------------------------+
|  LANGFLOW SIDEBAR CATEGORIES                                                                      |
|                                                                                                   |
|  1. Input & Output     -> Chat Input, Chat Output, Text Input, Text Output, Webhook (Invariant)   |
|  2. Triggers           -> [❄️] Schedule | [❄️] Table Change | [❄️] Stage File | [❄️] Alert | Webhook |
|  3. Data Sources       -> [❄️] Tables | [❄️] Views | [❄️] Iceberg | [❄️] Postgres | Search | Analyst |
|  4. Models & Agents    -> Language Model | Agent | [❄️] Cortex Agent | A2A | Embedding | History   |
|  5. LLM Operations     -> [❄️] Custom Tool | [❄️] Decision | [❄️] AI Redact | [❄️] AI Complete | ... |
|  6. Files & Knowledge  -> File System | Knowledge | Memory Base | Read File | Write File          |
+---------------------------------------------------------------------------------------------------+
```

---

### 3.1 `LLM Operations` — Cortex AI, Custom Tools & Structured Decisions

The **`LLM Operations`** category combines the 5 native Langflow operations (`Batch Run`, `Guardrails`, `Smart Router`, `Smart Transform`, `Structured Output`) with **7 Snowflake Cortex components**:

```mermaid
flowchart LR
    subgraph Context["Active [ ❄️ Context ]"]
        Routines["Procedures, UDFs & UDTFs<br/>in <DATABASE>.<SCHEMA>"]
        Stages["Internal Stages (SNOWFLAKE_SSE)<br/>& External Buckets (S3 / GCS / Azure)"]
        Tables["Snowflake Tables & Views"]
    end

    subgraph LLMOps["[ ❄️ ] LLM Operations Nodes"]
        CT["1. [ ❄️ ] Custom Tool<br/>Replicates Snowsight 'Add custom tool'<br/>(procedure | function | table_function)"]
        DEC["2. [ ❄️ ] Decision<br/>snowflake-decision-fast (default)<br/>snowflake-decision (high accuracy)<br/>(choice | score | noul)"]
        RED["3. [ ❄️ ] AI Redact<br/>13 Official PII Categories<br/>(redact | detect | detect_with_allowlist)"]
        AIC["4. [ ❄️ ] AI Complete<br/>Text, Structured JSON & Multimodal<br/>(Images, .mp3/Audio, Video, Docs)"]
        AIF["5. [ ❄️ ] AI Function<br/>14 Task-Specific AI_* Functions<br/>(TO_FILE & DIRECTORY(@stage))"]
        SQL["6. [ ❄️ ] SQL Statement<br/>Parameterized Governed SQL"]
        OPS["7. [ ❄️ ] Operations<br/>Multi-Mode Cortex Helper"]
    end

    Routines --> CT
    Stages --> AIC & AIF & RED
    Tables --> DEC & RED & AIC & AIF & SQL & OPS
```

| Component (`display_name`) | Class (`name`) | What It Does & Key Capabilities |
|---|---|---|
| **`[ ❄️ ] Custom Tool`** | `SnowflakeCustomTool` | Replicates the Snowsight Cortex Agent **"Add custom tool"** dialog 1:1. Supports `procedure`, scalar `function` (`UDF`), and `table_function` (`UDTF`). Auto-populates parameters from `DESCRIBE PROCEDURE/FUNCTION`, supports manual parameter editing (`+ Add parameter` / `Delete`, `Sort by: Latest added \| Alphabetical \| Required first`), blocks unsupported `OBJECT` parameters, supports `User's default` or `Custom` warehouse + `Query timeout (1–600s)`, and outputs both a Cortex Agent `Tool` (`tool_spec` + `tool_resources`) and direct SQL execution (`CALL` / `SELECT` / `TABLE(...)`). |
| **`[ ❄️ ] Decision`** | `SnowflakeDecision` | Implements official **Snowflake Decision with `AI_COMPLETE`** across the two dedicated models: **`snowflake-decision-fast`** *(Default — low latency, 2–16 `choice` options, 32k token limit)* and **`snowflake-decision`** *(High accuracy, 2–32 `choice` options, 256 KiB JSON limit)*. Supports **1–32 questions per call** (max 256 total options) across **`choice`**, **`score`** (`0..n-1` rubric with weighted average $\sum_{i=0}^{n-1} i \cdot p_i$), and **`noul`** (statement probability in `~0.08–0.92`), normalized confidence $\frac{n \cdot p_{\max} - 1}{n - 1}$, Single-State and Table-Level processing, and **High vs. Low Confidence Routing** outputs. |
| **`[ ❄️ ] AI Redact`** | `SnowflakeAIRedact` | Dedicated PII detection and masking component (`AI_REDACT`) supporting **`redact`**, **`detect`** (with automatic span masking in traces), and **`detect_with_allowlist`** (selective redaction splicing in reverse index order). Covers all **13 official Snowflake PII categories** (`NAME`, `EMAIL`, `PHONE_NUMBER`, `DATE_OF_BIRTH`, `GENDER`, `AGE`, `ADDRESS`, `NATIONAL_ID`, `PASSPORT`, `TAX_IDENTIFIER`, `PAYMENT_CARD_DATA`, `DRIVERS_LICENSE`, `IP_ADDRESS`), enforces the 4,096-token limit via `AI_COUNT_TOKENS('ai_redact', ...)` with automatic `SPLIT_TEXT_RECURSIVE_CHARACTER` chunking, and supports pre-conversion from stage documents/audio. |
| **`[ ❄️ ] AI Complete`** | `SnowflakeAIComplete` | Generative `AI_COMPLETE` node supporting text prompts, `model_parameters`, forced JSON schema output (`response_format`), and **direct multimodal inputs (`TO_FILE` & `DIRECTORY(@stage)`)** from Internal Stages and External Buckets (`S3`, `GCS`, `Azure`) for **images**, **`.mp3`/audio**, and **video** (including `gemini-3.1-pro`, `gemini-3.1-flash-lite`, `gemini-3.5-flash`). |
| **`[ ❄️ ] AI Function`** | `SnowflakeAIFunction` | Unified node for the **14 task-specific Cortex AI Functions**: `AI_CLASSIFY`, `AI_FILTER`, `AI_EXTRACT`, `AI_SENTIMENT`, `AI_SUMMARIZE`, `AI_SUMMARIZE_AGG`, `AI_AGG`, `AI_TRANSLATE`, `AI_SIMILARITY`, `AI_TRANSCRIBE` (with `word`/`speaker` timestamps on `.mp3`/audio/video), `AI_PARSE_DOCUMENT` (`LAYOUT`/`OCR`), `AI_EMBED`, `AI_MULTI_EMBED` (`twelvelabs-marengo-embed-3-0` on image/audio/video), and `AI_COUNT_TOKENS`, reading from both tables and **Internal / External Stages**. |
| **`[ ❄️ ] SQL Statement`** | `SnowflakeSQLStatement` | Executes governed parameterized SQL (`SELECT`, `CALL`) within `<DATABASE>.<SCHEMA>` under `CURRENT_ROLE()`, returning either a `DataFrame` or structured `JSON` (`OBJECT_CONSTRUCT(*)`). |
| **`[ ❄️ ] Operations`** | `SnowflakeLLMNode` | Multi-operation Snowflake Cortex and SQL utility node synchronized with the active flow context. |

---

### 3.2 Multimodal Stage & Cloud Bucket Support (`Internal` & `External`)

Both **`[ ❄️ ] AI Function`** and **`[ ❄️ ] AI Complete`** read media files directly from **Internal Stages** (`SNOWFLAKE_SSE` / `SNOWFLAKE_FULL`) and **External Cloud Stages** (`Amazon S3`, `Google Cloud Storage`, `Microsoft Azure`, `S3-Compatible`) enabled in `[ ❄️ Context ]`:

| Media Category | Supported Extensions | Enabled Cortex Functions | Execution Modes |
|---|---|---|---|
| **Images** | `.jpg`, `.jpeg`, `.png`, `.webp`, `.gif`, `.bmp` | `AI_COMPLETE`, `AI_CLASSIFY`, `AI_FILTER`, `AI_EXTRACT`, `AI_SUMMARIZE`, `AI_SIMILARITY`, `AI_PARSE_DOCUMENT`, `AI_EMBED`, `AI_MULTI_EMBED` | Single/Multi-file `TO_FILE('@<DATABASE>.<SCHEMA>.<STAGE_NAME>', '<path>')` or Batch `DIRECTORY(@<DATABASE>.<SCHEMA>.<STAGE_NAME>)` |
| **Audio / `.mp3`** | `.mp3`, `.wav`, `.flac`, `.m4a`, `.ogg`, `.aac`, `.webm`, `.mp4` | `AI_TRANSCRIBE` (`word` / `speaker` timestamps), `AI_COMPLETE` (`gemini-3.1-pro`, `gemini-3.1-flash-lite`, `gemini-3.5-flash`), `AI_MULTI_EMBED` (`twelvelabs-marengo-embed-3-0`) | Single file `TO_FILE(...)` or Batch `DIRECTORY(@...)` |
| **Video** | `.mp4`, `.mov`, `.webm`, `.mkv`, `.avi`, `.flv`, `.mpeg`, `.mpg`, `.wmv`, `.3gpp`, `.ogv` | `AI_COMPLETE` (scene & audio analysis), `AI_TRANSCRIBE` (audio track extraction), `AI_MULTI_EMBED` (`visual`, `audio`, `transcription` embeddings) | Single file `TO_FILE(...)` or Batch `DIRECTORY(@...)` |
| **Documents** | `.pdf`, `.docx`, `.pptx`, `.html`, `.txt`, `.md`, `.eml`, `.tiff` | `AI_PARSE_DOCUMENT` (`LAYOUT` / `OCR`, `page_split`), `AI_EXTRACT`, `AI_SUMMARIZE` | Single file `TO_FILE(...)` or Batch `DIRECTORY(@...)` |

---

### 3.3 `Models & Agents` — 4 Snowflake Model Providers, Cortex Agents & A2A

```mermaid
flowchart TB
    subgraph Nodes["Models & Agents Sidebar (7 Canonical Components)"]
        LM["Language Model"]
        AG["Agent"]
        CA["[ ❄️ ] Cortex Agent"]
        A2A["A2A Agent"]
        EMB["Embedding Model"]
        MH["[ ❄️ ] Message History"]
        PT["Prompt Template"]
    end

    subgraph Providers["Pre-Configured Snowflake Providers"]
        PChat["Snowflake Proxy - Chat<br/>(<LLM_PROXY_BASE_URL>/chat/v1)"]
        PResp["Snowflake Proxy - Responses<br/>(<LLM_PROXY_BASE_URL>/responses/v1)"]
        PMsg["Snowflake Proxy - Messages<br/>(<LLM_PROXY_BASE_URL>/messages)"]
        POllama["Snowflake Private - Ollama<br/>(<OLLAMA_BASE_URL>)"]
        CAgentAPI["Snowflake Cortex Agent Run API<br/>+ 2-Level Threads API Continuity"]
        CEmbed["Snowflake Cortex AI_EMBED<br/>(6 Official 768d / 1024d Models)"]
    end

    LM & AG --> PChat & PResp & PMsg & POllama
    CA --> CAgentAPI
    A2A --> CAgentAPI
    EMB --> CEmbed
```

- **Dynamic Reasoning Controls:** Selecting a reasoning-capable model under `Snowflake Proxy - Chat`, `Responses`, or `Messages` automatically reveals `Reasoning Effort` or `Extended Thinking` (`Thinking Budget Tokens`) directly below the model selector while hiding API key fields (handled by SPCS).
- **2-Level Cortex Agent Threads:** `[ ❄️ ] Cortex Agent` preserves multi-turn conversation state across turns using a local SQLite thread cache (`0 ms` overhead) backed by the Snowflake Cortex `Threads API` (`/api/v2/cortex/threads`).

---

### 3.4 `Files & Knowledge`, `Memories (#1..#5)` & `Stages & Volumes`

The left navigation rail includes dedicated views for **`Memories`** (with the **`[ + ❄️ Snowflake ]`** wizard, which automatically dismisses transient popovers on tab switch, outside click, or `Escape`) and **`Stages & Volumes`** (Bucket icon above `Traces`).

```mermaid
flowchart LR
    subgraph Wizard["Memories Page (+ ❄️ Snowflake)"]
        M1["#1 Graph Checkpointer<br/>(LangGraph State)"]
        M2["#2 Buffer Window<br/>(Short-Term K Turns)"]
        M3["#3 Summary Memory<br/>(Rolling Compression)"]
        M4["#4 User Profile / Entity<br/>(Cross-Session Facts)"]
        M5["#5 Episodic Few-Shot<br/>(Past Successful Runs)"]
    end

    subgraph Storage["Snowflake Storage Backends"]
        SFTable["Snowflake Tables<br/>(Standard | Hybrid | Dynamic | Interactive | Iceberg)<br/>Optional VECTOR(FLOAT, 768|1024) + AI_EMBED"]
        PG["Snowflake Postgres (:5432)<br/>(<POSTGRES_INSTANCE> Relational / JSONB / pgvector)"]
    end

    subgraph Canvas["Workflow Canvas"]
        MB["Memory Base Node<br/>(Adapts ports to #1..#5)"]
    end

    M1 & M2 & M3 & M4 & M5 --> SFTable & PG --> MB
```

---

### 3.5 `Triggers` — Event-Driven & Scheduled Snowflake Execution

Positioned immediately after `Input & Output`, the **`Triggers`** category provides **5 specialized components** powered by the SPCS Service Function `<DATABASE>.<SCHEMA>.RUN_LANGFLOW_FLOW(flow_id, trigger_payload)`:

| Trigger Component | Snowflake Primitive | When It Fires |
|---|---|---|
| **`[ ❄️ ] Schedule Trigger`** | `CREATE OR ALTER TASK ... SCHEDULE = 'USING CRON ...'` | Time-based cron or interval schedules (`Minutes`, `Hours`, `Days`, `Weeks`, `Months`). |
| **`[ ❄️ ] Table Change Trigger`** | `CREATE STREAM ON TABLE/VIEW` + Triggered Task `WHEN SYSTEM$STREAM_HAS_DATA(...)` | Row inserts/updates/deletes (CDC) on any table or view in `[ ❄️ Context ]`. |
| **`[ ❄️ ] Stage File Trigger`** | `DIRECTORY = (ENABLE = TRUE)` + `CREATE STREAM ON STAGE` + Triggered Task | New or updated files arriving on `@<DATABASE>.<SCHEMA>.<STAGE_NAME>`. |
| **`[ ❄️ ] Condition Alert Trigger`** | `CREATE OR ALTER ALERT ... IF (EXISTS (<sql>))` | SQL threshold or anomaly conditions evaluated periodically or on new data. |
| **`Webhook`** | Native HTTP Webhook Endpoint | External HTTP POST payloads (`WEBHOOK_RUN`). |

---

### 3.6 `Data Sources` — Governed Business Data & Semantic Models

| Component | Hierarchy in Node | Description |
|---|---|---|
| **`[ ❄️ ] Tables`** | `Database -> Schema -> Table Type -> Table` | Reads `Standard`, `Hybrid`, `Dynamic`, and `Interactive` tables in `[ ❄️ Context ]`. |
| **`[ ❄️ ] Views`** | `Database -> Schema -> View Type -> View` | Reads `Standard`, `Secure`, and `Materialized` views in `[ ❄️ Context ]`. |
| **`[ ❄️ ] Iceberg Tables`** | `Database -> Schema -> Iceberg Table` | Reads Apache Iceberg tables managed in Snowflake. |
| **`[ ❄️ ] Postgres`** | `Postgres Instance -> Table / Query` | Account-level connection to `<POSTGRES_INSTANCE>` (`:5432`). |
| **`[ ❄️ ] Cortex Search`** | `Database -> Schema -> Search Service` | Queries Cortex Search services (also usable as an Agent `StructuredTool`). |
| **`[ ❄️ ] Cortex Analyst`** | `Database -> Schema -> Semantic View / @Stage YAML` | Natural-language-to-SQL via Cortex Analyst Semantic Views or stage YAML models. |

---

## 4. Tested Cloud Environment & Parametric Placeholders

**Tested Cloud Environment: AWS — Snowflake on AWS**

This solution has been architected, developed, and verified on **Snowflake on AWS** (Amazon Web Services) using Snowpark Container Services (SPCS), Snowflake Postgres, Snowflake Cortex AI Functions (`AI_*`), Cortex Analyst, Cortex Search, and Cortex Agent Run APIs. Where cross-region Cortex model inference is required on AWS deployments, configure:

```sql
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

All configuration files, SQL scripts, and container specifications in this repository are **100% account-agnostic** and parameterized via the following placeholders:

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

## 5. Architecture Decision Records & Documentation Index

| Document | Purpose |
|---|---|
| [`docs/CAPABILITY_MATRIX.md`](docs/CAPABILITY_MATRIX.md) | Visual capability overview, component inventory, and qualification matrix (`Q1..Q4`, `REQ-01..REQ-17`, Wave 2 enhancements) |
| [`docs/DEPLOYMENT_SPCS.md`](docs/DEPLOYMENT_SPCS.md) | Step-by-step Snowpark Container Services (SPCS) deployment guide and SQL setup |
| [`docs/UPSTREAM_SYNC.md`](docs/UPSTREAM_SYNC.md) | Governance rules (`RULE-SYNC-01..07`) for synchronizing with `langflow-ai/langflow` |
| [`docs/ADR/ADR-001.md`](docs/ADR/ADR-001.md) .. [`ADR-007.md`](docs/ADR/ADR-007.md) | Architecture Decision Records covering UI invariants, active-role RBAC, fail-closed authorization, object provisioning, multi-protocol LLM proxy, and SQL trace replication |

---

## 6. Upstream Update & Synchronization Rules (`langflow-ai/langflow`)

Because this repository is a fork of [`langflow-ai/langflow`](https://github.com/langflow-ai/langflow), all updates from upstream follow the governance rules documented in [`docs/UPSTREAM_SYNC.md`](docs/UPSTREAM_SYNC.md) and enforced by [`scripts/sync_upstream.sh`](scripts/sync_upstream.sh):

1. **`RULE-SYNC-01` — Read-Only Upstream Remote:** `upstream` (`https://github.com/langflow-ai/langflow.git`) is strictly read-only; all fork commits and tags are pushed exclusively to `origin` (`snowflake/main` or scoped `feat/*` / `sync/upstream-*` branches).
2. **`RULE-SYNC-02` — Stable Release Tag Pinning Only:** Never merge arbitrary untagged commits from upstream `main`. Always synchronize against an explicit upstream release tag (e.g., `v1.12.5` → next stable tag).
3. **`RULE-SYNC-03` — Dedicated Staging Branch (`sync/upstream-<tag>`):** Every upstream update is staged on `sync/upstream-<tag>` via `bash scripts/sync_upstream.sh <tag>` before any fast-forward/merge into `snowflake/main`.
4. **`RULE-SYNC-04` — Strict `lfx_snowflake` Bundle Isolation & 6 Tracked Hook Points:** Keep 95%+ of Snowflake logic inside `src/lfx_snowflake/`, `frontend_patches/`, `sql/`, and `docs/`. Verify that the 6 tracked core hook points and invariant categories (`Input & Output`, `Prompt Template`, `Processing`, `Flow Control`, `LLM Selector`, `Calculator`, `Current Date`) remain intact.
5. **`RULE-SYNC-05` — Mandatory Pre-Commit & CI Sanitization Gate:** `python3 scripts/verify_public_repo_sanitization.py` must pass with **0 secrets** and **0 denylist matches** across all working-tree files and Git commit history before pushing.
6. **`RULE-SYNC-06` — Full Regression & Multi-Protocol Qualification Gate:** The full `pytest` suite (`tests/`) covering RBAC role switching, 3 Proxy Standard protocols (`Chat`, `Responses`, `Messages`), Ollama, AI Functions, and SQL trace replication must pass 100%.
7. **`RULE-SYNC-07` — Pre-Upgrade Quiesced Backup & Atomic Rollback:** Before deploying an updated image to `<COMPUTE_POOL>`, run `python3 scripts/backup_restore_lifecycle.py` (or create a point-in-time fork of `<POSTGRES_INSTANCE>`) so both container image and ORM schema can be rolled back atomically.

---

## 7. Verification & Sanitization Gate

Run the automated sanitization gate and test suite before any commit or push:

```bash
python3 scripts/verify_public_repo_sanitization.py
pytest
```
