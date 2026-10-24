/**
 * SnowflakeLangflow Preserved Concept & Controller Module (Wave 2 Round 4)
 *
 * PURPOSE:
 * Preserves 100% of the Section 1 (Governed Object Provisioning) and Section 2
 * (Snowflake Memories + 5 Canonical LangChain/Langflow Agent Memory Types + 3-Step
 * Light-Themed Memory Wizard) HTML templates, catalogs, and JavaScript controllers
 * that were previously rendered inside the Snowflake Langflow Hub modal.
 *
 * Starting in Wave 2 Round 4, the visible Snowflake Langflow Hub modal operates in
 * `data-hub-mode="context-only"` (rendering exclusively Section 0: Workflow-Personalized
 * Snowsight Hierarchical Context Selector + compact Automatic Snowflake Audit & Trace
 * Sync status badge), while all Object Provisioning and Snowflake Memory creation flows
 * are preserved intact here on `window.SNOWFLAKE_PRESERVED_PROVISIONING_AND_MEMORIES_CONCEPT`
 * for the upcoming dedicated Memory & Provisioning round.
 */
(function () {
  "use strict";

  const PRESERVED_OBJECT_FAMILIES = [
    "Table",
    "View",
    "Stage",
    "Snowflake Postgres",
  ];

  const PRESERVED_PROVISION_TARGETS_BY_CATEGORY = {
    table: [
      { value: "table:STANDARD", label: "Standard Table", subtype: "STANDARD" },
      { value: "table:HYBRID", label: "Hybrid Table", subtype: "HYBRID" },
      { value: "table:ICEBERG", label: "Iceberg Table", subtype: "ICEBERG" },
      { value: "table:INTERACTIVE", label: "Interactive Table", subtype: "INTERACTIVE" },
      { value: "table:DYNAMIC", label: "Dynamic Table", subtype: "DYNAMIC" },
    ],
    stage: [
      { value: "stage:INTERNAL", label: "Internal Stage (SNOWFLAKE_SSE + DIRECTORY)", subtype: "INTERNAL" },
      { value: "stage:EXTERNAL", label: "External Stage (URL + STORAGE_INTEGRATION)", subtype: "EXTERNAL" },
    ],
    view: [
      { value: "view:STANDARD", label: "View", subtype: "STANDARD" },
    ],
    postgres: [
      { value: "postgres:POSTGRES_INSTANCE", label: "Snowflake Postgres Instance", subtype: "POSTGRES_INSTANCE" },
    ],
    schema: [
      { value: "schema:SCHEMA", label: "Schema Only (Step 2)", subtype: "SCHEMA" },
    ],
    database: [
      { value: "database:DATABASE", label: "Database Only (Step 1)", subtype: "DATABASE" },
    ],
  };

  function provisionTargetOptionsForCategory(category) {
    const key = String(category || "table").toLowerCase();
    return PRESERVED_PROVISION_TARGETS_BY_CATEGORY[key] || PRESERVED_PROVISION_TARGETS_BY_CATEGORY.table;
  }

  const PRESERVED_AGENT_MEMORY_TYPES = [
    {
      id: "graph_state_checkpointer",
      label: "#1 Short-Term: Graph State & Checkpointer",
      category: "short_term",
      default_category: "postgres_non_vector",
      default_backend: "Snowflake Postgres (PostgresSaver)",
      recommended_snowflake_objects: [
        "Snowflake Postgres (PostgresSaver)",
        "Hybrid Table",
        "SnowflakeCheckpointer",
      ],
      needs_embedding_badge: "NO (Mai / Never)",
      how_to_feed_without_embedding:
        "SELECT state_blob, checkpoint_ns, step_id FROM <fqn> WHERE thread_id = :thread_id ORDER BY step_id DESC LIMIT 1",
      when_embedding_useful:
        "Mai: lo stato del grafo viene recuperato sempre per chiave primaria thread_id.",
      required_inputs: ["thread_id", "checkpoint_ns", "ttl_hours"],
      default_enable_embedding: false,
      description:
        "LangGraph state checkpoints and resumable execution state keyed by thread_id and checkpoint_ns.",
    },
    {
      id: "buffer_window_memory",
      label: "#2 Short-Term: Buffer / Window Memory (Storico Chat)",
      category: "short_term",
      default_category: "structured",
      default_backend: "Hybrid Table",
      recommended_snowflake_objects: [
        "Snowflake Postgres (Relational / JSONB)",
        "Hybrid Table",
        "Standard Table",
      ],
      needs_embedding_badge: "NO",
      how_to_feed_without_embedding:
        "SELECT sender, message_text, created_at FROM <fqn> WHERE session_id = :session_id ORDER BY created_at DESC LIMIT :window_k",
      when_embedding_useful:
        "Utile solo per recuperare messaggi detti 500+ turni fa fuori dalla finestra K.",
      required_inputs: ["session_id", "window_k", "retention_days"],
      default_enable_embedding: false,
      description:
        "Recent k conversational turns ordered chronologically by session_id without vector search overhead.",
    },
    {
      id: "summary_memory",
      label: "#3 Short-Term: Summary Memory (Riassunto)",
      category: "short_term",
      default_category: "structured",
      default_backend: "Hybrid Table",
      recommended_snowflake_objects: [
        "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG",
        "Standard Table + AI_COMPLETE",
        "Snowflake Postgres",
      ],
      needs_embedding_badge: "NO (Mai / Never)",
      how_to_feed_without_embedding:
        "SELECT running_summary, last_turn_id FROM <fqn> WHERE session_id = :session_id LIMIT 1 -- iniettato nel System Prompt",
      when_embedding_useful:
        "Mai: è un riassunto di 1 riga letto direttamente e aggiornato in background con AI_COMPLETE / AI_SUMMARIZE_AGG.",
      required_inputs: ["session_id", "summarizer_model", "max_summary_tokens"],
      default_enable_embedding: false,
      description:
        "Rolling single-row conversation summary updated via Snowflake Cortex AI_COMPLETE / AI_SUMMARIZE_AGG.",
    },
    {
      id: "user_profile_entity_memory",
      label: "#4 Long-Term Semantic: User Profile & Entity Memory",
      category: "long_term_semantic",
      default_category: "semi_structured",
      default_backend: "Standard Table (VARIANT)",
      recommended_snowflake_objects: [
        "Snowflake Postgres (PostgresStore)",
        "Hybrid Table (VARIANT)",
        "Standard Table (VARIANT)",
      ],
      needs_embedding_badge: "OPZIONALE (90% NO)",
      how_to_feed_without_embedding:
        "SELECT profile_json FROM <fqn> WHERE user_id = :user_id AND entity_namespace = :entity_namespace LIMIT 1 -- caricato a inizio sessione",
      when_embedding_useful:
        "Solo se l'utente accumula centinaia di fatti distinti e non entrano tutti nel prompt.",
      required_inputs: ["user_id", "entity_namespace", "schema_keys"],
      default_enable_embedding: false,
      description:
        "Persistent user preferences, facts, and structured entity attributes stored as JSONB or VARIANT.",
    },
    {
      id: "episodic_memory",
      label: "#5 Long-Term Semantic: Episodic Memory (Esperienze / Few-shot)",
      category: "long_term_semantic",
      default_category: "structured",
      default_backend: "Hybrid Table",
      recommended_snowflake_objects: [
        "Hybrid Table",
        "Standard Table",
        "Snowflake Postgres",
      ],
      needs_embedding_badge: "OPZIONALE",
      how_to_feed_without_embedding:
        "SELECT topic, tool_used, outcome_score, episode_text FROM <fqn> WHERE topic = :topic ORDER BY outcome_score DESC, created_at DESC LIMIT :few_shot_k",
      when_embedding_useful:
        "Utile quando i task passati non hanno categorie fisse e serve similarity search per trovare episodi simili.",
      required_inputs: ["topic_column", "tool_used_column", "outcome_score_threshold", "few_shot_k"],
      default_enable_embedding: false,
      description:
        "Past agent experiences and high-scoring few-shot examples retrieved by topic/tool or optional vector similarity.",
    },
  ];

  const PRESERVED_OFFICIAL_AI_EMBED_MODELS = [
    { model_id: "snowflake-arctic-embed-m-v1.5", dimension: 768, label: "snowflake-arctic-embed-m-v1.5 (768d)" },
    { model_id: "snowflake-arctic-embed-m", dimension: 768, label: "snowflake-arctic-embed-m (768d)" },
    { model_id: "e5-base-v2", dimension: 768, label: "e5-base-v2 (768d)" },
    { model_id: "nomic-embed-text", dimension: 768, label: "nomic-embed-text (768d - Ollama)" },
    { model_id: "snowflake-arctic-embed-l-v2.0", dimension: 1024, label: "snowflake-arctic-embed-l-v2.0 (1024d)" },
    { model_id: "snowflake-arctic-embed-l-v2.0-8k", dimension: 1024, label: "snowflake-arctic-embed-l-v2.0-8k (1024d)" },
    { model_id: "multilingual-e5-large", dimension: 1024, label: "multilingual-e5-large (1024d)" },
    { model_id: "voyage-multilingual-2", dimension: 1024, label: "voyage-multilingual-2 (1024d)" },
    { model_id: "nv-embed-qa-4", dimension: 1024, label: "nv-embed-qa-4 (1024d)" },
    { model_id: "voyage-multimodal-3", dimension: 1024, label: "voyage-multimodal-3 (1024d)" },
  ];

  const PRESERVED_EMBEDDING_PROVIDERS_CATALOG = {
    snowflake_cortex: {
      id: "snowflake_cortex",
      label: "Snowflake Cortex (AI_EMBED)",
      models: [
        { model_id: "snowflake-arctic-embed-m-v1.5", dimension: 768, label: "snowflake-arctic-embed-m-v1.5 (768d)" },
        { model_id: "e5-base-v2", dimension: 768, label: "e5-base-v2 (768d)" },
        { model_id: "snowflake-arctic-embed-l-v2.0", dimension: 1024, label: "snowflake-arctic-embed-l-v2.0 (1024d)" },
        { model_id: "nv-embed-qa-4", dimension: 1024, label: "nv-embed-qa-4 (1024d)" },
        { model_id: "multilingual-e5-large", dimension: 1024, label: "multilingual-e5-large (1024d)" },
      ],
    },
    openai: {
      id: "openai",
      label: "OpenAI (Langflow External)",
      models: [
        { model_id: "text-embedding-3-small", dimension: 1536, label: "text-embedding-3-small (1536d)" },
        { model_id: "text-embedding-3-large", dimension: 3072, label: "text-embedding-3-large (3072d)" },
        { model_id: "text-embedding-ada-002", dimension: 1536, label: "text-embedding-ada-002 (1536d)" },
      ],
    },
    cohere: {
      id: "cohere",
      label: "Cohere (Langflow External)",
      models: [
        { model_id: "embed-english-v3.0", dimension: 1024, label: "embed-english-v3.0 (1024d)" },
        { model_id: "embed-multilingual-v3.0", dimension: 1024, label: "embed-multilingual-v3.0 (1024d)" },
      ],
    },
    huggingface_ollama: {
      id: "huggingface_ollama",
      label: "HuggingFace / Ollama",
      models: [
        { model_id: "nomic-embed-text", dimension: 768, label: "nomic-embed-text (768d)" },
        { model_id: "bge-large-en-v1.5", dimension: 1024, label: "bge-large-en-v1.5 (1024d)" },
      ],
    },
    custom_endpoint: {
      id: "custom_endpoint",
      label: "Custom Endpoint (1..4096d)",
      models: [
        { model_id: "custom-embedding-endpoint", dimension: 768, label: "custom-embedding-endpoint (1..4096d)" },
      ],
    },
  };

  function buildPreservedSection1And2Html(sfState) {
    const state = sfState || {
      database: "SNOWFLAKE_LANGFLOW",
      schema: "CORE",
      active_role: "ACCOUNTADMIN",
    };
    const selectStyle =
      "background:#18181b;color:#fff;border:1px solid #3f3f46;padding:5px 8px;border-radius:6px;width:100%;min-width:0;box-sizing:border-box;font-size:11px;";
    const inputStyle =
      "background:#18181b;color:#fff;border:1px solid #3f3f46;padding:5px 8px;border-radius:6px;width:100%;min-width:0;box-sizing:border-box;font-size:11px;";
    const codeBoxStyle =
      "margin-top:8px;background-color:#09090b !important;color:#e4e4e7 !important;border:1px solid #3f3f46;padding:8px 10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:11px;line-height:1.45;white-space:pre-wrap;word-break:break-word;max-height:125px;overflow-y:auto;overflow-x:hidden;box-sizing:border-box;";
    const embedModelOptionsHtml = PRESERVED_OFFICIAL_AI_EMBED_MODELS.map(function (m) {
      return `<option value="${m.model_id}">${m.label}</option>`;
    }).join("");

    return `
      <!-- Preserved Section 1: Hierarchical Governed Object Provisioning (HUB-W2-03 & HUB-W2-04 & W2-R2-04B) -->
      <div id="sflf-sec1" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;">
        <h4 style="margin:0 0 4px 0;font-size:13px;color:#29B5E8;">1. Provision Snowflake Objects — Governed Object Provisioning (REQ-16 · W2-R2-04B · HUB-W2-03 / HUB-W2-04)</h4>
        <p style="margin:0 0 8px 0;font-size:11px;color:#a1a1aa;">4 Famiglie Infrastrutturali (<strong>Table, View, Stage, Snowflake Postgres</strong>) con ordine gerarchico <strong>1. Database -&gt; 2. Schema -&gt; 3. Oggetto</strong>. <code>Memory</code> si crea nella sezione <code>Memories (+ Snowflake)</code>.</p>
        <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:8px;margin-bottom:8px;">
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">1. Database</label>
            <select id="sflf-prov-db" style="${selectStyle}">
              <option value="${state.database}">${state.database}</option>
              <option value="__CREATE_NEW_DB__">+ Create New Database...</option>
            </select>
            <input id="sflf-prov-new-db" placeholder="New Database Name (e.g. ANALYTICS_DB)" style="${inputStyle};margin-top:4px;display:none;" />
          </div>
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">2. Schema (in Database)</label>
            <select id="sflf-prov-schema" style="${selectStyle}">
              <option value="${state.schema}">${state.schema}</option>
              <option value="__CREATE_NEW_SCHEMA__">+ Create New Schema...</option>
            </select>
            <input id="sflf-prov-new-schema" placeholder="New Schema Name (e.g. RAG_SCHEMA)" style="${inputStyle};margin-top:4px;display:none;" />
          </div>
        </div>
        <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:8px;margin-bottom:8px;">
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">3. Object Kind / Table or Stage Subtype</label>
            <select id="sflf-prov-kind" style="${selectStyle}">
              <option value="table:STANDARD">Standard Table</option>
              <option value="table:HYBRID">Hybrid Table</option>
              <option value="table:ICEBERG">Iceberg Table</option>
              <option value="table:INTERACTIVE">Interactive Table</option>
              <option value="table:DYNAMIC">Dynamic Table</option>
              <option value="stage:INTERNAL">Internal Stage (SNOWFLAKE_SSE + DIRECTORY)</option>
              <option value="stage:EXTERNAL">External Stage (URL + STORAGE_INTEGRATION)</option>
              <option value="view:STANDARD">View</option>
              <option value="postgres:POSTGRES_INSTANCE">Snowflake Postgres Instance</option>
              <option value="schema:SCHEMA">Schema Only (Step 2)</option>
              <option value="database:DATABASE">Database Only (Step 1)</option>
            </select>
          </div>
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">3b. Object Name</label>
            <input id="sflf-prov-name" placeholder="Object Name (e.g. DEMO_VECTORS)" value="DEMO_VECTORS" style="${inputStyle}" />
          </div>
        </div>
        <div id="sflf-prov-ext-stage-fields" style="display:none;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">
          <input id="sflf-prov-stage-url" placeholder="External URL (s3://bucket/path/)" value="s3://my-bucket/docs/" style="${inputStyle}" />
          <input id="sflf-prov-storage-integration" placeholder="STORAGE_INTEGRATION (e.g. S3_INT)" value="S3_DOCS_INT" style="${inputStyle}" />
        </div>
        <div id="sflf-prov-rbac-warning" style="display:none;margin-bottom:8px;padding:6px 8px;border-radius:6px;background:#7f1d1d;border:1px solid #ef4444;color:#fecaca;font-size:10px;font-weight:600;"></div>
        <div id="sflf-hub-no-fn-proc-notice" style="margin-bottom:8px;padding:6px 8px;border-radius:6px;background:#18181b;border:1px solid #3f3f46;color:#a1a1aa;font-size:10px;">
          <strong>Nota Governance (HUB-W2-04):</strong> La creazione di <code>FUNCTION</code> (UDF/UDTF) e <code>STORED PROCEDURE</code> è esclusa dallo Snowflake Langflow Hub. Utilizzare i componenti dedicati in <strong>LLM Operations -&gt; Stored Procedure / Function (UDF / UDTF)</strong>.
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <button id="sflf-btn-dryrun" style="background:#3f3f46;color:#fff;border:none;padding:6px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:500;">Dry-Run DDL Preview</button>
          <button id="sflf-btn-provision" style="background:#29B5E8;color:#fff;border:none;padding:6px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;">Execute on Snowflake</button>
        </div>
        <div id="sflf-prov-out" style="${codeBoxStyle}">Click "Dry-Run DDL Preview" to inspect the SQL DDL or "Execute on Snowflake" to provision with active role.</div>
      </div>

      <!-- Preserved Section 2: Snowflake Memories (+ Snowflake) 5 LangChain/Langflow Memory Types & Optional Embeddings (W2-R3-05 · W2-R2-13 · HUB-W2-05) -->
      <div id="sflf-sec2" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;border:1px solid rgba(41,181,232,0.45);">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:4px;">
          <h4 style="margin:0;font-size:13px;color:#29B5E8;">2. Snowflake Memories (+ Snowflake) — 5 LangChain/Langflow Memory Types (W2-R3-05 · W2-R2-13)</h4>
          <button type="button" id="sflf-open-light-memory-modal-btn" style="background:#29B5E8;color:#ffffff;border:none;padding:3px 8px;border-radius:5px;font-size:10px;font-weight:600;cursor:pointer;">+ Snowflake Memory Wizard</button>
        </div>
        <p style="margin:0 0 8px 0;font-size:11px;color:#a1a1aa;">Configura le 5 tipologie di <strong>Agent Memory LangChain/Langflow su Snowflake</strong> con lookup SQL diretto senza embedding (default) o embedding opzionale (<code>Snowflake AI_EMBED</code> / provider esterni).</p>
        <div style="margin-bottom:8px;">
          <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Agent Memory Type (+ Snowflake)</label>
          <select id="sflf-mem-agent-type" style="${selectStyle}">
            <option value="graph_state_checkpointer">#1 Short-Term: Graph State &amp; Checkpointer — Serve Embedding: NO (Mai)</option>
            <option value="buffer_window_memory">#2 Short-Term: Buffer / Window Memory (Storico Chat) — Serve Embedding: NO</option>
            <option value="summary_memory">#3 Short-Term: Summary Memory (Riassunto) — Serve Embedding: NO (Mai)</option>
            <option value="user_profile_entity_memory">#4 Long-Term Semantic: User Profile &amp; Entity Memory — Serve Embedding: OPZIONALE (90% NO)</option>
            <option value="episodic_memory">#5 Long-Term Semantic: Episodic Memory (Esperienze / Few-shot) — Serve Embedding: OPZIONALE</option>
            <option value="episodic_session">Legacy Alias: + Episodic / Session Memory</option>
            <option value="semantic_vector">Legacy Alias: + Semantic / Vector Memory</option>
            <option value="structured_long_term">Legacy Alias: + Structured / Semi-Structured Long-Term Memory</option>
          </select>
        </div>
        <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,180px),1fr));gap:8px;margin-bottom:8px;">
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Memory Category</label>
            <select id="sflf-mem-category" style="${selectStyle}">
              <option value="structured">Non-Vector: Structured Table (Standard/Hybrid/Interactive/Dynamic)</option>
              <option value="semi_structured">Non-Vector: Semi-Structured (VARIANT / OBJECT / ARRAY)</option>
              <option value="postgres_non_vector">Non-Vector: Snowflake Postgres (Relational / JSONB)</option>
              <option value="vector_table">Optional Vector: Snowflake Table (VECTOR)</option>
              <option value="postgres_pgvector">Optional Vector: Snowflake Postgres (pgvector)</option>
              <option value="cortex_search">Optional Vector: Cortex Search Service</option>
            </select>
          </div>
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Backend / Table Subtype</label>
            <select id="sflf-mem-backend" style="${selectStyle}">
              <option value="Hybrid Table">Hybrid Table</option>
              <option value="Standard Table">Standard Table</option>
              <option value="Snowflake Postgres (Relational / JSONB)">Snowflake Postgres (Relational / JSONB)</option>
              <option value="Standard Table (VECTOR)">Standard Table (VECTOR)</option>
            </select>
          </div>
        </div>
        <div id="sflf-mem-embed-row" style="display:none;grid-template-columns:1.4fr 0.6fr 0.6fr;gap:8px;margin-bottom:8px;">
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">AI_EMBED Model (Auto-Locks Dim)</label>
            <select id="sflf-mem-embed-model" style="${selectStyle}">
              ${embedModelOptionsHtml}
            </select>
          </div>
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">VECTOR Element</label>
            <select id="sflf-mem-vector-elem-type" style="${selectStyle}">
              <option value="FLOAT">FLOAT</option>
              <option value="INT">INT</option>
            </select>
          </div>
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Locked Dim</label>
            <input id="sflf-mem-vector-dim" value="768" readonly style="${inputStyle};opacity:0.75;cursor:not-allowed;" />
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr auto;gap:8px;align-items:end;margin-bottom:6px;">
          <div>
            <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Memory / Table Name</label>
            <input id="sflf-mem-name" placeholder="Memory Name" value="SNOWFLAKE_KB_MEM" style="${inputStyle}" />
          </div>
          <button id="sflf-btn-create-mem" style="background:#29B5E8;color:#fff;border:none;padding:6px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;height:28px;">Create Governed Memory</button>
        </div>
        <div id="sflf-mem-constraint-note" style="font-size:10px;color:#a1a1aa;margin-bottom:6px;"></div>
        <div id="sflf-mem-out" style="${codeBoxStyle}">Ready to create governed Snowflake memory under active role.</div>
      </div>
    `;
  }

  async function executePreservedProvisionRequest(payload) {
    const resp = await fetch("/api/v1/snowflake/provision", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return resp.json();
  }

  async function executePreservedMemoryCreationRequest(payload) {
    const resp = await fetch("/api/v1/snowflake/memories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    return resp.json();
  }

  function mountNewMemoryWizardModal(flowIdFromReact) {
    if (typeof window !== "undefined" && typeof window.openCreateSnowflakeMemoryModal === "function") {
      return window.openCreateSnowflakeMemoryModal(flowIdFromReact);
    }
    return null;
  }

  const preservedConceptBundle = {
    version: "0.4.6-w2r4-preserved",
    status: "preserved_for_next_memory_round",
    endpoints: {
      provision: "/api/v1/snowflake/provision",
      memories: "/api/v1/snowflake/memories",
    },
    OBJECT_FAMILIES: PRESERVED_OBJECT_FAMILIES,
    PROVISION_TARGETS_BY_CATEGORY: PRESERVED_PROVISION_TARGETS_BY_CATEGORY,
    AGENT_MEMORY_TYPES: PRESERVED_AGENT_MEMORY_TYPES,
    OFFICIAL_AI_EMBED_MODELS: PRESERVED_OFFICIAL_AI_EMBED_MODELS,
    EMBEDDING_PROVIDERS_CATALOG: PRESERVED_EMBEDDING_PROVIDERS_CATALOG,
    provisionTargetOptionsForCategory: provisionTargetOptionsForCategory,
    buildPreservedSection1And2Html: buildPreservedSection1And2Html,
    executePreservedProvisionRequest: executePreservedProvisionRequest,
    executePreservedMemoryCreationRequest: executePreservedMemoryCreationRequest,
    mountNewMemoryWizardModal: mountNewMemoryWizardModal,
  };

  if (typeof window !== "undefined") {
    window.SNOWFLAKE_PRESERVED_PROVISIONING_AND_MEMORIES_CONCEPT = preservedConceptBundle;
  }
  if (typeof module !== "undefined" && module.exports) {
    module.exports = preservedConceptBundle;
  }
})();
