/**
 * SnowflakeLangflow Live UI Integration Script (v0.4.0 — Wave 2 UI-W2-01 & HUB-W2-01..05)
 *
 * CRITICAL INVARIANT (React 18 Fiber Tree Integrity):
 * Never call `appendChild`, `insertBefore`, `removeChild`, or mutate `innerHTML` on ANY node
 * inside `<div id="root">`. React 18 owns `#root` exclusively. All native logos, links, and
 * i18n labels inside `#root` are compiled into `assets/index-*.js` at build time, while the
 * interactive Role Selector bar, dual-symbol loading overlay, and Snowflake Langflow Hub modal
 * are mounted directly on `document.body` outside `#root`.
 */
(function () {
  "use strict";

  // Guard Node.prototype.removeChild and Node.prototype.insertBefore so browser extensions,
  // page translators, or portal transitions can never trigger React 18 NotFoundError crashes.
  if (typeof Node === "function" && Node.prototype && !Node.prototype.__sflfPatched) {
    const _origRemoveChild = Node.prototype.removeChild;
    Node.prototype.removeChild = function (child) {
      if (child && child.parentNode !== this) {
        if (child.parentNode) {
          return _origRemoveChild.call(child.parentNode, child);
        }
        return child;
      }
      return _origRemoveChild.apply(this, arguments);
    };
    const _origInsertBefore = Node.prototype.insertBefore;
    Node.prototype.insertBefore = function (newNode, referenceNode) {
      if (referenceNode && referenceNode.parentNode !== this) {
        return this.appendChild(newNode);
      }
      return _origInsertBefore.apply(this, arguments);
    };
    Node.prototype.__sflfPatched = true;
  }

  // Guard window.fetch so any 1-argument fetch(url) or init without .headers never trips
  // Langflow's fetch-intercept hook (Cannot read properties of undefined (reading 'headers')).
  if (typeof window !== "undefined" && typeof window.fetch === "function" && !window.fetch.__sflfHeadersGuarded) {
    const _prevFetch = window.fetch;
    const _guardedFetch = function (input, init) {
      if (typeof input === "string" || (typeof URL === "function" && input instanceof URL)) {
        const safeInit = init && typeof init === "object" ? Object.assign({}, init) : {};
        if (!safeInit.headers) {
          safeInit.headers = {};
        }
        return _prevFetch.call(this, input, safeInit);
      }
      return _prevFetch.apply(this, arguments);
    };
    _guardedFetch.__sflfHeadersGuarded = true;
    window.fetch = _guardedFetch;
  }

  const FORK_GITHUB_URL = "https://github.com/fbattaglieri-snowflake/Native-Snowflake-Langflow";
  const SNOWFLAKE_DOCS_URL = "https://docs.snowflake.com/";
  const BROWSER_TAB_TITLE = "Snowflake Langflow";

  // Intercept window.open globally so any React button calling window.open("https://github.com/langflow-ai/...")
  // is redirected to the user's fork and any Discord/X popup is blocked.
  const _origWindowOpen = window.open;
  window.open = function (url, target, features) {
    if (typeof url === "string") {
      if (url.includes("github.com/langflow-ai/langflow")) {
        url = url.replace("https://github.com/langflow-ai/langflow", FORK_GITHUB_URL);
      }
      if (url.includes("discord.com") || url.includes("discord.gg") || url.includes("x.com/langflow")) {
        return null;
      }
    }
    return _origWindowOpen.call(window, url, target, features);
  };

  // Capture-phase click listener on document (never mutates React DOM nodes)
  document.addEventListener(
    "click",
    function (ev) {
      const target = ev.target && ev.target.closest ? ev.target.closest("a, button") : null;
      if (!target) return;
      const testId = target.getAttribute("data-testid") || "";
      const href = target.getAttribute("href") || "";
      if (
        testId === "empty_page_github_button" ||
        testId === "menu_github_button" ||
        href.includes("github.com/langflow-ai/langflow")
      ) {
        ev.preventDefault();
        ev.stopPropagation();
        _origWindowOpen.call(window, FORK_GITHUB_URL, "_blank", "noopener,noreferrer");
      }
    },
    true
  );

  const OFFICIAL_SNOWFLAKE_PATH =
    "M9.86 15.298l13.008 7.8a3.72 3.72 0 0 0 4.589-.601 4.01 4.01 0 0 0 1.227-2.908V3.956a3.81 3.81 0 0 0-1.861-3.42 3.81 3.81 0 0 0-3.893 0 3.81 3.81 0 0 0-1.861 3.42v8.896l-7.387-4.43a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.28 1.4 5.422m31.27 7.8l13.008-7.8c1.84-1.143 2.458-3.533 1.4-5.424a3.75 3.75 0 0 0-5.22-1.452l-7.3 4.37v-8.84a3.81 3.81 0 1 0-7.615 0v15.323a4.08 4.08 0 0 0 .494 2.367c.482.903 1.314 1.57 2.3 1.844a3.71 3.71 0 0 0 2.922-.4M29.552 31.97c.013-.25.108-.5.272-.68l1.52-1.58a1.06 1.06 0 0 1 .658-.282h.057a1.05 1.05 0 0 1 .656.282l1.52 1.58a1.12 1.12 0 0 1 .272.681v.06a1.13 1.13 0 0 1-.272.683l-1.52 1.58a1.04 1.04 0 0 1-.656.284h-.057c-.246-.014-.48-.115-.658-.284l-1.52-1.58a1.13 1.13 0 0 1-.272-.683zm-4.604-.65v1.364a1.54 1.54 0 0 0 .372.93l5.16 5.357a1.42 1.42 0 0 0 .895.386h1.312a1.42 1.42 0 0 0 .895-.386l5.16-5.357a1.54 1.54 0 0 0 .372-.93V31.32a1.54 1.54 0 0 0-.372-.93l-5.16-5.357a1.42 1.42 0 0 0-.895-.386h-1.312a1.42 1.42 0 0 0-.895.386L25.32 30.4a1.55 1.55 0 0 0-.372.93M3.13 27.62l7.365 4.417L3.13 36.45a4.06 4.06 0 0 0-1.399 5.424 3.75 3.75 0 0 0 2.3 1.844c.986.274 2.042.133 2.922-.392l13.008-7.8c1.2-.762 1.9-2.078 1.9-3.492a4.16 4.16 0 0 0-1.9-3.492l-13.008-7.8a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.278 1.4 5.422m38.995 4.442a4 4 0 0 0 1.91 3.477l13 7.8c.88.524 1.934.666 2.92.392s1.817-.94 2.3-1.843a4.05 4.05 0 0 0-1.4-5.424L53.5 32.038l7.365-4.417c1.84-1.143 2.457-3.53 1.4-5.422a3.74 3.74 0 0 0-2.3-1.844c-.987-.274-2.042-.134-2.92.4l-13 7.8a4 4 0 0 0-1.91 3.507M25.48 40.508a3.7 3.7 0 0 0-2.611.464l-13.008 7.8c-1.84 1.143-2.456 3.53-1.4 5.422.483.903 1.314 1.57 2.3 1.843a3.75 3.75 0 0 0 2.922-.392l7.387-4.43v8.83a3.81 3.81 0 1 0 7.614 0V44.4a3.91 3.91 0 0 0-3.205-3.903m28.66 8.276l-13.008-7.8a3.75 3.75 0 0 0-2.922-.392 3.74 3.74 0 0 0-2.3 1.843 4.09 4.09 0 0 0-.494 2.37v15.25a3.81 3.81 0 1 0 7.614 0V51.28l7.287 4.37a3.79 3.79 0 0 0 2.922.4c.986-.265 1.818-.94 2.3-1.844 1.057-1.9.44-4.28-1.4-5.422";

  function buildOfficialSnowflakeSvg(fill, sizePx) {
    const sz = sizePx || 20;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${sz}" height="${sz}" viewBox="0 0 64 64" fill="${fill}" style="display:inline-block;vertical-align:middle;flex-shrink:0;"><path d="${OFFICIAL_SNOWFLAKE_PATH}"/></svg>`;
  }

  const SNOWFLAKE_BLUE = "#29B5E8";
  const SNOWFLAKE_SVG_BLACK = buildOfficialSnowflakeSvg("#000000", 16);
  const SNOWFLAKE_SVG_COLOR = buildOfficialSnowflakeSvg("#29B5E8", 15);
  const SNOWFLAKE_SVG_BLUE = buildOfficialSnowflakeSvg("#29B5E8", 16);
  const SNOWFLAKE_FAVICON_DATA_URI =
    "data:image/svg+xml;utf8," +
    encodeURIComponent(
      `<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 64 64" fill="#29B5E8"><path d="${OFFICIAL_SNOWFLAKE_PATH}"/></svg>`
    );

  // W2-R3-05 & W2-R2-13: 5 Canonical LangChain/Langflow Agent Memory Types + Legacy Aliases
  const AGENT_MEMORY_TYPES = [
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

  const LEGACY_MEMORY_TYPE_ALIASES = {
    episodic_session: "buffer_window_memory",
    semantic_vector: "episodic_memory",
    structured_long_term: "user_profile_entity_memory",
  };

  function resolveCanonicalMemoryTypeSpec(typeId) {
    const normalized = LEGACY_MEMORY_TYPE_ALIASES[typeId] || typeId || "buffer_window_memory";
    return (
      AGENT_MEMORY_TYPES.find(function (t) {
        return t.id === normalized;
      }) || AGENT_MEMORY_TYPES[1]
    );
  }

  // Official AI_EMBED model catalog with locked vector dimensions (HUB-W2-05)
  const OFFICIAL_AI_EMBED_MODELS = [
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

  // W2-R3-05: Optional Embedding Provider Catalog (Snowflake Cortex AI_EMBED + External Providers)
  const EMBEDDING_PROVIDERS_CATALOG = {
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

  const MEMORY_BACKENDS_BY_CATEGORY = {
    vector_table: [
      { value: "Standard Table (VECTOR)", table_type: "STANDARD", is_vector: true },
      { value: "Hybrid Table (VECTOR)", table_type: "HYBRID", is_vector: true },
      { value: "Interactive Table (VECTOR)", table_type: "INTERACTIVE", is_vector: true },
      { value: "Dynamic Table (VECTOR)", table_type: "DYNAMIC", is_vector: true },
    ],
    postgres_pgvector: [
      { value: "Snowflake Postgres (pgvector)", table_type: "POSTGRES_PGVECTOR", is_vector: true },
    ],
    cortex_search: [
      { value: "Cortex Search Service", table_type: "CORTEX_SEARCH", is_vector: true },
    ],
    structured: [
      { value: "Hybrid Table", table_type: "HYBRID", is_vector: false },
      { value: "Standard Table", table_type: "STANDARD", is_vector: false },
      { value: "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG", table_type: "HYBRID", is_vector: false },
      { value: "Standard Table + AI_COMPLETE", table_type: "STANDARD", is_vector: false },
      { value: "SnowflakeCheckpointer", table_type: "HYBRID", is_vector: false },
      { value: "Interactive Table", table_type: "INTERACTIVE", is_vector: false },
      { value: "Dynamic Table", table_type: "DYNAMIC", is_vector: false },
    ],
    semi_structured: [
      { value: "Standard Table (VARIANT)", table_type: "STANDARD", semi_type: "VARIANT", is_vector: false },
      { value: "Hybrid Table (VARIANT)", table_type: "HYBRID", semi_type: "VARIANT", is_vector: false },
      { value: "Standard Table (OBJECT)", table_type: "STANDARD", semi_type: "OBJECT", is_vector: false },
      { value: "Standard Table (ARRAY)", table_type: "STANDARD", semi_type: "ARRAY", is_vector: false },
      { value: "Interactive Table (VARIANT)", table_type: "INTERACTIVE", semi_type: "VARIANT", is_vector: false },
      { value: "Dynamic Table (VARIANT)", table_type: "DYNAMIC", semi_type: "VARIANT", is_vector: false },
    ],
    postgres_non_vector: [
      { value: "Snowflake Postgres (PostgresSaver)", table_type: "POSTGRES_JSONB", is_vector: false },
      { value: "Snowflake Postgres (Relational / JSONB)", table_type: "POSTGRES_JSONB", is_vector: false },
      { value: "Snowflake Postgres (PostgresStore)", table_type: "POSTGRES_JSONB", is_vector: false },
      { value: "Snowflake Postgres", table_type: "POSTGRES_JSONB", is_vector: false },
    ],
  };

  let sfState = {
    active_role: "ACCOUNTADMIN",
    available_roles: [
      "ACCOUNTADMIN",
      "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
      "SNOWFLAKE_LANGFLOW_DEV_ROLE",
      "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
      "PUBLIC",
    ],
    context_epoch: 1,
    database: "SNOWFLAKE_LANGFLOW",
    schema: "CORE",
    stage_name: "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
    role_privileges: {
      can_create_database: true,
      can_create_schema: true,
      can_create_table: true,
      can_create_stage: true,
      can_create_view: true,
      can_create_function: true,
      can_create_procedure: true,
      can_create_cortex_search: true,
      can_create_postgres: true,
      can_create_object: true,
    },
    selected_context: {
      databases: ["SNOWFLAKE_LANGFLOW"],
      schemas_by_db: { SNOWFLAKE_LANGFLOW: ["CORE"] },
      full_databases: [],
      full_schemas: [],
      postgres_instances: [],
      postgres_tables_by_instance: {},
      inaccessible_objects: [],
      objects_by_db_schema: {
        "SNOWFLAKE_LANGFLOW.CORE": {
          tables: ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"],
          views: [],
          iceberg_tables: [],
          stages: ["SNOWFLAKE_LANGFLOW_STAGE"],
          cortex_search_services: [],
          semantic_views: [],
          semantic_models: [],
          cortex_agents: [],
          functions: [],
          procedures: [],
        },
      },
      visible_objects: {
        tables: ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"],
        views: [],
        iceberg_tables: [],
        stages: ["SNOWFLAKE_LANGFLOW_STAGE"],
        memories: ["SNOWFLAKE_KB_MEM"],
        cortex_search_services: [],
        semantic_views: [],
        semantic_models: [],
        cortex_agents: [],
        functions: [],
        procedures: [],
        postgres_instances: [],
      },
    },
    catalog: {
      databases: ["SNOWFLAKE_LANGFLOW"],
      schemas_by_db: { SNOWFLAKE_LANGFLOW: ["CORE", "PUBLIC"] },
      objects_by_db_schema: {
        "SNOWFLAKE_LANGFLOW.CORE": {
          tables: ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"],
          views: [],
          iceberg_tables: [],
          stages: ["SNOWFLAKE_LANGFLOW_STAGE"],
          cortex_search_services: [],
          semantic_views: [],
          semantic_models: [],
          cortex_agents: [],
          functions: [],
          procedures: [],
        },
      },
      tables: ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"],
      views: [],
      iceberg_tables: [],
      stages: ["SNOWFLAKE_LANGFLOW_STAGE"],
      memories: ["SNOWFLAKE_KB_MEM"],
      cortex_search_services: [],
      semantic_views: [],
      semantic_models: [],
      cortex_agents: [],
      functions: [],
      procedures: [],
      postgres_instances: [],
      postgres_tables_by_instance: {},
    },
  };

  window.SNOWFLAKE_LANGFLOW_CONTEXT = sfState;

  // W2-R2-14 & W2-R3-01..04: Inject official Snowflake Blue (#29B5E8) & fluid responsive CSS rules in document.head (outside #root)
  function ensureSnowflakeBlueBrandStyles() {
    if (!document.head || document.getElementById("sflf-brand-blue-style")) return;
    const styleEl = document.createElement("style");
    styleEl.id = "sflf-brand-blue-style";
    styleEl.textContent = `
      [data-testid="header_left_section_wrapper"],
      [data-testid="header_left_section_wrapper"] svg,
      [data-testid="header_left_section_wrapper"] span,
      [data-testid="snowflake-langflow-dual-loading-logo"],
      [data-testid="snowflake-langflow-dual-loading-logo"] svg,
      [data-testid="empty_page_title"],
      [data-testid="welcome-to-snowflake-langflow-title"],
      #root .loading-component,
      #root .loading-component *,
      [data-testid*="loading"],
      [data-testid*="loading"] *,
      [data-testid="loading_icon"] {
        color: #29B5E8 !important;
      }
      [data-testid="snowflake-langflow-dual-loading-logo"] svg,
      #root .loading-component svg,
      [data-testid*="loading"] svg,
      [data-testid="loading_icon"] svg {
        color: #29B5E8 !important;
        fill: #29B5E8 !important;
        stroke: #29B5E8 !important;
      }
      [data-testid="new-project-btn"],
      [data-testid="empty_page_create_first_flow_button"],
      button[data-testid="new-project-btn"],
      button[data-testid="empty_page_create_first_flow_button"] {
        background-color: #29B5E8 !important;
        border-color: #29B5E8 !important;
        color: #ffffff !important;
      }
      [data-testid="new-project-btn"]:hover,
      [data-testid="empty_page_create_first_flow_button"]:hover,
      button[data-testid="new-project-btn"]:hover,
      button[data-testid="empty_page_create_first_flow_button"]:hover {
        background-color: #0284c7 !important;
        border-color: #0284c7 !important;
        color: #ffffff !important;
      }
      .sflf-modal-dialog-surface {
        width: clamp(320px, 94vw, 980px) !important;
        max-width: 96vw !important;
        max-height: min(90dvh, 90vh) !important;
        display: flex !important;
        flex-direction: column !important;
        overflow: hidden !important;
        box-sizing: border-box !important;
      }
      .sflf-modal-scroll-body {
        flex: 1 1 auto !important;
        overflow-y: auto !important;
        overflow-x: hidden !important;
        min-height: 0 !important;
      }
      .sflf-responsive-grid {
        display: grid !important;
        grid-template-columns: repeat(auto-fit, minmax(min(100%, 280px), 1fr)) !important;
        gap: 12px !important;
      }
    `;
    document.head.appendChild(styleEl);
  }

  // W2-R3-02: Style '+ Create first flow' button in official Snowflake Blue (#29B5E8) without mutating React DOM tree
  function styleCreateFirstFlowButtons() {
    if (typeof document === "undefined" || !document.body) return;
    const buttons = document.querySelectorAll(
      '[data-testid="new-project-btn"], [data-testid="empty_page_create_first_flow_button"], button'
    );
    buttons.forEach(function (btn) {
      const tid = btn.getAttribute("data-testid") || "";
      const txt = (btn.textContent || "").trim();
      if (
        tid === "new-project-btn" ||
        tid === "empty_page_create_first_flow_button" ||
        txt === "+ Create first flow" ||
        txt === "Create first flow"
      ) {
        btn.style.setProperty("background-color", "#29B5E8", "important");
        btn.style.setProperty("border-color", "#29B5E8", "important");
        btn.style.setProperty("color", "#ffffff", "important");
      }
    });
  }

  // UI-W2-01: Idempotent browser tab title ("Snowflake Langflow") & Snowflake SVG favicon guard
  function ensureBrowserTabBranding() {
    if (document.title !== BROWSER_TAB_TITLE) {
      document.title = BROWSER_TAB_TITLE;
    }
    ensureSnowflakeBlueBrandStyles();
    if (!document.head) return;
    const iconLinks = document.head.querySelectorAll('link[rel*="icon"]');
    if (iconLinks.length === 0) {
      const link = document.createElement("link");
      link.rel = "icon";
      link.type = "image/svg+xml";
      link.href = SNOWFLAKE_FAVICON_DATA_URI;
      document.head.appendChild(link);
    } else {
      iconLinks.forEach(function (link) {
        if (link.getAttribute("href") !== SNOWFLAKE_FAVICON_DATA_URI) {
          link.setAttribute("type", "image/svg+xml");
          link.setAttribute("href", SNOWFLAKE_FAVICON_DATA_URI);
        }
      });
    }
  }

  function installHeadMutationObserver() {
    ensureBrowserTabBranding();
    if (typeof MutationObserver !== "function" || !document.head) return;
    const headObserver = new MutationObserver(function () {
      ensureBrowserTabBranding();
    });
    headObserver.observe(document.head, {
      childList: true,
      subtree: true,
      characterData: true,
      attributes: true,
      attributeFilter: ["href"],
    });
  }

  // UI-W2-01, W2-R2-14 & W2-R3-01: Clean Snowflake Blue (#29B5E8) dual-symbol loading overlay mounted on document.body (outside #root)
  // Shown ONLY while #root is still empty before React mounts (once React mounts, patched TTn/Og render the dual [Snowflake] [Langflow] symbols + Loading... in #29B5E8 directly without double overlap).
  let _loadingBodyObserverInstalled = false;
  function buildOfficialLangflowSvg(color, sizePx) {
    const c = color || "#29B5E8";
    const s = sizePx || 40;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${s}" height="${s}" viewBox="0 0 24 22" fill="${c}" style="width:${s}px;height:${s}px;color:${c};fill:${c};flex-shrink:0;" aria-hidden="true"><path d="M13.0486 0.462158C13.3589 0.462158 13.6565 0.586085 13.8759 0.806669L18.765 5.72232C18.9844 5.94291 19.282 6.06683 19.5923 6.06683H22.3486C22.9949 6.06683 23.5189 6.59362 23.5189 7.24344V9.97223C23.5189 10.6221 22.9949 11.1488 22.3486 11.1488H18.1997C17.8894 11.1488 17.5918 11.0249 17.3724 10.8043L12.4833 5.88868C12.2639 5.6681 11.9663 5.54417 11.656 5.54417H8.89971C8.25337 5.54417 7.72943 5.01738 7.72943 4.36756V1.63877C7.72943 0.988947 8.25337 0.462158 8.89971 0.462158H13.0486ZM5.31886 8.31618C5.62914 8.31618 5.92672 8.44011 6.14612 8.66069L11.0353 13.5763C11.2547 13.7969 11.5522 13.9209 11.8625 13.9209H14.6189C15.2652 13.9209 15.7891 14.4476 15.7891 15.0975V17.8263C15.7891 18.4761 15.2652 19.0029 14.6189 19.0029H10.47C10.1597 19.0029 9.8621 18.8789 9.64269 18.6584L4.75355 13.7427C4.53415 13.5221 4.23657 13.3982 3.92629 13.3982H1.17C0.523656 13.3982 0 12.8714 0 12.2216V9.49279C0 8.84297 0.523656 8.31618 1.17 8.31618H5.31886Z"/></svg>`;
  }

  function ensureDualSymbolLoadingOverlay() {
    const rootEl = document.getElementById("root");
    const isPreReactEmptyRoot = !rootEl || rootEl.children.length === 0;
    let overlay = document.getElementById("sflf-dual-loading-overlay");
    if (isPreReactEmptyRoot) {
      if (!overlay && document.body) {
        overlay = document.createElement("div");
        overlay.id = "sflf-dual-loading-overlay";
        overlay.setAttribute("data-testid", "snowflake_langflow_loading_dual_logo");
        overlay.style.cssText =
          "position:fixed;inset:0;z-index:99990;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;pointer-events:none;user-select:none;color:#29B5E8;font-family:Inter,sans-serif;font-weight:600;font-size:16px;";
        overlay.innerHTML = `<div style="display:inline-flex;align-items:center;justify-content:center;gap:14px;color:#29B5E8;">${buildOfficialSnowflakeSvg("#29B5E8", 40)}${buildOfficialLangflowSvg("#29B5E8", 40)}</div><span style="color:#29B5E8 !important;">Loading...</span>`;
        document.body.appendChild(overlay);
      } else if (overlay) {
        overlay.style.display = "flex";
      }
    } else if (overlay) {
      overlay.style.display = "none";
    }

    if (!_loadingBodyObserverInstalled && typeof MutationObserver === "function" && document.documentElement) {
      _loadingBodyObserverInstalled = true;
      const roObserver = new MutationObserver(function () {
        ensureSnowflakeBlueBrandStyles();
        styleCreateFirstFlowButtons();
        const rEl = document.getElementById("root");
        const emptyNow = !rEl || rEl.children.length === 0;
        const ov = document.getElementById("sflf-dual-loading-overlay");
        if (ov) {
          ov.style.display = emptyNow ? "flex" : "none";
        }
        ensureMemoriesPageSnowflakeButton();
      });
      roObserver.observe(document.documentElement, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["class", "style", "data-state", "aria-current"],
      });
    }
  }

  // W2-R3-06: Strict Memories View detection & modal dialog open check
  function isAnyModalDialogOpen() {
    if (typeof document === "undefined") return false;
    const hubModal = document.getElementById("sflf-hub-modal");
    if (hubModal && hubModal.style && hubModal.style.display && hubModal.style.display !== "none") {
      return true;
    }
    const memModal = document.getElementById("sflf-create-memory-modal");
    if (memModal && memModal.style && memModal.style.display && memModal.style.display !== "none") {
      return true;
    }
    const nativeDialog = document.querySelector(
      '[role="dialog"]:not([aria-hidden="true"]), [data-state="open"][role="dialog"], [data-radix-Collection-item][data-state="open"]'
    );
    return !!nativeDialog;
  }

  function isStrictMemoriesView() {
    if (typeof document === "undefined" || !document.body) return false;
    // Never treat the main Flows list (which has 'Search flows...') as the Memories view
    if (document.querySelector('input[placeholder*="Search flows" i]')) {
      return false;
    }
    const path = ((window.location && window.location.pathname) || "").toLowerCase();
    if (path.endsWith("/flows")) {
      return false;
    }
    if (path.includes("/memories")) {
      return true;
    }
    const hasSearchMemoriesInput = !!document.querySelector(
      'input[placeholder*="Search memories" i], [data-testid="search-memories-input"]'
    );
    if (hasSearchMemoriesInput) {
      return true;
    }
    const hasHeadingMemories = Array.from(document.querySelectorAll("aside h2, h1, h2, [data-testid='mainpage_title']")).some(
      function (el) {
        const t = (el.textContent || "").trim();
        return t === "Memories" || t === "Manage Memories";
      }
    );
    const hasActiveSidebarMemories = Array.from(
      document.querySelectorAll(
        '[aria-current="page"], [data-active="true"], [data-state="active"], .bg-muted, .font-semibold'
      )
    ).some(function (el) {
      return (el.textContent || "").trim() === "Memories";
    });
    const hasNoMemoriesEmptyState = Array.from(document.querySelectorAll("div, p, h3, span")).some(function (el) {
      const t = (el.textContent || "").trim();
      return t === "No memories" || t === "No memories found" || t === "No memory selected";
    });
    if (hasHeadingMemories && (hasNoMemoriesEmptyState || hasActiveSidebarMemories)) {
      return true;
    }
    if (hasActiveSidebarMemories && hasNoMemoriesEmptyState) {
      return true;
    }
    return false;
  }

  // W2-R2-13, W2-R2-14, W2-R3-05 & W2-R3-06: Companion `+ Snowflake` button in official #29B5E8
  // Strictly shown ONLY on the Memories view, placed inline next to `+ Create` with normal z-index (z-index: 10),
  // and hidden/inert whenever any modal dialog is open.
  function ensureMemoriesPageSnowflakeButton() {
    if (!document.body) return;
    const isMemoriesView = isStrictMemoriesView();
    const modalOpen = isAnyModalDialogOpen();
    const nativeReactBtn = document.querySelector(
      'aside button[data-testid="memories-plus-snowflake-btn"]:not(#sflf-memories-plus-snowflake-btn)'
    );
    let btn = document.getElementById("sflf-memories-plus-snowflake-btn");
    if (!btn) {
      btn = document.createElement("button");
      btn.id = "sflf-memories-plus-snowflake-btn";
      btn.type = "button";
      btn.setAttribute("data-testid", "memories-plus-snowflake-btn");
      btn.style.cssText =
        "position:fixed;top:72px;left:132px;z-index:10;display:none;align-items:center;gap:6px;background:#29B5E8;color:#ffffff;border:1px solid #29B5E8;border-radius:10px;padding:6px 12px;font-family:Inter,sans-serif;font-size:12px;font-weight:600;cursor:pointer;box-shadow:0 1px 3px rgba(41,181,232,0.28);white-space:nowrap;";
      btn.innerHTML = `${buildOfficialSnowflakeSvg("#ffffff", 14)}<span>+ Snowflake</span>`;
      btn.onclick = function () {
        openCreateSnowflakeMemoryModal();
      };
      document.body.appendChild(btn);
    }
    if (nativeReactBtn) {
      // React Too component already rendered the inline '+ Snowflake' button right next to '+ Create'
      btn.style.display = "none";
      btn.style.pointerEvents = "none";
      if (isMemoriesView) {
        ensureSnowflakeMemoriesSearchableCards();
      }
      return;
    }
    if (isMemoriesView && !modalOpen) {
      const createBtnCandidate = Array.from(document.querySelectorAll("aside button, button")).find(function (b) {
        if (b.id === "sflf-memories-plus-snowflake-btn") return false;
        const txt = (b.textContent || "").trim();
        return txt === "Create" || txt === "+ Create" || txt === "Add Memory" || txt === "Create Memory";
      });
      if (createBtnCandidate) {
        const r = createBtnCandidate.getBoundingClientRect();
        if (r && r.width > 0 && r.height > 0) {
          btn.style.top = Math.round(r.top) + "px";
          btn.style.left = Math.round(r.right + 8) + "px";
          btn.style.height = Math.round(r.height) + "px";
          btn.style.bottom = "auto";
          btn.style.right = "auto";
        }
      }
      btn.style.zIndex = "10";
      btn.style.pointerEvents = "auto";
      btn.style.display = "inline-flex";
      ensureSnowflakeMemoriesSearchableCards();
    } else {
      btn.style.display = "none";
      btn.style.pointerEvents = "none";
      const cardsGrid = document.getElementById("sflf-memories-cards-grid");
      if (cardsGrid && !isMemoriesView) {
        cardsGrid.style.display = "none";
      }
    }
  }

  // W2-R4-01: Per-Workflow (/flow/<flow_id>) Route & Scope Detection
  let _activeMemoryFlowId = null;
  let _lastObservedFlowId = null;

  function getActiveWorkflowFlowId() {
    const path = (typeof window !== "undefined" && window.location && window.location.pathname) || "";
    const hash = (typeof window !== "undefined" && window.location && window.location.hash) || "";
    const mPath = path.match(/\/flow\/([^/?#]+)/i);
    if (mPath && mPath[1]) return decodeURIComponent(mPath[1]);
    const mHash = hash.match(/\/flow\/([^/?#]+)/i);
    if (mHash && mHash[1]) return decodeURIComponent(mHash[1]);
    return null;
  }

  function extractCurrentFlowId() {
    const urlFlowId = getActiveWorkflowFlowId();
    if (urlFlowId) return urlFlowId;
    if (_activeMemoryFlowId) return _activeMemoryFlowId;
    return null;
  }
  window.getActiveWorkflowFlowId = getActiveWorkflowFlowId;
  window.extractCurrentFlowId = extractCurrentFlowId;

  // CTX-ARCH-05B, W2-R2-00, W2-R2-04A, W2-R2-14 & W2-R4-01:
  // 3-Level Live Canvas Node Auto-Sync (Backend updated_node_templates + React Fiber template mutation + Radix combobox DOM sync)
  function _resolveFieldCategoryAndOptions(fieldName, dbs, primaryDb, schemasForDb, primarySchema, vis, pgInstances, pgTablesMap, activeCtx) {
    const key = String(fieldName || "").toLowerCase();
    if (key === "database" || key.endsWith("_database") || key.includes("input-database") || key.includes("dropdown-database")) {
      return { options: dbs, fallbackValue: primaryDb, isDb: true };
    }
    if (key === "schema_name" || key === "schema" || key.includes("input-schema") || key.includes("dropdown-schema")) {
      return { options: schemasForDb, fallbackValue: primarySchema, isSchema: true };
    }
    if (key.includes("postgres_instance") || key === "instance_name") {
      const insts = Array.isArray(pgInstances) ? pgInstances : [];
      return { options: insts, fallbackValue: insts[0] || "" };
    }
    if (key.includes("iceberg_table") || key.includes("iceberg")) {
      const list = vis.iceberg_tables || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("semantic_view") || key.includes("semantic_model")) {
      const list = vis.semantic_views || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("search_service_name") || key.includes("search_service")) {
      const list = vis.cortex_search_services || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("agent_name") || key.includes("cortex_agent")) {
      const list = vis.cortex_agents || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("view_name") || (key.includes("view") && !key.includes("semantic"))) {
      const list = vis.views || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("stage_name") || key.includes("stage")) {
      const list = vis.stages || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("procedure")) {
      const list = vis.procedures || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("function")) {
      const list = vis.functions || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("table_name") || key.includes("table")) {
      const list = vis.tables || [];
      return { options: list, fallbackValue: list[0] || "" };
    }
    if (key.includes("role")) {
      const roles = (activeCtx && activeCtx.available_roles) || [(activeCtx && activeCtx.active_role) || "ACCOUNTADMIN"];
      return { options: roles, fallbackValue: (activeCtx && activeCtx.active_role) || "ACCOUNTADMIN" };
    }
    return null;
  }

  function _syncReactFiberNodeTemplate(nodeEl, dbs, primaryDb, schemasMap, schemasForDb, primarySchema, vis, pgInstances, pgTablesMap, activeCtx, updatedNodeTemplates) {
    if (!nodeEl || typeof nodeEl !== "object") return 0;
    let mutatedFields = 0;
    const keys = Object.keys(nodeEl);
    const fiberKey = keys.find(function (k) {
      return k.indexOf("__reactFiber$") === 0 || k.indexOf("__reactInternalInstance$") === 0;
    });
    const propsKey = keys.find(function (k) {
      return k.indexOf("__reactProps$") === 0;
    });

    const candidateTemplates = [];
    const seenTemplates = [];
    function collectTemplateFromProps(propsObj) {
      if (!propsObj || typeof propsObj !== "object") return;
      const dataObj = propsObj.data || (propsObj.node && propsObj.node.data);
      const tmpl =
        (dataObj && dataObj.node && dataObj.node.template) ||
        (propsObj.node && propsObj.node.template) ||
        propsObj.template;
      if (tmpl && typeof tmpl === "object" && seenTemplates.indexOf(tmpl) === -1) {
        seenTemplates.push(tmpl);
        candidateTemplates.push({
          template: tmpl,
          nodeId: (dataObj && dataObj.id) || propsObj.id || nodeEl.getAttribute("data-id") || "",
          nodeType: (dataObj && dataObj.type) || "",
        });
      }
    }

    if (propsKey && nodeEl[propsKey]) {
      collectTemplateFromProps(nodeEl[propsKey]);
    }
    if (fiberKey && nodeEl[fiberKey]) {
      let cur = nodeEl[fiberKey];
      let depth = 0;
      while (cur && depth < 16) {
        collectTemplateFromProps(cur.memoizedProps);
        collectTemplateFromProps(cur.pendingProps);
        cur = cur.return;
        depth += 1;
      }
    }

    candidateTemplates.forEach(function (entry) {
      const tmpl = entry.template;
      const nodeId = entry.nodeId;
      const backendNodePatch =
        updatedNodeTemplates && nodeId && updatedNodeTemplates[nodeId] ? updatedNodeTemplates[nodeId] : null;
      const isPostgresNode = Boolean(tmpl.postgres_instance && !tmpl.database);
      const nodeDb =
        (tmpl.database && tmpl.database.value && dbs.indexOf(tmpl.database.value) !== -1
          ? tmpl.database.value
          : primaryDb) || primaryDb;
      const nodeSchemas =
        (Array.isArray(schemasMap[nodeDb]) && schemasMap[nodeDb].length > 0 ? schemasMap[nodeDb] : schemasForDb) ||
        schemasForDb;
      const nodeSch =
        (tmpl.schema_name && tmpl.schema_name.value && nodeSchemas.indexOf(tmpl.schema_name.value) !== -1
          ? tmpl.schema_name.value
          : primarySchema) || primarySchema;

      Object.keys(tmpl).forEach(function (fieldKey) {
        const fieldObj = tmpl[fieldKey];
        if (!fieldObj || typeof fieldObj !== "object") return;
        if (backendNodePatch && backendNodePatch[fieldKey]) {
          if (Array.isArray(backendNodePatch[fieldKey].options)) {
            fieldObj.options = backendNodePatch[fieldKey].options.slice();
          }
          if (backendNodePatch[fieldKey].value !== undefined) {
            fieldObj.value = backendNodePatch[fieldKey].value;
          }
          mutatedFields += 1;
          return;
        }
        if (isPostgresNode && fieldKey === "table_name") {
          const chosenInst = (tmpl.postgres_instance && tmpl.postgres_instance.value) || pgInstances[0] || "";
          const pgTables = (pgTablesMap && chosenInst && pgTablesMap[chosenInst]) || [];
          fieldObj.options = pgTables.slice();
          if (pgTables.length > 0 && pgTables.indexOf(fieldObj.value) === -1) {
            fieldObj.value = pgTables[0];
          }
          mutatedFields += 1;
          return;
        }
        const resolved = _resolveFieldCategoryAndOptions(
          fieldKey,
          dbs,
          nodeDb,
          nodeSchemas,
          nodeSch,
          vis,
          pgInstances,
          pgTablesMap,
          activeCtx
        );
        if (!resolved) return;
        if (Array.isArray(fieldObj.options) || fieldObj.combobox || fieldObj.type === "str") {
          fieldObj.options = resolved.options.slice();
          if (resolved.options.length > 0 && resolved.options.indexOf(fieldObj.value) === -1) {
            fieldObj.value = resolved.fallbackValue;
          } else if (resolved.options.length === 0) {
            fieldObj.value = "";
          }
          mutatedFields += 1;
        }
      });
    });

    return mutatedFields;
  }

  function syncCanvasNodesWithSnowflakeContext(ctx) {
    const activeCtx = ctx || sfState;
    if (!activeCtx || typeof document === "undefined") return { syncedNodes: 0 };

    const activeFlowId = activeCtx.flow_id || getActiveWorkflowFlowId() || "global";
    const selCtx = activeCtx.selected_context || {};
    const dbs =
      Array.isArray(selCtx.databases) && selCtx.databases.length > 0
        ? selCtx.databases
        : [activeCtx.database || "SNOWFLAKE_LANGFLOW"];
    const primaryDb = dbs[0] || activeCtx.database || "SNOWFLAKE_LANGFLOW";
    const schemasMap = selCtx.schemas_by_db || (activeCtx.catalog && activeCtx.catalog.schemas_by_db) || {};
    const schemasForDb =
      Array.isArray(schemasMap[primaryDb]) && schemasMap[primaryDb].length > 0
        ? schemasMap[primaryDb]
        : [activeCtx.schema || "CORE"];
    const primarySchema = schemasForDb[0] || activeCtx.schema || "CORE";
    const vis = selCtx.visible_objects || activeCtx.catalog || {};
    const pgInstances =
      (Array.isArray(selCtx.postgres_instances) && selCtx.postgres_instances.length > 0
        ? selCtx.postgres_instances
        : Array.isArray(vis.postgres_instances) && vis.postgres_instances.length > 0
          ? vis.postgres_instances
          : (activeCtx.catalog && activeCtx.catalog.postgres_instances) || []) || [];
    const pgTablesMap =
      selCtx.postgres_tables_by_instance ||
      (activeCtx.catalog && activeCtx.catalog.postgres_tables_by_instance) ||
      {};
    const updatedNodeTemplates = activeCtx.updated_node_templates || {};

    const nodeEls = document.querySelectorAll('[data-testid^="rf__node-"], .react-flow__node');
    let syncedCount = 0;

    nodeEls.forEach(function (nodeEl) {
      nodeEl.setAttribute("data-snowflake-context-epoch", String(activeCtx.context_epoch || 1));
      nodeEl.setAttribute("data-snowflake-active-role", String(activeCtx.active_role || "ACCOUNTADMIN"));
      nodeEl.setAttribute("data-snowflake-database", primaryDb);
      nodeEl.setAttribute("data-snowflake-schema", primarySchema);
      nodeEl.setAttribute("data-snowflake-flow-id", String(activeFlowId));

      // Level 1 & 2: Sync in-memory React Fiber node.data.node.template options + value
      _syncReactFiberNodeTemplate(
        nodeEl,
        dbs,
        primaryDb,
        schemasMap,
        schemasForDb,
        primarySchema,
        vis,
        pgInstances,
        pgTablesMap,
        activeCtx,
        updatedNodeTemplates
      );

      // Level 3A: Update any native <select> fields inside the canvas node
      const selects = nodeEl.querySelectorAll("select");
      selects.forEach(function (sel) {
        const nameAttr = (sel.getAttribute("name") || sel.getAttribute("data-testid") || sel.id || "").toLowerCase();
        const resolved = _resolveFieldCategoryAndOptions(
          nameAttr,
          dbs,
          primaryDb,
          schemasForDb,
          primarySchema,
          vis,
          pgInstances,
          pgTablesMap,
          activeCtx
        );
        if (resolved) {
          populateSelectOptions(sel, resolved.options, resolved.fallbackValue);
          if (resolved.isDb || resolved.isSchema) {
            sel.dispatchEvent(new Event("change", { bubbles: true }));
          }
        }
      });

      // Level 3B: Update Radix UI Comboboxes (button[role="combobox"], [data-testid*="dropdown"])
      const comboboxes = nodeEl.querySelectorAll(
        'button[role="combobox"], [role="combobox"], [data-testid*="dropdown"], [data-testid^="value-dropdown-"]'
      );
      comboboxes.forEach(function (cbBtn) {
        const tid = (
          cbBtn.getAttribute("data-testid") ||
          cbBtn.getAttribute("name") ||
          cbBtn.getAttribute("aria-label") ||
          cbBtn.id ||
          ""
        ).toLowerCase();
        const wrapper = cbBtn.closest ? cbBtn.closest("[data-testid], . flex, div") : null;
        const wrapperTid = wrapper ? (wrapper.getAttribute("data-testid") || "").toLowerCase() : "";
        const lookupKey = tid || wrapperTid;
        const resolved = _resolveFieldCategoryAndOptions(
          lookupKey,
          dbs,
          primaryDb,
          schemasForDb,
          primarySchema,
          vis,
          pgInstances,
          pgTablesMap,
          activeCtx
        );
        if (!resolved) return;
        cbBtn.setAttribute("data-snowflake-options", JSON.stringify(resolved.options));
        cbBtn.setAttribute("data-snowflake-value", String(resolved.fallbackValue || ""));
        const labelSpan = cbBtn.querySelector("span") || cbBtn;
        const curText = (labelSpan.textContent || "").trim();
        if (
          resolved.options.length > 0 &&
          resolved.options.indexOf(curText) === -1 &&
          resolved.fallbackValue
        ) {
          labelSpan.textContent = resolved.fallbackValue;
        }
      });

      const inputs = nodeEl.querySelectorAll('input[type="text"], input:not([type])');
      inputs.forEach(function (inp) {
        const key = (inp.getAttribute("name") || inp.getAttribute("data-testid") || inp.id || "").toLowerCase();
        const nativeSetter =
          Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value") &&
          Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
        if (key === "database" || key.endsWith("_database") || key.includes("input-database")) {
          if (nativeSetter) nativeSetter.call(inp, primaryDb);
          else inp.value = primaryDb;
          inp.dispatchEvent(new Event("input", { bubbles: true }));
          inp.dispatchEvent(new Event("change", { bubbles: true }));
        } else if (key === "schema_name" || key === "schema" || key.includes("input-schema")) {
          if (nativeSetter) nativeSetter.call(inp, primarySchema);
          else inp.value = primarySchema;
          inp.dispatchEvent(new Event("input", { bubbles: true }));
          inp.dispatchEvent(new Event("change", { bubbles: true }));
        }
      });

      syncedCount += 1;
    });

    return {
      syncedNodes: syncedCount,
      flow_id: activeFlowId,
      database: primaryDb,
      schema: primarySchema,
      databases: dbs,
      schemas_by_db: schemasMap,
      postgres_instances: pgInstances,
      context_epoch: activeCtx.context_epoch || 1,
    };
  }

  window.syncCanvasNodesWithSnowflakeContext = syncCanvasNodesWithSnowflakeContext;
  window.addEventListener("snowflake:context-updated", function (ev) {
    syncCanvasNodesWithSnowflakeContext((ev && ev.detail) || sfState);
  });
  window.addEventListener("snowflake:context-changed", function (ev) {
    syncCanvasNodesWithSnowflakeContext((ev && ev.detail) || sfState);
  });

  function scanAll() {
    ensureBrowserTabBranding();
    styleCreateFirstFlowButtons();
    ensureDualSymbolLoadingOverlay();
    ensureMemoriesPageSnowflakeButton();
    alignTopbarControls();

    // CTX-ARCH-01 & CTX-ARCH-02: Per-workflow (/flow/<flow_id>) context lifecycle & [ ❄️ Context ] button visibility
    const currentFlowId = getActiveWorkflowFlowId();
    const hubBtn = document.getElementById("sflf-open-hub-btn");
    if (hubBtn) {
      // Outside /flow/<flow_id> (Home, Flows list, Memories, Flow Activity, Settings): hide [ ❄️ Context ]
      hubBtn.style.display = currentFlowId ? "inline-flex" : "none";
      hubBtn.setAttribute("data-flow-id", currentFlowId || "");
    }
    const pillEl = document.getElementById("sflf-topbar-context-pill");
    if (pillEl) {
      pillEl.style.display = "none";
    }
    if (currentFlowId !== _lastObservedFlowId) {
      _lastObservedFlowId = currentFlowId;
      sfState.flow_id = currentFlowId;
      updateTopbarWidgets();
      if (currentFlowId) {
        fetchSnowflakeContext(currentFlowId);
      }
    } else if (currentFlowId && typeof document !== "undefined") {
      const unsyncedNode = document.querySelector(
        `.react-flow__node:not([data-snowflake-flow-id="${currentFlowId}"]), [data-testid^="rf__node-"]:not([data-snowflake-flow-id="${currentFlowId}"])`
      );
      if (unsyncedNode) {
        syncCanvasNodesWithSnowflakeContext(sfState);
      }
    }
  }

  function computeEffectivePrivileges() {
    const role = (sfState.active_role || "").toUpperCase();
    const isReadOnlyRole =
      role === "PUBLIC" || role.includes("VIEWER") || role.includes("READONLY") || role.includes("READ_ONLY");
    const serverPrivs = sfState.role_privileges || {};
    const canCreateDb =
      typeof serverPrivs.can_create_database === "boolean"
        ? serverPrivs.can_create_database
        : !isReadOnlyRole && (role === "ACCOUNTADMIN" || role === "SYSADMIN" || role.includes("ADMIN"));
    const canCreateSchema =
      typeof serverPrivs.can_create_schema === "boolean" ? serverPrivs.can_create_schema : !isReadOnlyRole;
    const canCreateTable =
      typeof serverPrivs.can_create_table === "boolean" ? serverPrivs.can_create_table : !isReadOnlyRole;
    const canCreateStage =
      typeof serverPrivs.can_create_stage === "boolean" ? serverPrivs.can_create_stage : !isReadOnlyRole;
    const canCreateView =
      typeof serverPrivs.can_create_view === "boolean" ? serverPrivs.can_create_view : !isReadOnlyRole;
    const canCreatePostgres =
      typeof serverPrivs.can_create_postgres === "boolean"
        ? serverPrivs.can_create_postgres
        : !isReadOnlyRole && (role === "ACCOUNTADMIN" || role.includes("ADMIN"));
    const canCreateObject =
      typeof serverPrivs.can_create_object === "boolean" ? serverPrivs.can_create_object : !isReadOnlyRole;

    return {
      can_create_database: canCreateDb,
      can_create_schema: canCreateSchema,
      can_create_table: canCreateTable,
      can_create_stage: canCreateStage,
      can_create_view: canCreateView,
      can_create_postgres: canCreatePostgres,
      can_create_object: canCreateObject,
    };
  }

  function showDenialOrStatusBanner(kind, code, message) {
    let banner = document.getElementById("sflf-global-banner");
    if (!banner) {
      banner = document.createElement("div");
      banner.id = "sflf-global-banner";
      banner.style.cssText =
        "position:fixed;top:54px;right:18px;z-index:99999;max-width:520px;padding:12px 16px;border-radius:8px;font-family:Inter,sans-serif;font-size:13px;box-shadow:0 8px 24px rgba(0,0,0,0.35);display:flex;align-items:center;gap:10px;";
      document.body.appendChild(banner);
    }
    const isError = kind === "error";
    banner.style.background = isError ? "#7f1d1d" : "#064e3b";
    banner.style.border = isError ? "1px solid #ef4444" : "1px solid #10b981";
    banner.style.color = "#ffffff";
    banner.innerHTML = `<div style="flex:1;"><strong>[${code}]</strong> ${message}</div><button id="sflf-banner-close" style="background:transparent;border:none;color:#fff;cursor:pointer;font-weight:bold;">✕</button>`;
    banner.style.display = "flex";
    const closeBtn = document.getElementById("sflf-banner-close");
    if (closeBtn) {
      closeBtn.onclick = function () {
        banner.style.display = "none";
      };
    }
    setTimeout(function () {
      if (banner) banner.style.display = "none";
    }, 8000);
  }

  async function fetchSnowflakeContext(flowIdOverride) {
    try {
      const effectiveFlowId =
        flowIdOverride !== undefined && flowIdOverride !== null
          ? flowIdOverride
          : getActiveWorkflowFlowId();
      const ctxUrl = effectiveFlowId
        ? `/api/v1/snowflake/context?flow_id=${encodeURIComponent(effectiveFlowId)}`
        : "/api/v1/snowflake/context";
      const headers = { Accept: "application/json" };
      if (effectiveFlowId) {
        headers["X-Snowflake-Flow-Id"] = String(effectiveFlowId);
      }
      const resp = await fetch(ctxUrl, { headers: headers });
      if (!resp.ok) return;
      const data = await resp.json();
      sfState = Object.assign(sfState, data);
      if (effectiveFlowId) {
        sfState.flow_id = effectiveFlowId;
      }
      window.SNOWFLAKE_LANGFLOW_CONTEXT = sfState;
      updateTopbarWidgets();
      await fetchSnowflakeCatalog(sfState.database, sfState.schema);
      refreshHubModalPrivilegeAndContextViews();
      syncCanvasNodesWithSnowflakeContext(sfState);
    } catch (_e) {
      // Keep default state until backend route responds
    }
  }

  async function fetchSnowflakeCatalog(database, schema, databasesList, schemasByDbMap) {
    try {
      const dbParam = encodeURIComponent(database || sfState.database || "SNOWFLAKE_LANGFLOW");
      let schParam = encodeURIComponent(schema || sfState.schema || "CORE");
      if (Array.isArray(databasesList) && databasesList.length > 0) {
        schParam += `&databases=${encodeURIComponent(databasesList.join(","))}`;
      }
      if (schemasByDbMap && typeof schemasByDbMap === "object" && Object.keys(schemasByDbMap).length > 0) {
        schParam += `&schemas_by_db=${encodeURIComponent(JSON.stringify(schemasByDbMap))}`;
      }
      const resp = await fetch(`/api/v1/snowflake/catalog?database=${dbParam}&schema=${schParam}`, {
        headers: { Accept: "application/json" },
      });
      if (!resp.ok) return null;
      const data = await resp.json();
      sfState.catalog = Object.assign({}, sfState.catalog || {}, data);
      if (Array.isArray(data.databases) && data.databases.length > 0) {
        sfState.catalog.databases = data.databases;
      }
      if (data.schemas_by_db && typeof data.schemas_by_db === "object") {
        sfState.catalog.schemas_by_db = Object.assign({}, sfState.catalog.schemas_by_db || {}, data.schemas_by_db);
      }
      if (Array.isArray(data.schemas) && data.schemas.length > 0) {
        sfState.catalog.schemas_by_db = sfState.catalog.schemas_by_db || {};
        sfState.catalog.schemas_by_db[database || sfState.database] = data.schemas;
      }
      if (data.objects_by_db_schema && typeof data.objects_by_db_schema === "object") {
        sfState.catalog.objects_by_db_schema = Object.assign(
          {},
          sfState.catalog.objects_by_db_schema || {},
          data.objects_by_db_schema
        );
      }
      if (Array.isArray(data.objects)) {
        const tables = [];
        const views = [];
        data.objects.forEach(function (obj) {
          const name = typeof obj === "string" ? obj : obj.name || obj.OBJECT_NAME || "";
          const kind = (typeof obj === "object" && (obj.kind || obj.type || "")).toUpperCase();
          if (!name) return;
          if (kind.includes("VIEW")) {
            views.push(name);
          } else {
            tables.push(name);
          }
        });
        if (!Array.isArray(data.tables) && tables.length > 0) sfState.catalog.tables = tables;
        if (!Array.isArray(data.views) && views.length > 0) sfState.catalog.views = views;
      }
      if (Array.isArray(data.stages)) {
        sfState.catalog.stages = data.stages
          .map(function (s) {
            return typeof s === "string" ? s : s.name || s.STAGE_NAME || "";
          })
          .filter(Boolean);
      }
      return sfState.catalog;
    } catch (_e) {
      return null;
    }
  }

  async function persistSnowflakeContextSelection(payload, silentBanner) {
    try {
      const effectiveFlowId = (payload && payload.flow_id) || getActiveWorkflowFlowId() || sfState.flow_id || null;
      const bodyPayload = Object.assign({}, payload || {});
      if (effectiveFlowId) {
        bodyPayload.flow_id = effectiveFlowId;
        if (bodyPayload.selected_context && typeof bodyPayload.selected_context === "object") {
          bodyPayload.selected_context.flow_id = effectiveFlowId;
        }
      }
      const headers = { "Content-Type": "application/json" };
      if (effectiveFlowId) {
        headers["X-Snowflake-Flow-Id"] = String(effectiveFlowId);
      }
      const resp = await fetch("/api/v1/snowflake/context", {
        method: "POST",
        headers: headers,
        body: JSON.stringify(bodyPayload),
      });
      const data = await resp.json();
      if (!resp.ok || data.ok === false) {
        showDenialOrStatusBanner(
          "error",
          data.error_code || "CONTEXT_UPDATE_FAILED",
          data.message || "Impossibile aggiornare il contesto primario Snowflake."
        );
        return;
      }
      sfState = Object.assign(sfState, data);
      if (effectiveFlowId) {
        sfState.flow_id = effectiveFlowId;
      }
      sfState.database = bodyPayload.database || sfState.database;
      sfState.schema = bodyPayload.schema || sfState.schema;
      sfState.selected_context = data.selected_context || bodyPayload.selected_context || sfState.selected_context;
      window.SNOWFLAKE_LANGFLOW_CONTEXT = sfState;
      updateTopbarWidgets();
      refreshHubModalPrivilegeAndContextViews();
      syncCanvasNodesWithSnowflakeContext(sfState);
      window.dispatchEvent(new CustomEvent("snowflake:context-updated", { detail: sfState }));
      window.dispatchEvent(new CustomEvent("snowflake:context-changed", { detail: sfState }));
      if (!silentBanner) {
        showDenialOrStatusBanner(
          "ok",
          "CONTEXT_UPDATED",
          `Contesto attivo salvato${effectiveFlowId ? " per workflow " + effectiveFlowId : ""}: ${sfState.database}.${sfState.schema} (Role: ${sfState.active_role})`
        );
      }
    } catch (err) {
      showDenialOrStatusBanner("error", "TECHNICAL_EXECUTION_ERROR", String(err));
    }
  }

  async function switchSnowflakeRole(newRole) {
    try {
      const resp = await fetch("/api/v1/snowflake/role", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_role: newRole }),
      });
      const data = await resp.json();
      if (!resp.ok || !data.ok) {
        showDenialOrStatusBanner(
          "error",
          data.error_code || "AUTHZ_ROLE_DENIED",
          data.message || `Il ruolo attivo (${sfState.active_role}) non consente questa operazione.`
        );
        updateTopbarWidgets();
        return;
      }
      sfState.active_role = data.active_role;
      sfState.context_epoch = data.context_epoch;
      if (data.role_privileges) {
        sfState.role_privileges = data.role_privileges;
      }
      await fetchSnowflakeContext();
      updateTopbarWidgets();
      refreshHubModalPrivilegeAndContextViews();
      syncCanvasNodesWithSnowflakeContext(sfState);
      window.dispatchEvent(new CustomEvent("snowflake:context-updated", { detail: sfState }));
      window.dispatchEvent(new CustomEvent("snowflake:context-changed", { detail: sfState }));
      showDenialOrStatusBanner(
        "ok",
        "ROLE_SWITCHED",
        `Ruolo attivo: ${sfState.active_role} (epoch #${sfState.context_epoch})`
      );
    } catch (err) {
      showDenialOrStatusBanner("error", "TECHNICAL_EXECUTION_ERROR", String(err));
    }
  }

  // HUB-W2-01: Topbar right alignment dynamically left of GitHub and Docs
  function alignTopbarControls() {
    const ctrl = document.getElementById("sflf-body-topbar-controls");
    if (!ctrl) return;
    const rightWrapper = document.querySelector('[data-testid="header_right_section_wrapper"]');
    if (rightWrapper) {
      const rect = rightWrapper.getBoundingClientRect();
      if (rect && rect.left > 0) {
        const rightPx = Math.max(224, Math.round(window.innerWidth - rect.left + 14));
        ctrl.style.right = rightPx + "px";
        return;
      }
    }
    ctrl.style.right = "228px";
  }

  // Mount Role Selector & single per-flow [ ❄️ Context ] button directly on document.body (OUTSIDE #root)
  // CTX-ARCH-01, CTX-ARCH-02 & W2-R2-14: Colored in official Snowflake Blue (#29B5E8)
  function mountFixedTopbarControls() {
    if (document.getElementById("sflf-body-topbar-controls")) return;
    const initialFlowId = getActiveWorkflowFlowId();
    const ctrl = document.createElement("div");
    ctrl.id = "sflf-body-topbar-controls";
    ctrl.style.cssText =
      "position:fixed;top:10px;right:228px;z-index:25;display:inline-flex;align-items:center;gap:6px;flex-wrap:nowrap;max-width:min(55vw, 460px);overflow:hidden;text-overflow:ellipsis;background:transparent;color:#29B5E8;padding:1px 4px;font-family:Inter,sans-serif;font-size:11px;height:28px;";
    ctrl.innerHTML = `
      <span style="display:inline-flex;align-items:center;color:#29B5E8;flex-shrink:0;">${buildOfficialSnowflakeSvg("#29B5E8", 14)}</span>
      <label style="font-weight:600;color:#29B5E8;font-size:11px;flex-shrink:0;">Role:</label>
      <select id="sflf-role-select" style="background:#ffffff;color:#09090b;border:1px solid rgba(41,181,232,0.55);border-radius:5px;padding:1px 6px;font-size:11px;font-weight:600;cursor:pointer;max-width:142px;height:24px;text-overflow:ellipsis;"></select>
      <button id="sflf-open-hub-btn" data-testid="sflf-context-btn" data-flow-id="${initialFlowId || ""}" style="display:${
        initialFlowId ? "inline-flex" : "none"
      };align-items:center;gap:4px;background:#29B5E8;color:#ffffff;border:none;border-radius:5px;padding:2px 8px;height:24px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;flex-shrink:0;">${buildOfficialSnowflakeSvg(
        "#ffffff",
        12
      )}<span>[ ❄️ Context ]</span></button>
    `;
    document.body.appendChild(ctrl);
    alignTopbarControls();
    window.addEventListener("resize", alignTopbarControls);
    setInterval(scanAll, 800);

    const sel = document.getElementById("sflf-role-select");
    if (sel) {
      sel.onchange = function (ev) {
        switchSnowflakeRole(ev.target.value);
      };
    }
    const hubBtn = document.getElementById("sflf-open-hub-btn");
    if (hubBtn) {
      hubBtn.onclick = function () {
        openSnowflakeHubModal();
      };
    }
    updateTopbarWidgets();
  }

  function updateTopbarWidgets() {
    const selectEl = document.getElementById("sflf-role-select");
    const epochEl = document.getElementById("sflf-epoch-badge");
    const hubBtn = document.getElementById("sflf-open-hub-btn");
    const pillEl = document.getElementById("sflf-topbar-context-pill");
    const activeFlowId = getActiveWorkflowFlowId();
    if (selectEl) {
      selectEl.innerHTML = "";
      (sfState.available_roles || []).forEach(function (r) {
        const opt = document.createElement("option");
        opt.value = r;
        opt.textContent = r;
        if (r === sfState.active_role) opt.selected = true;
        selectEl.appendChild(opt);
      });
    }
    if (epochEl) {
      epochEl.textContent = `epoch #${sfState.context_epoch}`;
    }
    if (pillEl) {
      pillEl.style.display = "none";
    }
    if (hubBtn) {
      // CTX-ARCH-01 & CTX-ARCH-02: Hide [ ❄️ Context ] outside /flow/<flow_id>; show single [ ❄️ Context ] inside /flow/<flow_id>
      if (!activeFlowId) {
        hubBtn.style.display = "none";
        hubBtn.setAttribute("data-flow-id", "");
      } else {
        hubBtn.style.display = "inline-flex";
        hubBtn.setAttribute("data-flow-id", activeFlowId);
        const selDbs =
          (sfState.selected_context &&
            Array.isArray(sfState.selected_context.databases) &&
            sfState.selected_context.databases.length > 0 &&
            sfState.selected_context.databases) || [sfState.database || "SNOWFLAKE_LANGFLOW"];
        const primaryDb = selDbs[0] || sfState.database || "SNOWFLAKE_LANGFLOW";
        const schemasByDb = (sfState.selected_context && sfState.selected_context.schemas_by_db) || {};
        const primarySch =
          (Array.isArray(schemasByDb[primaryDb]) && schemasByDb[primaryDb][0]) || sfState.schema || "CORE";
        const primaryPair = `${primaryDb}.${primarySch}`;
        const summarySuffix = selDbs.length > 1 ? `${primaryPair} (+${selDbs.length - 1} DB)` : primaryPair;
        hubBtn.title = `Workflow ${activeFlowId} Context: ${summarySuffix} (Role: ${sfState.active_role})`;
      }
    }
  }

  function populateSelectOptions(selectEl, items, selectedVal, extraOption) {
    if (!selectEl) return;
    selectEl.innerHTML = "";
    const list = Array.isArray(items) && items.length > 0 ? items : selectedVal ? [selectedVal] : [];
    list.forEach(function (item) {
      const val = typeof item === "string" ? item : item.value || item.name || "";
      const label = typeof item === "string" ? item : item.label || item.name || val;
      if (!val) return;
      const opt = document.createElement("option");
      opt.value = val;
      opt.textContent = label;
      if (val === selectedVal) opt.selected = true;
      selectEl.appendChild(opt);
    });
    if (extraOption) {
      const opt = document.createElement("option");
      opt.value = extraOption.value;
      opt.textContent = extraOption.label;
      if (extraOption.disabled) opt.disabled = true;
      if (extraOption.value === selectedVal) opt.selected = true;
      selectEl.appendChild(opt);
    }
  }

  // CTX-ARCH-05: Block 1 (Snowflake Hierarchy) contains ONLY the 9 schema-scoped object categories
  // (memories belong to Files & Knowledge -> Memory Base; postgres_instances belong to Block 2 Account-Level)
  const TREE_OBJECT_CATEGORIES = [
    { key: "tables", label: "Tables" },
    { key: "views", label: "Views" },
    { key: "iceberg_tables", label: "Iceberg Tables" },
    { key: "stages", label: "Stages" },
    { key: "cortex_search_services", label: "Cortex Search" },
    { key: "semantic_views", label: "Cortex Analyst" },
    { key: "cortex_agents", label: "Cortex Agents" },
    { key: "functions", label: "Functions (UDF/UDTF)" },
    { key: "procedures", label: "Procedures" },
  ];

  let treeUiState = {
    searchQuery: "",
    expandedDbs: { SNOWFLAKE_LANGFLOW: true },
    expandedSchemas: { "SNOWFLAKE_LANGFLOW.CORE": true },
    expandedCategories: {},
    expandedPostgresInstances: {},
  };

  function getCatalogSchemasForDb(db, cat) {
    const catalog = cat || sfState.catalog || {};
    const schemasMap = catalog.schemas_by_db || {};
    const sel = sfState.selected_context || {};
    const selSchemas = (sel.schemas_by_db && sel.schemas_by_db[db]) || [];
    const baseSchemas =
      Array.isArray(schemasMap[db]) && schemasMap[db].length > 0
        ? schemasMap[db].slice()
        : db === sfState.database && Array.isArray(catalog.schemas) && catalog.schemas.length > 0
          ? catalog.schemas.slice()
          : ["CORE", "PUBLIC"];
    selSchemas.forEach(function (s) {
      if (s && baseSchemas.indexOf(s) === -1) baseSchemas.push(s);
    });
    return baseSchemas;
  }

  function isObjectInaccessibleForActiveRole(pairKey, catKey, objName, cat) {
    const inaccList =
      (sfState.inaccessible_objects && Array.isArray(sfState.inaccessible_objects)
        ? sfState.inaccessible_objects
        : sfState.selected_context && Array.isArray(sfState.selected_context.inaccessible_objects)
          ? sfState.selected_context.inaccessible_objects
          : []) || [];
    const fqn = pairKey ? `${pairKey}.${objName}` : String(objName || "");
    if (
      inaccList.some(function (entry) {
        if (typeof entry === "string") return entry === fqn || entry === objName || entry === pairKey;
        if (entry && typeof entry === "object") {
          return (
            entry.fqn === fqn ||
            (entry.pair === pairKey && entry.category === catKey && entry.name === objName) ||
            (entry.category === catKey && entry.name === objName)
          );
        }
        return false;
      })
    ) {
      return true;
    }
    const catalog = cat || sfState.catalog || {};
    if (catKey === "postgres_instances") {
      const catPg = Array.isArray(catalog.postgres_instances) ? catalog.postgres_instances : [];
      return catPg.length > 0 && catPg.indexOf(objName) === -1;
    }
    const objByPair = catalog.objects_by_db_schema || {};
    if (objByPair[pairKey] && Array.isArray(objByPair[pairKey][catKey])) {
      return objByPair[pairKey][catKey].indexOf(objName) === -1;
    }
    return false;
  }

  function getCatalogObjectsForPairCategory(pairKey, catKey, cat) {
    const catalog = cat || sfState.catalog || {};
    const objByPair = catalog.objects_by_db_schema || {};
    const bucket = objByPair[pairKey] || (pairKey === `${sfState.database}.${sfState.schema}` ? catalog : {});
    const baseList = Array.isArray(bucket[catKey]) ? bucket[catKey].slice() : [];
    const sel = sfState.selected_context || {};
    const storedList =
      (sel.selected_objects && sel.selected_objects[pairKey] && sel.selected_objects[pairKey][catKey]) || [];
    if (Array.isArray(storedList)) {
      storedList.forEach(function (item) {
        if (item && baseList.indexOf(item) === -1) {
          baseList.push(item);
        }
      });
    }
    return baseList;
  }

  function ensureSelectedContextStructure() {
    sfState.selected_context = sfState.selected_context || {};
    const sel = sfState.selected_context;
    if (!Array.isArray(sel.databases)) {
      sel.databases = [sfState.database || "SNOWFLAKE_LANGFLOW"];
    }
    sel.schemas_by_db = sel.schemas_by_db || {};
    sel.full_databases = Array.isArray(sel.full_databases) ? sel.full_databases : [];
    sel.full_schemas = Array.isArray(sel.full_schemas) ? sel.full_schemas : [];
    sel.postgres_instances = Array.isArray(sel.postgres_instances)
      ? sel.postgres_instances
      : Array.isArray(sel.visible_objects && sel.visible_objects.postgres_instances)
        ? sel.visible_objects.postgres_instances.slice()
        : [];
    sel.postgres_tables_by_instance = sel.postgres_tables_by_instance || {};
    sel.objects_by_db_schema = sel.objects_by_db_schema || {};
    sel.selected_objects = sel.selected_objects || {};
    sel.visible_objects = sel.visible_objects || {};
    return sel;
  }

  function getSelectedObjectsForPairCategory(pairKey, catKey, cat) {
    const sel = ensureSelectedContextStructure();
    const allObjs = getCatalogObjectsForPairCategory(pairKey, catKey, cat);
    const parts = pairKey.split(".");
    const db = parts[0];
    const sch = parts[1];
    if (sel.selected_objects[pairKey] === "*") {
      return allObjs.slice();
    }
    if (sel.selected_objects[pairKey] && Array.isArray(sel.selected_objects[pairKey][catKey])) {
      return sel.selected_objects[pairKey][catKey].filter(function (o) {
        return allObjs.indexOf(o) !== -1;
      });
    }
    if (sel.full_databases.indexOf(db) !== -1 || sel.full_schemas.indexOf(pairKey) !== -1) {
      return allObjs.slice();
    }
    const schList = Array.isArray(sel.schemas_by_db[db]) ? sel.schemas_by_db[db] : [];
    const schemaImplicitlySelected = sel.databases.indexOf(db) !== -1 && schList.indexOf(sch) !== -1;
    return schemaImplicitlySelected ? allObjs.slice() : [];
  }

  function computeCategoryTriState(pairKey, catKey, cat) {
    const sel = ensureSelectedContextStructure();
    const parts = pairKey.split(".");
    const db = parts[0];
    const sch = parts[1];
    const isWildcard =
      sel.full_databases.indexOf(db) !== -1 ||
      sel.full_schemas.indexOf(pairKey) !== -1 ||
      sel.selected_objects[pairKey] === "*";
    const allObjs = getCatalogObjectsForPairCategory(pairKey, catKey, cat);
    const selObjs = getSelectedObjectsForPairCategory(pairKey, catKey, cat);
    if (allObjs.length === 0) {
      const schSelected =
        isWildcard || (Array.isArray(sel.schemas_by_db[db]) && sel.schemas_by_db[db].indexOf(sch) !== -1);
      return { checked: schSelected, indeterminate: false, selectedCount: 0, totalCount: 0 };
    }
    const checked = selObjs.length === allObjs.length && allObjs.length > 0;
    const indeterminate = selObjs.length > 0 && selObjs.length < allObjs.length;
    return {
      checked: checked,
      indeterminate: indeterminate,
      selectedCount: selObjs.length,
      totalCount: allObjs.length,
    };
  }

  function computeSchemaTriState(db, sch, cat) {
    const pairKey = `${db}.${sch}`;
    const sel = ensureSelectedContextStructure();
    if (
      (sel.full_schemas.indexOf(pairKey) !== -1 || sel.full_databases.indexOf(db) !== -1) &&
      (!sel.selected_objects[pairKey] || sel.selected_objects[pairKey] === "*")
    ) {
      return { checked: true, indeterminate: false };
    }
    const schSelectedInList =
      sel.full_schemas.indexOf(pairKey) !== -1 ||
      sel.full_databases.indexOf(db) !== -1 ||
      (Array.isArray(sel.schemas_by_db[db]) && sel.schemas_by_db[db].indexOf(sch) !== -1);
    let totalItems = 0;
    let selectedItems = 0;
    let anyIndeterminate = false;
    TREE_OBJECT_CATEGORIES.forEach(function (c) {
      const st = computeCategoryTriState(pairKey, c.key, cat);
      totalItems += st.totalCount;
      selectedItems += st.selectedCount;
      if (st.indeterminate) anyIndeterminate = true;
    });
    if (totalItems === 0) {
      return { checked: schSelectedInList, indeterminate: false };
    }
    const checked = selectedItems === totalItems && totalItems > 0 && !anyIndeterminate;
    const indeterminate = (selectedItems > 0 && selectedItems < totalItems) || anyIndeterminate;
    return { checked: checked, indeterminate: indeterminate };
  }

  function computeDatabaseTriState(db, cat) {
    const sel = ensureSelectedContextStructure();
    if (sel.full_databases.indexOf(db) !== -1) {
      return { checked: true, indeterminate: false };
    }
    const schemas = getCatalogSchemasForDb(db, cat);
    let checkedSchemas = 0;
    let indeterminateSchemas = 0;
    schemas.forEach(function (sch) {
      const st = computeSchemaTriState(db, sch, cat);
      if (st.checked) checkedSchemas += 1;
      else if (st.indeterminate) indeterminateSchemas += 1;
    });
    const checked = schemas.length > 0 && checkedSchemas === schemas.length && indeterminateSchemas === 0;
    const indeterminate =
      indeterminateSchemas > 0 || (checkedSchemas > 0 && checkedSchemas < schemas.length);
    return { checked: checked, indeterminate: indeterminate };
  }

  function setSchemaAllObjectsSelection(db, sch, shouldSelect, cat) {
    const sel = ensureSelectedContextStructure();
    const pairKey = `${db}.${sch}`;
    const fsIdx = sel.full_schemas.indexOf(pairKey);
    if (shouldSelect && fsIdx === -1) {
      sel.full_schemas.push(pairKey);
    } else if (!shouldSelect && fsIdx !== -1) {
      sel.full_schemas.splice(fsIdx, 1);
    }
    if (!shouldSelect) {
      const fdIdx = sel.full_databases.indexOf(db);
      if (fdIdx !== -1) sel.full_databases.splice(fdIdx, 1);
    }
    sel.selected_objects[pairKey] = sel.selected_objects[pairKey] && typeof sel.selected_objects[pairKey] === "object"
      ? sel.selected_objects[pairKey]
      : {};
    TREE_OBJECT_CATEGORIES.forEach(function (c) {
      const allObjs = getCatalogObjectsForPairCategory(pairKey, c.key, cat);
      sel.selected_objects[pairKey][c.key] = shouldSelect ? allObjs.slice() : [];
    });
    sel.schemas_by_db[db] = Array.isArray(sel.schemas_by_db[db]) ? sel.schemas_by_db[db] : [];
    const idx = sel.schemas_by_db[db].indexOf(sch);
    if (shouldSelect && idx === -1) {
      sel.schemas_by_db[db].push(sch);
    } else if (!shouldSelect && idx !== -1) {
      sel.schemas_by_db[db].splice(idx, 1);
    }
  }

  function syncDatabaseListFromSchemas(db) {
    const sel = ensureSelectedContextStructure();
    const dbState = computeDatabaseTriState(db, sfState.catalog);
    const hasAnySelection =
      dbState.checked ||
      dbState.indeterminate ||
      sel.full_databases.indexOf(db) !== -1 ||
      (Array.isArray(sel.schemas_by_db[db]) && sel.schemas_by_db[db].length > 0);
    const idx = sel.databases.indexOf(db);
    if (hasAnySelection && idx === -1) {
      sel.databases.push(db);
    } else if (!hasAnySelection && idx !== -1 && sel.databases.length > 1) {
      sel.databases.splice(idx, 1);
    }
  }

  // CTX-ARCH-05: Zero-lag (<5ms) cascade when checking a closed or open Database
  function toggleTreeDatabaseSelection(db, shouldSelect) {
    const sel = ensureSelectedContextStructure();
    const fdIdx = sel.full_databases.indexOf(db);
    if (shouldSelect && fdIdx === -1) {
      sel.full_databases.push(db);
    } else if (!shouldSelect && fdIdx !== -1) {
      sel.full_databases.splice(fdIdx, 1);
    }
    const schemas = getCatalogSchemasForDb(db, sfState.catalog);
    schemas.forEach(function (sch) {
      setSchemaAllObjectsSelection(db, sch, shouldSelect, sfState.catalog);
    });
    if (shouldSelect && sel.full_databases.indexOf(db) === -1) {
      sel.full_databases.push(db);
    }
    const idx = sel.databases.indexOf(db);
    if (shouldSelect && idx === -1) {
      sel.databases.push(db);
    } else if (!shouldSelect && idx !== -1 && sel.databases.length > 1) {
      sel.databases.splice(idx, 1);
    }
    if (shouldSelect) {
      sfState.database = db;
      if (schemas[0]) sfState.schema = schemas[0];
    }
  }

  // CTX-ARCH-05: Zero-lag (<5ms) cascade when checking a closed or open Schema
  function toggleTreeSchemaSelection(db, sch, shouldSelect) {
    setSchemaAllObjectsSelection(db, sch, shouldSelect, sfState.catalog);
    syncDatabaseListFromSchemas(db);
    if (shouldSelect) {
      sfState.database = db;
      sfState.schema = sch;
    }
  }

  function toggleTreeCategorySelection(pairKey, catKey, shouldSelect) {
    const sel = ensureSelectedContextStructure();
    const parts = pairKey.split(".");
    const db = parts[0];
    const sch = parts[1];
    if (!shouldSelect) {
      const fdIdx = sel.full_databases.indexOf(db);
      if (fdIdx !== -1) sel.full_databases.splice(fdIdx, 1);
      const fsIdx = sel.full_schemas.indexOf(pairKey);
      if (fsIdx !== -1) sel.full_schemas.splice(fsIdx, 1);
    }
    sel.selected_objects[pairKey] =
      sel.selected_objects[pairKey] && typeof sel.selected_objects[pairKey] === "object"
        ? sel.selected_objects[pairKey]
        : {};
    const allObjs = getCatalogObjectsForPairCategory(pairKey, catKey, sfState.catalog);
    sel.selected_objects[pairKey][catKey] = shouldSelect ? allObjs.slice() : [];
    const schSt = computeSchemaTriState(db, sch, sfState.catalog);
    sel.schemas_by_db[db] = Array.isArray(sel.schemas_by_db[db]) ? sel.schemas_by_db[db] : [];
    const sIdx = sel.schemas_by_db[db].indexOf(sch);
    if ((schSt.checked || schSt.indeterminate) && sIdx === -1) {
      sel.schemas_by_db[db].push(sch);
    } else if (!schSt.checked && !schSt.indeterminate && sIdx !== -1) {
      sel.schemas_by_db[db].splice(sIdx, 1);
    }
    syncDatabaseListFromSchemas(db);
  }

  function toggleTreeObjectSelection(pairKey, catKey, objName, shouldSelect) {
    const sel = ensureSelectedContextStructure();
    const parts = pairKey.split(".");
    const db = parts[0];
    const sch = parts[1];
    if (!shouldSelect) {
      const fdIdx = sel.full_databases.indexOf(db);
      if (fdIdx !== -1) sel.full_databases.splice(fdIdx, 1);
      const fsIdx = sel.full_schemas.indexOf(pairKey);
      if (fsIdx !== -1) sel.full_schemas.splice(fsIdx, 1);
    }
    sel.selected_objects[pairKey] =
      sel.selected_objects[pairKey] && typeof sel.selected_objects[pairKey] === "object"
        ? sel.selected_objects[pairKey]
        : {};
    const current = getSelectedObjectsForPairCategory(pairKey, catKey, sfState.catalog).slice();
    const idx = current.indexOf(objName);
    if (shouldSelect && idx === -1) {
      current.push(objName);
    } else if (!shouldSelect && idx !== -1) {
      current.splice(idx, 1);
    }
    sel.selected_objects[pairKey][catKey] = current;
    const schSt = computeSchemaTriState(db, sch, sfState.catalog);
    sel.schemas_by_db[db] = Array.isArray(sel.schemas_by_db[db]) ? sel.schemas_by_db[db] : [];
    const sIdx = sel.schemas_by_db[db].indexOf(sch);
    if ((schSt.checked || schSt.indeterminate) && sIdx === -1) {
      sel.schemas_by_db[db].push(sch);
    } else if (!schSt.checked && !schSt.indeterminate && sIdx !== -1) {
      sel.schemas_by_db[db].splice(sIdx, 1);
    }
    syncDatabaseListFromSchemas(db);
  }

  function togglePostgresInstanceSelection(instName, shouldSelect) {
    const sel = ensureSelectedContextStructure();
    const idx = sel.postgres_instances.indexOf(instName);
    if (shouldSelect && idx === -1) {
      sel.postgres_instances.push(instName);
    } else if (!shouldSelect && idx !== -1) {
      sel.postgres_instances.splice(idx, 1);
    }
    const cat = sfState.catalog || {};
    const allTbls = (cat.postgres_tables_by_instance && cat.postgres_tables_by_instance[instName]) || [];
    sel.postgres_tables_by_instance[instName] = shouldSelect ? allTbls.slice() : [];
  }

  function togglePostgresTableSelection(instName, tblName, shouldSelect) {
    const sel = ensureSelectedContextStructure();
    const cur = Array.isArray(sel.postgres_tables_by_instance[instName])
      ? sel.postgres_tables_by_instance[instName].slice()
      : [];
    const idx = cur.indexOf(tblName);
    if (shouldSelect && idx === -1) cur.push(tblName);
    else if (!shouldSelect && idx !== -1) cur.splice(idx, 1);
    sel.postgres_tables_by_instance[instName] = cur;
    if (shouldSelect && sel.postgres_instances.indexOf(instName) === -1) {
      sel.postgres_instances.push(instName);
    }
  }

  async function lazyLoadSchemasForDatabase(db, forceRefresh) {
    if (
      !forceRefresh &&
      sfState.catalog &&
      sfState.catalog.schemas_by_db &&
      Array.isArray(sfState.catalog.schemas_by_db[db]) &&
      sfState.catalog.schemas_by_db[db].length > 0
    ) {
      return sfState.catalog.schemas_by_db[db];
    }
    try {
      const resp = await fetch(`/api/v1/snowflake/schemas?database=${encodeURIComponent(db)}`, {
        headers: { Accept: "application/json" },
      });
      if (resp.ok) {
        const data = await resp.json();
        if (Array.isArray(data.schemas) && data.schemas.length > 0) {
          sfState.catalog = sfState.catalog || {};
          sfState.catalog.schemas_by_db = sfState.catalog.schemas_by_db || {};
          sfState.catalog.schemas_by_db[db] = data.schemas;
          return data.schemas;
        }
      }
    } catch (_e) {
      // Return cached/default schemas on transient error
    }
    return getCatalogSchemasForDb(db, sfState.catalog);
  }

  async function lazyLoadObjectsForSchema(db, sch, forceRefresh) {
    const pairKey = `${db}.${sch}`;
    if (
      !forceRefresh &&
      sfState.catalog &&
      sfState.catalog.objects_by_db_schema &&
      sfState.catalog.objects_by_db_schema[pairKey]
    ) {
      return sfState.catalog.objects_by_db_schema[pairKey];
    }
    try {
      const resp = await fetch(
        `/api/v1/snowflake/objects?database=${encodeURIComponent(db)}&schema=${encodeURIComponent(sch)}`,
        { headers: { Accept: "application/json" } }
      );
      if (resp.ok) {
        const data = await resp.json();
        sfState.catalog = sfState.catalog || {};
        sfState.catalog.objects_by_db_schema = sfState.catalog.objects_by_db_schema || {};
        if (data.objects_by_db_schema && data.objects_by_db_schema[pairKey]) {
          sfState.catalog.objects_by_db_schema[pairKey] = data.objects_by_db_schema[pairKey];
        } else if (data.objects && typeof data.objects === "object" && !Array.isArray(data.objects)) {
          sfState.catalog.objects_by_db_schema[pairKey] = data.objects;
        }
        return sfState.catalog.objects_by_db_schema[pairKey];
      }
    } catch (_e) {
      // Return cached/default objects on transient error
    }
    return (sfState.catalog && sfState.catalog.objects_by_db_schema && sfState.catalog.objects_by_db_schema[pairKey]) || {};
  }

  function applyDomIndeterminateStates(containerEl) {
    if (!containerEl) return;
    containerEl.querySelectorAll('input[type="checkbox"][data-indeterminate]').forEach(function (cb) {
      cb.indeterminate = cb.getAttribute("data-indeterminate") === "true";
    });
  }

  function buildCurrentContextSelectionPayload() {
    const sel = ensureSelectedContextStructure();
    const checkedDbs =
      Array.isArray(sel.databases) && sel.databases.length > 0
        ? sel.databases.slice()
        : Array.isArray(sel.full_databases) && sel.full_databases.length > 0
          ? sel.full_databases.slice()
          : [sfState.database || "SNOWFLAKE_LANGFLOW"];
    const db = checkedDbs[0] || sfState.database || "SNOWFLAKE_LANGFLOW";
    const schemasByDb = Object.assign({}, sel.schemas_by_db || {});
    const schForPrimaryDb =
      Array.isArray(schemasByDb[db]) && schemasByDb[db].length > 0
        ? schemasByDb[db][0]
        : sfState.schema || "CORE";
    const sch = schForPrimaryDb || "CORE";
    sfState.database = db;
    sfState.schema = sch;

    const cat = sfState.catalog || {};
    const selectedObjects = Object.assign({}, sel.selected_objects || {});
    const fullDatabases = Array.isArray(sel.full_databases) ? sel.full_databases.slice() : [];
    const fullSchemas = Array.isArray(sel.full_schemas) ? sel.full_schemas.slice() : [];
    const pgInstances = Array.isArray(sel.postgres_instances) ? sel.postgres_instances.slice() : [];
    const pgTablesByInst = Object.assign({}, sel.postgres_tables_by_instance || {});

    const objectsByDbSchema = {};
    const allPairKeys = Object.keys(cat.objects_by_db_schema || {}).concat(Object.keys(selectedObjects));
    allPairKeys.forEach(function (pairKey) {
      if (objectsByDbSchema[pairKey]) return;
      if (selectedObjects[pairKey] && typeof selectedObjects[pairKey] === "object") {
        objectsByDbSchema[pairKey] = Object.assign({}, selectedObjects[pairKey]);
      } else if (cat.objects_by_db_schema && cat.objects_by_db_schema[pairKey]) {
        objectsByDbSchema[pairKey] = Object.assign({}, cat.objects_by_db_schema[pairKey]);
      }
    });
    const visibleObjects = {};
    const hasExplicitSelection =
      Object.keys(selectedObjects).length > 0 || fullDatabases.length > 0 || fullSchemas.length > 0;
    TREE_OBJECT_CATEGORIES.forEach(function (c) {
      const merged = [];
      Object.keys(selectedObjects).forEach(function (pairKey) {
        const list = (selectedObjects[pairKey] && selectedObjects[pairKey][c.key]) || [];
        if (Array.isArray(list)) {
          list.forEach(function (item) {
            if (merged.indexOf(item) === -1) merged.push(item);
          });
        }
      });
      visibleObjects[c.key] = hasExplicitSelection ? merged : cat[c.key] || [];
    });
    visibleObjects.postgres_instances = pgInstances.slice();
    sel.visible_objects = visibleObjects;
    sel.objects_by_db_schema = objectsByDbSchema;
    const activeFlowId = getActiveWorkflowFlowId() || sfState.flow_id || null;
    return {
      flow_id: activeFlowId,
      database: db,
      schema: sch,
      databases: checkedDbs,
      schemas_by_db: schemasByDb,
      full_databases: fullDatabases,
      full_schemas: fullSchemas,
      postgres_instances: pgInstances,
      postgres_tables_by_instance: pgTablesByInst,
      objects_by_db_schema: objectsByDbSchema,
      selected_objects: selectedObjects,
      visible_objects: visibleObjects,
      selected_context: {
        flow_id: activeFlowId,
        database: db,
        schema: sch,
        databases: checkedDbs,
        schemas_by_db: schemasByDb,
        full_databases: fullDatabases,
        full_schemas: fullSchemas,
        postgres_instances: pgInstances,
        postgres_tables_by_instance: pgTablesByInst,
        objects_by_db_schema: objectsByDbSchema,
        selected_objects: selectedObjects,
        visible_objects: visibleObjects,
      },
    };
  }

  let _debouncedPersistTimer = null;
  function scheduleDebouncedContextPersist() {
    if (_debouncedPersistTimer) {
      clearTimeout(_debouncedPersistTimer);
    }
    _debouncedPersistTimer = setTimeout(function () {
      _debouncedPersistTimer = null;
      const payload = buildCurrentContextSelectionPayload();
      persistSnowflakeContextSelection(payload, true);
    }, 180);
  }

  function updateObjectSummaryBarInPlace(cat) {
    const catalog = cat || sfState.catalog || {};
    const sel = ensureSelectedContextStructure();
    const hasExplicitSelection = Object.keys(sel.selected_objects || {}).length > 0;
    const countForCat = function (catKey, fallbackArr) {
      if (catKey === "postgres_instances") {
        return Array.isArray(sel.postgres_instances) ? sel.postgres_instances.length : 0;
      }
      if (hasExplicitSelection) {
        const merged = [];
        Object.keys(sel.selected_objects).forEach(function (pairKey) {
          const list = (sel.selected_objects[pairKey] && sel.selected_objects[pairKey][catKey]) || [];
          if (Array.isArray(list)) {
            list.forEach(function (item) {
              if (merged.indexOf(item) === -1) merged.push(item);
            });
          }
        });
        return merged.length;
      }
      return Array.isArray(fallbackArr) ? fallbackArr.length : 0;
    };

    const coneSummaryEl = document.getElementById("sflf-cone-summary-bar");
    if (coneSummaryEl) {
      const activeFlowId = getActiveWorkflowFlowId() || sfState.flow_id || "global";
      const dbsCount = Array.isArray(sel.databases) ? sel.databases.length : 1;
      const pgCount = Array.isArray(sel.postgres_instances) ? sel.postgres_instances.length : 0;
      coneSummaryEl.textContent = `Workflow Context (${activeFlowId}): ${dbsCount} Database(s) · ${pgCount} Postgres Instance(s) active · Instant O(DOM) <5ms sync`;
    }

    const objSummaryEl = document.getElementById("sflf-ctx-objects-summary");
    if (objSummaryEl) {
      objSummaryEl.innerHTML =
        `<span>Tables: <strong>${countForCat("tables", catalog.tables)}</strong></span> · ` +
        `<span>Views: <strong>${countForCat("views", catalog.views)}</strong></span> · ` +
        `<span>Iceberg: <strong>${countForCat("iceberg_tables", catalog.iceberg_tables)}</strong></span> · ` +
        `<span>Stages: <strong>${countForCat("stages", catalog.stages)}</strong></span> · ` +
        `<span>Cortex Search: <strong>${countForCat("cortex_search_services", catalog.cortex_search_services)}</strong></span> · ` +
        `<span>Analyst Views/YAML: <strong>${countForCat("semantic_views", catalog.semantic_views)}</strong></span> · ` +
        `<span>Cortex Agents: <strong>${countForCat("cortex_agents", catalog.cortex_agents)}</strong></span> · ` +
        `<span>Functions (UDF/UDTF): <strong>${countForCat("functions", catalog.functions)}</strong></span> · ` +
        `<span>Procedures: <strong>${countForCat("procedures", catalog.procedures)}</strong></span> · ` +
        `<span>Postgres (Account-Level): <strong>${countForCat("postgres_instances", catalog.postgres_instances)}</strong></span>`;
    }
  }

  // W2-R4-04 & CTX-ARCH-05: Instantaneous (<5ms) O(DOM) in-place checkbox & tri-state updater without innerHTML rebuilds
  function updateTreeCheckboxesInPlace(treeEl, cat) {
    const rootEl = treeEl || document.getElementById("sflf-sec0-tree");
    const pgBlockEl = document.getElementById("sflf-sec0-postgres-block");
    const catalog = cat || sfState.catalog || {};
    const sel = ensureSelectedContextStructure();
    if (rootEl) {
      rootEl.querySelectorAll(".sflf-tree-db-cb").forEach(function (cb) {
        const db = cb.getAttribute("data-db") || cb.value;
        if (!db) return;
        const st = computeDatabaseTriState(db, catalog);
        cb.checked = !!st.checked;
        cb.indeterminate = !!st.indeterminate;
        cb.setAttribute("data-indeterminate", st.indeterminate ? "true" : "false");
      });

      rootEl.querySelectorAll(".sflf-tree-schema-cb").forEach(function (cb) {
        const db = cb.getAttribute("data-db");
        const sch = cb.getAttribute("data-schema");
        if (!db || !sch) return;
        const st = computeSchemaTriState(db, sch, catalog);
        cb.checked = !!st.checked;
        cb.indeterminate = !!st.indeterminate;
        cb.setAttribute("data-indeterminate", st.indeterminate ? "true" : "false");
      });

      const selectedLeafMap = {};
      rootEl.querySelectorAll(".sflf-tree-cat-cb").forEach(function (cb) {
        const pairKey = cb.getAttribute("data-pair");
        const catKey = cb.getAttribute("data-category");
        if (!pairKey || !catKey) return;
        const st = computeCategoryTriState(pairKey, catKey, catalog);
        cb.checked = !!st.checked;
        cb.indeterminate = !!st.indeterminate;
        cb.setAttribute("data-indeterminate", st.indeterminate ? "true" : "false");
        const badgeEl =
          cb.parentElement &&
          (cb.parentElement.querySelector(".sflf-tree-cat-count") || cb.parentElement.querySelector("strong"));
        if (badgeEl) {
          badgeEl.textContent = `(${st.selectedCount}/${st.totalCount})`;
        }
      });

      rootEl.querySelectorAll(".sflf-tree-obj-cb").forEach(function (cb) {
        const pairKey = cb.getAttribute("data-pair");
        const catKey = cb.getAttribute("data-category");
        const objName = cb.value;
        if (!pairKey || !catKey || !objName) return;
        const cacheKey = `${pairKey}:${catKey}`;
        if (!selectedLeafMap[cacheKey]) {
          selectedLeafMap[cacheKey] = getSelectedObjectsForPairCategory(pairKey, catKey, catalog);
        }
        const isSelected = selectedLeafMap[cacheKey].indexOf(objName) !== -1;
        cb.checked = isSelected;
        cb.indeterminate = false;
        cb.setAttribute("data-indeterminate", "false");
      });
    }

    if (pgBlockEl) {
      pgBlockEl.querySelectorAll(".sflf-postgres-instance-cb").forEach(function (cb) {
        const inst = cb.getAttribute("data-postgres-instance") || cb.value;
        if (!inst) return;
        cb.checked = sel.postgres_instances.indexOf(inst) !== -1;
      });
      pgBlockEl.querySelectorAll(".sflf-postgres-table-cb").forEach(function (cb) {
        const inst = cb.getAttribute("data-postgres-instance");
        const tbl = cb.getAttribute("data-postgres-table") || cb.value;
        if (!inst || !tbl) return;
        const selTbls = Array.isArray(sel.postgres_tables_by_instance[inst])
          ? sel.postgres_tables_by_instance[inst]
          : [];
        cb.checked = selTbls.indexOf(tbl) !== -1;
      });
    }

    buildCurrentContextSelectionPayload();
    updateObjectSummaryBarInPlace(catalog);
    updateTopbarWidgets();
    syncCanvasNodesWithSnowflakeContext(sfState);
  }
  window.updateTreeCheckboxesInPlace = updateTreeCheckboxesInPlace;

  function renderMultiSelectConeInHubModal(databases, schemasMap, activeDb, activeSchema, cat) {
    const treeEl = document.getElementById("sflf-sec0-tree");
    const pgBlockEl = document.getElementById("sflf-sec0-postgres-block");
    const q = (treeUiState.searchQuery || "").trim().toLowerCase();
    const sel = ensureSelectedContextStructure();
    const mergedDbs = Array.isArray(databases) ? databases.slice() : [];
    (sel.databases || []).forEach(function (d) {
      if (d && mergedDbs.indexOf(d) === -1) mergedDbs.push(d);
    });

    if (treeEl) {
      const dbRowsHtml = mergedDbs
        .map(function (db) {
          const schemas = getCatalogSchemasForDb(db, cat);
          const dbMatches = !q || db.toLowerCase().includes(q);
          const matchingSchemas = schemas.filter(function (sch) {
            if (!q || dbMatches || sch.toLowerCase().includes(q)) return true;
            const pairKey = `${db}.${sch}`;
            return TREE_OBJECT_CATEGORIES.some(function (c) {
              if (c.label.toLowerCase().includes(q)) return true;
              return getCatalogObjectsForPairCategory(pairKey, c.key, cat).some(function (objName) {
                return String(objName).toLowerCase().includes(q);
              });
            });
          });
          if (q && !dbMatches && matchingSchemas.length === 0) return "";

          const dbState = computeDatabaseTriState(db, cat);
          const isDbExpanded = q ? true : !!treeUiState.expandedDbs[db];
          const dbArrow = isDbExpanded ? "▾" : "▸";
          const dbLocked = Array.isArray(cat.databases) && cat.databases.length > 0 && cat.databases.indexOf(db) === -1;
          const dbLockBadge = dbLocked
            ? `<span class="sflf-role-lock-badge" style="color:#f59e0b;font-size:10px;font-weight:600;">🔒 Non accessibile col ruolo ${sfState.active_role}</span>`
            : "";

          const schemaRowsHtml = isDbExpanded
            ? matchingSchemas
                .map(function (sch) {
                  const pairKey = `${db}.${sch}`;
                  const schState = computeSchemaTriState(db, sch, cat);
                  const isSchExpanded = q ? true : !!treeUiState.expandedSchemas[pairKey];
                  const schArrow = isSchExpanded ? "▾" : "▸";

                  const catRowsHtml = isSchExpanded
                    ? TREE_OBJECT_CATEGORIES.map(function (c) {
                        const allObjs = getCatalogObjectsForPairCategory(pairKey, c.key, cat);
                        const filteredObjs = !q
                          ? allObjs
                          : allObjs.filter(function (o) {
                              return (
                                String(o).toLowerCase().includes(q) ||
                                c.label.toLowerCase().includes(q) ||
                                sch.toLowerCase().includes(q) ||
                                dbMatches
                              );
                            });
                        if (q && filteredObjs.length === 0 && !c.label.toLowerCase().includes(q) && !sch.toLowerCase().includes(q) && !dbMatches) {
                          return "";
                        }
                        const catState = computeCategoryTriState(pairKey, c.key, cat);
                        const catExpandKey = `${pairKey}:${c.key}`;
                        const isCatExpanded = q ? filteredObjs.length > 0 : !!treeUiState.expandedCategories[catExpandKey];
                        const catArrow = isCatExpanded ? "▾" : "▸";
                        const selObjs = getSelectedObjectsForPairCategory(pairKey, c.key, cat);

                        const leafRowsHtml = isCatExpanded
                          ? filteredObjs
                              .map(function (objName) {
                                const objChecked = selObjs.indexOf(objName) !== -1;
                                const isLocked = isObjectInaccessibleForActiveRole(pairKey, c.key, objName, cat);
                                const lockBadgeHtml = isLocked
                                  ? `<span class="sflf-role-lock-badge" style="color:#f59e0b;font-size:10px;font-weight:600;">🔒 Non accessibile col ruolo ${sfState.active_role}</span>`
                                  : "";
                                return `<div class="sflf-tree-leaf-row" style="display:flex;align-items:center;gap:6px;padding:2px 6px 2px 54px;font-size:11px;color:#e4e4e7;">
                                  <input type="checkbox" class="sflf-tree-obj-cb" data-db="${db}" data-schema="${sch}" data-pair="${pairKey}" data-category="${c.key}" value="${objName}" data-indeterminate="false" ${
                                    objChecked ? "checked" : ""
                                  } />
                                  <span style="font-family:'JetBrains Mono',monospace;font-size:10.5px;">${objName}</span>
                                  ${lockBadgeHtml}
                                </div>`;
                              })
                              .join("")
                          : "";

                        return `<div class="sflf-tree-category-node" data-pair="${pairKey}" data-category="${c.key}">
                          <div style="display:flex;align-items:center;gap:6px;padding:2px 6px 2px 34px;font-size:11px;color:#d4d4d8;">
                            <button type="button" class="sflf-tree-toggle-cat" data-cat-key="${catExpandKey}" style="background:transparent;border:none;color:#29B5E8;cursor:pointer;padding:0 2px;font-size:11px;">${catArrow}</button>
                            <input type="checkbox" class="sflf-tree-cat-cb sflf-ctx-obj-chip" data-pair="${pairKey}" data-category="${c.key}" value="${pairKey}:${c.key}" data-indeterminate="${catState.indeterminate ? "true" : "false"}" ${
                              catState.checked ? "checked" : ""
                            } />
                            <span>${c.label} <strong class="sflf-tree-cat-count" style="color:#29B5E8;">(${catState.selectedCount}/${allObjs.length})</strong></span>
                          </div>
                          ${leafRowsHtml}
                        </div>`;
                      }).join("")
                    : "";

                  return `<div class="sflf-tree-schema-node" data-db="${db}" data-schema="${sch}">
                    <div style="display:flex;align-items:center;gap:6px;padding:3px 6px 3px 18px;font-size:11.5px;color:#f4f4f5;">
                      <button type="button" class="sflf-tree-toggle-schema" data-db="${db}" data-schema="${sch}" style="background:transparent;border:none;color:#29B5E8;cursor:pointer;padding:0 2px;font-size:11px;">${schArrow}</button>
                      <input type="checkbox" class="sflf-tree-schema-cb sflf-ctx-schema-cb" data-db="${db}" data-schema="${sch}" value="${db}.${sch}" data-indeterminate="${schState.indeterminate ? "true" : "false"}" ${
                        schState.checked ? "checked" : ""
                      } />
                      <span style="font-weight:500;">${sch}</span>
                    </div>
                    ${catRowsHtml}
                  </div>`;
                })
                .join("")
            : "";

          return `<div class="sflf-tree-db-node" data-db="${db}" style="border-bottom:1px solid #27272a;padding:2px 0;">
            <div style="display:flex;align-items:center;gap:6px;padding:4px 6px;font-size:12px;color:#ffffff;">
              <button type="button" class="sflf-tree-toggle-db" data-db="${db}" style="background:transparent;border:none;color:#29B5E8;cursor:pointer;padding:0 2px;font-size:12px;">${dbArrow}</button>
              <input type="checkbox" class="sflf-tree-db-cb sflf-ctx-db-cb" data-db="${db}" value="${db}" data-indeterminate="${dbState.indeterminate ? "true" : "false"}" ${
                dbState.checked ? "checked" : ""
              } />
              <strong style="color:#29B5E8;">${db}</strong>
              <span style="font-size:10px;color:#a1a1aa;">(${schemas.length} schemas)</span>
              ${dbLockBadge}
            </div>
            ${schemaRowsHtml}
          </div>`;
        })
        .join("");

      treeEl.innerHTML =
        `<div style="padding:4px 8px;background:#09090b;border-bottom:1px solid #27272a;font-size:11px;font-weight:600;color:#29B5E8;display:flex;justify-content:space-between;align-items:center;">
          <span>1. Snowflake Hierarchy — Databases (${mergedDbs.length}) · Active Role: ${sfState.active_role}</span>
          <span style="font-size:10px;color:#a1a1aa;">Database ▸ Schema ▸ Category ▸ Object</span>
        </div>` +
        (dbRowsHtml ||
          `<div style="padding:10px;color:#a1a1aa;font-size:11px;">No objects matching "${treeUiState.searchQuery}" under active role ${sfState.active_role}.</div>`);
      applyDomIndeterminateStates(treeEl);
    }

    // CTX-ARCH-05: Block 2 — Snowflake Postgres Account-Level Instances
    if (pgBlockEl) {
      const catPgList = Array.isArray(cat.postgres_instances) ? cat.postgres_instances.slice() : [];
      (sel.postgres_instances || []).forEach(function (inst) {
        if (inst && catPgList.indexOf(inst) === -1) catPgList.push(inst);
      });
      const filteredPg = !q
        ? catPgList
        : catPgList.filter(function (inst) {
            return String(inst).toLowerCase().includes(q) || "postgres".includes(q);
          });
      const pgTablesMap = cat.postgres_tables_by_instance || {};
      const pgRowsHtml = filteredPg
        .map(function (inst) {
          const isChecked = sel.postgres_instances.indexOf(inst) !== -1;
          const isExpanded = q ? true : !!treeUiState.expandedPostgresInstances[inst];
          const arrow = isExpanded ? "▾" : "▸";
          const isLocked = isObjectInaccessibleForActiveRole("", "postgres_instances", inst, cat);
          const lockBadge = isLocked
            ? `<span class="sflf-role-lock-badge" style="color:#f59e0b;font-size:10px;font-weight:600;">🔒 Non accessibile col ruolo ${sfState.active_role}</span>`
            : "";
          const instTables = Array.isArray(pgTablesMap[inst]) ? pgTablesMap[inst] : [];
          const selTables = Array.isArray(sel.postgres_tables_by_instance[inst])
            ? sel.postgres_tables_by_instance[inst]
            : instTables;
          const tblRows = isExpanded
            ? instTables
                .map(function (tbl) {
                  const tblChecked = selTables.indexOf(tbl) !== -1 || isChecked;
                  return `<div style="display:flex;align-items:center;gap:6px;padding:2px 6px 2px 32px;font-size:11px;color:#e4e4e7;">
                    <input type="checkbox" class="sflf-postgres-table-cb" data-postgres-instance="${inst}" data-postgres-table="${tbl}" value="${tbl}" ${
                      tblChecked ? "checked" : ""
                    } />
                    <span style="font-family:'JetBrains Mono',monospace;font-size:10.5px;">postgres.public.${tbl}</span>
                  </div>`;
                })
                .join("")
            : "";
          return `<div class="sflf-postgres-instance-row" data-postgres-instance="${inst}" style="border-bottom:1px solid #27272a;padding:3px 6px;">
            <div style="display:flex;align-items:center;gap:6px;font-size:11.5px;color:#ffffff;">
              <button type="button" class="sflf-toggle-postgres-inst" data-postgres-instance="${inst}" style="background:transparent;border:none;color:#29B5E8;cursor:pointer;padding:0 2px;font-size:11px;">${arrow}</button>
              <input type="checkbox" class="sflf-postgres-instance-cb" data-postgres-instance="${inst}" value="${inst}" ${
                isChecked ? "checked" : ""
              } />
              <strong style="color:#29B5E8;">${inst}</strong>
              <span style="font-size:10px;color:#a1a1aa;">(Account-Level · SHOW POSTGRES INSTANCES)</span>
              ${lockBadge}
            </div>
            ${tblRows}
          </div>`;
        })
        .join("");

      pgBlockEl.innerHTML =
        `<div style="padding:4px 8px;background:#09090b;border-bottom:1px solid #27272a;font-size:11px;font-weight:600;color:#29B5E8;display:flex;justify-content:space-between;align-items:center;">
          <span>2. Snowflake Postgres — Account-Level Instances (${catPgList.length})</span>
          <span style="font-size:10px;color:#a1a1aa;">Independent of Snowflake Database/Schema</span>
        </div>` +
        (pgRowsHtml ||
          `<div style="padding:8px 10px;color:#a1a1aa;font-size:11px;">No Snowflake Postgres instances discovered for active role ${sfState.active_role}.</div>`);
    }
  }

  function refreshHubModalPrivilegeAndContextViews() {
    const modal = document.getElementById("sflf-hub-modal");
    if (!modal) return;

    const roleSpan = document.getElementById("sflf-modal-role");
    if (roleSpan) roleSpan.textContent = sfState.active_role;
    const epochSpan = document.getElementById("sflf-epoch-badge");
    if (epochSpan) epochSpan.textContent = `epoch #${sfState.context_epoch}`;
    const flowScopeBadge = document.getElementById("sflf-modal-flow-scope-badge");
    if (flowScopeBadge) {
      const activeFlowId = getActiveWorkflowFlowId() || sfState.flow_id;
      flowScopeBadge.textContent = activeFlowId ? `Workflow: ${activeFlowId}` : "Scope: Global Default";
      flowScopeBadge.setAttribute("data-flow-id", activeFlowId || "");
    }

    const cat = sfState.catalog || {};
    const databases =
      Array.isArray(cat.databases) && cat.databases.length > 0
        ? cat.databases
        : [sfState.database || "SNOWFLAKE_LANGFLOW"];
    const activeDb = sfState.database || databases[0] || "SNOWFLAKE_LANGFLOW";
    const schemasMap = cat.schemas_by_db || {};
    const schemas =
      Array.isArray(schemasMap[activeDb]) && schemasMap[activeDb].length > 0
        ? schemasMap[activeDb]
        : Array.isArray(cat.schemas) && cat.schemas.length > 0
          ? cat.schemas
          : [sfState.schema || "CORE"];
    const activeSchema = sfState.schema || schemas[0] || "CORE";

    renderMultiSelectConeInHubModal(databases, schemasMap, activeDb, activeSchema, cat);
    updateObjectSummaryBarInPlace(cat);

    // Section 1 hierarchical selectors (if present in inert compatibility template)
    const privs = computeEffectivePrivileges();
    const provDbSel = document.getElementById("sflf-prov-db");
    const provSchSel = document.getElementById("sflf-prov-schema");
    if (provDbSel && !provDbSel.dataset.userEditing) {
      populateSelectOptions(provDbSel, databases, activeDb, {
        value: "__CREATE_NEW_DB__",
        label: privs.can_create_database
          ? "+ Create New Database..."
          : "+ Create New Database (Privilegi insufficienti per ruolo attivo)",
      });
    }
    if (provSchSel && !provSchSel.dataset.userEditing) {
      populateSelectOptions(provSchSel, schemas, activeSchema, {
        value: "__CREATE_NEW_SCHEMA__",
        label: privs.can_create_schema
          ? "+ Create New Schema..."
          : "+ Create New Schema (Privilegi insufficienti per ruolo attivo)",
      });
    }

    updateSection1HierarchyAndRbacState();
    updateSection2MemoryControlsState();
  }

  // HUB-W2-03 & HUB-W2-04: Enforce 1.Database -> 2.Schema -> 3.Object hierarchy & Active-Role Grey-Out Warning
  function updateSection1HierarchyAndRbacState() {
    const provDbSel = document.getElementById("sflf-prov-db");
    const provNewDbInput = document.getElementById("sflf-prov-new-db");
    const provSchSel = document.getElementById("sflf-prov-schema");
    const provNewSchInput = document.getElementById("sflf-prov-new-schema");
    const provKindSel = document.getElementById("sflf-prov-kind");
    const provNameInput = document.getElementById("sflf-prov-name");
    const extStageFields = document.getElementById("sflf-prov-ext-stage-fields");
    const btnProvision = document.getElementById("sflf-btn-provision");
    const rbacWarnBox = document.getElementById("sflf-prov-rbac-warning");

    if (!provDbSel || !provSchSel || !provKindSel || !btnProvision || !rbacWarnBox) return;

    const privs = computeEffectivePrivileges();
    const isCreatingDb = provDbSel.value === "__CREATE_NEW_DB__";
    const isCreatingSchema = provSchSel.value === "__CREATE_NEW_SCHEMA__";
    const kindVal = provKindSel.value || "table:STANDARD";

    if (provNewDbInput) {
      provNewDbInput.style.display = isCreatingDb ? "block" : "none";
      provNewDbInput.disabled = !privs.can_create_database;
      provNewDbInput.style.opacity = privs.can_create_database ? "1" : "0.5";
    }
    if (provNewSchInput) {
      provNewSchInput.style.display = isCreatingSchema ? "block" : "none";
      provNewSchInput.disabled = !privs.can_create_schema;
      provNewSchInput.style.opacity = privs.can_create_schema ? "1" : "0.5";
    }
    if (extStageFields) {
      extStageFields.style.display = kindVal === "stage:EXTERNAL" ? "grid" : "none";
    }

    const targetDb = isCreatingDb ? (provNewDbInput && provNewDbInput.value) || "<NEW_DB>" : provDbSel.value || sfState.database;
    const targetSch = isCreatingSchema ? (provNewSchInput && provNewSchInput.value) || "<NEW_SCHEMA>" : provSchSel.value || sfState.schema;

    let allowed = privs.can_create_object;
    if (isCreatingDb || kindVal.startsWith("database:")) {
      allowed = privs.can_create_database;
    } else if (isCreatingSchema || kindVal.startsWith("schema:")) {
      allowed = privs.can_create_schema;
    } else if (kindVal.startsWith("stage:")) {
      allowed = privs.can_create_stage;
    } else if (kindVal.startsWith("view:")) {
      allowed = privs.can_create_view;
    } else if (kindVal.startsWith("postgres:")) {
      allowed = privs.can_create_postgres;
    } else if (kindVal.startsWith("table:")) {
      allowed = privs.can_create_table;
    }

    if (!allowed) {
      btnProvision.disabled = true;
      btnProvision.style.opacity = "0.5";
      btnProvision.style.cursor = "not-allowed";
      if (provNameInput) {
        provNameInput.style.opacity = "0.65";
      }
      rbacWarnBox.style.display = "block";
      rbacWarnBox.textContent = `Il ruolo attivo (${sfState.active_role}) non dispone dei privilegi per creare questo oggetto in ${targetDb}.${targetSch}.`;
    } else {
      btnProvision.disabled = false;
      btnProvision.style.opacity = "1";
      btnProvision.style.cursor = "pointer";
      if (provNameInput) {
        provNameInput.style.opacity = "1";
      }
      rbacWarnBox.style.display = "none";
      rbacWarnBox.textContent = "";
    }
  }

  // HUB-W2-05 & W2-R2-13: Update Section 2 Memory backends, 3 Agent Memory types, AI_EMBED model dimension lock & compatibility guards
  function updateSection2MemoryControlsState() {
    const agentTypeSel = document.getElementById("sflf-mem-agent-type");
    const catSel = document.getElementById("sflf-mem-category");
    const backendSel = document.getElementById("sflf-mem-backend");
    const embedRow = document.getElementById("sflf-mem-embed-row");
    const embedSel = document.getElementById("sflf-mem-embed-model");
    const dimInput = document.getElementById("sflf-mem-vector-dim");
    const elemTypeSel = document.getElementById("sflf-mem-vector-elem-type");
    const noteEl = document.getElementById("sflf-mem-constraint-note");
    const createBtn = document.getElementById("sflf-btn-create-mem");

    if (!catSel || !backendSel) return;
    if (agentTypeSel && agentTypeSel.dataset.justChanged === "1") {
      agentTypeSel.dataset.justChanged = "";
      const chosenType = resolveCanonicalMemoryTypeSpec(agentTypeSel.value);
      catSel.value = chosenType.default_category;
    }
    const category = catSel.value || "vector_table";
    const backends = MEMORY_BACKENDS_BY_CATEGORY[category] || MEMORY_BACKENDS_BY_CATEGORY.vector_table;

    const currentBackend = backendSel.value;
    backendSel.innerHTML = "";
    backends.forEach(function (b) {
      const opt = document.createElement("option");
      opt.value = b.value;
      opt.textContent = b.value;
      if (b.value === currentBackend) opt.selected = true;
      backendSel.appendChild(opt);
    });

    const isVectorCategory =
      category === "vector_table" || category === "postgres_pgvector" || category === "cortex_search";
    if (embedRow) {
      embedRow.style.display = isVectorCategory ? "grid" : "none";
    }

    if (embedSel && dimInput) {
      const selectedModelId = embedSel.value || "snowflake-arctic-embed-m-v1.5";
      const modelSpec =
        OFFICIAL_AI_EMBED_MODELS.find(function (m) {
          return m.model_id === selectedModelId;
        }) || OFFICIAL_AI_EMBED_MODELS[0];
      dimInput.value = String(modelSpec.dimension);
    }

    if (noteEl) {
      const elemType = elemTypeSel ? elemTypeSel.value : "FLOAT";
      const dimVal = dimInput ? dimInput.value : "768";
      if (isVectorCategory) {
        noteEl.textContent = `Tipo colonna vettoriale bloccato su VECTOR(${elemType}, ${dimVal}) dall'embedding model selezionato (AI_EMBED). Vincoli governati attivi: VECTOR vietato in Iceberg Table, dentro VARIANT, o come PK/indice secondario in Hybrid Table.`;
      } else if (category === "semi_structured") {
        noteEl.textContent =
          "Memoria Semi-Strutturata Snowflake (VARIANT / OBJECT / ARRAY). Nota: i valori VECTOR non possono essere annidati dentro colonne VARIANT.";
      } else if (category === "postgres_non_vector") {
        noteEl.textContent =
          "Memoria Relazionale / JSONB su Snowflake Postgres (:5432) senza pgvector per stato conversazionale e payload strutturati.";
      } else {
        noteEl.textContent =
          "Memoria Strutturata Relazionale Snowflake (Standard, Hybrid, Interactive, Dynamic Table) governata dal ruolo attivo.";
      }
    }

    const privs = computeEffectivePrivileges();
    if (createBtn) {
      createBtn.disabled = !privs.can_create_object;
      createBtn.style.opacity = privs.can_create_object ? "1" : "0.5";
      createBtn.style.cursor = privs.can_create_object ? "pointer" : "not-allowed";
    }
  }

  function openSnowflakeHubModal(focusSection) {
    let modal = document.getElementById("sflf-hub-modal");
    if (modal) {
      modal.style.display = "flex";
      refreshHubModalPrivilegeAndContextViews();
      refreshTracesInModal(false);
      if (focusSection === "memories") {
        const sec2El = document.getElementById("sflf-sec2");
        if (sec2El && typeof sec2El.scrollIntoView === "function") {
          sec2El.scrollIntoView({ behavior: "smooth", block: "center" });
        }
      }
      return;
    }
    modal = document.createElement("div");
    modal.id = "sflf-hub-modal";
    modal.setAttribute("data-hub-mode", "context-only");
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,0.68);z-index:99998;display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box;font-family:Inter,sans-serif;";
    const codeBoxStyle =
      "margin-top:8px;background-color:#09090b !important;color:#e4e4e7 !important;border:1px solid #3f3f46;padding:8px 10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:11px;line-height:1.45;white-space:pre-wrap;word-break:break-word;max-height:125px;overflow-y:auto;overflow-x:hidden;box-sizing:border-box;";
    const inputStyle =
      "background:#18181b;color:#fff;border:1px solid #3f3f46;padding:5px 8px;border-radius:6px;width:100%;min-width:0;box-sizing:border-box;font-size:11px;";

    const embedModelOptionsHtml = OFFICIAL_AI_EMBED_MODELS.map(function (m) {
      return `<option value="${m.model_id}">${m.label}</option>`;
    }).join("");

    const activeFlowId = getActiveWorkflowFlowId() || sfState.flow_id;
    const flowScopeLabel = activeFlowId ? `Workflow: ${activeFlowId}` : "Scope: Global Default";

    modal.innerHTML = `
      <div class="sflf-modal-dialog-surface" data-hub-mode="context-only" style="background:#18181b;color:#f4f4f5;border:1px solid #3f3f46;border-radius:12px;width:clamp(320px,94vw,980px);max-width:96vw;max-height:min(90dvh,90vh);display:flex;flex-direction:column;overflow:hidden;padding:18px 20px;box-shadow:0 20px 50px rgba(0,0,0,0.6);font-size:12px;box-sizing:border-box;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;border-bottom:1px solid #27272a;padding-bottom:12px;margin-bottom:14px;flex-shrink:0;">
          <div style="display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap;">
            <span style="display:inline-flex;flex-shrink:0;">${buildOfficialSnowflakeSvg("#29B5E8", 20)}</span>
            <strong style="font-size:14px;color:#29B5E8;white-space:nowrap;">❄️ Flow Context</strong>
            <span style="background:#27272a;color:#29B5E8;border:1px solid #29B5E8;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:600;white-space:nowrap;">Role: <span id="sflf-modal-role">${sfState.active_role}</span> · <span id="sflf-epoch-badge">epoch #${sfState.context_epoch}</span></span>
            <span id="sflf-modal-flow-scope-badge" data-testid="hub-workflow-scope-badge" data-flow-id="${activeFlowId || ""}" style="background:rgba(41,181,232,0.12);color:#29B5E8;border:1px solid rgba(41,181,232,0.55);border-radius:5px;padding:2px 8px;font-size:11px;font-weight:600;white-space:nowrap;">${flowScopeLabel}</span>
          </div>
          <div style="display:flex;align-items:center;gap:12px;flex-shrink:0;white-space:nowrap;">
            <a href="${FORK_GITHUB_URL}" target="_blank" rel="noopener noreferrer" style="color:#a1a1aa;text-decoration:none;font-size:11px;font-weight:500;">Fork GitHub</a>
            <a href="${SNOWFLAKE_DOCS_URL}" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;gap:4px;color:#29B5E8;text-decoration:none;font-size:11px;font-weight:600;">
              ${SNOWFLAKE_SVG_COLOR} Docs
            </a>
            <button id="sflf-modal-close" style="background:#27272a;border:1px solid #3f3f46;color:#fff;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;">Close</button>
          </div>
        </div>

        <div class="sflf-modal-scroll-body" style="flex:1 1 auto;overflow-y:auto;overflow-x:hidden;min-height:0;">
          <!-- Section 0 ONLY (CTX-ARCH-04 & CTX-ARCH-05): Per-Workflow 2-Block Context Selector -->
          <div id="sflf-sec0" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;margin-bottom:10px;border:1px solid #29B5E8;">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:6px;">
              <h4 style="margin:0;font-size:13px;color:#29B5E8;">Databases, Schemas, Objects &amp; Postgres — Per-Flow Context Selector</h4>
              <div style="display:flex;gap:6px;flex-wrap:wrap;">
                <button id="sflf-btn-refresh-catalog" style="background:#3f3f46;color:#fff;border:none;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:500;">Refresh Catalog</button>
                <button id="sflf-btn-save-context" style="background:#29B5E8;color:#fff;border:none;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;">Apply &amp; Persist Context</button>
              </div>
            </div>
            <p style="margin:0 0 10px 0;font-size:11px;color:#a1a1aa;">Seleziona a cascata <strong>1. Snowflake Hierarchy (Database ▸ Schema ▸ Categoria ▸ Oggetto)</strong> e <strong>2. Snowflake Postgres (Account-Level Instances)</strong> isolati per questo workflow e governati da <code>CURRENT_ROLE()</code>. Tutti i nodi Snowflake nel canvas si sincronizzano istantaneamente in <code>&lt; 5 ms</code>.</p>
            <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,280px),1fr));gap:10px;margin-bottom:8px;">
              <div>
                <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Real-Time Search (Snowsight Explorer)</label>
                <input id="sflf-sec0-tree-search" data-testid="snowsight-hierarchy-search" type="search" placeholder="Search databases, schemas, tables, memories, services..." style="${inputStyle}" />
              </div>
            </div>
            <div id="sflf-sec0-tree" data-testid="snowsight-hierarchy-tree" style="background:#18181b;border:1px solid #3f3f46;border-radius:6px;max-height:280px;overflow-y:auto;overflow-x:hidden;margin-bottom:8px;"></div>
            <div id="sflf-sec0-postgres-block" data-testid="snowflake-postgres-account-block" style="background:#18181b;border:1px solid #3f3f46;border-radius:6px;max-height:160px;overflow-y:auto;overflow-x:hidden;margin-bottom:8px;"></div>
            <div id="sflf-cone-summary-bar" style="font-size:10.5px;color:#29B5E8;font-weight:600;margin-bottom:6px;"></div>
            <div id="sflf-ctx-objects-summary" style="font-size:10px;color:#a1a1aa;background:#18181b;padding:6px 10px;border-radius:6px;border:1px solid #3f3f46;"></div>
          </div>

          <div id="sflf-auto-sync-status-badge" data-testid="hub-auto-sync-status-badge" style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;background:#09090b;border:1px solid rgba(16,185,129,0.45);color:#a7f3d0;padding:6px 10px;border-radius:6px;font-size:10.5px;">
            <span><strong>Auto-Replicated to Snowflake:</strong> <code>LANGFLOW_FLOW_ACTIVITY</code> · <code>LANGFLOW_FLOW_RUNS</code> · <code>LANGFLOW_FLOW_SPANS</code> · <code>LANGFLOW_AUDIT_LOG</code></span>
            <span id="sflf-auto-sync-detail" style="color:#6ee7b7;font-family:'JetBrains Mono',monospace;">Active</span>
          </div>
        </div>

        <!-- Inert compatibility template preserving Section 1 (Object Provisioning) & Section 2 (Memories) for next Memory round & test suite -->
        <template id="sflf-preserved-sections-compat" style="display:none !important;" aria-hidden="true">
          <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,280px),1fr));gap:14px;margin-bottom:14px;">
            <div id="sflf-sec1" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;">
              <h4 style="margin:0 0 4px 0;font-size:13px;color:#29B5E8;">1. Governed Object Provisioning (REQ-16 · W2-R2-04B · HUB-W2-03 / HUB-W2-04)</h4>
              <div id="sflf-hub-no-fn-proc-notice">LLM Operations -&gt; Stored Procedure / Function (UDF / UDTF)</div>
              <select id="sflf-prov-db"><option value="${sfState.database}">${sfState.database}</option></select>
              <input id="sflf-prov-new-db" />
              <select id="sflf-prov-schema"><option value="${sfState.schema}">${sfState.schema}</option></select>
              <input id="sflf-prov-new-schema" />
              <select id="sflf-prov-kind"><option value="table:STANDARD">Standard Table</option></select>
              <input id="sflf-prov-name" value="DEMO_VECTORS" />
              <div id="sflf-prov-ext-stage-fields"></div>
              <div id="sflf-prov-rbac-warning"></div>
              <button id="sflf-btn-dryrun" type="button">Dry-Run DDL Preview</button>
              <button id="sflf-btn-provision" type="button">Execute on Snowflake</button>
              <div id="sflf-prov-out" style="${codeBoxStyle}"></div>
            </div>
            <div id="sflf-sec2" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;">
              <h4 style="margin:0;font-size:13px;color:#29B5E8;">2. Memories (+ Snowflake) — 5 LangChain/Langflow Memory Types</h4>
              <select id="sflf-mem-agent-type"></select>
              <select id="sflf-mem-category"></select>
              <select id="sflf-mem-backend"></select>
              <div id="sflf-mem-embed-row"><select id="sflf-mem-embed-model">${embedModelOptionsHtml}</select></div>
              <select id="sflf-mem-vector-elem-type"><option value="FLOAT">FLOAT</option></select>
              <input id="sflf-mem-vector-dim" value="768" readonly />
              <input id="sflf-mem-name" value="SNOWFLAKE_KB_MEM" />
              <button id="sflf-btn-create-mem" type="button">Create Governed Memory</button>
              <div id="sflf-mem-constraint-note"></div>
              <div id="sflf-mem-out" style="${codeBoxStyle}"></div>
            </div>
            <div id="sflf-sec3">
              <button id="sflf-btn-sync-traces" type="button">Sync Flow Activity to Snowflake SQL</button>
              <div id="sflf-traces-out"></div>
            </div>
          </div>
        </template>
      </div>
    `;
    document.body.appendChild(modal);

    document.getElementById("sflf-modal-close").onclick = function () {
      modal.style.display = "none";
      ensureMemoriesPageSnowflakeButton();
    };
    const openLightMemWizardBtn = document.getElementById("sflf-open-light-memory-modal-btn");
    if (openLightMemWizardBtn) {
      openLightMemWizardBtn.onclick = function () {
        modal.style.display = "none";
        openCreateSnowflakeMemoryModal();
      };
    }

    // Section 0 Snowsight Hierarchical Tree (Block 1), Postgres Account-Level (Block 2) & Search event listeners
    const treeSearchInput = document.getElementById("sflf-sec0-tree-search");
    const treeContainer = document.getElementById("sflf-sec0-tree");
    const pgContainer = document.getElementById("sflf-sec0-postgres-block");

    if (treeSearchInput) {
      treeSearchInput.addEventListener("input", function () {
        treeUiState.searchQuery = treeSearchInput.value || "";
        refreshHubModalPrivilegeAndContextViews();
      });
    }
    if (treeContainer) {
      treeContainer.addEventListener("click", async function (ev) {
        const target = ev.target;
        if (!target || !target.classList) return;
        if (target.classList.contains("sflf-tree-toggle-db")) {
          const db = target.getAttribute("data-db");
          if (!db) return;
          treeUiState.expandedDbs[db] = !treeUiState.expandedDbs[db];
          if (treeUiState.expandedDbs[db]) {
            await lazyLoadSchemasForDatabase(db);
          }
          refreshHubModalPrivilegeAndContextViews();
        } else if (target.classList.contains("sflf-tree-toggle-schema")) {
          const db = target.getAttribute("data-db");
          const sch = target.getAttribute("data-schema");
          if (!db || !sch) return;
          const pairKey = `${db}.${sch}`;
          treeUiState.expandedSchemas[pairKey] = !treeUiState.expandedSchemas[pairKey];
          if (treeUiState.expandedSchemas[pairKey]) {
            await lazyLoadObjectsForSchema(db, sch);
          }
          refreshHubModalPrivilegeAndContextViews();
        } else if (target.classList.contains("sflf-tree-toggle-cat")) {
          const catKey = target.getAttribute("data-cat-key");
          if (!catKey) return;
          treeUiState.expandedCategories[catKey] = !treeUiState.expandedCategories[catKey];
          refreshHubModalPrivilegeAndContextViews();
        }
      });

      // CTX-ARCH-05 & W2-R4-04: Zero-lag (<5ms) in-place checkbox updates via full_databases / full_schemas & updateTreeCheckboxesInPlace
      treeContainer.addEventListener("change", function (ev) {
        const cb = ev.target;
        if (!cb || !cb.classList) return;
        if (cb.classList.contains("sflf-tree-db-cb")) {
          const db = cb.getAttribute("data-db") || cb.value;
          const isChecked = !!cb.checked;
          toggleTreeDatabaseSelection(db, isChecked);
          updateTreeCheckboxesInPlace(treeContainer, sfState.catalog);
          scheduleDebouncedContextPersist();
        } else if (cb.classList.contains("sflf-tree-schema-cb")) {
          const db = cb.getAttribute("data-db");
          const sch = cb.getAttribute("data-schema");
          if (db && sch) {
            const isChecked = !!cb.checked;
            toggleTreeSchemaSelection(db, sch, isChecked);
            updateTreeCheckboxesInPlace(treeContainer, sfState.catalog);
            scheduleDebouncedContextPersist();
          }
        } else if (cb.classList.contains("sflf-tree-cat-cb")) {
          const pairKey = cb.getAttribute("data-pair");
          const catKey = cb.getAttribute("data-category");
          if (pairKey && catKey) {
            toggleTreeCategorySelection(pairKey, catKey, !!cb.checked);
            updateTreeCheckboxesInPlace(treeContainer, sfState.catalog);
            scheduleDebouncedContextPersist();
          }
        } else if (cb.classList.contains("sflf-tree-obj-cb")) {
          const pairKey = cb.getAttribute("data-pair");
          const catKey = cb.getAttribute("data-category");
          const objName = cb.value;
          if (pairKey && catKey && objName) {
            toggleTreeObjectSelection(pairKey, catKey, objName, !!cb.checked);
            updateTreeCheckboxesInPlace(treeContainer, sfState.catalog);
            scheduleDebouncedContextPersist();
          }
        }
      });
    }

    if (pgContainer) {
      pgContainer.addEventListener("click", function (ev) {
        const target = ev.target;
        if (!target || !target.classList) return;
        if (target.classList.contains("sflf-toggle-postgres-inst")) {
          const inst = target.getAttribute("data-postgres-instance");
          if (!inst) return;
          treeUiState.expandedPostgresInstances[inst] = !treeUiState.expandedPostgresInstances[inst];
          refreshHubModalPrivilegeAndContextViews();
        }
      });
      pgContainer.addEventListener("change", function (ev) {
        const cb = ev.target;
        if (!cb || !cb.classList) return;
        if (cb.classList.contains("sflf-postgres-instance-cb")) {
          const inst = cb.getAttribute("data-postgres-instance") || cb.value;
          if (inst) {
            togglePostgresInstanceSelection(inst, !!cb.checked);
            updateTreeCheckboxesInPlace(treeContainer, sfState.catalog);
            scheduleDebouncedContextPersist();
          }
        } else if (cb.classList.contains("sflf-postgres-table-cb")) {
          const inst = cb.getAttribute("data-postgres-instance");
          const tbl = cb.getAttribute("data-postgres-table") || cb.value;
          if (inst && tbl) {
            togglePostgresTableSelection(inst, tbl, !!cb.checked);
            updateTreeCheckboxesInPlace(treeContainer, sfState.catalog);
            scheduleDebouncedContextPersist();
          }
        }
      });
    }

    const refreshCatBtn = document.getElementById("sflf-btn-refresh-catalog");
    if (refreshCatBtn) {
      refreshCatBtn.onclick = async function () {
        await fetchSnowflakeCatalog(sfState.database, sfState.schema);
        refreshHubModalPrivilegeAndContextViews();
      };
    }
    const saveCtxBtn = document.getElementById("sflf-btn-save-context");
    if (saveCtxBtn) {
      saveCtxBtn.onclick = function () {
        if (_debouncedPersistTimer) {
          clearTimeout(_debouncedPersistTimer);
          _debouncedPersistTimer = null;
        }
        const payload = buildCurrentContextSelectionPayload();
        persistSnowflakeContextSelection(payload, false);
      };
    }

    // Preserved Section 1 & 2 optional event listeners (guarded for Context-Only mode)
    const provDbSel = document.getElementById("sflf-prov-db");
    const provSchSel = document.getElementById("sflf-prov-schema");
    const provKindSel = document.getElementById("sflf-prov-kind");
    if (provDbSel) {
      provDbSel.onchange = async function () {
        provDbSel.dataset.userEditing = "1";
        if (provDbSel.value !== "__CREATE_NEW_DB__") {
          await fetchSnowflakeCatalog(provDbSel.value, "");
          const schemasForDb =
            (sfState.catalog && sfState.catalog.schemas_by_db && sfState.catalog.schemas_by_db[provDbSel.value]) || [
              "CORE",
            ];
          populateSelectOptions(provSchSel, schemasForDb, schemasForDb[0] || "CORE", {
            value: "__CREATE_NEW_SCHEMA__",
            label: "+ Create New Schema...",
          });
        }
        updateSection1HierarchyAndRbacState();
      };
    }
    if (provSchSel) {
      provSchSel.onchange = function () {
        provSchSel.dataset.userEditing = "1";
        updateSection1HierarchyAndRbacState();
      };
    }
    if (provKindSel) {
      provKindSel.onchange = function () {
        updateSection1HierarchyAndRbacState();
      };
    }

    const memAgentTypeSel = document.getElementById("sflf-mem-agent-type");
    const memCatSel = document.getElementById("sflf-mem-category");
    const memEmbedSel = document.getElementById("sflf-mem-embed-model");
    const memElemTypeSel = document.getElementById("sflf-mem-vector-elem-type");
    if (memAgentTypeSel) {
      memAgentTypeSel.onchange = function () {
        memAgentTypeSel.dataset.justChanged = "1";
        updateSection2MemoryControlsState();
      };
    }
    if (memCatSel) {
      memCatSel.onchange = function () {
        updateSection2MemoryControlsState();
      };
    }
    if (memEmbedSel) {
      memEmbedSel.onchange = function () {
        updateSection2MemoryControlsState();
      };
    }
    if (memElemTypeSel) {
      memElemTypeSel.onchange = function () {
        updateSection2MemoryControlsState();
      };
    }

    const btnDryrun = document.getElementById("sflf-btn-dryrun");
    if (btnDryrun) {
      btnDryrun.onclick = function () {
        runProvisionCall(true);
      };
    }
    const btnProvision = document.getElementById("sflf-btn-provision");
    if (btnProvision) {
      btnProvision.onclick = function () {
        runProvisionCall(false);
      };
    }
    const btnCreateMem = document.getElementById("sflf-btn-create-mem");
    if (btnCreateMem) {
      btnCreateMem.onclick = function () {
        runCreateMemoryCall();
      };
    }
    const btnSyncTraces = document.getElementById("sflf-btn-sync-traces");
    if (btnSyncTraces) {
      btnSyncTraces.onclick = function () {
        refreshTracesInModal(true);
      };
    }

    refreshHubModalPrivilegeAndContextViews();
    refreshTracesInModal(false);
    ensureMemoriesPageSnowflakeButton();
    if (focusSection === "memories") {
      const sec2El = document.getElementById("sflf-sec2");
      if (sec2El && typeof sec2El.scrollIntoView === "function") {
        sec2El.scrollIntoView({ behavior: "smooth", block: "center" });
      }
    }
  }

  // W2-R3-05 & W2-R3-06: Dedicated Light-Themed "Create a Snowflake Memory" Modal & Searchable Memory Cards

  function openCreateSnowflakeMemoryModal(flowIdFromReact) {
    if (flowIdFromReact && typeof flowIdFromReact === "string") {
      _activeMemoryFlowId = flowIdFromReact;
    }
    let modal = document.getElementById("sflf-create-memory-modal");
    if (modal) {
      modal.style.display = "flex";
      ensureMemoriesPageSnowflakeButton();
      return;
    }

    modal = document.createElement("div");
    modal.id = "sflf-create-memory-modal";
    modal.setAttribute("data-testid", "create-snowflake-memory-modal");
    modal.setAttribute("role", "dialog");
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(15,23,42,0.45);backdrop-filter:blur(3px);z-index:99998;display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box;font-family:Inter,sans-serif;";

    const lightInputStyle =
      "background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;padding:7px 10px;border-radius:8px;width:100%;min-width:0;box-sizing:border-box;font-size:12px;";
    const lightSelectStyle =
      "background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;padding:7px 10px;border-radius:8px;width:100%;min-width:0;box-sizing:border-box;font-size:12px;";

    const typeOptionsHtml = AGENT_MEMORY_TYPES.map(function (t) {
      return `<option value="${t.id}">${t.label}</option>`;
    }).join("");

    const providerOptionsHtml = Object.keys(EMBEDDING_PROVIDERS_CATALOG)
      .map(function (k) {
        const p = EMBEDDING_PROVIDERS_CATALOG[k];
        return `<option value="${p.id}">${p.label}</option>`;
      })
      .join("");

    modal.innerHTML = `
      <div class="sflf-modal-dialog-surface" style="background:#ffffff;color:#0f172a;border:1px solid #e2e8f0;border-radius:12px;width:clamp(320px,94vw,860px);max-width:96vw;max-height:min(90dvh,90vh);display:flex;flex-direction:column;overflow:hidden;padding:20px 24px;box-shadow:0 20px 50px rgba(15,23,42,0.18);font-size:12px;box-sizing:border-box;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;border-bottom:1px solid #e2e8f0;padding-bottom:12px;margin-bottom:14px;flex-shrink:0;">
          <div style="display:flex;align-items:center;gap:8px;">
            <span style="display:inline-flex;">${buildOfficialSnowflakeSvg("#29B5E8", 20)}</span>
            <strong style="font-size:15px;color:#0f172a;">Create a Snowflake Memory</strong>
            <span style="background:rgba(41,181,232,0.12);color:#0284c7;border:1px solid #29B5E8;border-radius:6px;padding:2px 8px;font-size:11px;font-weight:600;">Role: <span id="sflf-light-mem-role">${sfState.active_role}</span></span>
          </div>
          <button type="button" id="sflf-light-mem-close" style="background:#f8fafc;border:1px solid #cbd5e1;color:#334155;padding:5px 10px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">Cancel</button>
        </div>

        <div class="sflf-modal-scroll-body" style="flex:1 1 auto;overflow-y:auto;overflow-x:hidden;min-height:0;display:flex;flex-direction:column;gap:12px;">
          <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1fr));gap:12px;">
            <div>
              <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:4px;">Memory Name</label>
              <input id="sflf-light-mem-name" data-testid="snowflake-memory-name-input" value="AGENT_MEMORY_STORE" placeholder="e.g. AGENT_CHAT_BUFFER" style="${lightInputStyle}" />
            </div>
            <div>
              <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:4px;">LangChain / Langflow Memory Type (5 Tipologie)</label>
              <select id="sflf-light-mem-type" data-testid="snowflake-memory-type-select" style="${lightSelectStyle}">
                ${typeOptionsHtml}
              </select>
            </div>
          </div>

          <div id="sflf-light-mem-guidance-card" style="background:#f8fafc;border:1px solid #bae6fd;border-radius:10px;padding:12px 14px;display:flex;flex-direction:column;gap:6px;">
            <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
              <strong id="sflf-light-mem-guidance-title" style="color:#0284c7;font-size:12.5px;"></strong>
              <span id="sflf-light-mem-embed-badge" style="background:#e0f2fe;color:#0369a1;border:1px solid #38bdf8;border-radius:999px;padding:2px 10px;font-size:10.5px;font-weight:700;"></span>
            </div>
            <div style="font-size:11px;color:#334155;"><strong>Oggetto Snowflake Consigliato:</strong> <span id="sflf-light-mem-rec-objs"></span></div>
            <div style="font-size:11px;color:#334155;"><strong>Come si passa all'Agente SENZA Embedding:</strong> <code id="sflf-light-mem-sql-preview" style="display:block;margin-top:3px;background:#0f172a;color:#e2e8f0;padding:6px 8px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:10.5px;white-space:pre-wrap;"></code></div>
            <div style="font-size:11px;color:#475569;"><strong>Quando l'Embedding diventa utile:</strong> <span id="sflf-light-mem-when-embed"></span></div>
          </div>

          <div class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px;">
            <div>
              <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:4px;">Database</label>
              <input id="sflf-light-mem-db" value="${sfState.database}" style="${lightInputStyle}" />
            </div>
            <div>
              <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:4px;">Schema</label>
              <input id="sflf-light-mem-schema" value="${sfState.schema}" style="${lightInputStyle}" />
            </div>
            <div>
              <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:4px;">Recommended Snowflake Target Object</label>
              <select id="sflf-light-mem-target-obj" style="${lightSelectStyle}"></select>
            </div>
          </div>

          <div id="sflf-light-mem-type-params" class="sflf-responsive-grid" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:10px;">
            <div>
              <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Session / Thread / User Key</label>
              <input id="sflf-light-mem-key-param" value="session_default" placeholder="thread_id / session_id / user_id" style="${lightInputStyle}" />
            </div>
            <div>
              <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Window K / Few-Shot K / TTL Hours</label>
              <input id="sflf-light-mem-k-param" type="number" value="12" min="1" max="500" style="${lightInputStyle}" />
            </div>
            <div>
              <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Summarizer / Entity Namespace / Topic</label>
              <input id="sflf-light-mem-extra-param" value="claude-sonnet-5-5" placeholder="model / namespace / topic" style="${lightInputStyle}" />
            </div>
          </div>

          <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;">
            <label style="display:inline-flex;align-items:center;gap:8px;font-size:12px;font-weight:600;color:#0f172a;cursor:pointer;">
              <input type="checkbox" id="sflf-light-mem-enable-embedding" data-testid="snowflake-memory-enable-embedding" />
              <span>Enable Embedding (Optional — disabled by default; direct SQL lookup used when off)</span>
            </label>
            <div id="sflf-light-mem-embedding-box" style="display:none;margin-top:10px;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:10px;">
              <div>
                <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Embedding Provider (Snowflake AI_EMBED or External)</label>
                <select id="sflf-light-mem-embed-provider" data-testid="snowflake-memory-embedding-provider" style="${lightSelectStyle}">
                  ${providerOptionsHtml}
                </select>
              </div>
              <div>
                <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Embedding Model</label>
                <select id="sflf-light-mem-embed-model" data-testid="snowflake-memory-embedding-model" style="${lightSelectStyle}"></select>
              </div>
              <div>
                <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Vector Dimension</label>
                <input id="sflf-light-mem-embed-dim" type="number" value="768" min="1" max="4096" style="${lightInputStyle}" />
              </div>
            </div>
          </div>

          <div id="sflf-light-mem-out" style="background:#0f172a;color:#e2e8f0;padding:8px 12px;border-radius:8px;font-family:'JetBrains Mono',monospace;font-size:11px;max-height:110px;overflow-y:auto;">Ready to create or bind Snowflake Agent Memory under active role ${sfState.active_role}.</div>
        </div>

        <div style="display:flex;justify-content:flex-end;gap:10px;border-top:1px solid #e2e8f0;padding-top:12px;margin-top:12px;flex-shrink:0;">
          <button type="button" id="sflf-light-mem-cancel-btn" style="background:#ffffff;border:1px solid #cbd5e1;color:#334155;padding:7px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">Cancel</button>
          <button type="button" id="sflf-light-mem-submit-btn" data-testid="btn-submit-snowflake-memory" style="background:#29B5E8;border:1px solid #29B5E8;color:#ffffff;padding:7px 16px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">Create Snowflake Memory</button>
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    const typeSel = document.getElementById("sflf-light-mem-type");
    const targetObjSel = document.getElementById("sflf-light-mem-target-obj");
    const enableEmbedCb = document.getElementById("sflf-light-mem-enable-embedding");
    const embedBox = document.getElementById("sflf-light-mem-embedding-box");
    const providerSel = document.getElementById("sflf-light-mem-embed-provider");
    const modelSel = document.getElementById("sflf-light-mem-embed-model");
    const dimInput = document.getElementById("sflf-light-mem-embed-dim");

    function syncLightMemoryModalTypeState() {
      const spec = resolveCanonicalMemoryTypeSpec(typeSel ? typeSel.value : "buffer_window_memory");
      const titleEl = document.getElementById("sflf-light-mem-guidance-title");
      const badgeEl = document.getElementById("sflf-light-mem-embed-badge");
      const recEl = document.getElementById("sflf-light-mem-rec-objs");
      const sqlEl = document.getElementById("sflf-light-mem-sql-preview");
      const whenEl = document.getElementById("sflf-light-mem-when-embed");
      const dbVal = (document.getElementById("sflf-light-mem-db") || {}).value || sfState.database;
      const schVal = (document.getElementById("sflf-light-mem-schema") || {}).value || sfState.schema;
      const nameVal = (document.getElementById("sflf-light-mem-name") || {}).value || "AGENT_MEMORY_STORE";

      if (titleEl) titleEl.textContent = spec.label;
      if (badgeEl) badgeEl.textContent = `Serve Embedding? ${spec.needs_embedding_badge}`;
      if (recEl) recEl.textContent = (spec.recommended_snowflake_objects || []).join(" · ");
      if (sqlEl) {
        sqlEl.textContent = (spec.how_to_feed_without_embedding || "").replace(
          "<fqn>",
          `${dbVal}.${schVal}.${nameVal}`
        );
      }
      if (whenEl) whenEl.textContent = spec.when_embedding_useful || "";
      if (targetObjSel) {
        populateSelectOptions(
          targetObjSel,
          spec.recommended_snowflake_objects || [spec.default_backend],
          spec.default_backend
        );
      }
    }

    function syncLightMemoryEmbeddingProviderModels() {
      const provKey = (providerSel && providerSel.value) || "snowflake_cortex";
      const prov = EMBEDDING_PROVIDERS_CATALOG[provKey] || EMBEDDING_PROVIDERS_CATALOG.snowflake_cortex;
      if (modelSel) {
        modelSel.innerHTML = "";
        (prov.models || []).forEach(function (m) {
          const opt = document.createElement("option");
          opt.value = m.model_id;
          opt.textContent = m.label;
          modelSel.appendChild(opt);
        });
      }
      if (dimInput && prov.models && prov.models[0]) {
        dimInput.value = String(prov.models[0].dimension || 768);
        dimInput.readOnly = provKey !== "custom_endpoint";
      }
    }

    if (typeSel) typeSel.onchange = syncLightMemoryModalTypeState;
    if (enableEmbedCb) {
      enableEmbedCb.onchange = function () {
        if (embedBox) embedBox.style.display = enableEmbedCb.checked ? "grid" : "none";
      };
    }
    if (providerSel) providerSel.onchange = syncLightMemoryEmbeddingProviderModels;
    if (modelSel) {
      modelSel.onchange = function () {
        const provKey = (providerSel && providerSel.value) || "snowflake_cortex";
        const prov = EMBEDDING_PROVIDERS_CATALOG[provKey] || EMBEDDING_PROVIDERS_CATALOG.snowflake_cortex;
        const found = (prov.models || []).find(function (m) {
          return m.model_id === modelSel.value;
        });
        if (found && dimInput) dimInput.value = String(found.dimension);
      };
    }

    function closeLightModal() {
      modal.style.display = "none";
      ensureMemoriesPageSnowflakeButton();
    }
    document.getElementById("sflf-light-mem-close").onclick = closeLightModal;
    document.getElementById("sflf-light-mem-cancel-btn").onclick = closeLightModal;

    document.getElementById("sflf-light-mem-submit-btn").onclick = async function () {
      const nameVal = (document.getElementById("sflf-light-mem-name") || {}).value || "AGENT_MEMORY_STORE";
      const dbVal = (document.getElementById("sflf-light-mem-db") || {}).value || sfState.database;
      const schVal = (document.getElementById("sflf-light-mem-schema") || {}).value || sfState.schema;
      const chosenType = (typeSel && typeSel.value) || "buffer_window_memory";
      const spec = resolveCanonicalMemoryTypeSpec(chosenType);
      const targetBackend = (targetObjSel && targetObjSel.value) || spec.default_backend;
      const enableEmbedding = !!(enableEmbedCb && enableEmbedCb.checked);
      const embProvider = (providerSel && providerSel.value) || "snowflake_cortex";
      const embModel = (modelSel && modelSel.value) || "snowflake-arctic-embed-m-v1.5";
      const embDim = parseInt((dimInput && dimInput.value) || "768", 10);
      const outEl = document.getElementById("sflf-light-mem-out");

      const resp = await fetch("/api/v1/snowflake/memories", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: nameVal,
          database: dbVal,
          schema: schVal,
          flow_id: extractCurrentFlowId(),
          agent_memory_type: chosenType,
          memory_category: enableEmbedding ? "vector_table" : spec.default_category,
          vector_database: targetBackend,
          enable_embedding: enableEmbedding,
          embedding_provider: enableEmbedding ? embProvider : "none",
          embedding_model: enableEmbedding ? embModel : null,
          dimension: enableEmbedding ? embDim : null,
          vector_dimension: enableEmbedding ? embDim : null,
        }),
      });
      const data = await resp.json();
      if (outEl) outEl.textContent = JSON.stringify(data, null, 2);
      if (!resp.ok || data.ok === false) {
        showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operazione negata");
      } else {
        sfState.memories = Array.isArray(sfState.memories) ? sfState.memories : [];
        if (data.memory) {
          sfState.memories = sfState.memories.filter(function (m) {
            return (typeof m === "string" ? m : m.name) !== data.memory.name;
          });
          sfState.memories.unshift(data.memory);
        }
        showDenialOrStatusBanner("ok", "MEMORY_CREATED", `Snowflake Memory ${nameVal} (${chosenType}) creata.`);
        ensureSnowflakeMemoriesSearchableCards();
        window.dispatchEvent(new Event("focus"));
        setTimeout(function () {
          closeLightModal();
        }, 450);
      }
    };

    syncLightMemoryModalTypeState();
    syncLightMemoryEmbeddingProviderModels();
    ensureMemoriesPageSnowflakeButton();
  }
  window.openCreateSnowflakeMemoryModal = openCreateSnowflakeMemoryModal;

  function syncSnowflakeMemoriesWindowList(rawMemories) {
    if (typeof window === "undefined") return;
    window.SNOWFLAKE_MEMORIES_LIST = (rawMemories || []).map(function (m) {
      const obj = typeof m === "string" ? { name: m, agent_memory_type: "buffer_window_memory" } : m;
      const spec = resolveCanonicalMemoryTypeSpec(obj.agent_memory_type);
      const fqn = obj.table_fqn || obj.fqn || `${obj.database || sfState.database}.${obj.schema || sfState.schema}.${obj.name}`;
      const embBadge = obj.enable_embedding
        ? `${obj.embedding_provider || "snowflake_cortex"} · ${obj.embedding_model || "AI_EMBED"} (${obj.dimension || 768}d)`
        : "No Embedding — Direct SQL";
      return {
        id: obj.langflow_memory_id || `sf-${obj.name}`,
        name: obj.name,
        is_active: true,
        description: `${spec.label} · ${fqn} · ${embBadge}`,
      };
    });
  }

  async function fetchSnowflakeMemoriesRegistry() {
    try {
      const resp = await fetch("/api/v1/snowflake/memories", { headers: { Accept: "application/json" } });
      if (!resp.ok) return;
      const data = await resp.json();
      if (Array.isArray(data.memories_catalog) && data.memories_catalog.length > 0) {
        sfState.memories = data.memories_catalog;
      } else if (Array.isArray(data.memories) && data.memories.length > 0) {
        sfState.memories = data.memories;
      }
      syncSnowflakeMemoriesWindowList(sfState.memories);
    } catch (_e) {
      // ignore
    }
  }

  let _memoriesRegistryFetchedOnce = false;
  function ensureSnowflakeMemoriesSearchableCards() {
    if (!isStrictMemoriesView() || !document.body) return;
    if (!_memoriesRegistryFetchedOnce) {
      _memoriesRegistryFetchedOnce = true;
      fetchSnowflakeMemoriesRegistry().then(function () {
        ensureSnowflakeMemoriesSearchableCards();
      });
    }
    let grid = document.getElementById("sflf-memories-cards-grid");
    const searchInput = document.querySelector(
      'input[placeholder*="Search memories" i], [data-testid="search-memories-input"]'
    );
    if (searchInput && !searchInput.dataset.sflfSearchBound) {
      searchInput.dataset.sflfSearchBound = "1";
      searchInput.addEventListener("input", function () {
        ensureSnowflakeMemoriesSearchableCards();
      });
    }
    const query = searchInput ? (searchInput.value || "").trim().toLowerCase() : "";
    const rawMemories =
      (Array.isArray(sfState.memories) && sfState.memories.length > 0
        ? sfState.memories
        : sfState.catalog && Array.isArray(sfState.catalog.memories)
          ? sfState.catalog.memories
          : []
      ).map(function (m) {
        if (typeof m === "string") {
          return {
            name: m,
            agent_memory_type: "buffer_window_memory",
            fqn: `${sfState.database}.${sfState.schema}.${m}`,
            enable_embedding: false,
          };
        }
        return m;
      });
    syncSnowflakeMemoriesWindowList(rawMemories);

    const hasNativeReactMemoriesBtn = !!document.querySelector(
      'aside button[data-testid="memories-plus-snowflake-btn"]:not(#sflf-memories-plus-snowflake-btn)'
    );
    if (!grid) {
      grid = document.createElement("div");
      grid.id = "sflf-memories-cards-grid";
      grid.setAttribute("data-testid", "snowflake-memories-cards-grid");
      grid.style.cssText =
        "margin:12px 24px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,280px),1fr));gap:12px;font-family:Inter,sans-serif;";
      document.body.appendChild(grid);
    }
    // When native React Too sidebar is mounted, Too renders SNOWFLAKE_MEMORIES_LIST natively inside <aside>; keep grid hidden on body
    grid.style.display = hasNativeReactMemoriesBtn ? "none" : "grid";

    const filtered = rawMemories.filter(function (m) {
      if (!query) return true;
      const spec = resolveCanonicalMemoryTypeSpec(m.agent_memory_type);
      const hay = `${m.name || ""} ${m.agent_memory_type || ""} ${spec.label || ""} ${m.table_fqn || m.fqn || ""} ${
        spec.how_to_feed_without_embedding || ""
      }`.toLowerCase();
      return hay.includes(query);
    });

    grid.innerHTML = filtered
      .map(function (m) {
        const spec = resolveCanonicalMemoryTypeSpec(m.agent_memory_type);
        const fqn = m.table_fqn || m.fqn || `${m.database || sfState.database}.${m.schema || sfState.schema}.${m.name}`;
        const embBadge = m.enable_embedding
          ? `${m.embedding_provider || "snowflake_cortex"} · ${m.embedding_model || "AI_EMBED"} (${
              m.dimension || 768
            }d)`
          : "No Embedding — Direct SQL Lookup";
        return `<div class="sflf-memory-card" data-memory-name="${m.name}" style="background:#ffffff;color:#0f172a;border:1px solid #e2e8f0;border-left:4px solid #29B5E8;border-radius:10px;padding:12px 14px;box-shadow:0 2px 8px rgba(15,23,42,0.06);display:flex;flex-direction:column;gap:4px;">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
            <strong style="font-size:13px;color:#0f172a;">${m.name}</strong>
            <span style="background:rgba(41,181,232,0.12);color:#0284c7;border:1px solid #29B5E8;border-radius:999px;padding:1px 8px;font-size:10px;font-weight:600;">${spec.label}</span>
          </div>
          <div style="font-size:11px;color:#475569;font-family:'JetBrains Mono',monospace;">${fqn}</div>
          <div style="font-size:10.5px;color:#0369a1;font-weight:600;">${embBadge}</div>
        </div>`;
      })
      .join("");
  }

  async function runProvisionCall(dryRun) {
    const provDbSel = document.getElementById("sflf-prov-db");
    const provNewDbInput = document.getElementById("sflf-prov-new-db");
    const provSchSel = document.getElementById("sflf-prov-schema");
    const provNewSchInput = document.getElementById("sflf-prov-new-schema");
    const rawKind = document.getElementById("sflf-prov-kind").value || "table:STANDARD";
    const nameInput = document.getElementById("sflf-prov-name").value || "DEMO_VECTORS";
    const stageUrl = (document.getElementById("sflf-prov-stage-url") || {}).value || "";
    const storageInt = (document.getElementById("sflf-prov-storage-integration") || {}).value || "";
    const out = document.getElementById("sflf-prov-out");

    const parts = rawKind.split(":");
    let objectKind = parts[0] || "table";
    const objectSubtype = parts[1] || "STANDARD";

    // HUB-W2-04 & W2-R2-04B client-side hard guard: never allow function, procedure, or memory creation from Object Provisioner
    if (
      objectKind === "function" ||
      objectKind === "procedure" ||
      objectKind === "udf" ||
      objectKind === "udtf" ||
      objectKind === "stored_procedure" ||
      objectKind === "memory"
    ) {
      const msg =
        objectKind === "memory"
          ? "La creazione di Memory è stata spostata nella pagina Memories (+ Snowflake) (W2-R2-04B / W2-R2-13)."
          : "La creazione di FUNCTION (UDF/UDTF) e STORED PROCEDURE non è consentita dallo Snowflake Langflow Hub (HUB-W2-04). Usa i componenti in LLM Operations.";
      out.textContent = JSON.stringify({ ok: false, error_code: "UNSUPPORTED_CAPABILITY", message: msg }, null, 2);
      showDenialOrStatusBanner("error", "UNSUPPORTED_CAPABILITY", msg);
      return;
    }

    const isCreatingDb = provDbSel && provDbSel.value === "__CREATE_NEW_DB__";
    const isCreatingSchema = provSchSel && provSchSel.value === "__CREATE_NEW_SCHEMA__";
    const targetDb = isCreatingDb
      ? (provNewDbInput && provNewDbInput.value.trim()) || nameInput
      : (provDbSel && provDbSel.value) || sfState.database;
    const targetSchema = isCreatingSchema
      ? (provNewSchInput && provNewSchInput.value.trim()) || nameInput
      : (provSchSel && provSchSel.value) || sfState.schema;

    let effectiveName = nameInput;
    if (isCreatingDb && objectKind === "database") {
      effectiveName = targetDb;
    } else if (isCreatingSchema && objectKind === "schema") {
      effectiveName = targetSchema;
    }

    const resp = await fetch("/api/v1/snowflake/provision", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: "hub",
        database: targetDb,
        schema: targetSchema,
        object_kind: objectKind,
        object_subtype: objectSubtype,
        table_type: objectSubtype,
        stage_type: objectSubtype,
        stage_url: stageUrl,
        storage_integration: storageInt,
        name: effectiveName,
        dry_run: dryRun,
        confirmed: !dryRun,
      }),
    });
    const data = await resp.json();
    out.textContent = JSON.stringify(data, null, 2);
    if (!resp.ok || data.ok === false) {
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operazione negata");
    } else if (!dryRun) {
      showDenialOrStatusBanner(
        "ok",
        "PROVISIONED",
        `Oggetto ${effectiveName} (${objectKind}/${objectSubtype}) creato in ${targetDb}.${targetSchema} col ruolo ${sfState.active_role}`
      );
      await fetchSnowflakeCatalog(targetDb, targetSchema);
      refreshHubModalPrivilegeAndContextViews();
      refreshTracesInModal();
    }
  }

  async function runCreateMemoryCall() {
    const name = document.getElementById("sflf-mem-name").value;
    const agentMemoryType = (document.getElementById("sflf-mem-agent-type") || {}).value || "buffer_window_memory";
    const category = (document.getElementById("sflf-mem-category") || {}).value || "structured";
    const backend = document.getElementById("sflf-mem-backend").value;
    const embedModel = (document.getElementById("sflf-mem-embed-model") || {}).value || "snowflake-arctic-embed-m-v1.5";
    const elemType = (document.getElementById("sflf-mem-vector-elem-type") || {}).value || "FLOAT";
    const dimVal = parseInt((document.getElementById("sflf-mem-vector-dim") || {}).value || "768", 10);
    const out = document.getElementById("sflf-mem-out");

    const backends = MEMORY_BACKENDS_BY_CATEGORY[category] || [];
    const matchedSpec =
      backends.find(function (b) {
        return b.value === backend;
      }) || {};
    const enableEmbedding =
      category === "vector_table" || category === "postgres_pgvector" || category === "cortex_search";

    const resp = await fetch("/api/v1/snowflake/memories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: name,
        database: sfState.database,
        schema: sfState.schema,
        agent_memory_type: agentMemoryType,
        memory_category: category,
        vector_database: backend,
        table_type: matchedSpec.table_type || "STANDARD",
        semi_structured_type: matchedSpec.semi_type || null,
        enable_embedding: enableEmbedding,
        embedding_provider: enableEmbedding ? "snowflake_cortex" : "none",
        embedding_model: enableEmbedding ? embedModel : null,
        vector_element_type: elemType,
        dimension: enableEmbedding ? dimVal : null,
        vector_dimension: enableEmbedding ? dimVal : null,
      }),
    });
    const data = await resp.json();
    out.textContent = JSON.stringify(data, null, 2);
    if (!resp.ok || data.ok === false) {
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operazione negata");
    } else {
      showDenialOrStatusBanner("ok", "MEMORY_CREATED", `Memoria ${name} (${agentMemoryType}) creata in ${backend}`);
      await fetchSnowflakeCatalog(sfState.database, sfState.schema);
      refreshHubModalPrivilegeAndContextViews();
      refreshTracesInModal();
    }
  }

  async function refreshTracesInModal(doFullSync) {
    const out = document.getElementById("sflf-traces-out");
    const autoSyncBadgeDetail = document.getElementById("sflf-auto-sync-detail");
    const roleSpan = document.getElementById("sflf-modal-role");
    if (roleSpan) roleSpan.textContent = sfState.active_role;
    if (!out && !autoSyncBadgeDetail) return;
    try {
      const resp = doFullSync
        ? await fetch("/api/v1/snowflake/traces/sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
          })
        : await fetch("/api/v1/snowflake/traces?limit=15", {
            headers: { Accept: "application/json" },
          });
      const data = await resp.json();
      if (out) out.textContent = JSON.stringify(data, null, 2);
      if (autoSyncBadgeDetail) {
        const runsCount =
          typeof data.synced_runs_count === "number"
            ? data.synced_runs_count
            : Array.isArray(data.runs)
              ? data.runs.length
              : 0;
        const spansCount =
          typeof data.synced_spans_count === "number"
            ? data.synced_spans_count
            : Array.isArray(data.spans)
              ? data.spans.length
              : 0;
        autoSyncBadgeDetail.textContent = `Active · Runs: ${runsCount} · Spans: ${spansCount}`;
      }
    } catch (e) {
      if (out) out.textContent = String(e);
    }
  }

  function bootSnowflakeUI() {
    installHeadMutationObserver();
    ensureDualSymbolLoadingOverlay();
    mountFixedTopbarControls();
    scanAll();
    fetchSnowflakeContext();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootSnowflakeUI);
  } else {
    bootSnowflakeUI();
  }
})();
