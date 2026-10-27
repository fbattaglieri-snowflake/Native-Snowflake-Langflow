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
        const urlStr = String(input);
        const safeInit = init && typeof init === "object" ? Object.assign({}, init) : {};
        if (!safeInit.headers) {
          safeInit.headers = {};
        }
        // W2-R5-02: Intercept native POST /api/v1/memories from Langflow's + Create modal to attach active [ ❄️ Context ]
        const methodUpper = String(safeInit.method || "GET").toUpperCase();
        if (
          methodUpper === "POST" &&
          (/\/api\/v1\/memories\/?(?:\?|$)/i.test(urlStr) || urlStr.endsWith("/api/v1/memories") || urlStr.endsWith("/api/v1/memories/")) &&
          typeof safeInit.body === "string"
        ) {
          try {
            const parsed = JSON.parse(safeInit.body);
            const activeCtx = (typeof window !== "undefined" && window.SNOWFLAKE_LANGFLOW_CONTEXT) || {};
            const ov = (typeof window !== "undefined" && window.__sflfNativeCreateMemoryOverrides) || {};
            if (!parsed.database) parsed.database = ov.database || activeCtx.database || "SNOWFLAKE_LANGFLOW";
            if (!parsed.schema) parsed.schema = ov.schema || activeCtx.schema || "CORE";
            if (!parsed.flow_id && typeof window.extractCurrentFlowId === "function") {
              parsed.flow_id = window.extractCurrentFlowId();
            }
            if (ov.backend_type && (!parsed.backend_type || parsed.backend_type === "chroma")) {
              parsed.backend_type = ov.backend_type;
            }
            if (ov.embedding_model && !parsed.embedding_model) {
              parsed.embedding_model = ov.embedding_model;
              parsed.embedding_provider = ov.embedding_provider || "Snowflake Cortex";
            }
            safeInit.body = JSON.stringify(parsed);
          } catch (_e) {
            // preserve original body if non-JSON
          }
          return _prevFetch.call(this, input, safeInit).then(function (resp) {
            if (resp && resp.ok && typeof window !== "undefined") {
              setTimeout(function () {
                if (typeof window.fetchSnowflakeMemoriesRegistry === "function") {
                  window.fetchSnowflakeMemoriesRegistry();
                }
                if (typeof window.__sflfRefetchMemories === "function") {
                  try {
                    window.__sflfRefetchMemories();
                  } catch (_e) {}
                }
              }, 60);
            }
            return resp;
          });
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

  // W2-R3-05, W2-R2-13 & W2-R4-02: 5 Canonical LangChain/Langflow Agent Memory Types + Slot #6 Coming Soon + Legacy Aliases
  const AGENT_MEMORY_TYPES = [
    {
      id: "graph_state_checkpointer",
      label: "#1 Short-Term: Graph State & Checkpointer",
      short_label: "#1 Graph State & Checkpointer",
      category: "short_term",
      default_category: "postgres_non_vector",
      default_storage_family: "snowflake_postgres",
      default_backend: "Snowflake Postgres (PostgresSaver)",
      default_table_type: "HYBRID",
      default_table_name: "AGENT_CHECKPOINTS",
      default_hybrid_pk: "(thread_id, checkpoint_ns, step_id)",
      default_hybrid_index: "(thread_id, updated_at)",
      default_pg_table: "public.langgraph_checkpoints",
      default_pg_mode: "PostgresSaver",
      filter_by_session: true,
      embedding_locked_off: true,
      default_llm_preprocessing: false,
      recommended_snowflake_objects: [
        "Snowflake Postgres (PostgresSaver)",
        "Hybrid Table",
        "SnowflakeCheckpointer",
      ],
      needs_embedding_badge: "NO (Never)",
      how_to_feed_without_embedding:
        "SELECT state_blob, checkpoint_ns, step_id FROM <fqn> WHERE thread_id = :thread_id ORDER BY step_id DESC LIMIT 1",
      when_embedding_useful:
        "Never: graph execution state is always retrieved by primary key thread_id.",
      purpose:
        "Saves and restores LangGraph/A2A execution state (steps, visited nodes, Human-in-the-Loop interrupts) indexed by thread_id and checkpoint_ns.",
      required_inputs: ["thread_id", "checkpoint_ns", "ttl_hours"],
      default_enable_embedding: false,
      description:
        "LangGraph state checkpoints and resumable execution state keyed by thread_id and checkpoint_ns.",
    },
    {
      id: "buffer_window_memory",
      label: "#2 Short-Term: Buffer / Window Memory (Chat History)",
      short_label: "#2 Buffer / Window Memory (Chat History)",
      category: "short_term",
      default_category: "structured",
      default_storage_family: "snowflake_sql",
      default_backend: "Hybrid Table",
      default_table_type: "HYBRID",
      default_table_name: "CHAT_WINDOW_HISTORY",
      default_hybrid_pk: "(session_id, turn_id)",
      default_hybrid_index: "(session_id, created_at)",
      default_pg_table: "public.chat_message_history",
      default_pg_mode: "Relational / JSONB",
      filter_by_session: true,
      embedding_locked_off: false,
      default_llm_preprocessing: false,
      recommended_snowflake_objects: [
        "Snowflake Postgres (Relational / JSONB)",
        "Hybrid Table",
        "Standard Table",
      ],
      needs_embedding_badge: "NO",
      how_to_feed_without_embedding:
        "SELECT sender, message_text, created_at FROM <fqn> WHERE session_id = :session_id ORDER BY created_at DESC LIMIT :window_k",
      when_embedding_useful:
        "Only useful when retrieving messages from 500+ turns ago outside the sliding K window.",
      purpose:
        "Keeps the last K conversational turns ordered chronologically by session_id to populate the Agent's Chat History without vector search latency.",
      required_inputs: ["session_id", "window_k", "retention_days"],
      default_enable_embedding: false,
      description:
        "Recent k conversational turns ordered chronologically by session_id without vector search overhead.",
    },
    {
      id: "summary_memory",
      label: "#3 Short-Term: Summary Memory (Conversation Summary)",
      short_label: "#3 Summary Memory (Running Summary)",
      category: "short_term",
      default_category: "structured",
      default_storage_family: "snowflake_sql",
      default_backend: "Hybrid Table",
      default_table_type: "HYBRID",
      default_table_name: "CHAT_SESSION_SUMMARY",
      default_hybrid_pk: "(session_id)",
      default_hybrid_index: "(session_id, updated_at)",
      default_pg_table: "public.chat_session_summaries",
      default_pg_mode: "Relational / JSONB",
      filter_by_session: true,
      embedding_locked_off: true,
      default_llm_preprocessing: true,
      recommended_snowflake_objects: [
        "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG",
        "Standard Table + AI_COMPLETE",
        "Snowflake Postgres",
      ],
      needs_embedding_badge: "NO (Never)",
      how_to_feed_without_embedding:
        "SELECT running_summary, last_turn_id FROM <fqn> WHERE session_id = :session_id LIMIT 1 -- injected into the System Prompt",
      when_embedding_useful:
        "Never: a compact rolling summary read directly and updated in the background via AI_COMPLETE / AI_SUMMARIZE_AGG.",
      purpose:
        "Maintains a compact rolling summary of the session updated via AI_COMPLETE / AI_SUMMARIZE_AGG and injected into the Agent's System Message.",
      required_inputs: ["session_id", "summarizer_model", "max_summary_tokens"],
      default_enable_embedding: false,
      description:
        "Rolling single-row conversation summary updated via Snowflake Cortex AI_COMPLETE / AI_SUMMARIZE_AGG.",
    },
    {
      id: "user_profile_entity_memory",
      label: "#4 Long-Term Semantic: User Profile & Entity Memory",
      short_label: "#4 User Profile & Entity Memory (Long-Term)",
      category: "long_term_semantic",
      default_category: "semi_structured",
      default_storage_family: "snowflake_sql",
      default_backend: "Standard Table (VARIANT)",
      default_table_type: "HYBRID",
      default_table_name: "USER_ENTITY_PROFILE",
      default_hybrid_pk: "(user_id, entity_namespace, chunk_id)",
      default_hybrid_index: "(user_id, entity_namespace)",
      default_pg_table: "public.user_entity_profiles",
      default_pg_mode: "PostgresStore JSONB",
      filter_by_session: false,
      embedding_locked_off: false,
      default_llm_preprocessing: true,
      recommended_snowflake_objects: [
        "Snowflake Postgres (PostgresStore)",
        "Hybrid Table (VARIANT)",
        "Standard Table (VARIANT)",
      ],
      needs_embedding_badge: "OPTIONAL (Default OFF)",
      how_to_feed_without_embedding:
        "SELECT profile_json FROM <fqn> WHERE user_id = :user_id AND entity_namespace = :entity_namespace LIMIT 1 -- loaded at session start",
      when_embedding_useful:
        "Only when a user accumulates hundreds of distinct facts that no longer fit in the prompt.",
      purpose:
        "Stores persistent user preferences, facts, and entity attributes (VARIANT/JSONB) across sessions with LLM distillation and Kill Phrase NO_INGEST.",
      required_inputs: ["user_id", "entity_namespace", "schema_keys"],
      default_enable_embedding: false,
      description:
        "Persistent user preferences, facts, and structured entity attributes stored as JSONB or VARIANT.",
    },
    {
      id: "episodic_memory",
      label: "#5 Long-Term Semantic: Episodic Memory (Few-Shot Experiences)",
      short_label: "#5 Episodic Memory (Few-Shot Experiences)",
      category: "long_term_semantic",
      default_category: "structured",
      default_storage_family: "snowflake_sql",
      default_backend: "Hybrid Table",
      default_table_type: "HYBRID",
      default_table_name: "AGENT_EPISODIC_MEMORY",
      default_hybrid_pk: "(user_id, chunk_id)",
      default_hybrid_index: "(user_id, topic, created_at)",
      default_pg_table: "public.agent_episodes",
      default_pg_mode: "JSONB + pgvector",
      filter_by_session: false,
      embedding_locked_off: false,
      default_llm_preprocessing: false,
      recommended_snowflake_objects: [
        "Hybrid Table",
        "Standard Table",
        "Interactive / Dynamic Table",
        "Snowflake Postgres",
      ],
      needs_embedding_badge: "OPTIONAL (Default OFF)",
      how_to_feed_without_embedding:
        "SELECT topic, tool_used, outcome_score, episode_text FROM <fqn> WHERE topic = :topic ORDER BY outcome_score DESC, created_at DESC LIMIT :few_shot_k",
      when_embedding_useful:
        "Useful when past tasks do not share fixed categories and vector similarity search (VECTOR_COSINE_SIMILARITY) is needed to find similar episodes.",
      purpose:
        "Stores past tasks successfully solved by the agent (outcome_score >= threshold) to provide dynamic few-shot examples or a Toolset to the Agent.",
      required_inputs: ["topic_column", "tool_used_column", "outcome_score_threshold", "few_shot_k"],
      default_enable_embedding: false,
      description:
        "Past agent experiences and high-scoring few-shot examples retrieved by topic/tool or optional vector similarity.",
    },
  ];

  // W2-R4-02: Slot #6 Business Ontology / Cortex Sense (Coming Soon) kept separate so AGENT_MEMORY_TYPES.length === 5
  const COMING_SOON_MEMORY_TYPES = [
    {
      id: "business_ontology_cortex_sense",
      label: "#6 Long-Term: Business Ontology / Cortex Sense (Coming Soon)",
      short_label: "#6 Business Ontology / Cortex Sense (Soon)",
      category: "long_term_ontology",
      enabled: false,
      badge: "Coming Soon",
      recommended_snowflake_objects: [
        "Cortex Sense Context",
        "Business Ontology Graph",
        "Semantic View",
      ],
      needs_embedding_badge: "COMING SOON (Cortex Sense)",
      how_to_feed_without_embedding:
        "-- Slot #6 (Coming Soon): resolved via Cortex Sense / Business Ontology",
      when_embedding_useful:
        "Managed automatically by the Cortex Sense semantic graph when enabled.",
      purpose:
        "Enterprise ontology memory powered by Cortex Sense and Business Ontology (domain concepts, semantic relationships, canonical metrics, and business rules). Coming in a future release.",
      description:
        "Enterprise Business Ontology and Cortex Sense semantic graph memory (Coming Soon).",
    },
  ];
  window.SNOWFLAKE_COMING_SOON_MEMORY_TYPES = COMING_SOON_MEMORY_TYPES;

  const LEGACY_MEMORY_TYPE_ALIASES = {
    episodic_session: "buffer_window_memory",
    semantic_vector: "episodic_memory",
    structured_long_term: "user_profile_entity_memory",
  };

  function resolveCanonicalMemoryTypeSpec(typeId) {
    if (typeId === "business_ontology_cortex_sense") {
      return COMING_SOON_MEMORY_TYPES[0];
    }
    const normalized = LEGACY_MEMORY_TYPE_ALIASES[typeId] || typeId || "buffer_window_memory";
    return (
      AGENT_MEMORY_TYPES.find(function (t) {
        return t.id === normalized;
      }) || AGENT_MEMORY_TYPES[1]
    );
  }

  // Official AI_EMBED model catalog with locked vector dimensions (HUB-W2-05 & W2-R4-04)
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
    { model_id: "twelvelabs-marengo-embed-3-0", dimension: 512, label: "twelvelabs-marengo-embed-3-0 (512d - AI_MULTI_EMBED)" },
  ];

  // W2-R3-05 & W2-R4-04: 4 Official Snowflake Embedding Sources + Backward-Compatible External Aliases
  const EMBEDDING_PROVIDERS_CATALOG = {
    snowflake_cortex: {
      id: "snowflake_cortex",
      label: "1. Snowflake Cortex Native (AI_EMBED / AI_MULTI_EMBED)",
      models: [
        { model_id: "snowflake-arctic-embed-m-v1.5", dimension: 768, label: "snowflake-arctic-embed-m-v1.5 (768d - AI_EMBED)" },
        { model_id: "snowflake-arctic-embed-m", dimension: 768, label: "snowflake-arctic-embed-m (768d - AI_EMBED)" },
        { model_id: "e5-base-v2", dimension: 768, label: "e5-base-v2 (768d - AI_EMBED)" },
        { model_id: "snowflake-arctic-embed-l-v2.0", dimension: 1024, label: "snowflake-arctic-embed-l-v2.0 (1024d - AI_EMBED)" },
        { model_id: "snowflake-arctic-embed-l-v2.0-8k", dimension: 1024, label: "snowflake-arctic-embed-l-v2.0-8k (1024d - AI_EMBED)" },
        { model_id: "multilingual-e5-large", dimension: 1024, label: "multilingual-e5-large (1024d - AI_EMBED)" },
        { model_id: "voyage-multilingual-2", dimension: 1024, label: "voyage-multilingual-2 (1024d - AI_EMBED)" },
        { model_id: "nv-embed-qa-4", dimension: 1024, label: "nv-embed-qa-4 (1024d - AI_EMBED)" },
        { model_id: "voyage-multimodal-3", dimension: 1024, label: "voyage-multimodal-3 (1024d - AI_EMBED)" },
        { model_id: "twelvelabs-marengo-embed-3-0", dimension: 512, label: "twelvelabs-marengo-embed-3-0 (512d - AI_MULTI_EMBED)" },
      ],
    },
    spcs_model_registry: {
      id: "spcs_model_registry",
      label: "2. Snowflake Model Registry (SPCS Compute Pool Service, 1..4096d)",
      models: [
        { model_id: "bge-large-en-v1.5-spcs", dimension: 1024, label: "bge-large-en-v1.5-spcs (1024d - SPCS Registry)" },
        { model_id: "intfloat-e5-mistral-7b-spcs", dimension: 4096, label: "intfloat-e5-mistral-7b-spcs (4096d - SPCS Registry)" },
        { model_id: "all-minilm-l6-v2-spcs", dimension: 384, label: "all-minilm-l6-v2-spcs (384d - SPCS Registry)" },
      ],
    },
    spcs_ollama: {
      id: "spcs_ollama",
      label: "3. Private - Ollama (SPCS Compute Pool Container)",
      models: [
        { model_id: "nomic-embed-text", dimension: 768, label: "nomic-embed-text (768d - SPCS Ollama)" },
        { model_id: "bge-large-en-v1.5", dimension: 1024, label: "bge-large-en-v1.5 (1024d - SPCS Ollama)" },
        { model_id: "mxbai-embed-large", dimension: 1024, label: "mxbai-embed-large (1024d - SPCS Ollama)" },
      ],
    },
    spcs_llm_proxy: {
      id: "spcs_llm_proxy",
      label: "4. Snowflake Standard Proxy (Chat / Responses / Messages, 1..4096d)",
      models: [
        { model_id: "proxy-chat-text-embedding-3-small", dimension: 1536, label: "Proxy - Chat: text-embedding-3-small (1536d)" },
        { model_id: "proxy-chat-text-embedding-3-large", dimension: 3072, label: "Proxy - Chat: text-embedding-3-large (3072d)" },
        { model_id: "proxy-responses-embed-v1", dimension: 1024, label: "Proxy - Responses: embed-v1 (1024d)" },
        { model_id: "proxy-messages-voyage-3", dimension: 1024, label: "Proxy - Messages: voyage-3 (1024d)" },
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

  // W2-R4-08F: Summarization & LLM Preprocessing Engines with selectable models (AI_COMPLETE vs AI_SUMMARIZE_AGG)
  const PREPROCESSING_ENGINES_CATALOG = {
    cortex_sql: {
      id: "cortex_sql",
      label: "Cortex SQL — AI_COMPLETE (Select Model)",
      default_model: "mistral-large2",
      hide_model_select: false,
      models: [
        "mistral-large2",
        "llama3.3-70b",
        "llama3.1-70b",
        "claude-3-7-sonnet",
        "claude-sonnet-4-5",
        "openai-gpt-5",
        "deepseek-r1",
      ],
    },
    cortex_summarize_agg: {
      id: "cortex_summarize_agg",
      label: "Cortex Native — AI_SUMMARIZE_AGG (Automatic SQL Aggregation)",
      default_model: "AI_SUMMARIZE_AGG",
      hide_model_select: true,
      models: ["AI_SUMMARIZE_AGG"],
    },
    proxy_chat: {
      id: "proxy_chat",
      label: "Proxy - Chat (Snowflake Standard Proxy)",
      default_model: "openai-gpt-6-luna",
      hide_model_select: false,
      models: ["openai-gpt-6-luna", "openai-gpt-6-astra", "claude-sonnet-5-5", "kimi-k3"],
    },
    proxy_messages: {
      id: "proxy_messages",
      label: "Proxy - Messages (Snowflake Standard Proxy)",
      default_model: "claude-sonnet-5-5",
      hide_model_select: false,
      models: ["claude-sonnet-5-5", "claude-opus-5-5"],
    },
    proxy_responses: {
      id: "proxy_responses",
      label: "Proxy - Responses (Snowflake Standard Proxy)",
      default_model: "openai-gpt-6-luna",
      hide_model_select: false,
      models: ["openai-gpt-6-luna", "openai-gpt-6-astra"],
    },
    spcs_ollama: {
      id: "spcs_ollama",
      label: "Private - Ollama (SPCS)",
      default_model: "llama3.2:3b",
      hide_model_select: false,
      models: ["llama3.2:3b", "qwen2.5-coder:7b", "mistral:7b"],
    },
    spcs_model_registry: {
      id: "spcs_model_registry",
      label: "Model Registry SPCS",
      default_model: "SNOWFLAKE_LANGFLOW.CORE.AGENT_PREPROC_LLM_SVC",
      hide_model_select: false,
      models: ["SNOWFLAKE_LANGFLOW.CORE.AGENT_PREPROC_LLM_SVC"],
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
      '[data-testid="new-project-btn"], [data-testid="empty_page_create_first_flow_button"]'
    );
    buttons.forEach(function (btn) {
      if (btn.dataset && btn.dataset.sflfBlueStyled === "1") return;
      const tid = btn.getAttribute("data-testid") || "";
      const txt = (btn.textContent || "").trim();
      if (
        tid === "new-project-btn" ||
        tid === "empty_page_create_first_flow_button" ||
        txt === "+ Create first flow" ||
        txt === "Create first flow"
      ) {
        if (btn.dataset) btn.dataset.sflfBlueStyled = "1";
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

  // UI-W2-01, W2-R2-14, W2-R3-01 & W2-R4-08A: Clean Snowflake Blue (#29B5E8) dual-symbol loading overlay mounted on document.body (outside #root)
  // W2-R4-08A: roObserver does NOT watch "style", coalesces callbacks via requestAnimationFrame (_rafScheduled), and guards with _isApplyingDomPatch.
  let _loadingBodyObserverInstalled = false;
  let _rafScheduled = false;
  let _isApplyingDomPatch = false;
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
      } else if (overlay && overlay.style.display !== "flex") {
        overlay.style.display = "flex";
      }
    } else if (overlay && overlay.style.display !== "none") {
      overlay.style.display = "none";
    }

    if (!_loadingBodyObserverInstalled && typeof MutationObserver === "function" && document.documentElement) {
      _loadingBodyObserverInstalled = true;
      const roObserver = new MutationObserver(function () {
        if (_isApplyingDomPatch || _rafScheduled) return;
        _rafScheduled = true;
        const applyCoalescedDomUpdates = function () {
          _rafScheduled = false;
          if (_isApplyingDomPatch) return;
          _isApplyingDomPatch = true;
          try {
            ensureSnowflakeBlueBrandStyles();
            styleCreateFirstFlowButtons();
            const rEl = document.getElementById("root");
            const emptyNow = !rEl || rEl.children.length === 0;
            const ov = document.getElementById("sflf-dual-loading-overlay");
            if (ov) {
              const targetDisp = emptyNow ? "flex" : "none";
              if (ov.style.display !== targetDisp) {
                ov.style.display = targetDisp;
              }
            }
            ensureMemoriesPageSnowflakeButton();
            if (typeof enhanceNativeCreateMemoryDialog === "function") {
              enhanceNativeCreateMemoryDialog();
            }
            if (typeof ensureLeftRailStagesVolumesTab === "function") {
              ensureLeftRailStagesVolumesTab();
            }
          } finally {
            _isApplyingDomPatch = false;
          }
        };
        if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
          window.requestAnimationFrame(applyCoalescedDomUpdates);
        } else {
          setTimeout(applyCoalescedDomUpdates, 16);
        }
      });
      roObserver.observe(document.documentElement, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["class", "data-state", "aria-current"],
      });
    }
  }

  // W2-R3-06 & W2-R4-08A: Strict O(1) Memories View detection & modal dialog open check
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
    const stgModal = document.getElementById("sflf-create-stage-modal");
    if (stgModal && stgModal.style && stgModal.style.display && stgModal.style.display !== "none") {
      return true;
    }
    const nativeDialog = document.querySelector(
      '[role="dialog"]:not([aria-hidden="true"]), [data-state="open"][role="dialog"], [data-radix-Collection-item][data-state="open"]'
    );
    return !!nativeDialog;
  }

  function isStrictMemoriesView() {
    if (typeof document === "undefined" || !document.body) return false;
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
    if (
      document.querySelector(
        'input[placeholder*="Search memories" i], [data-testid="search-memories-input"], aside button[data-testid="memories-plus-snowflake-btn"]'
      )
    ) {
      return true;
    }
    const titleEl = document.querySelector('[data-testid="mainpage_title"], aside h2');
    if (titleEl) {
      const t = (titleEl.textContent || "").trim();
      if (t === "Memories" || t === "Manage Memories") {
        return true;
      }
    }
    return false;
  }

  // W2-R2-13, W2-R2-14, W2-R3-05, W2-R3-06 & W2-R4-01: Companion `[ + ❄️ Snowflake ]` button in official #29B5E8
  // Strictly shown ONLY on the Memories view, placed inline next to `+ Create` with normal z-index (z-index: 10),
  // element order `+` BEFORE the Snowflake icon and text `Snowflake` (`[ + ❄️ Snowflake ]`),
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
      btn.innerHTML = '<span>+</span>' + buildOfficialSnowflakeSvg("#ffffff", 14) + '<span>Snowflake ▾</span>';
      btn.onclick = function (ev) {
        openSnowflakeMemoryTypeDropdown(extractCurrentFlowId(), ev || btn);
      };
      document.body.appendChild(btn);
    }
    if (nativeReactBtn) {
      if (btn.style.display !== "none") btn.style.display = "none";
      if (btn.style.pointerEvents !== "none") btn.style.pointerEvents = "none";
      if (isMemoriesView) {
        ensureSnowflakeMemoriesSearchableCards();
      }
      return;
    }
    if (isMemoriesView && !modalOpen) {
      const createBtnCandidate = Array.from(document.querySelectorAll("aside button")).find(function (b) {
        if (b.id === "sflf-memories-plus-snowflake-btn") return false;
        const txt = (b.textContent || "").trim();
        return txt === "Create" || txt === "+ Create" || txt === "Add Memory" || txt === "Create Memory";
      });
      if (createBtnCandidate) {
        const r = createBtnCandidate.getBoundingClientRect();
        if (r && r.width > 0 && r.height > 0) {
          const topStr = Math.round(r.top) + "px";
          const leftStr = Math.round(r.right + 8) + "px";
          const heightStr = Math.round(r.height) + "px";
          if (btn.style.top !== topStr) btn.style.top = topStr;
          if (btn.style.left !== leftStr) btn.style.left = leftStr;
          if (btn.style.height !== heightStr) btn.style.height = heightStr;
        }
      }
      if (btn.style.zIndex !== "10") btn.style.zIndex = "10";
      if (btn.style.pointerEvents !== "auto") btn.style.pointerEvents = "auto";
      if (btn.style.display !== "inline-flex") btn.style.display = "inline-flex";
      ensureSnowflakeMemoriesSearchableCards();
    } else {
      if (btn.style.display !== "none") btn.style.display = "none";
      if (btn.style.pointerEvents !== "none") btn.style.pointerEvents = "none";
      const cardsGrid = document.getElementById("sflf-memories-cards-grid");
      if (cardsGrid && !isMemoriesView && cardsGrid.style.display !== "none") {
        cardsGrid.style.display = "none";
      }
      const detailView = document.getElementById("sflf-memory-detail-view");
      if (detailView && !isMemoriesView && detailView.style.display !== "none") {
        detailView.style.display = "none";
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
    if (typeof window !== "undefined" && window.__sflfCurrentMemoryFlowId) {
      return window.__sflfCurrentMemoryFlowId;
    }
    if (_activeMemoryFlowId) return _activeMemoryFlowId;
    if (sfState && sfState.flow_id) return sfState.flow_id;
    return null;
  }
  window.getActiveWorkflowFlowId = getActiveWorkflowFlowId;
  window.extractCurrentFlowId = extractCurrentFlowId;

  // CTX-ARCH-05B, W2-R2-00, W2-R2-04A, W2-R2-14, W2-R4-01 & W2-R4-06B:
  // 3-Level Live Canvas Node Auto-Sync (Backend updated_node_templates + React Fiber template mutation + Radix combobox DOM sync)
  function _resolveFieldCategoryAndOptions(fieldName, dbs, primaryDb, schemasForDb, primarySchema, vis, pgInstances, pgTablesMap, activeCtx) {
    const key = String(fieldName || "").toLowerCase();
    if (
      key === "memory_base" ||
      key === "memory_catalog_ref" ||
      key === "memory_name" ||
      key.includes("memory_base") ||
      key.includes("memory_catalog")
    ) {
      const rawMems =
        Array.isArray(sfState.memories) && sfState.memories.length > 0
          ? sfState.memories
          : vis && Array.isArray(vis.memories)
            ? vis.memories
            : [];
      const memNames = rawMems
        .map(function (m) {
          return typeof m === "string" ? m : (m && m.name) || "";
        })
        .filter(Boolean);
      return { options: memNames, fallbackValue: memNames[0] || "" };
    }
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
    const pgInstances = Array.isArray(selCtx.postgres_instances)
      ? selCtx.postgres_instances
      : Array.isArray(vis.postgres_instances)
        ? vis.postgres_instances
        : (activeCtx.catalog && activeCtx.catalog.postgres_instances) || [];
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
        const wrapper = cbBtn.closest ? cbBtn.closest("[data-testid], .flex, div") : null;
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
    const nextState = (ev && ev.detail) || sfState;
    syncCanvasNodesWithSnowflakeContext(nextState);
    if (typeof syncCreateMemoryModalWithFlowContext === "function") {
      syncCreateMemoryModalWithFlowContext(nextState);
    }
    if (typeof syncStagesVolumesViewWithFlowContext === "function") {
      syncStagesVolumesViewWithFlowContext(nextState);
    }
  });
  window.addEventListener("snowflake:context-changed", function (ev) {
    const nextState = (ev && ev.detail) || sfState;
    syncCanvasNodesWithSnowflakeContext(nextState);
    if (typeof syncCreateMemoryModalWithFlowContext === "function") {
      syncCreateMemoryModalWithFlowContext(nextState);
    }
    if (typeof syncStagesVolumesViewWithFlowContext === "function") {
      syncStagesVolumesViewWithFlowContext(nextState);
    }
  });

  function scanAll() {
    ensureBrowserTabBranding();
    styleCreateFirstFlowButtons();
    ensureDualSymbolLoadingOverlay();
    ensureMemoriesPageSnowflakeButton();
    if (typeof enhanceNativeCreateMemoryDialog === "function") {
      enhanceNativeCreateMemoryDialog();
    }
    if (typeof ensureLeftRailStagesVolumesTab === "function") {
      ensureLeftRailStagesVolumesTab();
    }
    alignTopbarControls();

    // CTX-ARCH-01 & CTX-ARCH-02: Per-workflow (/flow/<flow_id>) context lifecycle & [ ❄️ Context ] button visibility
    const currentFlowId = getActiveWorkflowFlowId();
    const hubBtn = document.getElementById("sflf-open-hub-btn");
    if (hubBtn) {
      // Outside /flow/<flow_id> (Home, Flows list, Memories, Flow Activity, Settings): hide [ ❄️ Context ]
      if (hubBtn.style.display !== (currentFlowId ? "inline-flex" : "none")) {
        hubBtn.style.display = currentFlowId ? "inline-flex" : "none";
      }
      if (hubBtn.getAttribute("data-flow-id") !== (currentFlowId || "")) {
        hubBtn.setAttribute("data-flow-id", currentFlowId || "");
      }
    }
    const pillEl = document.getElementById("sflf-topbar-context-pill");
    if (pillEl && pillEl.style.display !== "none") {
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
    const canCreateExternalVolume =
      typeof serverPrivs.can_create_external_volume === "boolean"
        ? serverPrivs.can_create_external_volume
        : !isReadOnlyRole && (role === "ACCOUNTADMIN" || role === "SYSADMIN" || role.includes("ADMIN"));
    const canCreateObject =
      typeof serverPrivs.can_create_object === "boolean" ? serverPrivs.can_create_object : !isReadOnlyRole;

    return {
      can_create_database: canCreateDb,
      can_create_schema: canCreateSchema,
      can_create_table: canCreateTable,
      can_create_stage: canCreateStage,
      can_create_view: canCreateView,
      can_create_postgres: canCreatePostgres,
      can_create_external_volume: canCreateExternalVolume,
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
      if (typeof syncCreateMemoryModalWithFlowContext === "function") {
        syncCreateMemoryModalWithFlowContext(sfState);
      }
      await fetchSnowflakeMemoriesRegistry(effectiveFlowId);
      if (typeof window.__sflfRefetchMemories === "function") {
        try { window.__sflfRefetchMemories(); } catch (_e) {}
      }
      ensureSnowflakeMemoriesSearchableCards();
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
          data.message || "Unable to update primary Snowflake context."
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
      if (typeof syncCreateMemoryModalWithFlowContext === "function") {
        syncCreateMemoryModalWithFlowContext(sfState);
      }
      await fetchSnowflakeMemoriesRegistry(effectiveFlowId);
      if (typeof window.__sflfRefetchMemories === "function") {
        try { window.__sflfRefetchMemories(); } catch (_e) {}
      }
      ensureSnowflakeMemoriesSearchableCards();
      window.dispatchEvent(new CustomEvent("snowflake:context-updated", { detail: sfState }));
      window.dispatchEvent(new CustomEvent("snowflake:context-changed", { detail: sfState }));
      if (!silentBanner) {
        showDenialOrStatusBanner(
          "ok",
          "CONTEXT_UPDATED",
          `Active context saved${effectiveFlowId ? " for workflow " + effectiveFlowId : ""}: ${sfState.database}.${sfState.schema} (Role: ${sfState.active_role})`
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
          data.message || `Active role (${sfState.active_role}) does not permit this operation.`
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
      if (typeof syncCreateMemoryModalWithFlowContext === "function") {
        syncCreateMemoryModalWithFlowContext(sfState);
      }
      window.dispatchEvent(new CustomEvent("snowflake:context-updated", { detail: sfState }));
      window.dispatchEvent(new CustomEvent("snowflake:context-changed", { detail: sfState }));
      showDenialOrStatusBanner(
        "ok",
        "ROLE_SWITCHED",
        `Active role: ${sfState.active_role} (epoch #${sfState.context_epoch})`
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
      )}<span>Context</span></button>
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
    if (typeof syncCreateMemoryModalWithFlowContext === "function") {
      syncCreateMemoryModalWithFlowContext(sfState);
    }
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
          // Legacy CTX-ARCH-05 marker: Non accessibile col ruolo
          const dbLockBadge = dbLocked
            ? `<span class="sflf-role-lock-badge" style="color:#f59e0b;font-size:10px;font-weight:600;">🔒 Not accessible with role ${sfState.active_role}</span>`
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
                                  ? `<span class="sflf-role-lock-badge" style="color:#f59e0b;font-size:10px;font-weight:600;">🔒 Not accessible with role ${sfState.active_role}</span>`
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
            ? `<span class="sflf-role-lock-badge" style="color:#f59e0b;font-size:10px;font-weight:600;">🔒 Not accessible with role ${sfState.active_role}</span>`
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
          : "+ Create New Database (Insufficient privileges for active role)",
      });
    }
    if (provSchSel && !provSchSel.dataset.userEditing) {
      populateSelectOptions(provSchSel, schemas, activeSchema, {
        value: "__CREATE_NEW_SCHEMA__",
        label: privs.can_create_schema
          ? "+ Create New Schema..."
          : "+ Create New Schema (Insufficient privileges for active role)",
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
      rbacWarnBox.textContent = `Active role (${sfState.active_role}) lacks privileges to create this object in ${targetDb}.${targetSch}.`;
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
        noteEl.textContent = `Vector column type locked to VECTOR(${elemType}, ${dimVal}) by the selected embedding model (AI_EMBED). Active governed constraints: VECTOR is not permitted in Iceberg Tables, nested inside VARIANT, or as a primary key / secondary index in Hybrid Tables.`;
      } else if (category === "semi_structured") {
        noteEl.textContent =
          "Snowflake Semi-Structured Memory (VARIANT / OBJECT / ARRAY). Note: VECTOR values cannot be nested inside VARIANT columns.";
      } else if (category === "postgres_non_vector") {
        noteEl.textContent =
          "Relational / JSONB Memory on Snowflake Postgres (:5432) without pgvector for conversational state and structured payloads.";
      } else {
        noteEl.textContent =
          "Snowflake Relational Structured Memory (Standard, Hybrid, Interactive, Dynamic Table) governed by the active role.";
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
            <strong style="font-size:14px;color:#29B5E8;white-space:nowrap;">Flow Context</strong>
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
            <p style="margin:0 0 10px 0;font-size:11px;color:#a1a1aa;">Select in cascade <strong>1. Snowflake Hierarchy (Database ▸ Schema ▸ Category ▸ Object)</strong> and <strong>2. Snowflake Postgres (Account-Level Instances)</strong> isolated for this workflow and governed by <code>CURRENT_ROLE()</code>. All Snowflake nodes on the canvas synchronize instantaneously in <code>&lt; 5 ms</code>.</p>
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

  // W2-R3-05, W2-R3-06, W2-R4-02, W2-R4-03, W2-R4-04 & W2-R4-05:
  // Intermediate Hover Dropdown (#1..#5 + #6 Coming Soon), Compact 3-Step Paginated Wizard, & Memory Detail View

  function closeSnowflakeMemoryTypeDropdown() {
    const dd = document.getElementById("sflf-memory-type-dropdown");
    if (dd) {
      dd.style.display = "none";
    }
  }

  function openSnowflakeMemoryTypeDropdown(flowIdFromReact, anchorEventOrEl) {
    if (flowIdFromReact && typeof flowIdFromReact === "string") {
      _activeMemoryFlowId = flowIdFromReact;
    }
    if (!document.body) return;
    let dd = document.getElementById("sflf-memory-type-dropdown");
    if (!dd) {
      dd = document.createElement("div");
      dd.id = "sflf-memory-type-dropdown";
      dd.setAttribute("data-testid", "snowflake-memory-type-dropdown");
      dd.style.cssText =
        "position:fixed;top:114px;left:24px;z-index:99995;display:none;background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:12px;box-shadow:0 18px 42px rgba(15,23,42,0.20);padding:10px;font-family:Inter,sans-serif;font-size:12px;width:min(94vw,680px);grid-template-columns:minmax(250px,1.08fr) minmax(240px,1fr);gap:10px;box-sizing:border-box;";
      document.body.appendChild(dd);
    }

    const allItems = AGENT_MEMORY_TYPES.concat(COMING_SOON_MEMORY_TYPES);
    const itemsHtml = allItems
      .map(function (item) {
        const isDisabled = item.enabled === false || item.id === "business_ontology_cortex_sense";
        const badgeText = isDisabled ? item.badge || "Coming Soon" : item.needs_embedding_badge || "NO";
        return `<button type="button"
          class="sflf-mem-type-dd-item"
          data-testid="snowflake-memory-type-item-${item.id}"
          data-memory-type-id="${item.id}"
          ${isDisabled ? 'disabled aria-disabled="true"' : ""}
          style="display:flex;align-items:center;justify-content:space-between;gap:8px;width:100%;text-align:left;padding:8px 10px;border-radius:8px;border:1px solid ${
            isDisabled ? "#e2e8f0" : "#bae6fd"
          };background:${isDisabled ? "#f8fafc" : "#ffffff"};color:${
            isDisabled ? "#94a3b8" : "#0f172a"
          };opacity:${isDisabled ? "0.55" : "1"};cursor:${
            isDisabled ? "not-allowed" : "pointer"
          };font-size:11.5px;font-weight:600;box-sizing:border-box;">
          <span>${item.short_label || item.label}</span>
          <span style="font-size:10px;padding:1px 7px;border-radius:999px;background:${
            isDisabled ? "#e2e8f0" : "rgba(41,181,232,0.14)"
          };color:${isDisabled ? "#64748b" : "#0284c7"};white-space:nowrap;">${badgeText}</span>
        </button>`;
      })
      .join("");

    dd.innerHTML = `
      <div style="display:flex;flex-direction:column;gap:5px;border-right:1px solid #e2e8f0;padding-right:8px;">
        <div style="display:flex;align-items:center;justify-content:space-between;padding:2px 4px 6px 4px;border-bottom:1px solid #f1f5f9;">
          <strong style="color:#0284c7;font-size:11.5px;">Select Snowflake Memory Type (#1..#5)</strong>
          <button type="button" id="sflf-memory-type-dd-close" style="background:transparent;border:none;color:#64748b;cursor:pointer;font-size:12px;">✕</button>
        </div>
        ${itemsHtml}
      </div>
      <div id="sflf-memory-type-hover-card" data-testid="snowflake-memory-type-hover-tooltip" style="background:#f8fafc;border:1px solid #bae6fd;border-radius:10px;padding:10px 12px;display:flex;flex-direction:column;gap:7px;justify-content:space-between;">
        <div>
          <div id="sflf-dd-hover-title" style="font-weight:700;color:#0284c7;font-size:12px;margin-bottom:4px;"></div>
          <div style="font-size:11px;color:#334155;margin-bottom:6px;"><strong>What It Does (Purpose):</strong> <span id="sflf-dd-hover-purpose"></span></div>
          <div style="font-size:11px;color:#334155;margin-bottom:6px;"><strong>Recommended Snowflake Objects:</strong> <span id="sflf-dd-hover-objects"></span></div>
          <div style="font-size:11px;color:#334155;"><strong>Embedding Required?:</strong> <span id="sflf-dd-hover-embed" style="color:#0369a1;font-weight:700;"></span></div>
        </div>
        <div style="font-size:10px;color:#64748b;border-top:1px solid #e2e8f0;padding-top:6px;">
          Branch A: <code>Database ▸ Schema ▸ Table</code> · Branch B: <code>Snowflake Postgres (:5432)</code>
        </div>
      </div>
    `;

    function updateHoverTooltip(typeId) {
      const spec = resolveCanonicalMemoryTypeSpec(typeId);
      const tEl = document.getElementById("sflf-dd-hover-title");
      const pEl = document.getElementById("sflf-dd-hover-purpose");
      const oEl = document.getElementById("sflf-dd-hover-objects");
      const eEl = document.getElementById("sflf-dd-hover-embed");
      if (tEl) tEl.textContent = spec.label || "";
      if (pEl) pEl.textContent = spec.purpose || spec.description || "";
      if (oEl) oEl.textContent = (spec.recommended_snowflake_objects || []).join(" · ");
      if (eEl) eEl.textContent = spec.needs_embedding_badge || "NO";
    }

    updateHoverTooltip("graph_state_checkpointer");

    Array.from(dd.querySelectorAll(".sflf-mem-type-dd-item")).forEach(function (btnEl) {
      const tid = btnEl.getAttribute("data-memory-type-id") || "graph_state_checkpointer";
      btnEl.addEventListener("mouseenter", function () {
        updateHoverTooltip(tid);
      });
      btnEl.addEventListener("focus", function () {
        updateHoverTooltip(tid);
      });
      btnEl.addEventListener("click", function () {
        if (tid === "business_ontology_cortex_sense" || btnEl.disabled) {
          return;
        }
        closeSnowflakeMemoryTypeDropdown();
        openCreateSnowflakeMemoryModal(extractCurrentFlowId(), tid);
      });
    });

    const closeBtn = document.getElementById("sflf-memory-type-dd-close");
    if (closeBtn) {
      closeBtn.onclick = closeSnowflakeMemoryTypeDropdown;
    }

    let anchorRect = null;
    if (anchorEventOrEl && typeof anchorEventOrEl.getBoundingClientRect === "function") {
      anchorRect = anchorEventOrEl.getBoundingClientRect();
    } else if (anchorEventOrEl && anchorEventOrEl.currentTarget && typeof anchorEventOrEl.currentTarget.getBoundingClientRect === "function") {
      anchorRect = anchorEventOrEl.currentTarget.getBoundingClientRect();
    } else {
      const btnCandidate =
        document.querySelector('aside button[data-testid="memories-plus-snowflake-btn"]') ||
        document.getElementById("sflf-memories-plus-snowflake-btn");
      if (btnCandidate && typeof btnCandidate.getBoundingClientRect === "function") {
        anchorRect = btnCandidate.getBoundingClientRect();
      }
    }
    if (anchorRect && anchorRect.width > 0) {
      dd.style.top = Math.min(window.innerHeight - 320, Math.round(anchorRect.bottom + 6)) + "px";
      dd.style.left = Math.max(12, Math.min(window.innerWidth - 690, Math.round(anchorRect.left))) + "px";
    }
    dd.style.display = "grid";
  }
  window.openSnowflakeMemoryTypeDropdown = openSnowflakeMemoryTypeDropdown;

  const SFLF_SUMMARIZER_MODELS_BY_ENGINE = {
    cortex_sql: [
      "mistral-large2",
      "llama3.3-70b",
      "llama3.1-70b",
      "claude-3-7-sonnet",
      "claude-sonnet-4-5",
      "openai-gpt-5",
      "deepseek-r1",
    ],
    cortex_summarize_agg: ["AI_SUMMARIZE_AGG"],
    proxy_chat: ["openai-gpt-6-luna", "openai-gpt-5", "mistral-large2"],
    proxy_messages: ["claude-sonnet-5-5", "claude-3-7-sonnet", "claude-sonnet-4-5"],
    proxy_responses: ["openai-gpt-5", "openai-gpt-6-luna"],
    spcs_ollama: ["llama3.2:3b-instruct", "qwen2.5:7b-instruct", "mistral:7b"],
    spcs_model_registry: ["CUSTOM_SUMMARIZER_V1", "SNOWFLAKE_AGENT_SUMMARY_MODEL"],
  };

  function syncCreateMemoryModalWithFlowContext(state) {
    const st = state || sfState || {};
    const selCtx = st.selected_context || {};
    const activeRole = st.active_role || "LANGFLOW_DEV_ROLE";
    const activeDb = st.database || "SNOWFLAKE_LEARNING_DB";
    const activeSchema = st.schema || "PUBLIC";

    const roleEl = document.getElementById("sflf-light-mem-role");
    if (roleEl) roleEl.textContent = activeRole;

    const dbSel = document.getElementById("sflf-light-mem-db");
    const dbNewInput = document.getElementById("sflf-light-mem-db-new");
    const databases = Array.isArray(selCtx.databases) && selCtx.databases.length
      ? selCtx.databases.slice()
      : [activeDb, "SNOWFLAKE_LEARNING_DB", "CORTEX_GOVERNANCE_DB"];
    if (databases.indexOf(activeDb) === -1) databases.unshift(activeDb);
    if (dbSel && dbSel.tagName === "SELECT") {
      const prevDb = dbSel.value && dbSel.value !== "__create_new_db__" ? dbSel.value : activeDb;
      dbSel.innerHTML =
        databases.map(function (d) { return `<option value="${d}">${d}</option>`; }).join("") +
        `<option value="__create_new_db__">+ Create New Database...</option>`;
      dbSel.value = databases.indexOf(prevDb) !== -1 ? prevDb : activeDb;
      if (dbNewInput) dbNewInput.style.display = dbSel.value === "__create_new_db__" ? "block" : "none";
    }

    const effectiveDb =
      dbSel && dbSel.value === "__create_new_db__"
        ? (dbNewInput && dbNewInput.value.trim()) || activeDb
        : (dbSel && dbSel.value) || activeDb;

    const schSel = document.getElementById("sflf-light-mem-schema");
    const schNewInput = document.getElementById("sflf-light-mem-schema-new");
    const schemasByDb = selCtx.schemas_by_db || {};
    const rawSchemas = Array.isArray(schemasByDb[effectiveDb]) && schemasByDb[effectiveDb].length
      ? schemasByDb[effectiveDb].slice()
      : [activeSchema, "PUBLIC", "ANALYTICS", "CORTEX_MEMORY"];
    if (rawSchemas.indexOf(activeSchema) === -1 && effectiveDb === activeDb) rawSchemas.unshift(activeSchema);
    if (schSel && schSel.tagName === "SELECT") {
      const prevSch = schSel.value && schSel.value !== "__create_new_schema__" ? schSel.value : activeSchema;
      schSel.innerHTML =
        rawSchemas.map(function (s) { return `<option value="${s}">${s}</option>`; }).join("") +
        `<option value="__create_new_schema__">+ Create New Schema...</option>`;
      schSel.value = rawSchemas.indexOf(prevSch) !== -1 ? prevSch : rawSchemas[0] || activeSchema;
      if (schNewInput) schNewInput.style.display = schSel.value === "__create_new_schema__" ? "block" : "none";
    }

    const effectiveSchema =
      schSel && schSel.value === "__create_new_schema__"
        ? (schNewInput && schNewInput.value.trim()) || activeSchema
        : (schSel && schSel.value) || activeSchema;

    const existingTableSel = document.getElementById("sflf-light-mem-existing-table");
    const objsByDbSchema = selCtx.objects_by_db_schema || {};
    const dbSchemaKey = `${effectiveDb}.${effectiveSchema}`;
    const scopeEntry = objsByDbSchema[dbSchemaKey] || {};
    const tablesInScope = Array.isArray(scopeEntry.tables) && scopeEntry.tables.length
      ? scopeEntry.tables.slice()
      : ["AGENT_MEMORY_STORE", "MEM_1_GRAPH_CHECKPOINTER", "MEM_2_BUFFER_WINDOW", "MEM_3_RUNNING_SUMMARY", "MEM_4_USER_PROFILE", "MEM_5_EPISODIC_FEW_SHOT"];
    if (existingTableSel && existingTableSel.tagName === "SELECT") {
      const prevTbl = existingTableSel.value;
      existingTableSel.innerHTML = tablesInScope
        .map(function (t) { return `<option value="${t}">${t}</option>`; })
        .join("");
      existingTableSel.value = tablesInScope.indexOf(prevTbl) !== -1 ? prevTbl : tablesInScope[0];
    }

    const pgInstSel = document.getElementById("sflf-light-mem-pg-instance");
    const pgInstances = Array.isArray(selCtx.postgres_instances)
      ? selCtx.postgres_instances.slice()
      : ["LANGFLOW_PG_INSTANCE", "SNOWFLAKE_PG_MEMORY_POOL"];
    if (pgInstSel && pgInstSel.tagName === "SELECT") {
      const prevPg = pgInstSel.value;
      if (pgInstances.length === 0) {
        pgInstSel.innerHTML = `<option value="">(No Postgres Instance in ❄️ Context)</option>`;
        pgInstSel.value = "";
      } else {
        pgInstSel.innerHTML = pgInstances
          .map(function (inst) { return `<option value="${inst}">${inst}</option>`; })
          .join("");
        pgInstSel.value = pgInstances.indexOf(prevPg) !== -1 ? prevPg : pgInstances[0];
      }
    }

    const pgTableSel = document.getElementById("sflf-light-mem-pg-table");
    const pgTableNewInput = document.getElementById("sflf-light-mem-pg-table-new");
    const activePgInst = (pgInstSel && pgInstSel.value) || pgInstances[0] || "LANGFLOW_PG_INSTANCE";
    const pgTablesMap = selCtx.postgres_tables_by_instance || {};
    const pgTables = Array.isArray(pgTablesMap[activePgInst]) && pgTablesMap[activePgInst].length
      ? pgTablesMap[activePgInst].slice()
      : ["langgraph_checkpoints", "agent_chat_history", "agent_user_profile_jsonb", "agent_episodic_vectors"];
    if (pgTableSel && pgTableSel.tagName === "SELECT") {
      const prevPgTbl = pgTableSel.value && pgTableSel.value !== "__create_new_pg_table__" ? pgTableSel.value : pgTables[0];
      pgTableSel.innerHTML =
        pgTables.map(function (t) { return `<option value="${t}">${t}</option>`; }).join("") +
        `<option value="__create_new_pg_table__">+ Create New Postgres Table...</option>`;
      pgTableSel.value = pgTables.indexOf(prevPgTbl) !== -1 ? prevPgTbl : pgTables[0];
      if (pgTableNewInput) pgTableNewInput.style.display = pgTableSel.value === "__create_new_pg_table__" ? "block" : "none";
    }

    const whSel = document.getElementById("sflf-light-mem-warehouse");
    const warehouses = Array.isArray(selCtx.warehouses) && selCtx.warehouses.length
      ? selCtx.warehouses.slice()
      : [st.warehouse || "COMPUTE_WH", "CORTEX_WH", "INTERACTIVE_WH"];
    if (whSel && whSel.tagName === "SELECT") {
      const prevWh = whSel.value || st.warehouse || warehouses[0];
      whSel.innerHTML = warehouses.map(function (w) { return `<option value="${w}">${w}</option>`; }).join("");
      whSel.value = warehouses.indexOf(prevWh) !== -1 ? prevWh : warehouses[0];
    }
  }
  window.syncCreateMemoryModalWithFlowContext = syncCreateMemoryModalWithFlowContext;

  function openCreateSnowflakeMemoryModal(flowIdFromReact, preselectedTypeId) {
    if (flowIdFromReact && typeof flowIdFromReact === "string") {
      _activeMemoryFlowId = flowIdFromReact;
    }
    closeSnowflakeMemoryTypeDropdown();

    let modal = document.getElementById("sflf-create-memory-modal");
    if (!modal) {
      modal = document.createElement("div");
      modal.id = "sflf-create-memory-modal";
      modal.setAttribute("data-testid", "create-snowflake-memory-modal");
      modal.setAttribute("role", "dialog");
      modal.style.cssText =
        "position:fixed;inset:0;background:rgba(15,23,42,0.45);backdrop-filter:blur(3px);z-index:99998;display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box;font-family:Inter,sans-serif;";

      const lightInputStyle =
        "background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;padding:6px 9px;border-radius:8px;width:100%;min-width:0;box-sizing:border-box;font-size:12px;";
      const lightSelectStyle =
        "background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;padding:6px 9px;border-radius:8px;width:100%;min-width:0;box-sizing:border-box;font-size:12px;";
      const helperCaptionStyle = "display:block;font-size:10px;color:#64748b;margin-top:2px;line-height:1.3;";

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
        <div class="sflf-modal-dialog-surface" style="background:#ffffff;color:#0f172a;border:1px solid #e2e8f0;border-radius:12px;width:clamp(340px,94vw,860px);max-width:96vw;display:flex;flex-direction:column;overflow:hidden;padding:18px 22px;box-shadow:0 20px 50px rgba(15,23,42,0.18);font-size:12px;box-sizing:border-box;">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;border-bottom:1px solid #e2e8f0;padding-bottom:10px;margin-bottom:12px;flex-shrink:0;">
            <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
              <span style="display:inline-flex;">${buildOfficialSnowflakeSvg("#29B5E8", 20)}</span>
              <strong style="font-size:15px;color:#0f172a;">Create a Snowflake Memory</strong>
              <span id="sflf-mem-wizard-step-badge" data-testid="snowflake-memory-wizard-step-badge" style="background:#e0f2fe;color:#0369a1;border:1px solid #38bdf8;border-radius:999px;padding:2px 9px;font-size:11px;font-weight:700;">Step 1 of 3 · Storage Target</span>
              <span style="background:rgba(41,181,232,0.12);color:#0284c7;border:1px solid #29B5E8;border-radius:6px;padding:2px 8px;font-size:11px;font-weight:600;">Role: <span id="sflf-light-mem-role">${sfState.active_role}</span></span>
            </div>
            <button type="button" id="sflf-light-mem-close" style="background:#f8fafc;border:1px solid #cbd5e1;color:#334155;padding:4px 10px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">✕</button>
          </div>

          <!-- Wizard Step 1 of 3: Storage Target (Flow Context & Branch A vs Branch B) -->
          <div id="sflf-mem-wizard-step-1" data-testid="snowflake-memory-wizard-step-1" style="display:flex;flex-direction:column;gap:10px;">
            <div id="sflf-mem-step1-intro" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:7px 11px;font-size:11px;color:#0369a1;line-height:1.35;">
              <strong>Step 1 — Choose Memory Pattern &amp; Storage Location:</strong> Select which of the 5 Agent Memory patterns (#1..#5) you need and bind it to either a governed Snowflake SQL Table (Branch A) or an account-level Snowflake Postgres instance (Branch B).
            </div>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,250px),1fr));gap:10px;">
              <div>
                <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:3px;">Memory Name</label>
                <input id="sflf-light-mem-name" data-testid="snowflake-memory-name-input" value="AGENT_MEMORY_STORE" placeholder="e.g. MEM_1_GRAPH_CHECKPOINTER" style="${lightInputStyle}" />
              </div>
              <div>
                <label style="display:block;font-size:11px;font-weight:600;color:#334155;margin-bottom:3px;">Snowflake Memory Type (#1..#5)</label>
                <select id="sflf-light-mem-type" data-testid="snowflake-memory-type-select" style="${lightSelectStyle}">
                  ${typeOptionsHtml}
                </select>
              </div>
            </div>

            <div id="sflf-light-mem-guidance-card" style="background:#f8fafc;border:1px solid #bae6fd;border-radius:10px;padding:9px 12px;display:flex;flex-direction:column;gap:4px;">
              <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
                <strong id="sflf-light-mem-guidance-title" style="color:#0284c7;font-size:12px;"></strong>
                <span id="sflf-light-mem-embed-badge" style="background:#e0f2fe;color:#0369a1;border:1px solid #38bdf8;border-radius:999px;padding:1px 9px;font-size:10px;font-weight:700;"></span>
              </div>
              <div style="font-size:11px;color:#334155;"><strong>Recommended Snowflake Object:</strong> <span id="sflf-light-mem-rec-objs"></span></div>
              <div style="font-size:11px;color:#475569;"><strong>When Embedding Is Useful:</strong> <span id="sflf-light-mem-when-embed"></span></div>
            </div>

            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:9px 12px;display:flex;flex-direction:column;gap:8px;">
              <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
                <label style="font-size:11px;font-weight:700;color:#0f172a;">Storage Backend Family (Flow Context):</label>
                <select id="sflf-light-mem-storage-family" data-testid="snowflake-memory-storage-family" style="background:#ffffff;color:#0f172a;border:1px solid #29B5E8;padding:4px 10px;border-radius:8px;font-size:11.5px;font-weight:600;">
                  <option value="snowflake_sql">Branch A — Snowflake SQL (Database ▸ Schema ▸ Table)</option>
                  <option value="snowflake_postgres">Branch B — Snowflake Postgres (Account-Level :5432)</option>
                </select>
              </div>

              <!-- Branch A: Database ▸ Schema ▸ Table -->
              <div id="sflf-mem-branch-a-sql-box" data-testid="snowflake-memory-branch-a-sql" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,180px),1fr));gap:10px;">
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">1. Database (from Flow Context)</label>
                  <select id="sflf-light-mem-db" style="${lightSelectStyle}"></select>
                  <input id="sflf-light-mem-db-new" placeholder="New Database Name (e.g. AGENT_MEM_DB)" style="display:none;margin-top:4px;${lightInputStyle}" />
                </div>
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">2. Schema (from Flow Context)</label>
                  <select id="sflf-light-mem-schema" style="${lightSelectStyle}"></select>
                  <input id="sflf-light-mem-schema-new" placeholder="New Schema Name (e.g. AGENT_MEMORY)" style="display:none;margin-top:4px;${lightInputStyle}" />
                </div>
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">3. Snowflake Table Subtype</label>
                  <select id="sflf-light-mem-target-obj" style="${lightSelectStyle}"></select>
                </div>
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Action Mode</label>
                  <select id="sflf-light-mem-action-mode" style="${lightSelectStyle}">
                    <option value="create_new">Create New Table in Schema</option>
                    <option value="bind_existing">Use Existing Table in Context</option>
                  </select>
                </div>
                <div id="sflf-mem-new-table-wrap">
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">New Physical Table Name</label>
                  <input id="sflf-light-mem-table-name" value="AGENT_MEMORY_STORE" placeholder="e.g. MEM_1_GRAPH_CHECKPOINTER" style="${lightInputStyle}" />
                </div>
                <div id="sflf-mem-existing-table-wrap" style="display:none;">
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Existing Table in Schema</label>
                  <select id="sflf-light-mem-existing-table" style="${lightSelectStyle}"></select>
                </div>
              </div>

              <!-- Branch B: Account-Level Snowflake Postgres (:5432) outside Database ▸ Schema -->
              <div id="sflf-mem-branch-b-pg-box" data-testid="snowflake-memory-branch-b-postgres" style="display:none;grid-template-columns:repeat(auto-fit,minmax(min(100%,180px),1fr));gap:10px;">
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Postgres Instance (:5432 Account-Level)</label>
                  <select id="sflf-light-mem-pg-instance" style="${lightSelectStyle}"></select>
                </div>
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Postgres DB / Schema</label>
                  <input id="sflf-light-mem-pg-schema" value="postgres.public" style="${lightInputStyle}" />
                </div>
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Postgres Table</label>
                  <select id="sflf-light-mem-pg-table" style="${lightSelectStyle}"></select>
                  <input id="sflf-light-mem-pg-table-new" placeholder="New Postgres Table Name" style="display:none;margin-top:4px;${lightInputStyle}" />
                </div>
                <div>
                  <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Postgres Table Mode</label>
                  <select id="sflf-light-mem-pg-mode" style="${lightSelectStyle}">
                    <option value="PostgresSaver">PostgresSaver (#1 Checkpointer)</option>
                    <option value="Relational / JSONB">Relational / JSONB (#2 Window / #3 Summary)</option>
                    <option value="PostgresStore JSONB">PostgresStore JSONB (#4 User Profile)</option>
                    <option value="JSONB + pgvector">JSONB + pgvector (#5 Episodic)</option>
                  </select>
                </div>
              </div>
            </div>
          </div>

          <!-- Wizard Step 2 of 3: Object & Memory Configuration (Dynamic Single-Purpose Fields) -->
          <div id="sflf-mem-wizard-step-2" data-testid="snowflake-memory-wizard-step-2" style="display:none;flex-direction:column;gap:10px;">
            <div id="sflf-mem-step2-intro" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:7px 11px;font-size:11px;color:#0369a1;line-height:1.35;">
              <strong>Step 2 — Physical Storage &amp; Memory Behavior Settings:</strong> Configure the physical table indexes/refresh rules for your chosen Snowflake storage subtype (Section A) and the runtime retention/summarization behavior for your memory pattern (Section B).
            </div>

            <!-- Section A: Physical Object Settings -->
            <div id="sflf-mem-object-peculiarities-box" data-testid="snowflake-memory-object-peculiarities" style="background:#f8fafc;border:1px solid #bae6fd;border-radius:10px;padding:10px 12px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,195px),1fr));gap:10px;">
              <div id="sflf-step2-field-hybrid-pk">
                <label id="sflf-label-hybrid-pk" style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Primary Key Columns</label>
                <input id="sflf-light-mem-hybrid-pk" value="(session_id, turn_id)" style="${lightInputStyle}" />
                <span id="sflf-help-hybrid-pk" style="${helperCaptionStyle}">Defines the unique row identifier enforced by Snowflake Hybrid Tables for single-millisecond point lookups.</span>
              </div>
              <div id="sflf-step2-field-hybrid-idx">
                <label id="sflf-label-hybrid-idx" style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Secondary Index Columns</label>
                <input id="sflf-light-mem-hybrid-idx" value="(session_id, created_at)" style="${lightInputStyle}" />
                <span id="sflf-help-hybrid-idx" style="${helperCaptionStyle}">Speeds up secondary range scans by session and timestamp without scanning the full table.</span>
              </div>
              <div id="sflf-step2-field-cluster-by" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Cluster By Keys (Optional)</label>
                <input id="sflf-light-mem-cluster-keys" value="(session_id, created_at)" style="${lightInputStyle}" />
                <span style="${helperCaptionStyle}">Organizes micro-partitions on Standard Tables for faster pruning on large historical logs.</span>
              </div>
              <div id="sflf-step2-field-retention-days" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Time-Travel Retention (Days)</label>
                <input id="sflf-light-mem-retention-days" type="number" value="1" min="0" max="90" style="${lightInputStyle}" />
                <span style="${helperCaptionStyle}">Sets Snowflake DATA_RETENTION_TIME_IN_DAYS for point-in-time recovery and auditability.</span>
              </div>
              <div id="sflf-step2-field-target-lag" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Target Refresh Lag</label>
                <input id="sflf-light-mem-target-lag" value="5 minutes" style="${lightInputStyle}" />
                <span style="${helperCaptionStyle}">Maximum staleness window before Snowflake automatically refreshes the Dynamic or Interactive Table.</span>
              </div>
              <div id="sflf-step2-field-warehouse" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Compute Warehouse</label>
                <select id="sflf-light-mem-warehouse" style="${lightSelectStyle}">
                  <option value="COMPUTE_WH">COMPUTE_WH</option>
                </select>
                <span style="${helperCaptionStyle}">Snowflake virtual warehouse used to execute scheduled incremental refreshes.</span>
              </div>
              <div id="sflf-step2-field-refresh-mode" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Refresh Mode</label>
                <select id="sflf-light-mem-refresh-mode" style="${lightSelectStyle}">
                  <option value="INCREMENTAL">INCREMENTAL</option>
                  <option value="FULL">FULL</option>
                  <option value="AUTO">AUTO</option>
                </select>
                <span style="${helperCaptionStyle}">Selects incremental delta materialization vs full recomputation for Dynamic Tables.</span>
              </div>
              <div id="sflf-step2-field-pg-gin" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">JSONB GIN Index Columns</label>
                <input id="sflf-light-mem-pg-gin" value="metadata_jsonb" style="${lightInputStyle}" />
                <span style="${helperCaptionStyle}">Creates a PostgreSQL GIN index on JSONB payloads for fast containment queries on port 5432.</span>
              </div>
            </div>

            <!-- Section B: Memory Behavior Settings -->
            <div id="sflf-light-mem-type-params" style="background:#ffffff;border:1px solid #e2e8f0;border-radius:10px;padding:10px 12px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,190px),1fr));gap:10px;">
              <div id="sflf-step2-behav-key">
                <label id="sflf-label-behav-key" style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Conversation Session Key</label>
                <input id="sflf-light-mem-key-param" value="session_default" placeholder="session_id" style="${lightInputStyle}" />
                <span id="sflf-help-behav-key" style="${helperCaptionStyle}">Identifier key used to isolate conversation turns or checkpoints per thread, session, or user.</span>
              </div>
              <div id="sflf-step2-behav-k">
                <label id="sflf-label-behav-k" style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Sliding Window Size (Last K Messages)</label>
                <input id="sflf-light-mem-k-param" type="number" value="8" min="1" max="500" style="${lightInputStyle}" />
                <span id="sflf-help-behav-k" style="${helperCaptionStyle}">Controls how many recent turns, few-shot experiences, or TTL hours are retained and retrieved.</span>
              </div>
              <div id="sflf-step2-behav-sum-engine" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Summarization Engine</label>
                <select id="sflf-light-mem-sum-engine" style="${lightSelectStyle}">
                  <option value="cortex_sql">Cortex SQL — AI_COMPLETE (Select Model)</option>
                  <option value="cortex_summarize_agg">Cortex Native — AI_SUMMARIZE_AGG (Automatic SQL Aggregation)</option>
                  <option value="proxy_chat">SPCS LLM Proxy — Chat Completions</option>
                  <option value="proxy_messages">SPCS LLM Proxy — Anthropic Messages</option>
                  <option value="spcs_ollama">Private SPCS — Ollama</option>
                </select>
                <span style="${helperCaptionStyle}">Selects whether running summaries use a specific LLM via AI_COMPLETE or native SQL AI_SUMMARIZE_AGG.</span>
              </div>
              <div id="sflf-step2-behav-sum-model" style="display:none;">
                <label style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Summarization LLM Model</label>
                <select id="sflf-light-mem-sum-model" style="${lightSelectStyle}"></select>
                <span style="${helperCaptionStyle}">Specific foundation model invoked when compressing conversation turns into a running summary.</span>
              </div>
              <div id="sflf-step2-behav-extra">
                <label id="sflf-label-behav-extra" style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Memory Namespace</label>
                <input id="sflf-light-mem-extra-param" value="user.preferences" placeholder="user.preferences" style="${lightInputStyle}" />
                <span id="sflf-help-behav-extra" style="${helperCaptionStyle}">Logical namespace grouping extracted facts and preferences for the target user or entity.</span>
              </div>
              <div id="sflf-step2-behav-score">
                <label id="sflf-label-behav-score" style="display:block;font-size:10.5px;font-weight:600;color:#475569;margin-bottom:3px;">Minimum Outcome Score Threshold (0.00 - 1.00)</label>
                <input id="sflf-light-mem-score-param" type="number" step="0.05" value="0.70" style="${lightInputStyle}" />
                <span id="sflf-help-behav-score" style="${helperCaptionStyle}">Filters episodic trajectories so only successful past executions above this score are injected as few-shot examples.</span>
              </div>
            </div>
          </div>

          <!-- Wizard Step 3 of 3: Optional Embedding, LLM Preprocessing & Create -->
          <div id="sflf-mem-wizard-step-3" data-testid="snowflake-memory-wizard-step-3" style="display:none;flex-direction:column;gap:10px;">
            <div id="sflf-mem-step3-intro" style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:7px 11px;font-size:11px;color:#0369a1;line-height:1.35;">
              <strong>Step 3 — Optional Vector Embedding, Smart Preprocessing &amp; Provisioning:</strong> Enable semantic vector search only if needed, configure automatic LLM fact extraction or summarization on flow runs, and review the governed Snowflake DDL before creating.
            </div>
            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:9px 12px;">
              <label style="display:inline-flex;align-items:center;gap:8px;font-size:12px;font-weight:600;color:#0f172a;cursor:pointer;">
                <input type="checkbox" id="sflf-light-mem-enable-embedding" data-testid="snowflake-memory-enable-embedding" />
                <span>Enable Embedding Vectorization (Optional — Default OFF; locked OFF for #1 and #3)</span>
              </label>
              <div id="sflf-light-mem-embedding-box" style="display:none;margin-top:8px;grid-template-columns:repeat(auto-fit,minmax(min(100%,185px),1fr));gap:8px;">
                <div>
                  <label style="display:block;font-size:10px;font-weight:600;color:#475569;margin-bottom:2px;">Snowflake Embedding Source (4 Sources)</label>
                  <select id="sflf-light-mem-embed-provider" data-testid="snowflake-memory-embedding-provider" style="${lightSelectStyle}">
                    ${providerOptionsHtml}
                  </select>
                </div>
                <div>
                  <label style="display:block;font-size:10px;font-weight:600;color:#475569;margin-bottom:2px;">Embedding Model</label>
                  <select id="sflf-light-mem-embed-model" data-testid="snowflake-memory-embedding-model" style="${lightSelectStyle}"></select>
                </div>
                <div>
                  <label style="display:block;font-size:10px;font-weight:600;color:#475569;margin-bottom:2px;">Vector Dimension (1..4096)</label>
                  <input id="sflf-light-mem-embed-dim" type="number" value="768" min="1" max="4096" style="${lightInputStyle}" />
                </div>
              </div>
            </div>

            <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:9px 12px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,180px),1fr));gap:8px;">
              <label style="display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:600;color:#0f172a;">
                <input type="checkbox" id="sflf-light-mem-auto-capture" checked />
                <span>Auto-capture flow runs</span>
              </label>
              <label style="display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:600;color:#0f172a;">
                <input type="checkbox" id="sflf-light-mem-preproc-enabled" data-testid="snowflake-memory-preproc-enabled" />
                <span>Enable LLM Preprocessing</span>
              </label>
              <div>
                <label style="display:block;font-size:10px;font-weight:600;color:#475569;margin-bottom:2px;">Preprocessing / Summarizer Engine</label>
                <select id="sflf-light-mem-preproc-engine" style="${lightSelectStyle}">
                  <option value="cortex_sql">Cortex SQL — AI_COMPLETE (Select Model)</option>
                  <option value="cortex_summarize_agg">Cortex Native — AI_SUMMARIZE_AGG (Automatic SQL Aggregation)</option>
                  <option value="proxy_chat">SPCS LLM Proxy — Chat</option>
                  <option value="proxy_messages">SPCS LLM Proxy — Messages</option>
                  <option value="proxy_responses">SPCS LLM Proxy — Responses</option>
                  <option value="spcs_ollama">Private SPCS — Ollama</option>
                  <option value="spcs_model_registry">Model Registry SPCS</option>
                </select>
              </div>
              <div id="sflf-light-mem-preproc-model-wrap">
                <label style="display:block;font-size:10px;font-weight:600;color:#475569;margin-bottom:2px;">Preprocessing LLM Model</label>
                <select id="sflf-light-mem-preproc-model" style="${lightSelectStyle}"></select>
              </div>
              <div>
                <label style="display:block;font-size:10px;font-weight:600;color:#475569;margin-bottom:2px;">Kill Phrase</label>
                <input id="sflf-light-mem-kill-phrase" data-testid="snowflake-memory-kill-phrase" value="NO_INGEST" placeholder="Kill Phrase (NO_INGEST)" style="${lightInputStyle}" />
              </div>
            </div>

            <div>
              <div style="font-size:10.5px;font-weight:600;color:#334155;margin-bottom:3px;">Preview Query / DDL (without &amp; with Embedding):</div>
              <code id="sflf-light-mem-sql-preview" style="display:block;background:#0f172a;color:#e2e8f0;padding:7px 10px;border-radius:8px;font-family:'JetBrains Mono',monospace;font-size:10.5px;white-space:pre-wrap;max-height:76px;overflow-y:auto;"></code>
            </div>

            <div id="sflf-light-mem-out" style="background:#0f172a;color:#e2e8f0;padding:6px 10px;border-radius:8px;font-family:'JetBrains Mono',monospace;font-size:10.5px;max-height:72px;overflow-y:auto;">Ready to create or bind Snowflake Agent Memory under active role ${sfState.active_role}.</div>
          </div>

          <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;border-top:1px solid #e2e8f0;padding-top:10px;margin-top:10px;flex-shrink:0;">
            <div>
              <button type="button" id="sflf-mem-wizard-back-btn" data-testid="btn-back-snowflake-memory" style="display:none;background:#f8fafc;border:1px solid #cbd5e1;color:#334155;padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">&lt; Back</button>
            </div>
            <div style="display:flex;align-items:center;gap:8px;">
              <button type="button" id="sflf-light-mem-cancel-btn" data-testid="btn-cancel-snowflake-memory" style="background:#ffffff;border:1px solid #cbd5e1;color:#334155;padding:6px 14px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">Cancel</button>
              <button type="button" id="sflf-mem-wizard-next-btn" data-testid="btn-next-snowflake-memory" style="background:#29B5E8;border:1px solid #29B5E8;color:#ffffff;padding:6px 16px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">Next &gt;</button>
              <button type="button" id="sflf-light-mem-submit-btn" data-testid="btn-submit-snowflake-memory" style="display:none;background:#29B5E8;border:1px solid #29B5E8;color:#ffffff;padding:6px 16px;border-radius:8px;font-size:12px;font-weight:600;cursor:pointer;">+ ❄️ Create Memory</button>
            </div>
          </div>
        </div>
      `;
      document.body.appendChild(modal);
    }

    syncCreateMemoryModalWithFlowContext(sfState);

    const typeSel = document.getElementById("sflf-light-mem-type");
    const storageFamilySel = document.getElementById("sflf-light-mem-storage-family");
    const branchASqlBox = document.getElementById("sflf-mem-branch-a-sql-box");
    const branchBPgBox = document.getElementById("sflf-mem-branch-b-pg-box");
    const targetObjSel = document.getElementById("sflf-light-mem-target-obj");
    const actionModeSel = document.getElementById("sflf-light-mem-action-mode");
    const dbSel = document.getElementById("sflf-light-mem-db");
    const schSel = document.getElementById("sflf-light-mem-schema");
    const pgInstSel = document.getElementById("sflf-light-mem-pg-instance");
    const pgTableSel = document.getElementById("sflf-light-mem-pg-table");
    const enableEmbedCb = document.getElementById("sflf-light-mem-enable-embedding");
    const embedBox = document.getElementById("sflf-light-mem-embedding-box");
    const providerSel = document.getElementById("sflf-light-mem-embed-provider");
    const modelSel = document.getElementById("sflf-light-mem-embed-model");
    const dimInput = document.getElementById("sflf-light-mem-embed-dim");
    const preprocCb = document.getElementById("sflf-light-mem-preproc-enabled");
    const preprocEngineSel = document.getElementById("sflf-light-mem-preproc-engine");
    const preprocModelSel = document.getElementById("sflf-light-mem-preproc-model");
    const preprocModelWrap = document.getElementById("sflf-light-mem-preproc-model-wrap");
    const sumEngineSel = document.getElementById("sflf-light-mem-sum-engine");
    const sumModelSel = document.getElementById("sflf-light-mem-sum-model");

    if (preselectedTypeId && typeSel) {
      typeSel.value = preselectedTypeId;
    }

    function syncEngineModelDropdown(engineSelectEl, modelSelectEl, wrapEl) {
      const eng = (engineSelectEl && engineSelectEl.value) || "cortex_sql";
      const models = SFLF_SUMMARIZER_MODELS_BY_ENGINE[eng] || SFLF_SUMMARIZER_MODELS_BY_ENGINE.cortex_sql;
      if (modelSelectEl) {
        const prev = modelSelectEl.value;
        modelSelectEl.innerHTML = models.map(function (m) { return `<option value="${m}">${m}</option>`; }).join("");
        modelSelectEl.value = models.indexOf(prev) !== -1 ? prev : models[0];
      }
      if (wrapEl) {
        wrapEl.style.display = eng === "cortex_summarize_agg" ? "none" : "block";
      }
    }

    function syncActionModeVisibility() {
      const mode = (actionModeSel && actionModeSel.value) || "create_new";
      const newWrap = document.getElementById("sflf-mem-new-table-wrap");
      const existWrap = document.getElementById("sflf-mem-existing-table-wrap");
      if (newWrap) newWrap.style.display = mode === "bind_existing" ? "none" : "block";
      if (existWrap) existWrap.style.display = mode === "bind_existing" ? "block" : "none";
    }

    function syncStep2DynamicFields() {
      const fam = (storageFamilySel && storageFamilySel.value) || "snowflake_sql";
      const subtype = ((targetObjSel && targetObjSel.value) || "Hybrid Table").toLowerCase();
      const memType = (typeSel && typeSel.value) || "buffer_window_memory";

      const isHybrid = fam === "snowflake_sql" && subtype.indexOf("hybrid") !== -1;
      const isDynamicOrInteractive = fam === "snowflake_sql" && (subtype.indexOf("dynamic") !== -1 || subtype.indexOf("interactive") !== -1);
      const isStandard = fam === "snowflake_sql" && !isHybrid && !isDynamicOrInteractive;
      const isPostgres = fam === "snowflake_postgres";

      const pkBox = document.getElementById("sflf-step2-field-hybrid-pk");
      const idxBox = document.getElementById("sflf-step2-field-hybrid-idx");
      const clusterBox = document.getElementById("sflf-step2-field-cluster-by");
      const retBox = document.getElementById("sflf-step2-field-retention-days");
      const lagBox = document.getElementById("sflf-step2-field-target-lag");
      const whBox = document.getElementById("sflf-step2-field-warehouse");
      const refBox = document.getElementById("sflf-step2-field-refresh-mode");
      const ginBox = document.getElementById("sflf-step2-field-pg-gin");

      if (pkBox) pkBox.style.display = isHybrid ? "block" : "none";
      if (idxBox) idxBox.style.display = isHybrid ? "block" : "none";
      if (clusterBox) clusterBox.style.display = isStandard ? "block" : "none";
      if (retBox) retBox.style.display = isStandard ? "block" : "none";
      if (lagBox) lagBox.style.display = isDynamicOrInteractive ? "block" : "none";
      if (whBox) whBox.style.display = isDynamicOrInteractive ? "block" : "none";
      if (refBox) refBox.style.display = isDynamicOrInteractive ? "block" : "none";
      if (ginBox) ginBox.style.display = isPostgres ? "block" : "none";

      const keyWrap = document.getElementById("sflf-step2-behav-key");
      const kWrap = document.getElementById("sflf-step2-behav-k");
      const sumEngWrap = document.getElementById("sflf-step2-behav-sum-engine");
      const sumModWrap = document.getElementById("sflf-step2-behav-sum-model");
      const extraWrap = document.getElementById("sflf-step2-behav-extra");
      const scoreWrap = document.getElementById("sflf-step2-behav-score");
      const keyLbl = document.getElementById("sflf-label-behav-key");
      const kLbl = document.getElementById("sflf-label-behav-k");
      const extraLbl = document.getElementById("sflf-label-behav-extra");

      if (keyWrap) keyWrap.style.display = "block";
      if (memType === "graph_state_checkpointer") {
        if (keyLbl) keyLbl.textContent = "Thread Checkpoint Key";
        if (kLbl) kLbl.textContent = "Checkpoint Retention TTL (Hours)";
        if (kWrap) kWrap.style.display = "block";
        if (sumEngWrap) sumEngWrap.style.display = "none";
        if (sumModWrap) sumModWrap.style.display = "none";
        if (extraWrap) extraWrap.style.display = "none";
        if (scoreWrap) scoreWrap.style.display = "none";
      } else if (memType === "buffer_window_memory") {
        if (keyLbl) keyLbl.textContent = "Conversation Session Key";
        if (kLbl) kLbl.textContent = "Sliding Window Size (Last K Messages)";
        if (kWrap) kWrap.style.display = "block";
        if (sumEngWrap) sumEngWrap.style.display = "none";
        if (sumModWrap) sumModWrap.style.display = "none";
        if (extraWrap) extraWrap.style.display = "none";
        if (scoreWrap) scoreWrap.style.display = "none";
      } else if (memType === "running_summary_memory") {
        if (keyLbl) keyLbl.textContent = "Conversation Session Key";
        if (kLbl) kLbl.textContent = "Summary Compression Trigger (Every N Turns)";
        if (kWrap) kWrap.style.display = "block";
        if (sumEngWrap) sumEngWrap.style.display = "block";
        syncEngineModelDropdown(sumEngineSel, sumModelSel, sumModWrap);
        if (extraWrap) extraWrap.style.display = "none";
        if (scoreWrap) scoreWrap.style.display = "none";
      } else if (memType === "long_term_user_profile_memory") {
        if (keyLbl) keyLbl.textContent = "User Identifier Key";
        if (extraLbl) extraLbl.textContent = "Memory Namespace";
        if (kWrap) kWrap.style.display = "none";
        if (sumEngWrap) sumEngWrap.style.display = "none";
        if (sumModWrap) sumModWrap.style.display = "none";
        if (extraWrap) extraWrap.style.display = "block";
        if (scoreWrap) scoreWrap.style.display = "none";
      } else {
        if (keyLbl) keyLbl.textContent = "Agent Task Domain Key";
        if (kLbl) kLbl.textContent = "Few-Shot Examples to Retrieve (Top K)";
        if (kWrap) kWrap.style.display = "block";
        if (sumEngWrap) sumEngWrap.style.display = "none";
        if (sumModWrap) sumModWrap.style.display = "none";
        if (extraWrap) extraWrap.style.display = "none";
        if (scoreWrap) scoreWrap.style.display = "block";
      }
    }

    let wizardStep = 1;
    function setWizardStep(stepNum) {
      wizardStep = Math.max(1, Math.min(3, stepNum));
      const s1 = document.getElementById("sflf-mem-wizard-step-1");
      const s2 = document.getElementById("sflf-mem-wizard-step-2");
      const s3 = document.getElementById("sflf-mem-wizard-step-3");
      const badge = document.getElementById("sflf-mem-wizard-step-badge");
      const backBtn = document.getElementById("sflf-mem-wizard-back-btn");
      const nextBtn = document.getElementById("sflf-mem-wizard-next-btn");
      const submitBtn = document.getElementById("sflf-light-mem-submit-btn");

      if (s1) s1.style.display = wizardStep === 1 ? "flex" : "none";
      if (s2) s2.style.display = wizardStep === 2 ? "flex" : "none";
      if (s3) s3.style.display = wizardStep === 3 ? "flex" : "none";
      const stepTitles = {
        1: "Step 1 of 3 · Storage Target",
        2: "Step 2 of 3 · Object & Memory Configuration",
        3: "Step 3 of 3 · Optional Embedding, Preprocessing & Create",
      };
      if (badge) badge.textContent = stepTitles[wizardStep];
      if (backBtn) backBtn.style.display = wizardStep > 1 ? "inline-flex" : "none";
      if (nextBtn) nextBtn.style.display = wizardStep < 3 ? "inline-flex" : "none";
      if (submitBtn) submitBtn.style.display = wizardStep === 3 ? "inline-flex" : "none";
      syncStep2DynamicFields();
    }

    function syncStorageFamilyBoxes() {
      const fam = (storageFamilySel && storageFamilySel.value) || "snowflake_sql";
      if (branchASqlBox) branchASqlBox.style.display = fam === "snowflake_postgres" ? "none" : "grid";
      if (branchBPgBox) branchBPgBox.style.display = fam === "snowflake_postgres" ? "grid" : "none";
      syncStep2DynamicFields();
    }

    function syncLightMemoryModalTypeState() {
      const spec = resolveCanonicalMemoryTypeSpec(typeSel ? typeSel.value : "buffer_window_memory");
      const titleEl = document.getElementById("sflf-light-mem-guidance-title");
      const badgeEl = document.getElementById("sflf-light-mem-embed-badge");
      const recEl = document.getElementById("sflf-light-mem-rec-objs");
      const sqlEl = document.getElementById("sflf-light-mem-sql-preview");
      const whenEl = document.getElementById("sflf-light-mem-when-embed");
      const pkEl = document.getElementById("sflf-light-mem-hybrid-pk");
      const idxEl = document.getElementById("sflf-light-mem-hybrid-idx");
      const tblInput = document.getElementById("sflf-light-mem-table-name");
      const dbVal = (document.getElementById("sflf-light-mem-db") || {}).value || sfState.database;
      const schVal = (document.getElementById("sflf-light-mem-schema") || {}).value || sfState.schema;
      const nameVal = (document.getElementById("sflf-light-mem-name") || {}).value || spec.default_table_name || "AGENT_MEMORY_STORE";

      if (titleEl) titleEl.textContent = spec.label;
      if (badgeEl) badgeEl.textContent = `Embedding Required? ${spec.needs_embedding_badge}`;
      if (recEl) recEl.textContent = (spec.recommended_snowflake_objects || []).join(" · ");
      if (tblInput && spec.default_table_name) tblInput.value = spec.default_table_name;
      if (sqlEl) {
        sqlEl.textContent = (spec.how_to_feed_without_embedding || "").replace(
          "<fqn>",
          `${dbVal}.${schVal}.${nameVal}`
        );
      }
      if (whenEl) whenEl.textContent = spec.when_embedding_useful || "";
      if (pkEl && spec.default_hybrid_pk) pkEl.value = spec.default_hybrid_pk;
      if (idxEl && spec.default_hybrid_index) idxEl.value = spec.default_hybrid_index;
      if (storageFamilySel && spec.default_storage_family) {
        storageFamilySel.value = spec.default_storage_family;
        syncStorageFamilyBoxes();
      }
      if (preprocCb) {
        preprocCb.checked = !!spec.default_llm_preprocessing;
      }
      if (enableEmbedCb) {
        if (spec.embedding_locked_off) {
          enableEmbedCb.checked = false;
          enableEmbedCb.disabled = true;
          if (embedBox) embedBox.style.display = "none";
        } else {
          enableEmbedCb.disabled = false;
        }
      }
      if (targetObjSel) {
        const sqlOnlyObjects = (spec.recommended_snowflake_objects || [spec.default_backend]).filter(function (o) {
          return String(o).toLowerCase().indexOf("postgres") === -1;
        });
        const fallbackSqlObj =
          spec.default_backend && String(spec.default_backend).toLowerCase().indexOf("postgres") === -1
            ? spec.default_backend
            : sqlOnlyObjects[0] || "Hybrid Table";
        populateSelectOptions(
          targetObjSel,
          sqlOnlyObjects.length ? sqlOnlyObjects : ["Hybrid Table", "Standard Table", "Dynamic Table", "Interactive Table"],
          fallbackSqlObj
        );
      }
      syncStep2DynamicFields();
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
        dimInput.readOnly = ["custom_endpoint", "spcs_model_registry", "spcs_llm_proxy"].indexOf(provKey) === -1;
      }
    }

    if (typeSel) typeSel.onchange = syncLightMemoryModalTypeState;
    if (storageFamilySel) storageFamilySel.onchange = syncStorageFamilyBoxes;
    if (targetObjSel) targetObjSel.onchange = syncStep2DynamicFields;
    if (actionModeSel) actionModeSel.onchange = syncActionModeVisibility;
    if (dbSel) {
      dbSel.onchange = function () {
        const dbNew = document.getElementById("sflf-light-mem-db-new");
        if (dbNew) dbNew.style.display = dbSel.value === "__create_new_db__" ? "block" : "none";
        syncCreateMemoryModalWithFlowContext(sfState);
      };
    }
    if (schSel) {
      schSel.onchange = function () {
        const schNew = document.getElementById("sflf-light-mem-schema-new");
        if (schNew) schNew.style.display = schSel.value === "__create_new_schema__" ? "block" : "none";
        syncCreateMemoryModalWithFlowContext(sfState);
      };
    }
    if (pgInstSel) {
      pgInstSel.onchange = function () {
        syncCreateMemoryModalWithFlowContext(sfState);
      };
    }
    if (pgTableSel) {
      pgTableSel.onchange = function () {
        const pgTblNew = document.getElementById("sflf-light-mem-pg-table-new");
        if (pgTblNew) pgTblNew.style.display = pgTableSel.value === "__create_new_pg_table__" ? "block" : "none";
      };
    }
    if (sumEngineSel) {
      sumEngineSel.onchange = function () {
        syncEngineModelDropdown(sumEngineSel, sumModelSel, document.getElementById("sflf-step2-behav-sum-model"));
      };
    }
    if (preprocEngineSel) {
      preprocEngineSel.onchange = function () {
        syncEngineModelDropdown(preprocEngineSel, preprocModelSel, preprocModelWrap);
      };
    }
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
    document.getElementById("sflf-mem-wizard-back-btn").onclick = function () {
      setWizardStep(wizardStep - 1);
    };
    document.getElementById("sflf-mem-wizard-next-btn").onclick = function () {
      setWizardStep(wizardStep + 1);
    };

    document.getElementById("sflf-light-mem-submit-btn").onclick = async function () {
      const nameVal = (document.getElementById("sflf-light-mem-name") || {}).value || "AGENT_MEMORY_STORE";
      const rawDbVal = (document.getElementById("sflf-light-mem-db") || {}).value || sfState.database;
      const dbVal =
        rawDbVal === "__create_new_db__"
          ? (document.getElementById("sflf-light-mem-db-new") || {}).value || sfState.database
          : rawDbVal;
      const rawSchVal = (document.getElementById("sflf-light-mem-schema") || {}).value || sfState.schema;
      const schVal =
        rawSchVal === "__create_new_schema__"
          ? (document.getElementById("sflf-light-mem-schema-new") || {}).value || sfState.schema
          : rawSchVal;
      const storageFamily = (storageFamilySel && storageFamilySel.value) || "snowflake_sql";
      const pgInstance = (document.getElementById("sflf-light-mem-pg-instance") || {}).value || "LANGFLOW_PG_INSTANCE";
      const pgMode = (document.getElementById("sflf-light-mem-pg-mode") || {}).value || "PostgresSaver";
      const rawPgTable = (document.getElementById("sflf-light-mem-pg-table") || {}).value || "langgraph_checkpoints";
      const pgTable =
        rawPgTable === "__create_new_pg_table__"
          ? (document.getElementById("sflf-light-mem-pg-table-new") || {}).value || "langgraph_checkpoints"
          : rawPgTable;
      const actionMode = (document.getElementById("sflf-light-mem-action-mode") || {}).value || "create_new";
      const physicalTableName =
        actionMode === "bind_existing"
          ? (document.getElementById("sflf-light-mem-existing-table") || {}).value || nameVal
          : (document.getElementById("sflf-light-mem-table-name") || {}).value || nameVal;
      const chosenType = (typeSel && typeSel.value) || "buffer_window_memory";
      const spec = resolveCanonicalMemoryTypeSpec(chosenType);
      const targetBackend =
        storageFamily === "snowflake_postgres"
          ? `Snowflake Postgres (${pgMode})`
          : (targetObjSel && targetObjSel.value) || spec.default_backend;
      const enableEmbedding = !!(enableEmbedCb && enableEmbedCb.checked && !spec.embedding_locked_off);
      const embProvider = (providerSel && providerSel.value) || "snowflake_cortex";
      const embModel = (modelSel && modelSel.value) || "snowflake-arctic-embed-m-v1.5";
      const embDim = parseInt((dimInput && dimInput.value) || "768", 10);
      const autoCapture = !!((document.getElementById("sflf-light-mem-auto-capture") || {}).checked !== false);
      const preprocEnabled = !!(preprocCb && preprocCb.checked);
      const preprocEngine =
        chosenType === "running_summary_memory"
          ? (sumEngineSel && sumEngineSel.value) || (preprocEngineSel && preprocEngineSel.value) || "cortex_sql"
          : (preprocEngineSel && preprocEngineSel.value) || "cortex_sql";
      const preprocModel =
        preprocEngine === "cortex_summarize_agg"
          ? "AI_SUMMARIZE_AGG"
          : chosenType === "running_summary_memory"
          ? (sumModelSel && sumModelSel.value) || "mistral-large2"
          : (preprocModelSel && preprocModelSel.value) || "mistral-large2";
      const killPhrase = (document.getElementById("sflf-light-mem-kill-phrase") || {}).value || "NO_INGEST";
      const outEl = document.getElementById("sflf-light-mem-out");

      const resp = await fetch("/api/v1/snowflake/memories", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: nameVal,
          database: dbVal,
          schema: schVal,
          table_name: physicalTableName,
          flow_id: extractCurrentFlowId(),
          storage_family: storageFamily,
          postgres_instance: storageFamily === "snowflake_postgres" ? pgInstance : null,
          postgres_table: storageFamily === "snowflake_postgres" ? pgTable : null,
          pg_table_mode: storageFamily === "snowflake_postgres" ? pgMode : null,
          create_new_physical_store: actionMode !== "bind_existing",
          agent_memory_type: chosenType,
          memory_category: enableEmbedding ? "vector_table" : spec.default_category,
          vector_database: targetBackend,
          enable_embedding: enableEmbedding,
          embedding_provider: enableEmbedding ? embProvider : "none",
          embedding_model: enableEmbedding ? embModel : null,
          dimension: enableEmbedding ? embDim : null,
          vector_dimension: enableEmbedding ? embDim : null,
          auto_capture: autoCapture,
          filter_by_session: spec.filter_by_session,
          llm_preprocessing: {
            enabled: preprocEnabled,
            engine: preprocEngine,
            model: preprocModel,
            kill_phrase: killPhrase,
          },
        }),
      });
      const data = await resp.json();
      if (outEl) outEl.textContent = JSON.stringify(data, null, 2);
      if (!resp.ok || data.ok === false) {
        showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operation denied");
      } else {
        sfState.memories = Array.isArray(sfState.memories) ? sfState.memories : [];
        if (data.memory) {
          const normalizedMem = Object.assign({}, data.memory, {
            id: data.memory.memory_id || data.memory.langflow_memory_id || data.memory.id || data.memory.name,
          });
          sfState.memories = sfState.memories.filter(function (m) {
            return (typeof m === "string" ? m : m.name) !== normalizedMem.name;
          });
          sfState.memories.unshift(normalizedMem);
          window.SNOWFLAKE_MEMORIES_LIST = Array.isArray(window.SNOWFLAKE_MEMORIES_LIST)
            ? window.SNOWFLAKE_MEMORIES_LIST.filter(function (m) { return m && m.name !== normalizedMem.name; })
            : [];
          window.SNOWFLAKE_MEMORIES_LIST.unshift(normalizedMem);
          if (typeof window.__sflfRefetchMemories === "function") {
            try { window.__sflfRefetchMemories(); } catch (_e) {}
          }
          if (typeof window.__sflfSelectMemory === "function") {
            try { window.__sflfSelectMemory(normalizedMem); } catch (_e) {}
          }
          renderSnowflakeMemoryDetailView(normalizedMem);
        }
        showDenialOrStatusBanner("ok", "MEMORY_CREATED", `Snowflake Memory ${nameVal} (${chosenType}) created.`);
        ensureSnowflakeMemoriesSearchableCards();
        syncNodesToCurrentContext(sfState);
        window.dispatchEvent(new Event("focus"));
        setTimeout(function () {
          closeLightModal();
        }, 450);
      }
    };

    modal.style.display = "flex";
    setWizardStep(1);
    syncActionModeVisibility();
    syncEngineModelDropdown(sumEngineSel, sumModelSel, document.getElementById("sflf-step2-behav-sum-model"));
    syncEngineModelDropdown(preprocEngineSel, preprocModelSel, preprocModelWrap);
    syncLightMemoryModalTypeState();
    syncLightMemoryEmbeddingProviderModels();
    ensureMemoriesPageSnowflakeButton();
  }
  window.openCreateSnowflakeMemoryModal = openCreateSnowflakeMemoryModal;

  // W2-R4-05: Full Langflow-Compliant Memory Detail View in the Memories Right Pane
  let _selectedSnowflakeMemoryName = null;
  let _lastDetailSignature = "";
  let _lastCardsSignature = "";
  function renderSnowflakeMemoryDetailView(memoryOrRef) {
    if (!document.body || !isStrictMemoriesView()) return;
    let memObj = null;
    if (typeof memoryOrRef === "string") {
      const clean = memoryOrRef.replace(/^sf-/, "").trim();
      memObj = (sfState.memories || []).find(function (m) {
        return (typeof m === "string" ? m : m.name) === clean || (m && m.langflow_memory_id === memoryOrRef);
      });
      if (!memObj) {
        memObj = { name: clean, agent_memory_type: "buffer_window_memory" };
      }
    } else if (memoryOrRef && typeof memoryOrRef === "object") {
      const candidateName = memoryOrRef.name || "";
      memObj =
        (sfState.memories || []).find(function (m) {
          return (typeof m === "string" ? m : m.name) === candidateName;
        }) || memoryOrRef;
    }
    if (!memObj) return;
    if (typeof memObj === "string") {
      memObj = { name: memObj, agent_memory_type: "buffer_window_memory" };
    }
    _selectedSnowflakeMemoryName = memObj.name;

    const detailSig = JSON.stringify({
      name: memObj.name,
      role: sfState.active_role,
      type: memObj.canonical_memory_type || memObj.agent_memory_type,
      fqn: memObj.table_fqn || memObj.fqn,
      auto: memObj.auto_capture,
      recordsCount: Array.isArray(memObj.stored_records) ? memObj.stored_records.length : 0,
    });
    const existingPanel = document.getElementById("sflf-memory-detail-view");
    if (existingPanel && existingPanel.style.display !== "none" && detailSig === _lastDetailSignature) {
      return;
    }
    _lastDetailSignature = detailSig;

    const spec = resolveCanonicalMemoryTypeSpec(memObj.canonical_memory_type || memObj.agent_memory_type);
    const storageFamily =
      memObj.storage_family ||
      ((memObj.vector_database || "").toLowerCase().includes("postgres") || (memObj.table_fqn || "").includes(":5432")
        ? "snowflake_postgres"
        : "snowflake_sql");
    const fqn =
      memObj.table_fqn ||
      memObj.fqn ||
      `${memObj.database || sfState.database}.${memObj.schema || sfState.schema}.${memObj.name}`;
    const embSummary = memObj.enable_embedding
      ? `${memObj.embedding_provider || "snowflake_cortex"} · ${memObj.embedding_model || "AI_EMBED"} (${memObj.dimension || 768}d)`
      : "Disabled (Direct SQL / Key-Value Lookup)";
    const preprocObj = memObj.llm_preprocessing || {};
    const preprocSummary = preprocObj.enabled
      ? `Enabled (${preprocObj.engine || "cortex_sql"} · Kill Phrase: ${preprocObj.kill_phrase || "NO_INGEST"})`
      : `Optional / Off (Kill Phrase: ${preprocObj.kill_phrase || "NO_INGEST"})`;
    const activeSql = (spec.how_to_feed_without_embedding || "").replace("<fqn>", fqn);
    const autoCaptureOn = memObj.auto_capture !== false;

    let panel = document.getElementById("sflf-memory-detail-view");
    if (!panel) {
      panel = document.createElement("section");
      panel.id = "sflf-memory-detail-view";
      panel.setAttribute("data-testid", "snowflake-memory-detail-view");
      panel.style.cssText =
        "margin:14px 24px;background:#ffffff;color:#0f172a;border:1px solid #e2e8f0;border-radius:12px;padding:18px 22px;box-shadow:0 4px 16px rgba(15,23,42,0.06);font-family:Inter,sans-serif;font-size:12px;display:flex;flex-direction:column;gap:14px;";
      const mainTarget =
        document.querySelector("main") ||
        document.getElementById("sflf-memories-cards-grid") ||
        document.body;
      mainTarget.appendChild(panel);
    }
    panel.style.display = "flex";

    const sampleRecords = Array.isArray(memObj.stored_records) && memObj.stored_records.length > 0
      ? memObj.stored_records
      : [
          {
            session_id: memObj.session_id || memObj.thread_id || memObj.user_id || "fin-reconcile-session-001",
            topic_or_key: memObj.topic || memObj.entity_namespace || spec.id,
            metric: memObj.window_k ? `window_k=${memObj.window_k}` : "score=0.95",
            status: "Ingested",
            updated_at: "2026-10-26T10:00:00Z",
          },
          {
            session_id: "turn-preproc-check",
            topic_or_key: "trivial_greeting_check",
            metric: "NO_INGEST",
            status: "Skipped",
            updated_at: "2026-10-26T10:01:00Z",
          },
        ];

    const rowsHtml = sampleRecords
      .map(function (r) {
        const st = r.status || "Processed";
        const badgeColor =
          st === "Skipped"
            ? "background:#fef3c7;color:#92400e;"
            : st === "Empty"
              ? "background:#f1f5f9;color:#64748b;"
              : "background:#dcfce7;color:#166534;";
        return `<tr>
          <td style="padding:7px 10px;border-bottom:1px solid #f1f5f9;font-family:'JetBrains Mono',monospace;">${r.session_id || "default"}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #f1f5f9;">${r.topic_or_key || "-"}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #f1f5f9;">${r.metric || "-"}</td>
          <td style="padding:7px 10px;border-bottom:1px solid #f1f5f9;"><span style="padding:2px 8px;border-radius:999px;font-size:10.5px;font-weight:700;${badgeColor}">${st}</span></td>
          <td style="padding:7px 10px;border-bottom:1px solid #f1f5f9;color:#64748b;">${r.updated_at || "Just now"}</td>
        </tr>`;
      })
      .join("");

    panel.innerHTML = `
      <!-- Header Controls -->
      <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;border-bottom:1px solid #e2e8f0;padding-bottom:12px;">
        <div style="display:flex;flex-direction:column;gap:3px;">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            ${buildOfficialSnowflakeSvg("#29B5E8", 18)}
            <strong style="font-size:15px;color:#0f172a;">${memObj.name}</strong>
            <span style="background:rgba(41,181,232,0.12);color:#0284c7;border:1px solid #29B5E8;border-radius:6px;padding:1px 8px;font-size:11px;font-weight:600;">Role: ${sfState.active_role}</span>
          </div>
          <div style="font-size:11px;color:#475569;">${spec.label} • ${storageFamily === "snowflake_postgres" ? "Snowflake Postgres (:5432)" : "Snowflake SQL (Database ▸ Schema)"} • Flow: ${memObj.flow_id || extractCurrentFlowId() || "Global"}</div>
        </div>
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
          <select id="sflf-memory-detail-session-filter" data-testid="snowflake-memory-session-filter" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;padding:5px 9px;border-radius:8px;font-size:11.5px;">
            <option value="all">All Sessions (Cross-Session)</option>
            <option value="${memObj.session_id || "fin-reconcile-session-001"}">${memObj.session_id || "fin-reconcile-session-001"}</option>
          </select>
          <button type="button" id="sflf-memory-detail-reload-btn" data-testid="snowflake-memory-reload-btn" style="background:#f8fafc;border:1px solid #cbd5e1;color:#0f172a;padding:5px 11px;border-radius:8px;font-size:11.5px;font-weight:600;cursor:pointer;">↻ Reload sessions and messages</button>
          <span id="sflf-memory-detail-autocapture" data-testid="snowflake-memory-autocapture-toggle" style="background:${autoCaptureOn ? "#dcfce7" : "#f1f5f9"};color:${autoCaptureOn ? "#166534" : "#475569"};border:1px solid ${autoCaptureOn ? "#86efac" : "#cbd5e1"};padding:4px 10px;border-radius:999px;font-size:11px;font-weight:700;">Auto-capture: ${autoCaptureOn ? "● ON" : "○ OFF"}</span>
          <button type="button" id="sflf-memory-detail-delete-btn" data-testid="snowflake-memory-delete-btn" style="background:#fef2f2;border:1px solid #fecaca;color:#b91c1c;padding:5px 11px;border-radius:8px;font-size:11.5px;font-weight:600;cursor:pointer;">🗑 Delete</button>
        </div>
      </div>

      <!-- Expandable Configuration Summary -->
      <details open data-testid="snowflake-memory-config-summary" style="background:#f8fafc;border:1px solid #bae6fd;border-radius:10px;padding:10px 14px;">
        <summary style="font-weight:700;color:#0284c7;cursor:pointer;font-size:12.5px;">Configuration Summary</summary>
        <div style="margin-top:8px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,230px),1fr));gap:8px;font-size:11px;">
          <div><strong>Name:</strong> ${memObj.name}</div>
          <div><strong>Memory Type (#1..#5):</strong> ${spec.label}</div>
          <div><strong>Storage Target:</strong> ${storageFamily === "snowflake_postgres" ? "Snowflake Postgres (:5432)" : "Snowflake SQL (Database ▸ Schema)"}</div>
          <div><strong>Snowflake Object / FQN:</strong> <code>${fqn}</code></div>
          <div><strong>Object Subtype & PK/Lag:</strong> ${memObj.vector_database || spec.default_backend} (${spec.default_hybrid_pk || "PK"})</div>
          <div><strong>Filter by Session:</strong> ${spec.filter_by_session ? "True (Session-Scoped)" : "False (Cross-Session User/Role Scoped)"}</div>
          <div><strong>Embedding Model & Source:</strong> ${embSummary}</div>
          <div><strong>LLM Preprocessing & Kill Phrase:</strong> ${preprocSummary}</div>
        </div>
        <div style="margin-top:8px;font-size:11px;">
          <strong>Active SQL Query:</strong>
          <code style="display:block;margin-top:3px;background:#0f172a;color:#e2e8f0;padding:6px 9px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:10.5px;white-space:pre-wrap;">${activeSql}</code>
        </div>
      </details>

      <!-- Session History & Stored Records Table -->
      <div>
        <div style="font-weight:700;color:#0f172a;font-size:12.5px;margin-bottom:6px;">Session History & Stored Records</div>
        <table data-testid="snowflake-memory-records-table" style="width:100%;border-collapse:collapse;font-size:11.5px;">
          <thead>
            <tr style="background:#f8fafc;text-align:left;color:#475569;">
              <th style="padding:7px 10px;border-bottom:1px solid #cbd5e1;">Session / ID</th>
              <th style="padding:7px 10px;border-bottom:1px solid #cbd5e1;">Topic / Key</th>
              <th style="padding:7px 10px;border-bottom:1px solid #cbd5e1;">Outcome Score / Turn</th>
              <th style="padding:7px 10px;border-bottom:1px solid #cbd5e1;">Status</th>
              <th style="padding:7px 10px;border-bottom:1px solid #cbd5e1;">Updated</th>
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>
    `;

    const reloadBtn = document.getElementById("sflf-memory-detail-reload-btn");
    if (reloadBtn) {
      reloadBtn.onclick = async function () {
        try {
          const resp = await fetch(`/api/v1/snowflake/memories/${encodeURIComponent(memObj.name)}/reload`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({}),
          });
          if (resp.ok) {
            const d = await resp.json();
            if (d.memory) renderSnowflakeMemoryDetailView(d.memory);
          }
        } catch (_e) {
          // ignore
        }
      };
    }

    const deleteBtn = document.getElementById("sflf-memory-detail-delete-btn");
    if (deleteBtn) {
      deleteBtn.onclick = async function () {
        try {
          const resp = await fetch(`/api/v1/snowflake/memories/${encodeURIComponent(memObj.name)}`, {
            method: "DELETE",
          });
          if (resp.ok) {
            sfState.memories = (sfState.memories || []).filter(function (m) {
              return (typeof m === "string" ? m : m.name) !== memObj.name;
            });
            panel.style.display = "none";
            ensureSnowflakeMemoriesSearchableCards();
          }
        } catch (_e) {
          // ignore
        }
      };
    }
  }
  window.renderSnowflakeMemoryDetailView = renderSnowflakeMemoryDetailView;

  function isMemoryVisibleInActiveContext(memObj, ctxState) {
    if (!memObj) return false;
    const obj = typeof memObj === "string" ? { name: memObj } : memObj;
    const activeCtx = ctxState || sfState || {};
    const selCtx = activeCtx.selected_context || {};
    const fqn = String(obj.table_fqn || obj.fqn || obj.description || obj.backend_type || "").trim();
    const storageFamily = String(obj.storage_family || (obj.backend_config && obj.backend_config.storage_family) || "").toLowerCase();
    const vecDb = String(obj.vector_database || obj.backend_type || "").toLowerCase();
    const isPg =
      storageFamily === "snowflake_postgres" ||
      vecDb.includes("postgres") ||
      fqn.includes(":5432") ||
      (obj.name && (obj.name.toUpperCase() === "MEM_1_GRAPH_CHECKPOINTER" || obj.name.toUpperCase() === "MEM_2_CHAT_WINDOW") && !storageFamily);
    if (isPg) {
      const pgList = Array.isArray(selCtx.postgres_instances)
        ? selCtx.postgres_instances.map(function (p) { return String(p || "").trim().toUpperCase(); }).filter(Boolean)
        : [];
      if (pgList.length === 0) return false;
      let pgInst = String(obj.postgres_instance || "").trim().toUpperCase();
      if (!pgInst && fqn.includes(":5432")) {
        const mPg = fqn.match(/([A-Za-z0-9_]+):5432/);
        if (mPg && mPg[1]) pgInst = mPg[1].toUpperCase();
      }
      if (!pgInst) pgInst = "LANGFLOW_PG_INSTANCE";
      return pgList.indexOf(pgInst) !== -1;
    }
    let memDb = String(obj.database || "").trim().toUpperCase();
    let memSch = String(obj.schema || "").trim().toUpperCase();
    if ((!memDb || !memSch) && fqn.includes(".") && !fqn.includes(":5432")) {
      const mFqn = fqn.match(/([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.[A-Za-z0-9_]+/);
      if (mFqn) {
        memDb = memDb || mFqn[1].toUpperCase();
        memSch = memSch || mFqn[2].toUpperCase();
      }
    }
    if (Array.isArray(selCtx.databases) && memDb) {
      const activeDbs = selCtx.databases.map(function (d) { return String(d || "").trim().toUpperCase(); }).filter(Boolean);
      if (activeDbs.indexOf(memDb) === -1) return false;
    }
    const schemasMap = selCtx.schemas_by_db || {};
    if (memDb && memSch && schemasMap && typeof schemasMap === "object") {
      const dbKey = Object.keys(schemasMap).find(function (k) { return String(k).trim().toUpperCase() === memDb; });
      if (dbKey && Array.isArray(schemasMap[dbKey])) {
        const activeSchemas = schemasMap[dbKey].map(function (s) { return String(s || "").trim().toUpperCase(); }).filter(Boolean);
        if (activeSchemas.indexOf(memSch) === -1) return false;
      }
    }
    return true;
  }
  window.isMemoryVisibleInActiveContext = isMemoryVisibleInActiveContext;
  window.filterSnowflakeMemoriesByContext = function (list) {
    if (!Array.isArray(list)) return [];
    return list.filter(function (item) {
      return isMemoryVisibleInActiveContext(item, sfState);
    });
  };

  function syncSnowflakeMemoriesWindowList(rawMemories) {
    if (typeof window === "undefined") return;
    const contextVisible = (rawMemories || []).filter(function (m) {
      return isMemoryVisibleInActiveContext(m, sfState);
    });
    window.SNOWFLAKE_MEMORIES_LIST = contextVisible.map(function (m) {
      const obj = typeof m === "string" ? { name: m, agent_memory_type: "buffer_window_memory" } : m;
      const spec = resolveCanonicalMemoryTypeSpec(obj.agent_memory_type);
      const fqn = obj.table_fqn || obj.fqn || `${obj.database || sfState.database}.${obj.schema || sfState.schema}.${obj.name}`;
      const embBadge = obj.enable_embedding
        ? `${obj.embedding_provider || "snowflake_cortex"} · ${obj.embedding_model || "AI_EMBED"} (${obj.dimension || 768}d)`
        : "No Embedding — Direct SQL";
      return {
        id: obj.langflow_memory_id || `sf-${obj.name}`,
        name: obj.name,
        storage_family: obj.storage_family,
        postgres_instance: obj.postgres_instance,
        database: obj.database,
        schema: obj.schema,
        table_fqn: fqn,
        is_active: true,
        description: `${spec.label} · ${fqn} · ${embBadge}`,
      };
    });
  }

  async function fetchSnowflakeMemoriesRegistry(flowIdOverride) {
    try {
      const effFlowId =
        flowIdOverride !== undefined && flowIdOverride !== null
          ? flowIdOverride
          : extractCurrentFlowId();
      const url = effFlowId
        ? `/api/v1/snowflake/memories?flow_id=${encodeURIComponent(effFlowId)}`
        : "/api/v1/snowflake/memories";
      const headers = { Accept: "application/json" };
      if (effFlowId) {
        headers["X-Snowflake-Flow-Id"] = String(effFlowId);
      }
      const resp = await fetch(url, { headers: headers });
      if (!resp.ok) return;
      const data = await resp.json();
      if (Array.isArray(data.memories_catalog)) {
        sfState.memories = data.memories_catalog;
      } else if (Array.isArray(data.memories)) {
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
      (Array.isArray(sfState.memories)
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
      }).filter(function (m) {
        return isMemoryVisibleInActiveContext(m, sfState);
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

    if (
      _selectedSnowflakeMemoryName &&
      !filtered.some(function (m) { return m.name === _selectedSnowflakeMemoryName; })
    ) {
      _selectedSnowflakeMemoryName = null;
      if (filtered.length > 0) {
        renderSnowflakeMemoryDetailView(filtered[0]);
        if (typeof window.__sflfSelectMemory === "function") {
          try {
            window.__sflfSelectMemory(filtered[0].langflow_memory_id || filtered[0].id || filtered[0].name);
          } catch (_e) {}
        }
      } else {
        const existingDetail = document.getElementById("sflf-memory-detail-view");
        if (existingDetail) existingDetail.style.display = "none";
      }
    }

    const cardsSig = JSON.stringify({
      q: query,
      nativeBtn: hasNativeReactMemoriesBtn,
      items: filtered.map(function (m) {
        return [m.name, m.agent_memory_type, m.table_fqn || m.fqn, !!m.enable_embedding];
      }),
    });
    if (grid.childElementCount > 0 && cardsSig === _lastCardsSignature) {
      return;
    }
    _lastCardsSignature = cardsSig;

    grid.innerHTML = filtered
      .map(function (m) {
        const spec = resolveCanonicalMemoryTypeSpec(m.agent_memory_type);
        const fqn = m.table_fqn || m.fqn || `${m.database || sfState.database}.${m.schema || sfState.schema}.${m.name}`;
        const embBadge = m.enable_embedding
          ? `${m.embedding_provider || "snowflake_cortex"} · ${m.embedding_model || "AI_EMBED"} (${
              m.dimension || 768
            }d)`
          : "No Embedding — Direct SQL Lookup";
        return `<div class="sflf-memory-card" data-memory-name="${m.name}" style="background:#ffffff;color:#0f172a;border:1px solid #e2e8f0;border-left:4px solid #29B5E8;border-radius:10px;padding:12px 14px;box-shadow:0 2px 8px rgba(15,23,42,0.06);display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
            <strong style="font-size:13px;color:#0f172a;">${m.name}</strong>
            <span style="background:rgba(41,181,232,0.12);color:#0284c7;border:1px solid #29B5E8;border-radius:999px;padding:1px 8px;font-size:10px;font-weight:600;">${spec.label}</span>
          </div>
          <div style="font-size:11px;color:#475569;font-family:'JetBrains Mono',monospace;">${fqn}</div>
          <div style="font-size:10.5px;color:#0369a1;font-weight:600;">${embBadge}</div>
        </div>`;
      })
      .join("");

    Array.from(grid.querySelectorAll(".sflf-memory-card")).forEach(function (cardEl) {
      cardEl.onclick = function () {
        const mName = cardEl.getAttribute("data-memory-name");
        if (mName) renderSnowflakeMemoryDetailView(mName);
      };
    });

    if (filtered.length > 0 && !_selectedSnowflakeMemoryName) {
      renderSnowflakeMemoryDetailView(filtered[0]);
    }
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
          ? "Memory creation has been moved to the Memories (+ Snowflake) page (W2-R2-04B / W2-R2-13)."
          : "Creating FUNCTION (UDF/UDTF) and STORED PROCEDURE objects is not permitted from the Snowflake Langflow Hub (HUB-W2-04). Use the components in LLM Operations.";
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
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operation denied");
    } else if (!dryRun) {
      showDenialOrStatusBanner(
        "ok",
        "PROVISIONED",
        `Object ${effectiveName} (${objectKind}/${objectSubtype}) created in ${targetDb}.${targetSchema} with role ${sfState.active_role}`
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
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Operation denied");
    } else {
      showDenialOrStatusBanner("ok", "MEMORY_CREATED", `Snowflake Memory ${name} (${agentMemoryType}) created in ${backend}.`);
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

  window.fetchSnowflakeMemoriesRegistry = fetchSnowflakeMemoriesRegistry;

  // =========================================================================
  // W2-R5-02: Native `+ Create` Memory Dialog Enhancement (`Snowflake Cortex` 6 AI_EMBED Models + 6 Snowflake Backends)
  // =========================================================================
  const OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS = [
    { model_name: "snowflake-arctic-embed-m-v1.5", dimensions: 768, max_tokens: 512, language: "English" },
    { model_name: "snowflake-arctic-embed-m", dimensions: 768, max_tokens: 512, language: "English" },
    { model_name: "e5-base-v2", dimensions: 768, max_tokens: 512, language: "English" },
    { model_name: "snowflake-arctic-embed-l-v2.0", dimensions: 1024, max_tokens: 512, language: "Multilingual" },
    { model_name: "voyage-multilingual-2", dimensions: 1024, max_tokens: 32000, language: "Multilingual" },
    { model_name: "nv-embed-qa-4", dimensions: 1024, max_tokens: 512, language: "English" },
  ];

  const SNOWFLAKE_NATIVE_MEMORY_BACKENDS = [
    { id: "snowflake_table", label: "Snowflake Standard Table", table_type: "STANDARD", storage_family: "snowflake_sql" },
    { id: "snowflake_hybrid", label: "Snowflake Hybrid Table", table_type: "HYBRID", storage_family: "snowflake_sql" },
    { id: "snowflake_dynamic", label: "Snowflake Dynamic Table", table_type: "DYNAMIC", storage_family: "snowflake_sql" },
    { id: "snowflake_interactive", label: "Snowflake Interactive Table", table_type: "INTERACTIVE", storage_family: "snowflake_sql" },
    { id: "snowflake_iceberg", label: "Snowflake Iceberg Table", table_type: "ICEBERG", storage_family: "snowflake_sql" },
    { id: "snowflake_postgres", label: "Snowflake Postgres (:5432)", table_type: "POSTGRES", storage_family: "snowflake_postgres" },
  ];

  if (typeof window !== "undefined") {
    window.OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS = OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS;
    window.SNOWFLAKE_NATIVE_MEMORY_BACKENDS = SNOWFLAKE_NATIVE_MEMORY_BACKENDS;
    window.__sflfNativeCreateMemoryOverrides = {
      database: "SNOWFLAKE_LANGFLOW",
      schema: "CORE",
      backend_type: "snowflake_table",
      embedding_provider: "Snowflake Cortex",
      embedding_model: "snowflake-arctic-embed-m-v1.5",
    };
  }

  function enhanceNativeCreateMemoryDialog() {
    if (typeof document === "undefined" || !document.body) return;
    const nativeMemoryDialog =
      document.querySelector('[role="dialog"] #memory-name') &&
      document.querySelector('[role="dialog"] #memory-name').closest('[role="dialog"]');
    let helperBar = document.getElementById("sflf-native-create-memory-helper");
    if (!nativeMemoryDialog) {
      if (helperBar && helperBar.style.display !== "none") {
        helperBar.style.display = "none";
      }
      return;
    }

    const activeDb = (sfState && sfState.database) || "SNOWFLAKE_LANGFLOW";
    const activeSch = (sfState && sfState.schema) || "CORE";
    const activeRole = (sfState && sfState.active_role) || "ACCOUNTADMIN";
    if (window.__sflfNativeCreateMemoryOverrides) {
      window.__sflfNativeCreateMemoryOverrides.database = activeDb;
      window.__sflfNativeCreateMemoryOverrides.schema = activeSch;
    }

    if (!helperBar) {
      helperBar = document.createElement("div");
      helperBar.id = "sflf-native-create-memory-helper";
      helperBar.setAttribute("data-testid", "snowflake-native-create-memory-helper");
      helperBar.style.cssText =
        "position:fixed;z-index:100002;background:#f8fafc;color:#0f172a;border:1px solid #29B5E8;border-radius:10px;padding:10px 14px;font-family:Inter,sans-serif;font-size:11.5px;box-shadow:0 8px 24px rgba(15,23,42,0.18);display:none;flex-direction:column;gap:8px;max-width:440px;";
      helperBar.innerHTML = `
        <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
          <span style="display:inline-flex;align-items:center;gap:6px;font-weight:700;color:#0284c7;">
            ${buildOfficialSnowflakeSvg("#29B5E8", 14)}
            <span>Snowflake Cortex &amp; [ ❄️ Context ] Target</span>
          </span>
          <span id="sflf-native-create-ctx-badge" style="font-family:'JetBrains Mono',monospace;font-size:10.5px;background:rgba(41,181,232,0.12);color:#0369a1;padding:2px 7px;border-radius:999px;font-weight:600;">${activeDb}.${activeSch} (${activeRole})</span>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
          <label style="display:flex;flex-direction:column;gap:3px;font-size:10.5px;font-weight:600;color:#334155;">
            <span>Snowflake Cortex (AI_EMBED)</span>
            <select id="sflf-native-create-embed-select" data-testid="native-create-memory-embed-select" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:4px 6px;font-size:11px;">
              ${OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS.map(function (m) {
                return `<option value="${m.model_name}">${m.model_name} (${m.dimensions}d · ${m.language})</option>`;
              }).join("")}
            </select>
          </label>
          <label style="display:flex;flex-direction:column;gap:3px;font-size:10.5px;font-weight:600;color:#334155;">
            <span>Snowflake Vector Backend</span>
            <select id="sflf-native-create-backend-select" data-testid="native-create-memory-backend-select" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:4px 6px;font-size:11px;">
              ${SNOWFLAKE_NATIVE_MEMORY_BACKENDS.map(function (b) {
                return `<option value="${b.id}">${b.label}</option>`;
              }).join("")}
            </select>
          </label>
        </div>
      `;
      document.body.appendChild(helperBar);

      const embSel = document.getElementById("sflf-native-create-embed-select");
      if (embSel) {
        embSel.onchange = function () {
          window.__sflfNativeCreateMemoryOverrides.embedding_model = embSel.value;
          window.__sflfNativeCreateMemoryOverrides.embedding_provider = "Snowflake Cortex";
        };
      }
      const backSel = document.getElementById("sflf-native-create-backend-select");
      if (backSel) {
        backSel.onchange = function () {
          window.__sflfNativeCreateMemoryOverrides.backend_type = backSel.value;
        };
      }
    }

    const badge = document.getElementById("sflf-native-create-ctx-badge");
    if (badge) {
      badge.textContent = `${activeDb}.${activeSch} (${activeRole})`;
    }
    const rect = nativeMemoryDialog.getBoundingClientRect();
    if (rect && rect.width > 0) {
      helperBar.style.width = Math.round(rect.width) + "px";
      helperBar.style.maxWidth = Math.round(rect.width) + "px";
      helperBar.style.left = Math.round(rect.left) + "px";
      helperBar.style.top = Math.max(8, Math.round(rect.top - 96)) + "px";
    }
    if (helperBar.style.display !== "flex") {
      helperBar.style.display = "flex";
    }
  }
  window.enhanceNativeCreateMemoryDialog = enhanceNativeCreateMemoryDialog;

  // =========================================================================
  // W2-R5-05: Left-Rail Bucket Icon (`Stages & Volumes`) above `Traces` + View, File Browser/Upload & Create Wizard
  // =========================================================================
  let _stagesVolumesViewOpen = false;
  let _selectedStageOrVolumeKey = null;
  let _stagesVolumesState = {
    stages: [],
    external_volumes: [],
    storage_integrations: ["S3_DOCS_INT", "GCS_DOCS_INT", "AZURE_DOCS_INT"],
    files_by_stage: {},
    last_loaded_flow_id: null,
  };

  function buildBucketSvgIcon(sizePx) {
    const sz = sizePx || 20;
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${sz}" height="${sz}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:${sz}px;height:${sz}px;flex-shrink:0;" aria-hidden="true"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"/><path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"/></svg>`;
  }

  function isStageOrVolumeVisibleInActiveContext(item, ctxState) {
    if (!item) return false;
    const activeCtx = ctxState || sfState || {};
    const selCtx = activeCtx.selected_context || {};
    const kindUpper = String(item.object_kind || item.stage_type || item.type || "INTERNAL").toUpperCase();
    if (kindUpper === "EXTERNAL_VOLUME" || item.is_external_volume) {
      const volName = String(item.name || item.volume_name || "").trim().toUpperCase();
      if (!volName) return false;
      if (Array.isArray(selCtx.external_volumes) && selCtx.external_volumes.length > 0) {
        const allowedVols = selCtx.external_volumes.map(function (v) {
          return String(v || "").trim().toUpperCase();
        });
        return allowedVols.indexOf(volName) !== -1;
      }
      return true;
    }

    let db = String(item.database || "").trim().toUpperCase();
    let sch = String(item.schema || item.schema_name || "").trim().toUpperCase();
    let stgName = String(item.name || item.stage_name || "").trim();
    const rawFqn = String(item.stage_fqn || item.fqn || "").replace(/^@/, "").trim();
    if ((!db || !sch || !stgName) && rawFqn.includes(".")) {
      const parts = rawFqn.split(".");
      if (parts.length >= 3) {
        db = db || parts[0].toUpperCase();
        sch = sch || parts[1].toUpperCase();
        stgName = stgName || parts.slice(2).join(".");
      }
    }
    if (!db) db = String(activeCtx.database || "SNOWFLAKE_LANGFLOW").toUpperCase();
    if (!sch) sch = String(activeCtx.schema || "CORE").toUpperCase();

    if (Array.isArray(selCtx.databases) && selCtx.databases.length > 0) {
      const activeDbs = selCtx.databases.map(function (d) {
        return String(d || "").trim().toUpperCase();
      });
      if (activeDbs.indexOf(db) === -1) return false;
    } else if (Array.isArray(selCtx.databases) && selCtx.databases.length === 0) {
      return false;
    }

    const schemasMap = selCtx.schemas_by_db || {};
    if (schemasMap && typeof schemasMap === "object" && Object.keys(schemasMap).length > 0) {
      const dbKey = Object.keys(schemasMap).find(function (k) {
        return String(k).trim().toUpperCase() === db;
      });
      if (!dbKey || !Array.isArray(schemasMap[dbKey])) return false;
      const activeSchs = schemasMap[dbKey].map(function (s) {
        return String(s || "").trim().toUpperCase();
      });
      if (activeSchs.indexOf(sch) === -1) return false;
    }

    const selObjs = selCtx.selected_objects || selCtx.objects_by_db_schema || {};
    const schemaKey = `${db}.${sch}`;
    const matchedKey = Object.keys(selObjs).find(function (k) {
      return String(k).trim().toUpperCase() === schemaKey;
    });
    if (matchedKey && selObjs[matchedKey] && Array.isArray(selObjs[matchedKey].stages)) {
      const allowedStages = selObjs[matchedKey].stages.map(function (s) {
        return String(s || "").replace(/^@/, "").split(".").pop().trim().toUpperCase();
      });
      if (stgName && allowedStages.indexOf(stgName.toUpperCase()) === -1) {
        return false;
      }
    }
    return true;
  }
  window.isStageOrVolumeVisibleInActiveContext = isStageOrVolumeVisibleInActiveContext;

  function collectContextStagesAndVolumes(ctxState) {
    const activeCtx = ctxState || sfState || {};
    const selCtx = activeCtx.selected_context || {};
    const seenStageFqns = new Set();
    const outStages = [];

    function pushStage(stgObj) {
      if (!stgObj) return;
      const db = String(stgObj.database || activeCtx.database || "SNOWFLAKE_LANGFLOW").trim().toUpperCase();
      const sch = String(stgObj.schema || stgObj.schema_name || activeCtx.schema || "CORE").trim().toUpperCase();
      const name = String(stgObj.name || stgObj.stage_name || "").replace(/^@/, "").split(".").pop().trim();
      if (!name) return;
      const fqn = `@${db}.${sch}.${name}`;
      const fqnKey = fqn.toUpperCase();
      if (seenStageFqns.has(fqnKey)) return;
      const normalized = {
        key: fqn,
        name: name,
        stage_name: name,
        database: db,
        schema: sch,
        stage_fqn: fqn,
        fqn: fqn,
        object_kind: "STAGE",
        stage_type: String(stgObj.stage_type || stgObj.type || (stgObj.url ? "EXTERNAL" : "INTERNAL")).toUpperCase(),
        encryption: stgObj.encryption || (stgObj.url ? "AWS_SSE_S3" : "SNOWFLAKE_SSE"),
        directory_enabled: stgObj.directory_enabled !== undefined ? Boolean(stgObj.directory_enabled) : true,
        auto_refresh: Boolean(stgObj.auto_refresh),
        url: stgObj.url || "",
        cloud_provider: stgObj.cloud_provider || (stgObj.url ? String(stgObj.url).split("://")[0].toUpperCase() : "SNOWFLAKE_MANAGED"),
        storage_integration: stgObj.storage_integration || "",
        file_format: stgObj.file_format || "NONE",
        comment: stgObj.comment || "",
      };
      if (!isStageOrVolumeVisibleInActiveContext(normalized, activeCtx)) return;
      seenStageFqns.add(fqnKey);
      outStages.push(normalized);
    }

    (_stagesVolumesState.stages || []).forEach(pushStage);

    const dbs = Array.isArray(selCtx.databases) ? selCtx.databases : [activeCtx.database || "SNOWFLAKE_LANGFLOW"];
    const schemasMap = selCtx.schemas_by_db || {};
    const objsMap = selCtx.selected_objects || selCtx.objects_by_db_schema || (activeCtx.catalog && activeCtx.catalog.objects_by_db_schema) || {};
    dbs.forEach(function (db) {
      const schList = Array.isArray(schemasMap[db]) ? schemasMap[db] : [activeCtx.schema || "CORE"];
      schList.forEach(function (sch) {
        const key = `${db}.${sch}`;
        const bucket = objsMap[key] || {};
        const stgArr = Array.isArray(bucket.stages) ? bucket.stages : [];
        stgArr.forEach(function (sName) {
          pushStage({
            name: typeof sName === "string" ? sName : sName.name,
            database: db,
            schema: sch,
            stage_type: typeof sName === "object" && sName.stage_type ? sName.stage_type : "INTERNAL",
            url: typeof sName === "object" ? sName.url : "",
            storage_integration: typeof sName === "object" ? sName.storage_integration : "",
          });
        });
      });
    });

    const visStages = (selCtx.visible_objects && selCtx.visible_objects.stages) || [];
    visStages.forEach(function (sName) {
      pushStage({
        name: sName,
        database: activeCtx.database || "SNOWFLAKE_LANGFLOW",
        schema: activeCtx.schema || "CORE",
      });
    });

    const seenVols = new Set();
    const outVolumes = [];
    (_stagesVolumesState.external_volumes || []).forEach(function (v) {
      const vName = String((typeof v === "string" ? v : v.name || v.volume_name) || "").trim();
      if (!vName) return;
      const vKey = vName.toUpperCase();
      if (seenVols.has(vKey)) return;
      const normVol = {
        key: `VOL:${vName}`,
        name: vName,
        volume_name: vName,
        object_kind: "EXTERNAL_VOLUME",
        stage_type: "EXTERNAL_VOLUME",
        is_external_volume: true,
        allow_writes: typeof v === "object" && v.allow_writes !== undefined ? Boolean(v.allow_writes) : true,
        storage_provider: (typeof v === "object" && v.storage_provider) || "S3",
        storage_base_url: (typeof v === "object" && v.storage_base_url) || `s3://snowflake-iceberg-volume/${vName.toLowerCase()}/`,
        encryption: (typeof v === "object" && v.encryption) || "AWS_SSE_S3",
        comment: (typeof v === "object" && v.comment) || "Account-level External Volume for Apache Iceberg tables",
      };
      if (!isStageOrVolumeVisibleInActiveContext(normVol, activeCtx)) return;
      seenVols.add(vKey);
      outVolumes.push(normVol);
    });

    return { stages: outStages, external_volumes: outVolumes };
  }

  async function fetchSnowflakeStagesAndVolumes(flowIdOverride) {
    const effFlowId =
      flowIdOverride !== undefined && flowIdOverride !== null ? flowIdOverride : extractCurrentFlowId();
    const qs = effFlowId ? `?flow_id=${encodeURIComponent(effFlowId)}` : "";
    const headers = { Accept: "application/json" };
    if (effFlowId) headers["X-Snowflake-Flow-Id"] = String(effFlowId);

    try {
      const resp = await fetch(`/api/v1/snowflake/stages${qs}`, { headers: headers });
      if (resp.ok) {
        const data = await resp.json();
        if (Array.isArray(data.stages)) {
          _stagesVolumesState.stages = data.stages;
        }
        if (Array.isArray(data.external_volumes)) {
          _stagesVolumesState.external_volumes = data.external_volumes;
        }
        if (Array.isArray(data.storage_integrations) && data.storage_integrations.length > 0) {
          _stagesVolumesState.storage_integrations = data.storage_integrations;
        }
        if (typeof data.can_create_stage === "boolean") {
          sfState.role_privileges = sfState.role_privileges || {};
          sfState.role_privileges.can_create_stage = data.can_create_stage;
        }
        if (typeof data.can_create_external_volume === "boolean") {
          sfState.role_privileges = sfState.role_privileges || {};
          sfState.role_privileges.can_create_external_volume = data.can_create_external_volume;
        }
      }
    } catch (_e) {
      // fallback to context state
    }

    try {
      const volResp = await fetch(`/api/v1/snowflake/external-volumes${qs}`, { headers: headers });
      if (volResp.ok) {
        const volData = await volResp.json();
        if (Array.isArray(volData.external_volumes)) {
          _stagesVolumesState.external_volumes = volData.external_volumes;
        }
      }
    } catch (_e) {
      // ignore
    }

    _stagesVolumesState.last_loaded_flow_id = effFlowId;
    renderSnowflakeStagesVolumesList();
  }
  window.fetchSnowflakeStagesAndVolumes = fetchSnowflakeStagesAndVolumes;

  async function fetchStageFilesList(stageFqn) {
    if (!stageFqn) return [];
    const cleanFqn = String(stageFqn).replace(/^@/, "").trim();
    const effFlowId = extractCurrentFlowId();
    const headers = { Accept: "application/json" };
    if (effFlowId) headers["X-Snowflake-Flow-Id"] = String(effFlowId);
    try {
      let resp = await fetch(`/api/v1/snowflake/stages/${encodeURIComponent(cleanFqn)}/files`, { headers: headers });
      if (!resp.ok) {
        resp = await fetch(`/api/v1/snowflake/stages/files?stage_fqn=${encodeURIComponent(cleanFqn)}`, {
          headers: headers,
        });
      }
      if (resp.ok) {
        const data = await resp.json();
        const files = Array.isArray(data.files) ? data.files : Array.isArray(data.rows) ? data.rows : [];
        _stagesVolumesState.files_by_stage[`@${cleanFqn}`] = files;
        return files;
      }
    } catch (_e) {
      // ignore
    }
    return _stagesVolumesState.files_by_stage[`@${cleanFqn}`] || [];
  }

  function ensureLeftRailStagesVolumesTab() {
    if (typeof document === "undefined" || !document.body) return;
    const flowId = getActiveWorkflowFlowId();
    const tracesBtn = document.querySelector(
      '[data-testid="sidebar-nav-traces"], [data-sidebar-nav-item="traces"]'
    );
    const nativeStagesBtn = document.querySelector(
      '[data-testid="sidebar-nav-stages"]:not(#sflf-rail-stages-btn-floating), [data-sidebar-nav-item="stages"]:not(#sflf-rail-stages-btn-floating)'
    );

    if (nativeStagesBtn) {
      if (!nativeStagesBtn.id) nativeStagesBtn.id = "sflf-rail-stages-btn";
      if (!nativeStagesBtn.getAttribute("title")) nativeStagesBtn.setAttribute("title", "Stages & Volumes");
      if (!nativeStagesBtn.dataset.sflfStagesBound) {
        nativeStagesBtn.dataset.sflfStagesBound = "1";
        nativeStagesBtn.addEventListener("click", function () {
          setTimeout(function () {
            openSnowflakeStagesVolumesView(true);
          }, 10);
        });
      }
      const floatingFallback = document.getElementById("sflf-rail-stages-btn-floating");
      if (floatingFallback && floatingFallback.style.display !== "none") {
        floatingFallback.style.display = "none";
      }
    } else {
      let railBtn = document.getElementById("sflf-rail-stages-btn");
      if (!railBtn) {
        railBtn = document.createElement("button");
        railBtn.id = "sflf-rail-stages-btn";
        railBtn.type = "button";
        railBtn.setAttribute("data-testid", "sidebar-nav-stages");
        railBtn.setAttribute("data-sidebar-nav-item", "stages");
        railBtn.setAttribute("title", "Stages & Volumes");
        railBtn.setAttribute("aria-label", "Stages & Volumes");
        railBtn.style.cssText =
          "position:fixed;left:4px;top:248px;z-index:45;width:32px;height:32px;display:none;align-items:center;justify-content:center;border-radius:6px;border:none;background:transparent;color:#64748b;cursor:pointer;transition:all 0.15s ease;";
        railBtn.innerHTML = buildBucketSvgIcon(20);
        railBtn.onclick = function (ev) {
          ev.preventDefault();
          ev.stopPropagation();
          toggleSnowflakeStagesVolumesView();
        };
        document.body.appendChild(railBtn);
      }
      if (flowId && tracesBtn) {
        const memBtn = document.querySelector(
          '[data-testid="sidebar-nav-memories"], [data-sidebar-nav-item="memories"]'
        );
        const trRect = tracesBtn.getBoundingClientRect();
        const memRect = memBtn ? memBtn.getBoundingClientRect() : null;
        if (trRect && trRect.width > 0) {
          // Place Bucket button at Traces' original vertical slot and shift Traces down by 40px so Bucket is directly above Traces
          const targetTop = memRect && memRect.bottom > 0 ? Math.round(memRect.bottom + 8) : Math.round(trRect.top);
          railBtn.style.left = Math.round(trRect.left) + "px";
          railBtn.style.top = targetTop + "px";
          const tracesLi = tracesBtn.closest("li") || tracesBtn;
          if (tracesLi && tracesLi.style.marginTop !== "40px") {
            tracesLi.style.marginTop = "40px";
          }
          railBtn.style.display = "inline-flex";
          railBtn.style.background = _stagesVolumesViewOpen ? "rgba(41,181,232,0.16)" : "transparent";
          railBtn.style.color = _stagesVolumesViewOpen ? "#29B5E8" : "#64748b";
        }
      } else if (railBtn.style.display !== "none") {
        railBtn.style.display = "none";
      }
    }

    // Close Stages & Volumes view automatically if user switches to another rail section or leaves /flow/<id>
    const reactStagesHost = document.getElementById("sflf-stages-react-host");
    if (reactStagesHost && !_stagesVolumesViewOpen) {
      openSnowflakeStagesVolumesView(true);
    } else if (!flowId && _stagesVolumesViewOpen) {
      closeSnowflakeStagesVolumesView();
    }
  }
  window.ensureLeftRailStagesVolumesTab = ensureLeftRailStagesVolumesTab;

  // Close Stages view when any other sidebar-nav button (components, mcp, bundles, versions, agent, memories, traces) is clicked
  document.addEventListener(
    "click",
    function (ev) {
      const navBtn =
        ev.target &&
        ev.target.closest &&
        ev.target.closest('[data-testid^="sidebar-nav-"], [data-sidebar-nav-item]');
      if (!navBtn) return;
      const item =
        navBtn.getAttribute("data-sidebar-nav-item") ||
        (navBtn.getAttribute("data-testid") || "").replace("sidebar-nav-", "");
      if (item === "stages") {
        openSnowflakeStagesVolumesView(true);
      } else if (_stagesVolumesViewOpen) {
        closeSnowflakeStagesVolumesView();
      }
    },
    true
  );

  function toggleSnowflakeStagesVolumesView() {
    if (_stagesVolumesViewOpen) {
      closeSnowflakeStagesVolumesView();
    } else {
      openSnowflakeStagesVolumesView(true);
    }
  }
  window.toggleSnowflakeStagesVolumesView = toggleSnowflakeStagesVolumesView;

  function closeSnowflakeStagesVolumesView() {
    _stagesVolumesViewOpen = false;
    const viewEl = document.getElementById("sflf-stages-volumes-view");
    if (viewEl) viewEl.style.display = "none";
    const railBtn = document.getElementById("sflf-rail-stages-btn");
    if (railBtn && railBtn.parentNode === document.body) {
      railBtn.style.background = "transparent";
      railBtn.style.color = "#64748b";
    }
  }
  window.closeSnowflakeStagesVolumesView = closeSnowflakeStagesVolumesView;

  function ensureStagesVolumesDomMounted() {
    let viewEl = document.getElementById("sflf-stages-volumes-view");
    if (viewEl) return viewEl;

    viewEl = document.createElement("div");
    viewEl.id = "sflf-stages-volumes-view";
    viewEl.setAttribute("data-testid", "snowflake-stages-view");
    viewEl.style.cssText =
      "position:fixed;top:48px;left:40px;right:0;bottom:0;z-index:40;background:#ffffff;color:#0f172a;display:none;flex-direction:row;font-family:Inter,sans-serif;overflow:hidden;border-top:1px solid #e2e8f0;";

    viewEl.innerHTML = `
      <!-- Left Sidebar (288px, symmetrical to Memories view) -->
      <aside style="width:288px;flex-shrink:0;border-right:1px solid #e2e8f0;background:#f8fafc;display:flex;flex-direction:column;height:100%;">
        <div style="padding:14px 16px;border-bottom:1px solid #e2e8f0;">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">
            <div style="display:flex;align-items:center;gap:8px;">
              <span style="color:#29B5E8;display:inline-flex;">${buildBucketSvgIcon(17)}</span>
              <h2 style="margin:0;font-size:14px;font-weight:700;color:#0f172a;">Stages &amp; Volumes</h2>
            </div>
            <button id="sflf-stages-close-view-btn" type="button" title="Return to Flow Canvas" style="background:transparent;border:1px solid #cbd5e1;border-radius:6px;padding:2px 7px;font-size:11px;color:#475569;cursor:pointer;">✕</button>
          </div>
          <div style="margin-top:12px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <button id="sflf-stages-create-btn" data-testid="stages-create-btn" type="button" style="display:inline-flex;align-items:center;gap:5px;background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:10px;padding:5px 11px;font-size:12px;font-weight:600;cursor:pointer;">
              <span>+</span><span>Create</span>
            </button>
            <button id="sflf-stages-plus-snowflake-btn" data-testid="stages-plus-snowflake-btn" type="button" style="display:inline-flex;align-items:center;gap:6px;background:#29B5E8;color:#ffffff;border:1px solid #29B5E8;border-radius:10px;padding:5px 11px;font-size:12px;font-weight:600;cursor:pointer;">
              <span>+</span>${buildOfficialSnowflakeSvg("#ffffff", 13)}<span>Snowflake</span>
            </button>
          </div>
        </div>
        <div style="padding:10px 12px;border-bottom:1px solid #e2e8f0;">
          <input id="sflf-stages-search-input" data-testid="search-stages-input" type="text" placeholder="Search stages & volumes..." style="width:100%;box-sizing:border-box;background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
        </div>
        <div id="sflf-stages-context-summary" style="padding:6px 12px;background:rgba(41,181,232,0.08);border-bottom:1px solid #e2e8f0;font-size:10.5px;color:#0369a1;font-weight:600;"></div>
        <div id="sflf-stages-list-container" data-testid="snowflake-stages-list" style="flex:1;overflow-y:auto;padding:8px;"></div>
      </aside>

      <!-- Right Detail Panel (Stage File Browser & Native Upload to Snowflake Stage) -->
      <main id="sflf-stage-detail-panel" data-testid="snowflake-stage-detail-panel" style="flex:1;display:flex;flex-direction:column;height:100%;overflow-y:auto;padding:20px 24px;background:#ffffff;"></main>
    `;

    document.body.appendChild(viewEl);

    const closeBtn = document.getElementById("sflf-stages-close-view-btn");
    if (closeBtn) {
      closeBtn.onclick = function () {
        closeSnowflakeStagesVolumesView();
      };
    }
    const createBtn = document.getElementById("sflf-stages-create-btn");
    if (createBtn) {
      createBtn.onclick = function () {
        openCreateSnowflakeStageModal("INTERNAL");
      };
    }
    const sfCreateBtn = document.getElementById("sflf-stages-plus-snowflake-btn");
    if (sfCreateBtn) {
      sfCreateBtn.onclick = function () {
        openCreateSnowflakeStageModal("INTERNAL");
      };
    }
    const searchInp = document.getElementById("sflf-stages-search-input");
    if (searchInp) {
      searchInp.oninput = function () {
        renderSnowflakeStagesVolumesList();
      };
    }
    return viewEl;
  }

  function openSnowflakeStagesVolumesView(fetchFresh) {
    const viewEl = ensureStagesVolumesDomMounted();
    _stagesVolumesViewOpen = true;
    viewEl.style.display = "flex";
    const railBtn = document.getElementById("sflf-rail-stages-btn");
    if (railBtn && railBtn.parentNode === document.body) {
      railBtn.style.background = "rgba(41,181,232,0.16)";
      railBtn.style.color = "#29B5E8";
    }
    renderSnowflakeStagesVolumesList();
    if (fetchFresh !== false) {
      fetchSnowflakeStagesAndVolumes();
    }
  }
  window.openSnowflakeStagesVolumesView = openSnowflakeStagesVolumesView;

  function syncStagesVolumesViewWithFlowContext(nextState) {
    if (nextState && typeof nextState === "object") {
      sfState = Object.assign(sfState, nextState);
    }
    renderSnowflakeStagesVolumesList();
    const createModal = document.getElementById("sflf-create-stage-modal");
    if (createModal && createModal.style.display && createModal.style.display !== "none") {
      syncCreateStageModalWithFlowContext(sfState);
    }
  }
  window.syncStagesVolumesViewWithFlowContext = syncStagesVolumesViewWithFlowContext;

  function renderSnowflakeStagesVolumesList() {
    const listContainer = document.getElementById("sflf-stages-list-container");
    const ctxSummary = document.getElementById("sflf-stages-context-summary");
    if (!listContainer) return;

    const selCtx = (sfState && sfState.selected_context) || {};
    const dbs = Array.isArray(selCtx.databases) ? selCtx.databases : [sfState.database || "SNOWFLAKE_LANGFLOW"];
    if (ctxSummary) {
      ctxSummary.textContent = `[ ❄️ Context ] Active Role: ${sfState.active_role || "ACCOUNTADMIN"} · Databases: ${
        dbs.length ? dbs.join(", ") : "None"
      }`;
    }

    const searchInp = document.getElementById("sflf-stages-search-input");
    const q = searchInp ? (searchInp.value || "").trim().toLowerCase() : "";
    const collected = collectContextStagesAndVolumes(sfState);
    const allItems = collected.stages.concat(collected.external_volumes).filter(function (item) {
      if (!q) return true;
      const hay = `${item.name || ""} ${item.stage_fqn || ""} ${item.stage_type || ""} ${
        item.cloud_provider || ""
      } ${item.storage_base_url || ""}`.toLowerCase();
      return hay.includes(q);
    });

    if (
      _selectedStageOrVolumeKey &&
      !allItems.some(function (it) {
        return it.key === _selectedStageOrVolumeKey;
      })
    ) {
      _selectedStageOrVolumeKey = allItems.length > 0 ? allItems[0].key : null;
    } else if (!_selectedStageOrVolumeKey && allItems.length > 0) {
      _selectedStageOrVolumeKey = allItems[0].key;
    }

    if (allItems.length === 0) {
      listContainer.innerHTML = `
        <div style="padding:24px 12px;text-align:center;color:#64748b;font-size:12px;">
          <div style="margin-bottom:8px;color:#94a3b8;">${buildBucketSvgIcon(28)}</div>
          <div style="font-weight:600;color:#334155;">No stages or volumes in active [ ❄️ Context ]</div>
          <div style="margin-top:4px;font-size:11px;">Select a schema in [ ❄️ Context ] or click <strong>+ ❄️ Snowflake</strong> to create a Stage or External Volume.</div>
        </div>
      `;
      renderSelectedStageOrVolumeDetail(null);
      return;
    }

    listContainer.innerHTML = allItems
      .map(function (item) {
        const isSel = item.key === _selectedStageOrVolumeKey;
        const badgeColor =
          item.stage_type === "EXTERNAL_VOLUME"
            ? "#7c3aed"
            : item.stage_type === "EXTERNAL"
              ? "#0284c7"
              : "#059669";
        const subtitle =
          item.stage_type === "EXTERNAL_VOLUME"
            ? `${item.storage_provider || "S3"} · ${item.storage_base_url || ""}`
            : `${item.stage_fqn} · ${item.encryption || "SNOWFLAKE_SSE"}`;
        return `
          <div class="sflf-stage-list-item" data-stage-key="${item.key}" style="padding:10px 12px;margin-bottom:6px;border-radius:8px;cursor:pointer;border:1px solid ${
            isSel ? "#29B5E8" : "#e2e8f0"
          };background:${isSel ? "rgba(41,181,232,0.10)" : "#ffffff"};display:flex;flex-direction:column;gap:4px;">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:6px;">
              <strong style="font-size:12.5px;color:#0f172a;word-break:break-all;">${
                item.stage_type === "EXTERNAL_VOLUME" ? item.name : item.stage_fqn
              }</strong>
              <span style="background:rgba(41,181,232,0.12);color:${badgeColor};border:1px solid ${badgeColor};border-radius:999px;padding:1px 7px;font-size:9.5px;font-weight:700;white-space:nowrap;">${
                item.stage_type
              }</span>
            </div>
            <div style="font-size:10.5px;color:#64748b;font-family:'JetBrains Mono',monospace;word-break:break-all;">${subtitle}</div>
          </div>
        `;
      })
      .join("");

    Array.from(listContainer.querySelectorAll(".sflf-stage-list-item")).forEach(function (el) {
      el.onclick = function () {
        const k = el.getAttribute("data-stage-key");
        _selectedStageOrVolumeKey = k;
        renderSnowflakeStagesVolumesList();
      };
    });

    const selectedObj =
      allItems.find(function (it) {
        return it.key === _selectedStageOrVolumeKey;
      }) || allItems[0];
    renderSelectedStageOrVolumeDetail(selectedObj);
  }

  async function renderSelectedStageOrVolumeDetail(item) {
    const panel = document.getElementById("sflf-stage-detail-panel");
    if (!panel) return;
    if (!item) {
      panel.innerHTML = `
        <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;color:#64748b;text-align:center;">
          ${buildBucketSvgIcon(42)}
          <h3 style="margin:12px 0 4px;font-size:15px;color:#0f172a;">No Stage or External Volume Selected</h3>
          <p style="margin:0;font-size:12.5px;">Select a Stage or External Volume from the left sidebar or click <strong>+ ❄️ Snowflake</strong> to provision one.</p>
        </div>
      `;
      return;
    }

    if (item.stage_type === "EXTERNAL_VOLUME") {
      panel.innerHTML = `
        <div style="display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #e2e8f0;padding-bottom:14px;margin-bottom:16px;">
          <div>
            <div style="display:flex;align-items:center;gap:8px;">
              <h3 style="margin:0;font-size:17px;font-weight:700;color:#0f172a;">${item.name}</h3>
              <span style="background:#f3e8ff;color:#7c3aed;border:1px solid #7c3aed;border-radius:999px;padding:2px 9px;font-size:11px;font-weight:700;">EXTERNAL_VOLUME</span>
            </div>
            <div style="margin-top:4px;font-size:12px;color:#475569;">Account-level Snowflake External Volume for Apache Iceberg Tables</div>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:14px;">
          <div><strong style="font-size:11px;color:#64748b;display:block;">Type</strong><span style="font-size:12.5px;font-weight:600;">EXTERNAL_VOLUME</span></div>
          <div><strong style="font-size:11px;color:#64748b;display:block;">Cloud Provider</strong><span style="font-size:12.5px;font-weight:600;">${item.storage_provider || "S3"}</span></div>
          <div><strong style="font-size:11px;color:#64748b;display:block;">Storage Base URL</strong><code style="font-size:11.5px;">${item.storage_base_url || ""}</code></div>
          <div><strong style="font-size:11px;color:#64748b;display:block;">Allow Writes</strong><span style="font-size:12.5px;font-weight:600;">${item.allow_writes !== false ? "TRUE" : "FALSE"}</span></div>
          <div><strong style="font-size:11px;color:#64748b;display:block;">Encryption</strong><span style="font-size:12.5px;font-weight:600;">${item.encryption || "AWS_SSE_S3"}</span></div>
        </div>
      `;
      return;
    }

    const stageFqn = item.stage_fqn || `@${item.database}.${item.schema}.${item.name}`;
    const cachedFiles = _stagesVolumesState.files_by_stage[stageFqn] || [];

    panel.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;border-bottom:1px solid #e2e8f0;padding-bottom:14px;margin-bottom:14px;">
        <div>
          <div style="display:flex;align-items:center;gap:8px;">
            <h3 style="margin:0;font-size:16px;font-weight:700;color:#0f172a;font-family:'JetBrains Mono',monospace;">${stageFqn}</h3>
            <span style="background:rgba(41,181,232,0.14);color:#0284c7;border:1px solid #29B5E8;border-radius:999px;padding:2px 9px;font-size:10.5px;font-weight:700;">Type: ${item.stage_type}</span>
          </div>
          <div style="margin-top:4px;font-size:11.5px;color:#475569;">
            Database.Schema: <code>${item.database}.${item.schema}</code> · Encryption: <code>${item.encryption || "SNOWFLAKE_SSE"}</code> · Directory Table: <code>${item.directory_enabled !== false ? "ENABLED" : "DISABLED"}</code>
            ${item.url ? ` · Cloud URL: <code>${item.url}</code>` : ""}
            ${item.storage_integration ? ` · Storage Integration: <code>${item.storage_integration}</code>` : ""}
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
          <input id="sflf-stage-upload-subpath" type="text" placeholder="Optional subpath (e.g. docs/)" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:11.5px;width:170px;" />
          <input id="sflf-stage-upload-input" type="file" multiple style="display:none;" />
          <button id="sflf-stage-upload-file-btn" data-testid="stage-upload-file-btn" type="button" style="display:inline-flex;align-items:center;gap:6px;background:#29B5E8;color:#ffffff;border:1px solid #29B5E8;border-radius:8px;padding:6px 12px;font-size:12px;font-weight:600;cursor:pointer;">
            <span>Upload Files to Stage</span>
          </button>
          <button id="sflf-stage-refresh-btn" data-testid="stage-refresh-btn" type="button" style="background:#f8fafc;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;font-weight:600;cursor:pointer;">
            Refresh Stage
          </button>
          <button id="sflf-stage-copy-fqn-btn" type="button" style="background:#f8fafc;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;cursor:pointer;">
            Copy @stage/path
          </button>
        </div>
      </div>

      <!-- Drag-and-Drop Stage Upload Zone -->
      <div id="sflf-stage-dropzone" data-testid="stage-upload-dropzone" style="border:2px dashed #94a3b8;border-radius:10px;padding:12px 16px;margin-bottom:14px;background:#f8fafc;text-align:center;font-size:12px;color:#475569;cursor:pointer;">
        Drag &amp; drop files here or click <strong>Upload Files to Stage</strong> to run governed <code>PUT</code> + <code>ALTER STAGE ${stageFqn.replace(/^@/, "")} REFRESH</code>
      </div>

      <div id="sflf-stage-file-preview-box" style="display:none;margin-bottom:14px;background:#0f172a;color:#e2e8f0;border-radius:8px;padding:10px 14px;font-family:'JetBrains Mono',monospace;font-size:11px;white-space:pre-wrap;"></div>

      <!-- Stage File Browser Table -->
      <div style="flex:1;overflow-x:auto;">
        <table id="sflf-stage-files-table" data-testid="snowflake-stage-files-table" style="width:100%;border-collapse:collapse;font-size:12px;">
          <thead>
            <tr style="background:#f8fafc;text-align:left;color:#475569;">
              <th style="padding:8px 10px;border-bottom:1px solid #cbd5e1;">Name / Path</th>
              <th style="padding:8px 10px;border-bottom:1px solid #cbd5e1;">Size</th>
              <th style="padding:8px 10px;border-bottom:1px solid #cbd5e1;">Last Modified</th>
              <th style="padding:8px 10px;border-bottom:1px solid #cbd5e1;">MD5</th>
              <th style="padding:8px 10px;border-bottom:1px solid #cbd5e1;">Actions</th>
            </tr>
          </thead>
          <tbody id="sflf-stage-files-tbody">
            ${renderStageFileRowsHtml(stageFqn, cachedFiles)}
          </tbody>
        </table>
      </div>
    `;

    const fileInput = document.getElementById("sflf-stage-upload-input");
    const uploadBtn = document.getElementById("sflf-stage-upload-file-btn");
    const dropzone = document.getElementById("sflf-stage-dropzone");
    if (uploadBtn && fileInput) {
      uploadBtn.onclick = function () {
        fileInput.click();
      };
      fileInput.onchange = function () {
        if (fileInput.files && fileInput.files.length > 0) {
          uploadFilesToSnowflakeStage(item, Array.from(fileInput.files));
        }
      };
    }
    if (dropzone && fileInput) {
      dropzone.onclick = function () {
        fileInput.click();
      };
      dropzone.ondragover = function (ev) {
        ev.preventDefault();
        dropzone.style.borderColor = "#29B5E8";
      };
      dropzone.ondragleave = function () {
        dropzone.style.borderColor = "#94a3b8";
      };
      dropzone.ondrop = function (ev) {
        ev.preventDefault();
        dropzone.style.borderColor = "#94a3b8";
        if (ev.dataTransfer && ev.dataTransfer.files && ev.dataTransfer.files.length > 0) {
          uploadFilesToSnowflakeStage(item, Array.from(ev.dataTransfer.files));
        }
      };
    }

    const refreshBtn = document.getElementById("sflf-stage-refresh-btn");
    if (refreshBtn) {
      refreshBtn.onclick = async function () {
        try {
          await fetch("/api/v1/snowflake/stages/refresh", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              database: item.database,
              schema: item.schema,
              stage_name: item.name,
              stage_fqn: stageFqn,
            }),
          });
        } catch (_e) {}
        const freshFiles = await fetchStageFilesList(stageFqn);
        const tbody = document.getElementById("sflf-stage-files-tbody");
        if (tbody) {
          tbody.innerHTML = renderStageFileRowsHtml(stageFqn, freshFiles);
          bindStageFileRowActions(item, stageFqn);
        }
      };
    }

    const copyBtn = document.getElementById("sflf-stage-copy-fqn-btn");
    if (copyBtn) {
      copyBtn.onclick = function () {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(stageFqn);
        }
        showDenialOrStatusBanner("ok", "STAGE_PATH_COPIED", `Copied ${stageFqn} to clipboard`);
      };
    }

    bindStageFileRowActions(item, stageFqn);
    const files = await fetchStageFilesList(stageFqn);
    const tbody = document.getElementById("sflf-stage-files-tbody");
    if (tbody) {
      tbody.innerHTML = renderStageFileRowsHtml(stageFqn, files);
      bindStageFileRowActions(item, stageFqn);
    }
  }

  function renderStageFileRowsHtml(stageFqn, files) {
    if (!Array.isArray(files) || files.length === 0) {
      return `<tr><td colspan="5" style="padding:16px;text-align:center;color:#64748b;">No files found in ${stageFqn}. Click <strong>Upload Files to Stage</strong> to upload files.</td></tr>`;
    }
    return files
      .map(function (f) {
        const fName = f.name || f.NAME || f.file_name || "";
        const fSize = f.size !== undefined ? f.size : f.SIZE !== undefined ? f.SIZE : 0;
        const fMod = f.last_modified || f.LAST_MODIFIED || "—";
        const fMd5 = f.md5 || f.MD5 || "—";
        return `
          <tr style="border-bottom:1px solid #f1f5f9;">
            <td style="padding:8px 10px;font-family:'JetBrains Mono',monospace;font-size:11.5px;color:#0f172a;">${fName}</td>
            <td style="padding:8px 10px;color:#475569;">${fSize} B</td>
            <td style="padding:8px 10px;color:#475569;">${fMod}</td>
            <td style="padding:8px 10px;font-family:'JetBrains Mono',monospace;font-size:10.5px;color:#64748b;">${fMd5}</td>
            <td style="padding:8px 10px;display:flex;gap:6px;">
              <button type="button" class="sflf-stage-row-btn" data-action="preview-file" data-file-path="${fName}" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;padding:3px 8px;font-size:11px;cursor:pointer;">Preview / Read</button>
              <button type="button" class="sflf-stage-row-btn" data-action="copy-file-path" data-file-path="${fName}" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;padding:3px 8px;font-size:11px;cursor:pointer;">Copy @stage/path</button>
              <button type="button" class="sflf-stage-row-btn" data-action="delete-file" data-file-path="${fName}" style="background:#fef2f2;color:#b91c1c;border:1px solid #fecaca;border-radius:6px;padding:3px 8px;font-size:11px;cursor:pointer;">Delete File</button>
            </td>
          </tr>
        `;
      })
      .join("");
  }

  function bindStageFileRowActions(stageItem, stageFqn) {
    const tbody = document.getElementById("sflf-stage-files-tbody");
    if (!tbody) return;
    const cleanFqn = String(stageFqn).replace(/^@/, "");
    Array.from(tbody.querySelectorAll(".sflf-stage-row-btn")).forEach(function (btn) {
      btn.onclick = async function () {
        const action = btn.getAttribute("data-action");
        const rawPath = btn.getAttribute("data-file-path") || "";
        const relPath = rawPath.replace(/^@[^/]+\//, "").replace(new RegExp(`^${stageItem.name}/`, "i"), "");
        const fullStagePath = `@${cleanFqn}/${relPath}`;
        if (action === "copy-file-path") {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(fullStagePath);
          }
          showDenialOrStatusBanner("ok", "COPIED", `Copied ${fullStagePath}`);
        } else if (action === "preview-file") {
          const previewBox = document.getElementById("sflf-stage-file-preview-box");
          if (previewBox) {
            previewBox.style.display = "block";
            previewBox.textContent = `Stage File Reference: ${fullStagePath}\nSQL Read: SELECT $1 FROM ${fullStagePath} (FILE_FORMAT => (TYPE = 'CSV'))\nCortex Parse: SELECT SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT(TO_FILE('${stageFqn}', '${relPath}'), {'mode': 'LAYOUT'})`;
          }
        } else if (action === "delete-file") {
          try {
            let delResp = await fetch(
              `/api/v1/snowflake/stages/${encodeURIComponent(cleanFqn)}/files?file_path=${encodeURIComponent(relPath)}`,
              { method: "DELETE" }
            );
            if (!delResp.ok) {
              delResp = await fetch(
                `/api/v1/snowflake/stages/files?stage_fqn=${encodeURIComponent(cleanFqn)}&file_path=${encodeURIComponent(relPath)}`,
                { method: "DELETE" }
              );
            }
            if (delResp.ok) {
              showDenialOrStatusBanner("ok", "FILE_REMOVED", `Removed ${fullStagePath}`);
              const fresh = await fetchStageFilesList(stageFqn);
              tbody.innerHTML = renderStageFileRowsHtml(stageFqn, fresh);
              bindStageFileRowActions(stageItem, stageFqn);
            }
          } catch (_e) {}
        }
      };
    });
  }

  async function uploadFilesToSnowflakeStage(stageItem, fileList) {
    if (!stageItem || !Array.isArray(fileList) || fileList.length === 0) return;
    const subpathInp = document.getElementById("sflf-stage-upload-subpath");
    const subpath = subpathInp ? (subpathInp.value || "").trim().replace(/^\/+|\/+$/g, "") : "";
    const stageFqn = stageItem.stage_fqn || `@${stageItem.database}.${stageItem.schema}.${stageItem.name}`;

    for (let i = 0; i < fileList.length; i += 1) {
      const file = fileList[i];
      const base64Content = await new Promise(function (resolve) {
        const reader = new FileReader();
        reader.onload = function () {
          const res = String(reader.result || "");
          const commaIdx = res.indexOf(",");
          resolve(commaIdx !== -1 ? res.slice(commaIdx + 1) : res);
        };
        reader.onerror = function () {
          resolve("");
        };
        reader.readAsDataURL(file);
      });

      try {
        const resp = await fetch("/api/v1/snowflake/stages/upload", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            database: stageItem.database,
            schema: stageItem.schema,
            stage_name: stageItem.name,
            stage_fqn: stageFqn,
            subpath: subpath,
            file_name: file.name,
            content_base64: base64Content,
            flow_id: extractCurrentFlowId(),
          }),
        });
        const data = await resp.json();
        if (!resp.ok || data.ok === false) {
          showDenialOrStatusBanner("error", data.error_code || "STAGE_UPLOAD_FAILED", data.message || "Upload failed");
        } else {
          showDenialOrStatusBanner(
            "ok",
            "STAGE_FILE_UPLOADED",
            `Uploaded ${file.name} to ${stageFqn}${subpath ? "/" + subpath : ""}`
          );
        }
      } catch (err) {
        showDenialOrStatusBanner("error", "STAGE_UPLOAD_ERROR", String(err));
      }
    }

    const freshFiles = await fetchStageFilesList(stageFqn);
    const tbody = document.getElementById("sflf-stage-files-tbody");
    if (tbody) {
      tbody.innerHTML = renderStageFileRowsHtml(stageFqn, freshFiles);
      bindStageFileRowActions(stageItem, stageFqn);
    }
  }
  window.uploadFilesToSnowflakeStage = uploadFilesToSnowflakeStage;

  // =========================================================================
  // W2-R5-05: `+ Create Stage / Volume` Wizard Modal (`id="sflf-create-stage-modal"`, `data-testid="create-snowflake-stage-modal"`)
  // =========================================================================
  function buildStageOrVolumeWizardDdlPreview() {
    const kindEl = document.getElementById("sflf-stage-wiz-kind");
    const kind = (kindEl && kindEl.value) || "INTERNAL";
    const modeEl = document.getElementById("sflf-stage-wiz-create-mode");
    const createMode = (modeEl && modeEl.value) || "IF_NOT_EXISTS";
    const dbEl = document.getElementById("sflf-stage-wiz-db");
    const newDbEl = document.getElementById("sflf-stage-wiz-new-db");
    const schEl = document.getElementById("sflf-stage-wiz-schema");
    const newSchEl = document.getElementById("sflf-stage-wiz-new-schema");
    const db =
      dbEl && dbEl.value === "__CREATE_NEW_DB__"
        ? (newDbEl && newDbEl.value.trim()) || "NEW_DB"
        : (dbEl && dbEl.value) || sfState.database || "SNOWFLAKE_LANGFLOW";
    const sch =
      schEl && schEl.value === "__CREATE_NEW_SCHEMA__"
        ? (newSchEl && newSchEl.value.trim()) || "NEW_SCHEMA"
        : (schEl && schEl.value) || sfState.schema || "CORE";

    if (kind === "EXTERNAL_VOLUME") {
      const volName = ((document.getElementById("sflf-stage-wiz-vol-name") || {}).value || "ICEBERG_EXT_VOL").trim();
      const allowWrites = ((document.getElementById("sflf-stage-wiz-vol-allow-writes") || {}).value || "TRUE") === "TRUE";
      const locName = ((document.getElementById("sflf-stage-wiz-vol-loc-name") || {}).value || "primary_loc").trim();
      const provider = ((document.getElementById("sflf-stage-wiz-vol-provider") || {}).value || "S3").trim();
      const baseUrl = ((document.getElementById("sflf-stage-wiz-vol-base-url") || {}).value || "s3://my-iceberg-bucket/warehouse/").trim();
      const roleArn = ((document.getElementById("sflf-stage-wiz-vol-aws-role-arn") || {}).value || "").trim();
      const extId = ((document.getElementById("sflf-stage-wiz-vol-aws-external-id") || {}).value || "").trim();
      const apArn = ((document.getElementById("sflf-stage-wiz-vol-aws-access-point-arn") || {}).value || "").trim();
      const tenantId = ((document.getElementById("sflf-stage-wiz-vol-azure-tenant-id") || {}).value || "").trim();
      const storageEndpoint = ((document.getElementById("sflf-stage-wiz-vol-storage-endpoint") || {}).value || "").trim();
      const encType = ((document.getElementById("sflf-stage-wiz-vol-encryption") || {}).value || "NONE").trim();
      const kmsKey = ((document.getElementById("sflf-stage-wiz-vol-kms-key") || {}).value || "").trim();
      const usePrivatelink = ((document.getElementById("sflf-stage-wiz-vol-privatelink") || {}).value || "FALSE") === "TRUE";
      const comment = ((document.getElementById("sflf-stage-wiz-comment") || {}).value || "").trim();

      const createVerb =
        createMode === "OR_REPLACE"
          ? "CREATE OR REPLACE EXTERNAL VOLUME"
          : "CREATE EXTERNAL VOLUME IF NOT EXISTS";
      const locParts = [
        `NAME = '${locName}'`,
        `STORAGE_PROVIDER = '${provider}'`,
        `STORAGE_BASE_URL = '${baseUrl}'`,
      ];
      if (roleArn) locParts.push(`STORAGE_AWS_ROLE_ARN = '${roleArn}'`);
      if (extId) locParts.push(`STORAGE_AWS_EXTERNAL_ID = '${extId}'`);
      if (apArn) locParts.push(`STORAGE_AWS_ACCESS_POINT_ARN = '${apArn}'`);
      if (tenantId) locParts.push(`AZURE_TENANT_ID = '${tenantId}'`);
      if (storageEndpoint) locParts.push(`STORAGE_ENDPOINT = '${storageEndpoint}'`);
      if (encType && encType !== "NONE") {
        locParts.push(
          kmsKey
            ? `ENCRYPTION = (TYPE = '${encType}' KMS_KEY_ID = '${kmsKey}')`
            : `ENCRYPTION = (TYPE = '${encType}')`
        );
      }
      if (usePrivatelink) locParts.push("USE_PRIVATELINK_ENDPOINT = TRUE");
      let ddl = `${createVerb} ${volName}\n  STORAGE_LOCATIONS = (\n    (\n      ${locParts.join("\n      ")}\n    )\n  )\n  ALLOW_WRITES = ${allowWrites ? "TRUE" : "FALSE"}`;
      if (comment) {
        ddl += `\n  COMMENT = '${comment.replace(/'/g, "''")}'`;
      }
      return ddl + ";";
    }

    const stageName = ((document.getElementById("sflf-stage-wiz-name") || {}).value || "DOCS_STAGE").trim();
    const createPrefix =
      createMode === "OR_REPLACE"
        ? `CREATE OR REPLACE STAGE ${db}.${sch}.${stageName}`
        : createMode === "TEMPORARY"
          ? `CREATE TEMPORARY STAGE IF NOT EXISTS ${db}.${sch}.${stageName}`
          : `CREATE STAGE IF NOT EXISTS ${db}.${sch}.${stageName}`;
    const dirEnable = ((document.getElementById("sflf-stage-wiz-dir-enable") || {}).value || "TRUE") === "TRUE";
    const dirAutoRefresh = ((document.getElementById("sflf-stage-wiz-dir-autorefresh") || {}).value || "FALSE") === "TRUE";
    const notifInt = ((document.getElementById("sflf-stage-wiz-notif-int") || {}).value || "").trim();
    const fileFmt = ((document.getElementById("sflf-stage-wiz-file-format") || {}).value || "NONE").trim();
    const comment = ((document.getElementById("sflf-stage-wiz-comment") || {}).value || "").trim();

    if (kind === "INTERNAL") {
      const intEnc = ((document.getElementById("sflf-stage-wiz-internal-encryption") || {}).value || "SNOWFLAKE_SSE").trim();
      const clauses = [`ENCRYPTION = (TYPE = '${intEnc}')`];
      if (dirEnable) {
        clauses.push(`DIRECTORY = (ENABLE = TRUE${dirAutoRefresh ? " AUTO_REFRESH = TRUE" : ""})`);
      }
      if (fileFmt && fileFmt !== "NONE") {
        clauses.push(`FILE_FORMAT = (TYPE = '${fileFmt}')`);
      }
      if (comment) {
        clauses.push(`COMMENT = '${comment.replace(/'/g, "''")}'`);
      }
      return `${createPrefix}\n  ${clauses.join("\n  ")};`;
    }

    // EXTERNAL STAGE (S3 / GCS / AZURE / S3COMPAT)
    const extUrl = ((document.getElementById("sflf-stage-wiz-ext-url") || {}).value || "s3://enterprise-bucket/docs/").trim();
    const authMode = ((document.getElementById("sflf-stage-wiz-auth-mode") || {}).value || "STORAGE_INTEGRATION").trim();
    const storageInt = ((document.getElementById("sflf-stage-wiz-storage-integration") || {}).value || "S3_DOCS_INT").trim();
    const credsRaw = ((document.getElementById("sflf-stage-wiz-credentials") || {}).value || "").trim();
    const extEnc = ((document.getElementById("sflf-stage-wiz-ext-encryption") || {}).value || "AWS_SSE_S3").trim();
    const kmsKey = ((document.getElementById("sflf-stage-wiz-ext-kms-key") || {}).value || "").trim();
    const s3CompatEndpoint = ((document.getElementById("sflf-stage-wiz-s3compat-endpoint") || {}).value || "").trim();
    const usePrivatelink = ((document.getElementById("sflf-stage-wiz-ext-privatelink") || {}).value || "FALSE") === "TRUE";

    const extClauses = [`URL = '${extUrl}'`];
    if (s3CompatEndpoint && extUrl.toLowerCase().startsWith("s3compat://")) {
      extClauses.push(`ENDPOINT = '${s3CompatEndpoint}'`);
    }
    if (authMode === "STORAGE_INTEGRATION" && storageInt) {
      extClauses.push(`STORAGE_INTEGRATION = ${storageInt}`);
    } else if (authMode === "CREDENTIALS" && credsRaw) {
      extClauses.push(`CREDENTIALS = (${credsRaw})`);
    }
    if (extEnc && extEnc !== "NONE") {
      extClauses.push(
        kmsKey
          ? `ENCRYPTION = (TYPE = '${extEnc}' KMS_KEY_ID = '${kmsKey}')`
          : `ENCRYPTION = (TYPE = '${extEnc}')`
      );
    }
    if (usePrivatelink) {
      extClauses.push("USE_PRIVATELINK_ENDPOINT = TRUE");
    }
    if (dirEnable) {
      let dirSpec = `ENABLE = TRUE${dirAutoRefresh ? " AUTO_REFRESH = TRUE" : ""}`;
      if (notifInt) dirSpec += ` NOTIFICATION_INTEGRATION = '${notifInt}'`;
      extClauses.push(`DIRECTORY = (${dirSpec})`);
    }
    if (fileFmt && fileFmt !== "NONE") {
      extClauses.push(`FILE_FORMAT = (TYPE = '${fileFmt}')`);
    }
    if (comment) {
      extClauses.push(`COMMENT = '${comment.replace(/'/g, "''")}'`);
    }
    return `${createPrefix}\n  ${extClauses.join("\n  ")};`;
  }

  function syncCreateStageModalWithFlowContext(ctxState) {
    const activeCtx = ctxState || sfState || {};
    const selCtx = activeCtx.selected_context || {};
    const privs = computeEffectivePrivileges();
    const dbs =
      Array.isArray(selCtx.databases) && selCtx.databases.length > 0
        ? selCtx.databases.slice()
        : [activeCtx.database || "SNOWFLAKE_LANGFLOW"];
    const schemasMap = selCtx.schemas_by_db || {};

    const dbSel = document.getElementById("sflf-stage-wiz-db");
    const schSel = document.getElementById("sflf-stage-wiz-schema");
    if (dbSel) {
      const prevDb = dbSel.value;
      const opts = dbs.map(function (d) {
        return `<option value="${d}">${d}</option>`;
      });
      if (privs.can_create_database) {
        opts.push('<option value="__CREATE_NEW_DB__">+ Create New Database...</option>');
      }
      dbSel.innerHTML = opts.join("");
      if (prevDb && dbs.indexOf(prevDb) !== -1) {
        dbSel.value = prevDb;
      } else {
        dbSel.value = dbs[0] || "SNOWFLAKE_LANGFLOW";
      }
    }
    if (schSel) {
      const curDb = (dbSel && dbSel.value) || dbs[0] || "SNOWFLAKE_LANGFLOW";
      const schList =
        Array.isArray(schemasMap[curDb]) && schemasMap[curDb].length > 0
          ? schemasMap[curDb].slice()
          : [activeCtx.schema || "CORE"];
      const prevSch = schSel.value;
      const schOpts = schList.map(function (s) {
        return `<option value="${s}">${s}</option>`;
      });
      if (privs.can_create_schema) {
        schOpts.push('<option value="__CREATE_NEW_SCHEMA__">+ Create New Schema...</option>');
      }
      schSel.innerHTML = schOpts.join("");
      if (prevSch && schList.indexOf(prevSch) !== -1) {
        schSel.value = prevSch;
      } else {
        schSel.value = schList[0] || "CORE";
      }
    }

    const roleBadge = document.getElementById("sflf-stage-wiz-role-badge");
    if (roleBadge) {
      roleBadge.textContent = `Active Role: ${activeCtx.active_role || "ACCOUNTADMIN"}`;
    }

    const extVolBanner = document.getElementById("sflf-extvol-privilege-banner");
    const kindEl = document.getElementById("sflf-stage-wiz-kind");
    const curKind = (kindEl && kindEl.value) || "INTERNAL";
    if (extVolBanner) {
      if (curKind === "EXTERNAL_VOLUME" && !privs.can_create_external_volume) {
        extVolBanner.style.display = "block";
        extVolBanner.textContent = `Active role ${activeCtx.active_role || "PUBLIC"} does not have CREATE EXTERNAL VOLUME privilege`;
      } else if (curKind !== "EXTERNAL_VOLUME" && !privs.can_create_stage) {
        extVolBanner.style.display = "block";
        extVolBanner.textContent = `Active role ${activeCtx.active_role || "PUBLIC"} does not have CREATE STAGE privilege`;
      } else {
        extVolBanner.style.display = "none";
      }
    }

    const previewEl = document.getElementById("sflf-stage-wiz-ddl-preview");
    if (previewEl) {
      previewEl.textContent = buildStageOrVolumeWizardDdlPreview();
    }
  }
  window.syncCreateStageModalWithFlowContext = syncCreateStageModalWithFlowContext;

  function openCreateSnowflakeStageModal(initialKind) {
    if (typeof document === "undefined" || !document.body) return;
    let modal = document.getElementById("sflf-create-stage-modal");
    if (!modal) {
      modal = document.createElement("div");
      modal.id = "sflf-create-stage-modal";
      modal.setAttribute("data-testid", "create-snowflake-stage-modal");
      modal.style.cssText =
        "position:fixed;inset:0;z-index:100001;background:rgba(15,23,42,0.55);display:none;align-items:center;justify-content:center;padding:20px;font-family:Inter,sans-serif;";

      modal.innerHTML = `
        <div style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:14px;width:min(760px,96vw);max-height:90vh;display:flex;flex-direction:column;box-shadow:0 20px 50px rgba(15,23,42,0.3);overflow:hidden;">
          <div style="padding:16px 20px;border-bottom:1px solid #e2e8f0;display:flex;align-items:center;justify-content:space-between;background:#f8fafc;">
            <div style="display:flex;align-items:center;gap:10px;">
              ${buildOfficialSnowflakeSvg("#29B5E8", 20)}
              <div>
                <h3 style="margin:0;font-size:15px;font-weight:700;color:#0f172a;">Create Snowflake Stage or External Volume</h3>
                <div style="font-size:11.5px;color:#475569;">Governed SQL provisioning integrated with active workflow [ ❄️ Context ]</div>
              </div>
            </div>
            <div style="display:flex;align-items:center;gap:10px;">
              <span id="sflf-stage-wiz-role-badge" style="background:rgba(41,181,232,0.14);color:#0284c7;border:1px solid #29B5E8;border-radius:999px;padding:3px 10px;font-size:11px;font-weight:700;"></span>
              <button id="sflf-stage-wiz-close-btn" type="button" style="background:transparent;border:none;font-size:16px;color:#64748b;cursor:pointer;">✕</button>
            </div>
          </div>

          <div style="padding:18px 20px;overflow-y:auto;display:flex;flex-direction:column;gap:14px;">
            <div id="sflf-extvol-privilege-banner" data-testid="external-volume-privilege-banner" style="display:none;background:#fef2f2;color:#991b1b;border:1px solid #f87171;border-radius:8px;padding:10px 12px;font-size:12px;font-weight:600;"></div>

            <!-- Object Type Selector (Internal Stage | External Stage | External Volume) -->
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>1. Infrastructure Object Type</span>
                <select id="sflf-stage-wiz-kind" data-testid="stage-wizard-kind-select" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:7px 10px;font-size:12.5px;">
                  <option value="INTERNAL">Option 1 — Internal Stage (SNOWFLAKE_SSE / SNOWFLAKE_FULL)</option>
                  <option value="EXTERNAL">Option 2 — External Stage (S3 / GCS / Azure / S3-Compatible)</option>
                  <option value="EXTERNAL_VOLUME">Option 3 — External Volume (Account-Level for Iceberg)</option>
                </select>
              </label>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>2. DDL Creation Mode</span>
                <select id="sflf-stage-wiz-create-mode" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:7px 10px;font-size:12.5px;">
                  <option value="IF_NOT_EXISTS">CREATE IF NOT EXISTS (Safe Idempotent)</option>
                  <option value="OR_REPLACE">CREATE OR REPLACE</option>
                  <option value="TEMPORARY">CREATE TEMPORARY STAGE</option>
                </select>
              </label>
            </div>

            <!-- Database & Schema Scope (for Internal & External Stages) -->
            <div id="sflf-stage-wiz-db-schema-section" style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;">
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Database ([ ❄️ Context ])</span>
                <select id="sflf-stage-wiz-db" data-testid="stage-wizard-db-select" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:7px 10px;font-size:12px;"></select>
                <input id="sflf-stage-wiz-new-db" type="text" placeholder="NEW_DATABASE_NAME" style="display:none;margin-top:4px;background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
              </label>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Schema ([ ❄️ Context ])</span>
                <select id="sflf-stage-wiz-schema" data-testid="stage-wizard-schema-select" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:7px 10px;font-size:12px;"></select>
                <input id="sflf-stage-wiz-new-schema" type="text" placeholder="NEW_SCHEMA_NAME" style="display:none;margin-top:4px;background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
              </label>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Stage Name</span>
                <input id="sflf-stage-wiz-name" data-testid="stage-wizard-name-input" type="text" value="DOCS_STAGE" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:7px 10px;font-size:12px;" />
              </label>
            </div>

            <!-- Option 1: Internal Stage Section -->
            <div id="sflf-stage-wiz-internal-section" style="display:grid;grid-template-columns:1fr 1fr;gap:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:12px;">
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Internal Stage Encryption</span>
                <select id="sflf-stage-wiz-internal-encryption" data-testid="stage-wizard-internal-encryption" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                  <option value="SNOWFLAKE_SSE">SNOWFLAKE_SSE (Server-Side — Required for AI_PARSE_DOCUMENT &amp; Directory Tables)</option>
                  <option value="SNOWFLAKE_FULL">SNOWFLAKE_FULL (Client + Server Encryption)</option>
                </select>
              </label>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Default File Format</span>
                <select id="sflf-stage-wiz-file-format" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                  <option value="NONE">None (Unstructured PDF / DOCX / Images / Mixed)</option>
                  <option value="CSV">CSV</option>
                  <option value="JSON">JSON</option>
                  <option value="PARQUET">PARQUET</option>
                  <option value="AVRO">AVRO</option>
                  <option value="ORC">ORC</option>
                  <option value="XML">XML</option>
                </select>
              </label>
            </div>

            <!-- Option 2: External Stage Section -->
            <div id="sflf-stage-wiz-external-section" style="display:none;flex-direction:column;gap:10px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:12px;">
              <div style="display:grid;grid-template-columns:1fr 2fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>Cloud Provider</span>
                  <select id="sflf-stage-wiz-cloud-provider" data-testid="stage-wizard-cloud-provider" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                    <option value="S3">AWS S3 (s3:// / s3gov://)</option>
                    <option value="GCS">Google Cloud Storage (gcs://)</option>
                    <option value="AZURE">Microsoft Azure Blob / ADLS Gen2 (azure://)</option>
                    <option value="S3COMPAT">S3-Compatible Storage (s3compat://)</option>
                  </select>
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>Cloud Bucket / Container URL</span>
                  <input id="sflf-stage-wiz-ext-url" data-testid="stage-wizard-ext-url" type="text" value="s3://enterprise-bucket/docs/" placeholder="s3://bucket/path/ | gcs://bucket/path/ | azure://account.blob.core.windows.net/container/" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
                </label>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>Authentication Mode</span>
                  <select id="sflf-stage-wiz-auth-mode" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                    <option value="STORAGE_INTEGRATION">STORAGE_INTEGRATION (Recommended)</option>
                    <option value="CREDENTIALS">Direct Cloud CREDENTIALS</option>
                  </select>
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>Storage Integration</span>
                  <select id="sflf-stage-wiz-storage-integration" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                    ${(_stagesVolumesState.storage_integrations || ["S3_DOCS_INT", "GCS_DOCS_INT", "AZURE_DOCS_INT"])
                      .map(function (si) {
                        return `<option value="${si}">${si}</option>`;
                      })
                      .join("")}
                  </select>
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>Cloud Encryption</span>
                  <select id="sflf-stage-wiz-ext-encryption" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                    <option value="AWS_SSE_S3">AWS_SSE_S3</option>
                    <option value="AWS_SSE_KMS">AWS_SSE_KMS</option>
                    <option value="AWS_CSE">AWS_CSE</option>
                    <option value="GCS_SSE_KMS">GCS_SSE_KMS</option>
                    <option value="AZURE_CSE">AZURE_CSE</option>
                    <option value="NONE">NONE</option>
                  </select>
                </label>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>KMS Key ID (optional)</span>
                  <input id="sflf-stage-wiz-ext-kms-key" type="text" placeholder="arn:aws:kms:..." style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>S3-Compatible Endpoint (optional)</span>
                  <input id="sflf-stage-wiz-s3compat-endpoint" type="text" placeholder="s3.my-domain.com" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>USE_PRIVATELINK_ENDPOINT</span>
                  <select id="sflf-stage-wiz-ext-privatelink" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;">
                    <option value="FALSE">FALSE</option>
                    <option value="TRUE">TRUE</option>
                  </select>
                </label>
              </div>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                <span>Direct Credentials Clause (only when Authentication Mode = CREDENTIALS)</span>
                <input id="sflf-stage-wiz-credentials" type="text" placeholder="AWS_ROLE = 'arn:aws:iam::123456789012:role/myrole' | AZURE_SAS_TOKEN = '...'" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
              </label>
            </div>

            <!-- Directory Table Section (shared by Internal & External Stages) -->
            <div id="sflf-stage-wiz-directory-section" style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:12px;">
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Directory Table (DIRECTORY = ENABLE)</span>
                <select id="sflf-stage-wiz-dir-enable" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                  <option value="TRUE">TRUE (Recommended for Cortex Search &amp; File Browser)</option>
                  <option value="FALSE">FALSE</option>
                </select>
              </label>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>AUTO_REFRESH</span>
                <select id="sflf-stage-wiz-dir-autorefresh" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                  <option value="FALSE">FALSE</option>
                  <option value="TRUE">TRUE</option>
                </select>
              </label>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                <span>Notification Integration (optional)</span>
                <input id="sflf-stage-wiz-notif-int" type="text" placeholder="S3_EVENT_NOTIF_INT" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
              </label>
            </div>

            <!-- Option 3: External Volume Section -->
            <div id="sflf-stage-wiz-extvol-section" style="display:none;flex-direction:column;gap:10px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:12px;">
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>External Volume Name</span>
                  <input id="sflf-stage-wiz-vol-name" data-testid="stage-wizard-volume-name" type="text" value="ICEBERG_EXT_VOL" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>ALLOW_WRITES</span>
                  <select id="sflf-stage-wiz-vol-allow-writes" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                    <option value="TRUE">TRUE</option>
                    <option value="FALSE">FALSE</option>
                  </select>
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>Storage Location NAME</span>
                  <input id="sflf-stage-wiz-vol-loc-name" type="text" value="primary_s3_loc" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
                </label>
              </div>
              <div style="display:grid;grid-template-columns:1fr 2fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>STORAGE_PROVIDER</span>
                  <select id="sflf-stage-wiz-vol-provider" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;">
                    <option value="S3">S3</option>
                    <option value="S3GOV">S3GOV</option>
                    <option value="GCS">GCS</option>
                    <option value="AZURE">AZURE</option>
                    <option value="S3COMPAT">S3COMPAT</option>
                  </select>
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
                  <span>STORAGE_BASE_URL</span>
                  <input id="sflf-stage-wiz-vol-base-url" type="text" value="s3://my-iceberg-bucket/warehouse/" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
                </label>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>STORAGE_AWS_ROLE_ARN (S3)</span>
                  <input id="sflf-stage-wiz-vol-aws-role-arn" type="text" placeholder="arn:aws:iam::123456789012:role/iceberg-role" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>STORAGE_AWS_EXTERNAL_ID</span>
                  <input id="sflf-stage-wiz-vol-aws-external-id" type="text" placeholder="iceberg_ext_id" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>STORAGE_AWS_ACCESS_POINT_ARN</span>
                  <input id="sflf-stage-wiz-vol-aws-access-point-arn" type="text" placeholder="arn:aws:s3:..." style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:10px;">
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>AZURE_TENANT_ID</span>
                  <input id="sflf-stage-wiz-vol-azure-tenant-id" type="text" placeholder="tenant-uuid" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>STORAGE_ENDPOINT (S3COMPAT)</span>
                  <input id="sflf-stage-wiz-vol-storage-endpoint" type="text" placeholder="https://s3compat.local" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>ENCRYPTION TYPE</span>
                  <select id="sflf-stage-wiz-vol-encryption" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;">
                    <option value="AWS_SSE_S3">AWS_SSE_S3</option>
                    <option value="AWS_SSE_KMS">AWS_SSE_KMS</option>
                    <option value="GCS_SSE_KMS">GCS_SSE_KMS</option>
                    <option value="NONE">NONE</option>
                  </select>
                </label>
                <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                  <span>KMS_KEY_ID</span>
                  <input id="sflf-stage-wiz-vol-kms-key" type="text" placeholder="kms-key-id" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;" />
                </label>
              </div>
              <label style="display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:600;">
                <span>USE_PRIVATELINK_ENDPOINT</span>
                <select id="sflf-stage-wiz-vol-privatelink" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:6px;padding:5px 8px;font-size:11.5px;max-width:180px;">
                  <option value="FALSE">FALSE</option>
                  <option value="TRUE">TRUE</option>
                </select>
              </label>
            </div>

            <label style="display:flex;flex-direction:column;gap:4px;font-size:12px;font-weight:600;">
              <span>Comment (optional)</span>
              <input id="sflf-stage-wiz-comment" type="text" placeholder="Governed Snowflake Stage / External Volume created from Snowflake Langflow" style="background:#ffffff;color:#0f172a;border:1px solid #cbd5e1;border-radius:8px;padding:6px 10px;font-size:12px;" />
            </label>

            <div>
              <div style="font-size:11.5px;font-weight:700;color:#334155;margin-bottom:4px;">Live Governed Snowflake SQL DDL Preview</div>
              <pre id="sflf-stage-wiz-ddl-preview" data-testid="stage-wizard-ddl-preview" style="margin:0;background:#0f172a;color:#e2e8f0;padding:10px 14px;border-radius:8px;font-family:'JetBrains Mono',monospace;font-size:11px;white-space:pre-wrap;overflow-x:auto;"></pre>
            </div>
          </div>

          <div style="padding:14px 20px;border-top:1px solid #e2e8f0;background:#f8fafc;display:flex;align-items:center;justify-content:flex-end;gap:10px;">
            <button id="sflf-stage-wiz-cancel-btn" type="button" style="background:#ffffff;color:#334155;border:1px solid #cbd5e1;border-radius:8px;padding:7px 14px;font-size:12.5px;font-weight:600;cursor:pointer;">Cancel</button>
            <button id="sflf-stage-wiz-submit-btn" data-testid="stage-wizard-submit-btn" type="button" style="background:#29B5E8;color:#ffffff;border:1px solid #29B5E8;border-radius:8px;padding:7px 16px;font-size:12.5px;font-weight:700;cursor:pointer;">Create &amp; Add to [ ❄️ Context ]</button>
          </div>
        </div>
      `;

      document.body.appendChild(modal);

      const updateKindVisibility = function () {
        const k = (document.getElementById("sflf-stage-wiz-kind") || {}).value || "INTERNAL";
        const dbSchSec = document.getElementById("sflf-stage-wiz-db-schema-section");
        const intSec = document.getElementById("sflf-stage-wiz-internal-section");
        const extSec = document.getElementById("sflf-stage-wiz-external-section");
        const dirSec = document.getElementById("sflf-stage-wiz-directory-section");
        const volSec = document.getElementById("sflf-stage-wiz-extvol-section");

        if (dbSchSec) dbSchSec.style.display = k === "EXTERNAL_VOLUME" ? "none" : "grid";
        if (intSec) intSec.style.display = k === "INTERNAL" ? "grid" : "none";
        if (extSec) extSec.style.display = k === "EXTERNAL" ? "flex" : "none";
        if (dirSec) dirSec.style.display = k === "EXTERNAL_VOLUME" ? "none" : "grid";
        if (volSec) volSec.style.display = k === "EXTERNAL_VOLUME" ? "flex" : "none";
        syncCreateStageModalWithFlowContext(sfState);
      };

      const kindSel = document.getElementById("sflf-stage-wiz-kind");
      if (kindSel) kindSel.onchange = updateKindVisibility;

      const dbSel = document.getElementById("sflf-stage-wiz-db");
      if (dbSel) {
        dbSel.onchange = function () {
          const newDbInp = document.getElementById("sflf-stage-wiz-new-db");
          if (newDbInp) newDbInp.style.display = dbSel.value === "__CREATE_NEW_DB__" ? "block" : "none";
          syncCreateStageModalWithFlowContext(sfState);
        };
      }
      const schSel = document.getElementById("sflf-stage-wiz-schema");
      if (schSel) {
        schSel.onchange = function () {
          const newSchInp = document.getElementById("sflf-stage-wiz-new-schema");
          if (newSchInp) newSchInp.style.display = schSel.value === "__CREATE_NEW_SCHEMA__" ? "block" : "none";
          const previewEl = document.getElementById("sflf-stage-wiz-ddl-preview");
          if (previewEl) previewEl.textContent = buildStageOrVolumeWizardDdlPreview();
        };
      }

      Array.from(modal.querySelectorAll("input, select")).forEach(function (inp) {
        inp.addEventListener("input", function () {
          const previewEl = document.getElementById("sflf-stage-wiz-ddl-preview");
          if (previewEl) previewEl.textContent = buildStageOrVolumeWizardDdlPreview();
        });
        inp.addEventListener("change", function () {
          const previewEl = document.getElementById("sflf-stage-wiz-ddl-preview");
          if (previewEl) previewEl.textContent = buildStageOrVolumeWizardDdlPreview();
        });
      });

      const closeModal = function () {
        modal.style.display = "none";
      };
      const closeBtn = document.getElementById("sflf-stage-wiz-close-btn");
      if (closeBtn) closeBtn.onclick = closeModal;
      const cancelBtn = document.getElementById("sflf-stage-wiz-cancel-btn");
      if (cancelBtn) cancelBtn.onclick = closeModal;

      const submitBtn = document.getElementById("sflf-stage-wiz-submit-btn");
      if (submitBtn) {
        submitBtn.onclick = async function () {
          await submitCreateStageOrVolumeWizard();
        };
      }
    }

    const kindSel = document.getElementById("sflf-stage-wiz-kind");
    if (kindSel && initialKind) {
      kindSel.value = initialKind;
      kindSel.dispatchEvent(new Event("change"));
    }
    syncCreateStageModalWithFlowContext(sfState);
    modal.style.display = "flex";
  }
  window.openCreateSnowflakeStageModal = openCreateSnowflakeStageModal;

  async function submitCreateStageOrVolumeWizard() {
    const kind = ((document.getElementById("sflf-stage-wiz-kind") || {}).value || "INTERNAL").trim();
    const createMode = ((document.getElementById("sflf-stage-wiz-create-mode") || {}).value || "IF_NOT_EXISTS").trim();
    const comment = ((document.getElementById("sflf-stage-wiz-comment") || {}).value || "").trim();
    const flowId = extractCurrentFlowId();

    if (kind === "EXTERNAL_VOLUME") {
      const volName = ((document.getElementById("sflf-stage-wiz-vol-name") || {}).value || "ICEBERG_EXT_VOL").trim();
      const allowWrites = ((document.getElementById("sflf-stage-wiz-vol-allow-writes") || {}).value || "TRUE") === "TRUE";
      const locName = ((document.getElementById("sflf-stage-wiz-vol-loc-name") || {}).value || "primary_s3_loc").trim();
      const provider = ((document.getElementById("sflf-stage-wiz-vol-provider") || {}).value || "S3").trim();
      const baseUrl = ((document.getElementById("sflf-stage-wiz-vol-base-url") || {}).value || "s3://my-iceberg-bucket/warehouse/").trim();
      const roleArn = ((document.getElementById("sflf-stage-wiz-vol-aws-role-arn") || {}).value || "").trim();
      const extId = ((document.getElementById("sflf-stage-wiz-vol-aws-external-id") || {}).value || "").trim();
      const apArn = ((document.getElementById("sflf-stage-wiz-vol-aws-access-point-arn") || {}).value || "").trim();
      const tenantId = ((document.getElementById("sflf-stage-wiz-vol-azure-tenant-id") || {}).value || "").trim();
      const storageEndpoint = ((document.getElementById("sflf-stage-wiz-vol-storage-endpoint") || {}).value || "").trim();
      const encType = ((document.getElementById("sflf-stage-wiz-vol-encryption") || {}).value || "AWS_SSE_S3").trim();
      const kmsKey = ((document.getElementById("sflf-stage-wiz-vol-kms-key") || {}).value || "").trim();
      const usePrivatelink = ((document.getElementById("sflf-stage-wiz-vol-privatelink") || {}).value || "FALSE") === "TRUE";

      const resp = await fetch("/api/v1/snowflake/external-volumes/create", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: volName,
          volume_name: volName,
          create_mode: createMode,
          allow_writes: allowWrites,
          location_name: locName,
          storage_provider: provider,
          storage_base_url: baseUrl,
          storage_aws_role_arn: roleArn || undefined,
          storage_aws_external_id: extId || undefined,
          storage_aws_access_point_arn: apArn || undefined,
          azure_tenant_id: tenantId || undefined,
          storage_endpoint: storageEndpoint || undefined,
          encryption_type: encType,
          kms_key_id: kmsKey || undefined,
          use_privatelink_endpoint: usePrivatelink,
          comment: comment || undefined,
          flow_id: flowId,
        }),
      });
      const data = await resp.json();
      if (!resp.ok || data.ok === false) {
        const errMsg =
          data.message ||
          `Active role ${sfState.active_role || "PUBLIC"} does not have CREATE EXTERNAL VOLUME privilege`;
        const banner = document.getElementById("sflf-extvol-privilege-banner");
        if (banner) {
          banner.style.display = "block";
          banner.textContent = errMsg;
        }
        showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", errMsg);
        return;
      }

      _stagesVolumesState.external_volumes.push({
        name: volName,
        volume_name: volName,
        allow_writes: allowWrites,
        storage_provider: provider,
        storage_base_url: baseUrl,
        encryption: encType,
        comment: comment,
      });
      _selectedStageOrVolumeKey = `VOL:${volName}`;
      showDenialOrStatusBanner("ok", "EXTERNAL_VOLUME_CREATED", `Created External Volume ${volName}`);
      const modal = document.getElementById("sflf-create-stage-modal");
      if (modal) modal.style.display = "none";
      await fetchSnowflakeContext(flowId);
      await fetchSnowflakeStagesAndVolumes(flowId);
      return;
    }

    const dbEl = document.getElementById("sflf-stage-wiz-db");
    const newDbEl = document.getElementById("sflf-stage-wiz-new-db");
    const schEl = document.getElementById("sflf-stage-wiz-schema");
    const newSchEl = document.getElementById("sflf-stage-wiz-new-schema");
    const db =
      dbEl && dbEl.value === "__CREATE_NEW_DB__"
        ? (newDbEl && newDbEl.value.trim()) || "NEW_DB"
        : (dbEl && dbEl.value) || sfState.database || "SNOWFLAKE_LANGFLOW";
    const sch =
      schEl && schEl.value === "__CREATE_NEW_SCHEMA__"
        ? (newSchEl && newSchEl.value.trim()) || "NEW_SCHEMA"
        : (schEl && schEl.value) || sfState.schema || "CORE";
    const stageName = ((document.getElementById("sflf-stage-wiz-name") || {}).value || "DOCS_STAGE").trim();
    const dirEnable = ((document.getElementById("sflf-stage-wiz-dir-enable") || {}).value || "TRUE") === "TRUE";
    const dirAutoRefresh = ((document.getElementById("sflf-stage-wiz-dir-autorefresh") || {}).value || "FALSE") === "TRUE";
    const notifInt = ((document.getElementById("sflf-stage-wiz-notif-int") || {}).value || "").trim();
    const fileFmt = ((document.getElementById("sflf-stage-wiz-file-format") || {}).value || "NONE").trim();
    const intEnc = ((document.getElementById("sflf-stage-wiz-internal-encryption") || {}).value || "SNOWFLAKE_SSE").trim();
    const extUrl = ((document.getElementById("sflf-stage-wiz-ext-url") || {}).value || "").trim();
    const cloudProvider = ((document.getElementById("sflf-stage-wiz-cloud-provider") || {}).value || "S3").trim();
    const authMode = ((document.getElementById("sflf-stage-wiz-auth-mode") || {}).value || "STORAGE_INTEGRATION").trim();
    const storageInt = ((document.getElementById("sflf-stage-wiz-storage-integration") || {}).value || "S3_DOCS_INT").trim();
    const credsRaw = ((document.getElementById("sflf-stage-wiz-credentials") || {}).value || "").trim();
    const extEnc = ((document.getElementById("sflf-stage-wiz-ext-encryption") || {}).value || "AWS_SSE_S3").trim();
    const kmsKey = ((document.getElementById("sflf-stage-wiz-ext-kms-key") || {}).value || "").trim();
    const s3CompatEndpoint = ((document.getElementById("sflf-stage-wiz-s3compat-endpoint") || {}).value || "").trim();
    const usePrivatelink = ((document.getElementById("sflf-stage-wiz-ext-privatelink") || {}).value || "FALSE") === "TRUE";

    const resp = await fetch("/api/v1/snowflake/stages/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        database: db,
        schema: sch,
        name: stageName,
        stage_name: stageName,
        stage_type: kind,
        create_mode: createMode,
        encryption: kind === "INTERNAL" ? intEnc : extEnc,
        kms_key_id: kmsKey || undefined,
        directory_enable: dirEnable,
        auto_refresh: dirAutoRefresh,
        notification_integration: notifInt || undefined,
        file_format: fileFmt !== "NONE" ? fileFmt : undefined,
        cloud_provider: kind === "EXTERNAL" ? cloudProvider : undefined,
        url: kind === "EXTERNAL" ? extUrl : undefined,
        auth_mode: kind === "EXTERNAL" ? authMode : undefined,
        storage_integration: kind === "EXTERNAL" && authMode === "STORAGE_INTEGRATION" ? storageInt : undefined,
        credentials: kind === "EXTERNAL" && authMode === "CREDENTIALS" ? credsRaw : undefined,
        endpoint: kind === "EXTERNAL" && s3CompatEndpoint ? s3CompatEndpoint : undefined,
        use_privatelink_endpoint: kind === "EXTERNAL" ? usePrivatelink : undefined,
        comment: comment || undefined,
        flow_id: flowId,
      }),
    });
    const data = await resp.json();
    if (!resp.ok || data.ok === false) {
      showDenialOrStatusBanner("error", data.error_code || "AUTHZ_ROLE_DENIED", data.message || "Stage creation denied");
      return;
    }

    const newFqn = `@${db.toUpperCase()}.${sch.toUpperCase()}.${stageName}`;
    _stagesVolumesState.stages.push({
      name: stageName,
      stage_name: stageName,
      database: db.toUpperCase(),
      schema: sch.toUpperCase(),
      stage_fqn: newFqn,
      stage_type: kind,
      encryption: kind === "INTERNAL" ? intEnc : extEnc,
      directory_enabled: dirEnable,
      auto_refresh: dirAutoRefresh,
      url: kind === "EXTERNAL" ? extUrl : "",
      storage_integration: kind === "EXTERNAL" ? storageInt : "",
      file_format: fileFmt,
      comment: comment,
    });
    _selectedStageOrVolumeKey = newFqn;
    showDenialOrStatusBanner("ok", "STAGE_CREATED", `Created ${kind} Stage ${newFqn} and added to [ ❄️ Context ]`);
    const modal = document.getElementById("sflf-create-stage-modal");
    if (modal) modal.style.display = "none";
    await fetchSnowflakeContext(flowId);
    await fetchSnowflakeStagesAndVolumes(flowId);
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
