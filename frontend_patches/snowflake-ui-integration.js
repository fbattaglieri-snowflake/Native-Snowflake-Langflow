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

  // W2-R2-13: 3 Agent Memory Types for the Langflow `Memories` (+ Snowflake) wizard & Hub Section 2
  const AGENT_MEMORY_TYPES = [
    {
      id: "episodic_session",
      label: "+ Episodic / Session Memory",
      default_category: "structured",
      default_backend: "Hybrid Table",
      description:
        "Conversational turns, session state, and short-term episodic recall backed by Snowflake Hybrid Table or Snowflake Postgres (Relational / JSONB).",
    },
    {
      id: "semantic_vector",
      label: "+ Semantic / Vector Memory",
      default_category: "vector_table",
      default_backend: "Standard Table (VECTOR)",
      description:
        "Dense semantic embeddings and similarity search backed by Snowflake VECTOR(FLOAT|INT, 768|1024) coupled to AI_EMBED, Snowflake Postgres pgvector, or Cortex Search.",
    },
    {
      id: "structured_long_term",
      label: "+ Structured / Semi-Structured Long-Term Memory",
      default_category: "semi_structured",
      default_backend: "Standard Table (VARIANT)",
      description:
        "Persistent agent facts, user preferences, and entity profiles backed by Standard / Dynamic / Interactive Tables with VARIANT/OBJECT/ARRAY or consolidation Views.",
    },
  ];

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
      { value: "Standard Table", table_type: "STANDARD", is_vector: false },
      { value: "Hybrid Table", table_type: "HYBRID", is_vector: false },
      { value: "Interactive Table", table_type: "INTERACTIVE", is_vector: false },
      { value: "Dynamic Table", table_type: "DYNAMIC", is_vector: false },
    ],
    semi_structured: [
      { value: "Standard Table (VARIANT)", table_type: "STANDARD", semi_type: "VARIANT", is_vector: false },
      { value: "Standard Table (OBJECT)", table_type: "STANDARD", semi_type: "OBJECT", is_vector: false },
      { value: "Standard Table (ARRAY)", table_type: "STANDARD", semi_type: "ARRAY", is_vector: false },
      { value: "Hybrid Table (VARIANT)", table_type: "HYBRID", semi_type: "VARIANT", is_vector: false },
      { value: "Interactive Table (VARIANT)", table_type: "INTERACTIVE", semi_type: "VARIANT", is_vector: false },
      { value: "Dynamic Table (VARIANT)", table_type: "DYNAMIC", semi_type: "VARIANT", is_vector: false },
    ],
    postgres_non_vector: [
      { value: "Snowflake Postgres (Relational / JSONB)", table_type: "POSTGRES_JSONB", is_vector: false },
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
          postgres_instances: [],
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
          postgres_instances: [],
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
    },
  };

  window.SNOWFLAKE_LANGFLOW_CONTEXT = sfState;

  // W2-R2-14: Inject official Snowflake Blue (#29B5E8) CSS rules in document.head (outside #root)
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
      [data-testid="welcome-to-snowflake-langflow-title"] {
        color: #29B5E8 !important;
      }
      [data-testid="snowflake-langflow-dual-loading-logo"] svg {
        fill: #29B5E8 !important;
      }
    `;
    document.head.appendChild(styleEl);
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

  // UI-W2-01 & W2-R2-14: Clean Snowflake Blue (#29B5E8) loading screen overlay mounted on document.body (outside #root)
  // Strictly shows ONLY the official Snowflake Blue (#29B5E8) logo without any 3-layer Databricks-like icon.
  function ensureDualSymbolLoadingOverlay() {
    const rootEl = document.getElementById("root");
    const hasHeader = !!document.querySelector('[data-testid="header_left_section_wrapper"]');
    const rootText = rootEl ? (rootEl.textContent || "").trim() : "";
    const isLoadingState = !hasHeader && (rootText === "" || rootText === "Loading..." || rootText.includes("Loading"));
    let overlay = document.getElementById("sflf-dual-loading-overlay");
    if (isLoadingState) {
      if (!overlay && document.body) {
        overlay = document.createElement("div");
        overlay.id = "sflf-dual-loading-overlay";
        overlay.setAttribute("data-testid", "snowflake_langflow_loading_dual_logo");
        overlay.style.cssText =
          "position:fixed;top:calc(50% - 68px);left:50%;transform:translate(-50%,-50%);z-index:99990;display:inline-flex;align-items:center;justify-content:center;gap:10px;pointer-events:none;user-select:none;color:#29B5E8;";
        overlay.innerHTML = `${buildOfficialSnowflakeSvg("#29B5E8", 40)}`;
        document.body.appendChild(overlay);
      } else if (overlay) {
        overlay.style.display = "inline-flex";
      }
    } else if (overlay) {
      overlay.style.display = "none";
    }
  }

  // W2-R2-13 & W2-R2-14: Companion `+ Snowflake` button in official #29B5E8 on the Langflow Memories view
  function ensureMemoriesPageSnowflakeButton() {
    if (!document.body) return;
    const path = (window.location && window.location.pathname) || "";
    const pageText = document.body.textContent || "";
    const hasHeadingMemories = Array.from(document.querySelectorAll("h1, h2, h3, [data-testid='mainpage_title']")).some(
      function (el) {
        const t = (el.textContent || "").trim();
        return t === "Memories" || t === "Manage Memories";
      }
    );
    const isMemoriesView =
      path.toLowerCase().includes("memories") ||
      path.toLowerCase().includes("memory") ||
      hasHeadingMemories ||
      !!document.querySelector('[data-testid*="memories"], [data-testid*="memory-"]') ||
      (pageText.includes("Memories") &&
        (pageText.includes("Embedding Model") ||
          pageText.includes("No memories") ||
          pageText.includes("Create a Memory") ||
          pageText.includes("Vector Store")));
    let btn = document.getElementById("sflf-memories-plus-snowflake-btn");
    if (!btn) {
      btn = document.createElement("button");
      btn.id = "sflf-memories-plus-snowflake-btn";
      btn.type = "button";
      btn.setAttribute("data-testid", "memories-plus-snowflake-btn");
      btn.style.cssText =
        "position:fixed;bottom:22px;right:22px;z-index:9990;display:none;align-items:center;gap:6px;background:#29B5E8;color:#ffffff;border:1px solid #29B5E8;border-radius:8px;padding:7px 14px;font-family:Inter,sans-serif;font-size:12px;font-weight:600;cursor:pointer;box-shadow:0 6px 18px rgba(41,181,232,0.38);";
      btn.innerHTML = `${buildOfficialSnowflakeSvg("#ffffff", 14)}<span>+ Snowflake</span>`;
      btn.onclick = function () {
        openSnowflakeHubModal("memories");
      };
      document.body.appendChild(btn);
    }
    if (isMemoriesView) {
      // Position next to / right under the native `+ Create` button when visible on the Memories page
      const createBtnCandidate = Array.from(document.querySelectorAll("button")).find(function (b) {
        if (b.id === "sflf-memories-plus-snowflake-btn") return false;
        const txt = (b.textContent || "").trim();
        return txt === "Create" || txt === "+ Create" || txt === "Add Memory" || txt === "Create Memory";
      });
      if (createBtnCandidate) {
        const r = createBtnCandidate.getBoundingClientRect();
        if (r && r.width > 0 && r.height > 0) {
          btn.style.top = Math.round(r.bottom + 8) + "px";
          btn.style.left = Math.round(r.left) + "px";
          btn.style.bottom = "auto";
          btn.style.right = "auto";
        }
      }
      btn.style.display = "inline-flex";
    } else {
      btn.style.display = "none";
    }
  }

  // W2-R2-00, W2-R2-04A, W2-R2-14: Live Canvas Node Auto-Sync when Snowflake context or active role changes
  function syncCanvasNodesWithSnowflakeContext(ctx) {
    const activeCtx = ctx || sfState;
    if (!activeCtx || typeof document === "undefined") return { syncedNodes: 0 };

    const selCtx = activeCtx.selected_context || {};
    const dbs =
      Array.isArray(selCtx.databases) && selCtx.databases.length > 0
        ? selCtx.databases
        : [activeCtx.database || "SNOWFLAKE_LANGFLOW"];
    const primaryDb = activeCtx.database || dbs[0] || "SNOWFLAKE_LANGFLOW";
    const schemasMap = selCtx.schemas_by_db || (activeCtx.catalog && activeCtx.catalog.schemas_by_db) || {};
    const schemasForDb =
      Array.isArray(schemasMap[primaryDb]) && schemasMap[primaryDb].length > 0
        ? schemasMap[primaryDb]
        : [activeCtx.schema || "CORE"];
    const primarySchema = activeCtx.schema || schemasForDb[0] || "CORE";
    const vis = selCtx.visible_objects || activeCtx.catalog || {};

    const nodeEls = document.querySelectorAll('[data-testid^="rf__node-"], .react-flow__node');
    let syncedCount = 0;

    nodeEls.forEach(function (nodeEl) {
      nodeEl.setAttribute("data-snowflake-context-epoch", String(activeCtx.context_epoch || 1));
      nodeEl.setAttribute("data-snowflake-active-role", String(activeCtx.active_role || "ACCOUNTADMIN"));
      nodeEl.setAttribute("data-snowflake-database", primaryDb);
      nodeEl.setAttribute("data-snowflake-schema", primarySchema);

      // Update any native <select> or <input> fields inside the canvas node for database / schema / objects
      const selects = nodeEl.querySelectorAll("select");
      selects.forEach(function (sel) {
        const nameAttr = (sel.getAttribute("name") || sel.getAttribute("data-testid") || sel.id || "").toLowerCase();
        if (nameAttr.includes("database")) {
          populateSelectOptions(sel, dbs, primaryDb);
          sel.dispatchEvent(new Event("change", { bubbles: true }));
        } else if (nameAttr.includes("schema")) {
          populateSelectOptions(sel, schemasForDb, primarySchema);
          sel.dispatchEvent(new Event("change", { bubbles: true }));
        } else if (nameAttr.includes("table_name") || nameAttr.includes("table")) {
          populateSelectOptions(sel, vis.tables || [], (vis.tables && vis.tables[0]) || "");
        } else if (nameAttr.includes("view_name") || nameAttr.includes("view")) {
          populateSelectOptions(sel, vis.views || [], (vis.views && vis.views[0]) || "");
        } else if (nameAttr.includes("stage")) {
          populateSelectOptions(sel, vis.stages || [], (vis.stages && vis.stages[0]) || "");
        } else if (nameAttr.includes("search_service")) {
          populateSelectOptions(
            sel,
            vis.cortex_search_services || [],
            (vis.cortex_search_services && vis.cortex_search_services[0]) || ""
          );
        } else if (nameAttr.includes("semantic_view")) {
          populateSelectOptions(sel, vis.semantic_views || [], (vis.semantic_views && vis.semantic_views[0]) || "");
        } else if (nameAttr.includes("cortex_agent")) {
          populateSelectOptions(sel, vis.cortex_agents || [], (vis.cortex_agents && vis.cortex_agents[0]) || "");
        } else if (nameAttr.includes("procedure")) {
          populateSelectOptions(sel, vis.procedures || [], (vis.procedures && vis.procedures[0]) || "");
        } else if (nameAttr.includes("function")) {
          populateSelectOptions(sel, vis.functions || [], (vis.functions && vis.functions[0]) || "");
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
      database: primaryDb,
      schema: primarySchema,
      databases: dbs,
      schemas_by_db: schemasMap,
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
    ensureDualSymbolLoadingOverlay();
    ensureMemoriesPageSnowflakeButton();
    alignTopbarControls();
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

  async function fetchSnowflakeContext() {
    try {
      const resp = await fetch("/api/v1/snowflake/context", {
        headers: { Accept: "application/json" },
      });
      if (!resp.ok) return;
      const data = await resp.json();
      sfState = Object.assign(sfState, data);
      window.SNOWFLAKE_LANGFLOW_CONTEXT = sfState;
      updateTopbarWidgets();
      await fetchSnowflakeCatalog(
        sfState.database,
        sfState.schema,
        sfState.selected_context && sfState.selected_context.databases,
        sfState.selected_context && sfState.selected_context.schemas_by_db
      );
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

  async function persistSnowflakeContextSelection(payload) {
    try {
      const resp = await fetch("/api/v1/snowflake/context", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
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
      sfState.database = payload.database || sfState.database;
      sfState.schema = payload.schema || sfState.schema;
      sfState.selected_context = payload.selected_context || sfState.selected_context;
      window.SNOWFLAKE_LANGFLOW_CONTEXT = sfState;
      updateTopbarWidgets();
      refreshHubModalPrivilegeAndContextViews();
      syncCanvasNodesWithSnowflakeContext(sfState);
      window.dispatchEvent(new CustomEvent("snowflake:context-updated", { detail: sfState }));
      window.dispatchEvent(new CustomEvent("snowflake:context-changed", { detail: sfState }));
      showDenialOrStatusBanner(
        "ok",
        "CONTEXT_UPDATED",
        `Contesto attivo salvato: ${sfState.database}.${sfState.schema} (Role: ${sfState.active_role})`
      );
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

  // Mount Role Selector & Context indicator directly on document.body (OUTSIDE #root)
  // W2-R2-14: Colored in official Snowflake Blue (#29B5E8)
  function mountFixedTopbarControls() {
    if (document.getElementById("sflf-body-topbar-controls")) return;
    const ctrl = document.createElement("div");
    ctrl.id = "sflf-body-topbar-controls";
    ctrl.style.cssText =
      "position:fixed;top:10px;right:228px;z-index:25;display:inline-flex;align-items:center;gap:6px;background:transparent;color:#29B5E8;padding:1px 4px;font-family:Inter,sans-serif;font-size:11px;height:28px;";
    ctrl.innerHTML = `
      <span style="display:inline-flex;align-items:center;color:#29B5E8;">${buildOfficialSnowflakeSvg("#29B5E8", 14)}</span>
      <label style="font-weight:600;color:#29B5E8;font-size:11px;">Role:</label>
      <select id="sflf-role-select" style="background:#ffffff;color:#09090b;border:1px solid rgba(41,181,232,0.55);border-radius:5px;padding:1px 6px;font-size:11px;font-weight:600;cursor:pointer;max-width:142px;height:24px;text-overflow:ellipsis;"></select>
      <span id="sflf-topbar-context-pill" title="Active Database.Schema Context (Click to configure in Snowflake Hub)" style="background:rgba(41,181,232,0.10);color:#29B5E8;border:1px solid rgba(41,181,232,0.55);border-radius:5px;padding:2px 6px;font-size:10px;font-weight:600;cursor:pointer;max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${sfState.database}.${sfState.schema}</span>
      <button id="sflf-open-hub-btn" style="background:#29B5E8;color:#ffffff;border:none;border-radius:5px;padding:2px 8px;height:24px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;">Snowflake Hub</button>
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
    const pill = document.getElementById("sflf-topbar-context-pill");
    if (pill) {
      pill.onclick = function () {
        openSnowflakeHubModal();
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
    const pillEl = document.getElementById("sflf-topbar-context-pill");
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
      const selDbs =
        (sfState.selected_context &&
          Array.isArray(sfState.selected_context.databases) &&
          sfState.selected_context.databases) || [sfState.database || "SNOWFLAKE_LANGFLOW"];
      const primaryPair = `${sfState.database || selDbs[0] || "SNOWFLAKE_LANGFLOW"}.${sfState.schema || "CORE"}`;
      pillEl.textContent = selDbs.length > 1 ? `${primaryPair} (+${selDbs.length - 1} DB)` : primaryPair;
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

  function renderMultiSelectConeInHubModal(databases, schemasMap, activeDb, activeSchema, cat) {
    const selCtx = sfState.selected_context || {};
    const selectedDbs =
      Array.isArray(selCtx.databases) && selCtx.databases.length > 0 ? selCtx.databases : [activeDb];
    const selectedSchemasMap = selCtx.schemas_by_db || {};

    // Step 1: Multi-select Databases
    const dbsBox = document.getElementById("sflf-ctx-multi-dbs");
    if (dbsBox) {
      dbsBox.innerHTML = databases
        .map(function (db) {
          const isChecked = selectedDbs.indexOf(db) !== -1;
          return `<label style="display:inline-flex;align-items:center;gap:4px;padding:2px 6px;border-radius:4px;background:#27272a;border:1px solid ${
            isChecked ? "#29B5E8" : "#3f3f46"
          };cursor:pointer;"><input type="checkbox" class="sflf-ctx-db-cb" value="${db}" ${
            isChecked ? "checked" : ""
          } /> <span>${db}</span></label>`;
        })
        .join(" ");
    }

    // Step 2: Cascading Multi-Select Schemas per Database
    const schemasBox = document.getElementById("sflf-ctx-multi-schemas");
    if (schemasBox) {
      const groupsHtml = selectedDbs
        .map(function (db) {
          const dbSchemas =
            Array.isArray(schemasMap[db]) && schemasMap[db].length > 0
              ? schemasMap[db]
              : db === activeDb && Array.isArray(cat.schemas) && cat.schemas.length > 0
                ? cat.schemas
                : ["CORE", "PUBLIC"];
          const chosenForDb =
            Array.isArray(selectedSchemasMap[db]) && selectedSchemasMap[db].length > 0
              ? selectedSchemasMap[db]
              : [db === activeDb ? activeSchema : dbSchemas[0]];
          const chips = dbSchemas
            .map(function (sch) {
              const checked = chosenForDb.indexOf(sch) !== -1;
              return `<label style="display:inline-flex;align-items:center;gap:3px;padding:1px 5px;border-radius:4px;background:#18181b;border:1px solid ${
                checked ? "#29B5E8" : "#3f3f46"
              };"><input type="checkbox" class="sflf-ctx-schema-cb" data-db="${db}" value="${db}.${sch}" ${
                checked ? "checked" : ""
              } /> <span>${db}.${sch}</span></label>`;
            })
            .join(" ");
          return `<div style="margin-bottom:4px;"><strong style="color:#29B5E8;font-size:10px;">${db}:</strong> ${chips}</div>`;
        })
        .join("");
      schemasBox.innerHTML = groupsHtml || `<span style="color:#71717a;">Select at least one Database in Step 1.</span>`;
    }

    // Step 3: Cascading Objects per <DB>.<SCHEMA> (including functions & procedures chips)
    const objChipsBox = document.getElementById("sflf-ctx-multi-objects");
    if (objChipsBox) {
      const objByPair = cat.objects_by_db_schema || selCtx.objects_by_db_schema || {};
      const pairKeys = Object.keys(objByPair);
      const effectivePairs = pairKeys.length > 0 ? pairKeys : [`${activeDb}.${activeSchema}`];
      const categoriesOrder = [
        { key: "tables", label: "Tables" },
        { key: "views", label: "Views" },
        { key: "iceberg_tables", label: "Iceberg Tables" },
        { key: "stages", label: "Stages" },
        { key: "cortex_search_services", label: "Cortex Search" },
        { key: "semantic_views", label: "Semantic Views" },
        { key: "cortex_agents", label: "Cortex Agents" },
        { key: "functions", label: "Functions (UDF/UDTF)" },
        { key: "procedures", label: "Procedures" },
        { key: "postgres_instances", label: "Postgres" },
      ];
      objChipsBox.innerHTML = effectivePairs
        .map(function (pairKey) {
          const bucket = objByPair[pairKey] || cat;
          const catBadges = categoriesOrder
            .map(function (c) {
              const items = Array.isArray(bucket[c.key]) ? bucket[c.key] : Array.isArray(cat[c.key]) ? cat[c.key] : [];
              return `<label style="display:inline-flex;align-items:center;gap:3px;padding:1px 5px;border-radius:4px;background:#27272a;border:1px solid #3f3f46;"><input type="checkbox" class="sflf-ctx-obj-chip" data-pair="${pairKey}" data-category="${c.key}" value="${pairKey}:${c.key}" checked /> <span>${c.label} (${items.length})</span></label>`;
            })
            .join(" ");
          return `<div style="margin-bottom:4px;"><strong style="color:#29B5E8;font-size:10px;">${pairKey}:</strong> ${catBadges}</div>`;
        })
        .join("");
    }
  }

  function refreshHubModalPrivilegeAndContextViews() {
    const modal = document.getElementById("sflf-hub-modal");
    if (!modal) return;

    const roleSpan = document.getElementById("sflf-modal-role");
    if (roleSpan) roleSpan.textContent = sfState.active_role;
    const epochSpan = document.getElementById("sflf-epoch-badge");
    if (epochSpan) epochSpan.textContent = `epoch #${sfState.context_epoch}`;

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

    // Section 0 selectors
    const ctxDbSel = document.getElementById("sflf-ctx-db");
    const ctxSchSel = document.getElementById("sflf-ctx-schema");
    if (ctxDbSel && !ctxDbSel.dataset.userEditing) {
      populateSelectOptions(ctxDbSel, databases, activeDb);
    }
    if (ctxSchSel && !ctxSchSel.dataset.userEditing) {
      populateSelectOptions(ctxSchSel, schemas, activeSchema);
    }

    renderMultiSelectConeInHubModal(databases, schemasMap, activeDb, activeSchema, cat);

    // Section 0 object multi-select summary badges
    const objSummaryEl = document.getElementById("sflf-ctx-objects-summary");
    if (objSummaryEl) {
      const countOf = function (arr) {
        return Array.isArray(arr) ? arr.length : 0;
      };
      objSummaryEl.innerHTML =
        `<span>Tables: <strong>${countOf(cat.tables)}</strong></span> · ` +
        `<span>Views: <strong>${countOf(cat.views)}</strong></span> · ` +
        `<span>Iceberg: <strong>${countOf(cat.iceberg_tables)}</strong></span> · ` +
        `<span>Stages: <strong>${countOf(cat.stages)}</strong></span> · ` +
        `<span>Memories: <strong>${countOf(cat.memories || sfState.memories)}</strong></span> · ` +
        `<span>Cortex Search: <strong>${countOf(cat.cortex_search_services)}</strong></span> · ` +
        `<span>Analyst Views/YAML: <strong>${countOf(cat.semantic_views) + countOf(cat.semantic_models)}</strong></span> · ` +
        `<span>Cortex Agents: <strong>${countOf(cat.cortex_agents)}</strong></span> · ` +
        `<span>Functions (UDF/UDTF): <strong>${countOf(cat.functions)}</strong></span> · ` +
        `<span>Procedures: <strong>${countOf(cat.procedures)}</strong></span> · ` +
        `<span>Postgres: <strong>${countOf(cat.postgres_instances)}</strong></span>`;
    }

    // Section 1 hierarchical selectors: 1. Database -> 2. Schema -> 3. Object
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
      const chosenType =
        AGENT_MEMORY_TYPES.find(function (t) {
          return t.id === agentTypeSel.value;
        }) || AGENT_MEMORY_TYPES[1];
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
      fetchSnowflakeContext();
      refreshTracesInModal();
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
    modal.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,0.68);z-index:99998;display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box;font-family:Inter,sans-serif;";
    const codeBoxStyle =
      "margin-top:8px;background-color:#09090b !important;color:#e4e4e7 !important;border:1px solid #3f3f46;padding:8px 10px;border-radius:6px;font-family:'JetBrains Mono',monospace;font-size:11px;line-height:1.45;white-space:pre-wrap;word-break:break-word;max-height:125px;overflow-y:auto;overflow-x:hidden;box-sizing:border-box;";
    const selectStyle =
      "background:#18181b;color:#fff;border:1px solid #3f3f46;padding:5px 8px;border-radius:6px;width:100%;min-width:0;box-sizing:border-box;font-size:11px;";
    const inputStyle =
      "background:#18181b;color:#fff;border:1px solid #3f3f46;padding:5px 8px;border-radius:6px;width:100%;min-width:0;box-sizing:border-box;font-size:11px;";

    const embedModelOptionsHtml = OFFICIAL_AI_EMBED_MODELS.map(function (m) {
      return `<option value="${m.model_id}">${m.label}</option>`;
    }).join("");

    modal.innerHTML = `
      <div style="background:#18181b;color:#f4f4f5;border:1px solid #3f3f46;border-radius:12px;width:min(940px,95vw);max-height:88vh;overflow-y:auto;overflow-x:hidden;padding:18px 20px;box-shadow:0 20px 50px rgba(0,0,0,0.6);font-size:12px;box-sizing:border-box;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;border-bottom:1px solid #27272a;padding-bottom:12px;margin-bottom:14px;">
          <div style="display:flex;align-items:center;gap:8px;min-width:0;">
            <span style="display:inline-flex;flex-shrink:0;">${buildOfficialSnowflakeSvg("#29B5E8", 20)}</span>
            <strong style="font-size:14px;color:#29B5E8;white-space:nowrap;">Snowflake Langflow Hub</strong>
            <span style="background:#27272a;color:#29B5E8;border:1px solid #29B5E8;border-radius:5px;padding:2px 8px;font-size:11px;font-weight:600;white-space:nowrap;">Role: <span id="sflf-modal-role">${sfState.active_role}</span> · <span id="sflf-epoch-badge">epoch #${sfState.context_epoch}</span></span>
          </div>
          <div style="display:flex;align-items:center;gap:12px;flex-shrink:0;white-space:nowrap;">
            <a href="${FORK_GITHUB_URL}" target="_blank" rel="noopener noreferrer" style="color:#a1a1aa;text-decoration:none;font-size:11px;font-weight:500;">Fork GitHub</a>
            <a href="${SNOWFLAKE_DOCS_URL}" target="_blank" rel="noopener noreferrer" style="display:inline-flex;align-items:center;gap:4px;color:#29B5E8;text-decoration:none;font-size:11px;font-weight:600;">
              ${SNOWFLAKE_SVG_COLOR} Docs
            </a>
            <button id="sflf-modal-close" style="background:#27272a;border:1px solid #3f3f46;color:#fff;padding:4px 10px;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;">Close</button>
          </div>
        </div>

        <!-- Section 0: Primary Context & Cascading Visibility Selector (W2-R2-00 · HUB-W2-02) -->
        <div id="sflf-sec0" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;margin-bottom:14px;border:1px solid #29B5E8;">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:6px;">
            <h4 style="margin:0;font-size:13px;color:#29B5E8;">0. Multi-Database -&gt; Multi-Schema -&gt; Cascading Objects Cone of Visibility (W2-R2-00 · HUB-W2-02)</h4>
            <div style="display:flex;gap:6px;">
              <button id="sflf-btn-refresh-catalog" style="background:#3f3f46;color:#fff;border:none;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:500;">Refresh Catalog</button>
              <button id="sflf-btn-save-context" style="background:#29B5E8;color:#fff;border:none;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;">Apply &amp; Persist Context</button>
            </div>
          </div>
          <p style="margin:0 0 10px 0;font-size:11px;color:#a1a1aa;">Seleziona a cascata <strong>1. Multi-Database -&gt; 2. Multi-Schema -&gt; 3. Oggetti visibili</strong> per <code>CURRENT_ROLE()</code>. Tutti i nodi Snowflake nel canvas si aggiornano automaticamente al salvataggio (inclusi <strong>Functions UDF/UDTF</strong> e <strong>Stored Procedures</strong>).</p>
          <div style="display:grid;grid-template-columns:1fr 1fr 1.4fr;gap:8px;margin-bottom:8px;">
            <div>
              <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">1. Databases (Multi-Select + Primary)</label>
              <select id="sflf-ctx-db" style="${selectStyle};margin-bottom:4px;">
                <option value="${sfState.database}">${sfState.database}</option>
              </select>
              <div id="sflf-ctx-multi-dbs" style="display:flex;flex-wrap:wrap;gap:4px;background:#18181b;border:1px solid #3f3f46;padding:5px;border-radius:6px;font-size:10px;max-height:70px;overflow-y:auto;"></div>
            </div>
            <div>
              <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">2. Schemas per Database (Cascading)</label>
              <select id="sflf-ctx-schema" style="${selectStyle};margin-bottom:4px;">
                <option value="${sfState.schema}">${sfState.schema}</option>
              </select>
              <div id="sflf-ctx-multi-schemas" style="background:#18181b;border:1px solid #3f3f46;padding:5px;border-radius:6px;font-size:10px;max-height:70px;overflow-y:auto;"></div>
            </div>
            <div>
              <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">3. Object Visibility Scope per &lt;DB&gt;.&lt;SCHEMA&gt;</label>
              <div style="display:flex;flex-wrap:wrap;gap:6px;background:#18181b;border:1px solid #3f3f46;padding:5px 8px;border-radius:6px;font-size:10px;margin-bottom:4px;">
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="tables" checked /> Tables</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="views" checked /> Views</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="iceberg_tables" checked /> Iceberg Tables</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="stages" checked /> Stages</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="memories" checked /> Memories</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="cortex_search_services" checked /> Cortex Search</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="semantic_views" checked /> Cortex Analyst (Views/YAML)</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="cortex_agents" checked /> Cortex Agents</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="functions" checked /> Functions (UDF/UDTF)</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="procedures" checked /> Procedures</label>
                <label style="display:inline-flex;align-items:center;gap:3px;"><input type="checkbox" class="sflf-ctx-obj-filter" value="postgres_instances" checked /> Snowflake Postgres</label>
              </div>
              <div id="sflf-ctx-multi-objects" style="background:#18181b;border:1px solid #3f3f46;padding:5px;border-radius:6px;font-size:10px;max-height:70px;overflow-y:auto;"></div>
            </div>
          </div>
          <div id="sflf-ctx-objects-summary" style="font-size:10px;color:#a1a1aa;background:#18181b;padding:6px 10px;border-radius:6px;border:1px solid #3f3f46;"></div>
        </div>

        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:14px;margin-bottom:14px;">
          <!-- Section 1: Hierarchical Governed Object Provisioning (HUB-W2-03 & HUB-W2-04 & W2-R2-04B) -->
          <div style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;">
            <h4 style="margin:0 0 4px 0;font-size:13px;color:#29B5E8;">1. Governed Object Provisioning (REQ-16 · W2-R2-04B · HUB-W2-03 / HUB-W2-04)</h4>
            <p style="margin:0 0 8px 0;font-size:11px;color:#a1a1aa;">4 Famiglie Infrastrutturali (<strong>Table, View, Stage, Snowflake Postgres</strong>) con ordine gerarchico <strong>1. Database -&gt; 2. Schema -&gt; 3. Oggetto</strong>. <code>Memory</code> si crea nella sezione <code>Memories (+ Snowflake)</code>.</p>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">
              <div>
                <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">1. Database</label>
                <select id="sflf-prov-db" style="${selectStyle}">
                  <option value="${sfState.database}">${sfState.database}</option>
                  <option value="__CREATE_NEW_DB__">+ Create New Database...</option>
                </select>
                <input id="sflf-prov-new-db" placeholder="New Database Name (e.g. ANALYTICS_DB)" style="${inputStyle};margin-top:4px;display:none;" />
              </div>
              <div>
                <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">2. Schema (in Database)</label>
                <select id="sflf-prov-schema" style="${selectStyle}">
                  <option value="${sfState.schema}">${sfState.schema}</option>
                  <option value="__CREATE_NEW_SCHEMA__">+ Create New Schema...</option>
                </select>
                <input id="sflf-prov-new-schema" placeholder="New Schema Name (e.g. RAG_SCHEMA)" style="${inputStyle};margin-top:4px;display:none;" />
              </div>
            </div>
            <div style="display:grid;grid-template-columns:1.15fr 0.85fr;gap:8px;margin-bottom:8px;">
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

          <!-- Section 2: Memories (+ Snowflake) 3 Agent Memory Types & AI_EMBED Dimension Coupling (W2-R2-13 · HUB-W2-05) -->
          <div id="sflf-sec2" style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;border:1px solid rgba(41,181,232,0.45);">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:4px;">
              <h4 style="margin:0;font-size:13px;color:#29B5E8;">2. Memories (+ Snowflake) — 3 Agent Memory Types (W2-R2-13 · HUB-W2-05)</h4>
              <span style="background:#29B5E8;color:#ffffff;padding:2px 8px;border-radius:5px;font-size:10px;font-weight:600;">+ Snowflake</span>
            </div>
            <p style="margin:0 0 8px 0;font-size:11px;color:#a1a1aa;">Crea le 3 tipologie di <strong>Agent Memory Snowflake</strong>: <strong>1. Episodic / Session Memory</strong>, <strong>2. Semantic / Vector Memory</strong> (<code>VECTOR(FLOAT|INT, dim)</code> + <code>AI_EMBED</code> / <code>pgvector</code> / <code>Cortex Search</code>), <strong>3. Structured / Semi-Structured Long-Term Memory</strong>.</p>
            <div style="margin-bottom:8px;">
              <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Agent Memory Type (+ Snowflake)</label>
              <select id="sflf-mem-agent-type" style="${selectStyle}">
                <option value="semantic_vector">2. + Semantic / Vector Memory (VECTOR + AI_EMBED / pgvector / Cortex Search)</option>
                <option value="episodic_session">1. + Episodic / Session Memory (Hybrid Table / Postgres JSONB)</option>
                <option value="structured_long_term">3. + Structured / Semi-Structured Long-Term Memory (VARIANT / Dynamic / View)</option>
              </select>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">
              <div>
                <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Memory Category</label>
                <select id="sflf-mem-category" style="${selectStyle}">
                  <option value="vector_table">Vector: Snowflake Table (VECTOR)</option>
                  <option value="postgres_pgvector">Vector: Snowflake Postgres (pgvector)</option>
                  <option value="cortex_search">Vector: Cortex Search Service</option>
                  <option value="structured">Non-Vector: Structured Table (Standard/Hybrid/Interactive/Dynamic)</option>
                  <option value="semi_structured">Non-Vector: Semi-Structured (VARIANT / OBJECT / ARRAY)</option>
                  <option value="postgres_non_vector">Non-Vector: Snowflake Postgres (Relational / JSONB)</option>
                </select>
              </div>
              <div>
                <label style="display:block;font-size:10px;color:#d4d4d8;margin-bottom:3px;font-weight:600;">Backend / Table Subtype</label>
                <select id="sflf-mem-backend" style="${selectStyle}">
                  <option value="Standard Table (VECTOR)">Standard Table (VECTOR)</option>
                  <option value="Hybrid Table (VECTOR)">Hybrid Table (VECTOR)</option>
                  <option value="Interactive Table (VECTOR)">Interactive Table (VECTOR)</option>
                  <option value="Dynamic Table (VECTOR)">Dynamic Table (VECTOR)</option>
                </select>
              </div>
            </div>
            <div id="sflf-mem-embed-row" style="display:grid;grid-template-columns:1.4fr 0.6fr 0.6fr;gap:8px;margin-bottom:8px;">
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
        </div>

        <!-- Section 3: Replicated Flow Activity & Governance Audit Log (REQ-15) -->
        <div style="background:#27272a;padding:14px;border-radius:8px;min-width:0;box-sizing:border-box;">
          <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:6px;">
            <div>
              <h4 style="margin:0 0 2px 0;font-size:13px;color:#29B5E8;">3. Replicated Flow Activity &amp; Governance Audit Log in Snowflake SQL (REQ-15)</h4>
              <p style="margin:0;font-size:11px;color:#a1a1aa;">Sincronizza le esecuzioni di Langflow verso <code>LANGFLOW_FLOW_RUNS</code>, <code>LANGFLOW_FLOW_SPANS</code> e <code>LANGFLOW_AUDIT_LOG</code> (protette da Row Access Policy <code>LANGFLOW_TRACE_RAP</code>).</p>
            </div>
            <button id="sflf-btn-sync-traces" style="background:#10b981;color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:11px;font-weight:600;white-space:nowrap;">Sync Flow Activity to Snowflake SQL</button>
          </div>
          <div id="sflf-traces-out" style="${codeBoxStyle};max-height:150px;">Loading Snowflake SQL traces...</div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    document.getElementById("sflf-modal-close").onclick = function () {
      modal.style.display = "none";
    };

    // Section 0 event listeners
    const ctxDbSel = document.getElementById("sflf-ctx-db");
    const ctxSchSel = document.getElementById("sflf-ctx-schema");
    if (ctxDbSel) {
      ctxDbSel.onchange = async function () {
        const chosenDb = ctxDbSel.value;
        sfState.database = chosenDb;
        await fetchSnowflakeCatalog(chosenDb, "");
        const schemasForDb =
          (sfState.catalog && sfState.catalog.schemas_by_db && sfState.catalog.schemas_by_db[chosenDb]) || ["CORE"];
        sfState.schema = schemasForDb[0] || "CORE";
        await fetchSnowflakeCatalog(chosenDb, sfState.schema);
        refreshHubModalPrivilegeAndContextViews();
      };
    }
    if (ctxSchSel) {
      ctxSchSel.onchange = async function () {
        sfState.schema = ctxSchSel.value;
        await fetchSnowflakeCatalog(sfState.database, sfState.schema);
        refreshHubModalPrivilegeAndContextViews();
      };
    }
    const multiDbsBox = document.getElementById("sflf-ctx-multi-dbs");
    if (multiDbsBox) {
      multiDbsBox.addEventListener("change", async function (ev) {
        const cb = ev.target;
        if (!cb || !cb.classList || !cb.classList.contains("sflf-ctx-db-cb")) return;
        const checkedDbs = [];
        modal.querySelectorAll(".sflf-ctx-db-cb").forEach(function (el) {
          if (el.checked && el.value) checkedDbs.push(el.value);
        });
        if (checkedDbs.length === 0) checkedDbs.push(sfState.database || "SNOWFLAKE_LANGFLOW");
        sfState.selected_context = sfState.selected_context || {};
        sfState.selected_context.databases = checkedDbs;
        await fetchSnowflakeCatalog(sfState.database, sfState.schema, checkedDbs, sfState.selected_context.schemas_by_db);
        refreshHubModalPrivilegeAndContextViews();
      });
    }
    const multiSchemasBox = document.getElementById("sflf-ctx-multi-schemas");
    if (multiSchemasBox) {
      multiSchemasBox.addEventListener("change", async function (ev) {
        const cb = ev.target;
        if (!cb || !cb.classList || !cb.classList.contains("sflf-ctx-schema-cb")) return;
        const schemasByDb = {};
        modal.querySelectorAll(".sflf-ctx-schema-cb").forEach(function (el) {
          if (el.checked && el.value) {
            const parts = el.value.split(".");
            if (parts[0] && parts[1]) {
              schemasByDb[parts[0]] = schemasByDb[parts[0]] || [];
              if (schemasByDb[parts[0]].indexOf(parts[1]) === -1) schemasByDb[parts[0]].push(parts[1]);
            }
          }
        });
        sfState.selected_context = sfState.selected_context || {};
        sfState.selected_context.schemas_by_db = schemasByDb;
        await fetchSnowflakeCatalog(
          sfState.database,
          sfState.schema,
          sfState.selected_context.databases,
          schemasByDb
        );
        refreshHubModalPrivilegeAndContextViews();
      });
    }
    const refreshCatBtn = document.getElementById("sflf-btn-refresh-catalog");
    if (refreshCatBtn) {
      refreshCatBtn.onclick = async function () {
        const db = (ctxDbSel && ctxDbSel.value) || sfState.database;
        const sch = (ctxSchSel && ctxSchSel.value) || sfState.schema;
        await fetchSnowflakeCatalog(db, sch);
        refreshHubModalPrivilegeAndContextViews();
      };
    }
    const saveCtxBtn = document.getElementById("sflf-btn-save-context");
    if (saveCtxBtn) {
      saveCtxBtn.onclick = function () {
        const db = (ctxDbSel && ctxDbSel.value) || sfState.database;
        const sch = (ctxSchSel && ctxSchSel.value) || sfState.schema;
        const checkedDbs = [];
        modal.querySelectorAll(".sflf-ctx-db-cb").forEach(function (cb) {
          if (cb.checked && cb.value) checkedDbs.push(cb.value);
        });
        if (checkedDbs.indexOf(db) === -1) checkedDbs.unshift(db);

        const schemasByDb = {};
        checkedDbs.forEach(function (d) {
          schemasByDb[d] = [];
        });
        modal.querySelectorAll(".sflf-ctx-schema-cb").forEach(function (cb) {
          if (cb.checked && cb.value) {
            const parts = cb.value.split(".");
            const dName = parts[0];
            const sName = parts[1];
            if (dName && sName) {
              if (!schemasByDb[dName]) schemasByDb[dName] = [];
              if (schemasByDb[dName].indexOf(sName) === -1) schemasByDb[dName].push(sName);
            }
          }
        });
        if (!schemasByDb[db] || schemasByDb[db].length === 0) {
          schemasByDb[db] = [sch];
        }

        const checkedCategories = {};
        const cat = sfState.catalog || {};
        modal.querySelectorAll(".sflf-ctx-obj-filter").forEach(function (cb) {
          const key = cb.value;
          checkedCategories[key] = cb.checked ? cat[key] || [] : [];
        });
        const objectsByDbSchema = Object.assign({}, cat.objects_by_db_schema || {});
        const selectedObjects = {};
        modal.querySelectorAll(".sflf-ctx-obj-chip").forEach(function (chip) {
          const pairKey = chip.getAttribute("data-pair") || `${db}.${sch}`;
          const catKey = chip.getAttribute("data-category") || "";
          if (!catKey) return;
          selectedObjects[pairKey] = selectedObjects[pairKey] || {};
          const pairBucket = objectsByDbSchema[pairKey] || cat;
          selectedObjects[pairKey][catKey] = chip.checked ? pairBucket[catKey] || [] : [];
        });
        persistSnowflakeContextSelection({
          database: db,
          schema: sch,
          databases: checkedDbs,
          schemas_by_db: schemasByDb,
          objects_by_db_schema: objectsByDbSchema,
          selected_objects: selectedObjects,
          visible_objects: checkedCategories,
          selected_context: {
            databases: checkedDbs,
            schemas_by_db: schemasByDb,
            objects_by_db_schema: objectsByDbSchema,
            selected_objects: selectedObjects,
            visible_objects: checkedCategories,
          },
        });
      };
    }

    // Section 1 event listeners
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

    // Section 2 event listeners
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

    document.getElementById("sflf-btn-dryrun").onclick = function () {
      runProvisionCall(true);
    };
    document.getElementById("sflf-btn-provision").onclick = function () {
      runProvisionCall(false);
    };
    document.getElementById("sflf-btn-create-mem").onclick = function () {
      runCreateMemoryCall();
    };
    document.getElementById("sflf-btn-sync-traces").onclick = function () {
      refreshTracesInModal();
    };

    refreshHubModalPrivilegeAndContextViews();
    refreshTracesInModal();
    if (focusSection === "memories") {
      const sec2El = document.getElementById("sflf-sec2");
      if (sec2El && typeof sec2El.scrollIntoView === "function") {
        sec2El.scrollIntoView({ behavior: "smooth", block: "center" });
      }
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
    const agentMemoryType = (document.getElementById("sflf-mem-agent-type") || {}).value || "semantic_vector";
    const category = (document.getElementById("sflf-mem-category") || {}).value || "vector_table";
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
        embedding_model: embedModel,
        vector_element_type: elemType,
        dimension: dimVal,
        vector_dimension: dimVal,
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

  async function refreshTracesInModal() {
    const out = document.getElementById("sflf-traces-out");
    const roleSpan = document.getElementById("sflf-modal-role");
    if (roleSpan) roleSpan.textContent = sfState.active_role;
    if (!out) return;
    try {
      const resp = await fetch("/api/v1/snowflake/traces/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      const data = await resp.json();
      out.textContent = JSON.stringify(data, null, 2);
    } catch (e) {
      out.textContent = String(e);
    }
  }

  function bootSnowflakeUI() {
    installHeadMutationObserver();
    ensureDualSymbolLoadingOverlay();
    mountFixedTopbarControls();
    fetchSnowflakeContext();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootSnowflakeUI);
  } else {
    bootSnowflakeUI();
  }
})();
