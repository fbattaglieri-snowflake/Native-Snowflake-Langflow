import React, { useState } from "react";

export const SNOWFLAKE_MODEL_PROVIDERS = [
  {
    id: "snowflake_proxy_responses",
    displayName: "Snowflake Proxy - Responses",
    protocol: "responses",
    sdkBasePath: "/responses/v1",
    inferencePath: "/responses/v1/responses",
    modelsDiscoveryPath: "/responses/v1/models",
  },
  {
    id: "snowflake_proxy_messages",
    displayName: "Snowflake Proxy - Messages",
    protocol: "messages",
    sdkBasePath: "/messages",
    inferencePath: "/messages/v1/messages",
    modelsDiscoveryPath: "/messages/v1/models",
  },
  {
    id: "snowflake_proxy_chat",
    displayName: "Snowflake Proxy - Chat",
    protocol: "chat",
    sdkBasePath: "/chat/v1",
    inferencePath: "/chat/v1/chat/completions",
    modelsDiscoveryPath: "/chat/v1/models",
  },
  {
    id: "snowflake_ollama_hosted",
    displayName: "Ollama (Snowflake Hosted)",
    protocol: "ollama",
    sdkBasePath: "<OLLAMA_BASE_URL>",
    inferencePath: "/api/chat",
    modelsDiscoveryPath: "/api/tags",
  },
] as const;

export const SNOWFLAKE_DB_PROVIDERS = [
  {
    id: "snowflake_standard_vector",
    displayName: "Snowflake Standard Table (VECTOR)",
    maxDimension: 4096,
  },
  {
    id: "snowflake_hybrid_vector",
    displayName: "Snowflake Hybrid Table (VECTOR)",
    maxDimension: 4096,
  },
  {
    id: "snowflake_cortex_search",
    displayName: "Snowflake Cortex Search",
    executionRights: "owners_rights",
  },
  {
    id: "snowflake_postgres_pgvector",
    displayName: "Snowflake Postgres (pgvector)",
    port: 5432,
  },
] as const;

export const INVARIANT_SETTINGS_TABS = [
  "General",
  "MCP Servers",
  "Langflow API Keys",
  "Langflow MCP Client",
  "Shortcuts",
] as const;

export interface SnowflakeSettingsPanelProps {
  activeRole: string;
  contextEpoch: number;
  selectedTab:
    | "General"
    | "MCP Servers"
    | "Langflow API Keys"
    | "Langflow MCP Client"
    | "Shortcuts"
    | "Model Providers"
    | "DB Providers"
    | "Messages"
    | "Global Variables";
}

/**
 * Settings Panel Extension (REQ-04):
 * Preserves General, MCP Servers, Langflow API Keys, Langflow MCP Client, Shortcuts unchanged,
 * while extending Model Providers, DB Providers, Messages (3 protocols), and Global Variables (read-only role).
 */
export const SnowflakeSettingsPanel: React.FC<SnowflakeSettingsPanelProps> = ({
  activeRole,
  contextEpoch,
  selectedTab,
}) => {
  return (
    <section
      data-testid="snowflake-settings-panel"
      data-selected-tab={selectedTab}
      data-context-epoch={contextEpoch}
      className="flex flex-col gap-4 rounded-lg border border-border bg-background p-6 text-sm text-foreground"
    >
      {selectedTab === "Model Providers" && (
        <div data-testid="settings-model-providers" className="flex flex-col gap-3">
          <h3 className="text-base font-semibold">Model Providers</h3>
          <ul className="flex flex-col gap-2">
            {SNOWFLAKE_MODEL_PROVIDERS.map((p) => (
              <li
                key={p.id}
                data-testid={`model-provider-${p.id}`}
                className="flex items-center justify-between rounded border border-border p-3 text-xs"
              >
                <span className="font-medium">{p.displayName}</span>
                <code className="text-muted-foreground">{p.sdkBasePath}</code>
              </li>
            ))}
          </ul>
        </div>
      )}

      {selectedTab === "DB Providers" && (
        <div data-testid="settings-db-providers" className="flex flex-col gap-3">
          <h3 className="text-base font-semibold">DB Providers</h3>
          <ul className="flex flex-col gap-2">
            {SNOWFLAKE_DB_PROVIDERS.map((db) => (
              <li
                key={db.id}
                data-testid={`db-provider-${db.id}`}
                className="flex items-center justify-between rounded border border-border p-3 text-xs"
              >
                <span className="font-medium">{db.displayName}</span>
                <span className="text-muted-foreground">{db.id}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {selectedTab === "Messages" && (
        <div data-testid="settings-messages" className="flex flex-col gap-3">
          <h3 className="text-base font-semibold">
            Messages (Chat, Responses, Messages Protocols)
          </h3>
          <p className="text-xs text-muted-foreground">
            Inspect, edit, and delete conversation messages across all 3 Proxy Standard protocols. Editing or removing signed replay blocks (thinking.signature, redacted_thinking, reasoning.encrypted_content) safely branches/invalidates downstream signed replay state.
          </p>
        </div>
      )}

      {selectedTab === "Global Variables" && (
        <div data-testid="settings-global-variables" className="flex flex-col gap-3">
          <h3 className="text-base font-semibold">Global Variables</h3>
          <div className="flex items-center justify-between rounded border border-border bg-muted/30 p-3 text-xs">
            <span>SNOWFLAKE_ACTIVE_ROLE (Read-Only Backend Session Authority)</span>
            <input
              type="text"
              readOnly
              disabled
              data-testid="global-var-snowflake-active-role"
              value={activeRole}
              className="rounded border border-border bg-muted px-2 py-1 font-mono text-xs"
            />
          </div>
        </div>
      )}
    </section>
  );
};

export const SNOWFLAKE_BLUE = "#29B5E8";

/**
 * W2-R2-04B: Object Provisioner 4 Infrastructure Families (Memory relocated to Memories `+ Snowflake`).
 */
export const OBJECT_FAMILIES = [
  "Table",
  "View",
  "Stage",
  "Snowflake Postgres",
] as const;

export type SnowflakeObjectFamily = (typeof OBJECT_FAMILIES)[number];

/**
 * W2-R3-03: Snowsight-style Hierarchical Tree Categories (11 governed categories).
 */
export const SNOWSIGHT_TREE_CATEGORIES = [
  { key: "tables", label: "Tables" },
  { key: "views", label: "Views" },
  { key: "iceberg_tables", label: "Iceberg Tables" },
  { key: "stages", label: "Stages" },
  { key: "memories", label: "Memories" },
  { key: "cortex_search_services", label: "Cortex Search" },
  { key: "semantic_views", label: "Cortex Analyst" },
  { key: "cortex_agents", label: "Cortex Agents" },
  { key: "functions", label: "Functions (UDF/UDTF)" },
  { key: "procedures", label: "Procedures" },
  { key: "postgres_instances", label: "Postgres" },
] as const;

export interface SnowsightHierarchyTreeProps {
  activeRole: string;
  flowId?: string | null;
  databases: string[];
  schemasByDb: Record<string, string[]>;
  objectsByDbSchema: Record<string, Record<string, string[]>>;
  selectedDatabases: string[];
  selectedSchemasByDb: Record<string, string[]>;
  selectedObjectsByPair: Record<string, Record<string, string[]>>;
  onToggleDatabase: (database: string, checked: boolean) => void;
  onToggleSchema: (database: string, schema: string, checked: boolean) => void;
  onToggleObject: (pairKey: string, category: string, objectName: string, checked: boolean) => void;
}

/**
 * W2-R4 Preserved Provisioning & Memories Concept reference (`frontend_patches/preserved_provisioning_and_memories_concept.js`).
 * The visible Snowflake Hub modal operates in `data-hub-mode="context-only"` (Section 0 + compact auto-sync badge),
 * while Section 1 (Object Provisioning) and Section 2 (Snowflake Memories) are preserved intact in
 * `window.SNOWFLAKE_PRESERVED_PROVISIONING_AND_MEMORIES_CONCEPT` for the upcoming Memory round.
 */
export const PRESERVED_PROVISIONING_AND_MEMORIES_MODULE =
  "frontend_patches/preserved_provisioning_and_memories_concept.js";

/**
 * W2-R3-03 & W2-R4-01..04: Snowsight-Style Hierarchical Expandable Tree Selector with Real-Time Search,
 * Workflow-Personalized (`flow_id`) Context Scope, Instantaneous O(DOM) `updateTreeCheckboxesInPlace`,
 * and Tri-State Cascading Checkboxes (`checked`, `indeterminate`, `unchecked`).
 */
export const SnowsightHierarchyTreeSelector: React.FC<SnowsightHierarchyTreeProps> = ({
  activeRole,
  flowId,
  databases,
  schemasByDb,
  objectsByDbSchema,
  selectedDatabases,
  selectedSchemasByDb,
  selectedObjectsByPair,
  onToggleDatabase,
  onToggleSchema,
  onToggleObject,
}) => {
  const [searchQuery, setSearchQuery] = useState("");
  const [expandedDbs, setExpandedDbs] = useState<Record<string, boolean>>({});
  const [expandedSchemas, setExpandedSchemas] = useState<Record<string, boolean>>({});
  const q = searchQuery.trim().toLowerCase();

  return (
    <div
      data-hub-mode="context-only"
      className="flex flex-col gap-2 rounded-lg border border-border bg-background p-3 text-xs"
      style={{
        width: "clamp(320px, 94vw, 980px)",
        maxWidth: "96vw",
        boxSizing: "border-box",
      }}
    >
      <div className="flex items-center justify-between gap-2">
        <span
          id="sflf-modal-flow-scope-badge"
          data-testid="hub-workflow-scope-badge"
          data-flow-id={flowId || ""}
          className="rounded border px-2 py-0.5 text-[11px] font-semibold"
          style={{ color: SNOWFLAKE_BLUE, borderColor: SNOWFLAKE_BLUE }}
        >
          {flowId ? `Workflow: ${flowId}` : "Scope: Global Default"}
        </span>
        <span
          id="sflf-auto-sync-status-badge"
          data-testid="hub-auto-sync-status-badge"
          className="rounded border border-emerald-500/40 bg-zinc-950 px-2 py-0.5 text-[10.5px] text-emerald-200"
        >
          Auto-Replicated to Snowflake: LANGFLOW_FLOW_RUNS · LANGFLOW_FLOW_SPANS · LANGFLOW_AUDIT_LOG
        </span>
      </div>
      <input
        id="sflf-sec0-tree-search"
        data-testid="snowsight-hierarchy-search"
        type="search"
        placeholder="Search databases, schemas, tables, memories, services..."
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        className="rounded border border-border bg-background px-2.5 py-1.5 text-xs"
      />
      <div
        id="sflf-sec0-tree"
        data-testid="snowsight-hierarchy-tree"
        data-active-role={activeRole}
        className="flex flex-col gap-1 rounded border border-border p-2"
        style={{ maxHeight: "min(50dvh, 260px)", overflowY: "auto" }}
      >
        <div className="text-[11px] font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
          Databases ({databases.length}) — Active Role: {activeRole}
        </div>
        {databases.map((db) => {
          const schemas = schemasByDb[db] || ["CORE"];
          const selectedSchemas = selectedSchemasByDb[db] || [];
          const dbChecked = schemas.length > 0 && selectedSchemas.length === schemas.length;
          const dbIndeterminate =
            selectedSchemas.length > 0 && selectedSchemas.length < schemas.length;
          const isDbOpen = q ? true : !!expandedDbs[db];

          return (
            <div key={db} data-testid={`tree-db-${db}`} className="flex flex-col gap-1 pl-1">
              <div className="flex items-center gap-1.5">
                <button
                  type="button"
                  onClick={() => setExpandedDbs((p) => ({ ...p, [db]: !p[db] }))}
                  className="px-1 text-xs"
                  style={{ color: SNOWFLAKE_BLUE }}
                >
                  {isDbOpen ? "▾" : "▸"}
                </button>
                <input
                  type="checkbox"
                  checked={dbChecked || selectedDatabases.includes(db)}
                  data-indeterminate={dbIndeterminate ? "true" : "false"}
                  ref={(el) => {
                    if (el) el.indeterminate = dbIndeterminate;
                  }}
                  onChange={(e) => onToggleDatabase(db, e.target.checked)}
                />
                <span className="font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
                  {db}
                </span>
              </div>
              {isDbOpen &&
                schemas.map((sch) => {
                  const pairKey = `${db}.${sch}`;
                  const pairBucket = objectsByDbSchema[pairKey] || {};
                  const selBucket = selectedObjectsByPair[pairKey] || {};
                  const isSchOpen = q ? true : !!expandedSchemas[pairKey];
                  const schChecked = selectedSchemas.includes(sch);

                  return (
                    <div key={pairKey} data-testid={`tree-schema-${pairKey}`} className="flex flex-col gap-1 pl-5">
                      <div className="flex items-center gap-1.5">
                        <button
                          type="button"
                          onClick={() =>
                            setExpandedSchemas((p) => ({ ...p, [pairKey]: !p[pairKey] }))
                          }
                          className="px-1 text-xs"
                          style={{ color: SNOWFLAKE_BLUE }}
                        >
                          {isSchOpen ? "▾" : "▸"}
                        </button>
                        <input
                          type="checkbox"
                          checked={schChecked}
                          data-indeterminate="false"
                          onChange={(e) => onToggleSchema(db, sch, e.target.checked)}
                        />
                        <span className="font-medium">{sch}</span>
                      </div>
                      {isSchOpen &&
                        SNOWSIGHT_TREE_CATEGORIES.map((cat) => {
                          const items = pairBucket[cat.key] || [];
                          const selItems = selBucket[cat.key] || [];
                          const catIndeterminate =
                            selItems.length > 0 && selItems.length < items.length;
                          return (
                            <div key={`${pairKey}:${cat.key}`} className="flex flex-col gap-0.5 pl-5">
                              <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
                                <span>
                                  {cat.label} ({selItems.length}/{items.length})
                                </span>
                                {catIndeterminate && (
                                  <span data-indeterminate="true" className="text-[10px]">
                                    (partial)
                                  </span>
                                )}
                              </div>
                              {items.map((objName) => (
                                <label
                                  key={`${pairKey}:${cat.key}:${objName}`}
                                  className="flex items-center gap-1.5 pl-4 font-mono text-[10.5px]"
                                >
                                  <input
                                    type="checkbox"
                                    checked={selItems.includes(objName)}
                                    onChange={(e) =>
                                      onToggleObject(pairKey, cat.key, objName, e.target.checked)
                                    }
                                  />
                                  <span>{objName}</span>
                                </label>
                              ))}
                            </div>
                          );
                        })}
                    </div>
                  );
                })}
            </div>
          );
        })}
      </div>
    </div>
  );
};

/**
 * W2-R3-05 & W2-R2-13: 5 Canonical LangChain/Langflow Agent Memory Types + Legacy Aliases.
 */
export const SNOWFLAKE_AGENT_MEMORY_TYPES = [
  {
    id: "graph_state_checkpointer",
    label: "#1 Short-Term: Graph State & Checkpointer",
    category: "short_term",
    needsEmbeddingBadge: "NO (Mai / Never)",
    recommendedSnowflakeObjects: [
      "Snowflake Postgres (PostgresSaver)",
      "Hybrid Table",
      "SnowflakeCheckpointer",
    ],
    howToFeedWithoutEmbedding:
      "SELECT state_blob, checkpoint_ns, step_id FROM <fqn> WHERE thread_id = :thread_id ORDER BY step_id DESC LIMIT 1",
    whenEmbeddingUseful:
      "Mai: lo stato del grafo viene recuperato sempre per chiave primaria thread_id.",
    requiredInputs: ["thread_id", "checkpoint_ns", "ttl_hours"],
    defaultEnableEmbedding: false,
    description:
      "Execution graph state and thread checkpointer retrieved by primary key thread_id without embeddings.",
    backends: [
      "Snowflake Postgres (PostgresSaver)",
      "Hybrid Table",
      "SnowflakeCheckpointer",
    ],
  },
  {
    id: "buffer_window_memory",
    label: "#2 Short-Term: Buffer / Window Memory (Storico Chat)",
    category: "short_term",
    needsEmbeddingBadge: "NO",
    recommendedSnowflakeObjects: [
      "Snowflake Postgres (Relational / JSONB)",
      "Hybrid Table",
      "Standard Table",
    ],
    howToFeedWithoutEmbedding:
      "SELECT sender, message_text, created_at FROM <fqn> WHERE session_id = :session_id ORDER BY created_at DESC LIMIT :window_k",
    whenEmbeddingUseful:
      "Utile solo per recuperare messaggi detti 500+ turni fa fuori dalla finestra K.",
    requiredInputs: ["session_id", "window_k", "retention_days"],
    defaultEnableEmbedding: false,
    description:
      "Conversational chat history window (last K messages) retrieved chronologically by session_id.",
    backends: [
      "Snowflake Postgres (Relational / JSONB)",
      "Hybrid Table",
      "Standard Table",
    ],
  },
  {
    id: "summary_memory",
    label: "#3 Short-Term: Summary Memory (Riassunto)",
    category: "short_term",
    needsEmbeddingBadge: "NO (Mai / Never)",
    recommendedSnowflakeObjects: [
      "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG",
      "Standard Table + AI_COMPLETE",
      "Snowflake Postgres",
    ],
    howToFeedWithoutEmbedding:
      "SELECT running_summary, last_turn_id FROM <fqn> WHERE session_id = :session_id LIMIT 1 -- iniettato nel System Prompt",
    whenEmbeddingUseful:
      "Mai: è un riassunto di 1 riga letto direttamente e aggiornato in background con AI_COMPLETE / AI_SUMMARIZE_AGG.",
    requiredInputs: ["session_id", "summarizer_model", "max_summary_tokens"],
    defaultEnableEmbedding: false,
    description:
      "Running conversation summary updated incrementally via AI_COMPLETE / AI_SUMMARIZE_AGG and injected into the System Prompt.",
    backends: [
      "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG",
      "Standard Table + AI_COMPLETE",
      "Snowflake Postgres",
    ],
  },
  {
    id: "user_profile_entity_memory",
    label: "#4 Long-Term Semantic: User Profile & Entity Memory",
    category: "long_term_semantic",
    needsEmbeddingBadge: "OPZIONALE (90% NO)",
    recommendedSnowflakeObjects: [
      "Snowflake Postgres (PostgresStore)",
      "Hybrid Table (VARIANT)",
      "Standard Table (VARIANT)",
    ],
    howToFeedWithoutEmbedding:
      "SELECT profile_json FROM <fqn> WHERE user_id = :user_id AND entity_namespace = :entity_namespace LIMIT 1 -- caricato a inizio sessione",
    whenEmbeddingUseful:
      "Solo se l'utente accumula centinaia di fatti distinti e non entrano tutti nel prompt.",
    requiredInputs: ["user_id", "entity_namespace", "schema_keys"],
    defaultEnableEmbedding: false,
    description:
      "Structured JSON/VARIANT user profile and entity facts loaded by user_id and entity_namespace.",
    backends: [
      "Snowflake Postgres (PostgresStore)",
      "Hybrid Table (VARIANT)",
      "Standard Table (VARIANT)",
    ],
  },
  {
    id: "episodic_memory",
    label: "#5 Long-Term Semantic: Episodic Memory (Esperienze / Few-shot)",
    category: "long_term_semantic",
    needsEmbeddingBadge: "OPZIONALE",
    recommendedSnowflakeObjects: [
      "Hybrid Table",
      "Standard Table",
      "Snowflake Postgres",
    ],
    howToFeedWithoutEmbedding:
      "SELECT topic, tool_used, outcome_score, episode_text FROM <fqn> WHERE topic = :topic ORDER BY outcome_score DESC, created_at DESC LIMIT :few_shot_k",
    whenEmbeddingUseful:
      "Utile quando i task passati non hanno categorie fisse e serve similarity search per trovare episodi simili.",
    requiredInputs: ["topic_column", "tool_used_column", "outcome_score_threshold", "few_shot_k"],
    defaultEnableEmbedding: false,
    description:
      "Past agent experiences and high-scoring few-shot examples retrieved by topic/tool or optional vector similarity.",
    backends: [
      "Hybrid Table",
      "Standard Table",
      "Snowflake Postgres",
    ],
  },
] as const;

export const COMING_SOON_MEMORY_TYPES = [
  {
    id: "business_ontology_cortex_sense",
    label: "#6 Long-Term: Business Ontology / Cortex Sense (Coming Soon)",
    category: "long_term_ontology",
    enabled: false,
    badge: "Coming Soon",
    needsEmbeddingBadge: "OPZIONALE — Gestito da Cortex Sense",
    recommendedSnowflakeObjects: [
      "Cortex Sense Context",
      "Business Ontology Graph",
      "Semantic View",
    ],
    description:
      "Enterprise Business Ontology and Cortex Sense semantic graph memory (Coming Soon).",
  },
] as const;

export const LEGACY_MEMORY_TYPE_ALIASES = {
  episodic_session: "buffer_window_memory",
  semantic_vector: "episodic_memory",
  structured_long_term: "user_profile_entity_memory",
} as const;

export const EMBEDDING_PROVIDERS_CATALOG = {
  snowflake_cortex: {
    id: "snowflake_cortex",
    label: "1. Snowflake Cortex Native (AI_EMBED / AI_MULTI_EMBED)",
    models: [
      { modelId: "snowflake-arctic-embed-m-v1.5", dimension: 768 },
      { modelId: "e5-base-v2", dimension: 768 },
      { modelId: "snowflake-arctic-embed-l-v2.0", dimension: 1024 },
      { modelId: "nv-embed-qa-4", dimension: 1024 },
      { modelId: "multilingual-e5-large", dimension: 1024 },
      { modelId: "twelvelabs-marengo-embed-3-0", dimension: 512 },
    ],
  },
  spcs_model_registry: {
    id: "spcs_model_registry",
    label: "2. Snowflake Model Registry (SPCS Compute Pool, 1..4096d)",
    models: [
      { modelId: "bge-large-en-v1.5-spcs", dimension: 1024 },
      { modelId: "intfloat-e5-mistral-7b-spcs", dimension: 4096 },
    ],
  },
  spcs_ollama: {
    id: "spcs_ollama",
    label: "3. Private - Ollama (SPCS Compute Pool)",
    models: [
      { modelId: "nomic-embed-text", dimension: 768 },
      { modelId: "bge-large-en-v1.5", dimension: 1024 },
    ],
  },
  spcs_llm_proxy: {
    id: "spcs_llm_proxy",
    label: "4. Snowflake Standard Proxy (Chat / Responses / Messages)",
    models: [
      { modelId: "proxy-chat-text-embedding-3-small", dimension: 1536 },
      { modelId: "proxy-responses-embed-v1", dimension: 1024 },
    ],
  },
  openai: {
    id: "openai",
    label: "OpenAI (Langflow External)",
    models: [
      { modelId: "text-embedding-3-small", dimension: 1536 },
      { modelId: "text-embedding-3-large", dimension: 3072 },
      { modelId: "text-embedding-ada-002", dimension: 1536 },
    ],
  },
  cohere: {
    id: "cohere",
    label: "Cohere (Langflow External)",
    models: [
      { modelId: "embed-english-v3.0", dimension: 1024 },
      { modelId: "embed-multilingual-v3.0", dimension: 1024 },
    ],
  },
  huggingface_ollama: {
    id: "huggingface_ollama",
    label: "HuggingFace / Ollama",
    models: [
      { modelId: "nomic-embed-text", dimension: 768 },
      { modelId: "bge-large-en-v1.5", dimension: 1024 },
    ],
  },
  custom_endpoint: {
    id: "custom_endpoint",
    label: "Custom Endpoint (1..4096d)",
    models: [{ modelId: "custom-embedding-endpoint", dimension: 768 }],
  },
} as const;

export type SnowflakeAgentMemoryTypeId =
  | (typeof SNOWFLAKE_AGENT_MEMORY_TYPES)[number]["id"]
  | keyof typeof LEGACY_MEMORY_TYPE_ALIASES;

export type EmbeddingProviderId = keyof typeof EMBEDDING_PROVIDERS_CATALOG;

export interface CreateMemoryDialogProps {
  activeRole: string;
  embeddingModels: string[];
  onCancel: () => void;
  onCreateMemory: (payload: {
    name: string;
    agentMemoryType?: SnowflakeAgentMemoryTypeId;
    enableEmbedding?: boolean;
    embeddingProvider?: EmbeddingProviderId | "none";
    embeddingModel: string;
    vectorDatabase: string;
    batchSize: number;
    llmPreprocessingEnabled: boolean;
    llmPreprocessingProvider?: string;
    createNewPhysicalStore: boolean;
  }) => Promise<void>;
}

/**
 * `+ Snowflake` (`[ + ❄️ Snowflake ]`) Button on the Langflow `Memories` Page (`W2-R2-13`, `W2-R2-14`, `W2-R3-06`, `W2-R4-01`):
 * Styled in official Snowflake Blue (`#29B5E8`), with `+` before the Snowflake icon and text `Snowflake`,
 * strictly scoped to the Memories view with normal toolbar stacking order (`zIndex: 10`) and hidden when any modal dialog is open.
 */
export const MemoriesPlusSnowflakeButton: React.FC<{
  onClick: () => void;
  isMemoriesView?: boolean;
  isModalOpen?: boolean;
}> = ({ onClick, isMemoriesView = true, isModalOpen = false }) => {
  if (!isMemoriesView || isModalOpen) {
    return null;
  }
  return (
    <button
      type="button"
      data-testid="memories-plus-snowflake-btn"
      onClick={onClick}
      className="inline-flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold text-white shadow-sm"
      style={{
        backgroundColor: SNOWFLAKE_BLUE,
        borderColor: SNOWFLAKE_BLUE,
        color: "#ffffff",
        zIndex: 10,
      }}
    >
      <span data-testid="memories-plus-snowflake-plus">+</span>
      <span data-testid="memories-plus-snowflake-icon" aria-hidden="true">❄️</span>
      <span data-testid="memories-plus-snowflake-label">Snowflake</span>
    </button>
  );
};

/**
 * Create Memory Dialog (`REQ-10`, `G-A4`, `W2-R2-13`, `W2-R2-14`, `W2-R3-05`, `W2-R3-06`):
 * Light-themed modal matching Langflow's native `Create a Memory` modal (`#ffffff` surface, `#0f172a` text),
 * supporting the 5 LangChain/Langflow Memory Types (`graph_state_checkpointer`, `buffer_window_memory`,
 * `summary_memory`, `user_profile_entity_memory`, `episodic_memory` + legacy aliases), non-embedding
 * direct SQL lookup by default, and optional `Snowflake Cortex (AI_EMBED)` or external embedding providers.
 */
export const CreateMemoryDialog: React.FC<CreateMemoryDialogProps> = ({
  activeRole,
  embeddingModels,
  onCancel,
  onCreateMemory,
}) => {
  const [name, setName] = useState("");
  const [agentMemoryType, setAgentMemoryType] =
    useState<SnowflakeAgentMemoryTypeId>("buffer_window_memory");
  const [enableEmbedding, setEnableEmbedding] = useState<boolean>(false);
  const [embeddingProvider, setEmbeddingProvider] =
    useState<EmbeddingProviderId>("snowflake_cortex");
  const [embeddingModel, setEmbeddingModel] = useState(
    embeddingModels[0] || "snowflake-arctic-embed-m-v1.5"
  );
  const [vectorDatabase, setVectorDatabase] = useState<string>(
    SNOWFLAKE_DB_PROVIDERS[0].id
  );
  const [batchSize, setBatchSize] = useState<number>(64);
  const [llmPreprocessingEnabled, setLlmPreprocessingEnabled] = useState<boolean>(false);
  const [llmPreprocessingProvider, setLlmPreprocessingProvider] = useState<string>(
    SNOWFLAKE_MODEL_PROVIDERS[0].id
  );
  const [createNewPhysicalStore, setCreateNewPhysicalStore] = useState<boolean>(false);
  const [memorySearchFilter, setMemorySearchFilter] = useState<string>("");

  const canonicalId =
    (LEGACY_MEMORY_TYPE_ALIASES as Record<string, string>)[agentMemoryType] ||
    agentMemoryType;
  const activeSpec =
    SNOWFLAKE_AGENT_MEMORY_TYPES.find((m) => m.id === canonicalId) ||
    SNOWFLAKE_AGENT_MEMORY_TYPES[1];

  return (
    <div
      id="sflf-create-memory-modal"
      data-testid="create-snowflake-memory-modal"
      data-active-role={activeRole}
      className="flex flex-col gap-4 rounded-xl border p-6 text-sm"
      style={{
        backgroundColor: "#ffffff",
        color: "#0f172a",
        borderColor: "#e2e8f0",
        boxShadow: "0 20px 50px rgba(15, 23, 42, 0.18)",
        width: "clamp(320px, 94vw, 980px)",
        maxWidth: "96vw",
        maxHeight: "min(90dvh, 90vh)",
        overflowY: "auto",
      }}
    >
      <div data-testid="create-memory-dialog" className="flex flex-col gap-4">
        <div className="flex items-center justify-between">
          <h3 className="text-base font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
            Create a Snowflake Memory (+ Snowflake)
          </h3>
          <span
            data-testid="memory-active-role-badge"
            className="rounded px-2 py-0.5 text-xs font-semibold"
            style={{ color: SNOWFLAKE_BLUE, border: `1px solid ${SNOWFLAKE_BLUE}` }}
          >
            Role: {activeRole}
          </span>
        </div>

        {/* 5 LangChain/Langflow Agent Memory Type Selector (W2-R3-05) */}
        <div
          data-testid="memory-agent-type-cards"
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 280px), 1fr))",
            gap: "10px",
          }}
        >
          {SNOWFLAKE_AGENT_MEMORY_TYPES.map((memType) => {
            const isSelected = canonicalId === memType.id;
            return (
              <button
                key={memType.id}
                type="button"
                data-testid={`memory-type-${memType.id}`}
                onClick={() => setAgentMemoryType(memType.id)}
                className="flex flex-col items-start gap-1 rounded border p-2.5 text-left text-xs"
                style={{
                  borderColor: isSelected ? SNOWFLAKE_BLUE : "#cbd5e1",
                  backgroundColor: isSelected ? "rgba(41, 181, 232, 0.10)" : "#f8fafc",
                }}
              >
                <div className="flex w-full items-center justify-between gap-2">
                  <span className="font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
                    {memType.label}
                  </span>
                  <span className="rounded-full bg-sky-100 px-2 py-0.5 text-[10px] font-bold text-sky-800">
                    Serve Embedding? {memType.needsEmbeddingBadge}
                  </span>
                </div>
                <span className="text-[11px] text-slate-600">{memType.description}</span>
              </button>
            );
          })}
        </div>

        {/* Interactive Type Guidance Card (Non-Embedding SQL Lookup & Recommended Objects) */}
        <div
          data-testid="memory-type-guidance-card"
          className="flex flex-col gap-1.5 rounded-lg border border-sky-200 bg-slate-50 p-3 text-xs"
        >
          <div>
            <strong>Oggetto Snowflake Consigliato:</strong>{" "}
            {activeSpec.recommendedSnowflakeObjects.join(" · ")}
          </div>
          <div>
            <strong>Come si passa all&apos;Agente SENZA Embedding:</strong>
            <code className="mt-1 block rounded bg-slate-900 p-2 font-mono text-[11px] text-slate-100">
              {activeSpec.howToFeedWithoutEmbedding}
            </code>
          </div>
          <div>
            <strong>Quando l&apos;Embedding diventa utile:</strong>{" "}
            {activeSpec.whenEmbeddingUseful}
          </div>
        </div>

        <input
          type="text"
          placeholder="Memory Name"
          data-testid="memory-name-input"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="rounded border border-slate-300 bg-white px-2.5 py-1.5 text-xs text-slate-900"
        />

        {/* Optional Embedding Toggle (default: false) */}
        <label className="flex items-center gap-2 text-xs font-medium">
          <input
            type="checkbox"
            data-testid="snowflake-memory-enable-embedding"
            checked={enableEmbedding}
            onChange={(e) => setEnableEmbedding(e.target.checked)}
          />
          <span>
            Enable Embedding (Optional — default off; uses direct SQL lookup when disabled)
          </span>
        </label>

        {enableEmbedding && (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 240px), 1fr))",
              gap: "10px",
            }}
          >
            <select
              data-testid="snowflake-memory-embedding-provider"
              value={embeddingProvider}
              onChange={(e) => setEmbeddingProvider(e.target.value as EmbeddingProviderId)}
              className="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900"
            >
              {Object.values(EMBEDDING_PROVIDERS_CATALOG).map((prov) => (
                <option key={prov.id} value={prov.id}>
                  {prov.label}
                </option>
              ))}
            </select>
            <select
              data-testid="memory-embedding-model-select"
              value={embeddingModel}
              onChange={(e) => setEmbeddingModel(e.target.value)}
              className="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900"
            >
              {embeddingModels.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </div>
        )}

        <select
          data-testid="memory-vector-db-select"
          value={vectorDatabase}
          onChange={(e) => setVectorDatabase(e.target.value)}
          className="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900"
        >
          {SNOWFLAKE_DB_PROVIDERS.map((db) => (
            <option key={db.id} value={db.id}>
              {db.displayName}
            </option>
          ))}
        </select>
        <input
          type="number"
          min={1}
          max={1024}
          data-testid="memory-batch-size-input"
          value={batchSize}
          onChange={(e) => setBatchSize(Number(e.target.value))}
          className="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900"
        />
        <label className="flex items-center gap-2 text-xs">
          <input
            type="checkbox"
            data-testid="memory-llm-preprocessing-checkbox"
            checked={llmPreprocessingEnabled}
            onChange={(e) => setLlmPreprocessingEnabled(e.target.checked)}
          />
          <span>LLM Preprocessing (Optional — no generative call when disabled)</span>
        </label>
        {llmPreprocessingEnabled && (
          <select
            data-testid="memory-llm-provider-select"
            value={llmPreprocessingProvider}
            onChange={(e) => setLlmPreprocessingProvider(e.target.value)}
            className="rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-900"
          >
            {SNOWFLAKE_MODEL_PROVIDERS.map((p) => (
              <option key={p.id} value={p.id}>
                {p.displayName}
              </option>
            ))}
          </select>
        )}
        <label className="flex items-center gap-2 text-xs">
          <input
            type="checkbox"
            data-testid="memory-create-physical-checkbox"
            checked={createNewPhysicalStore}
            onChange={(e) => setCreateNewPhysicalStore(e.target.checked)}
          />
          <span>Provision new physical table / Cortex Search service (requires CREATE privilege)</span>
        </label>

        {/* Searchable Memories Cards Grid Preview (W2-R3-05) */}
        <input
          type="search"
          placeholder="Search memories..."
          value={memorySearchFilter}
          onChange={(e) => setMemorySearchFilter(e.target.value)}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-900"
        />
        <div
          id="sflf-memories-cards-grid"
          data-testid="snowflake-memories-cards-grid"
          data-search-query={memorySearchFilter}
          className="grid gap-2"
          style={{
            gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 280px), 1fr))",
          }}
        />

        <div className="flex justify-end gap-2">
          <button
            type="button"
            data-testid="btn-cancel-memory"
            onClick={onCancel}
            className="rounded border border-slate-300 bg-white px-3 py-1.5 text-xs text-slate-700"
          >
            Cancel
          </button>
          <button
            type="button"
            data-testid="btn-create-memory"
            onClick={() =>
              onCreateMemory({
                name,
                agentMemoryType,
                enableEmbedding,
                embeddingProvider: enableEmbedding ? embeddingProvider : "none",
                embeddingModel,
                vectorDatabase,
                batchSize,
                llmPreprocessingEnabled,
                llmPreprocessingProvider: llmPreprocessingEnabled
                  ? llmPreprocessingProvider
                  : undefined,
                createNewPhysicalStore,
              })
            }
            className="rounded px-3 py-1.5 text-xs font-medium text-white"
            style={{ backgroundColor: SNOWFLAKE_BLUE, color: "#ffffff" }}
          >
            Create Memory
          </button>
        </div>
      </div>
    </div>
  );
};

export interface SnowflakeObjectProvisionerModalProps {
  activeRole: string;
  database: string;
  schema: string;
  onProvision: (payload: {
    family: SnowflakeObjectFamily;
    subtype: string;
    name: string;
  }) => Promise<void>;
}

/**
 * Snowflake Object Provisioner Modal (`W2-R2-04B`):
 * Strictly exposes the 4 Snowflake Infrastructure Families (`Table`, `View`, `Stage`, `Snowflake Postgres`),
 * with `Memory` removed and relocated to the Langflow `Memories` (`+ Snowflake`) page.
 */
export const SnowflakeObjectProvisionerModal: React.FC<SnowflakeObjectProvisionerModalProps> = ({
  activeRole,
  database,
  schema,
  onProvision,
}) => {
  const [family, setFamily] = useState<SnowflakeObjectFamily>("Table");
  const [subtype, setSubtype] = useState<string>("STANDARD");
  const [name, setName] = useState<string>("DEMO_OBJECT");

  return (
    <div
      data-testid="snowflake-object-provisioner-modal"
      data-active-role={activeRole}
      className="flex flex-col gap-3 rounded-lg border border-border bg-background p-4 text-xs text-foreground"
    >
      <h4 className="text-sm font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
        Object Provisioner ({database}.{schema})
      </h4>
      <select
        data-testid="provisioner-family-select"
        value={family}
        onChange={(e) => setFamily(e.target.value as SnowflakeObjectFamily)}
        className="rounded border border-border bg-background px-2 py-1.5"
      >
        {OBJECT_FAMILIES.map((f) => (
          <option key={f} value={f}>
            {f}
          </option>
        ))}
      </select>
      <input
        type="text"
        data-testid="provisioner-subtype-input"
        value={subtype}
        onChange={(e) => setSubtype(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5"
      />
      <input
        type="text"
        data-testid="provisioner-name-input"
        value={name}
        onChange={(e) => setName(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5"
      />
      <button
        type="button"
        data-testid="btn-provision-object"
        onClick={() => onProvision({ family, subtype, name })}
        className="rounded px-3 py-1.5 font-semibold text-white"
        style={{ backgroundColor: SNOWFLAKE_BLUE, color: "#ffffff" }}
      >
        Provision {family}
      </button>
    </div>
  );
};

export interface ServeAsA2AAgentPanelProps {
  flowId: string;
  hasChatInput: boolean;
  hasChatOutput: boolean;
  onSaveCard: (card: { name: string; description: string }) => Promise<void>;
  onPublishEndpoint: () => Promise<void>;
}

/**
 * Serve as an A2A Agent Panel (REQ-14):
 * Shows `Unavailable` until both `Chat Input` and `Chat Output` are present in the flow,
 * distinguishes `Save` card metadata from `Publish` endpoint, and exposes credential-free Address/curl.
 */
export const ServeAsA2AAgentPanel: React.FC<ServeAsA2AAgentPanelProps> = ({
  flowId,
  hasChatInput,
  hasChatOutput,
  onSaveCard,
  onPublishEndpoint,
}) => {
  const isAvailable = hasChatInput && hasChatOutput;
  const [agentName, setAgentName] = useState("");
  const [agentDescription, setAgentDescription] = useState("");

  if (!isAvailable) {
    return (
      <div
        data-testid="a2a-serve-unavailable"
        className="rounded-lg border border-border bg-muted/30 p-4 text-xs text-muted-foreground"
      >
        <strong>Unavailable:</strong> Add both <code>Chat Input</code> and{" "}
        <code>Chat Output</code> components to this flow to serve it as an A2A agent.
      </div>
    );
  }

  return (
    <div
      data-testid="a2a-serve-panel"
      className="flex flex-col gap-3 rounded-lg border border-border bg-background p-4 text-xs text-foreground"
    >
      <h4 className="text-sm font-semibold">Serve as an A2A agent — Customize card</h4>
      <input
        type="text"
        placeholder="Agent Card Name"
        data-testid="a2a-card-name"
        value={agentName}
        onChange={(e) => setAgentName(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5"
      />
      <textarea
        placeholder="Agent Card Description"
        data-testid="a2a-card-description"
        value={agentDescription}
        onChange={(e) => setAgentDescription(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5"
      />
      <code data-testid="a2a-address-url" className="rounded bg-muted p-2">
        {`https://<SPCS_INGRESS_HOST>/api/v1/a2a/flows/${flowId}/invoke`}
      </code>
      <div className="flex justify-end gap-2">
        <button
          type="button"
          data-testid="btn-a2a-save-card"
          onClick={() => onSaveCard({ name: agentName, description: agentDescription })}
          className="rounded border border-border px-3 py-1.5"
        >
          Save
        </button>
        <button
          type="button"
          data-testid="btn-a2a-publish"
          onClick={onPublishEndpoint}
          className="rounded bg-primary px-3 py-1.5 font-medium text-primary-foreground"
        >
          Publish
        </button>
      </div>
    </div>
  );
};
