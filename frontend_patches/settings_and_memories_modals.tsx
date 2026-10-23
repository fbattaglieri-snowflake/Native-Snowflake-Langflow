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
 * W2-R2-13: 3 Agent Memory Types for the Langflow `Memories` (`+ Snowflake`) wizard.
 */
export const SNOWFLAKE_AGENT_MEMORY_TYPES = [
  {
    id: "episodic_session",
    label: "+ Episodic / Session Memory",
    description:
      "Conversational turns, session state, and short-term episodic recall backed by Snowflake Hybrid Table or Snowflake Postgres (Relational / JSONB).",
    backends: ["Hybrid Table", "Standard Table", "Snowflake Postgres (Relational / JSONB)"],
  },
  {
    id: "semantic_vector",
    label: "+ Semantic / Vector Memory",
    description:
      "Dense semantic embeddings and similarity search backed by Snowflake VECTOR(FLOAT|INT, 768|1024) coupled to AI_EMBED, Snowflake Postgres pgvector, or Cortex Search.",
    backends: [
      "Standard Table (VECTOR)",
      "Hybrid Table (VECTOR)",
      "Snowflake Postgres (pgvector)",
      "Cortex Search Service",
    ],
  },
  {
    id: "structured_long_term",
    label: "+ Structured / Semi-Structured Long-Term Memory",
    description:
      "Persistent agent facts, user preferences, and entity profiles backed by Standard / Dynamic / Interactive Tables with VARIANT/OBJECT/ARRAY or Views.",
    backends: [
      "Standard Table (VARIANT)",
      "Dynamic Table (VARIANT)",
      "Interactive Table (VARIANT)",
      "Standard View",
    ],
  },
] as const;

export type SnowflakeAgentMemoryTypeId = (typeof SNOWFLAKE_AGENT_MEMORY_TYPES)[number]["id"];

export interface CreateMemoryDialogProps {
  activeRole: string;
  embeddingModels: string[];
  onCancel: () => void;
  onCreateMemory: (payload: {
    name: string;
    agentMemoryType?: SnowflakeAgentMemoryTypeId;
    embeddingModel: string;
    vectorDatabase: string;
    batchSize: number;
    llmPreprocessingEnabled: boolean;
    llmPreprocessingProvider?: string;
    createNewPhysicalStore: boolean;
  }) => Promise<void>;
}

/**
 * `+ Snowflake` Button on the Langflow `Memories` Page (`W2-R2-13`, `W2-R2-14`):
 * Styled in official Snowflake Blue (`#29B5E8`).
 */
export const MemoriesPlusSnowflakeButton: React.FC<{ onClick: () => void }> = ({ onClick }) => (
  <button
    type="button"
    data-testid="memories-plus-snowflake-btn"
    onClick={onClick}
    className="inline-flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold text-white shadow-sm"
    style={{
      backgroundColor: SNOWFLAKE_BLUE,
      borderColor: SNOWFLAKE_BLUE,
      color: "#ffffff",
    }}
  >
    <span>+ Snowflake</span>
  </button>
);

/**
 * Create Memory Dialog (`REQ-10`, `G-A4`, `W2-R2-13`, `W2-R2-14`):
 * Supports the 3 Agent Memory Types (`episodic_session`, `semantic_vector`, `structured_long_term`)
 * under the `+ Snowflake` (`#29B5E8`) Memories wizard, while preserving Name, Embedding Model,
 * Vector Database, Batch Size, optional LLM Preprocessing, and Cancel / Create Memory controls.
 */
export const CreateMemoryDialog: React.FC<CreateMemoryDialogProps> = ({
  activeRole,
  embeddingModels,
  onCancel,
  onCreateMemory,
}) => {
  const [name, setName] = useState("");
  const [agentMemoryType, setAgentMemoryType] =
    useState<SnowflakeAgentMemoryTypeId>("semantic_vector");
  const [embeddingModel, setEmbeddingModel] = useState(
    embeddingModels[0] || "snowflake-arctic-embed-l-v2.0"
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

  return (
    <div
      data-testid="create-memory-dialog"
      data-active-role={activeRole}
      className="flex flex-col gap-4 rounded-lg border border-border bg-background p-6 text-sm text-foreground"
    >
      <div className="flex items-center justify-between">
        <h3 className="text-base font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
          Create Snowflake Agent Memory (+ Snowflake)
        </h3>
        <span
          data-testid="memory-active-role-badge"
          className="rounded px-2 py-0.5 text-xs font-semibold"
          style={{ color: SNOWFLAKE_BLUE, border: `1px solid ${SNOWFLAKE_BLUE}` }}
        >
          Role: {activeRole}
        </span>
      </div>

      {/* 3 Agent Memory Type Selector (W2-R2-13) */}
      <div
        data-testid="memory-agent-type-cards"
        className="grid grid-cols-1 gap-2 md:grid-cols-3"
      >
        {SNOWFLAKE_AGENT_MEMORY_TYPES.map((memType) => {
          const isSelected = agentMemoryType === memType.id;
          return (
            <button
              key={memType.id}
              type="button"
              data-testid={`memory-type-${memType.id}`}
              onClick={() => setAgentMemoryType(memType.id)}
              className="flex flex-col items-start gap-1 rounded border p-2.5 text-left text-xs"
              style={{
                borderColor: isSelected ? SNOWFLAKE_BLUE : "var(--border)",
                backgroundColor: isSelected ? "rgba(41, 181, 232, 0.12)" : "transparent",
              }}
            >
              <span className="font-semibold" style={{ color: SNOWFLAKE_BLUE }}>
                {memType.label}
              </span>
              <span className="text-[11px] text-muted-foreground">
                {memType.description}
              </span>
            </button>
          );
        })}
      </div>

      <input
        type="text"
        placeholder="Memory Name"
        data-testid="memory-name-input"
        value={name}
        onChange={(e) => setName(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5 text-xs"
      />
      <select
        data-testid="memory-embedding-model-select"
        value={embeddingModel}
        onChange={(e) => setEmbeddingModel(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5 text-xs"
      >
        {embeddingModels.map((m) => (
          <option key={m} value={m}>
            {m}
          </option>
        ))}
      </select>
      <select
        data-testid="memory-vector-db-select"
        value={vectorDatabase}
        onChange={(e) => setVectorDatabase(e.target.value)}
        className="rounded border border-border bg-background px-2 py-1.5 text-xs"
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
        className="rounded border border-border bg-background px-2 py-1.5 text-xs"
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
          className="rounded border border-border bg-background px-2 py-1.5 text-xs"
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
      <div className="flex justify-end gap-2">
        <button
          type="button"
          data-testid="btn-cancel-memory"
          onClick={onCancel}
          className="rounded border border-border px-3 py-1.5 text-xs"
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
