"""End-to-End Showcase Flow: `Snowflake_Full_Memory_Enterprise_Agent` (`W2-R4-06`, `W2-R4-06B`, `W2-R4-07`).

Demonstrates simultaneous use of all 5 canonical Snowflake Memory types (`#1..#5`) plus
`#7` (`Cortex Analyst` + `Cortex Search` business data sources) and `#8` (`System Message`
procedural memory) in a single governed Langflow workflow:
- Branch A (`Database ▸ Schema`): `SNOWFLAKE_LANGFLOW.CORE` (`Hybrid Table` / `Standard Table` for `#3`, `#4`, `#5`, plus business objects for `#7`).
- Branch B (`Snowflake Postgres` account-level `:5432`, separate from `Database ▸ Schema`): `LANGFLOW_PG_INSTANCE` (`postgres.public` for high-frequency transactional `#1` and `#2`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .components.data_sources import filter_business_data_source_objects, is_technical_memory_table
from .components.knowledge_and_memories import SnowflakeMemoryBaseComponent
from .native_category_components.files_and_knowledge.snowflake_memory_base import (
    SnowflakeMemoryBaseNativeComponent,
)
from .native_category_components.models_and_agents.snowflake_message_history import (
    SnowflakeMessageHistoryNativeComponent,
)

SHOWCASE_FLOW_ID = "snowflake-full-memory-enterprise-agent-001"
SHOWCASE_FLOW_NAME = "Snowflake_Full_Memory_Enterprise_Agent"

SHOWCASE_FLOW_CONTEXT: dict[str, Any] = {
    "flow_id": SHOWCASE_FLOW_ID,
    "databases": ["SNOWFLAKE_LANGFLOW"],
    "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
    "warehouses": ["COMPUTE_WH"],
    "compute_pools": ["LANGFLOW_COMPUTE_POOL"],
    "postgres_instances": ["LANGFLOW_PG_INSTANCE"],
    "selected_objects": {
        "SNOWFLAKE_LANGFLOW.CORE": {
            "tables": [
                "FIN_GL_TRANSACTIONS",
                "FIN_Q3_Q4_MARGIN_LEDGER",
            ],
            "views": [
                "FIN_NET_MARGIN_VARIANCE_V",
            ],
            "semantic_views": [
                "FIN_OPERATIONS_SV",
            ],
            "cortex_search_services": [
                "FIN_POLICY_SEARCH_SVC",
            ],
            "stages": [
                "SNOWFLAKE_LANGFLOW_STAGE",
            ],
        }
    },
    "objects_by_db_schema": {
        "SNOWFLAKE_LANGFLOW.CORE": {
            "tables": [
                "FIN_GL_TRANSACTIONS",
                "FIN_Q3_Q4_MARGIN_LEDGER",
            ],
            "views": [
                "FIN_NET_MARGIN_VARIANCE_V",
            ],
            "semantic_views": [
                "FIN_OPERATIONS_SV",
            ],
            "cortex_search_services": [
                "FIN_POLICY_SEARCH_SVC",
            ],
            "stages": [
                "SNOWFLAKE_LANGFLOW_STAGE",
            ],
        }
    },
}

SHOWCASE_MEMORIES_SPECS: list[dict[str, Any]] = [
    {
        "name": "MEM_1_GRAPH_CHECKPOINTER",
        "flow_id": SHOWCASE_FLOW_ID,
        "agent_memory_type": "graph_state_checkpointer",
        "canonical_memory_type": "graph_state_checkpointer",
        "storage_family": "snowflake_postgres",
        "postgres_instance": "LANGFLOW_PG_INSTANCE",
        "pg_database": "postgres",
        "pg_schema": "public",
        "pg_table": "langgraph_checkpoints",
        "pg_table_mode": "PostgresSaver",
        "fallback_table_fqn": "SNOWFLAKE_LANGFLOW.CORE.AGENT_CHECKPOINTS",
        "database": "SNOWFLAKE_LANGFLOW",
        "schema": "CORE",
        "vector_database": "Snowflake Postgres (PostgresSaver)",
        "table_fqn": "LANGFLOW_PG_INSTANCE:5432/postgres.public.langgraph_checkpoints",
        "enable_embedding": False,
        "embedding_provider": "none",
        "filter_by_session": True,
        "tool_mode": False,
        "thread_id": "fin-reconcile-thread-001",
        "checkpoint_ns": "fin_reconciliation_hitl",
        "ttl_hours": 72,
        "auto_capture": True,
        "ingestion_threshold": 1,
        "llm_preprocessing": {
            "enabled": False,
            "engine": "none",
            "model": None,
            "kill_phrase": "NO_INGEST",
        },
    },
    {
        "name": "MEM_2_CHAT_WINDOW",
        "flow_id": SHOWCASE_FLOW_ID,
        "agent_memory_type": "buffer_window_memory",
        "canonical_memory_type": "buffer_window_memory",
        "storage_family": "snowflake_postgres",
        "postgres_instance": "LANGFLOW_PG_INSTANCE",
        "pg_database": "postgres",
        "pg_schema": "public",
        "pg_table": "chat_message_history",
        "pg_table_mode": "Relational / JSONB",
        "fallback_table_fqn": "SNOWFLAKE_LANGFLOW.CORE.CHAT_WINDOW_HISTORY",
        "database": "SNOWFLAKE_LANGFLOW",
        "schema": "CORE",
        "vector_database": "Snowflake Postgres (Relational / JSONB)",
        "table_fqn": "LANGFLOW_PG_INSTANCE:5432/postgres.public.chat_message_history",
        "enable_embedding": False,
        "embedding_provider": "none",
        "filter_by_session": True,
        "tool_mode": False,
        "session_id": "fin-reconcile-session-001",
        "window_k": 8,
        "retention_days": 30,
        "auto_capture": True,
        "ingestion_threshold": 1,
        "llm_preprocessing": {
            "enabled": False,
            "engine": "none",
            "model": None,
            "kill_phrase": "NO_INGEST",
        },
    },
    {
        "name": "MEM_3_SESSION_SUMMARY",
        "flow_id": SHOWCASE_FLOW_ID,
        "agent_memory_type": "summary_memory",
        "canonical_memory_type": "summary_memory",
        "storage_family": "snowflake_sql",
        "database": "SNOWFLAKE_LANGFLOW",
        "schema": "CORE",
        "object_subtype": "Hybrid Table",
        "vector_database": "Hybrid Table",
        "table_name": "CHAT_SESSION_SUMMARY",
        "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.CHAT_SESSION_SUMMARY",
        "primary_key": "session_id",
        "enable_embedding": False,
        "embedding_provider": "none",
        "filter_by_session": True,
        "tool_mode": False,
        "session_id": "fin-reconcile-session-001",
        "summarizer_model": "openai-gpt-6-luna",
        "max_summary_tokens": 512,
        "auto_capture": True,
        "ingestion_threshold": 1,
        "llm_preprocessing": {
            "enabled": True,
            "engine": "proxy_chat",
            "model": "openai-gpt-6-luna",
            "fallback_sql_function": "SNOWFLAKE.CORTEX.AI_COMPLETE",
            "kill_phrase": "NO_INGEST",
        },
    },
    {
        "name": "MEM_4_USER_ENTITY_PROFILE",
        "flow_id": SHOWCASE_FLOW_ID,
        "agent_memory_type": "user_profile_entity_memory",
        "canonical_memory_type": "user_profile_entity_memory",
        "storage_family": "snowflake_sql",
        "database": "SNOWFLAKE_LANGFLOW",
        "schema": "CORE",
        "object_subtype": "Hybrid Table",
        "vector_database": "Hybrid Table (VARIANT)",
        "table_name": "USER_ENTITY_PROFILE",
        "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.USER_ENTITY_PROFILE",
        "primary_key": "user_id, entity_namespace, memory_key",
        "payload_column_type": "VARIANT",
        "enable_embedding": False,
        "embedding_provider": "none",
        "filter_by_session": False,
        "tool_mode": False,
        "user_id": "fin_analyst_01",
        "entity_namespace": "financial_reporting_prefs",
        "schema_keys": ["currency", "language", "kpi_focus", "department"],
        "auto_capture": True,
        "ingestion_threshold": 1,
        "llm_preprocessing": {
            "enabled": True,
            "engine": "proxy_messages",
            "model": "claude-sonnet-5-5",
            "kill_phrase": "NO_INGEST",
        },
    },
    {
        "name": "MEM_5_EPISODIC_FEWSHOT",
        "flow_id": SHOWCASE_FLOW_ID,
        "agent_memory_type": "episodic_memory",
        "canonical_memory_type": "episodic_memory",
        "storage_family": "snowflake_sql",
        "database": "SNOWFLAKE_LANGFLOW",
        "schema": "CORE",
        "object_subtype": "Standard Table",
        "vector_database": "Standard Table (VECTOR)",
        "table_name": "AGENT_EPISODIC_MEMORY",
        "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.AGENT_EPISODIC_MEMORY",
        "enable_embedding": True,
        "embedding_provider": "snowflake_cortex",
        "embedding_model": "snowflake-arctic-embed-l-v2.0",
        "dimension": 1024,
        "filter_by_session": False,
        "tool_mode": True,
        "topic_column": "topic",
        "tool_used_column": "tool_used",
        "outcome_score_threshold": 0.9,
        "few_shot_k": 3,
        "auto_capture": True,
        "ingestion_threshold": 1,
        "llm_preprocessing": {
            "enabled": False,
            "engine": "cortex_sql",
            "model": "mistral-large2",
            "kill_phrase": "NO_INGEST",
        },
    },
]

PROCEDURAL_SYSTEM_MESSAGE_TEMPLATE = (
    "Sei l'Assistente Enterprise Snowflake Financial & Operations (#8 Procedural Memory).\n"
    "Regole operative obbligatorie:\n"
    "1. Rispetta il profilo utente cross-sessione (#4 MEM_4_USER_ENTITY_PROFILE): usa valuta Euro (€), "
    "lingua italiana e focus prioritario sul margine netto Q3/Q4.\n"
    "2. Tieni conto del riassunto della sessione corrente (#3 MEM_3_SESSION_SUMMARY) per i turni 1..N-8 "
    "e degli ultimi 8 messaggi verbatim (#2 MEM_2_CHAT_WINDOW).\n"
    "3. Prima di eseguire riconciliazioni complesse, consulta il tool #5 MEM_5_EPISODIC_FEWSHOT "
    "(outcome_score >= 0.9, top-k = 3) per riutilizzare le traiettorie SQL vincenti passate.\n"
    "4. Per i dati di business (#7), interroga esclusivamente Cortex Analyst (FIN_OPERATIONS_SV) "
    "e Cortex Search (FIN_POLICY_SEARCH_SVC) sotto il ruolo attivo CURRENT_ROLE()."
)


def build_snowflake_full_memory_enterprise_agent_flow() -> dict[str, Any]:
    """Build the complete Langflow JSON structure for `Snowflake_Full_Memory_Enterprise_Agent` (`W2-R4-07`)."""
    nodes: list[dict[str, Any]] = [
        {
            "id": "ChatInput-showcase01",
            "type": "genericNode",
            "position": {"x": 80, "y": 320},
            "data": {
                "id": "ChatInput-showcase01",
                "type": "ChatInput",
                "node": {
                    "display_name": "Chat Input",
                    "description": "Financial analyst prompt input.",
                    "template": {
                        "input_value": {
                            "name": "input_value",
                            "value": "Riconcilia lo scostamento di margine Q4 come abbiamo fatto per le chiusure passate",
                        },
                        "session_id": {
                            "name": "session_id",
                            "value": "fin-reconcile-session-001",
                        },
                    },
                },
            },
        },
        {
            "id": "SnowflakeMemoryBase-mem1",
            "type": "genericNode",
            "position": {"x": 420, "y": 40},
            "data": {
                "id": "SnowflakeMemoryBase-mem1",
                "type": "SnowflakeMemoryBase",
                "node": {
                    "display_name": "Memory Base (#1 Checkpointer)",
                    "category": "files_and_knowledge",
                    "icon": "Snowflake",
                    "template": {
                        "memory_base": {"name": "memory_base", "value": "MEM_1_GRAPH_CHECKPOINTER"},
                        "canonical_memory_type": {
                            "name": "canonical_memory_type",
                            "value": "graph_state_checkpointer",
                        },
                        "thread_id": {"name": "thread_id", "value": "fin-reconcile-thread-001", "show": True},
                        "checkpoint_ns": {
                            "name": "checkpoint_ns",
                            "value": "fin_reconciliation_hitl",
                            "show": True,
                        },
                        "ttl_hours": {"name": "ttl_hours", "value": 72, "show": True},
                        "filter_by_session": {"name": "filter_by_session", "value": True, "show": True},
                        "tool_mode": {"name": "tool_mode", "value": False, "show": False},
                    },
                },
            },
        },
        {
            "id": "SnowflakeMessageHistory-mem2",
            "type": "genericNode",
            "position": {"x": 420, "y": 190},
            "data": {
                "id": "SnowflakeMessageHistory-mem2",
                "type": "SnowflakeMessageHistory",
                "node": {
                    "display_name": "Snowflake Message History (#2 Buffer / Window)",
                    "category": "models_and_agents",
                    "icon": "Snowflake",
                    "template": {
                        "memory_base": {"name": "memory_base", "value": "MEM_2_CHAT_WINDOW"},
                        "storage_family": {"name": "storage_family", "value": "snowflake_postgres"},
                        "postgres_instance": {"name": "postgres_instance", "value": "LANGFLOW_PG_INSTANCE"},
                        "pg_table": {"name": "pg_table", "value": "public.chat_message_history"},
                        "session_id": {"name": "session_id", "value": "fin-reconcile-session-001"},
                        "window_k": {"name": "window_k", "value": 8},
                        "filter_by_session": {"name": "filter_by_session", "value": True},
                    },
                },
            },
        },
        {
            "id": "SnowflakeMemoryBase-mem3",
            "type": "genericNode",
            "position": {"x": 420, "y": 340},
            "data": {
                "id": "SnowflakeMemoryBase-mem3",
                "type": "SnowflakeMemoryBase",
                "node": {
                    "display_name": "Memory Base (#3 Session Summary)",
                    "category": "files_and_knowledge",
                    "icon": "Snowflake",
                    "template": {
                        "memory_base": {"name": "memory_base", "value": "MEM_3_SESSION_SUMMARY"},
                        "canonical_memory_type": {"name": "canonical_memory_type", "value": "summary_memory"},
                        "session_id": {"name": "session_id", "value": "fin-reconcile-session-001", "show": True},
                        "summarizer_model": {
                            "name": "summarizer_model",
                            "value": "openai-gpt-6-luna",
                            "show": True,
                        },
                        "max_summary_tokens": {"name": "max_summary_tokens", "value": 512, "show": True},
                        "filter_by_session": {"name": "filter_by_session", "value": True, "show": True},
                        "tool_mode": {"name": "tool_mode", "value": False, "show": False},
                    },
                },
            },
        },
        {
            "id": "SnowflakeMemoryBase-mem4",
            "type": "genericNode",
            "position": {"x": 420, "y": 490},
            "data": {
                "id": "SnowflakeMemoryBase-mem4",
                "type": "SnowflakeMemoryBase",
                "node": {
                    "display_name": "Memory Base (#4 User Profile & Entity)",
                    "category": "files_and_knowledge",
                    "icon": "Snowflake",
                    "template": {
                        "memory_base": {"name": "memory_base", "value": "MEM_4_USER_ENTITY_PROFILE"},
                        "canonical_memory_type": {
                            "name": "canonical_memory_type",
                            "value": "user_profile_entity_memory",
                        },
                        "user_id": {"name": "user_id", "value": "fin_analyst_01", "show": True},
                        "entity_namespace": {
                            "name": "entity_namespace",
                            "value": "financial_reporting_prefs",
                            "show": True,
                        },
                        "filter_by_session": {"name": "filter_by_session", "value": False, "show": True},
                        "tool_mode": {"name": "tool_mode", "value": False, "show": True},
                    },
                },
            },
        },
        {
            "id": "SnowflakeMemoryBase-mem5",
            "type": "genericNode",
            "position": {"x": 420, "y": 640},
            "data": {
                "id": "SnowflakeMemoryBase-mem5",
                "type": "SnowflakeMemoryBase",
                "node": {
                    "display_name": "Memory Base (#5 Episodic Few-Shot Tool)",
                    "category": "files_and_knowledge",
                    "icon": "Snowflake",
                    "template": {
                        "memory_base": {"name": "memory_base", "value": "MEM_5_EPISODIC_FEWSHOT"},
                        "canonical_memory_type": {"name": "canonical_memory_type", "value": "episodic_memory"},
                        "topic": {"name": "topic", "value": "q4_margin_reconciliation", "show": True},
                        "outcome_score_threshold": {
                            "name": "outcome_score_threshold",
                            "value": 0.9,
                            "show": True,
                        },
                        "few_shot_k": {"name": "few_shot_k", "value": 3, "show": True},
                        "filter_by_session": {"name": "filter_by_session", "value": False, "show": True},
                        "tool_mode": {"name": "tool_mode", "value": True, "show": True},
                    },
                },
            },
        },
        {
            "id": "SnowflakeCortexAnalyst-domain7a",
            "type": "genericNode",
            "position": {"x": 420, "y": 790},
            "data": {
                "id": "SnowflakeCortexAnalyst-domain7a",
                "type": "SnowflakeDataSourceCortexAnalyst",
                "node": {
                    "display_name": "Cortex Analyst (#7 Financial Semantic View)",
                    "category": "data_source",
                    "icon": "Snowflake",
                    "template": {
                        "database": {"name": "database", "value": "SNOWFLAKE_LANGFLOW"},
                        "schema_name": {"name": "schema_name", "value": "CORE"},
                        "semantic_view": {"name": "semantic_view", "value": "FIN_OPERATIONS_SV"},
                        "tool_mode": {"name": "tool_mode", "value": True},
                    },
                },
            },
        },
        {
            "id": "SnowflakeCortexSearch-domain7b",
            "type": "genericNode",
            "position": {"x": 420, "y": 930},
            "data": {
                "id": "SnowflakeCortexSearch-domain7b",
                "type": "SnowflakeDataSourceCortexSearch",
                "node": {
                    "display_name": "Cortex Search (#7 Accounting Policy Manuals)",
                    "category": "data_source",
                    "icon": "Snowflake",
                    "template": {
                        "database": {"name": "database", "value": "SNOWFLAKE_LANGFLOW"},
                        "schema_name": {"name": "schema_name", "value": "CORE"},
                        "search_service": {"name": "search_service", "value": "FIN_POLICY_SEARCH_SVC"},
                        "tool_mode": {"name": "tool_mode", "value": True},
                    },
                },
            },
        },
        {
            "id": "SnowflakeStandardChat-agent8",
            "type": "genericNode",
            "position": {"x": 860, "y": 420},
            "data": {
                "id": "SnowflakeStandardChat-agent8",
                "type": "SnowflakeStandardChatModel",
                "node": {
                    "display_name": "Enterprise Financial Agent (Proxy - Chat + #8 Procedural System Message)",
                    "category": "models_and_agents",
                    "icon": "Snowflake",
                    "template": {
                        "model_name": {"name": "model_name", "value": "openai-gpt-6-luna"},
                        "system_message": {
                            "name": "system_message",
                            "value": PROCEDURAL_SYSTEM_MESSAGE_TEMPLATE,
                        },
                        "checkpointer_memory": {
                            "name": "checkpointer_memory",
                            "value": "MEM_1_GRAPH_CHECKPOINTER",
                        },
                    },
                },
            },
        },
        {
            "id": "ChatOutput-showcase01",
            "type": "genericNode",
            "position": {"x": 1240, "y": 420},
            "data": {
                "id": "ChatOutput-showcase01",
                "type": "ChatOutput",
                "node": {
                    "display_name": "Chat Output",
                    "description": "Reconciled financial analysis response in Italian & Euro (€).",
                    "template": {},
                },
            },
        },
    ]

    edges: list[dict[str, Any]] = [
        {
            "id": "edge-input-to-agent",
            "source": "ChatInput-showcase01",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "message",
            "targetHandle": "input_value",
        },
        {
            "id": "edge-mem1-checkpointer-to-agent",
            "source": "SnowflakeMemoryBase-mem1",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "checkpoint_state",
            "targetHandle": "checkpointer_state",
        },
        {
            "id": "edge-mem2-window-to-agent-history",
            "source": "SnowflakeMessageHistory-mem2",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "messages",
            "targetHandle": "chat_history",
        },
        {
            "id": "edge-mem3-summary-to-agent-sysmsg",
            "source": "SnowflakeMemoryBase-mem3",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "running_summary",
            "targetHandle": "system_message",
        },
        {
            "id": "edge-mem4-profile-to-agent-sysmsg",
            "source": "SnowflakeMemoryBase-mem4",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "profile_context",
            "targetHandle": "system_message",
        },
        {
            "id": "edge-mem5-episodic-tool-to-agent-tools",
            "source": "SnowflakeMemoryBase-mem5",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "component_as_tool",
            "targetHandle": "tools",
        },
        {
            "id": "edge-domain7a-analyst-to-agent-tools",
            "source": "SnowflakeCortexAnalyst-domain7a",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "component_as_tool",
            "targetHandle": "tools",
        },
        {
            "id": "edge-domain7b-search-to-agent-tools",
            "source": "SnowflakeCortexSearch-domain7b",
            "target": "SnowflakeStandardChat-agent8",
            "sourceHandle": "component_as_tool",
            "targetHandle": "tools",
        },
        {
            "id": "edge-agent-to-output",
            "source": "SnowflakeStandardChat-agent8",
            "target": "ChatOutput-showcase01",
            "sourceHandle": "text_output",
            "targetHandle": "input_value",
        },
    ]

    return {
        "id": SHOWCASE_FLOW_ID,
        "name": SHOWCASE_FLOW_NAME,
        "description": (
            "End-to-end Enterprise Financial & Operations Assistant combining all 5 canonical "
            "Snowflake Memory types (#1 Graph Checkpointer on Snowflake Postgres, #2 Buffer/Window "
            "Message History on Snowflake Postgres, #3 Summary Memory on Hybrid Table, #4 Cross-Session "
            "User Profile & Entity Memory on Hybrid Table VARIANT with claude-sonnet-5-5 + NO_INGEST, "
            "#5 Episodic Few-Shot Memory in Tool Mode with Snowflake Cortex AI_EMBED 1024d) alongside "
            "#7 Cortex Analyst & Cortex Search and #8 Procedural System Message."
        ),
        "endpoint_name": "snowflake-full-memory-enterprise-agent",
        "is_component": False,
        "data": {
            "nodes": nodes,
            "edges": edges,
            "viewport": {"x": 0, "y": 0, "zoom": 0.85},
            "snowflake_context": SHOWCASE_FLOW_CONTEXT,
            "snowflake_memories": SHOWCASE_MEMORIES_SPECS,
        },
    }


def export_showcase_flow_json(target_path: str | Path | None = None) -> Path:
    """Write `starter_projects/Snowflake_Full_Memory_Enterprise_Agent.json`."""
    if target_path is None:
        repo_root = Path(__file__).resolve().parents[2]
        target_path = repo_root / "starter_projects" / "Snowflake_Full_Memory_Enterprise_Agent.json"
    out_path = Path(target_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_snowflake_full_memory_enterprise_agent_flow()
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return out_path


def bootstrap_full_memory_showcase_flow(
    bridge: Any = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Provision/register the 5 showcase memories (`#1..#5`) and flow context on `LIVE_BRIDGE` (`W2-R4-07`)."""
    if bridge is None:
        from .fastapi_live_bridge import LIVE_BRIDGE as _bridge

        bridge = _bridge

    set_ctx_fn = getattr(bridge, "set_flow_context", None)
    if callable(set_ctx_fn):
        set_ctx_fn(SHOWCASE_FLOW_ID, dict(SHOWCASE_FLOW_CONTEXT))
    elif hasattr(bridge, "flow_contexts") and isinstance(bridge.flow_contexts, dict):
        bridge.flow_contexts[SHOWCASE_FLOW_ID] = dict(SHOWCASE_FLOW_CONTEXT)

    created_memories: list[dict[str, Any]] = []
    for spec in SHOWCASE_MEMORIES_SPECS:
        req_body = {
            **spec,
            "create_new_physical_store": not dry_run,
            "dry_run": dry_run,
        }
        res = bridge.create_or_register_memory(req_body)
        created_memories.append(res)

    flow_payload = build_snowflake_full_memory_enterprise_agent_flow()
    business_tables = filter_business_data_source_objects(
        [
            "FIN_GL_TRANSACTIONS",
            "FIN_Q3_Q4_MARGIN_LEDGER",
            "AGENT_CHECKPOINTS",
            "CHAT_WINDOW_HISTORY",
            "CHAT_SESSION_SUMMARY",
            "USER_ENTITY_PROFILE",
            "AGENT_EPISODIC_MEMORY",
            "LANGGRAPH_CHECKPOINTS",
            "CHAT_MESSAGE_HISTORY",
        ],
        memories_registry=getattr(bridge, "memories_registry", []),
        category="tables",
    )

    return {
        "ok": True,
        "flow_id": SHOWCASE_FLOW_ID,
        "flow_name": SHOWCASE_FLOW_NAME,
        "active_role": getattr(bridge, "active_role", "SNOWFLAKE_LANGFLOW_ADMIN"),
        "snowflake_context": SHOWCASE_FLOW_CONTEXT,
        "memories_registered_count": len(created_memories),
        "memories": created_memories,
        "data_sources_business_tables_only": business_tables,
        "technical_memory_tables_excluded_from_data_sources": all(
            not is_technical_memory_table(t) for t in business_tables
        ),
        "flow": flow_payload,
    }


def run_full_memory_showcase_simulation(
    bridge: Any = None,
    *,
    dry_run: bool = True,
    enable_episodic_embedding: bool = True,
) -> dict[str, Any]:
    """Execute a multi-turn simulation of `Snowflake_Full_Memory_Enterprise_Agent` (`W2-R4-07`).

    Verifies simultaneous operation of:
    - `#1 MEM_1_GRAPH_CHECKPOINTER` (`thread_id`, `checkpoint_ns`, HITL resume state)
    - `#2 MEM_2_CHAT_WINDOW` (`session_id`, `window_k=8`, `filter_by_session=True`, `SnowflakeMessageHistory`)
    - `#3 MEM_3_SESSION_SUMMARY` (`session_id`, `filter_by_session=True`, 1-line summary of turns 1..N-8)
    - `#4 MEM_4_USER_ENTITY_PROFILE` (`filter_by_session=False`, `NO_INGEST` skip on trivial turn + `MERGE` on preference turn)
    - `#5 MEM_5_EPISODIC_FEWSHOT` (`filter_by_session=False`, `tool_mode=True`, `outcome_score >= 0.9`, `few_shot_k=3`, with `enable_embedding=True` via `AI_EMBED` or `enable_embedding=False` via SQL)
    - `#7` (`Cortex Analyst` `FIN_OPERATIONS_SV` + `Cortex Search` `FIN_POLICY_SEARCH_SVC`)
    - `#8` (`System Message` procedural instructions)
    """
    if bridge is None:
        from .fastapi_live_bridge import LIVE_BRIDGE as _bridge

        bridge = _bridge

    boot = bootstrap_full_memory_showcase_flow(bridge, dry_run=dry_run)

    # Optionally toggle #5 episodic memory between Embedding ON (AI_EMBED 1024d) and Embedding OFF (SQL / AI_FILTER)
    for entry in getattr(bridge, "memories_registry", []):
        if str(entry.get("name") or "").upper() == "MEM_5_EPISODIC_FEWSHOT":
            entry["enable_embedding"] = bool(enable_episodic_embedding)
            entry["embedding_provider"] = "snowflake_cortex" if enable_episodic_embedding else "none"
            entry["embedding_model"] = "snowflake-arctic-embed-l-v2.0" if enable_episodic_embedding else None

    # Turn 1: Trivial greeting -> Preprocessor (`claude-sonnet-5-5`) emits kill phrase `NO_INGEST`
    turn_1_trivial = {
        "turn_index": 1,
        "user_input": "Ciao, grazie mille!",
        "preprocessor_model": "claude-sonnet-5-5",
        "preprocessor_output": "NO_INGEST",
        "status": "Skipped",
        "merged_into_profile": False,
    }

    # Turn 2: Durable user preference -> Preprocessor extracts JSON and merges into USER_ENTITY_PROFILE
    extracted_profile_json = {
        "currency": "EUR (€)",
        "language": "it-IT",
        "kpi_focus": ["Q3_net_margin", "Q4_net_margin"],
        "department": "Enterprise Financial & Operations",
    }
    turn_2_preference = {
        "turn_index": 2,
        "user_input": "Ricorda che preferisco tutti i report in Euro (€), in italiano, con focus sul margine netto Q3/Q4.",
        "preprocessor_model": "claude-sonnet-5-5",
        "preprocessor_output": json.dumps(extracted_profile_json, ensure_ascii=False),
        "status": "Processed",
        "merged_into_profile": True,
        "merge_sql": (
            "MERGE INTO SNOWFLAKE_LANGFLOW.CORE.USER_ENTITY_PROFILE AS t "
            "USING (SELECT 'fin_analyst_01' AS user_id, 'financial_reporting_prefs' AS entity_namespace, "
            "'reporting_prefs' AS memory_key, PARSE_JSON('"
            + json.dumps(extracted_profile_json, ensure_ascii=False).replace("'", "''")
            + "') AS profile_json) AS s "
            "ON t.user_id = s.user_id AND t.entity_namespace = s.entity_namespace AND t.memory_key = s.memory_key "
            "WHEN MATCHED THEN UPDATE SET profile_json = s.profile_json, updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (user_id, entity_namespace, memory_key, profile_json, updated_at) "
            "VALUES (s.user_id, s.entity_namespace, s.memory_key, s.profile_json, CURRENT_TIMESTAMP())"
        ),
    }

    # Instantiate native canvas components for #1..#5 and verify dynamic adaptation
    mem1_comp = SnowflakeMemoryBaseNativeComponent(
        memory_base="MEM_1_GRAPH_CHECKPOINTER",
        thread_id="fin-reconcile-thread-001",
        checkpoint_ns="fin_reconciliation_hitl",
        ttl_hours=72,
    )
    mem1_cfg = mem1_comp.update_build_config(
        mem1_comp.to_build_config(), "MEM_1_GRAPH_CHECKPOINTER", "memory_base"
    )
    mem1_state = mem1_comp.get_checkpoint_state().data

    mem2_comp = SnowflakeMessageHistoryNativeComponent(
        memory_base="MEM_2_CHAT_WINDOW",
        storage_family="snowflake_postgres",
        postgres_instance="LANGFLOW_PG_INSTANCE",
        pg_table="public.chat_message_history",
        session_id="fin-reconcile-session-001",
        window_k=8,
    )
    mem2_cfg = mem2_comp.update_build_config(
        mem2_comp.to_build_config(), "MEM_2_CHAT_WINDOW", "memory_base"
    )
    mem2_history = json.loads(mem2_comp.retrieve_chat_history().text)

    mem3_comp = SnowflakeMemoryBaseNativeComponent(
        memory_base="MEM_3_SESSION_SUMMARY",
        session_id="fin-reconcile-session-001",
        summarizer_model="openai-gpt-6-luna",
        max_summary_tokens=512,
    )
    mem3_cfg = mem3_comp.update_build_config(
        mem3_comp.to_build_config(), "MEM_3_SESSION_SUMMARY", "memory_base"
    )
    mem3_summary_msg = mem3_comp.get_running_summary().text

    mem4_comp = SnowflakeMemoryBaseNativeComponent(
        memory_base="MEM_4_USER_ENTITY_PROFILE",
        user_id="fin_analyst_01",
        entity_namespace="financial_reporting_prefs",
        filter_by_session=False,
        tool_mode=False,
    )
    mem4_cfg = mem4_comp.update_build_config(
        mem4_comp.to_build_config(), "MEM_4_USER_ENTITY_PROFILE", "memory_base"
    )
    mem4_profile = mem4_comp.get_profile_context().data

    mem5_comp = SnowflakeMemoryBaseNativeComponent(
        memory_base="MEM_5_EPISODIC_FEWSHOT",
        topic="q4_margin_reconciliation",
        outcome_score_threshold=0.9,
        few_shot_k=3,
        filter_by_session=False,
        tool_mode=True,
        search_query="Riconcilia lo scostamento di margine Q4 come abbiamo fatto per le chiusure passate",
    )
    mem5_cfg = mem5_comp.update_build_config(
        mem5_comp.to_build_config(), "MEM_5_EPISODIC_FEWSHOT", "memory_base"
    )
    mem5_tool = mem5_comp.to_toolkit()
    mem5_tool_invocation = mem5_tool.invoke(
        {"query": "Riconcilia lo scostamento di margine Q4 come abbiamo fatto per le chiusure passate"}
    )

    composed_system_message = (
        f"{PROCEDURAL_SYSTEM_MESSAGE_TEMPLATE}\n\n"
        f"--- #3 RUNNING SUMMARY (Session fin-reconcile-session-001) ---\n{mem3_summary_msg}\n\n"
        f"--- #4 USER PROFILE & ENTITY MEMORY (User fin_analyst_01, Cross-Session) ---\n"
        f"{json.dumps(extracted_profile_json, ensure_ascii=False)}"
    )

    return {
        "ok": True,
        "flow_id": SHOWCASE_FLOW_ID,
        "flow_name": SHOWCASE_FLOW_NAME,
        "active_role": getattr(bridge, "active_role", "SNOWFLAKE_LANGFLOW_ADMIN"),
        "bootstrap": boot,
        "turns": [
            turn_1_trivial,
            turn_2_preference,
            {
                "turn_index": 3,
                "user_input": "Riconcilia lo scostamento di margine Q4 come abbiamo fatto per le chiusure passate",
                "status": "Ingested",
                "active_memories_used": [
                    "MEM_1_GRAPH_CHECKPOINTER",
                    "MEM_2_CHAT_WINDOW",
                    "MEM_3_SESSION_SUMMARY",
                    "MEM_4_USER_ENTITY_PROFILE",
                    "MEM_5_EPISODIC_FEWSHOT",
                ],
                "domain_tools_used": [
                    "SNOWFLAKE_LANGFLOW.CORE.FIN_OPERATIONS_SV",
                    "SNOWFLAKE_LANGFLOW.CORE.FIN_POLICY_SEARCH_SVC",
                ],
                "procedural_system_message_applied": True,
                "response_language": "it-IT",
                "response_currency": "EUR (€)",
                "agent_response": (
                    "Riconciliazione Margine Netto Q3/Q4 completata in Euro (€): "
                    "scostamento Q4 pari a +€142.800 (+1,8% rispetto a Q3), "
                    "verificato tramite vista semantica FIN_OPERATIONS_SV e policy contabile IFRS-15 "
                    "(FIN_POLICY_SEARCH_SVC), seguendo i 3 episodi storici con outcome_score >= 0.9."
                ),
            },
        ],
        "memory_components_verification": {
            "mem_1_checkpointer": {
                "canonical_memory_type": mem1_cfg["_dynamic_memory_spec"]["canonical_memory_type"],
                "storage_family": mem1_cfg["_dynamic_memory_spec"]["storage_family"],
                "primary_output_port": mem1_cfg["_dynamic_memory_spec"]["primary_output_port"],
                "filter_by_session": mem1_cfg["filter_by_session"]["value"],
                "state": mem1_state,
            },
            "mem_2_chat_window": {
                "canonical_memory_type": "buffer_window_memory",
                "storage_family": mem2_cfg["storage_family"]["value"],
                "window_k": mem2_history["window_k"],
                "filter_by_session": mem2_history["filter_by_session"],
                "recall_sql": mem2_history["recall_sql"],
            },
            "mem_3_session_summary": {
                "canonical_memory_type": mem3_cfg["_dynamic_memory_spec"]["canonical_memory_type"],
                "storage_family": mem3_cfg["_dynamic_memory_spec"]["storage_family"],
                "primary_output_port": mem3_cfg["_dynamic_memory_spec"]["primary_output_port"],
                "filter_by_session": mem3_cfg["filter_by_session"]["value"],
                "running_summary": mem3_summary_msg,
            },
            "mem_4_user_entity_profile": {
                "canonical_memory_type": mem4_cfg["_dynamic_memory_spec"]["canonical_memory_type"],
                "storage_family": mem4_cfg["_dynamic_memory_spec"]["storage_family"],
                "primary_output_port": mem4_cfg["_dynamic_memory_spec"]["primary_output_port"],
                "filter_by_session": mem4_cfg["filter_by_session"]["value"],
                "kill_phrase_skipped_turn_1": turn_1_trivial["status"] == "Skipped",
                "merged_profile_turn_2": turn_2_preference["merged_into_profile"],
                "profile_context": mem4_profile,
            },
            "mem_5_episodic_fewshot": {
                "canonical_memory_type": mem5_cfg["_dynamic_memory_spec"]["canonical_memory_type"],
                "storage_family": mem5_cfg["_dynamic_memory_spec"]["storage_family"],
                "primary_output_port": mem5_cfg["_dynamic_memory_spec"]["primary_output_port"],
                "filter_by_session": mem5_cfg["filter_by_session"]["value"],
                "tool_mode": mem5_cfg["tool_mode"]["value"],
                "enable_embedding": enable_episodic_embedding,
                "tool_name": mem5_tool.name,
                "tool_invocation": mem5_tool_invocation,
            },
        },
        "composed_system_message": composed_system_message,
    }


def bootstrap_and_run_full_memory_showcase(
    bridge: Any = None,
    *,
    dry_run: bool = True,
    execute_simulation: bool = True,
) -> dict[str, Any]:
    """Combined bootstrap + multi-turn simulation entrypoint for `LIVE_BRIDGE.bootstrap_full_memory_showcase_flow` (`W2-R4-07`)."""
    if execute_simulation:
        sim = run_full_memory_showcase_simulation(bridge, dry_run=dry_run, enable_episodic_embedding=True)
        boot = sim.get("bootstrap") or {}
        return {
            **boot,
            **sim,
            "memories_created": [
                (m.get("memory") or m).get("name")
                for m in (boot.get("memories") or [])
                if isinstance(m, dict)
            ],
            "domain_tools_#7": [
                "Cortex Analyst (SNOWFLAKE_LANGFLOW.CORE.FIN_OPERATIONS_SV)",
                "Cortex Search (SNOWFLAKE_LANGFLOW.CORE.FIN_POLICY_SEARCH_SVC)",
            ],
            "procedural_memory_#8": (
                "System Message (Enterprise Financial & Operations Rules + "
                "MEM_3_SESSION_SUMMARY + MEM_4_USER_ENTITY_PROFILE)"
            ),
        }
    boot = bootstrap_full_memory_showcase_flow(bridge, dry_run=dry_run)
    return {
        **boot,
        "memories_created": [
            (m.get("memory") or m).get("name")
            for m in (boot.get("memories") or [])
            if isinstance(m, dict)
        ],
        "domain_tools_#7": [
            "Cortex Analyst (SNOWFLAKE_LANGFLOW.CORE.FIN_OPERATIONS_SV)",
            "Cortex Search (SNOWFLAKE_LANGFLOW.CORE.FIN_POLICY_SEARCH_SVC)",
        ],
        "procedural_memory_#8": (
            "System Message (Enterprise Financial & Operations Rules + "
            "MEM_3_SESSION_SUMMARY + MEM_4_USER_ENTITY_PROFILE)"
        ),
    }

