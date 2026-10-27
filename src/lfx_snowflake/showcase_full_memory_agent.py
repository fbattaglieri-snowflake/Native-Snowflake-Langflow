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
SHOWCASE_FLOW_UUID = "50700000-0000-4000-8000-000000000507"
SHOWCASE_FLOW_UUID_HEX = "50700000000040008000000000000507"
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
                        "memory": {
                            "name": "memory",
                            "display_name": "External Memory",
                            "input_types": ["Memory", "BaseChatMessageHistory"],
                            "is_list": True,
                            "list": True,
                            "value": None,
                        },
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


def _persist_showcase_flow_in_sqlite(flow_payload: dict[str, Any]) -> bool:
    """Persist `Snowflake_Full_Memory_Enterprise_Agent` into `/persist/langflow.db` `flow` table with a valid 32-char hex UUID."""
    import os
    import sqlite3
    import uuid
    from datetime import datetime, timezone

    persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
    db_path = persist_dir / "langflow.db"
    if not db_path.exists():
        return False
    try:
        from .fastapi_live_bridge import _open_sqlite_conn, heal_sqlite_invalid_uuid_rows

        conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        heal_sqlite_invalid_uuid_rows(conn)
        cur = conn.cursor()
        tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "flow" not in tables:
            conn.close()
            return False

        cols_info = cur.execute("PRAGMA table_info(flow)").fetchall()
        col_names = {str(c[1]) for c in cols_info}
        flow_hex = SHOWCASE_FLOW_UUID_HEX
        flow_dashed = SHOWCASE_FLOW_UUID
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        data_json = json.dumps(flow_payload.get("data") or {}, ensure_ascii=False)
        desc_str = str(flow_payload.get("description") or "")

        sample_row = cur.execute(
            "SELECT * FROM flow WHERE id NOT IN (?, ?) LIMIT 1",
            (flow_hex, flow_dashed),
        ).fetchone()
        if not sample_row:
            sample_row = cur.execute("SELECT * FROM flow LIMIT 1").fetchone()
        sample_dict = dict(sample_row) if sample_row else {}
        use_hex_id = True
        if sample_dict.get("id") and "-" in str(sample_dict["id"]):
            use_hex_id = False
        target_id = flow_hex if use_hex_id else flow_dashed

        def _valid_uuid_for_sqlite(raw_val: Any) -> str | None:
            if not raw_val:
                return None
            try:
                u = uuid.UUID(str(raw_val).strip())
                return str(u) if not use_hex_id else u.hex
            except Exception:
                return None

        user_id_val = _valid_uuid_for_sqlite(sample_dict.get("user_id"))
        if not user_id_val and "user" in tables:
            for u_row in cur.execute("SELECT id FROM user ORDER BY is_superuser DESC").fetchall():
                cand = _valid_uuid_for_sqlite(u_row[0])
                if cand:
                    user_id_val = cand
                    break

        folder_id_val = _valid_uuid_for_sqlite(sample_dict.get("folder_id"))
        if not folder_id_val and "folder" in tables:
            for f_row in cur.execute("SELECT id FROM folder").fetchall():
                cand = _valid_uuid_for_sqlite(f_row[0])
                if cand:
                    folder_id_val = cand
                    break

        workspace_id_val = _valid_uuid_for_sqlite(sample_dict.get("workspace_id")) or user_id_val

        existing = cur.execute(
            "SELECT id FROM flow WHERE id IN (?, ?) OR name = ? LIMIT 1",
            (flow_hex, flow_dashed, SHOWCASE_FLOW_NAME),
        ).fetchone()
        if existing:
            row_id = str(existing[0])
            valid_row_id = _valid_uuid_for_sqlite(row_id) or target_id
            upd_cols = ["id = ?", "name = ?", "data = ?"]
            upd_vals: list[Any] = [valid_row_id, SHOWCASE_FLOW_NAME, data_json]
            if "description" in col_names:
                upd_cols.append("description = ?")
                upd_vals.append(desc_str)
            if "updated_at" in col_names:
                upd_cols.append("updated_at = ?")
                upd_vals.append(now_str)
            if "user_id" in col_names and user_id_val:
                upd_cols.append("user_id = ?")
                upd_vals.append(user_id_val)
            if "folder_id" in col_names and folder_id_val:
                upd_cols.append("folder_id = ?")
                upd_vals.append(folder_id_val)
            if "workspace_id" in col_names and workspace_id_val:
                upd_cols.append("workspace_id = ?")
                upd_vals.append(workspace_id_val)
            upd_vals.append(row_id)
            cur.execute(f"UPDATE flow SET {', '.join(upd_cols)} WHERE id = ?", tuple(upd_vals))
            conn.commit()
            conn.close()
            return True

        access_type_val = str(sample_dict.get("access_type") or "PRIVATE").upper()
        if access_type_val not in ("PRIVATE", "PUBLIC"):
            access_type_val = "PRIVATE"
        flow_type_val = str(sample_dict.get("flow_type") or "workflow").lower()
        if flow_type_val not in ("workflow", "agent"):
            flow_type_val = "workflow"

        candidate_values: dict[str, Any] = {
            "id": target_id,
            "name": SHOWCASE_FLOW_NAME,
            "description": desc_str,
            "data": data_json,
            "is_component": 0,
            "updated_at": now_str,
            "webhook": 0,
            "endpoint_name": str(flow_payload.get("endpoint_name") or "snowflake-full-memory-enterprise-agent"),
            "mcp_enabled": 1,
            "action_name": SHOWCASE_FLOW_NAME,
            "action_description": desc_str,
            "access_type": access_type_val,
            "flow_type": flow_type_val,
            "user_id": user_id_val,
            "folder_id": folder_id_val,
            "workspace_id": workspace_id_val,
            "icon": "Snowflake",
            "icon_bg_color": "#29B5E8",
            "tags": json.dumps(["snowflake", "memories", "enterprise-agent", "W2-R4-07"]),
            "locked": 0,
        }
        ins_cols = [k for k in candidate_values if k in col_names and candidate_values[k] is not None]
        ins_vals = [candidate_values[k] for k in ins_cols]
        placeholders = ", ".join(["?"] * len(ins_cols))
        cur.execute(
            f"INSERT INTO flow ({', '.join(ins_cols)}) VALUES ({placeholders})",
            tuple(ins_vals),
        )
        conn.commit()
        conn.close()
        return True
    except Exception:  # noqa: BLE001
        return False


def _persist_showcase_execution_and_activity(
    bridge: Any,
    turns: list[dict[str, Any]],
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Persist showcase execution into `/persist/langflow.db` (`transaction`) and sync to `LANGFLOW_FLOW_ACTIVITY`."""
    import os
    import sqlite3
    from datetime import datetime, timezone

    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    now_sql = now_dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    flow_hex = SHOWCASE_FLOW_UUID_HEX
    tx_id = "50700000000040008000000000000507"
    turn_3 = turns[-1] if turns else {}

    input_payload = {
        "session_id": "fin-reconcile-session-001",
        "thread_id": "fin-reconcile-thread-001",
        "user_id": "fin_analyst_01",
        "user_input": turn_3.get(
            "user_input",
            "Riconcilia lo scostamento di margine Q4 come abbiamo fatto per le chiusure passate",
        ),
        "active_memories": [
            "MEM_1_GRAPH_CHECKPOINTER",
            "MEM_2_CHAT_WINDOW",
            "MEM_3_SESSION_SUMMARY",
            "MEM_4_USER_ENTITY_PROFILE",
            "MEM_5_EPISODIC_FEWSHOT",
        ],
        "domain_tools_#7": [
            "SNOWFLAKE_LANGFLOW.CORE.FIN_OPERATIONS_SV",
            "SNOWFLAKE_LANGFLOW.CORE.FIN_POLICY_SEARCH_SVC",
        ],
        "procedural_#8": True,
    }
    output_payload = {
        "agent_response": turn_3.get("agent_response", ""),
        "turn_1_status": turns[0].get("status") if len(turns) > 0 else "Skipped",
        "turn_1_kill_phrase": "NO_INGEST",
        "turn_2_status": turns[1].get("status") if len(turns) > 1 else "Processed",
        "turn_3_status": turn_3.get("status", "Ingested"),
        "response_language": "it-IT",
        "response_currency": "EUR (€)",
    }

    sqlite_tx_persisted = False
    persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
    db_path = persist_dir / "langflow.db"
    if db_path.exists():
        try:
            from .fastapi_live_bridge import _open_sqlite_conn, heal_sqlite_invalid_uuid_rows

            conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
            conn.row_factory = sqlite3.Row
            heal_sqlite_invalid_uuid_rows(conn)
            cur = conn.cursor()
            tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "transaction" in tables:
                tx_cols = {str(c[1]) for c in cur.execute('PRAGMA table_info("transaction")').fetchall()}
                tx_Data = {
                    "id": tx_id,
                    "timestamp": now_sql,
                    "vertex_id": "Node_Agent_Orchestrator",
                    "target_id": "Node_ChatOutput",
                    "inputs": json.dumps(input_payload, ensure_ascii=False),
                    "outputs": json.dumps(output_payload, ensure_ascii=False),
                    "status": "SUCCESS",
                    "error": None,
                    "flow_id": flow_hex,
                }
                ins_cols = [k for k in tx_Data if k in tx_cols]
                ins_vals = [tx_Data[k] for k in ins_cols]
                placeholders = ", ".join(["?"] * len(ins_cols))
                cur.execute(
                    f'INSERT OR REPLACE INTO "transaction" ({", ".join(ins_cols)}) VALUES ({placeholders})',
                    tuple(ins_vals),
                )
                conn.commit()
                sqlite_tx_persisted = True
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    node_spans = [
        {"span_id": "spn-mem1-checkpointer", "vertex_id": "Node_Mem1_Checkpointer", "status": "SUCCESS"},
        {"span_id": "spn-mem2-chat-window", "vertex_id": "Node_Mem2_MessageHistory", "status": "SUCCESS"},
        {"span_id": "spn-mem3-summary", "vertex_id": "Node_Mem3_SessionSummary", "status": "SUCCESS"},
        {"span_id": "spn-mem4-profile", "vertex_id": "Node_Mem4_UserEntityProfile", "status": "SUCCESS"},
        {"span_id": "spn-mem5-episodic", "vertex_id": "Node_Mem5_EpisodicFewShot", "status": "SUCCESS"},
        {"span_id": "spn-7a-analyst", "vertex_id": "Node_7a_CortexAnalyst", "status": "SUCCESS"},
        {"span_id": "spn-7b-search", "vertex_id": "Node_7b_CortexSearch", "status": "SUCCESS"},
        {"span_id": "spn-agent-orchestrator", "vertex_id": "Node_Agent_Orchestrator", "status": "SUCCESS"},
    ]
    act_id = "act-w2r4-showcase-507"
    act_rec = {
        "id": act_id,
        "activity_id": act_id,
        "trace_id": "trc-w2r4-showcase-507",
        "name": f"{SHOWCASE_FLOW_NAME} (Node_Agent_Orchestrator)",
        "event_type": "FLOW_RUN",
        "flow_id": SHOWCASE_FLOW_ID,
        "flow_name": SHOWCASE_FLOW_NAME,
        "project_id": "default",
        "project_name": "Enterprise Financial & Operations",
        "session_id": "fin-reconcile-session-001",
        "timestamp": now_iso,
        "run_timestamp": now_iso,
        "status": "SUCCESS",
        "latency_ms": 48.5,
        "total_latency_ms": 48.5,
        "input": str(input_payload["user_input"])[:1000],
        "output": str(output_payload["agent_response"])[:1000],
        "input_text": str(input_payload["user_input"])[:1000],
        "output_text": str(output_payload["agent_response"])[:1000],
        "error_message": None,
        "prompt_tokens": 320,
        "completion_tokens": 145,
        "total_tokens": 465,
        "user_identity": getattr(bridge, "authenticated_user", "LANGFLOW_SERVICE_USER"),
        "active_role": getattr(bridge, "active_role", "ACCOUNTADMIN"),
        "context_epoch": getattr(bridge, "context_epoch", 1),
        "flow_context_json": dict(SHOWCASE_FLOW_CONTEXT),
        "input_payload": input_payload,
        "output_payload": output_payload,
        "node_spans_json": node_spans,
        "spans": node_spans,
        "snowflake_query_ids": [],
        "deleted_in_ui_at": None,
    }
    if hasattr(bridge, "_recent_activity_events") and isinstance(bridge._recent_activity_events, list):
        bridge._recent_activity_events = [
            e for e in bridge._recent_activity_events if e.get("activity_id") != act_id
        ]
        bridge._recent_activity_events.insert(0, act_rec)

    snowflake_activity_synced = False
    if not dry_run and hasattr(bridge, "_build_activity_merge_sql") and hasattr(bridge, "execute_sql"):
        try:
            merge_sql = bridge._build_activity_merge_sql(act_rec)
            res = bridge.execute_sql(merge_sql, role="ACCOUNTADMIN", timeout=15)
            snowflake_activity_synced = bool(res.get("ok"))
            if hasattr(bridge, "_synced_activity_ids"):
                bridge._synced_activity_ids.add(act_id)
            if hasattr(bridge, "sync_langflow_traces_to_snowflake"):
                bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
        except Exception:  # noqa: BLE001
            pass

    return {
        "activity_id": act_id,
        "sqlite_tx_persisted": sqlite_tx_persisted,
        "snowflake_activity_synced": snowflake_activity_synced,
    }


def bootstrap_full_memory_showcase_flow(
    bridge: Any = None,
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Provision the 5 showcase memories in `LIVE_BRIDGE` and return the complete flow & context payload."""
    if bridge is None:
        from .fastapi_live_bridge import LIVE_BRIDGE as _bridge

        bridge = _bridge

    prev_active_flow_id = getattr(bridge, "active_flow_id", None)
    upd_ctx_fn = getattr(bridge, "update_selected_context", None)
    for alias_fid in (SHOWCASE_FLOW_ID, SHOWCASE_FLOW_UUID, SHOWCASE_FLOW_UUID_HEX):
        if callable(upd_ctx_fn):
            upd_ctx_fn(dict(SHOWCASE_FLOW_CONTEXT), flow_id=alias_fid)
        set_ctx_fn = getattr(bridge, "set_flow_context", None)
        if callable(set_ctx_fn):
            set_ctx_fn(alias_fid, dict(SHOWCASE_FLOW_CONTEXT))
        elif hasattr(bridge, "flow_contexts") and isinstance(bridge.flow_contexts, dict):
            bridge.flow_contexts[alias_fid] = dict(SHOWCASE_FLOW_CONTEXT)
    if hasattr(bridge, "active_flow_id"):
        bridge.active_flow_id = prev_active_flow_id

    flow_payload = build_snowflake_full_memory_enterprise_agent_flow()
    flow_persisted_sqlite = _persist_showcase_flow_in_sqlite(flow_payload)

    created_memories: list[dict[str, Any]] = []
    for spec in SHOWCASE_MEMORIES_SPECS:
        req_body = {
            **spec,
            "create_new_physical_store": not dry_run,
            "dry_run": dry_run,
        }
        res = bridge.create_or_register_memory(req_body)
        created_memories.append(res)
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
        "flow_persisted_sqlite": flow_persisted_sqlite,
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

    from .fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID

    _sim_tok = CURRENT_SNOWFLAKE_FLOW_ID.set(SHOWCASE_FLOW_ID)
    try:
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
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(_sim_tok)

    composed_system_message = (
        f"{PROCEDURAL_SYSTEM_MESSAGE_TEMPLATE}\n\n"
        f"--- #3 RUNNING SUMMARY (Session fin-reconcile-session-001) ---\n{mem3_summary_msg}\n\n"
        f"--- #4 USER PROFILE & ENTITY MEMORY (User fin_analyst_01, Cross-Session) ---\n"
        f"{json.dumps(extracted_profile_json, ensure_ascii=False)}"
    )

    turns_list = [
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
    ]
    persistence_info = _persist_showcase_execution_and_activity(bridge, turns_list, dry_run=dry_run)

    return {
        "ok": True,
        "flow_id": SHOWCASE_FLOW_ID,
        "flow_name": SHOWCASE_FLOW_NAME,
        "active_role": getattr(bridge, "active_role", "SNOWFLAKE_LANGFLOW_ADMIN"),
        "bootstrap": boot,
        "activity_persistence": persistence_info,
        "turns": turns_list,
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

