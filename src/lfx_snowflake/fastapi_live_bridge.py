"""FastAPI live bridge mounting `/api/v1/snowflake/*` inside Langflow and connecting to Snowflake SQL via SPCS OAuth."""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import os
import logging
import re
import sqlite3
import threading
import time
import uuid
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CATALOG_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="sflf-catalog")
_AUDIT_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="sflf-audit")

os.environ.setdefault("LANGFLOW_A2A_ENABLED", "true")

CURRENT_SNOWFLAKE_FLOW_ID: ContextVar[str | None] = ContextVar("current_snowflake_flow_id", default=None)
# BUG-TRG-04: role pinned for an unattended trigger run (`/service-trigger`): the role persisted on the
# flow's trigger at creation. While set it is the effective `active_role`, above any user/flow session.
TRIGGER_RUN_ROLE: ContextVar[str | None] = ContextVar("trigger_run_role", default=None)

from lfx_snowflake import traceability as _trace  # noqa: E402
from lfx_snowflake.traceability import (  # noqa: E402
    CURRENT_REQUEST_ID,
    CURRENT_RUN_ID,
    DEPRECATED_ACTIONS as DEPRECATED_AUDIT_ACTIONS,
    AuditAction,
    build_audit_insert,
    build_query_tag,
    deterministic_event_id,
    emit as emit_audit,
    sanitize_details,
)

CANONICAL_TECHNICAL_MEMORY_TABLES: frozenset[str] = frozenset(
    {
        "MEM_GRAPH_STATE_CHECKPOINTS",
        "MEM_CHAT_WINDOW_HISTORY",
        "MEM_RUNNING_SUMMARIES",
        "MEM_USER_ENTITY_PROFILES",
        "MEM_EPISODIC_EXPERIENCES",
        "AGENT_CHECKPOINTS",
        "CHAT_WINDOW_HISTORY",
        "CHAT_SESSION_SUMMARY",
        "USER_ENTITY_PROFILE",
        "AGENT_EPISODIC_MEMORY",
        "CHAT_MESSAGE_HISTORY",
        "LANGGRAPH_CHECKPOINTS",
    }
)


def get_current_snowflake_flow_id() -> str | None:
    """Return the active Snowflake flow_id bound to the current request ContextVar (`CTX-ARCH-03`)."""
    return CURRENT_SNOWFLAKE_FLOW_ID.get()


def _resolve_component_flow_id(
    component: Any = None,
    build_config: dict[str, Any] | None = None,
) -> str | None:
    """Resolve the isolated flow_id for any Langflow component from build_config, component attributes, or ContextVar (`CTX-ARCH-03`, `CTX-ARCH-05`)."""
    raw_fid: Any = None
    if isinstance(build_config, dict):
        fn_val = build_config.get("_frontend_node_flow_id")
        if isinstance(fn_val, dict):
            raw_fid = fn_val.get("value") or fn_val.get("flow_id")
        elif fn_val:
            raw_fid = fn_val
        if not raw_fid:
            fid_val = build_config.get("flow_id")
            if isinstance(fid_val, dict):
                raw_fid = fid_val.get("value")
            elif fid_val:
                raw_fid = fid_val
    if not raw_fid and component is not None:
        raw_fid = (
            getattr(component, "flow_id", None)
            or getattr(component, "_flow_id", None)
            or getattr(getattr(component, "graph", None), "flow_id", None)
            or getattr(getattr(component, "_vertex", None), "flow_id", None)
        )
    if not raw_fid:
        raw_fid = CURRENT_SNOWFLAKE_FLOW_ID.get()
    return LiveSnowflakeBridge._normalize_flow_id(str(raw_fid)) if raw_fid else None


resolve_component_flow_id = _resolve_component_flow_id

try:
    import httpx
except ImportError:  # pragma: no cover
    httpx = None  # type: ignore[assignment]

try:
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse
except ImportError:  # pragma: no cover
    APIRouter = Any  # type: ignore[assignment,misc]
    Request = Any  # type: ignore[assignment,misc]
    JSONResponse = Any  # type: ignore[assignment,misc]

from .bundle_manifest import (
    ERROR_TAXONOMY_CONTRACT,
    INVARIANT_CATEGORIES_MANIFEST,
    NEW_FLOW_IMPORT_MODES,
    SETTINGS_CONTRACT,
    SNOWFLAKE_BRANDING_CONTRACT,
    get_bundle_manifest,
)
from .governance.models import (
    ExecutionIdentityType as ExecutionMode,
    SnowflakeErrorCode as ErrorCode,
)
from .observability.redaction import redact_payload_for_sink as redact_for_sink
from . import identity as _sf_identity
from . import sql_safety as _sql_safety
from . import flow_context_guard as _flow_guard
from . import a2a_call_chain as _a2a_chain
from . import telemetry as _telemetry


def _show_agents_sql(db: Any, sch: Any) -> str | None:
    """Official ``SHOW AGENTS IN SCHEMA "<DB>"."<SCH>"`` (BUG-CA-10); ``None`` for invalid identifiers."""
    try:
        return f"SHOW AGENTS IN SCHEMA {_sql_safety.quote_identifier(db, 'database')}.{_sql_safety.quote_identifier(sch, 'schema')}"
    except _sql_safety.SqlSafetyError:
        return None

_STAGE_CLOUD_PROVIDERS = ("S3", "S3GOV", "S3CHINA", "GCS", "AZURE", "S3COMPAT")
_VOLUME_STORAGE_PROVIDERS = ("S3", "S3GOV", "GCS", "AZURE", "S3COMPAT")
_EXTERNAL_ENCRYPTION_TYPES = ("NONE", "AWS_SSE_S3", "AWS_SSE_KMS", "AWS_CSE", "GCS_SSE_KMS", "AZURE_CSE")
_MEMORY_TARGET_LAG_RE = re.compile(r"^(DOWNSTREAM|\d{1,5}\s+(seconds?|minutes?|hours?|days?))$", re.IGNORECASE)
from .providers.registry import SnowflakeProviderError

SPCS_TOKEN_PATH = Path("/snowflake/session/token")

LEGACY_MEMORY_TYPE_ALIASES: dict[str, str] = {
    "episodic_session": "buffer_window_memory",
    "semantic_vector": "episodic_memory",
    "structured_long_term": "user_profile_entity_memory",
}

EXTERNAL_EMBEDDING_PROVIDERS: dict[str, dict[str, Any]] = {
    "snowflake_cortex": {
        "id": "snowflake_cortex",
        "label": "1. Snowflake Cortex Native (AI_EMBED / AI_MULTI_EMBED)",
        "is_snowflake_native": True,
        # Derived from sql_safety (single AI_EMBED source of truth); Marengo is routed only via AI_MULTI_EMBED.
        "models": [
            *({"model_id": _m, "dimension": _d, "function": "AI_EMBED"} for _m, _d in _sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS.items()),
            {"model_id": "twelvelabs-marengo-embed-3-0", "dimension": 512, "function": "AI_MULTI_EMBED"},
        ],
        "dimensions": {
            **_sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS,
            "twelvelabs-marengo-embed-3-0": 512,
        },
    },
    "spcs_model_registry": {
        "id": "spcs_model_registry",
        "label": "2. Snowflake Model Registry (SPCS Compute Pool, 1..4096d)",
        "is_snowflake_native": True,
        "models": [
            {"model_id": "bge-large-en-v1.5-spcs", "dimension": 1024},
            {"model_id": "e5-mistral-7b-instruct-spcs", "dimension": 4096},
            {"model_id": "custom-registry-embedding", "dimension": 768},
        ],
        "dimensions": {
            "bge-large-en-v1.5-spcs": 1024,
            "e5-mistral-7b-instruct-spcs": 4096,
            "custom-registry-embedding": 768,
        },
        "supports_custom_dimension": True,
        "min_dimension": 1,
        "max_dimension": 4096,
    },
    "spcs_ollama": {
        "id": "spcs_ollama",
        "label": "3. Private - Ollama (SPCS Container on Compute Pool)",
        "is_snowflake_native": True,
        "models": [
            {"model_id": "nomic-embed-text", "dimension": 768},
            {"model_id": "bge-large-en-v1.5", "dimension": 1024},
            {"model_id": "mxbai-embed-large", "dimension": 1024},
        ],
        "dimensions": {
            "nomic-embed-text": 768,
            "bge-large-en-v1.5": 1024,
            "mxbai-embed-large": 1024,
        },
        "supports_custom_dimension": True,
        "min_dimension": 1,
        "max_dimension": 4096,
    },
    "spcs_llm_proxy": {
        "id": "spcs_llm_proxy",
        "label": "4. Snowflake Standard Proxy (Proxy - Chat / Responses / Messages)",
        "is_snowflake_native": True,
        "models": [
            {"model_id": "openai-gpt-6-luna-embed", "dimension": 1536},
            {"model_id": "snowflake-arctic-embed-l-v2.0", "dimension": 1024},
            {"model_id": "proxy-custom-embedding", "dimension": 768},
        ],
        "dimensions": {
            "openai-gpt-6-luna-embed": 1536,
            "snowflake-arctic-embed-l-v2.0": 1024,
            "proxy-custom-embedding": 768,
        },
        "supports_custom_dimension": True,
        "min_dimension": 1,
        "max_dimension": 4096,
    },
    "openai": {
        "id": "openai",
        "label": "OpenAI Embeddings (External)",
        "is_snowflake_native": False,
        "models": [
            {"model_id": "text-embedding-3-small", "dimension": 1536},
            {"model_id": "text-embedding-3-large", "dimension": 3072},
            {"model_id": "text-embedding-ada-002", "dimension": 1536},
        ],
        "dimensions": {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        },
    },
    "cohere": {
        "id": "cohere",
        "label": "Cohere Embeddings (External)",
        "is_snowflake_native": False,
        "models": [
            {"model_id": "embed-english-v3.0", "dimension": 1024},
            {"model_id": "embed-multilingual-v3.0", "dimension": 1024},
        ],
        "dimensions": {
            "embed-english-v3.0": 1024,
            "embed-multilingual-v3.0": 1024,
        },
    },
    "huggingface_ollama": {
        "id": "huggingface_ollama",
        "label": "HuggingFace / Ollama Embeddings",
        "is_snowflake_native": False,
        "models": [
            {"model_id": "nomic-embed-text", "dimension": 768},
            {"model_id": "bge-large-en-v1.5", "dimension": 1024},
        ],
        "dimensions": {
            "nomic-embed-text": 768,
            "bge-large-en-v1.5": 1024,
        },
    },
    "custom_endpoint": {
        "id": "custom_endpoint",
        "label": "Custom Embedding Endpoint",
        "is_snowflake_native": False,
        "models": [
            {"model_id": "custom-embedding-endpoint", "dimension": 768},
        ],
        "dimensions": {
            "custom-embedding-endpoint": 768,
        },
        "supports_custom_dimension": True,
        "min_dimension": 1,
        "max_dimension": 4096,
    },
}

SNOWFLAKE_EMBEDDING_SOURCES: tuple[dict[str, Any], ...] = (
    EXTERNAL_EMBEDDING_PROVIDERS["snowflake_cortex"],
    EXTERNAL_EMBEDDING_PROVIDERS["spcs_model_registry"],
    EXTERNAL_EMBEDDING_PROVIDERS["spcs_ollama"],
    EXTERNAL_EMBEDDING_PROVIDERS["spcs_llm_proxy"],
)

LLM_PREPROCESSING_ENGINES: tuple[dict[str, Any], ...] = (
    {
        "id": "cortex_sql",
        "label": "Cortex SQL — AI_COMPLETE (Select Model)",
        "sql_function": "AI_COMPLETE",
        "requires_model_selection": True,
        "default_model": "mistral-large2",
        "models": [
            "mistral-large2",
            "llama3.3-70b",
            "llama3.1-70b",
            "claude-3-7-sonnet",
            "claude-sonnet-4-5",
            "openai-gpt-5",
            "deepseek-r1",
        ],
    },
    {
        "id": "cortex_summarize_agg",
        "label": "Cortex Native — AI_SUMMARIZE_AGG (Automatic SQL Aggregation)",
        "sql_function": "AI_SUMMARIZE_AGG",
        "requires_model_selection": False,
        "default_model": "AI_SUMMARIZE_AGG",
        "models": ["AI_SUMMARIZE_AGG"],
    },
    {
        "id": "proxy_chat",
        "label": "SPCS LLM Proxy — Chat (/chat/v1)",
        "default_model": "openai-gpt-6-luna",
        "models": ["openai-gpt-6-luna", "openai-gpt-6-sol", "grok-4.7", "glm-5.3", "kimi-k3"],
    },
    {
        "id": "proxy_responses",
        "label": "SPCS LLM Proxy — Responses (/responses/v1)",
        "default_model": "openai-gpt-6-astra",
        "models": ["openai-gpt-6-astra", "openai-gpt-6.1-sol"],
    },
    {
        "id": "proxy_messages",
        "label": "SPCS LLM Proxy — Messages (/messages)",
        "default_model": "claude-sonnet-5-5",
        "models": ["claude-sonnet-5-5", "claude-opus-5-5", "claude-haiku-5-5"],
    },
    {
        "id": "spcs_ollama",
        "label": "Private SPCS — Ollama (SPCS Container)",
        "default_model": "llama3.1:8b",
        "models": ["llama3.1:8b", "qwen2.5:14b", "mistral:7b"],
    },
    {
        "id": "spcs_model_registry",
        "label": "Snowflake Model Registry (SPCS Service)",
        "default_model": "custom-summarizer-spcs",
        "models": ["custom-summarizer-spcs"],
    },
)

EMBEDDING_PROVIDERS_CATALOG: tuple[dict[str, Any], ...] = tuple(EXTERNAL_EMBEDDING_PROVIDERS.values())

_PREPROCESSING_ENGINE_PROTOCOLS: dict[str, str] = {
    "proxy_chat": "chat",
    "proxy_responses": "responses",
    "proxy_messages": "messages",
    "spcs_ollama": "ollama",
}


def get_llm_preprocessing_engines() -> list[dict[str, Any]]:
    """Return the preprocessing engines with proxy/Ollama models taken from the live proxy catalog.

    The static model lists in `LLM_PREPROCESSING_ENGINES` are only kept for the case where the
    catalog cannot be read, so a model promoted by the proxy is offered here without a release.
    """
    try:
        from lfx_snowflake.providers.language_models import get_snowflake_live_language_model_catalog

        live_options = get_snowflake_live_language_model_catalog(
            protocols=tuple(_PREPROCESSING_ENGINE_PROTOCOLS.values())
        )
    except Exception:  # noqa: BLE001
        live_options = []
    models_by_protocol: dict[str, list[str]] = {}
    for option in live_options:
        protocol = str((option.get("metadata") or {}).get("protocol") or option.get("protocol") or "")
        name = str(option.get("name") or option.get("model_name") or "")
        if protocol and name and name not in models_by_protocol.setdefault(protocol, []):
            models_by_protocol[protocol].append(name)
    engines: list[dict[str, Any]] = []
    for engine in LLM_PREPROCESSING_ENGINES:
        entry = dict(engine)
        live_models = models_by_protocol.get(_PREPROCESSING_ENGINE_PROTOCOLS.get(str(entry.get("id")), ""))
        if live_models:
            entry["models"] = list(live_models)
            if entry.get("default_model") not in live_models:
                entry["default_model"] = live_models[0]
        engines.append(entry)
    return engines


def compute_deterministic_memory_uuid(name: str | None) -> str:
    """Compute a deterministic UUIDv5 (`snowflake-memory:<NAME>`) so memory IDs stay stable across restarts and match between SQLite and `memories_registry` (`W2-R4-08G`)."""
    clean_name = str(name or "SNOWFLAKE_MEMORY").strip().upper() or "SNOWFLAKE_MEMORY"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"snowflake-memory:{clean_name}"))


AGENT_MEMORY_TYPES: tuple[dict[str, Any], ...] = (
    {
        "id": "graph_state_checkpointer",
        "agent_memory_type": "graph_state_checkpointer",
        "slot_number": 1,
        "enabled": True,
        "label": "#1 Short-Term: Graph State & Checkpointer",
        "category": "short_term",
        "purpose": (
            "Saves step-by-step LangGraph execution state, intermediate tool calls, and "
            "Human-in-the-Loop (HITL) pauses retrieved by primary key thread_id."
        ),
        "description": (
            "Short-term LangGraph execution state and step checkpoints retrieved by primary key thread_id "
            "backed by Snowflake Postgres (PostgresSaver), Hybrid Table, or SnowflakeCheckpointer."
        ),
        "recommended_snowflake_objects": [
            "Snowflake Postgres (PostgresSaver)",
            "Hybrid Table",
            "SnowflakeCheckpointer",
        ],
        "supported_backends": [
            "Snowflake Postgres (PostgresSaver)",
            "Hybrid Table",
            "SnowflakeCheckpointer",
            "Snowflake Postgres (Relational / JSONB)",
            "Standard Table",
        ],
        "needs_embedding_badge": "NO (Never)",
        "how_to_feed_without_embedding": (
            "SELECT state_blob, checkpoint_ns, step_id FROM <fqn> "
            "WHERE thread_id = :thread_id ORDER BY step_id DESC LIMIT 1"
        ),
        "when_embedding_useful": "Never: graph execution state is always retrieved deterministically by primary key thread_id.",
        "required_inputs": ["thread_id", "checkpoint_ns", "ttl_hours"],
        "default_hybrid_pk": "(thread_id, checkpoint_ns, step_id)",
        "filter_by_session": True,
        "embedding_locked_off": True,
        "default_llm_preprocessing": False,
        "default_kill_phrase": "NO_INGEST",
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "buffer_window_memory",
        "agent_memory_type": "buffer_window_memory",
        "slot_number": 2,
        "enabled": True,
        "legacy_alias": "episodic_session",
        "label": "#2 Short-Term: Buffer / Window Memory (Chat History)",
        "category": "short_term",
        "purpose": (
            "Keeps the last K verbatim messages of the active conversation ordered by timestamp "
            "and filtered by session_id to feed the Agent's Chat History port."
        ),
        "description": (
            "Low-latency conversational chat history window retrieved chronologically by session_id and window_k "
            "backed by Snowflake Postgres (Relational / JSONB), Hybrid Table, or Standard Table."
        ),
        "recommended_snowflake_objects": [
            "Snowflake Postgres (Relational / JSONB)",
            "Hybrid Table",
            "Standard Table",
        ],
        "supported_backends": [
            "Snowflake Postgres (Relational / JSONB)",
            "Hybrid Table",
            "Standard Table",
        ],
        "needs_embedding_badge": "NO",
        "how_to_feed_without_embedding": (
            "SELECT sender, message_text, created_at FROM <fqn> "
            "WHERE session_id = :session_id ORDER BY created_at DESC LIMIT :window_k"
        ),
        "when_embedding_useful": "Only useful when retrieving messages from 500+ turns ago outside the sliding window K.",
        "required_inputs": ["session_id", "window_k", "retention_days"],
        "default_hybrid_pk": "(session_id, turn_id)",
        "filter_by_session": True,
        "embedding_locked_off": False,
        "default_llm_preprocessing": False,
        "default_kill_phrase": "NO_INGEST",
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "summary_memory",
        "agent_memory_type": "summary_memory",
        "slot_number": 3,
        "enabled": True,
        "label": "#3 Short-Term: Running Summary Memory (Conversation Summary)",
        "category": "short_term",
        "purpose": (
            "Condenses all turns prior to window K into a progressive running summary and injects it into "
            "the Agent's System Message, updating it in the background with AI_COMPLETE or AI_SUMMARIZE_AGG."
        ),
        "description": (
            "Running conversation summary injected into the System Prompt and updated in background "
            "with AI_COMPLETE / AI_SUMMARIZE_AGG."
        ),
        "recommended_snowflake_objects": [
            "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG",
            "Standard Table + AI_COMPLETE",
            "Snowflake Postgres",
        ],
        "supported_backends": [
            "Hybrid Table + AI_COMPLETE / AI_SUMMARIZE_AGG",
            "Standard Table + AI_COMPLETE",
            "Snowflake Postgres",
            "Hybrid Table",
            "Standard Table",
        ],
        "needs_embedding_badge": "NO (Never)",
        "how_to_feed_without_embedding": (
            "SELECT running_summary, last_turn_id FROM <fqn> "
            "WHERE session_id = :session_id LIMIT 1 -- injected into System Prompt"
        ),
        "when_embedding_useful": (
            "Never: it is a progressive 1-line summary read directly and updated in the background with "
            "AI_COMPLETE / AI_SUMMARIZE_AGG."
        ),
        "required_inputs": ["session_id", "summarizer_model", "max_summary_tokens"],
        "default_hybrid_pk": "(session_id)",
        "filter_by_session": True,
        "embedding_locked_off": True,
        "default_llm_preprocessing": True,
        "default_kill_phrase": "NO_INGEST",
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "user_profile_entity_memory",
        "agent_memory_type": "user_profile_entity_memory",
        "slot_number": 4,
        "enabled": True,
        "legacy_alias": "structured_long_term",
        "label": "#4 Long-Term Semantic: User Profile & Entity Memory",
        "category": "long_term_semantic",
        "purpose": (
            "Persists cross-session user preferences, structured facts, and JSON entities "
            "by user_id + entity_namespace with LLM distillation and Kill Phrase NO_INGEST."
        ),
        "description": (
            "Persistent user preferences, entity facts, and structured JSON profiles loaded at session start "
            "backed by Snowflake Postgres (PostgresStore) or Hybrid/Standard Table (VARIANT)."
        ),
        "recommended_snowflake_objects": [
            "Snowflake Postgres (PostgresStore)",
            "Hybrid Table (VARIANT)",
            "Standard Table (VARIANT)",
        ],
        "supported_backends": [
            "Snowflake Postgres (PostgresStore)",
            "Hybrid Table (VARIANT)",
            "Standard Table (VARIANT)",
            "Semi-Structured (VARIANT)",
            "Semi-Structured (OBJECT)",
            "Semi-Structured (ARRAY)",
            "Standard Table",
            "Dynamic Table",
            "Interactive Table",
            "View",
        ],
        "needs_embedding_badge": "OPTIONAL (Default OFF)",
        "how_to_feed_without_embedding": (
            "SELECT profile_json FROM <fqn> WHERE user_id = :user_id "
            "AND entity_namespace = :entity_namespace LIMIT 1 -- loaded at session start"
        ),
        "when_embedding_useful": "Only when a user accumulates hundreds of distinct facts that no longer fit in the system prompt.",
        "required_inputs": ["user_id", "entity_namespace", "schema_keys"],
        "default_hybrid_pk": "(user_id, entity_namespace, chunk_id)",
        "filter_by_session": False,
        "embedding_locked_off": False,
        "default_llm_preprocessing": False,
        "default_kill_phrase": "NO_INGEST",
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "episodic_memory",
        "agent_memory_type": "episodic_memory",
        "slot_number": 5,
        "enabled": True,
        "legacy_alias": "semantic_vector",
        "label": "#5 Long-Term Semantic: Episodic / Procedural Memory (Few-Shot Experiences)",
        "category": "long_term_semantic",
        "purpose": (
            "Stores past tasks successfully solved by the agent (with outcome_score and trajectory) "
            "to supply dynamic few-shot examples or an on-demand Agent Tool."
        ),
        "description": (
            "Past agent task experiences and few-shot examples retrieved by topic/outcome_score or "
            "optional semantic vector similarity (Snowflake AI_EMBED or External Embedding Provider)."
        ),
        "recommended_snowflake_objects": [
            "Hybrid Table",
            "Standard Table",
            "Snowflake Postgres",
        ],
        "supported_backends": [
            "Hybrid Table",
            "Standard Table",
            "Snowflake Postgres",
            "Standard Table (VECTOR)",
            "Hybrid Table (VECTOR)",
            "Interactive Table (VECTOR)",
            "Dynamic Table (VECTOR)",
            "Snowflake Postgres (pgvector)",
            "Cortex Search Service",
        ],
        "needs_embedding_badge": "OPTIONAL (Rarely Needed)",
        "how_to_feed_without_embedding": (
            "SELECT topic, tool_used, outcome_score, episode_text FROM <fqn> "
            "WHERE topic = :topic ORDER BY outcome_score DESC, created_at DESC LIMIT :few_shot_k"
        ),
        "when_embedding_useful": (
            "Useful when past tasks do not follow fixed topic categories and semantic similarity search is needed to find similar episodes."
        ),
        "required_inputs": ["topic_column", "tool_used_column", "outcome_score_threshold", "few_shot_k"],
        "default_hybrid_pk": "(user_id, chunk_id)",
        "filter_by_session": False,
        "embedding_locked_off": False,
        "default_llm_preprocessing": False,
        "default_kill_phrase": "NO_INGEST",
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
)

COMING_SOON_MEMORY_TYPES: tuple[dict[str, Any], ...] = (
    {
        "id": "business_ontology_cortex_sense",
        "agent_memory_type": "business_ontology_cortex_sense",
        "slot_number": 6,
        "enabled": False,
        "badge": "Coming Soon",
        "label": "#6 Long-Term: Business Ontology / Cortex Sense (Coming Soon)",
        "category": "long_term_ontology",
        "purpose": (
            "Governed ontological memory backed by Snowflake Business Ontology and Cortex Sense for "
            "canonical domain definitions, entity relationships, and metric formulas (Coming Soon)."
        ),
        "description": (
            "Future slot reserved for Snowflake Business Ontology and Cortex Sense domain memory "
            "(disabled in the current release)."
        ),
        "recommended_snowflake_objects": [
            "Business Ontology",
            "Cortex Sense Context",
            "Semantic View",
        ],
        "supported_backends": [
            "Business Ontology",
            "Cortex Sense Context",
        ],
        "needs_embedding_badge": "COMING SOON (Cortex Sense)",
        "how_to_feed_without_embedding": "-- Coming Soon: Snowflake Business Ontology / Cortex Sense",
        "when_embedding_useful": "Managed internally by Cortex Sense when enabled.",
        "required_inputs": ["ontology_domain", "cortex_context_name"],
        "default_hybrid_pk": "(entity_id)",
        "filter_by_session": False,
        "embedding_locked_off": True,
        "default_llm_preprocessing": False,
        "default_kill_phrase": "NO_INGEST",
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
)

DROPDOWN_MEMORY_TYPES: tuple[dict[str, Any], ...] = AGENT_MEMORY_TYPES + COMING_SOON_MEMORY_TYPES

CANONICAL_MEMORY_TYPES_BY_ID: dict[str, dict[str, Any]] = {
    m["agent_memory_type"]: m for m in AGENT_MEMORY_TYPES
}


def resolve_canonical_memory_type(raw_type: str | None) -> str:
    """Resolve any canonical or legacy memory type identifier into one of the 5 canonical types."""
    cleaned = str(raw_type or "").strip().lower()
    if cleaned in CANONICAL_MEMORY_TYPES_BY_ID:
        return cleaned
    if cleaned in LEGACY_MEMORY_TYPE_ALIASES:
        return LEGACY_MEMORY_TYPE_ALIASES[cleaned]
    return "episodic_memory"


def resolve_memory_embedding_dimension(
    embedding_model: str,
    *,
    embedding_provider: str = "snowflake_cortex",
    custom_dimension: int | None = None,
    default: int = 768,
) -> int:
    """Resolve embedding dimension across the 4 Snowflake embedding sources and external Langflow providers."""
    from .providers.embeddings import resolve_embedding_dimension

    prov = (embedding_provider or "snowflake_cortex").strip().lower()
    clean_model = (embedding_model or "").strip().split(" ")[0].replace("ollama/", "")
    if prov in ("custom_endpoint", "spcs_model_registry", "spcs_llm_proxy") and custom_dimension is not None:
        return int(custom_dimension)
    if prov in EXTERNAL_EMBEDDING_PROVIDERS:
        dims_map = EXTERNAL_EMBEDDING_PROVIDERS[prov].get("dimensions", {})
        if clean_model in dims_map:
            return int(dims_map[clean_model])
    for p_entry in EXTERNAL_EMBEDDING_PROVIDERS.values():
        dims_map = p_entry.get("dimensions", {})
        if clean_model in dims_map:
            return int(dims_map[clean_model])
    if custom_dimension is not None:
        return int(custom_dimension)
    return resolve_embedding_dimension(clean_model, default=default)


def _open_sqlite_conn(
    db_path: str | Path,
    *,
    readonly: bool = False,
    timeout: float = 30.0,
) -> sqlite3.Connection:
    """Open a SQLite connection with busy_timeout=30000 and WAL mode (when writable) to prevent `database is locked`."""
    if readonly:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=timeout)
        conn.execute("PRAGMA busy_timeout=30000;")
        return conn
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _normalize_sqlite_uuid_val(raw_val: Any, *, use_hex: bool = True) -> str | None:
    """Return a valid UUID string (32-char hex or 36-char dashed) or None if `raw_val` is not a valid UUID."""
    s = str(raw_val or "").strip()
    if not s:
        return None
    if s in (
        "snowflake-full-memory-enterprise-agent-001",
        "snowflakefullmemoryenterpriseagent001",
    ):
        s = "50700000-0000-4000-8000-000000000507"
    try:
        u = uuid.UUID(s)
        return u.hex if use_hex else str(u)
    except Exception:  # noqa: BLE001
        return None


def heal_sqlite_invalid_uuid_rows(conn: Any) -> int:
    """Self-heal any non-hexadecimal UUID values in SQLite tables (`flow`, `transaction`, `memory_base`, `knowledge_base`, `message`, `trace`) so SQLAlchemy `Uuid` processors never raise `ValueError: badly formed hexadecimal UUID string`."""
    repaired = 0
    try:
        cur = conn.cursor()
        tables = {
            str(r[0]) for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        use_hex = True
        if "flow" in tables:
            for r in cur.execute("SELECT id FROM flow LIMIT 20").fetchall():
                rid = str(r[0] or "").strip()
                try:
                    uuid.UUID(rid)
                    if "-" in rid:
                        use_hex = False
                    break
                except Exception:  # noqa: BLE001
                    continue

        showcase_uuid = "50700000000040008000000000000507" if use_hex else "50700000-0000-4000-8000-000000000507"
        seed_showcase = os.getenv("SNOWFLAKE_SEED_SHOWCASE_FLOW", "").strip().lower() in ("1", "true", "yes")
        explicitly_bootstrapped = bool(
            globals().get("LIVE_BRIDGE") is not None
            and getattr(globals().get("LIVE_BRIDGE"), "_showcase_explicitly_bootstrapped", False)
        )
        valid_user_id: str | None = None
        if "user" in tables:
            for ur in cur.execute("SELECT id FROM user ORDER BY is_superuser DESC").fetchall():
                cand = _normalize_sqlite_uuid_val(ur[0], use_hex=use_hex)
                if cand:
                    valid_user_id = cand
                    break
        valid_folder_id: str | None = None
        if "folder" in tables:
            for fr in cur.execute("SELECT id FROM folder").fetchall():
                cand = _normalize_sqlite_uuid_val(fr[0], use_hex=use_hex)
                if cand:
                    valid_folder_id = cand
                    break

        if "flow" in tables:
            # W2-R9-06 (NP-01): Purge unsolicited New Flow (Backup)* and auto-seeded showcase flow
            # unless SNOWFLAKE_SEED_SHOWCASE_FLOW=true or explicitly requested, while preserving 100% of user flows.
            cur.execute(
                "DELETE FROM flow WHERE name = 'New Flow (Backup)' OR name LIKE 'New Flow (Backup)%' OR name LIKE '% (Backup)'"
            )
            if cur.rowcount and cur.rowcount > 0:
                repaired += int(cur.rowcount)
            if not seed_showcase and not explicitly_bootstrapped:
                cur.execute(
                    "DELETE FROM flow WHERE id IN (?, ?, ?, ?) OR name = ?",
                    (
                        "50700000000040008000000000000507",
                        "50700000-0000-4000-8000-000000000507",
                        "f0000000000040008000000000000507",
                        "f0000000-0000-4000-8000-000000000507",
                        "Snowflake_Full_Memory_Enterprise_Agent",
                    ),
                )
                if cur.rowcount and cur.rowcount > 0:
                    repaired += int(cur.rowcount)
                if "transaction" in tables:
                    cur.execute(
                        'DELETE FROM "transaction" WHERE id IN (?, ?) OR flow_id IN (?, ?, ?, ?)',
                        (
                            "50700000000040008000000000000507",
                            "50700000-0000-4000-8000-000000000507",
                            "50700000000040008000000000000507",
                            "50700000-0000-4000-8000-000000000507",
                            "f0000000000040008000000000000507",
                            "f0000000-0000-4000-8000-000000000507",
                        ),
                    )

            flow_cols = {str(c[1]) for c in cur.execute("PRAGMA table_info(flow)").fetchall()}
            select_cols = ["rowid", "id"] + [
                c
                for c in ("name", "user_id", "folder_id", "workspace_id", "access_type", "flow_type")
                if c in flow_cols
            ]
            rows = cur.execute(f"SELECT {', '.join(select_cols)} FROM flow").fetchall()
            existing_ids = {str(r[1]) for r in rows}
            for r in rows:
                row_map = dict(zip(select_cols, r))
                rowid = row_map["rowid"]
                raw_id = str(row_map.get("id") or "")
                norm_id = _normalize_sqlite_uuid_val(raw_id, use_hex=use_hex)
                if norm_id is None:
                    if (seed_showcase or explicitly_bootstrapped) and str(row_map.get("name") or "") == "Snowflake_Full_Memory_Enterprise_Agent":
                        norm_id = showcase_uuid
                    else:
                        norm_id = (
                            uuid.uuid5(uuid.NAMESPACE_DNS, raw_id or f"flow-{rowid}").hex
                            if use_hex
                            else str(uuid.uuid5(uuid.NAMESPACE_DNS, raw_id or f"flow-{rowid}"))
                        )
                if norm_id != raw_id:
                    if norm_id in existing_ids:
                        cur.execute("DELETE FROM flow WHERE rowid = ?", (rowid,))
                        repaired += 1
                        continue
                    cur.execute("UPDATE flow SET id = ? WHERE rowid = ?", (norm_id, rowid))
                    existing_ids.add(norm_id)
                    repaired += 1

                updates: list[str] = []
                vals: list[Any] = []
                for ucol, fallback in (
                    ("user_id", valid_user_id),
                    ("folder_id", valid_folder_id),
                    ("workspace_id", valid_user_id),
                ):
                    if ucol in row_map and row_map[ucol] is not None:
                        if _normalize_sqlite_uuid_val(row_map[ucol], use_hex=use_hex) is None:
                            updates.append(f"{ucol} = ?")
                            vals.append(fallback)
                if "access_type" in row_map:
                    at = str(row_map.get("access_type") or "").upper()
                    if at not in ("PRIVATE", "PUBLIC"):
                        updates.append("access_type = ?")
                        vals.append("PRIVATE")
                if "flow_type" in row_map:
                    ft = str(row_map.get("flow_type") or "").lower()
                    if ft not in ("workflow", "agent"):
                        updates.append("flow_type = ?")
                        vals.append("workflow")
                if updates:
                    vals.append(rowid)
                    cur.execute(f"UPDATE flow SET {', '.join(updates)} WHERE rowid = ?", tuple(vals))
                    repaired += 1

        table_uuid_cols: list[tuple[str, tuple[str, ...]]] = [
            ("transaction", ("id", "flow_id")),
            ("memory_base", ("id", "flow_id", "user_id")),
            ("knowledge_base", ("id", "user_id")),
            ("message", ("id", "flow_id", "run_id", "user_id")),
            ("trace", ("id", "flow_id")),
            ("span", ("id", "trace_id", "parent_span_id")),
        ]
        for tbl_name, ucols in table_uuid_cols:
            if tbl_name not in tables:
                continue
            existing_tcols = {
                str(c[1]) for c in cur.execute(f'PRAGMA table_info("{tbl_name}")').fetchall()
            }
            active_ucols = [c for c in ucols if c in existing_tcols]
            if not active_ucols:
                continue
            sel = ["rowid"] + active_ucols
            trows = cur.execute(f'SELECT {", ".join(sel)} FROM "{tbl_name}"').fetchall()
            seen_pks = {str(tr[1]) for tr in trows if len(tr) > 1 and tr[1] is not None}
            for tr in trows:
                tmap = dict(zip(sel, tr))
                t_rowid = tmap["rowid"]
                t_updates: list[str] = []
                t_vals: list[Any] = []
                delete_row = False
                for uc in active_ucols:
                    raw_v = tmap.get(uc)
                    if raw_v is None:
                        continue
                    norm_v = _normalize_sqlite_uuid_val(raw_v, use_hex=use_hex)
                    if norm_v is None:
                        if uc == "user_id" and valid_user_id:
                            norm_v = valid_user_id
                        elif uc == "flow_id":
                            u_flow = uuid.uuid5(uuid.NAMESPACE_DNS, f"flow:{raw_v}")
                            norm_v = u_flow.hex if use_hex else str(u_flow)
                        else:
                            seed = f"{tbl_name}:{uc}:{raw_v}:{t_rowid}"
                            u_gen = uuid.uuid5(uuid.NAMESPACE_DNS, seed)
                            norm_v = u_gen.hex if use_hex else str(u_gen)
                    if norm_v != str(raw_v):
                        if uc == "id" and norm_v in seen_pks:
                            delete_row = True
                            break
                        t_updates.append(f"{uc} = ?")
                        t_vals.append(norm_v)
                        if uc == "id":
                            seen_pks.add(norm_v)
                if delete_row:
                    cur.execute(f'DELETE FROM "{tbl_name}" WHERE rowid = ?', (t_rowid,))
                    repaired += 1
                elif t_updates:
                    t_vals.append(t_rowid)
                    cur.execute(
                        f'UPDATE "{tbl_name}" SET {", ".join(t_updates)} WHERE rowid = ?',
                        tuple(t_vals),
                    )
                    repaired += 1
        if repaired > 0 or getattr(conn, "in_transaction", False):
            conn.commit()
    except Exception:  # noqa: BLE001
        pass
    return repaired


def _should_seed_showcase_flow() -> bool:
    """Return True only when `SNOWFLAKE_SEED_SHOWCASE_FLOW` is explicitly enabled (`W2-R9-06`)."""
    return os.getenv("SNOWFLAKE_SEED_SHOWCASE_FLOW", "").strip().lower() in ("1", "true", "yes")


def ensure_sqlite_wal_and_uv_cache() -> dict[str, Any]:
    """Ensure writable UV_CACHE_DIR (/tmp/uv-cache), bind `standard_langflow` in builtins/sys.modules, and configure SQLite WAL + busy_timeout=30000 on /persist/langflow.db."""
    import builtins
    import sys

    try:
        from lfx_snowflake import context_helpers as _ctx_hlp
        from lfx_snowflake import standard_langflow as _std_lf

        sys.modules["standard_langflow"] = _std_lf
        setattr(builtins, "standard_langflow", _std_lf)
        sys.modules.setdefault(
            "lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner",
            _ctx_hlp,
        )
        sys.modules.setdefault(
            "lfx_snowflake.native_category_components.data_source.snowflake_object_provisioner",
            _ctx_hlp,
        )
    except Exception:  # noqa: BLE001
        pass

    os.environ.setdefault("UV_CACHE_DIR", "/tmp/uv-cache")
    os.environ.setdefault("XDG_CACHE_HOME", "/tmp/.cache")
    os.environ.setdefault("SQLITE_TMPDIR", "/tmp")

    for cache_dir in (
        Path(os.environ["UV_CACHE_DIR"]),
        Path(os.environ["XDG_CACHE_HOME"]),
        Path("/app/data/.cache/uv"),
    ):
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(cache_dir, 0o777)
        except OSError:
            pass

    persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
    candidate_dbs: list[Path] = [persist_dir / "langflow.db"]
    db_url = os.getenv("LANGFLOW_DATABASE_URL", "").strip()
    if db_url.startswith("sqlite:///"):
        raw_sqlite_path = db_url.replace("sqlite:///", "", 1).split("?")[0]
        if raw_sqlite_path:
            p = Path("/" + raw_sqlite_path.lstrip("/")) if raw_sqlite_path.startswith("/") else Path(raw_sqlite_path)
            if p not in candidate_dbs:
                candidate_dbs.append(p)

    old_try_import_snippet = (
        "try:\\n    import standard_langflow\\nexcept ImportError:  # pragma: no cover\\n"
        "    from lfx_snowflake import standard_langflow"
    )
    new_top_import_snippet = "from lfx_snowflake import standard_langflow"

    wal_configured: list[str] = []
    for db_file in candidate_dbs:
        if db_file.exists():
            try:
                conn = _open_sqlite_conn(db_file, readonly=False, timeout=30.0)
                try:
                    cur = conn.cursor()
                    tables = {
                        r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    }
                    if "flow" in tables:
                        cur.execute(
                            "UPDATE flow SET data = REPLACE(data, ?, ?) WHERE data LIKE '%import standard_langflow%'",
                            (old_try_import_snippet, new_top_import_snippet),
                        )
                        conn.commit()
                    heal_sqlite_invalid_uuid_rows(conn)
                    purge_snowflake_rows_from_native_knowledge_base(conn)
                except Exception:  # noqa: BLE001
                    pass
                conn.close()
                wal_configured.append(str(db_file))
            except Exception:  # noqa: BLE001
                pass

    return {
        "ok": True,
        "uv_cache_dir": os.environ.get("UV_CACHE_DIR"),
        "xdg_cache_home": os.environ.get("XDG_CACHE_HOME"),
        "sqlite_tmpdir": os.environ.get("SQLITE_TMPDIR"),
        "wal_configured_dbs": wal_configured,
        "busy_timeout_ms": 30000,
    }


class MemoryBasePersistenceError(RuntimeError):
    """Native ``memory_base`` registration failed (BUG-MEM-MB-01); carries the HTTP status to return."""

    def __init__(self, message: str, http_status: int = 500) -> None:
        super().__init__(message)
        self.http_status = http_status


class LiveSnowflakeBridge:
    """Executes real SQL against Snowflake via SPCS OAuth token and manages active role & trace replication."""

    def __init__(self) -> None:
        ensure_sqlite_wal_and_uv_cache()
        self.database = os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE_LANGFLOW")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "CORE")
        self._initial_database = self.database
        self._initial_schema = self.schema
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "")
        self.stage_name = os.getenv("SNOWFLAKE_STAGE_NAME", "SNOWFLAKE_LANGFLOW_STAGE")
        self.activity_table = os.getenv(
            "LANGFLOW_FLOW_ACTIVITY_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_ACTIVITY"
        )
        self.runs_table = os.getenv(
            "LANGFLOW_TRACES_RUNS_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_RUNS"
        )
        self.spans_table = os.getenv(
            "LANGFLOW_TRACES_SPANS_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_SPANS"
        )
        self.audit_table = os.getenv(
            "LANGFLOW_AUDIT_LOG_TABLE", f"{self.database}.{self.schema}.LANGFLOW_AUDIT_LOG"
        )
        # BUG-CA-02: role/epoch selections are tracked per (authenticated_user, flow_id); the base values
        # below are only the service defaults and are never mutated by a user's role switch.
        self._role_sessions: dict[tuple[str, str | None], dict[str, Any]] = {}
        self._role_sessions_lock = threading.RLock()
        self._base_context_epoch: int = 1
        self._secondary_guard_cache: dict[tuple[str, str, str, str], float] = {}
        # W3-R1-10e: no hard-coded role. Empty means "omit role" so the token's default role applies.
        self.active_role = (os.getenv("SNOWFLAKE_DEFAULT_ROLE") or "").strip().upper()
        self.available_roles: list[str] = [
            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
            "SNOWFLAKE_LANGFLOW_DEV_ROLE",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "PUBLIC",
        ]
        self._service_user: str = os.getenv("SNOWFLAKE_USER", "SPCS_CALLER")
        self.context_epoch = 1
        self.secondary_roles_mode: str = _sf_identity.secondary_roles_mode()
        self.memories_registry: list[dict[str, Any]] = []
        # Populated only from real Snowflake results (SHOW STAGES / successful CREATE STAGE).
        self.stages_registry: list[dict[str, Any]] = []
        self.external_volumes_registry: list[dict[str, Any]] = []
        self.stage_files_registry: dict[str, list[dict[str, Any]]] = {}
        # Stage metadata (type, url, ...) keyed by DB.SCHEMA.STAGE, harvested from SHOW STAGES rows.
        self._stage_meta: dict[str, dict[str, Any]] = {}
        self.role_accessible_catalog: dict[str, dict[str, Any]] = {}
        self.selected_context: dict[str, Any] = {
            "database": self.database,
            "schema": self.schema,
            "databases": [self.database],
            "schemas_by_db": {self.database: [self.schema, "PUBLIC"]},
            "full_databases": [],
            "full_schemas": [],
            "postgres_instances": [],
            "selected_postgres_instances": [],
            "postgres_tables_by_instance": {},
            "objects_by_db_schema": {},
            "selected_objects": {},
            "visible_objects": {
                "tables": [],
                "views": [],
                "iceberg_tables": [],
                "stages": [],
                "external_volumes": [],
                "memories": [],
                "cortex_search_services": [],
                "semantic_views": [],
                "semantic_models": [],
                "cortex_agents": [],
                "functions": [],
                "procedures": [],
                "postgres_instances": [],
            },
        }
        self.contexts_by_flow_id: dict[str, dict[str, Any]] = {}
        self.active_flow_id: str | None = None
        self._roles_discovered: bool = False
        self._superuser_synced: bool = False
        self._roles_last_refreshed_ts: float = 0.0
        self._dbs_cache: dict[str, tuple[float, list[str], list[str]]] = {}
        self._schemas_cache: dict[str, tuple[float, list[str]]] = {}
        self._objects_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._synced_tx_ids: set[str] = set()
        # T10: derived RUN rows already written (one per job/execution, never one per span row).
        self._synced_derived_run_ids: set[str] = set()
        self._synced_vertex_build_ids: set[str] = set()
        self._synced_activity_ids: set[str] = set()
        self._soft_deleted_activity_ids: set[str] = set()
        self._recent_activity_events: list[dict[str, Any]] = []
        self._auto_sync_daemon_started: bool = False
        self.auto_sync_enabled: bool = True
        self.last_auto_sync_ts: str | None = None
        self.synced_runs_count: int = 0
        self.synced_spans_count: int = 0
        self.synced_activity_count: int = 0
        self._http_client: Any = None
        self._cortex_agent_threads_mem: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self._cortex_threads_server_mem: dict[int, dict[str, Any]] = {}
        self._a2a_cortex_tasks_mem: dict[str, dict[str, Any]] = {}
        self._cortex_thread_seq: int = 100000
        self._cortex_msg_seq: int = 100
        self._cortex_rest_transport_fn: Any = None
        self._ensure_flow_contexts_sqlite_table()
        self._load_flow_contexts_sqlite()
        self._ensure_cortex_agent_threads_sqlite_table()
        self._load_cortex_agent_threads_sqlite()

    @staticmethod
    def resolve_component_flow_id(
        component: Any = None,
        build_config: dict[str, Any] | None = None,
    ) -> str | None:
        """Resolve isolated flow_id for a component via build_config, attributes, or ContextVar (`CTX-ARCH-03`)."""
        return _resolve_component_flow_id(component, build_config)

    _resolve_component_flow_id = resolve_component_flow_id

    @staticmethod
    def _normalize_flow_id(flow_id: str | None) -> str | None:
        """Normalize flow_id across URLs (hyphenated UUIDs), SQLite (32-char hex), and custom workflow keys."""
        if flow_id is None:
            return None
        clean = str(flow_id).strip()
        if not clean or clean.lower() in {"null", "undefined", "none", ""}:
            return None
        if re.fullmatch(r"[0-9a-fA-F]{32}", clean):
            try:
                return str(uuid.UUID(clean))
            except Exception:  # noqa: BLE001
                return clean
        if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", clean):
            try:
                return str(uuid.UUID(clean))
            except Exception:  # noqa: BLE001
                return clean.lower()
        return clean

    def _resolve_writable_sqlite_db_path(self) -> Path:
        """Return writable SQLite DB path (`/persist/langflow.db` in SPCS or `/tmp/sflf_persist/langflow.db` fallback)."""
        env_dir = os.getenv("LANGFLOW_PERSIST_DIR", "").strip()
        if env_dir:
            p_dir = Path(env_dir)
            try:
                p_dir.mkdir(parents=True, exist_ok=True)
                return p_dir / "langflow.db"
            except OSError:
                pass
        persist_dir = Path("/persist")
        try:
            if persist_dir.exists() and os.access(persist_dir, os.W_OK):
                return persist_dir / "langflow.db"
            persist_dir.mkdir(parents=True, exist_ok=True)
            if os.access(persist_dir, os.W_OK):
                return persist_dir / "langflow.db"
        except OSError:
            pass
        fallback_dir = Path("/tmp/sflf_persist")
        fallback_dir.mkdir(parents=True, exist_ok=True)
        return fallback_dir / "langflow.db"

    def _default_context_template(self) -> dict[str, Any]:
        """Return a fresh default Snowflake context dictionary for a newly initialized workflow (`CTX-ARCH-03`)."""
        default_db = (
            os.getenv("SNOWFLAKE_DATABASE")
            or getattr(self, "_initial_database", None)
            or "SNOWFLAKE_LANGFLOW"
        ).strip().upper()
        default_sch = (
            os.getenv("SNOWFLAKE_SCHEMA")
            or getattr(self, "_initial_schema", None)
            or "CORE"
        ).strip().upper()
        return {
            # BUG-CTX-02: a new flow starts with an empty `[ ❄️ Context ]`; `database`/`schema` are only the
            # session defaults used to resolve unqualified names, not a selection.
            "database": default_db,
            "schema": default_sch,
            "databases": [],
            "schemas_by_db": {},
            "full_databases": [],
            "full_schemas": [],
            "postgres_instances": [],
            "selected_postgres_instances": [],
            "postgres_tables_by_instance": {},
            "objects_by_db_schema": {},
            "selected_objects": {},
            "visible_objects": {
                "tables": [],
                "views": [],
                "iceberg_tables": [],
                "stages": [],
                "memories": [],
                "cortex_search_services": [],
                "semantic_views": [],
                "semantic_models": [],
                "cortex_agents": [],
                "functions": [],
                "procedures": [],
                "postgres_instances": [],
            },
        }

    def _ensure_flow_contexts_sqlite_table(self) -> Path | None:
        """Ensure `snowflake_flow_contexts` table exists in SQLite (`langflow.db`)."""
        try:
            db_path = self._resolve_writable_sqlite_db_path()
            conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snowflake_flow_contexts (
                    flow_id TEXT PRIMARY KEY,
                    active_role TEXT,
                    database_name TEXT,
                    schema_name TEXT,
                    context_epoch INTEGER,
                    selected_context_json TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.commit()
            conn.close()
            return db_path
        except Exception:  # noqa: BLE001
            return None

    def _load_flow_contexts_sqlite(self) -> dict[str, dict[str, Any]]:
        """Hydrate `self.contexts_by_flow_id` from SQLite `snowflake_flow_contexts` and `flow.data.snowflake_context` (`CTX-ARCH-07` #3)."""
        loaded: dict[str, dict[str, Any]] = {}
        try:
            db_path = self._resolve_writable_sqlite_db_path()
            if not db_path.exists():
                return loaded
            conn = _open_sqlite_conn(db_path, readonly=True, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "snowflake_flow_contexts" in tables:
                rows = cur.execute("SELECT * FROM snowflake_flow_contexts").fetchall()
                for row in rows:
                    row_d = dict(row)
                    raw_fid = str(row_d.get("flow_id") or "").strip()
                    norm_fid = self._normalize_flow_id(raw_fid) or raw_fid
                    if not norm_fid or norm_fid == "__global__":
                        continue
                    try:
                        parsed_ctx = json.loads(str(row_d.get("selected_context_json") or "{}"))
                    except Exception:  # noqa: BLE001
                        parsed_ctx = {}
                    if isinstance(parsed_ctx, dict) and parsed_ctx:
                        from lfx_snowflake.context_hygiene import drop_phantom_telemetry_objects  # noqa: PLC0415

                        drop_phantom_telemetry_objects(parsed_ctx)  # pre-v0.8.6 phantoms
                        parsed_ctx.setdefault("flow_id", norm_fid)
                        parsed_ctx.setdefault("full_databases", [])
                        parsed_ctx.setdefault("full_schemas", [])
                        parsed_ctx.setdefault("postgres_instances", [])
                        if row_d.get("database_name") and "database" not in parsed_ctx:
                            parsed_ctx["database"] = str(row_d["database_name"])
                        if row_d.get("schema_name") and "schema" not in parsed_ctx:
                            parsed_ctx["schema"] = str(row_d["schema_name"])
                        self.contexts_by_flow_id[norm_fid] = parsed_ctx
                        loaded[norm_fid] = parsed_ctx
            if "flow" in tables:
                flow_rows = cur.execute("SELECT id, name, data FROM flow").fetchall()
                for f_row in flow_rows:
                    f_dict = dict(f_row)
                    raw_fid = str(f_dict.get("id") or "").strip()
                    norm_fid = self._normalize_flow_id(raw_fid)
                    if not norm_fid or norm_fid in self.contexts_by_flow_id:
                        continue
                    raw_data = f_dict.get("data")
                    try:
                        flow_data = json.loads(raw_data) if isinstance(raw_data, str) else (raw_data or {})
                    except Exception:  # noqa: BLE001
                        flow_data = {}
                    if isinstance(flow_data, dict) and isinstance(flow_data.get("snowflake_context"), dict):
                        emb_ctx = copy.deepcopy(flow_data["snowflake_context"])
                        if emb_ctx:
                            tpl = self._default_context_template()
                            tpl.update(emb_ctx)
                            tpl["flow_id"] = norm_fid
                            tpl["active_role"] = self.active_role
                            self.contexts_by_flow_id[norm_fid] = tpl
                            loaded[norm_fid] = tpl
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        return loaded

    def sync_flow_node_templates_sqlite(
        self,
        flow_id: str | None,
        ctx: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Write `data.snowflake_context` into SQLite `flow.data` and update Snowflake node template options (`CTX-ARCH-05B`, `CTX-ARCH-07` #3)."""
        norm_fid = self._normalize_flow_id(flow_id)
        if not norm_fid or norm_fid == "__global__":
            return {}
        updated_templates: dict[str, dict[str, Any]] = {}
        try:
            target_ctx = copy.deepcopy(ctx or self.contexts_by_flow_id.get(norm_fid) or self._default_context_template())
            dbs = self.get_context_databases(flow_id=norm_fid)
            pg_insts = self.get_context_postgres_instances(flow_id=norm_fid)

            persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
            candidate_paths = [self._resolve_writable_sqlite_db_path(), persist_dir / "langflow.db"]
            seen_paths: set[str] = set()
            for db_path in candidate_paths:
                p_str = str(db_path)
                if p_str in seen_paths or not db_path.exists():
                    continue
                seen_paths.add(p_str)
                conn = _open_sqlite_conn(db_path, readonly=True, timeout=15.0)
                try:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                    if "flow" not in tables:
                        continue
                    hex_fid = norm_fid.replace("-", "")
                    row = cur.execute(
                        "SELECT id, data FROM flow WHERE id = ? OR id = ?",
                        (norm_fid, hex_fid),
                    ).fetchone()
                    if not row:
                        continue
                    row_id = row["id"]
                    raw_data = row["data"]
                finally:
                    conn.close()
                try:
                    flow_data = json.loads(raw_data) if isinstance(raw_data, str) else (dict(raw_data) if isinstance(raw_data, dict) else {})
                except Exception:  # noqa: BLE001
                    flow_data = {}
                if not isinstance(flow_data, dict):
                    flow_data = {}

                flow_data["snowflake_context"] = copy.deepcopy(target_ctx)
                nodes_list = flow_data.get("nodes")
                if isinstance(nodes_list, list):
                    for node_obj in nodes_list:
                        if not isinstance(node_obj, dict):
                            continue
                        node_id = str(node_obj.get("id") or "")
                        n_data = node_obj.get("data")
                        if not isinstance(n_data, dict):
                            continue
                        inner_node = n_data.get("node")
                        if not isinstance(inner_node, dict):
                            continue
                        tmpl = inner_node.get("template")
                        if not isinstance(tmpl, dict):
                            continue
                        node_type_str = str(n_data.get("type") or inner_node.get("display_name") or "")
                        is_sf_node = (
                            "snowflake" in node_type_str.lower()
                            or "postgres_instance" in tmpl
                            or ("database" in tmpl and any(k in tmpl for k in ("schema_name", "table_name", "view_name", "iceberg_table_name", "stage_name", "search_service", "service_name", "semantic_view", "agent_name", "function_name", "procedure_name")))
                        )
                        if not is_sf_node:
                            continue

                        node_changes: dict[str, Any] = {}
                        if "database" in tmpl and isinstance(tmpl["database"], dict):
                            tmpl["database"]["options"] = list(dbs)
                            if tmpl["database"].get("value") not in dbs:
                                tmpl["database"]["value"] = dbs[0] if dbs else ""
                            node_changes["database"] = {
                                "options": list(dbs),
                                "value": tmpl["database"].get("value"),
                            }
                        sel_db = str((tmpl.get("database") or {}).get("value") or (dbs[0] if dbs else self.database)).strip().upper()
                        schemas = self.get_context_schemas(sel_db, flow_id=norm_fid)
                        for sch_k in ("schema_name", "schema"):
                            if sch_k in tmpl and isinstance(tmpl[sch_k], dict) and ("options" in tmpl[sch_k] or sch_k == "schema_name"):
                                tmpl[sch_k]["options"] = list(schemas)
                                if tmpl[sch_k].get("value") not in schemas:
                                    tmpl[sch_k]["value"] = schemas[0] if schemas else ""
                                node_changes[sch_k] = {
                                    "options": list(schemas),
                                    "value": tmpl[sch_k].get("value"),
                                }
                        sel_sch = str(
                            ((tmpl.get("schema_name") or tmpl.get("schema") or {}).get("value"))
                            or (schemas[0] if schemas else self.schema)
                        ).strip().upper()

                        if "postgres_instance" in tmpl and isinstance(tmpl["postgres_instance"], dict):
                            tmpl["postgres_instance"]["options"] = list(pg_insts)
                            if tmpl["postgres_instance"].get("value") not in pg_insts:
                                tmpl["postgres_instance"]["value"] = pg_insts[0] if pg_insts else ""
                            node_changes["postgres_instance"] = {
                                "options": list(pg_insts),
                                "value": tmpl["postgres_instance"].get("value"),
                            }
                            if "table_name" in tmpl and isinstance(tmpl["table_name"], dict):
                                pg_tbls = self.get_context_postgres_tables(
                                    tmpl["postgres_instance"].get("value"),
                                    flow_id=norm_fid,
                                )
                                tmpl["table_name"]["options"] = list(pg_tbls)
                                if pg_tbls and tmpl["table_name"].get("value") not in pg_tbls:
                                    tmpl["table_name"]["value"] = pg_tbls[0]
                                node_changes["table_name"] = {
                                    "options": list(pg_tbls),
                                    "value": tmpl["table_name"].get("value"),
                                }

                        field_to_cat = {
                            "view_name": "views",
                            "iceberg_table_name": "iceberg_tables",
                            "stage_name": "stages",
                            "search_service": "cortex_search_services",
                            "service_name": "cortex_search_services",
                            "semantic_view": "semantic_views",
                            "agent_name": "cortex_agents",
                            "function_name": "functions",
                            "procedure_name": "procedures",
                        }
                        if "postgres_instance" not in tmpl:
                            field_to_cat["table_name"] = "tables"

                        for f_key, cat_key in field_to_cat.items():
                            if f_key in tmpl and isinstance(tmpl[f_key], dict) and "options" in tmpl[f_key]:
                                opts = self.get_context_objects(sel_db, sel_sch, cat_key, flow_id=norm_fid)
                                tmpl[f_key]["options"] = list(opts)
                                if tmpl[f_key].get("value") not in opts:
                                    tmpl[f_key]["value"] = opts[0] if opts else ""
                                node_changes[f_key] = {
                                    "options": list(opts),
                                    "value": tmpl[f_key].get("value"),
                                }
                        if node_id and node_changes:
                            updated_templates[node_id] = node_changes

                new_json = json.dumps(flow_data)
                if new_json != raw_data:
                    wconn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
                    try:
                        wconn.execute("UPDATE flow SET data = ? WHERE id = ?", (new_json, row_id))
                        wconn.commit()
                    finally:
                        wconn.close()
        except Exception:  # noqa: BLE001
            pass
        return updated_templates

    def hydrate_flow_context_from_flow_payload(
        self,
        flow_payload: dict[str, Any],
        flow_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Hydrate per-flow context from an imported or duplicated flow's `data.snowflake_context` (`CTX-ARCH-07` #3)."""
        if not isinstance(flow_payload, dict):
            return None
        norm_fid = self._normalize_flow_id(flow_id or flow_payload.get("id"))
        if not norm_fid:
            return None
        data_sec = flow_payload.get("data") if isinstance(flow_payload.get("data"), dict) else {}
        emb_ctx = data_sec.get("snowflake_context") if isinstance(data_sec.get("snowflake_context"), dict) else (
            flow_payload.get("snowflake_context") if isinstance(flow_payload.get("snowflake_context"), dict) else None
        )
        if not emb_ctx:
            return None
        res = self.update_selected_context(emb_ctx, flow_id=norm_fid)
        return res.get("selected_context")

    def _save_flow_context_sqlite(self, flow_id: str | None, ctx: dict[str, Any]) -> bool:
        """Persist a workflow's Snowflake context into SQLite `snowflake_flow_contexts` and `flow.data.snowflake_context`."""
        try:
            db_path = self._ensure_flow_contexts_sqlite_table()
            if db_path is None:
                return False
            norm_fid = self._normalize_flow_id(flow_id) or "__global__"
            db_name = str(ctx.get("database") or (ctx.get("databases") or [self.database])[0] or self.database)
            sch_map = ctx.get("schemas_by_db") or {}
            sch_name = str(
                ctx.get("schema")
                or (sch_map.get(db_name) or [self.schema])[0]
                or self.schema
            )
            now_iso = datetime.now(timezone.utc).isoformat()
            conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO snowflake_flow_contexts
                    (flow_id, active_role, database_name, schema_name, context_epoch, selected_context_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        norm_fid,
                        self.active_role,
                        db_name,
                        sch_name,
                        int(self.context_epoch),
                        json.dumps(ctx),
                        now_iso,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            if norm_fid != "__global__":
                self.sync_flow_node_templates_sqlite(norm_fid, ctx)
            return True
        except Exception:  # noqa: BLE001
            return False

    def get_flow_context(self, flow_id: str | None = None, *, create: bool = True) -> dict[str, Any]:
        """Return the Snowflake context scoped to `flow_id` (hydrating from SQLite if needed), or global context when `flow_id` is None (`CTX-ARCH-03`).

        BUG-CTX-01: an unknown flow gets an empty context that is never persisted to SQLite (no orphan
        rows); with ``create=False`` it is not even cached, so read paths have no side effects.
        """
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        if not norm_fid:
            return self.selected_context
        if norm_fid not in self.contexts_by_flow_id:
            self._load_flow_contexts_sqlite()
        if norm_fid not in self.contexts_by_flow_id:
            hex_alt = norm_fid.replace("-", "")
            if hex_alt in self.contexts_by_flow_id:
                self.contexts_by_flow_id[norm_fid] = copy.deepcopy(self.contexts_by_flow_id[hex_alt])
            else:
                fresh = self._default_context_template()
                fresh["flow_id"] = norm_fid
                if not create:
                    fresh["active_role"] = self.active_role
                    fresh.setdefault("context_epoch", self.context_epoch)
                    return fresh
                self.contexts_by_flow_id[norm_fid] = fresh
        ctx = self.contexts_by_flow_id[norm_fid]
        ctx["flow_id"] = norm_fid
        ctx["active_role"] = self.active_role
        ctx.setdefault("full_databases", [])
        ctx.setdefault("full_schemas", [])
        ctx.setdefault("postgres_instances", [])
        ctx.setdefault("context_epoch", self.context_epoch)
        return ctx

    def set_active_flow(self, flow_id: str | None) -> dict[str, Any]:
        """Activate a workflow's Snowflake context without clobbering other workflows (`CTX-ARCH-03`)."""
        norm_fid = self._normalize_flow_id(flow_id)
        self.active_flow_id = norm_fid
        if norm_fid:
            ctx = self.get_flow_context(norm_fid)
            dbs = ctx.get("databases") or []
            if ctx.get("database"):
                self.database = str(ctx["database"]).strip().upper()
            elif dbs:
                self.database = str(dbs[0]).strip().upper()
            sch_map = ctx.get("schemas_by_db") or {}
            if ctx.get("schema"):
                self.schema = str(ctx["schema"]).strip().upper()
            elif self.database in sch_map and sch_map[self.database]:
                self.schema = str(sch_map[self.database][0]).strip().upper()
            return ctx
        return self.selected_context

    def invalidate_catalog_cache(self) -> None:
        """Clear in-memory catalog caches on role switch or explicit refresh."""
        self._dbs_cache.clear()
        self._schemas_cache.clear()
        self._objects_cache.clear()
        self._roles_last_refreshed_ts = 0.0

    def is_memory_visible_in_context(
        self,
        entry: dict[str, Any],
        flow_id: str | None = None,
    ) -> bool:
        """Return True iff `entry`'s backing storage (`Snowflake Postgres` or `Database ▸ Schema`) is enabled in `[ ❄️ Context ]`."""
        if not isinstance(entry, dict):
            return False
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id)
        if norm_fid:
            target_ctx = self.get_flow_context(norm_fid)
            pg_list = self.get_context_postgres_instances(flow_id=norm_fid)
        else:
            target_ctx = self.selected_context
            pg_list = self.get_context_postgres_instances(flow_id=None)

        storage_family = str(entry.get("storage_family") or "").strip().lower()
        vec_db = str(entry.get("vector_database") or "").strip().upper()
        fqn = str(entry.get("table_fqn") or entry.get("fqn") or "").strip()
        is_pg_memory = (
            storage_family == "snowflake_postgres"
            or "POSTGRES" in vec_db
            or ":5432" in fqn
        )
        if is_pg_memory:
            active_pg = {str(p).strip().upper() for p in pg_list if str(p).strip()}
            if not active_pg:
                return False
            pg_inst = str(entry.get("postgres_instance") or "").strip().upper()
            if not pg_inst and ":5432" in fqn:
                pg_inst = fqn.split(":5432", 1)[0].strip().upper()
            if not pg_inst:
                pg_inst = str(entry.get("database") or "LANGFLOW_PG_INSTANCE").strip().upper()
            return pg_inst in active_pg

        mem_db = str(entry.get("database") or "").strip().upper()
        mem_sch = str(entry.get("schema") or "").strip().upper()
        if (not mem_db or not mem_sch) and "." in fqn and ":5432" not in fqn:
            parts = [p.strip().upper() for p in fqn.split(".") if p.strip()]
            if len(parts) >= 2:
                mem_db = mem_db or parts[0]
                mem_sch = mem_sch or parts[1]

        raw_dbs = target_ctx.get("databases")
        if isinstance(raw_dbs, list):
            active_dbs = {str(d).strip().upper() for d in raw_dbs if str(d).strip()}
            if mem_db and mem_db not in active_dbs:
                return False

        schemas_map = target_ctx.get("schemas_by_db")
        if isinstance(schemas_map, dict) and mem_db and mem_sch:
            for db_k, sch_list in schemas_map.items():
                if str(db_k).strip().upper() == mem_db and isinstance(sch_list, list):
                    active_schemas = {str(s).strip().upper() for s in sch_list if str(s).strip()}
                    if mem_sch not in active_schemas:
                        return False
                    break
        return True

    def get_memories_for_flow(self, flow_id: str | None = None) -> list[dict[str, Any]]:
        """Return Snowflake Memories filtered by the active `[ ❄️ Context ]` of `flow_id`."""
        eff_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id)
        items: list[dict[str, Any]] = []
        for raw_m in self.memories_registry:
            if not self.is_memory_visible_in_context(raw_m, flow_id=eff_fid):
                continue
            m = dict(raw_m)
            m_name = str(m.get("name") or "SNOWFLAKE_MEMORY").strip()
            det_id = str(m.get("memory_id") or m.get("langflow_memory_id") or compute_deterministic_memory_uuid(m_name))
            m["id"] = det_id
            m["memory_id"] = det_id
            m["langflow_memory_id"] = det_id
            items.append(m)
        known_names = {str(m.get("name") or "").strip().upper() for m in items if m.get("name")}
        vis_mem = list((self.selected_context.get("visible_objects") or {}).get("memories") or [])
        for mem_name in vis_mem:
            clean = str(mem_name or "").strip()
            if clean and clean.upper() not in known_names:
                inRegistry = any(
                    str(rm.get("name") or "").strip().upper() == clean.upper()
                    for rm in self.memories_registry
                )
                if inRegistry:
                    continue
                det_id = compute_deterministic_memory_uuid(clean)
                cand = {
                    "id": det_id,
                    "memory_id": det_id,
                    "langflow_memory_id": det_id,
                    "name": clean,
                    "database": self.database,
                    "schema": self.schema,
                    "table_fqn": f"{self.database}.{self.schema}.MEM_{clean.upper()}",
                    "agent_memory_type": "semantic_vector",
                    "canonical_memory_type": "episodic_memory",
                    "memory_category": "vector_table",
                    "vector_database": "Standard Table (VECTOR)",
                    "embedding_model": "snowflake-arctic-embed-m-v1.5",
                    "dimension": 768,
                    "auto_capture": True,
                    "created_by_role": self.active_role,
                }
                if self.is_memory_visible_in_context(cand, flow_id=eff_fid):
                    items.append(cand)
                    known_names.add(clean.upper())
        return items

    @property
    def memories_catalog(self) -> list[dict[str, Any]]:
        """Return governed Snowflake Agent Memories available for workflow recall (`W2-R2-13`, `W2-R4-08G`)."""
        return self.get_memories_for_flow(flow_id=None)

    def get_memory_catalog_options(
        self,
        *,
        include_none: bool = True,
        flow_id: str | None = None,
    ) -> list[str]:
        """Return dropdown options for `memory_catalog_ref` / `memory_base` filtered by `[ ❄️ Context ]` (`W2-R5-01`)."""
        opts: list[str] = ["None"] if include_none else []
        for entry in self.get_memories_for_flow(flow_id=flow_id):
            nm = str(entry.get("name") or "").strip()
            fqn = str(entry.get("table_fqn") or "").strip()
            agent_type = str(entry.get("agent_memory_type") or entry.get("memory_category") or "memory").strip()
            if nm and nm not in opts:
                opts.append(nm)
            label = f"{nm} ({agent_type} -> {fqn})" if nm and fqn else (fqn or nm)
            if label and label not in opts:
                opts.append(label)
        return opts

    def _recall_lf_mem(self, entry: dict[str, Any], *, query: str, session_id: str | None, limit: int) -> dict[str, Any]:
        """Recall from a governed LF_MEM Memory Base: AI_EMBED + VECTOR_COSINE_SIMILARITY, rows carry ``_score``."""
        from . import flow_context_guard as _guard  # noqa: PLC0415
        from .memories_lf_mem import LfMemError, LfMemSpec  # noqa: PLC0415

        spec = LfMemSpec(**entry["lf_mem"])
        sess = str(session_id or "").strip()
        filter_on = bool(spec.filter_by_session or entry.get("filter_by_session"))
        if filter_on and sess.lower() in ("", "default"):
            # BUG-MEM-API-04: fail closed - a session-scoped base never falls back to every session.
            return {"ok": False, "recalled": False, "error_code": "MISSING_SESSION_ID",
                    "message": "filter_by_session is on: a concrete session_id is required for recall",
                    "memory_ref": entry.get("name"), "filter_by_session": True, "rows": []}
        try:
            sql, bindings = spec.build_recall_request(
                query, flow_id=str(entry.get("flow_id") or ""), session_id=sess or None,
                filter_by_session=filter_on, top_k=limit,
            )
        except LfMemError as exc:
            return {"ok": False, "recalled": False, "error_code": exc.error_code, "message": str(exc), "rows": []}
        token = CURRENT_SNOWFLAKE_FLOW_ID.set(str(entry.get("flow_id") or ""))
        try:
            with _guard.flow_component_sql():
                res = self.execute_sql(sql, bindings=bindings, mode="caller")
        finally:
            CURRENT_SNOWFLAKE_FLOW_ID.reset(token)
        if not res.get("ok") or res.get("mode") == "offline":
            code = str(res.get("error_code") or ("NOT_CONFIGURED" if res.get("mode") == "offline" else "SNOWFLAKE_ERROR"))
            return {"ok": False, "recalled": False, "error_code": code, "message": str(res.get("message") or code),
                    "memory_ref": entry.get("name"), "rows": [], "recall_sql": sql}
        rows = LfMemSpec.recall_rows_to_results(list(res.get("rows") or []))
        return {"ok": True, "recalled": bool(rows), "memory_ref": entry.get("name"), "table_fqn": spec.table_fqn,
                "filter_by_session": filter_on, "rows": rows, "recall_sql": sql, "query_id": res.get("statement_handle")}

    def recall_memory(
        self,
        memory_ref: str,
        *,
        query: str = "",
        session_id: str = "default",
        thread_id: str = "default",
        user_id: str = "default",
        entity_namespace: str = "default",
        topic: str = "",
        limit: int = 10,
    ) -> dict[str, Any]:
        """Recall records from a Snowflake Memory across all 5 LangChain/Langflow Memory types (`W2-R2-13`, `W2-R3-05`)."""
        clean_ref = (memory_ref or "").strip()
        if not clean_ref or clean_ref == "None":
            return {"ok": False, "recalled": False, "memory_ref": None, "rows": []}
        short_ref = clean_ref.split(" (")[0].strip()
        matched: dict[str, Any] | None = None
        for entry in self.memories_catalog:
            if short_ref.upper() in {
                str(entry.get("name") or "").strip().upper(),
                str(entry.get("table_fqn") or "").strip().upper(),
            }:
                matched = entry
                break
        if matched is None:
            from lfx_snowflake.memories_lf_mem import legacy_memory_not_found  # noqa: PLC0415

            return legacy_memory_not_found(short_ref)
        if isinstance(matched.get("lf_mem"), dict):
            return self._recall_lf_mem(matched, query=query, session_id=session_id, limit=limit)
        fqn = str(matched.get("table_fqn") or "").strip()
        fqn_parts = fqn.split(".")
        if len(fqn_parts) != 3 or not all(self._STAGE_IDENT_RE.match(seg) for seg in fqn_parts):
            return {
                "ok": False,
                "recalled": False,
                "error_code": "INVALID_IDENTIFIER",
                "message": "Memory table FQN must be DATABASE.SCHEMA.TABLE with valid Snowflake identifiers.",
                "memory_ref": short_ref,
                "query_id": None,
                "rows": [],
            }
        agent_type = str(matched.get("agent_memory_type") or "episodic_memory")
        canonical_type = str(matched.get("canonical_memory_type") or resolve_canonical_memory_type(agent_type))
        type_spec = CANONICAL_MEMORY_TYPES_BY_ID.get(canonical_type, CANONICAL_MEMORY_TYPES_BY_ID["episodic_memory"])
        enable_embedding = bool(
            matched.get(
                "enable_embedding",
                matched.get("is_vector_memory", agent_type == "semantic_vector"),
            )
        )
        embedding_provider = str(
            matched.get("embedding_provider") or ("snowflake_cortex" if enable_embedding else "none")
        ).strip().lower()
        bounded_limit = max(1, min(int(limit or 10), 200))
        q = query or ""
        sess = session_id or str(matched.get("session_id") or "default")
        thread = thread_id or str(matched.get("thread_id") or sess)
        usr = user_id or str(matched.get("user_id") or "default")
        ns = entity_namespace or str(matched.get("entity_namespace") or "default")
        tpc = topic or q or str(matched.get("topic") or "general")
        binds: list[tuple[str, Any]] = []

        def _b(value: Any, typ: str = "TEXT") -> str:
            binds.append((typ, value))
            return "?"

        if enable_embedding and (q or agent_type == "semantic_vector"):
            embed_model = str(matched.get("embedding_model") or "").split(" ")[0]
            # BUG-MEM-RCL-01: AI_EMBED needs the model as a string literal; only allowlisted names
            # are emitted (via sql_string_literal), the query text stays a ``?`` binding.
            is_multi = embed_model in _sql_safety.AI_MULTI_EMBED_MODELS
            try:
                model_lit = _sql_safety.sql_string_literal(_sql_safety.validate_embed_model(embed_model, multi=is_multi))
            except _sql_safety.SqlSafetyError:
                model_lit = ""
            if not model_lit:
                return {
                    "ok": False,
                    "recalled": False,
                    "error_code": "NOT_CONFIGURED",
                    "message": "Memory has embeddings enabled but no valid embedding_model is registered.",
                    "memory_ref": short_ref,
                    "query_id": None,
                    "rows": [],
                }
            dim = int(matched.get("dimension") or 0)
            if is_multi:
                score_expr = f"VECTOR_COSINE_SIMILARITY(embedding, AI_MULTI_EMBED({model_lit}, {_b(q)}))"
            elif embedding_provider in ("", "none", "snowflake_cortex"):
                score_expr = f"VECTOR_COSINE_SIMILARITY(embedding, AI_EMBED({model_lit}, {_b(q)}))"
            else:
                if dim <= 0:
                    return {
                        "ok": False,
                        "recalled": False,
                        "error_code": "NOT_CONFIGURED",
                        "message": "External embedding provider requires a registered vector dimension.",
                        "memory_ref": short_ref,
                        "query_id": None,
                        "rows": [],
                    }
                score_expr = f"VECTOR_COSINE_SIMILARITY(embedding, CAST(:query_embedding_vec AS VECTOR(FLOAT, {dim})))"
            recall_sql = (
                f"SELECT chunk_id, memory_name, text_chunk, metadata, {score_expr} AS score "
                f"FROM {fqn} ORDER BY score DESC LIMIT {bounded_limit}"
            )
        elif canonical_type == "graph_state_checkpointer":
            recall_sql = (
                f"SELECT state_blob, checkpoint_ns, step_id FROM {fqn} "
                f"WHERE thread_id = {_b(thread)} ORDER BY step_id DESC LIMIT 1"
            )
        elif canonical_type == "buffer_window_memory":
            if agent_type == "episodic_session":
                recall_sql = (
                    f"SELECT * FROM {fqn} WHERE session_id = {_b(sess)} "
                    f"ORDER BY created_at DESC LIMIT {bounded_limit}"
                )
            else:
                win_k = max(1, min(int(matched.get("window_k") or bounded_limit), 200))
                recall_sql = (
                    f"SELECT sender, message_text, created_at FROM {fqn} "
                    f"WHERE session_id = {_b(sess)} ORDER BY created_at DESC LIMIT {win_k}"
                )
        elif canonical_type == "summary_memory":
            recall_sql = (
                f"SELECT running_summary, last_turn_id FROM {fqn} "
                f"WHERE session_id = {_b(sess)} LIMIT 1"
            )
        elif canonical_type == "user_profile_entity_memory":
            if agent_type == "structured_long_term" and not matched.get("user_id") and not matched.get("entity_namespace"):
                where_clause = f" WHERE text_chunk ILIKE {_b('%' + q + '%')}" if q else ""
                recall_sql = f"SELECT * FROM {fqn}{where_clause} ORDER BY created_at DESC LIMIT {bounded_limit}"
            else:
                recall_sql = (
                    f"SELECT profile_json FROM {fqn} "
                    f"WHERE user_id = {_b(usr)} AND entity_namespace = {_b(ns)} LIMIT 1"
                )
        elif canonical_type == "episodic_memory":
            few_shot_k = max(1, min(int(matched.get("few_shot_k") or bounded_limit), 200))
            min_score = matched.get("outcome_score_threshold")
            score_filter = f" AND outcome_score >= {_b(float(min_score), 'REAL')}" if min_score is not None else ""
            recall_sql = (
                f"SELECT topic, tool_used, outcome_score, episode_text FROM {fqn} "
                f"WHERE topic = {_b(tpc)}{score_filter} ORDER BY outcome_score DESC, created_at DESC LIMIT {few_shot_k}"
            )
        else:
            where_clause = f" WHERE text_chunk ILIKE {_b('%' + q + '%')}" if q else ""
            recall_sql = f"SELECT * FROM {fqn}{where_clause} ORDER BY created_at DESC LIMIT {bounded_limit}"

        sql_bindings = {str(i + 1): {"type": t, "value": str(v)} for i, (t, v) in enumerate(binds)}
        res = self.execute_sql(recall_sql, bindings=sql_bindings or None)
        ok = bool(res.get("ok"))
        out: dict[str, Any] = {
            "ok": ok,
            "recalled": ok,
            "memory_ref": short_ref,
            "memory": matched,
            "agent_memory_type": agent_type,
            "canonical_memory_type": canonical_type,
            "enable_embedding": enable_embedding,
            "embedding_provider": embedding_provider,
            "needs_embedding_badge": type_spec["needs_embedding_badge"],
            "how_to_feed_without_embedding": type_spec["how_to_feed_without_embedding"],
            "table_fqn": fqn,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "recall_sql": recall_sql,
            "query_id": res.get("statement_handle") if ok else None,
            "rows": res.get("rows", []) if ok else [],
        }
        if not ok:
            out["error_code"] = res.get("error_code") or ("NOT_CONFIGURED" if res.get("mode") == "offline" else "SNOWFLAKE_ERROR")
            out["message"] = res.get("message") or "Snowflake did not execute the recall query."
        # TR-01: audit the recall only now, with the real SQL outcome (metadata only, never rows or query text).
        emit_audit(
            self.record_audit_event,
            AuditAction.MEMORY_RECALL,
            res,
            target_object=fqn,
            details={
                "memory_base_id": matched.get("memory_id"),
                "name": matched.get("name"),
                "table_fqn": fqn,
                "top_k": bounded_limit,
                "n_results": len(out["rows"]),
            },
            flow_id=matched.get("flow_id"),
        )
        return out

    def create_or_register_memory(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create or register a Snowflake Agent Memory across all 5 canonical types, Branch A (`Database ▸ Schema`) vs Branch B (`Snowflake Postgres` :5432), 4 Snowflake embedding sources, and `NO_INGEST` LLM preprocessing (`W2-R4-02..W2-R4-05`)."""
        name = str(body.get("name") or "").strip()
        raw_flow_id = str(body.get("flow_id") or "").strip() or CURRENT_SNOWFLAKE_FLOW_ID.get()
        norm_fid = self._normalize_flow_id(str(raw_flow_id)) if raw_flow_id else None
        flow_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context

        ctx_dbs = list(flow_ctx.get("databases") or [flow_ctx.get("database") or self.database])
        default_db = str(flow_ctx.get("database") or (ctx_dbs[0] if ctx_dbs else self.database) or self.database).strip().upper()
        default_sch_list = list((flow_ctx.get("schemas_by_db") or {}).get(default_db) or [flow_ctx.get("schema") or self.schema])
        default_sch = str(flow_ctx.get("schema") or (default_sch_list[0] if default_sch_list else self.schema) or self.schema).strip().upper()

        db = str(body.get("database") or default_db).strip().upper()
        sch = str(body.get("schema") or default_sch).strip().upper()
        raw_agent_type = str(body.get("agent_memory_type") or body.get("memory_type") or "").strip().lower()
        category = str(body.get("memory_category") or "").strip().lower()
        backend = str(body.get("vector_database") or body.get("storage_backend") or body.get("recommended_object") or "").strip()

        if raw_agent_type in ("business_ontology_cortex_sense", "cortex_sense", "business_ontology"):
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": "Memory type '#6 Business Ontology / Cortex Sense' is Coming Soon and disabled in this release (W2-R4-02).",
                "active_role": self.active_role,
            }

        raw_storage_family = str(body.get("storage_family") or "").strip().lower()
        is_postgres_branch = bool(
            raw_storage_family in ("snowflake_postgres", "postgres", "branch_b")
            or body.get("postgres_instance")
            or "POSTGRES" in backend.upper()
            or category.startswith("postgres")
        )
        storage_family = "snowflake_postgres" if is_postgres_branch else "snowflake_sql"

        storage_mode = str(body.get("storage_mode") or "").strip().lower()
        create_physical = bool(
            body.get("create_new_physical_store", body.get("create_physical_resource", storage_mode != "bind_existing"))
        )
        safe_table = "".join(c if c.isalnum() or c == "_" else "_" for c in (name or "MEMORY_STORE")).upper()

        pg_instances_in_ctx = list(
            flow_ctx.get("postgres_instances")
            or self.selected_context.get("postgres_instances")
            or ["LANGFLOW_PG_INSTANCE"]
        )
        postgres_instance = (
            str(body.get("postgres_instance") or (pg_instances_in_ctx[0] if pg_instances_in_ctx else "LANGFLOW_PG_INSTANCE")).strip()
            if is_postgres_branch
            else None
        )
        pg_database = str(body.get("pg_database") or "postgres").strip() if is_postgres_branch else None
        pg_schema = str(body.get("pg_schema") or "public").strip() if is_postgres_branch else None

        if body.get("table_fqn"):
            fqn = str(body.get("table_fqn")).strip()
        elif is_postgres_branch and (body.get("storage_family") or body.get("postgres_instance") or body.get("pg_table")):
            raw_pg_tbl = str(body.get("pg_table") or body.get("table_name") or f"mem_{safe_table.lower()}").strip()
            short_pg_tbl = raw_pg_tbl.split(".")[-1]
            fqn = f"{postgres_instance}:5432/{pg_database}.{pg_schema}.{short_pg_tbl}"
        else:
            fqn = f"{db}.{sch}.MEM_{safe_table}"

        if create_physical:
            denied = self.check_write_privilege_or_deny("MEMORY_CREATE_PHYSICAL", fqn)
            if denied:
                return denied
        else:
            if self.active_role.upper() in ("UNAUTHORIZED_ROLE", "NO_SELECT_ROLE") or body.get("deny_bind_existing") is True:
                msg = (
                    f"Active role ({self.active_role}) does not have the MEMORY_USE_EXISTING "
                    f"(SELECT / USAGE) privilege on {fqn}."
                )
                self.record_audit_event(
                    action=AuditAction.AUTHZ_ROLE_DENIED.value,
                    outcome="DENIED",
                    error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                    target_object=fqn,
                    details={"object": fqn, "required_privilege": "SELECT / USAGE (bind existing memory)"},
                )
                return {
                    "ok": False,
                    "status_code": 403,
                    "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                    "message": msg,
                    "active_role": self.active_role,
                    "role": self.active_role,
                    "required_privilege": "MEMORY_USE_EXISTING (SELECT / USAGE)",
                    "object_fqn": fqn,
                    "context_epoch": self.context_epoch,
                    "role_privileges": self.role_privileges,
                }

        # Determine canonical memory type and whether embedding is enabled
        if "enable_embedding" in body and body.get("enable_embedding") is not None:
            enable_embedding = bool(body.get("enable_embedding"))
        elif raw_agent_type == "semantic_vector":
            enable_embedding = True
        elif raw_agent_type in CANONICAL_MEMORY_TYPES_BY_ID or raw_agent_type in LEGACY_MEMORY_TYPE_ALIASES:
            enable_embedding = False
        else:
            enable_embedding = bool(
                category in ("vector_table", "postgres_pgvector", "cortex_search")
                or "VECTOR" in backend.upper()
                or "PGVECTOR" in backend.upper()
                or "CORTEX SEARCH" in backend.upper()
            ) and category not in ("structured", "structured_table", "semi_structured", "postgres_non_vector")

        if raw_agent_type in CANONICAL_MEMORY_TYPES_BY_ID:
            canonical_type = raw_agent_type
            agent_memory_type = raw_agent_type
        elif raw_agent_type in LEGACY_MEMORY_TYPE_ALIASES:
            canonical_type = LEGACY_MEMORY_TYPE_ALIASES[raw_agent_type]
            agent_memory_type = raw_agent_type
        elif enable_embedding:
            canonical_type = "episodic_memory"
            agent_memory_type = "semantic_vector"
        elif category == "postgres_non_vector" or "HYBRID" in str(body.get("table_type") or backend).upper():
            canonical_type = "buffer_window_memory"
            agent_memory_type = "episodic_session"
        else:
            canonical_type = "user_profile_entity_memory"
            agent_memory_type = "structured_long_term"

        # Enforce embedding locked OFF for #1 graph_state_checkpointer and #3 summary_memory (W2-R4-04)
        if canonical_type in ("graph_state_checkpointer", "summary_memory"):
            if body.get("reject_locked_embedding") is True and enable_embedding:
                return {
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": f"Embedding vectorization is locked OFF for memory type '{canonical_type}' (W2-R4-04).",
                    "active_role": self.active_role,
                }
            enable_embedding = False

        type_spec = CANONICAL_MEMORY_TYPES_BY_ID[canonical_type]
        if not backend:
            if is_postgres_branch:
                if canonical_type == "graph_state_checkpointer":
                    backend = "Snowflake Postgres (PostgresSaver)"
                elif canonical_type == "user_profile_entity_memory":
                    backend = "Snowflake Postgres (PostgresStore)"
                elif enable_embedding:
                    backend = "Snowflake Postgres (pgvector)"
                else:
                    backend = "Snowflake Postgres (Relational / JSONB)"
            elif agent_memory_type == "episodic_session" or canonical_type in ("graph_state_checkpointer", "buffer_window_memory", "summary_memory"):
                backend = "Hybrid Table"
            elif agent_memory_type == "structured_long_term" or canonical_type == "user_profile_entity_memory":
                backend = "Semi-Structured (VARIANT)" if category == "semi_structured" else "Standard Table"
            elif enable_embedding:
                backend = "Standard Table (VECTOR)"
            else:
                backend = type_spec["recommended_snowflake_objects"][0]

        if not category:
            if (is_postgres_branch or "POSTGRES" in backend.upper()) and ("PGVECTOR" in backend.upper() or enable_embedding):
                category = "postgres_pgvector"
            elif is_postgres_branch or "POSTGRES" in backend.upper():
                category = "postgres_non_vector"
            elif "CORTEX SEARCH" in backend.upper():
                category = "cortex_search"
            elif enable_embedding:
                category = "vector_table"
            elif any(t in backend.upper() for t in ("VARIANT", "OBJECT", "ARRAY")) or canonical_type == "user_profile_entity_memory":
                category = "semi_structured" if any(t in backend.upper() for t in ("VARIANT", "OBJECT", "ARRAY")) else "structured"
            else:
                category = "structured"

        raw_tbl_type = str(body.get("table_type") or "").strip().upper()
        if "DYNAMIC" in raw_tbl_type or "DYNAMIC" in backend.upper():
            table_type = "DYNAMIC"
        elif "INTERACTIVE" in raw_tbl_type or "INTERACTIVE" in backend.upper():
            table_type = "INTERACTIVE"
        elif "ICEBERG" in raw_tbl_type or "ICEBERG" in backend.upper():
            table_type = "ICEBERG"
        elif "HYBRID" in raw_tbl_type:
            table_type = "HYBRID"
        elif "STANDARD" in raw_tbl_type:
            table_type = "STANDARD"
        elif (
            "HYBRID" in backend.upper()
            or agent_memory_type == "episodic_session"
            or canonical_type in ("graph_state_checkpointer", "buffer_window_memory", "summary_memory")
        ):
            table_type = "HYBRID"
        else:
            table_type = "STANDARD"

        semi_type = str(body.get("semi_structured_type") or body.get("payload_column_type") or "").strip().upper()
        embedding_provider = str(
            body.get("embedding_provider") or ("snowflake_cortex" if enable_embedding else "none")
        ).strip().lower()
        if enable_embedding and embedding_provider == "none":
            embedding_provider = "snowflake_cortex"
        embedding_model = str(body.get("embedding_model") or "snowflake-arctic-embed-m-v1.5").strip()
        elem_type = str(body.get("vector_element_type") or "FLOAT").strip().upper()
        batch_size = int(body.get("batch_size") or 64)
        ingestion_threshold = max(1, int(body.get("ingestion_threshold") or 1))
        auto_capture = bool(body.get("auto_capture", True))
        filter_by_session = bool(
            body.get(
                "filter_by_session",
                type_spec.get("filter_by_session", canonical_type in ("graph_state_checkpointer", "buffer_window_memory", "summary_memory")),
            )
        )
        tool_mode = bool(body.get("tool_mode", canonical_type == "episodic_memory"))

        raw_preproc = body.get("llm_preprocessing")
        if isinstance(raw_preproc, dict):
            preproc_enabled = bool(raw_preproc.get("enabled", True))
            preproc_engine = str(
                raw_preproc.get("engine")
                or raw_preproc.get("provider")
                or body.get("summarizer_engine")
                or "proxy_chat"
            ).strip()
            preproc_model = str(
                raw_preproc.get("model")
                or body.get("summarizer_model")
                or (
                    "AI_SUMMARIZE_AGG"
                    if preproc_engine == "cortex_summarize_agg"
                    else "mistral-large2"
                    if preproc_engine == "cortex_sql"
                    else "openai-gpt-6-luna"
                    if canonical_type == "summary_memory"
                    else "claude-sonnet-5-5"
                )
            ).strip()
            preproc_kill_phrase = str(raw_preproc.get("kill_phrase") or body.get("kill_phrase") or "NO_INGEST").strip() or "NO_INGEST"
            llm_preprocessing: dict[str, Any] | None = {
                "enabled": preproc_enabled,
                "engine": preproc_engine,
                "model": preproc_model,
                "kill_phrase": preproc_kill_phrase,
            }
        elif (
            body.get("llm_preprocessing_enabled") is not None
            or body.get("llm_preprocessing_engine") is not None
            or body.get("summarizer_engine") is not None
            or body.get("kill_phrase") is not None
            or canonical_type == "summary_memory"
        ):
            preproc_enabled = bool(body.get("llm_preprocessing_enabled", canonical_type == "summary_memory"))
            preproc_engine = str(
                body.get("llm_preprocessing_engine")
                or body.get("llm_preprocessing_provider")
                or body.get("summarizer_engine")
                or ("cortex_sql" if canonical_type == "summary_memory" else "proxy_messages")
            ).strip()
            preproc_model = str(
                body.get("llm_preprocessing_model")
                or body.get("summarizer_model")
                or (
                    "AI_SUMMARIZE_AGG"
                    if preproc_engine == "cortex_summarize_agg"
                    else "mistral-large2"
                    if preproc_engine == "cortex_sql"
                    else "openai-gpt-6-luna"
                    if canonical_type == "summary_memory"
                    else "claude-sonnet-5-5"
                )
            ).strip()
            preproc_kill_phrase = str(body.get("kill_phrase") or "NO_INGEST").strip() or "NO_INGEST"
            llm_preprocessing = {
                "enabled": preproc_enabled,
                "engine": preproc_engine,
                "model": preproc_model,
                "kill_phrase": preproc_kill_phrase,
            }
        else:
            preproc_kill_phrase = "NO_INGEST"
            llm_preprocessing = None

        endpoint_url = str(body.get("endpoint_url") or "").strip() or None
        custom_dim = body.get("custom_dimension")
        custom_dim_int = int(custom_dim) if custom_dim is not None else None

        is_vector = bool(enable_embedding)

        if is_vector and ("ICEBERG" in table_type or "ICEBERG" in backend.upper()):
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": "Native VECTOR columns are not supported on Snowflake Iceberg Tables (HUB-W2-05).",
                "active_role": self.active_role,
            }
        if body.get("vector_inside_variant") is True:
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": "VECTOR columns cannot be nested inside VARIANT columns (HUB-W2-05).",
                "active_role": self.active_role,
            }
        if elem_type not in {"FLOAT", "INT"}:
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": f"Unsupported VECTOR element type '{elem_type}'. Expected FLOAT or INT.",
                "active_role": self.active_role,
            }
        if is_vector and embedding_provider not in EXTERNAL_EMBEDDING_PROVIDERS:
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": (
                    f"Unsupported embedding_provider '{embedding_provider}'. "
                    f"Expected one of {sorted(EXTERNAL_EMBEDDING_PROVIDERS)}."
                ),
                "active_role": self.active_role,
            }

        expected_dim = resolve_memory_embedding_dimension(
            embedding_model,
            embedding_provider=embedding_provider,
            custom_dimension=custom_dim_int,
            default=768,
        )
        requested_dim = int(
            body.get("vector_dimension")
            or body.get("dimension")
            or custom_dim_int
            or expected_dim
        )
        if is_vector and (requested_dim < 1 or requested_dim > 4096):
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": f"VECTOR dimension {requested_dim} is out of bounds (1..4096).",
                "active_role": self.active_role,
            }
        if (
            is_vector
            and embedding_provider == "snowflake_cortex"
            and ("vector_dimension" in body or "dimension" in body)
            and requested_dim != expected_dim
        ):
            return {
                "ok": False,
                "status_code": 422,
                "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                "message": (
                    f"VECTOR dimension {requested_dim} does not match selected AI_EMBED model "
                    f"'{embedding_model}' ({expected_dim}d)."
                ),
                "active_role": self.active_role,
            }

        dim = (
            requested_dim
            if embedding_provider in ("custom_endpoint", "spcs_model_registry", "spcs_llm_proxy")
            else expected_dim
        )
        table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
        vec_col_ddl = f", embedding VECTOR({elem_type}, {dim})" if is_vector else ""

        # Object-specific peculiarities (W2-R4-04 Step 2)
        raw_pk = str(body.get("primary_key") or type_spec.get("default_hybrid_pk") or "(chunk_id)").strip().strip("()")
        pk_cols = [
            c.strip()
            for c in raw_pk.split(",")
            if c.strip() and c.strip().lower() not in ("embedding", "task_embedding", "vector")
        ]
        if not pk_cols:
            pk_cols = ["chunk_id"]
        sanitized_pk_expr = f"({', '.join(pk_cols)})"

        raw_sec_idx = str(body.get("secondary_index") or "").strip().strip("()")
        sec_idx_cols = [
            c.strip()
            for c in raw_sec_idx.split(",")
            if c.strip() and c.strip().lower() not in ("embedding", "task_embedding", "vector")
        ]
        sec_idx_ddl = (
            f", INDEX idx_{safe_table.lower()} ({', '.join(sec_idx_cols)})"
            if table_type == "HYBRID" and sec_idx_cols
            else ""
        )

        target_lag = str(body.get("target_lag") or ("1 minute" if table_type == "INTERACTIVE" else "5 minutes")).strip()
        warehouse_name = str(
            body.get("interactive_warehouse" if table_type == "INTERACTIVE" else "warehouse")
            or body.get("warehouse")
            or flow_ctx.get("warehouse")
            or self.warehouse
            or ("INTERACTIVE_WH" if table_type == "INTERACTIVE" else "COMPUTE_WH")
        ).strip()
        refresh_mode = str(body.get("refresh_mode") or "INCREMENTAL").strip().upper()
        source_table = str(body.get("source_table") or f"{db}.{sch}.RAW_AGENT_EVENTS").strip()
        raw_cluster = str(
            body.get("cluster_by")
            or ("user_id, entity_namespace" if canonical_type == "user_profile_entity_memory" else "user_id, topic")
        ).strip().strip("()")
        cluster_cols = [
            c.strip()
            for c in raw_cluster.split(",")
            if c.strip() and c.strip().lower() not in ("embedding", "task_embedding", "vector")
        ] or ["user_id"]
        sanitized_cluster_by = ", ".join(cluster_cols)

        retention_days = body.get("retention_days")
        retention_ddl = (
            f" DATA_RETENTION_TIME_IN_DAYS = {int(retention_days)}"
            if retention_days is not None and table_type == "STANDARD"
            else ""
        )

        default_pg_mode = (
            "PostgresSaver"
            if canonical_type == "graph_state_checkpointer"
            else "PostgresStore JSONB"
            if canonical_type == "user_profile_entity_memory"
            else "JSONB + pgvector"
            if is_vector
            else "Relational / JSONB"
        )
        pg_table_mode = str(body.get("pg_table_mode") or default_pg_mode).strip() if is_postgres_branch else None
        pg_table_short = (
            str(
                body.get("pg_table")
                or body.get("table_name")
                or (
                    "langgraph_checkpoints"
                    if canonical_type == "graph_state_checkpointer"
                    else "chat_message_history"
                    if canonical_type == "buffer_window_memory"
                    else f"mem_{safe_table.lower()}"
                )
            )
            .strip()
            .split(".")[-1]
            if is_postgres_branch
            else None
        )
        gin_index_jsonb = bool(body.get("gin_index_jsonb", True)) if is_postgres_branch else False

        # BUG-UI-SQLI-05: every body/Context value that reaches the DDL is validated first.
        try:
            if is_postgres_branch:
                for _pg_val, _pg_field in (
                    (postgres_instance, "postgres_instance"),
                    (pg_database, "pg_database"),
                    (pg_schema, "pg_schema"),
                    (pg_table_short, "pg_table"),
                ):
                    if _pg_val:
                        _sql_safety.validate_identifier(_pg_val, _pg_field)
            else:
                _sql_safety.validate_identifier(db, "database")
                _sql_safety.validate_identifier(sch, "schema")
                fqn = _sql_safety.validate_fqn(fqn, "table_fqn", min_parts=3, max_parts=3)
                for _col in pk_cols + sec_idx_cols:
                    _sql_safety.validate_identifier(_col, "primary_key/secondary_index")
                if table_type in ("DYNAMIC", "INTERACTIVE"):
                    _sql_safety.validate_identifier(warehouse_name, "warehouse")
                    source_table = _sql_safety.validate_fqn(source_table, "source_table", min_parts=3, max_parts=3)
                    if not _MEMORY_TARGET_LAG_RE.match(target_lag):
                        raise _sql_safety.SqlSafetyError(f"Invalid target_lag: {target_lag!r}", "target_lag")
                if table_type == "INTERACTIVE":
                    for _col in cluster_cols:
                        _sql_safety.validate_identifier(_col, "cluster_by")
                if table_type == "DYNAMIC":
                    refresh_mode = _sql_safety.validate_enum(refresh_mode, ("AUTO", "FULL", "INCREMENTAL"), "refresh_mode")
                if category == "cortex_search" or "CORTEX SEARCH" in backend.upper():
                    _cs_wh = str(body.get("warehouse") or self.warehouse or "").strip()
                    if _cs_wh:
                        _sql_safety.validate_identifier(_cs_wh, "warehouse")
                    if not re.match(r"^[A-Za-z0-9._-]{1,128}$", str(embedding_model or "").split(" ")[0] or "x"):
                        raise _sql_safety.SqlSafetyError("Invalid embedding_model.", "embedding_model")
        except _sql_safety.SqlSafetyError as exc:
            return {
                "ok": False,
                "status_code": 400,
                "error_code": "INVALID_IDENTIFIER",
                "message": str(exc),
                "active_role": self.active_role,
            }

        if not create_physical:
            ddl = f"-- Bound to existing Snowflake Memory store {fqn} (MEMORY_USE_EXISTING)"
        elif is_postgres_branch and (category == "postgres_pgvector" or is_vector or "PGVECTOR" in str(pg_table_mode or "").upper()):
            pg_sch_eff = (pg_schema or sch or "public").lower()
            pg_tbl_eff = pg_table_short or f"mem_{safe_table.lower()}"
            gin_ddl = (
                f" CREATE INDEX IF NOT EXISTS idx_{pg_tbl_eff}_gin ON {pg_sch_eff}.{pg_tbl_eff} USING GIN (metadata);"
                if gin_index_jsonb
                else ""
            )
            ddl = (
                f"CREATE EXTENSION IF NOT EXISTS vector; "
                f"CREATE TABLE IF NOT EXISTS {pg_sch_eff}.{pg_tbl_eff} ("
                f"chunk_id TEXT PRIMARY KEY, user_id TEXT, session_id TEXT, topic TEXT, tool_used TEXT, "
                f"outcome_score DOUBLE PRECISION DEFAULT 1.0, memory_name TEXT, text_chunk TEXT, "
                f"embedding vector({dim}), metadata JSONB DEFAULT '{{}}'::jsonb, "
                f"created_by_role TEXT, created_at TIMESTAMPTZ DEFAULT NOW());{gin_ddl}"
            )
        elif is_postgres_branch:
            pg_sch_eff = (pg_schema or sch or "public").lower()
            pg_tbl_eff = pg_table_short or f"mem_{safe_table.lower()}"
            if pg_table_mode == "PostgresSaver" or canonical_type == "graph_state_checkpointer":
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {pg_sch_eff}.{pg_tbl_eff} ("
                    f"thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT 'default', "
                    f"checkpoint_id TEXT NOT NULL, step_id INTEGER DEFAULT 0, memory_name TEXT, "
                    f"checkpoint JSONB NOT NULL DEFAULT '{{}}'::jsonb, metadata JSONB DEFAULT '{{}}'::jsonb, "
                    f"created_by_role TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), "
                    f"PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id));"
                )
            elif pg_table_mode == "PostgresStore JSONB" or canonical_type == "user_profile_entity_memory":
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {pg_sch_eff}.{pg_tbl_eff} ("
                    f"user_id TEXT NOT NULL, entity_namespace TEXT NOT NULL DEFAULT 'default', "
                    f"memory_key TEXT NOT NULL, memory_name TEXT, "
                    f"profile_json JSONB NOT NULL DEFAULT '{{}}'::jsonb, metadata JSONB DEFAULT '{{}}'::jsonb, "
                    f"created_by_role TEXT, updated_at TIMESTAMPTZ DEFAULT NOW(), "
                    f"PRIMARY KEY (user_id, entity_namespace, memory_key));"
                )
            else:
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {pg_sch_eff}.{pg_tbl_eff} ("
                    f"session_id TEXT, thread_id TEXT, user_id TEXT, entity_namespace TEXT, "
                    f"chunk_id TEXT PRIMARY KEY, memory_name TEXT, sender TEXT, message_text TEXT, text_chunk TEXT, "
                    f"payload JSONB DEFAULT '{{}}'::jsonb, metadata JSONB DEFAULT '{{}}'::jsonb, "
                    f"created_by_role TEXT, created_at TIMESTAMPTZ DEFAULT NOW());"
                )
        elif category == "cortex_search" or "CORTEX SEARCH" in backend.upper():
            wh = str(body.get("warehouse") or self.warehouse or "<WAREHOUSE>").strip()
            clean_embed = embedding_model.split(" ")[0]
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128), memory_name VARCHAR(256), text_chunk VARCHAR, "
                f"created_by_role VARCHAR(128), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()); "
                f"CREATE OR REPLACE CORTEX SEARCH SERVICE {db}.{sch}.SVC_{safe_table} "
                f"ON text_chunk ATTRIBUTES memory_name, created_by_role "
                f"WAREHOUSE = {wh} TARGET_LAG = '1 minute' EMBEDDING_MODEL = '{clean_embed}' "
                f"AS (SELECT * FROM {fqn});"
            )
        elif table_type == "DYNAMIC":
            vec_sel = (
                f", CAST(embedding AS VECTOR({elem_type}, {dim})) AS embedding"
                if is_vector
                else ""
            )
            ddl = (
                f"CREATE OR REPLACE DYNAMIC TABLE {fqn} "
                f"TARGET_LAG = '{target_lag}' WAREHOUSE = {warehouse_name} REFRESH_MODE = {refresh_mode} "
                f"AS SELECT chunk_id, user_id, entity_namespace, topic, tool_used, outcome_score, "
                f"profile_json, episode_text, text_chunk{vec_sel}, created_by_role, created_at "
                f"FROM {source_table};"
            )
        elif table_type == "INTERACTIVE":
            vec_sel = (
                f", CAST(embedding AS VECTOR({elem_type}, {dim})) AS embedding"
                if is_vector
                else ""
            )
            ddl = (
                f"CREATE OR REPLACE INTERACTIVE TABLE {fqn} "
                f"CLUSTER BY ({sanitized_cluster_by}) TARGET_LAG = '{target_lag}' WAREHOUSE = {warehouse_name} "
                f"AS SELECT chunk_id, user_id, entity_namespace, topic, tool_used, outcome_score, "
                f"profile_json, episode_text, text_chunk{vec_sel}, created_by_role, created_at "
                f"FROM {source_table};"
            )
        elif canonical_type == "graph_state_checkpointer":
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"thread_id VARCHAR(128), checkpoint_ns VARCHAR(128) DEFAULT 'default', "
                f"step_id INTEGER DEFAULT 0, memory_name VARCHAR(256), state_blob VARIANT, "
                f"ttl_hours INTEGER DEFAULT 24{vec_col_ddl}, metadata VARIANT, "
                f"created_by_role VARCHAR(128), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY {sanitized_pk_expr}{sec_idx_ddl}){retention_ddl};"
            )
        elif canonical_type == "buffer_window_memory":
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"session_id VARCHAR(128), turn_id VARCHAR(128), memory_name VARCHAR(256), "
                f"sender VARCHAR(64), message_text VARCHAR, text_chunk VARCHAR{vec_col_ddl}, "
                f"metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY {sanitized_pk_expr}{sec_idx_ddl}){retention_ddl};"
            )
        elif canonical_type == "summary_memory":
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"session_id VARCHAR(128), memory_name VARCHAR(256), "
                f"running_summary VARCHAR, last_turn_id VARCHAR(128), "
                f"summarizer_model VARCHAR(128) DEFAULT 'mistral-large2', "
                f"max_summary_tokens INTEGER DEFAULT 512{vec_col_ddl}, metadata VARIANT, "
                f"created_by_role VARCHAR(128), updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY {sanitized_pk_expr}{sec_idx_ddl}){retention_ddl};"
            )
        elif canonical_type == "user_profile_entity_memory":
            col_type = semi_type if semi_type in ("VARIANT", "OBJECT", "ARRAY") else "VARIANT"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128), user_id VARCHAR(128), "
                f"entity_namespace VARCHAR(128) DEFAULT 'default', memory_key VARCHAR(128) DEFAULT 'profile', "
                f"memory_name VARCHAR(256), profile_json {col_type}, payload {col_type}, "
                f"text_chunk VARCHAR{vec_col_ddl}, metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY {sanitized_pk_expr}{sec_idx_ddl}){retention_ddl};"
            )
        elif canonical_type == "episodic_memory" and not is_vector:
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128), user_id VARCHAR(128) DEFAULT 'default', memory_name VARCHAR(256), "
                f"topic VARCHAR(256), tool_used VARCHAR(256), outcome_score FLOAT DEFAULT 1.0, "
                f"episode_text VARCHAR, trajectory_json VARIANT, text_chunk VARCHAR, metadata VARIANT, "
                f"created_by_role VARCHAR(128), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY {sanitized_pk_expr}{sec_idx_ddl}){retention_ddl};"
            )
        else:
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128), user_id VARCHAR(128) DEFAULT 'default', memory_name VARCHAR(256), "
                f"topic VARCHAR(256), tool_used VARCHAR(256), outcome_score FLOAT DEFAULT 1.0, "
                f"episode_text VARCHAR, trajectory_json VARIANT, text_chunk VARCHAR, "
                f"embedding VECTOR({elem_type}, {dim}), metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY {sanitized_pk_expr}{sec_idx_ddl}){retention_ddl};"
            )

        if body.get("dry_run") or not create_physical:
            exec_res = {"ok": True, "dry_run": True}
        elif is_postgres_branch and not self._is_execute_sql_mocked():
            exec_res = {"ok": True, "postgres_ddl": True}
        else:
            exec_res = self.execute_sql(ddl)
            if (
                not exec_res.get("ok")
                and not self._is_execute_sql_mocked()
                and "CREATE HYBRID TABLE" in ddl
            ):
                fallback_std_ddl = ddl.replace("CREATE HYBRID TABLE", "CREATE TABLE")
                exec_res = self.execute_sql(fallback_std_ddl)
            if not exec_res.get("ok"):
                err_code = str(exec_res.get("error_code") or ("NOT_CONFIGURED" if exec_res.get("mode") == "offline" else "SNOWFLAKE_ERROR"))
                emit_audit(
                    self.record_audit_event,
                    AuditAction.MEMORY_BASE_CREATE,
                    exec_res,
                    target_object=fqn,
                    details={"name": name, "table_fqn": fqn},
                    flow_id=norm_fid,
                )
                return {
                    "ok": False,
                    "status_code": 501 if err_code == "NOT_CONFIGURED" else 422,
                    "error_code": err_code,
                    "message": str(exec_res.get("message") or "Snowflake did not create the memory table; nothing was registered."),
                    "query_id": exec_res.get("statement_handle"),
                    "active_role": self.active_role,
                }
        how_to_feed_sql = type_spec["how_to_feed_without_embedding"].replace("<fqn>", fqn)
        deterministic_memory_id = compute_deterministic_memory_uuid(name)
        raw_mem_id = str(body.get("memory_id") or "").strip()
        if raw_mem_id:
            try:
                memory_id_val = str(uuid.UUID(raw_mem_id))
            except Exception:  # noqa: BLE001
                memory_id_val = deterministic_memory_id
        else:
            memory_id_val = deterministic_memory_id
        summarizer_engine_val = str(
            body.get("summarizer_engine")
            or (llm_preprocessing.get("engine") if isinstance(llm_preprocessing, dict) else None)
            or ("cortex_sql" if canonical_type == "summary_memory" else "proxy_chat")
        ).strip()
        summarizer_model_val = str(
            body.get("summarizer_model")
            or (llm_preprocessing.get("model") if isinstance(llm_preprocessing, dict) else None)
            or ("AI_SUMMARIZE_AGG" if summarizer_engine_val == "cortex_summarize_agg" else "mistral-large2")
        ).strip()
        summary_trigger_turns_val = max(1, int(body.get("summary_trigger_turns") or 5))
        entry = {
            "id": memory_id_val,
            "memory_id": memory_id_val,
            "langflow_memory_id": memory_id_val,
            "name": name,
            "kb_name": name,
            "flow_id": norm_fid,
            "storage_family": storage_family,
            "database": None if is_postgres_branch and body.get("storage_family") == "snowflake_postgres" else db,
            "schema": None if is_postgres_branch and body.get("storage_family") == "snowflake_postgres" else sch,
            "postgres_instance": postgres_instance,
            "pg_database": pg_database,
            "pg_schema": pg_schema,
            "pg_table": pg_table_short,
            "pg_table_mode": pg_table_mode,
            "gin_index_jsonb": gin_index_jsonb,
            "table_fqn": fqn,
            "agent_memory_type": agent_memory_type,
            "canonical_memory_type": canonical_type,
            "label": type_spec["label"],
            "purpose": type_spec["purpose"],
            "memory_category": category,
            "vector_database": backend,
            "recommended_snowflake_objects": list(type_spec["recommended_snowflake_objects"]),
            "needs_embedding_badge": type_spec["needs_embedding_badge"],
            "how_to_feed_without_embedding": how_to_feed_sql,
            "when_embedding_useful": type_spec["when_embedding_useful"],
            "required_inputs": list(type_spec["required_inputs"]),
            "table_type": "POSTGRES" if is_postgres_branch else table_type,
            "primary_key": sanitized_pk_expr if not is_postgres_branch else "(thread_id, checkpoint_ns, checkpoint_id)",
            "secondary_index": ", ".join(sec_idx_cols) if sec_idx_cols else None,
            "vector_excluded_from_pk_and_index": True,
            "target_lag": target_lag if table_type in ("DYNAMIC", "INTERACTIVE") else None,
            "warehouse": warehouse_name if table_type in ("DYNAMIC", "INTERACTIVE") else None,
            "refresh_mode": refresh_mode if table_type == "DYNAMIC" else None,
            "cluster_by": sanitized_cluster_by if table_type == "INTERACTIVE" else None,
            "semi_structured_type": semi_type or ("VARIANT" if canonical_type == "user_profile_entity_memory" else None),
            "enable_embedding": is_vector,
            "is_vector_memory": is_vector,
            "embedding_provider": embedding_provider if is_vector else "none",
            "embedding_model": embedding_model if is_vector else None,
            "vector_element_type": elem_type if is_vector else None,
            "dimension": dim if is_vector else None,
            "endpoint_url": endpoint_url if is_vector else None,
            "batch_size": batch_size,
            "ingestion_threshold": ingestion_threshold,
            "threshold": ingestion_threshold,
            "auto_capture": auto_capture,
            "filter_by_session": filter_by_session,
            "tool_mode": tool_mode,
            "kill_phrase": preproc_kill_phrase,
            "llm_preprocessing": llm_preprocessing,
            "thread_id": body.get("thread_id") or "thr_default",
            "checkpoint_ns": body.get("checkpoint_ns") or "default",
            "ttl_hours": int(body.get("ttl_hours") or 24),
            "session_id": body.get("session_id") or "sess_default",
            "window_k": int(body.get("window_k") or 8),
            "retention_days": int(retention_days) if retention_days is not None else 90,
            "summarizer_engine": summarizer_engine_val,
            "summarizer_model": summarizer_model_val,
            "summary_trigger_turns": summary_trigger_turns_val,
            "max_summary_tokens": int(body.get("max_summary_tokens") or 512),
            "user_id": body.get("user_id") or "usr_default",
            "entity_namespace": body.get("entity_namespace") or "enterprise_finance",
            "schema_keys": body.get("schema_keys") or ["preferred_currency", "language", "kpi_focus"],
            "topic_column": body.get("topic_column") or "topic",
            "tool_used_column": body.get("tool_used_column") or "tool_used",
            "outcome_score_threshold": float(body.get("outcome_score_threshold") if body.get("outcome_score_threshold") is not None else 0.70),
            "few_shot_k": int(body.get("few_shot_k") or 3),
            "storage_mode": "create_physical" if create_physical else "bind_existing",
            "ddl": ddl,
            "created_by_role": self.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snowflake_created": bool(exec_res.get("ok")) and create_physical,
            "stored_records": list(body.get("stored_records") or []),
        }
        # Replace existing memory entry with same name if present, else append
        # BUG-MEM-API-05: an LF_MEM base is identified by its memory id, never by name (two flows may share a name).
        lf_mem_id = str((body.get("lf_mem") or {}).get("memory_base_id") or "") if isinstance(body.get("lf_mem"), dict) else ""
        existing_idx = next(
            (
                i
                for i, m in enumerate(self.memories_registry)
                if (
                    str(m.get("memory_id") or "").lower() == lf_mem_id.lower()
                    if lf_mem_id
                    else (
                        not isinstance(m.get("lf_mem"), dict)
                        and str(m.get("name") or "").strip().upper() == name.upper()
                        and name
                    )
                )
            ),
            None,
        )
        prev_records = (
            list(self.memories_registry[existing_idx].get("stored_records") or [])
            if existing_idx is not None
            else []
        )
        if body.get("stored_records"):
            entry["stored_records"] = list(body.get("stored_records") or [])
        elif prev_records:
            entry["stored_records"] = prev_records
        else:
            entry["stored_records"] = []

        if existing_idx is not None:
            self.memories_registry[existing_idx] = entry
        else:
            self.memories_registry.append(entry)

        vis_mem = self.selected_context.setdefault("visible_objects", {}).setdefault("memories", [])
        if name and name not in vis_mem:
            vis_mem.append(name)
        if norm_fid:
            f_ctx = self.get_flow_context(norm_fid)
            f_vis_mem = f_ctx.setdefault("visible_objects", {}).setdefault("memories", [])
            if name and name not in f_vis_mem:
                f_vis_mem.append(name)
            self.contexts_by_flow_id[norm_fid] = f_ctx

        if is_postgres_branch and postgres_instance:
            if create_physical or not body.get("_skip_context_auto_register"):
                self.register_object_in_context(
                    category="postgres_instances",
                    object_name=postgres_instance,
                    flow_id=norm_fid,
                )
        elif create_physical and not body.get("_skip_context_auto_register"):
            self.register_object_in_context(
                database=db,
                schema=sch,
                category="tables",
                object_name=fqn,
                flow_id=norm_fid,
            )

        if isinstance(body.get("lf_mem"), dict):
            entry["lf_mem"] = dict(body["lf_mem"])
        lf_user = str(body.get("langflow_user_id") or "").strip()
        if lf_user:
            # BUG-MEM-MB-01: native registration with the caller's user_id; failures are returned, not swallowed.
            try:
                self._persist_memory_in_langflow_sqlite(entry, flow_id=norm_fid, user_id=lf_user)
            except (MemoryBasePersistenceError, sqlite3.Error) as exc:
                if existing_idx is None:
                    self.memories_registry.remove(entry)
                status = exc.http_status if isinstance(exc, MemoryBasePersistenceError) else 500
                return {
                    "ok": False,
                    "status_code": status,
                    "error_code": "PERSISTENCE_ERROR",
                    "message": f"Memory Base could not be registered in Langflow: {exc}",
                    "active_role": self.active_role,
                }
            entry["langflow_registration"] = "REGISTERED"
        else:
            # No authenticated Langflow user (legacy /api/v1/snowflake/memories callers): never invent one.
            entry["langflow_registration"] = "NOT_REGISTERED_NO_LANGFLOW_USER"
        # TR-01: MEMORY_BASE_CREATE only for a real outcome: an executed DDL, or a bind of an existing table
        # (registration persisted above). Dry runs and non-executed branches are not audited as creations.
        if (not exec_res.get("dry_run") and not exec_res.get("postgres_ddl")) or not create_physical:
            emit_audit(
                self.record_audit_event,
                AuditAction.MEMORY_BASE_CREATE,
                exec_res if create_physical else {"ok": True},
                target_object=entry["table_fqn"],
                details={
                    "memory_base_id": entry.get("memory_id"),
                    "name": entry.get("name"),
                    "table_fqn": entry.get("table_fqn"),
                    "embedding_model": entry.get("embedding_model"),
                    "execution_mode": "create" if create_physical else "bind_existing",
                },
                flow_id=norm_fid,
            )
        return {"ok": True, "status_code": 201, "memory": entry, "ddl": ddl}

    def _persist_memory_in_langflow_sqlite(
        self, entry: dict[str, Any], flow_id: str | None = None, user_id: str | None = None
    ) -> None:
        """Persist a Snowflake Memory Base into Langflow's native ``memory_base`` table (BUG-MEM-MB-01).

        The row is keyed exactly like Langflow 1.13's MemoryBase service: the caller's real Langflow
        ``user_id`` and the explicit ``flow_id`` the base is attached to (no first-superuser or most-recent-flow
        fallback), so the native MemoryBaseComponent finds it by ``(name, flow_id, user_id)``. ``kb_name`` holds
        the durable ``LF_MEM`` marker (``LfMemSpec.to_kb_name``). SQLite errors propagate to the caller.
        """
        mem_name = str(entry.get("name") or "SNOWFLAKE_MEMORY").strip()
        det_uuid_str = compute_deterministic_memory_uuid(mem_name)
        try:
            det_uuid = uuid.UUID(str(entry.get("memory_id") or det_uuid_str))
        except Exception:  # noqa: BLE001
            det_uuid = uuid.UUID(det_uuid_str)
        entry["id"] = str(det_uuid)
        entry["memory_id"] = str(det_uuid)
        entry["langflow_memory_id"] = str(det_uuid)

        user_id_hex = _normalize_sqlite_uuid_val(user_id or entry.get("langflow_user_id"), use_hex=True)
        target_flow_hex = _normalize_sqlite_uuid_val(flow_id or entry.get("flow_id"), use_hex=True)
        if not user_id_hex:
            raise MemoryBasePersistenceError("The caller's Langflow user_id is required to register a Memory Base.", 401)
        if not target_flow_hex:
            raise MemoryBasePersistenceError("An explicit flow_id (UUID) is required to attach a Memory Base.", 422)

        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        if not db_path.exists():
            raise MemoryBasePersistenceError(f"Langflow database not found at {db_path}.", 503)
        conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
        try:
            conn.row_factory = sqlite3.Row
            heal_sqlite_invalid_uuid_rows(conn)
            cur = conn.cursor()
            tables = {
                r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if "memory_base" not in tables:
                raise MemoryBasePersistenceError("Langflow 'memory_base' table is missing (unsupported Langflow version).", 503)
            if "user" in tables and not cur.execute(
                "SELECT 1 FROM user WHERE replace(lower(id), '-', '') = ?", (user_id_hex,)
            ).fetchone():
                raise MemoryBasePersistenceError("Unknown Langflow user_id.", 403)
            if "flow" in tables:
                owner_row = cur.execute(
                    "SELECT user_id FROM flow WHERE replace(lower(id), '-', '') = ?", (target_flow_hex,)
                ).fetchone()
                if owner_row is None:
                    raise MemoryBasePersistenceError("flow_id does not exist in Langflow.", 404)
                if owner_row[0] is not None and _normalize_sqlite_uuid_val(owner_row[0], use_hex=True) != user_id_hex:
                    raise MemoryBasePersistenceError("The flow belongs to another Langflow user.", 403)

            lf_mem = entry.get("lf_mem") if isinstance(entry.get("lf_mem"), dict) else None
            if lf_mem:
                from .memories_lf_mem import LfMemSpec as _LfMemSpec  # noqa: PLC0415

                kb_name = _LfMemSpec(**lf_mem).to_kb_name()
            else:
                kb_name = str(entry.get("kb_name") or mem_name)
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

            is_vec = bool(entry.get("enable_embedding"))
            emb_provider = str(entry.get("embedding_provider") or ("Snowflake Cortex" if is_vec else "Snowflake Direct SQL"))
            emb_model = (
                f"{entry.get('embedding_model')} ({entry.get('dimension')}d)"
                if is_vec and entry.get("embedding_model")
                else f"No Embedding ({entry.get('needs_embedding_badge', 'Direct SQL')})"
            )
            backend_label = f"Snowflake · {entry.get('vector_database') or 'Hybrid Table'} ({entry.get('table_fqn')})"
            sf_backend_key = resolve_snowflake_backend_key(entry)
            native_backend_type = native_kb_backend_type_value(sf_backend_key)
            sql_lookup = str(entry.get("how_to_feed_without_embedding") or "")
            instructions = str(
                entry.get("preproc_instructions")
                or (
                    f"[{entry.get('label')}] Store: {entry.get('table_fqn')} | "
                    f"Embedding Required: {entry.get('needs_embedding_badge')} | "
                    f"Lookup SQL: {sql_lookup}"
                )
            )
            model_sel_json = json.dumps(
                {
                    "embedding_provider": emb_provider,
                    "embedding_model": emb_model,
                    "model": [{"name": emb_model, "provider": emb_provider}],
                }
            )
            backend_cfg_json = json.dumps(
                {
                    "mode": "snowflake",
                    "storage_family": entry.get("storage_family", "snowflake_sql"),
                    "table_fqn": entry.get("table_fqn"),
                    "canonical_memory_type": entry.get("canonical_memory_type"),
                    "enable_embedding": is_vec,
                    "backend_type": sf_backend_key,
                    "snowflake_backend_type": sf_backend_key,
                    "snowflake_backend_label": backend_label,
                }
            )

            # A8 Phase C VETO: Snowflake memories are NOT registered in native `knowledge_base`
            # (native KB routes would resolve a local SQLite vector store = split brain).
            # Only `memory_base`, owned by the intercepted /api/v1/memories* routes, is written.
            purge_snowflake_rows_from_native_knowledge_base(conn)

            mb_id_hex = det_uuid.hex
            existing_mb = cur.execute(
                "SELECT id FROM memory_base WHERE id = ? OR (user_id = ? AND name = ?)",
                (mb_id_hex, user_id_hex, mem_name),
            ).fetchone()
            if existing_mb and _normalize_sqlite_uuid_val(existing_mb[0], use_hex=True) != mb_id_hex:
                # BUG-MEM-API-05: native UNIQUE(user_id, name) - never overwrite another Memory Base (other flow).
                raise MemoryBasePersistenceError(
                    f"Memory Base name '{mem_name}' is already used by this user (memory_base {existing_mb[0]}); "
                    "choose a different name", 409,
                )
            threshold_val = max(1, int(entry.get("ingestion_threshold") or entry.get("threshold") or entry.get("batch_size") or 1))
            auto_cap_int = 1 if entry.get("auto_capture", True) else 0
            pre_cfg = entry.get("llm_preprocessing")
            preproc_int = 1 if (pre_cfg.get("enabled") if isinstance(pre_cfg, dict) else entry.get("preprocessing")) else 0
            if lf_mem:
                emb_model = str(lf_mem.get("embedding_model") or emb_model)
            llm_pre = entry.get("llm_preprocessing")
            preproc_model_val = str(
                (llm_pre.get("model") if isinstance(llm_pre, dict) else None)
                or entry.get("summarizer_model")
                or entry.get("canonical_memory_type")
                or "snowflake-sql"
            )
            kill_phrase_val = str(
                (llm_pre.get("kill_phrase") if isinstance(llm_pre, dict) else None)
                or entry.get("kill_phrase")
                or "NO_INGEST"
            ).strip() or "NO_INGEST"
            if existing_mb:
                cur.execute(
                    "UPDATE memory_base SET id = ?, flow_id = ?, threshold = ?, auto_capture = ?, embedding_model = ?, "
                    "preprocessing = ?, preproc_model = ?, preproc_instructions = ?, preproc_kill_phrase = ?, kb_name = ? WHERE id = ?",
                    (
                        mb_id_hex,
                        target_flow_hex,
                        threshold_val,
                        auto_cap_int,
                        emb_model,
                        preproc_int,
                        preproc_model_val,
                        instructions,
                        kill_phrase_val,
                        kb_name,
                        existing_mb["id"],
                    ),
                )
            else:
                cur.execute(
                    "INSERT INTO memory_base "
                    "(id, name, flow_id, user_id, threshold, auto_capture, embedding_model, preprocessing, "
                    "preproc_model, preproc_instructions, preproc_kill_phrase, kb_name, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        mb_id_hex,
                        mem_name,
                        target_flow_hex,
                        user_id_hex,
                        threshold_val,
                        auto_cap_int,
                        emb_model,
                        preproc_int,
                        preproc_model_val,
                        instructions,
                        kill_phrase_val,
                        kb_name,
                        now_str,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        entry["id"] = str(uuid.UUID(mb_id_hex))
        entry["memory_id"] = str(uuid.UUID(mb_id_hex))
        entry["langflow_memory_id"] = str(uuid.UUID(mb_id_hex))
        entry["flow_id"] = str(uuid.UUID(target_flow_hex))
        entry["langflow_user_id"] = str(uuid.UUID(user_id_hex))
        entry["kb_name"] = kb_name

    def _hydrate_lf_mem_registry(self) -> int:
        """BUG-MEM-MB-01: rebuild LF_MEM registry entries from Langflow's ``memory_base`` rows after a restart.

        Only rows carrying the ``SFLF_LF_MEM`` kb_name marker are read; the Snowflake table stays the source
        of truth for content and stats. Re-reads only when ``langflow.db`` changed. Returns entries added.
        """
        from .memories_lf_mem import LfMemSpec  # noqa: PLC0415

        db_path = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist")) / "langflow.db"
        try:
            stamp = (str(db_path), db_path.stat().st_mtime_ns)
        except OSError:
            return 0
        if getattr(self, "_lf_mem_hydrated_stamp", None) == stamp:
            return 0
        conn = _open_sqlite_conn(db_path, readonly=True, timeout=10.0)
        try:
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "memory_base" not in tables:
                rows = []
            else:
                rows = conn.execute(
                    "SELECT id, name, flow_id, user_id, threshold, auto_capture, preprocessing, preproc_model, "
                    "preproc_instructions, preproc_kill_phrase, kb_name, created_at FROM memory_base "
                    "WHERE kb_name LIKE 'SFLF_LF_MEM|%'"
                ).fetchall()
        finally:
            conn.close()
        known = {str(m.get("memory_id") or "").lower() for m in self.memories_registry if isinstance(m.get("lf_mem"), dict)}
        added = 0
        for (mb_id, name, flow_id, user_id, threshold, auto_capture, preprocessing, preproc_model,
             preproc_instructions, kill_phrase, kb_name, created_at) in rows:
            spec = LfMemSpec.from_kb_name(kb_name)
            if spec is None or spec.memory_base_id.lower() in known:
                continue
            fid = str(uuid.UUID(_normalize_sqlite_uuid_val(flow_id, use_hex=True))) if flow_id else None
            uid = str(uuid.UUID(_normalize_sqlite_uuid_val(user_id, use_hex=True))) if user_id else None
            self.memories_registry.append(
                {
                    "id": spec.memory_base_id, "memory_id": spec.memory_base_id, "langflow_memory_id": spec.memory_base_id,
                    "name": str(name), "kb_name": str(kb_name), "flow_id": fid, "langflow_user_id": uid,
                    "lf_mem": spec.to_dict(), "table_fqn": spec.table_fqn, "database": spec.database,
                    "schema": spec.memory_schema, "storage_family": "snowflake_sql", "backend_type": "snowflake_table",
                    "vector_database": "Standard Table (VECTOR)", "enable_embedding": True, "is_vector_memory": True,
                    "embedding_provider": "snowflake_cortex", "embedding_model": spec.embedding_model,
                    "dimension": spec.dimension, "threshold": int(threshold or 1), "ingestion_threshold": int(threshold or 1),
                    "auto_capture": bool(auto_capture), "filter_by_session": spec.filter_by_session,
                    "preprocessing": bool(preprocessing), "preproc_model": preproc_model,
                    "preproc_instructions": preproc_instructions, "preproc_kill_phrase": kill_phrase or "NO_INGEST",
                    "llm_preprocessing": {"enabled": bool(preprocessing), "model": preproc_model,
                                          "kill_phrase": kill_phrase or "NO_INGEST"},
                    "canonical_memory_type": "episodic_memory", "agent_memory_type": "semantic_vector",
                    "created_at": str(created_at) if created_at else None, "stored_records": [],
                    "langflow_registration": "REGISTERED",
                }
            )
            known.add(spec.memory_base_id.lower())
            added += 1
        self._lf_mem_hydrated_stamp = stamp
        return added

    def _lf_mem_entry_visible_to(self, entry: dict[str, Any], *, flow_id: str | None, user_id: str | None) -> bool:
        """BUG-MEM-API-01: an LF_MEM base is listed only for its own flow and its own Langflow owner."""
        if not isinstance(entry.get("lf_mem"), dict):
            return True
        if not flow_id or self._normalize_flow_id(str(entry.get("flow_id") or "")) != flow_id:
            return False
        owner = str(entry.get("langflow_user_id") or "").replace("-", "").lower()
        caller = str(user_id or "").replace("-", "").lower()
        return bool(owner) and bool(caller) and owner == caller

    def _find_memory_entry(self, memory_id_or_name: str, _hydrating: bool = False) -> dict[str, Any] | None:
        """Locate a memory entry in `memories_registry` or `memories_catalog` by name, UUID, `.hex`, `sf-<name>`, or FQN (`W2-R4-08G`)."""
        target = str(memory_id_or_name or "").strip()
        if not target:
            return None
        clean_target = re.sub(r"^sf-", "", target, flags=re.IGNORECASE).strip()
        target_upper = target.upper()
        clean_target_upper = clean_target.upper()
        target_hex_upper = target_upper.replace("-", "")
        clean_det_id_upper = compute_deterministic_memory_uuid(clean_target).upper() if clean_target else ""
        for source_list in (self.memories_registry, self.memories_catalog):
            for entry in source_list:
                e_name = str(entry.get("name") or "").strip()
                det_id = compute_deterministic_memory_uuid(e_name) if e_name else ""
                cand_ids = {
                    e_name.upper(),
                    f"SF-{e_name.upper()}",
                    str(entry.get("id") or "").strip().upper(),
                    str(entry.get("memory_id") or "").strip().upper(),
                    str(entry.get("langflow_memory_id") or "").strip().upper(),
                    str(entry.get("table_fqn") or "").strip().upper(),
                    det_id.upper(),
                }
                cand_hexes = {c.replace("-", "") for c in cand_ids if c}
                if (
                    target_upper in cand_ids
                    or clean_target_upper in cand_ids
                    or (clean_det_id_upper and clean_det_id_upper in cand_ids)
                    or (len(target_hex_upper) == 32 and target_hex_upper in cand_hexes)
                ):
                    return entry
        if not _hydrating and self._hydrate_lf_mem_registry():
            return self._find_memory_entry(memory_id_or_name, _hydrating=True)
        return None

    def get_memory_detail(
        self,
        memory_id_or_name: str,
        *,
        session_filter: str | None = None,
    ) -> dict[str, Any]:
        """Return full Langflow-compliant Memory Detail View payload (`W2-R4-05`)."""
        entry = self._find_memory_entry(memory_id_or_name)
        if entry is None:
            return {
                "ok": False,
                "status_code": 404,
                "error_code": ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                "message": f"Snowflake Memory '{memory_id_or_name}' not found or unauthorized for role {self.active_role}.",
                "active_role": self.active_role,
            }
        c_type = str(entry.get("canonical_memory_type") or resolve_canonical_memory_type(entry.get("agent_memory_type")))
        type_spec = CANONICAL_MEMORY_TYPES_BY_ID.get(c_type, CANONICAL_MEMORY_TYPES_BY_ID["episodic_memory"])
        storage_family = str(
            entry.get("storage_family")
            or ("snowflake_postgres" if "POSTGRES" in str(entry.get("vector_database") or "").upper() else "snowflake_sql")
        )
        is_vec = bool(entry.get("enable_embedding"))
        llm_pre = entry.get("llm_preprocessing")
        if isinstance(llm_pre, dict) and llm_pre.get("enabled"):
            preproc_summary = (
                f"Enabled ({llm_pre.get('engine', 'proxy_chat')}: {llm_pre.get('model', 'openai-gpt-6-luna')} "
                f"• Kill Phrase: {llm_pre.get('kill_phrase', 'NO_INGEST')})"
            )
        else:
            preproc_summary = f"Disabled (Kill Phrase ready: {entry.get('kill_phrase') or 'NO_INGEST'})"

        if storage_family == "snowflake_postgres":
            subtype_summary = (
                f"SNOWFLAKE POSTGRES (:5432) • {entry.get('pg_table_mode') or 'Relational / JSONB'} "
                f"({entry.get('pg_database') or 'postgres'}.{entry.get('pg_schema') or 'public'}.{entry.get('pg_table') or 'memory_store'})"
            )
        elif entry.get("table_type") == "DYNAMIC":
            subtype_summary = (
                f"DYNAMIC TABLE (TARGET_LAG = '{entry.get('target_lag') or '5 minutes'}', "
                f"WAREHOUSE = {entry.get('warehouse') or 'COMPUTE_WH'}, REFRESH_MODE = {entry.get('refresh_mode') or 'INCREMENTAL'})"
            )
        elif entry.get("table_type") == "INTERACTIVE":
            subtype_summary = (
                f"INTERACTIVE TABLE (CLUSTER BY ({entry.get('cluster_by') or 'user_id'}), "
                f"TARGET_LAG = '{entry.get('target_lag') or '1 minute'}')"
            )
        elif entry.get("table_type") == "HYBRID":
            subtype_summary = f"HYBRID TABLE (PK: {entry.get('primary_key') or type_spec.get('default_hybrid_pk')})"
        else:
            subtype_summary = (
                f"STANDARD TABLE ({entry.get('semi_structured_type') or 'RELATIONAL / VARIANT'}, "
                f"Retention: {entry.get('retention_days') or 90}d)"
            )

        active_sql = str(entry.get("how_to_feed_without_embedding") or type_spec["how_to_feed_without_embedding"]).replace(
            "<fqn>", str(entry.get("table_fqn") or "")
        )
        if is_vec:
            emb_model = str(entry.get("embedding_model") or "snowflake-arctic-embed-l-v2.0").split(" ")[0]
            active_sql = (
                f"SELECT chunk_id, topic, tool_used, outcome_score, episode_text, "
                f"VECTOR_COSINE_SIMILARITY(embedding, AI_EMBED('{emb_model}', :query)) AS score "
                f"FROM {entry.get('table_fqn')} WHERE outcome_score >= {entry.get('outcome_score_threshold', 0.7)} "
                f"ORDER BY score DESC LIMIT {entry.get('few_shot_k', 3)}"
            )

        config_summary = {
            "name": entry.get("name"),
            "memory_type_id": c_type,
            "memory_type_label": entry.get("label") or type_spec["label"],
            "flow_scope": entry.get("flow_id") or "Global / Active Flow",
            "storage_family": storage_family,
            "storage_target": (
                "Snowflake Postgres (:5432 — Account-Level)"
                if storage_family == "snowflake_postgres"
                else "Snowflake SQL (Database ▸ Schema)"
            ),
            "snowflake_object": entry.get("table_fqn"),
            "object_subtype": subtype_summary,
            "filter_by_session": (
                "Enabled (Session-Scoped)"
                if entry.get("filter_by_session", type_spec.get("filter_by_session", False))
                else "Disabled (Cross-Session Recall within user_id + role)"
            ),
            "embedding_model": (
                f"{entry.get('embedding_provider')} • {entry.get('embedding_model')} ({entry.get('dimension')}d)"
                if is_vec
                else "Disabled (Direct SQL / AI_FILTER Lookup)"
            ),
            "ingestion_batch": (
                f"Threshold = {entry.get('ingestion_threshold', 1)} run(s) • Batch Size = {entry.get('batch_size', 64)}"
            ),
            "llm_preprocessing": preproc_summary,
            "kill_phrase": entry.get("kill_phrase") or "NO_INGEST",
            "active_sql_query": active_sql,
            "ddl": entry.get("ddl"),
        }

        records = list(entry.get("stored_records") or [])

        sessions = sorted({str(r.get("session_id") or "default") for r in records})
        clean_filter = str(session_filter or "").strip()
        if clean_filter and clean_filter.upper() not in ("ALL", "ALL SESSIONS", "ALL SESSIONS (CROSS-SESSION)"):
            filtered_records = [r for r in records if str(r.get("session_id") or "") == clean_filter]
        else:
            filtered_records = records

        return {
            "ok": True,
            "status_code": 200,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "memory": entry,
            "configuration_summary": config_summary,
            "active_sql_query": active_sql,
            "sessions": ["All Sessions (Cross-Session)", *sessions],
            "stored_records": filtered_records,
        }

    def reload_memory_detail(
        self,
        memory_id_or_name: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Reload sessions and stored records for a Snowflake Memory and optionally toggle `auto_capture` or ingest a turn with `NO_INGEST` evaluation (`W2-R4-05`)."""
        payload = body if isinstance(body, dict) else {}
        entry = self._find_memory_entry(memory_id_or_name)
        if entry is None:
            return {
                "ok": False,
                "status_code": 404,
                "error_code": ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                "message": f"Snowflake Memory '{memory_id_or_name}' not found.",
                "active_role": self.active_role,
            }
        if "auto_capture" in payload and payload.get("auto_capture") is not None:
            entry["auto_capture"] = bool(payload.get("auto_capture"))

        if isinstance(payload.get("ingest_turn"), dict):
            return {
                "ok": False,
                "status_code": 501,
                "error_code": "NOT_CONFIGURED",
                "message": "Turn ingestion into Snowflake memories is not available in this release; nothing was stored.",
                "query_id": None,
                "active_role": self.active_role,
            }

        detail = self.get_memory_detail(
            memory_id_or_name,
            session_filter=str(payload.get("session_filter") or "").strip() or None,
        )
        detail["reloaded"] = True
        return detail

    def delete_memory(self, memory_id_or_name: str) -> dict[str, Any]:
        """Delete a Snowflake Memory registration under `CURRENT_ROLE()` RBAC enforcement (`W2-R4-05`, `W2-R4-08G`)."""
        target = str(memory_id_or_name or "").strip()
        entry = self._find_memory_entry(target)
        fqn = str((entry or {}).get("table_fqn") or target)
        denied = self.check_write_privilege_or_deny("MEMORY_DELETE", fqn)
        if denied:
            return denied
        if entry is None:
            return {
                "ok": False,
                "status_code": 404,
                "error_code": ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                "message": f"Snowflake Memory '{target}' not found.",
                "active_role": self.active_role,
            }
        mem_name = str(entry.get("name") or target).strip()
        mem_uuid_str = str(entry.get("memory_id") or entry.get("langflow_memory_id") or compute_deterministic_memory_uuid(mem_name))
        mem_hex_str = mem_uuid_str.replace("-", "")
        target_upper = target.upper()
        self.memories_registry = [
            m
            for m in self.memories_registry
            if str(m.get("name") or "").strip().upper() != mem_name.upper()
            and str(m.get("memory_id") or "").strip().upper() not in {target_upper, mem_uuid_str.upper()}
            and str(m.get("langflow_memory_id") or "").strip().upper() not in {target_upper, mem_uuid_str.upper()}
        ]
        vis_mem = (self.selected_context.get("visible_objects") or {}).get("memories")
        if isinstance(vis_mem, list):
            self.selected_context["visible_objects"]["memories"] = [
                x for x in vis_mem if str(x).strip().upper() != mem_name.upper()
            ]
        for f_ctx in self.contexts_by_flow_id.values():
            f_vis = (f_ctx.get("visible_objects") or {}).get("memories")
            if isinstance(f_vis, list):
                f_ctx["visible_objects"]["memories"] = [
                    x for x in f_vis if str(x).strip().upper() != mem_name.upper()
                ]
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        if db_path.exists():
            try:
                conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
                cur = conn.cursor()
                cur.execute(
                    "DELETE FROM memory_base WHERE name = ? OR id = ? OR id = ?",
                    (mem_name, mem_uuid_str, mem_hex_str),
                )
                # Never delete native knowledge_base rows by name; only snowflake-mode rows are purged.
                purge_snowflake_rows_from_native_knowledge_base(conn)
                conn.commit()
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        emit_audit(
            self.record_audit_event,
            AuditAction.MEMORY_BASE_DELETE,
            {"ok": True},
            target_object=fqn,
            details={"name": mem_name, "memory_base_id": mem_uuid_str},
        )
        return {
            "ok": True,
            "status_code": 200,
            "deleted": mem_name,
            "memory_id": mem_uuid_str,
            "table_fqn": fqn,
            "active_role": self.active_role,
        }

    def governed_memory_stats(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """BUG-MEM-API-01: ``total_processed`` / ``pending_count`` / ``last_sync_at`` / sessions from Snowflake.

        Computed from the governed ``LF_MEM_<NAME>`` table and ``LANGFLOW_MESSAGES`` filtered by ``flow_id`` and
        ``owner_user`` (the SPCS caller, bound; the RAP applies on top), in the caller session.
        Returns None for non-``LF_MEM`` memories; ``status`` reports a real failure instead of fake zeros.
        """
        lf_mem = entry.get("lf_mem") if isinstance(entry.get("lf_mem"), dict) else None
        if not lf_mem:
            return None
        from .memories_lf_mem import LfMemSpec, aggregate_memory_stats  # noqa: PLC0415

        spec = LfMemSpec(**lf_mem)
        flow_id = str(entry.get("flow_id") or "")
        owner = _sf_identity.current_identity().sf_user
        empty = {"total_processed": None, "pending_count": None, "last_sync_at": None, "sessions": []}
        if not flow_id:
            return {**empty, "status": "MISSING_FLOW_ID"}
        rows: list[list[Any]] = []
        for sql, bindings in (
            spec.build_session_stats_request(flow_id=flow_id, owner_user=owner),
            spec.build_pending_request(self._configured_messages_table(), flow_id=flow_id, owner_user=owner),
        ):
            res = self.execute_sql(sql, bindings=bindings, mode="caller")
            if not res.get("ok") or res.get("mode") == "offline":
                code = str(res.get("error_code") or ("NOT_CONFIGURED" if res.get("mode") == "offline" else "SNOWFLAKE_ERROR"))
                return {**empty, "status": code}
            rows.append(list(res.get("rows") or []))
        from .memories_lf_mem_ingest import PIPELINE as _ingest_pipeline  # noqa: PLC0415

        return {
            **aggregate_memory_stats(rows[0], rows[1]),
            "status": "OK",
            "ingestion": _ingest_pipeline.status(spec.memory_base_id),
        }

    def _to_langflow_memory_base_read(self, entry: dict[str, Any]) -> dict[str, Any]:
        """Convert a Snowflake Memory entry into a Langflow `MemoryBaseRead` compatible payload (`W2-R4-08G`)."""
        mem_name = str(entry.get("name") or "SNOWFLAKE_MEMORY").strip()
        mem_uuid = str(
            entry.get("memory_id")
            or entry.get("langflow_memory_id")
            or entry.get("id")
            or compute_deterministic_memory_uuid(mem_name)
        )
        c_type = str(entry.get("canonical_memory_type") or resolve_canonical_memory_type(entry.get("agent_memory_type")))
        type_spec = CANONICAL_MEMORY_TYPES_BY_ID.get(c_type, CANONICAL_MEMORY_TYPES_BY_ID["episodic_memory"])
        is_vec = bool(entry.get("enable_embedding"))
        emb_provider = str(entry.get("embedding_provider") or ("Snowflake Cortex" if is_vec else "Snowflake Direct SQL"))
        emb_model = (
            f"{entry.get('embedding_model')} ({entry.get('dimension')}d)"
            if is_vec and entry.get("embedding_model")
            else None
        )
        backend_label = f"Snowflake · {entry.get('vector_database') or 'Hybrid Table'} ({entry.get('table_fqn')})"
        sql_lookup = str(entry.get("how_to_feed_without_embedding") or type_spec["how_to_feed_without_embedding"]).replace(
            "<fqn>", str(entry.get("table_fqn") or "")
        )
        llm_pre = entry.get("llm_preprocessing")
        preproc_enabled = bool(llm_pre.get("enabled", True)) if isinstance(llm_pre, dict) else bool(entry.get("preprocessing", True))
        preproc_model = str(
            (llm_pre.get("model") if isinstance(llm_pre, dict) else None)
            or entry.get("preproc_model")
            or entry.get("summarizer_model")
            or c_type
            or "mistral-large2"
        )
        kill_phrase = str(
            (llm_pre.get("kill_phrase") if isinstance(llm_pre, dict) else None)
            or entry.get("preproc_kill_phrase")
            or entry.get("kill_phrase")
            or "NO_INGEST"
        ).strip() or "NO_INGEST"
        instructions = str(
            entry.get("preproc_instructions")
            or (
                f"[{entry.get('label') or type_spec['label']}] Store: {entry.get('table_fqn')} | "
                f"Embedding Required: {entry.get('needs_embedding_badge') or type_spec['needs_embedding_badge']} | "
                f"Lookup SQL: {sql_lookup}"
            )
        )
        threshold_val = max(1, int(entry.get("threshold") or entry.get("ingestion_threshold") or 1))
        detail_res = self.get_memory_detail(mem_name)
        stored_recs = list(detail_res.get("stored_records") or [])
        governed = self.governed_memory_stats(entry)
        if governed is not None:
            processed_cnt = governed["total_processed"]
            pending_cnt = governed["pending_count"]
        else:
            processed_cnt = sum(
                1 for r in stored_recs if str(r.get("status") or "Ingested").lower() in ("ingested", "processed")
            )
            pending_cnt = sum(1 for r in stored_recs if str(r.get("status") or "").lower() == "pending")
        created_iso = entry.get("created_at") or None
        desc_text = str(
            entry.get("description")
            or f"{entry.get('label') or type_spec['label']} · {entry.get('table_fqn') or ''}"
        )
        return {
            "id": mem_uuid,
            "memory_id": mem_uuid,
            "langflow_memory_id": mem_uuid,
            "name": mem_name,
            "description": desc_text,
            "kb_name": str(entry.get("kb_name") or mem_name),
            "flow_id": entry.get("flow_id"),
            "embedding_model": emb_model,
            "embedding_provider": emb_provider,
            "backend_type": backend_label,
            "backend_config": {
                "mode": "snowflake",
                "storage_family": entry.get("storage_family", "snowflake_sql"),
                "table_fqn": entry.get("table_fqn"),
                "canonical_memory_type": c_type,
                "agent_memory_type": entry.get("agent_memory_type") or c_type,
                "enable_embedding": is_vec,
            },
            "auto_capture": bool(entry.get("auto_capture", True)),
            "is_active": bool(entry.get("auto_capture", True)),
            "preprocessing": preproc_enabled,
            "preproc_model": preproc_model,
            "preproc_instructions": instructions,
            "preproc_kill_phrase": kill_phrase,
            "threshold": threshold_val,
            "user_id": entry.get("langflow_user_id"),
            "total_messages_processed": processed_cnt,
            "pending_messages_count": pending_cnt,
            "total_processed": processed_cnt,
            "pending_count": pending_cnt,
            "last_sync_at": governed["last_sync_at"] if governed is not None else None,
            "stats_status": governed["status"] if governed is not None else None,
            "ingestion_status": governed.get("ingestion") if governed is not None else None,
            "sessions_count": (
                len(governed["sessions"]) if governed is not None
                else len({str(r.get("session_id") or "default") for r in stored_recs})
            ),
            "last_generated_at": entry.get("last_generated_at"),
            "agent_memory_type": entry.get("agent_memory_type") or c_type,
            "canonical_memory_type": c_type,
            "storage_family": entry.get("storage_family", "snowflake_sql"),
            "database": entry.get("database"),
            "schema": entry.get("schema"),
            "table_fqn": entry.get("table_fqn"),
            "vector_database": entry.get("vector_database"),
            "created_at": created_iso,
            "snowflake_memory": entry,
            "configuration_summary": detail_res.get("configuration_summary"),
            "active_sql_query": detail_res.get("active_sql_query"),
            "sessions": governed["sessions"] if governed is not None else detail_res.get("sessions", []),
            "stored_records": stored_recs,
        }

    def list_native_memories(
        self,
        *,
        flow_id: str | None = None,
        search: str | None = None,
        page: int = 1,
        size: int = 50,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Return native Langflow `Page[MemoryBaseRead]` for one flow (`W2-R4-08G`, BUG-MEM-API-01).

        LF_MEM bases are listed only for their own ``flow_id`` and Langflow owner; items are keyed by memory id,
        never merged by name across flows.
        """
        eff_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id)
        self._hydrate_lf_mem_registry()
        merged_by_name: dict[str, dict[str, Any]] = {}
        seen_names: set[str] = set()
        for m in self.memories_registry:
            if isinstance(m.get("lf_mem"), dict):
                if not self._lf_mem_entry_visible_to(m, flow_id=eff_fid, user_id=user_id):
                    continue
            elif not self.is_memory_visible_in_context(m, flow_id=eff_fid):
                continue
            nm = str(m.get("name") or "").strip()
            key = str(m.get("memory_id") or m.get("id") or nm).lower()
            if nm and key not in merged_by_name:
                merged_by_name[key] = self._to_langflow_memory_base_read(m)
                seen_names.add(nm.upper())
        for m in self.get_memories_for_flow(flow_id=eff_fid):
            nm = str(m.get("name") or "").strip()
            in_registry = any(str(r.get("name") or "").strip().upper() == nm.upper() for r in self.memories_registry)
            if nm and not in_registry and nm.upper() not in seen_names:
                merged_by_name[str(m.get("memory_id") or nm).lower()] = self._to_langflow_memory_base_read(m)
                seen_names.add(nm.upper())

        items = list(merged_by_name.values())
        q = str(search or "").strip().lower()
        if q:
            filtered: list[dict[str, Any]] = []
            for item in items:
                haystack = " ".join(
                    str(item.get(k) or "")
                    for k in (
                        "name",
                        "kb_name",
                        "agent_memory_type",
                        "canonical_memory_type",
                        "backend_type",
                        "vector_database",
                        "database",
                        "schema",
                        "table_fqn",
                        "preproc_instructions",
                    )
                ).lower()
                if q in haystack:
                    filtered.append(item)
            items = filtered

        page_int = max(1, int(page or 1))
        size_int = max(1, int(size or 50))
        total = len(items)
        start = (page_int - 1) * size_int
        paged = items[start : start + size_int]
        pages = max(1, (total + size_int - 1) // size_int)
        return {
            "items": paged,
            "total": total,
            "page": page_int,
            "size": size_int,
            "pages": pages,
        }

    def create_native_memory(
        self,
        body: dict[str, Any],
        *,
        flow_id: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Handle `POST /api/v1/memories` from Langflow's native `+ Create` Memory modal (`W2-R5-02`).

        Creates a governed Snowflake Memory in the active `[ ❄️ Context ]` (`<DATABASE>.<SCHEMA>` or
        `Snowflake Postgres (:5432)`), supporting all 6 `SNOWFLAKE.CORTEX.AI_EMBED` models and
        `No Embedding` direct SQL mode, and persists it in both `memories_registry` and SQLite `memory_base`.
        """
        payload = dict(body or {})
        # BUG-MEM-MB-01: explicit flow_id (request/payload) and the caller's real Langflow user only.
        eff_fid = self._normalize_flow_id(
            flow_id or payload.get("flow_id") or (payload.get("backend_config") or {}).get("flow_id")
        )
        if not eff_fid:
            return {"ok": False, "status_code": 422, "error_code": "MISSING_FLOW_ID",
                    "detail": "flow_id is required to create a Memory Base."}
        if not str(user_id or "").strip():
            return {"ok": False, "status_code": 401, "error_code": "AUTHZ_UNAUTHENTICATED",
                    "detail": "An authenticated Langflow user is required to create a Memory Base."}
        payload["flow_id"] = eff_fid

        mem_name = str(payload.get("name") or payload.get("kb_name") or "SNOWFLAKE_MEMORY").strip()
        payload["name"] = mem_name
        payload.setdefault("kb_name", mem_name)

        backend_cfg = payload.get("backend_config") if isinstance(payload.get("backend_config"), dict) else {}
        raw_backend = str(
            payload.get("backend_type")
            or payload.get("vector_database")
            or payload.get("storage_backend")
            or backend_cfg.get("backend_type")
            or backend_cfg.get("vector_database")
            or "snowflake_table"
        ).strip()
        raw_backend_lower = raw_backend.lower()

        raw_emb_model = str(payload.get("embedding_model") or backend_cfg.get("embedding_model") or "").strip()
        raw_emb_provider = str(
            payload.get("embedding_provider") or backend_cfg.get("embedding_provider") or "Snowflake Cortex"
        ).strip()

        no_embedding_tokens = {"none", "no embedding", "no_embedding", "disabled", "false", "sql_only"}
        if payload.get("enable_embedding") is False or backend_cfg.get("enable_embedding") is False:
            enable_emb = False
        elif raw_emb_model.lower() in no_embedding_tokens or raw_emb_provider.lower() in no_embedding_tokens:
            enable_emb = False
        elif "iceberg" in raw_backend_lower:
            enable_emb = False
        else:
            enable_emb = True

        clean_model = raw_emb_model.split(" (")[0].strip() if raw_emb_model else "snowflake-arctic-embed-m-v1.5"
        if not enable_emb or clean_model.lower() in no_embedding_tokens:
            clean_model = "none"
            enable_emb = False

        dim_map = dict(_sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS)
        dim = int(payload.get("dimension") or dim_map.get(clean_model, 768))

        backend_to_vector_db = {
            "snowflake_table": "Standard Table (VECTOR)" if enable_emb else "Standard Table",
            "snowflake standard table": "Standard Table (VECTOR)" if enable_emb else "Standard Table",
            "standard table": "Standard Table (VECTOR)" if enable_emb else "Standard Table",
            "chroma": "Standard Table (VECTOR)" if enable_emb else "Standard Table",
            "snowflake_hybrid": "Hybrid Table (VECTOR)" if enable_emb else "Hybrid Table",
            "snowflake hybrid table": "Hybrid Table (VECTOR)" if enable_emb else "Hybrid Table",
            "hybrid table": "Hybrid Table (VECTOR)" if enable_emb else "Hybrid Table",
            "snowflake_dynamic": "Dynamic Table (VECTOR)" if enable_emb else "Dynamic Table",
            "snowflake dynamic table": "Dynamic Table (VECTOR)" if enable_emb else "Dynamic Table",
            "dynamic table": "Dynamic Table (VECTOR)" if enable_emb else "Dynamic Table",
            "snowflake_interactive": "Interactive Table (VECTOR)" if enable_emb else "Interactive Table",
            "snowflake interactive table": "Interactive Table (VECTOR)" if enable_emb else "Interactive Table",
            "interactive table": "Interactive Table (VECTOR)" if enable_emb else "Interactive Table",
            "snowflake_iceberg": "Standard Table",
            "snowflake iceberg table": "Standard Table",
            "iceberg table": "Standard Table",
            "snowflake_postgres": "Snowflake Postgres (pgvector)" if enable_emb else "Snowflake Postgres (Relational / JSONB)",
            "snowflake postgres": "Snowflake Postgres (pgvector)" if enable_emb else "Snowflake Postgres (Relational / JSONB)",
            "snowflake postgres (:5432)": "Snowflake Postgres (pgvector)" if enable_emb else "Snowflake Postgres (Relational / JSONB)",
        }
        mapped_vec_db = backend_to_vector_db.get(raw_backend_lower, raw_backend or "Standard Table (VECTOR)")
        is_pg = "postgres" in raw_backend_lower or "postgres" in mapped_vec_db.lower()

        payload.setdefault("storage_family", "snowflake_postgres" if is_pg else "snowflake_sql")
        payload.setdefault("vector_database", mapped_vec_db)
        payload["enable_embedding"] = enable_emb
        payload["embedding_provider"] = "snowflake_cortex" if enable_emb else "none"
        payload["embedding_model"] = clean_model if enable_emb else None
        payload["dimension"] = dim if enable_emb else None
        payload.setdefault(
            "agent_memory_type",
            backend_cfg.get("agent_memory_type")
            or payload.get("canonical_memory_type")
            or ("episodic_memory" if enable_emb else "buffer_window_memory"),
        )
        if "preprocessing" in payload and "llm_preprocessing" not in payload:
            payload["llm_preprocessing"] = {
                "enabled": bool(payload.get("preprocessing")),
                "engine": "cortex_sql",
                "model": str(payload.get("preproc_model") or "mistral-large2"),
                "custom_instructions": str(payload.get("preproc_instructions") or ""),
                "kill_phrase": str(payload.get("preproc_kill_phrase") or "NO_INGEST"),
            }
        if "threshold" in payload and "ingestion_threshold" not in payload:
            payload["ingestion_threshold"] = int(payload.get("threshold") or 1)

        # BUG-MEM-PROV-01: the Snowflake DB Provider stores every Memory Base in a governed LF_MEM_<NAME> table.
        if not enable_emb or clean_model not in _sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS:
            return {
                "ok": False,
                "status_code": 422,
                "error_code": "UNSUPPORTED_EMBEDDING_MODEL",
                "detail": (
                    "The Snowflake DB Provider computes embeddings with AI_EMBED; choose one of "
                    f"{sorted(_sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS)}."
                ),
            }
        from .memories_lf_mem import MEMORY_PRESETS, LfMemError, LfMemSpec  # noqa: PLC0415

        flow_ctx = self.get_flow_context(eff_fid, create=False) or {}
        lf_name = re.sub(r"[^A-Za-z0-9_]", "_", mem_name).strip("_")[:200]
        try:
            spec = LfMemSpec(
                name=lf_name,
                memory_base_id=str(uuid.uuid4()),
                database=str(payload.get("database") or flow_ctx.get("database") or self.database or ""),
                memory_schema=str(
                    payload.get("memory_schema") or os.getenv("LANGFLOW_MEMORY_SCHEMA") or flow_ctx.get("schema") or self.schema or ""
                ),
                embedding_model=clean_model,
                governance_schema=str(os.getenv("LANGFLOW_GOVERNANCE_SCHEMA") or self.schema or "") or None,
                filter_by_session=bool(
                    payload.get("filter_by_session")
                    if "filter_by_session" in payload
                    else (MEMORY_PRESETS.get(str(payload.get("preset") or ""), {}) or {}).get("filter_by_session")
                ),
            )
        except (LfMemError, _sql_safety.SqlSafetyError, ValueError) as exc:
            return {"ok": False, "status_code": 422, "error_code": getattr(exc, "error_code", "INVALID_MEMORY_BASE"), "detail": str(exc)}

        ddl = spec.build_ddl()
        if not payload.get("dry_run"):
            ddl_res = self.execute_sql(ddl, mode="caller")
            if not ddl_res.get("ok"):
                err_code = str(ddl_res.get("error_code") or ("NOT_CONFIGURED" if ddl_res.get("mode") == "offline" else "SNOWFLAKE_ERROR"))
                return {
                    "ok": False,
                    "status_code": 503 if err_code == "NOT_CONFIGURED" else 422,
                    "error_code": err_code,
                    "detail": str(ddl_res.get("message") or "Snowflake did not create the LF_MEM table."),
                }
        # The memory table becomes part of the flow [ ❄️ Context ] (needed by the flow-context guard).
        self.register_object_in_context(
            database=spec.database, schema=spec.memory_schema, category="memories",
            object_name=spec.table_name, flow_id=eff_fid,
        )
        from .memories_lf_mem import MEMORY_PRESETS  # noqa: PLC0415

        preset_name = str(payload.get("preset") or "").strip()
        if not preset_name:
            instr = str(payload.get("preproc_instructions") or "").strip()
            preset_name = next((n for n, p in MEMORY_PRESETS.items() if instr and instr == p["preproc_instructions"]), "")
        if preset_name:
            if preset_name not in MEMORY_PRESETS:
                return {"ok": False, "status_code": 422, "error_code": "UNKNOWN_PRESET",
                        "detail": f"Unknown preset {preset_name!r}; expected one of {sorted(MEMORY_PRESETS)}."}
            preset = MEMORY_PRESETS[preset_name]
            payload.setdefault("preproc_instructions", preset["preproc_instructions"])
            if payload.get("filter_by_session") is None:
                payload["filter_by_session"] = preset["filter_by_session"]
            payload["preset"] = preset_name
        payload.update(
            {
                "table_fqn": spec.table_fqn,
                "database": spec.database,
                "schema": spec.memory_schema,
                "memory_id": spec.memory_base_id,
                "create_new_physical_store": False,
                "_skip_context_auto_register": True,
                "lf_mem": spec.to_dict(),
                "langflow_user_id": str(user_id),
                "filter_by_session": payload.get("filter_by_session"),
            }
        )
        res = self.create_or_register_memory(payload)
        if not res.get("ok"):
            return res

        entry = res["memory"]
        entry["ddl"] = ddl
        entry["snowflake_created"] = not payload.get("dry_run")
        entry["backend_type"] = "snowflake_table"
        read_obj = self._to_langflow_memory_base_read(entry)
        return {
            **read_obj,
            "ok": True,
            "status_code": 201,
            "memory": entry,
            "ddl": ddl,
        }

    def get_native_memory(self, memory_id: str) -> dict[str, Any]:
        """Resolve a Snowflake Memory by UUID, `.hex`, or name and return a `MemoryBaseRead` payload (`W2-R4-08G`)."""
        entry = self._find_memory_entry(memory_id)
        if entry is None:
            return {
                "ok": False,
                "status_code": 404,
                "detail": f"Snowflake Memory '{memory_id}' not found.",
            }
        payload = self._to_langflow_memory_base_read(entry)
        payload["ok"] = True
        payload["status_code"] = 200
        return payload

    def list_native_memory_sessions(
        self,
        memory_id: str,
        *,
        page: int = 1,
        size: int = 50,
    ) -> dict[str, Any]:
        """Return native Langflow `Page[MemoryBaseSessionRead]` for `/api/v1/memories/{id}/sessions` (`W2-R4-08G`)."""
        entry = self._find_memory_entry(memory_id)
        mem_uuid = str(
            (entry or {}).get("memory_id")
            or (entry or {}).get("langflow_memory_id")
            or compute_deterministic_memory_uuid(memory_id)
        )
        governed = self.governed_memory_stats(entry) if entry else None
        if governed is not None:
            g_items = [
                {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"snowflake-memory-session:{mem_uuid}:{g['session_id']}")),
                    "memory_base_id": mem_uuid,
                    "session_id": g["session_id"],
                    "total_processed": g["total_processed"],
                    "pending_count": g["pending_count"],
                    "skipped_count": None,  # LF_MEM does not record kill-phrase skips per session
                    "last_sync_at": g["last_sync_at"],
                    "job_id": g.get("job_id"),
                }
                for g in governed["sessions"]
            ]
            p_int, s_int = max(1, int(page or 1)), max(1, int(size or 50))
            return {
                "items": g_items[(p_int - 1) * s_int : p_int * s_int],
                "total": len(g_items),
                "page": p_int,
                "size": s_int,
                "pages": max(1, (len(g_items) + s_int - 1) // s_int),
                "stats_status": governed["status"],
            }
        records = list(
            (entry or {}).get("stored_records")
            or []
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in records:
            s_id = str(r.get("session_id") or (entry or {}).get("session_id") or "default")
            grouped.setdefault(s_id, []).append(r)
        session_items: list[dict[str, Any]] = []
        for s_id, s_recs in grouped.items():
            session_items.append(
                {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"snowflake-memory-session:{mem_uuid}:{s_id}")),
                    "memory_base_id": mem_uuid,
                    "session_id": s_id,
                    "total_processed": sum(
                        1 for r in s_recs if str(r.get("status") or "Ingested").lower() in ("ingested", "processed")
                    ),
                    "pending_count": sum(1 for r in s_recs if str(r.get("status") or "").lower() == "pending"),
                    "skipped_count": sum(1 for r in s_recs if str(r.get("status") or "").lower() == "skipped"),
                    "last_sync_at": max(
                        (str(r["ingested_at"]) for r in s_recs if r.get("ingested_at")), default=None
                    ),
                }
            )
        page_int = max(1, int(page or 1))
        size_int = max(1, int(size or 50))
        total = len(session_items)
        return {
            "items": session_items[(page_int - 1) * size_int : page_int * size_int],
            "total": total,
            "page": page_int,
            "size": size_int,
            "pages": max(1, (total + size_int - 1) // size_int),
        }

    def list_native_memory_messages(
        self,
        memory_id: str,
        *,
        session_id: str | None = None,
        page: int = 1,
        size: int = 50,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Return native Langflow `Page[MessageReadResponse]` for `/api/v1/memories/{id}/messages` (`W2-R4-08G`).

        LF_MEM bases (BUG-MEM-API-02) read the governed ``LF_MEM_<NAME>`` table as the caller, bound and filtered
        by the base's flow_id and the caller's ``owner_user``; RAM ``stored_records`` are never used for them.
        """
        entry = self._find_memory_entry(memory_id)
        page_int = max(1, int(page or 1))
        size_int = max(1, min(500, int(size or 50)))
        if entry is not None and isinstance(entry.get("lf_mem"), dict):
            return self._list_lf_mem_messages(entry, session_id=session_id, page=page_int, size=size_int, user_id=user_id)
        mem_uuid = str(
            (entry or {}).get("memory_id")
            or (entry or {}).get("langflow_memory_id")
            or compute_deterministic_memory_uuid(memory_id)
        )
        records = list(
            (entry or {}).get("stored_records")
            or []
        )
        clean_sess = str(session_id or "").strip()
        if clean_sess and clean_sess.upper() not in ("ALL", "ALL SESSIONS", "ALL SESSIONS (CROSS-SESSION)"):
            records = [r for r in records if str(r.get("session_id") or "") == clean_sess]

        msg_items: list[dict[str, Any]] = []
        for idx, r in enumerate(records):
            s_id = str(r.get("session_id") or (entry or {}).get("session_id") or "default")
            txt = str(r.get("text") or r.get("message_text") or r.get("episode_text") or r.get("topic") or "")
            sender_val = str(r.get("sender") or "User")
            sender_name_val = str(r.get("sender_name") or sender_val)
            ts_raw = r.get("timestamp") or r.get("created_at")
            ts_val = str(ts_raw) if ts_raw else None
            job_val = str(r["job_id"]) if r.get("job_id") else None
            ing_raw = r.get("ingested_at")
            ing_val = str(ing_raw) if ing_raw else None
            msg_items.append(
                {
                    "id": str(
                        r.get("id")
                        or uuid.uuid5(uuid.NAMESPACE_DNS, f"snowflake-memory-msg:{mem_uuid}:{s_id}:{idx}:{txt[:32]}")
                    ),
                    "timestamp": ts_val,
                    "sender": sender_val,
                    "sender_name": sender_name_val,
                    "session_id": s_id,
                    "text": txt,
                    "content_blocks": r.get("content_blocks") or [],
                    "job_id": job_val,
                    "ingested_at": ing_val,
                    "ingestion_timestamp": ing_val,
                    "status": str(r.get("status") or "Ingested"),
                    "outcome_score": r.get("outcome_score"),
                }
            )
        page_int = max(1, int(page or 1))
        size_int = max(1, int(size or 50))
        total = len(msg_items)
        return {
            "items": msg_items[(page_int - 1) * size_int : page_int * size_int],
            "total": total,
            "page": page_int,
            "size": size_int,
            "pages": max(1, (total + size_int - 1) // size_int),
        }

    def _list_lf_mem_messages(
        self, entry: dict[str, Any], *, session_id: str | None, page: int, size: int, user_id: str | None
    ) -> dict[str, Any]:
        from . import flow_context_guard as _guard  # noqa: PLC0415
        from .memories_lf_mem import LfMemSpec  # noqa: PLC0415

        empty = {"items": [], "total": 0, "page": page, "size": size, "pages": 1}
        flow_id = self._normalize_flow_id(str(entry.get("flow_id") or ""))
        if not self._lf_mem_entry_visible_to(entry, flow_id=flow_id, user_id=user_id):
            return {**empty, "ok": False, "error_code": "MEMORY_NOT_VISIBLE"}
        ident = _sf_identity.current_identity()
        owner = str(getattr(ident, "sf_user", "") or "").strip()
        if not owner:
            return {**empty, "ok": False, "error_code": "NO_CALLER_IDENTITY"}
        sess = str(session_id or "").strip()
        if sess.upper() in ("ALL", "ALL SESSIONS", "ALL SESSIONS (CROSS-SESSION)"):
            sess = ""
        spec = LfMemSpec(**entry["lf_mem"])
        sql, bindings = spec.build_messages_request(
            flow_id=flow_id, owner_user=owner, session_id=sess or None, limit=size, offset=(page - 1) * size
        )
        token = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_id)
        try:
            with _guard.flow_component_sql():
                res = self.execute_sql(sql, bindings=bindings, mode="caller")
        finally:
            CURRENT_SNOWFLAKE_FLOW_ID.reset(token)
        if not res.get("ok") or res.get("mode") == "offline":
            code = str(res.get("error_code") or ("NOT_CONFIGURED" if res.get("mode") == "offline" else "SNOWFLAKE_ERROR"))
            return {**empty, "ok": False, "error_code": code, "message": str(res.get("message") or code)}
        items: list[dict[str, Any]] = []
        total = 0
        for r in list(res.get("rows") or []):
            g = {str(k).lower(): v for k, v in dict(r).items()}
            total = int(g.get("total") or total or 0)
            ing = str(g["ingested_at"]) if g.get("ingested_at") else None
            items.append(
                {
                    "id": str(g.get("chunk_id")), "timestamp": ing, "sender": str(g.get("sender") or "User"),
                    "sender_name": str(g.get("sender_name") or g.get("sender") or "User"),
                    "session_id": str(g.get("session_id") or ""), "text": str(g.get("text") or ""),
                    "content_blocks": [], "job_id": str(g["job_id"]) if g.get("job_id") else None,
                    "ingested_at": ing, "ingestion_timestamp": ing, "status": "Ingested", "outcome_score": None,
                }
            )
        return {"items": items, "total": total, "page": page, "size": size, "pages": max(1, (total + size - 1) // size)}

    def patch_native_memory(
        self, memory_id: str, body: dict[str, Any] | None = None, *, user_id: str | None = None
    ) -> dict[str, Any]:
        """Update a Snowflake Memory via `PATCH /api/v1/memories/{id}` (`W2-R4-08G`)."""
        payload = body if isinstance(body, dict) else {}
        entry = self._find_memory_entry(memory_id)
        if entry is None:
            return {
                "ok": False,
                "status_code": 404,
                "error_code": ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                "message": f"Snowflake Memory '{memory_id}' not found.",
            }
        fqn = str(entry.get("table_fqn") or memory_id)
        denied = self.check_write_privilege_or_deny("MEMORY_UPDATE", fqn)
        if denied:
            return denied

        if "name" in payload and str(payload.get("name") or "").strip():
            entry["name"] = str(payload["name"]).strip()
            if not isinstance(entry.get("lf_mem"), dict):
                entry["kb_name"] = entry["name"]
        if payload.get("flow_id"):
            # BUG-MEM-MB-01: re-attaching a Memory Base to another flow updates memory_base.flow_id.
            new_fid = self._normalize_flow_id(str(payload["flow_id"]))
            if not new_fid:
                return {"ok": False, "status_code": 422, "error_code": "INVALID_FLOW_ID", "message": "Invalid flow_id."}
            entry["flow_id"] = new_fid
            lf_mem_cfg = entry.get("lf_mem") if isinstance(entry.get("lf_mem"), dict) else None
            if lf_mem_cfg:
                from .memories_lf_mem import lf_mem_table_name  # noqa: PLC0415

                self.register_object_in_context(
                    database=lf_mem_cfg["database"], schema=lf_mem_cfg["memory_schema"], category="memories",
                    object_name=lf_mem_table_name(lf_mem_cfg["name"]), flow_id=new_fid,
                )
        if "auto_capture" in payload and payload.get("auto_capture") is not None:
            entry["auto_capture"] = bool(payload["auto_capture"])
        for t_key in ("threshold", "ingestion_threshold"):
            if t_key in payload and payload.get(t_key) is not None:
                val = max(1, int(payload[t_key]))
                entry["threshold"] = val
                entry["ingestion_threshold"] = val
        llm_pre = dict(entry.get("llm_preprocessing") or {})
        if "preprocessing" in payload and payload.get("preprocessing") is not None:
            llm_pre["enabled"] = bool(payload["preprocessing"])
            entry["preprocessing"] = bool(payload["preprocessing"])
        if "preproc_model" in payload and payload.get("preproc_model"):
            llm_pre["model"] = str(payload["preproc_model"]).strip()
            entry["preproc_model"] = llm_pre["model"]
        if "preproc_instructions" in payload and payload.get("preproc_instructions") is not None:
            entry["preproc_instructions"] = str(payload["preproc_instructions"])
        if "preproc_kill_phrase" in payload and payload.get("preproc_kill_phrase"):
            kp = str(payload["preproc_kill_phrase"]).strip() or "NO_INGEST"
            llm_pre["kill_phrase"] = kp
            entry["kill_phrase"] = kp
            entry["preproc_kill_phrase"] = kp
        if llm_pre:
            entry["llm_preprocessing"] = llm_pre

        audit_details = {
            "memory_base_id": entry.get("memory_id"),
            "name": entry.get("name"),
            "table_fqn": fqn,
            "threshold": entry.get("threshold"),
        }
        patch_uid = user_id or entry.get("langflow_user_id")
        try:
            if patch_uid:
                self._persist_memory_in_langflow_sqlite(entry, flow_id=entry.get("flow_id"), user_id=patch_uid)
            elif entry.get("langflow_registration") == "REGISTERED":
                raise MemoryBasePersistenceError("An authenticated Langflow user is required.", 401)
        except Exception as exc:  # noqa: BLE001 - fail closed: report and audit the real failure
            failure = {
                "ok": False,
                "status_code": int(getattr(exc, "http_status", 500)),
                "error_code": "PERSISTENCE_ERROR",
            }
            emit_audit(
                self.record_audit_event,
                AuditAction.MEMORY_BASE_UPDATE,
                failure,
                target_object=fqn,
                details=audit_details,
                flow_id=entry.get("flow_id"),
            )
            return {**failure, "message": f"Snowflake Memory '{memory_id}' could not be updated."}
        # TR-01: the update is the local persistence above; audit only after it succeeded.
        emit_audit(
            self.record_audit_event,
            AuditAction.MEMORY_BASE_UPDATE,
            {"ok": True},
            target_object=fqn,
            details=audit_details,
            flow_id=entry.get("flow_id"),
        )
        updated = self._to_langflow_memory_base_read(entry)
        updated["ok"] = True
        updated["status_code"] = 200
        return updated

    def bootstrap_full_memory_showcase_flow(
        self,
        *,
        dry_run: bool = True,
        execute_simulation: bool = True,
        explicit_user_request: bool = True,
    ) -> dict[str, Any]:
        """Bootstrap and optionally run the `Snowflake_Full_Memory_Enterprise_Agent` showcase flow combining `#1..#5` + `#7` + `#8` (`W2-R4-07`, `W2-R9-06`)."""
        if not explicit_user_request and not _should_seed_showcase_flow():
            return {
                "ok": True,
                "skipped_unsolicited_seed": True,
                "explicit_user_request": False,
                "flow_persisted_sqlite": False,
                "flow_id": "f0000000-0000-4000-8000-000000000507",
                "flow_name": "Snowflake_Full_Memory_Enterprise_Agent",
            }
        self._showcase_explicitly_bootstrapped = True
        try:
            from .showcase_full_memory_agent import bootstrap_and_run_full_memory_showcase

            return bootstrap_and_run_full_memory_showcase(
                self,
                dry_run=dry_run,
                execute_simulation=execute_simulation,
                explicit_user_request=explicit_user_request,
            )
        except ImportError:
            pass

        showcase_flow_id = "f0000000-0000-4000-8000-000000000507"
        self.update_selected_context(
            {
                "database": "SNOWFLAKE_LANGFLOW",
                "schema": "CORE",
                "databases": ["SNOWFLAKE_LANGFLOW"],
                "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
                "postgres_instances": ["LANGFLOW_PG_INSTANCE"],
                "objects_by_db_schema": {
                    "SNOWFLAKE_LANGFLOW.CORE": {
                        "tables": ["FINANCIAL_LEDGER_Q4", "COST_CENTERS_DIM"],
                        "semantic_views": ["FINANCE_MARGIN_SEMANTIC_VIEW"],
                        "cortex_search_services": ["ACCOUNTING_POLICY_SEARCH_SVC"],
                    }
                },
            },
            flow_id=showcase_flow_id,
        )
        showcase_specs = [
            {
                "name": "MEM_1_GRAPH_CHECKPOINTER",
                "flow_id": showcase_flow_id,
                "agent_memory_type": "graph_state_checkpointer",
                "storage_family": "snowflake_postgres",
                "postgres_instance": "LANGFLOW_PG_INSTANCE",
                "pg_database": "postgres",
                "pg_schema": "public",
                "pg_table": "langgraph_checkpoints",
                "pg_table_mode": "PostgresSaver",
                "enable_embedding": False,
                "dry_run": dry_run,
            },
            {
                "name": "MEM_2_CHAT_WINDOW",
                "flow_id": showcase_flow_id,
                "agent_memory_type": "buffer_window_memory",
                "storage_family": "snowflake_postgres",
                "postgres_instance": "LANGFLOW_PG_INSTANCE",
                "pg_database": "postgres",
                "pg_schema": "public",
                "pg_table": "chat_message_history",
                "pg_table_mode": "Relational / JSONB",
                "filter_by_session": True,
                "window_k": 8,
                "enable_embedding": False,
                "dry_run": dry_run,
            },
            {
                "name": "MEM_3_SESSION_SUMMARY",
                "flow_id": showcase_flow_id,
                "agent_memory_type": "summary_memory",
                "storage_family": "snowflake_sql",
                "database": "SNOWFLAKE_LANGFLOW",
                "schema": "CORE",
                "table_type": "HYBRID",
                "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.CHAT_SESSION_SUMMARY",
                "primary_key": "(session_id)",
                "filter_by_session": True,
                "enable_embedding": False,
                "summarizer_model": "openai-gpt-6-luna",
                "llm_preprocessing": {
                    "enabled": True,
                    "engine": "proxy_chat",
                    "model": "openai-gpt-6-luna",
                    "kill_phrase": "NO_INGEST",
                },
                "dry_run": dry_run,
            },
            {
                "name": "MEM_4_USER_ENTITY_PROFILE",
                "flow_id": showcase_flow_id,
                "agent_memory_type": "user_profile_entity_memory",
                "storage_family": "snowflake_sql",
                "database": "SNOWFLAKE_LANGFLOW",
                "schema": "CORE",
                "table_type": "HYBRID",
                "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.USER_ENTITY_PROFILE",
                "primary_key": "(user_id, entity_namespace, memory_key)",
                "semi_structured_type": "VARIANT",
                "filter_by_session": False,
                "enable_embedding": False,
                "llm_preprocessing": {
                    "enabled": True,
                    "engine": "proxy_messages",
                    "model": "claude-sonnet-5-5",
                    "kill_phrase": "NO_INGEST",
                },
                "dry_run": dry_run,
            },
            {
                "name": "MEM_5_EPISODIC_FEWSHOT",
                "flow_id": showcase_flow_id,
                "agent_memory_type": "episodic_memory",
                "storage_family": "snowflake_sql",
                "database": "SNOWFLAKE_LANGFLOW",
                "schema": "CORE",
                "table_type": "HYBRID",
                "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.AGENT_EPISODIC_MEMORY",
                "primary_key": "(user_id, chunk_id)",
                "filter_by_session": False,
                "tool_mode": True,
                "enable_embedding": True,
                "embedding_provider": "snowflake_cortex",
                "embedding_model": "snowflake-arctic-embed-l-v2.0",
                "outcome_score_threshold": 0.90,
                "few_shot_k": 3,
                "dry_run": dry_run,
            },
        ]
        created_memories = []
        for spec in showcase_specs:
            res = self.create_or_register_memory(spec)
            if res.get("ok"):
                created_memories.append(res["memory"])
        return {
            "ok": True,
            "flow_name": "Snowflake_Full_Memory_Enterprise_Agent",
            "flow_id": showcase_flow_id,
            "memories_created": [m["name"] for m in created_memories],
            "memories": created_memories,
            "domain_tools_#7": ["Cortex Analyst (FINANCE_MARGIN_SEMANTIC_VIEW)", "Cortex Search (ACCOUNTING_POLICY_SEARCH_SVC)"],
            "procedural_memory_#8": "System Message (Enterprise Financial & Operations Rules + MEM_3_SESSION_SUMMARY + MEM_4_USER_ENTITY_PROFILE)",
        }

    def _is_technical_memory_table(self, table_name: str) -> bool:
        """Return True if `table_name` is a technical agent-memory backing table (`W2-R4-06B`, `CTX-ARCH-07` #4)."""
        clean = str(table_name or "").strip().upper()
        if not clean:
            return False
        base = clean.split("(")[0].strip().split(".")[-1]
        if base in CANONICAL_TECHNICAL_MEMORY_TABLES:
            return True
        if base.startswith("LANGFLOW_MEM_") or base.startswith("MEM_"):
            return True
        for mem in self.memories_registry:
            m_name = str(mem.get("name") or "").strip().upper()
            m_fqn = str(mem.get("table_fqn") or "").strip().upper()
            m_base = m_fqn.split(".")[-1] if m_fqn else ""
            if base in {m_name, m_base, f"MEM_{m_name}"} and base:
                return True
        return False

    def set_role_accessible_catalog(self, role: str, catalog: dict[str, Any]) -> None:
        """Configure the RBAC-accessible catalog for a specific Snowflake role (`CTX-ARCH-07` #1)."""
        clean_role = str(role or "").strip().upper()
        if clean_role and isinstance(catalog, dict):
            self.role_accessible_catalog[clean_role] = copy.deepcopy(catalog)

    def compute_role_effective_context(
        self,
        flow_id: str | None = None,
        role: str | None = None,
    ) -> dict[str, Any]:
        """Compute non-destructive intersection between a flow's stored context and the active role's accessible catalog (`CTX-ARCH-07` #1)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        stored_ctx = copy.deepcopy(self.get_flow_context(norm_fid, create=False) if norm_fid else self.selected_context)
        target_role = str(role or self.active_role or "").strip().upper()

        role_cat = self.role_accessible_catalog.get(target_role)
        is_restricted_public = target_role == "PUBLIC" and role_cat is None

        if role_cat is None and not is_restricted_public:
            return {
                "stored_context": stored_ctx,
                "effective_context": copy.deepcopy(stored_ctx),
                "inaccessible_objects": [],
                "retained_objects_count": len(self._extract_context_object_set(stored_ctx)[2]),
                "hidden_by_role_count": 0,
            }

        allowed_dbs: set[str] | None = None
        allowed_schemas_by_db: dict[str, set[str]] = {}
        allowed_objs_by_pair: dict[str, dict[str, set[str]]] = {}
        allowed_pg: set[str] | None = None

        if isinstance(role_cat, dict):
            if "databases" in role_cat and isinstance(role_cat["databases"], list):
                allowed_dbs = {str(d).strip().upper() for d in role_cat["databases"] if str(d).strip()}
            if "schemas_by_db" in role_cat and isinstance(role_cat["schemas_by_db"], dict):
                for db_k, sch_list in role_cat["schemas_by_db"].items():
                    if isinstance(sch_list, list):
                        allowed_schemas_by_db[str(db_k).strip().upper()] = {
                            str(s).strip().upper() for s in sch_list if str(s).strip()
                        }
            if "objects_by_db_schema" in role_cat and isinstance(role_cat["objects_by_db_schema"], dict):
                for pair_k, pair_v in role_cat["objects_by_db_schema"].items():
                    if isinstance(pair_v, dict):
                        allowed_objs_by_pair[str(pair_k).strip().upper()] = {
                            str(c_k).strip().lower(): {str(x).strip().upper() for x in c_v if str(x).strip()}
                            for c_k, c_v in pair_v.items()
                            if isinstance(c_v, list)
                        }
            if "postgres_instances" in role_cat and isinstance(role_cat["postgres_instances"], list):
                allowed_pg = {str(p).strip().upper() for p in role_cat["postgres_instances"] if str(p).strip()}

        effective_ctx = copy.deepcopy(stored_ctx)
        inaccessible: list[dict[str, Any]] = []
        retained_count = 0

        stored_dbs = [str(d).strip().upper() for d in (stored_ctx.get("databases") or []) if str(d).strip()]
        if allowed_dbs is not None:
            eff_dbs = []
            for d in stored_dbs:
                if d in allowed_dbs:
                    eff_dbs.append(d)
                else:
                    inaccessible.append(
                        {
                            "fqn": d,
                            "category": "database",
                            "role": target_role,
                            "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                        }
                    )
            effective_ctx["databases"] = eff_dbs

        eff_schemas_map: dict[str, list[str]] = {}
        for db_k, sch_list in (stored_ctx.get("schemas_by_db") or {}).items():
            db_u = str(db_k).strip().upper()
            if allowed_dbs is not None and db_u not in allowed_dbs:
                continue
            if not isinstance(sch_list, list):
                continue
            kept_sch: list[str] = []
            for s in sch_list:
                s_u = str(s).strip().upper()
                sch_allowed = True
                if db_u in allowed_schemas_by_db and s_u not in allowed_schemas_by_db[db_u]:
                    sch_allowed = False
                elif is_restricted_public and s_u != "PUBLIC":
                    sch_allowed = False
                if sch_allowed:
                    kept_sch.append(s_u)
                else:
                    inaccessible.append(
                        {
                            "fqn": f"{db_u}.{s_u}",
                            "category": "schema",
                            "role": target_role,
                            "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                        }
                    )
            eff_schemas_map[db_u] = kept_sch
        effective_ctx["schemas_by_db"] = eff_schemas_map

        for map_key in ("objects_by_db_schema", "selected_objects"):
            src_map = stored_ctx.get(map_key)
            if not isinstance(src_map, dict):
                continue
            new_map: dict[str, Any] = {}
            for pair_k, pair_v in src_map.items():
                pair_u = str(pair_k).strip().upper()
                parts = pair_u.split(".", 1)
                db_u = parts[0] if parts else ""
                sch_u = parts[1] if len(parts) > 1 else ""
                pair_allowed = True
                if allowed_dbs is not None and db_u not in allowed_dbs:
                    pair_allowed = False
                elif db_u in allowed_schemas_by_db and sch_u not in allowed_schemas_by_db[db_u]:
                    pair_allowed = False
                elif is_restricted_public and sch_u != "PUBLIC":
                    pair_allowed = False
                if not isinstance(pair_v, dict):
                    if pair_allowed:
                        new_map[pair_u] = copy.deepcopy(pair_v)
                    continue
                new_pair_bucket: dict[str, list[str]] = {}
                for cat_k, items in pair_v.items():
                    cat_l = str(cat_k).strip().lower()
                    if not isinstance(items, list):
                        continue
                    kept_items: list[str] = []
                    for item in items:
                        item_str = str(item).strip()
                        item_u = item_str.upper()
                        obj_allowed = pair_allowed
                        if obj_allowed and pair_u in allowed_objs_by_pair and cat_l in allowed_objs_by_pair[pair_u]:
                            obj_allowed = item_u in allowed_objs_by_pair[pair_u][cat_l]
                        if obj_allowed:
                            kept_items.append(item_str)
                            if map_key == "objects_by_db_schema":
                                retained_count += 1
                        else:
                            if map_key == "objects_by_db_schema":
                                inaccessible.append(
                                    {
                                        "fqn": f"{pair_u}.{item_str}",
                                        "database": db_u,
                                        "schema": sch_u,
                                        "name": item_str,
                                        "category": cat_l,
                                        "role": target_role,
                                        "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                                    }
                                )
                    new_pair_bucket[cat_l] = kept_items
                new_map[pair_u] = new_pair_bucket
            effective_ctx[map_key] = new_map

        stored_pg = [str(p).strip() for p in (stored_ctx.get("postgres_instances") or []) if str(p).strip()]
        if allowed_pg is not None or is_restricted_public:
            kept_pg: list[str] = []
            for p in stored_pg:
                if allowed_pg is not None and p.upper() in allowed_pg:
                    kept_pg.append(p)
                else:
                    inaccessible.append(
                        {
                            "fqn": p,
                            "name": p,
                            "category": "postgres_instances",
                            "role": target_role,
                            "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                        }
                    )
            effective_ctx["postgres_instances"] = kept_pg
            effective_ctx["selected_postgres_instances"] = list(kept_pg)

        return {
            "stored_context": stored_ctx,
            "effective_context": effective_ctx,
            "inaccessible_objects": inaccessible,
            "retained_objects_count": retained_count,
            "hidden_by_role_count": len(inaccessible),
        }

    def get_context_databases(self, flow_id: str | None = None) -> list[str]:
        """Return active databases in the Hub cone of visibility (`W2-R2-00`, `CTX-ARCH-03`, `CTX-ARCH-05`)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        if self.active_role.upper() in self.role_accessible_catalog:
            target_ctx = self.compute_role_effective_context(norm_fid, self.active_role)["effective_context"]
        dbs: list[str] = []
        for d in [*(target_ctx.get("databases") or []), *(target_ctx.get("full_databases") or [])]:
            clean_d = str(d).strip().upper()
            if clean_d and clean_d not in dbs:
                dbs.append(clean_d)
        for fs in target_ctx.get("full_schemas") or []:
            parts = str(fs).strip().upper().split(".", 1)
            if parts and parts[0] and parts[0] not in dbs:
                dbs.append(parts[0])
        if norm_fid and dbs:
            return dbs
        default_db = (target_ctx.get("database") or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        if default_db and default_db not in dbs and not norm_fid:
            dbs.insert(0, default_db)
        return dbs or [default_db or "SNOWFLAKE_LANGFLOW"]

    def get_context_schemas(self, database: str | None = None, flow_id: str | None = None) -> list[str]:
        """Return active schemas for `<database>` in the Hub cone of visibility (`W2-R2-00`, `CTX-ARCH-05`)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        if self.active_role.upper() in self.role_accessible_catalog:
            target_ctx = self.compute_role_effective_context(norm_fid, self.active_role)["effective_context"]
        ctx_db = str(target_ctx.get("database") or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        db = (database or ctx_db or "SNOWFLAKE_LANGFLOW").strip().upper()
        schemas_by_db = target_ctx.get("schemas_by_db") or {}
        schemas = [str(s).strip().upper() for s in (schemas_by_db.get(db) or []) if str(s).strip()]

        for fs in target_ctx.get("full_schemas") or []:
            parts = str(fs).strip().upper().split(".", 1)
            if len(parts) == 2 and parts[0] == db and parts[1] and parts[1] not in schemas:
                schemas.append(parts[1])

        full_dbs = {str(d).strip().upper() for d in (target_ctx.get("full_databases") or []) if str(d).strip()}
        if db in full_dbs and not schemas:
            disc = self.discover_schemas_only(database=db)
            for s in disc.get("schemas") or []:
                s_u = str(s).strip().upper()
                if s_u and s_u not in schemas:
                    schemas.append(s_u)
            target_ctx.setdefault("schemas_by_db", {})[db] = list(schemas)

        if not schemas and db == ctx_db:
            default_sch = str(target_ctx.get("schema") or self.schema or "CORE").strip().upper()
            schemas = [default_sch, "PUBLIC"] if default_sch != "PUBLIC" else ["PUBLIC"]
        return schemas or ["CORE", "PUBLIC"]

    def get_context_objects(
        self,
        database: str | None = None,
        schema: str | None = None,
        category: str = "tables",
        flow_id: str | None = None,
        *,
        exclude_memory_tables: bool = True,
    ) -> list[str]:
        """Return objects for `(<database>, <schema>, <category>)` filtered by `selected_objects` and wildcard cascades (`CTX-ARCH-05`, `W2-R4-06B`)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        if self.active_role.upper() in self.role_accessible_catalog:
            target_ctx = self.compute_role_effective_context(norm_fid, self.active_role)["effective_context"]
        ctx_db = str(target_ctx.get("database") or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        ctx_sch = str(target_ctx.get("schema") or self.schema or "CORE").strip().upper()
        db = (database or ctx_db or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or ctx_sch or "CORE").strip().upper()
        cat = (category or "tables").strip().lower()
        if cat == "postgres_instances":
            return self.get_context_postgres_instances(flow_id=norm_fid)
        db_sch_key = f"{db}.{sch}"

        full_dbs = {str(d).strip().upper() for d in (target_ctx.get("full_databases") or []) if str(d).strip()}
        full_schemas = {str(s).strip().upper() for s in (target_ctx.get("full_schemas") or []) if str(s).strip()}
        sel_objs = target_ctx.get("selected_objects")
        is_wildcard = (
            db in full_dbs
            or db_sch_key in full_schemas
            or (isinstance(sel_objs, dict) and sel_objs.get(db_sch_key) == "*")
            or (
                isinstance(sel_objs, dict)
                and isinstance(sel_objs.get(db_sch_key), dict)
                and sel_objs[db_sch_key].get(cat) == "*"
            )
        )

        objs_by_pair = target_ctx.get("objects_by_db_schema") or {}
        pair_bucket = objs_by_pair.get(db_sch_key) if isinstance(objs_by_pair, dict) else None

        if is_wildcard and (not isinstance(pair_bucket, dict) or cat not in pair_bucket):
            disc = self.discover_catalog(
                database=db,
                schema=sch,
                databases=[db],
                schemas_by_db={db: [sch]},
            )
            disc_pair = (disc.get("objects_by_db_schema") or {}).get(db_sch_key)
            if isinstance(disc_pair, dict):
                merged_pair = copy.deepcopy(pair_bucket) if isinstance(pair_bucket, dict) else {}
                for k_cat, v_list in disc_pair.items():
                    if k_cat not in merged_pair or not merged_pair[k_cat]:
                        merged_pair[k_cat] = copy.deepcopy(v_list)
                merged_pair.setdefault(cat, [])
                target_ctx.setdefault("objects_by_db_schema", {})[db_sch_key] = merged_pair
                pair_bucket = target_ctx["objects_by_db_schema"][db_sch_key]

        vis = target_ctx.get("visible_objects") or {}

        candidates: list[str] = []
        if isinstance(pair_bucket, dict) and cat in pair_bucket and isinstance(pair_bucket[cat], list):
            candidates.extend(str(x).strip() for x in pair_bucket[cat] if str(x).strip())
        elif isinstance(vis.get(cat), list):
            for item in vis[cat]:
                s_item = str(item).strip()
                if not s_item:
                    continue
                if "." in s_item and not s_item.startswith("@"):
                    parts = s_item.split(".")
                    if len(parts) >= 3 and (parts[0].upper() != db or parts[1].upper() != sch):
                        continue
                if s_item not in candidates:
                    candidates.append(s_item)

        allowed_set: set[str] | None = None
        if not is_wildcard:
            if isinstance(sel_objs, dict) and sel_objs:
                if db_sch_key in sel_objs and isinstance(sel_objs[db_sch_key], dict):
                    cat_sel = sel_objs[db_sch_key].get(cat)
                    if isinstance(cat_sel, list):
                        allowed_set = {str(x).strip().upper() for x in cat_sel if str(x).strip()}
                elif db_sch_key in sel_objs and isinstance(sel_objs[db_sch_key], list):
                    allowed_set = {str(x).strip().upper() for x in sel_objs[db_sch_key] if str(x).strip()}
                elif cat in sel_objs and isinstance(sel_objs[cat], list):
                    allowed_set = {str(x).strip().upper() for x in sel_objs[cat] if str(x).strip()}
            elif isinstance(sel_objs, list) and sel_objs:
                allowed_set = {str(x).strip().upper() for x in sel_objs if str(x).strip()}

        result_list: list[str] = candidates
        if allowed_set is not None:
            allowed_base_set = {a.split("(")[0].strip().split(".")[-1] for a in allowed_set}
            filtered: list[str] = []
            for item in candidates:
                raw_u = item.upper()
                base_u = raw_u.split("(")[0].strip().split(".")[-1]
                fqn_u = f"{db}.{sch}.{base_u}"
                if (
                    raw_u in allowed_set
                    or base_u in allowed_set
                    or fqn_u in allowed_set
                    or base_u in allowed_base_set
                ):
                    filtered.append(item)
            if not filtered and allowed_set:
                for raw_allowed in sorted(allowed_set):
                    if "." in raw_allowed:
                        parts = raw_allowed.split(".")
                        if len(parts) >= 3 and parts[0].upper() == db and parts[1].upper() == sch:
                            filtered.append(parts[-1])
                    else:
                        filtered.append(raw_allowed)
            result_list = filtered

        if exclude_memory_tables and cat in ("tables", "views", "iceberg_tables"):
            result_list = [x for x in result_list if not self._is_technical_memory_table(x)]
        return result_list

    def get_context_postgres_instances(self, flow_id: str | None = None) -> list[str]:
        """Return account-level Snowflake Postgres instances selected in the flow's context (`CTX-ARCH-05`)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        if self.active_role.upper() in self.role_accessible_catalog:
            target_ctx = self.compute_role_effective_context(norm_fid, self.active_role)["effective_context"]

        instances: list[str] = []
        for top_key in ("postgres_instances", "selected_postgres_instances"):
            val = target_ctx.get(top_key)
            if isinstance(val, list):
                for item in val:
                    s_item = str(item).strip()
                    if s_item and s_item not in instances:
                        instances.append(s_item)

        # Backward-compatibility union with legacy nested selected_objects / objects_by_db_schema / visible_objects
        for map_key in ("selected_objects", "objects_by_db_schema"):
            m = target_ctx.get(map_key)
            if isinstance(m, dict):
                for pair_v in m.values():
                    if isinstance(pair_v, dict) and isinstance(pair_v.get("postgres_instances"), list):
                        for item in pair_v["postgres_instances"]:
                            s_item = str(item).strip()
                            if s_item and s_item not in instances:
                                instances.append(s_item)

        vis_pg = (target_ctx.get("visible_objects") or {}).get("postgres_instances")
        if isinstance(vis_pg, list):
            for item in vis_pg:
                s_item = str(item).strip()
                if s_item and s_item not in instances:
                    instances.append(s_item)
        return instances

    def get_context_postgres_tables(
        self,
        postgres_instance: str | None = None,
        postgres_database: str = "postgres",
        postgres_schema: str = "public",
        flow_id: str | None = None,
        *,
        exclude_memory_tables: bool = True,
    ) -> list[str]:
        """Return cascading Postgres tables for `<postgres_instance>` filtered by flow context (`CTX-ARCH-05`, `W2-R4-06B`)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        inst = str(postgres_instance or "").strip()
        pg_db = str(postgres_database or "postgres").strip()
        pg_sch = str(postgres_schema or "public").strip()

        tables: list[str] = []
        pg_tbl_map = target_ctx.get("postgres_tables_by_instance") or target_ctx.get("selected_postgres_tables") or {}
        if isinstance(pg_tbl_map, dict) and inst:
            for lookup_k in (inst, inst.upper(), f"{inst}.{pg_db}.{pg_sch}", f"{inst.upper()}.{pg_db.upper()}.{pg_sch.upper()}"):
                if lookup_k in pg_tbl_map and isinstance(pg_tbl_map[lookup_k], list):
                    for t in pg_tbl_map[lookup_k]:
                        s_t = str(t).strip()
                        if s_t and s_t not in tables:
                            tables.append(s_t)
        if not tables:
            ctx_tables = self.get_context_objects(
                target_ctx.get("database"),
                target_ctx.get("schema"),
                "tables",
                flow_id=norm_fid,
                exclude_memory_tables=exclude_memory_tables,
            )
            for t in ctx_tables:
                s_t = str(t).strip()
                if s_t and s_t not in tables:
                    tables.append(s_t)
        if not tables:
            tables = ["orders", "customers", "events"]
        if exclude_memory_tables:
            tables = [t for t in tables if not self._is_technical_memory_table(t)]
        return tables

    def register_object_in_context(
        self,
        database: str | None = None,
        schema: str | None = None,
        category: str = "tables",
        object_name: str = "",
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Automatically add a newly created/provisioned Snowflake object to the active flow context (`CTX-ARCH-07` #2)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        db = str(database or target_ctx.get("database") or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = str(schema or target_ctx.get("schema") or self.schema or "CORE").strip().upper()
        raw_cat = str(category or "tables").strip().lower()
        clean_name = str(object_name or "").strip()
        if not clean_name:
            return target_ctx

        cat_aliases = {
            "table": "tables",
            "tables": "tables",
            "standard": "tables",
            "hybrid": "tables",
            "interactive": "tables",
            "dynamic": "tables",
            "iceberg": "iceberg_tables",
            "iceberg_table": "iceberg_tables",
            "iceberg_tables": "iceberg_tables",
            "view": "views",
            "views": "views",
            "semantic_view": "semantic_views",
            "semantic_views": "semantic_views",
            "stage": "stages",
            "stages": "stages",
            "stream": "streams",
            "streams": "streams",
            "function": "functions",
            "functions": "functions",
            "udf": "functions",
            "udtf": "functions",
            "procedure": "procedures",
            "procedures": "procedures",
            "stored_procedure": "procedures",
            "stored procedure": "procedures",
            "cortex_search": "cortex_search_services",
            "cortex_search_service": "cortex_search_services",
            "cortex_search_services": "cortex_search_services",
            "search_service": "cortex_search_services",
            "agent": "cortex_agents",
            "cortex_agent": "cortex_agents",
            "cortex_agents": "cortex_agents",
            "postgres": "postgres_instances",
            "postgres_instance": "postgres_instances",
            "postgres_instances": "postgres_instances",
            "database": "database",
            "schema": "schema",
        }
        cat = cat_aliases.get(raw_cat, raw_cat)

        if cat == "database":
            db_u = clean_name.upper()
            dbs = target_ctx.setdefault("databases", [])
            if db_u not in dbs:
                dbs.append(db_u)
            target_ctx.setdefault("schemas_by_db", {}).setdefault(db_u, ["PUBLIC"])
        elif cat == "schema":
            sch_u = clean_name.upper().split(".")[-1]
            dbs = target_ctx.setdefault("databases", [])
            if db not in dbs:
                dbs.append(db)
            sch_list = target_ctx.setdefault("schemas_by_db", {}).setdefault(db, [])
            if sch_u not in sch_list:
                sch_list.append(sch_u)
        elif cat == "postgres_instances":
            for pg_k in ("postgres_instances", "selected_postgres_instances"):
                pg_list = target_ctx.setdefault(pg_k, [])
                if clean_name not in pg_list:
                    pg_list.append(clean_name)
            vis_pg = target_ctx.setdefault("visible_objects", {}).setdefault("postgres_instances", [])
            if clean_name not in vis_pg:
                vis_pg.append(clean_name)
        else:
            if cat in ("tables", "views", "iceberg_tables") and self._is_technical_memory_table(clean_name):
                return target_ctx
            short_name = clean_name.split(".")[-1] if "." in clean_name and not clean_name.startswith("@") else clean_name
            dbs = target_ctx.setdefault("databases", [])
            if db not in dbs:
                dbs.append(db)
            sch_list = target_ctx.setdefault("schemas_by_db", {}).setdefault(db, [])
            if sch not in sch_list:
                sch_list.append(sch)
            pair_key = f"{db}.{sch}"
            pair_bucket = target_ctx.setdefault("objects_by_db_schema", {}).setdefault(pair_key, {})
            cat_list = pair_bucket.setdefault(cat, [])
            if short_name not in cat_list:
                cat_list.append(short_name)
            if cat == "iceberg_tables":
                tbl_list = pair_bucket.setdefault("tables", [])
                if short_name not in tbl_list:
                    tbl_list.append(short_name)

            # PASSO3: the registered object itself becomes authorized for the flow guard (exactly this
            # object/category; a "*" selection already covers it). Previously only pre-existing category
            # lists were extended, so a component locked itself out of streams/tables it had just created.
            sel_objs = target_ctx.setdefault("selected_objects", {})
            if isinstance(sel_objs, dict):
                pair_sel = sel_objs.setdefault(pair_key, {})
                if isinstance(pair_sel, dict):
                    bucket = pair_sel.setdefault(cat, [])
                    if isinstance(bucket, list) and short_name not in bucket:
                        bucket.append(short_name)

            vis_list = target_ctx.setdefault("visible_objects", {}).setdefault(cat, [])
            if short_name not in vis_list:
                vis_list.append(short_name)

        self.context_epoch += 1
        target_ctx["context_epoch"] = self.context_epoch
        self._dbs_cache.pop(self.active_role, None)
        self._schemas_cache.pop(f"{self.active_role}:{db}", None)
        self._objects_cache.pop(f"{self.active_role}:{db}.{sch}", None)

        if norm_fid:
            self.contexts_by_flow_id[norm_fid] = target_ctx
            self._save_flow_context_sqlite(norm_fid, target_ctx)
        else:
            self.selected_context = target_ctx
            self._save_flow_context_sqlite("__global__", target_ctx)
        return target_ctx

    def _auto_register_ddl_object_if_create(
        self,
        statement: str,
        flow_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Inspect a SQL statement and, if it is a user CREATE DDL, auto-register the target object in the flow context (`CTX-ARCH-07` #2)."""
        stmt = (statement or "").strip()
        if not stmt or not stmt.upper().startswith("CREATE "):
            return None
        upper_stmt = stmt.upper()
        if any(
            skip_tok in upper_stmt
            for skip_tok in (
                "SNOWFLAKE_FLOW_CONTEXTS",
                "LANGFLOW_FLOW_RUNS",
                "LANGFLOW_FLOW_SPANS",
                "LANGFLOW_AUDIT_LOG",
                "LANGFLOW_FLOW_ACTIVITY",
                "LANGFLOW_TRACE_RAP",
                "V_LANGFLOW_ACTIVE_",
                "ROW ACCESS POLICY",
            )
        ):
            return None

        m_pg = re.match(
            r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?POSTGRES\s+INSTANCE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_$\".]+)",
            stmt,
        )
        if m_pg:
            pg_name = m_pg.group(1).strip().strip('"')
            return self.register_object_in_context(None, None, "postgres_instances", pg_name, flow_id=flow_id)

        m_db = re.match(
            r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+)?DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_$\"]+)",
            stmt,
        )
        if m_db:
            db_name = m_db.group(1).strip().strip('"').upper()
            return self.register_object_in_context(db_name, None, "database", db_name, flow_id=flow_id)

        m_sch = re.match(
            r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+)?SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_$\".]+)",
            stmt,
        )
        if m_sch:
            raw_target = m_sch.group(1).strip().strip('"')
            parts = [p.strip('"') for p in raw_target.split(".") if p.strip()]
            if len(parts) >= 2:
                return self.register_object_in_context(parts[0].upper(), parts[1].upper(), "schema", parts[1].upper(), flow_id=flow_id)
            return self.register_object_in_context(self.database, parts[0].upper(), "schema", parts[0].upper(), flow_id=flow_id)

        m_obj = re.match(
            r"(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?"
            r"(?:(?P<mod>TEMP|TEMPORARY|LOCAL|GLOBAL|VOLATILE|TRANSIENT|ICEBERG|DYNAMIC|INTERACTIVE|HYBRID|SEMANTIC|EXTERNAL|INTERNAL|SECURE)\s+)*"
            r"(?P<kind>CORTEX\s+SEARCH\s+SERVICE|CORTEX\s+AGENT|AGENT|TABLE|VIEW|STAGE|STREAM|FUNCTION|PROCEDURE)\s+"
            r"(?:IF\s+NOT\s+EXISTS\s+)?(?P<ident>[A-Za-z0-9_$\".]+)",
            stmt,
        )
        if not m_obj:
            return None
        full_prefix = stmt[: m_obj.start("kind")].upper()
        raw_kind = re.sub(r"\s+", "_", m_obj.group("kind").strip().upper())
        raw_ident = m_obj.group("ident").strip().strip('"')
        parts = [p.strip('"') for p in raw_ident.split(".") if p.strip()]
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context
        default_db = str(ctx.get("database") or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        default_sch = str(ctx.get("schema") or self.schema or "CORE").strip().upper()
        if len(parts) >= 3:
            db_n, sch_n, obj_n = parts[0].upper(), parts[1].upper(), parts[2]
        elif len(parts) == 2:
            db_n, sch_n, obj_n = default_db, parts[0].upper(), parts[1]
        else:
            db_n, sch_n, obj_n = default_db, default_sch, parts[0]

        if self._is_technical_memory_table(obj_n):
            return None

        if raw_kind == "TABLE":
            cat = "iceberg_tables" if "ICEBERG" in full_prefix else "tables"
        elif raw_kind == "VIEW":
            cat = "semantic_views" if "SEMANTIC" in full_prefix else "views"
        elif raw_kind == "STAGE":
            cat = "stages"
        elif raw_kind == "STREAM":
            cat = "streams"  # PASSO3: a stream the flow created is readable by the flow's components
        elif raw_kind == "FUNCTION":
            cat = "functions"
        elif raw_kind == "PROCEDURE":
            cat = "procedures"
        elif raw_kind == "CORTEX_SEARCH_SERVICE":
            cat = "cortex_search_services"
        elif raw_kind in ("CORTEX_AGENT", "AGENT"):
            cat = "cortex_agents"
        else:
            cat = "tables"

        return self.register_object_in_context(db_n, sch_n, cat, obj_n, flow_id=norm_fid)

    # PASSO1/A6: creation privileges come from Snowflake's own evaluation, never from role names.
    # EXPLAIN_PRIVILEGES(missing_only => TRUE, for_role => R) follows R's full role hierarchy and
    # ownership, analyses without executing, and all probes run in ONE query per (role, epoch, db, schema).
    _PRIVILEGE_PROBE_TTL_SECONDS = 300.0
    # A failing probe (e.g. EXPLAIN_PRIVILEGES error) is cached briefly so the 5 can_create_* calls
    # of one get_role_privileges() do not re-issue it; short so a transient error recovers fast.
    _PRIVILEGE_PROBE_DENIED_TTL_SECONDS = 10.0
    _PROBE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
    _PRIVILEGE_PROBES: tuple[tuple[str, str], ...] = (
        ("DATABASE", "CREATE DATABASE LF_PRIV_PROBE"),
        ("SCHEMA", "CREATE SCHEMA {db}.LF_PRIV_PROBE"),
        ("OBJECT", "CREATE TABLE {db}.{sch}.LF_PRIV_PROBE (X INT)"),
        ("STAGE", "CREATE STAGE {db}.{sch}.LF_PRIV_PROBE"),
        (
            "EXTERNAL_VOLUME",
            "CREATE EXTERNAL VOLUME LF_PRIV_PROBE STORAGE_LOCATIONS = ((NAME = ''p'' STORAGE_PROVIDER = ''S3'' "
            "STORAGE_BASE_URL = ''s3://probe/'' STORAGE_AWS_ROLE_ARN = ''arn:aws:iam::000000000000:role/p''))",
        ),
    )

    def _privilege_probe(self) -> dict[str, bool]:
        """{probe: authorized} for the active role; every probe is False when unknown (fail closed)."""
        denied = {name: False for name, _ in self._PRIVILEGE_PROBES}
        role = _sf_identity.validate_role_identifier(self.active_role)
        db = str(self.database or "").strip().upper()
        sch = str(self.schema or "").strip().upper()
        if role is None or not self._PROBE_IDENT_RE.match(db) or not self._PROBE_IDENT_RE.match(sch):
            return denied
        key = (role, self.context_epoch, db, sch)
        cache = self.__dict__.setdefault("_privilege_probe_cache", {})
        hit = cache.get(key)
        if hit and hit[0] > time.monotonic():
            return dict(hit[1])
        cols = ", ".join(
            f"EXPLAIN_PRIVILEGES(statement => '{tmpl.format(db=db, sch=sch)}', missing_only => TRUE, "
            f"for_role => '{role}') AS {name}"
            for name, tmpl in self._PRIVILEGE_PROBES
        )
        res = self.execute_sql(f"SELECT {cols}", role=role, timeout=15)
        rows = res.get("rows") if res.get("ok") else None
        if not rows:
            cache[key] = (time.monotonic() + self._PRIVILEGE_PROBE_DENIED_TTL_SECONDS, dict(denied))
            return denied
        verdict: dict[str, bool] = {}
        for name, _tmpl in self._PRIVILEGE_PROBES:
            raw = (rows[0] or {}).get(name)
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                verdict[name] = bool(isinstance(parsed, dict) and parsed.get("authorized") is True)
            except (TypeError, ValueError):
                verdict[name] = False
        cache[key] = (time.monotonic() + self._PRIVILEGE_PROBE_TTL_SECONDS, verdict)
        return dict(verdict)

    def can_create_database(self) -> bool:
        """True only if Snowflake reports CREATE DATABASE as authorized for the active role."""
        return self._privilege_probe()["DATABASE"]

    def can_create_schema(self) -> bool:
        """True only if Snowflake reports CREATE SCHEMA in the bridge database as authorized."""
        return self._privilege_probe()["SCHEMA"]

    def can_create_object(self) -> bool:
        """True only if the active role may create schema-level objects (probed as CREATE TABLE)."""
        return self._privilege_probe()["OBJECT"]

    def can_create_stage(self) -> bool:
        """True only if Snowflake reports CREATE STAGE in the bridge schema as authorized (`W2-R5-05`)."""
        return self._privilege_probe()["STAGE"]

    def can_create_external_volume(self) -> bool:
        """True only if Snowflake reports CREATE EXTERNAL VOLUME as authorized (`W2-R5-05`)."""
        return self._privilege_probe()["EXTERNAL_VOLUME"]

    def get_role_privileges(self) -> dict[str, bool]:
        """Compute granular active-role creation privileges for Hub Section 1 & sidebar components."""
        can_db = self.can_create_database()
        can_sch = self.can_create_schema()
        can_obj = self.can_create_object()
        can_stg = self.can_create_stage()
        can_ext_vol = self.can_create_external_volume()
        return {
            "can_create_database": can_db,
            "can_create_schema": can_sch,
            "can_create_table": can_obj,
            "can_create_stage": can_stg,
            "can_create_external_volume": can_ext_vol,
            "can_create_view": can_obj,
            "can_create_function": can_obj,
            "can_create_procedure": can_obj,
            "can_create_cortex_search": can_obj,
            "can_create_postgres": can_db,
            "can_create_object": can_obj,
        }

    def is_stage_visible_in_context(
        self,
        stage_entry: dict[str, Any],
        flow_id: str | None = None,
    ) -> bool:
        """Return True iff `stage_entry`'s `<DATABASE>.<SCHEMA>` is enabled in `[ ❄️ Context ]` (`W2-R5-05`)."""
        if not isinstance(stage_entry, dict):
            return False
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id)
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context

        stg_db = str(stage_entry.get("database") or "").strip().upper()
        stg_sch = str(stage_entry.get("schema") or "").strip().upper()
        raw_fqn = str(stage_entry.get("stage_fqn") or stage_entry.get("fqn") or "").lstrip("@").strip()
        if (not stg_db or not stg_sch) and "." in raw_fqn:
            parts = [p.strip().upper() for p in raw_fqn.split(".") if p.strip()]
            if len(parts) >= 2:
                stg_db = stg_db or parts[0]
                stg_sch = stg_sch or parts[1]

        raw_dbs = target_ctx.get("databases")
        if isinstance(raw_dbs, list):
            active_dbs = {str(d).strip().upper() for d in raw_dbs if str(d).strip()}
            if stg_db and stg_db not in active_dbs:
                return False

        schemas_map = target_ctx.get("schemas_by_db")
        if isinstance(schemas_map, dict) and stg_db and stg_sch:
            for db_k, sch_list in schemas_map.items():
                if str(db_k).strip().upper() == stg_db and isinstance(sch_list, list):
                    active_schemas = {str(s).strip().upper() for s in sch_list if str(s).strip()}
                    if stg_sch not in active_schemas:
                        return False
                    break

        sel_objs = target_ctx.get("selected_objects")
        pair_key = f"{stg_db}.{stg_sch}"
        if isinstance(sel_objs, dict) and pair_key in sel_objs and isinstance(sel_objs[pair_key], dict):
            pair_sel = sel_objs[pair_key]
            if "stages" in pair_sel and isinstance(pair_sel["stages"], list):
                allowed_stages = {str(s).lstrip("@").split(".")[-1].strip().upper() for s in pair_sel["stages"] if str(s).strip()}
                stg_nm = str(stage_entry.get("name") or raw_fqn.split(".")[-1]).lstrip("@").strip().upper()
                if allowed_stages and stg_nm not in allowed_stages:
                    return False
        return True

    def list_stages_for_context(
        self,
        *,
        flow_id: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Return Internal Stages (`@<DB>.<SCHEMA>.<STAGE>`) filtered by `[ ❄️ Context ]` (`W2-R5-05`).

        BUG-CTX-STG-01: only INTERNAL (Snowflake-managed) stages are returned; EXTERNAL stages and stages
        whose type is not known from real SHOW STAGES metadata are excluded.
        """
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id)
        target_ctx = self.get_flow_context(norm_fid) if norm_fid else self.selected_context

        by_fqn: dict[str, dict[str, Any]] = {}
        for stg in self.stages_registry:
            if not self.is_stage_visible_in_context(stg, flow_id=norm_fid):
                continue
            s_db = str(stg.get("database") or self.database).strip().upper()
            s_sch = str(stg.get("schema") or self.schema).strip().upper()
            if database and s_db != database.strip().upper():
                continue
            if schema and s_sch != schema.strip().upper():
                continue
            s_name = str(stg.get("name") or self.stage_name).strip().upper()
            fqn_key = f"{s_db}.{s_sch}.{s_name}"
            entry_copy = dict(stg)
            entry_copy["name"] = s_name
            entry_copy["database"] = s_db
            entry_copy["schema"] = s_sch
            entry_copy["fqn"] = fqn_key
            entry_copy["stage_fqn"] = f"@{fqn_key}"
            entry_copy["at_stage"] = f"@{fqn_key}"
            entry_copy["files_count"] = None
            by_fqn[fqn_key] = entry_copy

        # Also harvest stages from objects_by_db_schema / selected_objects in target_ctx
        for map_key in ("objects_by_db_schema", "selected_objects"):
            obj_map = target_ctx.get(map_key)
            if not isinstance(obj_map, dict):
                continue
            for pair_str, bucket in obj_map.items():
                if not isinstance(bucket, dict) or "." not in str(pair_str):
                    continue
                p_db, p_sch = [p.strip().upper() for p in str(pair_str).split(".", 1)]
                if database and p_db != database.strip().upper():
                    continue
                if schema and p_sch != schema.strip().upper():
                    continue
                for raw_stg in bucket.get("stages") or []:
                    s_nm = str(raw_stg or "").lstrip("@").split(".")[-1].strip().upper()
                    if not s_nm:
                        continue
                    fqn_key = f"{p_db}.{p_sch}.{s_nm}"
                    if fqn_key in by_fqn:
                        continue
                    meta = self._stage_meta.get(fqn_key) or {}
                    st_type = str(meta.get("type") or "").upper()
                    is_external = "EXTERNAL" in st_type
                    norm_type = "EXTERNAL" if is_external else ("INTERNAL" if st_type else "UNKNOWN")
                    cand = {
                        "name": s_nm,
                        "database": p_db,
                        "schema": p_sch,
                        "fqn": fqn_key,
                        "stage_fqn": f"@{fqn_key}",
                        "at_stage": f"@{fqn_key}",
                        "type": norm_type,
                        "stage_type": norm_type,
                        "kind": f"{norm_type}_STAGE",
                        "directory_enabled": bool(meta.get("directory_enabled")),
                        "url": str(meta.get("url") or ""),
                        "cloud_provider": str(meta.get("cloud") or ""),
                        "storage_integration": meta.get("storage_integration"),
                        "comment": str(meta.get("comment") or ""),
                        "upload_supported": False,
                        "files_count": None,
                    }
                    if self.is_stage_visible_in_context(cand, flow_id=norm_fid):
                        by_fqn[fqn_key] = cand

        storage_integrations: list[str] = []
        int_res = self.execute_sql("SHOW STORAGE INTEGRATIONS")
        for r in int_res.get("rows") or [] if int_res.get("ok") else []:
            if isinstance(r, dict):
                int_nm = str(r.get("name") or r.get("NAME") or "").strip()
                if int_nm and int_nm not in storage_integrations:
                    storage_integrations.append(int_nm)

        ext_vols_res = self.list_external_volumes_for_context(flow_id=norm_fid)
        stages_list = [stg for fqn, stg in by_fqn.items() if self._entry_stage_type(fqn, stg) == "INTERNAL"]
        return {
            "ok": True,
            "active_role": self.active_role,
            "flow_id": norm_fid,
            "can_create_stage": self.can_create_stage(),
            "can_create_external_volume": self.can_create_external_volume(),
            "role_privileges": self.role_privileges,
            "stages": stages_list,
            "stage_names": [s["stage_fqn"] for s in stages_list],
            "external_volumes": ext_vols_res.get("external_volumes", []),
            "storage_integrations": storage_integrations,
        }

    def list_external_volumes_for_context(
        self,
        *,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Return External Volumes (`SHOW EXTERNAL VOLUMES`) available in `[ ❄️ Context ]` (`W2-R5-05`)."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id)
        by_name: dict[str, dict[str, Any]] = {}
        vol_res = self.execute_sql("SHOW EXTERNAL VOLUMES")
        if not vol_res.get("ok", False):
            return {
                "ok": False,
                "status_code": int(vol_res.get("status_code") or 502),
                "error_code": "SHOW_EXTERNAL_VOLUMES_FAILED",
                "message": str(vol_res.get("message") or vol_res.get("error") or "SHOW EXTERNAL VOLUMES failed"),
                "active_role": self.active_role,
                "flow_id": norm_fid,
                "external_volumes": [],
            }
        for r in vol_res.get("rows") or []:
            if isinstance(r, dict):
                v_nm = str(r.get("name") or r.get("NAME") or "").strip()
                if v_nm and v_nm.upper() not in by_name:
                    # Only columns actually returned by SHOW EXTERNAL VOLUMES; nothing invented.
                    by_name[v_nm.upper()] = {
                        "name": v_nm,
                        "type": "EXTERNAL_VOLUME",
                        "allow_writes": r.get("allow_writes", r.get("ALLOW_WRITES")),
                        "comment": r.get("comment", r.get("COMMENT")),
                        "owner": r.get("owner", r.get("OWNER")),
                        "created_on": r.get("created_on", r.get("CREATED_ON")),
                    }

        return {
            "ok": True,
            "active_role": self.active_role,
            "flow_id": norm_fid,
            "can_create_external_volume": self.can_create_external_volume(),
            "external_volumes": list(by_name.values()),
        }

    def create_stage_governed(
        self,
        body: dict[str, Any],
        *,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Build & execute governed `CREATE STAGE` (Internal `SNOWFLAKE_SSE`/`SNOWFLAKE_FULL` or External `S3`/`GCS`/`AZURE`/`S3COMPAT`) and auto-register in `[ ❄️ Context ]` (`W2-R5-05`)."""
        payload = dict(body or {})
        eff_fid = self._normalize_flow_id(
            flow_id or payload.get("flow_id") or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id
        )
        target_ctx = self.get_flow_context(eff_fid) if eff_fid else self.selected_context
        ctx_dbs = list(target_ctx.get("databases") or [target_ctx.get("database") or self.database])
        default_db = str(target_ctx.get("database") or (ctx_dbs[0] if ctx_dbs else self.database) or self.database).strip().upper()
        default_sch_list = list((target_ctx.get("schemas_by_db") or {}).get(default_db) or [target_ctx.get("schema") or self.schema])
        default_sch = str(target_ctx.get("schema") or (default_sch_list[0] if default_sch_list else self.schema) or self.schema).strip().upper()

        db = str(payload.get("database") or default_db).strip().upper()
        sch = str(payload.get("schema") or payload.get("schema_name") or default_sch).strip().upper()
        raw_name = str(payload.get("name") or payload.get("stage_name") or "").lstrip("@").split(".")[-1].strip()
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in (raw_name or "LANGFLOW_STAGE")).upper()
        try:
            # BUG-UI-SQLI-05: body/Context identifiers and enum keywords are validated before the DDL.
            db = _sql_safety.validate_identifier(db, "database")
            sch = _sql_safety.validate_identifier(sch, "schema")
            safe_name = _sql_safety.validate_identifier(safe_name, "stage_name")
            _stg_provider = str(payload.get("cloud_provider") or payload.get("provider") or "S3").strip().upper()
            _sql_safety.validate_enum(_stg_provider, _STAGE_CLOUD_PROVIDERS, "cloud_provider")
            for _key in ("storage_integration", "notification_integration"):
                if str(payload.get(_key) or "").strip():
                    _sql_safety.validate_identifier(str(payload.get(_key)), _key)
            _ff = str(payload.get("file_format") or "").strip().upper()
            if _ff and _ff not in ("NONE", "CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"):
                _sql_safety.validate_fqn(_ff, "file_format", min_parts=1, max_parts=3)
            _enc = str(payload.get("encryption") or payload.get("encryption_type") or "").strip().upper()
            _is_ext = "EXTERNAL" in str(payload.get("stage_type") or payload.get("type") or "").upper() or bool(
                payload.get("url") or payload.get("cloud_url")
            )
            if _enc and _is_ext:
                _sql_safety.validate_enum(_enc, _EXTERNAL_ENCRYPTION_TYPES, "encryption")
        except _sql_safety.SqlSafetyError as exc:
            return {"ok": False, "status_code": 400, "error_code": "INVALID_IDENTIFIER", "message": str(exc)}
        fqn = f"{db}.{sch}.{safe_name}"

        if not self.can_create_stage():
            msg = f"Active role {self.active_role} does not have CREATE STAGE privilege on {db}.{sch}"
            self.record_audit_event(
                action="STAGE_CREATE",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=fqn,
                flow_id=eff_fid,
                details={"message": msg},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": msg,
                "banner": msg,
                "active_role": self.active_role,
            }

        denied = self.check_write_privilege_or_deny("STAGE_CREATE", fqn)
        if denied:
            return denied

        stage_type = str(payload.get("stage_type") or payload.get("type") or "INTERNAL").strip().upper()
        if "EXTERNAL" in stage_type or payload.get("url") or payload.get("cloud_url"):
            stage_type = "EXTERNAL"
        else:
            stage_type = "INTERNAL"

        create_mode = str(payload.get("create_mode") or "CREATE IF NOT EXISTS").strip().upper()
        if payload.get("or_replace") is True or "REPLACE" in create_mode:
            create_prefix = f"CREATE OR REPLACE STAGE {fqn}"
        elif payload.get("if_not_exists", True) or "IF NOT EXISTS" in create_mode:
            create_prefix = f"CREATE STAGE IF NOT EXISTS {fqn}"
        else:
            create_prefix = f"CREATE STAGE {fqn}"

        directory_enabled = bool(payload.get("directory_enabled", payload.get("enable_directory", True)))
        auto_refresh = bool(payload.get("auto_refresh", False))
        notification_integration = str(payload.get("notification_integration") or "").strip()
        file_format = str(payload.get("file_format") or "").strip().upper()
        comment = str(payload.get("comment") or "").strip()
        safe_comment = comment.replace("\\", "\\\\").replace("'", "''")

        clauses: list[str] = [create_prefix]
        cloud_url = ""
        cloud_provider = "SNOWFLAKE"
        storage_integration: str | None = None
        encryption_type = "SNOWFLAKE_SSE"

        if stage_type == "INTERNAL":
            raw_enc = str(payload.get("encryption") or payload.get("encryption_type") or "SNOWFLAKE_SSE").strip().upper()
            encryption_type = "SNOWFLAKE_FULL" if "FULL" in raw_enc else "SNOWFLAKE_SSE"
            clauses.append(f"ENCRYPTION = (TYPE = '{encryption_type}')")
            if directory_enabled:
                dir_parts = ["ENABLE = TRUE"]
                if auto_refresh:
                    dir_parts.append("AUTO_REFRESH = TRUE")
                clauses.append(f"DIRECTORY = ({' '.join(dir_parts)})")
        else:
            cloud_provider = str(payload.get("cloud_provider") or payload.get("provider") or "S3").strip().upper()
            raw_url = str(payload.get("url") or payload.get("cloud_url") or "").strip()
            if not raw_url:
                scheme_map = {
                    "S3": "s3://",
                    "S3GOV": "s3gov://",
                    "S3CHINA": "s3china://",
                    "GCS": "gcs://",
                    "AZURE": "azure://",
                    "S3COMPAT": "s3compat://",
                }
                scheme = scheme_map.get(cloud_provider, "s3://")
                raw_url = f"{scheme}snowflake-langflow-bucket/{safe_name.lower()}/"
            cloud_url = raw_url
            safe_url = cloud_url.replace("\\", "\\\\").replace("'", "''")
            clauses.append(f"URL = '{safe_url}'")

            auth_mode = str(payload.get("auth_mode") or "STORAGE_INTEGRATION").strip().upper()
            storage_integration = str(payload.get("storage_integration") or "").strip() or None
            if auth_mode == "CREDENTIALS" or (not storage_integration and payload.get("credentials")):
                creds = payload.get("credentials") if isinstance(payload.get("credentials"), dict) else {}
                aws_key = str(creds.get("aws_key_id") or payload.get("aws_key_id") or "").replace("\\", "\\\\").replace("'", "''")
                aws_secret = str(creds.get("aws_secret_key") or payload.get("aws_secret_key") or "").replace("\\", "\\\\").replace("'", "''")
                aws_token = str(creds.get("aws_token") or payload.get("aws_token") or "").replace("\\", "\\\\").replace("'", "''")
                aws_role = str(creds.get("aws_role") or payload.get("aws_role") or "").replace("\\", "\\\\").replace("'", "''")
                azure_sas = str(creds.get("azure_sas_token") or payload.get("azure_sas_token") or "").replace("\\", "\\\\").replace("'", "''")
                cred_parts: list[str] = []
                if azure_sas or cloud_provider == "AZURE":
                    cred_parts.append(f"AZURE_SAS_TOKEN = '{azure_sas or '<AZURE_SAS_TOKEN>'}'")
                elif aws_role:
                    cred_parts.append(f"AWS_ROLE = '{aws_role}'")
                else:
                    cred_parts.append(f"AWS_KEY_ID = '{aws_key or '<AWS_KEY_ID>'}'")
                    cred_parts.append(f"AWS_SECRET_KEY = '{aws_secret or '<AWS_SECRET_KEY>'}'")
                    if aws_token:
                        cred_parts.append(f"AWS_TOKEN = '{aws_token}'")
                clauses.append(f"CREDENTIALS = ({' '.join(cred_parts)})")
            else:
                storage_integration = storage_integration or "AWS_S3_INTEGRATION"
                clauses.append(f"STORAGE_INTEGRATION = {_sql_safety.validate_identifier(storage_integration, 'storage_integration')}")

            if cloud_provider == "S3COMPAT" or payload.get("endpoint"):
                ep = str(payload.get("endpoint") or "s3.compatible.endpoint.local").replace("\\", "\\\\").replace("'", "''")
                clauses.append(f"ENDPOINT = '{ep}'")

            raw_enc = str(payload.get("encryption") or payload.get("encryption_type") or "").strip().upper()
            encryption_type = raw_enc or "NONE"
            if raw_enc and raw_enc != "NONE":
                kms_id = str(payload.get("kms_key_id") or "").replace("\\", "\\\\").replace("'", "''")
                if kms_id:
                    clauses.append(f"ENCRYPTION = (TYPE = '{raw_enc}' KMS_KEY_ID = '{kms_id}')")
                else:
                    clauses.append(f"ENCRYPTION = (TYPE = '{raw_enc}')")

            if payload.get("use_privatelink_endpoint") is True:
                clauses.append("USE_PRIVATELINK_ENDPOINT = TRUE")

            if directory_enabled:
                dir_parts = ["ENABLE = TRUE"]
                if auto_refresh:
                    dir_parts.append("AUTO_REFRESH = TRUE")
                if notification_integration:
                    dir_parts.append(f"NOTIFICATION_INTEGRATION = '{_sql_safety.validate_identifier(notification_integration, 'notification_integration')}'")
                clauses.append(f"DIRECTORY = ({' '.join(dir_parts)})")

        if file_format and file_format != "NONE":
            if file_format in ("CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"):
                clauses.append(f"FILE_FORMAT = (TYPE = '{file_format}')")
            else:
                clauses.append(f"FILE_FORMAT = (FORMAT_NAME = '{_sql_safety.validate_fqn(file_format, 'file_format')}')")

        if safe_comment:
            clauses.append(f"COMMENT = '{safe_comment}'")

        ddl = " ".join(clauses) + ";"
        dry_run = bool(payload.get("dry_run", False))
        exec_res = {"ok": True, "mode": "dry_run"} if dry_run else self.execute_sql(ddl)

        stage_entry = {
            "name": safe_name,
            "database": db,
            "schema": sch,
            "fqn": fqn,
            "stage_fqn": f"@{fqn}",
            "at_stage": f"@{fqn}",
            "type": stage_type,
            "stage_type": stage_type,
            "kind": f"{stage_type}_STAGE",
            "encryption": encryption_type,
            "directory_enabled": directory_enabled,
            "auto_refresh": auto_refresh,
            "url": cloud_url,
            "cloud_provider": cloud_provider,
            "storage_integration": storage_integration,
            "file_format": file_format or None,
            "comment": comment,
            "ddl": ddl,
            "created_by_role": self.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        existing_idx = next(
            (i for i, s in enumerate(self.stages_registry) if str(s.get("fqn") or "").upper() == fqn.upper()),
            None,
        )
        if existing_idx is not None:
            self.stages_registry[existing_idx] = stage_entry
        else:
            self.stages_registry.append(stage_entry)
        self.stage_files_registry.setdefault(fqn, [])

        reg_ctx = self.register_object_in_context(db, sch, "stages", safe_name, flow_id=eff_fid)
        self.record_audit_event(
            action="STAGE_CREATE",
            outcome="ALLOWED",
            target_object=f"@{fqn}",
            flow_id=eff_fid,
            details={"ddl": ddl, "stage": stage_entry},
        )
        return {
            "ok": True,
            "status_code": 201,
            "stage": stage_entry,
            "ddl": ddl,
            "result": exec_res,
            "registered_in_context": True,
            "selected_context": reg_ctx,
            "active_role": self.active_role,
        }

    def create_external_volume_governed(
        self,
        body: dict[str, Any],
        *,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Build & execute governed `CREATE EXTERNAL VOLUME` after checking active role privilege (`W2-R5-05`)."""
        payload = dict(body or {})
        eff_fid = self._normalize_flow_id(
            flow_id or payload.get("flow_id") or CURRENT_SNOWFLAKE_FLOW_ID.get() or self.active_flow_id
        )
        raw_name = str(payload.get("name") or payload.get("volume_name") or "ICEBERG_EXT_VOL").strip()
        safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in raw_name).upper()
        try:
            safe_name = _sql_safety.validate_identifier(safe_name, "volume_name")
            _sql_safety.validate_enum(
                str(payload.get("storage_provider") or payload.get("cloud_provider") or "S3").strip().upper(),
                _VOLUME_STORAGE_PROVIDERS,
                "storage_provider",
            )
            _enc = str(payload.get("encryption") or payload.get("encryption_type") or "").strip().upper()
            if _enc:
                _sql_safety.validate_enum(_enc, _EXTERNAL_ENCRYPTION_TYPES, "encryption")
        except _sql_safety.SqlSafetyError as exc:
            return {"ok": False, "status_code": 400, "error_code": "INVALID_IDENTIFIER", "message": str(exc)}

        if not self.can_create_external_volume():
            msg = f"Active role {self.active_role} does not have CREATE EXTERNAL VOLUME privilege"
            self.record_audit_event(
                action="EXTERNAL_VOLUME_CREATE",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=safe_name,
                flow_id=eff_fid,
                details={"message": msg, "required_privilege": "CREATE EXTERNAL VOLUME ON ACCOUNT"},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": msg,
                "banner": msg,
                "active_role": self.active_role,
            }

        denied = self.check_write_privilege_or_deny("EXTERNAL_VOLUME_CREATE", safe_name)
        if denied:
            return denied

        create_mode = str(payload.get("create_mode") or "CREATE IF NOT EXISTS").strip().upper()
        if payload.get("or_replace") is True or "REPLACE" in create_mode:
            create_prefix = f"CREATE OR REPLACE EXTERNAL VOLUME {safe_name}"
        elif payload.get("if_not_exists", True) or "IF NOT EXISTS" in create_mode:
            create_prefix = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {safe_name}"
        else:
            create_prefix = f"CREATE EXTERNAL VOLUME {safe_name}"

        allow_writes = bool(payload.get("allow_writes", True))
        loc_name = str(payload.get("location_name") or payload.get("storage_location_name") or f"{safe_name.lower()}_loc").replace("\\", "\\\\").replace("'", "''")
        provider = str(payload.get("storage_provider") or payload.get("cloud_provider") or "S3").strip().upper()
        base_url = str(
            payload.get("storage_base_url")
            or payload.get("url")
            or f"s3://snowflake-langflow-iceberg/{safe_name.lower()}/"
        ).replace("\\", "\\\\").replace("'", "''")

        loc_parts = [
            f"NAME = '{loc_name}'",
            f"STORAGE_PROVIDER = '{provider}'",
            f"STORAGE_BASE_URL = '{base_url}'",
        ]
        if provider in ("S3", "S3GOV"):
            role_arn = str(payload.get("storage_aws_role_arn") or "arn:aws:iam::123456789012:role/snowflake-iceberg-role").replace("\\", "\\\\").replace("'", "''")
            loc_parts.append(f"STORAGE_AWS_ROLE_ARN = '{role_arn}'")
            if payload.get("storage_aws_external_id"):
                ext_id = str(payload.get("storage_aws_external_id")).replace("\\", "\\\\").replace("'", "''")
                loc_parts.append(f"STORAGE_AWS_EXTERNAL_ID = '{ext_id}'")
            if payload.get("storage_aws_access_point_arn"):
                ap_arn = str(payload.get("storage_aws_access_point_arn")).replace("\\", "\\\\").replace("'", "''")
                loc_parts.append(f"STORAGE_AWS_ACCESS_POINT_ARN = '{ap_arn}'")
        elif provider == "AZURE":
            tenant_id = str(payload.get("azure_tenant_id") or "00000000-0000-0000-0000-000000000000").replace("\\", "\\\\").replace("'", "''")
            loc_parts.append(f"AZURE_TENANT_ID = '{tenant_id}'")
        elif provider == "S3COMPAT":
            endpoint = str(payload.get("storage_endpoint") or payload.get("endpoint") or "s3.compatible.endpoint.local").replace("\\", "\\\\").replace("'", "''")
            loc_parts.append(f"STORAGE_ENDPOINT = '{endpoint}'")

        raw_enc = str(payload.get("encryption") or payload.get("encryption_type") or "").strip().upper()
        if raw_enc and raw_enc != "NONE":
            kms_id = str(payload.get("kms_key_id") or "").replace("\\", "\\\\").replace("'", "''")
            if kms_id:
                loc_parts.append(f"ENCRYPTION = (TYPE = '{raw_enc}' KMS_KEY_ID = '{kms_id}')")
            else:
                loc_parts.append(f"ENCRYPTION = (TYPE = '{raw_enc}')")

        if payload.get("use_privatelink_endpoint") is True:
            loc_parts.append("USE_PRIVATELINK_ENDPOINT = TRUE")

        comment = str(payload.get("comment") or "").strip()
        comment_sql = f" COMMENT = {_sql_safety.sql_string_literal(comment)}" if comment else ""
        ddl = (
            f"{create_prefix} STORAGE_LOCATIONS = (({' '.join(loc_parts)})) "
            f"ALLOW_WRITES = {'TRUE' if allow_writes else 'FALSE'}{comment_sql};"
        )
        dry_run = bool(payload.get("dry_run", False))
        exec_res = {"ok": True, "mode": "dry_run"} if dry_run else self.execute_sql(ddl)

        vol_entry = {
            "name": safe_name,
            "type": "EXTERNAL_VOLUME",
            "kind": "EXTERNAL_VOLUME",
            "allow_writes": allow_writes,
            "location_name": loc_name,
            "storage_provider": provider,
            "storage_base_url": base_url,
            "storage_locations": [
                {
                    "name": loc_name,
                    "storage_provider": provider,
                    "storage_base_url": base_url,
                }
            ],
            "comment": comment,
            "ddl": ddl,
            "created_by_role": self.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        existing_idx = next(
            (i for i, v in enumerate(self.external_volumes_registry) if str(v.get("name") or "").upper() == safe_name),
            None,
        )
        if existing_idx is not None:
            self.external_volumes_registry[existing_idx] = vol_entry
        else:
            self.external_volumes_registry.append(vol_entry)

        target_ctx = self.get_flow_context(eff_fid) if eff_fid else self.selected_context
        vis_vols = target_ctx.setdefault("visible_objects", {}).setdefault("external_volumes", [])
        if safe_name not in vis_vols:
            vis_vols.append(safe_name)

        self.record_audit_event(
            action="EXTERNAL_VOLUME_CREATE",
            outcome="ALLOWED",
            target_object=safe_name,
            flow_id=eff_fid,
            details={"ddl": ddl, "external_volume": vol_entry},
        )
        return {
            "ok": True,
            "status_code": 201,
            "external_volume": vol_entry,
            "ddl": ddl,
            "result": exec_res,
            "registered_in_context": True,
            "active_role": self.active_role,
        }

    _STAGE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
    _STAGE_PATH_RE = re.compile(r"^[A-Za-z0-9_\-./ ]{1,1024}$")

    @classmethod
    def _validate_stage_identifier(cls, segment: str) -> str:
        """Return an upper-cased unquoted identifier or raise ValueError (no quoting tricks accepted)."""
        seg = str(segment or "").strip()
        if not cls._STAGE_IDENT_RE.match(seg):
            raise ValueError(f"Invalid Snowflake identifier: {seg[:64]!r}")
        return seg.upper()

    @classmethod
    def _validate_stage_relpath(cls, rel_path: str) -> str:
        """Validate a stage-relative path: no traversal, quotes, semicolons or control characters."""
        clean = str(rel_path or "").strip().strip("/")
        if not clean:
            return ""
        if not cls._STAGE_PATH_RE.match(clean) or any(p in ("..", ".") for p in clean.split("/")):
            raise ValueError(f"Invalid stage path: {clean[:64]!r}")
        return clean

    def _normalize_stage_fqn_no_at(self, stage_fqn: str) -> str:
        """Return a validated `DB.SCHEMA.STAGE` (raises ValueError on empty or unsafe input)."""
        clean = str(stage_fqn or "").strip().lstrip("@").strip("/")
        if not clean:
            raise ValueError("stage_fqn is required")
        parts = clean.split(".")
        if len(parts) == 1:
            parts = [self.database, self.schema, parts[0]]
        elif len(parts) == 2:
            parts = [self.database, parts[0], parts[1]]
        elif len(parts) != 3:
            raise ValueError(f"Invalid stage name: {clean[:64]!r}")
        return ".".join(self._validate_stage_identifier(p) for p in parts)

    def _stage_input_error(self, exc: Exception) -> dict[str, Any]:
        return {
            "ok": False,
            "status_code": 400,
            "error_code": "INVALID_STAGE_INPUT",
            "message": str(exc),
            "active_role": self.active_role,
        }

    def _entry_stage_type(self, fqn: str, entry: dict[str, Any]) -> str:
        """INTERNAL / EXTERNAL / UNKNOWN from the entry itself or cached SHOW STAGES metadata (no extra SQL)."""
        raw = " ".join(
            str(entry.get(k) or "") for k in ("type", "stage_type", "kind", "TYPE", "storage_type")
        ).upper()
        if not raw.strip():
            raw = str((self._stage_meta.get(fqn) or {}).get("type") or "").upper()
        if "EXTERNAL" in raw:
            return "EXTERNAL"
        return "INTERNAL" if "INTERNAL" in raw else "UNKNOWN"

    def _stage_type(self, clean_fqn: str) -> str:
        """Return INTERNAL / EXTERNAL / UNKNOWN using cached SHOW STAGES rows, else a live SHOW STAGES LIKE."""
        meta = self._stage_meta.get(clean_fqn)
        if not meta:
            db, sch, nm = clean_fqn.split(".")
            res = self.execute_sql(f"SHOW STAGES LIKE '{nm}' IN SCHEMA {db}.{sch}")
            for r in res.get("rows") or [] if res.get("ok") else []:
                if isinstance(r, dict) and str(r.get("name") or r.get("NAME") or "").upper() == nm:
                    meta = {"type": str(r.get("type") or r.get("TYPE") or "").upper()}
                    self._stage_meta[clean_fqn] = {**meta, "url": str(r.get("url") or r.get("URL") or "")}
                    break
        st_type = str((meta or {}).get("type") or "")
        if "EXTERNAL" in st_type:
            return "EXTERNAL"
        return "INTERNAL" if st_type else "UNKNOWN"

    def list_stage_files(
        self,
        stage_fqn: str,
        *,
        subpath: str = "",
        pattern: str = "",
        media_filter: str | None = None,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute `LIST @<stage_fqn>` and return files with `name`, `size`, `md5`, `last_modified`, `media_type`, and `to_file_sql` (`W2-R5-05`, `W2-R9-04`)."""
        try:
            clean_fqn = self._normalize_stage_fqn_no_at(stage_fqn)
            clean_subpath = self._validate_stage_relpath(subpath)
        except ValueError as exc:
            return {**self._stage_input_error(exc), "files": [], "total": 0}
        target_at = f"@{clean_fqn}/{clean_subpath}" if clean_subpath else f"@{clean_fqn}"
        pattern_clause = f" PATTERN = {_sql_safety.sql_string_literal(str(pattern))}" if pattern else ""
        list_sql = f"LIST '{target_at}'{pattern_clause};"
        res = self.execute_sql(list_sql)
        if not res.get("ok", False):
            return {
                "ok": False,
                "status_code": int(res.get("status_code") or 502),
                "error_code": "LIST_STAGE_FAILED",
                "message": str(res.get("message") or res.get("error") or "LIST failed"),
                "stage_fqn": f"@{clean_fqn}",
                "sql": list_sql,
                "files": [],
                "total": 0,
                "active_role": self.active_role,
            }

        def _classify_media(rel_p: str) -> str:
            ext = ("." + rel_p.rsplit(".", 1)[-1].lower()) if "." in rel_p else ""
            if ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"):
                return "image"
            if ext in (".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac", ".aiff", ".pcm"):
                return "audio"
            if ext in (".mp4", ".mov", ".webm", ".mkv", ".avi", ".mpeg", ".flv", ".mpg", ".wmv", ".3gpp", ".ogv"):
                return "video"
            if ext in (".pdf", ".docx", ".pptx", ".txt", ".md", ".html", ".csv", ".json", ".tiff", ".tif"):
                return "document"
            return "other"

        files_by_path: dict[str, dict[str, Any]] = {}
        for r in res.get("rows") or []:
            if not isinstance(r, dict):
                continue
            raw_nm = str(r.get("name") or r.get("NAME") or "").strip()
            if not raw_nm:
                continue
            rel_path = raw_nm.split("/", 1)[1] if "/" in raw_nm else raw_nm
            files_by_path[rel_path] = {
                "name": raw_nm,
                "path": rel_path,
                "file_name": rel_path.split("/")[-1],
                "size": int(r.get("size") or r.get("SIZE") or 0),
                "md5": str(r.get("md5") or r.get("MD5") or ""),
                "last_modified": str(r.get("last_modified") or r.get("LAST_MODIFIED") or ""),
                "stage_fqn": f"@{clean_fqn}",
                "scoped_path": f"@{clean_fqn}/{rel_path}",
                "media_type": _classify_media(rel_path),
                "to_file_sql": f"TO_FILE('@{clean_fqn}', '{rel_path}')",
            }

        files_list = list(files_by_path.values())
        mf = str(media_filter or "").strip().lower()
        if mf and mf not in ("all", "*"):
            allowed_kinds: set[str] = set()
            if mf in ("image", "images"):
                allowed_kinds.add("image")
            elif mf in ("audio", "mp3"):
                allowed_kinds.update({"audio", "video"})
            elif mf in ("video", "videos"):
                allowed_kinds.add("video")
            elif mf in ("document", "documents", "doc", "docs"):
                allowed_kinds.update({"document", "image"})
            elif mf == "multimodal":
                allowed_kinds.update({"image", "audio", "video", "document"})
            if allowed_kinds:
                files_list = [f for f in files_list if str(f.get("media_type") or "") in allowed_kinds]

        return {
            "ok": True,
            "stage_fqn": f"@{clean_fqn}",
            "sql": list_sql,
            "directory_sql": f"SELECT * FROM DIRECTORY(@{clean_fqn});",
            "media_filter": mf or "all",
            "files": files_list,
            "total": len(files_list),
            "active_role": self.active_role,
        }

    def list_custom_tool_identifiers(
        self,
        resource_type: str = "procedure",
        database_schema: str = "Database All",
        flow_id: str | None = None,
    ) -> list[str]:
        """List Snowflake Custom Tool identifiers (`procedure`, `function`, `table_function`) with full argument signatures (`W2-R9-01`)."""
        res_type = str(resource_type or "procedure").strip().lower()
        db_sch = str(database_schema or "Database All").strip()
        target_pairs: list[tuple[str, str]] = []
        if db_sch and db_sch not in ("Database All", "All", "*") and "." in db_sch:
            d_part, s_part = db_sch.split(".", 1)
            target_pairs.append((d_part.strip().upper(), s_part.strip().upper()))
        else:
            for d_name in self.get_context_databases(flow_id=flow_id):
                for s_name in self.get_context_schemas(d_name, flow_id=flow_id):
                    pair = (str(d_name).strip().upper(), str(s_name).strip().upper())
                    if pair not in target_pairs:
                        target_pairs.append(pair)
        if not target_pairs:
            target_pairs.append((self.database.upper(), self.schema.upper()))

        identifiers: list[str] = []
        category = "procedures" if res_type == "procedure" else "functions"
        for db_n, sch_n in target_pairs:
            # Context values are client-persisted: never interpolate a non-identifier (BUG-UI-SQLI-05).
            if not (_sql_safety.is_valid_identifier(db_n) and _sql_safety.is_valid_identifier(sch_n)):
                continue
            if self._has_spcs_token() or self._is_execute_sql_mocked():
                show_kind = "PROCEDURES" if res_type == "procedure" else "FUNCTIONS"
                show_res = self.execute_sql(f"SHOW USER {show_kind} IN SCHEMA {db_n}.{sch_n};")
                for row in show_res.get("rows") or []:
                    if not isinstance(row, dict):
                        continue
                    is_builtin = str(row.get("is_builtin") or row.get("IS_BUILTIN") or "N").strip().upper()
                    if is_builtin == "Y":
                        continue
                    if res_type in ("function", "table_function"):
                        is_tbl = str(row.get("is_table_function") or row.get("IS_TABLE_FUNCTION") or "N").strip().upper()
                        if res_type == "table_function" and is_tbl != "Y":
                            continue
                        if res_type == "function" and is_tbl == "Y":
                            continue
                    raw_args = str(row.get("arguments") or row.get("ARGUMENTS") or row.get("name") or row.get("NAME") or "").strip()
                    if not raw_args:
                        continue
                    sig_only = raw_args.split(" RETURN ")[0].strip()
                    fqn_sig = sig_only if "." in sig_only.split("(")[0] else f"{db_n}.{sch_n}.{sig_only}"
                    if "(" not in fqn_sig:
                        fqn_sig = f"{fqn_sig}(VARCHAR)"
                    if fqn_sig not in identifiers:
                        identifiers.append(fqn_sig)

            for ctx_obj in self.get_context_objects(db_n, sch_n, category, flow_id=flow_id):
                obj_s = str(ctx_obj or "").strip()
                if not obj_s:
                    continue
                if res_type == "table_function" and "UDTF" not in obj_s.upper() and "TABLE" not in obj_s.upper():
                    continue
                if res_type == "function" and "UDTF" in obj_s.upper():
                    continue
                sig_only = obj_s.split(" RETURN ")[0].strip()
                fqn_sig = sig_only if "." in sig_only.split("(")[0] else f"{db_n}.{sch_n}.{sig_only}"
                if "(" not in fqn_sig:
                    continue  # BUG-LLMOP-005: never guess a signature
                if fqn_sig not in identifiers:
                    identifiers.append(fqn_sig)

        return identifiers

    def describe_custom_tool_signature(
        self,
        resource_type: str = "procedure",
        identifier: str = "",
        database_schema: str | None = None,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Inspect a Snowflake procedure/function signature via `DESCRIBE PROCEDURE/FUNCTION` and map parameters (`W2-R9-01`)."""
        from lfx_snowflake.components.llm_operations import SnowflakeCustomToolEngine

        res_type = str(resource_type or "procedure").strip().lower()
        ident = str(identifier or "").strip()
        if not ident and res_type not in ("procedure", "function", "table_function"):
            ident = str(resource_type).strip()
            res_type = "procedure"
        if not ident:
            return {
                "ok": False,
                "status_code": 400,
                "error_code": "MISSING_IDENTIFIER",
                "message": "Select a procedure or function to describe.",
                "resource_type": res_type,
                "parameters": [],
            }

        default_db_sch = (
            database_schema
            if database_schema and database_schema != "Database All" and "." in str(database_schema)
            else f"{self.database}.{self.schema}"
        )
        try:
            bare_fqn = SnowflakeCustomToolEngine.extract_bare_identifier(ident, default_db_schema=default_db_sch)
            arg_types_part = SnowflakeCustomToolEngine.extract_signature_types(ident)
        except ValueError as exc:
            return {
                "ok": False,
                "status_code": 400,
                "error_code": "INVALID_IDENTIFIER",
                "message": str(exc),
                "resource_type": res_type,
                "parameters": [],
            }
        desc_kw = "PROCEDURE" if res_type == "procedure" else "FUNCTION"
        describe_sql = f"DESCRIBE {desc_kw} {bare_fqn}{arg_types_part};"
        desc_rows: list[dict[str, Any]] = []
        if self._has_spcs_token() or self._is_execute_sql_mocked():
            desc_res = self.execute_sql(describe_sql)
            if isinstance(desc_res.get("rows"), list):
                desc_rows = [r for r in desc_res["rows"] if isinstance(r, dict)]

        suggested_name = SnowflakeCustomToolEngine.derive_tool_name(ident)
        try:
            params = SnowflakeCustomToolEngine.parse_signature_parameters(ident, describe_rows=desc_rows)
            payload = SnowflakeCustomToolEngine.build_cortex_agent_tool_payload(
                resource_type=res_type,
                custom_tool_identifier=ident,
                tool_name=suggested_name,
                parameters=params,
                default_db_schema=default_db_sch,
            )
            return {
                "ok": True,
                "status_code": 200,
                "resource_type": res_type,
                "identifier": bare_fqn,
                "full_signature": f"{bare_fqn}{arg_types_part}",
                "describe_sql": describe_sql,
                "suggested_tool_name": suggested_name,
                "parameters": params,
                "input_schema": payload["tool_spec"]["input_schema"],
                "tool_spec": payload["tool_spec"],
                "tool_resources": payload["tool_resources"],
                "active_role": self.active_role,
            }
        except Exception as exc:  # noqa: BLE001
            err_code = str(getattr(exc, "code", None) or getattr(exc, "error_code", None) or "ERROR_UNSUPPORTED_CAPABILITY")
            err_msg = str(getattr(exc, "message", None) or str(exc))
            return {
                "ok": False,
                "status_code": 422,
                "error_code": err_code,
                "unsupported_object_parameter": True,
                "message": f"{err_code}: {err_msg}" if not err_msg.startswith(err_code) else err_msg,
                "resource_type": res_type,
                "identifier": bare_fqn,
                "full_signature": f"{bare_fqn}{arg_types_part}",
                "describe_sql": describe_sql,
                "suggested_tool_name": suggested_name,
                "parameters": [],
                "active_role": self.active_role,
            }

    UPLOAD_DISABLED_MESSAGE = (
        "Upload is available after the identity upgrade. PUT is not supported over the Snowflake SQL API, "
        "so no file was uploaded."
    )

    def upload_stage_file(
        self,
        *,
        stage_fqn: str,
        file_name: str,
        content_bytes: bytes | None = None,
        content_text: str | None = None,
        content_base64: str | None = None,
        subpath: str = "",
        auto_compress: bool = False,
        overwrite: bool = True,
        refresh_directory: bool = True,
    ) -> dict[str, Any]:
        """Stage upload (W3-R1-04, Phase 0): fail closed, never report success for a PUT that did not run.

        The bridge only talks to the Snowflake SQL API, which does not execute `PUT`, so the upload is
        rejected with an explicit message. External stages are rejected because `PUT` only targets
        internal stages. No bytes are written to local disk.
        """
        try:
            clean_fqn = self._normalize_stage_fqn_no_at(stage_fqn)
            clean_subpath = self._validate_stage_relpath(subpath)
            clean_file_name = self._validate_stage_relpath(Path(str(file_name or "")).name)
            if not clean_file_name:
                raise ValueError("file_name is required")
        except ValueError as exc:
            return self._stage_input_error(exc)
        rel_path = f"{clean_subpath}/{clean_file_name}" if clean_subpath else clean_file_name
        denied = self.check_write_privilege_or_deny("STAGE_UPLOAD_FILE", f"@{clean_fqn}")
        if denied:
            return denied
        base = {
            "ok": False,
            "stage_fqn": f"@{clean_fqn}",
            "scoped_path": f"@{clean_fqn}/{rel_path}",
            "uploaded": False,
            "active_role": self.active_role,
        }
        if self._stage_type(clean_fqn) == "EXTERNAL":
            rejected = {
                **base,
                "status_code": 400,
                "error_code": "EXTERNAL_STAGE_UPLOAD_UNSUPPORTED",
                "message": (
                    f"@{clean_fqn} is an EXTERNAL stage. PUT only works on internal stages; "
                    "upload files to the external storage location directly."
                ),
            }
        else:
            rejected = {
                **base,
                "status_code": 501,
                "error_code": "UPLOAD_NOT_AVAILABLE",
                "message": self.UPLOAD_DISABLED_MESSAGE,
            }
        # The rejection is the real outcome (no PUT ran); it is audited as FAILURE, never as SUCCESS.
        emit_audit(
            self.record_audit_event,
            AuditAction.STAGE_FILE_UPLOAD,
            rejected,
            target_object=f"@{clean_fqn}/{rel_path}",
            details={"stage": f"@{clean_fqn}", "relative_path": rel_path},
        )
        return rejected

    def delete_stage_file(self, stage_fqn: str, file_path: str) -> dict[str, Any]:
        """Remove a file from a Snowflake Stage (`REMOVE '@<stage>/<file_path>'`); fails closed on SQL errors."""
        try:
            clean_fqn = self._normalize_stage_fqn_no_at(stage_fqn)
            clean_rel = self._validate_stage_relpath(file_path)
            stg_short = clean_fqn.split(".")[-1].lower()
            if clean_rel.lower().startswith(f"{stg_short}/"):
                clean_rel = clean_rel.split("/", 1)[1]
            if not clean_rel:
                raise ValueError("file_path is required")
        except ValueError as exc:
            return self._stage_input_error(exc)
        denied = self.check_write_privilege_or_deny("STAGE_REMOVE_FILE", f"@{clean_fqn}")
        if denied:
            return denied
        remove_sql = f"REMOVE '@{clean_fqn}/{clean_rel}';"
        res = self.execute_sql(remove_sql)
        emit_audit(
            self.record_audit_event,
            AuditAction.STAGE_FILE_REMOVE,
            res,
            target_object=f"@{clean_fqn}/{clean_rel}",
            details={"stage": f"@{clean_fqn}", "relative_path": clean_rel},
        )
        ok = bool(res.get("ok", False))
        out: dict[str, Any] = {
            "ok": ok,
            "stage_fqn": f"@{clean_fqn}",
            "removed_path": f"@{clean_fqn}/{clean_rel}" if ok else None,
            "sql": remove_sql,
            "result": res,
            "active_role": self.active_role,
        }
        if not ok:
            out.update(
                status_code=int(res.get("status_code") or 502),
                error_code="REMOVE_STAGE_FILE_FAILED",
                message=str(res.get("message") or res.get("error") or "REMOVE failed"),
            )
        return out

    def refresh_stage_directory(self, stage_fqn: str) -> dict[str, Any]:
        """Refresh a Snowflake Stage Directory Table (`ALTER STAGE <stage_fqn> REFRESH`); fails closed."""
        try:
            clean_fqn = self._normalize_stage_fqn_no_at(stage_fqn)
        except ValueError as exc:
            return self._stage_input_error(exc)
        refresh_sql = f"ALTER STAGE {clean_fqn} REFRESH;"
        res = self.execute_sql(refresh_sql)
        emit_audit(
            self.record_audit_event,
            AuditAction.STAGE_DIRECTORY_REFRESH,
            res,
            target_object=f"@{clean_fqn}",
            details={"stage": f"@{clean_fqn}"},
        )
        ok = bool(res.get("ok", False))
        out: dict[str, Any] = {
            "ok": ok,
            "stage_fqn": f"@{clean_fqn}",
            "sql": refresh_sql,
            "result": res,
            "active_role": self.active_role,
        }
        if not ok:
            out.update(
                status_code=int(res.get("status_code") or 502),
                error_code="REFRESH_STAGE_FAILED",
                message=str(res.get("message") or res.get("error") or "ALTER STAGE REFRESH failed"),
            )
        return out

    def describe_external_volume(self, name: str) -> dict[str, Any]:
        """Read-only `DESCRIBE EXTERNAL VOLUME <name>` (D-12 b); returns the raw rows only."""
        try:
            safe_name = self._validate_stage_identifier(name)
        except ValueError as exc:
            return self._stage_input_error(exc)
        res = self.execute_sql(f"DESCRIBE EXTERNAL VOLUME {safe_name}")
        if not res.get("ok", False):
            return {
                "ok": False,
                "status_code": int(res.get("status_code") or 502),
                "error_code": "DESCRIBE_EXTERNAL_VOLUME_FAILED",
                "message": str(res.get("message") or res.get("error") or "DESCRIBE EXTERNAL VOLUME failed"),
                "name": safe_name,
                "properties": [],
            }
        return {"ok": True, "name": safe_name, "properties": list(res.get("rows") or []), "active_role": self.active_role}

    @property
    def role_privileges(self) -> dict[str, bool]:
        return self.get_role_privileges()

    @staticmethod
    def _extract_context_object_set(ctx: dict[str, Any]) -> tuple[set[str], set[str], set[str]]:
        """Extract normalized `(databases_set, schemas_set, objects_set)` from a context dictionary."""
        dbs_set: set[str] = {
            str(d).strip().upper()
            for d in (ctx.get("databases") or [])
            if str(d).strip()
        }
        schemas_set: set[str] = set()
        schemas_by_db = ctx.get("schemas_by_db") or {}
        if isinstance(schemas_by_db, dict):
            for db_k, sch_list in schemas_by_db.items():
                db_u = str(db_k).strip().upper()
                if isinstance(sch_list, list):
                    for s in sch_list:
                        s_u = str(s).strip().upper()
                        if db_u and s_u:
                            schemas_set.add(f"{db_u}.{s_u}")

        objs_set: set[str] = set()
        for map_key in ("objects_by_db_schema", "selected_objects"):
            pair_map = ctx.get(map_key)
            if isinstance(pair_map, dict):
                for pair_k, pair_v in pair_map.items():
                    clean_pair = str(pair_k).strip().upper()
                    if isinstance(pair_v, dict):
                        for cat_k, cat_v in pair_v.items():
                            cat_l = str(cat_k).strip().lower()
                            if isinstance(cat_v, list):
                                for item in cat_v:
                                    item_u = str(item).strip().upper()
                                    if item_u:
                                        objs_set.add(f"{clean_pair}.{cat_l}:{item_u}")
                    elif isinstance(pair_v, list):
                        for item in pair_v:
                            item_u = str(item).strip().upper()
                            if item_u:
                                objs_set.add(f"{clean_pair}:{item_u}")
            elif isinstance(pair_map, list):
                for item in pair_map:
                    item_u = str(item).strip().upper()
                    if item_u:
                        objs_set.add(item_u)

        vis_map = ctx.get("visible_objects")
        if isinstance(vis_map, dict):
            for cat_k, cat_v in vis_map.items():
                cat_l = str(cat_k).strip().lower()
                if isinstance(cat_v, list):
                    for item in cat_v:
                        item_u = str(item).strip().upper()
                        if item_u:
                            objs_set.add(f"visible.{cat_l}:{item_u}")
        for pg_inst in ctx.get("postgres_instances") or []:
            pg_u = str(pg_inst).strip().upper()
            if pg_u:
                objs_set.add(f"postgres_instances:{pg_u}")
        return dbs_set, schemas_set, objs_set

    def remove_object_from_context(
        self,
        *,
        object_name: str,
        category: str = "tables",
        database: str | None = None,
        schema: str | None = None,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Explicitly remove an object from a workflow's Snowflake context and emit `OBJECT_REMOVED_FROM_CONTEXT`."""
        norm_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        target_ctx = copy.deepcopy(self.get_flow_context(norm_fid) if norm_fid else self.selected_context)
        db = (database or target_ctx.get("database") or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or target_ctx.get("schema") or self.schema or "CORE").strip().upper()
        cat = (category or "tables").strip().lower()
        obj_u = str(object_name or "").strip().upper()
        pair_key = f"{db}.{sch}"

        if cat == "postgres_instances":
            for pg_k in ("postgres_instances", "selected_postgres_instances"):
                if isinstance(target_ctx.get(pg_k), list):
                    target_ctx[pg_k] = [x for x in target_ctx[pg_k] if str(x).strip().upper() != obj_u]
        for map_key in ("objects_by_db_schema", "selected_objects"):
            m = target_ctx.get(map_key)
            if isinstance(m, dict) and pair_key in m and isinstance(m[pair_key], dict):
                if cat in m[pair_key] and isinstance(m[pair_key][cat], list):
                    m[pair_key][cat] = [
                        x for x in m[pair_key][cat] if str(x).strip().upper() != obj_u
                    ]
        vis = target_ctx.get("visible_objects")
        if isinstance(vis, dict) and cat in vis and isinstance(vis[cat], list):
            vis[cat] = [x for x in vis[cat] if str(x).strip().upper() != obj_u]

        return self.update_selected_context(target_ctx, flow_id=norm_fid)

    def update_selected_context(
        self,
        payload: dict[str, Any],
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist cascading primary context selection from `[ ❄️ Context ]` (`CTX-ARCH-03`, `CTX-ARCH-04`, `CTX-ARCH-05`)."""
        incoming_ctx = payload.get("selected_context") if isinstance(payload.get("selected_context"), dict) else {}
        effective_flow_id = self._normalize_flow_id(
            flow_id
            or payload.get("flow_id")
            or incoming_ctx.get("flow_id")
            or CURRENT_SNOWFLAKE_FLOW_ID.get()
        )

        # Snapshot previous context before applying update to detect removed databases/schemas/objects
        if effective_flow_id:
            base_ctx = copy.deepcopy(self.get_flow_context(effective_flow_id))
        else:
            base_ctx = copy.deepcopy(self.selected_context)

        prev_dbs, prev_schemas, prev_objs = self._extract_context_object_set(base_ctx)

        has_full_dbs = "full_databases" in payload or "full_databases" in incoming_ctx
        raw_full_dbs = payload.get("full_databases") if "full_databases" in payload else incoming_ctx.get("full_databases")
        if has_full_dbs and isinstance(raw_full_dbs, list):
            base_ctx["full_databases"] = [str(d).strip().upper() for d in raw_full_dbs if str(d).strip()]
        else:
            base_ctx.setdefault("full_databases", [])

        has_full_schemas = "full_schemas" in payload or "full_schemas" in incoming_ctx
        raw_full_schemas = payload.get("full_schemas") if "full_schemas" in payload else incoming_ctx.get("full_schemas")
        if has_full_schemas and isinstance(raw_full_schemas, list):
            base_ctx["full_schemas"] = [str(s).strip().upper() for s in raw_full_schemas if str(s).strip()]
        else:
            base_ctx.setdefault("full_schemas", [])

        new_db = str(payload.get("database") or incoming_ctx.get("database") or "").strip().upper()
        new_sch = str(payload.get("schema") or incoming_ctx.get("schema") or "").strip().upper()

        has_dbs_key = "databases" in payload or "databases" in incoming_ctx
        dbs = payload.get("databases") if "databases" in payload else incoming_ctx.get("databases")
        if has_dbs_key and isinstance(dbs, list):
            merged_dbs = [str(d).strip().upper() for d in dbs if str(d).strip()]
            for fd in base_ctx.get("full_databases") or []:
                if fd not in merged_dbs:
                    merged_dbs.append(fd)
            for fs in base_ctx.get("full_schemas") or []:
                fs_db = fs.split(".", 1)[0]
                if fs_db and fs_db not in merged_dbs:
                    merged_dbs.append(fs_db)
            base_ctx["databases"] = merged_dbs
        elif base_ctx.get("full_databases") or base_ctx.get("full_schemas"):
            merged_dbs = list(base_ctx.get("databases") or [])
            for fd in base_ctx.get("full_databases") or []:
                if fd not in merged_dbs:
                    merged_dbs.append(fd)
            for fs in base_ctx.get("full_schemas") or []:
                fs_db = fs.split(".", 1)[0]
                if fs_db and fs_db not in merged_dbs:
                    merged_dbs.append(fs_db)
            base_ctx["databases"] = merged_dbs

        if new_db:
            base_ctx["database"] = new_db
        elif base_ctx.get("databases"):
            base_ctx["database"] = base_ctx["databases"][0]
        else:
            base_ctx.setdefault("database", self.database)

        active_ctx_db = str(base_ctx.get("database") or self.database).strip().upper()
        if active_ctx_db and active_ctx_db not in base_ctx.get("databases", []):
            base_ctx.setdefault("databases", []).insert(0, active_ctx_db)

        has_schemas_key = "schemas_by_db" in payload or "schemas_by_db" in incoming_ctx
        schemas_by_db = payload.get("schemas_by_db") if "schemas_by_db" in payload else incoming_ctx.get("schemas_by_db")
        if has_schemas_key and isinstance(schemas_by_db, dict):
            normalized_map: dict[str, list[str]] = {}
            for k, v in schemas_by_db.items():
                db_k = str(k).strip().upper()
                if isinstance(v, list):
                    normalized_map[db_k] = [str(s).strip().upper() for s in v if str(s).strip()]
            for fs in base_ctx.get("full_schemas") or []:
                parts = fs.split(".", 1)
                if len(parts) == 2 and parts[0] and parts[1]:
                    sch_l = normalized_map.setdefault(parts[0], [])
                    if parts[1] not in sch_l:
                        sch_l.append(parts[1])
            base_ctx["schemas_by_db"] = normalized_map
        else:
            cur_map = base_ctx.setdefault("schemas_by_db", {})
            for fs in base_ctx.get("full_schemas") or []:
                parts = fs.split(".", 1)
                if len(parts) == 2 and parts[0] and parts[1]:
                    sch_l = cur_map.setdefault(parts[0], [])
                    if parts[1] not in sch_l:
                        sch_l.append(parts[1])

        if new_sch:
            base_ctx["schema"] = new_sch
        elif active_ctx_db in (base_ctx.get("schemas_by_db") or {}) and base_ctx["schemas_by_db"][active_ctx_db]:
            base_ctx["schema"] = base_ctx["schemas_by_db"][active_ctx_db][0]
        else:
            base_ctx.setdefault("schema", self.schema)

        active_ctx_sch = str(base_ctx.get("schema") or self.schema).strip().upper()
        if not has_schemas_key:
            cur_list = base_ctx.setdefault("schemas_by_db", {}).setdefault(active_ctx_db, [active_ctx_sch])
            if active_ctx_sch not in cur_list:
                cur_list.insert(0, active_ctx_sch)

        has_objs_key = "objects_by_db_schema" in payload or "objects_by_db_schema" in incoming_ctx
        objs_by_pair = (
            payload.get("objects_by_db_schema")
            if "objects_by_db_schema" in payload
            else incoming_ctx.get("objects_by_db_schema")
        )
        legacy_pg_instances: list[str] = []
        if has_objs_key and isinstance(objs_by_pair, dict):
            norm_objs: dict[str, dict[str, list[str]]] = {}
            for pair_k, pair_v in objs_by_pair.items():
                clean_pair = str(pair_k).strip().upper()
                if isinstance(pair_v, dict):
                    if isinstance(pair_v.get("postgres_instances"), list):
                        for pg_i in pair_v["postgres_instances"]:
                            s_pg = str(pg_i).strip()
                            if s_pg and s_pg not in legacy_pg_instances:
                                legacy_pg_instances.append(s_pg)
                    norm_objs[clean_pair] = {
                        str(cat_k).strip().lower(): [str(item).strip() for item in cat_v if str(item).strip()]
                        for cat_k, cat_v in pair_v.items()
                        if isinstance(cat_v, list)
                    }
            base_ctx["objects_by_db_schema"] = norm_objs

        sel_objs = (
            payload.get("selected_objects")
            if "selected_objects" in payload
            else incoming_ctx.get("selected_objects")
        )
        if sel_objs is not None:
            base_ctx["selected_objects"] = sel_objs
            if isinstance(sel_objs, dict):
                for pair_k, pair_v in sel_objs.items():
                    if pair_v == "*" and str(pair_k).strip().upper() not in base_ctx["full_schemas"]:
                        base_ctx["full_schemas"].append(str(pair_k).strip().upper())
                    elif isinstance(pair_v, dict) and isinstance(pair_v.get("postgres_instances"), list):
                        for pg_i in pair_v["postgres_instances"]:
                            s_pg = str(pg_i).strip()
                            if s_pg and s_pg not in legacy_pg_instances:
                                legacy_pg_instances.append(s_pg)

        has_pg_key = any(
            k in payload or k in incoming_ctx
            for k in ("postgres_instances", "selected_postgres_instances")
        )
        raw_pg = (
            payload.get("postgres_instances")
            if "postgres_instances" in payload
            else (
                incoming_ctx.get("postgres_instances")
                if "postgres_instances" in incoming_ctx
                else (
                    payload.get("selected_postgres_instances")
                    if "selected_postgres_instances" in payload
                    else incoming_ctx.get("selected_postgres_instances")
                )
            )
        )
        if has_pg_key and isinstance(raw_pg, list):
            clean_pg = [str(p).strip() for p in raw_pg if str(p).strip()]
            base_ctx["postgres_instances"] = clean_pg
            base_ctx["selected_postgres_instances"] = list(clean_pg)
            base_ctx.setdefault("visible_objects", {})["postgres_instances"] = list(clean_pg)
            for _map_k in ("selected_objects", "objects_by_db_schema"):
                _m_obj = base_ctx.get(_map_k)
                if isinstance(_m_obj, dict):
                    for _pv in _m_obj.values():
                        if isinstance(_pv, dict):
                            _pv.pop("postgres_instances", None)
        elif legacy_pg_instances:
            base_ctx["postgres_instances"] = list(legacy_pg_instances)
            base_ctx["selected_postgres_instances"] = list(legacy_pg_instances)
        else:
            base_ctx.setdefault("postgres_instances", [])
            base_ctx.setdefault("selected_postgres_instances", list(base_ctx.get("postgres_instances") or []))

        if "postgres_tables_by_instance" in payload or "postgres_tables_by_instance" in incoming_ctx:
            pg_tbls_in = payload.get("postgres_tables_by_instance") or incoming_ctx.get("postgres_tables_by_instance")
            if isinstance(pg_tbls_in, dict):
                base_ctx["postgres_tables_by_instance"] = copy.deepcopy(pg_tbls_in)

        vis = payload.get("visible_objects") if "visible_objects" in payload else incoming_ctx.get("visible_objects")
        if isinstance(vis, dict):
            cur_vis = base_ctx.setdefault("visible_objects", {})
            for k, v in vis.items():
                if isinstance(v, list):
                    cur_vis[k] = [str(item).strip() for item in v if str(item).strip()]
                elif isinstance(v, bool):
                    if not v:
                        cur_vis[k] = []
                    else:
                        cur_vis.setdefault(k, [])
        from lfx_snowflake.context_hygiene import drop_phantom_telemetry_objects  # noqa: PLC0415

        drop_phantom_telemetry_objects(base_ctx)  # pre-v0.8.6 template phantoms (v0.8.6 E2E)
        if has_pg_key and isinstance(raw_pg, list):
            base_ctx.setdefault("visible_objects", {})["postgres_instances"] = list(base_ctx["postgres_instances"])
        elif base_ctx.get("postgres_instances"):
            base_ctx.setdefault("visible_objects", {})["postgres_instances"] = list(base_ctx["postgres_instances"])

        base_ctx["active_role"] = self.active_role
        if effective_flow_id:
            base_ctx["flow_id"] = effective_flow_id

        new_dbs, new_schemas, new_objs = self._extract_context_object_set(base_ctx)
        removed_databases = sorted(prev_dbs - new_dbs) if has_dbs_key else []
        removed_schemas = sorted(prev_schemas - new_schemas) if has_schemas_key else []
        removed_objects = (
            sorted(prev_objs - new_objs)
            if (has_objs_key or sel_objs is not None or isinstance(vis, dict) or has_pg_key)
            else []
        )
        added_objects = sorted(new_objs - prev_objs)

        self.context_epoch += 1
        base_ctx["context_epoch"] = self.context_epoch

        updated_node_templates: dict[str, dict[str, Any]] = {}
        if effective_flow_id:
            self.active_flow_id = effective_flow_id
            self.contexts_by_flow_id[effective_flow_id] = copy.deepcopy(base_ctx)
            self._save_flow_context_sqlite(effective_flow_id, base_ctx)
            updated_node_templates = self.sync_flow_node_templates_sqlite(effective_flow_id, base_ctx)
        else:
            self.database = active_ctx_db
            self.schema = active_ctx_sch
            self.selected_context = copy.deepcopy(base_ctx)
            self.contexts_by_flow_id["__global__"] = copy.deepcopy(base_ctx)
            self._save_flow_context_sqlite("__global__", base_ctx)

        compact_audit_details = {
            "flow_id": effective_flow_id,
            "database": active_ctx_db,
            "schema": active_ctx_sch,
            "databases": list(base_ctx.get("databases") or []),
            "schemas_by_db": dict(base_ctx.get("schemas_by_db") or {}),
            "full_databases": list(base_ctx.get("full_databases") or []),
            "full_schemas": list(base_ctx.get("full_schemas") or []),
            "postgres_instances": list(base_ctx.get("postgres_instances") or []),
            "selected_pairs_count": len(base_ctx.get("selected_objects") or {}),
            "added_objects": added_objects,
            "removed_objects": removed_objects,
            "removed_databases": removed_databases,
            "removed_schemas": removed_schemas,
            "context_epoch": self.context_epoch,
        }
        self.record_audit_event(
            action="UPDATE_CONTEXT",
            outcome="ALLOWED",
            target_object=f"{active_ctx_db}.{active_ctx_sch}",
            flow_id=effective_flow_id,
            details=compact_audit_details,
        )

        if removed_databases or removed_schemas or removed_objects:
            self.record_audit_event(
                action="OBJECT_REMOVED_FROM_CONTEXT",
                outcome="ALLOWED",
                target_object=effective_flow_id or f"{active_ctx_db}.{active_ctx_sch}",
                flow_id=effective_flow_id,
                details={
                    "flow_id": effective_flow_id,
                    "removed_databases": removed_databases,
                    "removed_schemas": removed_schemas,
                    "removed_objects": removed_objects,
                    "context_epoch": self.context_epoch,
                },
            )

        role_intersection = self.compute_role_effective_context(effective_flow_id, self.active_role)
        reconciled_triggers = self.reconcile_triggers_with_flow_context(effective_flow_id)
        return {
            "ok": True,
            "status": "ok",
            "flow_id": effective_flow_id,
            "active_flow_id": self.active_flow_id,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "database": active_ctx_db,
            "schema": active_ctx_sch,
            "selected_context": copy.deepcopy(base_ctx),
            "effective_context": role_intersection["effective_context"],
            "inaccessible_objects": role_intersection["inaccessible_objects"],
            "updated_node_templates": updated_node_templates,
            "reconciled_triggers": reconciled_triggers,
            "role_privileges": self.role_privileges,
        }

    # BUG-CA-02 / BUG-MCP-03: per-session role state ------------------------------------------------

    @staticmethod
    def _role_session_user() -> str:
        return (_sf_identity.current_identity().sf_user or "").strip().upper()

    def _role_session(self) -> dict[str, Any] | None:
        """Session for the current (user, flow_id), falling back to the user's flow-less session."""
        sessions = self.__dict__.get("_role_sessions")
        if not sessions:
            return None
        user = self._role_session_user()
        fid = self._normalize_flow_id(CURRENT_SNOWFLAKE_FLOW_ID.get())
        return sessions.get((user, fid)) or (sessions.get((user, None)) if fid else None)

    @property
    def active_role(self) -> str:
        pinned = TRIGGER_RUN_ROLE.get()
        if pinned:
            return pinned
        sess = self._role_session()
        if sess is not None:
            return sess["role"]
        return self.__dict__.get("_base_active_role", "")

    @active_role.setter
    def active_role(self, value: str) -> None:
        sess = self._role_session()
        if sess is not None:
            sess["role"] = (value or "").strip().upper()
        else:
            self.__dict__["_base_active_role"] = (value or "").strip().upper()

    @property
    def service_active_role(self) -> str:
        """Service default role, independent of any user's role selection."""
        return self.__dict__.get("_base_active_role", "")

    @property
    def context_epoch(self) -> int:
        sess = self._role_session()
        if sess is not None:
            return int(sess["epoch"])
        return int(self.__dict__.get("_base_context_epoch", 1))

    @context_epoch.setter
    def context_epoch(self, value: int) -> None:
        sess = self._role_session()
        if sess is not None:
            sess["epoch"] = int(value)
        else:
            self.__dict__["_base_context_epoch"] = int(value)

    def _set_session_role(self, role: str, flow_id: str | None) -> dict[str, Any]:
        """Record a role selection for the current authenticated user and flow; returns the session.

        Precedence on read: (user, flow) > (user, *) > service base. A user-wide switch (``flow_id=None``)
        is the newest intent for every flow, so it drops that user's flow-scoped entries.
        """
        user = self._role_session_user()
        key = (user, flow_id or None)
        with self._role_sessions_lock:
            previous_epoch = self.context_epoch
            if key[1] is None:
                for stale in [k for k in self._role_sessions if k[0] == user and k[1] is not None]:
                    previous_epoch = max(previous_epoch, int(self._role_sessions[stale]["epoch"]))
                    del self._role_sessions[stale]
            sess = {"role": role, "epoch": previous_epoch + 1, "flow_id": flow_id or None, "user": user or None}
            self._role_sessions[key] = sess
        return sess

    def current_session_role(self) -> str | None:
        """Role explicitly selected (and verified) by the current user, or ``None`` if only the base applies."""
        sess = self._role_session()
        return sess["role"] if sess is not None else None

    def _is_execute_sql_mocked(self) -> bool:
        """Detect whether `self.execute_sql` has been monkeypatched by a unit test."""
        return "execute_sql" in self.__dict__

    def discover_schemas_only(
        self,
        *,
        database: str | None = None,
        databases: list[str] | str | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Fast schema-only discovery for Snowsight tree lazy-loading (`GET /api/v1/snowflake/schemas`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        parsed_dbs: list[str] = []
        if isinstance(databases, str) and databases.strip():
            parsed_dbs = [d.strip().upper() for d in databases.split(",") if d.strip()]
        elif isinstance(databases, list):
            parsed_dbs = [str(d).strip().upper() for d in databases if str(d).strip()]
        if db and db not in parsed_dbs:
            parsed_dbs.insert(0, db)

        def _extract_names(rows: list[dict[str, Any]], keys: tuple[str, ...] = ("name", "NAME", "OBJECT_NAME")) -> list[str]:
            out: list[str] = []
            for r in rows:
                for k in keys:
                    val = r.get(k)
                    if val and str(val).strip() and str(val).strip() not in out:
                        out.append(str(val).strip())
                        break
            return out

        use_cache = self._has_spcs_token() and not self._is_execute_sql_mocked() and not force_refresh
        now = time.monotonic()
        schemas_by_db_out: dict[str, list[str]] = dict(self.selected_context.get("schemas_by_db", {}))
        primary_schemas: list[str] = []

        for target_db in (parsed_dbs or [db]):
            if not _sql_safety.is_valid_identifier(target_db):
                continue
            cache_key = f"{self.active_role}:{target_db}"
            cached_entry = self._schemas_cache.get(cache_key) if use_cache else None
            if cached_entry and (now - cached_entry[0]) < 120.0:
                db_schemas = list(cached_entry[1])
            else:
                sch_res = self.execute_sql(f"SHOW SCHEMAS IN DATABASE {target_db}", timeout=12)
                db_schemas = _extract_names(sch_res.get("rows", [])) or list(
                    dict.fromkeys([*(schemas_by_db_out.get(target_db) or []), "CORE", "PUBLIC"])
                )
                if self._has_spcs_token() and not self._is_execute_sql_mocked():
                    self._schemas_cache[cache_key] = (now, list(db_schemas))
            schemas_by_db_out[target_db] = db_schemas
            if target_db == db:
                primary_schemas = db_schemas

        self.selected_context["schemas_by_db"] = schemas_by_db_out
        return {
            "ok": True,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "database": db,
            "schemas": primary_schemas or ["CORE", "PUBLIC"],
            "schemas_by_db": schemas_by_db_out,
        }

    def discover_catalog(
        self,
        *,
        database: str | None = None,
        schema: str | None = None,
        object_kind: str | None = None,
        databases: list[str] | str | None = None,
        schemas_by_db: dict[str, list[str]] | str | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Discover Multi-Database -> Multi-Schema -> Cascading Objects for CURRENT_ROLE() (`W2-R2-00`, `HUB-W2-02`)."""
        db = (database or self.database).strip().upper()
        sch = (schema or self.schema).strip().upper()

        parsed_dbs: list[str] = []
        if isinstance(databases, str) and databases.strip():
            parsed_dbs = [d.strip().upper() for d in databases.split(",") if d.strip()]
        elif isinstance(databases, list):
            parsed_dbs = [str(d).strip().upper() for d in databases if str(d).strip()]
        if db and db not in parsed_dbs:
            parsed_dbs.insert(0, db)

        parsed_schemas_map: dict[str, list[str]] = {}
        if isinstance(schemas_by_db, str) and schemas_by_db.strip():
            try:
                loaded_map = json.loads(schemas_by_db)
                if isinstance(loaded_map, dict):
                    for k, v in loaded_map.items():
                        if isinstance(v, list):
                            parsed_schemas_map[str(k).strip().upper()] = [str(s).strip().upper() for s in v if str(s).strip()]
            except Exception:  # noqa: BLE001
                pass
        elif isinstance(schemas_by_db, dict):
            for k, v in schemas_by_db.items():
                if isinstance(v, list):
                    parsed_schemas_map[str(k).strip().upper()] = [str(s).strip().upper() for s in v if str(s).strip()]

        def _extract_names(rows: list[dict[str, Any]], keys: tuple[str, ...] = ("name", "NAME", "OBJECT_NAME")) -> list[str]:
            out: list[str] = []
            for r in rows:
                for k in keys:
                    val = r.get(k)
                    if val and str(val).strip() and str(val).strip() not in out:
                        out.append(str(val).strip())
                        break
            return out

        use_live_parallel_cache = self._has_spcs_token() and not self._is_execute_sql_mocked() and not force_refresh
        now = time.monotonic()

        dbs_cache_entry = self._dbs_cache.get(self.active_role) if use_live_parallel_cache else None
        if dbs_cache_entry and (now - dbs_cache_entry[0]) < 120.0:
            discovered_databases = list(dbs_cache_entry[1])
            postgres_instances = list(dbs_cache_entry[2])
        elif self._has_spcs_token() and not self._is_execute_sql_mocked():
            fut_dbs = _sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, "SHOW DATABASES", timeout=12)
            fut_pg = _sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, "SHOW POSTGRES INSTANCES", timeout=12)
            dbs_res = fut_dbs.result()
            pg_res = fut_pg.result()
            discovered_databases = _extract_names(dbs_res.get("rows", [])) or list(
                dict.fromkeys([db, *parsed_dbs, *self.selected_context.get("databases", [self.database])])
            )
            postgres_instances = _extract_names(pg_res.get("rows", [])) or list(
                self.selected_context.get("visible_objects", {}).get("postgres_instances", [])
            )
            self._dbs_cache[self.active_role] = (now, list(discovered_databases), list(postgres_instances))
        else:
            dbs_res = self.execute_sql("SHOW DATABASES")
            pg_res = self.execute_sql("SHOW POSTGRES INSTANCES")
            discovered_databases = _extract_names(dbs_res.get("rows", [])) or list(
                dict.fromkeys([db, *parsed_dbs, *self.selected_context.get("databases", [self.database])])
            )
            postgres_instances = _extract_names(pg_res.get("rows", [])) or list(
                self.selected_context.get("visible_objects", {}).get("postgres_instances", [])
            )

        schemas_by_db_out: dict[str, list[str]] = dict(self.selected_context.get("schemas_by_db", {}))
        objects_by_db_schema: dict[str, dict[str, list[str]]] = dict(
            self.selected_context.get("objects_by_db_schema", {})
        )

        # Bound target_dbs so setting multi-DB context never triggers an N*M*8 SHOW query storm
        target_dbs = (parsed_dbs or [db])[:3]
        primary_schemas: list[str] = []
        primary_raw_objects: list[dict[str, Any]] = []
        primary_stages_rows: list[dict[str, Any]] = []

        merged_tables: list[str] = []
        merged_views: list[str] = []
        merged_iceberg_tables: list[str] = []
        merged_stages: list[str] = []
        merged_functions: list[str] = []
        merged_procedures: list[str] = []
        merged_search_services: list[str] = []
        merged_semantic_views: list[str] = []
        merged_cortex_agents: list[str] = []

        for target_db in target_dbs:
            if not _sql_safety.is_valid_identifier(target_db):
                continue
            sch_cache_key = f"{self.active_role}:{target_db}"
            cached_sch = self._schemas_cache.get(sch_cache_key) if use_live_parallel_cache else None
            if cached_sch and (now - cached_sch[0]) < 120.0:
                db_schemas = list(cached_sch[1])
            else:
                sch_res = self.execute_sql(f"SHOW SCHEMAS IN DATABASE {target_db}", timeout=12)
                db_schemas = _extract_names(sch_res.get("rows", [])) or list(
                    dict.fromkeys([*(parsed_schemas_map.get(target_db) or []), sch if target_db == db else "CORE", "CORE", "PUBLIC"])
                )
                if self._has_spcs_token() and not self._is_execute_sql_mocked():
                    self._schemas_cache[sch_cache_key] = (now, list(db_schemas))
            schemas_by_db_out[target_db] = db_schemas
            if target_db == db:
                primary_schemas = db_schemas

            # Bound active schemas per call so selecting a whole DB does not synchronously scan 20 schemas at once
            explicit_schemas = parsed_schemas_map.get(target_db)
            active_schemas_for_db = (explicit_schemas[:2] if explicit_schemas else None) or (
                [sch] if target_db == db else db_schemas[:1]
            )
            for target_sch in active_schemas_for_db:
                if not _sql_safety.is_valid_identifier(target_sch):
                    continue
                pair_key = f"{target_db}.{target_sch}"
                obj_cache_key = f"{self.active_role}:{pair_key}"
                cached_obj = self._objects_cache.get(obj_cache_key) if use_live_parallel_cache else None
                if cached_obj and (now - cached_obj[0]) < 120.0:
                    cached_payload = cached_obj[1]
                    raw_objs = cached_payload["raw_objs"]
                    stages_rows = cached_payload["stages_rows"]
                    pair_bucket = dict(cached_payload["pair_bucket"])
                    pair_bucket.pop("postgres_instances", None)
                    objects_by_db_schema[pair_key] = pair_bucket
                    if target_db == db and target_sch == sch:
                        primary_raw_objects = raw_objs
                        primary_stages_rows = stages_rows
                    pair_tables = pair_bucket["tables"]
                    pair_views = pair_bucket["views"]
                    pair_iceberg = pair_bucket["iceberg_tables"]
                    pair_stages = pair_bucket["stages"]
                    pair_funcs = pair_bucket["functions"]
                    pair_procs = pair_bucket["procedures"]
                    pair_search = pair_bucket["cortex_search_services"]
                    pair_sem_views = pair_bucket["semantic_views"]
                    pair_agents = pair_bucket["cortex_agents"]
                else:
                    show_queries = [
                        f"SHOW OBJECTS IN SCHEMA {pair_key}",
                        f"SHOW ICEBERG TABLES IN SCHEMA {pair_key}",
                        f"SHOW STAGES IN SCHEMA {pair_key}",
                        f"SHOW USER FUNCTIONS IN SCHEMA {pair_key}",
                        f"SHOW USER PROCEDURES IN SCHEMA {pair_key}",
                        f"SHOW CORTEX SEARCH SERVICES IN SCHEMA {pair_key}",
                        f"SHOW SEMANTIC VIEWS IN SCHEMA {pair_key}",
                        f"SHOW AGENTS IN SCHEMA {_sql_safety.quote_fqn(pair_key, 'schema', min_parts=2, max_parts=2)}",
                    ]
                    if self._has_spcs_token() and not self._is_execute_sql_mocked():
                        futs = [_sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, q, timeout=10) for q in show_queries]
                        (
                            objects_res,
                            iceberg_res,
                            stages_res,
                            funcs_res,
                            procs_res,
                            search_res,
                            sem_views_res,
                            agents_res,
                        ) = [f.result() for f in futs]
                    else:
                        objects_res = self.execute_sql(show_queries[0])
                        iceberg_res = self.execute_sql(show_queries[1])
                        stages_res = self.execute_sql(show_queries[2])
                        funcs_res = self.execute_sql(show_queries[3])
                        procs_res = self.execute_sql(show_queries[4])
                        search_res = self.execute_sql(show_queries[5])
                        sem_views_res = self.execute_sql(show_queries[6])
                        agents_res = self.execute_sql(show_queries[7])

                    raw_objs = objects_res.get("rows", [])
                    stages_rows = stages_res.get("rows", [])
                    if target_db == db and target_sch == sch:
                        primary_raw_objects = raw_objs
                        primary_stages_rows = stages_rows

                    pair_tables = []
                    pair_views = []
                    pair_iceberg = _extract_names(iceberg_res.get("rows", []))
                    for obj in raw_objs:
                        nm = str(obj.get("name") or obj.get("NAME") or "").strip()
                        kd = str(obj.get("kind") or obj.get("KIND") or "TABLE").upper()
                        is_ice = str(obj.get("is_iceberg") or obj.get("IS_ICEBERG") or "N").upper() == "Y" or "ICEBERG" in kd
                        if not nm:
                            continue
                        if "VIEW" in kd:
                            pair_views.append(nm)
                        elif is_ice:
                            if nm not in pair_iceberg:
                                pair_iceberg.append(nm)
                            pair_tables.append(nm)
                        else:
                            pair_tables.append(nm)

                    # BUG-CTX-PH-01: buckets come only from real SHOW results, never from synthesized defaults.

                    pair_stages = _extract_names(stages_rows)
                    pair_funcs = _extract_names(funcs_res.get("rows", []))
                    pair_procs = _extract_names(procs_res.get("rows", []))
                    pair_search = _extract_names(search_res.get("rows", []))
                    pair_sem_views = _extract_names(sem_views_res.get("rows", []))
                    pair_agents = _extract_names(agents_res.get("rows", []))

                    pair_bucket = {
                        "tables": pair_tables,
                        "views": pair_views,
                        "iceberg_tables": pair_iceberg,
                        "stages": pair_stages,
                        "functions": pair_funcs,
                        "procedures": pair_procs,
                        "cortex_search_services": pair_search,
                        "semantic_views": pair_sem_views,
                        "semantic_models": [],
                        "cortex_agents": pair_agents,
                    }
                    objects_by_db_schema[pair_key] = pair_bucket
                    if self._has_spcs_token() and not self._is_execute_sql_mocked():
                        self._objects_cache[obj_cache_key] = (
                            now,
                            {
                                "raw_objs": raw_objs,
                                "stages_rows": stages_rows,
                                "pair_bucket": dict(pair_bucket),
                            },
                        )

                for st_row in stages_rows or []:
                    if isinstance(st_row, dict):
                        st_nm = str(st_row.get("name") or st_row.get("NAME") or "").strip().upper()
                        if st_nm:
                            self._stage_meta[f"{pair_key}.{st_nm}"] = {
                                "type": str(st_row.get("type") or st_row.get("TYPE") or "").strip().upper(),
                                "url": str(st_row.get("url") or st_row.get("URL") or ""),
                                "cloud": str(st_row.get("cloud") or st_row.get("CLOUD") or ""),
                                "storage_integration": st_row.get("storage_integration") or st_row.get("STORAGE_INTEGRATION"),
                                "directory_enabled": str(
                                    st_row.get("directory_enabled") or st_row.get("DIRECTORY_ENABLED") or ""
                                ).upper() in ("Y", "TRUE"),
                                "comment": str(st_row.get("comment") or st_row.get("COMMENT") or ""),
                            }
                for item in pair_tables:
                    if item not in merged_tables:
                        merged_tables.append(item)
                for item in pair_views:
                    if item not in merged_views:
                        merged_views.append(item)
                for item in pair_iceberg:
                    if item not in merged_iceberg_tables:
                        merged_iceberg_tables.append(item)
                for item in pair_stages:
                    if item not in merged_stages:
                        merged_stages.append(item)
                for item in pair_funcs:
                    if item not in merged_functions:
                        merged_functions.append(item)
                for item in pair_procs:
                    if item not in merged_procedures:
                        merged_procedures.append(item)
                for item in pair_search:
                    if item not in merged_search_services:
                        merged_search_services.append(item)
                for item in pair_sem_views:
                    if item not in merged_semantic_views:
                        merged_semantic_views.append(item)
                for item in pair_agents:
                    if item not in merged_cortex_agents:
                        merged_cortex_agents.append(item)

        memories = [m.get("name", "") for m in self.memories_catalog if m.get("name")]
        for pair_val in objects_by_db_schema.values():
            if isinstance(pair_val, dict):
                pair_val.setdefault("memories", list(memories))
        self.selected_context["schemas_by_db"] = schemas_by_db_out
        self.selected_context["objects_by_db_schema"] = objects_by_db_schema

        return {
            "ok": True,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "database": db,
            "schema": sch,
            "object_kind": object_kind,
            "databases": discovered_databases,
            "schemas": primary_schemas or ["CORE", "PUBLIC"],
            "schemas_by_db": schemas_by_db_out,
            "objects_by_db_schema": objects_by_db_schema,
            "categories": objects_by_db_schema.get(f"{db}.{sch}", {}),
            "objects": primary_raw_objects
            or [{"name": t, "kind": "TABLE"} for t in merged_tables] + [{"name": v, "kind": "VIEW"} for v in merged_views],
            "tables": merged_tables,
            "views": merged_views,
            "iceberg_tables": merged_iceberg_tables,
            "stages": primary_stages_rows or [{"name": s} for s in merged_stages],
            "stage_names": merged_stages,
            "memories": memories,
            "functions": merged_functions,
            "procedures": merged_procedures,
            "cortex_search_services": merged_search_services,
            "semantic_views": merged_semantic_views,
            "semantic_models": list(self.selected_context.get("visible_objects", {}).get("semantic_models", [])),
            "cortex_agents": merged_cortex_agents,
            "postgres_instances": postgres_instances,
            "selected_context": self.selected_context,
            "role_privileges": self.role_privileges,
        }

    def invoke_cortex_analyst(
        self,
        *,
        database: str,
        schema: str,
        semantic_source: str,
        source_mode: str = "Semantic View",
        question: str,
        execute_sql: bool = True,
        max_rows: int = 50,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Invoke the official Snowflake Cortex Analyst REST API `/api/v2/cortex/analyst/message` (`W2-R2-06`).

        BUG-CTX-03: inside a flow, the semantic view (or stage YAML) must be in that flow's `[ ❄️ Context ]`.
        The SQL Analyst generates over the semantic view's base tables is authorized by the view and is not
        re-checked against the context.
        """
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or self.schema or "CORE").strip().upper()
        src = (semantic_source or "").strip()
        mode = (source_mode or "Semantic View").strip()
        ctx_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        if ctx_fid:
            try:
                if mode in ("Stage YAML", "Stage YAML Model") or src.startswith("@"):
                    ref = _flow_guard._stage_ref(src, db, sch)
                else:
                    parts = [p.strip('"').upper() for p in src.split(".") if p.strip()]
                    ref = _flow_guard._resolve(parts or ["EXAMPLE_SEMANTIC_VIEW"], db, sch, "object")
                in_ctx = _flow_guard.is_ref_in_context(ref, self.get_flow_context(ctx_fid, create=False))
                ref_fqn = ref.fqn
            except _flow_guard.UnverifiableStatement:
                in_ctx, ref_fqn = False, src
            if not in_ctx:
                return {
                    "ok": False,
                    "status_code": 403,
                    "error_code": _flow_guard.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT,
                    "message": f"Semantic source {ref_fqn} is not in the [ ❄️ Context ] of flow {ctx_fid}.",
                    "flow_id": ctx_fid,
                }
        user_q = (question or "Show summary metrics").strip()
        bounded_rows = max(1, min(int(max_rows or 50), 1000))

        is_stage_yaml = mode in ("Stage YAML", "Stage YAML Model") or src.startswith("@")
        if is_stage_yaml and not src.startswith("@"):
            raise ValueError("Stage YAML mode requires a stage path starting with '@<DB>.<SCHEMA>.<STAGE>/...'.")

        request_payload: dict[str, Any] = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_q}],
                }
            ]
        }
        if is_stage_yaml:
            request_payload["semantic_model_file"] = src
        else:
            sv_fqn = src if "." in src else f"{db}.{sch}.{src or 'EXAMPLE_SEMANTIC_VIEW'}"
            request_payload["semantic_view"] = sv_fqn

        rest_endpoint = "/api/v2/cortex/analyst/message"
        generated_sql = (
            f"SELECT run_id, flow_id, effective_role, status, start_ts "
            f"FROM {self.runs_table} LIMIT {bounded_rows}"
        )
        interpretation = (
            f"Cortex Analyst ({'semantic_model_file' if is_stage_yaml else 'semantic_view'}={src or f'{db}.{sch}.EXAMPLE_SEMANTIC_VIEW'}) "
            f"interpreted question: {user_q}"
        )

        if self._has_spcs_token() and httpx is not None:
            host = os.environ["SNOWFLAKE_HOST"]
            url = f"https://{host}{rest_endpoint}"
            try:
                with httpx.Client(timeout=60.0, verify=True) as client:
                    resp = client.post(url, headers=self._get_spcs_headers(_sf_identity.current_request_mode()), json=request_payload)
                    if resp.status_code < 400 and resp.content:
                        body = resp.json()
                        msg_obj = body.get("message") or {}
                        for block in msg_obj.get("content") or []:
                            if block.get("type") == "text" and block.get("text"):
                                interpretation = str(block["text"])
                            elif block.get("type") == "sql" and block.get("statement"):
                                generated_sql = str(block["statement"])
            except Exception:  # noqa: BLE001
                pass

        sql_rows: list[dict[str, Any]] = []
        generated_sql_blocked = False
        if execute_sql and generated_sql:
            # Cortex Analyst output is model-generated: only a single read-only statement is executed.
            if _sf_identity.is_read_only_statement(generated_sql):
                exec_res = self.execute_sql(generated_sql)
                sql_rows = exec_res.get("rows", [])
            else:
                generated_sql_blocked = True

        return {
            "ok": True,
            "generated_sql_blocked": generated_sql_blocked,
            "rest_endpoint": rest_endpoint,
            "database": db,
            "schema_name": sch,
            "source_mode": mode,
            "semantic_source": src,
            "request_payload": request_payload,
            "question": user_q,
            "interpretation": interpretation,
            "generated_sql": generated_sql,
            "execute_generated_sql": bool(execute_sql),
            "rows": sql_rows,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
        }

    # ------------------------------------------------------------------
    # MA-W2-R6-01 & MA-W2-R6-05: Snowflake Cortex Agent (:run), 2-Level
    # Threads API Tracking/Auto-Recovery & A2A v0.3.0 Server Bridge
    # ------------------------------------------------------------------

    def _ensure_cortex_agent_threads_sqlite_table(self) -> Path | None:
        """Ensure `snowflake_cortex_agent_threads` table exists in SQLite (`langflow.db`) (`MA-W2-R6-01`)."""
        try:
            db_path = self._resolve_writable_sqlite_db_path()
            conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snowflake_cortex_agent_threads (
                    flow_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    agent_fqn TEXT NOT NULL,
                    active_role TEXT NOT NULL,
                    thread_id INTEGER NOT NULL,
                    thread_name TEXT,
                    last_user_message_id INTEGER,
                    last_assistant_message_id INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (flow_id, session_id, agent_fqn, active_role)
                )
                """
            )
            conn.commit()
            conn.close()
            return db_path
        except Exception:  # noqa: BLE001
            return None

    def _load_cortex_agent_threads_sqlite(self) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        """Hydrate `_cortex_agent_threads_mem` and `_cortex_threads_server_mem` from SQLite `snowflake_cortex_agent_threads`."""
        loaded: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        try:
            db_path = self._resolve_writable_sqlite_db_path()
            if not db_path.exists():
                return loaded
            conn = _open_sqlite_conn(db_path, readonly=True, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "snowflake_cortex_agent_threads" in tables:
                rows = cur.execute("SELECT * FROM snowflake_cortex_agent_threads ORDER BY updated_at ASC").fetchall()
                for r in rows:
                    d = dict(r)
                    fid = str(d.get("flow_id") or "default")
                    sid = str(d.get("session_id") or "default")
                    afqn = str(d.get("agent_fqn") or "").upper()
                    rl = str(d.get("active_role") or self.active_role).upper()
                    tid = int(d.get("thread_id") or 0)
                    if not afqn or tid <= 0:
                        continue
                    rec = {
                        "flow_id": fid,
                        "session_id": sid,
                        "agent_fqn": afqn,
                        "active_role": rl,
                        "thread_id": tid,
                        "thread_name": str(d.get("thread_name") or f"lf:{fid[:8]}:{sid[:16]}:{afqn.split('.')[-1]}"),
                        "last_user_message_id": int(d["last_user_message_id"]) if d.get("last_user_message_id") is not None else None,
                        "last_assistant_message_id": int(d.get("last_assistant_message_id") or 0),
                        "updated_at": str(d.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                    }
                    key = (fid, sid, afqn, rl)
                    self._cortex_agent_threads_mem[key] = rec
                    loaded[key] = rec
                    if tid > self._cortex_thread_seq:
                        self._cortex_thread_seq = tid
                    self._cortex_threads_server_mem.setdefault(
                        tid,
                        {
                            "metadata": {
                                "thread_id": tid,
                                "thread_name": rec["thread_name"],
                                "origin_application": "langflow",
                                "created_on": rec["updated_at"],
                                "updated_on": rec["updated_at"],
                            },
                            "messages": [],
                        },
                    )
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        return loaded

    def _save_cortex_agent_thread_pointer(
        self,
        *,
        flow_id: str,
        session_id: str,
        agent_fqn: str,
        active_role: str,
        thread_id: int,
        thread_name: str | None = None,
        last_user_message_id: int | None = None,
        last_assistant_message_id: int = 0,
    ) -> dict[str, Any]:
        """Persist `(flow_id, session_id, agent_fqn, active_role)` thread pointer in memory and SQLite `snowflake_cortex_agent_threads`."""
        norm_fid = self._normalize_flow_id(flow_id) or str(flow_id or "default").strip() or "default"
        norm_sid = str(session_id or norm_fid or "default").strip() or "default"
        norm_fqn = str(agent_fqn or "").strip().upper()
        norm_role = str(active_role or self.active_role or "").strip().upper()
        tid = int(thread_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        short_agent = norm_fqn.split(".")[-1] if norm_fqn else "AGENT"
        t_name = str(thread_name or f"lf:{norm_fid[:8]}:{norm_sid[:16]}:{short_agent}")
        rec = {
            "flow_id": norm_fid,
            "session_id": norm_sid,
            "agent_fqn": norm_fqn,
            "active_role": norm_role,
            "thread_id": tid,
            "thread_name": t_name,
            "last_user_message_id": int(last_user_message_id) if last_user_message_id is not None else None,
            "last_assistant_message_id": int(last_assistant_message_id or 0),
            "updated_at": now_iso,
        }
        self._cortex_agent_threads_mem[(norm_fid, norm_sid, norm_fqn, norm_role)] = rec
        try:
            db_path = self._ensure_cortex_agent_threads_sqlite_table()
            if db_path is not None:
                conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO snowflake_cortex_agent_threads (
                        flow_id, session_id, agent_fqn, active_role,
                        thread_id, thread_name, last_user_message_id, last_assistant_message_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        norm_fid,
                        norm_sid,
                        norm_fqn,
                        norm_role,
                        tid,
                        t_name,
                        rec["last_user_message_id"],
                        rec["last_assistant_message_id"],
                        now_iso,
                    ),
                )
                conn.commit()
                conn.close()
        except Exception:  # noqa: BLE001
            pass
        return rec

    def _get_cortex_agent_thread_pointer(
        self,
        *,
        flow_id: str,
        session_id: str | None = None,
        agent_fqn: str,
        active_role: str | None = None,
        latest_for_agent: bool = False,
    ) -> dict[str, Any] | None:
        """Lookup persisted Cortex Agent thread pointer from SQLite or in-memory mirror."""
        norm_fid = self._normalize_flow_id(flow_id) or str(flow_id or "default").strip() or "default"
        norm_sid = str(session_id or norm_fid or "default").strip() or "default"
        norm_fqn = str(agent_fqn or "").strip().upper()
        norm_role = str(active_role or self.active_role or "").strip().upper()

        try:
            db_path = self._resolve_writable_sqlite_db_path()
            if db_path.exists():
                conn = _open_sqlite_conn(db_path, readonly=True, timeout=15.0)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                if "snowflake_cortex_agent_threads" in tables:
                    if latest_for_agent:
                        row = cur.execute(
                            """
                            SELECT * FROM snowflake_cortex_agent_threads
                            WHERE flow_id = ? AND agent_fqn = ? AND active_role = ?
                            ORDER BY updated_at DESC LIMIT 1
                            """,
                            (norm_fid, norm_fqn, norm_role),
                        ).fetchone()
                    else:
                        row = cur.execute(
                            """
                            SELECT * FROM snowflake_cortex_agent_threads
                            WHERE flow_id = ? AND session_id = ? AND agent_fqn = ? AND active_role = ?
                            ORDER BY updated_at DESC LIMIT 1
                            """,
                            (norm_fid, norm_sid, norm_fqn, norm_role),
                        ).fetchone()
                    conn.close()
                    if row is not None:
                        d = dict(row)
                        rec = {
                            "flow_id": str(d["flow_id"]),
                            "session_id": str(d["session_id"]),
                            "agent_fqn": str(d["agent_fqn"]).upper(),
                            "active_role": str(d["active_role"]).upper(),
                            "thread_id": int(d["thread_id"]),
                            "thread_name": str(d.get("thread_name") or ""),
                            "last_user_message_id": int(d["last_user_message_id"]) if d.get("last_user_message_id") is not None else None,
                            "last_assistant_message_id": int(d.get("last_assistant_message_id") or 0),
                            "updated_at": str(d.get("updated_at") or ""),
                        }
                        self._cortex_agent_threads_mem[(rec["flow_id"], rec["session_id"], rec["agent_fqn"], rec["active_role"])] = rec
                        return rec
                else:
                    conn.close()
        except Exception:  # noqa: BLE001
            pass

        if not latest_for_agent:
            exact = self._cortex_agent_threads_mem.get((norm_fid, norm_sid, norm_fqn, norm_role))
            if exact is not None:
                return dict(exact)
            return None

        candidates = [
            rec
            for (fid, _sid, fqn, rl), rec in self._cortex_agent_threads_mem.items()
            if fid == norm_fid and fqn == norm_fqn and rl == norm_role
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda r: str(r.get("updated_at") or ""), reverse=True)
        return dict(candidates[0])

    def create_cortex_thread(
        self,
        *,
        flow_id: str | None = None,
        session_id: str | None = None,
        agent_name: str | None = None,
        origin_application: str = "langflow",
    ) -> dict[str, Any]:
        """Create a new Snowflake Cortex Thread via `POST /api/v2/cortex/threads` and name it via `POST /api/v2/cortex/threads/{id}` (`MA-W2-R6-01`)."""
        norm_fid = self._normalize_flow_id(flow_id) or str(flow_id or self.active_flow_id or "default").strip() or "default"
        norm_sid = str(session_id or norm_fid or "default").strip() or "default"
        short_agent = str(agent_name or "AGENT").strip().split(".")[-1] or "AGENT"
        origin_app = str(origin_application or "langflow").strip()[:16] or "langflow"
        thread_name = f"lf:{norm_fid[:8]}:{norm_sid[:16]}:{short_agent}"
        now_iso = datetime.now(timezone.utc).isoformat()

        created_tid: int | None = None
        if callable(self._cortex_rest_transport_fn):
            raw = self._cortex_rest_transport_fn("POST", "/api/v2/cortex/threads", {"origin_application": origin_app})
            if isinstance(raw, dict) and raw.get("thread_id") is not None:
                created_tid = int(raw["thread_id"])
            elif isinstance(raw, (int, str)) and str(raw).strip().isdigit():
                created_tid = int(str(raw).strip())
            if created_tid is not None:
                self._cortex_rest_transport_fn(
                    "POST",
                    f"/api/v2/cortex/threads/{created_tid}",
                    {"thread_name": thread_name},
                )
        elif self._has_spcs_token() and httpx is not None:
            host = os.environ["SNOWFLAKE_HOST"]
            try:
                with httpx.Client(timeout=30.0, verify=True) as client:
                    resp = client.post(
                        f"https://{host}/api/v2/cortex/threads",
                        headers=self._get_spcs_headers(_sf_identity.current_request_mode()),
                        json={"origin_application": origin_app},
                    )
                    if resp.status_code < 400 and resp.content:
                        body = resp.json()
                        if isinstance(body, dict) and body.get("thread_id") is not None:
                            created_tid = int(body["thread_id"])
                        elif isinstance(body, (int, str)) and str(body).strip().isdigit():
                            created_tid = int(str(body).strip())
                    if created_tid is not None:
                        client.post(
                            f"https://{host}/api/v2/cortex/threads/{created_tid}",
                            headers=self._get_spcs_headers(_sf_identity.current_request_mode()),
                            json={"thread_name": thread_name},
                        )
            except Exception:  # noqa: BLE001
                pass

        if created_tid is None:
            self._cortex_thread_seq += 1
            created_tid = self._cortex_thread_seq

        meta = {
            "thread_id": int(created_tid),
            "thread_name": thread_name,
            "origin_application": origin_app,
            "created_on": now_iso,
            "updated_on": now_iso,
        }
        self._cortex_threads_server_mem[int(created_tid)] = {
            "metadata": meta,
            "messages": [],
        }
        return {"ok": True, **meta}

    def list_cortex_threads(
        self,
        *,
        origin_application: str = "langflow",
        flow_id: str | None = None,
        agent_fqn: str | None = None,
    ) -> list[dict[str, Any]]:
        """List Snowflake Cortex Threads via `GET /api/v2/cortex/threads?origin_application=langflow` (`MA-W2-R6-01`)."""
        origin_app = str(origin_application or "langflow").strip()[:16] or "langflow"
        threads_by_id: dict[int, dict[str, Any]] = {}

        for tid, entry in self._cortex_threads_server_mem.items():
            meta = dict(entry.get("metadata") or {})
            if meta.get("thread_id") is not None:
                threads_by_id[int(tid)] = meta

        for (_fid, _sid, _afqn, _rl), ptr in self._cortex_agent_threads_mem.items():
            tid = int(ptr.get("thread_id") or 0)
            if tid > 0 and tid not in threads_by_id:
                threads_by_id[tid] = {
                    "thread_id": tid,
                    "thread_name": str(ptr.get("thread_name") or f"lf:{_fid[:8]}:{_sid[:16]}:{_afqn.split('.')[-1]}"),
                    "origin_application": origin_app,
                    "created_on": str(ptr.get("updated_at") or ""),
                    "updated_on": str(ptr.get("updated_at") or ""),
                }

        remote_list: list[Any] = []
        if callable(self._cortex_rest_transport_fn):
            raw = self._cortex_rest_transport_fn(
                "GET",
                f"/api/v2/cortex/threads?origin_application={origin_app}",
                None,
            )
            if isinstance(raw, list):
                remote_list = raw
            elif isinstance(raw, dict) and isinstance(raw.get("threads"), list):
                remote_list = raw["threads"]
            elif isinstance(raw, dict) and isinstance(raw.get("data"), list):
                remote_list = raw["data"]
        elif self._has_spcs_token() and httpx is not None:
            host = os.environ["SNOWFLAKE_HOST"]
            try:
                with httpx.Client(timeout=20.0, verify=True) as client:
                    resp = client.get(
                        f"https://{host}/api/v2/cortex/threads",
                        params={"origin_application": origin_app},
                        headers=self._get_spcs_headers(_sf_identity.current_request_mode()),
                    )
                    if resp.status_code < 400 and resp.content:
                        body = resp.json()
                        if isinstance(body, list):
                            remote_list = body
                        elif isinstance(body, dict):
                            remote_list = body.get("threads") or body.get("data") or []
            except Exception:  # noqa: BLE001
                pass

        for item in remote_list:
            if not isinstance(item, dict):
                continue
            raw_tid = item.get("thread_id") or item.get("id")
            if raw_tid is None:
                continue
            try:
                tid = int(raw_tid)
            except (TypeError, ValueError):
                continue
            meta = {
                "thread_id": tid,
                "thread_name": str(item.get("thread_name") or item.get("name") or f"Thread #{tid}"),
                "origin_application": str(item.get("origin_application") or origin_app),
                "created_on": str(item.get("created_on") or item.get("created_at") or ""),
                "updated_on": str(item.get("updated_on") or item.get("updated_at") or item.get("created_on") or ""),
            }
            threads_by_id[tid] = meta
            self._cortex_threads_server_mem.setdefault(tid, {"metadata": meta, "messages": []})["metadata"] = meta

        results = list(threads_by_id.values())
        if flow_id:
            norm_fid = self._normalize_flow_id(flow_id) or str(flow_id).strip()
            fid_prefix = f"lf:{norm_fid[:8]}:"
            filtered = [t for t in results if str(t.get("thread_name") or "").startswith(fid_prefix)]
            if filtered:
                results = filtered
        if agent_fqn:
            short_ag = str(agent_fqn).strip().split(".")[-1].upper()
            filtered_ag = [
                t
                for t in results
                if short_ag in str(t.get("thread_name") or "").upper()
            ]
            if filtered_ag:
                results = filtered_ag

        results.sort(key=lambda x: (str(x.get("updated_on") or ""), int(x.get("thread_id") or 0)), reverse=True)
        return results

    def describe_cortex_thread(
        self,
        thread_id: int | str,
        *,
        page_size: int = 20,
        last_message_id: int | None = None,
    ) -> dict[str, Any]:
        """Describe a Snowflake Cortex Thread via `GET /api/v2/cortex/threads/{thread_id}?message_type=conversation&page_size=20` (`MA-W2-R6-01`).

        Note: Snowflake returns `messages` in descending order of creation (newest first).
        """
        raw_str = str(thread_id).strip()
        m_num = re.search(r"(\d+)", raw_str)
        tid = int(m_num.group(1)) if m_num else 0
        bounded_size = max(1, min(int(page_size or 20), 100))
        query_path = f"/api/v2/cortex/threads/{tid}?message_type=conversation&page_size={bounded_size}"
        if last_message_id is not None:
            query_path += f"&last_message_id={int(last_message_id)}"

        if callable(self._cortex_rest_transport_fn):
            raw = self._cortex_rest_transport_fn("GET", query_path, None)
            if isinstance(raw, dict):
                meta = dict(raw.get("metadata") or {"thread_id": tid})
                msgs = list(raw.get("messages") or [])
                self._cortex_threads_server_mem[tid] = {"metadata": meta, "messages": msgs}
                return {"metadata": meta, "messages": msgs[:bounded_size]}

        if self._has_spcs_token() and httpx is not None and tid > 0:
            host = os.environ["SNOWFLAKE_HOST"]
            params: dict[str, Any] = {"message_type": "conversation", "page_size": bounded_size}
            if last_message_id is not None:
                params["last_message_id"] = int(last_message_id)
            try:
                with httpx.Client(timeout=20.0, verify=True) as client:
                    resp = client.get(
                        f"https://{host}/api/v2/cortex/threads/{tid}",
                        params=params,
                        headers=self._get_spcs_headers(_sf_identity.current_request_mode()),
                    )
                    if resp.status_code < 400 and resp.content:
                        body = resp.json()
                        if isinstance(body, dict):
                            meta = dict(body.get("metadata") or {"thread_id": tid})
                            msgs = list(body.get("messages") or [])
                            self._cortex_threads_server_mem[tid] = {"metadata": meta, "messages": msgs}
                            return {"metadata": meta, "messages": msgs[:bounded_size]}
            except Exception:  # noqa: BLE001
                pass

        mem_entry = self._cortex_threads_server_mem.get(tid) or {
            "metadata": {"thread_id": tid, "thread_name": f"Thread #{tid}", "origin_application": "langflow"},
            "messages": [],
        }
        msgs = list(mem_entry.get("messages") or [])
        if last_message_id is not None:
            msgs = [m for m in msgs if int(m.get("message_id") or 0) < int(last_message_id)]
        return {
            "metadata": dict(mem_entry.get("metadata") or {"thread_id": tid}),
            "messages": msgs[:bounded_size],
        }

    def resolve_last_assistant_message_id(self, thread_id: int | str, *, page_size: int = 20) -> int:
        """Recover the last successful `assistant` `message_id` from Snowflake Threads API (`MA-W2-R6-01`).

        Calls `GET /api/v2/cortex/threads/{thread_id}?message_type=conversation&page_size=20` and takes
        the FIRST element of `messages` with `role == "assistant"` (since Snowflake returns messages
        in descending order of creation, newest first). Returns `0` if the thread has no assistant reply yet.
        """
        described = self.describe_cortex_thread(thread_id, page_size=page_size)
        for msg in described.get("messages") or []:
            if not isinstance(msg, dict):
                continue
            if str(msg.get("role") or "").strip().lower() == "assistant":
                raw_mid = msg.get("message_id")
                if raw_mid is not None:
                    try:
                        return int(raw_mid)
                    except (TypeError, ValueError):
                        continue
        return 0

    def sync_cortex_thread_to_playground_messages(
        self,
        *,
        thread_id: int | str,
        flow_id: str | None = None,
        session_id: str | None = None,
        agent_name: str | None = None,
    ) -> int:
        """Synchronize recovered Snowflake Cortex Thread messages into Langflow's `message` table for Playground visibility (`MA-W2-R6-01`)."""
        described = self.describe_cortex_thread(thread_id, page_size=20)
        messages_desc = list(described.get("messages") or [])
        if not messages_desc:
            return 0
        norm_fid = self._normalize_flow_id(flow_id) or str(flow_id or self.active_flow_id or "default")
        norm_sid = str(session_id or norm_fid or "default")
        short_agent = str(agent_name or "Cortex Agent").split(".")[-1]
        chronological = list(reversed(messages_desc))
        synced_count = 0
        try:
            db_path = self._resolve_writable_sqlite_db_path()
            conn = _open_sqlite_conn(db_path, readonly=False, timeout=15.0)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if "message" not in tables:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS message (
                        id TEXT PRIMARY KEY,
                        flow_id TEXT,
                        session_id TEXT,
                        sender TEXT,
                        sender_name TEXT,
                        text TEXT,
                        timestamp TEXT,
                        properties TEXT,
                        files TEXT,
                        error INTEGER DEFAULT 0,
                        edit INTEGER DEFAULT 0,
                        category TEXT DEFAULT 'message'
                    )
                    """
                )
            for msg in chronological:
                if not isinstance(msg, dict):
                    continue
                mid = msg.get("message_id")
                role = str(msg.get("role") or "user").lower()
                raw_body = msg.get("message_payload") or msg.get("content") or msg.get("text") or ""
                text_val = ""
                if isinstance(raw_body, str):
                    if raw_body.strip().startswith(("{", "[")):
                        try:
                            parsed_b = json.loads(raw_body)
                            if isinstance(parsed_b, dict) and isinstance(parsed_b.get("content"), list):
                                text_val = "\n".join(
                                    str(b.get("text") or "") for b in parsed_b["content"] if isinstance(b, dict) and b.get("text")
                                )
                            elif isinstance(parsed_b, list):
                                text_val = "\n".join(
                                    str(b.get("text") or "") for b in parsed_b if isinstance(b, dict) and b.get("text")
                                )
                        except Exception:  # noqa: BLE001
                            text_val = raw_body
                    else:
                        text_val = raw_body
                elif isinstance(raw_body, list):
                    text_val = "\n".join(
                        str(b.get("text") or "") for b in raw_body if isinstance(b, dict) and b.get("text")
                    )
                elif isinstance(raw_body, dict):
                    text_val = str(raw_body.get("text") or json.dumps(raw_body))
                det_msg_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, f"cortex-thread-{thread_id}-msg-{mid}-{role}"))
                existing = cur.execute("SELECT id FROM message WHERE id = ?", (det_msg_uuid,)).fetchone()
                if existing is not None:
                    continue
                sender = "Machine" if role == "assistant" else "User"
                sender_name = f"❄️ {short_agent}" if role == "assistant" else "User"
                ts = str(msg.get("created_on") or datetime.now(timezone.utc).isoformat())
                props = json.dumps(
                    {
                        "thread_id": int(str(thread_id)) if str(thread_id).isdigit() else thread_id,
                        "message_id": mid,
                        "parent_id": msg.get("parent_id"),
                        "source": "snowflake_cortex_threads_api",
                    }
                )
                cur.execute(
                    """
                    INSERT INTO message (id, flow_id, session_id, sender, sender_name, text, timestamp, properties)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        det_msg_uuid,
                        norm_fid.replace("-", "") if len(norm_fid) == 36 else norm_fid,
                        norm_sid,
                        sender,
                        sender_name,
                        text_val,
                        ts,
                        props,
                    ),
                )
                synced_count += 1
            conn.commit()
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        return synced_count

    @staticmethod
    def parse_cortex_agent_sse_or_json(raw_payload: Any) -> dict[str, Any]:
        """Parse Snowflake Cortex Agent `:run` SSE stream (`event: metadata`, `event: response.*`) or JSON payload (`MA-W2-R6-01`)."""
        result: dict[str, Any] = {
            "status": "SUCCESS",
            "text": "",
            "thinking": "",
            "user_message_id": None,
            "assistant_message_id": None,
            "tool_uses": [],
            "tool_results": [],
            "sql_queries": [],
            "citations": [],
            "charts": [],
            "tables": [],
            "error_message": None,
        }
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8", errors="replace")

        if isinstance(raw_payload, dict):
            if raw_payload.get("status") == "ERROR" or raw_payload.get("error"):
                result["status"] = "ERROR"
                result["error_message"] = str(
                    raw_payload.get("error_message")
                    or (raw_payload["error"].get("message") if isinstance(raw_payload.get("error"), dict) else raw_payload.get("error"))
                    or raw_payload.get("message")
                    or "Cortex Agent execution failed"
                )
                return result
            if raw_payload.get("user_message_id") is not None:
                result["user_message_id"] = int(raw_payload["user_message_id"])
            if raw_payload.get("assistant_message_id") is not None:
                result["assistant_message_id"] = int(raw_payload["assistant_message_id"])
            meta_obj = raw_payload.get("metadata")
            if isinstance(meta_obj, dict):
                if meta_obj.get("role") == "user" and meta_obj.get("message_id") is not None:
                    result["user_message_id"] = int(meta_obj["message_id"])
                elif meta_obj.get("role") == "assistant" and meta_obj.get("message_id") is not None:
                    result["assistant_message_id"] = int(meta_obj["message_id"])
            if isinstance(raw_payload.get("text"), str):
                result["text"] = raw_payload["text"]
            if isinstance(raw_payload.get("thinking"), str):
                result["thinking"] = raw_payload["thinking"]
            for k in ("tool_uses", "tool_results", "sql_queries", "citations", "charts", "tables"):
                if isinstance(raw_payload.get(k), list):
                    result[k].extend(raw_payload[k])
            if isinstance(raw_payload.get("content"), list):
                LiveSnowflakeBridge._extract_cortex_content_blocks(raw_payload["content"], result)
            return result

        if not isinstance(raw_payload, str):
            return result

        text_deltas: list[str] = []
        thinking_deltas: list[str] = []
        final_text_blocks: list[str] = []
        current_event = ""

        for raw_line in raw_payload.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("event:"):
                current_event = line[len("event:") :].strip()
                continue
            if not line.startswith("data:"):
                continue
            data_str = line[len("data:") :].strip()
            if not data_str or data_str == "[DONE]":
                continue
            try:
                data_obj = json.loads(data_str)
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(data_obj, dict):
                continue

            if current_event == "metadata" or "metadata" in data_obj:
                meta = data_obj.get("metadata") if isinstance(data_obj.get("metadata"), dict) else data_obj
                role = str(meta.get("role") or "").strip().lower()
                mid = meta.get("message_id")
                if mid is not None:
                    try:
                        if role == "user":
                            result["user_message_id"] = int(mid)
                        elif role == "assistant":
                            result["assistant_message_id"] = int(mid)
                    except (TypeError, ValueError):
                        pass

            if current_event == "error":
                result["status"] = "ERROR"
                result["error_message"] = str(data_obj.get("message") or data_obj.get("error") or "SSE error")
                continue

            if current_event == "response.text.delta":
                if isinstance(data_obj.get("text"), str):
                    text_deltas.append(data_obj["text"])
            elif current_event == "response.text":
                if isinstance(data_obj.get("text"), str):
                    final_text_blocks.append(data_obj["text"])
                if isinstance(data_obj.get("annotations"), list):
                    result["citations"].extend(data_obj["annotations"])
            elif current_event == "response.thinking.delta":
                if isinstance(data_obj.get("text"), str):
                    thinking_deltas.append(data_obj["text"])
            elif current_event == "response.thinking":
                if isinstance(data_obj.get("text"), str):
                    result["thinking"] = data_obj["text"]
            elif current_event == "response.tool_use":
                result["tool_uses"].append(data_obj)
            elif current_event == "response.tool_result":
                result["tool_results"].append(data_obj)
                LiveSnowflakeBridge._extract_tool_result_artifacts(data_obj, result)
            elif current_event == "response.chart":
                chart_spec = data_obj.get("chart_spec") or data_obj
                if isinstance(chart_spec, str):
                    try:
                        chart_spec = json.loads(chart_spec)
                    except Exception:  # noqa: BLE001
                        pass
                result["charts"].append(chart_spec)
            elif current_event == "response":
                if isinstance(data_obj.get("content"), list):
                    LiveSnowflakeBridge._extract_cortex_content_blocks(data_obj["content"], result)

        if not result["text"]:
            if final_text_blocks:
                result["text"] = "\n".join(final_text_blocks)
            elif text_deltas:
                result["text"] = "".join(text_deltas)
        if not result["thinking"] and thinking_deltas:
            result["thinking"] = "".join(thinking_deltas)
        return result

    @staticmethod
    def _extract_tool_result_artifacts(tool_res: dict[str, Any], out: dict[str, Any]) -> None:
        """Extract SQL queries, result sets, and search citations from a Cortex Agent `tool_result` block."""
        for item in tool_res.get("content") or []:
            if not isinstance(item, dict):
                continue
            j_obj = item.get("json")
            if not isinstance(j_obj, dict):
                continue
            if isinstance(j_obj.get("sql"), str) and j_obj["sql"] not in out["sql_queries"]:
                out["sql_queries"].append(j_obj["sql"])
            if isinstance(j_obj.get("result_set"), dict):
                out["tables"].append(j_obj["result_set"])
            if isinstance(j_obj.get("search_results"), list):
                out["citations"].extend(j_obj["search_results"])

    @staticmethod
    def _extract_cortex_content_blocks(content_list: list[Any], out: dict[str, Any]) -> None:
        """Extract text, thinking, tool_use, tool_result, and chart blocks from final `response` content."""
        texts: list[str] = []
        for blk in content_list:
            if not isinstance(blk, dict):
                continue
            b_type = str(blk.get("type") or "")
            if b_type == "text" and isinstance(blk.get("text"), str):
                texts.append(blk["text"])
                if isinstance(blk.get("annotations"), list):
                    out["citations"].extend(blk["annotations"])
            elif b_type == "thinking" and isinstance(blk.get("thinking"), dict):
                if isinstance(blk["thinking"].get("text"), str):
                    out["thinking"] = blk["thinking"]["text"]
            elif b_type == "tool_use" and isinstance(blk.get("tool_use"), dict):
                out["tool_uses"].append(blk["tool_use"])
            elif b_type == "tool_result" and isinstance(blk.get("tool_result"), dict):
                out["tool_results"].append(blk["tool_result"])
                LiveSnowflakeBridge._extract_tool_result_artifacts(blk["tool_result"], out)
            elif b_type == "chart" and isinstance(blk.get("chart"), dict):
                c_spec = blk["chart"].get("chart_spec") or blk["chart"]
                if isinstance(c_spec, str):
                    try:
                        c_spec = json.loads(c_spec)
                    except Exception:  # noqa: BLE001
                        pass
                out["charts"].append(c_spec)
        if texts and not out["text"]:
            out["text"] = "\n".join(texts)

    def invoke_cortex_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        user_message: str,
        flow_id: str | None = None,
        session_id: str | None = None,
        resume_thread: bool = True,
        thread_continuity_mode: str = "Resume Latest Thread of this Agent",
        conversation_thread: str | None = None,
        thread_id: int | str | None = None,
        parent_message_id: int | str | None = None,
        tool_choice: str = "auto",
        role: str | None = None,
    ) -> dict[str, Any]:
        """Invoke a Snowflake Cortex Agent via `POST /api/v2/databases/{db}/schemas/{schema}/agents/{name}:run` with 2-level Threads API continuity (`MA-W2-R6-01`)."""
        effective_role = (role or self.active_role or "").strip().upper()
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or self.schema or "CORE").strip().upper()
        raw_agent = (agent_name or "EXAMPLE_AGENT").strip()
        if "." in raw_agent:
            parts = [p.strip().upper() for p in raw_agent.split(".") if p.strip()]
            if len(parts) == 3:
                db, sch, short_agent = parts[0], parts[1], parts[2]
            else:
                short_agent = parts[-1]
        else:
            short_agent = raw_agent.upper()
        agent_fqn = f"{db}.{sch}.{short_agent}"
        prompt = (user_message or "Summarize active flow metrics").strip()
        norm_fid = (
            self._normalize_flow_id(flow_id)
            or self._normalize_flow_id(CURRENT_SNOWFLAKE_FLOW_ID.get())
            or self._normalize_flow_id(self.active_flow_id)
            or "default"
        )
        eff_session_id = str(session_id or norm_fid or "default").strip() or "default"
        rest_endpoint = f"/api/v2/databases/{db}/schemas/{sch}/agents/{short_agent}:run"

        # BUG-A2A-03: real check under the effective role (+ flow context), no hard-coded role denylist.
        try:
            denied, _desc = self._cortex_agent_access(db, sch, short_agent, effective_role, flow_id)
        except _sql_safety.SqlSafetyError as exc:
            denied = {"status_code": 400, "error_code": "INVALID_IDENTIFIER", "message": str(exc)}
        if denied is not None:
            self.record_audit_event(
                action="INVOKE_CORTEX_AGENT",
                outcome="DENIED",
                error_code=denied["error_code"],
                target_object=agent_fqn,
                flow_id=norm_fid,
                details={"role": effective_role, "agent_fqn": agent_fqn},
            )
            return {
                "ok": False,
                "status": "ERROR",
                "status_code": denied["status_code"],
                "error_code": denied["error_code"],
                "message": denied["message"],
                "agent_fqn": agent_fqn,
                "active_role": effective_role,
                "context_epoch": self.context_epoch,
            }

        continuity_mode = str(thread_continuity_mode or "Resume Latest Thread of this Agent").strip()
        conv_sel = str(conversation_thread or "").strip()
        is_stateful = bool(resume_thread) and not continuity_mode.lower().startswith("stateless")

        resolved_tid: int | None = None
        resolved_parent_mid: int | None = None
        resolved_thread_name: str | None = None

        if is_stateful:
            # BUG-CA-11: the "Active Snowflake Thread" dropdown (default "+ Start Fresh Thread") only applies in
            # "Select Specific Thread / Start Fresh Thread" mode; Resume Latest / One Thread per Session must
            # not open a new thread on every turn. An explicit thread_id ("new" or digits) always wins.
            select_mode = continuity_mode.lower().startswith("select specific thread")
            force_fresh = str(thread_id or "").strip().lower() in {"new", "new-thread", "fresh"} or (
                select_mode
                and (
                    conv_sel.startswith("+ Start Fresh Thread")
                    or conv_sel.lower() in {"start fresh thread", "new", "new-thread", "fresh"}
                )
            )
            explicit_tid: int | None = None
            if not force_fresh:
                if thread_id is not None and str(thread_id).strip():
                    m_t = re.search(r"(\d+)", str(thread_id).strip())
                    if m_t:
                        explicit_tid = int(m_t.group(1))
                if explicit_tid is None and select_mode and conv_sel.startswith("Thread #"):
                    m_c = re.search(r"Thread\s*#(\d+)", conv_sel)
                    if m_c:
                        explicit_tid = int(m_c.group(1))

            if force_fresh:
                created = self.create_cortex_thread(
                    flow_id=norm_fid,
                    session_id=eff_session_id,
                    agent_name=short_agent,
                )
                resolved_tid = int(created["thread_id"])
                resolved_thread_name = str(created.get("thread_name") or "")
                resolved_parent_mid = 0
            elif explicit_tid is not None:
                resolved_tid = explicit_tid
                if parent_message_id is not None and str(parent_message_id).strip().isdigit():
                    resolved_parent_mid = int(str(parent_message_id).strip())
                else:
                    ptr = self._get_cortex_agent_thread_pointer(
                        flow_id=norm_fid,
                        session_id=eff_session_id,
                        agent_fqn=agent_fqn,
                        active_role=effective_role,
                        latest_for_agent=True,
                    )
                    if ptr and int(ptr.get("thread_id") or 0) == resolved_tid and int(ptr.get("last_assistant_message_id") or 0) > 0:
                        resolved_parent_mid = int(ptr["last_assistant_message_id"])
                        resolved_thread_name = str(ptr.get("thread_name") or "")
                    else:
                        resolved_parent_mid = self.resolve_last_assistant_message_id(resolved_tid)
            elif continuity_mode.startswith("One Thread per Playground Session"):
                ptr = self._get_cortex_agent_thread_pointer(
                    flow_id=norm_fid,
                    session_id=eff_session_id,
                    agent_fqn=agent_fqn,
                    active_role=effective_role,
                    latest_for_agent=False,
                )
                if ptr is not None:
                    resolved_tid = int(ptr["thread_id"])
                    resolved_thread_name = str(ptr.get("thread_name") or "")
                    cached_amid = int(ptr.get("last_assistant_message_id") or 0)
                    resolved_parent_mid = cached_amid if cached_amid > 0 else self.resolve_last_assistant_message_id(resolved_tid)
                else:
                    expected_prefix = f"lf:{norm_fid[:8]}:{eff_session_id[:16]}:{short_agent}".upper()
                    remote_threads = self.list_cortex_threads(origin_application="langflow")
                    matched_remote = [
                        t
                        for t in remote_threads
                        if str(t.get("thread_name") or "").upper().startswith(expected_prefix)
                    ]
                    if matched_remote:
                        resolved_tid = int(matched_remote[0]["thread_id"])
                        resolved_thread_name = str(matched_remote[0].get("thread_name") or "")
                        resolved_parent_mid = self.resolve_last_assistant_message_id(resolved_tid)
                    else:
                        created = self.create_cortex_thread(
                            flow_id=norm_fid,
                            session_id=eff_session_id,
                            agent_name=short_agent,
                        )
                        resolved_tid = int(created["thread_id"])
                        resolved_thread_name = str(created.get("thread_name") or "")
                        resolved_parent_mid = 0
            else:
                # Default: "Resume Latest Thread of this Agent"
                ptr = self._get_cortex_agent_thread_pointer(
                    flow_id=norm_fid,
                    session_id=eff_session_id,
                    agent_fqn=agent_fqn,
                    active_role=effective_role,
                    latest_for_agent=True,
                )
                if ptr is not None:
                    resolved_tid = int(ptr["thread_id"])
                    resolved_thread_name = str(ptr.get("thread_name") or "")
                    cached_amid = int(ptr.get("last_assistant_message_id") or 0)
                    resolved_parent_mid = cached_amid if cached_amid > 0 else self.resolve_last_assistant_message_id(resolved_tid)
                else:
                    remote_threads = self.list_cortex_threads(
                        origin_application="langflow",
                        flow_id=norm_fid,
                        agent_fqn=agent_fqn,
                    )
                    fid_prefix = f"lf:{norm_fid[:8]}:".upper()
                    matched_remote = [
                        t
                        for t in remote_threads
                        if str(t.get("thread_name") or "").upper().startswith(fid_prefix)
                        and short_agent in str(t.get("thread_name") or "").upper()
                    ]
                    if matched_remote:
                        resolved_tid = int(matched_remote[0]["thread_id"])
                        resolved_thread_name = str(matched_remote[0].get("thread_name") or "")
                        resolved_parent_mid = self.resolve_last_assistant_message_id(resolved_tid)
                    else:
                        created = self.create_cortex_thread(
                            flow_id=norm_fid,
                            session_id=eff_session_id,
                            agent_name=short_agent,
                        )
                        resolved_tid = int(created["thread_id"])
                        resolved_thread_name = str(created.get("thread_name") or "")
                        resolved_parent_mid = 0

            if parent_message_id is not None and str(parent_message_id).strip().isdigit():
                resolved_parent_mid = int(str(parent_message_id).strip())

        request_payload: dict[str, Any] = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}],
                }
            ],
        }
        tc = str(tool_choice or "auto").strip().lower()
        if tc in ("auto", "required", "none"):
            request_payload["tool_choice"] = {"type": tc}
        if is_stateful and resolved_tid is not None:
            request_payload["thread_id"] = int(resolved_tid)
            request_payload["parent_message_id"] = int(resolved_parent_mid or 0)

        parsed_run: dict[str, Any] | None = None
        if callable(self._cortex_rest_transport_fn):
            raw_resp = self._cortex_rest_transport_fn("POST", rest_endpoint, request_payload)
            parsed_run = self.parse_cortex_agent_sse_or_json(raw_resp)
        elif self._has_spcs_token() and httpx is not None:
            host = os.environ["SNOWFLAKE_HOST"]
            url = f"https://{host}{rest_endpoint}"
            headers = self._get_spcs_headers(_sf_identity.current_request_mode())
            headers["Accept"] = "text/event-stream, application/json"
            try:
                with httpx.Client(timeout=90.0, verify=True) as client:
                    resp = client.post(url, headers=headers, json=request_payload)
                    if resp.status_code < 400 and resp.content:
                        ctype = str(resp.headers.get("content-type") or "").lower()
                        if "text/event-stream" in ctype or b"event:" in resp.content[:256]:
                            parsed_run = self.parse_cortex_agent_sse_or_json(resp.text)
                        else:
                            parsed_run = self.parse_cortex_agent_sse_or_json(resp.json())
                    elif resp.status_code >= 400:
                        parsed_run = {
                            "status": "ERROR",
                            "error_message": f"HTTP {resp.status_code}: {resp.text[:400]}",
                        }
            except Exception as exc:  # noqa: BLE001
                parsed_run = {"status": "ERROR", "error_message": str(exc)}

        if parsed_run is None:
            self._cortex_msg_seq += 1
            u_mid = self._cortex_msg_seq
            self._cortex_msg_seq += 1
            a_mid = self._cortex_msg_seq
            parsed_run = {
                "status": "SUCCESS",
                "text": f"Cortex Agent {agent_fqn} response to: {prompt}",
                "thinking": f"Evaluated request under role {effective_role} on {agent_fqn}.",
                "user_message_id": u_mid,
                "assistant_message_id": a_mid,
                "tool_uses": [],
                "tool_results": [],
                "sql_queries": [],
                "citations": [],
                "charts": [],
                "tables": [],
                "error_message": None,
            }

        if parsed_run.get("status") == "ERROR":
            # Do NOT advance last_assistant_message_id on error
            return {
                "ok": False,
                "status": "ERROR",
                "status_code": 502,
                "error_code": ErrorCode.TECHNICAL_EXECUTION_ERROR.value,
                "message": str(parsed_run.get("error_message") or "Cortex Agent execution failed"),
                "database": db,
                "schema_name": sch,
                "cortex_agent": short_agent,
                "agent_fqn": agent_fqn,
                "rest_endpoint": rest_endpoint,
                "thread_id": resolved_tid,
                "parent_message_id": resolved_parent_mid,
                "active_role": effective_role,
                "context_epoch": self.context_epoch,
            }

        if parsed_run.get("assistant_message_id") is None:
            self._cortex_msg_seq += 2
            parsed_run["user_message_id"] = self._cortex_msg_seq - 1
            parsed_run["assistant_message_id"] = self._cortex_msg_seq
        elif parsed_run.get("user_message_id") is None:
            parsed_run["user_message_id"] = max(1, int(parsed_run["assistant_message_id"]) - 1)

        user_mid = int(parsed_run["user_message_id"])
        assistant_mid = int(parsed_run["assistant_message_id"])
        reply_text = str(parsed_run.get("text") or f"Cortex Agent {agent_fqn} response to: {prompt}")
        now_iso = datetime.now(timezone.utc).isoformat()

        if is_stateful and resolved_tid is not None:
            t_entry = self._cortex_threads_server_mem.setdefault(
                int(resolved_tid),
                {
                    "metadata": {
                        "thread_id": int(resolved_tid),
                        "thread_name": resolved_thread_name or f"lf:{norm_fid[:8]}:{eff_session_id[:16]}:{short_agent}",
                        "origin_application": "langflow",
                        "created_on": now_iso,
                        "updated_on": now_iso,
                    },
                    "messages": [],
                },
            )
            t_entry["metadata"]["updated_on"] = now_iso
            # Insert user and assistant messages newest-first (assistant at index 0, user at index 1)
            existing_mids = {int(m.get("message_id") or 0) for m in t_entry["messages"] if isinstance(m, dict)}
            if user_mid not in existing_mids:
                t_entry["messages"].insert(
                    0,
                    {
                        "message_id": user_mid,
                        "parent_id": int(resolved_parent_mid or 0),
                        "created_on": now_iso,
                        "role": "user",
                        "message_payload": json.dumps({"content": [{"type": "text", "text": prompt}]}),
                        "raw_message": prompt,
                    },
                )
            if assistant_mid not in existing_mids:
                t_entry["messages"].insert(
                    0,
                    {
                        "message_id": assistant_mid,
                        "parent_id": user_mid,
                        "created_on": now_iso,
                        "role": "assistant",
                        "message_payload": json.dumps({"content": [{"type": "text", "text": reply_text}]}),
                        "raw_message": reply_text,
                    },
                )

            self._save_cortex_agent_thread_pointer(
                flow_id=norm_fid,
                session_id=eff_session_id,
                agent_fqn=agent_fqn,
                active_role=effective_role,
                thread_id=int(resolved_tid),
                thread_name=t_entry["metadata"].get("thread_name"),
                last_user_message_id=user_mid,
                last_assistant_message_id=assistant_mid,
            )
            self.sync_cortex_thread_to_playground_messages(
                thread_id=int(resolved_tid),
                flow_id=norm_fid,
                session_id=eff_session_id,
                agent_name=short_agent,
            )

        # BUG-LLMOP-A4-01: real session / Cortex thread ids on the flow run record.
        _telemetry.annotate_current_run(
            session_id=eff_session_id,
            cortex_thread_id=resolved_tid if is_stateful else None,
            cortex_parent_message_id=assistant_mid if is_stateful else None,
        )
        thread_indicator = (
            f"Thread #{resolved_tid} · Parent Msg #{int(resolved_parent_mid or 0)} -> New Msg #{assistant_mid}"
            if (is_stateful and resolved_tid is not None)
            else "Stateless (Single Turn)"
        )

        return {
            "ok": True,
            "status": "SUCCESS",
            "database": db,
            "schema_name": sch,
            "cortex_agent": short_agent,
            "agent_fqn": agent_fqn,
            "discovery_sql": _show_agents_sql(db, sch),
            "rest_endpoint": rest_endpoint,
            "active_role": effective_role,
            "context_epoch": self.context_epoch,
            "flow_id": norm_fid,
            "session_id": eff_session_id,
            "resume_thread": is_stateful,
            "thread_continuity_mode": continuity_mode,
            "thread_id": int(resolved_tid) if (is_stateful and resolved_tid is not None) else None,
            "thread_name": resolved_thread_name,
            "parent_message_id": int(resolved_parent_mid or 0) if (is_stateful and resolved_tid is not None) else None,
            "user_message_id": user_mid,
            "assistant_message_id": assistant_mid,
            "next_parent_message_id": assistant_mid if (is_stateful and resolved_tid is not None) else None,
            "parent_message_id_for_next_turn": assistant_mid if (is_stateful and resolved_tid is not None) else None,
            "thread_indicator": thread_indicator,
            "user_message": prompt,
            "prompt": prompt,
            "text": reply_text,
            "thinking": parsed_run.get("thinking") or "",
            "tool_uses": parsed_run.get("tool_uses") or [],
            "tool_results": parsed_run.get("tool_results") or [],
            "sql_queries": parsed_run.get("sql_queries") or [],
            "citations": parsed_run.get("citations") or [],
            "charts": parsed_run.get("charts") or [],
            "tables": parsed_run.get("tables") or [],
            "request_payload": request_payload,
        }

    _DESCRIBE_AGENT_TTL_SECONDS = 120.0

    # Placeholder flow ids used for session/thread keys when a request has no flow; never a flow context.
    _PSEUDO_FLOW_IDS = frozenset({"default", "a2a-bridge"})

    def _cortex_agent_access(
        self, db: str, sch: str, agent: str, role: str, flow_id: str | None
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """BUG-A2A-03: (denial, describe_result) for a Cortex Agent under the effective role.

        Denies with 403 when ``flow_id`` is set and the agent is not in that flow's `[ ❄️ Context ]`;
        then runs a real ``DESCRIBE AGENT`` under ``role``: an authorization failure is 403, a missing
        agent (or any other failure / empty result) is 404. Nothing is ever synthesized.
        """
        agent_fqn = f"{db}.{sch}.{agent}"
        fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        if fid and fid.lower() in self._PSEUDO_FLOW_IDS:
            fid = None  # BUG-A2A-03: no real flow (A2A without a flow) => RBAC only, no context check
        if fid:
            ctx = self.get_flow_context(fid, create=False)
            ref = _flow_guard.ObjectRef(db, sch, agent, "routine")
            if not _flow_guard.is_ref_in_context(ref, ctx):
                return (
                    {
                        "ok": False,
                        "status_code": 403,
                        "error_code": _flow_guard.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT,
                        "message": f"Cortex Agent {agent_fqn} is not in the [ ❄️ Context ] of flow {fid}.",
                    },
                    {},
                )
        # Successful DESCRIBE results are cached per (role, context_epoch, fqn); failures never are.
        cache = self.__dict__.setdefault("_describe_agent_cache", {})
        cache_key = ((role or "").upper(), self.context_epoch, agent_fqn)
        hit = cache.get(cache_key)
        if hit and hit[0] > time.monotonic():
            return None, hit[1]
        desc = self.execute_sql(
            f"DESCRIBE AGENT {_sql_safety.quote_fqn(agent_fqn, 'agent', min_parts=3, max_parts=3)}",
            role=role or None,
            timeout=15,
        )
        if desc.get("ok") and desc.get("rows"):
            cache[cache_key] = (time.monotonic() + self._DESCRIBE_AGENT_TTL_SECONDS, desc)
            return None, desc
        authz = desc.get("error_code") == ErrorCode.AUTHZ_ROLE_DENIED.value or int(desc.get("status_code") or 0) == 403
        return (
            {
                "ok": False,
                "status_code": 403 if authz else 404,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value if authz else ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                "message": (
                    f"Active role ({role or 'default'}) is not authorized to access Cortex Agent {agent_fqn}."
                    if authz
                    else f"Cortex Agent {agent_fqn} does not exist or is not authorized for the active role."
                ),
            },
            desc,
        )

    def get_cortex_agent_a2a_card(
        self,
        database: str,
        schema: str,
        agent_name: str,
        *,
        base_url: str = "",
        flow_id: str | None = None,
        role: str | None = None,
    ) -> dict[str, Any]:
        """Generate A2A v0.3.0 Agent Card for a Snowflake Cortex Agent via `DESCRIBE AGENT <DB>.<SCHEMA>.<NAME>` (`MA-W2-R6-05`)."""
        effective_role = (role or self.active_role or "").strip().upper()
        # BUG-A2A-04: path parameters are validated before any SQL is composed.
        try:
            db = _sql_safety.validate_identifier(database or self.database or "SNOWFLAKE_LANGFLOW", "database")
            sch = _sql_safety.validate_identifier(schema or self.schema or "CORE", "schema")
            short_agent = _sql_safety.validate_identifier(
                (agent_name or "EXAMPLE_AGENT").strip().split(".")[-1], "agent_name"
            )
        except _sql_safety.SqlSafetyError as exc:
            return {
                "ok": False,
                "status_code": 400,
                "error_code": "INVALID_IDENTIFIER",
                "message": str(exc),
            }
        agent_fqn = f"{db}.{sch}.{short_agent}"

        # BUG-A2A-03: real authorization + existence check; never a synthesized card.
        denied, desc_res = self._cortex_agent_access(db, sch, short_agent, effective_role, flow_id)
        if denied is not None:
            return denied
        description = (
            f"Snowflake Cortex Agent {agent_fqn} exposed via Snowflake Langflow A2A v0.3.0 Server Bridge "
            f"(governed by CURRENT_ROLE() = {effective_role})."
        )
        skills: list[dict[str, Any]] = [
            {
                "id": f"cortex_agent_{db.lower()}_{sch.lower()}_{short_agent.lower()}",
                "name": f"Cortex Agent {short_agent}",
                "description": f"Execute multi-turn reasoning, Cortex Analyst SQL, and Cortex Search on {agent_fqn}.",
                "tags": ["snowflake", "cortex-agent", db.lower(), sch.lower()],
                "examples": [f"Ask {short_agent} to summarize key metrics"],
                "inputModes": ["text/plain", "application/json"],
                "outputModes": ["text/plain", "application/json"],
            }
        ]
        if desc_res.get("ok") and desc_res.get("rows"):
            row0 = desc_res["rows"][0]
            if row0.get("comment") or row0.get("description"):
                description = str(row0.get("comment") or row0.get("description"))

        clean_base = str(base_url or "").rstrip("/")
        rpc_url = f"{clean_base}/api/v1/snowflake/a2a/cortex-agents/{db}/{sch}/{short_agent}/jsonrpc"
        return {
            "name": agent_fqn,
            "description": description,
            "url": rpc_url,
            "version": "1.0.0",
            "protocolVersion": "0.3.0",
            "preferredTransport": "JSONRPC",
            "capabilities": {
                "streaming": False,
                "pushNotifications": False,
                "stateTransitionHistory": True,
            },
            "defaultInputModes": ["text/plain", "application/json"],
            "defaultOutputModes": ["text/plain", "application/json"],
            "skills": skills,
            "metadata": {
                "agent_fqn": agent_fqn,
                "database": db,
                "schema": sch,
                "agent_name": short_agent,
                "active_role": effective_role,
                "describe_sql": f"DESCRIBE AGENT {agent_fqn}",
                "run_endpoint": f"/api/v2/databases/{db}/schemas/{sch}/agents/{short_agent}:run",
            },
        }

    def handle_cortex_agent_a2a_jsonrpc(
        self,
        database: str,
        schema: str,
        agent_name: str,
        rpc_body: dict[str, Any],
        *,
        flow_id: str | None = None,
        role: str | None = None,
    ) -> dict[str, Any]:
        """Handle A2A v0.3.0 JSON-RPC (`message/send`, `tasks/get`, `tasks/cancel`) for a Snowflake Cortex Agent (`MA-W2-R6-05`)."""
        effective_role = (role or self.active_role or "").strip().upper()
        # BUG-A2A-04: path parameters are validated before any SQL is composed.
        try:
            db = _sql_safety.validate_identifier(database or self.database or "SNOWFLAKE_LANGFLOW", "database")
            sch = _sql_safety.validate_identifier(schema or self.schema or "CORE", "schema")
            short_agent = _sql_safety.validate_identifier(
                (agent_name or "EXAMPLE_AGENT").strip().split(".")[-1], "agent_name"
            )
        except _sql_safety.SqlSafetyError as exc:
            return {
                "ok": False,
                "status_code": 400,
                "error_code": "INVALID_IDENTIFIER",
                "message": str(exc),
            }
        agent_fqn = f"{db}.{sch}.{short_agent}"

        rpc_id = rpc_body.get("id")
        method = str(rpc_body.get("method") or "").strip()
        params = rpc_body.get("params") if isinstance(rpc_body.get("params"), dict) else {}

        denied, _desc = self._cortex_agent_access(db, sch, short_agent, effective_role, flow_id)
        if denied is not None:
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "error": {
                    "code": -32000,
                    "message": f"{denied['error_code']}: {denied['message']}",
                    "data": {"error_code": denied["error_code"], "status_code": denied["status_code"]},
                },
            }

        if method in ("message/send", "message.send", "SendMessage", "tasks/send"):
            # BUG-A2A-01: the Cortex Agent is a hop of the A2A call chain (header + JSON-RPC metadata).
            rpc_chain, rpc_depth = _a2a_chain.chain_from_rpc_params(params)
            inbound_chain = _a2a_chain.merge_chains(_a2a_chain.inbound_call_chain(), rpc_chain)
            hop_depth = _a2a_chain.effective_max_depth(rpc_depth)
            try:
                next_chain = _a2a_chain.extend_call_chain(inbound_chain, agent_fqn, hop_depth)
            except _a2a_chain.A2ARecursionError as rec_exc:
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {
                        "code": -32000,
                        "message": f"A2A_RECURSION_BLOCKED: {rec_exc}",
                        "data": {
                            "error_code": "A2A_RECURSION_BLOCKED",
                            "status_code": 400,
                            "call_chain": list(rec_exc.chain),
                            "max_depth": rec_exc.max_depth,
                        },
                    },
                }
            msg_obj = params.get("message") if isinstance(params.get("message"), dict) else {}
            user_text = ""
            parts = msg_obj.get("parts") or params.get("parts") or []
            if isinstance(parts, list):
                extracted_parts: list[str] = []
                for p in parts:
                    if isinstance(p, dict) and isinstance(p.get("text"), str):
                        extracted_parts.append(p["text"])
                    elif isinstance(p, str):
                        extracted_parts.append(p)
                user_text = "\n".join(extracted_parts).strip()
            if not user_text:
                user_text = str(
                    msg_obj.get("text")
                    or msg_obj.get("content")
                    or params.get("text")
                    or params.get("message_text")
                    or "Hello"
                ).strip()

            context_id = str(
                msg_obj.get("contextId")
                or msg_obj.get("context_id")
                or params.get("contextId")
                or params.get("sessionId")
                or params.get("session_id")
                or ""
            ).strip()
            task_id = str(
                msg_obj.get("taskId")
                or msg_obj.get("task_id")
                or params.get("id")
                or params.get("taskId")
                or f"a2a-task-{uuid.uuid4().hex[:12]}"
            ).strip()

            _telemetry.annotate_current_run(a2a_task_id=task_id)
            explicit_tid: int | None = None
            if context_id and context_id.isdigit():
                explicit_tid = int(context_id)
            elif task_id in self._a2a_cortex_tasks_mem:
                prev_tid = self._a2a_cortex_tasks_mem[task_id].get("thread_id")
                if prev_tid is not None:
                    explicit_tid = int(prev_tid)

            inv_res = self.invoke_cortex_agent(
                database=db,
                schema=sch,
                agent_name=short_agent,
                user_message=user_text,
                flow_id=flow_id or "a2a-bridge",
                session_id=context_id or task_id,
                resume_thread=True,
                thread_continuity_mode=(
                    "Select Specific Thread / Start Fresh Thread"
                    if explicit_tid is not None
                    else "One Thread per Playground Session (Session ID)"
                ),
                thread_id=explicit_tid,
                role=effective_role,
            )
            if not inv_res.get("ok"):
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {
                        "code": -32000,
                        "message": str(inv_res.get("message") or "Cortex Agent execution failed"),
                    },
                }

            tid_str = str(inv_res.get("thread_id") or context_id or task_id)
            reply_text = str(inv_res.get("text") or "")
            agent_msg_id = str(inv_res.get("assistant_message_id") or uuid.uuid4().hex)
            task_payload = {
                "id": task_id,
                "contextId": tid_str,
                "kind": "task",
                "status": {
                    "state": "completed",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "message": {
                        "kind": "message",
                        "messageId": agent_msg_id,
                        "role": "agent",
                        "contextId": tid_str,
                        "taskId": task_id,
                        "parts": [{"kind": "text", "text": reply_text}],
                    },
                },
                "artifacts": [
                    {
                        "artifactId": f"art-{agent_msg_id}",
                        "name": "cortex_agent_response",
                        "parts": [
                            {"kind": "text", "text": reply_text},
                            {
                                "kind": "data",
                                "data": {
                                    "agent_fqn": agent_fqn,
                                    "thread_id": inv_res.get("thread_id"),
                                    "parent_message_id": inv_res.get("parent_message_id"),
                                    "user_message_id": inv_res.get("user_message_id"),
                                    "assistant_message_id": inv_res.get("assistant_message_id"),
                                    "next_parent_message_id": inv_res.get("next_parent_message_id"),
                                    "sql_queries": inv_res.get("sql_queries", []),
                                    "citations": inv_res.get("citations", []),
                                    "charts": inv_res.get("charts", []),
                                    "tool_uses": inv_res.get("tool_uses", []),
                                    "active_role": effective_role,
                                },
                            },
                        ],
                    }
                ],
                "metadata": {
                    "agent_fqn": agent_fqn,
                    "thread_id": inv_res.get("thread_id"),
                    "parent_message_id": inv_res.get("parent_message_id"),
                    "assistant_message_id": inv_res.get("assistant_message_id"),
                    "active_role": effective_role,
                    **_a2a_chain.outbound_metadata(next_chain, hop_depth),
                },
            }
            # BUG-A2A-05: the store owns its own copy; callers never share mutable state with it.
            self._a2a_cortex_tasks_mem[task_id] = {
                "task": copy.deepcopy(task_payload),
                "thread_id": inv_res.get("thread_id"),
                "agent_fqn": agent_fqn,
            }
            return {"jsonrpc": "2.0", "id": rpc_id, "result": copy.deepcopy(task_payload)}

        if method in ("tasks/get", "tasks.get", "GetTask"):
            task_id = str(params.get("id") or params.get("taskId") or "").strip()
            stored = self._a2a_cortex_tasks_mem.get(task_id)
            if stored and isinstance(stored.get("task"), dict):
                return {"jsonrpc": "2.0", "id": rpc_id, "result": copy.deepcopy(stored["task"])}
            desc = self.describe_cortex_thread(int(task_id), page_size=20) if task_id.isdigit() else {}
            msgs = (desc.get("messages") or []) if isinstance(desc, dict) else []
            # A6 veto: a numeric id is a task only for a thread that really exists (known here or with
            # messages on Snowflake); anything else is TaskNotFoundError, never a synthetic "completed" task.
            if task_id.isdigit() and (int(task_id) in self._cortex_threads_server_mem or msgs):
                last_amid = self.resolve_last_assistant_message_id(int(task_id))
                latest_text = str(msgs[0].get("raw_message") or "") if msgs else ""
                synth_task = {
                    "id": task_id,
                    "contextId": task_id,
                    "kind": "task",
                    "status": {
                        "state": "completed",
                        "message": {
                            "kind": "message",
                            "messageId": str(last_amid or 0),
                            "role": "agent",
                            "parts": [{"kind": "text", "text": latest_text}],
                        },
                    },
                    "artifacts": [],
                    "metadata": {"agent_fqn": agent_fqn, "thread_id": int(task_id), "last_assistant_message_id": last_amid},
                }
                return {"jsonrpc": "2.0", "id": rpc_id, "result": synth_task}
            return {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "error": {"code": -32001, "message": "Task not found", "data": {"taskId": task_id}},
            }

        if method in ("tasks/cancel", "tasks.cancel", "CancelTask"):
            # BUG-A2A-05 (A2A v0.3.0 §8.2): unknown -> -32001 TaskNotFoundError; terminal -> -32002 TaskNotCancelableError.
            task_id = str(params.get("id") or params.get("taskId") or "").strip()
            stored = self._a2a_cortex_tasks_mem.get(task_id)
            if not (stored and isinstance(stored.get("task"), dict)):
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {"code": -32001, "message": "Task not found", "data": {"taskId": task_id}},
                }
            state = str((stored["task"].get("status") or {}).get("state") or "").lower()
            if state in {"completed", "failed", "canceled", "rejected"}:
                return {
                    "jsonrpc": "2.0",
                    "id": rpc_id,
                    "error": {
                        "code": -32002,
                        "message": "Task cannot be canceled",
                        "data": {"taskId": task_id, "state": state},
                    },
                }
            canceled = copy.deepcopy(stored["task"])
            canceled["status"] = {"state": "canceled", "timestamp": datetime.now(timezone.utc).isoformat()}
            self._a2a_cortex_tasks_mem[task_id] = {**stored, "task": canceled}
            return {"jsonrpc": "2.0", "id": rpc_id, "result": copy.deepcopy(canceled)}

        return {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "error": {"code": -32601, "message": f"Unsupported A2A JSON-RPC method: {method}"},
        }

    def list_internal_and_external_a2a_targets(
        self,
        *,
        flow_id: str | None = None,
        base_url: str = "",
    ) -> dict[str, Any]:
        """List both Langflow A2A Flows and `[ ❄️ Context ]` Snowflake Cortex Agents for `A2A Agent` Internal & External modes (`MA-W2-R6-05`)."""
        norm_fid = (
            self._normalize_flow_id(flow_id)
            or self._normalize_flow_id(CURRENT_SNOWFLAKE_FLOW_ID.get())
            or self._normalize_flow_id(self.active_flow_id)
        )
        internal_options: list[str] = []
        internal_metadata: list[dict[str, Any]] = []
        quick_fill_options: list[str] = []
        quick_fill_map: dict[str, str] = {}
        clean_base = str(base_url or "http://127.0.0.1:7860").rstrip("/")

        # 1. Discover Langflow flows in SQLite `flow` table
        try:
            db_path = self._resolve_writable_sqlite_db_path()
            if db_path.exists():
                conn = _open_sqlite_conn(db_path, readonly=True, timeout=10.0)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                if "flow" in tables:
                    rows = cur.execute(
                        "SELECT id, name, updated_at, is_component FROM flow ORDER BY updated_at DESC"
                    ).fetchall()
                    for r in rows:
                        d = dict(r)
                        if d.get("is_component") in (True, 1, "1"):
                            continue
                        fid = self._normalize_flow_id(str(d.get("id") or "")) or str(d.get("id") or "").strip()
                        fname = str(d.get("name") or fid).strip()
                        if not fid:
                            continue
                        label = f"[Langflow Flow] {fname} ({fid})"
                        if label not in internal_options:
                            internal_options.append(label)
                            internal_metadata.append(
                                {
                                    "id": fid,
                                    "kind": "langflow_flow",
                                    "name": fname,
                                    "updated_at": str(d.get("updated_at") or ""),
                                }
                            )
                            url_val = f"{clean_base}/api/v1/a2a/{fid}"
                            qf_label = f"[Langflow Flow] {fname} -> {url_val}"
                            quick_fill_options.append(qf_label)
                            quick_fill_map[qf_label] = url_val
                conn.close()
        except Exception:  # noqa: BLE001
            pass

        # 2. Discover Cortex Agents visible in active `[ ❄️ Context ]` for `flow_id`
        dbs = self.get_context_databases(flow_id=norm_fid)
        seen_agents: set[str] = set()
        for db in dbs:
            for sch in self.get_context_schemas(db, flow_id=norm_fid):
                for ag in self.get_context_objects(db, sch, "cortex_agents", flow_id=norm_fid):
                    short_ag = str(ag).strip().split(".")[-1].upper()
                    if not short_ag:
                        continue
                    fqn = f"{db.upper()}.{sch.upper()}.{short_ag}"
                    if fqn in seen_agents:
                        continue
                    seen_agents.add(fqn)
                    label = f"[Cortex Agent] {fqn}"
                    internal_options.append(label)
                    internal_metadata.append(
                        {
                            "id": fqn,
                            "kind": "cortex_agent",
                            "database": db.upper(),
                            "schema": sch.upper(),
                            "agent_name": short_ag,
                        }
                    )
                    bridge_url = f"{clean_base}/api/v1/snowflake/a2a/cortex-agents/{db.upper()}/{sch.upper()}/{short_ag}"
                    qf_bridge = f"[Cortex Agent A2A] {fqn} -> {bridge_url}"
                    quick_fill_options.append(qf_bridge)
                    quick_fill_map[qf_bridge] = bridge_url

                    run_url = f"/api/v2/databases/{db.upper()}/schemas/{sch.upper()}/agents/{short_ag}:run"
                    qf_rest = f"[Cortex Agent REST :run] {fqn} -> {run_url}"
                    quick_fill_options.append(qf_rest)
                    quick_fill_map[qf_rest] = run_url

        # BUG-CA-10: no phantom EXAMPLE_AGENT fallback; empty discovery yields empty lists.
        return {
            "internal_options": internal_options,
            "internal_metadata": internal_metadata,
            "quick_fill_options": quick_fill_options,
            "quick_fill_map": quick_fill_map,
        }

    def sync_superuser_credentials(self) -> None:
        """Ensure the SQLite `user` table superuser matches `LANGFLOW_SUPERUSER` and `LANGFLOW_SUPERUSER_PASSWORD`."""
        if self._superuser_synced:
            return
        self._superuser_synced = True
        su_user = os.getenv("LANGFLOW_SUPERUSER", "").strip()
        su_pass = os.getenv("LANGFLOW_SUPERUSER_PASSWORD", "").strip()
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        if not su_user or not su_pass or not db_path.exists():
            return
        try:
            from langflow.services.auth.utils import get_password_hash

            hashed = get_password_hash(su_pass)
            conn = _open_sqlite_conn(db_path, readonly=False, timeout=30.0)
            cur = conn.cursor()
            cur.execute(
                "UPDATE user SET username = ?, password = ?, is_active = 1, is_superuser = 1 WHERE is_superuser = 1",
                (su_user, hashed),
            )
            conn.commit()
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    def sync_persistent_workflows_backup(self) -> dict[str, Any]:
        """Mirror workflows from `/persist/langflow.db` (read-only non-blocking) to `/persist/backups/flows_backup.json`."""
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        backup_dir = persist_dir / "backups"
        backup_file = backup_dir / "flows_backup.json"
        if not db_path.exists():
            return {"ok": False, "persist_dir": str(persist_dir), "db_exists": False, "flows_count": 0}
        try:
            backup_dir.mkdir(parents=True, exist_ok=True)
            conn = _open_sqlite_conn(db_path, readonly=True, timeout=30.0)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT id, name, description, updated_at, folder_id, user_id, is_component FROM flow")
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            backup_file.write_text(
                json.dumps({"updated_at": datetime.now(timezone.utc).isoformat(), "flows": rows}, indent=2),
                encoding="utf-8",
            )
            return {
                "ok": True,
                "persist_dir": str(persist_dir),
                "db_path": str(db_path),
                "db_size_bytes": db_path.stat().st_size,
                "backup_file": str(backup_file),
                "flows_count": len(rows),
                "volume_type": "SPCS Persistent Block Volume (10Gi, SNOWFLAKE_SSE, survives Compute Pool SUSPEND/RESUME)",
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": True,
                "persist_dir": str(persist_dir),
                "db_path": str(db_path),
                "db_size_bytes": db_path.stat().st_size if db_path.exists() else 0,
                "note": str(exc),
                "volume_type": "SPCS Persistent Block Volume (10Gi, SNOWFLAKE_SSE, survives Compute Pool SUSPEND/RESUME)",
            }

    @property
    def authenticated_user(self) -> str:
        """Per-request Snowflake user (ContextVar); falls back to the service user. Display/attribution only."""
        return _sf_identity.current_identity().sf_user or self._service_user

    @authenticated_user.setter
    def authenticated_user(self, value: str) -> None:
        # Only the service-session user may be stored on the shared bridge instance.
        self._service_user = value

    def _has_spcs_token(self) -> bool:
        return SPCS_TOKEN_PATH.exists() and bool(os.getenv("SNOWFLAKE_HOST"))

    def _service_role(self) -> str:
        """Role for background/service writes (W3-R1-10e): configured service role, never a hard-coded admin."""
        # Never a user's session role: service writes run as the configured or base service role.
        return _sf_identity.service_role() or self.service_active_role

    def _service_default_role(self) -> str | None:
        """Default role of the service token, resolved once via CURRENT_ROLE() and cached (never invented)."""
        cached = getattr(self, "_service_default_role_cache", None)
        if cached:
            return cached
        if time.monotonic() < getattr(self, "_service_default_role_retry_at", 0.0):
            return None
        try:
            # No role is sent here (active_role is empty), so CURRENT_ROLE() is the token's default role.
            # role="" sends no role, so this never inherits a user's session role.
            res = self.execute_sql("SELECT CURRENT_ROLE() AS RL", role="", timeout=10, mode=_sf_identity.MODE_SERVICE)
        except Exception:  # noqa: BLE001
            res = {}
        rows = res.get("rows") if isinstance(res, dict) and res.get("ok") else None
        role = str((rows[0] or {}).get("RL") or "").strip() if rows else ""
        if role:
            self._service_default_role_cache = role
            return role
        self._service_default_role_retry_at = time.monotonic() + 60.0
        return None

    def _effective_role_for_audit(self, mode: str, explicit_role: str | None = None) -> str | None:
        """Role actually used for the audited action: the role sent to the SQL API, else the token default."""
        if explicit_role and explicit_role.strip():
            return explicit_role.strip()
        if mode == _sf_identity.MODE_CALLER:
            # Caller mode sends no role unless explicit; the caller's default role is not resolved here.
            return None
        return (self.active_role or "").strip() or self._service_default_role()

    def _get_spcs_headers(self, mode: str = _sf_identity.MODE_SERVICE) -> dict[str, str]:
        token = SPCS_TOKEN_PATH.read_text(encoding="utf-8").strip()
        return {
            # Raises CallerIdentityMissing in caller mode without a caller token (fail closed).
            "Authorization": _sf_identity.authorization_value(token, mode),
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "SnowflakeLangflow/0.2.0",
        }

    def execute_sql(
        self,
        statement: str,
        *,
        role: str | None = None,
        bindings: dict[str, Any] | None = None,
        timeout: int = 60,
        query_tag: str | None = None,
        mode: str | None = None,
        warehouse: str | None = None,
    ) -> dict[str, Any]:
        """Execute a SQL statement via Snowflake SQL REST API (`/api/v2/statements`) using SPCS OAuth.

        ``mode="caller"`` runs with restricted caller's rights when SNOWFLAKE_LANGFLOW_CALLERS_RIGHTS=1;
        the role sent is the explicit ``role``, else the caller's own (user, flow) selection, else none
        (token default role) -- so the SQL API body always carries the reported active role.
        """
        # BUG-CTX-03: SQL issued by a flow component may only touch that flow's `[ ❄️ Context ]`.
        if _flow_guard.FLOW_COMPONENT_SQL.get():
            guard_token = _flow_guard.FLOW_COMPONENT_SQL.set(False)  # bridge-internal SQL below is exempt
            try:
                denied = self._flow_context_denial(statement)
                if denied is not None:
                    return denied
                # BUG-LLMOP-007: warehouse/timeout must survive the flow-context re-entry.
                return self.execute_sql(
                    statement,
                    role=role,
                    bindings=bindings,
                    timeout=timeout,
                    query_tag=query_tag,
                    mode=mode,
                    warehouse=warehouse,
                )
            finally:
                _flow_guard.FLOW_COMPONENT_SQL.reset(guard_token)
        # Default: the mode bound for this request (caller on user-facing routes, service elsewhere).
        exec_mode = _sf_identity.resolve_mode(mode or _sf_identity.current_request_mode())
        if exec_mode == _sf_identity.MODE_CALLER:
            # E2E T2: explicit role > the caller's own (user, flow) selection > token default role.
            # Never the service base role: it is not the caller's choice.
            if role is None:
                role = TRIGGER_RUN_ROLE.get() or self.current_session_role() or ""
            effective_role = role.strip()
        else:
            # An explicit "" means "token default role"; only ``None`` inherits the session role.
            effective_role = (self.active_role if role is None else role).strip()
        if effective_role and _sf_identity.validate_role_identifier(effective_role) is None:
            return {
                "ok": False,
                "mode": exec_mode,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": "Invalid Snowflake role identifier; nothing was executed.",
                "effective_role": None,
                "rows": [],
                "columns": [],
                "statement_handle": None,
            }
        if not self._has_spcs_token():
            # Fail closed: nothing was executed, so nothing may be registered locally.
            return {
                "ok": False,
                "mode": "offline",
                "error_code": "NOT_CONFIGURED",
                "message": "No Snowflake service token available; the statement was not executed.",
                "effective_role": effective_role,
                "statement": statement,
                "rows": [],
                "columns": [],
                "statement_handle": None,
            }

        host = os.environ["SNOWFLAKE_HOST"]
        url = f"https://{host}/api/v2/statements"
        # BUG-CA-01: every SQL API request is its own session, so secondary roles are pinned per execution.
        sr_mode = _sf_identity.secondary_roles_mode()
        self.secondary_roles_mode = sr_mode
        # Rerun-3: null bindings are unset in multi-statement requests -> inline ``NULL`` (constant only).
        statement, bindings = _sql_safety.inline_null_bindings(statement, bindings)
        payload: dict[str, Any] = {
            "statement": self._with_secondary_roles(statement, sr_mode),
            "timeout": timeout,
            "database": self.database,
            "schema": self.schema,
        }
        if effective_role:
            payload["role"] = effective_role
        # BUG-LLMOP-007: a per-call warehouse travels in the request body (validated identifier).
        if warehouse:
            payload["warehouse"] = _sql_safety.validate_identifier(str(warehouse), "warehouse")
        elif self.warehouse:
            payload["warehouse"] = self.warehouse
        if bindings:
            payload["bindings"] = bindings
        # TR-02: every SQL API call carries the JSON query tag (callers on worker threads pass it explicitly).
        payload["parameters"] = {"query_tag": query_tag or build_query_tag(), "MULTI_STATEMENT_COUNT": "0"}

        try:
            request_headers = (
                self._get_spcs_headers(exec_mode)
                if exec_mode == _sf_identity.MODE_CALLER
                else self._get_spcs_headers()
            )
        except _sf_identity.CallerIdentityMissing as exc:
            return {
                "ok": False,
                "mode": exec_mode,
                "status_code": 401,
                "error_code": exc.error_code,
                "message": str(exc),
                "effective_role": effective_role,
                "rows": [],
                "columns": [],
                "statement_handle": None,
            }

        try:
            if self._http_client is None and httpx is not None:
                self._http_client = httpx.Client(
                    timeout=35.0,
                    verify=True,
                    limits=httpx.Limits(max_keepalive_connections=16, max_connections=32),
                )
            client = self._http_client
            denied = self._secondary_roles_guard(
                client, url, request_headers, payload, effective_role, exec_mode, sr_mode
            )
            if denied is not None:
                return denied
            resp_status, body = self._post_sql_statements(
                client, url, request_headers, payload, float(min(max(timeout, 5), 60) + 5),
                max_wait=float(max(int(timeout or 0), 5)),
            )

            class _Status:  # keeps the error mapping below unchanged
                status_code = resp_status

            resp = _Status()
            if resp.status_code >= 400:
                msg = body.get("message") or f"HTTP {resp.status_code}"
                code = str(body.get("code") or "")
                if "not authorized" in msg.lower() or "insufficient privileges" in msg.lower() or resp.status_code == 403:
                    return {
                        "ok": False,
                        "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                        "status_code": 403,
                        "message": f"Il ruolo attivo ({effective_role}) non consente questa operazione: {msg}",
                        "snowflake_code": code,
                        "effective_role": effective_role,
                    }
                if "does not exist" in msg.lower():
                    return {
                        "ok": False,
                        "error_code": ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED.value,
                        "status_code": 404,
                        "message": f"Oggetto non trovato o non autorizzato per il ruolo ({effective_role}): {msg}",
                        "snowflake_code": code,
                        "effective_role": effective_role,
                    }
                return {
                    "ok": False,
                    "error_code": ErrorCode.TECHNICAL_EXECUTION_ERROR.value,
                    "status_code": 500,
                    "message": msg,
                    "snowflake_code": code,
                    "effective_role": effective_role,
                }

            meta = body.get("resultSetMetaData", {})
            row_types = meta.get("rowType", [])
            col_names = [col.get("name", f"COL_{idx}") for idx, col in enumerate(row_types)]
            raw_data = body.get("data", [])
            rows = [dict(zip(col_names, row)) for row in raw_data]
            self._auto_register_ddl_object_if_create(statement)
            return {
                "ok": True,
                "effective_role": effective_role,
                "statement_handle": body.get("statementHandle", ""),
                "columns": col_names,
                "rows": rows,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "error_code": ErrorCode.TECHNICAL_EXECUTION_ERROR.value,
                "status_code": 500,
                "message": str(exc),
                "effective_role": effective_role,
            }

    def _flow_guard_exempt_fqns(self) -> frozenset[str]:
        """BUG-CTX-03: bridge-owned governance objects every flow may touch; never user-selected objects.

        Exactly: the Flow Activity, Runs, Spans and Audit tables configured on the bridge
        (`LANGFLOW_FLOW_ACTIVITY_TABLE`, `LANGFLOW_TRACES_RUNS_TABLE`, `LANGFLOW_TRACES_SPANS_TABLE`,
        `LANGFLOW_AUDIT_LOG_TABLE`) and the governed message-history table (`LANGFLOW_MESSAGES_TABLE`,
        default `<DB>.<SCHEMA>.LANGFLOW_MESSAGES`). User memories (`MEM_*` / `LF_MEM_*`) are NOT exempt:
        they are context objects and must be selected in the flow's `[ ❄️ Context ]`.

        PASSO3/A6: the exemption is append-only (INSERT / MERGE / COPY INTO these exact FQNs); a flow
        component can never SELECT / UPDATE / DELETE / TRUNCATE / DROP them without selecting them.
        """
        messages = self._configured_messages_table()
        out: set[str] = set()
        for t in (self.activity_table, self.runs_table, self.spans_table, self.audit_table, messages):
            parts = [p.strip() for p in str(t or "").replace('"', "").split(".")]
            # Exact 3-part FQNs only: never a wildcard, a schema, or a partially qualified name.
            if len(parts) == 3 and all(self._PROBE_IDENT_RE.fullmatch(p) for p in parts):
                out.add(".".join(p.upper() for p in parts))
        return frozenset(out)

    def _flow_guard_bridge_routine_fqns(self) -> frozenset[str]:
        """Exact FQNs of bridge-owned routines (e.g. the LangChain splitter) flow components may invoke."""
        from lfx_snowflake import bridge_routines as _bridge_routines  # lazy: bridge_routines imports the bridge

        return _bridge_routines.bridge_routine_fqns(self)

    def _configured_messages_table(self) -> str:
        """The governed message-history table actually configured (LANGFLOW_MESSAGES_TABLE), fully qualified."""
        raw = str(getattr(self, "messages_table", "") or os.getenv("LANGFLOW_MESSAGES_TABLE") or "").strip()
        if not raw:
            raw = "LANGFLOW_MESSAGES"
        parts = [p for p in raw.replace('"', "").split(".") if p]
        if len(parts) == 1:
            parts = [str(self.database or ""), str(self.schema or ""), parts[0]]
        elif len(parts) == 2:
            parts = [str(self.database or ""), parts[0], parts[1]]
        return ".".join(parts)

    def _flow_context_denial(self, statement: str) -> dict[str, Any] | None:
        """BUG-CTX-03: ``None`` when ``statement`` stays inside the current flow's context, else a 403 result."""
        fid = self._normalize_flow_id(CURRENT_SNOWFLAKE_FLOW_ID.get())
        if not fid:
            return None
        ctx = self.contexts_by_flow_id.get(fid)
        if not isinstance(ctx, dict):
            self._load_flow_contexts_sqlite()
            ctx = self.contexts_by_flow_id.get(fid) or {}
        # Unqualified names resolve in the SQL API session (bridge database/schema). They are only accepted
        # when that is also the flow context's default; otherwise they are unverifiable (fail closed).
        sess_db, sess_sch = str(self.database or "").upper(), str(self.schema or "").upper()
        ctx_db = str((ctx or {}).get("database") or sess_db).upper()
        ctx_sch = str((ctx or {}).get("schema") or sess_sch).upper()
        def_db, def_sch = (sess_db, sess_sch) if (ctx_db, ctx_sch) == (sess_db, sess_sch) else ("", "")
        allowed, reason, offending = _flow_guard.check_statement(
            statement,
            ctx,
            def_db,
            def_sch,
            self._flow_guard_exempt_fqns(),
            bridge_routine_fqns=self._flow_guard_bridge_routine_fqns(),
        )
        if allowed:
            return None
        self.record_audit_event(
            action="FLOW_CONTEXT_ENFORCEMENT",
            outcome="DENIED",
            error_code=_flow_guard.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT,
            flow_id=fid,
            details={"reason": reason, "objects": offending},
        )
        return {
            "ok": False,
            "status_code": 403,
            "error_code": _flow_guard.AUTHZ_OBJECT_NOT_IN_FLOW_CONTEXT,
            "message": f"{reason} Nothing was executed." + (f" Objects: {', '.join(offending)}" if offending else ""),
            "flow_id": fid,
            "objects": offending,
            "rows": [],
            "columns": [],
            "statement_handle": None,
        }

    @staticmethod
    def _with_secondary_roles(statement: str, sr_mode: str) -> str:
        """Prefix ``USE SECONDARY ROLES <mode>`` so the primary role alone authorizes the statement."""
        mode = sr_mode if sr_mode in ("NONE", "ALL") else "NONE"  # never interpolate anything else
        body = (statement or "").strip()
        while body.endswith(";"):
            body = body[:-1].rstrip()
        return f"USE SECONDARY ROLES {mode};\n{body}"

    @staticmethod
    def _post_sql_statements(
        client: Any, url: str, headers: dict[str, str], payload: dict[str, Any], timeout: float,
        max_wait: float = 0.0,
    ) -> tuple[int, dict[str, Any]]:
        """POST a multi-statement request and return the result of its last statement.

        The SQL API answers a multi-statement request with ``statementHandles``; the result of the
        user statement is fetched from the last handle (GET /api/v2/statements/<handle>).
        """
        if max_wait > 60:
            # BUG-LLMOP-007: long statements run async (202 + statementHandle) and are polled.
            payload = {**payload, "async": True}
        # Sync requests also answer 202 after ~45s, so the deadline covers both modes.
        deadline = time.monotonic() + max(float(max_wait or 0.0), float(timeout or 0.0)) + 5

        def _wait(status: int, body: Any) -> tuple[int, Any]:
            """Poll GET /api/v2/statements/<handle> while the statement is still running (202 / 333334)."""
            while status == 202 and isinstance(body, dict) and body.get("statementHandle"):
                if time.monotonic() > deadline:
                    return 408, {
                        "code": "000630",
                        "message": "statement polling timed out",
                        "statementHandle": body.get("statementHandle"),
                    }
                time.sleep(1.0)
                poll = client.get(f"{url}/{body['statementHandle']}", headers=headers, timeout=timeout)
                body = poll.json() if poll.content else {}
                status = int(poll.status_code)
            return status, body

        resp = client.post(url, headers=headers, json=payload, timeout=timeout)
        status, body = _wait(int(resp.status_code), resp.json() if resp.content else {})
        # E2E rerun-2/T2 (CRITICAL): 100132 is the generic multi-statement child-failure code, so a denied
        # user SELECT also answers 100132. The statement is NEVER re-sent without the USE SECONDARY ROLES
        # prefix (that retry ran it with default secondary roles and leaked the row). Only a rejection of
        # the prefix itself (statement 1) is mapped to 403 AUTHZ_ROLE_DENIED; any other error is surfaced.
        stmt_txt = str(payload.get("statement") or "")
        if (
            status >= 400
            and isinstance(body, dict)
            and _SECONDARY_ROLES_PREFIX_RE.match(stmt_txt)
            and _secondary_roles_prefix_rejected(body)
        ):
            return 403, {
                "code": str(body.get("code") or ""),
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": (
                    "Secondary roles policy cannot be enforced in this session (USE SECONDARY ROLES "
                    "rejected); the statement was not executed."
                ),
            }
        handles = body.get("statementHandles") if isinstance(body, dict) else None
        if status < 400 and isinstance(handles, list) and handles:
            # Multi-statement (e.g. the USE SECONDARY ROLES prefix): the user statement is the last child,
            # which may itself still be running -> poll the child handle too.
            last = client.get(f"{url}/{handles[-1]}", headers=headers, timeout=timeout)
            child = last.json() if last.content else {}
            if isinstance(child, dict) and int(last.status_code) == 202 and not child.get("statementHandle"):
                child = {**child, "statementHandle": handles[-1]}
            status, body = _wait(int(last.status_code), child)
        return status, body if isinstance(body, dict) else {}

    @staticmethod
    def _secondary_roles_verdict(row: list[Any], sr_mode: str) -> bool:
        """True when the session is acceptable: see ``_secondary_roles_guard``."""
        admin = _sf_identity.GUARDED_ADMIN_ROLE
        admin_in_session = str(row[0]).strip().lower() in {"true", "1"}
        primary = str(row[1] if len(row) > 1 and row[1] is not None else "").strip().upper()
        secondary_active = False
        if len(row) > 2 and row[2] not in (None, ""):
            try:
                sec = json.loads(row[2]) if isinstance(row[2], str) else row[2]
                secondary_active = bool(str((sec or {}).get("roles") or "").strip())
            except (TypeError, ValueError, AttributeError):
                secondary_active = True  # unparseable -> fail closed
        if sr_mode == "NONE" and secondary_active:
            return False
        return not (admin_in_session and primary != admin)

    def _secondary_roles_guard(
        self,
        client: Any,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any],
        effective_role: str,
        exec_mode: str,
        sr_mode: str,
    ) -> dict[str, Any] | None:
        """BUG-CA-01: fail closed when ACCOUNTADMIN is in session although the primary role is not ACCOUNTADMIN.

        The verdict uses Snowflake's own CURRENT_ROLE(), so an empty requested role (service default role,
        or the caller's default role under caller's rights) is judged on the role actually activated.
        Under ``NONE`` any active secondary role also fails closed. Returns ``None`` when the statement may
        run, otherwise a 403 ``AUTHZ_ROLE_DENIED`` result. A positive verdict is cached for 60 s per
        (mode, user, role, secondary-roles mode).
        """
        admin = _sf_identity.GUARDED_ADMIN_ROLE
        if (effective_role or "").strip().upper() == admin:
            return None
        key = (exec_mode, self._role_session_user(), (effective_role or "").upper(), sr_mode)
        now = time.monotonic()
        if self._secondary_guard_cache.get(key, 0.0) > now:
            return None
        check_payload = dict(payload)
        check_payload["statement"] = self._with_secondary_roles(
            f"SELECT IS_ROLE_IN_SESSION('{admin}') AS ADMIN_IN_SESSION, CURRENT_ROLE() AS PRIMARY_ROLE, "
            "CURRENT_SECONDARY_ROLES() AS SECONDARY_ROLES",
            sr_mode,
        )
        check_payload.pop("bindings", None)
        verdict: bool | None = None
        try:
            status, body = self._post_sql_statements(client, url, headers, check_payload, 20.0)
            data = body.get("data") or []
            if status < 400 and data and data[0]:
                verdict = self._secondary_roles_verdict(list(data[0]), sr_mode)
        except Exception:  # noqa: BLE001
            verdict = None
        if verdict:
            self._secondary_guard_cache[key] = now + 60.0
            return None
        return {
            "ok": False,
            "mode": exec_mode,
            "status_code": 403,
            "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
            "message": (
                f"Active role ({effective_role or 'default'}) is not allowed to run this statement: "
                f"{admin} is active in the session (secondary roles {sr_mode}) or the check could not "
                "be completed; nothing was executed."
            ),
            "effective_role": effective_role,
            "secondary_roles_mode": sr_mode,
            "rows": [],
            "columns": [],
            "statement_handle": None,
        }

    def refresh_roles_from_snowflake(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if (
            not force
            and self._roles_discovered
            and not self._is_execute_sql_mocked()
            and (now - self._roles_last_refreshed_ts) < 60.0
        ):
            return
        # Shared bridge state is service-scoped: never let one caller's roles leak into the global selector.
        res = self.execute_sql(
            "SELECT CURRENT_USER() AS USR, CURRENT_ROLE() AS RL, CURRENT_AVAILABLE_ROLES() AS AVAIL",
            role=self._service_role(),
            timeout=10,
            mode=_sf_identity.MODE_SERVICE,
        )
        if res.get("ok") and res.get("rows"):
            row = res["rows"][0]
            # W3-R1-10b: the per-request user comes from the ContextVar; _service_user is never overwritten here.
            current_rl = row.get("RL")
            if current_rl and not self._roles_discovered:
                # Service-level discovery: write the base role, never the current user's session.
                self.__dict__["_base_active_role"] = str(current_rl).strip().upper()
            raw_avail = row.get("AVAIL")
            if raw_avail:
                try:
                    parsed = json.loads(raw_avail) if isinstance(raw_avail, str) else raw_avail
                    if isinstance(parsed, list) and parsed:
                        # ACCOUNTADMIN is never injected: it is listed only if Snowflake reports it as granted.
                        for mandatory in (
                            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
                            "SNOWFLAKE_LANGFLOW_DEV_ROLE",
                            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
                        ):
                            if mandatory not in parsed:
                                parsed.append(mandatory)
                        self.available_roles = parsed
                except Exception:  # noqa: BLE001
                    pass
            self._roles_discovered = True
            self._roles_last_refreshed_ts = now

    @classmethod
    def _safe_sql_json_str(cls, value: Any, max_chars: int = 4000, default_empty: str = "{}") -> str:
        """Serialize `value` to valid JSON and escape both backslashes and single quotes for Snowflake `PARSE_JSON('...')`."""
        return cls._safe_json_text(value, max_chars=max_chars, default_empty=default_empty).replace("\\", "\\\\").replace("'", "''")

    @staticmethod
    def _safe_json_text(value: Any, max_chars: int = 4000, default_empty: str = "{}") -> str:
        """Redacted, size-bounded JSON text (unescaped; meant for a ``PARSE_JSON(?)`` binding)."""
        parsed = value
        if isinstance(parsed, str):
            s = parsed.strip()
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    parsed = json.loads(s)
                except Exception:  # noqa: BLE001
                    parsed = {"raw": s[: max(256, max_chars // 2)]}
            else:
                parsed = {"raw": s[: max(256, max_chars // 2)]}
        redacted = redact_for_sink(parsed if parsed is not None else ({} if default_empty == "{}" else []))
        raw_json = json.dumps(redacted, default=str)
        if len(raw_json) > max_chars:
            raw_json = json.dumps(
                {"truncated": True, "preview": raw_json[: max(256, max_chars - 120)]}
                if default_empty == "{}"
                else [{"truncated": True, "preview": raw_json[: max(256, max_chars - 120)]}]
            )
        return raw_json

    @staticmethod
    def _merge_insert_request(
        table: str, key_col: str, key_value: Any, columns: list[tuple[str, Any, str]]
    ) -> tuple[str, dict[str, Any]]:
        """Idempotent ``MERGE ... WHEN NOT MATCHED THEN INSERT`` with every value as a SQL API binding.

        ``columns`` items are ``(column, value, kind)`` with kind ``TEXT``/``REAL``/``FIXED``/``JSON``
        (``PARSE_JSON(?)``) or ``NOW`` (``CURRENT_TIMESTAMP()``, value ignored). ``table`` and column
        names are code constants / bridge configuration, never request input (BUG-UI-SQLI-05).
        """
        b = _sql_safety.BindingBuilder()
        key_ph = b.add(str(key_value), "TEXT")
        values = LiveSnowflakeBridge._merge_value_exprs(b, columns)
        sql = (
            f"MERGE INTO {table} t "
            f"USING (SELECT {key_ph} AS {key_col}) s ON t.{key_col} = s.{key_col} "
            f"WHEN NOT MATCHED THEN INSERT "
            f"({', '.join(c for c, _, _ in columns)}) VALUES ({', '.join(values)})"
        )
        return sql, b.bindings or {}

    @staticmethod
    def _merge_value_exprs(b: Any, columns: list[tuple[str, Any, str]]) -> list[str]:
        """SQL value expressions of ``columns`` (every non-NULL value a binding added to ``b``)."""
        values: list[str] = []
        # Rerun-3: a NULL value is the SQL literal NULL, never a ``{"value": null}`` binding. Bridge statements
        # are multi-statement requests (``USE SECONDARY ROLES ...;`` prefix) and there the SQL API treats a null
        # binding as unset (002049 "Bind variable ? not set"): every RUN row (NULL cortex/a2a ids) was rejected.
        for _col, val, kind in columns:
            if kind == "NOW":
                # T10: always UTC (+00:00), never the session TIMEZONE (was -08:00 next to +00:00 rows).
                values.append("CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())")
            elif kind == "TS":
                # Real event time normalized to an explicit-UTC ISO-8601 binding; falls back to now (UTC).
                ts_iso = _telemetry.utc_iso(val)
                if ts_iso is None:
                    values.append("CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())")
                else:
                    values.append(
                        f"CONVERT_TIMEZONE('UTC', COALESCE(TRY_TO_TIMESTAMP_TZ({b.add(ts_iso, 'TEXT')}), "
                        f"CURRENT_TIMESTAMP()))"
                    )
            elif val is None:
                values.append("NULL")
            elif kind == "JSON":
                values.append(f"PARSE_JSON({b.add(val, 'TEXT')})")
            else:
                values.append(b.add(str(val) if kind == "TEXT" else val, kind))
        return values

    @staticmethod
    def _merge_insert_batch_request(
        table: str, key_col: str, rows: list[tuple[Any, list[tuple[str, Any, str]]]]
    ) -> tuple[str, dict[str, Any]]:
        """Rerun-4: one idempotent multi-row ``MERGE ... WHEN NOT MATCHED THEN INSERT`` for ``rows``.

        ``rows`` items are ``(key_value, columns)`` with the same column list/order as ``_merge_insert_request``.
        The source is ``SELECT <exprs> UNION ALL ...`` (not ``FROM VALUES``, which rejects ``PARSE_JSON(?)``);
        every value stays a binding, NULLs stay the literal NULL (typed by the target column on insert).
        Duplicate keys inside one batch are dropped (first wins) so a key is never inserted twice.
        """
        if not rows:
            raise ValueError("empty merge batch")
        cols = [c for c, _, _ in rows[0][1]]
        b = _sql_safety.BindingBuilder()
        selects: list[str] = []
        seen: set[str] = set()
        for key_value, columns in rows:
            k = str(key_value)
            if k in seen:
                continue
            seen.add(k)
            if [c for c, _, _ in columns] != cols:
                raise ValueError("merge batch rows must share one column list")
            key_ph = b.add(k, "TEXT")
            exprs = LiveSnowflakeBridge._merge_value_exprs(b, columns)
            selects.append(
                f"SELECT {key_ph} AS _k, " + ", ".join(f"{e} AS {c}" for e, c in zip(exprs, cols))
            )
        sql = (
            f"MERGE INTO {table} t "
            f"USING ({' UNION ALL '.join(selects)}) s ON t.{key_col} = s._k "
            f"WHEN NOT MATCHED THEN INSERT "
            f"({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
        )
        return sql, b.bindings or {}

    def _execute_bound(self, request: tuple[str, dict[str, Any]], *, timeout: int = 15) -> dict[str, Any]:
        """Run a ``(sql, bindings)`` telemetry request as the service role."""
        sql, binds = request
        result = self.execute_sql(sql, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, bindings=binds or None, timeout=timeout)
        _telemetry.check_write_result(result, sql)  # rerun-3: rejected span/RUN MERGEs are logged, not silent
        return result

    def _build_activity_merge_request(self, record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Idempotent `MERGE INTO <activity_table>` preserving `deleted_in_ui_at` (`CTX-ARCH-06`); values bound."""
        act_id, cols = self._activity_merge_row(record)
        return self._merge_insert_request(self.activity_table, "activity_id", act_id, cols)

    def _activity_merge_row(self, record: dict[str, Any]) -> tuple[str, list[tuple[str, Any, str]]]:
        """``(activity_id, columns)`` of an activity MERGE (shared by the single and batched forms)."""
        act_id = str(record.get("activity_id") or record.get("id") or uuid.uuid4().hex)
        flw_id = str(record.get("flow_id") or "unknown-flow")
        err_msg = record.get("error_message")
        p_tok = int(record.get("prompt_tokens") or 0)
        c_tok = int(record.get("completion_tokens") or 0)
        in_txt = record.get("input_text") if record.get("input_text") is not None else str(record.get("input") or "")
        out_txt = record.get("output_text") if record.get("output_text") is not None else str(record.get("output") or "")
        cols: list[tuple[str, Any, str]] = [
            ("activity_id", act_id, "TEXT"),
            ("trace_id", str(record.get("trace_id") or act_id), "TEXT"),
            ("event_type", str(record.get("event_type") or "FLOW_RUN"), "TEXT"),
            ("flow_id", flw_id, "TEXT"),
            ("flow_name", str(record.get("flow_name") or flw_id)[:250], "TEXT"),
            ("project_id", str(record.get("project_id") or "default")[:120], "TEXT"),
            ("project_name", str(record.get("project_name") or "Default Project")[:250], "TEXT"),
            ("session_id", str(record.get("session_id") or flw_id)[:250], "TEXT"),
            ("run_timestamp", None, "NOW"),
            ("status", str(record.get("status") or "SUCCESS").upper()[:32], "TEXT"),
            ("latency_ms", float(record.get("latency_ms") or 0.0), "REAL"),
            ("input_text", str(in_txt)[:4000], "TEXT"),
            ("output_text", str(out_txt)[:4000], "TEXT"),
            ("error_message", str(err_msg)[:2000] if err_msg else None, "TEXT"),
            ("prompt_tokens", p_tok, "FIXED"),
            ("completion_tokens", c_tok, "FIXED"),
            ("total_tokens", int(record.get("total_tokens") or (p_tok + c_tok)), "FIXED"),
            ("user_identity", str(record.get("user_identity") or self.authenticated_user), "TEXT"),
            ("active_role", str(record.get("active_role") or self.active_role), "TEXT"),
            ("context_epoch", int(record.get("context_epoch") or self.context_epoch), "FIXED"),
            ("flow_context_json", self._safe_json_text(record.get("flow_context_json") or {}, max_chars=8000, default_empty="{}"), "JSON"),
            ("input_payload", self._safe_json_text(record.get("input_payload") or {}, max_chars=4000, default_empty="{}"), "JSON"),
            ("output_payload", self._safe_json_text(record.get("output_payload") or {}, max_chars=4000, default_empty="{}"), "JSON"),
            ("node_spans_json", self._safe_json_text(record.get("node_spans_json") or [], max_chars=8000, default_empty="[]"), "JSON"),
            ("snowflake_query_ids", self._safe_json_text(list(record.get("snowflake_query_ids") or []), max_chars=2000, default_empty="[]"), "JSON"),
        ]
        return act_id, cols

    def _build_activity_merge_sql(self, record: dict[str, Any]) -> str:
        """SQL text of ``_build_activity_merge_request`` (``?`` placeholders; execute with its bindings)."""
        return self._build_activity_merge_request(record)[0]

    SPAN_MERGE_BATCH_ROWS = 50

    def _flush_merge_batch(
        self, table: str, key_col: str, batch: list[tuple[str, list[tuple[str, Any, str]], tuple[str, str]]]
    ) -> int:
        """Rerun-4: write queued rows as multi-row MERGEs of ``SPAN_MERGE_BATCH_ROWS``; returns rows accepted.

        A row is marked synced (``_synced_tx_ids`` / ``_synced_vertex_build_ids`` / ``_synced_activity_ids``) only
        when its chunk was accepted, so a rejected chunk is re-read and retried on the next sync (never dropped).
        """
        marks = {
            "tx": self._synced_tx_ids,
            "vb": self._synced_vertex_build_ids,
            "act": self._synced_activity_ids,
        }
        written = 0
        batch = self._drop_existing_keys(table, key_col, batch, marks)
        size = max(1, int(self.SPAN_MERGE_BATCH_ROWS))
        for i in range(0, len(batch), size):
            chunk = batch[i : i + size]
            res = self._execute_bound(
                self._merge_insert_batch_request(table, key_col, [(k, cols) for k, cols, _ in chunk]), timeout=30
            )
            if isinstance(res, dict) and res.get("ok") is False:
                # Rejected chunk: re-read by id on the next sync even if it left the newest-N window.
                pend: dict[str, float] = self.__dict__.setdefault("_telemetry_pending_since", {})
                for _k, _cols, (kind, rid) in chunk:
                    if kind in ("tx", "vb"):
                        pend.setdefault(f"{kind}:{rid}", time.monotonic())
                continue
            for _k, _cols, (kind, rid) in chunk:
                marks[kind].add(rid)
            written += len(chunk)
        return written

    PENDING_REREAD_CAP = 5000
    EXISTING_KEY_PROBE_ROWS = 1000
    NODE_SPANS_PREVIEW_ROWS = 64

    def _drop_existing_keys(
        self,
        table: str,
        key_col: str,
        batch: list[tuple[str, list[tuple[str, Any, str]], tuple[str, str]]],
        marks: dict[str, set[str]],
    ) -> list[tuple[str, list[tuple[str, Any, str]], tuple[str, str]]]:
        """v0.8.6-20261112b: skip rows already in ``table`` (one keys-only SELECT per 1000 rows).

        After a restart (or a targeted per-flow read of up to 2000 historical rows) the in-memory synced sets are
        empty, so every old row used to be re-MERGEd 50 at a time (~1.3 s each, 0 rows inserted). That held the
        sync lock for minutes and pushed the just-closed runs' spans to the 2nd/3rd sync (+100 s / +290 s live).
        Existing keys are marked synced; on a failed probe the batch is written unchanged (MERGE stays idempotent).
        """
        if not batch:
            return batch
        existing: set[str] = set()
        size = max(1, int(self.EXISTING_KEY_PROBE_ROWS))
        keys = list(dict.fromkeys(str(k) for k, _c, _m in batch))
        for i in range(0, len(keys), size):
            part = keys[i : i + size]
            b = _sql_safety.BindingBuilder()
            phs = ", ".join(b.add(k, "TEXT") for k in part)
            try:
                res = self.execute_sql(
                    f"SELECT {key_col} FROM {table} WHERE {key_col} IN ({phs})",
                    role=self._service_role(),
                    mode=_sf_identity.MODE_SERVICE,
                    bindings=b.bindings or None,
                    timeout=30,
                )
            except Exception:  # noqa: BLE001
                return batch
            if not isinstance(res, dict) or res.get("ok") is False:
                return batch
            for row in res.get("data") or []:
                val = row.get(key_col) or row.get(key_col.upper()) if isinstance(row, dict) else (row[0] if row else None)
                if val is not None:
                    existing.add(str(val))
        if not existing:
            return batch
        kept = []
        for item in batch:
            if str(item[0]) in existing:
                kind, rid = item[2]
                if kind in marks:
                    marks[kind].add(rid)
            else:
                kept.append(item)
        return kept

    def _telemetry_row_pending(self, key: str, ts: Any, flow_id: Any = None) -> bool:
        """T10: an ambiguous span row waits at most one match grace window, then is written with NULL run_id.

        The window is measured from when this process first saw the row (monotonic), never from the row's own
        timestamp, so a future-dated or skewed row cannot be held forever. Rows still pending are re-fetched by
        id on the next sync (``_pending_row_ids``), so they cannot fall out of the bounded newest-N fetch.
        v0.8.6-20261112: the grace ends early once every candidate run of ``flow_id`` is closed (no stamp can
        arrive any more), so spans land on the close-triggered sync instead of ~120 s later.
        """
        pending: dict[str, float] = self.__dict__.setdefault("_telemetry_pending_since", {})
        flows: dict[str, str] = self.__dict__.setdefault("_telemetry_pending_flow", {})
        first = pending.setdefault(key, time.monotonic())
        if flow_id:
            flows[key] = _telemetry.canonical_flow_id(flow_id) or str(flow_id)
        closed = bool(flow_id) and _telemetry.RUNS.candidates_all_closed(flow_id, ts)
        if not closed and time.monotonic() - first < _telemetry.RUNS.match_grace_s:
            return True
        self._resolve_pending(key)
        return False

    def _pending_row_ids(self, prefix: str) -> list[str]:
        pending: dict[str, float] = self.__dict__.get("_telemetry_pending_since") or {}
        return [k[len(prefix):] for k in list(pending) if k.startswith(prefix)][: int(self.PENDING_REREAD_CAP)]

    def _resolve_pending(self, key: str) -> None:
        (self.__dict__.get("_telemetry_pending_since") or {}).pop(key, None)
        (self.__dict__.get("_telemetry_pending_flow") or {}).pop(key, None)

    def _pending_flows(self) -> set[str]:
        pending: dict[str, float] = self.__dict__.get("_telemetry_pending_since") or {}
        flows: dict[str, str] = self.__dict__.get("_telemetry_pending_flow") or {}
        return {flows[k] for k in list(pending) if k in flows}

    def _arm_pending_expiry_sync(self) -> None:
        """After a sync that left rows pending, arm ONE timer for the earliest grace expiry (not the 15 s tick)."""
        pending: dict[str, float] = self.__dict__.get("_telemetry_pending_since") or {}
        if not pending:
            return
        guard = self.__dict__.setdefault("_close_sync_guard", threading.Lock())
        with guard:
            if self.__dict__.get("_pending_expiry_timer_armed"):
                return
            self._pending_expiry_timer_armed = True
        earliest = min(pending.values(), default=time.monotonic())
        delay = max(0.5, earliest + float(_telemetry.RUNS.match_grace_s) - time.monotonic() + 0.1)

        def _fire() -> None:
            with guard:
                self._pending_expiry_timer_armed = False
            try:
                self.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
            except Exception:  # noqa: BLE001
                pass

        timer = threading.Timer(delay, _fire)
        timer.daemon = True
        timer.start()

    def _submit_telemetry(self, request: tuple[str, dict[str, Any]], query_tag: str | None = None) -> None:
        """PASSO 8: telemetry DML runs off the turn's critical path (batched writer) in live mode."""
        sql, binds = request
        if self._has_spcs_token() and not self._is_execute_sql_mocked():
            _TELEMETRY_WRITER.submit(sql, binds or None, query_tag, ctx=_telemetry_ctx())
        else:
            result = self.execute_sql(sql, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, bindings=binds or None, query_tag=query_tag)
            _telemetry.check_write_result(result, sql)

    def _build_run_merge_request(self, rec: Any, *, extra: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        """BUG-LLMOP-A4-01: one LANGFLOW_FLOW_RUNS row per flow execution with real ids and timestamps."""
        extra = extra or {}
        start_iso = rec.start_ts.isoformat()
        end_iso = (rec.end_ts or rec.start_ts).isoformat()
        latency = max(0.0, ((rec.end_ts or rec.start_ts) - rec.start_ts).total_seconds() * 1000.0)
        cols: list[tuple[str, Any, str]] = [
            ("event_id", f"evr-{rec.run_id}", "TEXT"),
            ("run_id", rec.run_id, "TEXT"),
            ("trace_id", rec.trace_id, "TEXT"),
            ("project_id", str(extra.get("project_id") or "default"), "TEXT"),
            ("flow_id", rec.flow_id, "TEXT"),
            ("session_id", rec.session_id, "TEXT"),
            ("authenticated_user", self.authenticated_user, "TEXT"),
            ("effective_role", str(extra.get("effective_role") or self.active_role), "TEXT"),
            ("execution_mode", _trace.audit_execution_mode(_sf_identity.origin_mode()), "TEXT"),
            ("start_ts", start_iso, "TS"),
            ("end_ts", end_iso, "TS"),
            ("latency_ms", latency, "REAL"),
            ("status", rec.status, "TEXT"),
            ("cortex_thread_id", rec.cortex_thread_id, "TEXT"),
            ("cortex_parent_message_id", rec.cortex_parent_message_id, "TEXT"),
            ("a2a_task_id", rec.a2a_task_id, "TEXT"),
            ("input_payload", self._safe_json_text({"request_id": rec.request_id}, max_chars=512), "JSON"),
            ("output_payload", self._safe_json_text({"status": rec.status}, max_chars=512), "JSON"),
        ]
        return self._merge_insert_request(self.runs_table, "run_id", rec.run_id, cols)

    def record_flow_run(self, rec: Any) -> None:
        """Persist a finished flow execution (RUNS) asynchronously; spans/activity reuse its run_id/trace_id."""
        if rec is None:
            return
        tag = build_query_tag(flow_id=rec.flow_id, run_id=rec.run_id, component="telemetry")
        try:
            self._submit_telemetry(self._build_run_merge_request(rec), tag)
            self.synced_runs_count += 1
        except Exception as exc:  # noqa: BLE001 - telemetry never breaks a run
            logging.getLogger("lfx_snowflake.telemetry").warning(
                "sflf telemetry RUN record failed: run_id=%s error=%s", rec.run_id, type(exc).__name__
            )

    def flush_telemetry(self, timeout: float | None = None) -> bool:
        return _TELEMETRY_WRITER.flush(timeout)

    def record_audit_event(
        self,
        *,
        action: str,
        outcome: str,
        error_code: str | None = None,
        target_object: str | None = None,
        details: dict[str, Any] | None = None,
        flow_id: str | None = None,
        run_id: str | None = None,
        event_id: str | None = None,
        effective_role: str | None = None,
    ) -> str:
        if action in DEPRECATED_AUDIT_ACTIONS:
            raise ValueError(f"deprecated audit action: {action}")
        run_id = run_id if run_id is not None else CURRENT_RUN_ID.get()
        # TR-01: idempotent id, so a retried write cannot duplicate the event.
        event_id = event_id or deterministic_event_id(
            action, target_object, run_id, CURRENT_REQUEST_ID.get(), json.dumps(details or {}, sort_keys=True, default=str)
        )
        audit_query_tag = build_query_tag(flow_id=flow_id, run_id=run_id, component="audit")
        details = dict(details or {})
        # TR-06: background writes run as the service; record who originated them (attribution only).
        if _sf_identity.origin_user() and "origin_user" not in details:
            details["origin_user"] = _sf_identity.origin_user()
        # Effective mode of the originating request (service whenever the caller's-rights flag is OFF).
        eff_mode = _sf_identity.origin_mode()
        details.setdefault("execution_mode", eff_mode)
        # BUG-UI-TRC-01: correlation ids of the originating request/run (REDACTED_DETAILS; no REQUEST_ID column).
        run_rec = _telemetry.CURRENT_RUN_RECORD.get() or _telemetry.RUNS.get(run_id)
        if CURRENT_REQUEST_ID.get():
            details.setdefault("request_id", CURRENT_REQUEST_ID.get())
        if run_rec is not None:
            for _k in ("session_id", "cortex_thread_id", "a2a_task_id"):
                if getattr(run_rec, _k, None):
                    details.setdefault(_k, getattr(run_rec, _k))
        # TR-01: enforce the metadata allowlist for every caller (no content can reach the audit row).
        details = sanitize_details(details)
        redacted = redact_for_sink(details)
        details_json = json.dumps(redacted)
        if len(details_json) > 12000:
            details_json = json.dumps({"summary": "truncated_large_audit_payload", "keys": list(redacted.keys())})
        eff_fid = flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get()
        stmt, audit_bindings = build_audit_insert(
            self.audit_table,
            event_id=event_id,
            authenticated_user=self.authenticated_user,
            effective_role=self._effective_role_for_audit(eff_mode, effective_role),
            execution_mode=_trace.audit_execution_mode(eff_mode),
            context_epoch=int(self.context_epoch or 0),
            action=action,
            outcome=outcome,
            error_code=error_code,
            target_object=target_object,
            flow_id=eff_fid,
            run_id=run_id,
            details=None,
            trace_id=(run_rec.trace_id if run_rec is not None else None) or run_id or CURRENT_REQUEST_ID.get(),
        )
        # Keep the existing redaction path for details (redact_for_sink + 12 KB cap), still bound, never inlined.
        audit_bindings[str(len(audit_bindings) - 1)] = {"type": "TEXT", "value": details_json}
        ev_type = (
            "CONTEXT_CHANGE"
            if action in ("UPDATE_CONTEXT", "OBJECT_REMOVED_FROM_CONTEXT", "REGISTER_OBJECT_IN_CONTEXT")
            else "GOVERNANCE_AUDIT"
        )
        norm_fid = self._normalize_flow_id(eff_fid) or str(eff_fid or "system-governance")
        now_iso = datetime.now(timezone.utc).isoformat()
        act_record = {
            "id": event_id,
            "activity_id": event_id,
            "trace_id": str((run_rec.trace_id if run_rec is not None else None) or run_id or event_id),
            "name": f"{action} ({target_object or norm_fid})",
            "event_type": ev_type,
            "flow_id": norm_fid,
            "flow_name": norm_fid,
            "project_id": "default",
            "project_name": "Default Project",
            "session_id": (run_rec.session_id if run_rec is not None else None) or norm_fid,
            "timestamp": now_iso,
            "run_timestamp": now_iso,
            "status": "SUCCESS" if outcome in ("ALLOWED", "SUCCESS") else outcome,
            "latency_ms": 0.0,
            "total_latency_ms": 0.0,
            "input": action,
            "output": str(target_object or outcome),
            "input_text": action,
            "output_text": str(target_object or outcome),
            "error_message": error_code,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "user_identity": self.authenticated_user,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "flow_context_json": copy.deepcopy(self.contexts_by_flow_id.get(norm_fid, self.selected_context)),
            "input_payload": redacted,
            "output_payload": {"outcome": outcome, "target_object": target_object, "error_code": error_code},
            "node_spans_json": [],
            "spans": [],
            "snowflake_query_ids": [],
            "deleted_in_ui_at": None,
        }
        self._recent_activity_events.insert(0, act_record)
        if len(self._recent_activity_events) > 200:
            self._recent_activity_events = self._recent_activity_events[:200]

        # In live SPCS mode, dispatch audit INSERT and LANGFLOW_FLOW_ACTIVITY MERGE asynchronously so UI requests never block on Snowflake DML
        if self._has_spcs_token() and not self._is_execute_sql_mocked():
            self._submit_telemetry((stmt, audit_bindings), audit_query_tag)
            self._submit_telemetry(self._build_activity_merge_request(act_record), audit_query_tag)
            self._synced_activity_ids.add(event_id)
        else:
            self.execute_sql(stmt, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, bindings=audit_bindings)
        return event_id

    def record_trigger_execution_telemetry(
        self,
        *,
        flow_id: str,
        run_id: str,
        trace_id: str,
        trigger_type: str,
        status: str = "SUCCEEDED",
        duration_ms: float = 10.0,
        trigger_payload: dict[str, Any] | None = None,
        result_payload: dict[str, Any] | None = None,
        active_role: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        """Record 3-tier trigger telemetry (`LANGFLOW_FLOW_ACTIVITY`, `LANGFLOW_FLOW_RUNS`, `LANGFLOW_FLOW_SPANS`, `LANGFLOW_AUDIT_LOG`)."""
        norm_fid = self._normalize_flow_id(flow_id) or str(flow_id or "default-flow")
        eff_role = str(active_role or self.active_role or "SNOWFLAKE_LANGFLOW_ADMIN_ROLE").strip()
        ev_type = str(trigger_type or "SCHEDULED_RUN").strip().upper()
        now_iso = datetime.now(timezone.utc).isoformat()
        flw_ctx = copy.deepcopy(self.contexts_by_flow_id.get(norm_fid, self.selected_context))
        in_pay = redact_for_sink(trigger_payload or {})
        out_pay = redact_for_sink(result_payload or {"status": status, "run_id": run_id})
        span_id = f"spn-{run_id[:20]}"
        node_spans = [
            {
                "span_id": span_id,
                "vertex_id": ev_type,
                "component_name": ev_type,
                "timestamp": now_iso,
                "latency_ms": float(duration_ms or 0.0),
                "status": status,
            }
        ]
        act_record = {
            "id": run_id,
            "activity_id": run_id,
            "trace_id": trace_id,
            "name": f"{ev_type} ({norm_fid})",
            "event_type": ev_type,
            "flow_id": norm_fid,
            "flow_name": norm_fid,
            "project_id": "default",
            "project_name": "Default Project",
            "session_id": norm_fid,
            "timestamp": now_iso,
            "run_timestamp": now_iso,
            "status": status,
            "latency_ms": float(duration_ms or 0.0),
            "total_latency_ms": float(duration_ms or 0.0),
            "input": json.dumps(in_pay, default=str)[:1000],
            "output": json.dumps(out_pay, default=str)[:1000],
            "input_text": json.dumps(in_pay, default=str)[:1000],
            "output_text": json.dumps(out_pay, default=str)[:1000],
            "error_message": error_message,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "user_identity": self.authenticated_user,
            "active_role": eff_role,
            "context_epoch": self.context_epoch,
            "flow_context_json": flw_ctx,
            "input_payload": in_pay,
            "output_payload": out_pay,
            "node_spans_json": node_spans,
            "spans": node_spans,
            "snowflake_query_ids": [],
            "deleted_in_ui_at": None,
        }
        self._recent_activity_events.insert(0, act_record)
        if len(self._recent_activity_events) > 200:
            self._recent_activity_events = self._recent_activity_events[:200]

        act_req = self._build_activity_merge_request(act_record)
        in_json = self._safe_json_text(in_pay, max_chars=4000, default_empty="{}")
        out_json = self._safe_json_text(out_pay, max_chars=4000, default_empty="{}")
        safe_status = str(status or "SUCCEEDED")
        dur = float(duration_ms or 0.0)
        run_req = self._merge_insert_request(
            self.runs_table,
            "run_id",
            run_id,
            [
                ("event_id", f"evr-{run_id[:16]}", "TEXT"),
                ("run_id", run_id, "TEXT"),
                ("trace_id", trace_id, "TEXT"),
                ("project_id", "default", "TEXT"),
                ("flow_id", norm_fid, "TEXT"),
                ("authenticated_user", self.authenticated_user, "TEXT"),
                ("effective_role", eff_role, "TEXT"),
                ("execution_mode", ev_type, "TEXT"),
                ("start_ts", None, "NOW"),
                ("end_ts", None, "NOW"),
                ("latency_ms", dur, "REAL"),
                ("status", safe_status, "TEXT"),
                ("input_payload", in_json, "JSON"),
                ("output_payload", out_json, "JSON"),
            ],
        )
        span_req = self._merge_insert_request(
            self.spans_table,
            "span_id",
            span_id,
            [
                ("event_id", f"evs-{run_id[:16]}", "TEXT"),
                ("span_id", span_id, "TEXT"),
                ("run_id", run_id, "TEXT"),
                ("trace_id", trace_id, "TEXT"),
                ("project_id", "default", "TEXT"),
                ("flow_id", norm_fid, "TEXT"),
                ("component_id", ev_type, "TEXT"),
                ("component_name", ev_type, "TEXT"),
                ("authenticated_user", self.authenticated_user, "TEXT"),
                ("effective_role", eff_role, "TEXT"),
                ("execution_mode", ev_type, "TEXT"),
                ("start_ts", None, "NOW"),
                ("end_ts", None, "NOW"),
                ("latency_ms", dur, "REAL"),
                ("status", safe_status, "TEXT"),
                ("redacted_inputs", in_json, "JSON"),
                ("redacted_outputs", out_json, "JSON"),
            ],
        )

        trg_tag = build_query_tag(flow_id=norm_fid, run_id=run_id, component=f"trigger:{ev_type}")
        for _req in (act_req, run_req, span_req):
            self._submit_telemetry(_req, trg_tag)

        self.record_audit_event(
            action=f"TRIGGER_{ev_type}",
            outcome=status,
            error_code="TRIGGER_EXECUTION_FAILED" if status == "FAILED" else None,
            target_object=norm_fid,
            flow_id=norm_fid,
            run_id=run_id,
            details={"trigger_type": ev_type, "duration_ms": duration_ms, "trigger_payload": in_pay},
        )
        return act_record

    def invoke_service_trigger_batch(
        self, batch_body: dict[str, Any], main_loop: asyncio.AbstractEventLoop | None = None
    ) -> dict[str, Any]:
        """Execute SPCS Service Function `/api/v1/snowflake/service-trigger` batch protocol.

        BUG-TRG-04: `main_loop` is the server event loop; flows run natively on it (Langflow DB/services).
        """
        from lfx_snowflake.triggers_manager import TRIGGERS_MANAGER  # noqa: PLC0415

        return TRIGGERS_MANAGER.execute_service_trigger_batch(batch_body, bridge=self, main_loop=main_loop)

    def sync_flow_triggers(self, flow_id: str, flow_payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Provision/update Snowflake Tasks, Streams, and Alerts for a flow and drop removed trigger objects."""
        from lfx_snowflake.triggers_manager import TRIGGERS_MANAGER  # noqa: PLC0415

        return TRIGGERS_MANAGER.sync_flow_payload_triggers(flow_id, flow_payload, bridge=self)

    def cleanup_flow_triggers(self, flow_id: str) -> dict[str, Any]:
        """Suspend and drop all Snowflake Tasks, Streams, and Alerts registered for `flow_id`."""
        from lfx_snowflake.triggers_manager import TRIGGERS_MANAGER  # noqa: PLC0415

        return TRIGGERS_MANAGER.cleanup_flow_triggers(flow_id, bridge=self)

    def list_flow_triggers(self, flow_id: str | None = None) -> list[dict[str, Any]]:
        """List registered Snowflake triggers (optionally filtered by `flow_id`)."""
        from lfx_snowflake.triggers_manager import TRIGGERS_MANAGER  # noqa: PLC0415

        return TRIGGERS_MANAGER.list_triggers(flow_id=flow_id)

    def set_trigger_status(self, flow_id: str, node_id: str, status_mode: str) -> dict[str, Any]:
        """Toggle a registered Snowflake trigger between `Active` (`RESUME`) and `Paused` (`SUSPEND`)."""
        from lfx_snowflake.triggers_manager import TRIGGERS_MANAGER  # noqa: PLC0415

        return TRIGGERS_MANAGER.set_trigger_status(
            flow_id=flow_id,
            node_id=node_id,
            status_mode=status_mode,
            bridge=self,
        )

    def reconcile_triggers_with_flow_context(self, flow_id: str | None = None) -> dict[str, Any]:
        """Auto-suspend active triggers whose Database/Schema/Table/Stage was removed from `[ ❄️ Context ]` or role."""
        from lfx_snowflake.triggers_manager import TRIGGERS_MANAGER  # noqa: PLC0415

        try:
            return TRIGGERS_MANAGER.reconcile_triggers_with_flow_context(flow_id=flow_id, bridge=self)
        except Exception:  # noqa: BLE001
            return {"ok": True, "flow_id": flow_id, "auto_paused": [], "statements": []}

    def _caller_holds_role(self, role: str) -> bool:
        """True only if Snowflake activates ``role`` for the caller (caller's rights when enabled).

        Fails closed: offline, errors, or a different CURRENT_ROLE() all mean "not granted".
        """
        check = self.execute_sql(
            "SELECT CURRENT_ROLE() AS VERIFIED_ROLE", role=role, timeout=12, mode=_sf_identity.MODE_CALLER
        )
        rows = check.get("rows") or []
        verified = str((rows[0] or {}).get("VERIFIED_ROLE") or "").strip().upper() if rows else ""
        return bool(check.get("ok")) and verified == role

    def _audit_role_switch(
        self,
        outcome: str,
        new_role: str,
        flow_id: str | None,
        previous_role: str | None,
        channel: str,
        error_code: str | None = None,
        **extra: Any,
    ) -> None:
        """Rerun-3: one LANGFLOW_AUDIT_LOG ROLE_SWITCH row per REST/MCP switch attempt (allowed or denied).

        Metadata only (allowlisted): user, flow, old/new role, channel, outcome; request_id/trace come from
        ``record_audit_event``. A failed audit write never changes the switch result.
        """
        try:
            self.record_audit_event(
                action="ROLE_SWITCH",
                outcome=outcome,
                error_code=error_code,
                target_object=new_role or None,
                flow_id=flow_id,
                details={
                    "user": self._role_session_user() or self.authenticated_user,
                    "flow_id": flow_id,
                    "previous_role": previous_role,
                    "new_role": new_role,
                    "requested_role": new_role,
                    "channel": channel,
                    "outcome": outcome,
                    "error_code": error_code,
                    **extra,
                },
            )
        except Exception as exc:  # noqa: BLE001 - auditing never breaks the switch
            logging.getLogger("lfx_snowflake.telemetry").warning(
                "sflf ROLE_SWITCH audit failed: outcome=%s error=%s", outcome, type(exc).__name__
            )

    def switch_role(self, target_role: str, flow_id: str | None = None, *, channel: str = "REST") -> dict[str, Any]:
        clean = _sf_identity.validate_role_identifier(target_role) or ""
        audit_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        audit_prev = self.current_session_role() or self.active_role
        # BUG-MCP-03: REST and MCP share one policy. Privileged roles are selectable only after a live grant
        # check under the caller's identity; every other role must be in the discovered role list.
        privileged = clean in _sf_identity.PRIVILEGED_ROLES
        if privileged:
            role_allowed = self._caller_holds_role(clean)
        else:
            role_allowed = _sf_identity.is_selectable_role(clean, self.available_roles)
        if not role_allowed:
            self._audit_role_switch(
                "DENIED", clean, audit_fid, audit_prev, channel, ErrorCode.AUTHZ_ROLE_DENIED.value,
                available_roles=self.available_roles,
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": f"Il ruolo attivo ({self.active_role}) non consente il passaggio al ruolo non concesso '{clean}'.",
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        # Verify role activation on Snowflake (already done live for privileged roles)
        check = {"ok": True} if privileged else self.execute_sql(
            "SELECT CURRENT_ROLE() AS VERIFIED_ROLE", role=clean, timeout=12
        )
        if not check.get("ok") and self._has_spcs_token():
            self._audit_role_switch("DENIED", clean, audit_fid, audit_prev, channel, ErrorCode.AUTHZ_ROLE_DENIED.value)
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": f"Il ruolo attivo ({self.active_role}) non consente questa operazione: impossibile attivare {clean}.",
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        if _sf_identity.callers_rights_enabled():
            # W3-R1-10d: display-only. Under caller's rights the shared bridge role is never mutated, so one
            # user's selection cannot change what other users run as; Snowflake enforces the caller's grants.
            # E2E T2: the selection is still recorded for *this* authenticated user and flow, so caller-mode
            # execute_sql sends it as the SQL API role (otherwise the token default role ran, e.g. ACCOUNTADMIN).
            if self._role_session_user():
                self._set_session_role(clean, audit_fid)
            # Rerun-3: the caller's-rights switch was never audited (no ROLE_SWITCH row on REST/MCP).
            self._audit_role_switch(
                "ALLOWED", clean, audit_fid, audit_prev, channel, display_only=True, context_epoch=self.context_epoch
            )
            return {
                "ok": True,
                "display_only": True,
                "active_role": clean,
                "service_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        # BUG-CA-02: the selection is scoped to (authenticated_user, flow_id); the shared bridge role is untouched.
        session_fid = self._normalize_flow_id(flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        flow_token = CURRENT_SNOWFLAKE_FLOW_ID.set(session_fid)
        try:
            return self._apply_session_role_switch(clean, flow_id, session_fid, channel=channel)
        finally:
            CURRENT_SNOWFLAKE_FLOW_ID.reset(flow_token)

    def _apply_session_role_switch(
        self, clean: str, flow_id: str | None, session_fid: str | None, *, channel: str = "REST"
    ) -> dict[str, Any]:
        previous_role = self.active_role
        self._set_session_role(clean, session_fid)
        # CTX-ARCH-07 #1: Non-destructive role switch — only the switched flow's context is re-stamped.
        if session_fid is None:
            self.selected_context["active_role"] = self.active_role
            self.selected_context["context_epoch"] = self.context_epoch
        f_ctx = self.contexts_by_flow_id.get(session_fid) if session_fid else None
        if isinstance(f_ctx, dict):
            f_ctx["active_role"] = self.active_role
            f_ctx["context_epoch"] = self.context_epoch
        self.invalidate_catalog_cache()

        # BUG-MCP-03: never fall back to the process-global active flow; the session flow is authoritative.
        eff_fid = session_fid
        intersection = self.compute_role_effective_context(eff_fid, self.active_role)
        updated_node_templates = self.sync_flow_node_templates_sqlite(eff_fid) if eff_fid else {}
        # BUG-MCP-03: a user-wide switch has no flow scope; reconciling with flow_id=None would pause
        # every user's triggers, so it is skipped and runs on the next flow-scoped switch or context change.
        reconciled_triggers = (
            self.reconcile_triggers_with_flow_context(eff_fid)
            if eff_fid
            else {"ok": True, "flow_id": None, "auto_paused": [], "statements": [], "skipped": "user_wide_switch"}
        )

        self._audit_role_switch(
            "ALLOWED",
            clean,
            eff_fid,
            previous_role,
            channel,
            context_epoch=self.context_epoch,
            retained_objects_count=intersection["retained_objects_count"],
            hidden_by_role_count=intersection["hidden_by_role_count"],
        )
        return {
            "ok": True,
            "status_code": 200,
            "previous_role": previous_role,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "secondary_roles_mode": self.secondary_roles_mode,
            "role_privileges": self.role_privileges,
            "flow_id": eff_fid,
            "selected_context": intersection["stored_context"],
            "effective_context": intersection["effective_context"],
            "inaccessible_objects": intersection["inaccessible_objects"],
            "retained_objects_count": intersection["retained_objects_count"],
            "hidden_by_role_count": intersection["hidden_by_role_count"],
            "updated_node_templates": updated_node_templates,
            "reconciled_triggers": reconciled_triggers,
            "cache_invalidated": True,
        }

    def set_context(self, payload: dict[str, Any], flow_id: str | None = None) -> dict[str, Any]:
        """Alias for `update_selected_context` used by Snowsight hierarchical tree context sync."""
        return self.update_selected_context(payload, flow_id=flow_id)

    def check_write_privilege_or_deny(self, action: str, target_object: str) -> dict[str, Any] | None:
        """Fail-closed role check: VIEWER and PUBLIC roles are explicitly denied DDL/write actions."""
        if not self.can_create_object() or self.active_role.upper() in (
            "PUBLIC",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "VIEWER_ROLE",
            "ROLE_VIEWER",
        ):
            if str(action).upper().startswith("MEMORY"):
                msg = (
                    f"Active role ({self.active_role}) does not have privileges to create or modify this object "
                    f"in {self.database}.{self.schema} ({action} on {target_object})."
                )
            else:
                msg = (
                    f"Active role ({self.active_role}) does not have privileges to create this object "
                    f"in {self.database}.{self.schema} ({action} on {target_object}) "
                    f"[Il ruolo attivo ({self.active_role})]."
                )
            self.record_audit_event(
                action=AuditAction.AUTHZ_ROLE_DENIED.value,
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=target_object,
                details={"required_privilege": action, "object": target_object},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "error": {
                    "code": "PERMISSION_DENIED",
                    "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                    "message": msg,
                    "role": self.active_role,
                    "required_privilege": action,
                    "object_fqn": target_object,
                },
                "message": msg,
                "active_role": self.active_role,
                "role": self.active_role,
                "required_privilege": action,
                "object_fqn": target_object,
                "context_epoch": self.context_epoch,
                "role_privileges": self.role_privileges,
            }
        return None

    def soft_delete_flow_activity(
        self,
        activity_ids: list[str] | str | None = None,
        *,
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Soft-delete Flow Activity items in the UI (`DELETED_IN_UI_AT = CURRENT_TIMESTAMP()`) without deleting from Snowflake (`CTX-ARCH-07` #5)."""
        ids: list[str] = []
        if isinstance(activity_ids, str) and activity_ids.strip():
            ids = [x.strip() for x in activity_ids.split(",") if x.strip()]
        elif isinstance(activity_ids, list):
            ids = [str(x).strip() for x in activity_ids if str(x).strip()]

        norm_fid = self._normalize_flow_id(flow_id)
        now_iso = datetime.now(timezone.utc).isoformat()
        for item_id in ids:
            self._soft_deleted_activity_ids.add(item_id)
            if not item_id.startswith("act-"):
                self._soft_deleted_activity_ids.add(f"act-tx-{item_id[:24]}")
                self._soft_deleted_activity_ids.add(f"act-vb-{item_id[:24]}")
                self._soft_deleted_activity_ids.add(f"run-{item_id[:16]}")
                self._soft_deleted_activity_ids.add(f"vb-{item_id}")

        for ev in self._recent_activity_events:
            ev_id = str(ev.get("activity_id") or ev.get("id") or "")
            ev_trc = str(ev.get("trace_id") or "")
            ev_fid = self._normalize_flow_id(ev.get("flow_id")) or str(ev.get("flow_id") or "")
            if (
                ev_id in self._soft_deleted_activity_ids
                or ev_trc in self._soft_deleted_activity_ids
                or (norm_fid and ev_fid == norm_fid and not ids)
            ):
                ev["deleted_in_ui_at"] = now_iso

        executed_updates: list[str] = []
        if ids:
            quoted_ids = ", ".join(_sql_safety.sql_string_literal(str(i)) for i in sorted(self._soft_deleted_activity_ids))
            upd_act = (
                f"UPDATE {self.activity_table} "
                f"SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP() "
                f"WHERE (ACTIVITY_ID IN ({quoted_ids}) OR TRACE_ID IN ({quoted_ids})) "
                f"AND DELETED_IN_UI_AT IS NULL"
            )
            upd_runs = (
                f"UPDATE {self.runs_table} "
                f"SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP() "
                f"WHERE (RUN_ID IN ({quoted_ids}) OR TRACE_ID IN ({quoted_ids})) "
                f"AND DELETED_IN_UI_AT IS NULL"
            )
            upd_spans = (
                f"UPDATE {self.spans_table} "
                f"SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP() "
                f"WHERE (SPAN_ID IN ({quoted_ids}) OR RUN_ID IN ({quoted_ids})) "
                f"AND DELETED_IN_UI_AT IS NULL"
            )
            for stmt in (upd_act, upd_runs, upd_spans):
                self.execute_sql(stmt, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, timeout=12)
                executed_updates.append(stmt)
        elif norm_fid:
            safe_fid = norm_fid.replace("\\", "\\\\").replace("'", "''")
            upd_act = (
                f"UPDATE {self.activity_table} "
                f"SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP() "
                f"WHERE FLOW_ID = '{safe_fid}' AND DELETED_IN_UI_AT IS NULL"
            )
            self.execute_sql(upd_act, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, timeout=12)
            executed_updates.append(upd_act)

        return {
            "ok": True,
            "soft_deleted_ids": ids,
            "flow_id": norm_fid,
            "deleted_in_ui_at": now_iso,
            "physical_delete_executed": False,
            "snowflake_retained": True,
            "executed_updates": executed_updates,
        }

    def get_ui_flow_activity_traces(
        self,
        *,
        flow_id: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Return active Flow Activity runs/traces (`deleted_in_ui_at IS NULL`) formatted for Langflow `/api/v2/traces` (`CTX-ARCH-06`)."""
        self.sync_langflow_traces_to_snowflake(limit=min(limit, 50), replicate_sqlite=True)
        norm_fid = self._normalize_flow_id(flow_id)
        items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for ev in self._recent_activity_events:
            ev_id = str(ev.get("activity_id") or ev.get("id") or "")
            ev_trc = str(ev.get("trace_id") or "")
            if (
                not ev_id
                or ev_id in seen_ids
                or ev.get("deleted_in_ui_at") is not None
                or ev_id in self._soft_deleted_activity_ids
                or ev_trc in self._soft_deleted_activity_ids
            ):
                continue
            ev_fid = self._normalize_flow_id(ev.get("flow_id")) or str(ev.get("flow_id") or "")
            if norm_fid and ev_fid != norm_fid:
                continue
            seen_ids.add(ev_id)
            items.append(copy.deepcopy(ev))
            if len(items) >= limit:
                break

        return {
            "ok": True,
            "traces": items,
            "items": items,
            "total": len(items),
            "page": 1,
            "size": int(limit or 50),
            "pages": 1,
            "activity_table": self.activity_table,
        }

    def sync_langflow_traces_to_snowflake(
        self, limit: int = 25, *, replicate_sqlite: bool = True, flow_ids: Any = None
    ) -> dict[str, Any]:
        """Replicate Langflow SQLite `transaction`, `vertex_build`, and `message` rows into Snowflake `LANGFLOW_FLOW_ACTIVITY`, `LANGFLOW_FLOW_RUNS`, & `LANGFLOW_FLOW_SPANS` (`CTX-ARCH-06`).

        Rerun-4: serialized (daemon, close trigger and the UI endpoint never write the same rows twice);
        ``flow_ids`` plus every flow whose RUN closed since the last sync are read in full.
        """
        with self.__dict__.setdefault("_trace_sync_lock", threading.Lock()):
            closed: set[str] = self.__dict__.setdefault("_close_sync_flows", set())
            targets = {str(f) for f in (flow_ids or []) if f} | closed
            closed.clear()
            hint = int(self.__dict__.get("_span_backlog_hint", 0))
            self._span_backlog_hint = 0
            result = self._sync_langflow_traces_locked(
                limit, replicate_sqlite=replicate_sqlite, target_flows=targets, backlog_hint=hint
            )
            # v0.8.6-20261112: targeted flows (and the backlog hint) stay armed while their rows are pending.
            still = self._pending_flows()
            keep = {f for f in targets if (_telemetry.canonical_flow_id(f) or f) in still}
            if keep:
                closed.update(keep)
                self._span_backlog_hint = max(int(self.__dict__.get("_span_backlog_hint", 0)), hint)
        self._arm_pending_expiry_sync()
        return result

    @staticmethod
    def _sqlite_flow_id_variants(flow_id: Any) -> set[str]:
        """Langflow stores ``flow_id`` as 32-hex or dashed UUID depending on the writer: match both."""
        raw = str(flow_id or "").strip()
        if not raw:
            return set()
        hexed = raw.replace("-", "").lower()
        out = {raw, hexed}
        if len(hexed) == 32:
            out.add(str(uuid.UUID(hexed)))
        return out

    CLOSE_SYNC_DEBOUNCE_S = 3.0
    # Round-3: Langflow writes vertex_build/transaction rows of API runs after the RUN closes, so the
    # debounced close sync can miss them. Follow-ups at these offsets (seconds after the close sync).
    CLOSE_FOLLOWUP_DELAYS_S: tuple[float, ...] = (10.0, 30.0)
    TARGET_READ_MAX_ROWS = 10000

    def _close_since_for(self, flow_keys: Any) -> float | None:
        """Oldest just-closed run start (epoch s) among ``flow_keys`` variants, or None when unknown."""
        since_map: dict[str, float] = self.__dict__.get("_close_sync_since") or {}
        vals = [
            since_map[v]
            for f in flow_keys
            for v in self._sqlite_flow_id_variants(f)
            if v in since_map
        ]
        return min(vals) if vals else None

    def _note_close_since(self, flow_id: Any, since: Any) -> None:
        when = _telemetry._parse_ts(since)
        if not flow_id or when is None:
            return
        since_map: dict[str, float] = self.__dict__.setdefault("_close_sync_since", {})
        t = when.timestamp()
        for v in self._sqlite_flow_id_variants(flow_id):
            since_map[v] = min(since_map.get(v, t), t)

    def _followup_flows_have_unreplicated(self, flows: set[str]) -> bool:
        """Cheap SQLite probe: any row of ``flows`` newer than its close window not yet replicated?"""
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        paths = [persist_dir / "langflow.db", self._resolve_writable_sqlite_db_path()]
        synced = {
            "transaction": self.__dict__.get("_synced_tx_ids") or set(),
            "vertex_build": self.__dict__.get("_synced_vertex_build_ids") or set(),
        }
        keys = sorted({v for f in flows for v in self._sqlite_flow_id_variants(f)})
        if not keys:
            return False
        since = self._close_since_for(keys)
        for db_path in dict.fromkeys(paths):
            if not db_path.exists():
                continue
            try:
                conn = _open_sqlite_conn(db_path, readonly=True, timeout=5.0)
                try:
                    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    for tbl, pk in (("transaction", "id"), ("vertex_build", "build_id")):
                        if tbl not in tables:
                            continue
                        for i in range(0, len(keys), 50):
                            fk = keys[i : i + 50]
                            for rid, ts in conn.execute(
                                f'SELECT {pk}, timestamp FROM "{tbl}" WHERE flow_id IN ({",".join("?" * len(fk))}) '
                                "ORDER BY timestamp DESC LIMIT ?",
                                (*fk, int(self.TARGET_READ_MAX_ROWS)),
                            ):
                                when = _telemetry._parse_ts(ts)
                                if since is not None and when is not None and when.timestamp() < since - 1.0:
                                    break
                                rid_s = str(rid or "")
                                if rid_s and rid_s not in synced[tbl]:
                                    return True
                finally:
                    conn.close()
            except Exception:  # noqa: BLE001
                return True  # cannot tell: sync (bounded by the follow-up schedule)
        return False

    def _arm_close_followups(self, flows: set[str]) -> None:
        """ONE armed follow-up chain (coalesced across closes): sync at each CLOSE_FOLLOWUP_DELAYS_S offset
        only while the targeted flows still have unreplicated SQLite rows; the chain ends after the last."""
        if not flows:
            return
        guard = self.__dict__.setdefault("_close_sync_guard", threading.Lock())
        with guard:
            self.__dict__.setdefault("_close_followup_flows", set()).update(flows)
            if self.__dict__.get("_close_followup_armed"):
                return
            self._close_followup_armed = True
        delays = list(self.CLOSE_FOLLOWUP_DELAYS_S)

        def _schedule(idx: int, prev: float) -> None:
            if idx >= len(delays):
                with guard:
                    self._close_followup_armed = False
                    done = set(self.__dict__.get("_close_followup_flows") or set())
                    self._close_followup_flows = set()
                since_map: dict[str, float] = self.__dict__.get("_close_sync_since") or {}
                for f in done:
                    for v in self._sqlite_flow_id_variants(f):
                        since_map.pop(v, None)
                return

            def _fire() -> None:
                try:
                    with guard:
                        targets = set(self.__dict__.get("_close_followup_flows") or set())
                    if targets and self._followup_flows_have_unreplicated(targets):
                        self.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True, flow_ids=targets)
                except Exception:  # noqa: BLE001
                    pass
                _schedule(idx + 1, delays[idx])

            t = threading.Timer(max(0.0, delays[idx] - prev), _fire)
            t.daemon = True
            t.start()

        _schedule(0, 0.0)

    def trigger_close_span_sync(self, flow_id: Any, *, delay_s: float | None = None, since: Any = None) -> None:
        """Rerun-4: a RUN record closed -> replicate that execution's spans now, not on the next 15 s tick.

        Calls coalesce: every close within the debounce window joins one targeted sync (25 concurrent closes ->
        1-2 syncs, each writing chunked multi-row MERGEs). ``delay_s=0`` syncs inline (tests / callers that
        already run off the request path). Rows still ambiguous keep their pending grace semantics.
        """
        if flow_id:
            self.__dict__.setdefault("_close_sync_flows", set()).add(str(flow_id))
            self._note_close_since(flow_id, since)
        self._span_backlog_hint = int(self.__dict__.get("_span_backlog_hint", 0)) + 50
        delay = self.CLOSE_SYNC_DEBOUNCE_S if delay_s is None else max(0.0, float(delay_s))
        if delay == 0:
            self.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
            return
        guard = self.__dict__.setdefault("_close_sync_guard", threading.Lock())
        with guard:
            if self.__dict__.get("_close_sync_timer_armed"):
                return
            self._close_sync_timer_armed = True

        def _fire() -> None:
            with guard:
                self._close_sync_timer_armed = False
            flows = set(self.__dict__.get("_close_sync_flows") or set())
            try:
                self.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
            except Exception:  # noqa: BLE001
                pass
            self._arm_close_followups(flows)

        timer = threading.Timer(delay, _fire)
        timer.daemon = True
        timer.start()

    def _sync_langflow_traces_locked(
        self, limit: int, *, replicate_sqlite: bool, target_flows: set[str], backlog_hint: int
    ) -> dict[str, Any]:
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        candidate_db_paths = [persist_dir / "langflow.db"]
        writable_db = self._resolve_writable_sqlite_db_path()
        if writable_db not in candidate_db_paths:
            candidate_db_paths.append(writable_db)

        replicated_runs = 0
        replicated_spans = 0
        replicated_activities = 0
        # Queued run/audit rows land first so replicated spans/activity can join them on run_id.
        self.flush_telemetry(5.0)

        if replicate_sqlite:
            for db_path in candidate_db_paths:
                if not db_path.exists():
                    continue
                try:
                    conn = _open_sqlite_conn(db_path, readonly=True, timeout=30.0)
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    tables = {
                        r[0]
                        for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    }
                    bounded_tx = min(int(limit or 25), 25)

                    folder_names: dict[str, str] = {}
                    if "folder" in tables:
                        for f_r in cur.execute("SELECT id, name FROM folder").fetchall():
                            f_d = dict(f_r)
                            fid_k = str(f_d.get("id") or "").strip()
                            if fid_k:
                                folder_names[fid_k] = str(f_d.get("name") or "Default Project")
                                folder_names[fid_k.replace("-", "")] = folder_names[fid_k]

                    flow_meta: dict[str, dict[str, str]] = {}
                    if "flow" in tables:
                        for fl_r in cur.execute("SELECT id, name, folder_id FROM flow").fetchall():
                            fl_d = dict(fl_r)
                            raw_fl_id = str(fl_d.get("id") or "").strip()
                            norm_fl_id = self._normalize_flow_id(raw_fl_id) or raw_fl_id
                            fld_id = str(fl_d.get("folder_id") or "default").strip()
                            meta_entry = {
                                "flow_id": norm_fl_id,
                                "flow_name": str(fl_d.get("name") or norm_fl_id),
                                "project_id": fld_id,
                                "project_name": folder_names.get(fld_id) or folder_names.get(fld_id.replace("-", "")) or "Default Project",
                            }
                            flow_meta[raw_fl_id] = meta_entry
                            flow_meta[norm_fl_id] = meta_entry
                            flow_meta[norm_fl_id.replace("-", "")] = meta_entry

                    # T10: raw Langflow rows are read from the local SQLite (cheap; already-synced ids are skipped in
                    # memory), wider than the UI ``limit`` so concurrent runs cannot push unsynced rows out of view.
                    # Rerun-4: the window widens with the closed-run backlog (load) instead of a fixed 200 rows.
                    raw_bound = max(bounded_tx, 200, min(5000, int(backlog_hint)))
                    tx_rows = (
                        cur.execute(
                            'SELECT * FROM "transaction" ORDER BY timestamp DESC LIMIT ?', (raw_bound,)
                        ).fetchall()
                        if "transaction" in tables
                        else []
                    )
                    vb_rows = (
                        cur.execute(
                            'SELECT * FROM "vertex_build" ORDER BY timestamp DESC LIMIT ?', (raw_bound,)
                        ).fetchall()
                        if "vertex_build" in tables
                        else []
                    )
                    # Rerun-4: flows whose RUN just closed are always read in full (bound per flow), so other
                    # flows' traffic cannot push a finished execution's rows out of the newest-N window.
                    flow_keys = sorted({v for f in target_flows for v in self._sqlite_flow_id_variants(f)})
                    for tbl, pk, rows in (("transaction", "id", tx_rows), ("vertex_build", "build_id", vb_rows)):
                        if not flow_keys or tbl not in tables:
                            continue
                        seen_t = {str(dict(r).get(pk) or "") for r in rows}
                        for i in range(0, len(flow_keys), 50):
                            fk = flow_keys[i : i + 50]
                            # Round-3: page back until every row newer than the oldest just-closed run start
                            # is covered (bounded by TARGET_READ_MAX_ROWS), not only the newest page.
                            since = self._close_since_for(fk)
                            offset = 0
                            while True:
                                page = cur.execute(
                                    f'SELECT * FROM "{tbl}" WHERE flow_id IN ({",".join("?" * len(fk))}) '
                                    "ORDER BY timestamp DESC LIMIT 2000 OFFSET ?",
                                    (*fk, offset),
                                ).fetchall()
                                for r in page:
                                    rid_t = str(dict(r).get(pk) or "")
                                    if rid_t not in seen_t:
                                        seen_t.add(rid_t)
                                        rows.append(r)
                                offset += len(page)
                                if since is None or len(page) < 2000 or offset >= self.TARGET_READ_MAX_ROWS:
                                    break
                                last = _telemetry._parse_ts(dict(page[-1]).get("timestamp"))
                                if last is None or last.timestamp() < since:
                                    break
                    # Pending (ambiguous) rows are always re-read by primary key until written.
                    for tbl, pk, prefix, rows in (
                        ("transaction", "id", "tx:", tx_rows),
                        ("vertex_build", "build_id", "vb:", vb_rows),
                    ):
                        want = [i for i in self._pending_row_ids(prefix) if i] if tbl in tables else []
                        if want:
                            seen = {str(dict(r).get(pk) or "") for r in rows}
                            want = [i for i in want if i not in seen]
                        for i in range(0, len(want), 100):
                            chunk = want[i : i + 100]
                            rows.extend(
                                cur.execute(
                                    f'SELECT * FROM "{tbl}" WHERE {pk} IN ({",".join("?" * len(chunk))})', tuple(chunk)
                                ).fetchall()
                            )
                    conn.close()

                    _telemetry.install_langflow_row_stamping()
                    # Rerun-4: span/activity rows are queued and written as chunked multi-row MERGEs after the scan
                    # (was 2 sequential single-row MERGEs per row: ~500 round-trips for 25 runs x 5 turns).
                    span_batch: list[tuple[str, list[tuple[str, Any, str]], tuple[str, str]]] = []
                    act_batch: list[tuple[str, list[tuple[str, Any, str]], tuple[str, str]]] = []
                    spans_by_flow: dict[str, list[dict[str, Any]]] = {}
                    # T10: (flow, vertex, ts) -> job_id of the vertex_build sibling of a transaction row.
                    vb_job_by_vertex: dict[str, str] = {}
                    for vb in vb_rows:
                        vb_d = dict(vb)
                        if vb_d.get("job_id"):
                            vb_fp = _telemetry.row_fingerprint(
                                _trace.canonical_flow_id(self._normalize_flow_id(str(vb_d.get("flow_id") or "")) or vb_d.get("flow_id")),
                                vb_d.get("id"),
                                vb_d.get("timestamp"),
                            )
                            if vb_fp:
                                vb_job_by_vertex[vb_fp] = str(vb_d["job_id"])
                        raw_fid = str(vb_d.get("flow_id") or "unknown-flow")
                        norm_fid = self._normalize_flow_id(raw_fid) or raw_fid
                        spans_by_flow.setdefault(norm_fid, []).append(
                            {
                                "span_id": str(vb_d.get("build_id") or vb_d.get("id") or ""),
                                "vertex_id": str(vb_d.get("id") or vb_d.get("vertex_id") or "vertex"),
                                "timestamp": str(vb_d.get("timestamp") or ""),
                                "status": "SUCCESS" if vb_d.get("valid") in (True, 1, "1", "true", "TRUE", None) else "ERROR",
                            }
                        )
                    # v0.8.6-20261112b: node_spans_json is cut to an 8000-char preview (~60 entries) anyway; the
                    # full per-flow list (thousands after a targeted read) was redacted once per activity row: O(n^2).
                    for _fid in list(spans_by_flow):
                        del spans_by_flow[_fid][self.NODE_SPANS_PREVIEW_ROWS :]

                    for r in tx_rows:
                        row_dict = dict(r)
                        tx_id = str(row_dict.get("id") or uuid.uuid4().hex)
                        if tx_id in self._synced_tx_ids:
                            continue
                        raw_fid = str(row_dict.get("flow_id") or "unknown-flow")
                        norm_fid = self._normalize_flow_id(raw_fid) or raw_fid
                        # BUG-MCP-02 / BUG-LLMOP-A4-01: canonical flow id + the real run of this execution.
                        flow_id = _trace.canonical_flow_id(norm_fid) or norm_fid
                        tx_ts = row_dict.get("timestamp")
                        # T10: explicit linkage (row stamped at write time / job id / sibling vertex_build),
                        # then a unique active run; ambiguous rows are never given a synthetic run.
                        tx_job = row_dict.get("job_id") or vb_job_by_vertex.get(
                            _telemetry.row_fingerprint(flow_id, row_dict.get("vertex_id"), tx_ts) or ""
                        )
                        run_rec, tx_ambiguous = _telemetry.RUNS.resolve(
                            flow_id=flow_id,
                            vertex_id=row_dict.get("vertex_id"),
                            ts=tx_ts,
                            row_id=row_dict.get("id"),
                            job_id=tx_job,
                            session_id=row_dict.get("session_id"),
                        )
                        if run_rec is not None:
                            rid, tid, sess = run_rec.run_id, run_rec.trace_id, run_rec.session_id
                        elif tx_ambiguous:
                            if self._telemetry_row_pending(f"tx:{tx_id}", tx_ts, flow_id):
                                continue  # retry on a later sync: the owning run may still be stamped
                            rid = tid = sess = None  # still ambiguous after grace: written, run_id NULL
                        else:
                            # No live run at all (e.g. after a restart). Rerun-3: one derived run per execution
                            # (job id, else a burst of rows of this flow), never one per row (165 RUNs / 25 runs).
                            rid, tid = _telemetry.RUNS.derived_execution(flow_id=flow_id, ts=tx_ts, job_id=tx_job)
                            sess = None
                        self._resolve_pending(f"tx:{tx_id}")
                        sess = str(row_dict.get("session_id") or sess or "") or None
                        vertex_id = str(row_dict.get("vertex_id") or "vertex")
                        status_val = str(row_dict.get("status") or "SUCCESS").upper()
                        raw_in = row_dict.get("inputs") or {}
                        raw_out = row_dict.get("outputs") or {}
                        inputs_redacted = self._safe_json_text(raw_in, max_chars=4000, default_empty="{}")
                        outputs_redacted = self._safe_json_text(raw_out, max_chars=4000, default_empty="{}")
                        exec_mode_v = _trace.audit_execution_mode(_sf_identity.origin_mode())

                        span_batch.append(
                            (
                                tx_id,
                                [
                                ("event_id", f"evt-{tx_id[:16]}", "TEXT"),
                                ("span_id", tx_id, "TEXT"),
                                ("run_id", rid, "TEXT"),
                                ("trace_id", tid, "TEXT"),
                                ("project_id", "default", "TEXT"),
                                ("flow_id", flow_id, "TEXT"),
                                ("component_id", vertex_id, "TEXT"),
                                ("component_name", vertex_id, "TEXT"),
                                ("authenticated_user", self.authenticated_user, "TEXT"),
                                ("effective_role", self.active_role, "TEXT"),
                                ("execution_mode", exec_mode_v, "TEXT"),
                                ("start_ts", _telemetry.utc_iso_clamped(tx_ts), "TS"),
                                ("end_ts", _telemetry.utc_iso_clamped(tx_ts), "TS"),
                                ("status", status_val, "TEXT"),
                                ("redacted_inputs", inputs_redacted, "JSON"),
                                ("redacted_outputs", outputs_redacted, "JSON"),
                                ],
                                ("tx", tx_id),
                            )
                        )

                        if run_rec is None and rid is not None and rid not in self._synced_derived_run_ids:
                            # Executions without a live run record (e.g. before a restart): one derived run row.
                            derived_res = self._execute_bound(
                                self._merge_insert_request(
                                    self.runs_table,
                                    "run_id",
                                    rid,
                                    [
                                        ("event_id", f"evr-{rid}", "TEXT"),
                                        ("run_id", rid, "TEXT"),
                                        ("trace_id", tid, "TEXT"),
                                        ("project_id", "default", "TEXT"),
                                        ("flow_id", flow_id, "TEXT"),
                                        ("session_id", sess, "TEXT"),
                                        ("authenticated_user", self.authenticated_user, "TEXT"),
                                        ("effective_role", self.active_role, "TEXT"),
                                        ("execution_mode", exec_mode_v, "TEXT"),
                                        ("start_ts", _telemetry.utc_iso_clamped(tx_ts), "TS"),
                                        ("end_ts", _telemetry.utc_iso_clamped(tx_ts), "TS"),
                                        ("status", status_val, "TEXT"),
                                        ("input_payload", inputs_redacted, "JSON"),
                                        ("output_payload", outputs_redacted, "JSON"),
                                    ],
                                ),
                                timeout=12,
                            )
                            if not (isinstance(derived_res, dict) and derived_res.get("ok") is False):  # retried next sync
                                self._synced_derived_run_ids.add(rid)
                                replicated_runs += 1

                        act_id = f"act-tx-{tx_id[:24]}"
                        if act_id not in self._synced_activity_ids:
                            f_meta = flow_meta.get(norm_fid) or flow_meta.get(raw_fid) or {}
                            flw_ctx = copy.deepcopy(self.get_flow_context(norm_fid) if norm_fid != "unknown-flow" else self.selected_context)
                            act_rec = {
                                "id": act_id,
                                "activity_id": act_id,
                                "trace_id": tid,
                                "name": f"{f_meta.get('flow_name') or norm_fid} ({vertex_id})",
                                "event_type": "FLOW_RUN",
                                "flow_id": norm_fid,
                                "flow_name": f_meta.get("flow_name") or norm_fid,
                                "project_id": f_meta.get("project_id") or "default",
                                "project_name": f_meta.get("project_name") or "Default Project",
                                "session_id": sess or flow_id,
                                "timestamp": _telemetry.utc_iso_clamped(row_dict.get("timestamp")),
                                "run_timestamp": _telemetry.utc_iso_clamped(row_dict.get("timestamp")),
                                "status": status_val,
                                "latency_ms": float(row_dict.get("latency_ms") or 12.5),
                                "total_latency_ms": float(row_dict.get("latency_ms") or 12.5),
                                "input": str(raw_in)[:1000],
                                "output": str(raw_out)[:1000],
                                "input_text": str(raw_in)[:1000],
                                "output_text": str(raw_out)[:1000],
                                "error_message": row_dict.get("error"),
                                "prompt_tokens": int(row_dict.get("prompt_tokens") or 0),
                                "completion_tokens": int(row_dict.get("completion_tokens") or 0),
                                "total_tokens": int(row_dict.get("total_tokens") or 0),
                                "user_identity": self.authenticated_user,
                                "active_role": self.active_role,
                                "context_epoch": self.context_epoch,
                                "flow_context_json": flw_ctx,
                                "input_payload": raw_in if isinstance(raw_in, dict) else {"raw": str(raw_in)},
                                "output_payload": raw_out if isinstance(raw_out, dict) else {"raw": str(raw_out)},
                                "node_spans_json": spans_by_flow.get(norm_fid) or [{"span_id": tx_id, "vertex_id": vertex_id, "status": status_val}],
                                "spans": spans_by_flow.get(norm_fid) or [{"span_id": tx_id, "vertex_id": vertex_id, "status": status_val}],
                                "snowflake_query_ids": [],
                                "deleted_in_ui_at": None if act_id not in self._soft_deleted_activity_ids else datetime.now(timezone.utc).isoformat(),
                            }
                            self._recent_activity_events.insert(0, act_rec)
                            act_batch.append((*self._activity_merge_row(act_rec), ("act", act_rec["activity_id"])))


                    for vb in vb_rows:
                        vb_dict = dict(vb)
                        vb_id = str(vb_dict.get("build_id") or vb_dict.get("id") or uuid.uuid4().hex)
                        if vb_id in self._synced_vertex_build_ids:
                            continue
                        raw_fid = str(vb_dict.get("flow_id") or "unknown-flow")
                        norm_fid = self._normalize_flow_id(raw_fid) or raw_fid
                        flow_id = _trace.canonical_flow_id(norm_fid) or norm_fid
                        vb_ts = vb_dict.get("timestamp")
                        vb_run, vb_ambiguous = _telemetry.RUNS.resolve(
                            flow_id=flow_id,
                            vertex_id=vb_dict.get("id") or vb_dict.get("vertex_id"),
                            ts=vb_ts,
                            row_id=vb_dict.get("build_id"),
                            job_id=vb_dict.get("job_id"),
                        )
                        if vb_run is not None:
                            vb_rid, vb_tid, vb_sess = vb_run.run_id, vb_run.trace_id, vb_run.session_id
                        elif vb_ambiguous:
                            if self._telemetry_row_pending(f"vb:{vb_id}", vb_ts, flow_id):
                                continue
                            vb_rid = vb_tid = vb_sess = None  # still ambiguous after grace: written, run_id NULL
                        else:
                            vb_rid, vb_tid = _telemetry.RUNS.derived_execution(
                                flow_id=flow_id, ts=vb_ts, job_id=vb_dict.get("job_id")
                            )
                            vb_sess = None
                        self._resolve_pending(f"vb:{vb_id}")
                        vertex_id = str(vb_dict.get("id") or vb_dict.get("vertex_id") or "vertex")
                        valid_flag = vb_dict.get("valid")
                        status_val = "SUCCESS" if valid_flag in (True, 1, "1", "true", "TRUE", None) else "ERROR"
                        raw_params = vb_dict.get("params") or {}
                        raw_data = vb_dict.get("data") or vb_dict.get("artifacts") or {}
                        params_redacted = self._safe_json_text(raw_params, max_chars=4000, default_empty="{}")
                        data_redacted = self._safe_json_text(raw_data, max_chars=4000, default_empty="{}")

                        span_batch.append(
                            (
                                f"vb-{vb_id}",
                                [
                                    ("event_id", f"evb-{vb_id[:16]}", "TEXT"),
                                ("span_id", f"vb-{vb_id}", "TEXT"),
                                ("run_id", vb_rid, "TEXT"),
                                ("trace_id", vb_tid, "TEXT"),
                                ("project_id", "default", "TEXT"),
                                ("flow_id", flow_id, "TEXT"),
                                ("component_id", vertex_id, "TEXT"),
                                ("component_name", vertex_id, "TEXT"),
                                ("authenticated_user", self.authenticated_user, "TEXT"),
                                ("effective_role", self.active_role, "TEXT"),
                                ("execution_mode", _trace.audit_execution_mode(_sf_identity.origin_mode()), "TEXT"),
                                ("start_ts", _telemetry.utc_iso_clamped(vb_ts), "TS"),
                                ("end_ts", _telemetry.utc_iso_clamped(vb_ts), "TS"),
                                ("status", status_val, "TEXT"),
                                ("redacted_inputs", params_redacted, "JSON"),
                                ("redacted_outputs", data_redacted, "JSON"),
                                ],
                                ("vb", vb_id),
                            )
                        )

                        act_vb_id = f"act-vb-{vb_id[:24]}"
                        if act_vb_id not in self._synced_activity_ids:
                            f_meta = flow_meta.get(norm_fid) or flow_meta.get(raw_fid) or {}
                            flw_ctx = copy.deepcopy(self.get_flow_context(norm_fid) if norm_fid != "unknown-flow" else self.selected_context)
                            act_vb_rec = {
                                "id": act_vb_id,
                                "activity_id": act_vb_id,
                                "trace_id": vb_tid,
                                "name": f"{f_meta.get('flow_name') or norm_fid} :: {vertex_id}",
                                "event_type": "NODE_BUILD",
                                "flow_id": norm_fid,
                                "flow_name": f_meta.get("flow_name") or norm_fid,
                                "project_id": f_meta.get("project_id") or "default",
                                "project_name": f_meta.get("project_name") or "Default Project",
                                "session_id": vb_sess or flow_id,
                                "timestamp": _telemetry.utc_iso_clamped(vb_dict.get("timestamp")),
                                "run_timestamp": _telemetry.utc_iso_clamped(vb_dict.get("timestamp")),
                                "status": status_val,
                                "latency_ms": 8.0,
                                "total_latency_ms": 8.0,
                                "input": str(raw_params)[:1000],
                                "output": str(raw_data)[:1000],
                                "input_text": str(raw_params)[:1000],
                                "output_text": str(raw_data)[:1000],
                                "error_message": None if status_val == "SUCCESS" else "Node build failed",
                                "prompt_tokens": 0,
                                "completion_tokens": 0,
                                "total_tokens": 0,
                                "user_identity": self.authenticated_user,
                                "active_role": self.active_role,
                                "context_epoch": self.context_epoch,
                                "flow_context_json": flw_ctx,
                                "input_payload": raw_params if isinstance(raw_params, dict) else {"raw": str(raw_params)},
                                "output_payload": raw_data if isinstance(raw_data, dict) else {"raw": str(raw_data)},
                                "node_spans_json": spans_by_flow.get(norm_fid) or [{"span_id": f"vb-{vb_id}", "vertex_id": vertex_id, "status": status_val}],
                                "spans": spans_by_flow.get(norm_fid) or [{"span_id": f"vb-{vb_id}", "vertex_id": vertex_id, "status": status_val}],
                                "snowflake_query_ids": [],
                                "deleted_in_ui_at": None if act_vb_id not in self._soft_deleted_activity_ids else datetime.now(timezone.utc).isoformat(),
                            }
                            self._recent_activity_events.insert(0, act_vb_rec)
                            act_batch.append((*self._activity_merge_row(act_vb_rec), ("act", act_vb_rec["activity_id"])))

                    replicated_spans += self._flush_merge_batch(self.spans_table, "span_id", span_batch)
                    replicated_activities += self._flush_merge_batch(self.activity_table, "activity_id", act_batch)
                except Exception:  # noqa: BLE001
                    pass

        self.flush_telemetry(5.0)
        self.synced_runs_count += replicated_runs
        self.synced_spans_count += replicated_spans
        self.synced_activity_count += replicated_activities
        self.last_auto_sync_ts = datetime.now(timezone.utc).isoformat()

        q_activity = (
            f"SELECT activity_id, trace_id, event_type, flow_id, flow_name, project_id, project_name, session_id, "
            f"run_timestamp, status, latency_ms, input_text, output_text, total_tokens, user_identity, active_role, "
            f"context_epoch, flow_context_json, node_spans_json, deleted_in_ui_at, replicated_at "
            f"FROM {self.activity_table} WHERE deleted_in_ui_at IS NULL ORDER BY run_timestamp DESC LIMIT {int(limit)}"
        )
        q_runs = (
            f"SELECT run_id, trace_id, flow_id, authenticated_user, effective_role, status, start_ts, replicated_at "
            f"FROM {self.runs_table} WHERE deleted_in_ui_at IS NULL ORDER BY replicated_at DESC LIMIT {int(limit)}"
        )
        q_spans = (
            f"SELECT span_id, run_id, flow_id, component_id, effective_role, status, replicated_at "
            f"FROM {self.spans_table} WHERE deleted_in_ui_at IS NULL ORDER BY replicated_at DESC LIMIT {int(limit)}"
        )
        q_audit = (
            f"SELECT event_id, event_ts, authenticated_user, effective_role, context_epoch, action, outcome, error_code, target_object "
            f"FROM {self.audit_table} ORDER BY event_ts DESC LIMIT {int(limit)}"
        )
        if self._has_spcs_token() and not self._is_execute_sql_mocked():
            f_act = _sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, q_activity, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, timeout=12)
            f_runs = _sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, q_runs, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, timeout=12)
            f_spans = _sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, q_spans, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, timeout=12)
            f_audit = _sf_identity.submit_with_context(_CATALOG_EXECUTOR, self.execute_sql, q_audit, role=self._service_role(), mode=_sf_identity.MODE_SERVICE, timeout=12)
            act_res = f_act.result()
            runs_res = f_runs.result()
            spans_res = f_spans.result()
            audit_res = f_audit.result()
        else:
            act_res = self.execute_sql(q_activity, role=self._service_role(), mode=_sf_identity.MODE_SERVICE)
            runs_res = self.execute_sql(q_runs, role=self._service_role(), mode=_sf_identity.MODE_SERVICE)
            spans_res = self.execute_sql(q_spans, role=self._service_role(), mode=_sf_identity.MODE_SERVICE)
            audit_res = self.execute_sql(q_audit, role=self._service_role(), mode=_sf_identity.MODE_SERVICE)

        active_mem_events = [
            ev
            for ev in self._recent_activity_events
            if ev.get("deleted_in_ui_at") is None
            and str(ev.get("activity_id") or "") not in self._soft_deleted_activity_ids
        ][: int(limit)]

        return {
            "ok": True,
            "auto_sync_enabled": self.auto_sync_enabled,
            "last_auto_sync_ts": self.last_auto_sync_ts,
            "replicated_runs": replicated_runs,
            "replicated_spans": replicated_spans,
            "replicated_activities": replicated_activities,
            "synced_runs_count": self.synced_runs_count,
            "synced_spans_count": self.synced_spans_count,
            "synced_activity_count": self.synced_activity_count,
            "activity_table": self.activity_table,
            "activity": act_res.get("rows") or active_mem_events,
            "runs": runs_res.get("rows", []),
            "spans": spans_res.get("rows", []),
            "audit_log": audit_res.get("rows", []),
        }

    def start_auto_sync_daemon(self, interval_seconds: float = 15.0) -> bool:
        """Start a periodic 15s background daemon thread that replicates SQLite runs/spans to Snowflake automatically."""
        if self._auto_sync_daemon_started:
            return True
        self._auto_sync_daemon_started = True
        self.auto_sync_enabled = True

        def _daemon_loop() -> None:
            while True:
                time.sleep(max(5.0, float(interval_seconds)))
                try:
                    if self._has_spcs_token():
                        if not getattr(self, "_showcase_bootstrapped_on_spcs", False):
                            self._showcase_bootstrapped_on_spcs = True
                            self.bootstrap_full_memory_showcase_flow(
                                dry_run=False,
                                execute_simulation=True,
                                explicit_user_request=_should_seed_showcase_flow(),
                            )
                        self.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
                except Exception:  # noqa: BLE001
                    pass

        daemon_thread = threading.Thread(
            target=_daemon_loop,
            name="sflf-auto-trace-sync-daemon",
            daemon=True,
        )
        daemon_thread.start()
        return True


LIVE_BRIDGE = LiveSnowflakeBridge()

_SECONDARY_ROLES_REJECT_CODES = frozenset({"100132", "391911"})
_SECONDARY_ROLES_PREFIX_RE = re.compile(r"^\s*USE SECONDARY ROLES (?:NONE|ALL)\s*;\s*\n?", re.IGNORECASE)
# Messages Snowflake returns when the USE statement itself is refused (observed live, PASSO8/C3).
_SECONDARY_ROLES_REJECT_MSG_RE = re.compile(
    r"\bUSE(?:\s+SECONDARY)?\s+ROLES?\b.*not allowed|not supported by SQL API:\s*USE", re.IGNORECASE
)


def _secondary_roles_prefix_rejected(body: dict[str, Any]) -> bool:
    """True only when the error belongs to statement 1 (the USE SECONDARY ROLES prefix).

    Requires a known reject code AND either the failing handle == statementHandles[0] or the documented
    USE-rejection message; a failing handle pointing at a later statement is never the prefix.
    """
    if str(body.get("code") or "") not in _SECONDARY_ROLES_REJECT_CODES:
        return False
    handles = body.get("statementHandles")
    failing = body.get("statementHandle")
    if isinstance(handles, list) and handles and failing:
        if failing != handles[0]:
            return False
        return True
    return bool(_SECONDARY_ROLES_REJECT_MSG_RE.search(str(body.get("message") or "")))


async def _session_id_from_body(request: Any) -> str | None:
    """Langflow session of a run request body: /run ``session_id``; /build ``inputs.session`` / ``session_id``.

    Only small JSON POST bodies are read (Starlette caches the body, so the route still receives it).
    """
    try:
        if str(getattr(request, "method", "")).upper() != "POST":
            return None
        if "json" not in str(request.headers.get("content-type") or "").lower():
            return None
        if int(request.headers.get("content-length") or 0) > 262144:
            return None
        body = json.loads((await request.body()) or b"{}")
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(body, dict):
        return None
    inputs = body.get("inputs") if isinstance(body.get("inputs"), dict) else {}
    sid = body.get("session_id") or inputs.get("session") or inputs.get("session_id")
    if not isinstance(sid, (str, int)):
        return None
    return str(sid).strip()[:250] or None


# Flow execution starts: POST /api/v1/build/<flow>/flow, /api/v1/run/[advanced/]<flow>, /api/v1/webhook/<flow>.
_FLOW_EXEC_PATH_RE = re.compile(
    r"/api/v1/(?:build/(?P<b>[0-9A-Fa-f-]{32,36})/flow|run/(?:advanced/)?(?P<r>[^/?#]+)|webhook/(?P<w>[^/?#]+))"
)


def _telemetry_ctx() -> Any:
    """Context snapshot for a queued telemetry write (identity/trace attribution, origin mode pinned)."""
    from contextvars import copy_context  # noqa: PLC0415

    if _sf_identity.ORIGIN_MODE.get() is None:
        token = _sf_identity.ORIGIN_MODE.set(_sf_identity.resolve_mode(_sf_identity.current_request_mode()))
        ctx = copy_context()
        _sf_identity.ORIGIN_MODE.reset(token)
        return ctx
    return copy_context()


def _telemetry_execute(sql: str, bindings: dict[str, Any] | None, query_tag: str | None) -> Any:
    return _sf_identity._run_as_service(
        LIVE_BRIDGE.execute_sql,
        sql,
        role=LIVE_BRIDGE._service_role(),
        mode=_sf_identity.MODE_SERVICE,
        bindings=bindings,
        timeout=15,
        query_tag=query_tag,
    )


# PASSO 8: one batched writer for audit/activity/runs/spans; atexit drains it so no row is lost on shutdown.
_TELEMETRY_WRITER = _telemetry.TelemetryWriter(_telemetry_execute)


def _emit_mcp_denial(tool_name: str, result: dict[str, Any], target_object: str | None) -> None:
    """W3-R1-10a: every MCP 403 denial is audited as AUTHZ_ROLE_DENIED (nothing was executed)."""
    try:
        emit_audit(
            LIVE_BRIDGE.record_audit_event,
            AuditAction.AUTHZ_ROLE_DENIED,
            result,
            target_object=target_object or tool_name,
            details={"object": tool_name, "status_code": result.get("status_code")},
            discriminator=f"mcp:{tool_name}:{uuid.uuid4().hex}",
        )
    except Exception:  # noqa: BLE001 - auditing must never turn a denial into a success
        pass


# BUG-A2A-06: authentication perimeter for `/api/v1/snowflake/*`.
SNOWFLAKE_ROUTER_PREFIX = "/api/v1/snowflake"
AGENT_CARD_PUBLIC_ENV = "SNOWFLAKE_LANGFLOW_PUBLIC_AGENT_CARD"
_AGENT_CARD_SUFFIX = "/.well-known/agent-card.json"


def _is_public_snowflake_path(path: str) -> bool:
    """Only the A2A agent card may be public, and only when explicitly intended (default ON)."""
    if not str(path or "").endswith(_AGENT_CARD_SUFFIX):
        return False
    return os.getenv(AGENT_CARD_PUBLIC_ENV, "1").strip().lower() in {"1", "true", "yes", "on"}


SERVICE_TRIGGER_SECRET_ENV = "SNOWFLAKE_LANGFLOW_SERVICE_TRIGGER_SECRET"
SERVICE_TRIGGER_SECRET_HEADER = "X-Langflow-Service-Trigger-Secret"
SERVICE_FUNCTIONS_ENV = "SNOWFLAKE_LANGFLOW_SERVICE_FUNCTIONS"
_SERVICE_TRIGGER_SUFFIX = "/service-trigger"


def _is_snowflake_perimeter_path(path: str) -> bool:
    clean = str(path or "").rstrip("/")
    return clean == SNOWFLAKE_ROUTER_PREFIX or clean.startswith(f"{SNOWFLAKE_ROUTER_PREFIX}/")


def _is_middleware_intercepted_path(method: str, path: str) -> bool:
    """Paths the bridge middleware may answer itself (they never reach a guarded router)."""
    clean = str(path or "").rstrip("/")
    return any(
        clean == base or clean.startswith(f"{base}/")
        for base in ("/api/v2/traces", "/api/v1/memories", "/api/v1/monitor")
    )


def _is_protected_path(path: str) -> bool:
    return _is_snowflake_perimeter_path(path) or _is_middleware_intercepted_path("", path)


def _auto_login_enabled() -> bool:
    return os.getenv("LANGFLOW_AUTO_LOGIN", "true").strip().lower() not in ("0", "false", "no")


def _is_spcs_browser_session(request: Any) -> bool:
    """A6-2: same-origin browser request of a Snowflake-authenticated SPCS ingress user under auto-login.

    Only then may the native auto-login session token stand in for a missing cookie on protected paths
    (this is exactly what Langflow's own auto-login grants that user). Requests without the ingress
    identity, cross-site requests, and anything outside SPCS never qualify.
    """
    headers = getattr(request, "headers", {}) or {}
    return (
        _auto_login_enabled()
        and _running_behind_spcs_ingress()
        and bool(_sf_identity.identity_from_headers(headers).sf_user)
        and str(headers.get("sec-fetch-site") or "").strip().lower() == "same-origin"
    )


# Test seam: ``async (request) -> user | None``; ``None`` uses Langflow's native resolver.
_NATIVE_USER_RESOLVER: Any = None


async def _native_user_from_request(request: Any) -> Any:
    """Validate the request's Langflow credential (JWT cookie/Bearer or API key) with Langflow's auth service."""
    if _NATIVE_USER_RESOLVER is not None:
        return await _NATIVE_USER_RESOLVER(request)
    from langflow.services.auth.utils import get_current_user_optional  # noqa: PLC0415
    from lfx.services.deps import session_scope  # noqa: PLC0415

    async with session_scope() as db:
        user = await get_current_user_optional(request, db)
    return user if user is not None and getattr(user, "is_active", True) is not False else None


async def _langflow_user_id_from_request(request: Any) -> str | None:
    """BUG-MEM-MB-01: the caller's real Langflow user id (never a superuser fallback); None if unauthenticated."""
    if not _native_auth_available():
        return None
    try:
        user = await _native_user_from_request(request)
    except Exception:  # noqa: BLE001 - unauthenticated: callers answer 401
        return None
    uid = getattr(user, "id", None) if user is not None else None
    return str(uid) if uid else None


def _native_auth_available() -> bool:
    return _NATIVE_USER_RESOLVER is not None or _native_auth_dependency() is not None


async def _authorize_intercepted_request(request: Any) -> bool:
    """A6-2: same decision as the router guard, for requests answered inside the middleware."""
    if not _native_auth_available():
        return os.getenv("SNOWFLAKE_LANGFLOW_AUTH_TEST_BYPASS", "0").strip() == "1"
    try:
        user = await _native_user_from_request(request)
    except Exception:  # noqa: BLE001
        return False
    if user is None:
        return False
    headers = getattr(request, "headers", {}) or {}
    return not (_running_behind_spcs_ingress() and not _sf_identity.identity_from_headers(headers).sf_user)


def _inject_session_token(scope: dict[str, Any], token: str) -> bool:
    """Add the auto-login session token when the request carries no Langflow credential; True if injected."""
    raw_headers = list(scope.get("headers") or [])
    has_auth_hdr = any(k.lower() == b"authorization" and b"bearer " in v.lower() for k, v in raw_headers)
    has_api_key = any(k.lower() == b"x-api-key" for k, _v in raw_headers)
    cookie_hdr = b"; ".join(v for k, v in raw_headers if k.lower() == b"cookie")
    if has_auth_hdr or has_api_key or b"access_token_lf=" in cookie_hdr:
        return False
    raw_headers.append((b"authorization", f"Bearer {token}".encode("utf-8")))
    scope["headers"] = raw_headers
    return True


def _is_service_function_call(request: Any) -> bool:
    """A6-2: authenticate the Snowflake service-function caller of `/service-trigger` (service to service).

    With ``SNOWFLAKE_LANGFLOW_SERVICE_TRIGGER_SECRET`` set (recommended; pass it from the service function
    via ``HEADERS``) the secret header must match. Otherwise the request must carry the service-function
    protocol headers for an allow-listed function and must not carry a public-ingress user identity.
    """
    import hmac  # noqa: PLC0415

    if not str(request.url.path).rstrip("/").endswith(_SERVICE_TRIGGER_SUFFIX):
        return False
    headers = request.headers
    secret = (os.getenv(SERVICE_TRIGGER_SECRET_ENV) or "").strip()
    if secret:
        supplied = str(headers.get(SERVICE_TRIGGER_SECRET_HEADER) or headers.get(SERVICE_TRIGGER_SECRET_HEADER.lower()) or "")
        return hmac.compare_digest(supplied.encode("utf-8"), secret.encode("utf-8"))
    if _sf_identity.identity_from_headers(headers).sf_user:
        return False
    if not headers.get("sf-external-function-current-query-id"):
        return False
    allowed = {
        n.strip().upper().split(".")[-1]
        for n in (os.getenv(SERVICE_FUNCTIONS_ENV) or "RUN_LANGFLOW_FLOW").split(",")
        if n.strip()
    }
    fn_name = str(headers.get("sf-external-function-name") or "").strip().upper().split(".")[-1]
    return bool(fn_name) and fn_name in allowed


def _running_behind_spcs_ingress() -> bool:
    return SPCS_TOKEN_PATH.exists() and bool(os.getenv("SNOWFLAKE_HOST"))


def _native_auth_dependency() -> Any | None:
    """Langflow's own optional-user resolver (session cookie, Bearer JWT or x-api-key)."""
    try:
        from langflow.services.auth.utils import get_optional_user  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return None
    return get_optional_user


def _build_snowflake_route_guard(native_dependency: Any | None = None) -> Any:
    """Router-level dependency: native Langflow active user, plus SPCS ingress identity inside SPCS.

    Fails closed (401/403) except for the public agent card. When Langflow itself is not importable
    (standalone unit tests only) no native credential can exist, so access requires the explicit
    ``SNOWFLAKE_LANGFLOW_AUTH_TEST_BYPASS=1`` switch, which has no effect when Langflow is installed.
    """
    from fastapi import Depends, HTTPException, Request  # noqa: PLC0415

    native = native_dependency if native_dependency is not None else _native_auth_dependency()

    def _check(request: Request, user: Any) -> None:
        if _is_public_snowflake_path(request.url.path) or _is_service_function_call(request):
            return
        if native is None:
            if os.getenv("SNOWFLAKE_LANGFLOW_AUTH_TEST_BYPASS", "0").strip() == "1":
                return
            raise HTTPException(status_code=401, detail="Langflow authentication is unavailable.")
        if user is None or getattr(user, "is_active", True) is False:
            raise HTTPException(status_code=401, detail="Missing or invalid Langflow credentials.")
        if _running_behind_spcs_ingress() and not _sf_identity.identity_from_headers(request.headers).sf_user:
            raise HTTPException(status_code=403, detail="Missing SPCS ingress identity (Sf-Context-Current-User).")

    if native is None:

        async def _guard_without_native(request: Request) -> None:
            _check(request, None)

        return _guard_without_native

    async def _guard(request: Request, user: Any = Depends(native)) -> None:
        _check(request, user)

    return _guard


def register_snowflake_fastapi_routes(app: Any) -> None:
    """Mount `/api/v1/snowflake/*` endpoints and automatic Flow Activity replication middleware onto Langflow's FastAPI app."""
    # W3-R1-14d: structured JSON container logs (allowlisted fields only) from startup.
    _trace.get_trace_logger()
    _trace.log_event("bridge_startup")
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse

    ensure_sqlite_wal_and_uv_cache()
    # E2E stale flows: stored Snowflake component code is built from the installed source (or refused).
    from lfx_snowflake.stale_flow_guard import install_stale_component_guard

    _trace.log_event("stale_component_guard", patched_bindings=install_stale_component_guard())
    LIVE_BRIDGE.start_auto_sync_daemon(interval_seconds=15.0)
    from fastapi import Depends

    sf_router = APIRouter(
        prefix=SNOWFLAKE_ROUTER_PREFIX,
        tags=["SnowflakeLangflow"],
        dependencies=[Depends(_build_snowflake_route_guard())],
    )
    traces_v2_router = APIRouter(
        tags=["SnowflakeLangflowTracesV2"], dependencies=[Depends(_build_snowflake_route_guard())]
    )
    memories_v1_router = APIRouter(
        tags=["SnowflakeLangflowMemoriesV1"], dependencies=[Depends(_build_snowflake_route_guard())]
    )

    @memories_v1_router.get("/api/v1/memories")
    async def get_v1_memories_native(
        request: Request,
        flow_id: str | None = None,
        search: str | None = None,
        page: int = 1,
        size: int = 50,
    ) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id")
        req_search = search if search is not None else (request.query_params.get("search") or request.query_params.get("q"))
        req_page = int(request.query_params.get("page") or page or 1)
        req_size = int(request.query_params.get("size") or size or 50)
        req_uid = await _langflow_user_id_from_request(request)
        payload = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_native_memories(
                user_id=req_uid,
                flow_id=req_fid,
                search=req_search,
                page=req_page,
                size=req_size,
            )
        )
        return JSONResponse(status_code=200, content=payload)

    @memories_v1_router.post("/api/v1/memories", status_code=201)
    async def post_v1_memories_native(request: Request) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        try:
            body = await request.json()
            payload_in = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload_in = {}
        req_uid = await _langflow_user_id_from_request(request)
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.create_native_memory(payload_in, flow_id=req_fid, user_id=req_uid)
        )
        status_code = int(res.get("status_code") or (201 if res.get("ok") else 422))
        return JSONResponse(status_code=status_code, content=res)

    @memories_v1_router.get("/api/v1/memories/{memory_id}/sessions")
    async def get_v1_memory_sessions_native(memory_id: str, request: Request) -> JSONResponse:
        req_page = int(request.query_params.get("page") or 1)
        req_size = int(request.query_params.get("size") or 50)
        payload = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_native_memory_sessions(memory_id, page=req_page, size=req_size)
        )
        return JSONResponse(status_code=200, content=payload)

    @memories_v1_router.get("/api/v1/memories/{memory_id}/messages")
    async def get_v1_memory_messages_native(memory_id: str, request: Request) -> JSONResponse:
        req_sess = request.query_params.get("session_id") or request.query_params.get("session")
        req_page = int(request.query_params.get("page") or 1)
        req_size = int(request.query_params.get("size") or 50)
        msgs_uid = await _langflow_user_id_from_request(request)
        payload = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_native_memory_messages(
                memory_id, user_id=msgs_uid,
                session_id=req_sess,
                page=req_page,
                size=req_size,
            )
        )
        return JSONResponse(status_code=200, content=payload)

    @memories_v1_router.get("/api/v1/memories/{memory_id}")
    async def get_v1_memory_detail_native(memory_id: str) -> JSONResponse:
        payload = await asyncio.to_thread(LIVE_BRIDGE.get_native_memory, memory_id)
        status_code = int(payload.get("status_code") or (200 if payload.get("ok", True) else 404))
        return JSONResponse(status_code=status_code, content=payload)

    @memories_v1_router.patch("/api/v1/memories/{memory_id}")
    async def patch_v1_memory_native(memory_id: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
            payload_in = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload_in = {}
        req_uid = await _langflow_user_id_from_request(request)
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.patch_native_memory(memory_id, payload_in, user_id=req_uid)
        )
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 404))
        return JSONResponse(status_code=status_code, content=res)

    @memories_v1_router.delete("/api/v1/memories/{memory_id}")
    async def delete_v1_memory_native(memory_id: str) -> JSONResponse:
        res = await asyncio.to_thread(LIVE_BRIDGE.delete_memory, memory_id)
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 404))
        return JSONResponse(status_code=status_code, content=res)

    @traces_v2_router.get("/api/v2/traces")
    async def get_v2_traces_fallback(request: Request, flow_id: str | None = None, limit: int = 50) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        payload = await asyncio.to_thread(LIVE_BRIDGE.get_ui_flow_activity_traces, flow_id=req_fid, limit=limit)
        return JSONResponse(status_code=200, content=payload)

    @traces_v2_router.delete("/api/v2/traces")
    @traces_v2_router.delete("/api/v2/traces/{trace_id}")
    async def delete_v2_traces_soft(
        request: Request,
        trace_id: str | None = None,
        flow_id: str | None = None,
    ) -> JSONResponse:
        ids: list[str] = []
        if trace_id:
            ids.append(trace_id)
        for qk in ("id", "ids", "trace_id", "trace_ids", "activity_id", "transaction_id"):
            qv = request.query_params.get(qk)
            if qv:
                ids.extend(x.strip() for x in qv.split(",") if x.strip())
        try:
            body = await request.json()
            if isinstance(body, dict):
                for bk in ("ids", "trace_ids", "activity_ids", "id", "trace_id", "activity_id"):
                    bv = body.get(bk)
                    if isinstance(bv, list):
                        ids.extend(str(x).strip() for x in bv if str(x).strip())
                    elif isinstance(bv, str) and bv.strip():
                        ids.append(bv.strip())
                flow_id = flow_id or body.get("flow_id")
            elif isinstance(body, list):
                ids.extend(str(x).strip() for x in body if str(x).strip())
        except Exception:  # noqa: BLE001
            pass
        res = await asyncio.to_thread(LIVE_BRIDGE.soft_delete_flow_activity, ids, flow_id=flow_id)
        return JSONResponse(status_code=200, content=res)

    if hasattr(app, "middleware"):
        async def _guard_native_kb_request(request: Request, path: str, method: str) -> Any:
            """Fail closed (409) when a native KB route targets a Snowflake-mode knowledge_base row."""
            body = await request.body() if method == "DELETE" else None
            if not native_kb_target_names(path, method, body):
                return None
            request_id = (getattr(request, "scope", None) or {}).get(_trace.REQUEST_ID_SCOPE_KEY) or _trace.request_id_from_header(
                request.headers.get(_trace.REQUEST_ID_HEADER)
            )

            def _lookup() -> set[str] | None:
                db_path = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist")) / "langflow.db"
                if not db_path.exists():
                    return set()
                try:
                    conn = _open_sqlite_conn(db_path, readonly=True, timeout=10.0)
                    try:
                        return snowflake_kb_names_in_native_table(conn)
                    finally:
                        conn.close()
                except Exception:  # noqa: BLE001
                    return None

            target = native_kb_guard_decision(path, method, body, await asyncio.to_thread(_lookup))
            if target is None:
                return None
            try:
                LIVE_BRIDGE.record_audit_event(
                    action=AuditAction.AUTHZ_ROLE_DENIED.value,
                    outcome="DENIED",
                    error_code=SNOWFLAKE_KB_NOT_NATIVE,
                    target_object=target,
                    details={"route": "native_knowledge_bases", "method": method, "request_id": request_id},
                )
            except Exception:  # noqa: BLE001
                pass
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": SNOWFLAKE_KB_NOT_NATIVE,
                    "message": "Snowflake memories are not native Langflow knowledge bases; use /api/v1/memories.",
                    "target": target,
                    "request_id": request_id,
                },
            )

        async def _snowflake_flow_context_and_trace(request: Request, call_next: Any) -> Any:
            path = str(getattr(request.url, "path", "") or "")
            method = str(getattr(request, "method", "GET") or "GET").upper()
            if path.startswith(("/api/v1/knowledge_bases", "/api/v1/knowledge-base-storage")):
                blocked = await _guard_native_kb_request(request, path, method)
                if blocked is not None:
                    return blocked
            if not getattr(LIVE_BRIDGE, "_sqlite_uuid_healed_at_request", False) and path.startswith("/api/"):
                LIVE_BRIDGE._sqlite_uuid_healed_at_request = True
                await asyncio.to_thread(ensure_sqlite_wal_and_uv_cache)
            header_fid = (
                request.headers.get("X-Snowflake-Flow-Id")
                or request.query_params.get("flow_id")
                or request.headers.get("x-snowflake-flow-id")
            )
            matched_fid = header_fid
            if not matched_fid:
                m_flow = re.search(
                    r"/api/v1/(?:flows|build|run|monitor/builds|monitor/messages|webhook)/(?!(?:upload|download|basic_examples|batch|public_flow|c|sessions)\b)([A-Za-z0-9_-]{3,128})",
                    path,
                )
                if m_flow:
                    matched_fid = m_flow.group(1)

            norm_fid = LIVE_BRIDGE._normalize_flow_id(matched_fid) if matched_fid else None
            # BUG-CTX-01: GET /context is read-only, so it never re-targets the global active flow.
            is_context_read = method == "GET" and path.rstrip("/") == f"{SNOWFLAKE_ROUTER_PREFIX}/context"
            if norm_fid and norm_fid != LIVE_BRIDGE.active_flow_id and not is_context_read:
                await asyncio.to_thread(LIVE_BRIDGE.set_active_flow, norm_fid)

            # A6-2: requests answered directly by this middleware never reach a guarded router, so they are
            # validated here with Langflow's native auth. The auto-login session token may stand in for a
            # missing cookie only for a same-origin SPCS browser session (see _is_spcs_browser_session).
            if _is_middleware_intercepted_path(method, path):
                cached_gate_tok = getattr(LIVE_BRIDGE, "_cached_auto_login_access_token", None)
                gate_fresh = (time.time() - float(getattr(LIVE_BRIDGE, "_cached_auto_login_ts", 0.0) or 0.0)) < 2400.0
                if cached_gate_tok and gate_fresh and _is_spcs_browser_session(request):
                    _inject_session_token(request.scope, str(cached_gate_tok))
                gate_req = request
                if isinstance(getattr(request, "scope", None), dict):
                    try:  # fresh Request so the (possibly) injected header is visible to the validator
                        from starlette.requests import Request as _StarletteRequest  # noqa: PLC0415

                        gate_req = _StarletteRequest(request.scope)
                    except ImportError:  # pragma: no cover
                        gate_req = request
                if not await _authorize_intercepted_request(gate_req):
                    return JSONResponse(status_code=401, content={"detail": "Missing or invalid credentials."})

            # Intercept UI trace deletion requests so they perform soft-delete (DELETED_IN_UI_AT) instead of physical deletion on Snowflake
            if method == "DELETE" and any(
                p in path
                for p in ("/api/v2/traces", "/api/v1/monitor/transactions", "/api/v1/monitor/builds")
            ):
                del_ids: list[str] = []
                m_tid = re.search(r"/(?:traces|transactions|builds)/([^/?#]+)", path)
                if m_tid:
                    del_ids.append(m_tid.group(1))
                for qk in ("id", "ids", "trace_id", "trace_ids", "activity_id", "transaction_id"):
                    qv = request.query_params.get(qk)
                    if qv:
                        del_ids.extend(x.strip() for x in qv.split(",") if x.strip())
                soft_res = await asyncio.to_thread(
                    LIVE_BRIDGE.soft_delete_flow_activity,
                    del_ids,
                    flow_id=norm_fid or request.query_params.get("flow_id"),
                )
                if path.startswith("/api/v2/traces"):
                    return JSONResponse(status_code=200, content=soft_res)

            # Intercept GET /api/v2/traces to populate Langflow UI Flow Activity -> Runs (`CTX-ARCH-06`)
            if method == "GET" and path.rstrip("/") == "/api/v2/traces":
                traces_payload = await asyncio.to_thread(
                    LIVE_BRIDGE.get_ui_flow_activity_traces,
                    flow_id=norm_fid,
                    limit=int(request.query_params.get("size") or request.query_params.get("limit") or 50),
                )
                return JSONResponse(status_code=200, content=traces_payload)

            # Fast-path /health_check and /api/v1/health_check so frontend health polling never stalls or triggers "Unable to reach the server"
            clean_path = path.rstrip("/")
            if method == "GET" and clean_path in ("/health", "/health_check", "/api/v1/health", "/api/v1/health_check"):
                if clean_path.endswith("/health"):
                    return JSONResponse(status_code=200, content={"status": "ok"})
                return JSONResponse(status_code=200, content={"status": "ok", "chat": "ok", "db": "ok"})

            # SPCS Auto-Login Always-On Guard: never prompt for username/password on /login or expire /api/v1/* sessions when LANGFLOW_AUTO_LOGIN=true
            auto_login_enabled = os.getenv("LANGFLOW_AUTO_LOGIN", "true").strip().lower() not in ("0", "false", "no")
            if auto_login_enabled:
                if clean_path == "/api/v1/logout" and method in ("POST", "GET"):
                    return JSONResponse(status_code=200, content={"detail": "Auto-login active; session preserved"})
                if clean_path in ("/api/v1/login", "/api/v1/refresh") and method == "POST":
                    request.scope["method"] = "GET"
                    request.scope["path"] = "/api/v1/auto_login"
                    request.scope["raw_path"] = b"/api/v1/auto_login"
                    clean_path = "/api/v1/auto_login"
                    path = "/api/v1/auto_login"
                    method = "GET"
                elif clean_path == "/login" and method == "GET":
                    redirect_target = str(request.query_params.get("redirect") or "/").strip()
                    if not redirect_target.startswith("/") or redirect_target.startswith("/login"):
                        redirect_target = "/"
                    from starlette.responses import RedirectResponse
                    redir_resp = RedirectResponse(url=redirect_target, status_code=302)
                    cached_tok = getattr(LIVE_BRIDGE, "_cached_auto_login_access_token", None)
                    cached_ref = getattr(LIVE_BRIDGE, "_cached_auto_login_refresh_token", None)
                    if cached_tok:
                        redir_resp.set_cookie("access_token_lf", cached_tok, httponly=True, path="/", samesite="lax")
                    if cached_ref:
                        redir_resp.set_cookie("refresh_token_lf", cached_ref, httponly=True, path="/", samesite="lax")
                    redir_resp.set_cookie("auto_login_lf", "true", httponly=False, path="/", samesite="lax")
                    return redir_resp
                # Frontend bundle patches are applied on disk by scripts/install_live_hooks_in_container.py
                # (fail closed, per Langflow version). /assets/* must be served verbatim: Vite also names
                # lazy route chunks index-<hash>.js, so substituting the entry bundle here re-ran the app.

                # Inject cached superuser Bearer token into /api/v1/* requests when cookie/Authorization is missing
                cached_tok = getattr(LIVE_BRIDGE, "_cached_auto_login_access_token", None)
                cached_ts = float(getattr(LIVE_BRIDGE, "_cached_auto_login_ts", 0.0) or 0.0)
                if (
                    (not cached_tok or (time.time() - cached_ts) >= 2400.0)
                    and clean_path.startswith("/api/v1/")
                    and clean_path not in ("/api/v1/auto_login", "/api/v1/config")
                    and (not _is_protected_path(clean_path) or _is_spcs_browser_session(request))
                ):
                    def _warmup_auto_login_token() -> tuple[str | None, str | None]:
                        try:
                            port = os.getenv("LANGFLOW_PORT", "7860").strip() or "7860"
                            req_al = urllib_request.Request(
                                f"http://127.0.0.1:{port}/api/v1/auto_login",
                                headers={"Accept": "application/json"},
                            )
                            with urllib_request.urlopen(req_al, timeout=3.0) as r_al:
                                data_al = json.loads(r_al.read().decode("utf-8"))
                                return data_al.get("access_token"), data_al.get("refresh_token")
                        except Exception:  # noqa: BLE001
                            return None, None

                    warm_tok, warm_ref = await asyncio.to_thread(_warmup_auto_login_token)
                    if warm_tok:
                        LIVE_BRIDGE._cached_auto_login_access_token = str(warm_tok)
                        LIVE_BRIDGE._cached_auto_login_refresh_token = str(warm_ref or "")
                        LIVE_BRIDGE._cached_auto_login_ts = time.time()
                        cached_tok = str(warm_tok)
                        cached_ts = LIVE_BRIDGE._cached_auto_login_ts

                # A6-2: the cached superuser token never authenticates the Snowflake perimeter.
                if (
                    cached_tok
                    and (time.time() - cached_ts) < 2400.0
                    and clean_path.startswith("/api/v1/")
                    and clean_path != "/api/v1/auto_login"
                    and (not _is_protected_path(clean_path) or _is_spcs_browser_session(request))
                ):
                    raw_headers = list(request.scope.get("headers") or [])
                    has_auth_hdr = any(k.lower() == b"authorization" and b"bearer " in v.lower() for k, v in raw_headers)
                    cookie_hdr = b"; ".join(v for k, v in raw_headers if k.lower() == b"cookie")
                    if not has_auth_hdr and b"access_token_lf=" not in cookie_hdr:
                        raw_headers.append((b"authorization", f"Bearer {cached_tok}".encode("utf-8")))
                        if cookie_hdr:
                            raw_headers = [
                                (k, (v + f"; access_token_lf={cached_tok}; auto_login_lf=true".encode("utf-8")) if k.lower() == b"cookie" else v)
                                for k, v in raw_headers
                            ]
                        else:
                            raw_headers.append((b"cookie", f"access_token_lf={cached_tok}; auto_login_lf=true".encode("utf-8")))
                        request.scope["headers"] = raw_headers

            # Intercept native Langflow /api/v1/memories* so Snowflake Memories are first-class citizens (`W2-R4-08G`)
            if clean_path == "/api/v1/memories" and method == "GET":
                req_search = request.query_params.get("search") or request.query_params.get("q")
                req_page = int(request.query_params.get("page") or 1)
                req_size = int(request.query_params.get("size") or 50)
                mem_list_uid = await _langflow_user_id_from_request(request)
                mem_list_payload = await asyncio.to_thread(
                    lambda: LIVE_BRIDGE.list_native_memories(
                        user_id=mem_list_uid,
                        flow_id=norm_fid,
                        search=req_search,
                        page=req_page,
                        size=req_size,
                    )
                )
                return JSONResponse(status_code=200, content=mem_list_payload)

            if clean_path == "/api/v1/memories" and method == "POST":
                try:
                    create_body = await request.json()
                    create_in = create_body if isinstance(create_body, dict) else {}
                except Exception:  # noqa: BLE001
                    create_in = {}
                create_uid = await _langflow_user_id_from_request(request)
                create_fid = request.query_params.get("flow_id") or create_in.get("flow_id") or norm_fid
                create_res = await asyncio.to_thread(
                    lambda: LIVE_BRIDGE.create_native_memory(create_in, flow_id=create_fid, user_id=create_uid)
                )
                create_status = int(create_res.get("status_code") or (201 if create_res.get("ok") else 422))
                return JSONResponse(status_code=create_status, content=create_res)

            m_mem_sub = re.match(r"^/api/v1/memories/([^/]+)(?:/(sessions|messages))?$", clean_path)
            if m_mem_sub:
                target_mem_id = m_mem_sub.group(1)
                sub_resource = m_mem_sub.group(2)
                if sub_resource == "sessions" and method == "GET":
                    req_page = int(request.query_params.get("page") or 1)
                    req_size = int(request.query_params.get("size") or 50)
                    sess_payload = await asyncio.to_thread(
                        lambda: LIVE_BRIDGE.list_native_memory_sessions(
                            target_mem_id,
                            page=req_page,
                            size=req_size,
                        )
                    )
                    return JSONResponse(status_code=200, content=sess_payload)
                if sub_resource == "messages" and method == "GET":
                    req_sess = request.query_params.get("session_id") or request.query_params.get("session")
                    req_page = int(request.query_params.get("page") or 1)
                    req_size = int(request.query_params.get("size") or 50)
                    msgs_uid = await _langflow_user_id_from_request(request)
                    msgs_payload = await asyncio.to_thread(
                        lambda: LIVE_BRIDGE.list_native_memory_messages(
                            target_mem_id, user_id=msgs_uid,
                            session_id=req_sess,
                            page=req_page,
                            size=req_size,
                        )
                    )
                    return JSONResponse(status_code=200, content=msgs_payload)
                if sub_resource is None:
                    if method == "GET" and LIVE_BRIDGE._find_memory_entry(target_mem_id) is not None:
                        det_payload = await asyncio.to_thread(LIVE_BRIDGE.get_native_memory, target_mem_id)
                        return JSONResponse(status_code=int(det_payload.get("status_code") or 200), content=det_payload)
                    if method == "PATCH" and LIVE_BRIDGE._find_memory_entry(target_mem_id) is not None:
                        try:
                            patch_body = await request.json()
                            patch_in = patch_body if isinstance(patch_body, dict) else {}
                        except Exception:  # noqa: BLE001
                            patch_in = {}
                        patch_uid = await _langflow_user_id_from_request(request)
                        patch_res = await asyncio.to_thread(
                            lambda: LIVE_BRIDGE.patch_native_memory(target_mem_id, patch_in, user_id=patch_uid)
                        )
                        return JSONResponse(status_code=int(patch_res.get("status_code") or 200), content=patch_res)
                    if method == "DELETE" and LIVE_BRIDGE._find_memory_entry(target_mem_id) is not None:
                        del_res = await asyncio.to_thread(LIVE_BRIDGE.delete_memory, target_mem_id)
                        return JSONResponse(status_code=int(del_res.get("status_code") or 200), content=del_res)

            flow_body_for_triggers: dict[str, Any] | None = None
            batch_deleted_flow_ids: list[str] = []
            if method in ("PUT", "PATCH") and "/api/v1/flows/" in path and matched_fid:
                try:
                    raw_body_bytes = await request.body()
                    if raw_body_bytes:
                        parsed_body = json.loads(raw_body_bytes.decode("utf-8"))
                        if isinstance(parsed_body, dict):
                            flow_body_for_triggers = parsed_body
                except Exception:  # noqa: BLE001
                    flow_body_for_triggers = None
            elif method == "DELETE" and path.rstrip("/") == "/api/v1/flows":
                try:
                    raw_del_bytes = await request.body()
                    if raw_del_bytes:
                        parsed_del = json.loads(raw_del_bytes.decode("utf-8"))
                        if isinstance(parsed_del, list):
                            batch_deleted_flow_ids = [str(x).strip() for x in parsed_del if str(x).strip()]
                except Exception:  # noqa: BLE001
                    batch_deleted_flow_ids = []

            token = CURRENT_SNOWFLAKE_FLOW_ID.set(norm_fid)
            try:
                response = await call_next(request)
            finally:
                CURRENT_SNOWFLAKE_FLOW_ID.reset(token)

            # Bundle patches rewrite JS chunks in place without renaming them, so a content-hashed
            # name is not proof of freshness: browsers must revalidate (ETag) after every release.
            if method == "GET" and (
                (clean_path.startswith("/assets/") and clean_path.endswith(".js"))
                or clean_path in ("/snowflake-ui-integration.js", "/preserved_provisioning_and_memories_concept.js")
            ):
                response.headers["Cache-Control"] = "no-cache"

            if auto_login_enabled and clean_path == "/api/v1/auto_login" and getattr(response, "status_code", 200) == 200:
                try:
                    raw_resp_headers = getattr(response, "raw_headers", None) or []
                    for hk, hv in raw_resp_headers:
                        if hk.lower() == b"set-cookie":
                            sc_str = hv.decode("utf-8", errors="ignore")
                            m_acc = re.search(r"access_token_lf=([^;\s]+)", sc_str)
                            if m_acc:
                                LIVE_BRIDGE._cached_auto_login_access_token = m_acc.group(1).strip('"')
                                LIVE_BRIDGE._cached_auto_login_ts = time.time()
                            m_ref = re.search(r"refresh_token_lf=([^;\s]+)", sc_str)
                            if m_ref:
                                LIVE_BRIDGE._cached_auto_login_refresh_token = m_ref.group(1).strip('"')
                except Exception:  # noqa: BLE001
                    pass

            if method == "DELETE" and "/api/v1/flows/" in path and matched_fid:
                await asyncio.to_thread(
                    LIVE_BRIDGE.cleanup_flow_triggers,
                    str(norm_fid or matched_fid),
                )
                _sf_identity.submit_background(_AUDIT_EXECUTOR, 
                    LIVE_BRIDGE.record_audit_event,
                    action="FLOW_DELETED_IN_UI",
                    outcome="ALLOWED",
                    target_object=str(matched_fid),
                    flow_id=norm_fid,
                    details={"path": path, "method": method, "durable_snowflake_audit_preserved": True},
                )
            elif method == "DELETE" and batch_deleted_flow_ids:
                for b_fid in batch_deleted_flow_ids:
                    await asyncio.to_thread(LIVE_BRIDGE.cleanup_flow_triggers, b_fid)
            elif method in ("PUT", "PATCH") and "/api/v1/flows/" in path and matched_fid and flow_body_for_triggers is not None:
                if getattr(response, "status_code", 200) < 400:
                    try:
                        await asyncio.to_thread(
                            LIVE_BRIDGE.sync_flow_triggers,
                            str(norm_fid or matched_fid),
                            flow_body_for_triggers,
                        )
                    except Exception:  # noqa: BLE001
                        pass
            elif method == "POST" and path.rstrip("/") in ("/api/v1/flows", "/api/v1/flows/upload"):
                _sf_identity.submit_background(_AUDIT_EXECUTOR, LIVE_BRIDGE._load_flow_contexts_sqlite)
            elif any(seg in path for seg in ("/api/v1/build/", "/api/v1/run/", "/api/v1/webhook/")):
                if "/api/v1/webhook/" in path:
                    wh_fid = str(norm_fid or matched_fid or "webhook-flow")
                    wh_run_id = CURRENT_RUN_ID.get() or f"wh_run_{uuid.uuid4().hex[:16]}"
                    wh_status = "SUCCEEDED" if getattr(response, "status_code", 200) < 400 else "FAILED"
                    await asyncio.to_thread(
                        lambda: LIVE_BRIDGE.record_trigger_execution_telemetry(
                            flow_id=wh_fid,
                            run_id=wh_run_id,
                            trace_id=f"wh_trc_{uuid.uuid4().hex[:16]}",
                            trigger_type="WEBHOOK_RUN",
                            status=wh_status,
                            duration_ms=10.0,
                            trigger_payload={"path": path, "method": method, "trigger_type": "WEBHOOK_RUN"},
                            result_payload={"status": wh_status, "run_id": wh_run_id},
                        )
                    )
                _sf_identity.submit_background(_AUDIT_EXECUTOR, 
                    LIVE_BRIDGE.sync_langflow_traces_to_snowflake,
                    25,
                    replicate_sqlite=True,
                )
            return response

        @app.middleware("http")
        async def snowflake_flow_context_and_trace_middleware(request: Request, call_next: Any) -> Any:
            # W3-R1-10b: identity is bound per request from ingress headers, never stored globally.
            ident_token = _sf_identity.bind_identity(_sf_identity.identity_from_headers(request.headers))
            # W3-R1-10c: user-facing routes request caller mode (effective only when the flag is ON).
            mode_token = _sf_identity.bind_request_mode(_sf_identity.mode_for_path(request.url.path))
            # W3-R1-14a/14d: per-request correlation ids + allowlisted lifecycle logs (no content).
            req_path = str(getattr(request.url, "path", "") or "")
            # Reuse the id assigned by RequestIdASGIMiddleware (outermost) so headers and logs agree.
            request_id = (getattr(request, "scope", None) or {}).get(_trace.REQUEST_ID_SCOPE_KEY) or _trace.request_id_from_header(
                request.headers.get(_trace.REQUEST_ID_HEADER)
            )
            run_id = _trace.run_id_for_request(req_path, request.headers.get("X-Run-Id"))
            # BUG-LLMOP-A4-01: one run record (unique run_id + trace_id) per flow execution request.
            run_rec = None
            m_run_path = _FLOW_EXEC_PATH_RE.search(req_path) if run_id else None
            if m_run_path:
                path_fid = m_run_path.group("b") or m_run_path.group("r") or m_run_path.group("w")
                # T10: Langflow's transaction/vertex_build writers stamp rows with this run (contextvar).
                _telemetry.install_langflow_row_stamping()
                run_rec = _telemetry.RUNS.start(
                    flow_id=LIVE_BRIDGE._normalize_flow_id(path_fid) or path_fid,
                    run_id=run_id,
                    request_id=request_id,
                    session_id=(
                        request.query_params.get("session_id")
                        or request.headers.get("X-Session-Id")
                        or await _session_id_from_body(request)
                    ),
                )
            run_token = _telemetry.CURRENT_RUN_RECORD.set(run_rec)
            started = time.monotonic()
            outcome = "FAILURE"
            status_code = 500
            try:
                with _trace.trace_scope(
                    request_id=request_id,
                    run_id=run_id,
                    trace_id=run_rec.trace_id if run_rec is not None else None,
                    session_id=run_rec.session_id if run_rec is not None else None,
                ):
                    _trace.log_event("request_start")
                    if run_id:
                        _trace.log_event("flow_run_start")
                    try:
                        response = await _snowflake_flow_context_and_trace(request, call_next)
                        status_code = int(getattr(response, "status_code", 200) or 200)
                        outcome = "SUCCESS" if status_code < 400 else "FAILURE"
                        try:
                            response.headers[_trace.REQUEST_ID_HEADER] = request_id
                            response.headers[_trace.REQUEST_ID_FALLBACK_HEADER] = request_id
                        except Exception:  # noqa: BLE001
                            pass
                        return response
                    finally:
                        duration_ms = int((time.monotonic() - started) * 1000)
                        err = None if outcome == "SUCCESS" else f"HTTP_{status_code}"
                        _trace.log_event("request_end", outcome=outcome, duration_ms=duration_ms, error_code=err)
                        if run_id:
                            _trace.log_event(
                                "flow_run_end",
                                flow_id=CURRENT_SNOWFLAKE_FLOW_ID.get() or LIVE_BRIDGE.active_flow_id,
                                outcome=outcome,
                                duration_ms=duration_ms,
                                error_code=err,
                            )
            finally:
                _telemetry.CURRENT_RUN_RECORD.reset(run_token)
                if run_rec is not None:
                    try:
                        LIVE_BRIDGE.record_flow_run(_telemetry.RUNS.end(run_rec.run_id, outcome))
                        if LIVE_BRIDGE.auto_sync_enabled:
                            LIVE_BRIDGE.trigger_close_span_sync(run_rec.flow_id, since=run_rec.start_ts)  # rerun-4: spans within seconds
                    except Exception:  # noqa: BLE001
                        pass
                _sf_identity.reset_request_mode(mode_token)
                _sf_identity.reset_identity(ident_token)

        # Added last = outermost: every response (streaming, static, errors, unhandled exceptions)
        # carries the request id in both headers and in JSON error envelopes.
        if hasattr(app, "add_middleware"):
            # BUG-A2A-01: bind inbound X-Snowflake-A2A-Call-Chain / -Max-Depth for nested A2A hops.
            app.add_middleware(_a2a_chain.CallChainASGIMiddleware)
            # BUG-MCP-05: auto-registered project MCP servers use direct streamable HTTP (no uvx mcp-proxy).
            from lfx_snowflake.mcp_project_servers import ProjectMCPServerReconcileASGIMiddleware  # noqa: PLC0415

            app.add_middleware(ProjectMCPServerReconcileASGIMiddleware)
            app.add_middleware(_trace.RequestIdASGIMiddleware)

    @sf_router.get("/persistence")
    async def get_persistence_status() -> JSONResponse:
        info = await asyncio.to_thread(LIVE_BRIDGE.sync_persistent_workflows_backup)
        return JSONResponse(status_code=200, content=info)

    @sf_router.get("/context")
    async def get_snowflake_context(request: Request, flow_id: str | None = None) -> JSONResponse:
        await asyncio.to_thread(LIVE_BRIDGE.sync_superuser_credentials)
        await asyncio.to_thread(LIVE_BRIDGE.refresh_roles_from_snowflake)
        req_flow_id = LIVE_BRIDGE._normalize_flow_id(
            flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        )
        # BUG-CTX-01: read-only - never switches the global active flow/database/schema.
        if req_flow_id:
            active_ctx = copy.deepcopy(
                await asyncio.to_thread(lambda: LIVE_BRIDGE.get_flow_context(req_flow_id, create=False))
            )
        else:
            active_ctx = copy.deepcopy(LIVE_BRIDGE.selected_context)
        intersection = await asyncio.to_thread(
            LIVE_BRIDGE.compute_role_effective_context,
            req_flow_id,
            LIVE_BRIDGE.active_role,
        )
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "ok": True,
                "flow_id": req_flow_id,
                "active_flow_id": LIVE_BRIDGE.active_flow_id,
                "authenticated_user": LIVE_BRIDGE.authenticated_user,
                "active_role": LIVE_BRIDGE.active_role,
                "available_roles": LIVE_BRIDGE.available_roles,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "secondary_roles_mode": LIVE_BRIDGE.secondary_roles_mode,
                "database": active_ctx.get("database") or LIVE_BRIDGE.database,
                "schema": active_ctx.get("schema") or LIVE_BRIDGE.schema,
                "stage_name": f"@{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.{LIVE_BRIDGE.stage_name}",
                "role_privileges": LIVE_BRIDGE.role_privileges,
                "selected_context": active_ctx,
                "effective_context": intersection["effective_context"],
                "inaccessible_objects": intersection["inaccessible_objects"],
                "retained_objects_count": intersection["retained_objects_count"],
                "hidden_by_role_count": intersection["hidden_by_role_count"],
                # BUG-CTX-01: only the requested flow's context is returned.
                "auto_sync_enabled": LIVE_BRIDGE.auto_sync_enabled,
                "last_auto_sync_ts": LIVE_BRIDGE.last_auto_sync_ts,
                "synced_runs_count": LIVE_BRIDGE.synced_runs_count,
                "synced_spans_count": LIVE_BRIDGE.synced_spans_count,
                "synced_activity_count": LIVE_BRIDGE.synced_activity_count,
                "traces_tables": {
                    "activity": LIVE_BRIDGE.activity_table,
                    "runs": LIVE_BRIDGE.runs_table,
                    "spans": LIVE_BRIDGE.spans_table,
                    "audit": LIVE_BRIDGE.audit_table,
                },
                "branding": SNOWFLAKE_BRANDING_CONTRACT,
                "new_flow_import_modes": NEW_FLOW_IMPORT_MODES,
                "settings": SETTINGS_CONTRACT,
                "invariant_categories": INVARIANT_CATEGORIES_MANIFEST,
                "error_taxonomy": ERROR_TAXONOMY_CONTRACT,
                "bundle": get_bundle_manifest(),
            },
        )

    @sf_router.post("/context")
    @sf_router.put("/context")
    async def post_snowflake_context(request: Request, flow_id: str | None = None) -> JSONResponse:
        body = await request.json()
        req_flow_id = (
            flow_id
            or request.query_params.get("flow_id")
            or request.headers.get("X-Snowflake-Flow-Id")
            or (body.get("flow_id") if isinstance(body, dict) else None)
        )
        res = await asyncio.to_thread(
            LIVE_BRIDGE.update_selected_context,
            body if isinstance(body, dict) else {},
            req_flow_id,
        )
        return JSONResponse(status_code=200, content=res)

    @sf_router.post("/role")
    async def post_switch_role(request: Request) -> JSONResponse:
        body = await request.json()
        target_role = str(body.get("target_role") or body.get("role") or "")
        req_flow_id = (
            request.query_params.get("flow_id")
            or request.headers.get("X-Snowflake-Flow-Id")
            or (body.get("flow_id") if isinstance(body, dict) else None)
        )
        result = await asyncio.to_thread(LIVE_BRIDGE.switch_role, target_role, req_flow_id)
        return JSONResponse(status_code=result.get("status_code", 200), content=result)

    @sf_router.get("/catalog")
    async def get_snowflake_catalog(
        database: str | None = None,
        schema: str | None = None,
        object_kind: str | None = None,
        databases: str | None = None,
        schemas_by_db: str | None = None,
    ) -> JSONResponse:
        catalog_data = await asyncio.to_thread(
            LIVE_BRIDGE.discover_catalog,
            database=database,
            schema=schema,
            object_kind=object_kind,
            databases=databases,
            schemas_by_db=schemas_by_db,
        )
        return JSONResponse(status_code=200, content=catalog_data)

    @sf_router.get("/schemas")
    async def get_snowflake_schemas(
        database: str | None = None,
        databases: str | None = None,
    ) -> JSONResponse:
        catalog_data = await asyncio.to_thread(
            LIVE_BRIDGE.discover_schemas_only,
            database=database,
            databases=databases,
        )
        return JSONResponse(status_code=200, content=catalog_data)

    @sf_router.get("/objects")
    async def get_snowflake_objects(
        database: str | None = None,
        schema: str | None = None,
        object_kind: str | None = None,
    ) -> JSONResponse:
        catalog_data = await asyncio.to_thread(
            LIVE_BRIDGE.discover_catalog,
            database=database,
            schema=schema,
            object_kind=object_kind,
        )
        return JSONResponse(status_code=200, content=catalog_data)

    @sf_router.post("/analyst/message")
    async def post_cortex_analyst_message(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            res = await asyncio.to_thread(
                LIVE_BRIDGE.invoke_cortex_analyst,
                database=str(body.get("database") or LIVE_BRIDGE.database),
                schema=str(body.get("schema") or body.get("schema_name") or LIVE_BRIDGE.schema),
                semantic_source=str(
                    body.get("semantic_source")
                    or body.get("semantic_view")
                    or body.get("semantic_model_file")
                    or f"{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.EXAMPLE_SEMANTIC_VIEW"
                ),
                source_mode=str(body.get("source_mode") or "Semantic View"),
                question=str(body.get("question") or "Show latest flow metrics"),
                execute_sql=bool(body.get("execute_generated_sql", True)),
                max_rows=int(body.get("max_rows") or 50),
            )
            return JSONResponse(status_code=200, content=res)
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": str(exc),
                },
            )

    @sf_router.post("/sql")
    async def post_execute_governed_sql(request: Request) -> JSONResponse:
        body = await request.json()
        statement = str(body.get("statement") or body.get("query") or "").strip()
        if not statement:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error_code": "TECHNICAL_EXECUTION_ERROR", "message": "Empty SQL statement."},
            )
        is_write = any(
            statement.upper().startswith(prefix)
            for prefix in ("CREATE ", "ALTER ", "DROP ", "INSERT ", "UPDATE ", "DELETE ", "MERGE ", "COPY ", "PUT ")
        )
        if is_write:
            denied = await asyncio.to_thread(LIVE_BRIDGE.check_write_privilege_or_deny, "SQL_WRITE", statement[:80])
            if denied:
                return JSONResponse(status_code=403, content=denied)
        res = await asyncio.to_thread(LIVE_BRIDGE.execute_sql, statement)
        status_code = res.get("status_code", 200 if res.get("ok") else 500)
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.post("/provision")
    async def post_provision_object(request: Request) -> JSONResponse:
        body = await request.json()
        source = str(body.get("source") or "hub").strip().lower()
        obj_kind = str(body.get("object_kind") or "table").strip().lower()
        obj_subtype = str(
            body.get("object_subtype") or body.get("table_type") or body.get("stage_type") or "STANDARD"
        ).strip().upper()
        dry_run = bool(body.get("dry_run", False))
        confirmed = bool(body.get("confirmed", False))
        target_name = str(body.get("name") or body.get("object_name") or "NEW_OBJECT").strip()

        # W2-R2-04B / W2-R2-13: Memory is relocated exclusively to the Memories page (+ Snowflake)
        if obj_kind in {"memory", "memories", "knowledge_memory"}:
            msg = (
                "La creazione di 'memory' non è consentita da Object Provisioner (W2-R2-04B, W2-R2-13). "
                "Usa la pagina 'Memories -> + Snowflake' (Episodic / Session, Semantic / Vector, Structured / Semi-Structured Long-Term)."
            )
            LIVE_BRIDGE.record_audit_event(
                action="PROVISION_MEMORY_REJECTED",
                outcome="DENIED",
                error_code=ErrorCode.UNSUPPORTED_CAPABILITY.value,
                target_object=target_name,
                details={"source": source, "object_kind": obj_kind, "reason": msg},
            )
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": msg,
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                },
            )

        # HUB-W2-04: Strictly block FUNCTION / PROCEDURE creation from Snowflake Langflow Hub
        forbidden_hub_kinds = {"function", "procedure", "udf", "udtf", "stored_procedure", "stored procedure"}
        if source == "hub" and (obj_kind in forbidden_hub_kinds or obj_subtype.lower() in forbidden_hub_kinds):
            msg = (
                f"La creazione di '{obj_kind}' non è consentita dallo Snowflake Langflow Hub (HUB-W2-04). "
                "Usa i componenti dedicati in 'LLM Operations -> Stored Procedure / Function'."
            )
            LIVE_BRIDGE.record_audit_event(
                action="HUB_PROVISION_REJECTED",
                outcome="DENIED",
                error_code=ErrorCode.UNSUPPORTED_CAPABILITY.value,
                target_object=target_name,
                details={"source": source, "object_kind": obj_kind, "reason": msg},
            )
            return JSONResponse(
                status_code=422,
                content={
                    "ok": False,
                    "status_code": 422,
                    "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                    "message": msg,
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                },
            )

        db = str(body.get("database") or LIVE_BRIDGE.database).strip().upper()
        sch = str(body.get("schema") or LIVE_BRIDGE.schema).strip().upper()
        hierarchy_path = ["ACCOUNT", db] if obj_kind == "database" else (
            ["ACCOUNT", db, target_name.upper()] if obj_kind == "schema" else ["ACCOUNT", db, sch, target_name.upper()]
        )

        # Enforce active-role RBAC checks at the requested hierarchy level (HUB-W2-03)
        denied = LIVE_BRIDGE.check_write_privilege_or_deny(f"PROVISION_{obj_kind.upper()}", f"{db}.{sch}.{target_name}")
        if denied:
            return JSONResponse(status_code=403, content=denied)
        if obj_kind in ("database", "postgres") and not LIVE_BRIDGE.can_create_database():
            msg = f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi per creare {obj_kind.upper()} sull'Account."
            LIVE_BRIDGE.record_audit_event(
                action=f"PROVISION_{obj_kind.upper()}",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=target_name,
                details={"message": msg},
            )
            return JSONResponse(
                status_code=403,
                content={
                    "ok": False,
                    "status_code": 403,
                    "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                    "message": msg,
                    "active_role": LIVE_BRIDGE.active_role,
                    "role_privileges": LIVE_BRIDGE.role_privileges,
                },
            )

        if obj_kind == "database":
            ddl = f"CREATE DATABASE IF NOT EXISTS {target_name};"
        elif obj_kind == "schema":
            ddl = f"CREATE SCHEMA IF NOT EXISTS {db}.{target_name};"
        elif obj_kind == "stage":
            is_ext = "EXTERNAL" in obj_subtype or bool(body.get("stage_url"))
            if is_ext:
                stage_url = str(body.get("stage_url") or "s3://<BUCKET>/<PREFIX>/").strip()
                storage_int = str(body.get("storage_integration") or "<STORAGE_INTEGRATION>").strip()
                if any(tok in stage_url.lower() for tok in ("aws_secret", "sas=", "token=", "password=", "key=")):
                    return JSONResponse(
                        status_code=422,
                        content={
                            "ok": False,
                            "status_code": 422,
                            "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                            "message": "Inline cloud credentials in external stage URL are forbidden; use STORAGE_INTEGRATION.",
                            "active_role": LIVE_BRIDGE.active_role,
                        },
                    )
                ddl = (
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"URL = '{stage_url}' STORAGE_INTEGRATION = {storage_int} "
                    f"DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);"
                )
            else:
                ddl = (
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
                )
        elif obj_kind == "view":
            select_sql = str(body.get("as_select_sql") or f"SELECT * FROM {LIVE_BRIDGE.runs_table}").strip()
            ddl = f"CREATE OR REPLACE VIEW {db}.{sch}.{target_name} AS {select_sql};"
        elif obj_kind == "postgres":
            family = str(body.get("compute_family") or "STANDARD_M")
            storage_gb = int(body.get("storage_size_gb") or 10)
            ddl = (
                f"CREATE POSTGRES INSTANCE IF NOT EXISTS {target_name} "
                f"COMPUTE_FAMILY = '{family}' STORAGE_SIZE_GB = {storage_gb} "
                f"AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
            )
        elif obj_kind in forbidden_hub_kinds:
            # Allowed when source != "hub" (e.g., LLM Operations Stored Procedure / Function component)
            lang = str(body.get("language") or "SQL").upper()
            ret_type = str(body.get("return_type") or "VARIANT")
            sig_args = str(body.get("signature_args") or "")
            proc_body = str(body.get("body") or "SELECT CURRENT_ROLE()")
            kw = "PROCEDURE" if "proc" in obj_kind else "FUNCTION"
            ddl = f"CREATE OR REPLACE {kw} {db}.{sch}.{target_name}({sig_args}) RETURNS {ret_type} LANGUAGE {lang} AS $${proc_body}$$;"
        else:
            table_type = str(body.get("table_type") or obj_subtype or "STANDARD").upper()
            include_vector = bool(body.get("include_vector", "vector_dimension" in body and table_type != "ICEBERG"))
            vector_dim = int(body.get("vector_dimension") or 768)
            elem_type = str(body.get("vector_element_type") or "FLOAT").upper()
            if table_type == "ICEBERG":
                if body.get("include_vector") is True:
                    return JSONResponse(
                        status_code=422,
                        content={
                            "ok": False,
                            "status_code": 422,
                            "error_code": ErrorCode.UNSUPPORTED_CAPABILITY.value,
                            "message": "Native VECTOR columns are not supported on Iceberg Tables (HUB-W2-05).",
                            "active_role": LIVE_BRIDGE.active_role,
                        },
                    )
                ext_vol = str(body.get("external_volume") or "<EXTERNAL_VOLUME>").strip()
                base_loc = str(body.get("base_location") or target_name.lower()).strip()
                ddl = (
                    f"CREATE ICEBERG TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id STRING, content STRING, created_at TIMESTAMP_TZ) "
                    f"EXTERNAL_VOLUME = '{ext_vol}' CATALOG = 'SNOWFLAKE' BASE_LOCATION = '{base_loc}';"
                )
            elif table_type == "INTERACTIVE":
                wh = str(body.get("warehouse") or LIVE_BRIDGE.warehouse or "<WAREHOUSE>").strip()
                lag = str(body.get("target_lag") or "1 minute").strip()
                ddl = (
                    f"CREATE INTERACTIVE TABLE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"TARGET_LAG = '{lag}' WAREHOUSE = {wh} AS "
                    f"SELECT run_id AS id, flow_id AS content, start_ts AS created_at FROM {LIVE_BRIDGE.runs_table};"
                )
            elif table_type == "DYNAMIC":
                wh = str(body.get("warehouse") or LIVE_BRIDGE.warehouse or "<WAREHOUSE>").strip()
                lag = str(body.get("target_lag") or "5 minutes").strip()
                ddl = (
                    f"CREATE DYNAMIC TABLE IF NOT EXISTS {db}.{sch}.{target_name} "
                    f"TARGET_LAG = '{lag}' WAREHOUSE = {wh} REFRESH_MODE = AUTO AS "
                    f"SELECT run_id AS id, flow_id AS content, start_ts AS created_at FROM {LIVE_BRIDGE.runs_table};"
                )
            elif table_type == "HYBRID":
                vec_col = f"embedding VECTOR({elem_type}, {vector_dim}), " if include_vector else ""
                ddl = (
                    f"CREATE HYBRID TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                    f"{vec_col}created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
                )
            else:
                vec_col = f"embedding VECTOR({elem_type}, {vector_dim}), " if include_vector else ""
                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {db}.{sch}.{target_name} ("
                    f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                    f"{vec_col}created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
                )

        if dry_run or not confirmed:
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "dry_run": True,
                    "confirmed": confirmed,
                    "active_role": LIVE_BRIDGE.active_role,
                    "database": db,
                    "schema": sch,
                    "hierarchy_path": hierarchy_path,
                    "ddl_preview": ddl,
                    "role_privileges": LIVE_BRIDGE.role_privileges,
                    "message": "Dry-run DDL preview generated. Set confirmed=true to execute on Snowflake.",
                },
            )

        exec_res = await asyncio.to_thread(LIVE_BRIDGE.execute_sql, ddl)
        req_flow_id = (
            request.query_params.get("flow_id")
            or request.headers.get("X-Snowflake-Flow-Id")
            or (body.get("flow_id") if isinstance(body, dict) else None)
        )
        registered_ctx = None
        if exec_res.get("ok") or exec_res.get("mode") == "offline":
            registered_ctx = await asyncio.to_thread(
                LIVE_BRIDGE.register_object_in_context,
                db,
                sch,
                obj_kind,
                target_name,
                req_flow_id,
            )
        await asyncio.to_thread(
            LIVE_BRIDGE.record_audit_event,
            action=f"PROVISION_{obj_kind.upper()}",
            outcome="ALLOWED" if exec_res.get("ok") else "ERROR",
            error_code=exec_res.get("error_code"),
            target_object=f"{db}.{sch}.{target_name}",
            flow_id=req_flow_id,
            details={"ddl": ddl, "hierarchy_path": hierarchy_path, "result": exec_res},
        )
        return JSONResponse(
            status_code=exec_res.get("status_code", 200 if exec_res.get("ok") else 500),
            content={
                "ok": exec_res.get("ok", True),
                "database": db,
                "schema": sch,
                "hierarchy_path": hierarchy_path,
                "ddl_executed": ddl,
                "result": exec_res,
                "registered_in_context": registered_ctx is not None,
                "selected_context": registered_ctx,
            },
        )

    @sf_router.get("/traces")
    async def get_replicated_traces(limit: int = 25) -> JSONResponse:
        data = await asyncio.to_thread(
            LIVE_BRIDGE.sync_langflow_traces_to_snowflake,
            limit=limit,
            replicate_sqlite=False,
        )
        return JSONResponse(status_code=200, content=data)

    @sf_router.post("/traces/sync")
    async def post_sync_traces() -> JSONResponse:
        data = await asyncio.to_thread(
            LIVE_BRIDGE.sync_langflow_traces_to_snowflake,
            limit=25,
            replicate_sqlite=True,
        )
        return JSONResponse(status_code=200, content=data)

    @sf_router.delete("/traces/{activity_id}")
    @sf_router.delete("/traces")
    async def delete_snowflake_trace_soft(
        request: Request,
        activity_id: str | None = None,
        flow_id: str | None = None,
    ) -> JSONResponse:
        ids: list[str] = []
        if activity_id:
            ids.append(activity_id)
        for qk in ("id", "ids", "activity_id", "activity_ids", "trace_id", "trace_ids"):
            qv = request.query_params.get(qk)
            if qv:
                ids.extend(x.strip() for x in qv.split(",") if x.strip())
        try:
            body = await request.json()
            if isinstance(body, dict):
                for bk in ("id", "ids", "activity_id", "activity_ids", "trace_id", "trace_ids"):
                    bv = body.get(bk)
                    if isinstance(bv, list):
                        ids.extend(str(x).strip() for x in bv if str(x).strip())
                    elif isinstance(bv, str) and bv.strip():
                        ids.append(bv.strip())
                flow_id = flow_id or body.get("flow_id")
        except Exception:  # noqa: BLE001
            pass
        res = await asyncio.to_thread(LIVE_BRIDGE.soft_delete_flow_activity, ids, flow_id=flow_id)
        return JSONResponse(status_code=200, content=res)

    @sf_router.get("/memories")
    async def get_memories(request: Request, flow_id: str | None = None) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        filtered_catalog = LIVE_BRIDGE.get_memories_for_flow(flow_id=req_fid)
        filtered_registry = [
            m
            for m in LIVE_BRIDGE.memories_registry
            if LIVE_BRIDGE.is_memory_visible_in_context(m, flow_id=req_fid)
        ]
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "active_role": LIVE_BRIDGE.active_role,
                "memories": filtered_registry,
                "memories_catalog": filtered_catalog,
                "agent_memory_types": list(AGENT_MEMORY_TYPES),
                "coming_soon_memory_types": list(COMING_SOON_MEMORY_TYPES),
                "dropdown_memory_types": list(DROPDOWN_MEMORY_TYPES),
                "legacy_memory_type_aliases": dict(LEGACY_MEMORY_TYPE_ALIASES),
                "snowflake_embedding_sources": list(SNOWFLAKE_EMBEDDING_SOURCES),
                "llm_preprocessing_engines": get_llm_preprocessing_engines(),
                "external_embedding_providers": EXTERNAL_EMBEDDING_PROVIDERS,
                "embedding_providers_catalog": list(EMBEDDING_PROVIDERS_CATALOG),
                "supported_backends": [
                    "Standard Table",
                    "Hybrid Table",
                    "Interactive Table",
                    "Dynamic Table",
                    "Semi-Structured (VARIANT)",
                    "Semi-Structured (OBJECT)",
                    "Semi-Structured (ARRAY)",
                    "Snowflake Postgres (Relational / JSONB)",
                    "Snowflake Postgres (PostgresSaver)",
                    "Snowflake Postgres (PostgresStore)",
                    "SnowflakeCheckpointer",
                    "Standard Table (VECTOR)",
                    "Hybrid Table (VECTOR)",
                    "Interactive Table (VECTOR)",
                    "Dynamic Table (VECTOR)",
                    "Snowflake Postgres (pgvector)",
                    "Cortex Search Service",
                ],
                "supported_embeddings": [
                    "snowflake-arctic-embed-m-v1.5 (768d)",
                    "snowflake-arctic-embed-m (768d)",
                    "e5-base-v2 (768d)",
                    "snowflake-arctic-embed-l-v2.0 (1024d)",
                    "snowflake-arctic-embed-l-v2.0-8k (1024d)",
                    "multilingual-e5-large (1024d)",
                    "voyage-multilingual-2 (1024d)",
                    "nv-embed-qa-4 (1024d)",
                    "text-embedding-3-small (1536d)",
                    "text-embedding-3-large (3072d)",
                    "text-embedding-ada-002 (1536d)",
                    "embed-english-v3.0 (1024d)",
                    "embed-multilingual-v3.0 (1024d)",
                    "bge-large-en-v1.5 (1024d)",
                    "custom-embedding-endpoint (1..4096d)",
                ],
            },
        )

    @sf_router.post("/memories")
    async def post_create_memory(request: Request) -> JSONResponse:
        body = await request.json()
        res = await asyncio.to_thread(LIVE_BRIDGE.create_or_register_memory, body if isinstance(body, dict) else {})
        status_code = int(res.get("status_code") or (201 if res.get("ok") else 422))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.post("/memories/recall")
    async def post_recall_memory(request: Request) -> JSONResponse:
        body = await request.json()
        payload = body if isinstance(body, dict) else {}
        mem_ref = str(
            payload.get("memory_ref")
            or payload.get("memory_name")
            or payload.get("memory_base")
            or payload.get("name")
            or payload.get("memory_id")
            or ""
        ).strip()
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.recall_memory(
                mem_ref,
                query=str(payload.get("query") or ""),
                session_id=str(payload.get("session_id") or "default"),
                thread_id=str(payload.get("thread_id") or "default"),
                user_id=str(payload.get("user_id") or "default"),
                entity_namespace=str(payload.get("entity_namespace") or "default"),
                topic=str(payload.get("topic") or ""),
                limit=int(payload.get("limit") or 10),
            )
        )
        status_code = 200 if res.get("ok") else 404
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.get("/memories/showcase/full-memory-enterprise-agent")
    async def get_full_memory_showcase() -> JSONResponse:
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.bootstrap_full_memory_showcase_flow(dry_run=True, execute_simulation=True)
        )
        return JSONResponse(status_code=200, content=res)

    @sf_router.post("/memories/showcase/full-memory-enterprise-agent")
    async def post_full_memory_showcase(request: Request) -> JSONResponse:
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        dry_run = bool(payload.get("dry_run", True))
        execute_sim = bool(payload.get("execute_simulation", True))
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.bootstrap_full_memory_showcase_flow(dry_run=dry_run, execute_simulation=execute_sim)
        )
        return JSONResponse(status_code=200, content=res)

    @sf_router.get("/memories/{memory_id_or_name}")
    async def get_memory_detail_route(memory_id_or_name: str, request: Request) -> JSONResponse:
        session_filter = request.query_params.get("session_filter") or request.query_params.get("session")
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.get_memory_detail(memory_id_or_name, session_filter=session_filter)
        )
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 404))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.patch("/memories/{memory_id_or_name}")
    async def patch_memory_detail_route(memory_id_or_name: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        res = await asyncio.to_thread(LIVE_BRIDGE.patch_native_memory, memory_id_or_name, payload)
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 404))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.post("/memories/{memory_id_or_name}/reload")
    async def post_reload_memory_detail_route(memory_id_or_name: str, request: Request) -> JSONResponse:
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        res = await asyncio.to_thread(LIVE_BRIDGE.reload_memory_detail, memory_id_or_name, payload)
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 404))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.delete("/memories/{memory_id_or_name}")
    async def delete_memory_route(memory_id_or_name: str) -> JSONResponse:
        res = await asyncio.to_thread(LIVE_BRIDGE.delete_memory, memory_id_or_name)
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 404))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.get("/stages")
    async def get_stages_route(
        request: Request,
        flow_id: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_stages_for_context(flow_id=req_fid, database=database, schema=schema)
        )
        return JSONResponse(status_code=200, content=res)

    @sf_router.post("/stages/create", status_code=201)
    @sf_router.post("/stages", status_code=201)
    async def post_create_stage_route(request: Request) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.create_stage_governed(payload, flow_id=req_fid or payload.get("flow_id"))
        )
        status_code = int(res.get("status_code") or (201 if res.get("ok") else 403))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.get("/external-volumes")
    async def get_external_volumes_route(request: Request, flow_id: str | None = None) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_external_volumes_for_context(flow_id=req_fid)
        )
        return JSONResponse(status_code=200, content=res)

    @sf_router.get("/external-volumes/describe")
    async def get_describe_external_volume_route(request: Request) -> JSONResponse:
        name = str(request.query_params.get("name") or "")
        res = await asyncio.to_thread(lambda: LIVE_BRIDGE.describe_external_volume(name))
        return JSONResponse(status_code=int(res.get("status_code") or (200 if res.get("ok") else 502)), content=res)

    @sf_router.post("/external-volumes/create", status_code=201)
    @sf_router.post("/external-volumes", status_code=201)
    async def post_create_external_volume_route(request: Request) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.create_external_volume_governed(payload, flow_id=req_fid or payload.get("flow_id"))
        )
        status_code = int(res.get("status_code") or (201 if res.get("ok") else 403))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.get("/custom-tools")
    async def get_custom_tools_route(
        request: Request,
        resource_type: str = "procedure",
        database_schema: str = "Database All",
        flow_id: str | None = None,
    ) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        req_rt = request.query_params.get("resource_type") or resource_type or "procedure"
        req_dbs = request.query_params.get("database_schema") or database_schema or "Database All"
        items = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_custom_tool_identifiers(
                resource_type=req_rt,
                database_schema=req_dbs,
                flow_id=req_fid,
            )
        )
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "resource_type": req_rt,
                "database_schema": req_dbs,
                "identifiers": items,
                "count": len(items),
                "active_role": LIVE_BRIDGE.active_role,
            },
        )

    @sf_router.get("/custom-tools/describe")
    @sf_router.post("/custom-tools/describe")
    async def describe_custom_tool_route(
        request: Request,
        resource_type: str = "procedure",
        identifier: str = "",
        database_schema: str | None = None,
        flow_id: str | None = None,
    ) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        req_rt = request.query_params.get("resource_type") or resource_type or "procedure"
        id_list = (
            request.query_params.getlist("identifier")
            or request.query_params.getlist("custom_tool_identifier")
        )
        req_id = ", ".join(id_list) if id_list else identifier
        req_dbs = request.query_params.get("database_schema") or database_schema
        if request.method.upper() == "POST":
            try:
                body = await request.json()
                if isinstance(body, dict):
                    req_rt = str(body.get("resource_type") or req_rt)
                    req_id = str(body.get("identifier") or body.get("custom_tool_identifier") or req_id)
                    req_dbs = body.get("database_schema") or req_dbs
                    req_fid = body.get("flow_id") or req_fid
            except Exception:  # noqa: BLE001
                pass
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.describe_custom_tool_signature(
                resource_type=req_rt,
                identifier=req_id,
                database_schema=req_dbs,
                flow_id=req_fid,
            )
        )
        status_code = int(res.get("status_code") or (200 if res.get("ok", True) else 422))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.get("/stages/files")
    async def get_stage_files_query_route(
        request: Request,
        stage_fqn: str | None = None,
        stage: str | None = None,
        subpath: str = "",
        pattern: str = "",
        media_filter: str | None = None,
    ) -> JSONResponse:
        target_stage = (
            stage_fqn
            or stage
            or request.query_params.get("stage_fqn")
            or request.query_params.get("stage")
            or f"@{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.{LIVE_BRIDGE.stage_name}"
        )
        req_mf = media_filter or request.query_params.get("media_filter")
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_stage_files(
                target_stage,
                subpath=subpath,
                pattern=pattern,
                media_filter=req_mf,
                flow_id=req_fid,
            )
        )
        return JSONResponse(status_code=int(res.get("status_code") or (200 if res.get("ok") else 502)), content=res)

    @sf_router.post("/stages/upload", status_code=201)
    async def post_stage_upload_route(request: Request) -> JSONResponse:
        content_type = str(request.headers.get("content-type") or "").lower()
        stage_fqn = (
            request.query_params.get("stage_fqn")
            or request.query_params.get("stage")
            or ""
        )
        file_name = request.query_params.get("file_name") or ""
        subpath = request.query_params.get("subpath") or ""
        raw_bytes: bytes | None = None
        content_text: str | None = None
        content_b64: str | None = None
        refresh_dir = True

        if "multipart/form-data" in content_type:
            form = await request.form()
            if form.get("stage_fqn") or form.get("stage"):
                stage_fqn = str(form.get("stage_fqn") or form.get("stage"))
            if form.get("subpath"):
                subpath = str(form.get("subpath"))
            upload_obj = form.get("file")
            if upload_obj is not None and hasattr(upload_obj, "read"):
                raw_bytes = await upload_obj.read()
                file_name = str(getattr(upload_obj, "filename", None) or form.get("file_name") or file_name)
            elif form.get("content"):
                content_text = str(form.get("content"))
                file_name = str(form.get("file_name") or file_name)
        else:
            try:
                body = await request.json()
                payload = body if isinstance(body, dict) else {}
            except Exception:  # noqa: BLE001
                payload = {}
            stage_fqn = str(payload.get("stage_fqn") or payload.get("stage") or stage_fqn)
            file_name = str(payload.get("file_name") or payload.get("filename") or payload.get("name") or file_name)
            subpath = str(payload.get("subpath") or subpath)
            content_text = payload.get("content") or payload.get("content_text") or payload.get("text")
            content_b64 = payload.get("content_base64") or payload.get("base64")
            refresh_dir = bool(payload.get("refresh_directory", True))

        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.upload_stage_file(
                stage_fqn=stage_fqn,
                file_name=file_name,
                content_bytes=raw_bytes,
                content_text=str(content_text) if content_text is not None else None,
                content_base64=str(content_b64) if content_b64 is not None else None,
                subpath=subpath,
                refresh_directory=refresh_dir,
            )
        )
        status_code = int(res.get("status_code") or (201 if res.get("ok") else 403))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.delete("/stages/files")
    async def delete_stage_file_query_route(request: Request) -> JSONResponse:
        stage_fqn = request.query_params.get("stage_fqn") or request.query_params.get("stage") or ""
        file_path = (
            request.query_params.get("file_path")
            or request.query_params.get("path")
            or request.query_params.get("file_name")
            or ""
        )
        try:
            body = await request.json()
            if isinstance(body, dict):
                stage_fqn = str(body.get("stage_fqn") or body.get("stage") or stage_fqn)
                file_path = str(body.get("file_path") or body.get("path") or body.get("file_name") or file_path)
        except Exception:  # noqa: BLE001
            pass
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.delete_stage_file(stage_fqn, file_path)
        )
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 403))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.post("/stages/refresh")
    async def post_stage_refresh_route(request: Request) -> JSONResponse:
        stage_fqn = request.query_params.get("stage_fqn") or request.query_params.get("stage") or ""
        try:
            body = await request.json()
            if isinstance(body, dict):
                stage_fqn = str(body.get("stage_fqn") or body.get("stage") or stage_fqn)
        except Exception:  # noqa: BLE001
            pass
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.refresh_stage_directory(stage_fqn)
        )
        return JSONResponse(status_code=int(res.get("status_code") or (200 if res.get("ok") else 502)), content=res)

    @sf_router.get("/stages/{stage_fqn:path}/files")
    async def get_stage_files_path_route(
        stage_fqn: str,
        request: Request,
        subpath: str = "",
        pattern: str = "",
        media_filter: str | None = None,
    ) -> JSONResponse:
        req_mf = media_filter or request.query_params.get("media_filter")
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_stage_files(
                stage_fqn,
                subpath=subpath,
                pattern=pattern,
                media_filter=req_mf,
                flow_id=req_fid,
            )
        )
        return JSONResponse(status_code=int(res.get("status_code") or (200 if res.get("ok") else 502)), content=res)

    @sf_router.delete("/stages/{stage_fqn:path}/files")
    async def delete_stage_files_path_route(stage_fqn: str, request: Request) -> JSONResponse:
        file_path = (
            request.query_params.get("file_path")
            or request.query_params.get("path")
            or request.query_params.get("file_name")
            or ""
        )
        try:
            body = await request.json()
            if isinstance(body, dict):
                file_path = str(body.get("file_path") or body.get("path") or body.get("file_name") or file_path)
        except Exception:  # noqa: BLE001
            pass
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.delete_stage_file(stage_fqn, file_path)
        )
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 403))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.post("/stages/{stage_fqn:path}/refresh")
    async def post_stage_refresh_path_route(stage_fqn: str) -> JSONResponse:
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.refresh_stage_directory(stage_fqn)
        )
        return JSONResponse(status_code=int(res.get("status_code") or (200 if res.get("ok") else 502)), content=res)

    @sf_router.get("/embedding-models")
    async def get_embedding_models_route() -> JSONResponse:
        from lfx_snowflake.providers.embeddings import get_snowflake_cortex_embedding_catalog

        models = await asyncio.to_thread(get_snowflake_cortex_embedding_catalog)
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "provider": "Snowflake Cortex",
                "models": models,
                "vector_store_backends": list(SNOWFLAKE_VECTOR_STORE_BACKENDS.keys()),
                "active_role": LIVE_BRIDGE.active_role,
            },
        )

    @sf_router.get("/language-models")
    async def get_language_models_route(request: Request, force_refresh: bool = False) -> JSONResponse:
        from lfx_snowflake.providers.language_models import (
            SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS,
            get_snowflake_live_language_model_catalog,
        )

        raw_force = request.query_params.get("force_refresh")
        eff_force = (
            str(raw_force).lower() in ("1", "true", "yes")
            if raw_force is not None
            else bool(force_refresh)
        )
        models = await asyncio.to_thread(
            lambda: get_snowflake_live_language_model_catalog(force_refresh=eff_force)
        )
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "providers": list(SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS),
                "models": models,
                "active_role": LIVE_BRIDGE.active_role,
            },
        )

    @sf_router.get("/cortex/threads")
    async def get_cortex_threads_route(
        request: Request,
        origin_application: str = "langflow",
        flow_id: str | None = None,
        agent_fqn: str | None = None,
    ) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        req_afqn = agent_fqn or request.query_params.get("agent_fqn")
        req_origin = request.query_params.get("origin_application") or origin_application or "langflow"
        threads = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_cortex_threads(
                origin_application=req_origin,
                flow_id=req_fid,
                agent_fqn=req_afqn,
            )
        )
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "origin_application": req_origin,
                "threads": threads,
                "active_role": LIVE_BRIDGE.active_role,
            },
        )

    @sf_router.post("/cortex/threads", status_code=201)
    async def post_create_cortex_thread_route(request: Request) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.create_cortex_thread(
                flow_id=payload.get("flow_id") or req_fid,
                session_id=payload.get("session_id"),
                agent_name=payload.get("agent_name") or payload.get("cortex_agent"),
                origin_application=str(payload.get("origin_application") or "langflow"),
            )
        )
        return JSONResponse(status_code=201, content=res)

    @sf_router.get("/cortex/threads/{thread_id}")
    async def get_describe_cortex_thread_route(
        thread_id: str,
        request: Request,
        page_size: int = 20,
        last_message_id: int | None = None,
    ) -> JSONResponse:
        raw_ps = request.query_params.get("page_size")
        eff_ps = int(raw_ps) if (raw_ps and str(raw_ps).isdigit()) else int(page_size or 20)
        raw_lmid = request.query_params.get("last_message_id")
        eff_lmid = int(raw_lmid) if (raw_lmid and str(raw_lmid).isdigit()) else last_message_id
        desc = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.describe_cortex_thread(
                thread_id,
                page_size=eff_ps,
                last_message_id=eff_lmid,
            )
        )
        last_amid = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.resolve_last_assistant_message_id(thread_id, page_size=eff_ps)
        )
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                **desc,
                "last_assistant_message_id": last_amid,
            },
        )

    @sf_router.post("/cortex-agents/{database}/{schema}/{agent_name}/run")
    async def post_invoke_cortex_agent_route(
        database: str,
        schema: str,
        agent_name: str,
        request: Request,
    ) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.invoke_cortex_agent(
                database=database,
                schema=schema,
                agent_name=agent_name,
                user_message=str(payload.get("user_message") or payload.get("prompt") or payload.get("message") or ""),
                flow_id=payload.get("flow_id") or req_fid,
                session_id=payload.get("session_id"),
                resume_thread=bool(payload.get("resume_thread", True)),
                thread_continuity_mode=str(
                    payload.get("thread_continuity_mode") or "Resume Latest Thread of this Agent"
                ),
                conversation_thread=payload.get("conversation_thread"),
                thread_id=payload.get("thread_id"),
                parent_message_id=payload.get("parent_message_id"),
                tool_choice=str(payload.get("tool_choice") or "auto"),
                role=payload.get("role"),
            )
        )
        status_code = int(res.get("status_code") or (200 if res.get("ok") else 502))
        return JSONResponse(status_code=status_code, content=res)

    @sf_router.get("/a2a/targets")
    async def get_a2a_targets_route(request: Request, flow_id: str | None = None) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        base_url = request.query_params.get("base_url") or ""
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.list_internal_and_external_a2a_targets(flow_id=req_fid, base_url=base_url)
        )
        return JSONResponse(status_code=200, content={"ok": True, **res})

    @sf_router.get("/a2a/cortex-agents/{database}/{schema}/{agent_name}/.well-known/agent-card.json")
    async def get_cortex_agent_a2a_card_route(
        database: str,
        schema: str,
        agent_name: str,
        request: Request,
    ) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        base_url = request.query_params.get("base_url") or ""
        card = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.get_cortex_agent_a2a_card(
                database,
                schema,
                agent_name,
                base_url=base_url,
                flow_id=req_fid,
            )
        )
        status_code = int(card.get("status_code") or 200) if card.get("ok") is False else 200
        return JSONResponse(status_code=status_code, content=card)

    @sf_router.post("/a2a/cortex-agents/{database}/{schema}/{agent_name}/jsonrpc")
    async def post_cortex_agent_a2a_jsonrpc_route(
        database: str,
        schema: str,
        agent_name: str,
        request: Request,
    ) -> JSONResponse:
        req_fid = request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        try:
            body = await request.json()
            rpc_body = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            rpc_body = {}
        res = await asyncio.to_thread(
            lambda: LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc(
                database,
                schema,
                agent_name,
                rpc_body,
                flow_id=req_fid,
            )
        )
        return JSONResponse(status_code=200, content=res)

    @sf_router.post("/mcp/streamable")
    async def post_mcp_streamable(request: Request) -> JSONResponse:
        """Native MCP Streamable HTTP JSON-RPC endpoint for Cortex Code CLI & Cortex Code Desktop."""
        await asyncio.to_thread(LIVE_BRIDGE.sync_superuser_credentials)
        msg = await request.json()
        method = str(msg.get("method") or "")
        msg_id = msg.get("id")

        if method.startswith("notifications/"):
            return JSONResponse(status_code=202, content={"ok": True})

        if method == "initialize":
            return JSONResponse(
                status_code=200,
                content={
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {"listChanged": False},
                            "resources": {"listChanged": False, "subscribe": False},
                            "prompts": {"listChanged": False},
                        },
                        "serverInfo": {
                            "name": "snowflake-langflow",
                            "version": "0.3.1",
                        },
                    },
                },
            )

        if method == "ping":
            return JSONResponse(status_code=200, content={"jsonrpc": "2.0", "id": msg_id, "result": {}})

        if method == "tools/list":
            tools = [
                {
                    "name": "snowflake_get_context",
                    "description": "Get active Snowflake role, context_epoch, database, schema, and replicated trace tables in SnowflakeLangflow.",
                    "inputSchema": {"type": "object", "properties": {}},
                },
                {
                    "name": "snowflake_switch_role",
                    "description": "Switch the active Snowflake role in SnowflakeLangflow and increment context_epoch.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "target_role": {"type": "string", "description": "Target Snowflake role"},
                            "flow_id": {
                                "type": "string",
                                "description": "Optional flow id; omit for a user-wide switch",
                            },
                        },
                        "required": ["target_role"],
                    },
                },
                {
                    "name": "snowflake_query_sql",
                    "description": "Execute a governed SQL statement in SnowflakeLangflow under the active Snowflake role.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"statement": {"type": "string", "description": "SQL query to execute"}},
                        "required": ["statement"],
                    },
                },
                {
                    "name": "snowflake_sync_traces",
                    "description": "Synchronize Langflow Flow Activity runs and spans into Snowflake SQL tables (LANGFLOW_FLOW_ACTIVITY, LANGFLOW_FLOW_RUNS, LANGFLOW_FLOW_SPANS, LANGFLOW_AUDIT_LOG).",
                    "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "default": 20}}},
                },
            ]
            return JSONResponse(
                status_code=200,
                content={"jsonrpc": "2.0", "id": msg_id, "result": {"tools": tools}},
            )

        if method == "tools/call":
            params = msg.get("params") or {}
            tool_name = str(params.get("name") or "")
            args = params.get("arguments") or {}

            if tool_name == "snowflake_get_context":
                await asyncio.to_thread(LIVE_BRIDGE.refresh_roles_from_snowflake)
                payload = {
                    "active_role": LIVE_BRIDGE.active_role,
                    "available_roles": LIVE_BRIDGE.available_roles,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                    "database": LIVE_BRIDGE.database,
                    "schema": LIVE_BRIDGE.schema,
                    "traces_tables": {
                        "activity": LIVE_BRIDGE.activity_table,
                        "runs": LIVE_BRIDGE.runs_table,
                        "spans": LIVE_BRIDGE.spans_table,
                        "audit": LIVE_BRIDGE.audit_table,
                    },
                }
            elif tool_name == "snowflake_switch_role":
                requested_role = str(args.get("target_role") or "").strip()
                # BUG-MCP-03: only the empty role is rejected here; grant policy lives in LIVE_BRIDGE.switch_role.
                if not requested_role:
                    payload = {
                        "ok": False,
                        "status_code": 403,
                        "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                        "message": "target_role is required.",
                    }
                    _emit_mcp_denial("snowflake_switch_role", payload, requested_role.upper() or None)
                    LIVE_BRIDGE._audit_role_switch(
                        "DENIED",
                        "",
                        LIVE_BRIDGE._normalize_flow_id(request.headers.get("X-Snowflake-Flow-Id")),
                        LIVE_BRIDGE.current_session_role() or LIVE_BRIDGE.active_role,
                        "MCP",
                        ErrorCode.AUTHZ_ROLE_DENIED.value,
                    )
                else:
                    # BUG-MCP-03: same per-(user, flow) session semantics as REST POST /role.
                    mcp_flow_id = str(args.get("flow_id") or "").strip() or request.headers.get("X-Snowflake-Flow-Id")
                    payload = await asyncio.to_thread(
                        LIVE_BRIDGE.switch_role, requested_role, mcp_flow_id, channel="MCP"
                    )
            elif tool_name == "snowflake_query_sql":
                mcp_statement = str(args.get("statement") or "SELECT CURRENT_ROLE()")
                if not _sf_identity.is_read_only_statement(mcp_statement):
                    payload = {
                        "ok": False,
                        "status_code": 403,
                        "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                        "message": "snowflake_query_sql accepts a single read-only statement only; nothing was executed.",
                    }
                    _emit_mcp_denial("snowflake_query_sql", payload, None)
                else:
                    payload = await asyncio.to_thread(LIVE_BRIDGE.execute_sql, mcp_statement)
            elif tool_name == "snowflake_sync_traces":
                # In-process (never an HTTP self-call): runs under this request's bound caller identity and
                # returns the real replication counts of this sync.
                payload = await asyncio.to_thread(
                    LIVE_BRIDGE.sync_langflow_traces_to_snowflake,
                    limit=_sql_safety.bounded_int(args.get("limit"), 20, 1, 50),
                    replicate_sqlite=True,
                )
            else:
                return JSONResponse(
                    status_code=200,
                    content={
                        "jsonrpc": "2.0",
                        "id": msg_id,
                        "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
                    },
                )

            return JSONResponse(
                status_code=200,
                content={
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(payload, indent=2)}],
                        # BUG-MCP-01: tool-level failures (e.g. AUTHZ_ROLE_DENIED) are MCP tool errors.
                        "isError": isinstance(payload, dict) and payload.get("ok") is False,
                    },
                },
            )

        if method in ("resources/list", "resources/templates/list", "prompts/list"):
            key = "prompts" if "prompts" in method else ("resourceTemplates" if "templates" in method else "resources")
            return JSONResponse(status_code=200, content={"jsonrpc": "2.0", "id": msg_id, "result": {key: []}})

        return JSONResponse(
            status_code=200,
            content={"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": f"Unsupported method {method}"}},
        )

    @sf_router.post("/service-trigger")
    async def post_snowflake_service_trigger(request: Request) -> JSONResponse:
        """SPCS Service Function batch endpoint (`SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW`)."""
        try:
            body = await request.json()
        except Exception as exc:  # noqa: BLE001
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": f"Invalid JSON body: {exc}"},
            )
        try:
            res = await asyncio.to_thread(
                LIVE_BRIDGE.invoke_service_trigger_batch,
                body if isinstance(body, dict) else {},
                asyncio.get_running_loop(),
            )
            return JSONResponse(status_code=200, content=res)
        except SnowflakeProviderError as exc:
            return JSONResponse(
                status_code=int(getattr(exc, "status_code", 403) or 403),
                content=exc.to_dict(),
            )
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={"ok": False, "error": str(exc)},
            )

    @sf_router.get("/triggers")
    async def get_snowflake_triggers(request: Request, flow_id: str | None = None) -> JSONResponse:
        req_fid = flow_id or request.query_params.get("flow_id") or request.headers.get("X-Snowflake-Flow-Id")
        items = await asyncio.to_thread(LIVE_BRIDGE.list_flow_triggers, req_fid)
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "flow_id": req_fid,
                "active_role": LIVE_BRIDGE.active_role,
                "triggers": items,
                "count": len(items),
            },
        )

    @sf_router.post("/triggers/sync")
    async def post_sync_snowflake_triggers(request: Request) -> JSONResponse:
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        req_fid = str(
            payload.get("flow_id")
            or request.query_params.get("flow_id")
            or request.headers.get("X-Snowflake-Flow-Id")
            or "default-flow"
        )
        flow_payload = payload.get("flow_payload") if isinstance(payload.get("flow_payload"), dict) else payload
        try:
            res = await asyncio.to_thread(LIVE_BRIDGE.sync_flow_triggers, req_fid, flow_payload)
            return JSONResponse(status_code=200, content=res)
        except SnowflakeProviderError as exc:
            return JSONResponse(
                status_code=int(getattr(exc, "status_code", 403) or 403),
                content={"ok": False, **exc.to_dict()},
            )
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={"ok": False, "error": str(exc)},
            )

    @sf_router.post("/triggers/status")
    async def post_snowflake_trigger_status(request: Request) -> JSONResponse:
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:  # noqa: BLE001
            payload = {}
        req_fid = str(
            payload.get("flow_id")
            or request.query_params.get("flow_id")
            or request.headers.get("X-Snowflake-Flow-Id")
            or "default-flow"
        )
        node_id = str(payload.get("node_id") or "trigger_node")
        status_mode = str(payload.get("status_mode") or payload.get("status") or "Active")
        try:
            res = await asyncio.to_thread(LIVE_BRIDGE.set_trigger_status, req_fid, node_id, status_mode)
            return JSONResponse(status_code=200, content=res)
        except SnowflakeProviderError as exc:
            return JSONResponse(
                status_code=int(getattr(exc, "status_code", 403) or 403),
                content={"ok": False, **exc.to_dict()},
            )
        except ValueError as exc:
            return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})

    @sf_router.delete("/triggers/{flow_id}")
    async def delete_snowflake_triggers_for_flow(flow_id: str) -> JSONResponse:
        try:
            res = await asyncio.to_thread(LIVE_BRIDGE.cleanup_flow_triggers, flow_id)
            return JSONResponse(status_code=200, content=res)
        except ValueError as exc:
            return JSONResponse(status_code=422, content={"ok": False, "error": str(exc)})

    register_snowflake_vector_store_backends()
    app.include_router(sf_router)
    app.include_router(traces_v2_router)
    app.include_router(memories_v1_router)


try:
    from lfx.base.knowledge_bases.backends.base import (  # type: ignore[import-not-found]
        BaseVectorStoreBackend as _UpstreamBaseVectorStoreBackend,
    )
except Exception:  # noqa: BLE001
    class _UpstreamBaseVectorStoreBackend:  # type: ignore[no-redef]
        """Standalone fallback for `BaseVectorStoreBackend` when running outside the Langflow container."""

        backend_type: str = "snowflake_table"

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass


SNOWFLAKE_VECTOR_STORE_BACKENDS: dict[str, dict[str, Any]] = {
    "snowflake_table": {
        "key": "snowflake_table",
        "label": "Snowflake Standard Table",
        "table_type": "STANDARD",
        "storage_family": "snowflake_sql",
        "supports_vector": True,
    },
    "snowflake_hybrid": {
        "key": "snowflake_hybrid",
        "label": "Snowflake Hybrid Table",
        "table_type": "HYBRID",
        "storage_family": "snowflake_sql",
        "supports_vector": True,
    },
    "snowflake_dynamic": {
        "key": "snowflake_dynamic",
        "label": "Snowflake Dynamic Table",
        "table_type": "DYNAMIC",
        "storage_family": "snowflake_sql",
        "supports_vector": True,
    },
    "snowflake_interactive": {
        "key": "snowflake_interactive",
        "label": "Snowflake Interactive Table",
        "table_type": "INTERACTIVE",
        "storage_family": "snowflake_sql",
        "supports_vector": True,
    },
    "snowflake_iceberg": {
        "key": "snowflake_iceberg",
        "label": "Snowflake Iceberg Table",
        "table_type": "ICEBERG",
        "storage_family": "snowflake_sql",
        "supports_vector": False,
    },
    "snowflake_postgres": {
        "key": "snowflake_postgres",
        "label": "Snowflake Postgres (:5432)",
        "table_type": "POSTGRES",
        "storage_family": "snowflake_postgres",
        "supports_vector": True,
    },
}


def resolve_snowflake_backend_key(entry: dict[str, Any]) -> str:
    """Map a memory entry to a stable Snowflake backend key (`snowflake_*`), never a display label."""
    cfg = entry.get("backend_config") if isinstance(entry.get("backend_config"), dict) else {}
    for cand in (entry.get("backend_type"), cfg.get("snowflake_backend_type"), cfg.get("backend_type")):
        key = str(cand or "").strip().lower()
        if key in SNOWFLAKE_VECTOR_STORE_BACKENDS:
            return key
    vec_db = str(entry.get("vector_database") or entry.get("storage_backend") or "").strip().lower()
    for token, key in (
        ("postgres", "snowflake_postgres"),
        ("hybrid", "snowflake_hybrid"),
        ("dynamic", "snowflake_dynamic"),
        ("interactive", "snowflake_interactive"),
        ("iceberg", "snowflake_iceberg"),
    ):
        if token in vec_db:
            return key
    return "snowflake_table"


SNOWFLAKE_KB_NOT_NATIVE = "SNOWFLAKE_KB_NOT_NATIVE"
_NATIVE_KB_PATH_RE = re.compile(r"^/api/v1/knowledge_bases(?:/(?P<name>[^/]+))?(?:/.*)?/?$")
_NATIVE_KB_NON_NAME_SEGMENTS = frozenset({"connectors", "preview-chunks", "test-connection"})

_LEGACY_SNOWFLAKE_SEED_MEMORY_NAMES: frozenset[str] = frozenset(
    {
        "AGENT_MEMORY_STORE",
        "MEM_1_GRAPH_CHECKPOINTER",
        "MEM_2_CHAT_WINDOW",
        "MEM_2_BUFFER_WINDOW",
        "MEM_3_SESSION_SUMMARY",
        "MEM_3_RUNNING_SUMMARY",
        "MEM_4_USER_ENTITY_PROFILE",
        "MEM_4_USER_PROFILE",
        "MEM_5_EPISODIC_FEWSHOT",
        "MEM_5_EPISODIC_FEW_SHOT",
    }
)


def _is_snowflake_backend_config(raw: Any) -> bool:
    try:
        cfg = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except Exception:  # noqa: BLE001
        return False
    return isinstance(cfg, dict) and str(cfg.get("mode") or "").lower() == "snowflake"


def _collect_snowflake_memory_kb_links(cur: Any, tables: set[str]) -> tuple[list[Any], set[str], set[str]]:
    """Return `(seed_mb_ids, linked_kb_names_upper, linked_kb_norm_ids)` from `memory_base`.

    Legacy Wave 2 seed memories (`AGENT_MEMORY_STORE`, `MEM_1..5`) created via native 1.12
    `POST /api/v1/memories` also created an underlying `knowledge_base` row with `backend_type='chroma'`
    and `backend_config=NULL` linked via `memory_base.kb_name == knowledge_base.name`. On 1.13.1
    Alembic fences those empty Chroma rows into `storage_state='needs_attention'` (`legacy_source_missing`).
    """
    if "memory_base" not in tables:
        return [], set(), set()
    mb_cols = set(sqlite_table_columns(cur, "memory_base"))
    if not {"id", "name"}.issubset(mb_cols):
        return [], set(), set()
    select_cols = [
        c
        for c in ("id", "name", "kb_name", "storage_kb_id", "preproc_model", "preproc_instructions", "embedding_model")
        if c in mb_cols
    ]
    seed_mb_ids: list[Any] = []
    linked_kb_names_upper: set[str] = set()
    linked_kb_norm_ids: set[str] = set()
    for row in cur.execute(f"SELECT {', '.join(select_cols)} FROM memory_base").fetchall():
        row_map = dict(zip(select_cols, row))
        mb_id = row_map.get("id")
        mb_name_upper = str(row_map.get("name") or "").strip().upper()
        kb_name_upper = str(row_map.get("kb_name") or "").strip().upper()
        storage_kb_norm = str(row_map.get("storage_kb_id") or "").replace("-", "").strip().lower()
        preproc_model = str(row_map.get("preproc_model") or "").strip().lower()
        preproc_instr = str(row_map.get("preproc_instructions") or "")
        emb_model = str(row_map.get("embedding_model") or "")
        is_seed = mb_name_upper in _LEGACY_SNOWFLAKE_SEED_MEMORY_NAMES
        is_sf_mem = (
            is_seed
            or preproc_model == "snowflake-sql"
            or "Store: " in preproc_instr
            or "snowflake" in emb_model.lower()
            or emb_model.startswith("No Embedding")
        )
        if is_seed and mb_id is not None:
            seed_mb_ids.append(mb_id)
        if is_sf_mem:
            if mb_name_upper:
                linked_kb_names_upper.add(mb_name_upper)
            if kb_name_upper:
                linked_kb_names_upper.add(kb_name_upper)
            if storage_kb_norm:
                linked_kb_norm_ids.add(storage_kb_norm)
    return seed_mb_ids, linked_kb_names_upper, linked_kb_norm_ids


def snowflake_kb_names_in_native_table(conn: Any) -> set[str]:
    """Upper-cased names and dashless ids of native `knowledge_base` rows owned by Snowflake or legacy seeds."""
    cur = conn.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "knowledge_base" not in tables:
        return set()
    _, linked_kb_names_upper, linked_kb_norm_ids = _collect_snowflake_memory_kb_links(cur, tables)
    kb_cols = set(sqlite_table_columns(cur, "knowledge_base"))
    select_cols = [c for c in ("id", "name", "backend_config", "backend_type", "source_types") if c in kb_cols]
    out: set[str] = set()
    for row in cur.execute(f"SELECT {', '.join(select_cols)} FROM knowledge_base").fetchall():
        rm = dict(zip(select_cols, row))
        row_id = rm.get("id")
        name_upper = str(rm.get("name") or "").strip().upper()
        norm_id = str(row_id or "").replace("-", "").strip().lower()
        btype = str(rm.get("backend_type") or "").strip().lower()
        stypes = str(rm.get("source_types") or "").strip().lower()
        if (
            _is_snowflake_backend_config(rm.get("backend_config"))
            or name_upper in _LEGACY_SNOWFLAKE_SEED_MEMORY_NAMES
            or name_upper in linked_kb_names_upper
            or norm_id in linked_kb_norm_ids
            or btype.startswith("snowflake")
            or '"snowflake"' in stypes
        ):
            if name_upper:
                out.add(name_upper)
            if norm_id:
                out.add(norm_id.upper())
    return out


def purge_snowflake_rows_from_native_knowledge_base(conn: Any) -> int:
    """Idempotently delete Snowflake/seed rows from native `knowledge_base`, `memory_base`, and `knowledge_base_storage_migration`."""
    cur = conn.cursor()
    tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "knowledge_base" not in tables:
        return 0
    seed_mb_ids, linked_kb_names_upper, linked_kb_norm_ids = _collect_snowflake_memory_kb_links(cur, tables)
    kb_cols = set(sqlite_table_columns(cur, "knowledge_base"))
    select_cols = [c for c in ("id", "name", "backend_config", "backend_type", "source_types") if c in kb_cols]
    ids: list[Any] = []
    purged_norm_ids: set[str] = set()
    for row in cur.execute(f"SELECT {', '.join(select_cols)} FROM knowledge_base").fetchall():
        rm = dict(zip(select_cols, row))
        row_id = rm.get("id")
        name_upper = str(rm.get("name") or "").strip().upper()
        norm_id = str(row_id or "").replace("-", "").strip().lower()
        btype = str(rm.get("backend_type") or "").strip().lower()
        stypes = str(rm.get("source_types") or "").strip().lower()
        if (
            _is_snowflake_backend_config(rm.get("backend_config"))
            or name_upper in _LEGACY_SNOWFLAKE_SEED_MEMORY_NAMES
            or name_upper in linked_kb_names_upper
            or norm_id in linked_kb_norm_ids
            or btype.startswith("snowflake")
            or '"snowflake"' in stypes
        ):
            ids.append(row_id)
            if norm_id:
                purged_norm_ids.add(norm_id)

    for row_id in ids:
        cur.execute("DELETE FROM knowledge_base WHERE id = ?", (row_id,))

    mig_deleted = 0
    if "knowledge_base_storage_migration" in tables:
        remaining_norm_ids = {
            str(r[0] or "").replace("-", "").strip().lower()
            for r in cur.execute("SELECT id FROM knowledge_base").fetchall()
        }
        mig_ids = [
            mig_id
            for mig_id, mig_kb_id in cur.execute("SELECT id, kb_id FROM knowledge_base_storage_migration").fetchall()
            if str(mig_kb_id or "").replace("-", "").strip().lower() in purged_norm_ids
            or str(mig_kb_id or "").replace("-", "").strip().lower() not in remaining_norm_ids
        ]
        for mig_id in mig_ids:
            cur.execute("DELETE FROM knowledge_base_storage_migration WHERE id = ?", (mig_id,))
            mig_deleted += 1

    if seed_mb_ids and "memory_base" in tables:
        if "memory_base_session" in tables:
            for mb_id in seed_mb_ids:
                cur.execute("DELETE FROM memory_base_session WHERE memory_base_id = ?", (mb_id,))
        for mb_id in seed_mb_ids:
            cur.execute("DELETE FROM memory_base WHERE id = ?", (mb_id,))

    if ids or mig_deleted or seed_mb_ids:
        conn.commit()
        logging.getLogger(__name__).info(
            "purged %d snowflake/seed rows from native knowledge_base (%d migrations, %d seed memory_base rows)",
            len(ids),
            mig_deleted,
            len(seed_mb_ids),
        )
    return len(ids)


_NATIVE_KB_STORAGE_ID_RE = re.compile(r"^/api/v1/knowledge-base-storage/(?:pending-cleanup|attention)/(?P<kb_id>[^/]+)/")


def native_kb_target_names(path: str, method: str, body: bytes | None) -> list[str] | None:
    """KB names/ids targeted by a native KB request, or None if not a native KB route."""
    if (path or "").startswith("/api/v1/knowledge-base-storage"):
        sm = _NATIVE_KB_STORAGE_ID_RE.match(path)
        return [sm.group("kb_id").replace("-", "")] if sm else []
    m = _NATIVE_KB_PATH_RE.match(path or "")
    if not m:
        return None
    name = m.group("name")
    if name and name not in _NATIVE_KB_NON_NAME_SEGMENTS:
        from urllib.parse import unquote

        return [unquote(name)]
    if not name and method.upper() == "DELETE" and body:
        try:
            payload = json.loads(body)
        except Exception:  # noqa: BLE001
            return []
        names = payload.get("kb_names") if isinstance(payload, dict) else None
        return [str(n) for n in names] if isinstance(names, list) else []
    return []


def native_kb_guard_decision(
    path: str, method: str, body: bytes | None, snowflake_ids: set[str] | None
) -> str | None:
    """Return the blocked target if the request hits a Snowflake-mode KB (fail closed if lookup failed)."""
    names = native_kb_target_names(path, method, body)
    if not names:
        return None
    hits = [n for n in names if snowflake_ids is None or n.upper() in snowflake_ids]
    return hits[0] if hits else None


def native_kb_backend_type_value(snowflake_key: str) -> str:
    """Return a `knowledge_base.backend_type` value that native Langflow can resolve.

    Native `BackendType` is a closed str-Enum (1.13.2: chroma/sqlite/mongodb/astra/postgres/opensearch),
    so a Snowflake key or label raises `ValueError` in `get_backend_class`. If a future native enum
    exposes the Snowflake key it is used as-is; otherwise the model default (`sqlite`) is written and the
    real Snowflake backend lives in `backend_config.snowflake_backend_type` (`mode == "snowflake"`).
    """
    try:
        from lfx.base.knowledge_bases.backends.base import BackendType  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001
        return "sqlite"
    values = {str(getattr(m, "value", m)) for m in BackendType}
    if snowflake_key in values:
        return snowflake_key
    return "sqlite" if "sqlite" in values else "chroma"


def sqlite_table_columns(cur: Any, table: str) -> list[str]:
    """Column names of `table` via `PRAGMA table_info` (works across 1.12.5 and 1.13.2 schemas)."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"invalid table name: {table!r}")
    return [str(r[1]) for r in cur.execute(f'PRAGMA table_info("{table}")').fetchall()]


def upsert_sqlite_row_preserving_columns(
    cur: Any,
    table: str,
    values: dict[str, Any],
    *,
    conflict_column: str = "id",
    insert_only_columns: tuple[str, ...] = (),
) -> None:
    """Insert or update a row without touching columns this bridge does not own.

    Unlike `INSERT OR REPLACE` (delete + insert), columns absent from `values` (e.g. 1.13.2
    `storage_generation`/`storage_state`/`active_migration_id`) keep their current value or server default.
    Columns missing from the live schema are skipped; `insert_only_columns` are not overwritten on conflict.
    """
    present = set(sqlite_table_columns(cur, table))
    if conflict_column not in present:
        raise ValueError(f"{table}.{conflict_column} not found")
    cols = [c for c in values if c in present]
    placeholders = ", ".join("?" for _ in cols)
    col_sql = ", ".join(f'"{c}"' for c in cols)
    upd = [c for c in cols if c != conflict_column and c not in insert_only_columns]
    upd_sql = ", ".join(f'"{c}" = excluded."{c}"' for c in upd)
    action = f"DO UPDATE SET {upd_sql}" if upd else "DO NOTHING"
    cur.execute(
        f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders}) ON CONFLICT("{conflict_column}") {action}',
        [values[c] for c in cols],
    )


OFFICIAL_SNOWFLAKE_CORTEX_AI_EMBED_MODELS: dict[str, int] = dict(_sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS)


class SnowflakeVectorStoreBackend(_UpstreamBaseVectorStoreBackend):
    """Snowflake-native `BaseVectorStoreBackend` implementation (`W2-R5-02`).

    Supports all 6 Snowflake storage backends (`snowflake_table`, `snowflake_hybrid`,
    `snowflake_dynamic`, `snowflake_interactive`, `snowflake_iceberg`, `snowflake_postgres`)
    and the 6 official `SNOWFLAKE.CORTEX.AI_EMBED` models (`768d` & `1024d`) as well as
    `No Embedding` direct SQL mode. Computes `VECTOR_COSINE_SIMILARITY` strictly in the
    `SELECT` projection (never in `WHERE`), enabling relational predicate pruning first.
    """

    backend_type: str = "snowflake_table"
    SUPPORTED_BACKENDS = SNOWFLAKE_VECTOR_STORE_BACKENDS

    def __init__(
        self,
        kb_path: Any = None,
        *,
        backend_type: str = "snowflake_table",
        database: str | None = None,
        schema: str | None = None,
        table_name: str | None = None,
        table_fqn: str | None = None,
        embedding_model: str | None = "snowflake-arctic-embed-m-v1.5",
        embedding_provider: str | None = "Snowflake Cortex",
        enable_embedding: bool | None = None,
        dimension: int | None = None,
        session_id: str | None = None,
        user_id: str | None = None,
        postgres_instance: str | None = None,
        bridge: Any = None,
        **kwargs: Any,
    ) -> None:
        try:
            super().__init__(kb_path, **kwargs)
        except Exception:  # noqa: BLE001
            pass
        self.bridge = bridge or LIVE_BRIDGE
        self.kb_path = kb_path
        norm_bt = str(backend_type or getattr(self.__class__, "backend_type", "snowflake_table")).strip().lower()
        alias_map = {
            "standard": "snowflake_table",
            "standard table": "snowflake_table",
            "snowflake standard table": "snowflake_table",
            "hybrid": "snowflake_hybrid",
            "hybrid table": "snowflake_hybrid",
            "snowflake hybrid table": "snowflake_hybrid",
            "dynamic": "snowflake_dynamic",
            "dynamic table": "snowflake_dynamic",
            "snowflake dynamic table": "snowflake_dynamic",
            "interactive": "snowflake_interactive",
            "interactive table": "snowflake_interactive",
            "snowflake interactive table": "snowflake_interactive",
            "iceberg": "snowflake_iceberg",
            "iceberg table": "snowflake_iceberg",
            "snowflake iceberg table": "snowflake_iceberg",
            "postgres": "snowflake_postgres",
            "snowflake postgres": "snowflake_postgres",
            "snowflake postgres (:5432)": "snowflake_postgres",
        }
        self.backend_type = alias_map.get(norm_bt, norm_bt if norm_bt in SNOWFLAKE_VECTOR_STORE_BACKENDS else "snowflake_table")
        spec = SNOWFLAKE_VECTOR_STORE_BACKENDS[self.backend_type]
        self.storage_family = spec["storage_family"]
        self.table_type = spec["table_type"]

        self.database = str(database or getattr(self.bridge, "database", "SNOWFLAKE_LANGFLOW")).strip().upper()
        self.schema = str(schema or getattr(self.bridge, "schema", "CORE")).strip().upper()
        raw_tbl = str(table_name or (Path(str(kb_path)).name if kb_path else "MEM_VECTOR_STORE")).strip()
        safe_tbl = "".join(c if c.isalnum() or c == "_" else "_" for c in raw_tbl).upper() or "MEM_VECTOR_STORE"
        self.table_name = safe_tbl
        self.postgres_instance = str(postgres_instance or "LANGFLOW_PG_INSTANCE").strip()

        if table_fqn:
            self.table_fqn = str(table_fqn).strip()
        elif self.backend_type == "snowflake_postgres":
            self.table_fqn = f"{self.postgres_instance}:5432/postgres.public.{safe_tbl.lower()}"
        else:
            self.table_fqn = f"{self.database}.{self.schema}.{safe_tbl}"
        if self.backend_type != "snowflake_postgres":
            # The FQN is interpolated into DDL/DML: validate every part (BUG-UI-SQLI-05).
            self.table_fqn = _sql_safety.validate_fqn(self.table_fqn, "table_fqn", min_parts=3, max_parts=3)

        raw_model = str(embedding_model or "").split(" (")[0].strip()
        raw_prov = str(embedding_provider or "Snowflake Cortex").strip()
        no_emb_set = {"none", "no embedding", "no_embedding", "disabled", "false", "sql_only"}
        if enable_embedding is False or raw_model.lower() in no_emb_set or raw_prov.lower() in no_emb_set:
            self.enable_embedding = False
        elif not spec["supports_vector"]:
            self.enable_embedding = False
        else:
            self.enable_embedding = True if enable_embedding is None else bool(enable_embedding)

        self.embedding_model = (
            raw_model if (self.enable_embedding and raw_model) else ("snowflake-arctic-embed-m-v1.5" if self.enable_embedding else "none")
        )
        self.embedding_provider = raw_prov if self.enable_embedding else "none"
        self.dimension = int(dimension or OFFICIAL_SNOWFLAKE_CORTEX_AI_EMBED_MODELS.get(self.embedding_model, 768))
        self.session_id = str(session_id or "default")
        self.user_id = str(user_id or "default")
        self._records: list[dict[str, Any]] = []

    def build_ddl(self) -> str:
        """Build governed Snowflake DDL for the configured backend and embedding mode."""
        vec_col = f", EMBEDDING VECTOR(FLOAT, {self.dimension})" if self.enable_embedding else ""
        if self.backend_type == "snowflake_hybrid":
            return (
                f"CREATE HYBRID TABLE IF NOT EXISTS {self.table_fqn} ("
                f"CHUNK_ID VARCHAR(128) PRIMARY KEY, SESSION_ID VARCHAR(128), USER_ID VARCHAR(128), "
                f"TEXT_CHUNK VARCHAR, METADATA VARIANT{vec_col}, "
                f"CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"INDEX IDX_SESSION (SESSION_ID));"
            )
        if self.backend_type == "snowflake_dynamic":
            wh = getattr(self.bridge, "warehouse", "COMPUTE_WH")
            return (
                f"CREATE DYNAMIC TABLE IF NOT EXISTS {self.table_fqn} "
                f"TARGET_LAG = '1 minute' WAREHOUSE = {wh} AS "
                f"SELECT CHUNK_ID, SESSION_ID, USER_ID, TEXT_CHUNK, METADATA"
                f"{', EMBEDDING' if self.enable_embedding else ''}, CREATED_AT "
                f"FROM {self.database}.{self.schema}.{self.table_name}_SRC;"
            )
        if self.backend_type == "snowflake_interactive":
            wh = getattr(self.bridge, "warehouse", "COMPUTE_WH")
            return (
                f"CREATE INTERACTIVE TABLE IF NOT EXISTS {self.table_fqn} "
                f"CLUSTER BY (SESSION_ID) TARGET_LAG = '1 minute' WAREHOUSE = {wh} AS "
                f"SELECT CHUNK_ID, SESSION_ID, USER_ID, TEXT_CHUNK, METADATA"
                f"{', EMBEDDING' if self.enable_embedding else ''}, CREATED_AT "
                f"FROM {self.database}.{self.schema}.{self.table_name}_SRC;"
            )
        if self.backend_type == "snowflake_iceberg":
            return (
                f"CREATE ICEBERG TABLE IF NOT EXISTS {self.table_fqn} ("
                f"CHUNK_ID STRING, SESSION_ID STRING, USER_ID STRING, "
                f"TEXT_CHUNK STRING, CREATED_AT TIMESTAMP_TZ) "
                f"EXTERNAL_VOLUME = 'ICEBERG_EXT_VOL' CATALOG = 'SNOWFLAKE' "
                f"BASE_LOCATION = '{self.table_name.lower()}';"
            )
        if self.backend_type == "snowflake_postgres":
            pg_vec = f", embedding vector({self.dimension})" if self.enable_embedding else ""
            return (
                f"-- Instance: {self.postgres_instance} (:5432)\n"
                f"CREATE TABLE IF NOT EXISTS public.{self.table_name.lower()} ("
                f"chunk_id TEXT PRIMARY KEY, session_id TEXT, user_id TEXT, "
                f"text_chunk TEXT, metadata JSONB{pg_vec}, created_at TIMESTAMPTZ DEFAULT NOW());"
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_fqn} ("
            f"CHUNK_ID VARCHAR(128), SESSION_ID VARCHAR(128), USER_ID VARCHAR(128), "
            f"TEXT_CHUNK VARCHAR, METADATA VARIANT{vec_col}, "
            f"CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
        )

    def build_insert_sql(
        self,
        text: str,
        *,
        session_id: str | None = None,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        chunk_id: str | None = None,
    ) -> str:
        cid = str(chunk_id or f"chk-{uuid.uuid4().hex[:10]}").replace("\\", "\\\\").replace("'", "''")
        sess = str(session_id or self.session_id or "default").replace("\\", "\\\\").replace("'", "''")
        usr = str(user_id or self.user_id or "default").replace("\\", "\\\\").replace("'", "''")
        safe_txt = str(text or "").replace("\\", "\\\\").replace("'", "''")
        meta_json = json.dumps(metadata or {}).replace("\\", "\\\\").replace("'", "''")
        if self.enable_embedding:
            return (
                f"INSERT INTO {self.table_fqn} "
                f"(CHUNK_ID, SESSION_ID, USER_ID, TEXT_CHUNK, METADATA, EMBEDDING, CREATED_AT) "
                f"SELECT '{cid}', '{sess}', '{usr}', '{safe_txt}', PARSE_JSON('{meta_json}'), "
                f"AI_EMBED({_sql_safety.sql_string_literal(_sql_safety.validate_embed_model(self.embedding_model))}, '{safe_txt}'), CURRENT_TIMESTAMP();"
            )
        return (
            f"INSERT INTO {self.table_fqn} "
            f"(CHUNK_ID, SESSION_ID, USER_ID, TEXT_CHUNK, METADATA, CREATED_AT) "
            f"SELECT '{cid}', '{sess}', '{usr}', '{safe_txt}', PARSE_JSON('{meta_json}'), CURRENT_TIMESTAMP();"
        )

    def build_search_sql(
        self,
        query: str = "",
        *,
        top_k: int = 5,
        session_id: str | None = None,
        user_id: str | None = None,
        where_clause: str | None = None,
    ) -> str:
        """Build governed recall SQL with `VECTOR_COSINE_SIMILARITY` strictly in `SELECT` (never in `WHERE`)."""
        bounded_k = max(1, min(int(top_k or 5), 200))
        safe_q = str(query or "").replace("\\", "\\\\").replace("'", "''")
        predicates: list[str] = []
        if session_id:
            predicates.append(f"SESSION_ID = {_sql_safety.sql_string_literal(str(session_id))}")
        if user_id:
            predicates.append(f"USER_ID = {_sql_safety.sql_string_literal(str(user_id))}")
        if where_clause:
            clean_where = str(where_clause).strip()
            if clean_where.upper().startswith("WHERE "):
                clean_where = clean_where[6:].strip()
            if clean_where:
                # Structured predicates only (COLUMN <op> literal [AND ...] or JSON); free SQL raises.
                predicates.append(f"({_sql_safety.inline_bound_predicate(clean_where)})")

        if not self.enable_embedding and safe_q:
            predicates.append(f"TEXT_CHUNK ILIKE '%{safe_q}%'")

        where_sql = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        if self.enable_embedding:
            return (
                f"SELECT CHUNK_ID, SESSION_ID, USER_ID, TEXT_CHUNK, METADATA, "
                f"VECTOR_COSINE_SIMILARITY(EMBEDDING, AI_EMBED({_sql_safety.sql_string_literal(_sql_safety.validate_embed_model(self.embedding_model))}, '{safe_q}')) AS SCORE "
                f"FROM {self.table_fqn}{where_sql} ORDER BY SCORE DESC LIMIT {bounded_k};"
            )
        return (
            f"SELECT CHUNK_ID, SESSION_ID, USER_ID, TEXT_CHUNK, METADATA, 1.0 AS SCORE "
            f"FROM {self.table_fqn}{where_sql} ORDER BY CREATED_AT DESC LIMIT {bounded_k};"
        )

    def add_texts(
        self,
        texts: list[str],
        metadatas: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        sql_statements: list[str] = []
        sess = str(kwargs.get("session_id") or self.session_id)
        usr = str(kwargs.get("user_id") or self.user_id)
        for idx, txt in enumerate(texts or []):
            meta = (metadatas[idx] if metadatas and idx < len(metadatas) else {}) or {}
            cid = str(meta.get("chunk_id") or f"chk-{len(self._records) + 1}")
            stmt = self.build_insert_sql(txt, session_id=sess, user_id=usr, metadata=meta, chunk_id=cid)
            sql_statements.append(stmt)
            self.bridge.execute_sql(stmt)
            self._records.append(
                {
                    "chunk_id": cid,
                    "session_id": sess,
                    "user_id": usr,
                    "text_chunk": str(txt),
                    "metadata": meta,
                    "score": 1.0,
                }
            )
        return {
            "ok": True,
            "ingested_count": len(sql_statements),
            "table_fqn": self.table_fqn,
            "backend_type": self.backend_type,
            "enable_embedding": self.enable_embedding,
            "embedding_model": self.embedding_model if self.enable_embedding else None,
            "sql_statements": sql_statements,
        }

    def add_documents(self, documents: list[Any], **kwargs: Any) -> dict[str, Any]:
        texts: list[str] = []
        metas: list[dict[str, Any]] = []
        for doc in documents or []:
            if isinstance(doc, str):
                texts.append(doc)
                metas.append({})
            elif isinstance(doc, dict):
                texts.append(str(doc.get("page_content") or doc.get("text") or doc.get("content") or ""))
                metas.append(dict(doc.get("metadata") or {}))
            else:
                texts.append(str(getattr(doc, "page_content", None) or getattr(doc, "text", None) or str(doc)))
                metas.append(dict(getattr(doc, "metadata", None) or {}))
        return self.add_texts(texts, metadatas=metas, **kwargs)

    def ingest(self, documents: list[Any], **kwargs: Any) -> dict[str, Any]:
        return self.add_documents(documents, **kwargs)

    def search(self, query: str = "", top_k: int = 5, **kwargs: Any) -> dict[str, Any]:
        sql = self.build_search_sql(
            query,
            top_k=top_k,
            session_id=kwargs.get("session_id"),
            user_id=kwargs.get("user_id"),
            where_clause=kwargs.get("where_clause"),
        )
        res = self.bridge.execute_sql(sql)
        rows = list(res.get("rows") or []) if res.get("mode") != "offline" else []
        if not rows and self._records:
            q_lower = str(query or "").lower()
            q_tokens = [tok for tok in re.split(r"\W+", q_lower) if len(tok) >= 2]
            sess_filter = kwargs.get("session_id")
            for rec in self._records:
                if sess_filter and str(rec.get("session_id") or "") != str(sess_filter):
                    continue
                txt_lower = str(rec.get("text_chunk") or "").lower()
                if (
                    not q_lower
                    or q_lower in txt_lower
                    or (q_tokens and any(tok in txt_lower for tok in q_tokens))
                    or self.enable_embedding
                ):
                    rows.append(dict(rec))
            rows = rows[: max(1, int(top_k or 5))]
        return {
            "ok": True,
            "query": query,
            "sql": sql,
            "rows": rows,
            "table_fqn": self.table_fqn,
            "backend_type": self.backend_type,
            "enable_embedding": self.enable_embedding,
            "embedding_model": self.embedding_model if self.enable_embedding else None,
        }

    def similarity_search(self, query: str = "", k: int = 5, **kwargs: Any) -> list[dict[str, Any]]:
        return self.search(query, top_k=k, **kwargs).get("rows", [])

    def retrieve(self, query: str = "", top_k: int = 5, **kwargs: Any) -> list[dict[str, Any]]:
        return self.similarity_search(query, k=top_k, **kwargs)


def register_snowflake_vector_store_backends() -> dict[str, Any]:
    """Register all 6 Snowflake backends (`snowflake_table`, `snowflake_hybrid`, `snowflake_dynamic`, `snowflake_interactive`, `snowflake_iceberg`, `snowflake_postgres`) into Langflow's KB & Memory backend registries (`W2-R5-02`)."""
    registered_in_lfx = False
    registered_in_langflow = False
    backend_classes: dict[str, type[SnowflakeVectorStoreBackend]] = {}
    for b_key, b_spec in SNOWFLAKE_VECTOR_STORE_BACKENDS.items():
        cls_name = f"SnowflakeVectorStoreBackend_{b_spec['table_type']}"
        sub_cls = type(
            cls_name,
            (SnowflakeVectorStoreBackend,),
            {"backend_type": b_key, "display_name": b_spec["label"]},
        )
        backend_classes[b_key] = sub_cls

    try:
        import lfx.base.knowledge_bases.backends.registry as lfx_kb_registry  # type: ignore[import-not-found]

        for attr_name in ("_BACKEND_REGISTRY", "BACKEND_REGISTRY", "_REGISTRY"):
            reg_dict = getattr(lfx_kb_registry, attr_name, None)
            if isinstance(reg_dict, dict):
                for b_key, sub_cls in backend_classes.items():
                    reg_dict[b_key] = sub_cls
                registered_in_lfx = True
        reg_fn = getattr(lfx_kb_registry, "register_backend", None)
        if callable(reg_fn):
            for b_key, sub_cls in backend_classes.items():
                try:
                    reg_fn(b_key, sub_cls)
                    registered_in_lfx = True
                except Exception:  # noqa: BLE001
                    pass
    except Exception:  # noqa: BLE001
        pass

    try:
        import langflow.schema.knowledge_base as lf_kb_schema  # type: ignore[import-not-found]

        allowed = getattr(lf_kb_schema, "_CREATION_ALLOWED_BACKENDS", None)
        if isinstance(allowed, set):
            allowed.update(SNOWFLAKE_VECTOR_STORE_BACKENDS.keys())
            registered_in_langflow = True
        elif isinstance(allowed, (list, tuple, frozenset)):
            merged = set(allowed) | set(SNOWFLAKE_VECTOR_STORE_BACKENDS.keys())
            setattr(
                lf_kb_schema,
                "_CREATION_ALLOWED_BACKENDS",
                frozenset(merged) if isinstance(allowed, frozenset) else merged,
            )
            registered_in_langflow = True
    except Exception:  # noqa: BLE001
        pass

    return {
        "ok": True,
        "registered_backends": list(SNOWFLAKE_VECTOR_STORE_BACKENDS.keys()),
        "backend_labels": {k: v["label"] for k, v in SNOWFLAKE_VECTOR_STORE_BACKENDS.items()},
        "backend_class": "SnowflakeVectorStoreBackend",
        "registered_in_lfx": registered_in_lfx,
        "registered_in_langflow": registered_in_langflow,
    }

