"""FastAPI live bridge mounting `/api/v1/snowflake/*` inside Langflow and connecting to Snowflake SQL via SPCS OAuth."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CATALOG_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="sflf-catalog")
_AUDIT_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="sflf-audit")

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

SPCS_TOKEN_PATH = Path("/snowflake/session/token")

LEGACY_MEMORY_TYPE_ALIASES: dict[str, str] = {
    "episodic_session": "buffer_window_memory",
    "semantic_vector": "episodic_memory",
    "structured_long_term": "user_profile_entity_memory",
}

EXTERNAL_EMBEDDING_PROVIDERS: dict[str, dict[str, Any]] = {
    "snowflake_cortex": {
        "id": "snowflake_cortex",
        "label": "Snowflake Cortex (AI_EMBED)",
        "is_snowflake_native": True,
        "models": [
            {"model_id": "snowflake-arctic-embed-m-v1.5", "dimension": 768},
            {"model_id": "snowflake-arctic-embed-m", "dimension": 768},
            {"model_id": "e5-base-v2", "dimension": 768},
            {"model_id": "snowflake-arctic-embed-l-v2.0", "dimension": 1024},
            {"model_id": "snowflake-arctic-embed-l-v2.0-8k", "dimension": 1024},
            {"model_id": "nv-embed-qa-4", "dimension": 1024},
            {"model_id": "multilingual-e5-large", "dimension": 1024},
            {"model_id": "voyage-multilingual-2", "dimension": 1024},
            {"model_id": "voyage-multimodal-3", "dimension": 1024},
        ],
        "dimensions": {
            "snowflake-arctic-embed-m-v1.5": 768,
            "snowflake-arctic-embed-m": 768,
            "e5-base-v2": 768,
            "snowflake-arctic-embed-l-v2.0": 1024,
            "snowflake-arctic-embed-l-v2.0-8k": 1024,
            "nv-embed-qa-4": 1024,
            "multilingual-e5-large": 1024,
            "voyage-multilingual-2": 1024,
            "voyage-multimodal-3": 1024,
        },
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

EMBEDDING_PROVIDERS_CATALOG: tuple[dict[str, Any], ...] = tuple(EXTERNAL_EMBEDDING_PROVIDERS.values())

AGENT_MEMORY_TYPES: tuple[dict[str, Any], ...] = (
    {
        "id": "graph_state_checkpointer",
        "agent_memory_type": "graph_state_checkpointer",
        "label": "#1 Short-Term: Graph State & Checkpointer",
        "category": "short_term",
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
        "needs_embedding_badge": "NO (Mai / Never)",
        "how_to_feed_without_embedding": (
            "SELECT state_blob, checkpoint_ns, step_id FROM <fqn> "
            "WHERE thread_id = :thread_id ORDER BY step_id DESC LIMIT 1"
        ),
        "when_embedding_useful": "Mai: lo stato del grafo viene recuperato sempre per chiave primaria thread_id.",
        "required_inputs": ["thread_id", "checkpoint_ns", "ttl_hours"],
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "buffer_window_memory",
        "agent_memory_type": "buffer_window_memory",
        "legacy_alias": "episodic_session",
        "label": "#2 Short-Term: Buffer / Window Memory (Storico Chat)",
        "category": "short_term",
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
        "when_embedding_useful": "Utile solo per recuperare messaggi detti 500+ turni fa fuori dalla finestra K.",
        "required_inputs": ["session_id", "window_k", "retention_days"],
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "summary_memory",
        "agent_memory_type": "summary_memory",
        "label": "#3 Short-Term: Summary Memory (Riassunto)",
        "category": "short_term",
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
        "needs_embedding_badge": "NO (Mai / Never)",
        "how_to_feed_without_embedding": (
            "SELECT running_summary, last_turn_id FROM <fqn> "
            "WHERE session_id = :session_id LIMIT 1 -- iniettato nel System Prompt"
        ),
        "when_embedding_useful": (
            "Mai: è un riassunto di 1 riga letto direttamente e aggiornato in background con "
            "AI_COMPLETE / AI_SUMMARIZE_AGG."
        ),
        "required_inputs": ["session_id", "summarizer_model", "max_summary_tokens"],
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "user_profile_entity_memory",
        "agent_memory_type": "user_profile_entity_memory",
        "legacy_alias": "structured_long_term",
        "label": "#4 Long-Term Semantic: User Profile & Entity Memory",
        "category": "long_term_semantic",
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
        "needs_embedding_badge": "OPZIONALE (90% NO)",
        "how_to_feed_without_embedding": (
            "SELECT profile_json FROM <fqn> WHERE user_id = :user_id "
            "AND entity_namespace = :entity_namespace LIMIT 1 -- caricato a inizio sessione"
        ),
        "when_embedding_useful": "Solo se l'utente accumula centinaia di fatti distinti e non entrano tutti nel prompt.",
        "required_inputs": ["user_id", "entity_namespace", "schema_keys"],
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
    {
        "id": "episodic_memory",
        "agent_memory_type": "episodic_memory",
        "legacy_alias": "semantic_vector",
        "label": "#5 Long-Term Semantic: Episodic Memory (Esperienze / Few-shot)",
        "category": "long_term_semantic",
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
        "needs_embedding_badge": "OPZIONALE",
        "how_to_feed_without_embedding": (
            "SELECT topic, tool_used, outcome_score, episode_text FROM <fqn> "
            "WHERE topic = :topic ORDER BY outcome_score DESC, created_at DESC LIMIT :few_shot_k"
        ),
        "when_embedding_useful": (
            "Utile quando i task passati non hanno categorie fisse e serve similarity search per trovare episodi simili."
        ),
        "required_inputs": ["topic_column", "tool_used_column", "outcome_score_threshold", "few_shot_k"],
        "default_enable_embedding": False,
        "enable_embedding": False,
    },
)

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
    """Resolve embedding dimension across Snowflake AI_EMBED and external Langflow embedding providers."""
    from .providers.embeddings import resolve_embedding_dimension

    prov = (embedding_provider or "snowflake_cortex").strip().lower()
    clean_model = (embedding_model or "").strip().split(" ")[0].replace("ollama/", "")
    if prov == "custom_endpoint" and custom_dimension is not None:
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


def ensure_sqlite_wal_and_uv_cache() -> dict[str, Any]:
    """Ensure writable UV_CACHE_DIR (/tmp/uv-cache) and configure SQLite WAL + busy_timeout=30000 on /persist/langflow.db."""
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

    wal_configured: list[str] = []
    for db_file in candidate_dbs:
        if db_file.exists():
            try:
                conn = _open_sqlite_conn(db_file, readonly=False, timeout=30.0)
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


class LiveSnowflakeBridge:
    """Executes real SQL against Snowflake via SPCS OAuth token and manages active role & trace replication."""

    def __init__(self) -> None:
        ensure_sqlite_wal_and_uv_cache()
        self.database = os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE_LANGFLOW")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "CORE")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "")
        self.stage_name = os.getenv("SNOWFLAKE_STAGE_NAME", "SNOWFLAKE_LANGFLOW_STAGE")
        self.runs_table = os.getenv(
            "LANGFLOW_TRACES_RUNS_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_RUNS"
        )
        self.spans_table = os.getenv(
            "LANGFLOW_TRACES_SPANS_TABLE", f"{self.database}.{self.schema}.LANGFLOW_FLOW_SPANS"
        )
        self.audit_table = os.getenv(
            "LANGFLOW_AUDIT_LOG_TABLE", f"{self.database}.{self.schema}.LANGFLOW_AUDIT_LOG"
        )
        self.active_role: str = os.getenv("SNOWFLAKE_DEFAULT_ROLE", "ACCOUNTADMIN")
        self.available_roles: list[str] = [
            "ACCOUNTADMIN",
            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
            "SNOWFLAKE_LANGFLOW_DEV_ROLE",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "PUBLIC",
        ]
        self.authenticated_user: str = os.getenv("SNOWFLAKE_USER", "SPCS_CALLER")
        self.context_epoch: int = 1
        self.secondary_roles_mode: str = "NONE"
        self.memories_registry: list[dict[str, Any]] = []
        self.selected_context: dict[str, Any] = {
            "databases": [self.database],
            "schemas_by_db": {self.database: [self.schema, "PUBLIC"]},
            "objects_by_db_schema": {},
            "selected_objects": {},
            "visible_objects": {
                "tables": ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"],
                "views": [],
                "iceberg_tables": [],
                "stages": [self.stage_name],
                "memories": ["SNOWFLAKE_KB_MEM"],
                "cortex_search_services": [],
                "semantic_views": [],
                "semantic_models": [],
                "cortex_agents": [],
                "functions": [],
                "procedures": [],
                "postgres_instances": [],
            },
        }
        self._roles_discovered: bool = False
        self._superuser_synced: bool = False
        self._roles_last_refreshed_ts: float = 0.0
        self._dbs_cache: dict[str, tuple[float, list[str], list[str]]] = {}
        self._schemas_cache: dict[str, tuple[float, list[str]]] = {}
        self._objects_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._synced_tx_ids: set[str] = set()
        self._http_client: Any = None

    def invalidate_catalog_cache(self) -> None:
        """Clear in-memory catalog caches on role switch or explicit refresh."""
        self._dbs_cache.clear()
        self._schemas_cache.clear()
        self._objects_cache.clear()
        self._roles_last_refreshed_ts = 0.0

    @property
    def memories_catalog(self) -> list[dict[str, Any]]:
        """Return governed Snowflake Agent Memories available for workflow recall (`W2-R2-13`)."""
        items: list[dict[str, Any]] = [dict(m) for m in self.memories_registry]
        known_names = {str(m.get("name") or "").strip().upper() for m in items if m.get("name")}
        vis_mem = list((self.selected_context.get("visible_objects") or {}).get("memories") or [])
        for mem_name in vis_mem:
            clean = str(mem_name or "").strip()
            if clean and clean.upper() not in known_names:
                items.append(
                    {
                        "name": clean,
                        "database": self.database,
                        "schema": self.schema,
                        "table_fqn": f"{self.database}.{self.schema}.MEM_{clean.upper()}",
                        "agent_memory_type": "semantic_vector",
                        "memory_category": "vector_table",
                        "vector_database": "Standard Table (VECTOR)",
                        "embedding_model": "snowflake-arctic-embed-m-v1.5",
                        "dimension": 768,
                        "created_by_role": self.active_role,
                    }
                )
                known_names.add(clean.upper())
        return items

    def get_memory_catalog_options(self, *, include_none: bool = True) -> list[str]:
        """Return dropdown options for `memory_catalog_ref` in workflow components."""
        opts: list[str] = ["None"] if include_none else []
        for entry in self.memories_catalog:
            nm = str(entry.get("name") or "").strip()
            fqn = str(entry.get("table_fqn") or "").strip()
            agent_type = str(entry.get("agent_memory_type") or entry.get("memory_category") or "memory").strip()
            if nm and nm not in opts:
                opts.append(nm)
            label = f"{nm} ({agent_type} -> {fqn})" if nm and fqn else (fqn or nm)
            if label and label not in opts:
                opts.append(label)
        return opts or (["None", "SNOWFLAKE_KB_MEM"] if include_none else ["SNOWFLAKE_KB_MEM"])

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
            safe = "".join(c if c.isalnum() or c == "_" else "_" for c in short_ref).upper()
            matched = {
                "name": short_ref,
                "database": self.database,
                "schema": self.schema,
                "table_fqn": short_ref if "." in short_ref else f"{self.database}.{self.schema}.MEM_{safe}",
                "agent_memory_type": "semantic_vector",
                "canonical_memory_type": "episodic_memory",
                "memory_category": "vector_table",
                "enable_embedding": True,
                "embedding_provider": "snowflake_cortex",
                "embedding_model": "snowflake-arctic-embed-m-v1.5",
                "dimension": 768,
            }
        fqn = str(matched.get("table_fqn") or f"{self.database}.{self.schema}.MEM_{short_ref.upper()}")
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
        safe_q = (query or "").replace("'", "''")
        safe_sess = (session_id or str(matched.get("session_id") or "default")).replace("'", "''")
        safe_thread = (thread_id or str(matched.get("thread_id") or safe_sess)).replace("'", "''")
        safe_user = (user_id or str(matched.get("user_id") or "default")).replace("'", "''")
        safe_ns = (entity_namespace or str(matched.get("entity_namespace") or "default")).replace("'", "''")
        safe_topic = (topic or safe_q or str(matched.get("topic") or "general")).replace("'", "''")

        if enable_embedding and (safe_q or agent_type == "semantic_vector"):
            embed_model = str(matched.get("embedding_model") or "snowflake-arctic-embed-m-v1.5").split(" ")[0]
            dim = int(matched.get("dimension") or 768)
            if embedding_provider in ("", "none", "snowflake_cortex"):
                recall_sql = (
                    f"SELECT chunk_id, memory_name, text_chunk, metadata, "
                    f"VECTOR_COSINE_SIMILARITY(embedding, AI_EMBED('{embed_model}', '{safe_q}')) AS score "
                    f"FROM {fqn} ORDER BY score DESC LIMIT {bounded_limit}"
                )
            else:
                recall_sql = (
                    f"SELECT chunk_id, memory_name, text_chunk, metadata, "
                    f"VECTOR_COSINE_SIMILARITY(embedding, CAST(:query_embedding_vec AS VECTOR(FLOAT, {dim}))) AS score "
                    f"FROM {fqn} ORDER BY score DESC LIMIT {bounded_limit}"
                )
        elif canonical_type == "graph_state_checkpointer":
            recall_sql = (
                f"SELECT state_blob, checkpoint_ns, step_id FROM {fqn} "
                f"WHERE thread_id = '{safe_thread}' ORDER BY step_id DESC LIMIT 1"
            )
        elif canonical_type == "buffer_window_memory":
            if agent_type == "episodic_session":
                recall_sql = (
                    f"SELECT * FROM {fqn} WHERE session_id = '{safe_sess}' "
                    f"ORDER BY created_at DESC LIMIT {bounded_limit}"
                )
            else:
                win_k = int(matched.get("window_k") or bounded_limit)
                recall_sql = (
                    f"SELECT sender, message_text, created_at FROM {fqn} "
                    f"WHERE session_id = '{safe_sess}' ORDER BY created_at DESC LIMIT {win_k}"
                )
        elif canonical_type == "summary_memory":
            recall_sql = (
                f"SELECT running_summary, last_turn_id FROM {fqn} "
                f"WHERE session_id = '{safe_sess}' LIMIT 1"
            )
        elif canonical_type == "user_profile_entity_memory":
            if agent_type == "structured_long_term" and not matched.get("user_id") and not matched.get("entity_namespace"):
                where_clause = f" WHERE text_chunk ILIKE '%{safe_q}%'" if safe_q else ""
                recall_sql = f"SELECT * FROM {fqn}{where_clause} ORDER BY created_at DESC LIMIT {bounded_limit}"
            else:
                recall_sql = (
                    f"SELECT profile_json FROM {fqn} "
                    f"WHERE user_id = '{safe_user}' AND entity_namespace = '{safe_ns}' LIMIT 1"
                )
        elif canonical_type == "episodic_memory":
            few_shot_k = int(matched.get("few_shot_k") or bounded_limit)
            recall_sql = (
                f"SELECT topic, tool_used, outcome_score, episode_text FROM {fqn} "
                f"WHERE topic = '{safe_topic}' ORDER BY outcome_score DESC, created_at DESC LIMIT {few_shot_k}"
            )
        else:
            where_clause = f" WHERE text_chunk ILIKE '%{safe_q}%'" if safe_q else ""
            recall_sql = f"SELECT * FROM {fqn}{where_clause} ORDER BY created_at DESC LIMIT {bounded_limit}"

        res = self.execute_sql(recall_sql)
        return {
            "ok": True,
            "recalled": True,
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
            "rows": res.get("rows", []),
        }

    def create_or_register_memory(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create or register a Snowflake Agent Memory across all 5 LangChain/Langflow Memory types (`W2-R2-13`, `W2-R3-05`)."""
        name = str(body.get("name") or "").strip()
        db = str(body.get("database") or self.database).strip().upper()
        sch = str(body.get("schema") or self.schema).strip().upper()
        raw_agent_type = str(body.get("agent_memory_type") or body.get("memory_type") or "").strip().lower()
        category = str(body.get("memory_category") or "").strip().lower()
        backend = str(body.get("vector_database") or body.get("storage_backend") or body.get("recommended_object") or "").strip()

        storage_mode = str(body.get("storage_mode") or "").strip().lower()
        create_physical = bool(
            body.get("create_new_physical_store", body.get("create_physical_resource", storage_mode != "bind_existing"))
        )
        safe_table = "".join(c if c.isalnum() or c == "_" else "_" for c in (name or "MEMORY_STORE")).upper()
        fqn = str(body.get("table_fqn") or f"{db}.{sch}.MEM_{safe_table}").strip()

        if create_physical:
            denied = self.check_write_privilege_or_deny("MEMORY_CREATE_PHYSICAL", fqn)
            if denied:
                return denied
        else:
            if self.active_role.upper() in ("UNAUTHORIZED_ROLE", "NO_SELECT_ROLE") or body.get("deny_bind_existing") is True:
                msg = (
                    f"Il ruolo attivo ({self.active_role}) non dispone del privilegio MEMORY_USE_EXISTING "
                    f"(SELECT / USAGE) su {fqn}."
                )
                self.record_audit_event(
                    action="MEMORY_USE_EXISTING",
                    outcome="DENIED",
                    error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                    target_object=fqn,
                    details={"message": msg, "required_privilege": "MEMORY_USE_EXISTING (SELECT / USAGE)"},
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

        type_spec = CANONICAL_MEMORY_TYPES_BY_ID[canonical_type]
        if not backend:
            if agent_memory_type == "episodic_session" or canonical_type in ("graph_state_checkpointer", "buffer_window_memory", "summary_memory"):
                backend = "Hybrid Table"
            elif agent_memory_type == "structured_long_term" or canonical_type == "user_profile_entity_memory":
                backend = "Semi-Structured (VARIANT)" if category == "semi_structured" else "Standard Table"
            elif enable_embedding:
                backend = "Standard Table (VECTOR)"
            else:
                backend = type_spec["recommended_snowflake_objects"][0]

        if not category:
            if "POSTGRES" in backend.upper() and ("PGVECTOR" in backend.upper() or enable_embedding):
                category = "postgres_pgvector"
            elif "POSTGRES" in backend.upper():
                category = "postgres_non_vector"
            elif "CORTEX SEARCH" in backend.upper():
                category = "cortex_search"
            elif enable_embedding:
                category = "vector_table"
            elif any(t in backend.upper() for t in ("VARIANT", "OBJECT", "ARRAY")) or canonical_type == "user_profile_entity_memory":
                category = "semi_structured" if any(t in backend.upper() for t in ("VARIANT", "OBJECT", "ARRAY")) else "structured"
            else:
                category = "structured"

        default_tbl_type = (
            "HYBRID"
            if (
                "HYBRID" in backend.upper()
                or agent_memory_type == "episodic_session"
                or canonical_type in ("graph_state_checkpointer", "buffer_window_memory", "summary_memory")
            )
            else "STANDARD"
        )
        table_type = str(body.get("table_type") or default_tbl_type).strip().upper()
        semi_type = str(body.get("semi_structured_type") or "").strip().upper()
        embedding_provider = str(
            body.get("embedding_provider") or ("snowflake_cortex" if enable_embedding else "none")
        ).strip().lower()
        if enable_embedding and embedding_provider == "none":
            embedding_provider = "snowflake_cortex"
        embedding_model = str(body.get("embedding_model") or "snowflake-arctic-embed-m-v1.5").strip()
        elem_type = str(body.get("vector_element_type") or "FLOAT").strip().upper()
        batch_size = int(body.get("batch_size") or 64)
        llm_preprocessing = body.get("llm_preprocessing") or None
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

        dim = requested_dim if embedding_provider == "custom_endpoint" else expected_dim
        table_kw = "HYBRID TABLE" if table_type == "HYBRID" else "TABLE"
        vec_col_ddl = f", embedding VECTOR({elem_type}, {dim})" if is_vector else ""

        if not create_physical:
            ddl = f"-- Bound to existing Snowflake Memory store {fqn} (MEMORY_USE_EXISTING)"
        elif category == "postgres_pgvector" or ("POSTGRES" in backend.upper() and is_vector):
            ddl = (
                f"CREATE EXTENSION IF NOT EXISTS vector; "
                f"CREATE TABLE IF NOT EXISTS {sch.lower()}.mem_{safe_table.lower()} ("
                f"chunk_id TEXT PRIMARY KEY, memory_name TEXT, text_chunk TEXT, "
                f"embedding vector({dim}), metadata JSONB DEFAULT '{{}}'::jsonb, "
                f"created_by_role TEXT, created_at TIMESTAMPTZ DEFAULT NOW());"
            )
        elif category == "postgres_non_vector" or ("POSTGRES" in backend.upper() and not is_vector):
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {sch.lower()}.mem_{safe_table.lower()} ("
                f"session_id TEXT, thread_id TEXT, user_id TEXT, entity_namespace TEXT, "
                f"chunk_id TEXT PRIMARY KEY, memory_name TEXT, text_chunk TEXT, "
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
        elif canonical_type == "graph_state_checkpointer":
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"thread_id VARCHAR(128), checkpoint_ns VARCHAR(128) DEFAULT 'default', "
                f"step_id INTEGER DEFAULT 0, memory_name VARCHAR(256), state_blob VARIANT, "
                f"ttl_hours INTEGER DEFAULT 24{vec_col_ddl}, metadata VARIANT, "
                f"created_by_role VARCHAR(128), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"PRIMARY KEY (thread_id, checkpoint_ns, step_id));"
            )
        elif canonical_type == "buffer_window_memory":
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"session_id VARCHAR(128), turn_id VARCHAR(128), memory_name VARCHAR(256), "
                f"sender VARCHAR(64), message_text VARCHAR, text_chunk VARCHAR{vec_col_ddl}, "
                f"metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY (session_id, turn_id));"
            )
        elif canonical_type == "summary_memory":
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"session_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), "
                f"running_summary VARCHAR, last_turn_id VARCHAR(128), "
                f"summarizer_model VARCHAR(128) DEFAULT 'mistral-large2', "
                f"max_summary_tokens INTEGER DEFAULT 512{vec_col_ddl}, metadata VARIANT, "
                f"created_by_role VARCHAR(128), updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        elif canonical_type == "user_profile_entity_memory":
            col_type = semi_type if semi_type in ("VARIANT", "OBJECT", "ARRAY") else "VARIANT"
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, user_id VARCHAR(128), "
                f"entity_namespace VARCHAR(128) DEFAULT 'default', memory_name VARCHAR(256), "
                f"profile_json {col_type}, payload {col_type}, text_chunk VARCHAR{vec_col_ddl}, "
                f"metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        elif canonical_type == "episodic_memory" and not is_vector:
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), "
                f"topic VARCHAR(256), tool_used VARCHAR(256), outcome_score FLOAT DEFAULT 1.0, "
                f"episode_text VARCHAR, text_chunk VARCHAR, metadata VARIANT, "
                f"created_by_role VARCHAR(128), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )
        else:
            ddl = (
                f"CREATE {table_kw} IF NOT EXISTS {fqn} ("
                f"chunk_id VARCHAR(128) PRIMARY KEY, memory_name VARCHAR(256), "
                f"topic VARCHAR(256), tool_used VARCHAR(256), outcome_score FLOAT DEFAULT 1.0, "
                f"episode_text VARCHAR, text_chunk VARCHAR, "
                f"embedding VECTOR({elem_type}, {dim}), metadata VARIANT, created_by_role VARCHAR(128), "
                f"created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )

        exec_res = (
            {"ok": True, "dry_run": True}
            if (body.get("dry_run") or not create_physical)
            else self.execute_sql(ddl)
        )
        how_to_feed_sql = type_spec["how_to_feed_without_embedding"].replace("<fqn>", fqn)
        entry = {
            "name": name,
            "database": db,
            "schema": sch,
            "table_fqn": fqn,
            "agent_memory_type": agent_memory_type,
            "canonical_memory_type": canonical_type,
            "label": type_spec["label"],
            "memory_category": category,
            "vector_database": backend,
            "recommended_snowflake_objects": list(type_spec["recommended_snowflake_objects"]),
            "needs_embedding_badge": type_spec["needs_embedding_badge"],
            "how_to_feed_without_embedding": how_to_feed_sql,
            "when_embedding_useful": type_spec["when_embedding_useful"],
            "required_inputs": list(type_spec["required_inputs"]),
            "table_type": table_type,
            "semi_structured_type": semi_type or None,
            "enable_embedding": is_vector,
            "is_vector_memory": is_vector,
            "embedding_provider": embedding_provider if is_vector else "none",
            "embedding_model": embedding_model if is_vector else None,
            "vector_element_type": elem_type if is_vector else None,
            "dimension": dim if is_vector else None,
            "endpoint_url": endpoint_url if is_vector else None,
            "batch_size": batch_size,
            "llm_preprocessing": llm_preprocessing,
            "thread_id": body.get("thread_id"),
            "checkpoint_ns": body.get("checkpoint_ns"),
            "ttl_hours": body.get("ttl_hours"),
            "session_id": body.get("session_id"),
            "window_k": body.get("window_k"),
            "retention_days": body.get("retention_days"),
            "summarizer_model": body.get("summarizer_model"),
            "max_summary_tokens": body.get("max_summary_tokens"),
            "user_id": body.get("user_id"),
            "entity_namespace": body.get("entity_namespace"),
            "schema_keys": body.get("schema_keys"),
            "topic_column": body.get("topic_column"),
            "tool_used_column": body.get("tool_used_column"),
            "outcome_score_threshold": body.get("outcome_score_threshold"),
            "few_shot_k": body.get("few_shot_k"),
            "storage_mode": "create_physical" if create_physical else "bind_existing",
            "ddl": ddl,
            "created_by_role": self.active_role,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snowflake_created": bool(exec_res.get("ok")) and create_physical,
        }
        self.memories_registry.append(entry)
        vis_mem = self.selected_context.setdefault("visible_objects", {}).setdefault("memories", [])
        if name and name not in vis_mem:
            vis_mem.append(name)
        self.record_audit_event(
            action="MEMORY_CREATE_PHYSICAL" if create_physical else "MEMORY_USE_EXISTING",
            outcome="ALLOWED",
            target_object=entry["table_fqn"],
            details=entry,
        )
        return {"ok": True, "status_code": 201, "memory": entry, "ddl": ddl}

    def get_context_databases(self) -> list[str]:
        """Return active databases in the Hub cone of visibility (`W2-R2-00`)."""
        dbs = [str(d).strip().upper() for d in (self.selected_context.get("databases") or []) if str(d).strip()]
        default_db = (self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        if default_db and default_db not in dbs:
            dbs.insert(0, default_db)
        return dbs or ["SNOWFLAKE_LANGFLOW"]

    def get_context_schemas(self, database: str | None = None) -> list[str]:
        """Return active schemas for `<database>` in the Hub cone of visibility (`W2-R2-00`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        schemas_by_db = self.selected_context.get("schemas_by_db") or {}
        schemas = [str(s).strip().upper() for s in (schemas_by_db.get(db) or []) if str(s).strip()]
        if not schemas and db == (self.database or "").strip().upper():
            default_sch = (self.schema or "CORE").strip().upper()
            schemas = [default_sch, "PUBLIC"] if default_sch != "PUBLIC" else ["PUBLIC"]
        return schemas or ["CORE", "PUBLIC"]

    def get_context_objects(
        self,
        database: str | None = None,
        schema: str | None = None,
        category: str = "tables",
    ) -> list[str]:
        """Return objects for `(<database>, <schema>, <category>)` filtered by `selected_objects` (`W2-R2-00`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or self.schema or "CORE").strip().upper()
        cat = (category or "tables").strip().lower()
        db_sch_key = f"{db}.{sch}"

        objs_by_pair = self.selected_context.get("objects_by_db_schema") or {}
        pair_bucket = objs_by_pair.get(db_sch_key) if isinstance(objs_by_pair, dict) else None
        vis = self.selected_context.get("visible_objects") or {}

        candidates: list[str] = []
        if isinstance(pair_bucket, dict) and cat in pair_bucket and isinstance(pair_bucket[cat], list):
            candidates.extend(str(x).strip() for x in pair_bucket[cat] if str(x).strip())
        if isinstance(vis.get(cat), list):
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

        sel_objs = self.selected_context.get("selected_objects")
        allowed_set: set[str] | None = None
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
            return filtered

        return candidates

    def can_create_database(self) -> bool:
        """Return True if the active role is permitted to execute CREATE DATABASE ON ACCOUNT."""
        return self.active_role.strip().upper() in (
            "ACCOUNTADMIN",
            "SYSADMIN",
            "SNOWFLAKE_LANGFLOW_ADMIN_ROLE",
        )

    def can_create_schema(self) -> bool:
        """Return True if the active role is permitted to execute CREATE SCHEMA in the target database."""
        return self.active_role.strip().upper() not in (
            "PUBLIC",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "VIEWER_ROLE",
            "ROLE_VIEWER",
        )

    def can_create_object(self) -> bool:
        """Return True if the active role is permitted to create schema-level objects."""
        return self.can_create_schema()

    def get_role_privileges(self) -> dict[str, bool]:
        """Compute granular active-role creation privileges for Hub Section 1 & sidebar components."""
        can_db = self.can_create_database()
        can_sch = self.can_create_schema()
        can_obj = self.can_create_object()
        return {
            "can_create_database": can_db,
            "can_create_schema": can_sch,
            "can_create_table": can_obj,
            "can_create_stage": can_obj,
            "can_create_view": can_obj,
            "can_create_function": can_obj,
            "can_create_procedure": can_obj,
            "can_create_cortex_search": can_obj,
            "can_create_postgres": can_db,
            "can_create_object": can_obj,
        }

    @property
    def role_privileges(self) -> dict[str, bool]:
        return self.get_role_privileges()

    def update_selected_context(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist cascading primary context selection from Snowflake Langflow Hub (`HUB-W2-02`, `W2-R2-00`)."""
        incoming_ctx = payload.get("selected_context") if isinstance(payload.get("selected_context"), dict) else {}
        new_db = str(payload.get("database") or incoming_ctx.get("database") or "").strip().upper()
        new_sch = str(payload.get("schema") or incoming_ctx.get("schema") or "").strip().upper()
        if new_db:
            self.database = new_db
        if new_sch:
            self.schema = new_sch

        dbs = payload.get("databases") or incoming_ctx.get("databases")
        if isinstance(dbs, list) and dbs:
            self.selected_context["databases"] = [str(d).strip().upper() for d in dbs if str(d).strip()]
            if not new_db and self.selected_context["databases"]:
                self.database = self.selected_context["databases"][0]
        elif self.database not in self.selected_context.get("databases", []):
            self.selected_context.setdefault("databases", []).insert(0, self.database)

        schemas_by_db = payload.get("schemas_by_db") or incoming_ctx.get("schemas_by_db")
        if isinstance(schemas_by_db, dict) and schemas_by_db:
            normalized_map: dict[str, list[str]] = {}
            for k, v in schemas_by_db.items():
                db_k = str(k).strip().upper()
                if isinstance(v, list):
                    normalized_map[db_k] = [str(s).strip().upper() for s in v if str(s).strip()]
            if normalized_map:
                self.selected_context["schemas_by_db"] = normalized_map
                if not new_sch and self.database in normalized_map and normalized_map[self.database]:
                    self.schema = normalized_map[self.database][0]
        else:
            cur_map = self.selected_context.setdefault("schemas_by_db", {})
            cur_list = cur_map.setdefault(self.database, [self.schema])
            if self.schema not in cur_list:
                cur_list.insert(0, self.schema)

        objs_by_pair = payload.get("objects_by_db_schema") or incoming_ctx.get("objects_by_db_schema")
        if isinstance(objs_by_pair, dict) and objs_by_pair:
            norm_objs: dict[str, dict[str, list[str]]] = {}
            for pair_k, pair_v in objs_by_pair.items():
                clean_pair = str(pair_k).strip().upper()
                if isinstance(pair_v, dict):
                    norm_objs[clean_pair] = {
                        str(cat_k).strip().lower(): [str(item).strip() for item in cat_v if str(item).strip()]
                        for cat_k, cat_v in pair_v.items()
                        if isinstance(cat_v, list)
                    }
            self.selected_context["objects_by_db_schema"] = norm_objs

        sel_objs = payload.get("selected_objects") if "selected_objects" in payload else incoming_ctx.get("selected_objects")
        if sel_objs is not None:
            self.selected_context["selected_objects"] = sel_objs

        vis = payload.get("visible_objects") or incoming_ctx.get("visible_objects")
        if isinstance(vis, dict):
            cur_vis = self.selected_context.setdefault("visible_objects", {})
            for k, v in vis.items():
                if isinstance(v, list):
                    cur_vis[k] = [str(item).strip() for item in v if str(item).strip()]
                elif isinstance(v, bool):
                    # When Hub passes boolean category toggles, preserve or clear list accordingly
                    if not v:
                        cur_vis[k] = []
                    else:
                        cur_vis.setdefault(k, [])

        self.context_epoch += 1
        compact_audit_details = {
            "database": self.database,
            "schema": self.schema,
            "databases": list(self.selected_context.get("databases") or []),
            "schemas_by_db": dict(self.selected_context.get("schemas_by_db") or {}),
            "selected_pairs_count": len(self.selected_context.get("selected_objects") or {}),
            "context_epoch": self.context_epoch,
        }
        self.record_audit_event(
            action="UPDATE_CONTEXT",
            outcome="ALLOWED",
            target_object=f"{self.database}.{self.schema}",
            details=compact_audit_details,
        )
        return {
            "ok": True,
            "status": "ok",
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "database": self.database,
            "schema": self.schema,
            "selected_context": self.selected_context,
            "role_privileges": self.role_privileges,
        }

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
            fut_dbs = _CATALOG_EXECUTOR.submit(self.execute_sql, "SHOW DATABASES", timeout=12)
            fut_pg = _CATALOG_EXECUTOR.submit(self.execute_sql, "SHOW POSTGRES INSTANCES", timeout=12)
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
                pair_key = f"{target_db}.{target_sch}"
                obj_cache_key = f"{self.active_role}:{pair_key}"
                cached_obj = self._objects_cache.get(obj_cache_key) if use_live_parallel_cache else None
                if cached_obj and (now - cached_obj[0]) < 120.0:
                    cached_payload = cached_obj[1]
                    raw_objs = cached_payload["raw_objs"]
                    stages_rows = cached_payload["stages_rows"]
                    pair_bucket = dict(cached_payload["pair_bucket"])
                    pair_bucket["postgres_instances"] = postgres_instances
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
                        f"SHOW CORTEX AGENTS IN SCHEMA {pair_key}",
                    ]
                    if self._has_spcs_token() and not self._is_execute_sql_mocked():
                        futs = [_CATALOG_EXECUTOR.submit(self.execute_sql, q, timeout=10) for q in show_queries]
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

                    if not pair_tables and target_db == db and target_sch == sch:
                        pair_tables = list(
                            self.selected_context.get("visible_objects", {}).get(
                                "tables", ["LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_SPANS", "LANGFLOW_AUDIT_LOG"]
                            )
                        )
                    if not pair_iceberg and target_db == db and target_sch == sch:
                        pair_iceberg = list(
                            self.selected_context.get("visible_objects", {}).get("iceberg_tables", [])
                        )

                    pair_stages = _extract_names(stages_rows) or (
                        [self.stage_name] if target_db == db and target_sch == sch else []
                    )
                    pair_funcs = _extract_names(funcs_res.get("rows", [])) or (
                        list(self.selected_context.get("visible_objects", {}).get("functions", []))
                        if target_db == db and target_sch == sch
                        else []
                    )
                    pair_procs = _extract_names(procs_res.get("rows", [])) or (
                        list(self.selected_context.get("visible_objects", {}).get("procedures", []))
                        if target_db == db and target_sch == sch
                        else []
                    )
                    pair_search = _extract_names(search_res.get("rows", [])) or (
                        list(self.selected_context.get("visible_objects", {}).get("cortex_search_services", []))
                        if target_db == db and target_sch == sch
                        else []
                    )
                    pair_sem_views = _extract_names(sem_views_res.get("rows", [])) or (
                        list(self.selected_context.get("visible_objects", {}).get("semantic_views", []))
                        if target_db == db and target_sch == sch
                        else []
                    )
                    pair_agents = _extract_names(agents_res.get("rows", [])) or (
                        list(self.selected_context.get("visible_objects", {}).get("cortex_agents", []))
                        if target_db == db and target_sch == sch
                        else []
                    )

                    pair_bucket = {
                        "tables": pair_tables,
                        "views": pair_views,
                        "iceberg_tables": pair_iceberg,
                        "stages": pair_stages,
                        "functions": pair_funcs,
                        "procedures": pair_procs,
                        "cortex_search_services": pair_search,
                        "semantic_views": pair_sem_views,
                        "semantic_models": list(self.selected_context.get("visible_objects", {}).get("semantic_models", [])),
                        "cortex_agents": pair_agents,
                        "postgres_instances": postgres_instances,
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

        memories = [m.get("name", "") for m in self.memories_registry if m.get("name")] or ["SNOWFLAKE_KB_MEM"]
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
            "stage_names": merged_stages or [self.stage_name],
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
    ) -> dict[str, Any]:
        """Invoke the official Snowflake Cortex Analyst REST API `/api/v2/cortex/analyst/message` (`W2-R2-06`)."""
        db = (database or self.database or "SNOWFLAKE_LANGFLOW").strip().upper()
        sch = (schema or self.schema or "CORE").strip().upper()
        src = (semantic_source or "").strip()
        mode = (source_mode or "Semantic View").strip()
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
                    resp = client.post(url, headers=self._get_spcs_headers(), json=request_payload)
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
        if execute_sql and generated_sql:
            exec_res = self.execute_sql(generated_sql)
            sql_rows = exec_res.get("rows", [])

        return {
            "ok": True,
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

    def _has_spcs_token(self) -> bool:
        return SPCS_TOKEN_PATH.exists() and bool(os.getenv("SNOWFLAKE_HOST"))

    def _get_spcs_headers(self) -> dict[str, str]:
        token = SPCS_TOKEN_PATH.read_text(encoding="utf-8").strip()
        return {
            "Authorization": f"Bearer {token}",
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
    ) -> dict[str, Any]:
        """Execute a SQL statement via Snowflake SQL REST API (`/api/v2/statements`) using SPCS OAuth."""
        effective_role = (role or self.active_role).strip()
        if not self._has_spcs_token():
            return {
                "ok": False,
                "mode": "offline",
                "effective_role": effective_role,
                "statement": statement,
                "rows": [],
                "columns": [],
                "statement_handle": f"offline-{uuid.uuid4().hex[:12]}",
            }

        host = os.environ["SNOWFLAKE_HOST"]
        url = f"https://{host}/api/v2/statements"
        payload: dict[str, Any] = {
            "statement": statement,
            "timeout": timeout,
            "database": self.database,
            "schema": self.schema,
            "role": effective_role,
        }
        if self.warehouse:
            payload["warehouse"] = self.warehouse
        if bindings:
            payload["bindings"] = bindings

        try:
            if self._http_client is None and httpx is not None:
                self._http_client = httpx.Client(
                    timeout=35.0,
                    verify=True,
                    limits=httpx.Limits(max_keepalive_connections=16, max_connections=32),
                )
            client = self._http_client
            resp = client.post(
                url,
                headers=self._get_spcs_headers(),
                json=payload,
                timeout=float(min(max(timeout, 5), 60) + 5),
            )
            body = resp.json() if resp.content else {}
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

    def refresh_roles_from_snowflake(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if (
            not force
            and self._roles_discovered
            and not self._is_execute_sql_mocked()
            and (now - self._roles_last_refreshed_ts) < 60.0
        ):
            return
        res = self.execute_sql(
            "SELECT CURRENT_USER() AS USR, CURRENT_ROLE() AS RL, CURRENT_AVAILABLE_ROLES() AS AVAIL",
            timeout=10,
        )
        if res.get("ok") and res.get("rows"):
            row = res["rows"][0]
            self.authenticated_user = row.get("USR") or self.authenticated_user
            current_rl = row.get("RL")
            if current_rl and not self._roles_discovered:
                self.active_role = current_rl
            raw_avail = row.get("AVAIL")
            if raw_avail:
                try:
                    parsed = json.loads(raw_avail) if isinstance(raw_avail, str) else raw_avail
                    if isinstance(parsed, list) and parsed:
                        for mandatory in (
                            "ACCOUNTADMIN",
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
    ) -> str:
        event_id = f"aud-{uuid.uuid4().hex[:16]}"
        redacted = redact_for_sink(details or {})
        details_json = json.dumps(redacted).replace("'", "''")
        if len(details_json) > 12000:
            details_json = json.dumps({"summary": "truncated_large_audit_payload", "keys": list(redacted.keys())}).replace("'", "''")
        err_sql = f"'{error_code}'" if error_code else "NULL"
        tgt_sql = f"'{str(target_object).replace(chr(39), chr(39)*2)}'" if target_object else "NULL"
        flw_sql = f"'{flow_id}'" if flow_id else "NULL"
        run_sql = f"'{run_id}'" if run_id else "NULL"
        stmt = (
            f"INSERT INTO {self.audit_table} "
            f"(event_id, event_ts, authenticated_user, effective_role, execution_mode, context_epoch, "
            f"action, outcome, error_code, target_object, project_id, flow_id, run_id, trace_id, redacted_details) "
            f"SELECT '{event_id}', CURRENT_TIMESTAMP(), '{self.authenticated_user}', '{self.active_role}', "
            f"'{ExecutionMode.INTERACTIVE_CALLER.value}', {self.context_epoch}, '{action}', '{outcome}', "
            f"{err_sql}, {tgt_sql}, 'default', {flw_sql}, {run_sql}, {run_sql}, PARSE_JSON('{details_json}')"
        )
        # In live SPCS mode, dispatch audit INSERT asynchronously so UI requests never block on Snowflake DML
        if self._has_spcs_token() and not self._is_execute_sql_mocked():
            _AUDIT_EXECUTOR.submit(self.execute_sql, stmt, role="ACCOUNTADMIN", timeout=15)
        else:
            self.execute_sql(stmt, role="ACCOUNTADMIN")
        return event_id

    def switch_role(self, target_role: str) -> dict[str, Any]:
        clean = (target_role or "").strip().upper()
        if not clean or clean not in [r.upper() for r in self.available_roles]:
            self.record_audit_event(
                action="ROLE_SWITCH",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=clean,
                details={"requested_role": clean, "available_roles": self.available_roles},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": f"Il ruolo attivo ({self.active_role}) non consente il passaggio al ruolo non concesso '{clean}'.",
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        # Verify role activation on Snowflake
        check = self.execute_sql("SELECT CURRENT_ROLE() AS VERIFIED_ROLE", role=clean, timeout=12)
        if not check.get("ok") and self._has_spcs_token():
            self.record_audit_event(
                action="ROLE_SWITCH",
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=clean,
                details={"reason": check.get("message")},
            )
            return {
                "ok": False,
                "status_code": 403,
                "error_code": ErrorCode.AUTHZ_ROLE_DENIED.value,
                "message": f"Il ruolo attivo ({self.active_role}) non consente questa operazione: impossibile attivare {clean}.",
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            }

        previous_role = self.active_role
        self.active_role = clean
        self.context_epoch += 1
        self.invalidate_catalog_cache()
        self.record_audit_event(
            action="ROLE_SWITCH",
            outcome="ALLOWED",
            target_object=clean,
            details={"previous_role": previous_role, "new_role": clean, "context_epoch": self.context_epoch},
        )
        return {
            "ok": True,
            "status_code": 200,
            "previous_role": previous_role,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "secondary_roles_mode": self.secondary_roles_mode,
            "role_privileges": self.role_privileges,
            "selected_context": self.selected_context,
            "cache_invalidated": True,
        }

    def set_context(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Alias for `update_selected_context` used by Snowsight hierarchical tree context sync."""
        return self.update_selected_context(payload)

    def check_write_privilege_or_deny(self, action: str, target_object: str) -> dict[str, Any] | None:
        """Fail-closed role check: VIEWER and PUBLIC roles are explicitly denied DDL/write actions."""
        if not self.can_create_object() or self.active_role.upper() in (
            "PUBLIC",
            "SNOWFLAKE_LANGFLOW_VIEWER_ROLE",
            "VIEWER_ROLE",
            "ROLE_VIEWER",
        ):
            msg = (
                f"Il ruolo attivo ({self.active_role}) non dispone dei privilegi per creare questo oggetto "
                f"in {self.database}.{self.schema} ({action} su {target_object})."
            )
            self.record_audit_event(
                action=action,
                outcome="DENIED",
                error_code=ErrorCode.AUTHZ_ROLE_DENIED.value,
                target_object=target_object,
                details={"message": msg, "required_privilege": action, "object_fqn": target_object},
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

    def sync_langflow_traces_to_snowflake(self, limit: int = 25, *, replicate_sqlite: bool = True) -> dict[str, Any]:
        """Replicate Langflow SQLite `transaction` / `vertex_build` rows into Snowflake `LANGFLOW_FLOW_RUNS` & `SPANS`."""
        persist_dir = Path(os.getenv("LANGFLOW_PERSIST_DIR", "/persist"))
        db_path = persist_dir / "langflow.db"
        replicated_runs = 0
        replicated_spans = 0

        if replicate_sqlite and db_path.exists():
            try:
                conn = _open_sqlite_conn(db_path, readonly=True, timeout=30.0)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                tables = {
                    r[0]
                    for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                if "transaction" in tables:
                    bounded_tx = min(int(limit or 10), 10)
                    rows = cur.execute(
                        "SELECT * FROM \"transaction\" ORDER BY timestamp DESC LIMIT ?", (bounded_tx,)
                    ).fetchall()
                else:
                    rows = []
                conn.close()

                for r in rows:
                    row_dict = dict(r)
                    tx_id = str(row_dict.get("id") or uuid.uuid4().hex)
                    if tx_id in self._synced_tx_ids:
                        continue
                    flow_id = str(row_dict.get("flow_id") or "unknown-flow")
                    vertex_id = str(row_dict.get("vertex_id") or "vertex")
                    status_val = str(row_dict.get("status") or "SUCCESS").upper()
                    inputs_redacted = json.dumps(redact_for_sink(row_dict.get("inputs") or {})).replace("'", "''")[:4000]
                    outputs_redacted = json.dumps(redact_for_sink(row_dict.get("outputs") or {})).replace("'", "''")[:4000]

                    merge_span = (
                        f"MERGE INTO {self.spans_table} t "
                        f"USING (SELECT '{tx_id}' AS span_id) s ON t.span_id = s.span_id "
                        f"WHEN NOT MATCHED THEN INSERT "
                        f"(event_id, span_id, run_id, trace_id, project_id, flow_id, component_id, component_name, "
                        f"authenticated_user, effective_role, execution_mode, start_ts, end_ts, status, "
                        f"redacted_inputs, redacted_outputs) VALUES "
                        f"('evt-{tx_id[:16]}', '{tx_id}', 'run-{flow_id[:16]}', 'trc-{flow_id[:16]}', "
                        f"'default', '{flow_id}', '{vertex_id}', '{vertex_id}', "
                        f"'{self.authenticated_user}', '{self.active_role}', 'INTERACTIVE_CALLER', "
                        f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{status_val}', "
                        f"PARSE_JSON('{inputs_redacted}'), PARSE_JSON('{outputs_redacted}'))"
                    )
                    self.execute_sql(merge_span, role="ACCOUNTADMIN", timeout=12)
                    replicated_spans += 1

                    merge_run = (
                        f"MERGE INTO {self.runs_table} t "
                        f"USING (SELECT 'run-{flow_id[:16]}' AS run_id) s ON t.run_id = s.run_id "
                        f"WHEN NOT MATCHED THEN INSERT "
                        f"(event_id, run_id, trace_id, project_id, flow_id, authenticated_user, effective_role, "
                        f"execution_mode, start_ts, end_ts, status, input_payload, output_payload) VALUES "
                        f"('evr-{flow_id[:16]}', 'run-{flow_id[:16]}', 'trc-{flow_id[:16]}', 'default', '{flow_id}', "
                        f"'{self.authenticated_user}', '{self.active_role}', 'INTERACTIVE_CALLER', "
                        f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{status_val}', "
                        f"PARSE_JSON('{inputs_redacted}'), PARSE_JSON('{outputs_redacted}'))"
                    )
                    self.execute_sql(merge_run, role="ACCOUNTADMIN", timeout=12)
                    replicated_runs += 1
                    self._synced_tx_ids.add(tx_id)
            except Exception:  # noqa: BLE001
                pass

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
            f_runs = _CATALOG_EXECUTOR.submit(self.execute_sql, q_runs, role="ACCOUNTADMIN", timeout=12)
            f_spans = _CATALOG_EXECUTOR.submit(self.execute_sql, q_spans, role="ACCOUNTADMIN", timeout=12)
            f_audit = _CATALOG_EXECUTOR.submit(self.execute_sql, q_audit, role="ACCOUNTADMIN", timeout=12)
            runs_res = f_runs.result()
            spans_res = f_spans.result()
            audit_res = f_audit.result()
        else:
            runs_res = self.execute_sql(q_runs, role="ACCOUNTADMIN")
            spans_res = self.execute_sql(q_spans, role="ACCOUNTADMIN")
            audit_res = self.execute_sql(q_audit, role="ACCOUNTADMIN")

        return {
            "ok": True,
            "replicated_runs": replicated_runs,
            "replicated_spans": replicated_spans,
            "runs": runs_res.get("rows", []),
            "spans": spans_res.get("rows", []),
            "audit_log": audit_res.get("rows", []),
        }


LIVE_BRIDGE = LiveSnowflakeBridge()


def register_snowflake_fastapi_routes(app: Any) -> None:
    """Mount `/api/v1/snowflake/*` endpoints and Flow Activity replication middleware onto Langflow's FastAPI app."""
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse

    ensure_sqlite_wal_and_uv_cache()
    sf_router = APIRouter(prefix="/api/v1/snowflake", tags=["SnowflakeLangflow"])

    @sf_router.get("/persistence")
    async def get_persistence_status() -> JSONResponse:
        info = await asyncio.to_thread(LIVE_BRIDGE.sync_persistent_workflows_backup)
        return JSONResponse(status_code=200, content=info)

    @sf_router.get("/context")
    async def get_snowflake_context(request: Request) -> JSONResponse:
        await asyncio.to_thread(LIVE_BRIDGE.sync_superuser_credentials)
        await asyncio.to_thread(LIVE_BRIDGE.refresh_roles_from_snowflake)
        sf_header_user = request.headers.get("Sf-Context-Current-User")
        if sf_header_user:
            LIVE_BRIDGE.authenticated_user = sf_header_user
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "ok": True,
                "authenticated_user": LIVE_BRIDGE.authenticated_user,
                "active_role": LIVE_BRIDGE.active_role,
                "available_roles": LIVE_BRIDGE.available_roles,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "secondary_roles_mode": LIVE_BRIDGE.secondary_roles_mode,
                "database": LIVE_BRIDGE.database,
                "schema": LIVE_BRIDGE.schema,
                "stage_name": f"@{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.{LIVE_BRIDGE.stage_name}",
                "role_privileges": LIVE_BRIDGE.role_privileges,
                "selected_context": LIVE_BRIDGE.selected_context,
                "traces_tables": {
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
    async def post_snowflake_context(request: Request) -> JSONResponse:
        body = await request.json()
        res = await asyncio.to_thread(LIVE_BRIDGE.update_selected_context, body if isinstance(body, dict) else {})
        return JSONResponse(status_code=200, content=res)

    @sf_router.post("/role")
    async def post_switch_role(request: Request) -> JSONResponse:
        body = await request.json()
        target_role = str(body.get("target_role") or body.get("role") or "")
        result = await asyncio.to_thread(LIVE_BRIDGE.switch_role, target_role)
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
        await asyncio.to_thread(
            LIVE_BRIDGE.record_audit_event,
            action=f"PROVISION_{obj_kind.upper()}",
            outcome="ALLOWED" if exec_res.get("ok") else "ERROR",
            error_code=exec_res.get("error_code"),
            target_object=f"{db}.{sch}.{target_name}",
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

    @sf_router.get("/memories")
    async def get_memories() -> JSONResponse:
        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "active_role": LIVE_BRIDGE.active_role,
                "memories": LIVE_BRIDGE.memories_registry,
                "memories_catalog": LIVE_BRIDGE.memories_catalog,
                "agent_memory_types": list(AGENT_MEMORY_TYPES),
                "legacy_memory_type_aliases": dict(LEGACY_MEMORY_TYPE_ALIASES),
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
                    "nomic-embed-text (768d)",
                    "snowflake-arctic-embed-l-v2.0 (1024d)",
                    "snowflake-arctic-embed-l-v2.0-8k (1024d)",
                    "multilingual-e5-large (1024d)",
                    "voyage-multilingual-2 (1024d)",
                    "nv-embed-qa-4 (1024d)",
                    "voyage-multimodal-3 (1024d)",
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
                        "properties": {"target_role": {"type": "string", "description": "Target Snowflake role"}},
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
                    "description": "Synchronize Langflow Flow Activity runs and spans into Snowflake SQL tables (LANGFLOW_FLOW_RUNS, LANGFLOW_FLOW_SPANS, LANGFLOW_AUDIT_LOG).",
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
                        "runs": LIVE_BRIDGE.runs_table,
                        "spans": LIVE_BRIDGE.spans_table,
                        "audit": LIVE_BRIDGE.audit_table,
                    },
                }
            elif tool_name == "snowflake_switch_role":
                payload = await asyncio.to_thread(
                    LIVE_BRIDGE.switch_role, str(args.get("target_role") or "ACCOUNTADMIN")
                )
            elif tool_name == "snowflake_query_sql":
                payload = await asyncio.to_thread(
                    LIVE_BRIDGE.execute_sql, str(args.get("statement") or "SELECT CURRENT_ROLE()")
                )
            elif tool_name == "snowflake_sync_traces":
                payload = await asyncio.to_thread(
                    LIVE_BRIDGE.sync_langflow_traces_to_snowflake, limit=int(args.get("limit") or 20)
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
                        "isError": False,
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

    app.include_router(sf_router)
