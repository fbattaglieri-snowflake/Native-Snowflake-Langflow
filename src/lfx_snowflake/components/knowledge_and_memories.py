"""Snowflake Knowledge, Memory Base & Create Memory Components (`REQ-10`, `[G-A4]`).

Covers:
- Embedding catalog separation & `(model_id, dimension, space_fingerprint)` compatibility guard
- All 4 Snowflake Vector Backends:
  1. `STANDARD_VECTOR`: Standard Snowflake table with `VECTOR(FLOAT, dim)` (`dim <= 4096`)
  2. `HYBRID_VECTOR`: Snowflake Hybrid table with `VECTOR(FLOAT, dim)` where the `VECTOR`
     column is never part of a primary key or secondary index
  3. `CORTEX_SEARCH`: Separates CRUD source table from indexed Cortex Search Service
     (which runs in owner's rights), enforces explicit `(user_id, project_id, effective_role)`
     isolation filters on every query, forbids `SEARCH_PREVIEW` for end-user serving, and
     aligns managed vs precomputed embeddings without duplicate vector generation
  4. `POSTGRES_PGVECTOR`: Snowflake Postgres (`:5432`) with `pgvector`, verifying both
     Snowflake active role and PostgreSQL role/table privileges
- Granular permission separation (`[G-A4]`): `CREATE` privileges are required only when
  provisioning new physical resources, never when reading/writing an existing store
- `Create Memory` dialog contract: `Name`, `Embedding Model`, `Vector Database`,
  `Batch Size`, strictly optional `LLM Preprocessing` (zero generative calls when disabled),
  and UI states (`EMPTY`, `LOADING`, `READY`, `ERROR`, `CANCELLED`, `CREATED`)
- Cross-session isolation in `Memory Base`: `filter_by_session=False` searches across
  sessions only within the caller's `(user_id, project_id, effective_role)` boundary.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping

from ..fastapi_live_bridge import (
    AGENT_MEMORY_TYPES,
    CANONICAL_MEMORY_TYPES_BY_ID,
    EMBEDDING_PROVIDERS_CATALOG,
    EXTERNAL_EMBEDDING_PROVIDERS,
    LEGACY_MEMORY_TYPE_ALIASES,
    LIVE_BRIDGE,
    resolve_canonical_memory_type,
    resolve_memory_embedding_dimension,
)
from .files_and_stage import (
    ErrorCode,
    SnowflakeComponentError,
    raise_role_denied,
    validate_snowflake_identifier,
)

MAX_SNOWFLAKE_VECTOR_DIMENSION = 4096

GENERATIVE_ONLY_PROXY_PROVIDERS = frozenset(
    {
        "Snowflake Proxy - Responses",
        "Snowflake Proxy - Messages",
        "Snowflake Proxy - Chat",
    }
)

ALLOWED_GENERATIVE_PREPROCESSING_PROVIDERS = frozenset(
    {
        "Snowflake Proxy - Responses",
        "Snowflake Proxy - Messages",
        "Snowflake Proxy - Chat",
        "Ollama (Snowflake Hosted)",
        "Cortex SQL",
        "Cortex SQL — AI_COMPLETE (Select Model)",
        "Cortex Native — AI_SUMMARIZE_AGG (Automatic SQL Aggregation)",
        "AI_SUMMARIZE_AGG",
        "Private - Ollama",
        "Model Registry SPCS",
        "Proxy - Chat",
        "Proxy - Responses",
        "Proxy - Messages",
        "cortex_sql",
        "cortex_summarize_agg",
        "proxy_chat",
        "proxy_responses",
        "proxy_messages",
        "spcs_ollama",
        "spcs_model_registry",
    }
)


class VectorBackendType(str, Enum):
    STANDARD_VECTOR = "STANDARD_VECTOR"
    HYBRID_VECTOR = "HYBRID_VECTOR"
    INTERACTIVE_VECTOR = "INTERACTIVE_VECTOR"
    DYNAMIC_VECTOR = "DYNAMIC_VECTOR"
    CORTEX_SEARCH = "CORTEX_SEARCH"
    POSTGRES_PGVECTOR = "POSTGRES_PGVECTOR"
    STANDARD_RELATIONAL = "STANDARD_RELATIONAL"
    HYBRID_RELATIONAL = "HYBRID_RELATIONAL"
    INTERACTIVE_RELATIONAL = "INTERACTIVE_RELATIONAL"
    DYNAMIC_RELATIONAL = "DYNAMIC_RELATIONAL"
    SEMI_STRUCTURED_VARIANT = "SEMI_STRUCTURED_VARIANT"
    SEMI_STRUCTURED_OBJECT = "SEMI_STRUCTURED_OBJECT"
    SEMI_STRUCTURED_ARRAY = "SEMI_STRUCTURED_ARRAY"
    POSTGRES_RELATIONAL = "POSTGRES_RELATIONAL"
    POSTGRES_JSONB = "POSTGRES_JSONB"


MemoryBackendType = VectorBackendType

VECTOR_MEMORY_BACKENDS = frozenset(
    {
        VectorBackendType.STANDARD_VECTOR,
        VectorBackendType.HYBRID_VECTOR,
        VectorBackendType.INTERACTIVE_VECTOR,
        VectorBackendType.DYNAMIC_VECTOR,
        VectorBackendType.CORTEX_SEARCH,
        VectorBackendType.POSTGRES_PGVECTOR,
    }
)

NON_VECTOR_MEMORY_BACKENDS = frozenset(
    {
        VectorBackendType.STANDARD_RELATIONAL,
        VectorBackendType.HYBRID_RELATIONAL,
        VectorBackendType.INTERACTIVE_RELATIONAL,
        VectorBackendType.DYNAMIC_RELATIONAL,
        VectorBackendType.SEMI_STRUCTURED_VARIANT,
        VectorBackendType.SEMI_STRUCTURED_OBJECT,
        VectorBackendType.SEMI_STRUCTURED_ARRAY,
        VectorBackendType.POSTGRES_RELATIONAL,
        VectorBackendType.POSTGRES_JSONB,
    }
)

_MEMORY_BACKEND_ALIASES: dict[str, VectorBackendType] = {
    "STANDARD_VECTOR": VectorBackendType.STANDARD_VECTOR,
    "STANDARD TABLE (VECTOR)": VectorBackendType.STANDARD_VECTOR,
    "HYBRID_VECTOR": VectorBackendType.HYBRID_VECTOR,
    "HYBRID TABLE (VECTOR)": VectorBackendType.HYBRID_VECTOR,
    "INTERACTIVE_VECTOR": VectorBackendType.INTERACTIVE_VECTOR,
    "INTERACTIVE TABLE (VECTOR)": VectorBackendType.INTERACTIVE_VECTOR,
    "DYNAMIC_VECTOR": VectorBackendType.DYNAMIC_VECTOR,
    "DYNAMIC TABLE (VECTOR)": VectorBackendType.DYNAMIC_VECTOR,
    "CORTEX_SEARCH": VectorBackendType.CORTEX_SEARCH,
    "CORTEX SEARCH": VectorBackendType.CORTEX_SEARCH,
    "CORTEX SEARCH SERVICE": VectorBackendType.CORTEX_SEARCH,
    "POSTGRES_PGVECTOR": VectorBackendType.POSTGRES_PGVECTOR,
    "SNOWFLAKE POSTGRES (PGVECTOR)": VectorBackendType.POSTGRES_PGVECTOR,
    "STANDARD_RELATIONAL": VectorBackendType.STANDARD_RELATIONAL,
    "STANDARD TABLE": VectorBackendType.STANDARD_RELATIONAL,
    "STANDARD TABLE (RELATIONAL)": VectorBackendType.STANDARD_RELATIONAL,
    "STRUCTURED TABLE MEMORY": VectorBackendType.STANDARD_RELATIONAL,
    "HYBRID_RELATIONAL": VectorBackendType.HYBRID_RELATIONAL,
    "HYBRID TABLE": VectorBackendType.HYBRID_RELATIONAL,
    "HYBRID TABLE (RELATIONAL)": VectorBackendType.HYBRID_RELATIONAL,
    "INTERACTIVE_RELATIONAL": VectorBackendType.INTERACTIVE_RELATIONAL,
    "INTERACTIVE TABLE": VectorBackendType.INTERACTIVE_RELATIONAL,
    "INTERACTIVE TABLE (RELATIONAL)": VectorBackendType.INTERACTIVE_RELATIONAL,
    "DYNAMIC_RELATIONAL": VectorBackendType.DYNAMIC_RELATIONAL,
    "DYNAMIC TABLE": VectorBackendType.DYNAMIC_RELATIONAL,
    "DYNAMIC TABLE (RELATIONAL)": VectorBackendType.DYNAMIC_RELATIONAL,
    "SEMI_STRUCTURED_VARIANT": VectorBackendType.SEMI_STRUCTURED_VARIANT,
    "SEMI-STRUCTURED (VARIANT)": VectorBackendType.SEMI_STRUCTURED_VARIANT,
    "SEMI-STRUCTURED (VARIANT/OBJECT/ARRAY)": VectorBackendType.SEMI_STRUCTURED_VARIANT,
    "VARIANT": VectorBackendType.SEMI_STRUCTURED_VARIANT,
    "SEMI_STRUCTURED_OBJECT": VectorBackendType.SEMI_STRUCTURED_OBJECT,
    "SEMI-STRUCTURED (OBJECT)": VectorBackendType.SEMI_STRUCTURED_OBJECT,
    "OBJECT": VectorBackendType.SEMI_STRUCTURED_OBJECT,
    "SEMI_STRUCTURED_ARRAY": VectorBackendType.SEMI_STRUCTURED_ARRAY,
    "SEMI-STRUCTURED (ARRAY)": VectorBackendType.SEMI_STRUCTURED_ARRAY,
    "ARRAY": VectorBackendType.SEMI_STRUCTURED_ARRAY,
    "POSTGRES_RELATIONAL": VectorBackendType.POSTGRES_RELATIONAL,
    "SNOWFLAKE POSTGRES": VectorBackendType.POSTGRES_RELATIONAL,
    "SNOWFLAKE POSTGRES (RELATIONAL)": VectorBackendType.POSTGRES_RELATIONAL,
    "SNOWFLAKE POSTGRES (RELATIONAL/JSONB)": VectorBackendType.POSTGRES_RELATIONAL,
    "SNOWFLAKE POSTGRES (RELATIONAL / JSONB)": VectorBackendType.POSTGRES_RELATIONAL,
    "SNOWFLAKE POSTGRES (POSTGRESSAVER)": VectorBackendType.POSTGRES_RELATIONAL,
    "SNOWFLAKE POSTGRES (POSTGRESSTORE)": VectorBackendType.POSTGRES_JSONB,
    "SNOWFLAKECHECKPOINTER": VectorBackendType.HYBRID_RELATIONAL,
    "HYBRID TABLE + AI_COMPLETE / AI_SUMMARIZE_AGG": VectorBackendType.HYBRID_RELATIONAL,
    "STANDARD TABLE + AI_COMPLETE": VectorBackendType.STANDARD_RELATIONAL,
    "HYBRID TABLE (VARIANT)": VectorBackendType.HYBRID_RELATIONAL,
    "STANDARD TABLE (VARIANT)": VectorBackendType.SEMI_STRUCTURED_VARIANT,
    "POSTGRES_JSONB": VectorBackendType.POSTGRES_JSONB,
    "SNOWFLAKE POSTGRES (JSONB)": VectorBackendType.POSTGRES_JSONB,
}


def normalize_memory_backend_type(val: VectorBackendType | str) -> VectorBackendType:
    """Resolve enum values or UI display labels into a canonical VectorBackendType."""
    if isinstance(val, VectorBackendType):
        return val
    cleaned = str(val or "").strip().upper()
    if "ICEBERG" in cleaned and "VECTOR" in cleaned:
        raise SnowflakeComponentError(
            ErrorCode.UNSUPPORTED_CAPABILITY,
            "Native VECTOR columns are not supported on Snowflake Iceberg Tables (HUB-W2-05).",
            http_status=422,
        )
    if cleaned in _MEMORY_BACKEND_ALIASES:
        return _MEMORY_BACKEND_ALIASES[cleaned]
    try:
        return VectorBackendType(cleaned)
    except ValueError as exc:
        raise SnowflakeComponentError(
            ErrorCode.UNSUPPORTED_CAPABILITY,
            f"Unsupported memory backend '{val}'.",
            http_status=422,
        ) from exc


class CortexSearchEmbeddingMode(str, Enum):
    MANAGED = "MANAGED"
    PRECOMPUTED = "PRECOMPUTED"


class DialogState(str, Enum):
    EMPTY = "EMPTY"
    LOADING = "LOADING"
    READY = "READY"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    CREATED = "CREATED"


@dataclass(frozen=True)
class EmbeddingModelSpec:
    model_id: str
    provider: str  # "Snowflake Cortex Embedding" | "Ollama (Snowflake Hosted)"
    dimension: int
    version: str = "v1"
    is_generative: bool = False

    def __post_init__(self) -> None:
        if self.is_generative or self.provider in GENERATIVE_ONLY_PROXY_PROVIDERS:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Generative model/provider '{self.provider}:{self.model_id}' cannot be used as an Embedding Model.",
                http_status=422,
            )
        if self.dimension <= 0 or self.dimension > MAX_SNOWFLAKE_VECTOR_DIMENSION:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Embedding dimension {self.dimension} for '{self.model_id}' must be between 1 and {MAX_SNOWFLAKE_VECTOR_DIMENSION}.",
                http_status=422,
            )

    @property
    def space_fingerprint(self) -> str:
        raw = f"{self.provider.strip().lower()}::{self.model_id.strip().lower()}::{self.version.strip().lower()}::{self.dimension}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG: dict[str, EmbeddingModelSpec] = {
    "snowflake-arctic-embed-l-v2.0": EmbeddingModelSpec(
        model_id="snowflake-arctic-embed-l-v2.0",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="2.0",
    ),
    "snowflake-arctic-embed-l-v2.0-8k": EmbeddingModelSpec(
        model_id="snowflake-arctic-embed-l-v2.0-8k",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="2.0-8k",
    ),
    "multilingual-e5-large": EmbeddingModelSpec(
        model_id="multilingual-e5-large",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="1.0",
    ),
    "voyage-multilingual-2": EmbeddingModelSpec(
        model_id="voyage-multilingual-2",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="2.0",
    ),
    "nv-embed-qa-4": EmbeddingModelSpec(
        model_id="nv-embed-qa-4",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="4.0",
    ),
    "voyage-multimodal-3": EmbeddingModelSpec(
        model_id="voyage-multimodal-3",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="3.0",
    ),
    "snowflake-arctic-embed-m-v1.5": EmbeddingModelSpec(
        model_id="snowflake-arctic-embed-m-v1.5",
        provider="Snowflake Cortex Embedding",
        dimension=768,
        version="1.5",
    ),
    "snowflake-arctic-embed-m": EmbeddingModelSpec(
        model_id="snowflake-arctic-embed-m",
        provider="Snowflake Cortex Embedding",
        dimension=768,
        version="1.0",
    ),
    "e5-base-v2": EmbeddingModelSpec(
        model_id="e5-base-v2",
        provider="Snowflake Cortex Embedding",
        dimension=768,
        version="2.0",
    ),
    "nomic-embed-text": EmbeddingModelSpec(
        model_id="nomic-embed-text",
        provider="Ollama (Snowflake Hosted)",
        dimension=768,
        version="1.5",
    ),
    "twelvelabs-marengo-embed-3-0": EmbeddingModelSpec(
        model_id="twelvelabs-marengo-embed-3-0",
        provider="Snowflake Cortex Embedding",
        dimension=512,
        version="3.0",
    ),
}

EXTERNAL_EMBEDDING_MODELS_CATALOG: dict[str, EmbeddingModelSpec] = {
    **DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG,
    "text-embedding-3-small": EmbeddingModelSpec(
        model_id="text-embedding-3-small",
        provider="OpenAI Embeddings (External)",
        dimension=1536,
        version="3.0",
    ),
    "text-embedding-3-large": EmbeddingModelSpec(
        model_id="text-embedding-3-large",
        provider="OpenAI Embeddings (External)",
        dimension=3072,
        version="3.0",
    ),
    "text-embedding-ada-002": EmbeddingModelSpec(
        model_id="text-embedding-ada-002",
        provider="OpenAI Embeddings (External)",
        dimension=1536,
        version="2.0",
    ),
    "embed-english-v3.0": EmbeddingModelSpec(
        model_id="embed-english-v3.0",
        provider="Cohere Embeddings (External)",
        dimension=1024,
        version="3.0",
    ),
    "embed-multilingual-v3.0": EmbeddingModelSpec(
        model_id="embed-multilingual-v3.0",
        provider="Cohere Embeddings (External)",
        dimension=1024,
        version="3.0",
    ),
    "bge-large-en-v1.5": EmbeddingModelSpec(
        model_id="bge-large-en-v1.5",
        provider="HuggingFace / Ollama Embeddings",
        dimension=1024,
        version="1.5",
    ),
    "model-registry-spcs-embedding": EmbeddingModelSpec(
        model_id="model-registry-spcs-embedding",
        provider="Snowflake Model Registry SPCS",
        dimension=768,
        version="1.0",
    ),
    "proxy-chat-embedding": EmbeddingModelSpec(
        model_id="proxy-chat-embedding",
        provider="Snowflake Standard Proxy SPCS",
        dimension=1024,
        version="1.0",
    ),
    "proxy-responses-embedding": EmbeddingModelSpec(
        model_id="proxy-responses-embedding",
        provider="Snowflake Standard Proxy SPCS",
        dimension=1024,
        version="1.0",
    ),
    "proxy-messages-embedding": EmbeddingModelSpec(
        model_id="proxy-messages-embedding",
        provider="Snowflake Standard Proxy SPCS",
        dimension=1024,
        version="1.0",
    ),
    "custom-embedding-endpoint": EmbeddingModelSpec(
        model_id="custom-embedding-endpoint",
        provider="Custom Embedding Endpoint",
        dimension=768,
        version="1.0",
    ),
}


@dataclass(frozen=True)
class LLMPreprocessingConfig:
    enabled: bool = False
    provider: str | None = None
    model_id: str | None = None
    instructions: str | None = None
    kill_phrase: str = "NO_INGEST"

    def validate(self) -> None:
        if not self.enabled:
            return
        if not self.provider or self.provider not in ALLOWED_GENERATIVE_PREPROCESSING_PROVIDERS:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"LLM Preprocessing requires a qualified generative provider in {sorted(ALLOWED_GENERATIVE_PREPROCESSING_PROVIDERS)}, got {self.provider!r}.",
                http_status=422,
            )
        if not self.model_id or not self.model_id.strip():
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                "LLM Preprocessing is enabled but no generative model_id was selected.",
                http_status=422,
            )


@dataclass
class VectorStoreConfig:
    name: str
    backend_type: VectorBackendType
    database: str
    schema: str
    table_name: str
    embedding_spec: EmbeddingModelSpec = field(
        default_factory=lambda: DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-m-v1.5"]
    )
    batch_size: int = 64
    llm_preprocessing: LLMPreprocessingConfig = field(default_factory=LLMPreprocessingConfig)
    # Hybrid table options
    primary_key_column: str = "RECORD_ID"
    secondary_index_columns: tuple[str, ...] = ("PROJECT_ID", "SESSION_ID")
    vector_column: str = "EMBEDDING_VEC"
    vector_element_type: str = "FLOAT"
    vector_dimension_override: int | None = None
    vector_inside_variant: bool = False
    # Interactive / Dynamic table options
    warehouse: str | None = None
    target_lag: str = "1 minute"
    # Cortex Search options
    cortex_search_service_name: str | None = None
    cortex_embedding_mode: CortexSearchEmbeddingMode = CortexSearchEmbeddingMode.MANAGED
    # Postgres options (:5432 relational / JSONB / pgvector)
    postgres_instance: str | None = None
    postgres_role: str | None = None

    def __post_init__(self) -> None:
        self.backend_type = normalize_memory_backend_type(self.backend_type)
        self.database = validate_snowflake_identifier(self.database, "database")
        self.schema = validate_snowflake_identifier(self.schema, "schema")
        self.table_name = validate_snowflake_identifier(self.table_name, "table_name")
        self.primary_key_column = validate_snowflake_identifier(self.primary_key_column, "primary_key_column")
        self.vector_column = validate_snowflake_identifier(self.vector_column, "vector_column")
        self.vector_element_type = (self.vector_element_type or "FLOAT").strip().upper()
        if self.vector_element_type not in {"FLOAT", "INT"}:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"VECTOR element type must be FLOAT or INT, got '{self.vector_element_type}'.",
                http_status=422,
            )
        if self.vector_inside_variant:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                "Snowflake VECTOR values cannot be nested inside VARIANT columns (HUB-W2-05).",
                http_status=422,
            )
        if self.vector_dimension_override is not None:
            if self.vector_dimension_override < 1 or self.vector_dimension_override > MAX_SNOWFLAKE_VECTOR_DIMENSION:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"VECTOR dimension {self.vector_dimension_override} must be between 1 and {MAX_SNOWFLAKE_VECTOR_DIMENSION}.",
                    http_status=422,
                )
            if self.is_vector_memory and self.vector_dimension_override != self.embedding_spec.dimension:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"VECTOR dimension {self.vector_dimension_override} does not match selected AI_EMBED model "
                    f"'{self.embedding_spec.model_id}' ({self.embedding_spec.dimension}d).",
                    http_status=422,
                )
        if self.batch_size <= 0 or self.batch_size > 2048:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"batch_size must be between 1 and 2048, got {self.batch_size}.",
                http_status=400,
            )
        self.llm_preprocessing.validate()

        if self.backend_type in (VectorBackendType.HYBRID_VECTOR, VectorBackendType.HYBRID_RELATIONAL):
            if self.backend_type == VectorBackendType.HYBRID_VECTOR and (
                self.vector_column.upper() == self.primary_key_column.upper()
                or self.vector_column.upper() in {c.upper() for c in self.secondary_index_columns}
            ):
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "In a Snowflake Hybrid Table, the VECTOR column cannot be a primary key or secondary index column.",
                    http_status=422,
                )

        if self.backend_type == VectorBackendType.CORTEX_SEARCH:
            if not self.cortex_search_service_name:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "CORTEX_SEARCH backend requires 'cortex_search_service_name' in addition to the CRUD source table.",
                    http_status=422,
                )
            self.cortex_search_service_name = validate_snowflake_identifier(
                self.cortex_search_service_name, "cortex_search_service_name"
            )
            if (
                self.cortex_embedding_mode == CortexSearchEmbeddingMode.MANAGED
                and self.embedding_spec.provider != "Snowflake Cortex Embedding"
            ):
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "Cortex Search in MANAGED embedding mode requires a native 'Snowflake Cortex Embedding' model.",
                    http_status=422,
                )

        if self.backend_type in (
            VectorBackendType.POSTGRES_PGVECTOR,
            VectorBackendType.POSTGRES_RELATIONAL,
            VectorBackendType.POSTGRES_JSONB,
        ):
            if not self.postgres_instance or not self.postgres_role:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    f"{self.backend_type.value} backend requires both 'postgres_instance' and explicit 'postgres_role'.",
                    http_status=422,
                )

    @property
    def is_vector_memory(self) -> bool:
        return self.backend_type in VECTOR_MEMORY_BACKENDS

    @property
    def table_fqn(self) -> str:
        return f"{self.database}.{self.schema}.{self.table_name}"

    @property
    def search_service_fqn(self) -> str | None:
        if not self.cortex_search_service_name:
            return None
        return f"{self.database}.{self.schema}.{self.cortex_search_service_name}"


def deterministic_embed_text(text: str, spec: EmbeddingModelSpec) -> list[float]:
    """Generate a deterministic unit-normalized vector for offline/contract tests."""
    seed = hashlib.sha256(f"{spec.space_fingerprint}::{text}".encode("utf-8")).digest()
    raw = [((seed[i % len(seed)] / 255.0) * 2.0 - 1.0) for i in range(spec.dimension)]
    norm = math.sqrt(sum(v * v for v in raw)) or 1.0
    return [round(v / norm, 6) for v in raw]


class InMemoryVectorStoreEngine:
    """In-memory multi-backend vector store supporting Standard, Hybrid, Cortex Search, and PG pgvector."""

    def __init__(
        self,
        *,
        active_role: str = "ROLE_A",
        snowflake_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
        postgres_privileges: Mapping[str, set[tuple[str, str]]] | None = None,
        preprocessor_fn: Callable[[str, LLMPreprocessingConfig], str] | None = None,
    ) -> None:
        self.active_role = active_role
        self.snowflake_privileges: dict[str, set[tuple[str, str]]] = {
            k: set(v) for k, v in (snowflake_privileges or {}).items()
        }
        self.postgres_privileges: dict[str, set[tuple[str, str]]] = {
            k: set(v) for k, v in (postgres_privileges or {}).items()
        }
        self.stores: dict[str, VectorStoreConfig] = {}
        self.records: dict[str, list[dict[str, Any]]] = {}
        self.audit_log: list[dict[str, Any]] = []
        self.executed_ddl: list[str] = []
        self.generative_preprocessing_calls: int = 0
        self.embedding_calls: int = 0
        self._preprocessor_fn = preprocessor_fn

    def record_audit(self, event: dict[str, Any]) -> None:
        self.audit_log.append(event)

    def has_sf_privilege(self, role: str, privilege: str, fqn: str) -> bool:
        allowed = self.snowflake_privileges.get(role, set())
        return (privilege.upper(), fqn.upper()) in allowed or (privilege.upper(), "*") in allowed

    def has_pg_privilege(self, pg_role: str, privilege: str, table_fqn: str) -> bool:
        allowed = self.postgres_privileges.get(pg_role, set())
        return (privilege.upper(), table_fqn.upper()) in allowed or (privilege.upper(), "*") in allowed

    def provision_physical_store(self, config: VectorStoreConfig, *, confirmed: bool = True) -> dict[str, Any]:
        """Provision physical storage (`CREATE TABLE` / `CREATE CORTEX SEARCH SERVICE` / PG `pgvector` / structured / semi-structured)."""
        role = self.active_role
        schema_fqn = f"{config.database}.{config.schema}"
        elem_type = config.vector_element_type
        dim = config.embedding_spec.dimension
        wh_name = config.warehouse or "<WAREHOUSE>"
        lag = config.target_lag or "1 minute"

        if config.backend_type == VectorBackendType.STANDARD_VECTOR:
            if not self.has_sf_privilege(role, "CREATE TABLE", schema_fqn):
                raise_role_denied(role, "CREATE TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE TABLE {config.table_fqn} ("
                f"{config.primary_key_column} VARCHAR PRIMARY KEY, "
                f"USER_ID VARCHAR NOT NULL, PROJECT_ID VARCHAR NOT NULL, SESSION_ID VARCHAR, "
                f"EFFECTIVE_ROLE VARCHAR NOT NULL, CONTENT VARCHAR, METADATA VARIANT, "
                f"SPACE_FINGERPRINT VARCHAR NOT NULL, "
                f"{config.vector_column} VECTOR({elem_type}, {dim}))"
            )
        elif config.backend_type == VectorBackendType.HYBRID_VECTOR:
            if not self.has_sf_privilege(role, "CREATE HYBRID TABLE", schema_fqn):
                raise_role_denied(role, "CREATE HYBRID TABLE", schema_fqn, self.record_audit)
            idx_cols = ", ".join(config.secondary_index_columns)
            ddl = (
                f"CREATE HYBRID TABLE {config.table_fqn} ("
                f"{config.primary_key_column} VARCHAR PRIMARY KEY, "
                f"USER_ID VARCHAR NOT NULL, PROJECT_ID VARCHAR NOT NULL, SESSION_ID VARCHAR, "
                f"EFFECTIVE_ROLE VARCHAR NOT NULL, CONTENT VARCHAR, METADATA VARIANT, "
                f"SPACE_FINGERPRINT VARCHAR NOT NULL, "
                f"{config.vector_column} VECTOR({elem_type}, {dim}), "
                f"INDEX idx_scope ({idx_cols}))"
            )
        elif config.backend_type == VectorBackendType.INTERACTIVE_VECTOR:
            if not (
                self.has_sf_privilege(role, "CREATE INTERACTIVE TABLE", schema_fqn)
                or self.has_sf_privilege(role, "CREATE TABLE", schema_fqn)
            ):
                raise_role_denied(role, "CREATE INTERACTIVE TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE INTERACTIVE TABLE {config.table_fqn} TARGET_LAG = '{lag}' WAREHOUSE = {wh_name} AS "
                f"SELECT CAST(NULL AS VARCHAR) AS {config.primary_key_column}, "
                f"CAST(NULL AS VARCHAR) AS USER_ID, CAST(NULL AS VARCHAR) AS PROJECT_ID, "
                f"CAST(NULL AS VARCHAR) AS SESSION_ID, CAST(NULL AS VARCHAR) AS EFFECTIVE_ROLE, "
                f"CAST(NULL AS VARCHAR) AS CONTENT, CAST(NULL AS VARIANT) AS METADATA, "
                f"CAST(NULL AS VARCHAR) AS SPACE_FINGERPRINT, "
                f"CAST(NULL AS VECTOR({elem_type}, {dim})) AS {config.vector_column} WHERE 1=0"
            )
        elif config.backend_type == VectorBackendType.DYNAMIC_VECTOR:
            if not (
                self.has_sf_privilege(role, "CREATE DYNAMIC TABLE", schema_fqn)
                or self.has_sf_privilege(role, "CREATE TABLE", schema_fqn)
            ):
                raise_role_denied(role, "CREATE DYNAMIC TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE DYNAMIC TABLE {config.table_fqn} TARGET_LAG = '{lag}' WAREHOUSE = {wh_name} REFRESH_MODE = AUTO AS "
                f"SELECT CAST(NULL AS VARCHAR) AS {config.primary_key_column}, "
                f"CAST(NULL AS VARCHAR) AS USER_ID, CAST(NULL AS VARCHAR) AS PROJECT_ID, "
                f"CAST(NULL AS VARCHAR) AS SESSION_ID, CAST(NULL AS VARCHAR) AS EFFECTIVE_ROLE, "
                f"CAST(NULL AS VARCHAR) AS CONTENT, CAST(NULL AS VARIANT) AS METADATA, "
                f"CAST(NULL AS VARCHAR) AS SPACE_FINGERPRINT, "
                f"CAST(NULL AS VECTOR({elem_type}, {dim})) AS {config.vector_column} WHERE 1=0"
            )
        elif config.backend_type == VectorBackendType.CORTEX_SEARCH:
            if not self.has_sf_privilege(role, "CREATE TABLE", schema_fqn):
                raise_role_denied(role, "CREATE TABLE", schema_fqn, self.record_audit)
            if not self.has_sf_privilege(role, "CREATE CORTEX SEARCH SERVICE", schema_fqn):
                raise_role_denied(role, "CREATE CORTEX SEARCH SERVICE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE TABLE {config.table_fqn} (RECORD_ID VARCHAR, USER_ID VARCHAR, PROJECT_ID VARCHAR, "
                f"SESSION_ID VARCHAR, EFFECTIVE_ROLE VARCHAR, CONTENT VARCHAR, SPACE_FINGERPRINT VARCHAR); "
                f"CREATE CORTEX SEARCH SERVICE {config.search_service_fqn} ON CONTENT "
                f"ATTRIBUTES USER_ID, PROJECT_ID, SESSION_ID, EFFECTIVE_ROLE "
                f"WAREHOUSE = <WAREHOUSE> TARGET_LAG = '1 minute' "
                f"EMBEDDING_MODEL = '{config.embedding_spec.model_id}' "
                f"AS (SELECT * FROM {config.table_fqn})"
            )
        elif config.backend_type == VectorBackendType.POSTGRES_PGVECTOR:
            if not self.has_sf_privilege(role, "USAGE", config.postgres_instance or ""):
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", config.postgres_instance or "", self.record_audit)
            pg_role = config.postgres_role or ""
            if not self.has_pg_privilege(pg_role, "CREATE", schema_fqn):
                raise_role_denied(
                    f"{role}/pg:{pg_role}",
                    "POSTGRES CREATE TABLE/EXTENSION",
                    config.table_fqn,
                    self.record_audit,
                )
            ddl = (
                f"CREATE EXTENSION IF NOT EXISTS vector; "
                f"CREATE TABLE {config.schema}.{config.table_name} ("
                f"{config.primary_key_column} TEXT PRIMARY KEY, "
                f"user_id TEXT NOT NULL, project_id TEXT NOT NULL, session_id TEXT, "
                f"effective_role TEXT NOT NULL, content TEXT, space_fingerprint TEXT NOT NULL, "
                f"{config.vector_column} vector({dim}))"
            )
        elif config.backend_type == VectorBackendType.STANDARD_RELATIONAL:
            if not self.has_sf_privilege(role, "CREATE TABLE", schema_fqn):
                raise_role_denied(role, "CREATE TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE TABLE {config.table_fqn} ("
                f"{config.primary_key_column} VARCHAR PRIMARY KEY, "
                f"USER_ID VARCHAR NOT NULL, PROJECT_ID VARCHAR NOT NULL, SESSION_ID VARCHAR, "
                f"EFFECTIVE_ROLE VARCHAR NOT NULL, CONTENT VARCHAR, METADATA VARIANT, "
                f"CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP())"
            )
        elif config.backend_type == VectorBackendType.HYBRID_RELATIONAL:
            if not self.has_sf_privilege(role, "CREATE HYBRID TABLE", schema_fqn):
                raise_role_denied(role, "CREATE HYBRID TABLE", schema_fqn, self.record_audit)
            idx_cols = ", ".join(config.secondary_index_columns)
            ddl = (
                f"CREATE HYBRID TABLE {config.table_fqn} ("
                f"{config.primary_key_column} VARCHAR PRIMARY KEY, "
                f"USER_ID VARCHAR NOT NULL, PROJECT_ID VARCHAR NOT NULL, SESSION_ID VARCHAR, "
                f"EFFECTIVE_ROLE VARCHAR NOT NULL, CONTENT VARCHAR, METADATA VARIANT, "
                f"CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "
                f"INDEX idx_scope ({idx_cols}))"
            )
        elif config.backend_type == VectorBackendType.INTERACTIVE_RELATIONAL:
            if not (
                self.has_sf_privilege(role, "CREATE INTERACTIVE TABLE", schema_fqn)
                or self.has_sf_privilege(role, "CREATE TABLE", schema_fqn)
            ):
                raise_role_denied(role, "CREATE INTERACTIVE TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE INTERACTIVE TABLE {config.table_fqn} TARGET_LAG = '{lag}' WAREHOUSE = {wh_name} AS "
                f"SELECT CAST(NULL AS VARCHAR) AS {config.primary_key_column}, "
                f"CAST(NULL AS VARCHAR) AS USER_ID, CAST(NULL AS VARCHAR) AS PROJECT_ID, "
                f"CAST(NULL AS VARCHAR) AS SESSION_ID, CAST(NULL AS VARCHAR) AS EFFECTIVE_ROLE, "
                f"CAST(NULL AS VARCHAR) AS CONTENT, CAST(NULL AS VARIANT) AS METADATA WHERE 1=0"
            )
        elif config.backend_type == VectorBackendType.DYNAMIC_RELATIONAL:
            if not (
                self.has_sf_privilege(role, "CREATE DYNAMIC TABLE", schema_fqn)
                or self.has_sf_privilege(role, "CREATE TABLE", schema_fqn)
            ):
                raise_role_denied(role, "CREATE DYNAMIC TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE DYNAMIC TABLE {config.table_fqn} TARGET_LAG = '{lag}' WAREHOUSE = {wh_name} REFRESH_MODE = AUTO AS "
                f"SELECT CAST(NULL AS VARCHAR) AS {config.primary_key_column}, "
                f"CAST(NULL AS VARCHAR) AS USER_ID, CAST(NULL AS VARCHAR) AS PROJECT_ID, "
                f"CAST(NULL AS VARCHAR) AS SESSION_ID, CAST(NULL AS VARCHAR) AS EFFECTIVE_ROLE, "
                f"CAST(NULL AS VARCHAR) AS CONTENT, CAST(NULL AS VARIANT) AS METADATA WHERE 1=0"
            )
        elif config.backend_type in (
            VectorBackendType.SEMI_STRUCTURED_VARIANT,
            VectorBackendType.SEMI_STRUCTURED_OBJECT,
            VectorBackendType.SEMI_STRUCTURED_ARRAY,
        ):
            if not self.has_sf_privilege(role, "CREATE TABLE", schema_fqn):
                raise_role_denied(role, "CREATE TABLE", schema_fqn, self.record_audit)
            semi_col_type = (
                "OBJECT"
                if config.backend_type == VectorBackendType.SEMI_STRUCTURED_OBJECT
                else ("ARRAY" if config.backend_type == VectorBackendType.SEMI_STRUCTURED_ARRAY else "VARIANT")
            )
            ddl = (
                f"CREATE TABLE {config.table_fqn} ("
                f"{config.primary_key_column} VARCHAR PRIMARY KEY, "
                f"USER_ID VARCHAR NOT NULL, PROJECT_ID VARCHAR NOT NULL, SESSION_ID VARCHAR, "
                f"EFFECTIVE_ROLE VARCHAR NOT NULL, CONTENT VARCHAR, "
                f"PAYLOAD {semi_col_type}, METADATA VARIANT, "
                f"CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP())"
            )
        elif config.backend_type in (VectorBackendType.POSTGRES_RELATIONAL, VectorBackendType.POSTGRES_JSONB):
            if not self.has_sf_privilege(role, "USAGE", config.postgres_instance or ""):
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", config.postgres_instance or "", self.record_audit)
            pg_role = config.postgres_role or ""
            if not self.has_pg_privilege(pg_role, "CREATE", schema_fqn):
                raise_role_denied(
                    f"{role}/pg:{pg_role}",
                    "POSTGRES CREATE TABLE",
                    config.table_fqn,
                    self.record_audit,
                )
            ddl = (
                f"CREATE SCHEMA IF NOT EXISTS {config.schema.lower()}; "
                f"CREATE TABLE {config.schema.lower()}.{config.table_name.lower()} ("
                f"{config.primary_key_column.lower()} TEXT PRIMARY KEY, "
                f"user_id TEXT NOT NULL, project_id TEXT NOT NULL, session_id TEXT, "
                f"effective_role TEXT NOT NULL, content TEXT, "
                f"metadata JSONB DEFAULT '{{}}'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW())"
            )
        else:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Unsupported memory backend: {config.backend_type}",
                http_status=422,
            )

        if not confirmed:
            return {"status": "DRY_RUN_PREVIEW", "ddl": ddl, "store_name": config.name}

        self.executed_ddl.append(ddl)
        self.stores[config.name] = config
        self.records.setdefault(config.name, [])
        self.record_audit(
            {
                "event_type": "PROVISION_VECTOR_STORE",
                "effective_role": role,
                "store_name": config.name,
                "backend_type": config.backend_type.value,
                "table_fqn": config.table_fqn,
                "ddl": ddl,
                "status": "SUCCESS",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        return {"status": "CREATED", "ddl": ddl, "store_name": config.name, "backend_type": config.backend_type.value}

    def register_existing_store(self, config: VectorStoreConfig) -> None:
        """Register an already-existing physical vector store without requiring CREATE privileges."""
        self.stores[config.name] = config
        self.records.setdefault(config.name, [])


class CreateMemoryDialogService:
    """Implements the Langflow `Create Memory` dialog contract (`REQ-10`, `[G-A4]`, `HUB-W2-05`).

    Preserves:
    - Controls: `Name`, `Embedding Model`, `Vector Database` / `Memory Backend`, `Batch Size`,
      `LLM Preprocessing` (optional), `Cancel` / `Create Memory`
    - States: `EMPTY`, `LOADING`, `READY`, `ERROR`, `CANCELLED`, `CREATED`
    - Separates physical resource creation (`create_physical_resource=True`) from
      binding to an existing store (`create_physical_resource=False`, `[G-A4]`).
    """

    def __init__(self, engine: InMemoryVectorStoreEngine) -> None:
        self.engine = engine
        self.state: DialogState = DialogState.EMPTY
        self.last_error: dict[str, Any] | None = None

    def cancel(self) -> dict[str, Any]:
        self.state = DialogState.CANCELLED
        return {"state": self.state.value}

    def create_memory(
        self,
        *,
        name: str,
        embedding_model: EmbeddingModelSpec | str | None = None,
        vector_database: VectorBackendType | str = VectorBackendType.STANDARD_VECTOR,
        database: str,
        schema: str,
        table_name: str,
        batch_size: int = 64,
        llm_preprocessing: LLMPreprocessingConfig | None = None,
        create_physical_resource: bool = True,
        cortex_search_service_name: str | None = None,
        cortex_embedding_mode: CortexSearchEmbeddingMode = CortexSearchEmbeddingMode.MANAGED,
        postgres_instance: str | None = None,
        postgres_role: str | None = None,
        vector_element_type: str = "FLOAT",
        vector_dimension_override: int | None = None,
        vector_inside_variant: bool = False,
        primary_key_column: str = "RECORD_ID",
        secondary_index_columns: tuple[str, ...] = ("PROJECT_ID", "SESSION_ID"),
        vector_column: str = "EMBEDDING_VEC",
        warehouse: str | None = None,
        target_lag: str = "1 minute",
        agent_memory_type: str = "episodic_memory",
        enable_embedding: bool | None = None,
        embedding_provider: str = "snowflake_cortex",
        custom_dimension: int | None = None,
        endpoint_url: str | None = None,
    ) -> dict[str, Any]:
        self.state = DialogState.LOADING
        try:
            if not (name or "").strip():
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    "Memory 'Name' cannot be empty.",
                    http_status=400,
                )
            backend_enum = normalize_memory_backend_type(vector_database)
            if enable_embedding is False and backend_enum in VECTOR_MEMORY_BACKENDS:
                backend_enum = (
                    VectorBackendType.HYBRID_RELATIONAL
                    if backend_enum == VectorBackendType.HYBRID_VECTOR
                    else VectorBackendType.STANDARD_RELATIONAL
                )

            prov_clean = (embedding_provider or "snowflake_cortex").strip().lower()
            if isinstance(embedding_model, EmbeddingModelSpec):
                resolved_embed = embedding_model
            elif prov_clean in ("custom_endpoint", "spcs_model_registry", "spcs_llm_proxy"):
                dim_val = int(custom_dimension or vector_dimension_override or 768)
                provider_label = {
                    "spcs_model_registry": "Snowflake Model Registry SPCS",
                    "spcs_llm_proxy": "Snowflake Standard Proxy SPCS",
                }.get(prov_clean, "Custom Embedding Endpoint")
                default_mid = {
                    "spcs_model_registry": "model-registry-spcs-embedding",
                    "spcs_llm_proxy": "proxy-chat-embedding",
                }.get(prov_clean, "custom-embedding-endpoint")
                resolved_embed = EmbeddingModelSpec(
                    model_id=str(embedding_model or default_mid).strip(),
                    provider=provider_label,
                    dimension=dim_val,
                    version="1.0",
                )
            elif isinstance(embedding_model, str) and embedding_model.strip():
                clean_model = embedding_model.strip().split(" ")[0].replace("ollama/", "")
                catalog_lookup = (
                    EXTERNAL_EMBEDDING_MODELS_CATALOG
                    if prov_clean not in ("", "snowflake_cortex")
                    else DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG
                )
                if clean_model not in catalog_lookup and clean_model in EXTERNAL_EMBEDDING_MODELS_CATALOG:
                    catalog_lookup = EXTERNAL_EMBEDDING_MODELS_CATALOG
                if clean_model not in catalog_lookup:
                    raise SnowflakeComponentError(
                        ErrorCode.UNSUPPORTED_CAPABILITY,
                        f"Unsupported embedding model '{embedding_model}'. Expected one of {sorted(DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG)}.",
                        http_status=422,
                    )
                resolved_embed = catalog_lookup[clean_model]
            else:
                resolved_embed = DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-m-v1.5"]

            preproc = llm_preprocessing or LLMPreprocessingConfig(enabled=False)
            config = VectorStoreConfig(
                name=name.strip(),
                backend_type=backend_enum,
                database=database,
                schema=schema,
                table_name=table_name,
                embedding_spec=resolved_embed,
                batch_size=batch_size,
                llm_preprocessing=preproc,
                primary_key_column=primary_key_column,
                secondary_index_columns=secondary_index_columns,
                vector_column=vector_column,
                vector_element_type=vector_element_type,
                vector_dimension_override=vector_dimension_override,
                vector_inside_variant=vector_inside_variant,
                warehouse=warehouse,
                target_lag=target_lag,
                cortex_search_service_name=cortex_search_service_name,
                cortex_embedding_mode=cortex_embedding_mode,
                postgres_instance=postgres_instance,
                postgres_role=postgres_role,
            )

            if create_physical_resource:
                res = self.engine.provision_physical_store(config, confirmed=True)
            else:
                # Using existing physical store — verify read/use permission ONLY, never CREATE!
                self._verify_existing_store_access(config)
                self.engine.register_existing_store(config)
                res = {
                    "status": "BOUND_EXISTING",
                    "store_name": config.name,
                    "backend_type": config.backend_type.value,
                }

            canonical_type = resolve_canonical_memory_type(agent_memory_type)
            type_spec = CANONICAL_MEMORY_TYPES_BY_ID[canonical_type]
            effective_enable_embedding = (
                bool(enable_embedding) if enable_embedding is not None else config.is_vector_memory
            )

            self.state = DialogState.CREATED
            self.last_error = None
            return {
                "state": self.state.value,
                "memory_name": config.name,
                "agent_memory_type": agent_memory_type,
                "canonical_memory_type": canonical_type,
                "recommended_snowflake_objects": list(type_spec["recommended_snowflake_objects"]),
                "needs_embedding_badge": type_spec["needs_embedding_badge"],
                "how_to_feed_without_embedding": type_spec["how_to_feed_without_embedding"].replace(
                    "<fqn>", config.table_fqn
                ),
                "when_embedding_useful": type_spec["when_embedding_useful"],
                "required_inputs": list(type_spec["required_inputs"]),
                "backend_type": config.backend_type.value,
                "enable_embedding": effective_enable_embedding,
                "is_vector_memory": config.is_vector_memory,
                "embedding_provider": prov_clean if effective_enable_embedding else "none",
                "embedding_model": config.embedding_spec.model_id if config.is_vector_memory else None,
                "dimension": config.embedding_spec.dimension if config.is_vector_memory else None,
                "endpoint_url": endpoint_url if effective_enable_embedding else None,
                "space_fingerprint": config.embedding_spec.space_fingerprint,
                "batch_size": config.batch_size,
                "llm_preprocessing_enabled": preproc.enabled,
                "provisioning": res,
            }
        except SnowflakeComponentError as exc:
            self.state = DialogState.ERROR
            self.last_error = exc.to_dict()
            raise

    def _verify_existing_store_access(self, config: VectorStoreConfig) -> None:
        role = self.engine.active_role
        if config.backend_type in (
            VectorBackendType.STANDARD_VECTOR,
            VectorBackendType.HYBRID_VECTOR,
            VectorBackendType.INTERACTIVE_VECTOR,
            VectorBackendType.DYNAMIC_VECTOR,
            VectorBackendType.STANDARD_RELATIONAL,
            VectorBackendType.HYBRID_RELATIONAL,
            VectorBackendType.INTERACTIVE_RELATIONAL,
            VectorBackendType.DYNAMIC_RELATIONAL,
            VectorBackendType.SEMI_STRUCTURED_VARIANT,
            VectorBackendType.SEMI_STRUCTURED_OBJECT,
            VectorBackendType.SEMI_STRUCTURED_ARRAY,
        ):
            if not (
                self.engine.has_sf_privilege(role, "SELECT", config.table_fqn)
                or self.engine.has_sf_privilege(role, "INSERT", config.table_fqn)
            ):
                raise_role_denied(role, "USE EXISTING VECTOR TABLE (SELECT/INSERT)", config.table_fqn, self.engine.record_audit)
        elif config.backend_type == VectorBackendType.CORTEX_SEARCH:
            svc_fqn = config.search_service_fqn or config.table_fqn
            if not (
                self.engine.has_sf_privilege(role, "USAGE", svc_fqn)
                or self.engine.has_sf_privilege(role, "SELECT", config.table_fqn)
            ):
                raise_role_denied(role, "USAGE CORTEX SEARCH SERVICE", svc_fqn, self.engine.record_audit)
        elif config.backend_type in (
            VectorBackendType.POSTGRES_PGVECTOR,
            VectorBackendType.POSTGRES_RELATIONAL,
            VectorBackendType.POSTGRES_JSONB,
        ):
            if not self.engine.has_sf_privilege(role, "USAGE", config.postgres_instance or ""):
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", config.postgres_instance or "", self.engine.record_audit)
            pg_role = config.postgres_role or ""
            if not (
                self.engine.has_pg_privilege(pg_role, "SELECT", config.table_fqn)
                or self.engine.has_pg_privilege(pg_role, "INSERT", config.table_fqn)
            ):
                raise_role_denied(
                    f"{role}/pg:{pg_role}",
                    "POSTGRES SELECT/INSERT",
                    config.table_fqn,
                    self.engine.record_audit,
                )


class SnowflakeKnowledgeComponent:
    """Langflow `Knowledge` (`Ingest` / `Retrieve`) component for Snowflake vector backends (`REQ-10`)."""

    display_name = "Knowledge"
    category = "Files & Knowledge"
    ports = ("input", "search_query", "results")

    def __init__(self, engine: InMemoryVectorStoreEngine, store_name: str) -> None:
        self.engine = engine
        self.store_name = store_name

    def _get_store(self) -> VectorStoreConfig:
        store = self.engine.stores.get(self.store_name)
        if store is None:
            raise SnowflakeComponentError(
                ErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Knowledge/Memory store '{self.store_name}' not found.",
                http_status=404,
            )
        return store

    def ingest(
        self,
        records: list[dict[str, Any]],
        *,
        user_id: str,
        project_id: str,
        session_id: str,
        active_embedding_spec: EmbeddingModelSpec | None = None,
        content_column: str = "content",
        id_column: str = "id",
    ) -> dict[str, Any]:
        store = self._get_store()
        role = self.engine.active_role
        spec = active_embedding_spec or store.embedding_spec

        # Guard against incompatible embedding model / vector space reuse
        if spec.space_fingerprint != store.embedding_spec.space_fingerprint:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Embedding space mismatch: store '{store.name}' requires "
                f"{store.embedding_spec.model_id} (dim={store.embedding_spec.dimension}, fp={store.embedding_spec.space_fingerprint}), "
                f"got {spec.model_id} (dim={spec.dimension}, fp={spec.space_fingerprint}). "
                "Silent reuse of incompatible vectors is forbidden.",
                http_status=409,
                role=role,
            )

        # Verify INSERT permission on the underlying CRUD table (NOT CREATE TABLE!)
        if store.backend_type in (
            VectorBackendType.STANDARD_VECTOR,
            VectorBackendType.HYBRID_VECTOR,
            VectorBackendType.INTERACTIVE_VECTOR,
            VectorBackendType.DYNAMIC_VECTOR,
            VectorBackendType.CORTEX_SEARCH,
            VectorBackendType.STANDARD_RELATIONAL,
            VectorBackendType.HYBRID_RELATIONAL,
            VectorBackendType.INTERACTIVE_RELATIONAL,
            VectorBackendType.DYNAMIC_RELATIONAL,
            VectorBackendType.SEMI_STRUCTURED_VARIANT,
            VectorBackendType.SEMI_STRUCTURED_OBJECT,
            VectorBackendType.SEMI_STRUCTURED_ARRAY,
        ):
            if not self.engine.has_sf_privilege(role, "INSERT", store.table_fqn):
                raise_role_denied(role, "INSERT INTO VECTOR SOURCE TABLE", store.table_fqn, self.engine.record_audit)
        elif store.backend_type in (
            VectorBackendType.POSTGRES_PGVECTOR,
            VectorBackendType.POSTGRES_RELATIONAL,
            VectorBackendType.POSTGRES_JSONB,
        ):
            if not self.engine.has_sf_privilege(role, "USAGE", store.postgres_instance or ""):
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", store.postgres_instance or "", self.engine.record_audit)
            pg_role = store.postgres_role or ""
            if not self.engine.has_pg_privilege(pg_role, "INSERT", store.table_fqn):
                raise_role_denied(f"{role}/pg:{pg_role}", "POSTGRES INSERT", store.table_fqn, self.engine.record_audit)

        stored_rows = self.engine.records.setdefault(store.name, [])
        ingested_count = 0
        skipped_count = 0
        batches_processed = 0
        kill_phrase = (store.llm_preprocessing.kill_phrase or "NO_INGEST").strip().upper()

        for i in range(0, len(records), store.batch_size):
            batch = records[i : i + store.batch_size]
            batches_processed += 1
            for row in batch:
                raw_text = str(row.get(content_column, "")).strip()
                if not raw_text:
                    continue
                processed_text = raw_text
                if store.llm_preprocessing.enabled:
                    self.engine.generative_preprocessing_calls += 1
                    if self.engine._preprocessor_fn is not None:
                        processed_text = self.engine._preprocessor_fn(raw_text, store.llm_preprocessing)
                    else:
                        processed_text = f"[PREPROCESSED:{store.llm_preprocessing.model_id}] {raw_text}"
                    cleaned_proc = (processed_text or "").strip().upper()
                    if cleaned_proc in (kill_phrase, "NO_INGEST", "SKIP", f"[{kill_phrase}]", "[NO_INGEST]"):
                        skipped_count += 1
                        continue

                # Avoid duplicate vector computation when Cortex Search manages embeddings natively or store is non-vector
                if not store.is_vector_memory or (
                    store.backend_type == VectorBackendType.CORTEX_SEARCH
                    and store.cortex_embedding_mode == CortexSearchEmbeddingMode.MANAGED
                ):
                    vec = None
                else:
                    self.engine.embedding_calls += 1
                    vec = deterministic_embed_text(processed_text, spec)

                rec_id = str(
                    row.get(id_column)
                    or hashlib.sha256(f"{user_id}:{project_id}:{session_id}:{processed_text}".encode("utf-8")).hexdigest()[:16]
                )
                entry = {
                    "record_id": rec_id,
                    "user_id": user_id,
                    "project_id": project_id,
                    "session_id": session_id,
                    "effective_role": role,
                    "content": processed_text,
                    "metadata": {k: v for k, v in row.items() if k not in (content_column, id_column)},
                    "space_fingerprint": spec.space_fingerprint,
                    "vector": vec,
                    "search_availability": (
                        "INDEXING_REFRESH_REQUIRED"
                        if store.backend_type == VectorBackendType.CORTEX_SEARCH
                        else "IMMEDIATE"
                    ),
                }
                # Upsert deduplication by record_id within same user/project
                existing_idx = next(
                    (
                        idx
                        for idx, r in enumerate(stored_rows)
                        if r["record_id"] == rec_id and r["user_id"] == user_id and r["project_id"] == project_id
                    ),
                    None,
                )
                if existing_idx is not None:
                    stored_rows[existing_idx] = entry
                else:
                    stored_rows.append(entry)
                ingested_count += 1

        return {
            "store_name": store.name,
            "backend_type": store.backend_type.value,
            "ingested_count": ingested_count,
            "skipped_count": skipped_count,
            "batches_processed": batches_processed,
            "llm_preprocessing_used": store.llm_preprocessing.enabled,
            "kill_phrase": kill_phrase if store.llm_preprocessing.enabled else None,
            "availability_status": (
                "SOURCE_TABLE_COMMITTED_INDEX_REFRESH_PENDING"
                if store.backend_type == VectorBackendType.CORTEX_SEARCH
                else "SEARCH_READY"
            ),
            "effective_role": role,
        }

    def retrieve(
        self,
        search_query: str,
        *,
        user_id: str,
        project_id: str,
        session_id: str | None = None,
        filter_by_session: bool = False,
        include_metadata: bool = True,
        top_k: int = 5,
        active_embedding_spec: EmbeddingModelSpec | None = None,
        use_search_preview: bool = False,
    ) -> dict[str, Any]:
        store = self._get_store()
        role = self.engine.active_role
        spec = active_embedding_spec or store.embedding_spec

        if use_search_preview:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                "SEARCH_PREVIEW is for administrative inspection only and cannot be used for end-user serving.",
                http_status=422,
                role=role,
            )

        if spec.space_fingerprint != store.embedding_spec.space_fingerprint:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Embedding space mismatch on retrieval: store '{store.name}' uses fingerprint "
                f"{store.embedding_spec.space_fingerprint}, query model uses {spec.space_fingerprint}.",
                http_status=409,
                role=role,
            )

        # Verify read/search privilege (NOT CREATE!)
        if store.backend_type in (
            VectorBackendType.STANDARD_VECTOR,
            VectorBackendType.HYBRID_VECTOR,
            VectorBackendType.INTERACTIVE_VECTOR,
            VectorBackendType.DYNAMIC_VECTOR,
            VectorBackendType.STANDARD_RELATIONAL,
            VectorBackendType.HYBRID_RELATIONAL,
            VectorBackendType.INTERACTIVE_RELATIONAL,
            VectorBackendType.DYNAMIC_RELATIONAL,
            VectorBackendType.SEMI_STRUCTURED_VARIANT,
            VectorBackendType.SEMI_STRUCTURED_OBJECT,
            VectorBackendType.SEMI_STRUCTURED_ARRAY,
        ):
            if not self.engine.has_sf_privilege(role, "SELECT", store.table_fqn):
                raise_role_denied(role, "SELECT VECTOR TABLE", store.table_fqn, self.engine.record_audit)
        elif store.backend_type == VectorBackendType.CORTEX_SEARCH:
            svc_fqn = store.search_service_fqn or store.table_fqn
            if not self.engine.has_sf_privilege(role, "USAGE", svc_fqn):
                raise_role_denied(role, "USAGE CORTEX SEARCH SERVICE", svc_fqn, self.engine.record_audit)
        elif store.backend_type in (
            VectorBackendType.POSTGRES_PGVECTOR,
            VectorBackendType.POSTGRES_RELATIONAL,
            VectorBackendType.POSTGRES_JSONB,
        ):
            if not self.engine.has_sf_privilege(role, "USAGE", store.postgres_instance or ""):
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", store.postgres_instance or "", self.engine.record_audit)
            pg_role = store.postgres_role or ""
            if not self.engine.has_pg_privilege(pg_role, "SELECT", store.table_fqn):
                raise_role_denied(f"{role}/pg:{pg_role}", "POSTGRES SELECT", store.table_fqn, self.engine.record_audit)

        # Mandatory isolation filter: even when filter_by_session=False (cross-session Memory Base),
        # results are strictly isolated by (user_id, project_id, effective_role) — crucial for
        # Cortex Search which executes with owner's rights!
        applied_isolation_filter = {
            "user_id": user_id,
            "project_id": project_id,
            "effective_role": role,
        }
        if filter_by_session:
            applied_isolation_filter["session_id"] = session_id

        candidates = [
            r
            for r in self.engine.records.get(store.name, [])
            if r["user_id"] == user_id
            and r["project_id"] == project_id
            and r["effective_role"] == role
            and (not filter_by_session or r["session_id"] == session_id)
        ]

        q_lower = (search_query or "").lower()
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in candidates:
            text_lower = row["content"].lower()
            overlap = sum(1.0 for tok in q_lower.split() if tok in text_lower)
            score = overlap + (0.5 if q_lower and q_lower in text_lower else 0.0)
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)

        results = []
        for score, row in scored[:top_k]:
            item: dict[str, Any] = {
                "record_id": row["record_id"],
                "content": row["content"],
                "session_id": row["session_id"],
                "score": score,
            }
            if include_metadata:
                item["metadata"] = row["metadata"]
            results.append(item)

        return {
            "store_name": store.name,
            "backend_type": store.backend_type.value,
            "owner_rights_filter_enforced": store.backend_type == VectorBackendType.CORTEX_SEARCH,
            "isolation_filter": applied_isolation_filter,
            "filter_by_session": filter_by_session,
            "results": results,
            "effective_role": role,
        }


DYNAMIC_MEMORY_BASE_TYPE_SPECS: dict[str, dict[str, Any]] = {
    "graph_state_checkpointer": {
        "id": "graph_state_checkpointer",
        "order": 1,
        "label": "1. Short-Term: Graph State & Checkpointer",
        "filter_by_session": True,
        "tool_mode_default": False,
        "tool_mode_supported": False,
        "visible_fields": [
            "memory_base",
            "thread_id",
            "checkpoint_ns",
            "ttl_hours",
            "filter_by_session",
        ],
        "hidden_fields": [
            "session_id",
            "window_k",
            "summarizer_model",
            "max_summary_tokens",
            "user_id",
            "entity_namespace",
            "topic",
            "outcome_score_threshold",
            "few_shot_k",
            "tool_mode",
            "search_query",
        ],
        "primary_output_port": "checkpoint_state",
        "primary_output_display_name": "Checkpoint State",
        "output_ports": ("checkpoint_state", "results"),
    },
    "buffer_window_memory": {
        "id": "buffer_window_memory",
        "order": 2,
        "label": "2. Short-Term: Buffer / Window Memory",
        "filter_by_session": True,
        "tool_mode_default": False,
        "tool_mode_supported": False,
        "visible_fields": [
            "memory_base",
            "session_id",
            "window_k",
            "filter_by_session",
        ],
        "hidden_fields": [
            "thread_id",
            "checkpoint_ns",
            "ttl_hours",
            "summarizer_model",
            "max_summary_tokens",
            "user_id",
            "entity_namespace",
            "topic",
            "outcome_score_threshold",
            "few_shot_k",
            "tool_mode",
            "search_query",
        ],
        "primary_output_port": "messages",
        "primary_output_display_name": "Messages / Chat History",
        "output_ports": ("messages", "chat_history", "results"),
    },
    "summary_memory": {
        "id": "summary_memory",
        "order": 3,
        "label": "3. Short-Term: Summary Memory",
        "filter_by_session": True,
        "tool_mode_default": False,
        "tool_mode_supported": False,
        "visible_fields": [
            "memory_base",
            "session_id",
            "summarizer_model",
            "max_summary_tokens",
            "filter_by_session",
        ],
        "hidden_fields": [
            "thread_id",
            "checkpoint_ns",
            "ttl_hours",
            "window_k",
            "user_id",
            "entity_namespace",
            "topic",
            "outcome_score_threshold",
            "few_shot_k",
            "tool_mode",
            "search_query",
        ],
        "primary_output_port": "running_summary",
        "primary_output_display_name": "Running Summary",
        "output_ports": ("running_summary", "results"),
    },
    "user_profile_entity_memory": {
        "id": "user_profile_entity_memory",
        "order": 4,
        "label": "4. Long-Term Semantic: User Profile & Entity Memory",
        "filter_by_session": False,
        "tool_mode_default": False,
        "tool_mode_supported": True,
        "visible_fields": [
            "memory_base",
            "user_id",
            "entity_namespace",
            "search_query",
            "filter_by_session",
            "tool_mode",
        ],
        "hidden_fields": [
            "thread_id",
            "checkpoint_ns",
            "ttl_hours",
            "session_id",
            "window_k",
            "summarizer_model",
            "max_summary_tokens",
            "topic",
            "outcome_score_threshold",
            "few_shot_k",
        ],
        "primary_output_port": "profile_context",
        "primary_output_display_name": "Profile Context",
        "output_ports": ("profile_context", "tool", "results"),
    },
    "episodic_memory": {
        "id": "episodic_memory",
        "order": 5,
        "label": "5. Long-Term Semantic: Episodic Memory",
        "filter_by_session": False,
        "tool_mode_default": True,
        "tool_mode_supported": True,
        "visible_fields": [
            "memory_base",
            "topic",
            "outcome_score_threshold",
            "few_shot_k",
            "search_query",
            "filter_by_session",
            "tool_mode",
        ],
        "hidden_fields": [
            "thread_id",
            "checkpoint_ns",
            "ttl_hours",
            "session_id",
            "window_k",
            "summarizer_model",
            "max_summary_tokens",
            "user_id",
            "entity_namespace",
        ],
        "primary_output_port": "few_shot_context",
        "primary_output_display_name": "Few-Shot Context",
        "output_ports": ("tool", "few_shot_context", "results"),
    },
}


class SnowflakeMemoryBaseComponent:
    """Langflow `Memory Base` long-term and short-term memory component (`REQ-10`, `[G-A4]`, `W2-R4-06`, `W2-R4-06B`).

    Preserves `Memory Base` selector, `Search Query`, `Filter by Session`, `Results`, and `Tool Mode`,
    while dynamically adapting inputs and outputs to all 5 canonical Snowflake Memory types (`#1..#5`):
    - `#1 graph_state_checkpointer`: `thread_id`, `checkpoint_ns`, `ttl_hours`, `checkpoint_state`
    - `#2 buffer_window_memory`: `session_id`, `window_k=8`, `filter_by_session=True`, `messages` (`Chat History`)
    - `#3 summary_memory`: `session_id`, `summarizer_model`, `max_summary_tokens`, `running_summary`
    - `#4 user_profile_entity_memory`: `user_id`, `entity_namespace`, `filter_by_session=False`, `profile_context` + `tool`
    - `#5 episodic_memory`: `topic`, `outcome_score_threshold`, `few_shot_k`, `tool_mode=True`, `tool` + `few_shot_context`
    Enforces that `filter_by_session=False` allows cross-session search ONLY within the
    caller's authorized `(user_id, project_id, effective_role)` scope.
    """

    display_name = "Memory Base"
    category = "Files & Knowledge"
    ports = (
        "search_query",
        "results",
        "tool",
        "checkpoint_state",
        "messages",
        "running_summary",
        "profile_context",
        "few_shot_context",
    )

    def __init__(
        self,
        engine: InMemoryVectorStoreEngine | None = None,
        memory_name: str = "SNOWFLAKE_KB_MEM",
        *,
        filter_by_session: bool | None = None,
        tool_mode: bool | None = None,
        canonical_memory_type: str | None = None,
        thread_id: str = "default",
        checkpoint_ns: str = "default",
        ttl_hours: int = 24,
        session_id: str = "default",
        window_k: int = 8,
        summarizer_model: str = "openai-gpt-6-luna",
        max_summary_tokens: int = 512,
        user_id: str = "default",
        entity_namespace: str = "default",
        topic: str = "general",
        outcome_score_threshold: float = 0.7,
        few_shot_k: int = 3,
    ) -> None:
        if isinstance(engine, str) and memory_name == "SNOWFLAKE_KB_MEM":
            memory_name = engine
            engine = None
        self.engine = engine
        self.memory_name = memory_name
        self.knowledge = SnowflakeKnowledgeComponent(engine, memory_name) if engine is not None else None

        catalog_entry = self._resolve_catalog_entry(memory_name)
        resolved_type = canonical_memory_type or (
            str(catalog_entry.get("canonical_memory_type") or catalog_entry.get("agent_memory_type") or "")
            if catalog_entry
            else ""
        )
        self.canonical_memory_type = resolve_canonical_memory_type(resolved_type or "episodic_memory")
        spec = DYNAMIC_MEMORY_BASE_TYPE_SPECS[self.canonical_memory_type]

        raw_backend = str(
            (catalog_entry or {}).get("storage_family")
            or (catalog_entry or {}).get("vector_database")
            or (catalog_entry or {}).get("storage_backend")
            or "Standard Table"
        )
        raw_fqn = str((catalog_entry or {}).get("table_fqn") or f"{LIVE_BRIDGE.database}.{LIVE_BRIDGE.schema}.{memory_name}")
        self.storage_family = (
            "snowflake_postgres"
            if ("postgres" in raw_backend.lower() or ":5432" in raw_fqn)
            else "snowflake_sql"
        )
        self.table_fqn = raw_fqn
        self.enable_embedding = bool((catalog_entry or {}).get("enable_embedding", False))

        if filter_by_session is not None:
            self.filter_by_session = bool(filter_by_session)
        elif catalog_entry is not None and "filter_by_session" in catalog_entry:
            self.filter_by_session = bool(catalog_entry["filter_by_session"])
        elif engine is not None and canonical_memory_type is None and catalog_entry is None:
            self.filter_by_session = True
        else:
            self.filter_by_session = bool(spec["filter_by_session"])

        if tool_mode is not None:
            self.tool_mode = bool(tool_mode)
        elif catalog_entry is not None and "tool_mode" in catalog_entry:
            self.tool_mode = bool(catalog_entry["tool_mode"])
        elif engine is not None and canonical_memory_type is None and catalog_entry is None:
            self.tool_mode = False
        else:
            self.tool_mode = bool(spec["tool_mode_default"])

        self.thread_id = str((catalog_entry or {}).get("thread_id") or thread_id)
        self.checkpoint_ns = str((catalog_entry or {}).get("checkpoint_ns") or checkpoint_ns)
        self.ttl_hours = int((catalog_entry or {}).get("ttl_hours") or ttl_hours)
        self.session_id = str((catalog_entry or {}).get("session_id") or session_id)
        self.window_k = int((catalog_entry or {}).get("window_k") or window_k)
        self.summarizer_model = str((catalog_entry or {}).get("summarizer_model") or summarizer_model)
        self.max_summary_tokens = int((catalog_entry or {}).get("max_summary_tokens") or max_summary_tokens)
        self.user_id = str((catalog_entry or {}).get("user_id") or user_id)
        self.entity_namespace = str((catalog_entry or {}).get("entity_namespace") or entity_namespace)
        self.topic = str((catalog_entry or {}).get("topic") or topic)
        self.outcome_score_threshold = float(
            (catalog_entry or {}).get("outcome_score_threshold")
            if (catalog_entry or {}).get("outcome_score_threshold") is not None
            else outcome_score_threshold
        )
        self.few_shot_k = int((catalog_entry or {}).get("few_shot_k") or few_shot_k)

    @staticmethod
    def _resolve_catalog_entry(memory_name: str | None) -> dict[str, Any] | None:
        clean = str(memory_name or "").split(" (")[0].strip()
        if not clean or clean == "None":
            return None
        for entry in getattr(LIVE_BRIDGE, "memories_catalog", []):
            if clean.upper() in {
                str(entry.get("name") or "").strip().upper(),
                str(entry.get("table_fqn") or "").strip().upper(),
            }:
                return dict(entry)
        return None

    def get_dynamic_port_and_field_spec(
        self,
        memory_name: str | None = None,
        *,
        override_canonical_type: str | None = None,
    ) -> dict[str, Any]:
        """Return dynamic field visibility and output port specification for the selected memory (`W2-R4-06`, `W2-R4-06B`)."""
        target_name = memory_name or self.memory_name
        entry = self._resolve_catalog_entry(target_name)
        if override_canonical_type:
            canonical_type = resolve_canonical_memory_type(override_canonical_type)
            raw_backend = str(
                (entry or {}).get("storage_family")
                or (entry or {}).get("vector_database")
                or (entry or {}).get("storage_backend")
                or self.storage_family
            )
            raw_fqn = str((entry or {}).get("table_fqn") or self.table_fqn)
            storage_family = (
                "snowflake_postgres"
                if ("postgres" in raw_backend.lower() or ":5432" in raw_fqn)
                else "snowflake_sql"
            )
            enable_embed = bool((entry or {}).get("enable_embedding", self.enable_embedding))
        elif entry is not None:
            canonical_type = resolve_canonical_memory_type(
                str(entry.get("canonical_memory_type") or entry.get("agent_memory_type") or self.canonical_memory_type)
            )
            raw_backend = str(
                entry.get("storage_family") or entry.get("vector_database") or entry.get("storage_backend") or ""
            )
            raw_fqn = str(entry.get("table_fqn") or self.table_fqn)
            storage_family = (
                "snowflake_postgres"
                if ("postgres" in raw_backend.lower() or ":5432" in raw_fqn)
                else "snowflake_sql"
            )
            enable_embed = bool(entry.get("enable_embedding", False))
        else:
            canonical_type = self.canonical_memory_type
            storage_family = self.storage_family
            raw_fqn = self.table_fqn
            enable_embed = self.enable_embedding

        spec = dict(DYNAMIC_MEMORY_BASE_TYPE_SPECS[canonical_type])
        effective_tool_mode = bool(self.tool_mode if memory_name is None else spec["tool_mode_default"])
        active_ports = list(spec["output_ports"])
        if canonical_type == "user_profile_entity_memory" and not effective_tool_mode and "tool" in active_ports:
            active_ports.remove("tool")
        return {
            "memory_name": str(target_name or "").split(" (")[0].strip(),
            "canonical_memory_type": canonical_type,
            "order": spec["order"],
            "label": spec["label"],
            "storage_family": storage_family,
            "table_fqn": raw_fqn,
            "enable_embedding": enable_embed,
            "filter_by_session": bool(spec["filter_by_session"]),
            "tool_mode": effective_tool_mode,
            "tool_mode_supported": bool(spec["tool_mode_supported"]),
            "visible_fields": list(spec["visible_fields"]),
            "hidden_fields": list(spec["hidden_fields"]),
            "primary_output_port": spec["primary_output_port"],
            "primary_output_display_name": spec["primary_output_display_name"],
            "output_ports": tuple(active_ports),
        }

    def update_build_config(
        self,
        build_config: dict[str, Any],
        field_value: Any = None,
        field_name: str | None = None,
    ) -> dict[str, Any]:
        """Dynamically adapt `Memory Base` node fields and defaults when a memory `#1..#5` is selected (`W2-R4-06B`)."""
        mem_opts = LIVE_BRIDGE.get_memory_catalog_options(include_none=False)
        for key in ("memory_base", "memory_name", "memory_catalog_ref"):
            if key in build_config and isinstance(build_config[key], dict):
                build_config[key]["options"] = mem_opts
                if field_name == key and isinstance(field_value, str) and field_value.strip():
                    build_config[key]["value"] = field_value

        selected_mem = (
            str(field_value)
            if field_name in ("memory_base", "memory_name", "memory_catalog_ref") and isinstance(field_value, str)
            else str(
                (build_config.get("memory_base") or {}).get("value")
                or (build_config.get("memory_name") or {}).get("value")
                or self.memory_name
            )
        )
        self.memory_name = selected_mem.split(" (")[0].strip() or self.memory_name
        entry = self._resolve_catalog_entry(self.memory_name)
        override_type: str | None = None
        if field_name == "canonical_memory_type" and isinstance(field_value, str) and field_value.strip():
            override_type = resolve_canonical_memory_type(field_value)
            self.canonical_memory_type = override_type
        elif entry is not None:
            self.canonical_memory_type = resolve_canonical_memory_type(
                str(entry.get("canonical_memory_type") or entry.get("agent_memory_type") or "episodic_memory")
            )

        dynamic_spec = self.get_dynamic_port_and_field_spec(
            self.memory_name,
            override_canonical_type=override_type,
        )
        visible_set = set(dynamic_spec["visible_fields"])
        hidden_set = set(dynamic_spec["hidden_fields"])

        for fname in visible_set:
            if fname in build_config and isinstance(build_config[fname], dict):
                build_config[fname]["show"] = True
        for fname in hidden_set:
            if fname in build_config and isinstance(build_config[fname], dict):
                build_config[fname]["show"] = False

        if "filter_by_session" in build_config and isinstance(build_config["filter_by_session"], dict):
            build_config["filter_by_session"]["value"] = dynamic_spec["filter_by_session"]
            self.filter_by_session = dynamic_spec["filter_by_session"]
        if "tool_mode" in build_config and isinstance(build_config["tool_mode"], dict):
            if field_name == "tool_mode" and isinstance(field_value, bool):
                build_config["tool_mode"]["value"] = field_value
                self.tool_mode = field_value
            else:
                build_config["tool_mode"]["value"] = dynamic_spec["tool_mode"]
                self.tool_mode = dynamic_spec["tool_mode"]
        if "canonical_memory_type" in build_config and isinstance(build_config["canonical_memory_type"], dict):
            build_config["canonical_memory_type"]["value"] = dynamic_spec["canonical_memory_type"]
        if "storage_target_summary" in build_config and isinstance(build_config["storage_target_summary"], dict):
            build_config["storage_target_summary"]["value"] = (
                f"{dynamic_spec['label']} -> {dynamic_spec['table_fqn']} ({dynamic_spec['storage_family']})"
            )
        build_config["_dynamic_memory_spec"] = dynamic_spec
        return build_config

    def store_memory(
        self,
        content: str,
        *,
        user_id: str = "default",
        project_id: str = "default",
        session_id: str = "default",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.knowledge is not None:
            row = {"content": content, **(metadata or {})}
            return self.knowledge.ingest(
                [row],
                user_id=user_id,
                project_id=project_id,
                session_id=session_id,
            )
        ingest_fn = getattr(LIVE_BRIDGE, "ingest_memory_record", None)
        if callable(ingest_fn):
            return ingest_fn(
                self.memory_name,
                content=content,
                user_id=user_id,
                project_id=project_id,
                session_id=session_id,
                metadata=metadata or {},
            )
        return {
            "ok": True,
            "store_name": self.memory_name,
            "canonical_memory_type": self.canonical_memory_type,
            "table_fqn": self.table_fqn,
            "ingested_count": 1,
            "effective_role": LIVE_BRIDGE.active_role,
        }

    def search_memory(
        self,
        search_query: str = "",
        *,
        user_id: str = "default",
        project_id: str = "default",
        session_id: str | None = None,
        thread_id: str | None = None,
        entity_namespace: str | None = None,
        topic: str | None = None,
        top_k: int = 5,
    ) -> dict[str, Any]:
        if self.filter_by_session and not session_id:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "session_id is required when filter_by_session=True.",
                http_status=400,
            )
        if self.knowledge is not None and self.engine is not None and self.memory_name in self.engine.stores:
            return self.knowledge.retrieve(
                search_query,
                user_id=user_id,
                project_id=project_id,
                session_id=session_id,
                filter_by_session=self.filter_by_session,
                include_metadata=True,
                top_k=top_k,
            )
        recalled = LIVE_BRIDGE.recall_memory(
            self.memory_name,
            query=search_query,
            session_id=session_id or self.session_id,
            thread_id=thread_id or self.thread_id,
            user_id=user_id or self.user_id,
            entity_namespace=entity_namespace or self.entity_namespace,
            topic=topic or self.topic or search_query,
            limit=self.few_shot_k if self.canonical_memory_type == "episodic_memory" else (self.window_k or top_k),
        )
        recalled["filter_by_session"] = self.filter_by_session
        recalled["isolation_filter"] = {
            "user_id": user_id,
            "project_id": project_id,
            "effective_role": LIVE_BRIDGE.active_role,
            **({"session_id": session_id} if self.filter_by_session else {}),
        }
        recalled["dynamic_spec"] = self.get_dynamic_port_and_field_spec(self.memory_name)
        return recalled

    def build_tool(self) -> dict[str, Any]:
        """Build an Agent-compatible Tool descriptor when `tool_mode=True` (`#4`, `#5`)."""
        spec = self.get_dynamic_port_and_field_spec(self.memory_name)
        return {
            "tool_name": f"snowflake_memory_{self.memory_name.lower()}",
            "memory_name": self.memory_name,
            "canonical_memory_type": self.canonical_memory_type,
            "table_fqn": self.table_fqn,
            "storage_family": self.storage_family,
            "enable_embedding": self.enable_embedding,
            "filter_by_session": self.filter_by_session,
            "tool_mode": True,
            "outcome_score_threshold": self.outcome_score_threshold,
            "few_shot_k": self.few_shot_k,
            "primary_output_port": spec["primary_output_port"],
            "active_role": LIVE_BRIDGE.active_role,
        }


class SnowflakeMemoryManagerComponent:
    """Langflow `SnowflakeMemoryManagerComponent` supporting all 5 LangChain/Langflow Memory Types (`W2-R3-05`, `W2-R4-04`).

    Supports:
    1. `graph_state_checkpointer` (#1 Short-Term: Graph State & Checkpointer)
    2. `buffer_window_memory` (#2 Short-Term: Buffer / Window Memory, alias `episodic_session`)
    3. `summary_memory` (#3 Short-Term: Summary Memory)
    4. `user_profile_entity_memory` (#4 Long-Term Semantic: User Profile & Entity Memory, alias `structured_long_term`)
    5. `episodic_memory` (#5 Long-Term Semantic: Episodic Memory, alias `semantic_vector`)

    Includes non-embedding SQL lookup mode (`enable_embedding=False`, default) and optional
    Snowflake `AI_EMBED` / `AI_MULTI_EMBED` (`snowflake_cortex`), `spcs_model_registry`, `spcs_ollama`,
    `spcs_llm_proxy`, or external embedding providers (`openai`, `cohere`, `huggingface_ollama`, `custom_endpoint`).
    """

    display_name = "Snowflake Memory Manager"
    category = "Memories"
    icon = "Snowflake"
    memory_types = AGENT_MEMORY_TYPES
    legacy_aliases = LEGACY_MEMORY_TYPE_ALIASES
    external_embedding_providers = EXTERNAL_EMBEDDING_PROVIDERS
    embedding_providers_catalog = EMBEDDING_PROVIDERS_CATALOG

    def __init__(
        self,
        *,
        name: str = "SNOWFLAKE_AGENT_MEMORY",
        agent_memory_type: str = "episodic_memory",
        database: str | None = None,
        schema: str | None = None,
        storage_family: str | None = None,
        storage_backend: str | None = None,
        postgres_instance: str | None = None,
        pg_database: str = "postgres",
        pg_schema: str = "public",
        pg_table_mode: str | None = None,
        enable_embedding: bool = False,
        embedding_provider: str = "snowflake_cortex",
        embedding_model: str = "snowflake-arctic-embed-m-v1.5",
        custom_dimension: int | None = None,
        endpoint_url: str | None = None,
        thread_id: str = "default",
        checkpoint_ns: str = "default",
        ttl_hours: int = 24,
        session_id: str = "default",
        window_k: int = 10,
        retention_days: int = 30,
        summarizer_model: str = "mistral-large2",
        max_summary_tokens: int = 512,
        user_id: str = "default",
        entity_namespace: str = "default",
        schema_keys: list[str] | None = None,
        topic_column: str = "topic",
        tool_used_column: str = "tool_used",
        outcome_score_threshold: float = 0.7,
        few_shot_k: int = 3,
        filter_by_session: bool | None = None,
        tool_mode: bool | None = None,
        auto_capture: bool = True,
        ingestion_threshold: int = 1,
        llm_preprocessing: dict[str, Any] | None = None,
        flow_id: str | None = None,
        create_new_physical_store: bool = True,
    ) -> None:
        self.name = name
        self.agent_memory_type = agent_memory_type
        self.canonical_memory_type = resolve_canonical_memory_type(agent_memory_type)
        self.database = (database or LIVE_BRIDGE.database).strip().upper()
        self.schema = (schema or LIVE_BRIDGE.schema).strip().upper()
        type_spec = CANONICAL_MEMORY_TYPES_BY_ID[self.canonical_memory_type]
        self.storage_backend = storage_backend or type_spec["recommended_snowflake_objects"][0]
        self.storage_family = storage_family or (
            "snowflake_postgres" if "postgres" in self.storage_backend.lower() else "snowflake_sql"
        )
        self.postgres_instance = postgres_instance
        self.pg_database = pg_database
        self.pg_schema = pg_schema
        self.pg_table_mode = pg_table_mode
        self.enable_embedding = bool(enable_embedding)
        self.embedding_provider = (embedding_provider or "snowflake_cortex").strip().lower()
        self.embedding_model = embedding_model
        self.custom_dimension = custom_dimension
        self.endpoint_url = endpoint_url
        self.thread_id = thread_id
        self.checkpoint_ns = checkpoint_ns
        self.ttl_hours = ttl_hours
        self.session_id = session_id
        self.window_k = window_k
        self.retention_days = retention_days
        self.summarizer_model = summarizer_model
        self.max_summary_tokens = max_summary_tokens
        self.user_id = user_id
        self.entity_namespace = entity_namespace
        self.schema_keys = schema_keys or ["preferences", "role", "department"]
        self.topic_column = topic_column
        self.tool_used_column = tool_used_column
        self.outcome_score_threshold = outcome_score_threshold
        self.few_shot_k = few_shot_k
        dyn_spec = DYNAMIC_MEMORY_BASE_TYPE_SPECS[self.canonical_memory_type]
        self.filter_by_session = bool(
            filter_by_session if filter_by_session is not None else dyn_spec["filter_by_session"]
        )
        self.tool_mode = bool(tool_mode if tool_mode is not None else dyn_spec["tool_mode_default"])
        self.auto_capture = bool(auto_capture)
        self.ingestion_threshold = int(ingestion_threshold)
        self.llm_preprocessing = dict(
            llm_preprocessing
            or {
                "enabled": self.canonical_memory_type == "summary_memory",
                "engine": "proxy_chat" if self.canonical_memory_type == "summary_memory" else "cortex_sql",
                "model": self.summarizer_model,
                "kill_phrase": "NO_INGEST",
            }
        )
        self.flow_id = flow_id
        self.create_new_physical_store = create_new_physical_store

    def get_type_metadata(self) -> dict[str, Any]:
        return dict(CANONICAL_MEMORY_TYPES_BY_ID[self.canonical_memory_type])

    def provision_or_register(self, *, dry_run: bool = True) -> dict[str, Any]:
        return LIVE_BRIDGE.create_or_register_memory(
            {
                "name": self.name,
                "database": self.database,
                "schema": self.schema,
                "storage_family": self.storage_family,
                "postgres_instance": self.postgres_instance,
                "pg_database": self.pg_database,
                "pg_schema": self.pg_schema,
                "pg_table_mode": self.pg_table_mode,
                "agent_memory_type": self.agent_memory_type,
                "vector_database": self.storage_backend,
                "enable_embedding": self.enable_embedding,
                "embedding_provider": self.embedding_provider,
                "embedding_model": self.embedding_model,
                "custom_dimension": self.custom_dimension,
                "endpoint_url": self.endpoint_url,
                "thread_id": self.thread_id,
                "checkpoint_ns": self.checkpoint_ns,
                "ttl_hours": self.ttl_hours,
                "session_id": self.session_id,
                "window_k": self.window_k,
                "retention_days": self.retention_days,
                "summarizer_model": self.summarizer_model,
                "max_summary_tokens": self.max_summary_tokens,
                "user_id": self.user_id,
                "entity_namespace": self.entity_namespace,
                "schema_keys": self.schema_keys,
                "topic_column": self.topic_column,
                "tool_used_column": self.tool_used_column,
                "outcome_score_threshold": self.outcome_score_threshold,
                "few_shot_k": self.few_shot_k,
                "filter_by_session": self.filter_by_session,
                "tool_mode": self.tool_mode,
                "auto_capture": self.auto_capture,
                "ingestion_threshold": self.ingestion_threshold,
                "llm_preprocessing": self.llm_preprocessing,
                "flow_id": self.flow_id,
                "create_new_physical_store": self.create_new_physical_store,
                "dry_run": dry_run,
            }
        )

    def recall(
        self,
        *,
        query: str = "",
        session_id: str | None = None,
        thread_id: str | None = None,
        user_id: str | None = None,
        entity_namespace: str | None = None,
        topic: str = "",
        limit: int | None = None,
    ) -> dict[str, Any]:
        return LIVE_BRIDGE.recall_memory(
            self.name,
            query=query,
            session_id=session_id or self.session_id,
            thread_id=thread_id or self.thread_id,
            user_id=user_id or self.user_id,
            entity_namespace=entity_namespace or self.entity_namespace,
            topic=topic,
            limit=limit or self.window_k,
        )

