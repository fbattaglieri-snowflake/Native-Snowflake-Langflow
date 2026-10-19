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
    }
)


class VectorBackendType(str, Enum):
    STANDARD_VECTOR = "STANDARD_VECTOR"
    HYBRID_VECTOR = "HYBRID_VECTOR"
    CORTEX_SEARCH = "CORTEX_SEARCH"
    POSTGRES_PGVECTOR = "POSTGRES_PGVECTOR"


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
    "snowflake-arctic-embed-m-v1.5": EmbeddingModelSpec(
        model_id="snowflake-arctic-embed-m-v1.5",
        provider="Snowflake Cortex Embedding",
        dimension=768,
        version="1.5",
    ),
    "nv-embed-qa-4": EmbeddingModelSpec(
        model_id="nv-embed-qa-4",
        provider="Snowflake Cortex Embedding",
        dimension=1024,
        version="4.0",
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
}


@dataclass(frozen=True)
class LLMPreprocessingConfig:
    enabled: bool = False
    provider: str | None = None
    model_id: str | None = None
    instructions: str | None = None

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
    embedding_spec: EmbeddingModelSpec
    batch_size: int = 64
    llm_preprocessing: LLMPreprocessingConfig = field(default_factory=LLMPreprocessingConfig)
    # Hybrid table options
    primary_key_column: str = "RECORD_ID"
    secondary_index_columns: tuple[str, ...] = ("PROJECT_ID", "SESSION_ID")
    vector_column: str = "EMBEDDING_VEC"
    # Cortex Search options
    cortex_search_service_name: str | None = None
    cortex_embedding_mode: CortexSearchEmbeddingMode = CortexSearchEmbeddingMode.MANAGED
    # Postgres pgvector options
    postgres_instance: str | None = None
    postgres_role: str | None = None

    def __post_init__(self) -> None:
        self.database = validate_snowflake_identifier(self.database, "database")
        self.schema = validate_snowflake_identifier(self.schema, "schema")
        self.table_name = validate_snowflake_identifier(self.table_name, "table_name")
        self.primary_key_column = validate_snowflake_identifier(self.primary_key_column, "primary_key_column")
        self.vector_column = validate_snowflake_identifier(self.vector_column, "vector_column")
        if self.batch_size <= 0 or self.batch_size > 2048:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"batch_size must be between 1 and 2048, got {self.batch_size}.",
                http_status=400,
            )
        self.llm_preprocessing.validate()

        if self.backend_type == VectorBackendType.HYBRID_VECTOR:
            if self.vector_column.upper() == self.primary_key_column.upper() or self.vector_column.upper() in {
                c.upper() for c in self.secondary_index_columns
            }:
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

        if self.backend_type == VectorBackendType.POSTGRES_PGVECTOR:
            if not self.postgres_instance or not self.postgres_role:
                raise SnowflakeComponentError(
                    ErrorCode.UNSUPPORTED_CAPABILITY,
                    "POSTGRES_PGVECTOR backend requires both 'postgres_instance' and explicit 'postgres_role'.",
                    http_status=422,
                )

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
        """Provision physical storage (`CREATE TABLE` / `CREATE CORTEX SEARCH SERVICE` / PG `pgvector`)."""
        role = self.active_role
        schema_fqn = f"{config.database}.{config.schema}"

        if config.backend_type == VectorBackendType.STANDARD_VECTOR:
            if not self.has_sf_privilege(role, "CREATE TABLE", schema_fqn):
                raise_role_denied(role, "CREATE TABLE", schema_fqn, self.record_audit)
            ddl = (
                f"CREATE TABLE {config.table_fqn} ("
                f"{config.primary_key_column} VARCHAR PRIMARY KEY, "
                f"USER_ID VARCHAR NOT NULL, PROJECT_ID VARCHAR NOT NULL, SESSION_ID VARCHAR, "
                f"EFFECTIVE_ROLE VARCHAR NOT NULL, CONTENT VARCHAR, METADATA VARIANT, "
                f"SPACE_FINGERPRINT VARCHAR NOT NULL, "
                f"{config.vector_column} VECTOR(FLOAT, {config.embedding_spec.dimension}))"
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
                f"{config.vector_column} VECTOR(FLOAT, {config.embedding_spec.dimension}), "
                f"INDEX idx_scope ({idx_cols}))"
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
            # Verify both Snowflake role access to the Postgres integration and PG role privileges
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
                f"{config.vector_column} vector({config.embedding_spec.dimension}))"
            )
        else:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Unsupported vector backend: {config.backend_type}",
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
    """Implements the Langflow `Create Memory` dialog contract (`REQ-10`, `[G-A4]`).

    Preserves:
    - Controls: `Name`, `Embedding Model`, `Vector Database`, `Batch Size`,
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
        embedding_model: EmbeddingModelSpec,
        vector_database: VectorBackendType | str,
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
    ) -> dict[str, Any]:
        self.state = DialogState.LOADING
        try:
            if not (name or "").strip():
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    "Memory 'Name' cannot be empty.",
                    http_status=400,
                )
            backend_enum = (
                vector_database
                if isinstance(vector_database, VectorBackendType)
                else VectorBackendType(str(vector_database))
            )
            preproc = llm_preprocessing or LLMPreprocessingConfig(enabled=False)
            config = VectorStoreConfig(
                name=name.strip(),
                backend_type=backend_enum,
                database=database,
                schema=schema,
                table_name=table_name,
                embedding_spec=embedding_model,
                batch_size=batch_size,
                llm_preprocessing=preproc,
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

            self.state = DialogState.CREATED
            self.last_error = None
            return {
                "state": self.state.value,
                "memory_name": config.name,
                "backend_type": config.backend_type.value,
                "embedding_model": config.embedding_spec.model_id,
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
        if config.backend_type in (VectorBackendType.STANDARD_VECTOR, VectorBackendType.HYBRID_VECTOR):
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
        elif config.backend_type == VectorBackendType.POSTGRES_PGVECTOR:
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
            VectorBackendType.CORTEX_SEARCH,
        ):
            if not self.engine.has_sf_privilege(role, "INSERT", store.table_fqn):
                raise_role_denied(role, "INSERT INTO VECTOR SOURCE TABLE", store.table_fqn, self.engine.record_audit)
        elif store.backend_type == VectorBackendType.POSTGRES_PGVECTOR:
            if not self.engine.has_sf_privilege(role, "USAGE", store.postgres_instance or ""):
                raise_role_denied(role, "USAGE POSTGRES INSTANCE", store.postgres_instance or "", self.engine.record_audit)
            pg_role = store.postgres_role or ""
            if not self.engine.has_pg_privilege(pg_role, "INSERT", store.table_fqn):
                raise_role_denied(f"{role}/pg:{pg_role}", "POSTGRES INSERT", store.table_fqn, self.engine.record_audit)

        stored_rows = self.engine.records.setdefault(store.name, [])
        ingested_count = 0
        batches_processed = 0

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

                # Avoid duplicate vector computation when Cortex Search manages embeddings natively
                if (
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
            "batches_processed": batches_processed,
            "llm_preprocessing_used": store.llm_preprocessing.enabled,
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
        if store.backend_type in (VectorBackendType.STANDARD_VECTOR, VectorBackendType.HYBRID_VECTOR):
            if not self.engine.has_sf_privilege(role, "SELECT", store.table_fqn):
                raise_role_denied(role, "SELECT VECTOR TABLE", store.table_fqn, self.engine.record_audit)
        elif store.backend_type == VectorBackendType.CORTEX_SEARCH:
            svc_fqn = store.search_service_fqn or store.table_fqn
            if not self.engine.has_sf_privilege(role, "USAGE", svc_fqn):
                raise_role_denied(role, "USAGE CORTEX SEARCH SERVICE", svc_fqn, self.engine.record_audit)
        elif store.backend_type == VectorBackendType.POSTGRES_PGVECTOR:
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


class SnowflakeMemoryBaseComponent:
    """Langflow `Memory Base` long-term memory component (`REQ-10`, `[G-A4]`).

    Preserves `Memory Base` selector, `Search Query`, `Filter by Session`, `Results`, and `Tool Mode`.
    Enforces that `filter_by_session=False` allows cross-session search ONLY within the
    caller's authorized `(user_id, project_id, effective_role)` scope.
    """

    display_name = "Memory Base"
    category = "Files & Knowledge"
    ports = ("search_query", "results", "tool")

    def __init__(
        self,
        engine: InMemoryVectorStoreEngine,
        memory_name: str,
        *,
        filter_by_session: bool = True,
        tool_mode: bool = False,
    ) -> None:
        self.knowledge = SnowflakeKnowledgeComponent(engine, memory_name)
        self.memory_name = memory_name
        self.filter_by_session = filter_by_session
        self.tool_mode = tool_mode

    def store_memory(
        self,
        content: str,
        *,
        user_id: str,
        project_id: str,
        session_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = {"content": content, **(metadata or {})}
        return self.knowledge.ingest(
            [row],
            user_id=user_id,
            project_id=project_id,
            session_id=session_id,
        )

    def search_memory(
        self,
        search_query: str,
        *,
        user_id: str,
        project_id: str,
        session_id: str | None = None,
        top_k: int = 5,
    ) -> dict[str, Any]:
        if self.filter_by_session and not session_id:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "session_id is required when filter_by_session=True.",
                http_status=400,
            )
        return self.knowledge.retrieve(
            search_query,
            user_id=user_id,
            project_id=project_id,
            session_id=session_id,
            filter_by_session=self.filter_by_session,
            include_metadata=True,
            top_k=top_k,
        )
