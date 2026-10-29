from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..providers.registry import (
    ERROR_AUTHZ_ROLE_DENIED,
    ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
)


SUPPORTED_SNOWFLAKE_OBJECT_TYPES: tuple[str, ...] = (
    "STANDARD_TABLE",
    "VIEW",
    "SECURE_VIEW",
    "MATERIALIZED_VIEW",
    "HYBRID_TABLE",
    "DYNAMIC_TABLE",
    "INTERACTIVE_TABLE",
    "ICEBERG_TABLE",
    "SNOWFLAKE_POSTGRES_TABLE",
)

SUPPORTED_SNOWFLAKE_OBJECT_LABELS: tuple[str, ...] = (
    "Standard Table",
    "View",
    "Secure View",
    "Materialized View",
    "Hybrid Table",
    "Dynamic Table",
    "Interactive Table",
    "Iceberg Table",
    "Snowflake Postgres Table",
)

DATA_SOURCE_HIERARCHY_ORDER: tuple[str, ...] = (
    "engine",
    "database",
    "schema_name",
    "object_type",
    "object_name",
    "sql_filter",
    "max_rows",
)

TECHNICAL_MEMORY_TABLE_NAMES: frozenset[str] = frozenset(
    {
        "MEM_GRAPH_STATE_CHECKPOINTS",
        "MEM_CHAT_WINDOW_HISTORY",
        "MEM_RUNNING_SUMMARIES",
        "MEM_USER_ENTITY_PROFILES",
        "MEM_EPISODIC_EXPERIENCES",
        "AGENT_CHECKPOINTS",
        "CHAT_WINDOW_HISTORY",
        "CHAT_WINDOW_BUFFER",
        "CHAT_SESSION_SUMMARY",
        "AGENT_SUMMARY_STORE",
        "USER_ENTITY_PROFILE",
        "AGENT_EPISODIC_MEMORY",
        "AGENT_EPISODIC_TURNS",
        "AGENT_SEMANTIC_VEC",
        "AGENT_LONG_TERM_FACTS",
        "CHAT_MESSAGE_HISTORY",
        "LANGGRAPH_CHECKPOINTS",
        "CHECKPOINTS",
        "CHECKPOINT_BLOBS",
        "CHECKPOINT_WRITES",
        "AGENT_EPISODES",
        "SNOWFLAKE_KB_MEM",
        "MEM_1_GRAPH_CHECKPOINTER",
        "MEM_2_CHAT_WINDOW",
        "MEM_3_SESSION_SUMMARY",
        "MEM_4_USER_ENTITY_PROFILE",
        "MEM_5_EPISODIC_FEWSHOT",
        "LANGFLOW_TRIGGER_REGISTRY",
    }
)

TECHNICAL_MEMORY_TABLE_PREFIXES: tuple[str, ...] = (
    "LANGFLOW_MEM_",
    "MEM_GRAPH_STATE_",
    "MEM_CHAT_WINDOW_",
    "MEM_RUNNING_SUMMAR",
    "MEM_USER_ENTITY_",
    "MEM_EPISODIC_",
    "MEM_1_",
    "MEM_2_",
    "MEM_3_",
    "MEM_4_",
    "MEM_5_",
    "_LF_STREAM_BATCH_",
    "_LF_STAGE_BATCH_",
    "LF_STRM_",
    "LF_STG_STRM_",
    "LF_SCHED_",
    "LF_TRG_",
    "LF_STG_TRG_",
    "LF_ALRT_",
    "LANGFLOW_TRIGGER_",
)


def is_technical_memory_table(
    object_name: str,
    memories_registry: list[dict[str, Any]] | None = None,
) -> bool:
    """Return True if `object_name` is an agent-memory backing table (`CTX-ARCH-07` #4 / `W2-R4-06B`)."""
    raw = (object_name or "").strip()
    if not raw:
        return False
    short_upper = raw.split("/")[-1].split(".")[-1].strip().strip('"').upper()
    full_upper = raw.replace('"', "").upper()
    if short_upper in TECHNICAL_MEMORY_TABLE_NAMES:
        return True
    if any(short_upper.startswith(prefix) for prefix in TECHNICAL_MEMORY_TABLE_PREFIXES):
        return True
    for entry in memories_registry or []:
        if not isinstance(entry, dict):
            continue
        for key in ("name", "table_name", "target_table", "backing_table", "qualified_name", "table_fqn", "pg_table"):
            val = str(entry.get(key) or "").strip()
            if not val:
                continue
            val_upper = val.replace('"', "").upper()
            val_short = val_upper.split("/")[-1].split(".")[-1]
            if short_upper == val_short or full_upper == val_upper:
                return True
    return False


def filter_business_data_source_objects(
    objects: list[str],
    memories_registry: list[dict[str, Any]] | None = None,
    *,
    category: str | None = None,
) -> list[str]:
    """Filter out technical agent-memory tables so `Data Sources` remains strictly business data (`W2-R4-06B`)."""
    cat = (category or "tables").strip().lower()
    if cat not in ("tables", "views", "iceberg_tables", "postgres_tables", "all", ""):
        return list(objects)
    return [obj for obj in objects if not is_technical_memory_table(obj, memories_registry=memories_registry)]


@dataclass(frozen=True)
class DataSourceObjectStatus:
    """
    Represents a catalog object in the unified `Database -> Schema -> Object` browser
    with the three independent readiness states required by [G-A5]:
      1. `catalog_visible`: visible in the catalog to the active role
      2. `select_permitted`: SQL `SELECT` privilege granted to the active role
      3. `analyst_semantic_ready`: backed by an explicit `Semantic View` or validated semantic YAML model
    """

    database: str
    schema: str
    name: str
    object_type: str  # One of SUPPORTED_SNOWFLAKE_OBJECT_TYPES or "POSTGRES_TABLE"
    engine: str = "snowflake"  # "snowflake" or "snowflake_postgres"
    columns: tuple[str, ...] = ()
    catalog_visible: bool = True
    select_permitted: bool = False
    semantic_view_fqn: str | None = None
    semantic_model_yaml_stage_path: str | None = None
    allowed_roles_visible: tuple[str, ...] = ("PUBLIC",)
    allowed_roles_select: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"

    @property
    def analyst_semantic_ready(self) -> bool:
        """
        [G-A5] Selecting a table is NEVER enough for Cortex Analyst readiness.
        Requires an explicit Semantic View or validated semantic YAML model on the Snowflake engine.
        """
        if self.engine != "snowflake":
            return False
        return bool(self.semantic_view_fqn or self.semantic_model_yaml_stage_path)

    def evaluate_for_role(self, active_role: str) -> dict[str, Any]:
        visible = active_role in self.allowed_roles_visible or "PUBLIC" in self.allowed_roles_visible
        can_select = visible and (active_role in self.allowed_roles_select)
        return {
            "qualified_name": self.qualified_name,
            "database": self.database,
            "schema": self.schema,
            "name": self.name,
            "object_type": self.object_type,
            "engine": self.engine,
            "columns": list(self.columns),
            "active_role": active_role,
            "catalog_visible": visible,
            "select_permitted": can_select,
            "analyst_semantic_ready": bool(can_select and self.analyst_semantic_ready),
            "semantic_view_fqn": self.semantic_view_fqn if can_select else None,
            "semantic_model_yaml_stage_path": self.semantic_model_yaml_stage_path if can_select else None,
        }


class SnowflakePostgresDataSourceAdapter:
    """
    Distinct adapter for Snowflake Postgres (`:5432`) with separate PostgreSQL credentials,
    privileges, and SQL dialect.
    Strictly forbids Cortex Analyst direct attachment, cross-engine joins, or implicit mirroring.
    """

    engine = "snowflake_postgres"
    dialect = "postgresql"
    port = 5432

    def __init__(
        self,
        instance_host: str = "<POSTGRES_INSTANCE>",
        database: str = "postgres",
        active_role: str = "PUBLIC",
        pg_role_mappings: dict[str, str] | None = None,
    ) -> None:
        self.instance_host = instance_host
        self.database = database
        self.active_role = active_role
        self.pg_role_mappings = pg_role_mappings or {}

    def resolve_pg_role(self) -> str:
        pg_role = self.pg_role_mappings.get(self.active_role)
        if not pg_role:
            raise SnowflakeProviderError.role_denied(
                active_role=self.active_role,
                operation=f"connect_snowflake_postgres:{self.instance_host}:{self.port}",
                required_privilege="POSTGRES_ROLE_MAPPING",
            )
        return pg_role

    def validate_query(self, sql: str, *, allow_analyst: bool = False, join_snowflake_objects: list[str] | None = None) -> dict[str, Any]:
        if allow_analyst:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Cortex Analyst is not supported directly on Snowflake Postgres (:5432). "
                "Implicit replication or mirroring is forbidden [G-A5].",
                active_role=self.active_role,
                operation="validate_postgres_analyst",
            )
        if join_snowflake_objects:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Cross-engine joins between Snowflake Postgres and Snowflake objects {join_snowflake_objects} "
                "are strictly forbidden [G-A5].",
                active_role=self.active_role,
                operation="validate_postgres_cross_engine_join",
            )
        pg_role = self.resolve_pg_role()
        return {
            "engine": self.engine,
            "dialect": self.dialect,
            "port": self.port,
            "pg_role": pg_role,
            "active_snowflake_role": self.active_role,
            "sql": sql,
        }


class SnowflakeDataSourcesComponent:
    """
    Langflow `Data Sources -> Snowflake` component with official Snowflake icon and unified
    `Database -> Schema -> Object` browser filtered by the active Snowflake role.
    """

    display_name = "Snowflake"
    category = "Data Sources"
    icon = "Snowflake"

    def __init__(
        self,
        active_role: str = "PUBLIC",
        catalog: list[DataSourceObjectStatus] | None = None,
        postgres_adapter: SnowflakePostgresDataSourceAdapter | None = None,
    ) -> None:
        self.active_role = active_role
        self._catalog: dict[str, DataSourceObjectStatus] = {}
        for obj in catalog or []:
            self.register_object(obj)
        self.postgres_adapter = postgres_adapter
        self.context_epoch: int = 1
        self._query_cache: dict[tuple[str, int, str], list[dict[str, Any]]] = {}

    def register_object(self, obj: DataSourceObjectStatus) -> None:
        if obj.engine == "snowflake" and obj.object_type not in SUPPORTED_SNOWFLAKE_OBJECT_TYPES:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Unsupported Snowflake Data Source object_type '{obj.object_type}'. "
                f"Supported: {SUPPORTED_SNOWFLAKE_OBJECT_TYPES}",
                active_role=self.active_role,
                operation="register_data_source_object",
            )
        self._catalog[obj.qualified_name] = obj

    def switch_role(self, new_role: str) -> int:
        if not new_role or not new_role.strip():
            raise SnowflakeProviderError(
                ERROR_AUTHZ_ROLE_DENIED,
                "Active role cannot be empty.",
                active_role=new_role,
                operation="switch_role",
                status_code=403,
            )
        self.active_role = new_role.strip()
        if self.postgres_adapter is not None:
            self.postgres_adapter.active_role = self.active_role
        self.context_epoch += 1
        self._query_cache.clear()
        return self.context_epoch

    def browse_catalog(
        self,
        *,
        database: str | None = None,
        schema: str | None = None,
        object_type: str | None = None,
        search: str | None = None,
        exclude_memory_tables: bool = True,
        active_role: str | None = None,
        available_objects: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        eff_role = (active_role or self.active_role or "PUBLIC").strip()
        if available_objects is not None:
            adhoc_results: list[dict[str, Any]] = []
            for item in available_objects:
                if not isinstance(item, dict):
                    continue
                fqn = str(item.get("object_fqn") or item.get("qualified_name") or item.get("name") or "").strip()
                if exclude_memory_tables and is_technical_memory_table(fqn):
                    continue
                allowed = item.get("allowed_roles")
                if isinstance(allowed, (list, tuple, set)) and eff_role not in allowed and "PUBLIC" not in allowed:
                    continue
                if object_type and item.get("object_type") != object_type:
                    continue
                if search and search.lower() not in fqn.lower():
                    continue
                adhoc_results.append(dict(item))
            return adhoc_results

        results: list[dict[str, Any]] = []
        for obj in self._catalog.values():
            if exclude_memory_tables and is_technical_memory_table(obj.name):
                continue
            status = obj.evaluate_for_role(eff_role)
            if not status["catalog_visible"]:
                continue
            if database and obj.database != database:
                continue
            if schema and obj.schema != schema:
                continue
            if object_type and obj.object_type != object_type:
                continue
            if search and search.lower() not in obj.qualified_name.lower():
                continue
            results.append(status)
        return sorted(results, key=lambda item: item["qualified_name"])

    def inspect_object_readiness(
        self,
        qualified_name: str,
        *,
        auto_create_semantic_view: bool = False,
        invent_relationships: bool = False,
    ) -> dict[str, Any]:
        if auto_create_semantic_view or invent_relationships:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                "Automatic creation/publication of Semantic Views or invented relationships is strictly forbidden [G-A5]. "
                "Reuse an explicit Semantic View or validated semantic model.",
                active_role=self.active_role,
                operation=f"inspect_object_readiness:{qualified_name}",
            )

        obj = self._catalog.get(qualified_name)
        if obj is None:
            raise SnowflakeProviderError(
                ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Object '{qualified_name}' does not exist or is not visible to role '{self.active_role}'.",
                active_role=self.active_role,
                operation=f"inspect_object_readiness:{qualified_name}",
                status_code=404,
            )
        status = obj.evaluate_for_role(self.active_role)
        if not status["catalog_visible"]:
            raise SnowflakeProviderError(
                ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
                f"Object '{qualified_name}' is not visible to active role '{self.active_role}'.",
                active_role=self.active_role,
                operation=f"inspect_object_readiness:{qualified_name}",
                status_code=404,
            )
        return status

    def execute_select(
        self,
        qualified_name: str,
        *,
        limit: int = 100,
        rows_provider: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        status = self.inspect_object_readiness(qualified_name)
        if not status["select_permitted"]:
            raise SnowflakeProviderError.role_denied(
                active_role=self.active_role,
                operation=f"SELECT ON {qualified_name}",
                required_privilege=f"SELECT ON {qualified_name}",
            )
        bounded_limit = max(1, min(int(limit), 1000))
        rows = (rows_provider or [])[:bounded_limit]
        return {
            "qualified_name": qualified_name,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "explain_privileges_sql": (
                f"CALL SYSTEM$EXPLAIN_PRIVILEGES('{self.active_role}', 'SELECT', '{qualified_name}', true)"
            ),
            "row_count": len(rows),
            "rows": rows,
        }
