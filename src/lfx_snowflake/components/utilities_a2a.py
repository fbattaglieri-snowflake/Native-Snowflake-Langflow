"""Snowflake Utilities, A2A, and Workflow Memory Recall Adapters (`W2-R2-10`, `W2-R2-13`).

Provides:
- `SnowflakeMessageHistoryAdapter` & `SnowflakeMemoryBaseRecallHelper` with `memory_catalog_ref`
  dropdown options populated from `LIVE_BRIDGE.memories_catalog` so any memory created via
  `Memories -> + Snowflake` (`episodic_session`, `semantic_vector`, `structured_long_term`)
  can be recalled directly in workflows or exposed as a Tool.
- Re-exports `SnowflakeMessageHistoryComponent`, `SnowflakeMemoryBaseComponent`,
  `SnowflakePythonInterpreterComponent`, `SnowflakeToolGuardPoliciesComponent`, and
  `SnowflakeA2AAgentComponent` for backward compatibility.
"""

from __future__ import annotations

from typing import Any

from ..fastapi_live_bridge import AGENT_MEMORY_TYPES, LIVE_BRIDGE
from .a2a_and_cortex_agent import (
    A2AAgentCard,
    A2APublicationStatus,
    A2ATargetType,
    CortexAgentRunAdapter,
    ServeAsA2AAgentRegistry,
    SnowflakeA2AAgentComponent,
)
from .knowledge_and_memories import (
    CreateMemoryDialogService,
    DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG,
    InMemoryVectorStoreEngine,
    SnowflakeKnowledgeComponent,
    SnowflakeMemoryBaseComponent,
    VectorBackendType,
)
from .message_history import (
    CortexThreadReference,
    InMemoryMessageHistoryBackend,
    MessageHistoryBackendType,
    MessageHistoryStorageConfig,
    SignedReplayEnvelope,
    SnowflakeMessageHistoryComponent,
)
from .python_interpreter_pypi import (
    INVARIANT_UTILITIES_COMPONENTS,
    SNOWFLAKE_PYPI_REQUIRED_DB_ROLE,
    SNOWFLAKE_PYPI_SHARED_REPOSITORY,
    SnowflakePythonInterpreterComponent,
)
from .toolguard_policies import (
    SNOWFLAKE_TOOLGUARD_PROVIDERS,
    UNCHANGED_DISCOVER_MORE_BUNDLES,
    PolicySpec,
    SnowflakeToolGuardPoliciesComponent,
    ToolDefinition,
    ToolGuardActivity,
    ToolGuardModelSelection,
)


def get_memory_catalog_options(*, include_none: bool = True) -> list[str]:
    """Return `memory_catalog_ref` dropdown options from `LIVE_BRIDGE.memories_catalog` (`W2-R2-13`)."""
    return LIVE_BRIDGE.get_memory_catalog_options(include_none=include_none)


def recall_memory_from_catalog(
    memory_catalog_ref: str,
    *,
    query: str = "",
    session_id: str = "default",
    limit: int = 10,
) -> dict[str, Any]:
    """Recall a Snowflake Memory created via `Memories -> + Snowflake` (`W2-R2-13`)."""
    return LIVE_BRIDGE.recall_memory(
        memory_catalog_ref,
        query=query,
        session_id=session_id,
        limit=limit,
    )


class SnowflakeMessageHistoryAdapter:
    """Workflow recall adapter for `Message History` / `Memory Base` bound to `LIVE_BRIDGE.memories_catalog` (`W2-R2-13`)."""

    display_name = "Memory Base / Message History"
    category = "Memories"
    icon = "Snowflake"

    def __init__(
        self,
        *,
        memory_catalog_ref: str = "SNOWFLAKE_KB_MEM",
        session_id: str = "default",
        query: str = "",
        limit: int = 10,
        history_component: SnowflakeMessageHistoryComponent | None = None,
    ) -> None:
        self.memory_catalog_ref = memory_catalog_ref
        self.session_id = session_id
        self.query = query
        self.limit = limit
        self.history_component = history_component

    @staticmethod
    def list_memory_catalog_options() -> list[str]:
        return get_memory_catalog_options(include_none=True)

    def update_build_config(
        self,
        build_config: dict[str, Any],
        field_value: Any = None,
        field_name: str | None = None,
    ) -> dict[str, Any]:
        if "memory_catalog_ref" in build_config:
            build_config["memory_catalog_ref"]["options"] = self.list_memory_catalog_options()
        return build_config

    def recall(
        self,
        *,
        memory_catalog_ref: str | None = None,
        query: str | None = None,
        session_id: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        target_ref = (memory_catalog_ref if memory_catalog_ref is not None else self.memory_catalog_ref) or "SNOWFLAKE_KB_MEM"
        return recall_memory_from_catalog(
            target_ref,
            query=query if query is not None else self.query,
            session_id=session_id if session_id is not None else self.session_id,
            limit=limit if limit is not None else self.limit,
        )


SnowflakeMemoryBaseRecallHelper = SnowflakeMessageHistoryAdapter

__all__ = [
    "A2AAgentCard",
    "A2APublicationStatus",
    "A2ATargetType",
    "AGENT_MEMORY_TYPES",
    "CortexAgentRunAdapter",
    "CortexThreadReference",
    "CreateMemoryDialogService",
    "DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG",
    "INVARIANT_UTILITIES_COMPONENTS",
    "InMemoryMessageHistoryBackend",
    "InMemoryVectorStoreEngine",
    "MessageHistoryBackendType",
    "MessageHistoryStorageConfig",
    "PolicySpec",
    "SNOWFLAKE_PYPI_REQUIRED_DB_ROLE",
    "SNOWFLAKE_PYPI_SHARED_REPOSITORY",
    "SNOWFLAKE_TOOLGUARD_PROVIDERS",
    "ServeAsA2AAgentRegistry",
    "SignedReplayEnvelope",
    "SnowflakeA2AAgentComponent",
    "SnowflakeKnowledgeComponent",
    "SnowflakeMemoryBaseComponent",
    "SnowflakeMemoryBaseRecallHelper",
    "SnowflakeMessageHistoryAdapter",
    "SnowflakeMessageHistoryComponent",
    "SnowflakePythonInterpreterComponent",
    "SnowflakeToolGuardPoliciesComponent",
    "ToolDefinition",
    "ToolGuardActivity",
    "ToolGuardModelSelection",
    "UNCHANGED_DISCOVER_MORE_BUNDLES",
    "VectorBackendType",
    "get_memory_catalog_options",
    "recall_memory_from_catalog",
]
