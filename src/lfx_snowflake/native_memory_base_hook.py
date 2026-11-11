"""Attach Snowflake ``LF_MEM_*`` Memory Bases to Langflow 1.13's native ``MemoryBaseComponent`` (BUG-MEM-MB-01).

Langflow 1.13.2 resolves a Memory Base's store through ``kb_helpers.backend_for_name`` -> ``knowledge_base``
row -> ``BackendType`` (a closed enum: chroma/sqlite/mongodb/astra/postgres/opensearch). A Snowflake backend
cannot be added to that enum, and writing Snowflake rows into ``knowledge_base`` is the A8 split-brain veto.
So the native component keeps its own ``memory_base`` lookup by ``(name, flow_id, user_id)`` (rows written
by ``LiveSnowflakeBridge._persist_memory_in_langflow_sqlite``) and only its two store-resolution hooks are
redirected for Snowflake memories:

- ``_build_backend`` returns :class:`SnowflakeLfMemBackend` (``AI_EMBED`` + ``VECTOR_COSINE_SIMILARITY`` in SQL);
- ``_additional_model_provider_policy_ids`` returns ``()``: embeddings run inside Snowflake, so no Langflow
  model-provider credential is used.

Non-Snowflake Memory Bases are untouched.
"""

from __future__ import annotations

from typing import Any

from lfx_snowflake.memories_lf_mem import LfMemError, LfMemSpec

_HOOK_FLAG = "_snowflake_lf_mem_hook_installed"


def snowflake_spec_for_memory(name_or_kb: Any, *, bridge: Any = None) -> LfMemSpec | None:
    """``LfMemSpec`` for a ``memory_base.kb_name`` marker or a registered memory name; None if not Snowflake."""
    key = str(name_or_kb or "").strip()
    if not key:
        return None
    spec = LfMemSpec.from_kb_name(key)
    if spec is not None:
        return spec
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415 - import cycle
    entry = bridge._find_memory_entry(key)
    if not entry or not isinstance(entry.get("lf_mem"), dict):
        return None
    return LfMemSpec(**entry["lf_mem"])


async def _selected_kb_name(component: Any, selected: Any) -> str | None:
    """kb_name of the attached native ``memory_base`` row (same (name, flow_id, user_id) scope as upstream)."""
    import uuid  # noqa: PLC0415

    try:
        flow_id = uuid.UUID(str(getattr(getattr(component, "graph", None), "flow_id", "") or ""))
        user_id = uuid.UUID(str(getattr(component, "user_id", "") or ""))
    except (TypeError, ValueError):
        return None
    from langflow.services.database.models.memory_base.model import MemoryBase  # noqa: PLC0415
    from lfx.services.deps import session_scope  # noqa: PLC0415
    from sqlmodel import select  # noqa: PLC0415

    async with session_scope() as db:
        mb = (
            await db.exec(
                select(MemoryBase).where(
                    MemoryBase.name == str(selected), MemoryBase.flow_id == flow_id, MemoryBase.user_id == user_id
                )
            )
        ).first()
    return mb.kb_name if mb is not None else None


def _document(content: str, metadata: dict[str, Any]) -> Any:
    try:
        from langchain_core.documents import Document  # noqa: PLC0415

        return Document(page_content=content, metadata=metadata)
    except Exception:  # noqa: BLE001 - langchain_core absent in some unit-test envs
        from types import SimpleNamespace  # noqa: PLC0415

        return SimpleNamespace(page_content=content, metadata=metadata)


class SnowflakeLfMemBackend:
    """Minimal ``BaseVectorStoreBackend`` surface used by ``MemoryBaseComponent.retrieve_memory``."""

    def __init__(self, spec: LfMemSpec, *, flow_id: str, bridge: Any = None) -> None:
        if bridge is None:
            from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
        self.spec = spec
        self.flow_id = str(flow_id or "")
        self.bridge = bridge
        self.last_sql: str | None = None

    async def ensure_ready(self) -> None:
        return None

    async def teardown(self) -> None:
        return None

    @staticmethod
    def normalize_score(score: Any) -> Any:
        return score  # VECTOR_COSINE_SIMILARITY: higher is closer, already in [-1, 1]

    def _run(self, sql: str, bindings: dict[str, Any]) -> dict[str, Any]:
        from lfx_snowflake import flow_context_guard as _guard  # noqa: PLC0415
        from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID  # noqa: PLC0415

        self.last_sql = sql
        token = CURRENT_SNOWFLAKE_FLOW_ID.set(self.flow_id or CURRENT_SNOWFLAKE_FLOW_ID.get())
        try:
            # User memory tables are context objects: the flow [ ❄️ Context ] must contain LF_MEM_<NAME>.
            with _guard.flow_component_sql():
                res = self.bridge.execute_sql(sql, bindings=bindings, mode="caller")
        finally:
            CURRENT_SNOWFLAKE_FLOW_ID.reset(token)
        if not isinstance(res, dict) or res.get("mode") == "offline" or res.get("ok") is False:
            err = res if isinstance(res, dict) else {}
            raise LfMemError(
                str(err.get("message") or "Snowflake Memory Base query failed."),
                error_code=str(err.get("error_code") or "SNOWFLAKE_ERROR"),
                http_status=int(err.get("status_code") or 502),
            )
        return res

    def search_sync(self, query: str, *, k: int, filter: dict[str, Any] | None) -> list[tuple[Any, float]]:  # noqa: A002
        where = dict(filter or {})
        session_id = where.get("session_id")
        sql, bindings = self.spec.build_recall_request(
            query, flow_id=self.flow_id, session_id=session_id, filter_by_session=bool(session_id), top_k=k
        )
        rows = list(self._run(sql, bindings).get("rows") or [])
        out: list[tuple[Any, float]] = []
        for item in LfMemSpec.recall_rows_to_results(rows, include_metadata=True):
            content = item.pop("content")
            score = item.pop("_score")
            out.append((_document(content, item), score))
        return out

    async def similarity_search(self, query: str, k: int = 5, filter: dict[str, Any] | None = None, with_scores: bool = True, **_: Any):  # noqa: A002
        import asyncio  # noqa: PLC0415

        results = await asyncio.to_thread(self.search_sync, query, k=k, filter=filter)
        return results if with_scores else [doc for doc, _score in results]


def install_snowflake_memory_base_hook(component_cls: Any) -> bool:
    """Idempotently redirect store resolution of ``component_cls`` (native MemoryBaseComponent) for Snowflake memories."""
    if getattr(component_cls, _HOOK_FLAG, False):
        return False
    orig_build_backend = component_cls._build_backend
    orig_policy_ids = getattr(component_cls, "_additional_model_provider_policy_ids", None)

    async def _build_backend(self, owner, owner_username, kb_name):  # type: ignore[no-untyped-def]
        spec = snowflake_spec_for_memory(kb_name)
        if spec is None:
            return await orig_build_backend(self, owner, owner_username, kb_name)
        flow_id = getattr(getattr(self, "graph", None), "flow_id", None)
        return SnowflakeLfMemBackend(spec, flow_id=str(flow_id or ""))

    async def _additional_model_provider_policy_ids(self, purpose, parameters=None):  # type: ignore[no-untyped-def]
        params = parameters if isinstance(parameters, dict) else getattr(self, "_parameters", None) or {}
        selected = params.get("memory_base", getattr(self, "memory_base", None)) if isinstance(params, dict) else None
        if isinstance(selected, str) and selected.strip():
            kb_name = await _selected_kb_name(self, selected.strip())
            if snowflake_spec_for_memory(kb_name) is not None:
                return ()
        if orig_policy_ids is None:
            return ()
        return await orig_policy_ids(self, purpose, parameters)

    component_cls._build_backend = _build_backend
    component_cls._additional_model_provider_policy_ids = _additional_model_provider_policy_ids
    setattr(component_cls, _HOOK_FLAG, True)
    return True
