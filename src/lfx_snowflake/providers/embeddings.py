from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Callable

from .ollama_provider import DEFAULT_OLLAMA_MODELS, OllamaModelEntry
from .registry import (
    DEFAULT_CATALOG_MODELS,
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
)

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmbeddingModelSpec:
    model_id: str
    dimension: int
    provider_backend: str  # "cortex_embed" or "ollama_embed"
    endpoint: str
    space_fingerprint: str

    @classmethod
    def create(
        cls,
        model_id: str,
        dimension: int,
        provider_backend: str = "cortex_embed",
        endpoint: str = "/api/v2/cortex/inference:embed",
    ) -> "EmbeddingModelSpec":
        raw_fp = f"{provider_backend}:{model_id}:{dimension}"
        fingerprint = hashlib.sha256(raw_fp.encode("utf-8")).hexdigest()[:16]
        return cls(
            model_id=model_id,
            dimension=dimension,
            provider_backend=provider_backend,
            endpoint=endpoint,
            space_fingerprint=fingerprint,
        )

    def to_metadata(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "dimension": self.dimension,
            "space_fingerprint": self.space_fingerprint,
            "provider_backend": self.provider_backend,
        }


from lfx_snowflake.sql_safety import AI_EMBED_TEXT_MODEL_DIMENSIONS as _AI_EMBED_TEXT_DIMS

_AI_EMBED_MODEL_INFO: dict[str, tuple[int, str]] = {
    "snowflake-arctic-embed-m-v1.5": (512, "English"),
    "snowflake-arctic-embed-m": (512, "English"),
    "e5-base-v2": (512, "English"),
    "snowflake-arctic-embed-l-v2.0": (512, "Multilingual"),
    "snowflake-arctic-embed-l-v2.0-8k": (8192, "Multilingual"),
    "multilingual-e5-large": (512, "Multilingual"),
    "voyage-multilingual-2": (32000, "Multilingual"),
    "nv-embed-qa-4": (512, "English"),
}

# Derived from sql_safety.AI_EMBED_TEXT_MODEL_DIMENSIONS (single source of truth, BUG-MEM-EMB-01/02).
DEFAULT_CORTEX_EMBEDDING_MODELS: list[EmbeddingModelSpec] = [
    EmbeddingModelSpec.create(model_id, dim, "cortex_embed") for model_id, dim in _AI_EMBED_TEXT_DIMS.items()
]


def _official_entry(model_id: str, dim: int) -> dict[str, Any]:
    max_tokens, language = _AI_EMBED_MODEL_INFO.get(model_id, (512, "English"))
    return {
        "id": model_id,
        "model_id": model_id,
        "model_name": model_id,
        "name": model_id,
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": dim,
        "dimensions": dim,
        "max_tokens": max_tokens,
        "language": language,
        "sql_function": f"SNOWFLAKE.CORTEX.AI_EMBED('{model_id}', ?)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    }


OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS: list[dict[str, Any]] = [
    _official_entry(model_id, dim) for model_id, dim in _AI_EMBED_TEXT_DIMS.items()
]

OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS: list[str] = [m["model_name"] for m in OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS]

OFFICIAL_AI_EMBED_MODEL_DIMENSIONS: dict[str, int] = dict(_AI_EMBED_TEXT_DIMS)


def resolve_embedding_dimension(model_id: str, default: int = 768) -> int:
    """Return the official vector dimension (768 or 1024) for a Snowflake AI_EMBED / Ollama embedding model."""
    cleaned = (model_id or "").strip().split(" ")[0].replace("ollama/", "")
    if cleaned in OFFICIAL_AI_EMBED_MODEL_DIMENSIONS:
        return OFFICIAL_AI_EMBED_MODEL_DIMENSIONS[cleaned]
    if "1024" in model_id or "l-v2" in cleaned or "large" in cleaned or "voyage" in cleaned or "nv-embed" in cleaned:
        return 1024
    return default


class EmbeddingSpaceGuard:
    """
    Validates `(model_id, dimension, space_fingerprint)` metadata before reading/writing a vector index.
    Blocks silent reuse of incompatible vectors even when dimensions match (e.g. 768d arctic-embed-m-v1.5 vs 768d e5-base-v2).
    """

    @staticmethod
    def assert_compatible(
        expected: EmbeddingModelSpec | dict[str, Any],
        actual: EmbeddingModelSpec | dict[str, Any],
        *,
        active_role: str = "PUBLIC",
    ) -> None:
        exp_meta = expected.to_metadata() if isinstance(expected, EmbeddingModelSpec) else dict(expected)
        act_meta = actual.to_metadata() if isinstance(actual, EmbeddingModelSpec) else dict(actual)

        if exp_meta.get("dimension") != act_meta.get("dimension"):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Vector dimension mismatch: index requires {exp_meta.get('dimension')}d "
                f"({exp_meta.get('model_id')}), but active embedder produces {act_meta.get('dimension')}d "
                f"({act_meta.get('model_id')}).",
                active_role=active_role,
                operation="verify_embedding_space",
            )

        if (
            exp_meta.get("model_id") != act_meta.get("model_id")
            or exp_meta.get("space_fingerprint") != act_meta.get("space_fingerprint")
        ):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Semantic vector space mismatch (same dimension {exp_meta.get('dimension')}d is not sufficient): "
                f"index was built with '{exp_meta.get('model_id')}' "
                f"(fingerprint={exp_meta.get('space_fingerprint')}), "
                f"cannot silently reuse with '{act_meta.get('model_id')}' "
                f"(fingerprint={act_meta.get('space_fingerprint')}).",
                active_role=active_role,
                operation="verify_embedding_space",
            )


class SnowflakeEmbeddingModelAdapter:
    """
    Dedicated Snowflake Embedding Model component & catalog.
    Kept strictly separate from the 3 generative Proxy catalogs (`Chat`, `Responses`, `Messages`)
    because `LLM_PROXY_STANDARD` does not expose `/embeddings`.
    Connects to `/api/v2/cortex/inference:embed` / `AI_EMBED` plus verified Ollama embedders.
    """

    def __init__(
        self,
        registry: SnowflakeProviderRegistry,
        cortex_models: list[EmbeddingModelSpec] | None = None,
        ollama_models: list[OllamaModelEntry] | None = None,
        embed_executor: Callable[[EmbeddingModelSpec, list[str], str], list[list[float]]] | None = None,
    ) -> None:
        self.registry = registry
        self._cortex_models = (
            list(cortex_models) if cortex_models is not None else list(DEFAULT_CORTEX_EMBEDDING_MODELS)
        )
        self._ollama_models = list(ollama_models) if ollama_models is not None else list(DEFAULT_OLLAMA_MODELS)
        self._embed_executor = embed_executor

    def list_embedding_models(self) -> list[EmbeddingModelSpec]:
        self.registry.check_role_authorized("list_embedding_models", "SNOWFLAKE.CORTEX_USER")
        catalog = list(self._cortex_models)
        for entry in self._ollama_models:
            if entry.is_embedding and entry.embedding_dimension:
                catalog.append(
                    EmbeddingModelSpec.create(
                        model_id=entry.name,
                        dimension=entry.embedding_dimension,
                        provider_backend="ollama_embed",
                        endpoint=f"{self.registry.ollama_base_url}/api/embed",
                    )
                )
        return catalog

    def get_embedding_spec(self, model_id: str) -> EmbeddingModelSpec:
        self.registry.check_role_authorized(f"get_embedding_spec:{model_id}", "SNOWFLAKE.CORTEX_USER")
        # Explicitly reject generative proxy models if passed as embedders
        for proto_models in DEFAULT_CATALOG_MODELS.values():
            for gen_model in proto_models:
                if gen_model.id == model_id:
                    raise SnowflakeProviderError(
                        ERROR_UNSUPPORTED_CAPABILITY,
                        f"Model '{model_id}' is a generative model on protocol '{gen_model.protocol}' "
                        "and cannot be used as an Embedding Model.",
                        active_role=self.registry.active_role,
                        operation="get_embedding_spec",
                    )
        for entry in self._ollama_models:
            if entry.name == model_id and entry.is_generative and not entry.is_embedding:
                raise SnowflakeProviderError(
                    ERROR_UNSUPPORTED_CAPABILITY,
                    f"Ollama model '{model_id}' is generative-only and not a verified embedding model.",
                    active_role=self.registry.active_role,
                    operation="get_embedding_spec",
                )

        for spec in self.list_embedding_models():
            if spec.model_id == model_id:
                return spec

        raise SnowflakeProviderError(
            ERROR_UNSUPPORTED_CAPABILITY,
            f"Embedding model '{model_id}' is not in the qualified Snowflake/Ollama embedding catalog.",
            active_role=self.registry.active_role,
            operation="get_embedding_spec",
        )

    def embed_documents(
        self,
        model_id: str,
        texts: list[str],
        *,
        target_index_metadata: EmbeddingModelSpec | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        spec = self.get_embedding_spec(model_id)
        if target_index_metadata is not None:
            EmbeddingSpaceGuard.assert_compatible(
                target_index_metadata,
                spec,
                active_role=self.registry.active_role,
            )

        if not texts:
            return {"vectors": [], "metadata": spec.to_metadata()}

        if self._embed_executor is not None:
            try:
                vectors = self._embed_executor(spec, texts, self.registry.active_role)
            except SnowflakeProviderError:
                raise
            except Exception as exc:
                raise SnowflakeProviderError(
                    ERROR_TECHNICAL_EXECUTION_ERROR,
                    f"Embedding execution failed for '{model_id}': {exc}",
                    active_role=self.registry.active_role,
                    operation="embed_documents",
                ) from exc
        else:
            # BUG-MEM-EMB-03: never fabricate vectors when no real embedding executor is wired.
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"No embedding executor configured for '{model_id}'; refusing to return synthetic vectors.",
                active_role=self.registry.active_role,
                operation="embed_documents",
            )
        if not isinstance(vectors, list) or len(vectors) != len(texts) or any(
            not isinstance(v, (list, tuple)) or len(v) != spec.dimension for v in vectors
        ):
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Embedding executor returned malformed vectors for '{model_id}' (expected {len(texts)}x{spec.dimension}).",
                active_role=self.registry.active_role,
                operation="embed_documents",
            )

        return {
            "vectors": vectors,
            "metadata": spec.to_metadata(),
            "sql_function": f"AI_EMBED('{spec.model_id}', :input_text)" if spec.provider_backend == "cortex_embed" else None,
        }


try:
    from langchain_core.embeddings import Embeddings as _LangChainEmbeddingsBase
except Exception:  # noqa: BLE001

    class _LangChainEmbeddingsBase:  # type: ignore[no-redef]
        """Fallback LangChain Embeddings base class when langchain_core is unavailable."""


class SnowflakeCortexEmbeddingError(RuntimeError):
    """Raised when AI_EMBED cannot produce a real vector (no synthetic fallback, BUG-MEM-EMB-03)."""


class SnowflakeCortexEmbeddings(_LangChainEmbeddingsBase):
    """
    LangChain Embeddings implementation backed natively by SNOWFLAKE.CORTEX.AI_EMBED.

    Supports all 6 official Snowflake Cortex AI_EMBED models without requiring any external API key:
      - snowflake-arctic-embed-m-v1.5 (768d, 512 max tokens, English)
      - snowflake-arctic-embed-m (768d, 512 max tokens, English)
      - e5-base-v2 (768d, 512 max tokens, English)
      - snowflake-arctic-embed-l-v2.0 (1024d, 512 max tokens, Multilingual)
      - voyage-multilingual-2 (1024d, 32000 max tokens, Multilingual)
      - nv-embed-qa-4 (1024d, 512 max tokens, English)
    """

    api_key_required: bool = False
    provider: str = "Snowflake Cortex"

    def __init__(
        self,
        model: Any = "snowflake-arctic-embed-m-v1.5",
        dimensions: int | None = None,
        model_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        resolved_model = self._normalize_model_id(model or model_name or "snowflake-arctic-embed-m-v1.5")
        self.model: str = resolved_model
        self.model_name: str = resolved_model
        self.dimension: int = int(dimensions or resolve_embedding_dimension(resolved_model))
        self.dimensions: int = self.dimension
        self.api_key_required = False
        self.provider = "Snowflake Cortex"
        self.last_sql: str = ""
        self.kwargs: dict[str, Any] = dict(kwargs)

    @staticmethod
    def _normalize_model_id(raw_model: Any) -> str:
        if isinstance(raw_model, list) and raw_model:
            first = raw_model[0]
            if isinstance(first, dict):
                raw_model = first.get("name") or first.get("model_name") or first.get("id") or "snowflake-arctic-embed-m-v1.5"
            else:
                raw_model = str(first)
        elif isinstance(raw_model, dict):
            raw_model = raw_model.get("name") or raw_model.get("model_name") or raw_model.get("id") or "snowflake-arctic-embed-m-v1.5"
        cleaned = str(raw_model or "snowflake-arctic-embed-m-v1.5").strip().split(" ")[0]
        return cleaned or "snowflake-arctic-embed-m-v1.5"

    def build_ai_embed_request(self, text: str) -> tuple[str, dict[str, Any]]:
        """``AI_EMBED(<allowlisted model literal>, ?)`` with the text as a binding (BUG-MEM-RCL-01)."""
        from lfx_snowflake import sql_safety as _sql_safety

        model_lit = _sql_safety.sql_string_literal(_sql_safety.validate_embed_model(self.model))
        sql = f"SELECT SNOWFLAKE.CORTEX.AI_EMBED({model_lit}, ?) AS EMBEDDING"
        return sql, _sql_safety.to_sql_bindings([("TEXT", str(text or ""))])

    def build_ai_embed_sql(self, text: str) -> str:
        return self.build_ai_embed_request(text)[0]

    def _extract_vector_from_sql_result(self, res: dict[str, Any] | None) -> list[float] | None:
        if not isinstance(res, dict) or res.get("mode") == "offline":
            return None
        rows = res.get("rows")
        if not isinstance(rows, list) or not rows:
            return None
        first = rows[0]
        val: Any = None
        if isinstance(first, dict):
            val = first.get("EMBEDDING") or first.get("embedding")
            if val is None and first:
                val = next(iter(first.values()))
        elif isinstance(first, (list, tuple)) and first:
            val = first[0]
        if isinstance(val, str):
            import json

            try:
                val = json.loads(val)
            except Exception:  # noqa: BLE001
                return None
        if isinstance(val, (list, tuple)) and len(val) == self.dimension:
            try:
                return [float(x) for x in val]
            except Exception:  # noqa: BLE001
                return None
        return None

    def embed_query(self, text: str) -> list[float]:
        """Embed via AI_EMBED; raises ``SnowflakeCortexEmbeddingError`` instead of faking a vector (BUG-MEM-EMB-03)."""
        try:
            sql, sql_bindings = self.build_ai_embed_request(text)
        except Exception as exc:  # invalid / non-allowlisted model
            raise SnowflakeCortexEmbeddingError(f"Invalid AI_EMBED model '{self.model}': {exc}") from exc
        self.last_sql = sql
        try:
            from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

            res = LIVE_BRIDGE.execute_sql(sql, bindings=sql_bindings)
        except Exception as exc:  # noqa: BLE001
            raise SnowflakeCortexEmbeddingError(f"AI_EMBED execution failed for model '{self.model}': {exc}") from exc
        if isinstance(res, dict) and res.get("mode") == "offline":
            raise SnowflakeCortexEmbeddingError(
                f"AI_EMBED unavailable (no live Snowflake session) for model '{self.model}'."
            )
        if isinstance(res, dict) and (res.get("error") or res.get("status") == "error"):
            raise SnowflakeCortexEmbeddingError(
                f"AI_EMBED failed for model '{self.model}': {res.get('error') or res.get('message')}"
            )
        vec = self._extract_vector_from_sql_result(res)
        if vec is None:
            raise SnowflakeCortexEmbeddingError(
                f"AI_EMBED returned no valid {self.dimension}-dimensional vector for model '{self.model}'."
            )
        return vec

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        return [self.embed_query(str(t)) for t in texts]

    async def aembed_query(self, text: str) -> list[float]:
        return self.embed_query(text)

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        return self.embed_documents(texts)


def get_snowflake_cortex_embedding_catalog() -> list[dict[str, Any]]:
    """Return the 6 official SNOWFLAKE.CORTEX.AI_EMBED models for Langflow's unified model catalog."""
    entries: list[dict[str, Any]] = []
    for item in OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS:
        m_name = str(item["model_name"])
        dim = int(item["dimension"])
        max_tok = int(item["max_tokens"])
        lang = str(item["language"])
        sql_fn = str(item["sql_function"])
        entries.append(
            {
                "id": m_name,
                "model_id": m_name,
                "model_name": m_name,
                "name": m_name,
                "provider": "Snowflake Cortex",
                "model_type": "embeddings",
                "icon": "Snowflake",
                "dimension": dim,
                "dimensions": dim,
                "max_tokens": max_tok,
                "language": lang,
                "sql_function": sql_fn,
                "default": True,
                "deprecated": False,
                "unsupported": False,
                "api_key_required": False,
                "metadata": {
                    "embedding_class": "SnowflakeCortexEmbeddings",
                    "param_mapping": {
                        "model": "model",
                        "dimensions": "dimensions",
                        "chunk_size": "chunk_size",
                        "request_timeout": "request_timeout",
                        "max_retries": "max_retries",
                        "model_kwargs": "model_kwargs",
                    },
                    "dimension": dim,
                    "dimensions": dim,
                    "max_tokens": max_tok,
                    "language": lang,
                    "api_key_required": False,
                    "sql_function": sql_fn,
                },
            }
        )
    return entries


def register_snowflake_cortex_embedding_provider() -> dict[str, Any]:
    """
    Register 'Snowflake Cortex' and its 6 official AI_EMBED models in Langflow's
    provider_registry, class_registry, model_catalog, and MemoryBase embedding_helpers.

    Ordering note: provider_registry.register_provider() MUST be called BEFORE any
    manual assignment to EMBEDDING_PARAM_MAPPINGS / EMBEDDING_PROVIDER_CLASS_MAPPING
    because register_provider raises ValueError if EMBEDDING_PARAM_MAPPINGS['Snowflake Cortex']
    already exists.
    """
    param_mapping = {
        "model": "model",
        "dimensions": "dimensions",
        "chunk_size": "chunk_size",
        "request_timeout": "request_timeout",
        "max_retries": "max_retries",
        "model_kwargs": "model_kwargs",
    }
    status: dict[str, Any] = {
        "provider": "Snowflake Cortex",
        "embedding_class": "SnowflakeCortexEmbeddings",
        "models": list(OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS),
        "provider_registry_registered": False,
        "class_registry_registered": False,
        "embedding_helpers_registered": False,
        "model_catalog_patched": False,
    }

    try:
        import lfx.custom as _lfx_custom  # noqa: F401
    except Exception:  # noqa: BLE001
        pass

    embedding_class_import = ("lfx_snowflake.providers.embeddings", "SnowflakeCortexEmbeddings", None)

    # 1. Register through provider_registry. Langflow 1.13 only lists providers that are in the
    #    registry (with their catalog_loader), so a manual class_registry entry is not enough, and
    #    register_provider() rejects a name whose embedding params already exist. An earlier failed
    #    attempt (e.g. during the circular import of unified_models) leaves exactly those mappings
    #    behind, so remove ours (never anyone else's) before registering.
    registry_available = False
    try:
        from lfx.base.models import provider_registry as _pr

        if hasattr(_pr, "ProviderDescriptor") and hasattr(_pr, "register_provider"):
            registry_available = True
            if hasattr(_pr, "is_registered") and _pr.is_registered("Snowflake Cortex"):
                status["provider_registry_registered"] = True
            else:
                try:
                    from lfx.base.models.unified_models import class_registry as _cr_stale

                    if _cr_stale.EMBEDDING_PARAM_MAPPINGS.get("Snowflake Cortex") == param_mapping:
                        del _cr_stale.EMBEDDING_PARAM_MAPPINGS["Snowflake Cortex"]
                    if _cr_stale.EMBEDDING_PROVIDER_CLASS_MAPPING.get("Snowflake Cortex") == "SnowflakeCortexEmbeddings":
                        del _cr_stale.EMBEDDING_PROVIDER_CLASS_MAPPING["Snowflake Cortex"]
                    if _cr_stale._EMBEDDING_CLASS_IMPORTS.get("SnowflakeCortexEmbeddings") == embedding_class_import:
                        del _cr_stale._EMBEDDING_CLASS_IMPORTS["SnowflakeCortexEmbeddings"]
                except Exception:  # noqa: BLE001
                    pass
                descriptor = _pr.ProviderDescriptor(
                    name="Snowflake Cortex",
                    metadata={
                        "icon": "Snowflake",
                        "variables": [],
                        "mapping": {"model_class": "ChatOpenAI"},
                    },
                    embedding_class_name="SnowflakeCortexEmbeddings",
                    embedding_class=embedding_class_import,
                    embedding_param_mapping=dict(param_mapping),
                    api_key_required=False,
                    catalog_loader="lfx_snowflake.providers.embeddings:get_snowflake_cortex_embedding_catalog",
                )
                _pr.register_provider(descriptor)
                status["provider_registry_registered"] = bool(
                    not hasattr(_pr, "is_registered") or _pr.is_registered("Snowflake Cortex")
                )
    except Exception as exc:  # noqa: BLE001
        status["provider_registry_error"] = f"{type(exc).__name__}: {exc}"
        _logger.warning("Snowflake Cortex embedding provider registration failed: %s", status["provider_registry_error"])

    # 2. Older Langflow versions have no provider_registry: populate class_registry directly.
    #    With a registry this is only done once the provider is registered, so a failed attempt
    #    never leaves conflicting mappings behind.
    if not registry_available or status["provider_registry_registered"]:
        try:
            from lfx.base.models.unified_models import class_registry as _cr

            _cr._EMBEDDING_CLASS_IMPORTS.setdefault("SnowflakeCortexEmbeddings", embedding_class_import)
            _cr.EMBEDDING_PROVIDER_CLASS_MAPPING["Snowflake Cortex"] = "SnowflakeCortexEmbeddings"
            _cr.EMBEDDING_PARAM_MAPPINGS["Snowflake Cortex"] = dict(param_mapping)
            _cr._embedding_class_cache["SnowflakeCortexEmbeddings"] = SnowflakeCortexEmbeddings
            status["class_registry_registered"] = True
        except Exception:  # noqa: BLE001
            pass

    # 3. Register model prefix pattern in langflow.services.memory_base.embedding_helpers
    try:
        from langflow.services.memory_base import embedding_helpers as _eh

        patterns = list(OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS) + ["snowflake-arctic-embed"]
        already = any(prov == "Snowflake Cortex" for _, prov in getattr(_eh, "_MODEL_TO_PROVIDER", []))
        if not already and hasattr(_eh, "_MODEL_TO_PROVIDER"):
            _eh._MODEL_TO_PROVIDER.insert(0, (patterns, "Snowflake Cortex"))
        status["embedding_helpers_registered"] = True
    except Exception:  # noqa: BLE001
        pass

    # 4. Ensure all 6 Snowflake Cortex models stay default=True in model_catalog and get_embedding_model_options
    try:
        from lfx.base.models import unified_models as _um

        orig_get_options = getattr(_um, "get_embedding_model_options", None)
        if callable(orig_get_options) and not getattr(orig_get_options, "__sflf_wrapped__", False):

            def _wrapped_get_embedding_model_options(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
                try:
                    base_opts = list(orig_get_options(*args, **kwargs))
                except Exception:  # noqa: BLE001
                    base_opts = []
                existing_names = {
                    str(opt.get("name") or opt.get("model_name") or "")
                    for opt in base_opts
                    if isinstance(opt, dict)
                }
                sf_opts: list[dict[str, Any]] = []
                for cat_entry in get_snowflake_cortex_embedding_catalog():
                    m_name = cat_entry["model_name"]
                    if m_name not in existing_names:
                        sf_opts.append(
                            {
                                "name": m_name,
                                "model_name": m_name,
                                "provider": "Snowflake Cortex",
                                "icon": "Snowflake",
                                "default": True,
                                "metadata": dict(cat_entry["metadata"]),
                            }
                        )
                return sf_opts + base_opts

            setattr(_wrapped_get_embedding_model_options, "__sflf_wrapped__", True)
            setattr(_um, "get_embedding_model_options", _wrapped_get_embedding_model_options)
            try:
                from lfx.components.models_and_agents import embedding_model as _em_mod

                setattr(_em_mod, "get_embedding_model_options", _wrapped_get_embedding_model_options)
            except Exception:  # noqa: BLE001
                pass
        try:
            from lfx.base.models.unified_models import model_catalog as _mc

            orig_ollama = getattr(_mc, "get_ollama_models", None)
            if callable(orig_ollama) and not getattr(orig_ollama, "__sflf_ollama_fast_fail__", False):

                async def _fast_fail_get_ollama_models(
                    base_url_value: str,
                    desired_capability: str,
                    json_models_key: str,
                    json_name_key: str,
                    json_capabilities_key: str,
                ) -> list[str]:
                    cache_key = (base_url_value, desired_capability)
                    if hasattr(_mc, "_ollama_cache_get"):
                        cached = _mc._ollama_cache_get(cache_key)
                        if cached is not None:
                            return cached
                    try:
                        import asyncio as _aio

                        res = await _aio.wait_for(
                            orig_ollama(
                                base_url_value,
                                desired_capability,
                                json_models_key,
                                json_name_key,
                                json_capabilities_key,
                            ),
                            timeout=1.5,
                        )
                        return list(res) if res else []
                    except Exception:  # noqa: BLE001
                        if hasattr(_mc, "_ollama_cache_set"):
                            _mc._ollama_cache_set(cache_key, [])
                        return []

                setattr(_fast_fail_get_ollama_models, "__sflf_ollama_fast_fail__", True)
                setattr(_mc, "get_ollama_models", _fast_fail_get_ollama_models)
                setattr(_um, "get_ollama_models", _fast_fail_get_ollama_models)
        except Exception:  # noqa: BLE001
            pass
        status["model_catalog_patched"] = True
    except Exception:  # noqa: BLE001
        pass

    return status

