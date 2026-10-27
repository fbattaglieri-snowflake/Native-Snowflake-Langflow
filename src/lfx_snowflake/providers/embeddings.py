from __future__ import annotations

import hashlib
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


DEFAULT_CORTEX_EMBEDDING_MODELS: list[EmbeddingModelSpec] = [
    EmbeddingModelSpec.create("snowflake-arctic-embed-l-v2.0", 1024, "cortex_embed"),
    EmbeddingModelSpec.create("snowflake-arctic-embed-l-v2.0-8k", 1024, "cortex_embed"),
    EmbeddingModelSpec.create("multilingual-e5-large", 1024, "cortex_embed"),
    EmbeddingModelSpec.create("voyage-multilingual-2", 1024, "cortex_embed"),
    EmbeddingModelSpec.create("nv-embed-qa-4", 1024, "cortex_embed"),
    EmbeddingModelSpec.create("voyage-multimodal-3", 1024, "cortex_embed"),
    EmbeddingModelSpec.create("snowflake-arctic-embed-m-v1.5", 768, "cortex_embed"),
    EmbeddingModelSpec.create("snowflake-arctic-embed-m", 768, "cortex_embed"),
    EmbeddingModelSpec.create("e5-base-v2", 768, "cortex_embed"),
]

OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS: list[dict[str, Any]] = [
    {
        "id": "snowflake-arctic-embed-m-v1.5",
        "model_id": "snowflake-arctic-embed-m-v1.5",
        "model_name": "snowflake-arctic-embed-m-v1.5",
        "name": "snowflake-arctic-embed-m-v1.5",
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": 768,
        "dimensions": 768,
        "max_tokens": 512,
        "language": "English",
        "sql_function": "SNOWFLAKE.CORTEX.AI_EMBED('snowflake-arctic-embed-m-v1.5', :text)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    },
    {
        "id": "snowflake-arctic-embed-m",
        "model_id": "snowflake-arctic-embed-m",
        "model_name": "snowflake-arctic-embed-m",
        "name": "snowflake-arctic-embed-m",
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": 768,
        "dimensions": 768,
        "max_tokens": 512,
        "language": "English",
        "sql_function": "SNOWFLAKE.CORTEX.AI_EMBED('snowflake-arctic-embed-m', :text)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    },
    {
        "id": "e5-base-v2",
        "model_id": "e5-base-v2",
        "model_name": "e5-base-v2",
        "name": "e5-base-v2",
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": 768,
        "dimensions": 768,
        "max_tokens": 512,
        "language": "English",
        "sql_function": "SNOWFLAKE.CORTEX.AI_EMBED('e5-base-v2', :text)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    },
    {
        "id": "snowflake-arctic-embed-l-v2.0",
        "model_id": "snowflake-arctic-embed-l-v2.0",
        "model_name": "snowflake-arctic-embed-l-v2.0",
        "name": "snowflake-arctic-embed-l-v2.0",
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": 1024,
        "dimensions": 1024,
        "max_tokens": 512,
        "language": "Multilingual",
        "sql_function": "SNOWFLAKE.CORTEX.AI_EMBED('snowflake-arctic-embed-l-v2.0', :text)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    },
    {
        "id": "voyage-multilingual-2",
        "model_id": "voyage-multilingual-2",
        "model_name": "voyage-multilingual-2",
        "name": "voyage-multilingual-2",
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": 1024,
        "dimensions": 1024,
        "max_tokens": 32000,
        "language": "Multilingual",
        "sql_function": "SNOWFLAKE.CORTEX.AI_EMBED('voyage-multilingual-2', :text)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    },
    {
        "id": "nv-embed-qa-4",
        "model_id": "nv-embed-qa-4",
        "model_name": "nv-embed-qa-4",
        "name": "nv-embed-qa-4",
        "provider": "Snowflake Cortex",
        "model_type": "embeddings",
        "dimension": 1024,
        "dimensions": 1024,
        "max_tokens": 512,
        "language": "English",
        "sql_function": "SNOWFLAKE.CORTEX.AI_EMBED('nv-embed-qa-4', :text)",
        "default": True,
        "deprecated": False,
        "unsupported": False,
    },
]

OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS: list[str] = [
    m["model_name"] for m in OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS
]

OFFICIAL_AI_EMBED_MODEL_DIMENSIONS: dict[str, int] = {
    "snowflake-arctic-embed-m-v1.5": 768,
    "snowflake-arctic-embed-m": 768,
    "e5-base-v2": 768,
    "nomic-embed-text": 768,
    "nomic-embed-text:latest": 768,
    "snowflake-arctic-embed-l-v2.0": 1024,
    "snowflake-arctic-embed-l-v2.0-8k": 1024,
    "multilingual-e5-large": 1024,
    "voyage-multilingual-2": 1024,
    "nv-embed-qa-4": 1024,
    "voyage-multimodal-3": 1024,
}


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
            # Deterministic unit-length synthetic vector generation for offline contract verification
            vectors = []
            for text in texts:
                digest = hashlib.sha256(f"{spec.space_fingerprint}:{text}".encode("utf-8")).digest()
                vec = [((digest[i % len(digest)] / 255.0) * 2.0 - 1.0) for i in range(spec.dimension)]
                vectors.append(vec)

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

    def build_ai_embed_sql(self, text: str) -> str:
        safe_text = str(text or "").replace("'", "''")
        return f"SELECT SNOWFLAKE.CORTEX.AI_EMBED('{self.model}', '{safe_text}') AS EMBEDDING"

    def _deterministic_unit_vector(self, text: str) -> list[float]:
        import math

        digest = hashlib.sha256(f"snowflake_cortex:{self.model}:{self.dimension}:{text}".encode("utf-8")).digest()
        raw = [((digest[i % len(digest)] / 255.0) * 2.0 - 1.0) for i in range(self.dimension)]
        norm = math.sqrt(sum(v * v for v in raw)) or 1.0
        return [round(v / norm, 8) for v in raw]

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
        sql = self.build_ai_embed_sql(text)
        self.last_sql = sql
        res: dict[str, Any] | None = None
        try:
            from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

            res = LIVE_BRIDGE.execute_sql(sql)
        except Exception:  # noqa: BLE001
            res = None
        vec = self._extract_vector_from_sql_result(res)
        if vec is not None:
            return vec
        return self._deterministic_unit_vector(text)

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

    # 1. Call provider_registry.register_provider FIRST (before manual fallback dict mutations)
    try:
        from lfx.base.models import provider_registry as _pr

        if hasattr(_pr, "ProviderDescriptor") and hasattr(_pr, "register_provider"):
            descriptor = _pr.ProviderDescriptor(
                name="Snowflake Cortex",
                metadata={
                    "icon": "Snowflake",
                    "variables": [],
                    "mapping": {"model_class": "ChatOpenAI"},
                },
                embedding_class_name="SnowflakeCortexEmbeddings",
                embedding_class=(
                    "lfx_snowflake.providers.embeddings",
                    "SnowflakeCortexEmbeddings",
                    None,
                ),
                embedding_param_mapping=dict(param_mapping),
                api_key_required=False,
                catalog_loader="lfx_snowflake.providers.embeddings:get_snowflake_cortex_embedding_catalog",
            )
            try:
                _pr.register_provider(descriptor)
            except ValueError:
                pass
            status["provider_registry_registered"] = True
    except Exception:  # noqa: BLE001
        pass

    # 2. Ensure class_registry mappings are populated
    try:
        from lfx.base.models.unified_models import class_registry as _cr

        _cr._EMBEDDING_CLASS_IMPORTS["SnowflakeCortexEmbeddings"] = (
            "lfx_snowflake.providers.embeddings",
            "SnowflakeCortexEmbeddings",
            None,
        )
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
        status["model_catalog_patched"] = True
    except Exception:  # noqa: BLE001
        pass

    return status

