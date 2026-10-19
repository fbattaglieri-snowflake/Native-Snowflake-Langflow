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
    EmbeddingModelSpec.create("snowflake-arctic-embed-m-v1.5", 768, "cortex_embed"),
    EmbeddingModelSpec.create("snowflake-arctic-embed-m", 768, "cortex_embed"),
    EmbeddingModelSpec.create("e5-base-v2", 768, "cortex_embed"),
]


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
