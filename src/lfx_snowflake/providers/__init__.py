from .embeddings import (
    DEFAULT_CORTEX_EMBEDDING_MODELS,
    EmbeddingModelSpec,
    EmbeddingSpaceGuard,
    SnowflakeEmbeddingModelAdapter,
)
from .ollama_provider import (
    DEFAULT_OLLAMA_MODELS,
    OllamaModelEntry,
    SnowflakeOllamaAdapter,
)
from .proxy_chat import SnowflakeProxyChatAdapter
from .proxy_messages import SnowflakeProxyMessagesAdapter
from .proxy_responses import SnowflakeProxyResponsesAdapter
from .registry import (
    DEFAULT_CATALOG_MODELS,
    ERROR_AUTHZ_ROLE_DENIED,
    ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    ModelProfile,
    ProviderDescriptor,
    SignedBlockReplayCache,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    deduplicate_tool_call_ids,
)

# Shared contract alias
SnowflakeEmbeddingAdapter = SnowflakeEmbeddingModelAdapter

__all__ = [
    "DEFAULT_CATALOG_MODELS",
    "DEFAULT_CORTEX_EMBEDDING_MODELS",
    "DEFAULT_OLLAMA_MODELS",
    "ERROR_AUTHZ_ROLE_DENIED",
    "ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED",
    "ERROR_TECHNICAL_EXECUTION_ERROR",
    "ERROR_UNSUPPORTED_CAPABILITY",
    "EmbeddingModelSpec",
    "EmbeddingSpaceGuard",
    "ModelProfile",
    "OllamaModelEntry",
    "ProviderDescriptor",
    "SignedBlockReplayCache",
    "SnowflakeEmbeddingAdapter",
    "SnowflakeEmbeddingModelAdapter",
    "SnowflakeOllamaAdapter",
    "SnowflakeProviderError",
    "SnowflakeProviderRegistry",
    "SnowflakeProxyChatAdapter",
    "SnowflakeProxyMessagesAdapter",
    "SnowflakeProxyResponsesAdapter",
    "deduplicate_tool_call_ids",
]
