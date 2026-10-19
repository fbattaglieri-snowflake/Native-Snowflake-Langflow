from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable

from .registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    deduplicate_tool_call_ids,
)


@dataclass(frozen=True)
class OllamaModelEntry:
    name: str
    is_generative: bool = True
    is_embedding: bool = False
    embedding_dimension: int | None = None
    capabilities: tuple[str, ...] = ("text", "tools", "streaming", "structured_output")


DEFAULT_OLLAMA_MODELS: list[OllamaModelEntry] = [
    OllamaModelEntry(
        name="llama3.3:70b",
        is_generative=True,
        is_embedding=False,
        capabilities=("text", "tools", "streaming", "structured_output"),
    ),
    OllamaModelEntry(
        name="qwen2.5-coder:32b",
        is_generative=True,
        is_embedding=False,
        capabilities=("text", "tools", "streaming", "structured_output"),
    ),
    OllamaModelEntry(
        name="nomic-embed-text:latest",
        is_generative=False,
        is_embedding=True,
        embedding_dimension=768,
        capabilities=("embedding",),
    ),
]


class SnowflakeOllamaAdapter:
    """
    Native adapter for `Ollama (Snowflake Hosted)` on SPCS.
    Separates generative models from models with verified embedding capability.
    Enforces active role authorization, deduplicates multi-round tool calls, and forbids silent retries/fallbacks.
    """

    protocol = "ollama"
    provider_name = SnowflakeProviderRegistry.PROVIDER_OLLAMA

    def __init__(
        self,
        registry: SnowflakeProviderRegistry,
        models: list[OllamaModelEntry] | None = None,
        transport: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
    ) -> None:
        self.registry = registry
        self._models = list(models) if models is not None else list(DEFAULT_OLLAMA_MODELS)
        self._transport = transport
        self.max_retries = 0

    @property
    def base_url(self) -> str:
        return self.registry.ollama_base_url

    @property
    def chat_url(self) -> str:
        return f"{self.base_url}/api/chat"

    @property
    def tags_url(self) -> str:
        return f"{self.base_url}/api/tags"

    def list_generative_models(self) -> list[str]:
        self.registry.check_role_authorized("list_ollama_generative_models", "USAGE ON SERVICE OLLAMA_SPCS")
        return [m.name for m in self._models if m.is_generative]

    def list_embedding_models(self) -> list[OllamaModelEntry]:
        self.registry.check_role_authorized("list_ollama_embedding_models", "USAGE ON SERVICE OLLAMA_SPCS")
        return [m for m in self._models if m.is_embedding and m.embedding_dimension is not None]

    def get_generative_model(self, model_name: str) -> OllamaModelEntry:
        self.registry.check_role_authorized("get_ollama_generative_model", "USAGE ON SERVICE OLLAMA_SPCS")
        for entry in self._models:
            if entry.name == model_name:
                if not entry.is_generative:
                    raise SnowflakeProviderError(
                        ERROR_UNSUPPORTED_CAPABILITY,
                        f"Ollama model '{model_name}' is an embedding-only model and cannot be used for generation.",
                        active_role=self.registry.active_role,
                        operation="get_ollama_generative_model",
                    )
                return entry
        raise SnowflakeProviderError(
            ERROR_UNSUPPORTED_CAPABILITY,
            f"Ollama model '{model_name}' is not registered on this Snowflake-hosted Ollama endpoint.",
            active_role=self.registry.active_role,
            operation="get_ollama_generative_model",
        )

    def build_chat_payload(
        self,
        model: str,
        messages: list[dict[str, Any]],
        *,
        stream: bool = False,
        tools: list[dict[str, Any]] | None = None,
        format_schema: dict[str, Any] | str | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        entry = self.get_generative_model(model)
        deduped_messages = deduplicate_tool_call_ids(messages)
        payload: dict[str, Any] = {
            "model": entry.name,
            "messages": deduped_messages,
            "stream": bool(stream),
        }
        if tools:
            payload["tools"] = copy.deepcopy(tools)
        if format_schema is not None:
            payload["format"] = copy.deepcopy(format_schema)
        if options:
            payload["options"] = copy.deepcopy(options)
        return payload

    def invoke(
        self,
        model: str,
        messages: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = self.build_chat_payload(model, messages, **kwargs)
        headers = {
            "X-Snowflake-Role": self.registry.active_role,
            "X-Snowflake-Context-Epoch": str(self.registry.context_epoch),
            "X-Provider": "snowflake_ollama",
            "X-Max-Retries": "0",
        }
        if self._transport is None:
            return {
                "model": payload["model"],
                "request_payload": payload,
                "headers": headers,
                "message": {"role": "assistant", "content": "OK"},
                "done": True,
            }
        try:
            return self._transport(self.chat_url, payload, headers)
        except SnowflakeProviderError:
            raise
        except Exception as exc:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Ollama (Snowflake Hosted) invocation failed without retry/fallback: {exc}",
                active_role=self.registry.active_role,
                operation=f"invoke_ollama:{model}",
                status_code=502,
            ) from exc
