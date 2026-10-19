from __future__ import annotations

import copy
from typing import Any, Callable

from .registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    deduplicate_tool_call_ids,
)


class SnowflakeProxyChatAdapter:
    """
    Native adapter for `Snowflake Proxy - Chat` (`/chat/v1`).
    - Base URL: `<PROXY_BASE_URL>/chat/v1`
    - Discovery: `/chat/v1/models`
    - Inference: `/chat/v1/chat/completions`
    - Enforces `reasoning_effort="none"` on Luna/Sol (`openai-gpt-6-luna`, `openai-gpt-6-sol`)
    - Strips unsupported `reasoning_effort` on non-effort Chat models (`grok-4.6`, `grok-4.7`, `kimi-k3`, `glm-5.3`)
    - Resolves `max_tokens` vs `max_completion_tokens` conflict and clamps token budget to `1..128000`
    - Sets `stream_options.include_usage = True` when streaming
    - Deduplicates reused `call_0..call_N` tool call IDs across multi-round turns
    - Zero automatic retries or cross-protocol fallbacks on 429/500/timeout
    """

    protocol = "chat"
    provider_name = SnowflakeProviderRegistry.PROVIDER_CHAT

    def __init__(
        self,
        registry: SnowflakeProviderRegistry,
        transport: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
    ) -> None:
        self.registry = registry
        self._transport = transport
        self.max_retries = 0

    @property
    def base_url(self) -> str:
        return f"{self.registry.proxy_base_url}/chat/v1"

    @property
    def discovery_url(self) -> str:
        return f"{self.base_url}/models"

    @property
    def inference_url(self) -> str:
        return f"{self.base_url}/chat/completions"

    def list_models(self, *, force_refresh: bool = False) -> list[str]:
        profiles = self.registry.discover_models("chat", force_refresh=force_refresh)
        return [p.id for p in profiles]

    def build_request_payload(
        self,
        model: str,
        messages: list[dict[str, Any]],
        *,
        stream: bool = False,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: Any = None,
        temperature: float | None = None,
        top_p: float | None = None,
        max_tokens: int | None = None,
        max_completion_tokens: int | None = None,
        reasoning_effort: str | None = None,
        response_format: dict[str, Any] | None = None,
        parallel_tool_calls: bool | None = None,
    ) -> dict[str, Any]:
        self.registry.check_role_authorized("invoke_proxy_chat", "USAGE ON SERVICE LLM_PROXY_STANDARD")
        profile = self.registry.get_model_profile("chat", model)
        normalized_messages = deduplicate_tool_call_ids(messages)

        payload: dict[str, Any] = {
            "model": profile.id,
            "messages": normalized_messages,
            "stream": bool(stream),
        }

        if stream:
            payload["stream_options"] = {"include_usage": True}

        # Resolve max_tokens / max_completion_tokens conflict and clamp 1..128000
        effective_budget = max_completion_tokens if max_completion_tokens is not None else max_tokens
        if effective_budget is not None:
            clamped = max(1, min(int(effective_budget), profile.max_token_limit))
            payload["max_completion_tokens"] = clamped

        # Normalize reasoning_effort per model profile
        if profile.requires_reasoning_none:
            payload["reasoning_effort"] = "none"
        elif profile.supports_reasoning_effort and reasoning_effort is not None:
            payload["reasoning_effort"] = reasoning_effort
        # Else omit reasoning_effort completely for grok-4.6, grok-4.7, kimi-k3, glm-5.3

        if temperature is not None:
            payload["temperature"] = float(temperature)
        if top_p is not None:
            payload["top_p"] = float(top_p)

        if tools:
            payload["tools"] = copy.deepcopy(tools)
            if tool_choice is not None:
                payload["tool_choice"] = tool_choice
            if parallel_tool_calls is not None:
                payload["parallel_tool_calls"] = bool(parallel_tool_calls)

        if response_format is not None:
            payload["response_format"] = copy.deepcopy(response_format)

        return payload

    def invoke(
        self,
        model: str,
        messages: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = self.build_request_payload(model, messages, **kwargs)
        headers = {
            "X-Snowflake-Role": self.registry.active_role,
            "X-Snowflake-Context-Epoch": str(self.registry.context_epoch),
            "X-Proxy-Protocol": "chat",
            "X-Max-Retries": "0",
        }
        if self._transport is None:
            return {
                "id": "chatcmpl-dryrun",
                "object": "chat.completion",
                "model": payload["model"],
                "request_payload": payload,
                "headers": headers,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "OK"},
                        "finish_reason": "stop",
                    }
                ],
            }
        try:
            # Strict single call: zero automatic retry or silent model fallback
            return self._transport(self.inference_url, payload, headers)
        except SnowflakeProviderError:
            raise
        except Exception as exc:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Chat proxy invocation failed without retry/fallback: {exc}",
                active_role=self.registry.active_role,
                operation=f"invoke_proxy_chat:{model}",
                status_code=502,
            ) from exc
