from __future__ import annotations

import copy
from typing import Any, Callable

from .registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    ERROR_UNSUPPORTED_CAPABILITY,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    deduplicate_tool_call_ids,
)


class SnowflakeProxyMessagesAdapter:
    """
    Native adapter for `Snowflake Proxy - Messages` (Anthropic Messages protocol).
    - SDK base URL: `<PROXY_BASE_URL>/messages`
    - Inference URL: `<PROXY_BASE_URL>/messages/v1/messages`
    - Discovery URL: `<PROXY_BASE_URL>/messages/v1/models`
    - Preserves `thinking` (`signature`) and `redacted_thinking` blocks in multi-tool roundtrips
    - Rejects forced / `any` `tool_choice` where forbidden by profile
    - Normalizes string `tool_choice` ("auto" / "none") to `{"type": ...}`
    - When `thinking.type == "enabled"`, strips `temperature`/`top_p` and guarantees `max_tokens > thinking.budget_tokens`
    - Drops orphan `tool_choice` when `tools` is absent
    - Deduplicates reused `call_0..call_N` tool_use IDs across multi-round turns
    - Zero automatic retries or cross-protocol fallbacks
    """

    protocol = "messages"
    provider_name = SnowflakeProviderRegistry.PROVIDER_MESSAGES

    def __init__(
        self,
        registry: SnowflakeProviderRegistry,
        transport: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
    ) -> None:
        self.registry = registry
        self._transport = transport
        self.max_retries = 0

    @property
    def sdk_base_url(self) -> str:
        """Anthropic SDK base URL (`/messages`) because the SDK appends `/v1/messages`."""
        return f"{self.registry.proxy_base_url}/messages"

    @property
    def discovery_url(self) -> str:
        return f"{self.sdk_base_url}/v1/models"

    @property
    def inference_url(self) -> str:
        return f"{self.sdk_base_url}/v1/messages"

    def list_models(self, *, force_refresh: bool = False) -> list[str]:
        profiles = self.registry.discover_models("messages", force_refresh=force_refresh)
        return [p.id for p in profiles]

    def build_request_payload(
        self,
        model: str,
        messages: list[dict[str, Any]],
        *,
        system: str | list[dict[str, Any]] | None = None,
        max_tokens: int | None = None,
        stream: bool = False,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: Any = None,
        thinking: dict[str, Any] | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
    ) -> dict[str, Any]:
        self.registry.check_role_authorized("invoke_proxy_messages", "USAGE ON SERVICE LLM_PROXY_STANDARD")
        profile = self.registry.get_model_profile("messages", model)

        deduped_messages = deduplicate_tool_call_ids(messages)
        rehydrated_messages = self._rehydrate_thinking_blocks(deduped_messages)

        effective_max_tokens = max_tokens if max_tokens is not None else (profile.default_max_tokens or 4096)
        effective_max_tokens = max(1, min(int(effective_max_tokens), profile.max_token_limit))

        thinking_enabled = isinstance(thinking, dict) and thinking.get("type") == "enabled"
        if thinking_enabled:
            budget_tokens = int(thinking.get("budget_tokens", 1024))
            if effective_max_tokens <= budget_tokens:
                effective_max_tokens = min(profile.max_token_limit, budget_tokens + 1024)

        payload: dict[str, Any] = {
            "model": profile.id,
            "messages": rehydrated_messages,
            "max_tokens": effective_max_tokens,
            "stream": bool(stream),
        }

        if system:
            payload["system"] = copy.deepcopy(system)

        if thinking is not None:
            payload["thinking"] = copy.deepcopy(thinking)

        # Strip temperature and top_p when thinking is enabled
        if not thinking_enabled:
            if temperature is not None:
                payload["temperature"] = float(temperature)
            if top_p is not None:
                payload["top_p"] = float(top_p)

        if tools:
            payload["tools"] = copy.deepcopy(tools)
            if tool_choice is not None:
                normalized_tc = self._normalize_and_validate_tool_choice(tool_choice, profile.forbids_forced_tool_choice)
                if normalized_tc is not None:
                    payload["tool_choice"] = normalized_tc
        # If tools is absent, orphan tool_choice is intentionally omitted

        return payload

    def _normalize_and_validate_tool_choice(
        self,
        tool_choice: Any,
        forbids_forced: bool,
    ) -> dict[str, Any] | None:
        if isinstance(tool_choice, str):
            tc_type = tool_choice.strip().lower()
            normalized: dict[str, Any] = {"type": tc_type}
        elif isinstance(tool_choice, dict):
            normalized = copy.deepcopy(tool_choice)
            tc_type = str(normalized.get("type", "")).strip().lower()
        else:
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Invalid tool_choice format: {tool_choice!r}",
                active_role=self.registry.active_role,
                operation="validate_messages_tool_choice",
            )

        if forbids_forced and tc_type in ("any", "tool", "forced"):
            raise SnowflakeProviderError(
                ERROR_UNSUPPORTED_CAPABILITY,
                f"Messages model profile forbids forced tool_choice '{tc_type}'. Only 'auto' or 'none' is supported.",
                active_role=self.registry.active_role,
                operation="validate_messages_tool_choice",
            )
        return normalized

    def record_response_blocks(self, content_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Records `thinking` (with `signature`) and `redacted_thinking` blocks in the replay cache
        keyed by tool_use id so stateless callers never lose thinking signatures in multi-tool roundtrips.
        """
        preserved = copy.deepcopy(content_blocks)
        thinking_blocks = [
            b for b in preserved if isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking")
        ]
        for b in thinking_blocks:
            sig = b.get("signature") or b.get("data")
            if sig:
                self.registry.replay_cache.put(str(sig), b)

        if thinking_blocks:
            for b in preserved:
                if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("id"):
                    self.registry.replay_cache.put(
                        str(b["id"]),
                        {"thinking_blocks": thinking_blocks},
                    )
        return preserved

    def _rehydrate_thinking_blocks(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for msg in messages:
            if msg.get("role") == "assistant" and isinstance(msg.get("content"), list):
                blocks = copy.deepcopy(msg["content"])
                has_thinking = any(
                    isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking")
                    for b in blocks
                )
                if not has_thinking:
                    injected: list[dict[str, Any]] = []
                    for b in blocks:
                        if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("id"):
                            cached = self.registry.replay_cache.get(str(b["id"]))
                            if cached and isinstance(cached.get("thinking_blocks"), list):
                                injected.extend(copy.deepcopy(cached["thinking_blocks"]))
                                break
                    if injected:
                        blocks = injected + blocks
                new_msg = copy.deepcopy(msg)
                new_msg["content"] = blocks
                result.append(new_msg)
            else:
                result.append(copy.deepcopy(msg))
        return result

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
            "X-Proxy-Protocol": "messages",
            "anthropic-version": "2023-06-01",
            "X-Max-Retries": "0",
        }
        if self._transport is None:
            content = [
                {
                    "type": "thinking",
                    "thinking": "Internal verification step.",
                    "signature": "sig_thinking_01",
                },
                {"type": "text", "text": "OK"},
            ]
            self.record_response_blocks(content)
            return {
                "id": "msg-dryrun",
                "type": "message",
                "role": "assistant",
                "model": payload["model"],
                "request_payload": payload,
                "headers": headers,
                "content": content,
                "stop_reason": "end_turn",
            }
        try:
            response = self._transport(self.inference_url, payload, headers)
            if isinstance(response.get("content"), list):
                response["content"] = self.record_response_blocks(response["content"])
            return response
        except SnowflakeProviderError:
            raise
        except Exception as exc:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Messages proxy invocation failed without retry/fallback: {exc}",
                active_role=self.registry.active_role,
                operation=f"invoke_proxy_messages:{model}",
                status_code=502,
            ) from exc
