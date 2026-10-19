from __future__ import annotations

import copy
from typing import Any, Callable

from .registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    deduplicate_tool_call_ids,
)


class SnowflakeProxyResponsesAdapter:
    """
    Native adapter for `Snowflake Proxy - Responses` (`/responses/v1`).
    - Base URL: `<PROXY_BASE_URL>/responses/v1`
    - Discovery: `/responses/v1/models`
    - Inference: `/responses/v1/responses`
    - Enforces `use_responses_api=True`, `store=False`, `include=["reasoning.encrypted_content"]`
    - Enforces `response_stream="ordered-items"` so `encrypted_content` and tool call items stay paired in langchain-openai
    - Strips incompatible `temperature`/`top_p` and empty `text={}`
    - Caches and re-hydrates `reasoning.encrypted_content` across multi-turn replays
    - Deduplicates reused `call_0..call_N` tool call IDs across multi-round turns
    - Zero automatic retries or cross-protocol fallbacks
    """

    protocol = "responses"
    provider_name = SnowflakeProviderRegistry.PROVIDER_RESPONSES

    def __init__(
        self,
        registry: SnowflakeProviderRegistry,
        transport: Callable[[str, dict[str, Any], dict[str, str]], dict[str, Any]] | None = None,
    ) -> None:
        self.registry = registry
        self._transport = transport
        self.use_responses_api = True
        self.max_retries = 0

    @property
    def base_url(self) -> str:
        return f"{self.registry.proxy_base_url}/responses/v1"

    @property
    def discovery_url(self) -> str:
        return f"{self.base_url}/models"

    @property
    def inference_url(self) -> str:
        return f"{self.base_url}/responses"

    def list_models(self, *, force_refresh: bool = False) -> list[str]:
        profiles = self.registry.discover_models("responses", force_refresh=force_refresh)
        return [p.id for p in profiles]

    def build_request_payload(
        self,
        model: str,
        input_items: list[dict[str, Any]],
        *,
        stream: bool = False,
        tools: list[dict[str, Any]] | None = None,
        max_output_tokens: int | None = None,
        store: bool | None = None,
        include: list[str] | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        text: dict[str, Any] | None = None,
        reasoning: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Intentionally ignore caller's store=True, temperature, top_p, and empty text={}
        _ = (store, temperature, top_p)
        self.registry.check_role_authorized("invoke_proxy_responses", "USAGE ON SERVICE LLM_PROXY_STANDARD")
        profile = self.registry.get_model_profile("responses", model)

        deduped_items = deduplicate_tool_call_ids(input_items)
        rehydrated_items = self._rehydrate_encrypted_reasoning(deduped_items)

        include_list = list(include) if include else []
        if "reasoning.encrypted_content" not in include_list:
            include_list.append("reasoning.encrypted_content")

        payload: dict[str, Any] = {
            "model": profile.id,
            "input": rehydrated_items,
            "store": False,
            "include": include_list,
            "response_stream": profile.response_stream_mode or "ordered-items",
            "stream": bool(stream),
        }

        if max_output_tokens is not None:
            payload["max_output_tokens"] = max(1, min(int(max_output_tokens), profile.max_token_limit))

        if tools:
            payload["tools"] = copy.deepcopy(tools)

        # Only include non-empty text format config (strip empty text={})
        if isinstance(text, dict) and len(text) > 0:
            payload["text"] = copy.deepcopy(text)

        if isinstance(reasoning, dict) and len(reasoning) > 0:
            payload["reasoning"] = copy.deepcopy(reasoning)

        return payload

    def record_response_items(self, output_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Orders output items (`ordered-items` contract: reasoning with encrypted_content precedes function_call)
        and records encrypted_content in the replay cache keyed by reasoning item id and paired call_id.
        """
        reasoning_items = [item for item in output_items if item.get("type") == "reasoning"]
        other_items = [item for item in output_items if item.get("type") != "reasoning"]
        ordered = reasoning_items + other_items

        latest_reasoning = reasoning_items[-1] if reasoning_items else None
        for item in ordered:
            if item.get("type") == "reasoning" and item.get("encrypted_content"):
                r_id = item.get("id") or "reasoning_latest"
                self.registry.replay_cache.put(r_id, item)
            elif item.get("type") == "function_call" and latest_reasoning and latest_reasoning.get("encrypted_content"):
                call_id = item.get("call_id") or item.get("id")
                if call_id:
                    self.registry.replay_cache.put(call_id, latest_reasoning)
        return ordered

    def _rehydrate_encrypted_reasoning(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen_reasoning_ids: set[str] = set()
        for item in items:
            if item.get("type") == "reasoning":
                r_id = item.get("id")
                if not item.get("encrypted_content") and r_id:
                    cached = self.registry.replay_cache.get(r_id)
                    if cached and cached.get("encrypted_content"):
                        item = copy.deepcopy(item)
                        item["encrypted_content"] = cached["encrypted_content"]
                if r_id:
                    seen_reasoning_ids.add(r_id)
                result.append(item)
            elif item.get("type") == "function_call":
                call_id = item.get("call_id") or item.get("id")
                if call_id:
                    cached_reasoning = self.registry.replay_cache.get(call_id)
                    if cached_reasoning:
                        r_id = cached_reasoning.get("id")
                        if not r_id or r_id not in seen_reasoning_ids:
                            result.append(copy.deepcopy(cached_reasoning))
                            if r_id:
                                seen_reasoning_ids.add(r_id)
                result.append(item)
            else:
                result.append(item)
        return result

    def invoke(
        self,
        model: str,
        input_items: list[dict[str, Any]],
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = self.build_request_payload(model, input_items, **kwargs)
        headers = {
            "X-Snowflake-Role": self.registry.active_role,
            "X-Snowflake-Context-Epoch": str(self.registry.context_epoch),
            "X-Proxy-Protocol": "responses",
            "X-Response-Stream-Mode": "ordered-items",
            "X-Max-Retries": "0",
        }
        if self._transport is None:
            default_output = [
                {
                    "id": "rs_01",
                    "type": "reasoning",
                    "encrypted_content": "enc_sig_01",
                    "summary": [],
                },
                {
                    "id": "msg_01",
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "OK"}],
                },
            ]
            ordered = self.record_response_items(default_output)
            return {
                "id": "resp-dryrun",
                "object": "response",
                "model": payload["model"],
                "request_payload": payload,
                "headers": headers,
                "output": ordered,
            }
        try:
            response = self._transport(self.inference_url, payload, headers)
            if isinstance(response.get("output"), list):
                response["output"] = self.record_response_items(response["output"])
            return response
        except SnowflakeProviderError:
            raise
        except Exception as exc:
            raise SnowflakeProviderError(
                ERROR_TECHNICAL_EXECUTION_ERROR,
                f"Responses proxy invocation failed without retry/fallback: {exc}",
                active_role=self.registry.active_role,
                operation=f"invoke_proxy_responses:{model}",
                status_code=502,
            ) from exc
