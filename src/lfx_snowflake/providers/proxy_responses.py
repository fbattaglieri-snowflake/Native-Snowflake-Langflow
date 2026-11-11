from __future__ import annotations

import copy
from typing import Any, Callable

from . import messages_replay as _replay

from .registry import (
    ERROR_TECHNICAL_EXECUTION_ERROR,
    SnowflakeProviderError,
    SnowflakeProviderRegistry,
    deduplicate_tool_call_ids,
)


# --- T7: cross-turn replay of Responses tool loops (function_call / function_call_output items) ---------------
# Langflow history keeps only the final assistant text, so turn N+1 never saw turn N's tool calls. Per the
# Responses contract (store=False) the client re-sends prior output items as input: every ``function_call`` is
# followed by its ``function_call_output`` with the same ``call_id``; ``reasoning`` items travel with their
# ``encrypted_content``. Isolated per user|flow|session scope exactly like the Messages replay.
RESPONSES_REPLAY = _replay.MessagesReplayStore()


def _item_text(item: dict[str, Any]) -> str:
    content = item.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(b.get("text") or "")
            for b in content
            if isinstance(b, dict) and b.get("type") in ("input_text", "output_text", "text")
        )
    return ""


def _is_user(item: dict[str, Any]) -> bool:
    return item.get("role") == "user" and item.get("type", "message") == "message"


def _is_assistant_text(item: dict[str, Any]) -> bool:
    return item.get("role") == "assistant" and item.get("type", "message") == "message"


def _complete_loop(seq: list[dict[str, Any]]) -> bool:
    calls = {i.get("call_id") for i in seq if i.get("type") == "function_call"}
    outputs = {i.get("call_id") for i in seq if i.get("type") == "function_call_output"}
    return bool(calls) and calls <= outputs


def _summarize_items(seq: list[dict[str, Any]], final: dict[str, Any]) -> dict[str, Any]:
    outputs = {i.get("call_id"): i.get("output") for i in seq if i.get("type") == "function_call_output"}
    lines = [
        f"- function_call {i.get('name')}({str(i.get('arguments') or '')[:400]})\n  output: {str(outputs.get(i.get('call_id')) or '')[:400]}"
        for i in seq
        if i.get("type") == "function_call"
    ]
    text = ("[Tool calls made in this turn]\n" + "\n".join(lines))[:2000] + "\n\n" + _item_text(final)
    return {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]}


def record_responses_turn(
    input_items: list[dict[str, Any]],
    output_items: list[dict[str, Any]],
    *,
    scope: str | None = None,
    store: _replay.MessagesReplayStore = RESPONSES_REPLAY,
) -> bool:
    """Record the items after the last user message plus the final output when the tool loop is complete."""
    sc = scope if scope is not None else _replay._scope()
    if not sc:  # no session -> never store (tool outputs must not leak across sessions)
        return False
    last_user = max((i for i, it in enumerate(input_items) if isinstance(it, dict) and _is_user(it)), default=-1)
    finals = [it for it in output_items if isinstance(it, dict) and _is_assistant_text(it)]
    if last_user < 0 or not finals or any(it.get("type") == "function_call" for it in output_items):
        return False
    seq = [copy.deepcopy(it) for it in [*input_items[last_user + 1 :], *output_items] if isinstance(it, dict)]
    if not _complete_loop(seq):
        return False
    store.put(_replay._key(sc, _item_text(input_items[last_user]), _item_text(finals[-1])), seq)
    return True


def rehydrate_responses_history(
    input_items: list[dict[str, Any]],
    *,
    tools_bound: bool,
    scope: str | None = None,
    store: _replay.MessagesReplayStore = RESPONSES_REPLAY,
) -> list[dict[str, Any]]:
    """Replace earlier plain assistant messages with their recorded reasoning/function_call/output sequence."""
    sc = scope if scope is not None else _replay._scope()
    if not sc:
        return list(input_items)
    out: list[dict[str, Any]] = []
    user_text: str | None = None
    for it in input_items:
        if isinstance(it, dict) and _is_user(it):
            user_text = _item_text(it)
        elif isinstance(it, dict) and user_text is not None and _is_assistant_text(it):
            seq = store.get(_replay._key(sc, user_text, _item_text(it)))
            user_text = None
            if seq:
                # function_call without a tools definition is rejected: keep the facts as text instead.
                out.extend(seq if tools_bound else [_summarize_items(seq, seq[-1])])
                continue
        out.append(it)
    return out


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
        session_id: str | None = None,
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

        # T7: re-inject earlier turns' function_call / function_call_output items (session-isolated).
        history = rehydrate_responses_history(input_items, tools_bound=bool(tools), scope=_replay._scope(session_id))
        deduped_items = deduplicate_tool_call_ids(history)
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
                record_responses_turn(payload["input"], response["output"], scope=_replay._scope(kwargs.get("session_id")))
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
