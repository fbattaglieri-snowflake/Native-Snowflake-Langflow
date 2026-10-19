from __future__ import annotations

import copy
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable


ERROR_AUTHZ_ROLE_DENIED = "AUTHZ_ROLE_DENIED"
ERROR_OBJECT_NOT_FOUND_OR_UNAUTHORIZED = "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"
ERROR_UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"
ERROR_TECHNICAL_EXECUTION_ERROR = "TECHNICAL_EXECUTION_ERROR"


class SnowflakeProviderError(Exception):
    """Typed exception enforcing the 4-code governance/provider error taxonomy without silent fallback."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        active_role: str | None = None,
        operation: str | None = None,
        required_privilege: str | None = None,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.active_role = active_role
        self.operation = operation
        self.required_privilege = required_privilege
        self.status_code = status_code
        self.details = details or {}

    @classmethod
    def role_denied(
        cls,
        active_role: str,
        operation: str,
        required_privilege: str | None = None,
    ) -> "SnowflakeProviderError":
        msg = f"Il ruolo attivo (`{active_role}`) non consente questa operazione: {operation}"
        if required_privilege:
            msg += f" (privilegio richiesto: {required_privilege})"
        return cls(
            code=ERROR_AUTHZ_ROLE_DENIED,
            message=msg,
            active_role=active_role,
            operation=operation,
            required_privilege=required_privilege,
            status_code=403,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.code,
            "message": self.message,
            "active_role": self.active_role,
            "operation": self.operation,
            "required_privilege": self.required_privilege,
            "status_code": self.status_code,
            "details": self.details,
        }


@dataclass(frozen=True)
class ModelProfile:
    id: str
    protocol: str
    provider_id: str
    capabilities: tuple[str, ...] = ("text", "tools", "parallel_tools", "streaming")
    requires_reasoning_none: bool = False
    supports_reasoning_effort: bool = True
    supports_native_structured_output: bool = True
    forbids_forced_tool_choice: bool = False
    default_max_tokens: int | None = None
    max_token_limit: int = 128000
    response_stream_mode: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


DEFAULT_CATALOG_MODELS: dict[str, list[ModelProfile]] = {
    "chat": [
        ModelProfile(
            id="glm-5.3",
            protocol="chat",
            provider_id="snowflake_proxy_chat",
            capabilities=("text", "tools", "parallel_tools", "streaming", "structured_output"),
            supports_reasoning_effort=False,
        ),
        ModelProfile(
            id="grok-4.6",
            protocol="chat",
            provider_id="snowflake_proxy_chat",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            supports_reasoning_effort=False,
        ),
        ModelProfile(
            id="grok-4.7",
            protocol="chat",
            provider_id="snowflake_proxy_chat",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            supports_reasoning_effort=False,
        ),
        ModelProfile(
            id="kimi-k3",
            protocol="chat",
            provider_id="snowflake_proxy_chat",
            capabilities=("text", "tools", "parallel_tools", "streaming", "structured_output"),
            supports_reasoning_effort=False,
        ),
        ModelProfile(
            id="openai-gpt-6-luna",
            protocol="chat",
            provider_id="snowflake_proxy_chat",
            capabilities=("text", "tools", "parallel_tools", "streaming", "structured_output"),
            requires_reasoning_none=True,
            supports_reasoning_effort=True,
        ),
        ModelProfile(
            id="openai-gpt-6-sol",
            protocol="chat",
            provider_id="snowflake_proxy_chat",
            capabilities=("text", "tools", "parallel_tools", "streaming", "structured_output"),
            requires_reasoning_none=True,
            supports_reasoning_effort=True,
        ),
    ],
    "responses": [
        ModelProfile(
            id="openai-gpt-6-astra",
            protocol="responses",
            provider_id="snowflake_proxy_responses",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            response_stream_mode="ordered-items",
        ),
        ModelProfile(
            id="openai-gpt-6.1-sol",
            protocol="responses",
            provider_id="snowflake_proxy_responses",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            response_stream_mode="ordered-items",
        ),
    ],
    "messages": [
        ModelProfile(
            id="claude-haiku-5-5",
            protocol="messages",
            provider_id="snowflake_proxy_messages",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            forbids_forced_tool_choice=True,
            default_max_tokens=4096,
        ),
        ModelProfile(
            id="claude-opus-5-5",
            protocol="messages",
            provider_id="snowflake_proxy_messages",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            forbids_forced_tool_choice=True,
            default_max_tokens=4096,
        ),
        ModelProfile(
            id="claude-sonnet-5-5",
            protocol="messages",
            provider_id="snowflake_proxy_messages",
            capabilities=("text", "tools", "parallel_tools", "streaming", "reasoning", "structured_output"),
            forbids_forced_tool_choice=True,
            default_max_tokens=4096,
        ),
    ],
}


def deduplicate_tool_call_ids(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Deterministically rename reused tool_call IDs (e.g. call_0..call_N across multi-round turns)
    to call_0_r2, call_0_r3, etc., while keeping tool result references synchronized.
    """
    normalized = copy.deepcopy(messages)
    closed_ids: set[str] = set()
    round_counts: dict[str, int] = {}
    active_remap: dict[str, str] = {}
    pending_batch_ids: set[str] = set()

    for idx, msg in enumerate(normalized):
        role = msg.get("role") or msg.get("type")

        # Handle OpenAI Chat / LangChain assistant tool_calls
        if role == "assistant" and isinstance(msg.get("tool_calls"), list):
            if pending_batch_ids:
                closed_ids.update(pending_batch_ids)
                pending_batch_ids.clear()
                active_remap.clear()

            current_seen: set[str] = set()
            for tc in msg["tool_calls"]:
                orig_id = tc.get("id") or tc.get("call_id")
                if not orig_id:
                    continue
                if orig_id in current_seen:
                    raise SnowflakeProviderError(
                        ERROR_TECHNICAL_EXECUTION_ERROR,
                        f"Duplicate tool_call id '{orig_id}' within the same assistant turn at message {idx}.",
                    )
                current_seen.add(orig_id)
                round_num = round_counts.get(orig_id, 0) + 1
                round_counts[orig_id] = round_num
                if orig_id in closed_ids or round_num > 1:
                    new_id = f"{orig_id}_r{round_num}"
                    active_remap[orig_id] = new_id
                    if "id" in tc:
                        tc["id"] = new_id
                    if "call_id" in tc:
                        tc["call_id"] = new_id
                    pending_batch_ids.add(new_id)
                else:
                    pending_batch_ids.add(orig_id)
                closed_ids.add(orig_id)

        # Handle Responses API function_call item
        elif msg.get("type") == "function_call":
            orig_id = msg.get("call_id") or msg.get("id")
            if orig_id:
                round_num = round_counts.get(orig_id, 0) + 1
                round_counts[orig_id] = round_num
                if orig_id in closed_ids or round_num > 1:
                    new_id = f"{orig_id}_r{round_num}"
                    active_remap[orig_id] = new_id
                    if "call_id" in msg:
                        msg["call_id"] = new_id
                    if "id" in msg and msg["id"] == orig_id:
                        msg["id"] = new_id
                closed_ids.add(orig_id)

        # Handle Anthropic Messages assistant content blocks with tool_use
        elif role == "assistant" and isinstance(msg.get("content"), list):
            if pending_batch_ids:
                closed_ids.update(pending_batch_ids)
                pending_batch_ids.clear()
                active_remap.clear()

            for block in msg["content"]:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    orig_id = block.get("id")
                    if not orig_id:
                        continue
                    round_num = round_counts.get(orig_id, 0) + 1
                    round_counts[orig_id] = round_num
                    if orig_id in closed_ids or round_num > 1:
                        new_id = f"{orig_id}_r{round_num}"
                        active_remap[orig_id] = new_id
                        block["id"] = new_id
                        pending_batch_ids.add(new_id)
                    else:
                        pending_batch_ids.add(orig_id)
                    closed_ids.add(orig_id)

        # Remap tool output messages
        elif role == "tool" and msg.get("tool_call_id"):
            orig_id = msg["tool_call_id"]
            if orig_id in active_remap:
                msg["tool_call_id"] = active_remap[orig_id]

        elif msg.get("type") == "function_call_output" and msg.get("call_id"):
            orig_id = msg["call_id"]
            if orig_id in active_remap:
                msg["call_id"] = active_remap[orig_id]

        elif role == "user" and isinstance(msg.get("content"), list):
            for block in msg["content"]:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    orig_id = block.get("tool_use_id")
                    if orig_id and orig_id in active_remap:
                        block["tool_use_id"] = active_remap[orig_id]

    return normalized


class SignedBlockReplayCache:
    """
    Bounded LRU replay cache for Responses `reasoning.encrypted_content` and
    Messages `thinking.signature` / `redacted_thinking` blocks.
    Supports branching/invalidation when a signed block is edited or deleted in Settings -> Messages.
    """

    def __init__(self, max_entries: int = 512, ttl_seconds: float = 3600.0) -> None:
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
        self._invalidated_keys: set[str] = set()

    def put(self, key: str, payload: dict[str, Any]) -> None:
        now = time.monotonic()
        self._evict_expired(now)
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = (now, copy.deepcopy(payload))
        self._invalidated_keys.discard(key)
        while len(self._store) > self.max_entries:
            self._store.popitem(last=False)

    def get(self, key: str) -> dict[str, Any] | None:
        if key in self._invalidated_keys:
            return None
        now = time.monotonic()
        self._evict_expired(now)
        entry = self._store.get(key)
        if not entry:
            return None
        ts, payload = entry
        if now - ts > self.ttl_seconds:
            self._store.pop(key, None)
            return None
        self._store.move_to_end(key)
        return copy.deepcopy(payload)

    def invalidate_on_message_edit_or_delete(self, key: str) -> dict[str, Any]:
        """Invalidate a signed block when edited/deleted in Settings -> Messages and return branch metadata."""
        self._store.pop(key, None)
        self._invalidated_keys.add(key)
        return {
            "key": key,
            "invalidated": True,
            "requires_branch": True,
            "reason": "Signed reasoning/thinking block was modified or deleted; replay state branched.",
        }

    def clear(self) -> None:
        self._store.clear()
        self._invalidated_keys.clear()

    def _evict_expired(self, now: float) -> None:
        expired = [k for k, (ts, _) in self._store.items() if now - ts > self.ttl_seconds]
        for k in expired:
            self._store.pop(k, None)


@dataclass
class ProviderDescriptor:
    provider_id: str
    display_name: str
    protocol: str
    base_url: str
    discovery_endpoint: str
    inference_endpoint: str
    icon: str = "Snowflake"
    supports_dynamic_discovery: bool = True


class SnowflakeProviderRegistry:
    """
    Central registry shared across Settings -> Model Providers, Agent, Language Model,
    LLM Operations, and ToolGuard / Policies.
    Maintains distinct native adapters per protocol with zero silent fallback or retry.
    """

    PROVIDER_CHAT = "Snowflake Proxy - Chat"
    PROVIDER_RESPONSES = "Snowflake Proxy - Responses"
    PROVIDER_MESSAGES = "Snowflake Proxy - Messages"
    PROVIDER_OLLAMA = "Ollama (Snowflake Hosted)"

    def __init__(
        self,
        proxy_base_url: str = "<LLM_PROXY_BASE_URL>",
        ollama_base_url: str = "<OLLAMA_BASE_URL>",
        active_role: str = "PUBLIC",
        allowed_roles: set[str] | None = None,
        http_get: Callable[[str, dict[str, str]], dict[str, Any]] | None = None,
    ) -> None:
        self.proxy_base_url = proxy_base_url.rstrip("/")
        self.ollama_base_url = ollama_base_url.rstrip("/")
        self.active_role = active_role
        self.allowed_roles = allowed_roles
        self._http_get = http_get
        self.context_epoch: int = 1
        self.replay_cache = SignedBlockReplayCache()
        self._catalog_cache: dict[tuple[str, str, int], list[ModelProfile]] = {}

    def switch_role(self, new_role: str) -> int:
        if not new_role or not new_role.strip():
            raise SnowflakeProviderError(
                ERROR_AUTHZ_ROLE_DENIED,
                "Active role cannot be empty.",
                active_role=new_role,
                operation="switch_role",
                status_code=403,
            )
        self.active_role = new_role.strip()
        self.context_epoch += 1
        self._catalog_cache.clear()
        self.replay_cache.clear()
        return self.context_epoch

    def check_role_authorized(self, operation: str, required_privilege: str = "USAGE") -> None:
        if self.allowed_roles is not None and self.active_role not in self.allowed_roles:
            raise SnowflakeProviderError.role_denied(
                active_role=self.active_role,
                operation=operation,
                required_privilege=required_privilege,
            )

    def list_providers(self) -> list[ProviderDescriptor]:
        self.check_role_authorized("list_model_providers", "USAGE ON SERVICE LLM_PROXY_STANDARD")
        return [
            ProviderDescriptor(
                provider_id="snowflake_proxy_responses",
                display_name=self.PROVIDER_RESPONSES,
                protocol="responses",
                base_url=f"{self.proxy_base_url}/responses/v1",
                discovery_endpoint=f"{self.proxy_base_url}/responses/v1/models",
                inference_endpoint=f"{self.proxy_base_url}/responses/v1/responses",
                icon="Snowflake",
            ),
            ProviderDescriptor(
                provider_id="snowflake_proxy_messages",
                display_name=self.PROVIDER_MESSAGES,
                protocol="messages",
                base_url=f"{self.proxy_base_url}/messages",
                discovery_endpoint=f"{self.proxy_base_url}/messages/v1/models",
                inference_endpoint=f"{self.proxy_base_url}/messages/v1/messages",
                icon="Snowflake",
            ),
            ProviderDescriptor(
                provider_id="snowflake_proxy_chat",
                display_name=self.PROVIDER_CHAT,
                protocol="chat",
                base_url=f"{self.proxy_base_url}/chat/v1",
                discovery_endpoint=f"{self.proxy_base_url}/chat/v1/models",
                inference_endpoint=f"{self.proxy_base_url}/chat/v1/chat/completions",
                icon="Snowflake",
            ),
            ProviderDescriptor(
                provider_id="snowflake_ollama",
                display_name=self.PROVIDER_OLLAMA,
                protocol="ollama",
                base_url=self.ollama_base_url,
                discovery_endpoint=f"{self.ollama_base_url}/api/tags",
                inference_endpoint=f"{self.ollama_base_url}/api/chat",
                icon="Ollama",
            ),
        ]

    def discover_models(
        self,
        protocol: str,
        *,
        force_refresh: bool = False,
        required_capability: str | None = None,
    ) -> list[ModelProfile]:
        self.check_role_authorized(f"discover_models:{protocol}", "USAGE ON SERVICE LLM_PROXY_STANDARD")
        cache_key = (protocol, self.active_role, self.context_epoch)
        if not force_refresh and cache_key in self._catalog_cache:
            models = self._catalog_cache[cache_key]
        else:
            if self._http_get is not None:
                endpoint_map = {
                    "chat": f"{self.proxy_base_url}/chat/v1/models",
                    "responses": f"{self.proxy_base_url}/responses/v1/models",
                    "messages": f"{self.proxy_base_url}/messages/v1/models",
                    "ollama": f"{self.ollama_base_url}/api/tags",
                }
                if protocol not in endpoint_map:
                    raise SnowflakeProviderError(
                        ERROR_UNSUPPORTED_CAPABILITY,
                        f"Unsupported protocol '{protocol}'.",
                        active_role=self.active_role,
                        operation="discover_models",
                    )
                payload = self._http_get(
                    endpoint_map[protocol],
                    {"X-Snowflake-Role": self.active_role},
                )
                models = self._parse_discovered_models(protocol, payload)
            else:
                if protocol not in DEFAULT_CATALOG_MODELS:
                    raise SnowflakeProviderError(
                        ERROR_UNSUPPORTED_CAPABILITY,
                        f"Unsupported protocol '{protocol}' without live discovery handler.",
                        active_role=self.active_role,
                        operation="discover_models",
                    )
                models = list(DEFAULT_CATALOG_MODELS[protocol])
            self._catalog_cache[cache_key] = models

        if required_capability:
            return [m for m in models if required_capability in m.capabilities]
        return list(models)

    def get_model_profile(self, protocol: str, model_id: str) -> ModelProfile:
        models = self.discover_models(protocol)
        for profile in models:
            if profile.id == model_id:
                return profile
        raise SnowflakeProviderError(
            ERROR_UNSUPPORTED_CAPABILITY,
            f"Model '{model_id}' is not registered or qualified under protocol '{protocol}'. "
            "No silent cross-protocol fallback is permitted.",
            active_role=self.active_role,
            operation=f"get_model_profile:{protocol}:{model_id}",
        )

    def _parse_discovered_models(self, protocol: str, payload: dict[str, Any]) -> list[ModelProfile]:
        defaults_by_id = {
            m.id: m for m in DEFAULT_CATALOG_MODELS.get(protocol, [])
        }
        items = payload.get("data") or payload.get("models") or []
        parsed: list[ModelProfile] = []
        for item in items:
            model_id = item.get("id") or item.get("name")
            if not model_id:
                continue
            if model_id in defaults_by_id:
                parsed.append(defaults_by_id[model_id])
            else:
                caps = tuple(item.get("capabilities", ("text", "tools", "streaming")))
                parsed.append(
                    ModelProfile(
                        id=model_id,
                        protocol=protocol,
                        provider_id=f"snowflake_proxy_{protocol}" if protocol != "ollama" else "snowflake_ollama",
                        capabilities=caps,
                        response_stream_mode="ordered-items" if protocol == "responses" else None,
                    )
                )
        return parsed
