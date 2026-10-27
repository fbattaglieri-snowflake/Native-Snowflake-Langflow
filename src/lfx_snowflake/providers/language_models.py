"""Unified Snowflake Language Model & Agent provider integration (`MA-W2-R6-02` & `MA-W2-R6-03`).

Registers the 4 Snowflake model providers directly into Langflow's native
`Language Model` (`LanguageModelComponent`) and `Agent` (`AgentComponent`) components:
1. `Snowflake Proxy - Chat (/chat/v1)` (`protocol="chat"`)
2. `Snowflake Proxy - Responses (/responses/v1)` (`protocol="responses"`)
3. `Snowflake Proxy - Messages (/messages)` (`protocol="messages"`)
4. `Snowflake Private - Ollama` (`protocol="ollama"`)

Also provides `SnowflakeStructuredTool` (a `langchain_core.tools.StructuredTool` subclass)
for multi-tool `Agent` orchestration with `Cortex Analyst`, `Cortex Search`, and `Cortex Agent`.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import types
from typing import Any, Callable

from pydantic import BaseModel, Field

from lfx_snowflake import standard_langflow
from lfx_snowflake._lfx_compat import Tool as _CompatTool

PROVIDER_PROXY_CHAT = "Snowflake Proxy - Chat (/chat/v1)"
PROVIDER_PROXY_RESPONSES = "Snowflake Proxy - Responses (/responses/v1)"
PROVIDER_PROXY_MESSAGES = "Snowflake Proxy - Messages (/messages)"
PROVIDER_PRIVATE_OLLAMA = "Snowflake Private - Ollama"

SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS: tuple[str, ...] = (
    PROVIDER_PROXY_CHAT,
    PROVIDER_PROXY_RESPONSES,
    PROVIDER_PROXY_MESSAGES,
    PROVIDER_PRIVATE_OLLAMA,
)

SNOWFLAKE_PROVIDER_TO_PROTOCOL: dict[str, str] = {
    PROVIDER_PROXY_CHAT: "chat",
    PROVIDER_PROXY_RESPONSES: "responses",
    PROVIDER_PROXY_MESSAGES: "messages",
    PROVIDER_PRIVATE_OLLAMA: "ollama",
    # Aliases for backward compatibility
    "Proxy - Chat": "chat",
    "Proxy - Responses": "responses",
    "Proxy - Messages": "messages",
    "Private - Ollama": "ollama",
    "Snowflake Proxy - Chat": "chat",
    "Snowflake Proxy - Responses": "responses",
    "Snowflake Proxy - Messages": "messages",
    "chat": "chat",
    "responses": "responses",
    "messages": "messages",
    "ollama": "ollama",
}

PROTOCOL_TO_SNOWFLAKE_PROVIDER: dict[str, str] = {
    "chat": PROVIDER_PROXY_CHAT,
    "responses": PROVIDER_PROXY_RESPONSES,
    "messages": PROVIDER_PROXY_MESSAGES,
    "ollama": PROVIDER_PRIVATE_OLLAMA,
}

# Offline fallback catalog used ONLY when no live proxy endpoint or mock transport is reachable
FALLBACK_OFFLINE_CATALOG: dict[str, dict[str, dict[str, Any]]] = {
    "chat": {
        "openai-gpt-6-luna": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["chat", "streaming", "tool_calling", "reasoning"],
            "reasoning": {"supported": True, "required": True},
        },
        "openai-gpt-6-sol": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["chat", "streaming", "tool_calling", "reasoning"],
            "reasoning": {"supported": True, "required": True},
        },
        "grok-4.7": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["chat", "streaming", "tool_calling"],
            "reasoning": False,
        },
    },
    "responses": {
        "openai-gpt-6-astra": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["responses", "streaming", "tool_calling", "reasoning"],
            "response_stream": "ordered-items",
            "reasoning": {"supported": True, "required": True},
        },
        "openai-gpt-6-luna": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["responses", "streaming", "tool_calling", "reasoning"],
            "response_stream": "ordered-items",
            "reasoning": {"supported": True, "required": True},
        },
    },
    "messages": {
        "claude-sonnet-5-5": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["messages", "streaming", "tool_calling", "extended_thinking"],
            "reasoning": True,
        },
        "claude-opus-5-5": {
            "release_status": "qualified",
            "expires_at": "2099-12-31T23:59:59Z",
            "capabilities": ["messages", "streaming", "tool_calling", "extended_thinking"],
            "reasoning": True,
        },
    },
    "ollama": {
        "llama3.1:8b": {
            "capabilities": ["chat", "tool_calling"],
            "reasoning": False,
        },
    },
}


def sanitize_tool_identifier(raw: str) -> str:
    """Convert a Snowflake object name or FQN into a clean `[a-z0-9_]+` tool identifier segment."""
    cleaned = str(raw or "").strip()
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    cleaned = re.sub(r"\.(yaml|yml)$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", cleaned).strip("_").lower()
    return cleaned or "default"


def _install_or_resolve_structured_tool() -> tuple[type[Any], type[Any]]:
    """Return `(StructuredTool, BaseTool)`, installing a lightweight shim when `langchain_core` is absent offline."""
    if "langchain_core.tools" in sys.modules or importlib.util.find_spec("langchain_core") is not None:
        try:
            from langchain_core.tools import BaseTool as _LCBaseTool
            from langchain_core.tools import StructuredTool as _LCStructuredTool

            return _LCStructuredTool, _LCBaseTool
        except Exception:  # noqa: BLE001
            pass

    class _ShimBaseTool(_CompatTool):
        args_schema: type[BaseModel] | None = None

    class _ShimStructuredTool(_ShimBaseTool):
        def __init__(
            self,
            *,
            name: str,
            description: str,
            func: Callable[..., Any] | None = None,
            coroutine: Callable[..., Any] | None = None,
            args_schema: type[BaseModel] | None = None,
            metadata: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                name=name,
                description=description,
                func=func,
                coroutine=coroutine,
                metadata=metadata,
            )
            self.args_schema = args_schema
            for k, v in kwargs.items():
                setattr(self, k, v)

        @classmethod
        def from_function(
            cls,
            func: Callable[..., Any] | None = None,
            coroutine: Callable[..., Any] | None = None,
            name: str | None = None,
            description: str | None = None,
            return_direct: bool = False,
            args_schema: type[BaseModel] | None = None,
            infer_schema: bool = True,
            metadata: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> _ShimStructuredTool:
            resolved_name = name or getattr(func, "__name__", "structured_tool")
            resolved_desc = description or (getattr(func, "__doc__", None) or resolved_name)
            return cls(
                name=resolved_name,
                description=resolved_desc,
                func=func,
                coroutine=coroutine,
                args_schema=args_schema,
                return_direct=return_direct,
                infer_schema=infer_schema,
                metadata=metadata,
                **kwargs,
            )

        def invoke(self, input: Any = None, config: Any = None, **kwargs: Any) -> Any:  # noqa: A002
            del config
            if callable(self.func):
                if isinstance(input, dict):
                    return self.func(**input, **kwargs)
                if input is None and kwargs:
                    return self.func(**kwargs)
                if input is not None:
                    return self.func(input, **kwargs) if kwargs else self.func(input)
                return self.func()
            return {"tool": self.name, "input": input}

    lc_mod = sys.modules.get("langchain_core") or types.ModuleType("langchain_core")
    lc_tools_mod = sys.modules.get("langchain_core.tools") or types.ModuleType("langchain_core.tools")
    lc_tools_mod.BaseTool = _ShimBaseTool  # type: ignore[attr-defined]
    lc_tools_mod.StructuredTool = _ShimStructuredTool  # type: ignore[attr-defined]
    lc_tools_mod.Tool = _ShimBaseTool  # type: ignore[attr-defined]
    lc_mod.tools = lc_tools_mod  # type: ignore[attr-defined]
    sys.modules["langchain_core"] = lc_mod
    sys.modules["langchain_core.tools"] = lc_tools_mod
    return _ShimStructuredTool, _ShimBaseTool


StructuredTool, BaseTool = _install_or_resolve_structured_tool()


class CortexAnalystToolSchema(BaseModel):
    question: str = Field(
        default="",
        description="Natural-language analytical question to translate into governed SQL via Snowflake Cortex Analyst.",
    )


class CortexSearchToolSchema(BaseModel):
    query: str = Field(
        default="",
        description="Natural-language search query for hybrid lexical and vector retrieval over Snowflake Cortex Search.",
    )
    limit: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Maximum number of search results/chunks to return.",
    )


class CortexAgentToolSchema(BaseModel):
    user_message: str = Field(
        default="",
        description="Natural-language prompt or task to send to the Snowflake Cortex Agent.",
    )


def build_structured_tool(
    *,
    name: str,
    description: str,
    func: Callable[..., Any],
    args_schema: type[BaseModel],
    metadata: dict[str, Any] | None = None,
    coroutine: Callable[..., Any] | None = None,
) -> Any:
    """Create a `langchain_core.tools.StructuredTool` compatible with both LangGraph `create_agent` and direct `.invoke(str | dict)` calls (`MA-W2-R6-03`)."""
    meta_dict = dict(metadata or {})
    schema_fields = list(getattr(args_schema, "model_fields", {}).keys())
    primary_field = schema_fields[0] if schema_fields else "query"

    def _flexible_entrypoint(*args: Any, **kwargs: Any) -> Any:
        if args and isinstance(args[0], dict):
            merged = dict(args[0])
            merged.update(kwargs)
            return func(**merged)
        if args and isinstance(args[0], str):
            call_kwargs = dict(kwargs)
            call_kwargs.setdefault(primary_field, args[0])
            return func(**call_kwargs)
        return func(*args, **kwargs)

    tool_obj = StructuredTool.from_function(
        func=_flexible_entrypoint,
        coroutine=coroutine,
        name=name,
        description=description,
        args_schema=args_schema,
        metadata=meta_dict,
    )
    if getattr(tool_obj, "metadata", None) is None:
        tool_obj.metadata = meta_dict
    else:
        tool_obj.metadata.update(meta_dict)

    orig_invoke = getattr(tool_obj, "invoke", None)
    if callable(orig_invoke):

        def _patched_invoke(input_val: Any = None, config: Any = None, **kw: Any) -> Any:
            if isinstance(input_val, str):
                return _flexible_entrypoint(input_val, **kw)
            if input_val is None and not kw:
                return _flexible_entrypoint()
            if isinstance(input_val, dict):
                try:
                    return orig_invoke(input_val, config=config, **kw)
                except TypeError:
                    return orig_invoke(input_val, **kw)
            return _flexible_entrypoint(input_val, **kw)

        try:
            object.__setattr__(tool_obj, "invoke", _patched_invoke)
        except Exception:  # noqa: BLE001
            pass

    return tool_obj


def _build_option_entry(
    protocol: str,
    model_id: str,
    raw_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provider_name = PROTOCOL_TO_SNOWFLAKE_PROVIDER[protocol]
    meta = dict(raw_metadata or {})
    caps = list(meta.get("capabilities") or [])
    supports_reasoning, reasoning_required = standard_langflow.model_supports_reasoning(
        protocol,
        model_id,
        metadata=meta,
    )
    tool_calling = bool(
        meta.get("tool_calling", True)
        if "tool_calling" in meta
        else ("no_tools" not in caps and "no_tool_calling" not in caps)
    )
    option_meta: dict[str, Any] = {
        **meta,
        "protocol": protocol,
        "model_type": "llm",
        "default": True,
        "tool_calling": tool_calling,
        "reasoning": supports_reasoning,
        "reasoning_required": reasoning_required,
        "context_length": int(meta.get("context_length", 128000)),
        "model_class": "ProtectedAnthropic" if protocol == "messages" else "ChatOpenAI",
        "model_name_param": "model",
        "api_key_param": "api_key",
    }
    if supports_reasoning:
        option_meta["reasoning_models"] = [model_id]
    return {
        "name": model_id,
        "model_name": model_id,
        "provider": provider_name,
        "category": provider_name,
        "icon": "Snowflake",
        "default": True,
        "metadata": option_meta,
    }


def get_snowflake_live_language_model_catalog(
    *,
    force_refresh: bool = False,
    tool_calling: bool | None = None,
    protocols: tuple[str, ...] = ("chat", "responses", "messages", "ollama"),
    strict_live: bool = False,
) -> list[dict[str, Any]]:
    """Return live model option dicts across the 4 Snowflake providers (`MA-W2-R6-02`)."""
    options: list[dict[str, Any]] = []
    for protocol in protocols:
        live_map: dict[str, dict[str, Any]] = {}
        try:
            live_map = standard_langflow.catalog_with_metadata(protocol, force_refresh=force_refresh)
        except Exception:  # noqa: BLE001
            if strict_live:
                raise
            base_url = standard_langflow._get_base(protocol)
            if "<PROXY_SERVICE>" in base_url or "<OLLAMA_SERVICE>" in base_url or not hasattr(
                standard_langflow.provider_httpx_clients(base_url).get("http_client"), "get"
            ):
                live_map = dict(FALLBACK_OFFLINE_CATALOG.get(protocol, {}))
            else:
                live_map = {}

        for model_id, model_meta in live_map.items():
            entry = _build_option_entry(protocol, model_id, model_meta)
            if tool_calling is True and not entry["metadata"].get("tool_calling", True):
                continue
            options.append(entry)
    return options


def get_snowflake_reasoning_inputs() -> list[Any]:
    """Return the 3 dynamic Snowflake Proxy reasoning/thinking inputs for `LanguageModelComponent` and `AgentComponent`."""
    from lfx.io import DropdownInput, IntInput

    return [
        DropdownInput(
            name="reasoning_effort",
            display_name="Reasoning Effort",
            options=list(standard_langflow.VALID_CHAT_REASONING_EFFORTS),
            value="unspecified",
            show=False,
            advanced=False,
            real_time_refresh=True,
            info="Reasoning effort forwarded to Snowflake Proxy (/chat/v1 or /responses/v1) when supported by the selected model.",
        ),
        DropdownInput(
            name="extended_thinking",
            display_name="Extended Thinking",
            options=["disabled", "enabled"],
            value="disabled",
            show=False,
            advanced=False,
            real_time_refresh=True,
            info="Enable Claude Extended Thinking on Snowflake Proxy - Messages (/messages).",
        ),
        IntInput(
            name="thinking_budget_tokens",
            display_name="Thinking Budget Tokens (1024..32768)",
            value=2048,
            show=False,
            advanced=False,
            info="Token budget for Claude Extended Thinking (1024..32768). max_tokens is auto-raised to >= budget + 1024.",
        ),
    ]


def _ensure_dynamic_fields_in_build_config(build_config: dict[str, Any]) -> None:
    if "reasoning_effort" not in build_config:
        build_config["reasoning_effort"] = {
            "name": "reasoning_effort",
            "display_name": "Reasoning Effort",
            "options": list(standard_langflow.VALID_CHAT_REASONING_EFFORTS),
            "value": "unspecified",
            "show": False,
            "advanced": False,
            "real_time_refresh": True,
            "info": "Reasoning effort forwarded to Snowflake Proxy (/chat/v1 or /responses/v1) when supported by the selected model.",
            "_input_type": "DropdownInput",
        }
    if "extended_thinking" not in build_config:
        build_config["extended_thinking"] = {
            "name": "extended_thinking",
            "display_name": "Extended Thinking",
            "options": ["disabled", "enabled"],
            "value": "disabled",
            "show": False,
            "advanced": False,
            "real_time_refresh": True,
            "info": "Enable Claude Extended Thinking on Snowflake Proxy - Messages (/messages).",
            "_input_type": "DropdownInput",
        }
    if "thinking_budget_tokens" not in build_config:
        build_config["thinking_budget_tokens"] = {
            "name": "thinking_budget_tokens",
            "display_name": "Thinking Budget Tokens (1024..32768)",
            "value": 2048,
            "show": False,
            "advanced": False,
            "real_time_refresh": False,
            "info": "Token budget for Claude Extended Thinking (1024..32768). max_tokens is auto-raised to >= budget + 1024.",
            "_input_type": "IntInput",
        }


def _extract_selected_model_entry(
    build_config: dict[str, Any],
    field_value: Any = None,
    field_name: str | None = None,
    model_field_name: str = "model",
) -> dict[str, Any] | None:
    candidate = field_value if (field_name == model_field_name and field_value) else None
    if candidate is None:
        candidate = build_config.get(model_field_name, {}).get("value")
    if isinstance(candidate, list) and candidate:
        first = candidate[0]
        if isinstance(first, dict):
            return dict(first)
        if isinstance(first, str) and first.strip():
            return {"name": first.strip(), "provider": build_config.get("provider", {}).get("value") or PROVIDER_PROXY_CHAT}
    if isinstance(candidate, dict):
        return dict(candidate)
    if isinstance(candidate, str) and candidate.strip():
        return {"name": candidate.strip(), "provider": build_config.get("provider", {}).get("value") or PROVIDER_PROXY_CHAT}
    return None


def update_snowflake_language_model_build_config(
    build_config: dict[str, Any],
    field_value: Any = None,
    field_name: str | None = None,
    *,
    model_field_name: str = "model",
    force_refresh: bool | None = None,
) -> dict[str, Any]:
    """Apply Snowflake provider catalog refresh and Table 3.2 dynamic visibility rules on `build_config` (`MA-W2-R6-02`)."""
    _ensure_dynamic_fields_in_build_config(build_config)
    should_force_refresh = force_refresh if force_refresh is not None else (field_name in (None, model_field_name))

    # Preserve prior selection before refreshing options so expired/removed models are never silently replaced
    selected_entry = _extract_selected_model_entry(
        build_config,
        field_value=field_value,
        field_name=field_name,
        model_field_name=model_field_name,
    )

    sf_live_options = get_snowflake_live_language_model_catalog(force_refresh=bool(should_force_refresh))
    if model_field_name in build_config and isinstance(build_config[model_field_name], dict):
        existing_opts = list(build_config[model_field_name].get("options") or [])
        non_sf_opts = [
            opt
            for opt in existing_opts
            if isinstance(opt, dict) and opt.get("provider") not in SNOWFLAKE_PROVIDER_TO_PROTOCOL
        ]
        merged_options = sf_live_options + non_sf_opts
        build_config[model_field_name]["options"] = merged_options

    if selected_entry is None:
        selected_entry = _extract_selected_model_entry(
            build_config,
            field_value=None,
            field_name=None,
            model_field_name=model_field_name,
        )

    selected_provider = str((selected_entry or {}).get("provider") or "").strip()
    selected_model_name = str(
        (selected_entry or {}).get("name") or (selected_entry or {}).get("model_name") or ""
    ).strip()
    selected_meta = dict((selected_entry or {}).get("metadata") or {})
    protocol = SNOWFLAKE_PROVIDER_TO_PROTOCOL.get(selected_provider)

    if protocol is not None:
        # 1. Safeguard existing flows: never silently replace a saved Snowflake model if removed/expired from live catalog
        live_names_for_protocol = {
            str(opt.get("name") or "")
            for opt in sf_live_options
            if SNOWFLAKE_PROVIDER_TO_PROTOCOL.get(str(opt.get("provider") or "")) == protocol
        }
        if model_field_name in build_config and isinstance(build_config[model_field_name], dict):
            if selected_model_name:
                # Keep the user's saved selection intact
                matching_live = next(
                    (
                        opt
                        for opt in sf_live_options
                        if opt.get("name") == selected_model_name
                        and SNOWFLAKE_PROVIDER_TO_PROTOCOL.get(str(opt.get("provider") or "")) == protocol
                    ),
                    None,
                )
                if matching_live is not None:
                    selected_meta = dict(matching_live.get("metadata") or selected_meta)
                    build_config[model_field_name]["value"] = [dict(matching_live)]
                    build_config[model_field_name]["info"] = (
                        "Live published catalog. Refresh to see new models; saved selection never changes automatically."
                    )
                else:
                    build_config[model_field_name]["value"] = [
                        {
                            "name": selected_model_name,
                            "model_name": selected_model_name,
                            "provider": PROTOCOL_TO_SNOWFLAKE_PROVIDER[protocol],
                            "category": PROTOCOL_TO_SNOWFLAKE_PROVIDER[protocol],
                            "icon": "Snowflake",
                            "metadata": selected_meta,
                        }
                    ]
                    build_config[model_field_name]["info"] = (
                        f"Saved model '{selected_model_name}' is no longer in the live published catalog; "
                        "choose a model explicitly"
                    )

        # 2. Hide API Key for all 4 Snowflake providers (authenticated internally on SPCS)
        if "api_key" in build_config and isinstance(build_config["api_key"], dict):
            build_config["api_key"]["show"] = False
            build_config["api_key"]["required"] = False
            build_config["api_key"]["value"] = ""

        # 3. Protocol & model specific reasoning/thinking controls (Table 3.2)
        if protocol == "chat":
            supports_reasoning, reasoning_required = standard_langflow.model_supports_reasoning(
                "chat",
                selected_model_name,
                metadata=selected_meta,
            )
            re_cfg = build_config["reasoning_effort"]
            re_cfg["options"] = list(standard_langflow.VALID_CHAT_REASONING_EFFORTS)
            if supports_reasoning:
                re_cfg["show"] = True
                cur_val = (
                    field_value
                    if field_name == "reasoning_effort" and isinstance(field_value, str)
                    else re_cfg.get("value")
                )
                if reasoning_required and cur_val in (None, "", "unspecified"):
                    re_cfg["value"] = "none"
                elif cur_val in standard_langflow.VALID_CHAT_REASONING_EFFORTS:
                    re_cfg["value"] = cur_val
                else:
                    re_cfg["value"] = "none" if reasoning_required else "unspecified"
            else:
                re_cfg["show"] = False
                re_cfg["value"] = "unspecified"
            build_config["extended_thinking"]["show"] = False
            build_config["thinking_budget_tokens"]["show"] = False

        elif protocol == "responses":
            re_cfg = build_config["reasoning_effort"]
            re_cfg["show"] = True
            re_cfg["options"] = ["low", "medium", "high"]
            cur_val = (
                field_value
                if field_name == "reasoning_effort" and isinstance(field_value, str)
                else re_cfg.get("value")
            )
            re_cfg["value"] = cur_val if cur_val in ("low", "medium", "high") else "medium"
            build_config["extended_thinking"]["show"] = False
            build_config["thinking_budget_tokens"]["show"] = False

        elif protocol == "messages":
            build_config["reasoning_effort"]["show"] = False
            et_cfg = build_config["extended_thinking"]
            et_cfg["show"] = True
            et_cfg["options"] = ["disabled", "enabled"]
            cur_et = (
                field_value
                if field_name == "extended_thinking" and isinstance(field_value, str)
                else et_cfg.get("value")
            )
            et_cfg["value"] = cur_et if cur_et in ("disabled", "enabled") else "disabled"

            tb_cfg = build_config["thinking_budget_tokens"]
            tb_cfg["show"] = et_cfg["value"] == "enabled"
            raw_budget = (
                field_value
                if field_name == "thinking_budget_tokens" and field_value is not None
                else tb_cfg.get("value", 2048)
            )
            try:
                tb_cfg["value"] = max(1024, min(32768, int(raw_budget or 2048)))
            except (TypeError, ValueError):
                tb_cfg["value"] = 2048

        elif protocol == "ollama":
            build_config["reasoning_effort"]["show"] = False
            build_config["extended_thinking"]["show"] = False
            build_config["thinking_budget_tokens"]["show"] = False
            if "temperature" in build_config and isinstance(build_config["temperature"], dict):
                build_config["temperature"]["show"] = True
            if "max_tokens" in build_config and isinstance(build_config["max_tokens"], dict):
                build_config["max_tokens"]["show"] = True
    else:
        # External provider (OpenAI, Anthropic, Google, etc.)
        if selected_provider and "api_key" in build_config and isinstance(build_config["api_key"], dict):
            if selected_provider != "Ollama":
                build_config["api_key"]["show"] = True
        build_config["reasoning_effort"]["show"] = False
        build_config["extended_thinking"]["show"] = False
        build_config["thinking_budget_tokens"]["show"] = False

    return build_config


def build_snowflake_language_model(
    *,
    provider: str,
    model_name: str,
    stream: bool = False,
    max_tokens: int | None = 2048,
    temperature: float | None = None,
    reasoning_effort: str | None = None,
    extended_thinking: str = "disabled",
    thinking_budget_tokens: int = 2048,
) -> Any:
    """Instantiate a Snowflake Proxy or Private Ollama LangChain chat model via `standard_langflow.build()` (`MA-W2-R6-02`)."""
    protocol = SNOWFLAKE_PROVIDER_TO_PROTOCOL.get(provider)
    if not protocol:
        raise ValueError(f"Unsupported Snowflake language model provider: {provider}")
    effective_max_tokens = int(max_tokens) if (max_tokens is not None and str(max_tokens).strip() and int(max_tokens) > 0) else 2048
    effective_stream = False if protocol == "ollama" else bool(stream)
    return standard_langflow.build(
        protocol,
        model_name,
        stream=effective_stream,
        reasoning=reasoning_effort or "unspecified",
        max_tokens=effective_max_tokens,
        reasoning_effort=reasoning_effort,
        extended_thinking=extended_thinking or "disabled",
        thinking_budget_tokens=int(thinking_budget_tokens or 2048),
        temperature=temperature,
    )


def register_snowflake_language_model_providers() -> dict[str, Any]:
    """Register the 4 Snowflake Language Model providers into Langflow's `unified_models`, `LanguageModelComponent`, and `AgentComponent` (`MA-W2-R6-02` & `MA-W2-R6-03`)."""
    status: dict[str, Any] = {
        "providers": list(SNOWFLAKE_LANGUAGE_MODEL_PROVIDERS),
        "provider_registry_registered": False,
        "unified_models_patched": False,
        "language_model_component_patched": False,
        "agent_component_patched": False,
    }

    # 1. Register ProviderDescriptors in lfx.base.models.provider_registry if available
    try:
        from lfx.base.models import provider_registry as _pr

        if hasattr(_pr, "ProviderDescriptor") and hasattr(_pr, "register_provider"):
            for prov_name, proto in (
                (PROVIDER_PROXY_CHAT, "chat"),
                (PROVIDER_PROXY_RESPONSES, "responses"),
                (PROVIDER_PROXY_MESSAGES, "messages"),
                (PROVIDER_PRIVATE_OLLAMA, "ollama"),
            ):
                model_cls_name = "ProtectedAnthropic" if proto == "messages" else "ChatOpenAI"
                descriptor = _pr.ProviderDescriptor(
                    name=prov_name,
                    metadata={
                        "icon": "Snowflake",
                        "variables": [],
                        "mapping": {
                            "model_class": model_cls_name,
                            "model_param": "model",
                            "api_key_param": "api_key",
                        },
                    },
                    api_key_required=False,
                )
                try:
                    _pr.register_provider(descriptor)
                except ValueError:
                    pass
            status["provider_registry_registered"] = True
    except Exception:  # noqa: BLE001
        pass

    # 2. Patch unified_models: get_language_model_options, handle_model_input_update, get_llm
    try:
        from lfx.base.models import unified_models as _um

        orig_get_opts = getattr(_um, "get_language_model_options", None)
        if callable(orig_get_opts) and not getattr(orig_get_opts, "__sflf_lm_wrapped__", False):

            def _wrapped_get_language_model_options(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
                filters = kwargs.get("filters") or {}
                tool_calling = kwargs.get("tool_calling")
                if tool_calling is None and isinstance(filters, dict):
                    tool_calling = filters.get("tool_calling")
                try:
                    base_opts = list(orig_get_opts(*args, **kwargs))
                except Exception:  # noqa: BLE001
                    base_opts = []
                sf_opts = get_snowflake_live_language_model_catalog(
                    force_refresh=False,
                    tool_calling=tool_calling,
                )
                non_sf = [
                    opt
                    for opt in base_opts
                    if isinstance(opt, dict) and opt.get("provider") not in SNOWFLAKE_PROVIDER_TO_PROTOCOL
                ]
                return sf_opts + non_sf

            setattr(_wrapped_get_language_model_options, "__sflf_lm_wrapped__", True)
            setattr(_um, "get_language_model_options", _wrapped_get_language_model_options)
            try:
                from lfx.base.models.unified_models import model_catalog as _mc

                setattr(_mc, "get_language_model_options", _wrapped_get_language_model_options)
            except Exception:  # noqa: BLE001
                pass

        orig_handle_update = getattr(_um, "handle_model_input_update", None)
        if callable(orig_handle_update) and not getattr(orig_handle_update, "__sflf_lm_wrapped__", False):

            def _wrapped_handle_model_input_update(
                component: Any,
                build_config: dict[str, Any],
                field_value: Any,
                field_name: str | None = None,
                **kwargs: Any,
            ) -> dict[str, Any]:
                model_field_name = kwargs.get("model_field_name", "model")
                prior_entry = _extract_selected_model_entry(
                    build_config,
                    field_value=field_value,
                    field_name=field_name,
                    model_field_name=model_field_name,
                )
                updated = orig_handle_update(component, build_config, field_value, field_name, **kwargs)
                if prior_entry and prior_entry.get("provider") in SNOWFLAKE_PROVIDER_TO_PROTOCOL:
                    # Restore prior Snowflake selection before applying Snowflake build_config rules
                    if model_field_name in updated and isinstance(updated[model_field_name], dict):
                        updated[model_field_name]["value"] = [prior_entry]
                return update_snowflake_language_model_build_config(
                    updated,
                    field_value=field_value,
                    field_name=field_name,
                    model_field_name=model_field_name,
                )

            setattr(_wrapped_handle_model_input_update, "__sflf_lm_wrapped__", True)
            setattr(_um, "handle_model_input_update", _wrapped_handle_model_input_update)
            try:
                from lfx.base.models.unified_models import build_config as _bc_mod

                setattr(_bc_mod, "handle_model_input_update", _wrapped_handle_model_input_update)
            except Exception:  # noqa: BLE001
                pass

        orig_get_llm = getattr(_um, "get_llm", None)
        if callable(orig_get_llm) and not getattr(orig_get_llm, "__sflf_lm_wrapped__", False):

            def _wrapped_get_llm(
                model: Any,
                user_id: Any = None,
                api_key: Any = None,
                temperature: Any = None,
                *,
                stream: bool = False,
                max_tokens: Any = None,
                watsonx_url: Any = None,
                watsonx_project_id: Any = None,
                ollama_base_url: Any = None,
                overrides: dict[str, Any] | None = None,
                provider_policy: Any = None,
                reasoning_effort: str | None = None,
                extended_thinking: str = "disabled",
                thinking_budget_tokens: int = 2048,
            ) -> Any:
                selected_dict: dict[str, Any] | None = None
                if isinstance(model, list) and model and isinstance(model[0], dict):
                    selected_dict = model[0]
                elif isinstance(model, dict):
                    selected_dict = model

                if selected_dict is not None:
                    prov = str(selected_dict.get("provider") or "").strip()
                    m_name = str(selected_dict.get("name") or selected_dict.get("model_name") or "").strip()
                    meta = dict(selected_dict.get("metadata") or {})
                    if prov in SNOWFLAKE_PROVIDER_TO_PROTOCOL and m_name:
                        ov = dict(overrides or {})
                        eff_reasoning = (
                            reasoning_effort
                            or ov.get("reasoning_effort")
                            or meta.get("reasoning_effort")
                            or "unspecified"
                        )
                        eff_thinking = (
                            extended_thinking
                            if extended_thinking != "disabled"
                            else str(ov.get("extended_thinking") or meta.get("extended_thinking") or "disabled")
                        )
                        eff_budget = int(
                            thinking_budget_tokens
                            if thinking_budget_tokens != 2048
                            else (ov.get("thinking_budget_tokens") or meta.get("thinking_budget_tokens") or 2048)
                        )
                        return build_snowflake_language_model(
                            provider=prov,
                            model_name=m_name,
                            stream=stream,
                            max_tokens=max_tokens,
                            temperature=temperature,
                            reasoning_effort=eff_reasoning,
                            extended_thinking=eff_thinking,
                            thinking_budget_tokens=eff_budget,
                        )

                return orig_get_llm(
                    model=model,
                    user_id=user_id,
                    api_key=api_key,
                    temperature=temperature,
                    stream=stream,
                    max_tokens=max_tokens,
                    watsonx_url=watsonx_url,
                    watsonx_project_id=watsonx_project_id,
                    ollama_base_url=ollama_base_url,
                    overrides=overrides,
                    provider_policy=provider_policy,
                )

            setattr(_wrapped_get_llm, "__sflf_lm_wrapped__", True)
            setattr(_um, "get_llm", _wrapped_get_llm)
            try:
                from lfx.base.models.unified_models import instantiation as _inst_mod

                setattr(_inst_mod, "get_llm", _wrapped_get_llm)
            except Exception:  # noqa: BLE001
                pass

        status["unified_models_patched"] = True
    except Exception:  # noqa: BLE001
        pass

    # 3. Patch LanguageModelComponent in-memory if importable
    try:
        from lfx.components.models_and_agents import language_model as _lm_mod

        lm_cls = getattr(_lm_mod, "LanguageModelComponent", None)
        if lm_cls is not None:
            existing_names = {getattr(inp, "name", "") for inp in getattr(lm_cls, "inputs", [])}
            for sf_inp in get_snowflake_reasoning_inputs():
                if sf_inp.name not in existing_names:
                    lm_cls.inputs.append(sf_inp)

            if hasattr(_um, "get_language_model_options"):
                setattr(_lm_mod, "get_language_model_options", _um.get_language_model_options)
            if hasattr(_um, "handle_model_input_update"):
                setattr(_lm_mod, "handle_model_input_update", _um.handle_model_input_update)
            if hasattr(_um, "get_llm"):
                setattr(_lm_mod, "get_llm", _um.get_llm)

            def _patched_lm_build_model(self: Any) -> Any:
                from lfx.components.models_and_agents.model_selection import apply_model_overrides

                model = apply_model_overrides(
                    self.model,
                    model_name=getattr(self, "model_name", None),
                    provider=getattr(self, "provider", None),
                    user_id=getattr(self, "user_id", None),
                    get_options=_um.get_language_model_options,
                )
                return _um.get_llm(
                    model=model,
                    user_id=getattr(self, "user_id", None),
                    api_key=getattr(self, "api_key", None),
                    temperature=getattr(self, "temperature", None),
                    stream=bool(getattr(self, "stream", False)),
                    max_tokens=getattr(self, "max_tokens", None),
                    watsonx_url=getattr(self, "base_url_ibm_watsonx", None),
                    watsonx_project_id=getattr(self, "project_id", None),
                    ollama_base_url=getattr(self, "ollama_base_url", None),
                    reasoning_effort=getattr(self, "reasoning_effort", None),
                    extended_thinking=getattr(self, "extended_thinking", "disabled"),
                    thinking_budget_tokens=int(getattr(self, "thinking_budget_tokens", 2048) or 2048),
                )

            setattr(lm_cls, "build_model", _patched_lm_build_model)
            status["language_model_component_patched"] = True
    except Exception:  # noqa: BLE001
        pass

    # 4. Patch AgentComponent in-memory if importable (including multi-tool `tools` is_list=True)
    try:
        from lfx.components.models_and_agents import agent as _ag_mod

        ag_cls = getattr(_ag_mod, "AgentComponent", None)
        if ag_cls is not None:
            existing_names = {getattr(inp, "name", "") for inp in getattr(ag_cls, "inputs", [])}
            for sf_inp in get_snowflake_reasoning_inputs():
                if sf_inp.name not in existing_names:
                    ag_cls.inputs.append(sf_inp)

            for inp in getattr(ag_cls, "inputs", []):
                if getattr(inp, "name", "") == "tools":
                    inp.is_list = True
                    cur_types = list(getattr(inp, "input_types", None) or ["Tool"])
                    for req_t in ("Tool", "StructuredTool", "BaseTool"):
                        if req_t not in cur_types:
                            cur_types.append(req_t)
                    inp.input_types = cur_types

            if hasattr(_um, "get_language_model_options"):
                setattr(_ag_mod, "get_language_model_options", _um.get_language_model_options)
            if hasattr(_um, "handle_model_input_update"):
                setattr(_ag_mod, "handle_model_input_update", _um.handle_model_input_update)
            if hasattr(_um, "get_llm"):
                setattr(_ag_mod, "get_llm", _um.get_llm)

            def _patched_agent_get_llm(self: Any) -> Any:
                return _um.get_llm(
                    model=self.model,
                    user_id=getattr(self, "user_id", None),
                    api_key=getattr(self, "api_key", None),
                    stream=True,
                    max_tokens=self._get_max_tokens_value() if hasattr(self, "_get_max_tokens_value") else getattr(self, "max_tokens", None),
                    watsonx_url=getattr(self, "base_url_ibm_watsonx", None),
                    watsonx_project_id=getattr(self, "project_id", None),
                    overrides=getattr(self, "_model_overrides", None),
                    reasoning_effort=getattr(self, "reasoning_effort", None),
                    extended_thinking=getattr(self, "extended_thinking", "disabled"),
                    thinking_budget_tokens=int(getattr(self, "thinking_budget_tokens", 2048) or 2048),
                )

            setattr(ag_cls, "_get_llm", _patched_agent_get_llm)
            status["agent_component_patched"] = True
    except Exception:  # noqa: BLE001
        pass

    return status
