"""Sanitized in-repo mirror of `standard_langflow` for `W2-R2-07` reasoning & token validation."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import cached_property
import os
import time
from typing import Any
import weakref

try:
    from lfx.base.models.provider_ssrf import provider_httpx_clients
except ImportError:  # pragma: no cover
    try:
        import httpx

        def provider_httpx_clients(base_url: str) -> dict:
            return {
                "http_client": httpx.Client(base_url=base_url),
                "http_async_client": httpx.AsyncClient(base_url=base_url),
            }
    except ImportError:
        class _DummyHttpClient:
            def __init__(self, base_url: str = "") -> None:
                self.base_url = base_url

            def close(self) -> None:
                pass

            async def aclose(self) -> None:
                pass

        def provider_httpx_clients(base_url: str) -> dict:
            return {
                "http_client": _DummyHttpClient(base_url=base_url),
                "http_async_client": _DummyHttpClient(base_url=base_url),
            }


class _FallbackChatOpenAI:
    def __init__(self, **kwargs):
        if "reasoning_effort" in kwargs:
            raise ValueError(
                "1 validation error for ChatOpenAI\nreasoning_effort\n"
                "  Extra inputs are not permitted [type=extra_forbidden]"
            )
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FallbackChatAnthropic:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


try:
    from langchain_openai import ChatOpenAI
except ImportError:  # pragma: no cover
    ChatOpenAI = _FallbackChatOpenAI  # type: ignore[assignment,misc]

try:
    from langchain_anthropic import ChatAnthropic
except ImportError:  # pragma: no cover
    ChatAnthropic = _FallbackChatAnthropic  # type: ignore[assignment,misc]

try:
    from pydantic import PrivateAttr
except ImportError:  # pragma: no cover
    def PrivateAttr(default=None):  # type: ignore[misc]
        return default

try:
    import anthropic
except ImportError:  # pragma: no cover
    anthropic = None  # type: ignore[assignment]

def _resolve_proxy_origin() -> str:
    for env_key in ("SNOWFLAKE_STANDARD_PROXY_ORIGIN", "LLM_PROXY_BASE_URL", "SNOWFLAKE_PROXY_BASE_URL"):
        val = (os.environ.get(env_key) or "").strip().rstrip("/")
        if val and "<PROXY_SERVICE>" not in val:
            if val.endswith("/v1"):
                val = val[:-3].rstrip("/")
            return val
    allowed = (os.environ.get("LANGFLOW_SSRF_ALLOWED_HOSTS") or "").strip()
    if allowed:
        for candidate in (p.strip() for p in allowed.split(",") if p.strip()):
            if "ollama" not in candidate.lower() and "<" not in candidate:
                if "://" in candidate:
                    return candidate.rstrip("/")
                port = ":8080" if ":" not in candidate else ""
                return f"http://{candidate}{port}"
    return "http://<PROXY_SERVICE>.<DNS_NAMESPACE>:8080"


def _resolve_ollama_origin() -> str:
    for env_key in ("SNOWFLAKE_OLLAMA_ORIGIN", "OLLAMA_BASE_URL", "SNOWFLAKE_OLLAMA_BASE_URL"):
        val = (os.environ.get(env_key) or "").strip().rstrip("/")
        if val and "<OLLAMA_SERVICE>" not in val:
            return val if val.endswith("/v1") else f"{val}/v1"
    allowed = (os.environ.get("LANGFLOW_SSRF_ALLOWED_HOSTS") or "").strip()
    if allowed:
        for candidate in (p.strip() for p in allowed.split(",") if p.strip()):
            if "ollama" in candidate.lower() and "<" not in candidate:
                base = (
                    candidate.rstrip("/")
                    if "://" in candidate
                    else f"http://{candidate}{':11434' if ':' not in candidate else ''}"
                )
                return base if base.endswith("/v1") else f"{base}/v1"
    return "http://<OLLAMA_SERVICE>.<DNS_NAMESPACE>:11434/v1"


ORIGIN = _resolve_proxy_origin()
OLLAMA_ORIGIN = _resolve_ollama_origin()
BASES = {
    'chat': ORIGIN + '/chat/v1',
    'responses': ORIGIN + '/responses/v1',
    'messages': ORIGIN + '/messages',
    'ollama': OLLAMA_ORIGIN,
}


def _get_base(protocol: str) -> str:
    """Resolve and synchronize `BASES[protocol]` dynamically from environment variables if placeholders were present at import time."""
    global ORIGIN, OLLAMA_ORIGIN
    cur_base = BASES.get(protocol, "")
    if not cur_base or "<PROXY_SERVICE>" in cur_base or "<OLLAMA_SERVICE>" in cur_base:
        new_origin = _resolve_proxy_origin()
        new_ollama = _resolve_ollama_origin()
        if "<PROXY_SERVICE>" not in new_origin:
            ORIGIN = new_origin
            BASES["chat"] = ORIGIN + "/chat/v1"
            BASES["responses"] = ORIGIN + "/responses/v1"
            BASES["messages"] = ORIGIN + "/messages"
        if "<OLLAMA_SERVICE>" not in new_ollama:
            OLLAMA_ORIGIN = new_ollama
            BASES["ollama"] = OLLAMA_ORIGIN
    return BASES[protocol]

CHAT_REASONING_REQUIRED_MODELS = ('openai-gpt-6-luna', 'openai-gpt-6-sol')
CHAT_REASONING_UNSUPPORTED_MODELS = ('grok-4.6', 'grok-4.7', 'kimi-k3', 'glm-5.3')
VALID_CHAT_REASONING_EFFORTS = ('unspecified', 'none', 'low', 'medium', 'high')
VALID_RESPONSES_REASONING_EFFORTS = ('unspecified', 'low', 'medium', 'high')

CATALOG_CACHE_TTL_SECONDS = 15.0
OLLAMA_NEGATIVE_CACHE_TTL_SECONDS = 1.5
_CATALOG_CACHE: dict[tuple[str, bool], tuple[float, dict[str, dict[str, Any]]]] = {}
_CATALOG_NEGATIVE_CACHE: dict[tuple[str, bool], tuple[float, Exception]] = {}


def clear_catalog_cache() -> None:
    """Clear the in-memory live model catalog cache (`MA-W2-R6-02`)."""
    _CATALOG_CACHE.clear()
    _CATALOG_NEGATIVE_CACHE.clear()


def close_clients(clients):
    try:
        clients['http_client'].close()
    finally:
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(lambda: asyncio.run(clients['http_async_client'].aclose())).result()
        except RuntimeError:
            pass


@contextmanager
def discovery_transport(protocol):
    clients = provider_httpx_clients(_get_base(protocol))
    if not {'http_client', 'http_async_client'} <= clients.keys():
        raise ValueError('STANDARD_PROTECTED_TRANSPORT_REQUIRED')
    try:
        yield clients['http_client']
    finally:
        close_clients(clients)


def catalog_with_metadata(
    protocol: str,
    *,
    stream: bool = False,
    force_refresh: bool = False,
) -> dict[str, dict[str, Any]]:
    """Query the live `/models` endpoint for `protocol` and return `{model_id: metadata_dict}` with a 15s TTL cache (`MA-W2-R6-02`)."""
    cache_key = (protocol, bool(stream))
    now = time.monotonic()
    if not force_refresh:
        cached_entry = _CATALOG_CACHE.get(cache_key)
        if cached_entry is not None:
            cached_ts, cached_map = cached_entry
            if now - cached_ts < CATALOG_CACHE_TTL_SECONDS:
                return {k: dict(v) for k, v in cached_map.items()}
        if protocol == 'ollama':
            neg_entry = _CATALOG_NEGATIVE_CACHE.get(cache_key)
            if neg_entry is not None:
                neg_ts, neg_exc = neg_entry
                if now - neg_ts < OLLAMA_NEGATIVE_CACHE_TTL_SECONDS:
                    raise neg_exc

    base = _get_base(protocol)
    path = '/v1/models' if protocol == 'messages' else '/models'
    try:
        with discovery_transport(protocol) as client:
            response = client.get(
                base + path,
                params={'limit': 1000} if protocol == 'messages' else None,
                timeout=1.5 if protocol == 'ollama' else 20,
                follow_redirects=False,
            )
            response.raise_for_status()
            body = response.json()
    except Exception as exc:
        if protocol == 'ollama':
            _CATALOG_NEGATIVE_CACHE[cache_key] = (time.monotonic(), exc)
        raise

    if body.get('has_more'):
        raise ValueError('STANDARD_CATALOG_TRUNCATED')
    rows = body['data']
    seen_ids: set[str] = set()
    entries: dict[str, dict[str, Any]] = {}
    for row in rows:
        identifier = row['id']
        if not isinstance(identifier, str) or not identifier or identifier in seen_ids:
            raise ValueError('STANDARD_CATALOG_INVALID')
        seen_ids.add(identifier)
        if protocol != 'ollama':
            metadata = row['metadata']
            if metadata['release_status'] not in ('experimental', 'qualified'):
                raise ValueError('STANDARD_RELEASE_INVALID')
            if datetime.fromisoformat(metadata['expires_at'].replace('Z', '+00:00')) <= datetime.now(timezone.utc):
                continue
            if stream and (
                'streaming' not in metadata.get('capabilities', [])
                or protocol == 'responses'
                and metadata.get('response_stream') != 'ordered-items'
            ):
                continue
            entries[identifier] = dict(metadata)
        else:
            if stream:
                continue
            raw_meta = row.get('metadata') if isinstance(row, dict) else None
            entries[identifier] = (
                dict(raw_meta)
                if isinstance(raw_meta, dict)
                else {'capabilities': ['chat', 'tool_calling'], 'reasoning': False}
            )
    sorted_entries = {k: entries[k] for k in sorted(entries.keys())}
    _CATALOG_CACHE[cache_key] = (time.monotonic(), {k: dict(v) for k, v in sorted_entries.items()})
    _CATALOG_NEGATIVE_CACHE.pop(cache_key, None)
    return sorted_entries


def catalog(protocol, *, stream=False, force_refresh=True):
    return list(catalog_with_metadata(protocol, stream=stream, force_refresh=force_refresh).keys())


def get_cached_model_metadata(protocol: str, identifier: str) -> dict[str, Any] | None:
    """Return cached live metadata for `(protocol, identifier)` if present in `_CATALOG_CACHE`."""
    for s_flag in (False, True):
        cached_entry = _CATALOG_CACHE.get((protocol, s_flag))
        if cached_entry is not None:
            _, cached_map = cached_entry
            if identifier in cached_map:
                return dict(cached_map[identifier])
    return None


def model_supports_reasoning(
    protocol: str,
    identifier: str,
    metadata: dict[str, Any] | None = None,
) -> tuple[bool, bool]:
    """Return `(supports_reasoning, reasoning_required)` for `(protocol, identifier)` using both static guards and live `/models` metadata (`MA-W2-R6-02`)."""
    if protocol == 'responses':
        return (True, True)
    if protocol != 'chat':
        return (False, False)
    if identifier in CHAT_REASONING_UNSUPPORTED_MODELS:
        return (False, False)
    meta = metadata if isinstance(metadata, dict) else (get_cached_model_metadata('chat', identifier) or {})
    caps = meta.get('capabilities') or []
    meta_reasoning = meta.get('reasoning')
    if (
        meta_reasoning is False
        or (isinstance(meta_reasoning, dict) and meta_reasoning.get('supported') is False)
        or 'no_reasoning' in caps
    ):
        return (False, False)
    if identifier in CHAT_REASONING_REQUIRED_MODELS or (
        isinstance(meta_reasoning, dict) and meta_reasoning.get('required') is True
    ):
        return (True, True)
    if (
        meta_reasoning is True
        or (isinstance(meta_reasoning, dict) and meta_reasoning.get('supported') is True)
        or 'reasoning' in caps
    ):
        return (True, False)
    return (True, False)


def refresh(config, protocol):
    field = config['model_id']
    field['options'] = []
    try:
        field['options'] = list(catalog_with_metadata(protocol, force_refresh=True).keys())
    except Exception:
        field['info'] = 'Discovery unavailable. No cached fallback; retry Refresh explicitly.'
        raise ValueError('STANDARD_DISCOVERY_UNAVAILABLE') from None
    field['info'] = 'Live published catalog. Refresh to see new models; saved selection never changes automatically.'
    if field.get('value') and field['value'] not in field['options']:
        field['info'] += ' Saved model unavailable: choose explicitly.'
    return config


class ProtectedAnthropic(ChatAnthropic):
    _transports: dict = PrivateAttr()

    @cached_property
    def _client(self):
        return anthropic.Anthropic(**self._client_params, http_client=self._transports['http_client'])

    @cached_property
    def _async_client(self):
        return anthropic.AsyncAnthropic(**self._client_params, http_client=self._transports['http_async_client'])


def resolve_reasoning_options(
    protocol: str,
    identifier: str,
    *,
    reasoning: str = 'unspecified',
    reasoning_effort: str | None = None,
    extended_thinking: str = 'disabled',
    thinking_budget_tokens: int = 2048,
    max_tokens: int = 2048,
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Resolve protocol-specific reasoning/thinking options without triggering ChatOpenAI extra_forbidden (`W2-R2-07` & `MA-W2-R6-02`)."""
    effective_max_tokens = int(max_tokens or 2048)
    if effective_max_tokens < 1 or effective_max_tokens > 128000:
        raise ValueError('STANDARD_TOKEN_LIMIT_OUT_OF_RANGE')

    raw_effort = (reasoning_effort if reasoning_effort is not None else reasoning) or 'unspecified'
    raw_effort = str(raw_effort).strip().lower()

    if protocol == 'chat':
        if raw_effort not in VALID_CHAT_REASONING_EFFORTS:
            raise ValueError(f'Invalid chat reasoning_effort: {raw_effort}')
        supports_reasoning, reasoning_required = model_supports_reasoning('chat', identifier, metadata=metadata)
        if not supports_reasoning:
            return {'max_tokens': effective_max_tokens, 'extra_body': None}
        if reasoning_required:
            effort_val = 'none' if raw_effort == 'unspecified' else raw_effort
            return {'max_tokens': effective_max_tokens, 'extra_body': {'reasoning_effort': effort_val}}
        if raw_effort != 'unspecified':
            return {'max_tokens': effective_max_tokens, 'extra_body': {'reasoning_effort': raw_effort}}
        return {'max_tokens': effective_max_tokens, 'extra_body': None}

    if protocol == 'responses':
        if raw_effort not in VALID_RESPONSES_REASONING_EFFORTS and raw_effort != 'none':
            raise ValueError(f'Invalid responses reasoning_effort: {raw_effort}')
        effort_val = 'medium' if raw_effort in ('unspecified', 'none', '') else raw_effort
        return {
            'max_tokens': effective_max_tokens,
            'store': False,
            'include': ['reasoning.encrypted_content'],
            'output_version': 'responses/v1',
            'extra_body': {'reasoning': {'effort': effort_val, 'summary': 'auto'}},
        }

    if protocol == 'messages':
        mode = str(extended_thinking or 'disabled').strip().lower()
        if mode == 'enabled':
            budget = max(1024, min(32768, int(thinking_budget_tokens or 2048)))
            if effective_max_tokens <= budget:
                effective_max_tokens = budget + 1024
            return {
                'max_tokens': effective_max_tokens,
                'thinking': {'type': 'enabled', 'budget_tokens': budget},
            }
        return {'max_tokens': effective_max_tokens, 'thinking': None}

    return {'max_tokens': effective_max_tokens, 'extra_body': None}


def build(
    protocol,
    identifier,
    stream=False,
    reasoning='unspecified',
    max_tokens=2048,
    *,
    reasoning_effort=None,
    extended_thinking='disabled',
    thinking_budget_tokens=2048,
    temperature=None,
):
    if type(stream) is not bool:
        raise ValueError('STANDARD_STREAM_BOOLEAN_REQUIRED')
    resolved = resolve_reasoning_options(
        protocol,
        identifier,
        reasoning=reasoning,
        reasoning_effort=reasoning_effort,
        extended_thinking=extended_thinking,
        thinking_budget_tokens=thinking_budget_tokens,
        max_tokens=max_tokens,
    )
    if identifier not in (catalog(protocol, stream=True) if stream else catalog(protocol)):
        raise ValueError('STANDARD_SELECTED_MODEL_UNAVAILABLE: no automatic replacement')
    base_url = _get_base(protocol)
    clients = provider_httpx_clients(base_url)
    if not {'http_client', 'http_async_client'} <= clients.keys():
        raise ValueError('STANDARD_PROTECTED_TRANSPORT_REQUIRED')
    common = dict(
        model=identifier,
        api_key='private-spcs',
        base_url=base_url,
        max_tokens=resolved['max_tokens'],
        max_retries=0,
        timeout=180,
        streaming=stream,
    )
    try:
        if protocol == 'messages':
            anth_kwargs = dict(common)
            if resolved.get('thinking'):
                anth_kwargs['thinking'] = resolved['thinking']
            elif temperature is not None:
                anth_kwargs['temperature'] = float(temperature)
            model = ProtectedAnthropic(**anth_kwargs)
            model._transports = clients
        else:
            options = {}
            if protocol == 'responses':
                options.update(
                    store=False,
                    include=['reasoning.encrypted_content'],
                    output_version='responses/v1',
                )
            if resolved.get('extra_body'):
                options['extra_body'] = resolved['extra_body']
            if protocol == 'ollama':
                options['temperature'] = float(temperature) if temperature is not None else 0.1
            elif temperature is not None and not resolved.get('extra_body') and protocol == 'chat':
                options['temperature'] = float(temperature)
            model = ChatOpenAI(
                **common,
                **clients,
                use_responses_api=protocol == 'responses',
                **options,
            )
        weakref.finalize(model, close_clients, clients)
        return model
    except BaseException:
        close_clients(clients)
        raise
