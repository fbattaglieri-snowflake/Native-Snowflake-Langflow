"""A model released (or revoked) by the LLM proxy must show up in Langflow without a release.

The proxy `/models` endpoints are the source of truth. If the proxy is briefly unreachable, the
last catalog it returned must be kept: falling back to the static offline catalog would silently
hide every model promoted after that list was written.
"""

from __future__ import annotations

import pytest

from lfx_snowflake import standard_langflow
from lfx_snowflake.providers import language_models as lm


def _meta() -> dict:
    return {"release_status": "qualified", "expires_at": "2099-12-31T23:59:59Z", "capabilities": ["chat"], "reasoning": False}


@pytest.fixture
def proxy(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Fake proxy catalog per protocol; set `state["down"] = True` to simulate an outage."""
    state: dict = {"down": False, "models": {"chat": ["glm-5.3", "kimi-k3"], "responses": [], "messages": [], "ollama": []}}

    def fake_catalog(protocol: str, *, stream: bool = False, force_refresh: bool = False) -> dict:
        if state["down"]:
            raise ConnectionError("proxy unreachable")
        return {model_id: _meta() for model_id in state["models"][protocol]}

    monkeypatch.setattr(standard_langflow, "catalog_with_metadata", fake_catalog)
    monkeypatch.setattr(lm, "_LAST_LIVE_CATALOG", {})
    monkeypatch.setattr(lm, "_DISCOVERY_FAILING", set())
    return state


def _chat_models() -> list[str]:
    return sorted(o["name"] for o in lm.get_snowflake_live_language_model_catalog(protocols=("chat",)))


def test_released_model_appears_and_revoked_model_disappears(proxy: dict) -> None:
    assert _chat_models() == ["glm-5.3", "kimi-k3"]
    proxy["models"]["chat"].append("brand-new-model")
    assert "brand-new-model" in _chat_models()
    proxy["models"]["chat"].remove("brand-new-model")
    assert "brand-new-model" not in _chat_models()


def test_outage_keeps_last_live_catalog_instead_of_static_list(proxy: dict) -> None:
    proxy["models"]["chat"].append("brand-new-model")
    assert "brand-new-model" in _chat_models()
    proxy["down"] = True
    models = _chat_models()
    assert "brand-new-model" in models
    assert models == ["brand-new-model", "glm-5.3", "kimi-k3"]


def test_cold_start_outage_uses_static_catalog(proxy: dict) -> None:
    proxy["down"] = True
    assert _chat_models() == sorted(lm.FALLBACK_OFFLINE_CATALOG["chat"])


def test_static_catalog_lists_all_promoted_proxy_models() -> None:
    assert sorted(lm.FALLBACK_OFFLINE_CATALOG["responses"]) == [
        "openai-gpt-6-astra",
        "openai-gpt-6.1-sol",
        "openai-gpt-6.1-terra",
    ]
    assert sum(len(lm.FALLBACK_OFFLINE_CATALOG[p]) for p in ("chat", "responses", "messages")) == 12


def test_memory_preprocessing_engines_follow_live_catalog(proxy: dict) -> None:
    from lfx_snowflake.fastapi_live_bridge import get_llm_preprocessing_engines

    proxy["models"]["chat"].append("brand-new-model")
    engines = {e["id"]: e for e in get_llm_preprocessing_engines()}
    assert sorted(engines["proxy_chat"]["models"]) == ["brand-new-model", "glm-5.3", "kimi-k3"]
    assert engines["proxy_chat"]["default_model"] in engines["proxy_chat"]["models"]
    # Engines without a proxy protocol keep their own list.
    assert engines["cortex_sql"]["models"][0] == "mistral-large2"
