"""Snowflake Cortex embedding provider must reach Langflow 1.13's provider registry.

On 1.13, ``register_provider`` rejects a provider whose embedding params already exist, and the
model list (``/api/v1/models``) only shows registered providers. A first failed attempt used to
leave ``EMBEDDING_PARAM_MAPPINGS['Snowflake Cortex']`` behind, so every later attempt conflicted,
the provider was never listed and saved Snowflake embedding models showed as "Not available".
"""

from __future__ import annotations

import sys
import types

import pytest

from lfx_snowflake.providers import embeddings as emb


class _Registry:
    """Minimal stand-in for lfx.base.models.provider_registry on Langflow 1.13."""

    def __init__(self, class_registry: types.SimpleNamespace) -> None:
        self._cr = class_registry
        self.registered: dict[str, object] = {}

    class ProviderDescriptor:  # noqa: D106 - mirrors the upstream dataclass
        def __init__(self, **kwargs: object) -> None:
            self.__dict__.update(kwargs)

    def is_registered(self, name: str) -> bool:
        return name in self.registered

    def register_provider(self, spec: object) -> bool:
        name = spec.name  # type: ignore[attr-defined]
        if name in self._cr.EMBEDDING_PARAM_MAPPINGS:
            msg = f"ProviderSpec {name!r} embedding params {name!r} conflict with an existing mapping"
            raise ValueError(msg)
        self._cr.EMBEDDING_PARAM_MAPPINGS[name] = dict(spec.embedding_param_mapping)  # type: ignore[attr-defined]
        self._cr.EMBEDDING_PROVIDER_CLASS_MAPPING[name] = spec.embedding_class_name  # type: ignore[attr-defined]
        self.registered[name] = spec
        return True


@pytest.fixture
def fake_lfx(monkeypatch: pytest.MonkeyPatch) -> tuple[_Registry, types.SimpleNamespace]:
    class_registry = types.SimpleNamespace(
        EMBEDDING_PARAM_MAPPINGS={"OpenAI": {"model": "model"}},
        EMBEDDING_PROVIDER_CLASS_MAPPING={"OpenAI": "OpenAIEmbeddings"},
        _EMBEDDING_CLASS_IMPORTS={},
        _embedding_class_cache={},
    )
    registry = _Registry(class_registry)
    provider_registry = types.ModuleType("lfx.base.models.provider_registry")
    for attr in ("ProviderDescriptor", "is_registered", "register_provider"):
        setattr(provider_registry, attr, getattr(registry, attr))
    unified_models = types.ModuleType("lfx.base.models.unified_models")
    unified_models.class_registry = class_registry  # type: ignore[attr-defined]
    base_models = types.ModuleType("lfx.base.models")
    base_models.provider_registry = provider_registry  # type: ignore[attr-defined]
    base_models.unified_models = unified_models  # type: ignore[attr-defined]
    for name, module in {
        "lfx": types.ModuleType("lfx"),
        "lfx.base": types.ModuleType("lfx.base"),
        "lfx.base.models": base_models,
        "lfx.base.models.provider_registry": provider_registry,
        "lfx.base.models.unified_models": unified_models,
        "lfx.base.models.unified_models.class_registry": class_registry,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    return registry, class_registry


def test_registers_even_after_a_failed_attempt_left_stale_mappings(fake_lfx) -> None:
    registry, class_registry = fake_lfx
    # What a first failed attempt left behind before this fix.
    class_registry.EMBEDDING_PARAM_MAPPINGS["Snowflake Cortex"] = {
        "model": "model",
        "dimensions": "dimensions",
        "chunk_size": "chunk_size",
        "request_timeout": "request_timeout",
        "max_retries": "max_retries",
        "model_kwargs": "model_kwargs",
    }
    class_registry.EMBEDDING_PROVIDER_CLASS_MAPPING["Snowflake Cortex"] = "SnowflakeCortexEmbeddings"

    status = emb.register_snowflake_cortex_embedding_provider()

    assert registry.is_registered("Snowflake Cortex")
    assert status["provider_registry_registered"] is True
    assert "provider_registry_error" not in status
    assert class_registry.EMBEDDING_PROVIDER_CLASS_MAPPING["Snowflake Cortex"] == "SnowflakeCortexEmbeddings"
    # Other providers are untouched.
    assert class_registry.EMBEDDING_PARAM_MAPPINGS["OpenAI"] == {"model": "model"}


def test_second_call_is_a_no_op(fake_lfx) -> None:
    registry, _ = fake_lfx
    assert emb.register_snowflake_cortex_embedding_provider()["provider_registry_registered"] is True
    assert emb.register_snowflake_cortex_embedding_provider()["provider_registry_registered"] is True
    assert list(registry.registered) == ["Snowflake Cortex"]


def test_reports_failure_instead_of_claiming_success(fake_lfx, monkeypatch: pytest.MonkeyPatch) -> None:
    registry, class_registry = fake_lfx

    def boom(spec: object) -> bool:
        raise ValueError("rejected")

    monkeypatch.setattr(sys.modules["lfx.base.models.provider_registry"], "register_provider", boom)

    status = emb.register_snowflake_cortex_embedding_provider()

    assert status["provider_registry_registered"] is False
    assert "rejected" in status["provider_registry_error"]
    # A failed registration must not leave mappings that block the next attempt.
    assert "Snowflake Cortex" not in class_registry.EMBEDDING_PARAM_MAPPINGS
    assert not registry.is_registered("Snowflake Cortex")
