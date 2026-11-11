"""Rerun-2: MemoryBaseComponent built from unpatched installed source still uses the Snowflake LF_MEM backend."""

import asyncio
import sys
import types

from lfx_snowflake import native_memory_base_hook as hook
from lfx_snowflake import stale_flow_guard as sg

UNPATCHED_SOURCE = '''
class MemoryBaseComponent:
    async def _build_backend(self, owner, owner_username, kb_name):
        raise ValueError("OpenAI API key is required")

    async def _additional_model_provider_policy_ids(self, purpose, parameters=None):
        return ("openai",)
'''


def _fake_eval(code, *args, **kwargs):
    ns: dict = {}
    exec(code, ns)  # noqa: S102 - test stand-in for lfx eval_custom_component_code
    return ns["MemoryBaseComponent"]


def test_built_memory_base_class_is_hooked_without_openai(monkeypatch):
    lfx = types.ModuleType("lfx")
    custom = types.ModuleType("lfx.custom")
    ev = types.ModuleType("lfx.custom.eval")
    ev.eval_custom_component_code = _fake_eval
    for name, mod in {"lfx": lfx, "lfx.custom": custom, "lfx.custom.eval": ev}.items():
        monkeypatch.setitem(sys.modules, name, mod)
    monkeypatch.setattr(sg, "resolve_component_code", lambda code: code)
    assert sg.install_stale_component_guard() >= 1

    cls = ev.eval_custom_component_code(UNPATCHED_SOURCE)
    assert getattr(cls, hook._HOOK_FLAG) is True

    spec = object()
    monkeypatch.setattr(hook, "snowflake_spec_for_memory", lambda kb: spec if kb == "LF_MEM_X" else None)
    monkeypatch.setattr(hook, "_selected_kb_name", lambda comp, sel: asyncio.sleep(0, "LF_MEM_X"))
    comp = cls()
    comp.graph = types.SimpleNamespace(flow_id="f1")
    backend = asyncio.run(comp._build_backend(None, None, "LF_MEM_X"))
    assert isinstance(backend, hook.SnowflakeLfMemBackend) and backend.flow_id == "f1"
    assert asyncio.run(comp._additional_model_provider_policy_ids("embed", {"memory_base": "m"})) == ()


def test_non_memory_classes_untouched():
    class Other:
        pass

    assert sg._apply_runtime_class_hooks(Other) is Other
    assert not hasattr(Other, hook._HOOK_FLAG)
