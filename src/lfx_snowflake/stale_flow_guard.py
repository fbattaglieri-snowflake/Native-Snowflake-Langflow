"""Stale stored flows: Snowflake component code embedded in a flow must match the installed component.

Langflow stores every node's component source in the flow JSON (``template.code.value``) and builds the
node from that stored source, not from the installed class. The frontend marks such nodes ``outdated``
("Update component"), but the backend runs them as stored. A flow saved before a security release would
therefore keep running old Snowflake component code (e.g. code predating the flow-context guard).

Server-side safeguard (installed at startup by ``register_snowflake_fastapi_routes``): Langflow's
``eval_custom_component_code`` is wrapped so stored code that defines a Snowflake component class but
differs from the installed source is

* ``SNOWFLAKE_LANGFLOW_STALE_COMPONENT_POLICY=upgrade`` (default): transparently built from the installed
  source -- the same code the "Update component" button would load; stored field values are untouched;
* ``SNOWFLAKE_LANGFLOW_STALE_COMPONENT_POLICY=refuse``: refused with ``COMPONENT_CODE_OUTDATED``.

"Snowflake component" means every Component subclass of the plugin's native modules plus the upstream
modules the installer patches (``PATCHED_UPSTREAM_MODULES``). Code that uses ``lfx_snowflake`` but defines
none of those classes (a renamed copy, or a custom component calling the bridge) is always refused with
``UNREGISTERED_SNOWFLAKE_COMPONENT_CODE``; custom components that do not use ``lfx_snowflake`` are untouched.

Code edited in the UI's code editor for a Snowflake component is treated the same way (it must not bypass
the guards). Stored flows can be rewritten once with ``scripts/refresh_stored_flow_component_code.py``.
"""

from __future__ import annotations

import ast
import logging
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COMPONENT_CODE_OUTDATED = "COMPONENT_CODE_OUTDATED"
UNREGISTERED_SNOWFLAKE_COMPONENT_CODE = "UNREGISTERED_SNOWFLAKE_COMPONENT_CODE"
POLICY_ENV = "SNOWFLAKE_LANGFLOW_STALE_COMPONENT_POLICY"
POLICY_UPGRADE = "upgrade"
POLICY_REFUSE = "refuse"

_PLUGIN_PACKAGE = "lfx_snowflake"
_NATIVE_DIR = Path(__file__).resolve().parent / "native_category_components"
# Upstream Langflow modules the installer patches in place (scripts/install_live_hooks_in_container.py):
# language_model/agent/a2a_agent get Snowflake providers and A2A routing, memory_retrieval the LF_MEM hook,
# and the five canonical Files & Knowledge modules are restored/re-indexed by the installer.
PATCHED_UPSTREAM_MODULES: tuple[str, ...] = (
    "models_and_agents/language_model.py",
    "models_and_agents/agent.py",
    "models_and_agents/a2a_agent.py",
    "files_and_knowledge/memory_retrieval.py",
    "files_and_knowledge/filesystem.py",
    "files_and_knowledge/knowledge.py",
    "files_and_knowledge/file.py",
    "files_and_knowledge/save_file.py",
)


class StaleComponentCode(RuntimeError):
    """A stored flow node embeds outdated Snowflake component code and the policy is ``refuse``."""

    error_code = COMPONENT_CODE_OUTDATED

    def __init__(self, class_names: list[str]):
        self.class_names = list(class_names)
        super().__init__(
            f"{COMPONENT_CODE_OUTDATED}: this flow embeds outdated code for {', '.join(class_names)}. "
            "Use 'Update component' on the node (or ask an admin to run "
            "scripts/refresh_stored_flow_component_code.py) and run the flow again."
        )


class UnregisteredSnowflakeComponentCode(StaleComponentCode):
    """Code uses ``lfx_snowflake`` internals but defines no registered Snowflake/patched component class.

    Covers renamed copies of Snowflake components and custom components calling the bridge directly
    (e.g. ``LIVE_BRIDGE.execute_sql``), which would otherwise run without the component guards. Always
    refused, whatever the policy.
    """

    error_code = UNREGISTERED_SNOWFLAKE_COMPONENT_CODE

    def __init__(self, class_names: list[str]):
        self.class_names = list(class_names)
        RuntimeError.__init__(
            self,
            f"{UNREGISTERED_SNOWFLAKE_COMPONENT_CODE}: component code {', '.join(class_names) or '(no class)'} "
            f"uses {_PLUGIN_PACKAGE} but is not a registered Snowflake component. Replace the node with the "
            "installed Snowflake component from the sidebar.",
        )


def policy() -> str:
    value = (os.getenv(POLICY_ENV) or POLICY_UPGRADE).strip().lower()
    return POLICY_REFUSE if value == POLICY_REFUSE else POLICY_UPGRADE


def _normalize(code: str) -> str:
    return "\n".join(line.rstrip() for line in str(code or "").replace("\r\n", "\n").strip().split("\n"))


def _parse(code: str) -> ast.Module | None:
    try:
        return ast.parse(code)
    except (SyntaxError, ValueError):
        return None


def _class_names(code: str) -> list[str]:
    tree = _parse(code)
    return [node.name for node in tree.body if isinstance(node, ast.ClassDef)] if tree is not None else []


def _component_class_names(source: str) -> list[str]:
    """Top-level classes with at least one base: every Component subclass, whatever its name.

    Component bases vary (``Component``, ``LCModelComponent``, ``LCAgentComponent``, local helper bases), so
    any subclassing top-level class is indexed; over-indexing only means that class is built from its
    installed module, never that unknown code runs.
    """
    tree = _parse(source)
    if tree is None:
        return []
    return [n.name for n in tree.body if isinstance(n, ast.ClassDef) and (n.bases or n.keywords)]


def references_plugin(code: str) -> bool:
    """True when ``code`` may reach ``lfx_snowflake`` internals (conservative).

    Any occurrence of the package name counts -- ``import``/``from`` statements, ``importlib`` /
    ``__import__`` string arguments, ``sys.modules`` lookups -- as does unparsable code naming it.
    Strings assembled at runtime cannot be detected statically; this is a tripwire for stale or copied
    Snowflake code, not a sandbox for arbitrary custom components.
    """
    return isinstance(code, str) and _PLUGIN_PACKAGE in code


def _lfx_components_dir() -> Path | None:
    try:
        import lfx.components as lfx_components  # noqa: PLC0415

        return Path(next(iter(lfx_components.__path__)))
    except (ImportError, StopIteration, TypeError, AttributeError):
        return None


@lru_cache(maxsize=1)
def installed_component_sources() -> dict[str, str]:
    """Class name -> current installed source, for every Snowflake and patched upstream component module.

    The copy the installer placed (and possibly patched) under ``lfx/components`` is what Langflow serves
    as the current code, so it wins over the plugin's own source of the same module. Patched upstream
    modules are read from the installed lfx package only.
    """
    lfx_dir = _lfx_components_dir()
    index: dict[str, str] = {}
    plugin_files = {p.relative_to(_NATIVE_DIR): p for p in _NATIVE_DIR.rglob("*.py") if p.name != "__init__.py"}
    for rel, path in sorted(plugin_files.items()):
        installed = lfx_dir / rel if lfx_dir is not None else None
        source = (installed if installed is not None and installed.is_file() else path).read_text(encoding="utf-8")
        for name in _component_class_names(source):
            index.setdefault(name, source)
    if lfx_dir is not None:
        for rel_str in PATCHED_UPSTREAM_MODULES:
            path = lfx_dir / rel_str
            if path.is_file():
                source = path.read_text(encoding="utf-8")
                for name in _component_class_names(source):
                    index.setdefault(name, source)
    return index


def outdated_classes(code: str) -> tuple[list[str], str | None]:
    """Registered component classes ``code`` defines with a source that differs from the installed one.

    Returns ``(class_names, installed_source)``; ``([], None)`` when ``code`` is current or not registered.
    """
    if not isinstance(code, str) or "class " not in code:
        return [], None
    index = installed_component_sources()
    registered = [n for n in _class_names(code) if n in index]
    if not registered:
        return [], None
    current = index[registered[0]]
    if _normalize(current) == _normalize(code):
        return [], None
    return registered, current


def resolve_component_code(code: str) -> str:
    """Code Langflow must build for a stored/UI node.

    * registered Snowflake/patched class, current code: unchanged;
    * registered class, outdated or edited code: installed source (policy ``upgrade``) or refused;
    * code using ``lfx_snowflake`` without a registered class: always refused;
    * any other custom component: unchanged.
    """
    if not isinstance(code, str):
        return code
    index = installed_component_sources()
    classes = _class_names(code)
    if not any(n in index for n in classes):
        if references_plugin(code):
            logger.warning("Refused unregistered component code using %s: %s", _PLUGIN_PACKAGE, classes)
            raise UnregisteredSnowflakeComponentCode(classes)
        return code
    stale, current = outdated_classes(code)
    if not stale:
        return code
    if policy() == POLICY_REFUSE:
        logger.warning("Refused outdated stored Snowflake component code: %s", stale)
        raise StaleComponentCode(stale)
    logger.info("Building outdated stored Snowflake component %s from the installed source", stale)
    return current or code


def _apply_runtime_class_hooks(built: Any) -> Any:
    """Hook classes built from source text, independent of that text (BUG-MEM-MB-01 rerun-2).

    The installer appends the LF_MEM hook to ``memory_retrieval.py``, but nodes are built by exec'ing the
    stored or installed source, which may be an unpatched copy. Hooking the built class guarantees the
    native ``MemoryBaseComponent`` always resolves Snowflake memories to the governed LF_MEM backend.
    """
    if isinstance(built, type) and built.__name__ == "MemoryBaseComponent" and hasattr(built, "_build_backend"):
        from lfx_snowflake.native_memory_base_hook import install_snowflake_memory_base_hook  # noqa: PLC0415

        install_snowflake_memory_base_hook(built)
    return built


def _install_lf_mem_auto_capture() -> bool:
    """Route native Memory Base auto-capture of LF_MEM bases to the Snowflake pipeline (rerun-3: was never installed)."""
    try:
        import langflow.services.memory_base.ingestion as ingestion  # noqa: PLC0415
    except ImportError:
        return False
    from lfx_snowflake.memories_lf_mem_ingest import install_snowflake_auto_capture  # noqa: PLC0415

    return install_snowflake_auto_capture(ingestion)


def install_stale_component_guard() -> int:
    """Wrap every loaded binding of lfx ``eval_custom_component_code``; returns the number of bindings."""
    try:
        import lfx.custom.eval as lfx_eval  # noqa: PLC0415
    except ImportError:
        return 0
    original = getattr(lfx_eval, "eval_custom_component_code", None)
    if original is None:
        return 0
    if getattr(original, "_sflf_stale_guard", False):
        return 0
    # Pre-import the module Langflow builds nodes through, so its by-name import is rebound too.
    try:
        import lfx.interface.initialize.loading  # noqa: F401, PLC0415
    except ImportError:
        pass

    def guarded_eval_custom_component_code(code: str, *args: Any, **kwargs: Any):
        built = original(resolve_component_code(code), *args, **kwargs)
        return _apply_runtime_class_hooks(built)

    _install_lf_mem_auto_capture()
    guarded_eval_custom_component_code._sflf_stale_guard = True  # type: ignore[attr-defined]
    guarded_eval_custom_component_code.__wrapped__ = original  # type: ignore[attr-defined]
    patched = 0
    for name, module in list(sys.modules.items()):
        if module is None or not (name == "lfx" or name.startswith(("lfx.", "langflow."))):
            continue
        if getattr(module, "eval_custom_component_code", None) is original:
            setattr(module, "eval_custom_component_code", guarded_eval_custom_component_code)
            patched += 1
    return patched


def refresh_flow_data(flow_data: dict[str, Any]) -> list[str]:
    """Rewrite outdated Snowflake component code in a stored flow's ``data`` in place; returns node ids.

    Only ``template.code.value`` is replaced (field values and edges are kept), which is what the backend
    builds from; the frontend refreshes the remaining template on the next "Update component".
    """
    changed: list[str] = []
    for node in (flow_data or {}).get("nodes") or []:
        data = node.get("data") or {}
        code_field = ((data.get("node") or {}).get("template") or {}).get("code")
        if not isinstance(code_field, dict):
            continue
        stale, current = outdated_classes(code_field.get("value"))
        if stale and current:
            code_field["value"] = current
            changed.append(str(data.get("id") or node.get("id")))
    return changed
