"""W3-R1 Phase 2: fail-closed patch engine of scripts/install_live_hooks_in_container.py.

The minified snippets below are copied verbatim from the Langflow 1.12.2 and 1.13.2rc0 release
bundles (same component, different Vite identifiers), so they exercise the real rename shapes.
Set SFLF_LF_BUNDLE_DIRS="<dir>:<version>,..." (dirs holding `frontend/` and `py/` extracted from the
langflowai/langflow images) to also run --report against real bundles.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "scripts" / "install_live_hooks_in_container.py"

LOGIN_GUARD_1122 = (
    "const Xct=({children:e})=>{const t=Kl(o=>o.autoLogin),n=Kl(o=>o.isAuthenticated);if(t===!0||n){"
)
LOGIN_GUARD_1132 = (
    "const y0t=({children:e})=>{const t=wa(i=>i.autoLogin),n=wa(i=>i.isAuthenticated);if(t===!0||n){"
)
LOGIN_GUARD_NEW = "const Xct=({children:e})=>{const t=Kl(o=>o.autoLogin),n=Kl(o=>o.isAuthenticated);if(!0){"


def _load():
    spec = importlib.util.spec_from_file_location("sflf_installer_under_test", INSTALLER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.LANGFLOW_FAMILY = "1.13"
    return mod


def _bundle(tmp_path: Path, chunk: str) -> Path:
    assets = tmp_path / "frontend" / "assets"
    assets.mkdir(parents=True)
    (assets / "index-Entry.js").write_text(chunk, encoding="utf-8")
    (tmp_path / "frontend" / "index.html").write_text('<script src="./assets/index-Entry.js"></script>', encoding="utf-8")
    return assets


def test_mini_patch_survives_minifier_renames(tmp_path):
    mod = _load()
    assets = _bundle(tmp_path, "x;" + LOGIN_GUARD_1132 + "return e}")
    mod.apply_bundle_patches(assets, [mod.BundlePatch("auth.login_guard", LOGIN_GUARD_1122, LOGIN_GUARD_NEW)])
    out = (assets / "index-Entry.js").read_text(encoding="utf-8")
    assert "const y0t=({children:e})=>{const t=wa(i=>i.autoLogin),n=wa(i=>i.isAuthenticated);if(!0){" in out
    assert mod.PATCH_LEDGER[-1]["status"] == "OK"


def test_missing_anchor_fails_closed(tmp_path):
    mod = _load()
    assets = _bundle(tmp_path, "const unrelated=1;")
    mod.apply_bundle_patches(assets, [mod.BundlePatch("auth.login_guard", LOGIN_GUARD_1122, LOGIN_GUARD_NEW)])
    assert mod.PATCH_LEDGER[-1]["status"] == "FAIL"
    with pytest.raises(SystemExit, match="auth.login_guard"):
        mod.assert_patch_ledger_clean()


def test_too_many_matches_fail_closed_and_write_nothing(tmp_path):
    mod = _load()
    chunk = LOGIN_GUARD_1132 + "}" + LOGIN_GUARD_1122 + "}"
    assets = _bundle(tmp_path, chunk)
    mod.apply_bundle_patches(assets, [mod.BundlePatch("auth.login_guard", LOGIN_GUARD_1122, LOGIN_GUARD_NEW)])
    assert mod.PATCH_LEDGER[-1]["status"] == "FAIL"
    assert (assets / "index-Entry.js").read_text(encoding="utf-8") == chunk


def test_already_applied_is_idempotent(tmp_path):
    mod = _load()
    assets = _bundle(tmp_path, LOGIN_GUARD_NEW.replace("Xct", "y0t").replace("Kl", "wa") + "}")
    before = (assets / "index-Entry.js").read_text(encoding="utf-8")
    mod.apply_bundle_patches(assets, [mod.BundlePatch("auth.login_guard", LOGIN_GUARD_1122, LOGIN_GUARD_NEW)])
    assert mod.PATCH_LEDGER[-1]["status"] == "ALREADY"
    assert (assets / "index-Entry.js").read_text(encoding="utf-8") == before


def test_unbound_identifier_in_replacement_fails_closed(tmp_path):
    mod = _load()
    assets = _bundle(tmp_path, LOGIN_GUARD_1132 + "}")
    # `c6e` only exists in the replacement and is not resolved from the bundle
    patch = mod.BundlePatch("brand.bad", LOGIN_GUARD_1122, LOGIN_GUARD_NEW + "v.jsx(c6e,{})")
    mod.apply_bundle_patches(assets, [patch])
    assert mod.PATCH_LEDGER[-1]["status"] == "FAIL"
    assert "unbound:c6e" in mod.PATCH_LEDGER[-1]["detail"]


def test_symbol_resolver_binds_logo_component(tmp_path):
    mod = _load()
    chunk = 'b.jsx(fje,{"aria-hidden":"true",title:t("common.langflowLogo"),className:"x"});' + LOGIN_GUARD_1132 + "}"
    assets = _bundle(tmp_path, chunk)
    patch = mod.BundlePatch("brand.logo", LOGIN_GUARD_1122, LOGIN_GUARD_NEW + "return c6e;", symbols={"c6e": "LOGO"})
    mod.apply_bundle_patches(assets, [patch])
    assert mod.PATCH_LEDGER[-1]["status"] == "OK"
    assert "if(!0){return fje;" in (assets / "index-Entry.js").read_text(encoding="utf-8")


def test_declared_absent_needs_explicit_family_count(tmp_path):
    mod = _load()
    assets = _bundle(tmp_path, "const unrelated=1;")
    mod.apply_bundle_patches(
        assets,
        [mod.BundlePatch("x.only_112", LOGIN_GUARD_1122, LOGIN_GUARD_NEW, expect={"1.12": 1, "1.13": 0}, reason="gone in 1.13")],
    )
    assert mod.PATCH_LEDGER[-1]["status"] == "ABSENT-DECLARED"
    mod.LANGFLOW_FAMILY = "1.14"
    mod.apply_bundle_patches(assets, [mod.BundlePatch("x.only_112", LOGIN_GUARD_1122, LOGIN_GUARD_NEW, expect={"1.12": 1})])
    assert mod.PATCH_LEDGER[-1]["status"] == "FAIL"


def test_report_mode_writes_nothing(tmp_path):
    mod = _load()
    mod.REPORT_ONLY = True
    assets = _bundle(tmp_path, LOGIN_GUARD_1132 + "}")
    before = (assets / "index-Entry.js").read_text(encoding="utf-8")
    mod.apply_bundle_patches(assets, [mod.BundlePatch("auth.login_guard", LOGIN_GUARD_1122, LOGIN_GUARD_NEW)], prepend_index="/*x*/")
    assert mod.PATCH_LEDGER[-1]["status"] == "OK"
    assert (assets / "index-Entry.js").read_text(encoding="utf-8") == before


def test_unknown_langflow_version_fails_closed(tmp_path):
    mod = _load()
    with pytest.raises(SystemExit, match="unsupported or unknown Langflow version"):
        mod._configure_paths(tmp_path, tmp_path, "2.0.0")
    mod._configure_paths(tmp_path, tmp_path, "1.13.2rc0")
    assert mod.LANGFLOW_FAMILY == "1.13"


def test_site_packages_is_not_hard_coded_to_python314(monkeypatch, tmp_path):
    mod = _load()
    monkeypatch.setenv("SFLF_SITE_PACKAGES", str(tmp_path))
    assert mod._detect_site_packages() == tmp_path


def test_native_trigger_dispatcher_guard(tmp_path):
    mod = _load()
    mod.SITE_PACKAGES = tmp_path
    target = tmp_path / "langflow" / "services" / "triggers" / "dispatcher.py"
    target.parent.mkdir(parents=True)
    body = (
        "def start_dispatcher_if_enabled() -> TriggerDispatcher | None:\n"
        "    if not get_settings_service().settings.trigger_dispatcher_enabled:\n"
        "        return None\n"
    )
    target.write_text(body, encoding="utf-8")
    anchor = "    if not get_settings_service().settings.trigger_dispatcher_enabled:\n        return None\n"
    assert mod.text_patch(target, "t", anchor, mod.NATIVE_DISPATCHER_GUARD + anchor, applied="SFLF_NATIVE_TRIGGER_DISPATCHER")
    patched = target.read_text(encoding="utf-8")
    assert patched.index("SFLF_NATIVE_TRIGGER_DISPATCHER") < patched.index("trigger_dispatcher_enabled")
    compile(patched, str(target), "exec")
    assert not mod.text_patch(target, "t", anchor, mod.NATIVE_DISPATCHER_GUARD + anchor, applied="SFLF_NATIVE_TRIGGER_DISPATCHER")
    assert mod.PATCH_LEDGER[-1]["status"] == "ALREADY"


def test_python_anchor_missing_fails_closed(tmp_path):
    mod = _load()
    mod.SITE_PACKAGES = tmp_path
    target = tmp_path / "main.py"
    target.write_text("app = create_app()\n", encoding="utf-8")
    assert not mod.text_patch(target, "main.load_plugin_routes", "    load_plugin_routes(app)\n", "x", applied="register_snowflake_fastapi_routes(app)")
    assert mod.PATCH_LEDGER[-1]["status"] == "FAIL"


def test_index_html_refresh_collapses_stacked_inject_blocks():
    mod = _load()
    block = (
        '    <meta name="google" content="notranslate" />\n    <script>guard()</script>\n'
        '    <style id="sflf-brand-blue-style">a{}</style>\n'
        '    <script defer src="./snowflake-ui-integration.js?v=sflf047&r=2"></script>\n  </head>'
    )
    stacked = "<head>\n" + '    <meta name="google" content="notranslate" />\n    <script>guard()</script>\n' * 3 + block[block.index("    <style"):]
    pattern = mod.INJECT_BLOCK_REFRESH_RE
    out = mod._html_sub(stacked, "html.inject_block_refresh", pattern, lambda _m: block, None)
    assert out.count('name="google" content="notranslate"') == 1
    assert mod._html_sub(out, "again", pattern, lambda _m: block, None) == out


def test_frozenset_creation_backends_not_in_installer():
    """Item 4: the frozenset no-op lives in fastapi_live_bridge.py (backend owner), not in the installer."""
    assert "_CREATION_ALLOWED_BACKENDS" not in INSTALLER.read_text(encoding="utf-8")


@pytest.mark.skipif(not os.environ.get("SFLF_LF_BUNDLE_DIRS"), reason="real Langflow bundles not extracted")
def test_report_against_real_bundles():
    import subprocess
    import sys

    for item in os.environ["SFLF_LF_BUNDLE_DIRS"].split(","):
        root, version = item.rsplit(":", 1)
        res = subprocess.run(
            [sys.executable, str(INSTALLER), "--report", "--site-packages", f"{root}/py", "--frontend", f"{root}/frontend", "--langflow-version", version],
            capture_output=True,
            text=True,
            check=False,
        )
        assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-2000:]
        assert " 0 failed" in res.stdout
