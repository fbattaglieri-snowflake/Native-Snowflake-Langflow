"""W3-R1 Phase 2 follow-up: Langflow 1.13.1 UI stuck on the "Loading..." overlay.

Root cause (reproduced in a headless browser against the release image):
- The bridge middleware answered every GET /assets/index-*.js with the entry bundle. Vite also
  names lazy route chunks `index-<hash>.js`, so each of them re-ran the whole app (another
  createRoot on #root) and React.lazy resolved to undefined (React error #306).
- index.html loaded the entry as `index-<hash>.js?v=...` while the lazy chunks import
  `./index-<hash>.js`: two module URLs, so the entry ran twice even on the home page.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "scripts" / "install_live_hooks_in_container.py"
BRIDGE = REPO_ROOT / "src" / "lfx_snowflake" / "fastapi_live_bridge.py"

ENTRY_JS = "export const entry=1;createRoot(document.getElementById('root'));"
LAZY_CHUNK_JS = "import{a as e}from'./index-Entry.js';export{e as default};"


def _load_installer():
    spec = importlib.util.spec_from_file_location("sflf_installer_ui_loading", INSTALLER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.LANGFLOW_FAMILY = "1.13"
    return mod


def test_bridge_does_not_rewrite_index_chunks():
    src = BRIDGE.read_text(encoding="utf-8")
    assert "_cached_patched_index_js" not in src
    assert 're.match(r"^/assets/index-[^/]+\\.js$", clean_path)' not in src


def test_entry_script_has_no_query_string_and_classic_scripts_are_bumped():
    mod = _load_installer()
    html = '<script type="module" crossorigin src="./assets/index-Entry.js?v=sflf047"></script>'
    out = mod._html_sub(html, "html.entry_single_module_url", mod.ENTRY_SCRIPT_RE, mod.ENTRY_SCRIPT_REPL, None)
    assert 'src="./assets/index-Entry.js"' in out
    assert "?v=" not in out
    assert mod.PATCH_LEDGER[-1]["status"] == "OK"
    hooks = INSTALLER.read_text(encoding="utf-8")
    assert "snowflake-ui-integration.js?v=sflf048" in hooks
    assert "preserved_provisioning_and_memories_concept.js?v=sflf048" in hooks
    assert "?v=sflf047" not in hooks


def test_entry_script_rewrite_is_idempotent_on_clean_url():
    mod = _load_installer()
    html = '<script type="module" crossorigin src="./assets/index-Entry.js"></script>'
    out = mod._html_sub(html, "html.entry_single_module_url", mod.ENTRY_SCRIPT_RE, mod.ENTRY_SCRIPT_REPL, None)
    assert out == html


def _app_with_assets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fastapi = pytest.importorskip("fastapi")
    # Other tests may install a minimal `fastapi` shim on hosts without it; these need the real one.
    StaticFiles = pytest.importorskip("fastapi.staticfiles").StaticFiles
    TestClient = pytest.importorskip("fastapi.testclient").TestClient

    from lfx_snowflake.fastapi_live_bridge import register_snowflake_fastapi_routes

    assets = tmp_path / "frontend" / "assets"
    assets.mkdir(parents=True)
    (assets / "index-Entry.js").write_text(ENTRY_JS, encoding="utf-8")
    (assets / "index-LazyRoute.js").write_text(LAZY_CHUNK_JS, encoding="utf-8")
    monkeypatch.setenv("LANGFLOW_FRONTEND_DIR", str(tmp_path / "frontend"))
    # The SPCS service runs with auto-login on; the old interception only fired on that path.
    monkeypatch.setenv("LANGFLOW_AUTO_LOGIN", "true")
    app = fastapi.FastAPI()
    register_snowflake_fastapi_routes(app)
    app.mount("/assets", StaticFiles(directory=str(assets)), name="assets")
    return TestClient(app)


def test_lazy_index_chunk_is_served_verbatim(tmp_path, monkeypatch):
    client = _app_with_assets(tmp_path, monkeypatch)
    entry = client.get("/assets/index-Entry.js")
    lazy = client.get("/assets/index-LazyRoute.js")
    assert entry.status_code == 200 and entry.text == ENTRY_JS
    assert lazy.status_code == 200 and lazy.text == LAZY_CHUNK_JS


def test_patched_js_assets_are_always_revalidated(tmp_path, monkeypatch):
    client = _app_with_assets(tmp_path, monkeypatch)
    resp = client.get("/assets/index-LazyRoute.js")
    assert resp.headers.get("cache-control") == "no-cache"
    assert resp.headers.get("etag")
