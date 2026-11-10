"""W3-R1 Phase 2 follow-up: Langflow 1.13.1 UI stuck on the "Loading..." overlay.

Root cause (reproduced in a headless browser against the release image):
- The bridge middleware answered every GET /assets/index-*.js with the entry bundle. Vite also
  names lazy route chunks `index-<hash>.js`, so each of them re-ran the whole app (another
  createRoot on #root) and React.lazy resolved to undefined (React error #306).
- index.html loaded the entry as `index-<hash>.js?v=...` while the lazy chunks import
  `./index-<hash>.js`: two module URLs, so the entry ran twice even on the home page.

Found in the same session: Langflow 1.13 builds with ENABLE_TRIGGERS=false and drops the whole
`triggers` sidebar category, so the Snowflake triggers were no longer listed.
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


# Langflow 1.13.1 sidebar filter, verbatim except for renamed minified identifiers.
SIDEBAR_FILTER_1131 = (
    'const Qxo="triggers";function Zxo(e){return typeof e?.metadata?.trigger_kind=="string"}'
    "function Wxo(e){let t=!1;const n=[];for(const[i,s]of Object.entries(e)){if(i===Qxo){t=!0;continue}"
    "const a=Object.entries(s).filter(([,l])=>!Zxo(l));if(a.length===Object.keys(s).length){n.push([i,s]);continue}"
    "t=!0,a.length>0&&n.push([i,Object.fromEntries(a)])}return t?Object.fromEntries(n):e}"
)
SIDEBAR_CATEGORIES_1131 = (
    '{display_name:"sidebar.category.inputOutput",name:"input_output",icon:"Cable"},'
    '{display_name:"sidebar.category.triggers",name:"triggers",icon:"Zap"},'
    '{display_name:"sidebar.category.flowControl",name:"flow_controls",icon:"ArrowRightLeft"},'
    '{display_name:"sidebar.category.triggers",name:"triggers",icon:"Clock"},'
    '{display_name:"sidebar.category.utilities",name:"utilities",icon:"Wand2"}'
)


def _patch_bundle(tmp_path: Path, mod, chunk: str) -> str:
    assets = tmp_path / "frontend" / "assets"
    assets.mkdir(parents=True)
    (assets / "index-Entry.js").write_text(chunk, encoding="utf-8")
    mod.apply_bundle_patches(
        assets,
        [
            mod.BundlePatch("triggers.keep_sidebar_category", mod.TRIGGERS_KEEP_CATEGORY_ORIG, mod.TRIGGERS_KEEP_CATEGORY_NEW),
            mod.BundlePatch("triggers.drop_native_sidebar_entry", mod.TRIGGERS_NATIVE_ENTRY_ORIG, mod.TRIGGERS_NATIVE_ENTRY_NEW),
        ],
    )
    assert [r["status"] for r in mod.PATCH_LEDGER[-2:]] == ["OK", "OK"]
    return (assets / "index-Entry.js").read_text(encoding="utf-8")


def test_triggers_category_survives_disabled_native_triggers(tmp_path):
    out = _patch_bundle(tmp_path, _load_installer(), SIDEBAR_FILTER_1131 + ";" + SIDEBAR_CATEGORIES_1131)
    assert "if(i===Qxo){t=!0;continue}" not in out
    assert "if(!1){t=!0;continue}" in out
    # The upstream per-component filter for native triggers (metadata.trigger_kind) stays in place.
    assert "filter(([,l])=>!Zxo(l))" in out
    assert out.count('name:"triggers"') == 1
    assert '{display_name:"sidebar.category.triggers",name:"triggers",icon:"Zap"}' in out


def test_triggers_sidebar_patches_are_idempotent(tmp_path):
    mod = _load_installer()
    first = _patch_bundle(tmp_path, mod, SIDEBAR_FILTER_1131 + ";" + SIDEBAR_CATEGORIES_1131)
    mod.apply_bundle_patches(
        tmp_path / "frontend" / "assets",
        [mod.BundlePatch("triggers.keep_sidebar_category", mod.TRIGGERS_KEEP_CATEGORY_ORIG, mod.TRIGGERS_KEEP_CATEGORY_NEW)],
    )
    assert mod.PATCH_LEDGER[-1]["status"] == "ALREADY"
    assert (tmp_path / "frontend" / "assets" / "index-Entry.js").read_text(encoding="utf-8") == first


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


def test_empty_wildcard_category_does_not_rediscover_catalog_on_every_call(tmp_path, monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    fid = "eb2a60b9-5de1-450d-b1e7-d941f5ac45d4"
    bridge.update_selected_context(
        {
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "databases": ["SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
            "full_schemas": ["SNOWFLAKE_LANGFLOW.CORE"],
            "objects_by_db_schema": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["LANGFLOW_FLOW_RUNS"],
                    "views": [],
                }
            },
        },
        flow_id=fid,
    )
    calls = 0

    def _boom(*_a, **_kw):
        nonlocal calls
        calls += 1
        return {"objects_by_db_schema": {"SNOWFLAKE_LANGFLOW.CORE": {"tables": ["LANGFLOW_FLOW_RUNS"], "views": []}}}

    monkeypatch.setattr(bridge, "discover_catalog", _boom)
    assert bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "views", flow_id=fid) == []
    assert bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "views", flow_id=fid) == []
    assert calls == 0


def test_legacy_provisioner_import_shim_and_a2a_patch_use_context_helpers():
    import lfx_snowflake  # noqa: F401
    from lfx_snowflake.context_helpers import _resolve_flow_id as canonical_resolve
    from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import (
        _resolve_flow_id as shimmed_resolve,
    )

    assert shimmed_resolve is canonical_resolve
    hooks = INSTALLER.read_text(encoding="utf-8")
    assert "from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import _resolve_flow_id" not in hooks
    assert "from lfx_snowflake.context_helpers import _resolve_flow_id" in hooks


def test_sync_canvas_nodes_only_unsynced_and_guarded_input_events():
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert "syncCanvasNodesWithSnowflakeContext(sfState, { onlyUnsynced: true })" in ui_js
    assert "document.activeElement === inp" in ui_js
    assert 'fieldObj.value = "";' not in ui_js.split("function _syncReactFiberNodeTemplate")[1].split("function syncCanvasNodesWithSnowflakeContext")[0]

