"""Build-time installer that wires SnowflakeLangflow components into native built-in categories, patches the React bundle safely, and mounts `/api/v1/snowflake/*`."""

from __future__ import annotations

import hashlib
import re
import shutil
import urllib.parse
from pathlib import Path

import orjson

SITE_PACKAGES = Path("/app/.venv/lib64/python3.14/site-packages")
LANGFLOW_MAIN = SITE_PACKAGES / "langflow" / "main.py"
LANGFLOW_FRONTEND = SITE_PACKAGES / "langflow" / "frontend"
LFX_COMPONENTS_DIR = SITE_PACKAGES / "lfx" / "components"
LFX_INDEX_JSON = SITE_PACKAGES / "lfx" / "_assets" / "component_index.json"
COMPONENTS_ROOT = Path("/opt/standard-components")
SOURCE_ROOT = Path("/opt/snowflake-langflow")

OFFICIAL_SNOWFLAKE_PATH = (
    "M9.86 15.298l13.008 7.8a3.72 3.72 0 0 0 4.589-.601 4.01 4.01 0 0 0 1.227-2.908V3.956a3.81 3.81 0 0 0-1.861-3.42 "
    "3.81 3.81 0 0 0-3.893 0 3.81 3.81 0 0 0-1.861 3.42v8.896l-7.387-4.43a3.79 3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 "
    "1.844-1.057 1.9-.44 4.28 1.4 5.422m31.27 7.8l13.008-7.8c1.84-1.143 2.458-3.533 1.4-5.424a3.75 3.75 0 0 0-5.22-1.452l-7.3 "
    "4.37v-8.84a3.81 3.81 0 1 0-7.615 0v15.323a4.08 4.08 0 0 0 .494 2.367c.482.903 1.314 1.57 2.3 1.844a3.71 3.71 0 0 0 "
    "2.922-.4M29.552 31.97c.013-.25.108-.5.272-.68l1.52-1.58a1.06 1.06 0 0 1 .658-.282h.057a1.05 1.05 0 0 1 .656.282l1.52 "
    "1.58a1.12 1.12 0 0 1 .272.681v.06a1.13 1.13 0 0 1-.272.683l-1.52 1.58a1.04 1.04 0 0 1-.656.284h-.057c-.246-.014-.48-.115-.658-.284l-1.52-1.58a1.13 "
    "1.13 0 0 1-.272-.683zm-4.604-.65v1.364a1.54 1.54 0 0 0 .372.93l5.16 5.357a1.42 1.42 0 0 0 .895.386h1.312a1.42 1.42 0 0 0 "
    ".895-.386l5.16-5.357a1.54 1.54 0 0 0 .372-.93V31.32a1.54 1.54 0 0 0-.372-.93l-5.16-5.357a1.42 1.42 0 0 0-.895-.386h-1.312a1.42 "
    "1.42 0 0 0-.895.386L25.32 30.4a1.55 1.55 0 0 0-.372.93M3.13 27.62l7.365 4.417L3.13 36.45a4.06 4.06 0 0 0-1.399 5.424 3.75 "
    "3.75 0 0 0 2.3 1.844c.986.274 2.042.133 2.922-.392l13.008-7.8c1.2-.762 1.9-2.078 1.9-3.492a4.16 4.16 0 0 0-1.9-3.492l-13.008-7.8a3.79 "
    "3.79 0 0 0-2.922-.4c-.986.265-1.818.94-2.3 1.844-1.057 1.9-.44 4.278 1.4 5.422m38.995 4.442a4 4 0 0 0 1.91 3.477l13 7.8c.88.524 "
    "1.934.666 2.92.392s1.817-.94 2.3-1.843a4.05 4.05 0 0 0-1.4-5.424L53.5 32.038l7.365-4.417c1.84-1.143 2.457-3.53 1.4-5.422a3.74 "
    "3.74 0 0 0-2.3-1.844c-.987-.274-2.042-.134-2.92.4l-13 7.8a4 4 0 0 0-1.91 3.507M25.48 40.508a3.7 3.7 0 0 0-2.611.464l-13.008 "
    "7.8c-1.84 1.143-2.456 3.53-1.4 5.422.483.903 1.314 1.57 2.3 1.843a3.75 3.75 0 0 0 2.922-.392l7.387-4.43v8.83a3.81 3.81 0 1 0 "
    "7.614 0V44.4a3.91 3.91 0 0 0-3.205-3.903m28.66 8.276l-13.008-7.8a3.75 3.75 0 0 0-2.922-.392 3.74 3.74 0 0 0-2.3 1.843 4.09 "
    "4.09 0 0 0-.494 2.37v15.25a3.81 3.81 0 1 0 7.614 0V51.28l7.287 4.37a3.79 3.79 0 0 0 2.922.4c.986-.265 1.818-.94 2.3-1.844 "
    "1.057-1.9.44-4.28-1.4-5.422"
)


def _svg_data_uri(fill: str) -> str:
    raw = f'<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 64 64" fill="{fill}"><path d="{OFFICIAL_SNOWFLAKE_PATH}"/></svg>'
    return "data:image/svg+xml;utf8," + urllib.parse.quote(raw)


def install_all() -> None:
    # 1. Empty /opt/standard-components so NO pink-cube external bundle tabs appear at the bottom of the sidebar
    if COMPONENTS_ROOT.exists():
        for child in COMPONENTS_ROOT.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    print("[install_live_hooks] Cleared /opt/standard-components to eliminate separate bottom bundle tabs.")

    # 2. Copy the 14 single-class component modules directly into lfx/components/<native_category>/
    native_src = SOURCE_ROOT / "src" / "lfx_snowflake" / "native_category_components"
    installed_modules: list[tuple[str, str]] = []
    for cat_dir in sorted(native_src.iterdir()):
        if not cat_dir.is_dir() or cat_dir.name.startswith("_"):
            continue
        target_cat_dir = LFX_COMPONENTS_DIR / cat_dir.name
        target_cat_dir.mkdir(parents=True, exist_ok=True)
        for py_file in sorted(cat_dir.glob("*.py")):
            if py_file.name == "__init__.py":
                continue
            dest_file = target_cat_dir / py_file.name
            shutil.copyfile(py_file, dest_file)
            mod_fqn = f"lfx.components.{cat_dir.name}.{py_file.stem}"
            installed_modules.append((cat_dir.name, mod_fqn))
            print(f"[install_live_hooks] Installed native component module: {mod_fqn}")

    # 3. Rebuild lfx/_assets/component_index.json with the 14 Snowflake components merged into native categories
    from lfx.interface.components import _process_single_module

    blob = orjson.loads(LFX_INDEX_JSON.read_bytes())
    entries_map: dict[str, dict] = {cat: dict(comps) for cat, comps in blob["entries"]}

    for cat_name, mod_fqn in installed_modules:
        res = _process_single_module(mod_fqn)
        if not res or not res[1]:
            raise RuntimeError(f"Failed to template native component module: {mod_fqn}")
        top_level, comp_templates = res
        assert top_level == cat_name
        entries_map.setdefault(cat_name, {}).update(comp_templates)
        print(f"[install_live_hooks] Merged {list(comp_templates.keys())} into native category '{cat_name}'")

    blob["entries"] = [[cat, comps] for cat, comps in entries_map.items()]
    blob["metadata"]["num_modules"] = len(blob["entries"])
    blob["metadata"]["num_components"] = sum(len(comps) for _, comps in blob["entries"])
    blob.pop("sha256", None)
    digest = hashlib.sha256(orjson.dumps(blob, option=orjson.OPT_SORT_KEYS)).hexdigest()
    blob["sha256"] = digest
    LFX_INDEX_JSON.write_bytes(orjson.dumps(blob, option=orjson.OPT_SORT_KEYS))
    print(f"[install_live_hooks] Updated component_index.json (sha256={digest[:16]}...)")

    # 4. Patch langflow/main.py to mount /api/v1/snowflake/* routes
    main_text = LANGFLOW_MAIN.read_text(encoding="utf-8")
    hook_snippet = (
        "    load_plugin_routes(app)\n"
        "    from lfx_snowflake.fastapi_live_bridge import register_snowflake_fastapi_routes\n"
        "    register_snowflake_fastapi_routes(app)\n"
    )
    if "register_snowflake_fastapi_routes(app)" not in main_text:
        if "    load_plugin_routes(app)\n" not in main_text:
            raise RuntimeError("Could not find load_plugin_routes(app) in langflow/main.py")
        main_text = main_text.replace("    load_plugin_routes(app)\n", hook_snippet, 1)
        LANGFLOW_MAIN.write_text(main_text, encoding="utf-8")
        print("[install_live_hooks] Patched langflow/main.py with register_snowflake_fastapi_routes(app)")

    # 5. Patch compiled React bundle in langflow/frontend/assets/*.js (100% React Virtual-DOM safe!)
    light_svg_uri = _svg_data_uri("#000000")
    dark_svg_uri = _svg_data_uri("#ffffff")
    native_rki_fn = (
        'function rki(){return v.jsx("span",{className:"inline-flex items-center pl-1",'
        'children:v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:20,height:20,viewBox:"0 0 64 64",fill:"currentColor",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})})}'
    )

    for js_asset in (LANGFLOW_FRONTEND / "assets").glob("*.js"):
        txt = js_asset.read_text(encoding="utf-8", errors="ignore")
        orig = txt
        txt = txt.replace(
            "https://github.com/langflow-ai/langflow",
            "https://github.com/fbattaglieri-snowflake/SnowflakeLangflow",
        )
        txt = txt.replace(
            'xNn="https://docs.langflow.org"',
            'xNn="https://docs.snowflake.com/"',
        )
        txt = txt.replace(
            '"account.docs":"Docs"',
            '"account.docs":"Snowflake Docs"',
        )
        txt = txt.replace(
            'function rki(){return v.jsx(v.Fragment,{})}',
            native_rki_fn,
        )
        txt = txt.replace(
            'Vvo=""+new URL("logo_dark-DEVbuISE.png",import.meta.url).href,Uvo=""+new URL("logo_light-CIlityAH.png",import.meta.url).href',
            f'Vvo="{dark_svg_uri}",Uvo="{light_svg_uri}"',
        )
        txt = txt.replace(
            '"page.githubDescription":"Follow development, star the repo, and shape the future."',
            '"page.githubDescription":"Unofficial personal fork by Francesco Battaglieri (fbattaglieri-snowflake/SnowflakeLangflow) — Under Active Testing."',
        )
        txt = txt.replace(
            '"page.welcomeTitle":"Welcome to Langflow"',
            '"page.welcomeTitle":"Welcome to SnowflakeLangflow"',
        )
        txt = txt.replace(
            '"settings.nav.modelProviders":"Model Providers"',
            '"settings.nav.modelProviders":"Additional Model Providers"',
        )
        txt = txt.replace(
            '"modelProviders.pageTitle":"Model Providers"',
            '"modelProviders.pageTitle":"Additional Model Providers"',
        )
        txt = txt.replace(
            '"modelProviders.pageDescription":"Configure AI model providers and manage their API keys."',
            '"modelProviders.pageDescription":"Snowflake Proxy (Chat /chat/v1, Responses /responses/v1, Messages /messages) and Snowflake-hosted Ollama are pre-configured in Models & Agents. Configure any additional external model providers below."',
        )
        txt = txt.replace(
            '"settings.nav.dbProviders":"DB Providers"',
            '"settings.nav.dbProviders":"Additional DB Providers"',
        )
        txt = txt.replace(
            '"settings.dbProviders.title":"DB Providers"',
            '"settings.dbProviders.title":"Additional DB Providers"',
        )
        txt = txt.replace(
            '"settings.dbProviders.description":"Configure vector-store providers for Knowledge Bases."',
            '"settings.dbProviders.description":"Snowflake Vector Stores (Standard Table VECTOR, Hybrid Table VECTOR, Cortex Search, Snowflake Postgres pgvector) are pre-configured in Files & Knowledge. Configure any additional DB providers below."',
        )
        if txt != orig:
            js_asset.write_text(txt, encoding="utf-8")
            print(f"[install_live_hooks] Patched React bundle {js_asset.name}")

    # 6. Copy snowflake-ui-integration.js into Langflow static frontend and link in index.html + cache-busting query param
    ui_js_src = SOURCE_ROOT / "frontend_patches" / "snowflake-ui-integration.js"
    ui_js_dest = LANGFLOW_FRONTEND / "snowflake-ui-integration.js"
    shutil.copyfile(ui_js_src, ui_js_dest)

    index_html = LANGFLOW_FRONTEND / "index.html"
    html_text = index_html.read_text(encoding="utf-8")
    html_text = html_text.replace("<title>Langflow</title>", "<title>SnowflakeLangflow</title>")
    html_text = re.sub(
        r'src="(\./assets/index-[^"?]+\.js)(?:\?[^"]*)?"',
        r'src="\1?v=sflf033"',
        html_text,
    )
    inject_block = (
        "    <style>\n"
        '      [data-testid="empty_page_discord_button"],\n'
        '      [data-testid="menu_discord_button"],\n'
        '      [data-testid="menu_twitter_button"],\n'
        '      [data-testid="header_right_section_wrapper"] [data-testid="icon-Discord"] { display: none !important; }\n'
        "    </style>\n"
        '    <script defer src="./snowflake-ui-integration.js?v=sflf033"></script>\n'
        "  </head>"
    )
    if "snowflake-ui-integration.js" not in html_text:
        html_text = html_text.replace("  </head>", inject_block, 1)
    else:
        html_text = re.sub(
            r"<style>[\s\S]*?</style>\s*<script defer src=\"\./snowflake-ui-integration\.js[^\"]*\"></script>\s*</head>",
            inject_block,
            html_text,
        )
    index_html.write_text(html_text, encoding="utf-8")
    print("[install_live_hooks] Updated index.html with cache-busted bundle and snowflake-ui-integration.js?v=sflf033")

    # 7. Verify component_index.json integrity and FastAPI routes
    from langflow.main import create_app
    from langflow.plugin_routes import _get_route_keys
    from lfx.interface.components import _read_component_index

    verified_idx = _read_component_index()
    assert verified_idx is not None, "component_index.json failed SHA256 or structure verification!"
    idx_cats = dict(verified_idx["entries"])
    assert "SnowflakeDataSource" in idx_cats["data_source"]
    assert "SnowflakeCortexAnalyst" in idx_cats["data_source"]
    assert "SnowflakeObjectProvisioner" in idx_cats["data_source"]
    assert "StandardChat" in idx_cats["models_and_agents"]
    assert "StandardResponses" in idx_cats["models_and_agents"]
    assert "StandardMessages" in idx_cats["models_and_agents"]
    assert "PrivateOllama" in idx_cats["models_and_agents"]
    assert "SnowflakeCortexAgentA2A" in idx_cats["models_and_agents"]
    assert "SnowflakeAIFunction" in idx_cats["llm_operations"]
    assert "SnowflakeSmartRouter" in idx_cats["llm_operations"]
    assert "SnowflakeStageFileSystem" in idx_cats["files_and_knowledge"]
    assert "SnowflakeKnowledgeMemory" in idx_cats["files_and_knowledge"]
    assert "SnowflakePythonInterpreterPyPI" in idx_cats["utilities"]
    assert "SnowflakeToolGuardPolicies" in idx_cats["utilities"]

    app = create_app()
    sf_paths = sorted({path for path, _m in _get_route_keys(app) if "/api/v1/snowflake" in path})
    assert "/api/v1/snowflake/context" in sf_paths
    print("[install_live_hooks] Verification passed: 14 components merged into 5 native categories + 0 bottom bundles.")


if __name__ == "__main__":
    install_all()
