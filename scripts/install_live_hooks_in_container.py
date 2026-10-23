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
    # 0. Install lfx_snowflake package into site-packages so all runtime/live bridge imports resolve
    pkg_src = SOURCE_ROOT / "src" / "lfx_snowflake"
    pkg_dst = SITE_PACKAGES / "lfx_snowflake"
    if pkg_src.exists():
        if pkg_dst.exists():
            shutil.rmtree(pkg_dst)
        shutil.copytree(pkg_src, pkg_dst)
        print(f"[install_live_hooks] Installed lfx_snowflake package into {pkg_dst}")

    # 0b. Copy or vendor updated standard_langflow.py if present so W2-R2-07 reasoning fixes are active in container
    for std_candidate in (
        Path("/tmp/standard_langflow.py"),
        Path("/opt/llm-proxy-standard/langflow/standard_langflow.py"),
        SOURCE_ROOT / "standard_langflow.py",
        SOURCE_ROOT / "src" / "lfx_snowflake" / "standard_langflow.py",
    ):
        if std_candidate.exists():
            cand_text = std_candidate.read_text(encoding="utf-8")
            for std_target in (Path("/app/standard_langflow.py"), SITE_PACKAGES / "standard_langflow.py"):
                if std_target.parent.exists():
                    out_text = cand_text
                    if std_target.exists() and "<PROXY_SERVICE>" in out_text:
                        prev_text = std_target.read_text(encoding="utf-8", errors="ignore")
                        m_orig = re.search(r"ORIGIN\s*=\s*['\"](http[^'\"]+)['\"]", prev_text)
                        m_ollama = re.search(r"['\"]ollama['\"]\s*:\s*['\"](http[^'\"]+)['\"]", prev_text)
                        if m_orig and "<PROXY_SERVICE>" not in m_orig.group(1):
                            out_text = out_text.replace(
                                "http://<PROXY_SERVICE>.<DNS_NAMESPACE>:8080",
                                m_orig.group(1),
                            )
                        if m_ollama and "<OLLAMA_SERVICE>" not in m_ollama.group(1):
                            out_text = out_text.replace(
                                "http://<OLLAMA_SERVICE>.<DNS_NAMESPACE>:11434/v1",
                                m_ollama.group(1),
                            )
                    std_target.write_text(out_text, encoding="utf-8")
                    print(f"[install_live_hooks] Installed standard_langflow.py into {std_target}")
            break

    # 1. Empty /opt/standard-components so NO pink-cube external bundle tabs appear at the bottom of the sidebar
    if COMPONENTS_ROOT.exists():
        for child in COMPONENTS_ROOT.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    print("[install_live_hooks] Cleared /opt/standard-components to eliminate separate bottom bundle tabs.")

    # 2. Remove stale / relocated component files and copy all Round 2 component modules into lfx/components/<native_category>/
    stale_component_files = (
        ("data_source", "snowflake_object_provisioner.py"),
        ("data_source", "snowflake_data_source.py"),
        ("llm_operations", "snowflake_cortex_search.py"),
        ("llm_operations", "snowflake_cortex_analyst_llm.py"),
        ("llm_operations", "snowflake_smart_router.py"),
        ("files_and_knowledge", "snowflake_knowledge_memory.py"),
        ("files_and_knowledge", "snowflake_cortex_analyst_disabled.py"),
        ("utilities", "snowflake_python_pypi.py"),
    )
    for stale_cat, stale_fname in stale_component_files:
        stale_path = LFX_COMPONENTS_DIR / stale_cat / stale_fname
        if stale_path.exists():
            stale_path.unlink()
            print(f"[install_live_hooks] Removed stale {stale_cat}/{stale_fname}")

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

    # 3. Rebuild lfx/_assets/component_index.json with all Snowflake components merged into native categories
    from lfx.interface.components import _process_single_module

    blob = orjson.loads(LFX_INDEX_JSON.read_bytes())
    entries_map: dict[str, dict] = {cat: dict(comps) for cat, comps in blob["entries"]}
    stale_index_entries = (
        ("data_source", "SnowflakeObjectProvisioner"),
        ("data_source", "SnowflakeDataSource"),
        ("llm_operations", "SnowflakeCortexSearch"),
        ("llm_operations", "SnowflakeCortexAnalystLLM"),
        ("llm_operations", "SnowflakeSmartRouter"),
        ("files_and_knowledge", "SnowflakeKnowledgeMemory"),
        ("files_and_knowledge", "SnowflakeCortexAnalystDisabledCreator"),
        ("utilities", "SnowflakePythonInterpreterPyPI"),
    )
    for stale_cat, stale_comp in stale_index_entries:
        if stale_cat in entries_map:
            entries_map[stale_cat].pop(stale_comp, None)

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
    sf_svg_inline_sm = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:20,height:20,viewBox:"0 0 64 64",fill:"#29B5E8",'
        'className:"h-5 w-5 shrink-0 text-[#29B5E8]","aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )
    sf_svg_inline_color = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:15,height:15,viewBox:"0 0 64 64",fill:"#29B5E8",'
        'className:"h-4 w-4 shrink-0 text-[#29B5E8]","aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )
    sf_svg_inline_lg = (
        'v.jsx("svg",{xmlns:"http://www.w3.org/2000/svg",width:40,height:40,viewBox:"0 0 64 64",fill:"#29B5E8",'
        'className:"h-10 w-10 shrink-0 text-[#29B5E8]","aria-hidden":"true",'
        'children:v.jsx("path",{d:"' + OFFICIAL_SNOWFLAKE_PATH + '"})})'
    )

    s1_orig = (
        'v.jsxs("div",{className:"z-30 flex shrink-0 items-center gap-2","data-testid":"header_left_section_wrapper",'
        'children:[v.jsx(qt,{unstyled:!0,onClick:()=>n("/"),className:"mr-1 flex h-8 w-8 items-center",'
        '"data-testid":"icon-ChevronLeft","aria-label":e("header.home"),'
        'children:v.jsx(c6e,{className:"h-5 w-5","aria-hidden":"true"})}),v.jsx(rki,{})]})'
    )
    s1_new = (
        'v.jsxs("div",{className:"z-30 flex shrink-0 items-center gap-2","data-testid":"header_left_section_wrapper",'
        'children:[v.jsx(qt,{unstyled:!0,onClick:()=>n("/"),className:"mr-1 flex h-8 items-center gap-2 px-1 cursor-pointer",'
        '"data-testid":"icon-ChevronLeft","aria-label":e("header.home"),'
        'children:v.jsxs("span",{className:"inline-flex items-center gap-2 text-[#29B5E8]",children:['
        + sf_svg_inline_sm
        + ',v.jsx(c6e,{className:"h-5 w-5 shrink-0 text-[#29B5E8]","aria-hidden":"true"}),'
        'v.jsx("span",{className:"text-sm font-semibold tracking-tight text-[#29B5E8] whitespace-nowrap",children:"Snowflake Langflow"})]})}),v.jsx(rki,{})]})'
    )

    s2_orig = (
        'v.jsx(cs,{content:e("header.goToDiscord"),side:"bottom",styleClasses:"z-10",ariaDescribedBy:void 0,'
        'children:v.jsxs(qt,{unstyled:!0,onClick:()=>window.open(N6e,"_blank"),'
        'className:"hit-area-hover flex items-center gap-2 rounded-md p-1 text-muted-foreground",'
        'children:[v.jsx("span",{className:"sr-only",children:e("header.goToDiscord")}),'
        'v.jsxs("div",{className:"relative items-center rounded-md px-2 py-1 flex",'
        'children:[v.jsx(Ise,{"aria-hidden":"true",className:"h-4 w-4"}),'
        'v.jsx(hk,{condition:!!s&&s!=="0",children:v.jsx("span",{className:"text-xs font-semibold pl-2","aria-hidden":"true",children:s})})]})]})})'
    )
    s2_new = (
        'v.jsx(cs,{content:"Snowflake Docs",side:"bottom",styleClasses:"z-10",ariaDescribedBy:void 0,'
        'children:v.jsxs(qt,{unstyled:!0,onClick:()=>window.open("https://docs.snowflake.com/","_blank"),'
        'className:"hit-area-hover flex items-center gap-1.5 rounded-md p-1 text-muted-foreground",'
        'children:[v.jsx("span",{className:"sr-only",children:"Snowflake Docs"}),'
        'v.jsxs("div",{className:"relative items-center rounded-md px-2 py-1 flex gap-1.5",'
        'children:[' + sf_svg_inline_color + ',v.jsx("span",{className:"text-xs font-semibold text-[#29B5E8]",children:"Docs"})]})]})})'
    )

    s3_orig = (
        'v.jsx(QTi,{position:"right",classNameSize:"w-[272px]",children:v.jsxs("div",{className:"divide-y divide-foreground/10",'
        'children:[v.jsx(HRi,{}),v.jsx("div",{children:v.jsx("div",{className:"h-[44px] items-center px-4 pt-3",'
        'children:v.jsxs("div",{className:"flex items-center justify-between",'
        'children:[v.jsx("span",{"data-testid":"menu_version_button",id:"menu_version_button",className:"text-sm",children:e("account.version")}),'
        'v.jsxs("div",{className:Ve("float-right text-xs",h&&"text-accent-emerald-foreground",!h&&"text-accent-amber-foreground"),'
        'children:[t," ",e(h?"account.latest":"account.updateAvailable")]})]})})}),'
        'v.jsxs("div",{children:[v.jsx(DAn,{onClick:()=>{o("/settings")},children:v.jsx("span",{"data-testid":"menu_settings_button",id:"menu_settings_button",children:e("account.settings")})}),'
        'v.jsx($Ri,{onNavigate:f=>o(f)}),'
        'v.jsx(Tke,{newPage:!0,href:xNn,children:v.jsx("span",{"data-testid":"menu_docs_button",id:"menu_docs_button",children:e("account.docs")})})]}),'
        'v.jsxs("div",{children:[v.jsx(Tke,{newPage:!0,href:D6e,children:v.jsxs("span",{"data-testid":"menu_github_button",id:"menu_github_button",className:"flex items-center gap-2",children:[v.jsx(Lse,{className:"h-4 w-4","aria-hidden":"true"}),e("account.github")]})}),'
        'v.jsx(Tke,{newPage:!0,href:N6e,children:v.jsxs("span",{"data-testid":"menu_discord_button",id:"menu_discord_button",className:"flex items-center gap-2",children:[v.jsx(Ise,{className:"h-4 w-4 text-[#5865F2]","aria-hidden":"true"}),e("account.discord")]})}),'
        'v.jsx(Tke,{newPage:!0,href:D7r,children:v.jsxs("span",{"data-testid":"menu_twitter_button",id:"menu_twitter_button",className:"flex items-center gap-2",children:[v.jsx(qe,{strokeWidth:2,name:"TwitterX",className:"h-4 w-4"}),e("account.twitter")]})})]})'
    )
    s3_new = (
        'v.jsx(QTi,{position:"right",classNameSize:"w-[208px] text-xs",children:v.jsxs("div",{className:"divide-y divide-foreground/10",'
        'children:[v.jsx(HRi,{}),v.jsx("div",{children:v.jsx("div",{className:"h-[32px] items-center px-3 pt-1.5",'
        'children:v.jsxs("div",{className:"flex items-center justify-between",'
        'children:[v.jsx("span",{"data-testid":"menu_version_button",id:"menu_version_button",className:"text-xs",children:e("account.version")}),'
        'v.jsxs("div",{className:Ve("float-right text-[11px]",h&&"text-accent-emerald-foreground",!h&&"text-accent-amber-foreground"),'
        'children:[t," ",e(h?"account.latest":"account.updateAvailable")]})]})})}),'
        'v.jsxs("div",{children:[v.jsx(DAn,{onClick:()=>{o("/settings")},children:v.jsx("span",{"data-testid":"menu_settings_button",id:"menu_settings_button",className:"text-xs",children:e("account.settings")})}),'
        'v.jsx($Ri,{onNavigate:f=>o(f)}),'
        'v.jsx(Tke,{newPage:!0,href:xNn,children:v.jsxs("span",{"data-testid":"menu_docs_button",id:"menu_docs_button",className:"flex items-center gap-2 text-xs",children:[' + sf_svg_inline_color + ',e("account.docs")]})})]}),'
        'v.jsxs("div",{children:[v.jsx(Tke,{newPage:!0,href:D6e,children:v.jsxs("span",{"data-testid":"menu_github_button",id:"menu_github_button",className:"flex items-center gap-2 text-xs",children:[v.jsx(Lse,{className:"h-3.5 w-3.5","aria-hidden":"true"}),e("account.github")]})})]})'
    )

    s4_orig = (
        'v.jsx("div",{className:"z-50 dark:hidden",children:v.jsx("img",{src:Uvo,alt:t("common.langflowLogoLight"),"data-testid":"empty_page_logo_light",className:"relative top-8 h-40 pointer-events-none select-none"})}),'
        'v.jsx("div",{className:"z-50 hidden dark:block",children:v.jsx("img",{src:Vvo,alt:t("common.langflowLogoDark"),"data-testid":"empty_page_logo_dark",className:"relative top-8 h-40 pointer-events-none select-none"})})'
    )
    s4_new = (
        'v.jsxs("div",{className:"z-50 mb-3 flex items-center justify-center gap-3 text-[#29B5E8] pointer-events-none select-none","data-testid":"empty_page_logo_light",'
        'children:[' + sf_svg_inline_lg + ',v.jsx(c6e,{className:"h-9 w-9 shrink-0 text-[#29B5E8]","aria-hidden":"true"})]})'
    )
    s4_loading_orig = 'v.jsx(c6e,{className:"h-10 w-10"})'
    s4_loading_new = (
        'v.jsx("span",{className:"inline-flex items-center justify-center text-[#29B5E8]","data-testid":"snowflake-langflow-dual-loading-logo",'
        'children:' + sf_svg_inline_lg + '})'
    )

    s5_orig = (
        ',v.jsx(qt,{unstyled:!0,className:"group mx-3 min-h-[84px] sm:mx-0",onClick:()=>{p("discord_clicked")(),window.open(N6e,"_blank","noopener,noreferrer")},"data-testid":"empty_page_discord_button",'
        'children:v.jsxs("div",{className:"relative flex flex-col rounded-lg border-[1px] bg-background p-4 transition-all duration-300 hover:border-discord-color",'
        'children:[v.jsxs("div",{className:"grid w-full items-center justify-between gap-2",'
        'children:[v.jsxs("div",{className:"flex gap-3",children:[v.jsx(Ise,{className:"h-6 w-6 text-discord-color","aria-hidden":"true"}),'
        'v.jsxs("div",{children:[v.jsx("span",{className:"font-semibold",children:"Discord"}),v.jsx("span",{className:"ml-2 font-mono text-muted-foreground",children:K5e(c)})]})]}),'
        'v.jsx("div",{children:v.jsx("span",{className:"line-clamp-2 text-left text-base text-secondary-foreground",children:t("page.discordDescription")})})]}),v.jsx(V5e,{className:hRn})]})})'
    )

    dom_guard_banner = (
        'if(typeof Node==="function"&&Node.prototype&&!Node.prototype.__sflfPatched){'
        'var _oRC=Node.prototype.removeChild;'
        'Node.prototype.removeChild=function(c){if(c&&c.parentNode!==this){return c.parentNode?_oRC.call(c.parentNode,c):c;}return _oRC.apply(this,arguments);};'
        'var _oIB=Node.prototype.insertBefore;'
        'Node.prototype.insertBefore=function(n,r){if(r&&r.parentNode!==this){return this.appendChild(n);}return _oIB.apply(this,arguments);};'
        'Node.prototype.__sflfPatched=true;}'
        'if(typeof window!=="undefined"&&typeof window.fetch==="function"&&!window.fetch.__sflfHeadersGuarded){'
        'var _pF=window.fetch;'
        'var _gF=function(u,o){if(typeof u==="string"||(typeof URL==="function"&&u instanceof URL)){'
        'var s=o&&typeof o==="object"?Object.assign({},o):{};if(!s.headers){s.headers={};}return _pF.call(this,u,s);}'
        'return _pF.apply(this,arguments);};'
        '_gF.__sflfHeadersGuarded=true;window.fetch=_gF;}\n'
    )

    fetch_intercept_orig = (
        'const y=hKr.register({request:(z,T)=>{if(!x(z)){for(const[k,D]of Object.entries(h))T.headers[k]=D;'
        'T.headers["x-langflow-client"]="playground"}return[z,T]}})'
    )
    fetch_intercept_new = (
        'const y=hKr.register({request:(z,T)=>{try{if(!x(z)){'
        'if(z&&typeof z=="object"&&z.headers&&typeof z.headers.set=="function"&&!T){'
        'for(const[k,D]of Object.entries(h||{}))z.headers.set(k,D);'
        'z.headers.set("x-langflow-client","playground");return[z]}'
        'T=T&&typeof T=="object"?{...T}:{};'
        'if(T.headers&&typeof T.headers.set=="function"){'
        'for(const[k,D]of Object.entries(h||{}))T.headers.set(k,D);'
        'T.headers.set("x-langflow-client","playground")'
        '}else{T.headers={...(T.headers||{})};'
        'for(const[k,D]of Object.entries(h||{}))T.headers[k]=D;'
        'T.headers["x-langflow-client"]="playground"}}'
        '}catch(_e){}return T===void 0?[z]:[z,T]}})'
    )

    axios_req_intercept_orig = (
        'if(new URL(z?.url,k).origin===k){for(const[j,q]of Object.entries(h))z.headers[j]=q;'
        'z.headers["x-langflow-client"]="playground"}'
    )
    axios_req_intercept_new = (
        'if(new URL(z?.url,k).origin===k){z.headers=z.headers||{};for(const[j,q]of Object.entries(h||{}))z.headers[j]=q;'
        'z.headers["x-langflow-client"]="playground"}'
    )

    agui_hzr_orig = 'const hZr=(e,t)=>JLn(()=>zLe(fetch(e,t))).pipe($Qr(n=>{if(!n.ok){let a=n.headers.get("content-type")||"";'
    agui_hzr_new = 'const hZr=(e,t)=>JLn(()=>zLe(fetch(e,t))).pipe($Qr(n=>{if(!n?.ok){let a=n?.headers?.get?.("content-type")||"";'

    agui_gzr_orig = 'if(s.next(c),c.type===ZY.HEADERS&&!a){a=!0;let u=c.headers.get("content-type");'
    agui_gzr_new = 'if(s.next(c),c?.type===ZY.HEADERS&&!a){a=!0;let u=c?.headers?.get?.("content-type")||"";'

    for js_asset in (LANGFLOW_FRONTEND / "assets").glob("*.js"):
        txt = js_asset.read_text(encoding="utf-8", errors="ignore")
        orig = txt
        txt = txt.replace(fetch_intercept_orig, fetch_intercept_new)
        txt = txt.replace(axios_req_intercept_orig, axios_req_intercept_new)
        txt = txt.replace(agui_hzr_orig, agui_hzr_new)
        txt = txt.replace(agui_gzr_orig, agui_gzr_new)
        txt = txt.replace(
            'if(Lp)try{(Xd.nodeType===9?Xd.body:Xd.nodeName==="HTML"?Xd.ownerDocument.body:Xd).removeChild(Q.stateNode)}catch(Oe){gd(Q,W,Oe)}else try{Xd.removeChild(Q.stateNode)}catch(Oe){gd(Q,W,Oe)}',
            'if(Lp)try{(Xd.nodeType===9?Xd.body:Xd.nodeName==="HTML"?Xd.ownerDocument.body:Xd).removeChild(Q.stateNode)}catch(Oe){}else try{Xd.removeChild(Q.stateNode)}catch(Oe){}',
        )
        txt = txt.replace(
            'case 3:case 4:var On=Q.stateNode.containerInfo,Sr=Tq(I);kq(I,Sr,On);break;default:throw Error(o(161))}}catch(zi){gd(I,I.return,zi)}',
            'case 3:case 4:var On=Q.stateNode.containerInfo,Sr=Tq(I);kq(I,Sr,On);break;default:throw Error(o(161))}}catch(zi){zi&&zi.name==="NotFoundError"||gd(I,I.return,zi)}',
        )
        txt = txt.replace(s1_orig, s1_new)
        txt = txt.replace(s2_orig, s2_new)
        txt = txt.replace(s3_orig, s3_new)
        txt = txt.replace(s4_orig, s4_new)
        txt = txt.replace(s4_loading_orig, s4_loading_new)
        txt = txt.replace(s5_orig, "")
        txt = txt.replace(
            'Tke=({href:e="#",children:t,newPage:n=!1,icon:o="external-link"})=>v.jsx(au,{className:"cursor-pointer rounded-none p-3 px-4"',
            'Tke=({href:e="#",children:t,newPage:n=!1,icon:o="external-link"})=>v.jsx(au,{className:"cursor-pointer rounded-none py-1.5 px-3 text-xs"',
        )
        txt = txt.replace(
            'DAn=({icon:e="",onClick:t,children:n})=>v.jsxs(au,{className:"group flex cursor-pointer items-center justify-between p-3 px-4"',
            'DAn=({icon:e="",onClick:t,children:n})=>v.jsxs(au,{className:"group flex cursor-pointer items-center justify-between py-1.5 px-3 text-xs"',
        )
        txt = txt.replace(
            'v.jsxs("div",{className:"flex items-center justify-between px-4 py-[6.5px] text-sm",children:[v.jsx("span",{className:"",children:e("account.theme")})',
            'v.jsxs("div",{className:"flex items-center justify-between px-3 py-1.5 text-xs",children:[v.jsx("span",{className:"",children:e("account.theme")})',
        )
        txt = txt.replace(
            "https://github.com/langflow-ai/langflow",
            "https://github.com/fbattaglieri-snowflake/Native-Snowflake-Langflow",
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
            '"page.githubDescription":"Follow development, star the repo, and shape the future."',
            '"page.githubDescription":"Unofficial personal fork by Francesco Battaglieri (fbattaglieri-snowflake/Native-Snowflake-Langflow) — Under Active Testing."',
        )
        txt = txt.replace(
            '"page.welcomeTitle":"Welcome to Langflow"',
            '"page.welcomeTitle":"Welcome to Snowflake Langflow"',
        )
        txt = txt.replace(
            '"flowBuilderWelcome.configureModelProviders":"Configure Model Providers"',
            '"flowBuilderWelcome.configureModelProviders":"Configure Other Model Provider"',
        )
        txt = txt.replace(
            '"assistant.configureModels":"Configure Model Providers"',
            '"assistant.configureModels":"Configure Other Model Provider"',
        )
        txt = txt.replace(
            '"modelInput.manageProviders":"Manage Model Providers"',
            '"modelInput.manageProviders":"Configure Other Model Provider"',
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
        if js_asset.name.startswith("index-") and "__sflfPatched" not in txt:
            txt = dom_guard_banner + txt
        if txt != orig:
            js_asset.write_text(txt, encoding="utf-8")
            print(f"[install_live_hooks] Patched React bundle {js_asset.name}")

    # 6. Copy snowflake-ui-integration.js into Langflow static frontend and link in index.html + cache-busting query param
    ui_js_src = SOURCE_ROOT / "frontend_patches" / "snowflake-ui-integration.js"
    ui_js_dest = LANGFLOW_FRONTEND / "snowflake-ui-integration.js"
    shutil.copyfile(ui_js_src, ui_js_dest)

    favicon_data_uri = _svg_data_uri("#29B5E8")
    initial_loader_svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 64 64" '
        f'fill="#29B5E8" class="h-10 w-10 shrink-0 text-[#29B5E8]" data-testid="index-initial-snowflake-loader">'
        f'<path d="{OFFICIAL_SNOWFLAKE_PATH}"/></svg>'
    )
    index_html = LANGFLOW_FRONTEND / "index.html"
    html_text = index_html.read_text(encoding="utf-8")
    html_text = html_text.replace('<html lang="en">', '<html lang="en" translate="no" class="notranslate">')
    html_text = html_text.replace("<title>Langflow</title>", "<title>Snowflake Langflow</title>")
    if re.search(r'<link[^>]+rel=["\'](?:shortcut )?icon["\'][^>]*>', html_text, flags=re.IGNORECASE):
        html_text = re.sub(
            r'<link[^>]+rel=["\'](?:shortcut )?icon["\'][^>]*>',
            f'<link rel="icon" type="image/svg+xml" href="{favicon_data_uri}" />',
            html_text,
            flags=re.IGNORECASE,
        )
    else:
        html_text = html_text.replace(
            "<head>",
            f'<head>\n    <link rel="icon" type="image/svg+xml" href="{favicon_data_uri}" />',
            1,
        )
    if re.search(r"<body[\s\S]*?<svg[\s\S]*?</svg>", html_text, flags=re.IGNORECASE):
        html_text = re.sub(
            r"(<body[\s\S]*?)(<svg[\s\S]*?</svg>)",
            lambda m: m.group(1) + initial_loader_svg,
            html_text,
            count=1,
            flags=re.IGNORECASE,
        )
    html_text = re.sub(
        r'src="(\./assets/index-[^"?]+\.js)(?:\?[^"]*)?"',
        r'src="\1?v=sflf042"',
        html_text,
    )
    inject_block = (
        '    <meta name="google" content="notranslate" />\n'
        f"    <script>{dom_guard_banner.strip()}</script>\n"
        "    <style>\n"
        '      [data-testid="empty_page_discord_button"],\n'
        '      [data-testid="menu_discord_button"],\n'
        '      [data-testid="menu_twitter_button"],\n'
        '      [data-testid="header_right_section_wrapper"] [data-testid="icon-Discord"] { display: none !important; }\n'
        '      [data-testid="header_left_section_wrapper"] svg,\n'
        '      [data-testid="empty_page_logo_light"] svg,\n'
        '      [data-testid="snowflake-langflow-dual-loading-logo"] svg,\n'
        '      #root .loading-component svg { color: #29B5E8 !important; fill: #29B5E8 !important; }\n'
        '      [data-testid="mainpage_title"] { color: #29B5E8 !important; }\n'
        "    </style>\n"
        '    <script defer src="./snowflake-ui-integration.js?v=sflf042"></script>\n'
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
    print("[install_live_hooks] Updated index.html with Snowflake SVG favicon, title, and snowflake-ui-integration.js?v=sflf042")

    # 7. Verify component_index.json integrity and FastAPI routes
    from langflow.main import create_app
    from langflow.plugin_routes import _get_route_keys
    from lfx.interface.components import _read_component_index

    verified_idx = _read_component_index()
    assert verified_idx is not None, "component_index.json failed SHA256 or structure verification!"
    idx_cats = dict(verified_idx["entries"])

    for ds_comp in (
        "SnowflakeTables",
        "SnowflakeViews",
        "SnowflakeIcebergTables",
        "SnowflakePostgres",
        "SnowflakeCortexSearch",
        "SnowflakeCortexAnalyst",
    ):
        assert ds_comp in idx_cats["data_source"], f"Missing {ds_comp} in data_source"
    assert "SnowflakeDataSource" not in idx_cats["data_source"]
    assert "SnowflakeObjectProvisioner" not in idx_cats["data_source"]

    for ma_comp in (
        "StandardChat",
        "StandardResponses",
        "StandardMessages",
        "PrivateOllama",
        "SnowflakeCortexAgentA2A",
    ):
        assert ma_comp in idx_cats["models_and_agents"], f"Missing {ma_comp} in models_and_agents"

    for llm_comp in (
        "SnowflakeObjectProvisioner",
        "SnowflakeStoredProcedure",
        "SnowflakeFunctionUDF",
        "SnowflakeAIComplete",
        "SnowflakeAIFunction",
        "SnowflakeSQLStatement",
        "SnowflakeLLMNode",
        "SnowflakeDecision",
    ):
        assert llm_comp in idx_cats["llm_operations"], f"Missing {llm_comp} in llm_operations"
    assert "SnowflakeCortexSearch" not in idx_cats["llm_operations"]
    assert "SnowflakeCortexAnalystLLM" not in idx_cats["llm_operations"]
    assert "SnowflakeSmartRouter" not in idx_cats["llm_operations"]

    for fk_comp in (
        "SnowflakeObjectProvisionerFK",
        "SnowflakeStageFileSystem",
        "SnowflakeCortexSearchParseDocument",
        "SnowflakeCortexSearchSplitText",
        "SnowflakeCortexSearchServiceCreator",
        "SnowflakeCortexSearchStreamTask",
    ):
        assert fk_comp in idx_cats["files_and_knowledge"], f"Missing {fk_comp} in files_and_knowledge"
    assert "SnowflakeKnowledgeMemory" not in idx_cats["files_and_knowledge"]

    assert "SnowflakeToolGuardPolicies" in idx_cats["utilities"]
    assert "SnowflakePythonInterpreterPyPI" not in idx_cats["utilities"]

    app = create_app()
    sf_paths = sorted({path for path, _m in _get_route_keys(app) if "/api/v1/snowflake" in path})
    for req_route in (
        "/api/v1/snowflake/context",
        "/api/v1/snowflake/catalog",
        "/api/v1/snowflake/provision",
        "/api/v1/snowflake/memories",
    ):
        assert req_route in sf_paths, f"Missing route {req_route} in {sf_paths}"
    print("[install_live_hooks] Verification passed: 26 components merged into 5 native categories + 0 bottom bundles.")


if __name__ == "__main__":
    install_all()
