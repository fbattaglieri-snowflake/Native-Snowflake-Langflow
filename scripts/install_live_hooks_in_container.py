"""Build-time installer that wires `lfx_snowflake` routes, custom components, and frontend UI into Langflow."""

from __future__ import annotations

import shutil
from pathlib import Path

SITE_PACKAGES = Path("/app/.venv/lib64/python3.14/site-packages")
LANGFLOW_MAIN = SITE_PACKAGES / "langflow" / "main.py"
LANGFLOW_FRONTEND = SITE_PACKAGES / "langflow" / "frontend"
COMPONENTS_ROOT = Path("/opt/standard-components")
SOURCE_ROOT = Path("/opt/snowflake-langflow")


def install_all() -> None:
    # 1. Copy custom component categories into /opt/standard-components
    comp_src = SOURCE_ROOT / "src" / "lfx_snowflake" / "langflow_custom_components"
    for subdir in comp_src.iterdir():
        if subdir.is_dir() and subdir.name.startswith("snowflake_"):
            dest = COMPONENTS_ROOT / subdir.name
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(subdir, dest)
            print(f"[install_live_hooks] Installed component category: {dest}")

    # 2. Patch langflow/main.py to mount /api/v1/snowflake/* routes
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

    # 3. Copy snowflake-ui-integration.js into Langflow static frontend and link in index.html
    ui_js_src = SOURCE_ROOT / "frontend_patches" / "snowflake-ui-integration.js"
    ui_js_dest = LANGFLOW_FRONTEND / "snowflake-ui-integration.js"
    shutil.copyfile(ui_js_src, ui_js_dest)

    index_html = LANGFLOW_FRONTEND / "index.html"
    html_text = index_html.read_text(encoding="utf-8")
    html_text = html_text.replace("<title>Langflow</title>", "<title>SnowflakeLangflow</title>")
    script_tag = '    <script defer src="./snowflake-ui-integration.js"></script>\n  </head>'
    if "snowflake-ui-integration.js" not in html_text:
        html_text = html_text.replace("  </head>", script_tag, 1)
        index_html.write_text(html_text, encoding="utf-8")
        print("[install_live_hooks] Injected snowflake-ui-integration.js into index.html")

    # 4. Verify all custom components and FastAPI routes load cleanly
    from langflow.main import create_app
    from lfx_snowflake.langflow_custom_components.snowflake_agents_and_governance.agents_and_governance import (
        SnowflakeCortexAgentA2AComponent,
        SnowflakeObjectProvisionerSidebarComponent,
        SnowflakePythonInterpreterPyPIComponent,
        SnowflakeToolGuardPoliciesComponent,
    )
    from lfx_snowflake.langflow_custom_components.snowflake_data_sources.data_sources_and_analyst import (
        SnowflakeCortexAnalystComponent,
        SnowflakeDataSourceComponent,
    )
    from lfx_snowflake.langflow_custom_components.snowflake_files_and_knowledge.stage_and_memories import (
        SnowflakeKnowledgeMemoryComponent,
        SnowflakeStageFileSystemComponent,
    )
    from lfx_snowflake.langflow_custom_components.snowflake_llm_operations.ai_function_and_ops import (
        SnowflakeAIFunctionComponent,
        SnowflakeSmartRouterComponent,
    )

    assert all(
        cls.icon == "Snowflake"
        for cls in (
            SnowflakeDataSourceComponent,
            SnowflakeCortexAnalystComponent,
            SnowflakeAIFunctionComponent,
            SnowflakeSmartRouterComponent,
            SnowflakeStageFileSystemComponent,
            SnowflakeKnowledgeMemoryComponent,
            SnowflakeCortexAgentA2AComponent,
            SnowflakeObjectProvisionerSidebarComponent,
            SnowflakePythonInterpreterPyPIComponent,
            SnowflakeToolGuardPoliciesComponent,
        )
    )

    from langflow.plugin_routes import _get_route_keys

    app = create_app()
    sf_paths = sorted(
        {
            path
            for path, _method in _get_route_keys(app)
            if "/api/v1/snowflake" in path
        }
    )
    print("[install_live_hooks] Mounted FastAPI routes:", sf_paths)
    assert "/api/v1/snowflake/context" in sf_paths
    assert "/api/v1/snowflake/role" in sf_paths
    assert "/api/v1/snowflake/provision" in sf_paths
    assert "/api/v1/snowflake/traces" in sf_paths
    assert "/api/v1/snowflake/memories" in sf_paths
    print("[install_live_hooks] Verification passed: 10 custom components + FastAPI routes active.")


if __name__ == "__main__":
    install_all()
