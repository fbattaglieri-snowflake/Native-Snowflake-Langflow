"""End-to-End Automated Test Suite for Wave 2 Round 4 (`W2-R4-01` .. `W2-R4-07` & Priority A/B Gates).

Covers:
- `W2-R4-01`: `[ + ❄️ Snowflake ]` button element ordering (`+` before Snowflake icon and `Snowflake` text) across `snowflake-ui-integration.js`, `install_live_hooks_in_container.py`, and `settings_and_memories_modals.tsx`.
- `W2-R4-02`: Intermediate dropdown menu (`#sflf-memory-type-dropdown`) with `#1..#5` active (`len(AGENT_MEMORY_TYPES) == 5`) + `#6 Business Ontology / Cortex Sense (Coming Soon)` disabled (`COMING_SOON_MEMORY_TYPES`), plus contextual hover tooltip card (`#sflf-memory-type-hover-card`).
- `W2-R4-03` & `W2-R4-04`: 3-step paginated Wizard (`Step 1` Branch A `Database ▸ Schema ▸ Table` vs Branch B `Snowflake Postgres` `:5432` separation; `Step 2` dynamic object peculiarities `Hybrid Table` PK without `VECTOR`, `Dynamic Table` `TARGET_LAG`/`WAREHOUSE`/`REFRESH_MODE`, `Interactive Table` `CLUSTER BY`, `Standard Table` `VARIANT`, `PostgresSaver`/`PostgresStore`/`pgvector`; `Step 3` 4 Snowflake embedding sources + `NO_INGEST` LLM preprocessing).
- `W2-R4-05`: `GET /api/v1/snowflake/memories/{name}`, `POST /api/v1/snowflake/memories/{name}/reload`, `DELETE /api/v1/snowflake/memories/{name}`, `POST /api/v1/snowflake/memories/recall`, and frontend `renderSnowflakeMemoryDetailView` (`Header Controls`, `Configuration Summary`, `Session History & Stored Records`).
- `W2-R4-06` & `W2-R4-06B`: `SnowflakeMemoryBaseComponent` and `SnowflakeMemoryBaseNativeComponent` dynamic field/port adaptation across all 5 types (`#1..#5`), `SnowflakeMessageHistoryNativeComponent` binding, and strict `Data Sources` purity.
- `W2-R4-07`: End-to-end instantiation and multi-turn execution of `Snowflake_Full_Memory_Enterprise_Agent` verifying simultaneous operation of `MEM_1_GRAPH_CHECKPOINTER`, `MEM_2_CHAT_WINDOW`, `MEM_3_SESSION_SUMMARY`, `MEM_4_USER_ENTITY_PROFILE` (`NO_INGEST` skip on trivial turn and `MERGE` on preference turn), `MEM_5_EPISODIC_FEWSHOT` (`Tool Mode` recall with `outcome_score >= 0.9`), `#7` (`Cortex Analyst` + `Cortex Search`), and `#8` (`System Message`).
- Priority A & B Gates: RBAC fail-closed (`403 AUTHZ_ROLE_DENIED`, `422 UNSUPPORTED_CAPABILITY`), zero diff on `frontend_patches/preserved_provisioning_and_memories_concept.js`, and public repository sanitization.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import subprocess
import sys
import types
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lfx_snowflake.api_routes import SnowflakeAPIRouter  # noqa: E402
from lfx_snowflake.bundle_manifest import get_bundle_manifest  # noqa: E402
from lfx_snowflake.components.data_sources import (  # noqa: E402
    SnowflakeDataSourcesComponent,
    filter_business_data_source_objects,
    is_technical_memory_table,
)
from lfx_snowflake.components.knowledge_and_memories import (  # noqa: E402
    CreateMemoryDialogService,
    DYNAMIC_MEMORY_BASE_TYPE_SPECS,
    InMemoryVectorStoreEngine,
    LLMPreprocessingConfig,
    SnowflakeKnowledgeComponent,
    SnowflakeMemoryBaseComponent,
)
from lfx_snowflake.fastapi_live_bridge import (  # noqa: E402
    AGENT_MEMORY_TYPES,
    COMING_SOON_MEMORY_TYPES,
    DROPDOWN_MEMORY_TYPES,
    LIVE_BRIDGE,
    LLM_PREPROCESSING_ENGINES,
    SNOWFLAKE_EMBEDDING_SOURCES,
    LiveSnowflakeBridge,
    register_snowflake_fastapi_routes,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_memory_base import (  # noqa: E402
    SnowflakeMemoryBaseNativeComponent,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_message_history import (  # noqa: E402
    SnowflakeMessageHistoryNativeComponent,
)
from lfx_snowflake.showcase_full_memory_agent import (  # noqa: E402
    SHOWCASE_FLOW_ID,
    SHOWCASE_FLOW_NAME,
    SHOWCASE_MEMORIES_SPECS,
    bootstrap_and_run_full_memory_showcase,
    build_snowflake_full_memory_enterprise_agent_flow,
    run_full_memory_showcase_simulation,
)


def _build_fastapi_test_client() -> Any:
    """Build a TestClient for `register_snowflake_fastapi_routes(app)` with fallback shim when FastAPI is absent on host."""
    try:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()
        register_snowflake_fastapi_routes(app)
        return TestClient(app)
    except (ModuleNotFoundError, ImportError):
        pass

    class _StubJSONResponse:
        def __init__(self, status_code: int = 200, content: Any = None) -> None:
            self.status_code = int(status_code)
            self.content = content

        def json(self) -> Any:
            return self.content

    class _StubRequest:
        def __init__(self, method: str, url: str, json_body: Any = None) -> None:
            parsed = urlsplit(url)
            self.method = method.upper()
            self.url = types.SimpleNamespace(path=parsed.path)
            self.query_params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            self.headers: dict[str, str] = {}
            self._json_body = json_body

        async def json(self) -> Any:
            if self._json_body is None:
                raise ValueError("No JSON body")
            return self._json_body

    class _StubAPIRouter:
        def __init__(self, prefix: str = "", tags: list[str] | None = None) -> None:
            self.prefix = prefix.rstrip("/")
            self.tags = tags or []
            self.routes: list[tuple[str, str, Any]] = []

        def _register(self, method: str, path: str):
            full_path = f"{self.prefix}{path}"

            def _decorator(fn: Any) -> Any:
                self.routes.append((method.upper(), full_path, fn))
                return fn

            return _decorator

        def get(self, path: str, **_kwargs: Any):
            return self._register("GET", path)

        def post(self, path: str, **_kwargs: Any):
            return self._register("POST", path)

        def put(self, path: str, **_kwargs: Any):
            return self._register("PUT", path)

        def patch(self, path: str, **_kwargs: Any):
            return self._register("PATCH", path)

        def delete(self, path: str, **_kwargs: Any):
            return self._register("DELETE", path)

    class _StubApp:
        def __init__(self) -> None:
            self.routers: list[_StubAPIRouter] = []
            self.middlewares: list[Any] = []

        def include_router(self, router: _StubAPIRouter) -> None:
            self.routers.append(router)

        def middleware(self, _kind: str):
            def _decorator(fn: Any) -> Any:
                self.middlewares.append(fn)
                return fn

            return _decorator

    class _StubClient:
        def __init__(self, app: _StubApp) -> None:
            self.app = app

        def _dispatch(self, method: str, url: str, json_body: Any = None) -> _StubJSONResponse:
            req = _StubRequest(method, url, json_body=json_body)
            req_path = req.url.path
            for router in self.app.routers:
                for r_method, r_pattern, handler in router.routes:
                    if r_method != method.upper():
                        continue
                    param_names = re.findall(r"\{([a-zA-Z0-9_]+)\}", r_pattern)
                    regex_pat = "^" + re.sub(r"\{[a-zA-Z0-9_]+\}", r"([^/]+)", r_pattern) + "$"
                    m = re.match(regex_pat, req_path)
                    if not m:
                        continue
                    path_kwargs = {
                        name: unquote(val) for name, val in zip(param_names, m.groups(), strict=False)
                    }
                    sig = inspect.signature(handler)
                    call_kwargs: dict[str, Any] = {}
                    for p_name in sig.parameters:
                        if p_name == "request":
                            call_kwargs["request"] = req
                        elif p_name in path_kwargs:
                            call_kwargs[p_name] = path_kwargs[p_name]
                    res = asyncio.run(handler(**call_kwargs))
                    if hasattr(res, "json") and hasattr(res, "status_code"):
                        return res
                    return _StubJSONResponse(status_code=200, content=res)
            return _StubJSONResponse(status_code=404, content={"ok": False, "error": f"Route not found: {method} {req_path}"})

        def get(self, url: str) -> _StubJSONResponse:
            return self._dispatch("GET", url)

        def post(self, url: str, json: Any = None) -> _StubJSONResponse:
            return self._dispatch("POST", url, json_body=json)

        def delete(self, url: str) -> _StubJSONResponse:
            return self._dispatch("DELETE", url)

    fastapi_mod = types.ModuleType("fastapi")
    fastapi_mod.APIRouter = _StubAPIRouter  # type: ignore[attr-defined]
    fastapi_mod.Request = _StubRequest  # type: ignore[attr-defined]
    fastapi_resp_mod = types.ModuleType("fastapi.responses")
    fastapi_resp_mod.JSONResponse = _StubJSONResponse  # type: ignore[attr-defined]
    sys.modules.setdefault("fastapi", fastapi_mod)
    sys.modules.setdefault("fastapi.responses", fastapi_resp_mod)

    stub_app = _StubApp()
    register_snowflake_fastapi_routes(stub_app)
    return _StubClient(stub_app)


def test_w2_r4_01_memories_header_button_plus_precedes_snowflake_icon_and_label() -> None:
    """W2-R4-01: `[ + ❄️ Snowflake ]` button puts `+` BEFORE the Snowflake icon and text across all patches."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    live_hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    modals_tsx = (REPO_ROOT / "frontend_patches" / "settings_and_memories_modals.tsx").read_text(encoding="utf-8")

    # 1. Verify in snowflake-ui-integration.js (`#sflf-memories-plus-snowflake-btn`) that '+' comes before buildOfficialSnowflakeSvg and 'Snowflake'
    m_ui = re.search(
        r'btn\.id\s*=\s*"sflf-memories-plus-snowflake-btn";[\s\S]{0,500}?btn\.innerHTML\s*=\s*([^\n;]+);',
        ui_js,
    )
    assert m_ui is not None, "Missing #sflf-memories-plus-snowflake-btn innerHTML in snowflake-ui-integration.js"
    inner_expr = m_ui.group(1)
    plus_pos = inner_expr.find("<span>+</span>")
    svg_pos = inner_expr.find("buildOfficialSnowflakeSvg")
    text_pos = inner_expr.find("Snowflake")
    assert 0 <= plus_pos < svg_pos < text_pos, (
        f"Expected '+' before Snowflake SVG and 'Snowflake' text in snowflake-ui-integration.js, got: {inner_expr}"
    )

    # 2. Verify in install_live_hooks_in_container.py (`too_new`) that Plus icon comes before sf_svg_inline_xs_white and "Snowflake"
    too_pos = live_hooks_py.find('"data-testid":"memories-plus-snowflake-btn"')
    assert too_pos != -1, "Missing memories-plus-snowflake-btn in install_live_hooks_in_container.py"
    too_slice = live_hooks_py[too_pos : too_pos + 550]
    plus_idx = too_slice.find('name:"Plus"')
    svg_idx = too_slice.find("sf_svg_inline_xs_white")
    label_idx = too_slice.find(',"Snowflake"')
    assert 0 <= plus_idx < svg_idx < label_idx, (
        f"Expected Plus icon before sf_svg_inline_xs_white and 'Snowflake' in install_live_hooks_in_container.py, got: {too_slice}"
    )
    assert "window.openSnowflakeMemoryTypeDropdown" in too_slice

    # 3. Verify in settings_and_memories_modals.tsx (`MemoriesPlusSnowflakeButton`) that '+' precedes icon and 'Snowflake'
    btn_pos = modals_tsx.find('data-testid="memories-plus-snowflake-btn"')
    assert btn_pos != -1
    btn_slice = modals_tsx[btn_pos : btn_pos + 600]
    assert (
        btn_slice.find('data-testid="memories-plus-snowflake-plus">+<')
        < btn_slice.find('data-testid="memories-plus-snowflake-icon"')
        < btn_slice.find('data-testid="memories-plus-snowflake-label">Snowflake<')
    )


def test_w2_r4_02_five_canonical_memory_types_plus_coming_soon_slot_6_and_hover_card() -> None:
    """W2-R4-02: 5 canonical memory types remain intact (`len(AGENT_MEMORY_TYPES) == 5`) while slot #6 is Coming Soon."""
    assert len(AGENT_MEMORY_TYPES) == 5
    canonical_ids = [t["id"] for t in AGENT_MEMORY_TYPES]
    assert canonical_ids == [
        "graph_state_checkpointer",
        "buffer_window_memory",
        "summary_memory",
        "user_profile_entity_memory",
        "episodic_memory",
    ]

    assert len(COMING_SOON_MEMORY_TYPES) == 1
    slot6 = COMING_SOON_MEMORY_TYPES[0]
    assert slot6["id"] == "business_ontology_cortex_sense"
    assert slot6["enabled"] is False
    assert slot6["badge"] == "Coming Soon"
    assert len(DROPDOWN_MEMORY_TYPES) == 6

    bridge = LiveSnowflakeBridge()
    res_slot6 = bridge.create_or_register_memory(
        {
            "name": "TRY_CORTEX_SENSE_MEM",
            "agent_memory_type": "business_ontology_cortex_sense",
        }
    )
    assert res_slot6["ok"] is False
    assert res_slot6["status_code"] == 422
    assert res_slot6["error_code"] == "UNSUPPORTED_CAPABILITY"

    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert "sflf-memory-type-dropdown" in ui_js
    assert '"snowflake-memory-type-dropdown"' in ui_js
    assert "sflf-memory-type-hover-card" in ui_js
    assert 'data-testid="snowflake-memory-type-hover-tooltip"' in ui_js
    assert "business_ontology_cortex_sense" in ui_js
    assert "Coming Soon" in ui_js
    for field_label in (
        "Per cosa usarla (Purpose)",
        "Oggetti Snowflake consigliati (Recommended Objects)",
        "Serve Embedding?",
    ):
        assert field_label in ui_js


def test_w2_r4_03_and_04_branch_a_vs_branch_b_ddl_and_object_peculiarities() -> None:
    """W2-R4-03 & W2-R4-04: Branch A (`Database ▸ Schema ▸ Table`) vs Branch B (`Snowflake Postgres` :5432) DDL & rules."""
    bridge = LiveSnowflakeBridge()
    bridge.switch_role("ACCOUNTADMIN")

    # 1. Branch A Hybrid Table with VECTOR: ensure VECTOR column is excluded from PRIMARY KEY and INDEX
    res_hybrid_vec = bridge.create_or_register_memory(
        {
            "name": "EPISODIC_HYBRID_VEC",
            "agent_memory_type": "episodic_memory",
            "storage_family": "snowflake_sql",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "table_type": "HYBRID",
            "primary_key": "(user_id, chunk_id, embedding)",
            "secondary_index": "topic, embedding",
            "enable_embedding": True,
            "embedding_provider": "snowflake_cortex",
            "embedding_model": "snowflake-arctic-embed-l-v2.0",
            "dry_run": True,
        }
    )
    assert res_hybrid_vec["ok"] is True
    mem_hv = res_hybrid_vec["memory"]
    assert mem_hv["storage_family"] == "snowflake_sql"
    assert mem_hv["primary_key"] == "(user_id, chunk_id)"
    assert mem_hv["secondary_index"] == "topic"
    assert mem_hv["vector_excluded_from_pk_and_index"] is True
    assert "PRIMARY KEY (user_id, chunk_id)" in res_hybrid_vec["ddl"]
    assert "INDEX idx_episodic_hybrid_vec (topic)" in res_hybrid_vec["ddl"]
    assert "embedding VECTOR(FLOAT, 1024)" in res_hybrid_vec["ddl"]

    # 2. Branch A Dynamic Table: requires TARGET_LAG, WAREHOUSE, REFRESH_MODE
    res_dynamic = bridge.create_or_register_memory(
        {
            "name": "PROFILE_DYNAMIC_MEM",
            "agent_memory_type": "user_profile_entity_memory",
            "storage_family": "snowflake_sql",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "table_type": "DYNAMIC",
            "target_lag": "5 minutes",
            "warehouse": "COMPUTE_WH",
            "refresh_mode": "INCREMENTAL",
            "source_table": "SNOWFLAKE_LANGFLOW.CORE.RAW_USER_EVENTS",
            "dry_run": True,
        }
    )
    assert res_dynamic["ok"] is True
    assert "CREATE OR REPLACE DYNAMIC TABLE SNOWFLAKE_LANGFLOW.CORE.MEM_PROFILE_DYNAMIC_MEM" in res_dynamic["ddl"]
    assert "TARGET_LAG = '5 minutes' WAREHOUSE = COMPUTE_WH REFRESH_MODE = INCREMENTAL" in res_dynamic["ddl"]

    # 3. Branch A Interactive Table: requires CLUSTER BY (excluding VECTOR), TARGET_LAG, WAREHOUSE
    res_interactive = bridge.create_or_register_memory(
        {
            "name": "EPISODIC_INTERACTIVE_MEM",
            "agent_memory_type": "episodic_memory",
            "storage_family": "snowflake_sql",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "table_type": "INTERACTIVE",
            "cluster_by": "user_id, topic, embedding",
            "target_lag": "1 minute",
            "interactive_warehouse": "INTERACTIVE_WH",
            "enable_embedding": True,
            "embedding_provider": "snowflake_cortex",
            "embedding_model": "snowflake-arctic-embed-m-v1.5",
            "dry_run": True,
        }
    )
    assert res_interactive["ok"] is True
    assert res_interactive["memory"]["cluster_by"] == "user_id, topic"
    assert "CLUSTER BY (user_id, topic) TARGET_LAG = '1 minute' WAREHOUSE = INTERACTIVE_WH" in res_interactive["ddl"]

    # 4. Branch B Snowflake Postgres (:5432) for #1 Graph Checkpointer (PostgresSaver) & #5 Episodic (pgvector + GIN)
    res_pg_ckpt = bridge.create_or_register_memory(
        {
            "name": "PG_GRAPH_CHECKPOINTER",
            "agent_memory_type": "graph_state_checkpointer",
            "storage_family": "snowflake_postgres",
            "postgres_instance": "LANGFLOW_PG_INSTANCE",
            "pg_database": "postgres",
            "pg_schema": "public",
            "pg_table": "langgraph_checkpoints",
            "pg_table_mode": "PostgresSaver",
            "enable_embedding": True,  # Must be locked OFF for #1
            "dry_run": True,
        }
    )
    assert res_pg_ckpt["ok"] is True
    mem_pg1 = res_pg_ckpt["memory"]
    assert mem_pg1["storage_family"] == "snowflake_postgres"
    assert mem_pg1["database"] is None
    assert mem_pg1["schema"] is None
    assert mem_pg1["enable_embedding"] is False
    assert mem_pg1["table_fqn"] == "LANGFLOW_PG_INSTANCE:5432/postgres.public.langgraph_checkpoints"
    assert "PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)" in res_pg_ckpt["ddl"]

    res_pg_vec = bridge.create_or_register_memory(
        {
            "name": "PG_EPISODIC_PGVECTOR",
            "agent_memory_type": "episodic_memory",
            "storage_family": "snowflake_postgres",
            "postgres_instance": "LANGFLOW_PG_INSTANCE",
            "pg_database": "postgres",
            "pg_schema": "public",
            "pg_table": "episodic_vectors",
            "pg_table_mode": "JSONB + pgvector",
            "gin_index_jsonb": True,
            "enable_embedding": True,
            "embedding_provider": "spcs_ollama",
            "embedding_model": "nomic-embed-text",
            "dry_run": True,
        }
    )
    assert res_pg_vec["ok"] is True
    assert "CREATE EXTENSION IF NOT EXISTS vector;" in res_pg_vec["ddl"]
    assert "embedding vector(768)" in res_pg_vec["ddl"]
    assert "USING GIN (metadata)" in res_pg_vec["ddl"]

    # 5. Verify 3-step Wizard DOM structure in snowflake-ui-integration.js
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    for wizard_id in (
        "sflf-mem-wizard-step-1",
        "sflf-mem-wizard-step-2",
        "sflf-mem-wizard-step-3",
        "sflf-mem-wizard-back-btn",
        "sflf-mem-wizard-next-btn",
        "sflf-light-mem-cancel-btn",
        "sflf-light-mem-submit-btn",
        "sflf-mem-branch-a-sql-box",
        "sflf-mem-branch-b-pg-box",
    ):
        assert wizard_id in ui_js, f"Missing wizard element #{wizard_id} in snowflake-ui-integration.js"


def test_w2_r4_04_four_snowflake_embedding_sources_and_llm_preprocessing_no_ingest() -> None:
    """W2-R4-04: 4 Snowflake embedding sources, locked-off embedding for #1/#3, and NO_INGEST kill phrase."""
    source_ids = [s["id"] for s in SNOWFLAKE_EMBEDDING_SOURCES]
    assert source_ids == [
        "snowflake_cortex",
        "spcs_model_registry",
        "spcs_ollama",
        "spcs_llm_proxy",
    ]
    engine_ids = [e["id"] for e in LLM_PREPROCESSING_ENGINES]
    assert engine_ids == [
        "cortex_sql",
        "proxy_chat",
        "proxy_responses",
        "proxy_messages",
        "spcs_ollama",
        "spcs_model_registry",
    ]

    bridge = LiveSnowflakeBridge()
    bridge.switch_role("ACCOUNTADMIN")

    # Verify locked embedding rejection when reject_locked_embedding=True on #1 and #3
    for locked_type in ("graph_state_checkpointer", "summary_memory"):
        res_locked = bridge.create_or_register_memory(
            {
                "name": f"LOCKED_{locked_type.upper()}",
                "agent_memory_type": locked_type,
                "enable_embedding": True,
                "reject_locked_embedding": True,
                "dry_run": True,
            }
        )
        assert res_locked["ok"] is False
        assert res_locked["status_code"] == 422
        assert res_locked["error_code"] == "UNSUPPORTED_CAPABILITY"

    # Verify AI_MULTI_EMBED 512d (`twelvelabs-marengo-embed-3-0`) and SPCS Model Registry custom dim
    res_marengo = bridge.create_or_register_memory(
        {
            "name": "MARENGO_MULTIMODAL_MEM",
            "agent_memory_type": "episodic_memory",
            "enable_embedding": True,
            "embedding_provider": "snowflake_cortex",
            "embedding_model": "twelvelabs-marengo-embed-3-0",
            "dry_run": True,
        }
    )
    assert res_marengo["ok"] is True
    assert res_marengo["memory"]["dimension"] == 512
    assert "VECTOR(FLOAT, 512)" in res_marengo["ddl"]

    res_spcs_reg = bridge.create_or_register_memory(
        {
            "name": "SPCS_REGISTRY_CUSTOM_MEM",
            "agent_memory_type": "episodic_memory",
            "enable_embedding": True,
            "embedding_provider": "spcs_model_registry",
            "embedding_model": "SNOWFLAKE_LANGFLOW.CORE.CUSTOM_FIN_EMBED_SVC",
            "custom_dimension": 1536,
            "dry_run": True,
        }
    )
    assert res_spcs_reg["ok"] is True
    assert res_spcs_reg["memory"]["dimension"] == 1536
    assert "VECTOR(FLOAT, 1536)" in res_spcs_reg["ddl"]

    # Verify SnowflakeKnowledgeComponent.ingest skips storage when preprocessor returns NO_INGEST
    engine = InMemoryVectorStoreEngine(
        active_role="DATA_ENGINEER_ROLE",
        snowflake_privileges={
            "DATA_ENGINEER_ROLE": {
                ("CREATE TABLE", "*"),
                ("CREATE HYBRID TABLE", "*"),
                ("SELECT", "*"),
                ("INSERT", "*"),
                ("USAGE", "*"),
            }
        },
        preprocessor_fn=lambda _txt, _cfg: "NO_INGEST",
    )
    dialog = CreateMemoryDialogService(engine)
    dialog.create_memory(
        name="USER_PREF_KILL_PHRASE_TEST",
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        table_name="USER_ENTITY_PROFILE",
        vector_database="HYBRID_RELATIONAL",
        llm_preprocessing=LLMPreprocessingConfig(
            enabled=True,
            provider="proxy_messages",
            model_id="claude-sonnet-5-5",
            kill_phrase="NO_INGEST",
        ),
        create_physical_resource=True,
    )
    kb_comp = SnowflakeKnowledgeComponent(engine, "USER_PREF_KILL_PHRASE_TEST")
    skip_result = kb_comp.ingest(
        [{"id": "turn-1", "content": "Ciao grazie"}],
        user_id="FIN_USER",
        project_id="proj_fin",
        session_id="s1",
    )
    assert skip_result["ingested_count"] == 0
    assert skip_result["skipped_count"] == 1
    assert skip_result["kill_phrase"] == "NO_INGEST"


def test_w2_r4_05_memory_detail_view_and_rest_routes_and_rbac() -> None:
    """W2-R4-05: Memory Detail View endpoints (`GET/POST reload/DELETE /api/v1/snowflake/memories/{id}`) and RBAC."""
    client = _build_fastapi_test_client()

    # Create memory under ACCOUNTADMIN
    client.post("/api/v1/snowflake/role", json={"target_role": "ACCOUNTADMIN"})
    create_resp = client.post(
        "/api/v1/snowflake/memories",
        json={
            "name": "DETAIL_VIEW_PROFILE_MEM",
            "agent_memory_type": "user_profile_entity_memory",
            "storage_family": "snowflake_sql",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "table_type": "HYBRID",
            "semi_structured_type": "VARIANT",
            "filter_by_session": False,
            "enable_embedding": False,
            "llm_preprocessing": {
                "enabled": True,
                "engine": "proxy_messages",
                "model": "claude-sonnet-5-5",
                "kill_phrase": "NO_INGEST",
            },
            "dry_run": True,
        },
    )
    assert create_resp.status_code == 201
    created_payload = create_resp.json()
    assert created_payload["ok"] is True

    # GET /api/v1/snowflake/memories/{name}
    detail_resp = client.get("/api/v1/snowflake/memories/DETAIL_VIEW_PROFILE_MEM")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["ok"] is True
    assert detail["configuration_summary"]["name"] == "DETAIL_VIEW_PROFILE_MEM"
    assert detail["configuration_summary"]["kill_phrase"] == "NO_INGEST"
    assert (
        "Claude" in detail["configuration_summary"]["llm_preprocessing"]
        or "claude-sonnet-5-5" in detail["configuration_summary"]["llm_preprocessing"]
    )
    assert len(detail["stored_records"]) >= 1

    # POST /api/v1/snowflake/memories/{name}/reload with trivial greeting -> Skipped via NO_INGEST
    reload_skip_resp = client.post(
        "/api/v1/snowflake/memories/DETAIL_VIEW_PROFILE_MEM/reload",
        json={
            "auto_capture": True,
            "ingest_turn": {"session_id": "sess_live_99", "text": "ok grazie"},
        },
    )
    assert reload_skip_resp.status_code == 200
    reload_skip = reload_skip_resp.json()
    assert reload_skip["reloaded"] is True
    assert reload_skip["stored_records"][0]["status"] == "Skipped"
    assert reload_skip["stored_records"][0]["outcome_score"] == "NO_INGEST"

    # POST /api/v1/snowflake/memories/{name}/reload with durable preference -> Ingested
    reload_ingest_resp = client.post(
        "/api/v1/snowflake/memories/DETAIL_VIEW_PROFILE_MEM/reload",
        json={
            "ingest_turn": {
                "session_id": "sess_live_99",
                "text": "Imposta valuta EUR e lingua italiano per Q4",
                "topic": "currency=EUR, lang=it",
            },
            "session_filter": "sess_live_99",
        },
    )
    assert reload_ingest_resp.status_code == 200
    reload_ingest = reload_ingest_resp.json()
    assert reload_ingest["stored_records"][0]["status"] == "Ingested"
    assert all(r["session_id"] == "sess_live_99" for r in reload_ingest["stored_records"])

    # Switch to PUBLIC role -> physical creation and DELETE must fail closed with 403 AUTHZ_ROLE_DENIED
    client.post("/api/v1/snowflake/role", json={"target_role": "PUBLIC"})
    denied_create = client.post(
        "/api/v1/snowflake/memories",
        json={
            "name": "PUBLIC_FORBIDDEN_MEM",
            "agent_memory_type": "buffer_window_memory",
            "create_new_physical_store": True,
        },
    )
    assert denied_create.status_code == 403
    assert denied_create.json()["error_code"] == "AUTHZ_ROLE_DENIED"

    denied_delete = client.delete("/api/v1/snowflake/memories/DETAIL_VIEW_PROFILE_MEM")
    assert denied_delete.status_code == 403
    assert denied_delete.json()["error_code"] == "AUTHZ_ROLE_DENIED"

    # Switch back to ACCOUNTADMIN -> DELETE succeeds
    client.post("/api/v1/snowflake/role", json={"target_role": "ACCOUNTADMIN"})
    ok_delete = client.delete("/api/v1/snowflake/memories/DETAIL_VIEW_PROFILE_MEM")
    assert ok_delete.status_code == 200
    assert ok_delete.json()["deleted"] == "DETAIL_VIEW_PROFILE_MEM"

    # Verify frontend Detail View markers in snowflake-ui-integration.js
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    for detail_marker in (
        "function renderSnowflakeMemoryDetailView(",
        '"snowflake-memory-detail-view"',
        'data-testid="snowflake-memory-reload-btn"',
        'data-testid="snowflake-memory-delete-btn"',
        'data-testid="snowflake-memory-config-summary"',
        'data-testid="snowflake-memory-records-table"',
    ):
        assert detail_marker in ui_js, f"Missing Detail View marker {detail_marker} in snowflake-ui-integration.js"


def test_w2_r4_06_and_06b_dynamic_memory_base_message_history_and_data_sources_purity() -> None:
    """W2-R4-06 & W2-R4-06B: Dynamic `Memory Base` (#1..#5), `Message History` (#2), and `Data Sources` purity."""
    manifest = get_bundle_manifest()
    comp_ids = {c["component_id"] for c in manifest["components"]}
    assert "snowflake_memory_base" in comp_ids
    assert "snowflake_message_history" in comp_ids

    assert SnowflakeMemoryBaseNativeComponent.display_name == "Memory Base"
    assert SnowflakeMemoryBaseNativeComponent.name == "SnowflakeMemoryBase"
    assert SnowflakeMessageHistoryNativeComponent.display_name in ("Message History", "Snowflake Message History")
    assert SnowflakeMessageHistoryNativeComponent.name == "SnowflakeMessageHistory"

    # Verify SnowflakeMemoryBaseComponent & SnowflakeMemoryBaseNativeComponent adapt to all 5 canonical types
    expected_ports = {
        "graph_state_checkpointer": "checkpoint_state",
        "buffer_window_memory": "messages",
        "summary_memory": "running_summary",
        "user_profile_entity_memory": "profile_context",
        "episodic_memory": "few_shot_context",
    }
    for c_type, expected_port in expected_ports.items():
        base_comp = SnowflakeMemoryBaseComponent(
            memory_name=f"UNREGISTERED_{c_type.upper()}",
            canonical_memory_type=c_type,
        )
        spec = base_comp.get_dynamic_port_and_field_spec()
        assert spec["canonical_memory_type"] == c_type
        assert spec["primary_output_port"] == expected_port
        for req_field in DYNAMIC_MEMORY_BASE_TYPE_SPECS[c_type]["visible_fields"]:
            assert req_field in spec["visible_fields"]

    # Verify SnowflakeMessageHistoryNativeComponent supports #2 Buffer / Window Memory on Postgres and SQL
    msg_hist_pg = SnowflakeMessageHistoryNativeComponent(
        memory_base="MEM_2_CHAT_WINDOW",
        storage_family="snowflake_postgres",
        postgres_instance="LANGFLOW_PG_INSTANCE",
        pg_table="public.chat_message_history",
        session_id="sess_window_01",
        window_k=8,
    )
    pg_payload = json.loads(msg_hist_pg.retrieve_chat_history().text)
    assert pg_payload["canonical_memory_type"] == "buffer_window_memory"
    assert pg_payload["storage_family"] == "snowflake_postgres"
    assert pg_payload["window_k"] == 8
    assert pg_payload["filter_by_session"] is True
    assert "LIMIT 8" in pg_payload["recall_sql"]

    # Verify W2-R4-06B: Data Sources strictly excludes technical memory tables
    mixed_tables = [
        "FIN_GL_TRANSACTIONS",
        "FIN_Q3_Q4_MARGIN_LEDGER",
        "AGENT_CHECKPOINTS",
        "CHAT_WINDOW_HISTORY",
        "CHAT_SESSION_SUMMARY",
        "USER_ENTITY_PROFILE",
        "AGENT_EPISODIC_MEMORY",
        "LANGGRAPH_CHECKPOINTS",
        "CHAT_MESSAGE_HISTORY",
        "MEM_EPISODIC_CUSTOM_STORE",
        "LANGFLOW_MEM_VECTORS",
    ]
    filtered = filter_business_data_source_objects(mixed_tables, category="tables")
    assert filtered == ["FIN_GL_TRANSACTIONS", "FIN_Q3_Q4_MARGIN_LEDGER"]

    ds_comp = SnowflakeDataSourcesComponent(active_role="ANALYST_ROLE")
    available_objs = [
        {"object_fqn": f"SNOWFLAKE_LANGFLOW.CORE.{tbl}", "name": tbl, "allowed_roles": ["ANALYST_ROLE"]}
        for tbl in mixed_tables
    ]
    for tech_tbl in (
        "AGENT_CHECKPOINTS",
        "CHAT_WINDOW_HISTORY",
        "CHAT_SESSION_SUMMARY",
        "USER_ENTITY_PROFILE",
        "AGENT_EPISODIC_MEMORY",
    ):
        assert is_technical_memory_table(tech_tbl) is True
    browsed = ds_comp.browse_catalog(available_objects=available_objs, exclude_memory_tables=True)
    assert [item["name"] for item in browsed] == ["FIN_GL_TRANSACTIONS", "FIN_Q3_Q4_MARGIN_LEDGER"]


def test_w2_r4_07_end_to_end_full_memory_enterprise_agent_showcase_flow() -> None:
    """W2-R4-07: End-to-End `Snowflake_Full_Memory_Enterprise_Agent` starter project & multi-turn simulation."""
    starter_json_path = REPO_ROOT / "starter_projects" / "Snowflake_Full_Memory_Enterprise_Agent.json"
    assert starter_json_path.exists(), "Missing starter_projects/Snowflake_Full_Memory_Enterprise_Agent.json"
    starter_flow = json.loads(starter_json_path.read_text(encoding="utf-8"))

    assert starter_flow["id"] == SHOWCASE_FLOW_ID
    assert starter_flow["name"] == SHOWCASE_FLOW_NAME
    node_ids = {n["id"] for n in starter_flow["data"]["nodes"]}
    assert {
        "ChatInput-showcase01",
        "SnowflakeMemoryBase-mem1",
        "SnowflakeMessageHistory-mem2",
        "SnowflakeMemoryBase-mem3",
        "SnowflakeMemoryBase-mem4",
        "SnowflakeMemoryBase-mem5",
        "SnowflakeCortexAnalyst-domain7a",
        "SnowflakeCortexSearch-domain7b",
        "SnowflakeStandardChat-agent8",
        "ChatOutput-showcase01",
    }.issubset(node_ids)
    assert len(SHOWCASE_MEMORIES_SPECS) == 5

    # Run multi-turn simulation with #5 Episodic Embedding ON (AI_EMBED 1024d) and OFF (SQL / AI_FILTER)
    LIVE_BRIDGE.switch_role("ACCOUNTADMIN")

    sim_vec_on = run_full_memory_showcase_simulation(LIVE_BRIDGE, dry_run=True, enable_episodic_embedding=True)
    assert sim_vec_on["ok"] is True
    assert sim_vec_on["bootstrap"]["memories_registered_count"] == 5
    assert sim_vec_on["bootstrap"]["technical_memory_tables_excluded_from_data_sources"] is True
    assert sim_vec_on["turns"][0]["status"] == "Skipped"
    assert sim_vec_on["turns"][0]["preprocessor_output"] == "NO_INGEST"
    assert sim_vec_on["turns"][1]["status"] == "Processed"
    assert sim_vec_on["turns"][1]["merged_into_profile"] is True
    assert sim_vec_on["turns"][2]["status"] == "Ingested"
    assert sim_vec_on["turns"][2]["response_currency"] == "EUR (€)"

    ver_on = sim_vec_on["memory_components_verification"]
    assert ver_on["mem_1_checkpointer"]["storage_family"] == "snowflake_postgres"
    assert ver_on["mem_2_chat_window"]["storage_family"] == "snowflake_postgres"
    assert ver_on["mem_3_session_summary"]["filter_by_session"] is True
    assert ver_on["mem_4_user_entity_profile"]["filter_by_session"] is False
    assert ver_on["mem_5_episodic_fewshot"]["tool_mode"] is True
    assert ver_on["mem_5_episodic_fewshot"]["enable_embedding"] is True
    assert "VECTOR_COSINE_SIMILARITY" in ver_on["mem_5_episodic_fewshot"]["tool_invocation"]["recall_sql"]

    sim_vec_off = run_full_memory_showcase_simulation(LIVE_BRIDGE, dry_run=True, enable_episodic_embedding=False)
    ver_off = sim_vec_off["memory_components_verification"]
    assert ver_off["mem_5_episodic_fewshot"]["enable_embedding"] is False
    assert "ORDER BY outcome_score DESC" in ver_off["mem_5_episodic_fewshot"]["tool_invocation"]["recall_sql"]

    # Also verify FastAPI showcase route & combined entrypoint
    combined = bootstrap_and_run_full_memory_showcase(LIVE_BRIDGE, dry_run=True, execute_simulation=True)
    assert combined["ok"] is True
    assert len(combined["memories_created"]) == 5

    client = _build_fastapi_test_client()
    api_showcase = client.get("/api/v1/snowflake/memories/showcase/full-memory-enterprise-agent")
    assert api_showcase.status_code == 200
    assert len(api_showcase.json()["memories"]) == 5

    post_showcase = client.post(
        "/api/v1/snowflake/memories/showcase/full-memory-enterprise-agent",
        json={"dry_run": True, "execute_simulation": True, "enable_episodic_embedding": True},
    )
    assert post_showcase.status_code == 200
    assert post_showcase.json()["ok"] is True
    assert len(post_showcase.json()["memories_created"]) == 5


def test_w2_r4_verifier_route_ordering_recall_and_native_build_config_and_router() -> None:
    """Verifier test: route ordering for `/memories/recall`, native `update_build_config`, and `SnowflakeAPIRouter.post_memories`."""
    client = _build_fastapi_test_client()
    client.post("/api/v1/snowflake/role", json={"target_role": "ACCOUNTADMIN"})

    # 1. Verify POST /api/v1/snowflake/memories/recall is routed before {memory_id_or_name}
    recall_resp = client.post(
        "/api/v1/snowflake/memories/recall",
        json={
            "memory_name": "MEM_5_EPISODIC_FEWSHOT",
            "canonical_memory_type": "episodic_memory",
            "query": "Q3 vs Q4 net margin variance reconciliation",
            "outcome_score_threshold": 0.9,
            "few_shot_k": 3,
        },
    )
    assert recall_resp.status_code == 200
    recall_payload = recall_resp.json()
    assert recall_payload["ok"] is True
    assert recall_payload["canonical_memory_type"] == "episodic_memory"

    # 2. Verify SnowflakeMemoryBaseNativeComponent.update_build_config toggles type-specific fields across #1..#5
    native_comp = SnowflakeMemoryBaseNativeComponent()
    sample_build_cfg: dict[str, Any] = {
        f.name: {"show": getattr(f, "show", True), "value": getattr(f, "value", None)}
        for f in SnowflakeMemoryBaseNativeComponent.inputs
    }
    updated_cfg_1 = native_comp.update_build_config(
        dict(sample_build_cfg), "graph_state_checkpointer", "canonical_memory_type"
    )
    assert updated_cfg_1["thread_id"]["show"] is True
    assert updated_cfg_1["checkpoint_ns"]["show"] is True
    assert updated_cfg_1["outcome_score_threshold"]["show"] is False

    updated_cfg_5 = native_comp.update_build_config(
        dict(sample_build_cfg), "episodic_memory", "canonical_memory_type"
    )
    assert updated_cfg_5["topic"]["show"] is True
    assert updated_cfg_5["outcome_score_threshold"]["show"] is True
    assert updated_cfg_5["few_shot_k"]["show"] is True
    assert updated_cfg_5["thread_id"]["show"] is False

    # 3. Verify SnowflakeAPIRouter.create_or_bind_memory supports Branch B Snowflake Postgres and NO_INGEST kill phrase
    router = SnowflakeAPIRouter(initial_role="DATA_ENGINEER_ROLE")
    cat_res = router.list_memories_catalog()
    assert cat_res.status_code == 200
    assert len(cat_res.body["agent_memory_types"]) == 5
    assert len(cat_res.body["coming_soon_memory_types"]) == 1
    assert len(cat_res.body["snowflake_embedding_sources"]) == 4
    assert build_snowflake_full_memory_enterprise_agent_flow()["id"] == SHOWCASE_FLOW_ID

    pg_mem_res = router.create_or_bind_memory(
        name="VERIFIER_PG_WINDOW",
        agent_memory_type="buffer_window_memory",
        vector_database="Snowflake Postgres",
        storage_family="snowflake_postgres",
        postgres_instance="LANGFLOW_PG_INSTANCE",
        llm_preprocessing_enabled=True,
        llm_preprocessing_provider="proxy_messages",
        kill_phrase="NO_INGEST",
    )
    assert pg_mem_res.status_code == 201
    assert pg_mem_res.body["memory"]["storage_family"] == "snowflake_postgres"
    assert pg_mem_res.body["memory"]["postgres_instance"] == "LANGFLOW_PG_INSTANCE"
    assert pg_mem_res.body["memory"]["kill_phrase"] == "NO_INGEST"


def test_priority_a_and_b_preserved_concept_zero_diff_and_public_repo_sanitization() -> None:
    """Priority A & B: Verify 0 git diff on preserved_provisioning_and_memories_concept.js and public repo sanitization."""
    diff_proc = subprocess.run(
        ["git", "diff", "frontend_patches/preserved_provisioning_and_memories_concept.js"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert diff_proc.returncode == 0
    assert diff_proc.stdout.strip() == "", (
        "Priority A violation: frontend_patches/preserved_provisioning_and_memories_concept.js must have 0 diff!"
    )

    san_proc = subprocess.run(
        [sys.executable, "scripts/verify_public_repo_sanitization.py"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert san_proc.returncode == 0, f"Public repo sanitization failed:\n{san_proc.stdout}\n{san_proc.stderr}"
    assert "[SANITIZATION PASSED]" in san_proc.stdout
