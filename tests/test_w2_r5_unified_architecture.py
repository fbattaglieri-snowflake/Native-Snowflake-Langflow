"""Comprehensive Wave 2 Round 5 (W2-R5-01..W2-R5-07) Verification Suite."""

from __future__ import annotations

import asyncio
import inspect
import re
import sys
import types
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlencode, urlsplit

from lfx_snowflake.fastapi_live_bridge import (
    LIVE_BRIDGE,
    OFFICIAL_SNOWFLAKE_CORTEX_AI_EMBED_MODELS,
    SNOWFLAKE_VECTOR_STORE_BACKENDS,
    LiveSnowflakeBridge,
    SnowflakeVectorStoreBackend,
    register_snowflake_fastapi_routes,
    register_snowflake_vector_store_backends,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_analyst import (
    SnowflakeCortexAnalystComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_search import (
    SnowflakeCortexSearchComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_canonical_fk import (
    CANONICAL_FILES_AND_KNOWLEDGE_NAMES,
    SnowflakeCanonicalMemoryBaseComponent,
    SnowflakeFileSystemToolComponent,
    SnowflakeKnowledgeComponent,
    SnowflakeReadFileComponent,
    SnowflakeWriteFileComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_memory_base import (
    SnowflakeMemoryBaseNativeComponent,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_message_history import (
    SnowflakeMessageHistoryNativeComponent,
)
from lfx_snowflake.native_category_components.processing.split_text import (
    SplitTextComponent,
)
from lfx_snowflake.providers.embeddings import (
    OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS,
    OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS,
    SnowflakeCortexEmbeddings,
    get_snowflake_cortex_embedding_catalog,
    register_snowflake_cortex_embedding_provider,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_client() -> Any:
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
        def __init__(
            self,
            method: str,
            url: str,
            json_body: Any = None,
            params: dict[str, str] | None = None,
        ) -> None:
            if params:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{urlencode(params)}"
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

        def _dispatch(
            self,
            method: str,
            url: str,
            json_body: Any = None,
            params: dict[str, str] | None = None,
        ) -> _StubJSONResponse:
            req = _StubRequest(method, url, json_body=json_body, params=params)
            req_path = req.url.path
            for router in self.app.routers:
                for r_method, r_pattern, handler in router.routes:
                    if r_method != method.upper():
                        continue
                    param_names = re.findall(r"\{([a-zA-Z0-9_]+)(?::path)?\}", r_pattern)
                    regex_pat = (
                        "^"
                        + re.sub(
                            r"\{[a-zA-Z0-9_]+:path\}",
                            r"(.+)",
                            re.sub(r"\{[a-zA-Z0-9_]+\}", r"([^/]+)", r_pattern),
                        )
                        + "$"
                    )
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
                        elif p_name in req.query_params:
                            call_kwargs[p_name] = req.query_params[p_name]
                    res = asyncio.run(handler(**call_kwargs))
                    if hasattr(res, "json") and hasattr(res, "status_code"):
                        return res
                    return _StubJSONResponse(status_code=200, content=res)
            return _StubJSONResponse(
                status_code=404,
                content={"ok": False, "error": f"Route not found: {method} {req_path}"},
            )

        def get(self, url: str, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("GET", url, params=params)

        def post(self, url: str, json: Any = None, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("POST", url, json_body=json, params=params)

        def patch(self, url: str, json: Any = None, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("PATCH", url, json_body=json, params=params)

        def delete(self, url: str, params: dict[str, str] | None = None) -> _StubJSONResponse:
            return self._dispatch("DELETE", url, params=params)

        def request(
            self,
            method: str,
            url: str,
            json: Any = None,
            params: dict[str, str] | None = None,
        ) -> _StubJSONResponse:
            return self._dispatch(method, url, json_body=json, params=params)

    fastapi_mod = types.ModuleType("fastapi")
    fastapi_mod.APIRouter = _StubAPIRouter  # type: ignore[attr-defined]
    fastapi_mod.Request = _StubRequest  # type: ignore[attr-defined]
    fastapi_resp_mod = types.ModuleType("fastapi.responses")
    fastapi_resp_mod.JSONResponse = _StubJSONResponse  # type: ignore[attr-defined]
    sys.modules["fastapi"] = fastapi_mod
    sys.modules["fastapi.responses"] = fastapi_resp_mod

    stub_app = _StubApp()
    register_snowflake_fastapi_routes(stub_app)
    return _StubClient(stub_app)


def test_w2_r5_01_context_driven_memory_visibility_and_dropdown_sync() -> None:
    """Verify W2-R5-01: Context-driven memory visibility in Memories view and Memory Base / Message History dropdowns."""
    bridge = LiveSnowflakeBridge()
    flow_id = "flow-w2-r5-01-visibility"

    bridge.memories_registry = [
        {
            "name": "MEM_1_CONVERSATION_BUFFER",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "table_fqn": "SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_MEMORY_CONVERSATION_BUFFER",
            "storage_family": "snowflake_sql",
            "agent_memory_type": "conversation_buffer",
        },
        {
            "name": "MEM_2_CHAT_WINDOW",
            "database": "LANGFLOW_PG_INSTANCE",
            "schema": "public",
            "postgres_instance": "LANGFLOW_PG_INSTANCE",
            "table_fqn": "LANGFLOW_PG_INSTANCE:5432/postgres.public.chat_message_history",
            "storage_family": "snowflake_postgres",
            "agent_memory_type": "buffer_window_memory",
        },
        {
            "name": "MEM_4_VECTOR_KNOWLEDGE",
            "database": "ANALYTICS_DB",
            "schema": "MART",
            "table_fqn": "ANALYTICS_DB.MART.LANGFLOW_MEMORY_VECTOR_KNOWLEDGE",
            "storage_family": "snowflake_sql",
            "agent_memory_type": "semantic_vector",
        },
    ]

    # Restrict flow context to SNOWFLAKE_LANGFLOW.CORE with postgres_instances=[]
    bridge.update_selected_context(
        {
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "databases": ["SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
            "full_databases": [],
            "full_schemas": [],
            "selected_objects": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["LANGFLOW_MEMORY_CONVERSATION_BUFFER"],
                }
            },
            "postgres_instances": [],
        },
        flow_id=flow_id,
    )

    visible_mems = bridge.get_memories_for_flow(flow_id=flow_id)
    visible_names = [m["name"] for m in visible_mems]
    assert "MEM_1_CONVERSATION_BUFFER" in visible_names
    assert "MEM_2_CHAT_WINDOW" not in visible_names
    assert "MEM_4_VECTOR_KNOWLEDGE" not in visible_names

    # Dropdown options must respect flow_id context filtering
    dropdown_opts = bridge.get_memory_catalog_options(include_none=False, flow_id=flow_id)
    assert any("MEM_1_CONVERSATION_BUFFER" in opt for opt in dropdown_opts)
    assert not any("MEM_2_CHAT_WINDOW" in opt for opt in dropdown_opts)

    # Enable Postgres instance in context -> MEM_2_CHAT_WINDOW becomes visible immediately
    bridge.update_selected_context(
        {
            "postgres_instances": ["LANGFLOW_PG_INSTANCE"],
        },
        flow_id=flow_id,
    )
    visible_after_pg = [m["name"] for m in bridge.get_memories_for_flow(flow_id=flow_id)]
    assert "MEM_1_CONVERSATION_BUFFER" in visible_after_pg
    assert "MEM_2_CHAT_WINDOW" in visible_after_pg

    # Verify Memory Base and Message History update_build_config filter options by flow_id
    prev_reg = list(LIVE_BRIDGE.memories_registry)
    try:
        LIVE_BRIDGE.memories_registry = list(bridge.memories_registry)
        LIVE_BRIDGE.contexts_by_flow_id[flow_id] = bridge.contexts_by_flow_id[flow_id]
        mb_comp = SnowflakeMemoryBaseNativeComponent()
        mb_cfg = {"_frontend_node_flow_id": flow_id, "memory_name": {"options": [], "value": ""}}
        mb_comp.update_build_config(mb_cfg, None, "memory_name")
        assert any("MEM_1_CONVERSATION_BUFFER" in opt for opt in mb_cfg["memory_name"]["options"])
        assert any("MEM_2_CHAT_WINDOW" in opt for opt in mb_cfg["memory_name"]["options"])
        assert not any("MEM_4_VECTOR_KNOWLEDGE" in opt for opt in mb_cfg["memory_name"]["options"])
    finally:
        LIVE_BRIDGE.memories_registry = prev_reg


def test_w2_r5_02_unified_memories_native_create_and_snowflake_vector_store_backend() -> None:
    """Verify W2-R5-02: Native POST /api/v1/memories, + Snowflake wizard, and SnowflakeVectorStoreBackend."""
    reg_res = register_snowflake_vector_store_backends()
    assert reg_res["ok"] is True
    for expected_backend in (
        "snowflake_table",
        "snowflake_hybrid",
        "snowflake_dynamic",
        "snowflake_interactive",
        "snowflake_iceberg",
        "snowflake_postgres",
    ):
        assert expected_backend in reg_res["registered_backends"]
        assert expected_backend in SNOWFLAKE_VECTOR_STORE_BACKENDS

    # Test SnowflakeVectorStoreBackend with AI_EMBED (768d and 1024d)
    vec_backend = SnowflakeVectorStoreBackend(
        "kb_fin_docs",
        backend_type="snowflake_hybrid",
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        table_name="KB_FIN_DOCS_VEC",
        embedding_model="snowflake-arctic-embed-l-v2.0",
        enable_embedding=True,
    )
    assert vec_backend.dimension == 1024
    ddl = vec_backend.build_ddl()
    assert "CREATE HYBRID TABLE IF NOT EXISTS SNOWFLAKE_LANGFLOW.CORE.KB_FIN_DOCS_VEC" in ddl
    assert "VECTOR(FLOAT, 1024)" in ddl

    ins_res = vec_backend.add_texts(
        ["Snowflake Cortex AI_EMBED supports 768d and 1024d vectors."],
        metadatas=[{"topic": "embeddings"}],
        session_id="sess-vec-01",
    )
    assert ins_res["ok"] is True
    assert ins_res["ingested_count"] == 1
    assert "SNOWFLAKE.CORTEX.AI_EMBED('snowflake-arctic-embed-l-v2.0'" in ins_res["sql_statements"][0]

    search_res = vec_backend.search("AI_EMBED vectors", top_k=3, session_id="sess-vec-01")
    search_sql = search_res["sql"]
    select_part, where_part = search_sql.split(" WHERE ", 1)
    assert "VECTOR_COSINE_SIMILARITY(EMBEDDING, SNOWFLAKE.CORTEX.AI_EMBED('snowflake-arctic-embed-l-v2.0'" in select_part
    assert "VECTOR_COSINE_SIMILARITY" not in where_part
    assert "SESSION_ID = 'sess-vec-01'" in where_part
    assert len(search_res["rows"]) >= 1

    # Test SnowflakeVectorStoreBackend in No Embedding direct SQL mode
    no_emb_backend = SnowflakeVectorStoreBackend(
        "kb_plain_sql",
        backend_type="snowflake_table",
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        table_name="KB_PLAIN_SQL",
        enable_embedding=False,
    )
    assert no_emb_backend.enable_embedding is False
    no_emb_ddl = no_emb_backend.build_ddl()
    assert "VECTOR(" not in no_emb_ddl
    no_emb_search = no_emb_backend.build_search_sql("reconciliation", session_id="sess-plain-01")
    assert "VECTOR_COSINE_SIMILARITY" not in no_emb_search
    assert "TEXT_CHUNK ILIKE '%reconciliation%'" in no_emb_search

    # Test native POST /api/v1/memories endpoint
    client = _make_client()
    resp = client.post(
        "/api/v1/memories",
        json={
            "name": "W2_R5_NATIVE_CREATED_MEM",
            "embedding_provider": "Snowflake Cortex",
            "embedding_model": "snowflake-arctic-embed-m-v1.5",
            "backend": "snowflake_table",
            "chunk_size": 1000,
            "chunk_overlap": 200,
        },
    )
    assert resp.status_code in (200, 201)
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["name"] == "W2_R5_NATIVE_CREATED_MEM"
    assert "snowflake-arctic-embed-m-v1.5" in payload["embedding_model"]
    assert payload.get("size") == 768 or payload.get("dimension") == 768 or payload.get("memory", {}).get("dimension") == 768

    # Verify it appears in GET /api/v1/memories
    list_resp = client.get("/api/v1/memories")
    assert list_resp.status_code == 200
    items = list_resp.json()["items"]
    assert any(it["name"] == "W2_R5_NATIVE_CREATED_MEM" for it in items)


def test_w2_r5_03_multi_memory_wiring_on_message_history_and_agent_tools() -> None:
    """Verify W2-R5-03: Multi-Memory fan-in/fan-out on Message History (is_list=True) and Tool Mode for Agent."""
    mh_inputs = {inp.name: inp for inp in SnowflakeMessageHistoryNativeComponent.inputs}
    assert "memory" in mh_inputs
    assert getattr(mh_inputs["memory"], "is_list", False) is True

    mb_conv = SnowflakeMemoryBaseNativeComponent(
        memory_name="MEM_1_CONVERSATION_BUFFER",
        session_id="multi-mem-sess-001",
        user_id="analyst_multi",
    )
    mb_win = SnowflakeMemoryBaseNativeComponent(
        memory_name="MEM_2_CHAT_WINDOW",
        session_id="multi-mem-sess-001",
        user_id="analyst_multi",
    )
    adapter_conv = mb_conv.build_memory()
    adapter_win = mb_win.build_memory()

    # 1. Store mode with multiple connected External Memories (fan-out write)
    mh_store = SnowflakeMessageHistoryNativeComponent(
        mode="Store",
        memory=[adapter_conv, adapter_win],
        session_id="multi-mem-sess-001",
        user_id="analyst_multi",
        sender="User",
        sender_name="Analyst",
        message_text="Multi-memory fan-out test message across Buffer and Window.",
    )
    store_data = mh_store.retrieve_history_table().data
    assert store_data["external_memory_connected"] is True
    assert store_data["external_memory_count"] == 2
    assert "MEM_1_CONVERSATION_BUFFER" in store_data["external_memory_names"]
    assert "MEM_2_CHAT_WINDOW" in store_data["external_memory_names"]
    assert "MEM_1_CONVERSATION_BUFFER" in store_data["stored_in_memories"]
    assert "MEM_2_CHAT_WINDOW" in store_data["stored_in_memories"]

    # Add a distinct message only into MEM_1_CONVERSATION_BUFFER to verify merged + deduplicated Retrieve
    adapter_conv.add_message(
        "Exclusive message in MEM_1_CONVERSATION_BUFFER.",
        session_id="multi-mem-sess-001",
        sender="AI",
        sender_name="Cortex Agent",
        user_id="analyst_multi",
        message_id="msg-exclusive-conv-2",
    )

    # 2. Retrieve mode with multiple connected External Memories (fan-in merge & deduplication)
    mh_retrieve = SnowflakeMessageHistoryNativeComponent(
        mode="Retrieve",
        memory=[adapter_conv, adapter_win],
        session_id="multi-mem-sess-001",
        user_id="analyst_multi",
        window_k=10,
    )
    ret_data = mh_retrieve.retrieve_history_table().data
    assert ret_data["external_memory_count"] == 2
    texts = [m.get("text") or m.get("content") for m in ret_data["messages"]]
    assert "Multi-memory fan-out test message across Buffer and Window." in texts
    assert "Exclusive message in MEM_1_CONVERSATION_BUFFER." in texts
    # Shared message must be deduplicated and list both memory sources
    shared_rows = [
        m for m in ret_data["messages"]
        if (m.get("text") or m.get("content")) == "Multi-memory fan-out test message across Buffer and Window."
    ]
    assert len(shared_rows) == 1
    assert set(shared_rows[0]["memory_sources"]) == {"MEM_1_CONVERSATION_BUFFER", "MEM_2_CHAT_WINDOW"}

    # 3. Multiple Memory Base nodes in Tool Mode connected to Agent.tools have distinct tool names
    mb_vec = SnowflakeMemoryBaseNativeComponent(memory_name="MEM_4_VECTOR_KNOWLEDGE", tool_mode=True)
    mb_ent = SnowflakeMemoryBaseNativeComponent(memory_name="MEM_5_STRUCTURED_ENTITY", tool_mode=True)
    t_vec = mb_vec.to_toolkit()
    t_ent = mb_ent.to_toolkit()
    combined_tools = (list(t_vec) if isinstance(t_vec, (list, tuple)) else [t_vec]) + (
        list(t_ent) if isinstance(t_ent, (list, tuple)) else [t_ent]
    )
    tool_names = [t.name for t in combined_tools]
    assert len(tool_names) == len(set(tool_names))
    assert any("mem_4_vector_knowledge" in n for n in tool_names)
    assert any("mem_5_structured_entity" in n for n in tool_names)


def test_w2_r5_04_snowflake_cortex_embeddings_and_split_text_component() -> None:
    """Verify W2-R5-04: 6 official SNOWFLAKE.CORTEX.AI_EMBED models and SplitTextComponent with SPLIT_TEXT_RECURSIVE_CHARACTER."""
    expected_models = {
        "snowflake-arctic-embed-m-v1.5": 768,
        "snowflake-arctic-embed-m": 768,
        "e5-base-v2": 768,
        "snowflake-arctic-embed-l-v2.0": 1024,
        "voyage-multilingual-2": 1024,
        "nv-embed-qa-4": 1024,
    }
    assert set(OFFICIAL_SNOWFLAKE_AI_EMBED_MODEL_IDS) == set(expected_models.keys())
    assert OFFICIAL_SNOWFLAKE_CORTEX_AI_EMBED_MODELS == expected_models
    assert len(OFFICIAL_SNOWFLAKE_AI_EMBED_MODELS) == 6

    catalog = get_snowflake_cortex_embedding_catalog()
    assert len(catalog) == 6
    for entry in catalog:
        assert entry["provider"] == "Snowflake Cortex"
        assert entry["api_key_required"] is False
        assert entry["default"] is True
        assert entry["dimension"] == expected_models[entry["model_name"]]

    reg_status = register_snowflake_cortex_embedding_provider()
    assert reg_status["provider"] == "Snowflake Cortex"
    assert reg_status["embedding_class"] == "SnowflakeCortexEmbeddings"
    assert set(reg_status["models"]) == set(expected_models.keys())

    client = _make_client()
    emb_resp = client.get("/api/v1/snowflake/embedding-models")
    assert emb_resp.status_code == 200
    emb_json = emb_resp.json()
    assert emb_json["ok"] is True
    assert emb_json["provider"] == "Snowflake Cortex"
    assert len(emb_json["models"]) == 6

    for model_id, expected_dim in expected_models.items():
        emb = SnowflakeCortexEmbeddings(model=model_id)
        assert emb.dimension == expected_dim
        vec = emb.embed_query("Test Snowflake Cortex AI_EMBED query")
        assert len(vec) == expected_dim
        assert f"SNOWFLAKE.CORTEX.AI_EMBED('{model_id}'" in emb.last_sql
        docs_vecs = emb.embed_documents(["Doc A", "Doc B"])
        assert len(docs_vecs) == 2
        assert all(len(v) == expected_dim for v in docs_vecs)

    # Test SplitTextComponent with Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)
    splitter = SplitTextComponent(
        data_inputs="## Section 1\nSnowflake Cortex recursive text splitting.\n\n## Section 2\nChunking for RAG and Memories.",
        split_engine="Snowflake Cortex (SPLIT_TEXT_RECURSIVE_CHARACTER)",
        split_format="markdown",
        chunk_size=60,
        chunk_overlap=10,
    )
    sql = splitter.build_cortex_split_sql("Sample text")
    assert "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER('Sample text', 'markdown', 60, 10)" in sql
    df = splitter.split_text()
    rows = getattr(df, "rows", None) or list(df)
    assert len(rows) >= 2


def test_w2_r5_05_stages_and_volumes_backend_and_frontend_bucket_tab() -> None:
    """Verify W2-R5-05: Stages & Volumes endpoints, stage file upload/list/delete/refresh, privilege checks, and frontend Bucket tab."""
    client = _make_client()

    # 1. GET /api/v1/snowflake/stages
    stg_resp = client.get("/api/v1/snowflake/stages")
    assert stg_resp.status_code == 200
    stg_data = stg_resp.json()
    assert stg_data["ok"] is True
    assert any(s["name"] == "SNOWFLAKE_LANGFLOW_STAGE" for s in stg_data["stages"])
    assert "can_create_stage" in stg_data["role_privileges"]
    assert "can_create_external_volume" in stg_data["role_privileges"]

    # 2. POST /api/v1/snowflake/stages/create (Internal SNOWFLAKE_SSE & External S3)
    int_create = client.post(
        "/api/v1/snowflake/stages/create",
        json={
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "name": "W2_R5_DOCS_INTERNAL_STAGE",
            "stage_type": "INTERNAL",
            "encryption_type": "SNOWFLAKE_SSE",
            "directory_enable": True,
            "file_format": "CSV",
            "comment": "Internal SSE stage for AI_PARSE_DOCUMENT",
        },
    )
    assert int_create.status_code in (200, 201)
    int_payload = int_create.json()
    assert int_payload["ok"] is True
    assert "ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')" in int_payload["ddl"]
    assert "DIRECTORY = (ENABLE = TRUE)" in int_payload["ddl"]

    ext_create = client.post(
        "/api/v1/snowflake/stages/create",
        json={
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "name": "W2_R5_S3_EXT_STAGE",
            "stage_type": "EXTERNAL",
            "cloud_provider": "S3",
            "url": "s3://enterprise-analytics-bucket/langflow/",
            "storage_integration": "S3_ENTERPRISE_INT",
            "encryption_type": "AWS_SSE_S3",
            "directory_enable": True,
            "directory_auto_refresh": True,
        },
    )
    assert ext_create.status_code in (200, 201)
    ext_payload = ext_create.json()
    assert ext_payload["ok"] is True
    assert "URL = 's3://enterprise-analytics-bucket/langflow/'" in ext_payload["ddl"]
    assert "STORAGE_INTEGRATION = S3_ENTERPRISE_INT" in ext_payload["ddl"]

    # 3. Upload file to stage, list stage files, refresh stage, and delete file
    up_resp = client.post(
        "/api/v1/snowflake/stages/upload",
        json={
            "stage_fqn": "@SNOWFLAKE_LANGFLOW.CORE.W2_R5_DOCS_INTERNAL_STAGE",
            "file_name": "reports/q4_summary.md",
            "content": "# Q4 Summary\nRevenue grew 28% YoY.",
        },
    )
    assert up_resp.status_code in (200, 201)
    up_data = up_resp.json()
    assert up_data["ok"] is True
    assert "PUT " in up_data["put_sql"]
    assert "ALTER STAGE SNOWFLAKE_LANGFLOW.CORE.W2_R5_DOCS_INTERNAL_STAGE REFRESH" in (up_data["refresh_sql"] or "")

    files_resp = client.get("/api/v1/snowflake/stages/SNOWFLAKE_LANGFLOW.CORE.W2_R5_DOCS_INTERNAL_STAGE/files")
    assert files_resp.status_code == 200
    files_list = files_resp.json()["files"]
    assert any("q4_summary.md" in f["name"] for f in files_list)

    ref_resp = client.post(
        "/api/v1/snowflake/stages/refresh",
        json={"stage_fqn": "@SNOWFLAKE_LANGFLOW.CORE.W2_R5_DOCS_INTERNAL_STAGE"},
    )
    assert ref_resp.status_code == 200
    assert ref_resp.json()["ok"] is True

    del_resp = client.request(
        "DELETE",
        "/api/v1/snowflake/stages/SNOWFLAKE_LANGFLOW.CORE.W2_R5_DOCS_INTERNAL_STAGE/files",
        params={"file_path": "reports/q4_summary.md"},
    )
    assert del_resp.status_code == 200
    assert "REMOVE @SNOWFLAKE_LANGFLOW.CORE.W2_R5_DOCS_INTERNAL_STAGE/reports/q4_summary.md" in del_resp.json()["sql"]

    # 4. External Volumes & Role Privilege Check
    prev_role = LIVE_BRIDGE.active_role
    try:
        LIVE_BRIDGE.active_role = "ACCOUNTADMIN"
        vol_ok = client.post(
            "/api/v1/snowflake/external-volumes/create",
            json={
                "name": "W2_R5_ICEBERG_VOL",
                "storage_provider": "S3",
                "storage_base_url": "s3://enterprise-iceberg-bucket/warehouse/",
                "storage_aws_role_arn": "arn:aws:iam::123456789012:role/snowflake-iceberg-role",
                "allow_writes": True,
            },
        )
        assert vol_ok.status_code in (200, 201)
        assert "CREATE EXTERNAL VOLUME IF NOT EXISTS W2_R5_ICEBERG_VOL" in vol_ok.json()["ddl"]

        LIVE_BRIDGE.active_role = "PUBLIC"
        vol_denied = client.post(
            "/api/v1/snowflake/external-volumes/create",
            json={
                "name": "UNAUTHORIZED_VOL",
                "storage_provider": "S3",
                "storage_base_url": "s3://bucket/path/",
            },
        )
        assert vol_denied.status_code == 403
        assert "Active role PUBLIC does not have CREATE EXTERNAL VOLUME privilege" in vol_denied.json()["message"]
    finally:
        LIVE_BRIDGE.active_role = prev_role

    # 5. Frontend DOM IDs and data-testid attributes in snowflake-ui-integration.js
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    for required_marker in (
        "sflf-rail-stages-btn",
        "sidebar-nav-stages",
        "sflf-stages-volumes-view",
        "snowflake-stages-view",
        "stages-plus-snowflake-btn",
        "stage-upload-file-btn",
        "sflf-create-stage-modal",
        "create-snowflake-stage-modal",
        "enhanceNativeCreateMemoryDialog",
    ):
        assert required_marker in ui_js, f"Missing marker {required_marker} in snowflake-ui-integration.js"


def test_w2_r5_06_files_and_knowledge_exact_5_canonical_components() -> None:
    """Verify W2-R5-06: Files & Knowledge has strictly the 5 canonical components with zero duplicates and [ ❄️ Context ] integration."""
    assert CANONICAL_FILES_AND_KNOWLEDGE_NAMES == (
        "FileSystemTool",
        "Knowledge",
        "MemoryBase",
        "File",
        "SaveToFile",
    )

    fs_comp = SnowflakeFileSystemToolComponent(
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        stage_name="SNOWFLAKE_LANGFLOW_STAGE",
    )
    assert fs_comp.name == "FileSystemTool"
    assert fs_comp.display_name == "File System"
    fs_data = fs_comp.list_stage_files().data
    assert fs_data["stage_fqn"] == "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"
    assert len(fs_comp.to_toolkit()) == 1

    kn_comp = SnowflakeKnowledgeComponent(
        action="Retrieve",
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        knowledge_base="LANGFLOW_KNOWLEDGE_VECTORS",
        embedding_model="snowflake-arctic-embed-m-v1.5",
        search_query="Snowflake governance",
    )
    assert kn_comp.name == "Knowledge"
    assert kn_comp.display_name == "Knowledge"
    kn_data = kn_comp.as_data().data
    assert "VECTOR_COSINE_SIMILARITY" in kn_data["sql"]
    assert " WHERE " not in kn_data["sql"] or "VECTOR_COSINE_SIMILARITY" not in kn_data["sql"].split(" WHERE ", 1)[1]

    mb_comp = SnowflakeCanonicalMemoryBaseComponent()
    assert mb_comp.name == "MemoryBase"
    assert mb_comp.display_name == "Memory Base"

    rf_comp = SnowflakeReadFileComponent(
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        stage_name="SNOWFLAKE_LANGFLOW_STAGE",
        stage_file_path="README.md",
        use_ai_parse_document=True,
        parse_mode="LAYOUT",
    )
    assert rf_comp.name == "File"
    assert rf_comp.display_name == "Read File"
    rf_data = rf_comp.read_stage_file_data().data
    assert "SNOWFLAKE.CORTEX.AI_PARSE_DOCUMENT" in rf_data["sql"]

    wf_comp = SnowflakeWriteFileComponent(
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        stage_name="SNOWFLAKE_LANGFLOW_STAGE",
        file_name="exports/summary.json",
        content='{"status": "ok"}',
    )
    assert wf_comp.name == "SaveToFile"
    assert wf_comp.display_name == "Write File"
    wf_data = wf_comp.save_to_file_data().data
    assert wf_data["target"] == "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE/exports/summary.json"

    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    for removed_dup in (
        "SnowflakeCortexSearchParseDocument",
        "SnowflakeCortexSearchSplitText",
        "SnowflakeCortexSearchServiceCreator",
        "SnowflakeCortexSearchStreamTask",
        "SnowflakeObjectProvisionerFK",
        "SnowflakeStageFileSystem",
    ):
        assert f'"{removed_dup}"' in hooks_py

    # Verify install_live_hooks_in_container.py enforces 5 distinct display_names & icons (never 4x 'File System')
    assert "non_indexed_fk_helper_files" in hooks_py
    assert "expected_fk_display_and_icons" in hooks_py
    for fk_key, fk_disp, fk_icon in (
        ("FileSystemTool", "File System", "folder"),
        ("Knowledge", "Knowledge", "database"),
        ("MemoryBase", "Memory Base", "brain"),
        ("File", "Read File", "file-text"),
        ("SaveToFile", "Write File", "file-text"),
    ):
        assert f'"{fk_key}": ("{fk_disp}", "{fk_icon}")' in hooks_py


def test_w2_r5_07_data_sources_cortex_search_and_cortex_analyst_context_and_tool_mode() -> None:
    """Verify W2-R5-07: Data Sources -> Cortex Search & Cortex Analyst governed by [ ❄️ Context ], REST endpoints, and Tool Mode."""
    cs = SnowflakeCortexSearchComponent(
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        search_service="DOC_SEARCH_SERVICE",
        query="How does role switching work?",
        columns_csv="chunk,relative_path",
        limit=3,
    )
    cs_meta = cs.get_search_metadata().data
    assert cs_meta["rest_endpoint"] == "/api/v2/databases/SNOWFLAKE_LANGFLOW/schemas/CORE/cortex-search-services/DOC_SEARCH_SERVICE:query"
    cs_tools = cs.to_toolkit()
    assert len(cs_tools) == 1
    assert cs_tools[0].metadata["rest_endpoint"] == cs_meta["rest_endpoint"]

    ca = SnowflakeCortexAnalystComponent(
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        source_mode="Semantic View",
        semantic_source="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_SEMANTIC_VIEW",
        question="Show flow run counts by role",
    )
    ca_meta = ca.get_analyst_payload().data
    assert ca_meta["rest_endpoint"] == "/api/v2/cortex/analyst/message"
    ca_tools = ca.to_toolkit()
    assert len(ca_tools) == 1
    assert ca_tools[0].metadata["rest_endpoint"] == "/api/v2/cortex/analyst/message"


def test_w2_r5_08_preserved_concept_zero_diff_and_sanitization_gate() -> None:
    """Verify W2-R5-08: 0 git diff on preserved_provisioning_and_memories_concept.js and [SANITIZATION PASSED]."""
    import subprocess

    diff_res = subprocess.run(
        ["git", "diff", "--exit-code", "frontend_patches/preserved_provisioning_and_memories_concept.js"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert diff_res.returncode == 0, f"Unexpected diff on preserved file: {diff_res.stdout}"

    san_res = subprocess.run(
        [sys.executable, "scripts/verify_public_repo_sanitization.py"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert san_res.returncode == 0, f"Sanitization failed: {san_res.stdout}\n{san_res.stderr}"
    assert "[SANITIZATION PASSED]" in san_res.stdout

