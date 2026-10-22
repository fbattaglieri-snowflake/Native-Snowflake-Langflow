"""Comprehensive Wave 2 Test Suite (`UI-W2-01`, `HUB-W2-01..05`, `DS-W2-01`, `PROV-W2-01`, `MA-W2-01`, `LLM-W2-01..06`, `FK-W2-01..04`)."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lfx_snowflake.bundle_manifest import get_bundle_manifest  # noqa: E402
from lfx_snowflake.components.ai_function import (  # noqa: E402
    AGGREGATE_AI_FUNCTIONS,
    ALL_SUPPORTED_AI_FUNCTIONS,
    TASK_SPECIFIC_AI_FUNCTIONS,
    SnowflakeAICompleteEngine,
    SnowflakeAIFunctionComponent,
)
from lfx_snowflake.components.data_sources import (  # noqa: E402
    DATA_SOURCE_HIERARCHY_ORDER,
    SUPPORTED_SNOWFLAKE_OBJECT_LABELS,
    SUPPORTED_SNOWFLAKE_OBJECT_TYPES,
)
from lfx_snowflake.components.files_and_stage import (  # noqa: E402
    AI_PARSE_DOCUMENT_BILLING_NOTICE,
    CORTEX_ANALYST_SNOWSIGHT_CREATION_MESSAGE,
    CORTEX_SEARCH_CHUNK_SIZE_NOTICE,
    CortexAnalystDisabledCreatorSpec,
    CortexSearchParseDocumentBuilder,
    CortexSearchParseDocumentSpec,
    CortexSearchServiceCreatorBuilder,
    CortexSearchServiceCreatorSpec,
    CortexSearchSplitTextBuilder,
    CortexSearchSplitTextSpec,
    CortexSearchStreamTaskBuilder,
    CortexSearchStreamTaskSpec,
    build_stage_creation_ddl,
)
from lfx_snowflake.components.knowledge_and_memories import (  # noqa: E402
    DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG,
    CreateMemoryDialogService,
    InMemoryVectorStoreEngine,
    VectorBackendType,
)
from lfx_snowflake.components.llm_operations import (  # noqa: E402
    SnowflakeCortexSearchQueryEngine,
    SnowflakeDecisionEngine,
    SnowflakeFunctionEngine,
    SnowflakeSQLStatementEngine,
    StoredProcedureEngine,
)
from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge  # noqa: E402
from lfx_snowflake.governance.models import SnowflakeErrorCode, SnowflakeGovernanceError  # noqa: E402
from lfx_snowflake.governance.provisioner import SnowflakeObjectProvisioner  # noqa: E402


def test_ui_w2_01_and_hub_w2_01_to_05_frontend_and_installer() -> None:
    """Verify UI-W2-01 (browser tab title, Snowflake SVG favicon, dual-symbol Loading... screen) and HUB-W2-01..05 in JS & installer."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    installer_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")

    # UI-W2-01: Browser tab title & Snowflake favicon guard + dual-symbol Loading... screen
    assert "Snowflake Langflow" in ui_js
    assert "ensureBrowserTabBranding" in ui_js
    assert "SNOWFLAKE_SVG_BLACK" in ui_js
    assert "SNOWFLAKE_SVG_COLOR" in ui_js
    assert "<title>Snowflake Langflow</title>" in installer_py
    assert 'rel="icon" type="image/svg+xml"' in installer_py
    assert "snowflake-langflow-dual-loading-logo" in installer_py

    # HUB-W2-01 & HUB-W2-FIX-FETCH
    assert "alignTopbarControls" in ui_js
    assert '"Content-Type": "application/json"' in ui_js or "'Content-Type': 'application/json'" in ui_js

    # HUB-W2-02..05: Section 0 cascading context, Section 1 hierarchical provisioner, Section 2 full memory matrix
    assert "/api/v1/snowflake/context" in ui_js
    assert "/api/v1/snowflake/catalog" in ui_js
    assert "/api/v1/snowflake/provision" in ui_js
    assert "/api/v1/snowflake/memories" in ui_js
    assert "LLM Operations" in ui_js
    assert "snowflake-arctic-embed-m-v1.5" in ui_js
    assert "snowflake-arctic-embed-l-v2.0" in ui_js


def test_hub_w2_02_to_05_backend_context_catalog_provision_and_memories() -> None:
    """Verify LiveSnowflakeBridge cascading context store, role privileges, HUB-W2-04 exclusion, and full memory matrix."""
    bridge = LiveSnowflakeBridge()
    assert "databases" in bridge.selected_context
    assert "visible_objects" in bridge.selected_context
    assert "can_create_database" in bridge.role_privileges

    # Update cascading context selection (HUB-W2-02)
    upd = bridge.update_selected_context(
        {
            "databases": ["SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE", "PUBLIC"]},
            "visible_objects": {
                "tables": ["LANGFLOW_FLOW_RUNS"],
                "stages": ["SNOWFLAKE_LANGFLOW_STAGE"],
                "cortex_search_services": ["DOCS_SEARCH_SRV"],
                "semantic_views": ["SALES_SEMANTIC_VIEW"],
                "cortex_agents": ["SALES_CORTEX_AGENT"],
                "functions": ["MY_SCALAR_UDF"],
                "procedures": ["MY_STORED_PROC"],
            },
        }
    )
    assert upd["ok"] is True
    assert "DOCS_SEARCH_SRV" in upd["selected_context"]["visible_objects"]["cortex_search_services"]

    # HUB-W2-04: Provisioning FUNCTION or PROCEDURE from Hub is strictly blocked
    for blocked_kind in ("function", "procedure", "udf", "udtf", "stored_procedure"):
        with pytest.raises(SnowflakeGovernanceError) as exc_info:
            SnowflakeObjectProvisioner.enforce_hub_object_exclusion(blocked_kind, source="hub", active_role="ACCOUNTADMIN")
        assert exc_info.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    # Allowed when invoked from LLM Operations (LLM-W2-01)
    SnowflakeObjectProvisioner.enforce_hub_object_exclusion("procedure", source="llm_operations", active_role="ACCOUNTADMIN")
    SnowflakeObjectProvisioner.enforce_hub_object_exclusion("udf", source="llm_operations", active_role="ACCOUNTADMIN")

    # HUB-W2-05: Official AI_EMBED models (768 & 1024 dimensions) and memory matrix
    assert DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-m-v1.5"].dimension == 768
    assert DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["e5-base-v2"].dimension == 768
    assert DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-l-v2.0"].dimension == 1024
    assert DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["voyage-multilingual-2"].dimension == 1024

    engine = InMemoryVectorStoreEngine(
        active_role="DATA_ENGINEER",
        snowflake_privileges={
            "DATA_ENGINEER": {
                ("CREATE TABLE", "SNOWFLAKE_LANGFLOW.CORE"),
                ("USAGE", "SNOWFLAKE_LANGFLOW.CORE"),
            }
        },
    )
    dialog = CreateMemoryDialogService(engine)

    # Structured, semi-structured, Postgres relational, and Vector memory creation
    mem_vec = dialog.create_memory(
        name="VecMem768",
        embedding_model=DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-m-v1.5"],
        vector_database=VectorBackendType.STANDARD_VECTOR,
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        table_name="VEC_MEM_768",
        create_physical_resource=True,
    )
    assert mem_vec["embedding_model"] == "snowflake-arctic-embed-m-v1.5"
    assert mem_vec["state"] == "CREATED"


def test_ds_w2_01_prov_w2_01_and_ma_w2_01_sidebar_components() -> None:
    """Verify DS-W2-01 input ordering, PROV-W2-01 relocation out of Data Sources, and MA-W2-01 cascading Cortex Agent."""
    comp_dir = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components"

    # DS-W2-01: Engine -> Database -> Schema -> Object Type -> Object Name
    assert DATA_SOURCE_HIERARCHY_ORDER[:5] == ("engine", "database", "schema_name", "object_type", "object_name")
    assert "SECURE_VIEW" in SUPPORTED_SNOWFLAKE_OBJECT_TYPES
    assert "MATERIALIZED_VIEW" in SUPPORTED_SNOWFLAKE_OBJECT_TYPES
    assert "SNOWFLAKE_POSTGRES_TABLE" in SUPPORTED_SNOWFLAKE_OBJECT_TYPES
    assert "Snowflake Postgres Table" in SUPPORTED_SNOWFLAKE_OBJECT_LABELS

    ds_src = (comp_dir / "data_source" / "snowflake_data_source.py").read_text(encoding="utf-8")
    idx_db = ds_src.index('name="database"')
    idx_sch = ds_src.index('name="schema_name"')
    idx_type = ds_src.index('name="object_type"')
    idx_obj = ds_src.index('name="object_name"')
    assert idx_db < idx_sch < idx_type < idx_obj

    # PROV-W2-01: Removed from data_source, present in llm_operations and files_and_knowledge with distinct internal names
    assert not (comp_dir / "data_source" / "snowflake_object_provisioner.py").exists()
    prov_llm = (comp_dir / "llm_operations" / "snowflake_object_provisioner.py").read_text(encoding="utf-8")
    prov_fk = (comp_dir / "files_and_knowledge" / "snowflake_object_provisioner_fk.py").read_text(encoding="utf-8")
    assert 'name = "SnowflakeObjectProvisioner"' in prov_llm
    assert 'name = "SnowflakeObjectProvisionerFK"' in prov_fk
    assert prov_llm.index('name="database"') < prov_llm.index('name="schema_name"') < prov_llm.index('name="object_kind"')
    assert prov_fk.index('name="database"') < prov_fk.index('name="schema_name"') < prov_fk.index('name="object_kind"')

    # MA-W2-01: Models & Agents Cortex Agent cascading Database -> Schema -> Cortex Agent
    agent_src = (comp_dir / "models_and_agents" / "snowflake_cortex_agent.py").read_text(encoding="utf-8")
    assert 'name="database"' in agent_src
    assert 'name="schema_name"' in agent_src
    assert 'name="cortex_agent"' in agent_src
    assert agent_src.index('name="database"') < agent_src.index('name="schema_name"') < agent_src.index('name="cortex_agent"')


def test_llm_w2_01_to_06_llm_operations_engines_and_components() -> None:
    """Verify LLM-W2-01..06: Stored Procedure (with graphical child chaining), UDF/UDTF, AI Complete vs AI Function, SQL Statement, Cortex Search, Cortex Analyst, Snowflake, and Snowflake Decision."""
    # Backward compatibility invariant on ALL_SUPPORTED_AI_FUNCTIONS (length 9)
    assert len(ALL_SUPPORTED_AI_FUNCTIONS) == 9
    assert AGGREGATE_AI_FUNCTIONS == ("AI_SUMMARIZE_AGG", "AI_AGG")
    assert "AI_COMPLETE" not in TASK_SPECIFIC_AI_FUNCTIONS
    assert "AI_TRANSCRIBE" in TASK_SPECIFIC_AI_FUNCTIONS

    # LLM-W2-01: Stored Procedure with child procedure graphical chain + Function (UDF/UDTF)
    sp_engine = StoredProcedureEngine()
    ddl = sp_engine.build_procedure_ddl(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        procedure_name="PARENT_ORCHESTRATOR",
        language="SQL",
        signature_args="input_id VARCHAR",
        return_type="VARIANT",
        execute_as="EXECUTE AS CALLER",
        body="RETURN OBJECT_CONSTRUCT('status', 'ok');",
        child_procedures=["SNOWFLAKE_LANGFLOW.CORE.CHILD_STEP_1", "SNOWFLAKE_LANGFLOW.CORE.CHILD_STEP_2"],
    )
    assert "CREATE OR REPLACE PROCEDURE SNOWFLAKE_LANGFLOW.CORE.PARENT_ORCHESTRATOR" in ddl
    assert "EXECUTE AS CALLER" in ddl
    assert "CALL SNOWFLAKE_LANGFLOW.CORE.CHILD_STEP_1" in ddl
    assert "CALL SNOWFLAKE_LANGFLOW.CORE.CHILD_STEP_2" in ddl

    fn_engine = SnowflakeFunctionEngine()
    udf_ddl = fn_engine.build_function_ddl(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        function_name="CLEAN_TEXT_UDF",
        function_kind="Scalar UDF",
        language="PYTHON",
        signature_args="val VARCHAR",
        return_type="VARCHAR",
        body="def compute(val):\n    return val.strip()",
    )
    assert "CREATE OR REPLACE FUNCTION SNOWFLAKE_LANGFLOW.CORE.CLEAN_TEXT_UDF" in udf_ddl
    assert "LANGUAGE PYTHON" in udf_ddl

    # LLM-W2-02: Dedicated AI_COMPLETE engine + task-specific AI Function with return_as_json
    aic = SnowflakeAICompleteEngine()
    aic_sql = aic.build_sql(
        model="claude-3-7-sonnet",
        prompt="Summarize strictly as JSON",
        temperature=0.1,
        max_tokens=512,
        guardrails=True,
        response_format_mode="Structured Decision Schema",
        show_details=True,
    )
    assert "AI_COMPLETE" in aic_sql
    assert "response_format" in aic_sql

    aif_sql = SnowflakeAIFunctionComponent.build_task_specific_sql(
        ai_function="AI_SENTIMENT",
        input_text="Great Snowflake experience",
        return_as_json=True,
    )
    assert "OBJECT_CONSTRUCT" in aif_sql
    assert "AI_SENTIMENT" in aif_sql

    # LLM-W2-03, LLM-W2-04, LLM-W2-06
    sql_eng = SnowflakeSQLStatementEngine()
    stmt = sql_eng.build_sql(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        sql_statement="SELECT id, name FROM SNOWFLAKE_LANGFLOW.CORE.ITEMS",
        output_format="JSON Object (OBJECT_CONSTRUCT)",
        max_rows=25,
    )
    assert "OBJECT_CONSTRUCT(*)" in stmt
    assert "LIMIT 25" in stmt

    cs_eng = SnowflakeCortexSearchQueryEngine()
    cs_sql, cs_payload = cs_eng.build_search_preview_sql(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        search_service="DOCS_SEARCH_SRV",
        query="data governance policy",
        columns_csv="chunk,relative_path",
        filter_json='{"@eq": {"language": "it"}}',
        limit=5,
    )
    assert "SNOWFLAKE.CORTEX.SEARCH_PREVIEW" in cs_sql
    assert "DOCS_SEARCH_SRV" in cs_sql
    assert cs_payload["limit"] == 5

    dec_obj = SnowflakeDecisionEngine.evaluate_decision(
        model="claude-3-7-sonnet",
        input_context="Customer requests approve_refund for order #42",
        branches_csv="approve_refund,escalate_human,reject",
    )
    assert "AI_COMPLETE" in dec_obj["sql_preview"]
    assert dec_obj["selected_branch"] == "approve_refund"


def test_fk_w2_01_to_04_files_and_knowledge_and_4_tab_cortex_search_pipeline() -> None:
    """Verify FK-W2-01..04: Internal/External Stage DDL, 4-tab Cortex Search pipeline (AI_PARSE_DOCUMENT, SPLIT_TEXT_RECURSIVE_CHARACTER, CREATE CORTEX SEARCH SERVICE, DOC_STREAM + DOC_INGEST_TASK with _DOC_STREAM_BATCH), and disabled Cortex Analyst tab."""
    # FK-W2-02: Internal & External Stage DDL
    int_ddl = build_stage_creation_ddl(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        stage_name="INTERNAL_DOCS_STAGE",
        stage_type="INTERNAL",
    )
    assert "ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')" in int_ddl
    assert "DIRECTORY = (ENABLE = TRUE)" in int_ddl

    ext_ddl = build_stage_creation_ddl(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        stage_name="EXTERNAL_S3_STAGE",
        stage_type="EXTERNAL",
        external_url="s3://enterprise-bucket/docs/",
        storage_integration="S3_DOCS_INT",
    )
    assert "URL = 's3://enterprise-bucket/docs/'" in ext_ddl
    assert "STORAGE_INTEGRATION = S3_DOCS_INT" in ext_ddl
    assert "DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)" in ext_ddl

    # FK-W2-03 Tab 1: Parse Document (exclusively AI_PARSE_DOCUMENT + LIMIT 1 preview + errorInformation + billing notice)
    p_spec = CortexSearchParseDocumentSpec(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        stage_name="INTERNAL_DOCS_STAGE",
        target_table="PARSED_DOCS",
        parse_mode="LAYOUT",
        page_split=True,
    )
    p_builder = CortexSearchParseDocumentBuilder(p_spec)
    test_1_sql = p_builder.build_parse_sql(test_single_file=True)
    assert "AI_PARSE_DOCUMENT(" in test_1_sql
    assert "TO_FILE('@SNOWFLAKE_LANGFLOW.CORE.INTERNAL_DOCS_STAGE', RELATIVE_PATH)" in test_1_sql
    assert "LATERAL FLATTEN(INPUT => d.parsed:pages" in test_1_sql
    assert "d.parsed:errorInformation AS error_information" in test_1_sql
    assert "LIMIT 1" in test_1_sql
    assert "AI_PARSE_DOCUMENT" in AI_PARSE_DOCUMENT_BILLING_NOTICE

    # FK-W2-03 Tab 2: Split Text (SPLIT_TEXT_RECURSIVE_CHARACTER + Python UDF + 1800/300 + <512 token notice)
    s_spec = CortexSearchSplitTextSpec(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        source_table="PARSED_DOCS",
        target_table="DOC_CHUNKS",
        split_format="markdown",
        chunk_size=1800,
        chunk_overlap=300,
    )
    s_builder = CortexSearchSplitTextBuilder(s_spec)
    split_sql = s_builder.build_split_insert_sql()
    assert "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(p.page_content, 'markdown', 1800, 300)" in split_sql
    assert "512 token" in CORTEX_SEARCH_CHUNK_SIZE_NOTICE
    udf_sql = s_builder.build_langchain_python_udf_ddl()
    assert "RecursiveCharacterTextSplitter" in udf_sql

    # FK-W2-03 Tab 3: Cortex Search Service Creator (CREATE OR REPLACE CORTEX SEARCH SERVICE + Suspend/Resume)
    c_spec = CortexSearchServiceCreatorSpec(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        service_name="DOCS_SEARCH_SRV",
        source_table="DOC_CHUNKS",
        search_column="chunk",
        attribute_columns=("relative_path", "page_index", "language"),
        warehouse="COMPUTE_WH",
        target_lag="1 hour",
        embedding_model="snowflake-arctic-embed-m-v1.5",
    )
    c_builder = CortexSearchServiceCreatorBuilder(c_spec)
    create_srv_ddl = c_builder.build_create_service_ddl()
    assert "CREATE OR REPLACE CORTEX SEARCH SERVICE SNOWFLAKE_LANGFLOW.CORE.DOCS_SEARCH_SRV" in create_srv_ddl
    assert "ON CHUNK" in create_srv_ddl
    assert "ATTRIBUTES RELATIVE_PATH, PAGE_INDEX, LANGUAGE" in create_srv_ddl
    assert "ALTER CORTEX SEARCH SERVICE SNOWFLAKE_LANGFLOW.CORE.DOCS_SEARCH_SRV SUSPEND INDEXING" == c_builder.build_suspend_resume_sql("SUSPEND", "INDEXING")
    assert "ALTER CORTEX SEARCH SERVICE SNOWFLAKE_LANGFLOW.CORE.DOCS_SEARCH_SRV RESUME SERVING" == c_builder.build_suspend_resume_sql("RESUME", "SERVING")

    # FK-W2-03 Tab 4: Stream + Task with atomic _DOC_STREAM_BATCH temp table inside BEGIN...END
    st_spec = CortexSearchStreamTaskSpec(
        database="SNOWFLAKE_LANGFLOW",
        schema="CORE",
        stage_name="INTERNAL_DOCS_STAGE",
    )
    st_builder = CortexSearchStreamTaskBuilder(st_spec)
    assert st_builder.build_stream_ddl() == "CREATE OR REPLACE STREAM SNOWFLAKE_LANGFLOW.CORE.DOC_STREAM ON STAGE SNOWFLAKE_LANGFLOW.CORE.INTERNAL_DOCS_STAGE"
    task_ddl = st_builder.build_task_ddl()
    assert "WHEN SYSTEM$STREAM_HAS_DATA('SNOWFLAKE_LANGFLOW.CORE.DOC_STREAM')" in task_ddl
    assert "BEGIN" in task_ddl and "END;" in task_ddl
    assert "CREATE OR REPLACE TEMP TABLE _DOC_STREAM_BATCH AS" in task_ddl
    assert "WHERE STREAM_ACTION = 'DELETE'" in task_ddl
    assert "EXECUTE TASK SNOWFLAKE_LANGFLOW.CORE.DOC_INGEST_TASK" == st_builder.build_task_operation_sql("EXECUTE_NOW")
    assert "INFORMATION_SCHEMA.TASK_HISTORY" in st_builder.build_task_operation_sql("HISTORY")

    # FK-W2-04: Disabled grey Cortex Analyst creator tab in Files & Knowledge
    dis_state = CortexAnalystDisabledCreatorSpec().to_ui_state()
    assert dis_state["disabled"] is True
    assert dis_state["snowsight_navigation_path"] == "AI & ML -> Cortex Analyst"
    assert "Snowsight" in CORTEX_ANALYST_SNOWSIGHT_CREATION_MESSAGE

    # Verify service-spec.template.yaml mountPath is /persist and bundle_manifest has all Wave 2 components
    spec_yaml = (REPO_ROOT / "service-spec.template.yaml").read_text(encoding="utf-8")
    assert "mountPath: /persist" in spec_yaml
    manifest = get_bundle_manifest()
    comp_ids = {c["component_id"] for c in manifest["components"]}
    for expected_id in (
        "snowflake_object_provisioner_llm",
        "snowflake_object_provisioner_fk",
        "snowflake_stored_procedure",
        "snowflake_function_udf",
        "snowflake_ai_complete",
        "snowflake_sql_statement",
        "snowflake_cortex_search_llm",
        "snowflake_cortex_analyst_llm",
        "snowflake_llm_node",
        "snowflake_decision",
        "snowflake_cortex_search_parse_document",
        "snowflake_cortex_search_split_text",
        "snowflake_cortex_search_service_creator",
        "snowflake_cortex_search_stream_task",
        "snowflake_cortex_analyst_disabled",
    ):
        assert expected_id in comp_ids

    # Verify every native_category_components/*.py file parses cleanly and has a unique internal component `name`
    seen_names: dict[str, str] = {}
    for py_path in sorted((REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components").rglob("*.py")):
        if py_path.name == "__init__.py":
            continue
        tree = ast.parse(py_path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                comp_name = node.name
                for stmt in node.body:
                    if isinstance(stmt, ast.Assign):
                        for tgt in stmt.targets:
                            if isinstance(tgt, ast.Name) and tgt.id == "name" and isinstance(stmt.value, ast.Constant):
                                comp_name = str(stmt.value.value)
                assert comp_name not in seen_names, f"Duplicate component name {comp_name} in {py_path} and {seen_names[comp_name]}"
                seen_names[comp_name] = str(py_path)
    assert len(seen_names) == 28


def test_wave2_undefined_headers_fetch_intercept_guard() -> None:
    """Verify P3-I5b regression guard against 'Cannot read properties of undefined (reading headers)'."""
    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")

    # 1. install_live_hooks_in_container.py patches oXr's hKr.register and Ur.interceptors.request.use
    assert "fetch_intercept_orig" in hooks_py
    assert "fetch_intercept_new" in hooks_py
    assert 'T=T&&typeof T=="object"?{...T}:{};' in hooks_py
    assert 'T.headers={...(T.headers||{})};' in hooks_py
    assert "axios_req_intercept_orig" in hooks_py
    assert "z.headers=z.headers||{};" in hooks_py
    assert "agui_hzr_orig" in hooks_py
    assert 'n?.headers?.get?.("content-type")' in hooks_py
    assert "agui_gzr_orig" in hooks_py
    assert 'c?.headers?.get?.("content-type")' in hooks_py
    assert "__sflfHeadersGuarded" in hooks_py

    # 2. snowflake-ui-integration.js guards window.fetch and passes explicit headers on all fetch calls
    assert "__sflfHeadersGuarded" in ui_js
    assert 'fetch("/api/v1/snowflake/context", {' in ui_js
    assert 'fetch(`/api/v1/snowflake/catalog?database=${dbParam}&schema=${schParam}`, {' in ui_js

