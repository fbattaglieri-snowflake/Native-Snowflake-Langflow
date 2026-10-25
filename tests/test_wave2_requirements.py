"""Comprehensive Wave 2 & Round 2 Test Suite (`UI-W2-01`, `HUB-W2-01..05`, `DS-W2-01`, `PROV-W2-01`, `MA-W2-01`, `LLM-W2-01..06`, `FK-W2-01..04`, `W2-R2-00..W2-R2-14`)."""

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

from lfx_snowflake import standard_langflow  # noqa: E402
from lfx_snowflake._lfx_compat import Tool  # noqa: E402
from lfx_snowflake.bundle_manifest import SNOWFLAKE_BUNDLE_COMPONENTS, get_bundle_manifest  # noqa: E402
from lfx_snowflake.components.ai_function import (  # noqa: E402
    AGGREGATE_AI_FUNCTIONS,
    AI_COMPLETE_MODEL_CAPABILITIES,
    ALL_SUPPORTED_AI_FUNCTIONS,
    SUPPORTED_AI_COMPLETE_MODELS,
    TASK_SPECIFIC_AI_FUNCTIONS,
    SnowflakeAICompleteEngine,
    SnowflakeAIFunctionComponent,
    get_compatible_models,
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
from lfx_snowflake.components.utilities_a2a import (  # noqa: E402
    SnowflakeMessageHistoryAdapter,
    get_memory_catalog_options,
    recall_memory_from_catalog,
)
from lfx_snowflake.fastapi_live_bridge import AGENT_MEMORY_TYPES, LiveSnowflakeBridge  # noqa: E402
from lfx_snowflake.governance.models import SnowflakeErrorCode, SnowflakeGovernanceError  # noqa: E402
from lfx_snowflake.governance.provisioner import SnowflakeObjectProvisioner  # noqa: E402
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_analyst import (  # noqa: E402
    SnowflakeCortexAnalystComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_search import (  # noqa: E402
    SnowflakeCortexSearchComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_iceberg_tables import (  # noqa: E402
    SnowflakeIcebergTablesComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_postgres import (  # noqa: E402
    SnowflakePostgresComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_tables import (  # noqa: E402
    SnowflakeTablesComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_views import (  # noqa: E402
    SnowflakeViewsComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_parse_document import (  # noqa: E402
    SnowflakeCortexSearchParseDocumentComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_service_creator import (  # noqa: E402
    SnowflakeCortexSearchServiceCreatorComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_split_text import (  # noqa: E402
    SnowflakeCortexSearchSplitTextComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_stream_task import (  # noqa: E402
    SnowflakeCortexSearchStreamTaskComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_object_provisioner_fk import (  # noqa: E402
    SnowflakeObjectProvisionerFKComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_complete import (  # noqa: E402
    SnowflakeAICompleteComponent as SidebarAICompleteComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_function import (  # noqa: E402
    SnowflakeAIFunctionComponent as SidebarAIFunctionComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_decision import (  # noqa: E402
    SnowflakeDecisionComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_function_udf import (  # noqa: E402
    SnowflakeFunctionComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import (  # noqa: E402
    SnowflakeObjectProvisionerSidebarComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_stored_procedure import (  # noqa: E402
    SnowflakeStoredProcedureComponent,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_standard_chat import (  # noqa: E402
    StandardChat,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_standard_messages import (  # noqa: E402
    StandardMessages,
)
from lfx_snowflake.native_category_components.models_and_agents.snowflake_standard_responses import (  # noqa: E402
    StandardResponses,
)
from lfx_snowflake.native_category_components.utilities.snowflake_toolguard import (  # noqa: E402
    SnowflakeToolGuardPoliciesComponent,
)


def test_ui_w2_01_and_hub_w2_01_to_05_frontend_and_installer() -> None:
    """Verify UI-W2-01 (browser tab title, Snowflake SVG favicon, Loading... screen) and HUB-W2-01..05 in JS & installer."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    installer_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")

    # UI-W2-01: Browser tab title & Snowflake favicon guard + Loading... screen
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

    # HUB-W2-04 & W2-R2-04B: Provisioning FUNCTION, PROCEDURE, or MEMORY from Hub/Object Provisioner is strictly blocked
    for blocked_kind in ("function", "procedure", "udf", "udtf", "stored_procedure", "memory"):
        with pytest.raises(SnowflakeGovernanceError) as exc_info:
            SnowflakeObjectProvisioner.enforce_hub_object_exclusion(blocked_kind, source="hub", active_role="ACCOUNTADMIN")
        assert exc_info.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    # Allowed when routines are invoked from LLM Operations (LLM-W2-01)
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
    """Verify DS-W2-01 hierarchy, W2-R2-12 4 split Data Sources, PROV-W2-01 relocation out of Data Sources, and MA-W2-01 cascading Cortex Agent."""
    comp_dir = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components"

    # DS-W2-01: Engine -> Database -> Schema -> Object Type -> Object Name preserved in domain contract
    assert DATA_SOURCE_HIERARCHY_ORDER[:5] == ("engine", "database", "schema_name", "object_type", "object_name")
    assert "SECURE_VIEW" in SUPPORTED_SNOWFLAKE_OBJECT_TYPES
    assert "MATERIALIZED_VIEW" in SUPPORTED_SNOWFLAKE_OBJECT_TYPES
    assert "SNOWFLAKE_POSTGRES_TABLE" in SUPPORTED_SNOWFLAKE_OBJECT_TYPES
    assert "Snowflake Postgres Table" in SUPPORTED_SNOWFLAKE_OBJECT_LABELS

    # W2-R2-12: Monolithic snowflake_data_source.py replaced by 4 split Data Source components
    assert not (comp_dir / "data_source" / "snowflake_data_source.py").exists()
    tables_src = (comp_dir / "data_source" / "snowflake_tables.py").read_text(encoding="utf-8")
    assert tables_src.index('name="database"') < tables_src.index('name="schema_name"') < tables_src.index('name="table_name"')

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
    """Verify LLM-W2-01..06: Stored Procedure, UDF/UDTF, AI Complete vs AI Function, SQL Statement, Cortex Search, and Decision."""
    # Backward compatibility invariant on ALL_SUPPORTED_AI_FUNCTIONS (length 9)
    assert len(ALL_SUPPORTED_AI_FUNCTIONS) == 9
    assert AGGREGATE_AI_FUNCTIONS == ("AI_SUMMARIZE_AGG", "AI_AGG")
    assert "AI_COMPLETE" not in TASK_SPECIFIC_AI_FUNCTIONS
    assert "AI_TRANSCRIBE" in TASK_SPECIFIC_AI_FUNCTIONS

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

    aic = SnowflakeAICompleteEngine()
    aic_sql = aic.build_sql(
        model="claude-sonnet-5-5",
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
        model="claude-sonnet-5-5",
        input_context="Customer requests approve_refund for order #42",
        branches_csv="approve_refund,escalate_human,reject",
    )
    assert "AI_COMPLETE" in dec_obj["sql_preview"]
    assert dec_obj["selected_branch"] == "approve_refund"


def test_fk_w2_01_to_04_files_and_knowledge_and_4_tab_cortex_search_pipeline() -> None:
    """Verify FK-W2-01..04 & W2-R2-05..14: Stage DDL, 4-node Cortex Search pipeline, and 26-component Round 2 roster."""
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

    dis_state = CortexAnalystDisabledCreatorSpec().to_ui_state()
    assert dis_state["disabled"] is True
    assert dis_state["snowsight_navigation_path"] == "AI & ML -> Cortex Analyst"
    assert "Snowsight" in CORTEX_ANALYST_SNOWSIGHT_CREATION_MESSAGE

    spec_yaml = (REPO_ROOT / "service-spec.template.yaml").read_text(encoding="utf-8")
    assert "mountPath: /persist" in spec_yaml
    manifest = get_bundle_manifest()
    comp_ids = {c["component_id"] for c in manifest["components"]}
    for expected_id in (
        "snowflake_tables",
        "snowflake_views",
        "snowflake_iceberg_tables",
        "snowflake_postgres",
        "snowflake_cortex_search",
        "snowflake_cortex_analyst",
        "snowflake_object_provisioner_llm",
        "snowflake_object_provisioner_fk",
        "snowflake_stored_procedure",
        "snowflake_function_udf",
        "snowflake_ai_complete",
        "snowflake_ai_function",
        "snowflake_sql_statement",
        "snowflake_llm_node",
        "snowflake_decision",
        "snowflake_cortex_search_parse_document",
        "snowflake_cortex_search_split_text",
        "snowflake_cortex_search_service_creator",
        "snowflake_cortex_search_stream_task",
        "snowflake_toolguard_policies",
    ):
        assert expected_id in comp_ids

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
    assert len(seen_names) == 26


def test_wave2_undefined_headers_fetch_intercept_guard() -> None:
    """Verify P3-I5b regression guard against 'Cannot read properties of undefined (reading headers)'."""
    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")

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

    assert "__sflfHeadersGuarded" in ui_js
    assert 'fetch("/api/v1/snowflake/context", {' in ui_js


def test_w2_r2_00_multi_db_multi_schema_objects_cone_and_canvas_sync() -> None:
    """Verify W2-R2-00: Multi-Database -> Multi-Schema -> Cascading Objects cone, functions/procedures filtering, and syncCanvasNodesWithSnowflakeContext."""
    bridge = LiveSnowflakeBridge()
    cat = bridge.discover_catalog(
        databases=["SNOWFLAKE_LANGFLOW", "ANALYTICS_DB"],
        schemas_by_db={"SNOWFLAKE_LANGFLOW": ["CORE", "PUBLIC"], "ANALYTICS_DB": ["MART"]},
    )
    assert "SNOWFLAKE_LANGFLOW.CORE" in cat["objects_by_db_schema"]
    assert "ANALYTICS_DB.MART" in cat["objects_by_db_schema"]

    # Restrict selected_objects for functions and procedures in SNOWFLAKE_LANGFLOW.CORE
    bridge.update_selected_context(
        {
            "databases": ["SNOWFLAKE_LANGFLOW", "ANALYTICS_DB"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"], "ANALYTICS_DB": ["MART"]},
            "objects_by_db_schema": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["LANGFLOW_FLOW_RUNS", "EXTRA_TBL"],
                    "functions": ["ALLOWED_UDF(VARCHAR)", "HIDDEN_UDF(NUMBER)"],
                    "procedures": ["ALLOWED_PROC(VARCHAR)", "HIDDEN_PROC()"],
                }
            },
            "selected_objects": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["LANGFLOW_FLOW_RUNS"],
                    "functions": ["ALLOWED_UDF(VARCHAR)"],
                    "procedures": ["ALLOWED_PROC(VARCHAR)"],
                }
            },
        }
    )
    assert bridge.get_context_databases() == ["SNOWFLAKE_LANGFLOW", "ANALYTICS_DB"]
    assert bridge.get_context_schemas("ANALYTICS_DB") == ["MART"]
    assert bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "tables") == ["LANGFLOW_FLOW_RUNS"]
    assert bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "functions") == ["ALLOWED_UDF(VARCHAR)"]
    assert bridge.get_context_objects("SNOWFLAKE_LANGFLOW", "CORE", "procedures") == ["ALLOWED_PROC(VARCHAR)"]

    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert "syncCanvasNodesWithSnowflakeContext" in ui_js
    assert '"functions"' in ui_js and '"procedures"' in ui_js


def test_w2_r2_01_02_03_model_catalog_and_dynamic_update_build_config() -> None:
    """Verify W2-R2-01 (model catalog & capability filtering), W2-R2-02 (Stored Procedure & Function dynamic update_build_config), and W2-R2-03 (AI Function per-function fields)."""
    # W2-R2-01: Current Cortex models & capability filtering
    for expected_model in (
        "claude-sonnet-5-5",
        "claude-opus-5-5",
        "claude-haiku-5-5",
        "openai-gpt-6-astra",
        "openai-gpt-6.1-sol",
        "openai-gpt-6-luna",
        "openai-gpt-6-sol",
        "grok-4.7",
        "grok-4.6",
        "kimi-k3",
        "glm-5.3",
    ):
        assert expected_model in SUPPORTED_AI_COMPLETE_MODELS
        assert expected_model in AI_COMPLETE_MODEL_CAPABILITIES

    structured_models = get_compatible_models(require_structured_outputs=True)
    assert "claude-sonnet-5-5" in structured_models
    assert "openai-gpt-6-astra" in structured_models
    assert "glm-5.3" not in structured_models

    multimodal_models = get_compatible_models(require_multimodal=True)
    assert "claude-sonnet-5-5" in multimodal_models
    assert "grok-4.7" not in multimodal_models

    aic_comp = SidebarAICompleteComponent()
    aic_cfg = aic_comp.to_build_config()
    aic_comp.update_build_config(aic_cfg, "JSON Schema Object", "response_format_mode")
    assert aic_cfg["response_json_schema"]["show"] is True
    assert "glm-5.3" not in aic_cfg["model"]["options"]

    dec_comp = SnowflakeDecisionComponent()
    dec_cfg = dec_comp.to_build_config()
    dec_comp.update_build_config(dec_cfg, "AI_COMPLETE (Structured Decision JSON)", "engine_mode")
    assert "glm-5.3" not in dec_cfg["model"]["options"]

    # W2-R2-02: Stored Procedure & Function dynamic update_build_config
    sp_comp = SnowflakeStoredProcedureComponent()
    sp_cfg = sp_comp.to_build_config()
    sp_comp.update_build_config(sp_cfg, "CALL Existing", "action_mode")
    assert sp_cfg["body"]["show"] is False
    assert sp_cfg["procedure_name"]["show"] is False
    assert sp_cfg["call_args"]["show"] is True

    sp_comp.update_build_config(sp_cfg, "CREATE OR REPLACE", "action_mode")
    assert sp_cfg["body"]["show"] is True
    sp_comp.update_build_config(sp_cfg, "PYTHON", "language")
    assert "def run(session):" in sp_cfg["body"]["value"]

    fn_comp = SnowflakeFunctionComponent()
    fn_cfg = fn_comp.to_build_config()
    fn_comp.update_build_config(fn_cfg, "Execute Existing", "action_mode")
    assert fn_cfg["body"]["show"] is False
    assert fn_cfg["function_name"]["show"] is False
    assert fn_cfg["call_args"]["show"] is True

    fn_comp.update_build_config(fn_cfg, "CREATE OR REPLACE", "action_mode")
    assert fn_cfg["body"]["show"] is True
    fn_comp.update_build_config(fn_cfg, "PYTHON", "language")
    assert "def compute(" in fn_cfg["body"]["value"]

    # W2-R2-03: AI Function dynamic per-function field visibility
    aif_comp = SidebarAIFunctionComponent()
    aif_cfg = aif_comp.to_build_config()

    aif_comp.update_build_config(aif_cfg, "AI_TRANSLATE", "ai_function")
    assert aif_cfg["source_lang"]["show"] is True
    assert aif_cfg["target_lang"]["show"] is True
    assert aif_cfg["redact_mode"]["show"] is False
    assert aif_cfg["pii_categories"]["show"] is False

    aif_comp.update_build_config(aif_cfg, "AI_REDACT", "ai_function")
    assert aif_cfg["redact_mode"]["show"] is True
    assert aif_cfg["pii_categories"]["show"] is True
    assert aif_cfg["source_lang"]["show"] is False

    aif_comp.update_build_config(aif_cfg, "AI_COUNT_TOKENS", "ai_function")
    assert aif_cfg["model_for_count_tokens"]["show"] is True
    assert aif_cfg["redact_mode"]["show"] is False

    aif_comp.update_build_config(aif_cfg, "AI_TRANSCRIBE", "ai_function")
    assert aif_cfg["second_text_or_stage_file"]["show"] is True
    assert aif_cfg["input_text"]["show"] is False


def test_w2_r2_04_object_provisioner_4_families_without_memory() -> None:
    """Verify W2-R2-04B: Object Provisioner enforces the 4 infrastructure families (table, view, stage, postgres) and excludes memory."""
    for comp_cls in (SnowflakeObjectProvisionerSidebarComponent, SnowflakeObjectProvisionerFKComponent):
        comp = comp_cls()
        cfg = comp.to_build_config()
        assert cfg["object_kind"]["options"] == ["table", "view", "stage", "postgres"]
        assert "memory" not in cfg["object_kind"]["options"]

        comp.update_build_config(cfg, "view", "object_kind")
        assert cfg["view_type"]["show"] is True
        assert cfg["as_select_query"]["show"] is True
        assert cfg["table_subtype"]["show"] is False

        comp.update_build_config(cfg, "stage", "object_kind")
        cfg["stage_type"]["value"] = "EXTERNAL"
        comp.update_build_config(cfg, "EXTERNAL", "stage_type")
        assert cfg["external_url"]["show"] is True
        assert cfg["storage_integration"]["show"] is True

        comp.update_build_config(cfg, "postgres", "object_kind")
        assert cfg["postgres_instance_name"]["show"] is True
        assert cfg["compute_family"]["show"] is True

        comp.object_kind = "memory"
        with pytest.raises(ValueError, match="Memories"):
            comp.provision_object()


def test_w2_r2_05_deprefixed_sidebar_display_names() -> None:
    """Verify W2-R2-05: Every sidebar component in native_category_components and bundle_manifest has icon='Snowflake' and no 'Snowflake ' prefix in display_name."""
    for spec in SNOWFLAKE_BUNDLE_COMPONENTS:
        assert not spec.display_name.startswith("Snowflake "), f"BundleComponentSpec {spec.component_id} still has 'Snowflake ' prefix: {spec.display_name}"

    for py_path in sorted((REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components").rglob("*.py")):
        if py_path.name == "__init__.py":
            continue
        tree = ast.parse(py_path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                for stmt in node.body:
                    if isinstance(stmt, ast.Assign):
                        for tgt in stmt.targets:
                            if isinstance(tgt, ast.Name) and tgt.id == "display_name" and isinstance(stmt.value, ast.Constant):
                                dname = str(stmt.value.value)
                                assert not dname.startswith("Snowflake "), f"{py_path.name} still has 'Snowflake ' prefix: {dname}"


def test_w2_r2_06_and_12_data_sources_split_4_components_and_cortex_search_analyst_tool_mode() -> None:
    """Verify W2-R2-06 and W2-R2-12: 6 Data Sources components (Tables, Views, Iceberg Tables, Postgres, Cortex Search, Cortex Analyst) with Tool Mode."""
    tbl_comp = SnowflakeTablesComponent()
    assert tbl_comp.display_name == "Tables"
    assert len(tbl_comp.to_toolkit()) == 1
    assert tbl_comp.to_toolkit()[0].metadata["tool_mode"] is True

    vw_comp = SnowflakeViewsComponent()
    assert vw_comp.display_name == "Views"
    assert len(vw_comp.to_toolkit()) == 1

    ice_comp = SnowflakeIcebergTablesComponent()
    assert ice_comp.display_name == "Iceberg Tables"
    assert len(ice_comp.to_toolkit()) == 1

    pg_comp = SnowflakePostgresComponent()
    assert pg_comp.display_name == "Postgres"
    assert len(pg_comp.to_toolkit()) == 1

    cs_comp = SnowflakeCortexSearchComponent()
    assert cs_comp.display_name == "Cortex Search"
    cs_tools = cs_comp.to_toolkit()
    assert len(cs_tools) == 1
    assert cs_tools[0].metadata["tool_mode"] is True

    ca_comp = SnowflakeCortexAnalystComponent()
    assert ca_comp.display_name == "Cortex Analyst"
    ca_payload = ca_comp.get_analyst_payload().data
    assert ca_payload["rest_endpoint"] == "/api/v2/cortex/analyst/message"
    ca_tools = ca_comp.to_toolkit()
    assert len(ca_tools) == 1
    assert ca_tools[0].metadata["rest_endpoint"] == "/api/v2/cortex/analyst/message"


def test_w2_r2_07_standard_langflow_reasoning_across_3_proxy_apis() -> None:
    """Verify W2-R2-07: ChatOpenAI reasoning_effort fix (via extra_body, no extra_forbidden), Responses reasoning, Messages Extended Thinking, and flexible max_tokens."""
    assert StandardChat.display_name == "Proxy - Chat"
    assert StandardResponses.display_name == "Proxy - Responses"
    assert StandardMessages.display_name == "Proxy - Messages"

    # 1. /chat/v1: openai-gpt-6-luna defaults unspecified -> none in extra_body; grok-4.7 omits reasoning_effort
    chat_luna = standard_langflow.resolve_reasoning_options("chat", "openai-gpt-6-luna", reasoning="unspecified", max_tokens=4096)
    assert chat_luna["max_tokens"] == 4096
    assert chat_luna["extra_body"] == {"reasoning_effort": "none"}

    chat_sol_high = standard_langflow.resolve_reasoning_options("chat", "openai-gpt-6-sol", reasoning_effort="high", max_tokens=8192)
    assert chat_sol_high["extra_body"] == {"reasoning_effort": "high"}

    chat_grok = standard_langflow.resolve_reasoning_options("chat", "grok-4.7", reasoning_effort="high", max_tokens=2048)
    assert chat_grok["extra_body"] is None

    # 2. /responses/v1: store=False, include=['reasoning.encrypted_content'], extra_body.reasoning
    resp_astra = standard_langflow.resolve_reasoning_options("responses", "openai-gpt-6-astra", reasoning_effort="high", max_tokens=4096)
    assert resp_astra["store"] is False
    assert resp_astra["include"] == ["reasoning.encrypted_content"]
    assert resp_astra["extra_body"] == {"reasoning": {"effort": "high", "summary": "auto"}}

    # 3. /messages: Extended Thinking budget_tokens and auto-adjusted max_tokens > budget_tokens
    msg_thinking = standard_langflow.resolve_reasoning_options(
        "messages",
        "claude-sonnet-5-5",
        extended_thinking="enabled",
        thinking_budget_tokens=4096,
        max_tokens=2048,
    )
    assert msg_thinking["thinking"] == {"type": "enabled", "budget_tokens": 4096}
    assert msg_thinking["max_tokens"] >= 5120


def test_w2_r2_08_09_10_utilities_toolguard_redesign_and_sidebar_removals() -> None:
    """Verify W2-R2-08, W2-R2-09, and W2-R2-10: Smart Router & Python Interpreter PyPI removed from sidebar; ToolGuard wraps Tools and enforces policies."""
    comp_dir = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components"
    assert not (comp_dir / "llm_operations" / "snowflake_smart_router.py").exists()
    assert not (comp_dir / "utilities" / "snowflake_python_pypi.py").exists()

    raw_tool = Tool(
        name="query_orders",
        description="Query orders table",
        func=lambda q: {"query": q, "rows": [{"order_id": 1}]},
    )
    tg = SnowflakeToolGuardPoliciesComponent(tools=[raw_tool], enabled=True)
    assert tg.display_name == "ToolGuard"
    guarded = tg.build_guarded_tools()
    assert len(guarded) == 1
    assert guarded[0].name == "guarded_query_orders"

    safe_res = guarded[0].invoke("SELECT * FROM ORDERS WHERE id = 1")
    assert safe_res["toolguard_enforced"] is True
    assert safe_res["output"]["rows"] == [{"order_id": 1}]

    with pytest.raises(ValueError, match="BLOCKED_BY_TOOLGUARD_POLICY"):
        guarded[0].invoke("DROP TABLE ORDERS")


def test_w2_r2_11_4_node_cortex_search_pipeline_guidance_and_chaining_ports() -> None:
    """Verify W2-R2-11: 4-node Cortex Search pipeline guidance banner and upstream/downstream chaining ports (1 -> 2 -> 3 -> 4)."""
    step1 = SnowflakeCortexSearchParseDocumentComponent(
        database="SNOWFLAKE_LANGFLOW",
        schema_name="CORE",
        stage_name="DOCS_STAGE",
        target_table="CUSTOM_PARSED_DOCS",
        action_mode="Dry-Run SQL Preview",
    )
    ref1 = step1.as_table_reference()
    assert ref1.data["target_table"] == "CUSTOM_PARSED_DOCS"

    step2 = SnowflakeCortexSearchSplitTextComponent(
        upstream_parse_result=ref1,
        target_table="CUSTOM_DOC_CHUNKS",
        action_mode="Dry-Run SQL Preview",
    )
    res2 = step2.run_split_text()
    assert res2.data["source_table"] == "CUSTOM_PARSED_DOCS"
    assert res2.data["target_table"] == "CUSTOM_DOC_CHUNKS"
    assert "W2-R2-11" in res2.data["pipeline_guide"]

    step3 = SnowflakeCortexSearchServiceCreatorComponent(
        upstream_chunks_result=step2.as_chunks_reference(),
        service_name="CUSTOM_SEARCH_SRV",
        action_mode="Dry-Run DDL Preview",
    )
    res3 = step3.run_search_service_creator()
    assert res3.data["source_table"] == "CUSTOM_DOC_CHUNKS"
    assert "CUSTOM_DOC_CHUNKS" in res3.data["create_service_ddl"]

    step4 = SnowflakeCortexSearchStreamTaskComponent(
        upstream_stage_or_service=step3.as_service_reference(),
        action_mode="Dry-Run DDL Preview",
    )
    res4 = step4.run_stream_task()
    assert res4.data["chunks_table_fqn"] == "SNOWFLAKE_LANGFLOW.CORE.CUSTOM_DOC_CHUNKS"
    assert "_DOC_STREAM_BATCH" in res4.data["task_ddl"]


def test_w2_r2_13_memories_plus_snowflake_3_agent_memory_types_and_workflow_recall() -> None:
    """Verify W2-R2-13 & W2-R3-05: 5 Canonical Agent Memory types + 3 Legacy aliases (episodic_session, semantic_vector, structured_long_term) and workflow recall."""
    from lfx_snowflake.fastapi_live_bridge import LEGACY_MEMORY_TYPE_ALIASES, LIVE_BRIDGE

    mem_type_ids = {m["id"] if "id" in m else m["agent_memory_type"] for m in AGENT_MEMORY_TYPES}
    assert mem_type_ids == {
        "graph_state_checkpointer",
        "buffer_window_memory",
        "summary_memory",
        "user_profile_entity_memory",
        "episodic_memory",
    }
    assert set(LEGACY_MEMORY_TYPE_ALIASES.keys()) == {
        "episodic_session",
        "semantic_vector",
        "structured_long_term",
    }

    ep_res = LIVE_BRIDGE.create_or_register_memory(
        {
            "name": "AGENT_EPISODIC_TURNS",
            "agent_memory_type": "episodic_session",
            "vector_database": "Hybrid Table",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "dry_run": True,
        }
    )
    assert ep_res["ok"] is True
    assert ep_res["memory"]["agent_memory_type"] == "episodic_session"
    assert ep_res["memory"]["canonical_memory_type"] == "buffer_window_memory"
    assert "HYBRID TABLE" in ep_res["ddl"]

    sem_res = LIVE_BRIDGE.create_or_register_memory(
        {
            "name": "AGENT_SEMANTIC_VEC",
            "agent_memory_type": "semantic_vector",
            "vector_database": "Standard Table (VECTOR)",
            "embedding_model": "snowflake-arctic-embed-l-v2.0",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "dry_run": True,
        }
    )
    assert sem_res["ok"] is True
    assert sem_res["memory"]["dimension"] == 1024
    assert "VECTOR(FLOAT, 1024)" in sem_res["ddl"]

    lt_res = LIVE_BRIDGE.create_or_register_memory(
        {
            "name": "AGENT_LONG_TERM_FACTS",
            "agent_memory_type": "structured_long_term",
            "vector_database": "Standard Table",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "dry_run": True,
        }
    )
    assert lt_res["ok"] is True
    assert lt_res["memory"]["agent_memory_type"] == "structured_long_term"
    assert lt_res["memory"]["canonical_memory_type"] == "user_profile_entity_memory"

    opts = get_memory_catalog_options(include_none=True)
    assert "None" in opts
    assert "AGENT_EPISODIC_TURNS" in opts
    adapter = SnowflakeMessageHistoryAdapter(memory_catalog_ref="AGENT_SEMANTIC_VEC")
    recalled = adapter.recall(query="governance")
    assert recalled["ok"] is True
    assert recalled["memory"]["name"] == "AGENT_SEMANTIC_VEC"
    assert recall_memory_from_catalog("AGENT_EPISODIC_TURNS")["memory"]["agent_memory_type"] == "episodic_session"

    modals_tsx = (REPO_ROOT / "frontend_patches" / "settings_and_memories_modals.tsx").read_text(encoding="utf-8")
    assert "+ Snowflake" in modals_tsx
    assert "episodic_session" in modals_tsx
    assert "semantic_vector" in modals_tsx
    assert "structured_long_term" in modals_tsx


def test_w2_r2_14_official_snowflake_blue_29b5e8_and_loading_screen_fix() -> None:
    """Verify W2-R2-14: Official Snowflake Blue (#29B5E8) branding across topbar/welcome/loading/memories and removal of 3-layer icon from Loading... screen."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    topbar_tsx = (REPO_ROOT / "frontend_patches" / "topbar_and_branding.tsx").read_text(encoding="utf-8")
    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")

    assert "#29B5E8" in ui_js
    assert "#29B5E8" in topbar_tsx
    assert "#29B5E8" in hooks_py
    assert 'data-testid="snowflake-official-logo-blue"' in topbar_tsx
    assert "?v=sflf042" in hooks_py or "?v=sflf043" in hooks_py or "?v=sflf046" in hooks_py

    # Verify s4_loading_new in install_live_hooks_in_container.py does NOT contain c6e (3-layer stacked icon)
    s4_idx = hooks_py.index("s4_loading_new = (")
    s4_snippet = hooks_py[s4_idx : s4_idx + 260]
    assert "sf_svg_inline_lg" in s4_snippet
    assert "c6e" not in s4_snippet


def test_w2_r2_00_04a_data_sources_cascading_build_config_and_installer_origin_preservation() -> None:
    """Verify W2-R2-00 / W2-R2-04A cascading update_build_config across all 6 Data Source components and installer origin preservation."""
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    LIVE_BRIDGE.update_selected_context(
        {
            "database": "RETAIL_DATA_PLATFORM",
            "schema": "ANALYTICS",
            "databases": ["RETAIL_DATA_PLATFORM", "SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {
                "RETAIL_DATA_PLATFORM": ["ANALYTICS", "RAW"],
                "SNOWFLAKE_LANGFLOW": ["CORE"],
            },
            "objects_by_db_schema": {
                "RETAIL_DATA_PLATFORM.ANALYTICS": {
                    "tables": ["ORDERS_FACT", "CUSTOMERS_DIM"],
                    "views": ["REVENUE_SUMMARY_VW"],
                    "iceberg_tables": ["CLICKSTREAM_ICEBERG"],
                    "postgres_instances": ["RETAIL_PG_MAIN"],
                    "cortex_search_services": ["PRODUCT_CATALOG_SEARCH"],
                    "semantic_views": ["RETAIL_SALES_SEMANTIC_VIEW"],
                }
            },
            "selected_objects": {
                "RETAIL_DATA_PLATFORM.ANALYTICS": {
                    "tables": ["ORDERS_FACT"],
                    "views": ["REVENUE_SUMMARY_VW"],
                    "iceberg_tables": ["CLICKSTREAM_ICEBERG"],
                    "postgres_instances": ["RETAIL_PG_MAIN"],
                    "cortex_search_services": ["PRODUCT_CATALOG_SEARCH"],
                    "semantic_views": ["RETAIL_SALES_SEMANTIC_VIEW"],
                }
            },
        }
    )

    tbl = SnowflakeTablesComponent()
    tbl_cfg = tbl.to_build_config()
    tbl.update_build_config(tbl_cfg, "RETAIL_DATA_PLATFORM", "database")
    assert "RETAIL_DATA_PLATFORM" in tbl_cfg["database"]["options"]
    assert "ANALYTICS" in tbl_cfg["schema_name"]["options"]
    assert tbl_cfg["table_name"]["options"] == ["ORDERS_FACT"]

    vw = SnowflakeViewsComponent()
    vw_cfg = vw.to_build_config()
    vw.update_build_config(vw_cfg, "RETAIL_DATA_PLATFORM", "database")
    assert vw_cfg["view_name"]["options"] == ["REVENUE_SUMMARY_VW"]

    ice = SnowflakeIcebergTablesComponent()
    ice_cfg = ice.to_build_config()
    ice.update_build_config(ice_cfg, "RETAIL_DATA_PLATFORM", "database")
    assert ice_cfg["iceberg_table_name"]["options"] == ["CLICKSTREAM_ICEBERG"]

    pg = SnowflakePostgresComponent()
    pg_cfg = pg.to_build_config()
    pg.update_build_config(pg_cfg, "RETAIL_DATA_PLATFORM", "database")
    assert pg_cfg["postgres_instance"]["options"] == ["RETAIL_PG_MAIN"]

    cs = SnowflakeCortexSearchComponent()
    cs_cfg = cs.to_build_config()
    cs.update_build_config(cs_cfg, "RETAIL_DATA_PLATFORM", "database")
    assert cs_cfg["search_service"]["options"] == ["PRODUCT_CATALOG_SEARCH"]

    ca = SnowflakeCortexAnalystComponent()
    ca_cfg = ca.to_build_config()
    ca.update_build_config(ca_cfg, "RETAIL_DATA_PLATFORM", "database")
    assert ca_cfg["semantic_view"]["options"] == ["RETAIL_SALES_SEMANTIC_VIEW"]

    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert 'id="sflf-sec2"' in ui_js
    assert "openSnowflakeHubModal(focusSection)" in ui_js


def test_w2_r3_01_02_loading_snowflake_blue_mutation_observer_and_create_first_flow_button() -> None:
    """Verify W2-R3 Items 1 & 2: Loading... screen #29B5E8 with read-only MutationObserver and '+ Create first flow' button in #29B5E8."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")

    # Item 1: Loading... screen & MutationObserver in #29B5E8
    assert "#root .loading-component" in ui_js
    assert '[data-testid*="loading"]' in ui_js
    assert "fill: #29B5E8 !important;" in ui_js
    assert "stroke: #29B5E8 !important;" in ui_js
    assert "_loadingBodyObserverInstalled" in ui_js
    assert "new MutationObserver" in ui_js
    assert "#root .loading-component" in hooks_py

    # Item 2: '+ Create first flow' / new-project-btn in #29B5E8
    assert '[data-testid="new-project-btn"]' in ui_js
    assert '[data-testid="empty_page_create_first_flow_button"]' in ui_js
    assert "styleCreateFirstFlowButtons" in ui_js
    assert "+ Create first flow" in ui_js
    assert 'data-testid="new-project-btn"' in hooks_py


def test_w2_r3_03_04_snowsight_hierarchical_tree_tristate_checkboxes_and_responsive_css() -> None:
    """Verify W2-R3 Items 3 & 4: Snowsight-style hierarchical expandable tree, tri-state cascading checkboxes, lazy-load routes, and fluid responsive CSS."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    modals_tsx = (REPO_ROOT / "frontend_patches" / "settings_and_memories_modals.tsx").read_text(encoding="utf-8")
    bridge_py = (REPO_ROOT / "src" / "lfx_snowflake" / "fastapi_live_bridge.py").read_text(encoding="utf-8")

    # Item 3: Snowsight hierarchical tree + search + tri-state cascading checkboxes
    for content in (ui_js, modals_tsx):
        assert 'data-testid="snowsight-hierarchy-tree"' in content
        assert 'data-testid="snowsight-hierarchy-search"' in content
        assert "Search databases, schemas, tables, memories, services..." in content
        assert "data-indeterminate" in content
        assert ".indeterminate =" in content

    assert "computeDatabaseTriState" in ui_js
    assert "computeSchemaTriState" in ui_js
    assert "computeCategoryTriState" in ui_js
    assert "toggleTreeDatabaseSelection" in ui_js
    assert "toggleTreeSchemaSelection" in ui_js
    assert "toggleTreeObjectSelection" in ui_js
    assert "/api/v1/snowflake/schemas" in ui_js
    assert "/api/v1/snowflake/objects" in ui_js
    assert '@sf_router.get("/schemas")' in bridge_py
    assert '@sf_router.get("/objects")' in bridge_py

    # Item 4: Fluid responsive CSS across zoom levels
    assert "clamp(320px, 94vw, 980px)" in ui_js or "clamp(320px,94vw,980px)" in ui_js
    assert "min(90dvh, 90vh)" in ui_js or "min(90dvh,90vh)" in ui_js
    assert "repeat(auto-fit, minmax(min(100%, 280px), 1fr))" in ui_js
    assert "clamp(320px, 94vw, 980px)" in modals_tsx


def test_w2_r3_05_06_five_langchain_memory_types_optional_embeddings_and_memories_only_button() -> None:
    """Verify W2-R3 Items 5 & 6: 5 LangChain/Langflow Memory Types, non-embedding SQL lookup, optional Snowflake/external embeddings, strict Memories-only '+ Snowflake' button, light modal, and searchable cards."""
    from lfx_snowflake.components.knowledge_and_memories import (
        AGENT_MEMORY_TYPES as KM_MEMORY_TYPES,
        EMBEDDING_PROVIDERS_CATALOG as KM_EMBED_CATALOG,
        SnowflakeMemoryManagerComponent,
    )
    from lfx_snowflake.fastapi_live_bridge import (
        EMBEDDING_PROVIDERS_CATALOG,
        EXTERNAL_EMBEDDING_PROVIDERS,
        LIVE_BRIDGE,
    )

    expected_5_types = [
        "graph_state_checkpointer",
        "buffer_window_memory",
        "summary_memory",
        "user_profile_entity_memory",
        "episodic_memory",
    ]
    assert [m["id"] for m in AGENT_MEMORY_TYPES] == expected_5_types
    assert [m["id"] for m in KM_MEMORY_TYPES] == expected_5_types

    bridge_prov_ids = {p["id"] if isinstance(p, dict) else p for p in EMBEDDING_PROVIDERS_CATALOG}
    km_prov_ids = {p["id"] if isinstance(p, dict) else p for p in KM_EMBED_CATALOG}
    for prov_key in ("snowflake_cortex", "openai", "cohere", "huggingface_ollama", "custom_endpoint"):
        assert prov_key in bridge_prov_ids
        assert prov_key in km_prov_ids
    assert "openai" in EXTERNAL_EMBEDDING_PROVIDERS

    # Non-embedding (default) creation and direct SQL lookup across all 5 canonical types
    for mem_type_id in expected_5_types:
        res = LIVE_BRIDGE.create_or_register_memory(
            {
                "name": f"TEST_{mem_type_id.upper()}",
                "agent_memory_type": mem_type_id,
                "enable_embedding": False,
                "database": "SNOWFLAKE_LANGFLOW",
                "schema": "CORE",
                "dry_run": True,
            }
        )
        assert res["ok"] is True
        mem = res["memory"]
        assert mem["canonical_memory_type"] == mem_type_id
        assert mem["enable_embedding"] is False
        assert "VECTOR(" not in res["ddl"]
        assert "SELECT " in mem["how_to_feed_without_embedding"]

        recall_res = LIVE_BRIDGE.recall_memory(
            f"TEST_{mem_type_id.upper()}",
            query="status check",
            session_id="sess_01",
            thread_id="thr_01",
            user_id="usr_01",
        )
        assert recall_res["ok"] is True
        assert recall_res["enable_embedding"] is False
        assert "SELECT " in recall_res["recall_sql"]

    # Optional embedding enabled: Snowflake Cortex AI_EMBED (768d) and External OpenAI (1536d)
    vec_sf = LIVE_BRIDGE.create_or_register_memory(
        {
            "name": "TEST_EPISODIC_SF_EMBED",
            "agent_memory_type": "episodic_memory",
            "enable_embedding": True,
            "embedding_provider": "snowflake_cortex",
            "embedding_model": "snowflake-arctic-embed-m-v1.5",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "dry_run": True,
        }
    )
    assert vec_sf["ok"] is True
    assert vec_sf["memory"]["enable_embedding"] is True
    assert vec_sf["memory"]["dimension"] == 768
    assert "VECTOR(FLOAT, 768)" in vec_sf["ddl"]

    vec_ext = LIVE_BRIDGE.create_or_register_memory(
        {
            "name": "TEST_EPISODIC_OPENAI_EMBED",
            "agent_memory_type": "episodic_memory",
            "enable_embedding": True,
            "embedding_provider": "openai",
            "embedding_model": "text-embedding-3-small",
            "database": "SNOWFLAKE_LANGFLOW",
            "schema": "CORE",
            "dry_run": True,
        }
    )
    assert vec_ext["ok"] is True
    assert vec_ext["memory"]["enable_embedding"] is True
    assert vec_ext["memory"]["embedding_provider"] == "openai"
    assert vec_ext["memory"]["dimension"] == 1536

    # Component metadata & recall
    comp = SnowflakeMemoryManagerComponent(
        name="COMP_MEM_TEST",
        agent_memory_type="buffer_window_memory",
        enable_embedding=False,
    )
    meta = comp.get_type_metadata()
    assert meta["id"] == "buffer_window_memory"
    assert meta["needs_embedding_badge"] == "NO"

    # Item 6: Strict Memories view check, normal z-index behind modals, light modal & searchable cards
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    modals_tsx = (REPO_ROOT / "frontend_patches" / "settings_and_memories_modals.tsx").read_text(encoding="utf-8")
    assert "function isStrictMemoriesView()" in ui_js
    assert "function isAnyModalDialogOpen()" in ui_js
    assert 'z-index:10' in ui_js
    assert '"create-snowflake-memory-modal"' in ui_js
    assert '"snowflake-memories-cards-grid"' in ui_js
    assert 'data-testid="create-snowflake-memory-modal"' in modals_tsx
    assert 'data-testid="snowflake-memories-cards-grid"' in modals_tsx


def test_w2_r3_07_spcs_sqlite_wal_busy_timeout_and_writable_uv_cache_dir(tmp_path: Path) -> None:
    """Verify W2-R3 Item 7: SQLite WAL mode + busy_timeout=30000 and writable UV_CACHE_DIR=/tmp/uv-cache."""
    from lfx_snowflake.fastapi_live_bridge import _open_sqlite_conn, ensure_sqlite_wal_and_uv_cache

    test_db = tmp_path / "test_langflow.db"
    status = ensure_sqlite_wal_and_uv_cache()
    assert status["uv_cache_dir"]
    assert status["busy_timeout_ms"] == 30000

    conn_rw = _open_sqlite_conn(str(test_db), readonly=False, timeout=30.0)
    try:
        journal = conn_rw.execute("PRAGMA journal_mode;").fetchone()[0]
        busy = conn_rw.execute("PRAGMA busy_timeout;").fetchone()[0]
        assert str(journal).lower() == "wal"
        assert int(busy) == 30000
        conn_rw.execute("CREATE TABLE IF NOT EXISTS t (id INT);")
        conn_rw.commit()
    finally:
        conn_rw.close()

    conn_ro = _open_sqlite_conn(str(test_db), readonly=True, timeout=30.0)
    try:
        busy_ro = conn_ro.execute("PRAGMA busy_timeout;").fetchone()[0]
        assert int(busy_ro) == 30000
    finally:
        conn_ro.close()

    dockerfile_txt = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    assert "UV_CACHE_DIR=/tmp/uv-cache" in dockerfile_txt
    assert "PRAGMA busy_timeout=30000" in hooks_py
    assert "PRAGMA journal_mode=WAL" in hooks_py


def test_w2_r3_08_verifier_rbac_api_routes_and_custom_embedding_dimensions() -> None:
    """Verify W2-R3 RBAC enforcement on 5 memory types, custom endpoint dimensions, and Snowsight tree context epoch."""
    from lfx_snowflake.api_routes import SnowflakeAPIRouter
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    router = SnowflakeAPIRouter(initial_role="DATA_ENGINEER_ROLE")
    cat_resp = router.list_memories_catalog()
    assert cat_resp.status_code == 200
    assert len(cat_resp.body["agent_memory_types"]) == 5
    assert "openai" in cat_resp.body["external_embedding_providers"]

    # Custom endpoint dimension (e.g. 2048d) when enable_embedding=True
    custom_resp = router.create_or_bind_memory(
        name="CUSTOM_ENDPOINT_MEM",
        agent_memory_type="episodic_memory",
        enable_embedding=True,
        embedding_provider="custom_endpoint",
        embedding_model="custom-bge-2048",
        custom_dimension=2048,
        endpoint_url="https://embeddings.internal.example/v1/embeddings",
        create_new_physical_store=True,
    )
    assert custom_resp.status_code == 201
    assert custom_resp.body["memory"]["dimension"] == 2048
    assert custom_resp.body["memory"]["embedding_provider"] == "custom_endpoint"

    # Switch to ANALYST_ROLE -> physical store creation denied (403), binding existing allowed (201)
    router.switch_active_role("ANALYST_ROLE")
    denied_resp = router.create_or_bind_memory(
        name="FORBIDDEN_PHYSICAL_MEM",
        agent_memory_type="graph_state_checkpointer",
        create_new_physical_store=True,
    )
    assert denied_resp.status_code == 403
    assert denied_resp.body["error_code"] == "AUTHZ_ROLE_DENIED"

    bind_resp = router.create_or_bind_memory(
        name="ALLOWED_BIND_MEM",
        agent_memory_type="graph_state_checkpointer",
        create_new_physical_store=False,
    )
    assert bind_resp.status_code == 201
    assert bind_resp.body["memory"]["canonical_memory_type"] == "graph_state_checkpointer"

    # Verify Snowsight hierarchical tree context persistence & context_epoch increment
    epoch_before = LIVE_BRIDGE.context_epoch
    ctx_res = LIVE_BRIDGE.set_context(
        {
            "databases": ["SNOWFLAKE_LANGFLOW"],
            "schemas_by_db": {"SNOWFLAKE_LANGFLOW": ["CORE"]},
            "objects_by_db_schema": {
                "SNOWFLAKE_LANGFLOW.CORE": {
                    "tables": ["LANGFLOW_FLOW_RUNS"],
                    "memories": ["ALLOWED_BIND_MEM"],
                }
            },
        }
    )
    assert ctx_res["ok"] is True
    assert LIVE_BRIDGE.context_epoch == epoch_before + 1
    assert "SNOWFLAKE_LANGFLOW.CORE" in ctx_res["selected_context"]["objects_by_db_schema"]

    # Verify LIVE_BRIDGE HTTP 403 PERMISSION_DENIED when PUBLIC role attempts non-dry-run physical memory creation
    prev_role = LIVE_BRIDGE.active_role
    try:
        LIVE_BRIDGE.active_role = "PUBLIC"
        forbidden_live = LIVE_BRIDGE.create_or_register_memory(
            {
                "name": "PUBLIC_DENIED_MEM",
                "agent_memory_type": "summary_memory",
                "create_new_physical_store": True,
                "dry_run": False,
            }
        )
        assert forbidden_live["ok"] is False
        assert forbidden_live["status_code"] == 403
        assert forbidden_live["error"]["code"] == "PERMISSION_DENIED"
    finally:
        LIVE_BRIDGE.active_role = prev_role


# ---------------------------------------------------------------------------
# Wave 2 Round 4 Tests (W2-R4-01 .. W2-R4-04)
# ---------------------------------------------------------------------------
def test_w2_r4_01_per_workflow_flow_id_context_isolation_and_sqlite_persistence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify W2-R4-01: Per-workflow (flow_id) context isolation, SQLite snowflake_flow_contexts persistence, and component dropdown filtering."""
    import sqlite3

    from lfx_snowflake.api_routes import SnowflakeAPIRouter
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge
    from lfx_snowflake.native_category_components.data_source.snowflake_tables import SnowflakeTablesComponent

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()

    # Configure isolated context for workflow Alpha (32-char hex normalized to hyphenated UUID)
    res_alpha = bridge.update_selected_context(
        {
            "database": "ALPHA_ANALYTICS_DB",
            "schema": "GOLD",
            "databases": ["ALPHA_ANALYTICS_DB"],
            "schemas_by_db": {"ALPHA_ANALYTICS_DB": ["GOLD", "SILVER"]},
            "objects_by_db_schema": {
                "ALPHA_ANALYTICS_DB.GOLD": {"tables": ["ALPHA_REVENUE_TBL", "ALPHA_CUSTOMERS_TBL"]}
            },
        },
        flow_id="flow-alpha-111",
    )
    assert res_alpha["ok"] is True
    assert res_alpha["flow_id"] == "flow-alpha-111"

    # Configure isolated context for workflow Beta
    res_beta = bridge.update_selected_context(
        {
            "database": "BETA_WAREHOUSE_DB",
            "schema": "RAW",
            "databases": ["BETA_WAREHOUSE_DB"],
            "schemas_by_db": {"BETA_WAREHOUSE_DB": ["RAW"]},
            "objects_by_db_schema": {
                "BETA_WAREHOUSE_DB.RAW": {"tables": ["BETA_EVENTS_STREAM_TBL"]}
            },
        },
        flow_id="flow-beta-222",
    )
    assert res_beta["ok"] is True
    assert res_beta["flow_id"] == "flow-beta-222"

    # Verify per-workflow dropdown helpers remain strictly isolated
    assert bridge.get_context_databases(flow_id="flow-alpha-111") == ["ALPHA_ANALYTICS_DB"]
    assert bridge.get_context_schemas("ALPHA_ANALYTICS_DB", flow_id="flow-alpha-111") == ["GOLD", "SILVER"]
    assert bridge.get_context_objects("ALPHA_ANALYTICS_DB", "GOLD", "tables", flow_id="flow-alpha-111") == [
        "ALPHA_REVENUE_TBL",
        "ALPHA_CUSTOMERS_TBL",
    ]

    assert bridge.get_context_databases(flow_id="flow-beta-222") == ["BETA_WAREHOUSE_DB"]
    assert bridge.get_context_schemas("BETA_WAREHOUSE_DB", flow_id="flow-beta-222") == ["RAW"]
    assert bridge.get_context_objects("BETA_WAREHOUSE_DB", "RAW", "tables", flow_id="flow-beta-222") == [
        "BETA_EVENTS_STREAM_TBL"
    ]

    # Verify SQLite persistence in snowflake_flow_contexts survives fresh bridge instantiation
    db_file = tmp_path / "langflow.db"
    assert db_file.exists()
    conn = sqlite3.connect(str(db_file))
    rows = conn.execute("SELECT flow_id, database_name, schema_name FROM snowflake_flow_contexts").fetchall()
    conn.close()
    saved_map = {r[0]: (r[1], r[2]) for r in rows}
    assert saved_map["flow-alpha-111"] == ("ALPHA_ANALYTICS_DB", "GOLD")
    assert saved_map["flow-beta-222"] == ("BETA_WAREHOUSE_DB", "RAW")

    rehydrated_bridge = LiveSnowflakeBridge()
    assert rehydrated_bridge.get_context_databases(flow_id="flow-alpha-111") == ["ALPHA_ANALYTICS_DB"]
    assert rehydrated_bridge.get_context_databases(flow_id="flow-beta-222") == ["BETA_WAREHOUSE_DB"]

    # Verify SnowflakeAPIRouter per-workflow context isolation
    router = SnowflakeAPIRouter()
    router.update_session_context({"databases": ["ROUTER_ALPHA_DB"]}, flow_id="flow-alpha-111")
    router.update_session_context({"databases": ["ROUTER_BETA_DB"]}, flow_id="flow-beta-222")
    assert router.get_session_context(flow_id="flow-alpha-111").body["selected_context"]["databases"] == [
        "ROUTER_ALPHA_DB"
    ]
    assert router.get_session_context(flow_id="flow-beta-222").body["selected_context"]["databases"] == [
        "ROUTER_BETA_DB"
    ]

    # Verify SnowflakeTablesComponent passes flow_id from component or build_config
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    LIVE_BRIDGE.update_selected_context(
        {
            "database": "ALPHA_ANALYTICS_DB",
            "schema": "GOLD",
            "databases": ["ALPHA_ANALYTICS_DB"],
            "schemas_by_db": {"ALPHA_ANALYTICS_DB": ["GOLD"]},
            "objects_by_db_schema": {"ALPHA_ANALYTICS_DB.GOLD": {"tables": ["ALPHA_REVENUE_TBL"]}},
        },
        flow_id="flow-alpha-111",
    )
    comp = SnowflakeTablesComponent()
    cfg = comp.update_build_config(
        {
            "_frontend_node_flow_id": "flow-alpha-111",
            "database": {"options": [], "value": "ALPHA_ANALYTICS_DB"},
            "schema_name": {"options": [], "value": "GOLD"},
            "table_name": {"options": [], "value": ""},
        },
        field_value="ALPHA_ANALYTICS_DB",
        field_name="database",
    )
    assert cfg["database"]["options"] == ["ALPHA_ANALYTICS_DB"]
    assert cfg["table_name"]["options"] == ["ALPHA_REVENUE_TBL"]


def test_w2_r4_02_durable_object_removed_from_context_audit_and_empty_collection_clear(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify W2-R4-02: Removing/deselecting an object, schema, or database (including the last item) emits OBJECT_REMOVED_FROM_CONTEXT to LANGFLOW_AUDIT_LOG with SQL quote escaping."""
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()
    executed_sql_statements: list[str] = []

    def _capture_sql(statement: str, **kwargs):
        executed_sql_statements.append(statement)
        return {"ok": True, "rows": [], "columns": []}

    monkeypatch.setattr(bridge, "execute_sql", _capture_sql)

    bridge.update_selected_context(
        {
            "database": "FINANCE_DB",
            "schema": "LEDGER",
            "databases": ["FINANCE_DB", "ARCHIVE_DB"],
            "schemas_by_db": {"FINANCE_DB": ["LEDGER", "STAGING"]},
            "objects_by_db_schema": {
                "FINANCE_DB.LEDGER": {"tables": ["GL_ENTRIES", "INVOICES"]}
            },
        },
        flow_id="flow-audit'test-01",
    )
    executed_sql_statements.clear()

    # 1. Remove one object via remove_object_from_context
    bridge.remove_object_from_context(
        object_name="INVOICES",
        category="tables",
        database="FINANCE_DB",
        schema="LEDGER",
        flow_id="flow-audit'test-01",
    )
    removal_audits = [s for s in executed_sql_statements if "OBJECT_REMOVED_FROM_CONTEXT" in s]
    assert len(removal_audits) == 1
    assert "INVOICES" in removal_audits[0]
    assert "'flow-audit''test-01'" in removal_audits[0]

    # 2. Deselect the LAST object (empty objects_by_db_schema: {}) and verify empty-collection fix clears it and audits
    executed_sql_statements.clear()
    res_empty = bridge.update_selected_context(
        {
            "database": "FINANCE_DB",
            "schema": "LEDGER",
            "databases": ["FINANCE_DB"],
            "schemas_by_db": {"FINANCE_DB": ["LEDGER"]},
            "objects_by_db_schema": {},
        },
        flow_id="flow-audit'test-01",
    )
    assert res_empty["selected_context"]["objects_by_db_schema"] == {}
    removal_audits_last = [s for s in executed_sql_statements if "OBJECT_REMOVED_FROM_CONTEXT" in s]
    assert len(removal_audits_last) == 1
    assert "ARCHIVE_DB" in removal_audits_last[0]
    assert "FINANCE_DB.STAGING" in removal_audits_last[0]
    assert "GL_ENTRIES" in removal_audits_last[0]


def test_w2_r4_03_automatic_sqlite_transaction_and_vertex_build_replication_and_middleware(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify W2-R4-03: sync_langflow_traces_to_snowflake replicates both SQLite transaction and vertex_build rows idempotently, and FastAPI middleware + 15s daemon are registered."""
    import sqlite3

    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge, register_snowflake_fastapi_routes

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    db_file = tmp_path / "langflow.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute(
        'CREATE TABLE "transaction" (id TEXT PRIMARY KEY, timestamp TEXT, vertex_id TEXT, inputs TEXT, outputs TEXT, status TEXT, flow_id TEXT)'
    )
    conn.execute(
        'CREATE TABLE "vertex_build" (build_id TEXT PRIMARY KEY, id TEXT, timestamp TEXT, flow_id TEXT, valid INTEGER, params TEXT, data TEXT, artifacts TEXT)'
    )
    conn.execute(
        'INSERT INTO "transaction" VALUES (?, ?, ?, ?, ?, ?, ?)',
        ("tx-001", "2026-10-17T10:00:00Z", "SnowflakeTables-1", '{"q":"1"}', '{"rows":5}', "SUCCESS", "flow-rep-1"),
    )
    conn.execute(
        'INSERT INTO "vertex_build" VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        ("vb-001", "CortexAgent-1", "2026-10-17T10:00:01Z", "flow-rep-1", 1, '{"model":"llama"}', '{"ok":true}', "{}"),
    )
    conn.commit()
    conn.close()

    bridge = LiveSnowflakeBridge()
    executed_merges: list[str] = []

    def _mock_sql(statement: str, **kwargs):
        if statement.strip().upper().startswith("MERGE INTO"):
            executed_merges.append(statement)
        return {"ok": True, "rows": [], "columns": []}

    monkeypatch.setattr(bridge, "execute_sql", _mock_sql)

    sync1 = bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
    assert sync1["ok"] is True
    assert sync1["replicated_runs"] == 1
    assert sync1["replicated_spans"] == 2  # 1 from transaction + 1 from vertex_build
    assert sync1["auto_sync_enabled"] is True
    assert any("LANGFLOW_FLOW_RUNS" in m for m in executed_merges)
    assert any("vb-vb-001" in m for m in executed_merges)

    # Idempotency check: second sync without new rows replicates 0 new runs/spans
    executed_merges.clear()
    sync2 = bridge.sync_langflow_traces_to_snowflake(limit=25, replicate_sqlite=True)
    assert sync2["replicated_runs"] == 0
    assert sync2["replicated_spans"] == 0
    assert len(executed_merges) == 0


def test_w2_r4_04_context_only_hub_ui_preserved_concept_module_and_in_place_tree_checkboxes() -> None:
    """Verify W2-R4-04: Context-only Snowflake Hub UI, preserved_provisioning_and_memories_concept.js module, workflow-scoped topbar pill lifecycle, and updateTreeCheckboxesInPlace."""
    preserved_js_path = REPO_ROOT / "frontend_patches" / "preserved_provisioning_and_memories_concept.js"
    assert preserved_js_path.exists()
    preserved_js = preserved_js_path.read_text(encoding="utf-8")
    assert "SNOWFLAKE_PRESERVED_PROVISIONING_AND_MEMORIES_CONCEPT" in preserved_js
    assert "mountNewMemoryWizardModal" in preserved_js
    assert "/api/v1/snowflake/provision" in preserved_js
    assert "/api/v1/snowflake/memories" in preserved_js

    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert 'data-hub-mode="context-only"' in ui_js
    assert "sflf-auto-sync-status-badge" in ui_js
    assert "sflf-preserved-sections-compat" in ui_js
    assert "function getActiveWorkflowFlowId()" in ui_js
    assert "function updateTreeCheckboxesInPlace(" in ui_js
    assert "syncCanvasNodesWithSnowflakeContext(sfState)" in ui_js

    hooks_py = (REPO_ROOT / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")
    assert "?v=sflf046" in hooks_py
    assert "preserved_provisioning_and_memories_concept.js" in hooks_py


def test_w2_r4_05_verifier_uuid_normalization_and_role_scoped_workflow_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify W2-R4-05 (Verifier): 32-char hex vs 36-char hyphenated UUID normalization on flow_id and legacy global context non-bleeding."""
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    bridge = LiveSnowflakeBridge()

    hex_id = "11223344556677889900aabbccddeeff"
    hyphen_id = "11223344-5566-7788-9900-aabbccddeeff"
    assert bridge._normalize_flow_id(hex_id) == hyphen_id
    assert bridge._normalize_flow_id(hyphen_id) == hyphen_id

    # Save using 32-char hex flow_id and retrieve using 36-char hyphenated UUID
    bridge.update_selected_context(
        {
            "database": "UUID_NORM_DB",
            "schema": "PUBLIC",
            "databases": ["UUID_NORM_DB"],
            "schemas_by_db": {"UUID_NORM_DB": ["PUBLIC"]},
            "objects_by_db_schema": {"UUID_NORM_DB.PUBLIC": {"tables": ["NORM_TBL"]}},
        },
        flow_id=hex_id,
    )
    assert bridge.get_context_databases(flow_id=hyphen_id) == ["UUID_NORM_DB"]
    assert bridge.get_context_objects("UUID_NORM_DB", "PUBLIC", "tables", flow_id=hyphen_id) == ["NORM_TBL"]

    # Legacy global call (flow_id=None) updates global context without overwriting hyphen_id's workflow context
    bridge.update_selected_context(
        {
            "database": "GLOBAL_FALLBACK_DB",
            "schema": "CORE",
            "databases": ["GLOBAL_FALLBACK_DB"],
            "schemas_by_db": {"GLOBAL_FALLBACK_DB": ["CORE"]},
            "objects_by_db_schema": {"GLOBAL_FALLBACK_DB.CORE": {"tables": ["GLOBAL_TBL"]}},
        },
        flow_id=None,
    )
    assert bridge.get_context_databases(flow_id=None) == ["GLOBAL_FALLBACK_DB"]
    assert bridge.get_context_databases(flow_id=hyphen_id) == ["UUID_NORM_DB"]


def test_proxy_components_ast_top_level_imports_and_origin_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that all 26 native category components avoid module-level ast.Try imports and resolve LLM_PROXY_BASE_URL."""
    import ast
    import builtins

    comp_root = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components"
    for py_file in sorted(comp_root.rglob("*.py")):
        if py_file.name == "__init__.py":
            continue
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        top_try_nodes = [node for node in tree.body if isinstance(node, ast.Try)]
        assert not top_try_nodes, (
            f"{py_file.name} contains module-level try/except which Langflow's AST component loader strips"
        )

    assert hasattr(builtins, "standard_langflow")

    monkeypatch.setenv("LLM_PROXY_BASE_URL", "http://proxy-unit-test.internal:8080")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama-unit-test.internal:11434")
    standard_langflow.BASES["chat"] = "http://<PROXY_SERVICE>.<DNS_NAMESPACE>:8080/chat/v1"
    standard_langflow.BASES["ollama"] = "http://<OLLAMA_SERVICE>.<DNS_NAMESPACE>:11434/v1"

    assert standard_langflow._get_base("chat") == "http://proxy-unit-test.internal:8080/chat/v1"
    assert standard_langflow._get_base("responses") == "http://proxy-unit-test.internal:8080/responses/v1"
    assert standard_langflow._get_base("messages") == "http://proxy-unit-test.internal:8080/messages"
    assert standard_langflow._get_base("ollama") == "http://ollama-unit-test.internal:11434/v1"


def test_w2_r4_06b_and_ctx_arch_05_data_sources_purity_and_account_level_postgres() -> None:
    """Verify W2-R4-06B & CTX-ARCH-05: Data Sources exclude technical memory tables and SnowflakePostgres starts at account-level postgres_instance."""
    from lfx_snowflake.components.data_sources import (
        SnowflakeDataSourcesComponent,
        filter_business_data_source_objects,
        is_technical_memory_table,
    )
    from lfx_snowflake.native_category_components.data_source.snowflake_postgres import SnowflakePostgresComponent
    from lfx_snowflake.native_category_components.data_source.snowflake_tables import SnowflakeTablesComponent

    assert is_technical_memory_table("MEM_GRAPH_STATE_CHECKPOINTS") is True
    assert is_technical_memory_table("LANGFLOW_MEM_SUPPORT_TICKETS") is True
    assert is_technical_memory_table("CUSTOMERS") is False

    filtered = filter_business_data_source_objects(
        ["ORDERS", "MEM_CHAT_WINDOW_HISTORY", "CUSTOMERS", "LANGFLOW_MEM_VEC_01"],
        category="tables",
    )
    assert filtered == ["ORDERS", "CUSTOMERS"]

    ds_comp = SnowflakeDataSourcesComponent()
    cat_entries = ds_comp.browse_catalog(
        active_role="ANALYST_ROLE",
        available_objects=[
            {"object_type": "STANDARD_TABLE", "object_fqn": "<DATABASE>.<SCHEMA>.ORDERS", "allowed_roles": ["ANALYST_ROLE"]},
            {"object_type": "STANDARD_TABLE", "object_fqn": "<DATABASE>.<SCHEMA>.MEM_RUNNING_SUMMARIES", "allowed_roles": ["ANALYST_ROLE"]},
        ],
    )
    assert len(cat_entries) == 1
    assert cat_entries[0]["object_fqn"] == "<DATABASE>.<SCHEMA>.ORDERS"

    # Data Sources components must not expose memory_catalog_ref input
    tbl_input_names = [inp.name for inp in SnowflakeTablesComponent.inputs]
    pg_input_names = [inp.name for inp in SnowflakePostgresComponent.inputs]
    assert "memory_catalog_ref" not in tbl_input_names
    assert "memory_catalog_ref" not in pg_input_names
    assert pg_input_names[0] == "postgres_instance"







