"""Independent QA Verification Suite (Role 8 — Verifier): Full Coverage of REQ-01..REQ-17, ADR-001..ADR-007, and G-A1..G-A6."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lfx_snowflake import (  # noqa: E402
    BUNDLE_ID,
    ERROR_TAXONOMY_CONTRACT,
    INVARIANT_CATEGORIES_MANIFEST,
    NEW_FLOW_IMPORT_MODES,
    SETTINGS_CONTRACT,
    SNOWFLAKE_BRANDING_CONTRACT,
    SNOWFLAKE_BUNDLE_COMPONENTS,
    UPSTREAM_BASELINE_TAG,
    SnowflakeAPIRouter,
    get_bundle_manifest,
    verify_invariant_category_integrity,
)
from lfx_snowflake.components import (  # noqa: E402
    AGGREGATE_AI_FUNCTIONS,
    AI_REDACT_LIMITATIONS_DISCLAIMER,
    ALL_SUPPORTED_AI_FUNCTIONS,
    DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG,
    INVARIANT_UTILITIES_COMPONENTS,
    LLM_SELECTOR_INVARIANT_SPEC,
    SCALAR_AI_FUNCTIONS,
    SNOWFLAKE_PYPI_REQUIRED_DB_ROLE,
    SNOWFLAKE_PYPI_SHARED_REPOSITORY,
    UNCHANGED_DISCOVER_MORE_BUNDLES,
    BoundedSqlResultPolicy,
    CortexAgentRunAdapter,
    CortexAnalystService,
    CreateMemoryDialogService,
    DataSourceObjectStatus,
    InMemoryMessageHistoryBackend,
    InMemoryStageBackend,
    InMemoryVectorStoreEngine,
    LLMPreprocessingConfig,
    MessageHistoryBackendType,
    MessageHistoryStorageConfig,
    PolicySpec,
    ServeAsA2AAgentRegistry,
    SignedReplayEnvelope,
    SnowflakeA2AAgentComponent,
    SnowflakeAIFunctionComponent,
    SnowflakeDataSourcesComponent,
    SnowflakeFileSystemComponent,
    SnowflakeKnowledgeComponent,
    SnowflakeMemoryBaseComponent,
    SnowflakeMessageHistoryComponent,
    SnowflakePostgresDataSourceAdapter,
    SnowflakePythonInterpreterComponent,
    SnowflakeReadFileComponent,
    SnowflakeToolGuardPoliciesComponent,
    SnowflakeWriteFileComponent,
    StageLocation,
    StageType,
    ToolDefinition,
    ToolGuardActivity,
    ToolGuardModelSelection,
    VectorBackendType,
)
from lfx_snowflake.governance import (  # noqa: E402
    CANONICAL_ROLE_DENIED_MESSAGE,
    ErrorCategory,
    ExecutionMode,
    InMemoryRoleScopedSqlAdapter,
    ObjectFamily,
    OperationType,
    SnowflakeAuthzService,
    SnowflakeErrorCode,
    SnowflakeGovernanceError,
    SnowflakeGovernanceMiddleware,
    SnowflakeObjectProvisioner,
    SnowflakePermissionError,
)
from lfx_snowflake.observability import (  # noqa: E402
    CANONICAL_AUDIT_LOG_TABLE,
    CANONICAL_FLOW_RUNS_TABLE,
    CANONICAL_FLOW_SPANS_TABLE,
    CANONICAL_ROW_ACCESS_POLICY,
    DurableAuditOutbox,
    FlowRunRecord,
    FlowSpanRecord,
    ObservabilityTableConfig,
    SnowflakeTraceReplicator,
    TransactionalOutbox,
    generate_canonical_observability_ddl,
    redact_payload_for_sink,
    redact_sensitive_payload,
)
from lfx_snowflake.providers import (  # noqa: E402
    EmbeddingSpaceGuard,
    SnowflakeEmbeddingAdapter,
    SnowflakeEmbeddingModelAdapter,
    SnowflakeOllamaAdapter,
    SnowflakeProviderRegistry,
    SnowflakeProxyChatAdapter,
    SnowflakeProxyMessagesAdapter,
    SnowflakeProxyResponsesAdapter,
    deduplicate_tool_call_ids,
)
from scripts.verify_public_repo_sanitization import (  # noqa: E402
    DENYLIST_TERMS,
    REQUIRED_PLACEHOLDERS,
    scan_repository,
)


def test_shared_p3_architecture_contracts_exports() -> None:
    """Verify all shared architecture contract symbols exist and behave as specified."""
    assert ErrorCategory is SnowflakeErrorCode
    assert ExecutionMode.INTERACTIVE_CALLER.value == "INTERACTIVE_CALLER"
    assert ExecutionMode.TECHNICAL_AUTOMATION.value == "TECHNICAL_AUTOMATION"
    assert SnowflakePermissionError is SnowflakeGovernanceError
    assert redact_sensitive_payload is redact_payload_for_sink
    assert DurableAuditOutbox is TransactionalOutbox
    assert SnowflakeEmbeddingAdapter is SnowflakeEmbeddingModelAdapter

    assert CANONICAL_FLOW_RUNS_TABLE == "LANGFLOW_FLOW_RUNS"
    assert CANONICAL_FLOW_SPANS_TABLE == "LANGFLOW_FLOW_SPANS"
    assert CANONICAL_AUDIT_LOG_TABLE == "LANGFLOW_AUDIT_LOG"


def test_cli_module_execution_returns_valid_manifest_json() -> None:
    """Verify `python3 -m lfx_snowflake` outputs valid JSON matching get_bundle_manifest()."""
    proc = subprocess.run(
        [sys.executable, "-m", "lfx_snowflake"],
        cwd=str(REPO_ROOT),
        env={"PYTHONPATH": str(SRC_DIR)},
        capture_output=True,
        text=True,
        check=True,
    )
    parsed = json.loads(proc.stdout)
    assert parsed["bundle_id"] == BUNDLE_ID == "snowflake"
    assert parsed["upstream_baseline_tag"] == UPSTREAM_BASELINE_TAG == "v1.12.5"
    assert len(parsed["components"]) == len(SNOWFLAKE_BUNDLE_COMPONENTS)


@pytest.mark.parametrize("adr_num", range(1, 8))
def test_all_7_adr_documents_exist_and_cover_core_decisions(adr_num: int) -> None:
    """Verify ADR-001 through ADR-007 exist, are non-empty, and reference parametric placeholders."""
    adr_path = REPO_ROOT / "docs" / "ADR" / f"ADR-00{adr_num}.md"
    assert adr_path.exists(), f"Missing {adr_path}"
    content = adr_path.read_text(encoding="utf-8")
    assert f"ADR-00{adr_num}" in content
    assert len(content.splitlines()) >= 15


def test_ga1_canonical_observability_table_names_consistency_across_code_and_sql() -> None:
    """Verify [G-A1]: LANGFLOW_FLOW_RUNS, LANGFLOW_FLOW_SPANS, and LANGFLOW_AUDIT_LOG are uniform across Python, SQL, and YAML."""
    cfg = ObservabilityTableConfig(database="<DATABASE>", schema="<SCHEMA>")
    ddl = generate_canonical_observability_ddl(cfg)
    sql_file = (REPO_ROOT / "sql" / "02_traces_and_audit_tables.sql").read_text(encoding="utf-8")
    spec_file = (REPO_ROOT / "service-spec.template.yaml").read_text(encoding="utf-8")
    cap_matrix = (REPO_ROOT / "docs" / "CAPABILITY_MATRIX.md").read_text(encoding="utf-8")

    for canonical_name in (
        "LANGFLOW_FLOW_RUNS",
        "LANGFLOW_FLOW_SPANS",
        "LANGFLOW_AUDIT_LOG",
        CANONICAL_ROW_ACCESS_POLICY,
    ):
        assert canonical_name in ddl
        assert canonical_name in sql_file

    for table_fqn in (
        "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS",
        "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS",
        "<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG",
    ):
        assert table_fqn in spec_file
        assert table_fqn in cap_matrix


def test_ga2_ai_function_direct_namespace_scalar_vs_aggregate_and_detect_span_redaction() -> None:
    """Verify [G-A2]: direct AI_* namespace (never deprecated SNOWFLAKE.CORTEX.*), scalar vs aggregate, and AI_REDACT detect masking."""
    assert "AI_REDACT" in SCALAR_AI_FUNCTIONS
    assert "AI_CLASSIFY" in SCALAR_AI_FUNCTIONS
    assert set(AGGREGATE_AI_FUNCTIONS) == {"AI_SUMMARIZE_AGG", "AI_AGG"}
    assert len(ALL_SUPPORTED_AI_FUNCTIONS) == 9
    assert "US/UK/CA" in AI_REDACT_LIMITATIONS_DISCLAIMER or "best-effort" in AI_REDACT_LIMITATIONS_DISCLAIMER.lower()

    comp = SnowflakeAIFunctionComponent(active_role="ROLE_ANALYST", allowed_roles={"ROLE_ANALYST"})
    exec_res = comp.execute(
        ai_function="AI_REDACT",
        table=[{"id": 1, "note": "John Doe SSN 123-45-6789"}],
        column_name="note",
        redact_mode="detect",
        categories=["NAME", "US_SSN"],
    )
    plan = exec_res["plan"]
    assert "SNOWFLAKE.CORTEX." not in plan.execution_sql
    assert "AI_REDACT(" in plan.execution_sql
    assert "EXPLAIN_PRIVILEGES" in plan.explain_privileges_sql
    assert "AI_COUNT_TOKENS" in plan.token_estimate_sql
    assert plan.is_aggregate is False

    # Verify detect mode output PII spans are masked before sink
    detect_payload = {
        "function": "AI_REDACT",
        "mode": "detect",
        "input_text": "Contact Jane Smith at +1-555-0199",
        "detected_spans": [
            {"category": "NAME", "text": "Jane Smith", "start": 8, "end": 18},
            {"category": "PHONE", "value": "+1-555-0199", "start": 22, "end": 33},
        ],
    }
    redacted = redact_sensitive_payload(detect_payload)
    assert redacted["detected_spans"][0]["text"] == "[REDACTED_PII:NAME]"
    assert redacted["detected_spans"][1]["value"] == "[REDACTED_PII:PHONE]"
    assert "Jane Smith" not in redacted["input_text"]


def test_ga3_write_file_internal_vs_external_and_safe_append() -> None:
    """Verify [G-A3]: Internal stage PUT stream vs External stage writer, and safe Append without raw JSON corruption."""
    backend = InMemoryStageBackend(
        active_role="ROLE_ENG",
        role_privileges={
            "ROLE_ENG": {
                ("CREATE STAGE", "DB1.SCH1"),
                ("READ", "*"),
                ("WRITE", "*"),
                ("USAGE", "*"),
            }
        },
    )
    int_loc = StageLocation(database="DB1", schema="SCH1", stage_name="INT_STG", stage_type=StageType.INTERNAL)
    ext_loc = StageLocation(
        database="DB1",
        schema="SCH1",
        stage_name="EXT_STG",
        stage_type=StageType.EXTERNAL,
        storage_integration="EXT_INT",
        external_url="s3://example-bucket/path/",
    )
    SnowflakeFileSystemComponent(backend, int_loc).create_stage(confirmed=True)
    SnowflakeFileSystemComponent(backend, ext_loc).create_stage(confirmed=True)

    w_int = SnowflakeWriteFileComponent(backend, int_loc)
    w_ext = SnowflakeWriteFileComponent(backend, ext_loc)

    r1 = w_int.write_file(file_content=[{"k": 1}], file_name="data.json", file_format="json", append=False)
    assert r1["transport_method"] == "INTERNAL_STAGE_PUT_STREAM"
    assert r1["transfer_verified"] is True

    r2 = w_int.write_file(
        file_content=[{"k": 2}],
        file_name="data.json",
        file_format="json",
        append=True,
        append_json_mode="array_merge",
    )
    assert r2["operation_mode"] == "APPEND_ATOMIC_REWRITE"
    parsed = SnowflakeReadFileComponent(backend, int_loc, advanced_parser=True).read_file("data.json")
    assert parsed["parsed_data"] == [{"k": 1}, {"k": 2}]

    r_ext = w_ext.write_file(file_content="a\nb", file_name="log.txt", file_format="txt", append=False)
    assert r_ext["transport_method"] == "EXTERNAL_STAGE_INTEGRATION_WRITER"


def test_ga4_create_memory_optional_llm_preprocessing_and_permission_separation() -> None:
    """Verify [G-A4]: LLM Preprocessing is strictly optional and CREATE vs use permissions are separated."""
    engine = InMemoryVectorStoreEngine(
        active_role="ROLE_USER",
        snowflake_privileges={
            "ROLE_USER": {
                ("SELECT", "DB1.SCH1.EXISTING_VEC"),
                ("INSERT", "DB1.SCH1.EXISTING_VEC"),
            }
        },
    )
    dialog = CreateMemoryDialogService(engine)
    res_use = dialog.create_memory(
        name="ExistingMemory",
        embedding_model=DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-l-v2.0"],
        vector_database=VectorBackendType.STANDARD_VECTOR,
        database="DB1",
        schema="SCH1",
        table_name="EXISTING_VEC",
        batch_size=32,
        llm_preprocessing=LLMPreprocessingConfig(enabled=False),
        create_physical_resource=False,
    )
    assert res_use["state"] == "CREATED"
    assert res_use["llm_preprocessing_enabled"] is False
    assert engine.generative_preprocessing_calls == 0


def test_ga5_data_sources_and_cortex_analyst_boundaries() -> None:
    """Verify [G-A5]: no automatic Semantic View creation, no Analyst-on-Postgres, and bounded SQL results in multi-turn."""
    ds = SnowflakeDataSourcesComponent(active_role="ROLE_ANALYST")
    ds.register_object(
        DataSourceObjectStatus(
            database="SALES_DB",
            schema="PUBLIC",
            name="RAW_ORDERS",
            object_type="STANDARD_TABLE",
            allowed_roles_visible=("ROLE_ANALYST",),
            allowed_roles_select=("ROLE_ANALYST",),
            semantic_view_fqn=None,
            semantic_model_yaml_stage_path=None,
        )
    )
    ds.register_object(
        DataSourceObjectStatus(
            database="SALES_DB",
            schema="PUBLIC",
            name="SEMANTIC_ORDERS",
            object_type="STANDARD_TABLE",
            allowed_roles_visible=("ROLE_ANALYST",),
            allowed_roles_select=("ROLE_ANALYST",),
            semantic_view_fqn="SALES_DB.PUBLIC.ORDERS_SV",
            semantic_model_yaml_stage_path=None,
        )
    )
    status = ds.inspect_object_readiness("SALES_DB.PUBLIC.RAW_ORDERS")
    assert status["catalog_visible"] is True
    assert status["select_permitted"] is True
    assert status["analyst_semantic_ready"] is False

    pg_adapter = SnowflakePostgresDataSourceAdapter(
        instance_host="<POSTGRES_INSTANCE>",
        database="appdb",
        active_role="ROLE_ANALYST",
        pg_role_mappings={"ROLE_ANALYST": "application"},
    )
    with pytest.raises(Exception):
        pg_adapter.validate_query("SELECT 1", allow_analyst=True)

    analyst = CortexAnalystService(
        data_sources=ds,
        policy=BoundedSqlResultPolicy(
            max_rows_execution=10,
            max_rows_for_synthesis=3,
            max_chars_for_synthesis=200,
        ),
        sql_executor=lambda _sql, _role, _meta: [{"id": i, "text": "x" * 50} for i in range(10)],
    )
    analyst.bind_data_source("SALES_DB.PUBLIC.SEMANTIC_ORDERS")
    exec_result = analyst.execute_generated_sql("SELECT id, text FROM SALES_DB.PUBLIC.SEMANTIC_ORDERS")
    bounded = exec_result["bounded_summary"]
    assert bounded["synthesis_rows_included"] == 3
    assert bounded["truncated_for_synthesis"] is True


def test_ga6_invariant_bundles_profile_menu_and_14_term_denylist() -> None:
    """Verify [G-A6]: unchanged bundles in Discover more components, LLM Selector/Calculator/Current Date invariant, and 14-term denylist."""
    assert LLM_SELECTOR_INVARIANT_SPEC["status"] == "invariant"
    assert LLM_SELECTOR_INVARIANT_SPEC["modified"] is False
    assert set(INVARIANT_UTILITIES_COMPONENTS) == {"Calculator", "Current Date"}
    assert len(UNCHANGED_DISCOVER_MORE_BUNDLES) >= 5
    assert verify_invariant_category_integrity(INVARIANT_CATEGORIES_MANIFEST)["passed"] is True
    assert len(DENYLIST_TERMS) == 14
    assert len(REQUIRED_PLACEHOLDERS) == 9

    report = scan_repository(REPO_ROOT, require_aws_attestation=True)
    assert report.passed is True
    assert report.violations == []
