"""Comprehensive test suite for Step I-5 (Ruolo 3 Parte B):
- `files_and_stage.py` (`File System`, `Read File`, `Write File`, `[G-A3]`)
- `knowledge_and_memories.py` (`Knowledge`, `Memory Base`, `Create Memory`, `[G-A4]`)
- `message_history.py` (`Message History` on Snowflake Postgres & Standard Tables, signed replay branching)
- `python_interpreter_pypi.py` (`Python Interpreter` on `snowflake.snowpark.pypi_shared_repository` without public PyPI fallback)
- `toolguard_policies.py` (`ToolGuard / Policies` ALTK fail-closed & other bundles invariant, `[G-A6]`)
- `a2a_and_cortex_agent.py` (`A2A Agent` client + `Cortex Agent Run API` adapter & `Serve as an A2A agent` server)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))

from lfx_snowflake.components.a2a_and_cortex_agent import (
    A2AAgentCard,
    A2APublicationStatus,
    A2ATargetType,
    CortexAgentRunAdapter,
    ServeAsA2AAgentRegistry,
    SnowflakeA2AAgentComponent,
)
from lfx_snowflake.components.files_and_stage import (
    ErrorCode,
    InMemoryStageBackend,
    SnowflakeComponentError,
    SnowflakeFileSystemComponent,
    SnowflakeReadFileComponent,
    SnowflakeWriteFileComponent,
    StageLocation,
    StageType,
    normalize_and_validate_subpath,
)
from lfx_snowflake.components.knowledge_and_memories import (
    CortexSearchEmbeddingMode,
    CreateMemoryDialogService,
    DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG,
    DialogState,
    EmbeddingModelSpec,
    InMemoryVectorStoreEngine,
    LLMPreprocessingConfig,
    SnowflakeKnowledgeComponent,
    SnowflakeMemoryBaseComponent,
    VectorBackendType,
)
from lfx_snowflake.components.message_history import (
    CortexThreadReference,
    InMemoryMessageHistoryBackend,
    MessageHistoryBackendType,
    MessageHistoryStorageConfig,
    SignedReplayEnvelope,
    SnowflakeMessageHistoryComponent,
)
from lfx_snowflake.components.python_interpreter_pypi import (
    INVARIANT_UTILITIES_COMPONENTS,
    SNOWFLAKE_PYPI_REQUIRED_DB_ROLE,
    SNOWFLAKE_PYPI_SHARED_REPOSITORY,
    SnowflakePythonInterpreterComponent,
)
from lfx_snowflake.components.toolguard_policies import (
    SNOWFLAKE_TOOLGUARD_PROVIDERS,
    UNCHANGED_DISCOVER_MORE_BUNDLES,
    PolicySpec,
    SnowflakeToolGuardPoliciesComponent,
    ToolDefinition,
    ToolGuardActivity,
    ToolGuardModelSelection,
)


# ============================================================================
# 1. FILES & STAGE (`File System`, `Read File`, `Write File`, `[G-A3]`)
# ============================================================================


def test_file_system_stage_creation_preview_and_confirmed_ownership_and_denial() -> None:
    backend = InMemoryStageBackend(
        active_role="ROLE_STAGE_ADMIN",
        role_privileges={
            "ROLE_STAGE_ADMIN": {("CREATE STAGE", "APP_DB.APP_SCHEMA")},
            "ROLE_READONLY": set(),
        },
    )
    loc = StageLocation(
        database="app_db",
        schema="app_schema",
        stage_name="langflow_files_stg",
        stage_type=StageType.INTERNAL,
        workspace_subpath="project_a/docs",
    )
    fs = SnowflakeFileSystemComponent(backend, loc, tool_mode=True)

    # Dry-run preview first
    preview = fs.create_stage(confirmed=False)
    assert preview["status"] == "DRY_RUN_PREVIEW"
    assert "CREATE STAGE APP_DB.APP_SCHEMA.LANGFLOW_FILES_STG" in preview["ddl"]
    assert not backend.stage_exists(loc.fqn)

    # Confirmed creation grants ownership/access to active role
    created = fs.create_stage(confirmed=True)
    assert created["status"] == "CREATED"
    assert backend.stage_exists(loc.fqn)
    assert fs.resolve_stage_uri("report.txt") == "@APP_DB.APP_SCHEMA.LANGFLOW_FILES_STG/project_a/docs/report.txt"

    # Duplicate creation without silent OR REPLACE is blocked
    with pytest.raises(SnowflakeComponentError) as dup_exc:
        fs.create_stage(confirmed=True)
    assert dup_exc.value.http_status == 409

    # Role without CREATE STAGE gets HTTP 403 AUTHZ_ROLE_DENIED + audit entry
    backend.active_role = "ROLE_READONLY"
    loc2 = StageLocation(database="app_db", schema="app_schema", stage_name="unauth_stg")
    fs2 = SnowflakeFileSystemComponent(backend, loc2)
    with pytest.raises(SnowflakeComponentError) as deny_exc:
        fs2.create_stage(confirmed=True)
    assert deny_exc.value.code == ErrorCode.AUTHZ_ROLE_DENIED
    assert deny_exc.value.http_status == 403
    assert "Il ruolo attivo (ROLE_READONLY) non consente questa operazione" in str(deny_exc.value)
    assert backend.audit_log[-1]["event_type"] == "AUTHZ_ROLE_DENIED"


def test_file_system_subpath_traversal_blocked_in_tool_mode() -> None:
    with pytest.raises(SnowflakeComponentError) as exc_info:
        normalize_and_validate_subpath("tenant_a", "../tenant_b/secrets.json")
    assert exc_info.value.code == ErrorCode.AUTHZ_ROLE_DENIED
    assert exc_info.value.http_status == 403


def test_write_and_read_file_internal_vs_external_and_atomic_append_semantics() -> None:
    backend = InMemoryStageBackend(
        active_role="ROLE_WRITER",
        role_privileges={
            "ROLE_WRITER": {
                ("CREATE STAGE", "APP_DB.APP_SCHEMA"),
                ("READ", "*"),
                ("WRITE", "*"),
                ("USAGE", "*"),
            }
        },
    )
    internal_loc = StageLocation(
        database="APP_DB",
        schema="APP_SCHEMA",
        stage_name="INTERNAL_STG",
        stage_type=StageType.INTERNAL,
        workspace_subpath="runs",
    )
    external_loc = StageLocation(
        database="APP_DB",
        schema="APP_SCHEMA",
        stage_name="EXTERNAL_STG",
        stage_type=StageType.EXTERNAL,
        workspace_subpath="exports",
        storage_integration="S3_INT",
        external_url="s3://bucket-placeholder/prefix/",
    )
    SnowflakeFileSystemComponent(backend, internal_loc).create_stage(confirmed=True)
    SnowflakeFileSystemComponent(backend, external_loc).create_stage(confirmed=True)

    int_writer = SnowflakeWriteFileComponent(backend, internal_loc)
    ext_writer = SnowflakeWriteFileComponent(backend, external_loc)

    # 1. Internal stage write uses PUT stream; external stage uses EXTERNAL_STAGE_INTEGRATION_WRITER
    res_int = int_writer.write_file(file_content="line1", file_name="notes.txt", file_format="txt", append=False)
    assert res_int["transport_method"] == "INTERNAL_STAGE_PUT_STREAM"
    assert res_int["file_path"] == "@APP_DB.APP_SCHEMA.INTERNAL_STG/runs/notes.txt"
    assert res_int["transfer_verified"] is True

    res_ext = ext_writer.write_file(
        file_content="col1,col2\n1,a\n",
        file_name="metrics.csv",
        file_format="csv",
        append=False,
    )
    assert res_ext["transport_method"] == "EXTERNAL_STAGE_INTEGRATION_WRITER"
    assert res_ext["file_path"] == "@APP_DB.APP_SCHEMA.EXTERNAL_STG/exports/metrics.csv"

    # 2. Writing again with append=False and overwrite=False is blocked (never silent overwrite!)
    with pytest.raises(SnowflakeComponentError) as ow_exc:
        int_writer.write_file(file_content="line2", file_name="notes.txt", file_format="txt", append=False)
    assert ow_exc.value.http_status == 409

    # 3. Atomic Append on txt and header-deduplicating Append on csv
    res_append_txt = int_writer.write_file(file_content="line2", file_name="notes.txt", file_format="txt", append=True)
    assert res_append_txt["operation_mode"] == "APPEND_ATOMIC_REWRITE"
    reader = SnowflakeReadFileComponent(backend, internal_loc, advanced_parser=True)
    read_back = reader.read_file("notes.txt")
    assert read_back["raw_content"] == "line1\nline2"
    assert read_back["used_ai_functions"] is False

    ext_writer.write_file(
        file_content="col1,col2\n2,b\n",
        file_name="metrics.csv",
        file_format="csv",
        append=True,
    )
    csv_read = SnowflakeReadFileComponent(backend, external_loc, advanced_parser=True).read_file("metrics.csv")
    assert csv_read["parsed_data"] == [{"col1": "1", "col2": "a"}, {"col1": "2", "col2": "b"}]

    # 4. JSON Append: raw text append on JSON is blocked (`[G-A3]`), array_merge preserves valid JSON
    int_writer.write_file(file_content=[{"id": 1}], file_name="items.json", file_format="json", append=False)
    with pytest.raises(SnowflakeComponentError) as json_raw_exc:
        int_writer.write_file(
            file_content=[{"id": 2}],
            file_name="items.json",
            file_format="json",
            append=True,
            append_json_mode="forbid_raw",
        )
    assert json_raw_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY

    int_writer.write_file(
        file_content=[{"id": 2}],
        file_name="items.json",
        file_format="json",
        append=True,
        append_json_mode="array_merge",
    )
    json_read = reader.read_file("items.json")
    assert json.loads(json_read["raw_content"]) == [{"id": 1}, {"id": 2}]


def test_write_file_transfer_verification_failure_raises_error() -> None:
    backend = InMemoryStageBackend(
        active_role="ROLE_WRITER",
        role_privileges={"ROLE_WRITER": {("CREATE STAGE", "APP_DB.APP_SCHEMA"), ("WRITE", "*"), ("READ", "*")}},
        simulate_incomplete_transfer=True,
    )
    loc = StageLocation(database="APP_DB", schema="APP_SCHEMA", stage_name="BROKEN_STG")
    SnowflakeFileSystemComponent(backend, loc).create_stage(confirmed=True)
    writer = SnowflakeWriteFileComponent(backend, loc)

    with pytest.raises(SnowflakeComponentError) as exc_info:
        writer.write_file(file_content="important payload", file_name="data.txt", file_format="txt")
    assert exc_info.value.code == ErrorCode.TECHNICAL_EXECUTION_ERROR
    assert exc_info.value.http_status == 502


# ============================================================================
# 2. KNOWLEDGE, MEMORY BASE & CREATE MEMORY (`REQ-10`, `[G-A4]`)
# ============================================================================


def test_create_memory_and_4_vector_backends_with_permission_separation_and_isolation() -> None:
    engine = InMemoryVectorStoreEngine(
        active_role="ROLE_CREATOR",
        snowflake_privileges={
            "ROLE_CREATOR": {
                ("CREATE TABLE", "MEM_DB.MEM_SCH"),
                ("CREATE HYBRID TABLE", "MEM_DB.MEM_SCH"),
                ("CREATE CORTEX SEARCH SERVICE", "MEM_DB.MEM_SCH"),
                ("USAGE", "PG_INST_MAIN"),
                ("SELECT", "*"),
                ("INSERT", "*"),
                ("USAGE", "*"),
            },
            # ROLE_USER has NO CREATE privileges, only SELECT/INSERT/USAGE on existing stores!
            "ROLE_USER": {
                ("SELECT", "MEM_DB.MEM_SCH.STD_VEC_TBL"),
                ("INSERT", "MEM_DB.MEM_SCH.STD_VEC_TBL"),
                ("USAGE", "MEM_DB.MEM_SCH.CS_SVC"),
                ("INSERT", "MEM_DB.MEM_SCH.CS_SRC_TBL"),
                ("USAGE", "PG_INST_MAIN"),
            },
            "ROLE_UNAUTHORIZED": set(),
        },
        postgres_privileges={
            "pg_creator": {("CREATE", "MEM_DB.MEM_SCH"), ("SELECT", "*"), ("INSERT", "*")},
            "pg_user": {("SELECT", "MEM_DB.MEM_SCH.PG_VEC_TBL"), ("INSERT", "MEM_DB.MEM_SCH.PG_VEC_TBL")},
        },
    )
    dialog = CreateMemoryDialogService(engine)
    embed_l = DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-l-v2.0"]

    # 1. Create all 4 vector backends with LLM Preprocessing disabled (default)
    res_std = dialog.create_memory(
        name="mem_standard",
        embedding_model=embed_l,
        vector_database=VectorBackendType.STANDARD_VECTOR,
        database="MEM_DB",
        schema="MEM_SCH",
        table_name="STD_VEC_TBL",
        batch_size=32,
        llm_preprocessing=LLMPreprocessingConfig(enabled=False),
        create_physical_resource=True,
    )
    assert res_std["state"] == DialogState.CREATED.value
    assert res_std["llm_preprocessing_enabled"] is False

    res_hyb = dialog.create_memory(
        name="mem_hybrid",
        embedding_model=embed_l,
        vector_database=VectorBackendType.HYBRID_VECTOR,
        database="MEM_DB",
        schema="MEM_SCH",
        table_name="HYB_VEC_TBL",
        create_physical_resource=True,
    )
    assert res_hyb["backend_type"] == "HYBRID_VECTOR"

    res_cs = dialog.create_memory(
        name="mem_cortex_search",
        embedding_model=embed_l,
        vector_database=VectorBackendType.CORTEX_SEARCH,
        database="MEM_DB",
        schema="MEM_SCH",
        table_name="CS_SRC_TBL",
        cortex_search_service_name="CS_SVC",
        cortex_embedding_mode=CortexSearchEmbeddingMode.MANAGED,
        create_physical_resource=True,
    )
    assert res_cs["backend_type"] == "CORTEX_SEARCH"

    res_pg = dialog.create_memory(
        name="mem_pgvector",
        embedding_model=embed_l,
        vector_database=VectorBackendType.POSTGRES_PGVECTOR,
        database="MEM_DB",
        schema="MEM_SCH",
        table_name="PG_VEC_TBL",
        postgres_instance="PG_INST_MAIN",
        postgres_role="pg_creator",
        create_physical_resource=True,
    )
    assert res_pg["backend_type"] == "POSTGRES_PGVECTOR"

    # 2. Granular permission separation (`[G-A4]`): ROLE_USER cannot CREATE physical store,
    #    but CAN bind to and use an existing vector store without CREATE TABLE privilege!
    engine.active_role = "ROLE_USER"
    with pytest.raises(SnowflakeComponentError) as create_denied:
        dialog.create_memory(
            name="mem_illegal_create",
            embedding_model=embed_l,
            vector_database=VectorBackendType.STANDARD_VECTOR,
            database="MEM_DB",
            schema="MEM_SCH",
            table_name="NEW_TBL",
            create_physical_resource=True,
        )
    assert create_denied.value.code == ErrorCode.AUTHZ_ROLE_DENIED
    assert create_denied.value.http_status == 403

    bound = dialog.create_memory(
        name="mem_existing_for_user",
        embedding_model=embed_l,
        vector_database=VectorBackendType.STANDARD_VECTOR,
        database="MEM_DB",
        schema="MEM_SCH",
        table_name="STD_VEC_TBL",
        create_physical_resource=False,
    )
    assert bound["provisioning"]["status"] == "BOUND_EXISTING"

    # 3. Ingest with LLM Preprocessing=False makes ZERO generative LLM calls
    mem_comp = SnowflakeMemoryBaseComponent(engine, "mem_existing_for_user", filter_by_session=False)
    mem_comp.store_memory("User A session 1 fact", user_id="user_a", project_id="proj_1", session_id="s1")
    mem_comp.store_memory("User A session 2 fact", user_id="user_a", project_id="proj_1", session_id="s2")
    mem_comp.store_memory("User B other tenant secret", user_id="user_b", project_id="proj_1", session_id="s1")
    assert engine.generative_preprocessing_calls == 0

    # 4. Cross-session search (`filter_by_session=False`) returns both s1 and s2 for user_a,
    #    but NEVER leaks user_b's memories!
    search_res = mem_comp.search_memory("fact secret", user_id="user_a", project_id="proj_1")
    returned_texts = [r["content"] for r in search_res["results"]]
    assert "User A session 1 fact" in returned_texts
    assert "User A session 2 fact" in returned_texts
    assert "User B other tenant secret" not in returned_texts


def test_embedding_space_fingerprint_guard_and_cortex_search_owner_rights_isolation() -> None:
    engine = InMemoryVectorStoreEngine(
        active_role="ROLE_A",
        snowflake_privileges={"ROLE_A": {("CREATE TABLE", "*"), ("CREATE CORTEX SEARCH SERVICE", "*"), ("INSERT", "*"), ("USAGE", "*")}},
    )
    dialog = CreateMemoryDialogService(engine)
    arctic_1024 = DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["snowflake-arctic-embed-l-v2.0"]
    nv_1024 = DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG["nv-embed-qa-4"]
    assert arctic_1024.dimension == nv_1024.dimension == 1024
    assert arctic_1024.space_fingerprint != nv_1024.space_fingerprint

    dialog.create_memory(
        name="cs_memory",
        embedding_model=arctic_1024,
        vector_database=VectorBackendType.CORTEX_SEARCH,
        database="DB1",
        schema="SCH1",
        table_name="CS_RAW",
        cortex_search_service_name="CS_INDEX",
        create_physical_resource=True,
    )
    kc = SnowflakeKnowledgeComponent(engine, "cs_memory")
    kc.ingest([{"content": "Alpha project doc"}], user_id="u1", project_id="p1", session_id="s1")

    # Same 1024 dimension but different embedding model must be blocked!
    with pytest.raises(SnowflakeComponentError) as fp_exc:
        kc.retrieve("Alpha", user_id="u1", project_id="p1", active_embedding_spec=nv_1024)
    assert fp_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY
    assert fp_exc.value.http_status == 409

    # SEARCH_PREVIEW is forbidden for end-user serving
    with pytest.raises(SnowflakeComponentError) as preview_exc:
        kc.retrieve("Alpha", user_id="u1", project_id="p1", use_search_preview=True)
    assert preview_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY

    # Generative model cannot be registered as an EmbeddingModelSpec
    with pytest.raises(SnowflakeComponentError):
        EmbeddingModelSpec(
            model_id="claude-sonnet-4-5",
            provider="Snowflake Proxy - Messages",
            dimension=1024,
            is_generative=True,
        )


# ============================================================================
# 3. MESSAGE HISTORY (`REQ-08`, `ADR-002`, `ADR-003`)
# ============================================================================


def test_message_history_postgres_and_standard_table_and_signed_replay_branching() -> None:
    backend = InMemoryMessageHistoryBackend(
        active_role="ROLE_CHAT",
        snowflake_privileges={
            "ROLE_CHAT": {
                ("INSERT", "HIST_DB.HIST_SCH.LANGFLOW_MESSAGE_HISTORY"),
                ("SELECT", "HIST_DB.HIST_SCH.LANGFLOW_MESSAGE_HISTORY"),
                ("USAGE", "PG_HIST_INST"),
            }
        },
        postgres_privileges={
            "pg_chat_role": {
                ("INSERT", "HIST_DB.HIST_SCH.LANGFLOW_MESSAGE_HISTORY"),
                ("SELECT", "HIST_DB.HIST_SCH.LANGFLOW_MESSAGE_HISTORY"),
            }
        },
    )

    for backend_type in (
        MessageHistoryBackendType.SNOWFLAKE_STANDARD_TABLE,
        MessageHistoryBackendType.SNOWFLAKE_POSTGRES,
    ):
        cfg = MessageHistoryStorageConfig(
            backend_type=backend_type,
            database="HIST_DB",
            schema="HIST_SCH",
            postgres_instance="PG_HIST_INST" if backend_type == MessageHistoryBackendType.SNOWFLAKE_POSTGRES else None,
            postgres_role="pg_chat_role" if backend_type == MessageHistoryBackendType.SNOWFLAKE_POSTGRES else None,
        )
        comp = SnowflakeMessageHistoryComponent(backend, cfg)

        # Turn 1: User message + Cortex thread reference
        comp.store_message(
            message_id="m1",
            user_id="u1",
            project_id="p1",
            session_id="sess1",
            sender="User",
            sender_name="User",
            visible_text="Calcola il margine Q3",
            cortex_thread_ref=CortexThreadReference(cortex_thread_id="ct-1", cortex_parent_message_id=0),
        )

        # Turn 2: AI message with signed Anthropic thinking block
        signed_env = SignedReplayEnvelope(
            protocol="Snowflake Proxy - Messages",
            thinking_signature="sig-anthropic-xyz987",
            redacted_thinking="redacted-blob-1",
            tool_call_ids=("call_0",),
        )
        comp.store_message(
            message_id="m2",
            user_id="u1",
            project_id="p1",
            session_id="sess1",
            sender="AI",
            sender_name="Assistant",
            visible_text="Il margine Q3 è 42%.",
            signed_replay=signed_env,
            cortex_thread_ref=CortexThreadReference(
                cortex_thread_id="ct-1",
                cortex_parent_message_id=0,
                last_assistant_message_id=102,
            ),
        )

        # Retrieve verifies separation between visible `Messages`, `Table`, `signed_replay_envelopes`, and `cortex_thread_references`
        retrieved = comp.retrieve_messages(user_id="u1", project_id="p1", session_id="sess1")
        assert len(retrieved["Messages"]) == 2
        assert "sig-anthropic-xyz987" not in json.dumps(retrieved["Messages"])
        assert len(retrieved["signed_replay_envelopes"]) == 1
        assert retrieved["signed_replay_envelopes"][0]["thinking_signature"] == "sig-anthropic-xyz987"
        assert retrieved["cortex_thread_references"][-1]["last_assistant_message_id"] == 102

        # Editing m2 creates a new branch without mutating the original signed block on `main`!
        branched = comp.edit_message_with_branching(
            user_id="u1",
            project_id="p1",
            session_id="sess1",
            target_message_id="m2",
            new_visible_text="Il margine Q3 rettificato è 43%.",
            source_branch_id="main",
            new_branch_id="branch-edit-1",
        )
        assert branched["branch_id"] == "branch-edit-1"
        assert branched["Messages"][-1]["text"] == "Il margine Q3 rettificato è 43%."
        assert branched["Table"][-1]["REPLAY_STATUS"] == "INVALIDATED_BY_EDIT"
        assert len(branched["signed_replay_envelopes"]) == 0

        # Original `main` branch still has its immutable signed replay envelope intact
        main_after = comp.retrieve_messages(user_id="u1", project_id="p1", session_id="sess1", branch_id="main")
        assert main_after["Messages"][-1]["text"] == "Il margine Q3 è 42%."
        assert len(main_after["signed_replay_envelopes"]) == 1


# ============================================================================
# 4. UTILITIES: PYTHON INTERPRETER WITH SNOWFLAKE ARTIFACT REPOSITORY PyPI (`REQ-12`)
# ============================================================================


def test_python_interpreter_pypi_shared_repository_and_no_public_pypi_fallback() -> None:
    assert INVARIANT_UTILITIES_COMPONENTS == ("Calculator", "Current Date")

    interp = SnowflakePythonInterpreterComponent(
        active_role="ROLE_DS",
        granted_database_roles={
            "ROLE_DS": {SNOWFLAKE_PYPI_REQUIRED_DB_ROLE},
            "ROLE_NO_PYPI": set(),
        },
    )

    # 1. Valid execution with package from snowflake.snowpark.pypi_shared_repository
    res = interp.run_interpreter(
        global_imports=["json", "math"],
        python_code="import math\nresult = math.factorial(6)",
    )
    assert res["results"] == 720
    assert res["artifact_repository"] == SNOWFLAKE_PYPI_SHARED_REPOSITORY
    assert res["public_pypi_fallback_used"] is False

    # 2. Missing package in Snowflake Artifact Repository NEVER falls back to public PyPI
    with pytest.raises(SnowflakeComponentError) as missing_pkg_exc:
        interp.run_interpreter(
            global_imports=["nonexistent-public-pypi-pkg==1.0.0"],
            python_code="result = 1",
        )
    assert missing_pkg_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY
    assert "Public PyPI fallback is disabled" in str(missing_pkg_exc.value)

    # 3. Explicit public PyPI index URL is blocked
    with pytest.raises(SnowflakeComponentError) as idx_exc:
        interp.run_interpreter(
            global_imports=["--extra-index-url https://pypi.org/simple pandas"],
            python_code="result = 1",
        )
    assert idx_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY

    # 4. Attempt to read SPCS token or import socket in sandbox is blocked
    with pytest.raises(SnowflakeComponentError) as sec_exc:
        interp.run_interpreter(
            global_imports=["math"],
            python_code="token_path = '/snowflake/session/token'",
        )
    assert sec_exc.value.code == ErrorCode.AUTHZ_ROLE_DENIED

    # 5. Role without SNOWFLAKE.PYPI_REPOSITORY_USER receives HTTP 403 AUTHZ_ROLE_DENIED
    interp.active_role = "ROLE_NO_PYPI"
    with pytest.raises(SnowflakeComponentError) as role_exc:
        interp.run_interpreter(global_imports=["math"], python_code="result = 2 + 2")
    assert role_exc.value.code == ErrorCode.AUTHZ_ROLE_DENIED
    assert role_exc.value.http_status == 403


# ============================================================================
# 5. TOOLGUARD / POLICIES (ALTK, FAIL-CLOSED, `[G-A6]`)
# ============================================================================


def test_toolguard_policies_providers_fail_closed_and_bundle_invariance() -> None:
    assert "Snowflake Proxy - Responses" in SNOWFLAKE_TOOLGUARD_PROVIDERS
    assert "Snowflake Proxy - Messages" in SNOWFLAKE_TOOLGUARD_PROVIDERS
    assert "Snowflake Proxy - Chat" in SNOWFLAKE_TOOLGUARD_PROVIDERS
    assert "Ollama (Snowflake Hosted)" in SNOWFLAKE_TOOLGUARD_PROVIDERS
    assert "Agentics" in UNCHANGED_DISCOVER_MORE_BUNDLES

    refund_tool = ToolDefinition(
        name="issue_refund",
        description="Issue a customer refund",
        handler=lambda args: {"refunded": args["amount"]},
    )
    policy = PolicySpec(
        policy_id="POL_MAX_REFUND",
        description="Refund must not exceed 500 and must not contain 'override_approval'",
        target_tool_name="issue_refund",
        forbidden_arg_patterns=("override_approval",),
        max_numeric_limits={"amount": 500.0},
        required_args=("amount", "reason"),
    )

    comp = SnowflakeToolGuardPoliciesComponent(
        active_role="ROLE_GUARD",
        allowed_roles={"ROLE_GUARD"},
    )
    out = comp.build_guarded_tools(
        activity=ToolGuardActivity.GUARD,
        enabled=True,
        policies_project="finance_guard_proj",
        tools=[refund_tool],
        policies=[policy],
        language_model=ToolGuardModelSelection(
            provider="Snowflake Proxy - Responses",
            model_id="openai-gpt-5",
        ),
    )
    assert out["beta"] is True
    guarded_refund = out["guarded_tools"][0]

    # Compliant invocation succeeds
    ok_res = guarded_refund.invoke({"amount": 250, "reason": "duplicate charge"})
    assert ok_res == {"refunded": 250}
    assert refund_tool.invocation_count == 1

    # Policy violation is blocked fail-closed without executing underlying tool handler
    with pytest.raises(SnowflakeComponentError) as pol_exc:
        guarded_refund.invoke({"amount": 1200, "reason": "large claim"})
    assert pol_exc.value.code == ErrorCode.AUTHZ_ROLE_DENIED
    assert refund_tool.invocation_count == 1  # Not incremented!

    # Fail-closed when generator fails during Generate activity
    failing_comp = SnowflakeToolGuardPoliciesComponent(
        active_role="ROLE_GUARD",
        allowed_roles={"ROLE_GUARD"},
        policy_generator_fn=lambda tools, pols, lm: (_ for _ in ()).throw(RuntimeError("Proxy timeout")),
    )
    with pytest.raises(SnowflakeComponentError) as gen_fail_exc:
        failing_comp.build_guarded_tools(
            activity=ToolGuardActivity.GENERATE,
            enabled=True,
            policies_project="finance_guard_proj",
            tools=[refund_tool],
            policies=[policy],
            language_model=ToolGuardModelSelection(
                provider="Snowflake Proxy - Messages",
                model_id="claude-sonnet-4-5",
            ),
        )
    assert gen_fail_exc.value.code == ErrorCode.TECHNICAL_EXECUTION_ERROR
    assert gen_fail_exc.value.http_status == 502


# ============================================================================
# 6. A2A AGENT (CLIENT + CORTEX AGENT ADAPTER) & SERVE AS AN A2A AGENT (`REQ-14`)
# ============================================================================


def test_serve_as_a2a_agent_lifecycle_rbac_and_anti_recursion() -> None:
    registry = ServeAsA2AAgentRegistry(
        spcs_ingress_host="<SPCS_INGRESS_HOST>",
        role_privileges={
            "ROLE_OWNER": {
                ("EDIT_FLOW", "flow_sales"),
                ("PUBLISH_A2A", "flow_sales"),
                ("INVOKE_A2A", "flow_sales"),
                ("SELECT", "SALES_DB.SCH.ORDERS"),
                ("EDIT_FLOW", "flow_incomplete"),
                ("PUBLISH_A2A", "flow_incomplete"),
            },
            # ROLE_CALLER_NO_TABLE can invoke the flow but lacks SELECT on SALES_DB.SCH.ORDERS
            "ROLE_CALLER_NO_TABLE": {
                ("INVOKE_A2A", "flow_sales"),
            },
        },
        max_recursion_hops=3,
    )

    # 1. Flow missing Chat Output remains Unavailable and cannot be published
    registry.register_flow_topology(
        flow_id="flow_incomplete",
        owner_role="ROLE_OWNER",
        has_chat_input=True,
        has_chat_output=False,
    )
    saved_incomplete = registry.save_card_metadata(
        flow_id="flow_incomplete",
        active_role="ROLE_OWNER",
        card=A2AAgentCard(flow_id="flow_incomplete", name="Incomplete Agent"),
    )
    assert saved_incomplete["status"] == A2APublicationStatus.UNAVAILABLE.value
    with pytest.raises(SnowflakeComponentError) as pub_inc_exc:
        registry.publish_flow(flow_id="flow_incomplete", active_role="ROLE_OWNER")
    assert pub_inc_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY

    # 2. Complete flow: Save != Publish, then Publish exposes credential-free URL/curl
    registry.register_flow_topology(
        flow_id="flow_sales",
        owner_role="ROLE_OWNER",
        has_chat_input=True,
        has_chat_output=True,
        required_resources=("SALES_DB.SCH.ORDERS",),
    )
    saved_card = registry.save_card_metadata(
        flow_id="flow_sales",
        active_role="ROLE_OWNER",
        card=A2AAgentCard(flow_id="flow_sales", name="Sales Assistant", description="Safe card"),
    )
    assert saved_card["status"] == A2APublicationStatus.SAVED_NOT_PUBLISHED.value
    assert saved_card["is_published"] is False

    # Cannot invoke before Publish
    with pytest.raises(SnowflakeComponentError) as not_pub_exc:
        registry.invoke_published_flow(
            flow_id="flow_sales",
            message="Hi",
            caller_authenticated=True,
            caller_effective_role="ROLE_OWNER",
        )
    assert not_pub_exc.value.http_status == 404

    pub_res = registry.publish_flow(flow_id="flow_sales", active_role="ROLE_OWNER")
    assert pub_res["status"] == A2APublicationStatus.PUBLISHED.value
    assert "<SPCS_BEARER_TOKEN>" in pub_res["curl_example"]

    # 3. Caller without underlying table privilege is denied (owner privileges are NEVER inherited,
    #    even if claimed_payload_role="ROLE_OWNER" is forged in the A2A payload!)
    with pytest.raises(SnowflakeComponentError) as no_inherit_exc:
        registry.invoke_published_flow(
            flow_id="flow_sales",
            message="Show orders",
            caller_authenticated=True,
            caller_effective_role="ROLE_CALLER_NO_TABLE",
            claimed_payload_role="ROLE_OWNER",
        )
    assert no_inherit_exc.value.code == ErrorCode.AUTHZ_ROLE_DENIED
    assert no_inherit_exc.value.http_status == 403

    # 4. Authorized caller succeeds and anti-recursion blocks self-loop
    ok_call = registry.invoke_published_flow(
        flow_id="flow_sales",
        message="Show orders",
        caller_authenticated=True,
        caller_effective_role="ROLE_OWNER",
    )
    assert ok_call["effective_role"] == "ROLE_OWNER"

    with pytest.raises(SnowflakeComponentError) as loop_exc:
        registry.invoke_published_flow(
            flow_id="flow_sales",
            message="Loop",
            caller_authenticated=True,
            caller_effective_role="ROLE_OWNER",
            call_chain=("flow_entry", "flow_sales"),
        )
    assert loop_exc.value.code == ErrorCode.TECHNICAL_EXECUTION_ERROR

    # 5. Revoke immediately disables endpoint
    registry.revoke_flow(flow_id="flow_sales", active_role="ROLE_OWNER")
    with pytest.raises(SnowflakeComponentError) as rev_exc:
        registry.invoke_published_flow(
            flow_id="flow_sales",
            message="After revoke",
            caller_authenticated=True,
            caller_effective_role="ROLE_OWNER",
        )
    assert rev_exc.value.http_status == 404


def test_cortex_agent_run_adapter_multi_turn_thread_and_no_model_override() -> None:
    adapter = CortexAgentRunAdapter(
        active_role="ROLE_ANALYST",
        role_privileges={"ROLE_ANALYST": {("USAGE", "ANALYTICS_DB.AGENTS_SCH.FINANCE_AGENT")}},
    )
    client = SnowflakeA2AAgentComponent(cortex_adapter=adapter)

    # Turn 1 starts with parent_message_id == 0
    turn1 = client.invoke(
        target_type=A2ATargetType.CORTEX_AGENT_REST,
        message="What was Q1 revenue?",
        caller_effective_role="ROLE_ANALYST",
        database="ANALYTICS_DB",
        schema="AGENTS_SCH",
        cortex_agent_name="FINANCE_AGENT",
        thread_id="thread-fin-001",
    )
    assert turn1["endpoint"] == "/api/v2/databases/ANALYTICS_DB/schemas/AGENTS_SCH/agents/FINANCE_AGENT:run"
    assert turn1["parent_message_id_sent"] == 0
    assert turn1["assistant_message_id"] > turn1["user_message_id"]
    assert len(turn1["citations"]) == 1
    assert len(turn1["tables"]) == 1

    # Turn 2 uses turn1's assistant_message_id (NOT user_message_id!) as parent_message_id
    turn2 = client.invoke(
        target_type=A2ATargetType.CORTEX_AGENT_REST,
        message="Break it down by region",
        caller_effective_role="ROLE_ANALYST",
        database="ANALYTICS_DB",
        schema="AGENTS_SCH",
        cortex_agent_name="FINANCE_AGENT",
        thread_id="thread-fin-001",
    )
    assert turn2["parent_message_id_sent"] == turn1["assistant_message_id"]
    assert turn2["parent_message_id_sent"] != turn1["user_message_id"]

    # Overriding model or orchestration of a saved Cortex Agent object is strictly forbidden
    with pytest.raises(SnowflakeComponentError) as ovr_exc:
        client.invoke(
            target_type=A2ATargetType.CORTEX_AGENT_REST,
            message="Try override",
            caller_effective_role="ROLE_ANALYST",
            database="ANALYTICS_DB",
            schema="AGENTS_SCH",
            cortex_agent_name="FINANCE_AGENT",
            override_model="openai-gpt-5",
        )
    assert ovr_exc.value.code == ErrorCode.UNSUPPORTED_CAPABILITY
