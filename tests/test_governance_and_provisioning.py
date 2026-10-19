"""Comprehensive test suite for Governance, Real Role Selector, Explicit Denials, and Object Provisioning.

Covers REQ-02, REQ-03, REQ-04, REQ-10 ([G-A4]), REQ-14, REQ-16, ADR-003, ADR-004, ADR-005.
"""

from __future__ import annotations

import pytest

from lfx_snowflake.governance import (
    CANONICAL_ROLE_DENIED_MESSAGE,
    ExecutionIdentityType,
    InMemoryRoleScopedSqlAdapter,
    ObjectFamily,
    OperationType,
    SnowflakeAuthzService,
    SnowflakeErrorCode,
    SnowflakeGovernanceError,
    SnowflakeGovernanceMiddleware,
    SnowflakeObjectProvisioner,
    SubsystemBoundary,
    require_snowflake_permission,
)


@pytest.fixture()
def governance_env() -> tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware]:
    sql_adapter = InMemoryRoleScopedSqlAdapter(
        user_roles={
            "alice@example.org": ["ROLE_BUILDER", "ROLE_VIEWER"],
            "bob@example.org": ["ROLE_VIEWER"],
        },
        role_privileges={
            "ROLE_BUILDER": {
                "CREATE POSTGRES INSTANCE ON ACCOUNT",
                "CREATE DATABASE ON ACCOUNT",
                "CREATE SCHEMA ON DATABASE",
                "USAGE ON DATABASE",
                "USAGE ON SCHEMA",
                "USAGE ON WAREHOUSE",
                "USAGE ON EXTERNAL VOLUME",
                "USAGE ON INTEGRATION",
                "CREATE TABLE ON SCHEMA",
                "CREATE HYBRID TABLE ON SCHEMA",
                "CREATE ICEBERG TABLE ON SCHEMA",
                "CREATE INTERACTIVE TABLE ON SCHEMA",
                "CREATE DYNAMIC TABLE ON SCHEMA",
                "CREATE STAGE ON SCHEMA",
                "SELECT ON SOURCE TABLE",
                "SELECT ON TABLE",
                "SNOWFLAKE.CORTEX_USER",
            },
            "ROLE_VIEWER": {
                "USAGE ON DATABASE",
                "USAGE ON SCHEMA",
                "SELECT ON TABLE",
            },
        },
        service_owner_role="SERVICE_OWNER_INTERNAL",
    )
    authz = SnowflakeAuthzService(
        sql_adapter=sql_adapter,
        service_owner_role="SERVICE_OWNER_INTERNAL",
    )
    authz.configure_role_permissions(
        "ROLE_BUILDER",
        [op.value for op in OperationType],
    )
    authz.configure_role_permissions(
        "ROLE_VIEWER",
        [
            OperationType.BROWSE_CATALOG.value,
            OperationType.QUERY_DATA_SOURCE.value,
            OperationType.READ_STAGE_FILE.value,
            OperationType.USE_EXISTING_VECTOR_STORE.value,
            OperationType.READ_FLOW_TRACES.value,
        ],
    )
    authz.configure_postgres_grant("alice@example.org", "application", {"CREATE", "SELECT", "INSERT"})
    authz.configure_postgres_grant("bob@example.org", "application", {"SELECT"})

    provisioner = SnowflakeObjectProvisioner(authz)
    middleware = SnowflakeGovernanceMiddleware(authz, provisioner)
    return authz, provisioner, middleware


# ----------------------------------------------------------------------
# 1. Role Discovery, Per-Role Session, context_epoch & No Silent Fallback
# ----------------------------------------------------------------------
def test_role_discovery_and_per_role_session_with_current_role_verification(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, middleware = governance_env

    parsed = authz.parse_available_roles_Result('["role_builder", "role_viewer"]')
    assert parsed == ["ROLE_BUILDER", "ROLE_VIEWER"]

    ctx1 = authz.establish_interactive_session("alice@example.org", requested_role="ROLE_BUILDER")
    assert ctx1.active_role == "ROLE_BUILDER"
    assert ctx1.verified_current_role == "ROLE_BUILDER"
    assert ctx1.session_token_scope == "session:role:ROLE_BUILDER"
    assert ctx1.context_epoch == 1
    assert ctx1.secondary_roles_mode == "NONE"

    # Populate scoped cache under ROLE_BUILDER (epoch 1)
    authz.put_scoped_cache("alice@example.org", "catalog:schemas", ["PUBLIC", "ANALYTICS"])
    assert authz.get_scoped_cache("alice@example.org", "catalog:schemas") == ["PUBLIC", "ANALYTICS"]

    # Switch to ROLE_VIEWER via middleware POST /api/v1/snowflake/role
    status, payload = middleware.handle_post_role(
        user_principal="alice@example.org",
        target_role="ROLE_VIEWER",
    )
    assert status == 200
    assert payload["active_role"] == "ROLE_VIEWER"
    assert payload["verified_current_role"] == "ROLE_VIEWER"
    assert payload["context_epoch"] == 2
    assert payload["session_token_scope"] == "session:role:ROLE_VIEWER"

    # Previous epoch cache must be invalidated immediately
    assert authz.get_scoped_cache("alice@example.org", "catalog:schemas") is None
    with pytest.raises(SnowflakeGovernanceError) as exc_info:
        authz.validate_epoch("alice@example.org", request_epoch=1)
    assert exc_info.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert exc_info.value.http_status == 403


def test_secondary_roles_all_and_service_owner_fallback_are_strictly_forbidden(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, middleware = governance_env

    # Reject SECONDARY ROLES ALL on session creation
    with pytest.raises(SnowflakeGovernanceError) as exc_info:
        authz.establish_interactive_session(
            "alice@example.org",
            requested_role="ROLE_BUILDER",
            secondary_roles_mode="ALL",
        )
    assert exc_info.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert exc_info.value.http_status == 403
    assert CANONICAL_ROLE_DENIED_MESSAGE in exc_info.value.message

    # Reject switching to ungranted role or service owner role (no silent fallback)
    status, err_payload = middleware.handle_post_role(
        user_principal="bob@example.org",
        target_role="SERVICE_OWNER_INTERNAL",
    )
    assert status == 403
    assert err_payload["error_code"] == "AUTHZ_ROLE_DENIED"
    assert err_payload["base_message"] == CANONICAL_ROLE_DENIED_MESSAGE


def test_mid_session_role_revocation_invalidates_session_and_caches(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, _ = governance_env
    authz.establish_interactive_session("alice@example.org", requested_role="ROLE_BUILDER")
    authz.put_scoped_cache("alice@example.org", "preview", {"rows": 10})

    new_epoch = authz.revoke_role("alice@example.org", "ROLE_BUILDER")
    assert new_epoch == 2

    with pytest.raises(SnowflakeGovernanceError) as exc_info:
        authz.get_session_context("alice@example.org")
    assert exc_info.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert exc_info.value.http_status == 403


# ----------------------------------------------------------------------
# 2. 4-Code Error Taxonomy & Anti-Leak Rules (ADR-004)
# ----------------------------------------------------------------------
def test_four_code_error_taxonomy_classification_and_anti_leak(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, _ = governance_env
    authz.establish_interactive_session("bob@example.org", requested_role="ROLE_VIEWER")

    # 1. AUTHZ_ROLE_DENIED (HTTP 403)
    with pytest.raises(SnowflakeGovernanceError) as denied_exc:
        authz.check_operation_permission(
            user_principal="bob@example.org",
            operation=OperationType.PROVISION_SNOWFLAKE_OBJECT,
            target_ref="SECRET_DB.SECRET_SCHEMA.HIDDEN_TABLE",
            required_privilege="CREATE TABLE ON SCHEMA",
            disclosable=False,
        )
    err1 = denied_exc.value
    assert err1.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert err1.http_status == 403
    assert CANONICAL_ROLE_DENIED_MESSAGE in err1.message
    # Must NOT leak hidden object name or undisclosable privilege in user message
    assert "HIDDEN_TABLE" not in err1.message
    assert err1.required_privilege is None

    # 2. OBJECT_NOT_FOUND_OR_UNAUTHORIZED (HTTP 403) on Snowflake 002003
    err2 = authz.classify_exception(
        user_principal="bob@example.org",
        operation=OperationType.QUERY_DATA_SOURCE,
        exc=RuntimeError("002003 (42S02): SQL compilation error: Object 'PRIVATE_TBL' does not exist or not authorized."),
    )
    assert err2.error_code == SnowflakeErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED
    assert err2.http_status == 403
    assert "PRIVATE_TBL" not in err2.message

    # 3. UNSUPPORTED_CAPABILITY (HTTP 422)
    err3 = authz.classify_exception(
        user_principal="bob@example.org",
        operation=OperationType.INVOKE_PROXY_MODEL,
        exc=ValueError("Feature not supported for selected protocol"),
    )
    assert err3.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY
    assert err3.http_status == 422

    # 4. TECHNICAL_EXECUTION_ERROR (HTTP 502/504)
    err4 = authz.classify_exception(
        user_principal="bob@example.org",
        operation=OperationType.INVOKE_CORTEX_ANALYST,
        exc=TimeoutError("Upstream gateway timed out after 30s"),
    )
    assert err4.error_code == SnowflakeErrorCode.TECHNICAL_EXECUTION_ERROR
    assert err4.http_status == 504


# ----------------------------------------------------------------------
# 3. Dual Identity & Subsystem Identity Separation (ADR-003, [G-A4])
# ----------------------------------------------------------------------
def test_dual_identity_interactive_vs_technical_automation(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, _ = governance_env

    # Cannot bind service_owner_role to automation
    with pytest.raises(SnowflakeGovernanceError):
        authz.bind_automation_role("proj_1", "flow_1", "SERVICE_OWNER_INTERNAL")

    authz.bind_automation_role("proj_1", "flow_1", "ROLE_AUTOMATION_MINIMAL")
    auto_ctx = authz.establish_automation_session(
        automation_principal="svc_runner",
        project_id="proj_1",
        flow_id="flow_1",
    )
    assert auto_ctx.identity_type == ExecutionIdentityType.TECHNICAL_AUTOMATION
    assert auto_ctx.active_role == "ROLE_AUTOMATION_MINIMAL"

    # Reject escalation attempt on automation session
    with pytest.raises(SnowflakeGovernanceError) as exc_info:
        authz.establish_automation_session(
            automation_principal="svc_runner",
            project_id="proj_1",
            flow_id="flow_1",
            requested_role="ROLE_BUILDER",
        )
    assert exc_info.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED


def test_subsystem_separation_postgres_proxy_and_cortex_search_owner_rights(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, _ = governance_env
    authz.establish_interactive_session("bob@example.org", requested_role="ROLE_VIEWER")

    # Bob has PG SELECT but not PG CREATE on :5432
    pg_ok = authz.authorize_postgres_operation(
        user_principal="bob@example.org",
        pg_role="application",
        required_pg_privilege="SELECT",
    )
    assert pg_ok["sslmode"] == "verify-ca"
    assert pg_ok["connect_timeout"] == 10

    with pytest.raises(SnowflakeGovernanceError) as pg_denied:
        authz.authorize_postgres_operation(
            user_principal="bob@example.org",
            pg_role="application",
            required_pg_privilege="CREATE",
        )
    assert pg_denied.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert pg_denied.value.subsystem == SubsystemBoundary.SNOWFLAKE_POSTGRES

    # Reject insecure sslmode on Snowflake Postgres (:5432)
    with pytest.raises(SnowflakeGovernanceError) as ssl_err:
        authz.authorize_postgres_operation(
            user_principal="bob@example.org",
            pg_role="application",
            required_pg_privilege="SELECT",
            sslmode="disable",
        )
    assert ssl_err.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    # Cortex Search owner's rights: blocks SEARCH_PREVIEW and injects mandatory isolation filters
    with pytest.raises(SnowflakeGovernanceError) as preview_err:
        authz.authorize_cortex_search_query(
            user_principal="bob@example.org",
            service_fqn="APP_DB.APP_SCHEMA.DOC_SEARCH_SVC",
            use_search_preview=True,
        )
    assert preview_err.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    search_auth = authz.authorize_cortex_search_query(
        user_principal="bob@example.org",
        service_fqn="APP_DB.APP_SCHEMA.DOC_SEARCH_SVC",
        filter_by_session=False,
    )
    and_clauses = search_auth["enforced_filter"]["@and"]
    assert {"@eq": {"authenticated_user": "bob@example.org"}} in and_clauses
    assert {"@eq": {"authorized_role": "ROLE_VIEWER"}} in and_clauses

    # [G-A4] Granular separation of USE existing vector store vs CREATE physical resource
    use_res = authz.authorize_memory_operation(
        user_principal="bob@example.org",
        memory_ref="APP_DB.APP_SCHEMA.EXISTING_VEC_TBL",
        is_create_physical_resource=False,
        filter_by_session=False,
    )
    assert use_res["operation"] == OperationType.USE_EXISTING_VECTOR_STORE.value
    assert use_res["scope_boundary"]["active_role"] == "ROLE_VIEWER"

    with pytest.raises(SnowflakeGovernanceError) as create_mem_denied:
        authz.authorize_memory_operation(
            user_principal="bob@example.org",
            memory_ref="APP_DB.APP_SCHEMA.NEW_VEC_TBL",
            is_create_physical_resource=True,
        )
    assert create_mem_denied.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED


# ----------------------------------------------------------------------
# 4. Middleware: Global Variables, Import Flow, A2A & Decorator Guard
# ----------------------------------------------------------------------
def test_middleware_protects_readonly_role_variable_and_a2a_boundaries(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, middleware = governance_env
    authz.establish_interactive_session("alice@example.org", requested_role="ROLE_VIEWER")

    # Attempting to overwrite SNOWFLAKE_ACTIVE_ROLE in Global Variables must fail with 403
    with pytest.raises(SnowflakeGovernanceError) as var_err:
        middleware.enforce_global_variable_write(
            user_principal="alice@example.org",
            variable_name="SNOWFLAKE_ACTIVE_ROLE",
            variable_value="ROLE_BUILDER",
        )
    assert var_err.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED

    # Effective global variables always force SNOWFLAKE_ACTIVE_ROLE from backend session
    effective_vars = middleware.get_effective_global_variables(
        user_principal="alice@example.org",
        user_variables={"SNOWFLAKE_ACTIVE_ROLE": "FORGED_ADMIN", "CUSTOM_VAR": "ok"},
    )
    assert effective_vars["SNOWFLAKE_ACTIVE_ROLE"] == "ROLE_VIEWER"
    assert effective_vars["CUSTOM_VAR"] == "ok"

    # Switch Alice to ROLE_BUILDER and test stage flow import + secret redaction
    authz.switch_role("alice@example.org", "ROLE_BUILDER")
    imported = middleware.enforce_flow_import(
        user_principal="alice@example.org",
        source_type="stage",
        flow_payload={"name": "SampleFlow", "config": {"api_key": "super_secret_val", "node": "ok"}},
        stage_coords={
            "database": "APP_DB",
            "schema": "FLOWS_SCHEMA",
            "stage": "FLOW_STAGE",
            "subpath": "team_a/flow_v1.json",
        },
    )
    assert imported["validated_stage_uri"] == "@APP_DB.FLOWS_SCHEMA.FLOW_STAGE/team_a/flow_v1.json"
    assert imported["auto_execute_custom_code"] is False
    assert imported["flow"]["config"]["api_key"] == ""

    # A2A Server: unavailable without Chat Input + Chat Output, blocks forged card role & recursion
    with pytest.raises(SnowflakeGovernanceError) as a2a_missing_io:
        middleware.enforce_a2a_invocation(
            caller_principal="alice@example.org",
            flow_id="flow_a2a",
            has_chat_input=True,
            has_chat_output=False,
            is_published=True,
        )
    assert a2a_missing_io.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    with pytest.raises(SnowflakeGovernanceError) as a2a_forged_role:
        middleware.enforce_a2a_invocation(
            caller_principal="alice@example.org",
            flow_id="flow_a2a",
            has_chat_input=True,
            has_chat_output=True,
            is_published=True,
            card_claimed_role="SERVICE_OWNER_INTERNAL",
        )
    assert a2a_forged_role.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED

    a2a_ok = middleware.enforce_a2a_invocation(
        caller_principal="alice@example.org",
        flow_id="flow_a2a",
        has_chat_input=True,
        has_chat_output=True,
        is_published=True,
        flow_owner_role="SERVICE_OWNER_INTERNAL",
    )
    assert a2a_ok["effective_role"] == "ROLE_BUILDER"
    assert a2a_ok["inherited_owner_role"] is False


# ----------------------------------------------------------------------
# 5. SnowflakeObjectProvisioner: All Object Families & Constraints (ADR-005)
# ----------------------------------------------------------------------
def test_provisioner_postgres_instance_vs_internal_pgvector_table(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, provisioner, _ = governance_env
    authz.establish_interactive_session("alice@example.org", requested_role="ROLE_BUILDER")

    # 1. Account-level Snowflake Postgres instance preview + confirmed execution
    pg_inst_preview = provisioner.preview_postgres_instance(
        user_principal="alice@example.org",
        instance_name="LF_PG_INST",
        compute_family="STANDARD_M",
        storage_size_gb=20,
        authentication_authority="POSTGRES_OR_SNOWFLAKE",
    )
    assert pg_inst_preview.object_family == ObjectFamily.POSTGRES_INSTANCE
    assert "CREATE POSTGRES INSTANCE LF_PG_INST" in pg_inst_preview.ddl_statements[0]
    assert pg_inst_preview.owner_role == "ROLE_BUILDER"
    assert pg_inst_preview.requires_confirmation is True

    # Must reject execution if user_confirmed=False
    with pytest.raises(SnowflakeGovernanceError) as unconfirmed_err:
        provisioner.execute_previewed_provisioning(
            user_principal="alice@example.org",
            preview=pg_inst_preview,
            user_confirmed=False,
        )
    assert unconfirmed_err.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    pg_inst_res = provisioner.execute_previewed_provisioning(
        user_principal="alice@example.org",
        preview=pg_inst_preview,
        user_confirmed=True,
    )
    assert pg_inst_res.status == "CREATED"
    assert pg_inst_res.owner_role == "ROLE_BUILDER"

    # 2. Internal PostgreSQL (:5432) pgvector table
    pg_tbl_preview = provisioner.preview_postgres_internal_vector_table(
        user_principal="alice@example.org",
        pg_role="application",
        instance_name="LF_PG_INST",
        pg_database="langflow_db",
        pg_schema="vectors",
        pg_table="memory_items",
        vector_dimension=1024,
    )
    assert pg_tbl_preview.subsystem == SubsystemBoundary.SNOWFLAKE_POSTGRES
    assert "CREATE EXTENSION IF NOT EXISTS vector" in pg_tbl_preview.ddl_statements[0]
    assert "embedding vector(1024)" in pg_tbl_preview.ddl_statements[2]

    pg_tbl_res = provisioner.execute_previewed_provisioning(
        user_principal="alice@example.org",
        preview=pg_tbl_preview,
        user_confirmed=True,
    )
    assert pg_tbl_res.status == "CREATED"


def test_provisioner_database_schema_and_all_five_table_families(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, provisioner, _ = governance_env
    authz.establish_interactive_session("alice@example.org", requested_role="ROLE_BUILDER")

    # Database & Schema
    db_prev = provisioner.preview_database(user_principal="alice@example.org", database="APP_DB")
    assert db_prev.ddl_statements == ("CREATE DATABASE APP_DB",)
    sch_prev = provisioner.preview_schema(
        user_principal="alice@example.org",
        database="APP_DB",
        schema="CORE_SCHEMA",
    )
    assert sch_prev.ddl_statements == ("CREATE SCHEMA APP_DB.CORE_SCHEMA",)

    # 1. Standard Table with VECTOR(FLOAT, 1024)
    std_prev = provisioner.preview_table(
        user_principal="alice@example.org",
        table_family=ObjectFamily.STANDARD_TABLE,
        database="APP_DB",
        schema="CORE_SCHEMA",
        table="DOC_VECTORS",
        columns=[("ID", "VARCHAR"), ("EMBEDDING", "VECTOR(FLOAT, 1024)")],
        expected_embedding_dim=1024,
    )
    assert "CREATE TABLE APP_DB.CORE_SCHEMA.DOC_VECTORS" in std_prev.ddl_statements[0]

    # Reject VECTOR > 4096 or dimension mismatch
    with pytest.raises(SnowflakeGovernanceError) as dim_err:
        provisioner.preview_table(
            user_principal="alice@example.org",
            table_family=ObjectFamily.STANDARD_TABLE,
            database="APP_DB",
            schema="CORE_SCHEMA",
            table="BAD_VECTORS",
            columns=[("ID", "VARCHAR"), ("EMBEDDING", "VECTOR(FLOAT, 8192)")],
        )
    assert dim_err.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    # 2. Hybrid Table: enforces PK required and VECTOR cannot be PK or secondary index
    with pytest.raises(SnowflakeGovernanceError) as hybrid_vec_pk_err:
        provisioner.preview_table(
            user_principal="alice@example.org",
            table_family=ObjectFamily.HYBRID_TABLE,
            database="APP_DB",
            schema="CORE_SCHEMA",
            table="HYB_BAD",
            columns=[("ID", "VARCHAR"), ("VEC", "VECTOR(FLOAT, 768)")],
            primary_key_columns=["VEC"],
        )
    assert hybrid_vec_pk_err.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    with pytest.raises(SnowflakeGovernanceError) as hybrid_vec_idx_err:
        provisioner.preview_table(
            user_principal="alice@example.org",
            table_family=ObjectFamily.HYBRID_TABLE,
            database="APP_DB",
            schema="CORE_SCHEMA",
            table="HYB_BAD_IDX",
            columns=[("ID", "VARCHAR"), ("VEC", "VECTOR(FLOAT, 768)")],
            primary_key_columns=["ID"],
            secondary_index_columns=["VEC"],
        )
    assert hybrid_vec_idx_err.value.error_code == SnowflakeErrorCode.UNSUPPORTED_CAPABILITY

    hyb_prev = provisioner.preview_table(
        user_principal="alice@example.org",
        table_family=ObjectFamily.HYBRID_TABLE,
        database="APP_DB",
        schema="CORE_SCHEMA",
        table="HYB_MEMORIES",
        columns=[("ID", "VARCHAR"), ("VEC", "VECTOR(FLOAT, 768)")],
        primary_key_columns=["ID"],
    )
    assert "CREATE HYBRID TABLE APP_DB.CORE_SCHEMA.HYB_MEMORIES" in hyb_prev.ddl_statements[0]
    assert "PRIMARY KEY (ID)" in hyb_prev.ddl_statements[0]

    # 3. Iceberg Table
    ice_prev = provisioner.preview_table(
        user_principal="alice@example.org",
        table_family=ObjectFamily.ICEBERG_TABLE,
        database="APP_DB",
        schema="CORE_SCHEMA",
        table="ICE_EVENTS",
        columns=[("EVENT_ID", "VARCHAR"), ("PAYLOAD", "STRING")],
        external_volume="EXT_VOL_MAIN",
    )
    assert "CREATE ICEBERG TABLE APP_DB.CORE_SCHEMA.ICE_EVENTS" in ice_prev.ddl_statements[0]
    assert "EXTERNAL_VOLUME = 'EXT_VOL_MAIN'" in ice_prev.ddl_statements[0]

    # 4. Interactive Table
    int_prev = provisioner.preview_table(
        user_principal="alice@example.org",
        table_family=ObjectFamily.INTERACTIVE_TABLE,
        database="APP_DB",
        schema="CORE_SCHEMA",
        table="INT_SUMMARY",
        target_lag="1 minute",
        warehouse="COMPUTE_WH",
        as_select_sql="SELECT ID, PAYLOAD FROM APP_DB.CORE_SCHEMA.SOURCE_TBL",
    )
    assert "CREATE INTERACTIVE TABLE APP_DB.CORE_SCHEMA.INT_SUMMARY" in int_prev.ddl_statements[0]

    # 5. Dynamic Table
    dyn_prev = provisioner.preview_table(
        user_principal="alice@example.org",
        table_family=ObjectFamily.DYNAMIC_TABLE,
        database="APP_DB",
        schema="CORE_SCHEMA",
        table="DYN_ROLLUP",
        target_lag="5 minutes",
        warehouse="COMPUTE_WH",
        refresh_mode="INCREMENTAL",
        as_select_sql="SELECT ID, COUNT(*) AS CNT FROM APP_DB.CORE_SCHEMA.SOURCE_TBL GROUP BY ID",
    )
    assert "CREATE DYNAMIC TABLE APP_DB.CORE_SCHEMA.DYN_ROLLUP" in dyn_prev.ddl_statements[0]
    assert "REFRESH_MODE = INCREMENTAL" in dyn_prev.ddl_statements[0]


def test_provisioner_stages_and_subpath_validation_and_role_denials(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, provisioner, _ = governance_env
    authz.establish_interactive_session("alice@example.org", requested_role="ROLE_BUILDER")

    # Internal stage
    int_stg = provisioner.preview_stage(
        user_principal="alice@example.org",
        database="APP_DB",
        schema="CORE_SCHEMA",
        stage="INTERNAL_ASSETS",
        is_external=False,
    )
    assert "ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')" in int_stg.ddl_statements[0]

    # External stage with STORAGE_INTEGRATION (and block inline credentials)
    with pytest.raises(SnowflakeGovernanceError):
        provisioner.preview_stage(
            user_principal="alice@example.org",
            database="APP_DB",
            schema="CORE_SCHEMA",
            stage="BAD_EXT_STAGE",
            is_external=True,
            external_url="s3://bucket/path?aws_secret=123",
            storage_integration="S3_INT",
        )

    ext_stg = provisioner.preview_stage(
        user_principal="alice@example.org",
        database="APP_DB",
        schema="CORE_SCHEMA",
        stage="EXT_ASSETS",
        is_external=True,
        external_url="s3://enterprise-bucket/langflow-artifacts/",
        storage_integration="S3_STORAGE_INT",
    )
    assert "STORAGE_INTEGRATION = S3_STORAGE_INT" in ext_stg.ddl_statements[0]

    # Stage sub-path traversal rejection
    with pytest.raises(SnowflakeGovernanceError):
        provisioner.validate_stage_subpath(
            database="APP_DB",
            schema="CORE_SCHEMA",
            stage="INTERNAL_ASSETS",
            subpath="../../etc/passwd",
            active_role="ROLE_BUILDER",
        )

    # Role denial test: ROLE_VIEWER cannot provision any object and gets HTTP 403 + audit entry
    authz.establish_interactive_session("bob@example.org", requested_role="ROLE_VIEWER")
    before_audit_len = len(authz.audit_log)
    with pytest.raises(SnowflakeGovernanceError) as viewer_denied:
        provisioner.preview_database(user_principal="bob@example.org", database="UNAUTHORIZED_DB")
    assert viewer_denied.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert viewer_denied.value.http_status == 403
    assert CANONICAL_ROLE_DENIED_MESSAGE in viewer_denied.value.message
    assert len(authz.audit_log) == before_audit_len + 1
    assert authz.audit_log[-1].status == "DENIED"
    assert authz.audit_log[-1].error_code == "AUTHZ_ROLE_DENIED"


def test_decorator_guard_enforces_permissions_and_classifies_exceptions(
    governance_env: tuple[SnowflakeAuthzService, SnowflakeObjectProvisioner, SnowflakeGovernanceMiddleware],
) -> None:
    authz, _, _ = governance_env
    authz.establish_interactive_session("bob@example.org", requested_role="ROLE_VIEWER")

    @require_snowflake_permission(
        authz,
        OperationType.RUN_PYTHON_INTERPRETER,
        target_ref_arg="package_name",
        required_privilege="SNOWFLAKE.PYPI_REPOSITORY_USER",
        disclosable=True,
    )
    def run_isolated_python(*, user_principal: str, package_name: str) -> str:
        return f"loaded:{package_name}"

    with pytest.raises(SnowflakeGovernanceError) as py_denied:
        run_isolated_python(user_principal="bob@example.org", package_name="numpy==2.1.0")
    assert py_denied.value.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
    assert py_denied.value.required_privilege == "SNOWFLAKE.PYPI_REPOSITORY_USER"
