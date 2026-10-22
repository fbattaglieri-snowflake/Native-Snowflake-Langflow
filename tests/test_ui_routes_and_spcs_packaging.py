"""Tests for Step I-6 (Roles 6 & 7): Frontend UI/Settings/Import Flow, API Routes, Invariant Categories, SPCS Packaging, SQL DDL, and Backup/Restore."""

from __future__ import annotations

import json
import sqlite3
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
    UPSTREAM_BASELINE_TAG,
    SnowflakeAPIRouter,
    get_bundle_manifest,
    verify_invariant_category_integrity,
)
from scripts.backup_restore_lifecycle import (  # noqa: E402
    build_postgres_fork_backup_plan,
    create_quiesced_sqlite_backup_and_verify_restore,
    verify_preserved_services_fingerprint,
)


def test_branding_topbar_and_profile_menu_contract() -> None:
    """Verify REQ-01: same Langflow UI style, official black Snowflake logo in topbar (home + flow), social links removed, Snowflake Docs link added."""
    manifest = get_bundle_manifest()
    assert manifest["bundle_id"] == BUNDLE_ID == "snowflake"
    assert manifest["upstream_baseline_tag"] == UPSTREAM_BASELINE_TAG == "v1.12.5"

    branding = SNOWFLAKE_BRANDING_CONTRACT
    assert (
        branding["ui_style_policy"]
        == "PRESERVE_LANGFLOW_NATIVE_COMPONENTS_TYPOGRAPHY_SPACING_THEMES"
    )
    logo = branding["topbar_logo"]
    assert logo["placement"] == ["home_topbar_top_left", "flow_editor_topbar_top_left"]
    assert logo["variant"] == "official_snowflake_black_unmodified"
    assert logo["recolor_allowed"] is False
    assert logo["preserve_aspect_ratio"] is True

    assert set(branding["removed_toolbar_links"]) == {"GitHub", "Discord"}
    assert {"GitHub", "Discord", "X"}.issubset(set(branding["removed_profile_menu_links"]))

    docs_items = branding["profile_menu_additions"]
    assert len(docs_items) == 1
    assert docs_items[0]["label"] == "Snowflake Documentation"
    assert docs_items[0]["url"] == "https://docs.snowflake.com/"
    assert docs_items[0]["icon_variant"] == "official_snowflake_color"

    topbar_tsx = (REPO_ROOT / "frontend_patches" / "topbar_and_branding.tsx").read_text(
        encoding="utf-8"
    )
    assert 'data-testid="snowflake-official-logo-black"' in topbar_tsx
    assert 'stroke="#000000"' in topbar_tsx
    assert 'data-testid="snowflake-official-logo-color"' in topbar_tsx
    assert "https://docs.snowflake.com/" in topbar_tsx
    assert "Snowflake Documentation" in topbar_tsx


def test_invariant_categories_and_beta_badges() -> None:
    """Verify REQ-11, REQ-09, REQ-12, REQ-13: Input & Output, Prompt Template, Processing, Flow Control (with Beta badges), LLM Selector, Calculator, Current Date."""
    report = verify_invariant_category_integrity(INVARIANT_CATEGORIES_MANIFEST)
    assert report["passed"] is True
    assert report["missing_categories"] == []
    assert report["missing_beta_badges"] == []
    assert report["missing_standalone_components"] == []
    assert report["discover_other_bundles_invariant"] is True

    json_manifest_path = REPO_ROOT / "frontend_patches" / "invariant_categories_manifest.json"
    frontend_manifest = json.loads(json_manifest_path.read_text(encoding="utf-8"))
    assert set(frontend_manifest["invariant_categories"].keys()) == {
        "Input & Output",
        "Prompt Template",
        "Processing",
        "Flow Control",
    }
    assert frontend_manifest["invariant_categories"]["Flow Control"]["preserve_beta_badges"] == [
        "Listen",
        "Notify",
        "Run Flow",
    ]
    assert set(frontend_manifest["standalone_invariant_components"].keys()) == {
        "LLM Selector",
        "Calculator",
        "Current Date",
    }


def test_role_selector_and_4_code_denial_taxonomy() -> None:
    """Verify REQ-02 & ADR-004: active role switching, cache invalidation, context_epoch increment, and 4 explicit error codes."""
    assert set(ERROR_TAXONOMY_CONTRACT.keys()) == {
        "AUTHZ_ROLE_DENIED",
        "OBJECT_NOT_FOUND_OR_UNAUTHORIZED",
        "UNSUPPORTED_CAPABILITY",
        "TECHNICAL_EXECUTION_ERROR",
    }

    router = SnowflakeAPIRouter(
        available_roles=["ANALYST_ROLE", "DATA_ENGINEER_ROLE", "PUBLIC"],
        initial_role="ANALYST_ROLE",
    )
    router.role_scoped_cache["catalog_key"] = ["cached_table"]

    ctx_resp = router.get_session_context()
    assert ctx_resp.status_code == 200
    assert ctx_resp.body["active_role"] == "ANALYST_ROLE"
    assert ctx_resp.body["context_epoch"] == 1

    # Switch to authorized role -> increments context_epoch and clears role_scoped_cache
    switch_resp = router.switch_active_role("DATA_ENGINEER_ROLE")
    assert switch_resp.status_code == 200
    assert switch_resp.body["active_role"] == "DATA_ENGINEER_ROLE"
    assert switch_resp.body["context_epoch"] == 2
    assert switch_resp.body["cache_invalidated"] is True
    assert router.role_scoped_cache == {}

    # Reject SECONDARY ROLES ALL and service-role fallback
    sec_resp = router.switch_active_role("DATA_ENGINEER_ROLE", use_secondary_roles_all=True)
    assert sec_resp.status_code == 403
    assert sec_resp.body["error_code"] == "AUTHZ_ROLE_DENIED"
    assert (
        sec_resp.body["message"]
        == "Il ruolo attivo (DATA_ENGINEER_ROLE) non consente questa operazione"
    )

    fallback_resp = router.switch_active_role(
        "DATA_ENGINEER_ROLE", allow_service_role_fallback=True
    )
    assert fallback_resp.status_code == 403
    assert fallback_resp.body["error_code"] == "AUTHZ_ROLE_DENIED"

    # Switch to ungranted role -> explicit 403 AUTHZ_ROLE_DENIED + audit entry
    denied_role = router.switch_active_role("ACCOUNTADMIN")
    assert denied_role.status_code == 403
    assert denied_role.body["error_code"] == "AUTHZ_ROLE_DENIED"
    assert (
        denied_role.body["message"]
        == "Il ruolo attivo (DATA_ENGINEER_ROLE) non consente questa operazione"
    )
    assert router.audit_outbox[-1]["outcome"] == "DENIED"
    assert router.audit_outbox[-1]["error_code"] == "AUTHZ_ROLE_DENIED"

    # Verify frontend patch contains the 4 error codes and exact Italian message
    banner_tsx = (
        REPO_ROOT / "frontend_patches" / "role_selector_and_denial_banner.tsx"
    ).read_text(encoding="utf-8")
    for code in ERROR_TAXONOMY_CONTRACT:
        assert code in banner_tsx
    assert "Il ruolo attivo (${payload.active_role}) non consente questa operazione" in banner_tsx


def test_new_flow_import_modes_and_rbac() -> None:
    """Verify REQ-03: Upload from computer, Add from stage, Add from Git with secret redaction, no custom code exec, and role enforcement."""
    assert set(NEW_FLOW_IMPORT_MODES.keys()) == {
        "upload_from_computer",
        "add_from_stage",
        "add_from_git",
    }

    router = SnowflakeAPIRouter(initial_role="ANALYST_ROLE")
    sample_flow = {
        "name": "Customer Support Flow",
        "auto_execute_on_import": True,
        "api_key": "super-secret-value-12345",
        "data": {
            "nodes": [{"id": "n1", "auto_execute_on_import": True, "data": {"token": "abc123456"}}],
            "edges": [],
        },
    }

    # 1. Upload from computer
    up_resp = router.import_flow_from_upload("support_flow.json", sample_flow)
    assert up_resp.status_code == 201
    flow_doc = up_resp.body["flow"]
    assert flow_doc["api_key"] == "[REDACTED_SECRET]"
    assert flow_doc["data"]["nodes"][0]["data"]["token"] == "[REDACTED_SECRET]"
    assert "auto_execute_on_import" not in flow_doc["data"]["nodes"][0]
    assert flow_doc["snowflake_import_metadata"]["custom_code_executed_on_import"] is False

    # 2. Add from stage (authorized stage vs path traversal vs unauthorized stage)
    stg_resp = router.import_flow_from_stage(
        "@<DATABASE>.<SCHEMA>.<STAGE_NAME>/flows/support.json",
        sample_flow,
    )
    assert stg_resp.status_code == 201

    traversal_resp = router.import_flow_from_stage(
        "@<DATABASE>.<SCHEMA>.<STAGE_NAME>/../secrets.json",
        sample_flow,
    )
    assert traversal_resp.status_code == 422
    assert traversal_resp.body["error_code"] == "UNSUPPORTED_CAPABILITY"

    unauth_stg = router.import_flow_from_stage(
        "@OTHER_DB.OTHER_SCHEMA.RESTRICTED_STAGE/flow.json",
        sample_flow,
    )
    assert unauth_stg.status_code == 403
    assert unauth_stg.body["error_code"] == "AUTHZ_ROLE_DENIED"
    # Must not leak existence of restricted object in audit target_object
    assert router.audit_outbox[-1]["target_object"] is None

    # 3. Add from Git (with commit provenance)
    git_resp = router.import_flow_from_git(
        repo_url="https://github.com/example-org/example-flows.git",
        git_ref="main",
        commit_sha="a1b2c3d4e5f60718293a4b5c6d7e8f9012345678",
        file_path="flows/agent_flow.json",
        secret_ref="<DATABASE>.<SCHEMA>.<GIT_SECRET>",
        eai_name="<GIT_EAI_NAME>",
        fetched_flow_payload=sample_flow,
    )
    assert git_resp.status_code == 201
    git_meta = git_resp.body["flow"]["snowflake_import_metadata"]
    assert git_meta["commit_sha"] == "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"
    assert git_meta["secret_ref"] == "[REDACTED_SECRET]"


def test_settings_messages_3_protocols_and_global_variables_readonly_role() -> None:
    """Verify REQ-04: Model Providers, DB Providers, Messages across 3 protocols with signed block invalidation, and read-only role Global Variable."""
    router = SnowflakeAPIRouter(initial_role="ANALYST_ROLE")

    mp_resp = router.get_settings_section("Model Providers")
    assert mp_resp.status_code == 200
    provider_ids = [p["id"] for p in mp_resp.body["config"]["providers"]]
    assert provider_ids == [
        "snowflake_proxy_responses",
        "snowflake_proxy_messages",
        "snowflake_proxy_chat",
        "snowflake_ollama_hosted",
    ]
    # Verify SDK base path for Messages is `/messages` (NOT `/messages/v1`)
    messages_provider = next(
        p for p in mp_resp.body["config"]["providers"] if p["id"] == "snowflake_proxy_messages"
    )
    assert messages_provider["sdk_base_path"] == "/messages"
    assert messages_provider["inference_path"] == "/messages/v1/messages"

    db_resp = router.get_settings_section("DB Providers")
    assert db_resp.status_code == 200
    backend_ids = [b["id"] for b in db_resp.body["config"]["backends"]]
    assert backend_ids == [
        "snowflake_standard_vector",
        "snowflake_hybrid_vector",
        "snowflake_cortex_search",
        "snowflake_postgres_pgvector",
    ]

    # Messages across all 3 protocols + signed block mutation branching
    for proto in ("chat", "responses", "messages"):
        m_resp = router.upsert_protocol_message(
            message_id=f"msg-{proto}-1",
            protocol=proto,
            visible_text="Initial assistant answer",
            signed_replay_blocks={"thinking.signature": "sig-xyz"},
        )
        assert m_resp.status_code == 200
        assert m_resp.body["message"]["signed_block_invalidated"] is False

    # Editing a message that carries a signed replay block invalidates/branches the signed state
    edited_resp = router.upsert_protocol_message(
        message_id="msg-messages-1",
        protocol="messages",
        visible_text="Modified assistant answer",
    )
    assert edited_resp.status_code == 200
    assert edited_resp.body["message"]["signed_block_invalidated"] is True
    assert edited_resp.body["message"]["branched_from_message_id"] == "msg-messages-1"
    assert edited_resp.body["message"]["signed_replay_blocks"] is None

    del_resp = router.delete_protocol_message("msg-responses-1")
    assert del_resp.status_code == 200
    assert del_resp.body["signed_replay_chain_invalidated"] is True

    # Global Variables: attempting to override SNOWFLAKE_ACTIVE_ROLE is rejected with 403 AUTHZ_ROLE_DENIED
    forged_var = router.update_global_variable("SNOWFLAKE_ACTIVE_ROLE", "ACCOUNTADMIN")
    assert forged_var.status_code == 403
    assert forged_var.body["error_code"] == "AUTHZ_ROLE_DENIED"
    assert router.global_variables["SNOWFLAKE_ACTIVE_ROLE"]["value"] == "ANALYST_ROLE"

    valid_var = router.update_global_variable("SNOWFLAKE_DEFAULT_WAREHOUSE", "COMPUTE_WH")
    assert valid_var.status_code == 200
    assert valid_var.body["variable"]["value"] == "COMPUTE_WH"


def test_create_memory_permissions_and_a2a_serve() -> None:
    """Verify REQ-10 [G-A4] (separate CREATE vs use permissions, optional LLM Preprocessing) and REQ-14 (Serve as A2A agent)."""
    router = SnowflakeAPIRouter(initial_role="ANALYST_ROLE")

    # ANALYST_ROLE has MEMORY_USE_EXISTING but NOT MEMORY_CREATE_PHYSICAL
    bind_existing = router.create_or_bind_memory(
        name="CustomerKB",
        embedding_model="snowflake-arctic-embed-l-v2.0",
        vector_database="snowflake_cortex_search",
        batch_size=64,
        llm_preprocessing_enabled=False,
        create_new_physical_store=False,
    )
    assert bind_existing.status_code == 201
    mem = bind_existing.body["memory"]
    assert mem["llm_preprocessing_enabled"] is False
    assert mem["llm_calls_when_disabled"] == 0
    assert mem["owners_rights_isolation_filter_enforced"] is True

    create_physical_denied = router.create_or_bind_memory(
        name="NewPhysicalVectorTable",
        embedding_model="snowflake-arctic-embed-l-v2.0",
        vector_database="snowflake_standard_vector",
        create_new_physical_store=True,
    )
    assert create_physical_denied.status_code == 403
    assert create_physical_denied.body["error_code"] == "AUTHZ_ROLE_DENIED"

    # Switch to DATA_ENGINEER_ROLE which has MEMORY_CREATE_PHYSICAL
    router.switch_active_role("DATA_ENGINEER_ROLE")
    create_physical_ok = router.create_or_bind_memory(
        name="NewPhysicalVectorTable",
        embedding_model="snowflake-arctic-embed-l-v2.0",
        vector_database="snowflake_standard_vector",
        create_new_physical_store=True,
    )
    assert create_physical_ok.status_code == 201
    assert create_physical_ok.body["memory"]["created_physical_store"] is True

    # A2A Serve: Unavailable without both Chat Input and Chat Output
    unavail = router.configure_a2a_serving(
        flow_id="flow-123",
        flow_component_types=["Chat Input"],
        card_metadata={"name": "SupportAgent"},
        action="publish",
    )
    assert unavail.status_code == 422
    assert unavail.body["availability"] == "Unavailable"

    avail_save = router.configure_a2a_serving(
        flow_id="flow-123",
        flow_component_types=["Chat Input", "Chat Output"],
        card_metadata={"name": "SupportAgent", "token": "secret-tok"},
        action="save",
    )
    assert avail_save.status_code == 200
    svc = avail_save.body["a2a_service"]
    assert svc["availability"] == "Available"
    assert svc["published"] is False
    assert svc["credentials_embedded_in_address"] is False
    assert svc["card_metadata"]["token"] == "[REDACTED_SECRET]"


def test_spcs_dockerfile_service_spec_sql_ddl_and_backup_restore(tmp_path: Path) -> None:
    """Verify Step 6 (Role 7): Dockerfile (linux/amd64, UID 1000), service-spec.template.yaml, SQL DDL, and backup/restore lifecycle."""
    dockerfile_text = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "--platform=linux/amd64" in dockerfile_text
    assert "1000:1000" in dockerfile_text
    assert "USER 1000:1000" in dockerfile_text

    spec_text = (REPO_ROOT / "service-spec.template.yaml").read_text(encoding="utf-8")
    assert "uid: 1000" in spec_text
    assert "executeAsCaller: true" in spec_text
    assert "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS" in spec_text
    assert "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS" in spec_text
    assert "<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG" in spec_text

    rbac_sql = (REPO_ROOT / "sql" / "01_governance_rbac.sql").read_text(encoding="utf-8")
    assert "SNOWFLAKE.PYPI_REPOSITORY_USER" in rbac_sql
    assert "GRANT CALLER USAGE" in rbac_sql

    traces_sql = (REPO_ROOT / "sql" / "02_traces_and_audit_tables.sql").read_text(encoding="utf-8")
    assert "LANGFLOW_FLOW_RUNS" in traces_sql
    assert "LANGFLOW_FLOW_SPANS" in traces_sql
    assert "LANGFLOW_AUDIT_LOG" in traces_sql
    assert "ROW ACCESS POLICY" in traces_sql
    assert "deleted_in_ui_at" in traces_sql
    assert "purged_for_retention_at" in traces_sql

    spcs_sql = (REPO_ROOT / "sql" / "03_spcs_service_template.sql").read_text(encoding="utf-8")
    assert "CREATE POSTGRES INSTANCE" in spcs_sql
    assert "ALL_ENDPOINTS_USAGE" in spcs_sql

    # Quiesced SQLite backup + PRAGMA integrity_check + SHA-256 + isolated restore verification
    source_db = tmp_path / "langflow_source.db"
    with sqlite3.connect(str(source_db)) as conn:
        conn.execute("CREATE TABLE flow (id TEXT PRIMARY KEY, name TEXT NOT NULL);")
        conn.execute("INSERT INTO flow (id, name) VALUES ('f1', 'Demo Flow');")
        conn.commit()

    backup_db = tmp_path / "backups" / "langflow_backup.db"
    manifest = create_quiesced_sqlite_backup_and_verify_restore(source_db, backup_db)
    assert manifest.quiesced is True
    assert manifest.integrity_check_result.lower() == "ok"
    assert manifest.restore_verified is True
    assert manifest.sha256_digest is not None and len(manifest.sha256_digest) == 64

    # Snowflake Postgres FORK plan
    pg_plan = build_postgres_fork_backup_plan(
        source_instance="LANGFLOW_PG_MAIN",
        fork_instance="LANGFLOW_PG_FORK_20261017",
    )
    assert pg_plan.backend_type == "snowflake_postgres"
    assert "FORK LANGFLOW_PG_MAIN" in (pg_plan.generated_sql or "")
    assert pg_plan.restore_verified is True

    # Preserved services fingerprint comparison
    before_services = [
        {"name": "SERVICE_ALPHA", "database_name": "DB1", "schema_name": "SCH1", "compute_pool": "POOL1", "status": "READY", "min_instances": 1, "max_instances": 1},
        {"name": "SERVICE_BETA", "database_name": "DB1", "schema_name": "SCH1", "compute_pool": "POOL2", "status": "SUSPENDED", "min_instances": 1, "max_instances": 1},
    ]
    diff_ok = verify_preserved_services_fingerprint(before_services, list(before_services))
    assert diff_ok["passed"] is True
    assert diff_ok["protected_service_count"] == 2


def test_live_fastapi_bridge_and_ui_integration_bundle() -> None:
    """Verify LiveSnowflakeBridge role switching, explicit viewer denial, UI script, and custom component files."""
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    bridge = LiveSnowflakeBridge()
    sw_ok = bridge.switch_role("SNOWFLAKE_LANGFLOW_VIEWER_ROLE")
    assert sw_ok["ok"] is True
    assert sw_ok["active_role"] == "SNOWFLAKE_LANGFLOW_VIEWER_ROLE"
    assert sw_ok["context_epoch"] == 2

    # Viewer role is explicitly denied write/provision operations
    denied = bridge.check_write_privilege_or_deny("PROVISION_TABLE", "DEMO_TABLE")
    assert denied is not None
    assert denied["status_code"] == 403
    assert denied["error_code"] == "AUTHZ_ROLE_DENIED"
    assert "Il ruolo attivo (SNOWFLAKE_LANGFLOW_VIEWER_ROLE)" in denied["message"]

    # Unauthorized role switch is explicitly denied
    sw_bad = bridge.switch_role("NON_EXISTENT_ROLE_XYZ")
    assert sw_bad["ok"] is False
    assert sw_bad["status_code"] == 403
    assert sw_bad["error_code"] == "AUTHZ_ROLE_DENIED"

    # Verify UI integration script and custom component files exist
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert "/api/v1/snowflake/context" in ui_js
    assert "/api/v1/snowflake/role" in ui_js
    assert "https://docs.snowflake.com/" in ui_js
    assert "SNOWFLAKE_SVG_BLACK" in ui_js
    assert "SNOWFLAKE_SVG_COLOR" in ui_js

    comp_dir = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components"
    assert (comp_dir / "data_source" / "snowflake_data_source.py").exists()
    assert (comp_dir / "data_source" / "snowflake_cortex_analyst.py").exists()
    assert not (comp_dir / "data_source" / "snowflake_object_provisioner.py").exists()
    assert (comp_dir / "models_and_agents" / "snowflake_standard_chat.py").exists()
    assert (comp_dir / "models_and_agents" / "snowflake_standard_responses.py").exists()
    assert (comp_dir / "models_and_agents" / "snowflake_standard_messages.py").exists()
    assert (comp_dir / "models_and_agents" / "snowflake_standard_ollama.py").exists()
    assert (comp_dir / "models_and_agents" / "snowflake_cortex_agent.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_object_provisioner.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_stored_procedure.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_function_udf.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_ai_complete.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_ai_function.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_sql_statement.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_cortex_search.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_cortex_analyst_llm.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_node.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_decision.py").exists()
    assert (comp_dir / "llm_operations" / "snowflake_smart_router.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_object_provisioner_fk.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_stage_files.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_knowledge_memory.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_cortex_search_parse_document.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_cortex_search_split_text.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_cortex_search_service_creator.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_cortex_search_stream_task.py").exists()
    assert (comp_dir / "files_and_knowledge" / "snowflake_cortex_analyst_disabled.py").exists()
    assert (comp_dir / "utilities" / "snowflake_python_pypi.py").exists()
    assert (comp_dir / "utilities" / "snowflake_toolguard.py").exists()
