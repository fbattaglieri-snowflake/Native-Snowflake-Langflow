"""Comprehensive unit and integration tests for Context Architecture & Unified Flow Activity (CTX-ARCH-01..07, W2-R4-06B).

Validates:
- CTX-ARCH-01 & CTX-ARCH-02: Topbar outside vs inside `/flow/<flow_id>` (`[ ❄️ Context ]` button hidden outside flow, visible inside flow).
- CTX-ARCH-03: Strict per-flow context isolation (`contexts_by_flow_id`, `CURRENT_SNOWFLAKE_FLOW_ID`, zero cross-flow contamination).
- CTX-ARCH-04: Streamlined `[ ❄️ Context ]` modal (`❄️ Flow Context`, removal of redundant `#sflf-ctx-db` / `#sflf-ctx-schema` dropdowns from active UI).
- CTX-ARCH-05: 2-block hierarchy (Block 1: 9 schema-scoped categories with `<5ms` `full_databases`/`full_schemas` cascade; Block 2: account-level `postgres_instances` -> `table_name`).
- CTX-ARCH-05B: 3-level reactive canvas sync (`sync_flow_node_templates_sqlite` + React Fiber & Radix combobox sync).
- CTX-ARCH-06: Unified real-time `LANGFLOW_FLOW_ACTIVITY` table DDL, `FlowActivityRecord`, `GET /api/v2/traces` enrichment, and MCP `traces_tables["activity"]`.
- CTX-ARCH-07 #1: Non-destructive role switch intersection (`compute_role_effective_context` + `inaccessible_objects` lock badges).
- CTX-ARCH-07 #2: Auto-registration of newly provisioned objects in the active flow's context (`register_object_in_context`).
- CTX-ARCH-07 #3: Flow export/import/duplicate context persistence (`data.snowflake_context` in SQLite `flow.data`).
- CTX-ARCH-07 #4 & W2-R4-06B: Data Sources purity (exclusion of technical agent-memory backing tables while keeping them in Memory Base).
- CTX-ARCH-07 #5: Soft-delete in Flow Activity UI (`DELETED_IN_UI_AT = CURRENT_TIMESTAMP()`, retained in Snowflake).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from lfx_snowflake.api_routes import SnowflakeAPIRouter
from lfx_snowflake.components.data_sources import (
    SnowflakeDataSourcesComponent,
    filter_business_data_source_objects,
    is_technical_memory_table,
)
from lfx_snowflake.fastapi_live_bridge import (
    CURRENT_SNOWFLAKE_FLOW_ID,
    LIVE_BRIDGE,
    LiveSnowflakeBridge,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_analyst import (
    SnowflakeCortexAnalystComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_cortex_search import (
    SnowflakeCortexSearchComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_iceberg_tables import (
    SnowflakeIcebergTablesComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_postgres import (
    SnowflakePostgresComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_tables import (
    SnowflakeTablesComponent,
)
from lfx_snowflake.native_category_components.data_source.snowflake_views import (
    SnowflakeViewsComponent,
)
from lfx_snowflake.components.knowledge_and_memories import (
    SnowflakeMemoryBaseComponent,
    SnowflakeMemoryManagerComponent,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_stage_files import (
    SnowflakeStageFileSystemComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_function_udf import (
    SnowflakeFunctionComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import (
    SnowflakeObjectProvisionerComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_stored_procedure import (
    SnowflakeStoredProcedureComponent,
)
from lfx_snowflake.observability import (
    CANONICAL_ACTIVE_FLOW_ACTIVITY_VIEW,
    CANONICAL_FLOW_ACTIVITY_TABLE,
    FLOW_ACTIVITY_GOVERNED_COLUMNS,
    FlowActivityEventType,
    FlowActivityRecord,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_ctx_arch_01_02_04_frontend_topbar_and_streamlined_context_modal() -> None:
    """Verify CTX-ARCH-01, CTX-ARCH-02, CTX-ARCH-04: Topbar [ ❄️ Context ] button visibility and streamlined modal."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    topbar_tsx = (REPO_ROOT / "frontend_patches" / "topbar_and_branding.tsx").read_text(encoding="utf-8")
    role_tsx = (REPO_ROOT / "frontend_patches" / "role_selector_and_denial_banner.tsx").read_text(encoding="utf-8")
    preserved_js = (REPO_ROOT / "frontend_patches" / "preserved_provisioning_and_memories_concept.js").read_text(
        encoding="utf-8"
    )

    # 1. Single [ ❄️ Context ] button in topbar with #29B5E8 styling, hidden outside /flow/<flow_id>
    assert "❄️ Context" in ui_js
    assert 'data-testid="sflf-context-btn"' in ui_js
    assert "#29B5E8" in ui_js
    assert 'hubBtn.style.display = currentFlowId ? "inline-flex" : "none"' in ui_js
    assert "❄️ Context" in topbar_tsx
    assert "❄️ Context" in role_tsx

    # 2. Modal header renamed to ❄️ Flow Context and redundant #sflf-ctx-db / #sflf-ctx-schema dropdowns removed from active DOM
    assert "❄️ Flow Context" in ui_js
    assert 'id="sflf-ctx-db"' not in ui_js
    assert 'id="sflf-ctx-schema"' not in ui_js
    assert 'data-hub-mode="context-only"' in ui_js
    assert 'id="sflf-preserved-sections-compat"' in ui_js
    assert "SNOWFLAKE_PRESERVED_PROVISIONING_AND_MEMORIES_CONCEPT" in preserved_js

    # 3. 2-block tree: Block 1 has 9 schema-scoped categories (no memories or postgres_instances), Block 2 is #sflf-sec0-postgres-block
    assert 'id="sflf-sec0-postgres-block"' in ui_js
    assert "Snowflake Postgres (Account-Level Instances)" in ui_js
    assert "Non accessibile col ruolo" in ui_js

    # 4. 3-level reactive canvas sync: React Fiber + Radix comboboxes
    assert "__reactFiber$" in ui_js
    assert "__reactProps$" in ui_js
    assert '[role="combobox"]' in ui_js


def test_ctx_arch_03_strict_per_flow_context_isolation() -> None:
    """Verify CTX-ARCH-03: Flow A and Flow B have completely isolated contexts and Flow C starts clean without inheriting Flow A."""
    flow_a = "11111111-1111-4111-8111-111111111111"
    flow_b = "22222222-2222-4222-8222-222222222222"
    flow_c = "33333333-3333-4333-8333-333333333333"

    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_a, None)
    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_b, None)
    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_c, None)

    LIVE_BRIDGE.update_selected_context(
        {
            "database": "RETAIL_DB",
            "schema": "SALES",
            "databases": ["RETAIL_DB"],
            "schemas_by_db": {"RETAIL_DB": ["SALES"]},
            "postgres_instances": ["PG_RETAIL_OLTP"],
            "objects_by_db_schema": {
                "RETAIL_DB.SALES": {
                    "tables": ["ORDERS_A", "CUSTOMERS_A"],
                    "views": ["V_SALES_A"],
                }
            },
            "selected_objects": {
                "RETAIL_DB.SALES": {
                    "tables": ["ORDERS_A"],
                    "views": ["V_SALES_A"],
                }
            },
        },
        flow_id=flow_a,
    )

    LIVE_BRIDGE.update_selected_context(
        {
            "database": "FINANCE_DB",
            "schema": "LEDGER",
            "databases": ["FINANCE_DB"],
            "schemas_by_db": {"FINANCE_DB": ["LEDGER"]},
            "postgres_instances": ["PG_FINANCE_CORE"],
            "objects_by_db_schema": {
                "FINANCE_DB.LEDGER": {
                    "tables": ["INVOICES_B", "JOURNAL_B"],
                    "views": ["V_LEDGER_B"],
                }
            },
            "selected_objects": {
                "FINANCE_DB.LEDGER": {
                    "tables": ["INVOICES_B"],
                    "views": ["V_LEDGER_B"],
                }
            },
        },
        flow_id=flow_b,
    )

    # Verify Flow A vs Flow B isolation
    assert LIVE_BRIDGE.get_context_databases(flow_id=flow_a) == ["RETAIL_DB"]
    assert LIVE_BRIDGE.get_context_databases(flow_id=flow_b) == ["FINANCE_DB"]
    assert LIVE_BRIDGE.get_context_objects("RETAIL_DB", "SALES", "tables", flow_id=flow_a) == ["ORDERS_A"]
    assert LIVE_BRIDGE.get_context_objects("FINANCE_DB", "LEDGER", "tables", flow_id=flow_b) == ["INVOICES_B"]
    assert LIVE_BRIDGE.get_context_postgres_instances(flow_id=flow_a) == ["PG_RETAIL_OLTP"]
    assert LIVE_BRIDGE.get_context_postgres_instances(flow_id=flow_b) == ["PG_FINANCE_CORE"]

    # Verify fresh Flow C does NOT inherit Flow A or Flow B selections
    ctx_c = LIVE_BRIDGE.get_flow_context(flow_c)
    assert "RETAIL_DB" not in (ctx_c.get("databases") or [])
    assert "FINANCE_DB" not in (ctx_c.get("databases") or [])
    assert ctx_c.get("selected_objects") == {}
    assert ctx_c.get("postgres_instances") == []

    # Verify ContextVar propagation into components without cross-flow contamination
    comp_a = SnowflakeTablesComponent()
    bc_a: dict[str, Any] = {
        "database": {"value": "RETAIL_DB", "options": []},
        "schema_name": {"value": "SALES", "options": []},
        "table_name": {"value": "", "options": []},
    }
    tok = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_a)
    try:
        updated_a = comp_a.update_build_config(bc_a, "RETAIL_DB", "database")
        assert updated_a["database"]["options"] == ["RETAIL_DB"]
        assert updated_a["schema_name"]["options"] == ["SALES"]
        assert updated_a["table_name"]["options"] == ["ORDERS_A"]
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)

    comp_b = SnowflakeTablesComponent()
    bc_b: dict[str, Any] = {
        "database": {"value": "FINANCE_DB", "options": []},
        "schema_name": {"value": "LEDGER", "options": []},
        "table_name": {"value": "", "options": []},
    }
    tok_b = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_b)
    try:
        updated_b = comp_b.update_build_config(bc_b, "FINANCE_DB", "database")
        assert updated_b["database"]["options"] == ["FINANCE_DB"]
        assert updated_b["schema_name"]["options"] == ["LEDGER"]
        assert updated_b["table_name"]["options"] == ["INVOICES_B"]
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(tok_b)


def test_ctx_arch_05_account_level_postgres_and_wildcard_cascade() -> None:
    """Verify CTX-ARCH-05: Account-level Snowflake Postgres cascading to table_name and full_databases/full_schemas wildcard cascade."""
    flow_pg = "44444444-4444-4444-8444-444444444444"
    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_pg, None)

    LIVE_BRIDGE.update_selected_context(
        {
            "database": "ANALYTICS_DB",
            "schema": "MART",
            "databases": ["ANALYTICS_DB"],
            "schemas_by_db": {"ANALYTICS_DB": ["MART", "STAGING"]},
            "full_databases": ["ANALYTICS_DB"],
            "full_schemas": ["ANALYTICS_DB.MART"],
            "postgres_instances": ["PG_ACCOUNT_PROD", "PG_ACCOUNT_DEV"],
            "postgres_tables_by_instance": {
                "PG_ACCOUNT_PROD": ["pg_orders", "pg_customers", "MEM_CHAT_WINDOW_HISTORY"],
                "PG_ACCOUNT_DEV": ["pg_sandbox_events"],
            },
            "objects_by_db_schema": {
                "ANALYTICS_DB.MART": {
                    "tables": ["FACT_REVENUE", "DIM_PRODUCT", "MEM_GRAPH_STATE_CHECKPOINTS"],
                    "views": ["V_REVENUE_SUMMARY"],
                }
            },
        },
        flow_id=flow_pg,
    )

    # Wildcard full_databases / full_schemas returns all business objects without requiring explicit per-object selected_objects list
    mart_tables = LIVE_BRIDGE.get_context_objects("ANALYTICS_DB", "MART", "tables", flow_id=flow_pg)
    assert "FACT_REVENUE" in mart_tables
    assert "DIM_PRODUCT" in mart_tables
    assert "MEM_GRAPH_STATE_CHECKPOINTS" not in mart_tables

    # Account-level SnowflakePostgresComponent starts at postgres_instance -> table_name
    pg_comp = SnowflakePostgresComponent()
    pg_comp._flow_id = flow_pg
    pg_bc: dict[str, Any] = {
        "postgres_instance": {"value": "PG_ACCOUNT_PROD", "options": []},
        "postgres_database": {"value": "postgres"},
        "postgres_schema": {"value": "public"},
        "table_name": {"value": "", "options": []},
    }
    pg_updated = pg_comp.update_build_config(pg_bc, "PG_ACCOUNT_PROD", "postgres_instance")
    assert pg_updated["postgres_instance"]["options"] == ["PG_ACCOUNT_PROD", "PG_ACCOUNT_DEV"]
    assert pg_updated["table_name"]["options"] == ["pg_orders", "pg_customers"]
    assert pg_updated["table_name"]["value"] == "pg_orders"

    # Switching postgres_instance cascades table_name options
    pg_updated_dev = pg_comp.update_build_config(pg_bc, "PG_ACCOUNT_DEV", "postgres_instance")
    assert pg_updated_dev["table_name"]["options"] == ["pg_sandbox_events"]
    assert pg_updated_dev["table_name"]["value"] == "pg_sandbox_events"


def test_ctx_arch_05b_and_07_3_sqlite_node_template_sync_and_export_import_persistence(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Verify CTX-ARCH-05B & CTX-ARCH-07 #3: SQLite flow.data.snowflake_context persistence, node template sync, and import/duplicate hydration."""
    db_file = tmp_path / "langflow.db"
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))

    flow_id = "55555555-5555-4555-8555-555555555555"
    initial_flow_data = {
        "nodes": [
            {
                "id": "SnowflakeTables-node1",
                "data": {
                    "type": "SnowflakeTablesComponent",
                    "node": {
                        "display_name": "Snowflake Tables",
                        "template": {
                            "database": {"value": "OLD_DB", "options": ["OLD_DB"]},
                            "schema_name": {"value": "OLD_SCHEMA", "options": ["OLD_SCHEMA"]},
                            "table_name": {"value": "OLD_TABLE", "options": ["OLD_TABLE"]},
                        },
                    },
                },
            },
            {
                "id": "SnowflakePostgres-node2",
                "data": {
                    "type": "SnowflakePostgresComponent",
                    "node": {
                        "display_name": "Snowflake Postgres",
                        "template": {
                            "postgres_instance": {"value": "OLD_PG", "options": ["OLD_PG"]},
                            "table_name": {"value": "old_pg_tbl", "options": ["old_pg_tbl"]},
                        },
                    },
                },
            },
        ],
        "edges": [],
    }

    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE flow (id TEXT PRIMARY KEY, name TEXT, folder_id TEXT, data TEXT)")
    conn.execute(
        "INSERT INTO flow (id, name, folder_id, data) VALUES (?, ?, ?, ?)",
        (flow_id, "Test Sync Flow", "folder-1", json.dumps(initial_flow_data)),
    )
    conn.commit()
    conn.close()

    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "_resolve_writable_sqlite_db_path", lambda: db_file)

    upd_res = bridge.update_selected_context(
        {
            "database": "NEW_DB",
            "schema": "NEW_SCHEMA",
            "databases": ["NEW_DB"],
            "schemas_by_db": {"NEW_DB": ["NEW_SCHEMA"]},
            "postgres_instances": ["PG_NEW_INST"],
            "postgres_tables_by_instance": {"PG_NEW_INST": ["pg_live_orders"]},
            "objects_by_db_schema": {
                "NEW_DB.NEW_SCHEMA": {
                    "tables": ["NEW_FACT_ORDERS"],
                }
            },
            "selected_objects": {
                "NEW_DB.NEW_SCHEMA": {
                    "tables": ["NEW_FACT_ORDERS"],
                }
            },
        },
        flow_id=flow_id,
    )

    assert "SnowflakeTables-node1" in upd_res["updated_node_templates"]
    assert "SnowflakePostgres-node2" in upd_res["updated_node_templates"]
    assert upd_res["updated_node_templates"]["SnowflakeTables-node1"]["database"] == {
        "options": ["NEW_DB"],
        "value": "NEW_DB",
    }
    assert upd_res["updated_node_templates"]["SnowflakeTables-node1"]["table_name"] == {
        "options": ["NEW_FACT_ORDERS"],
        "value": "NEW_FACT_ORDERS",
    }
    assert upd_res["updated_node_templates"]["SnowflakePostgres-node2"]["postgres_instance"] == {
        "options": ["PG_NEW_INST"],
        "value": "PG_NEW_INST",
    }
    assert upd_res["updated_node_templates"]["SnowflakePostgres-node2"]["table_name"] == {
        "options": ["pg_live_orders"],
        "value": "pg_live_orders",
    }

    # Verify SQLite flow.data now embeds snowflake_context for JSON Export/Import/Duplicate
    conn = sqlite3.connect(str(db_file))
    saved_data = json.loads(conn.execute("SELECT data FROM flow WHERE id = ?", (flow_id,)).fetchone()[0])
    conn.close()
    assert saved_data["snowflake_context"]["databases"] == ["NEW_DB"]
    assert saved_data["snowflake_context"]["postgres_instances"] == ["PG_NEW_INST"]

    # Duplicating/importing the exported flow payload into a new flow_id hydrates its context automatically
    duplicated_flow_id = "66666666-6666-4666-8666-666666666666"
    hydrated = bridge.hydrate_flow_context_from_flow_payload(
        {"id": duplicated_flow_id, "name": "Duplicated Flow", "data": saved_data},
        flow_id=duplicated_flow_id,
    )
    assert hydrated is not None
    assert bridge.get_context_databases(flow_id=duplicated_flow_id) == ["NEW_DB"]
    assert bridge.get_context_objects("NEW_DB", "NEW_SCHEMA", "tables", flow_id=duplicated_flow_id) == [
        "NEW_FACT_ORDERS"
    ]
    assert bridge.get_context_postgres_instances(flow_id=duplicated_flow_id) == ["PG_NEW_INST"]


def test_ctx_arch_07_1_non_destructive_role_switch_intersection() -> None:
    """Verify CTX-ARCH-07 #1: Switching active role computes non-destructive intersection and returns inaccessible_objects with lock badges."""
    router = SnowflakeAPIRouter(
        authenticated_user="<SNOWFLAKE_USER>",
        available_roles=["DATA_ENGINEER_ROLE", "ANALYST_ROLE", "PUBLIC"],
        initial_role="DATA_ENGINEER_ROLE",
    )
    flow_id = "77777777-7777-4777-8777-777777777777"

    router.update_session_context(
        {
            "flow_id": flow_id,
            "database": "CORP_DB",
            "schema": "RESTRICTED",
            "databases": ["CORP_DB", "PUBLIC_DB"],
            "schemas_by_db": {
                "CORP_DB": ["RESTRICTED", "ANALYTICS"],
                "PUBLIC_DB": ["PUBLIC"],
            },
            "postgres_instances": ["PG_CORP_RESTRICTED", "PG_PUBLIC_SHARED"],
            "objects_by_db_schema": {
                "CORP_DB.RESTRICTED": {"tables": ["PAYROLL_SECRET"]},
                "CORP_DB.ANALYTICS": {"tables": ["SALES_KPIS", "HR_SALARY_BANDS"]},
                "PUBLIC_DB.PUBLIC": {"tables": ["OPEN_REF_DATA"]},
            },
            "selected_objects": {
                "CORP_DB.RESTRICTED": {"tables": ["PAYROLL_SECRET"]},
                "CORP_DB.ANALYTICS": {"tables": ["SALES_KPIS", "HR_SALARY_BANDS"]},
                "PUBLIC_DB.PUBLIC": {"tables": ["OPEN_REF_DATA"]},
            },
        },
        flow_id=flow_id,
    )

    # Configure ANALYST_ROLE accessible catalog (cannot see RESTRICTED schema, HR_SALARY_BANDS, or PG_CORP_RESTRICTED)
    router.role_accessible_catalog["ANALYST_ROLE"] = {
        "databases": ["CORP_DB", "PUBLIC_DB"],
        "schemas_by_db": {
            "CORP_DB": ["ANALYTICS"],
            "PUBLIC_DB": ["PUBLIC"],
        },
        "postgres_instances": ["PG_PUBLIC_SHARED"],
        "objects_by_db_schema": {
            "CORP_DB.ANALYTICS": {"tables": ["SALES_KPIS"]},
            "PUBLIC_DB.PUBLIC": {"tables": ["OPEN_REF_DATA"]},
        },
    }

    switch_resp = router.switch_active_role("ANALYST_ROLE", flow_id=flow_id)
    assert switch_resp.status_code == 200
    body = switch_resp.body

    # Stored context is preserved intact (non-destructive)
    assert "RESTRICTED" in body["selected_context"]["schemas_by_db"]["CORP_DB"]
    assert "HR_SALARY_BANDS" in body["selected_context"]["objects_by_db_schema"]["CORP_DB.ANALYTICS"]["tables"]
    assert "PG_CORP_RESTRICTED" in body["selected_context"]["postgres_instances"]

    # Effective context contains only objects accessible to ANALYST_ROLE
    assert body["effective_context"]["schemas_by_db"]["CORP_DB"] == ["ANALYTICS"]
    assert body["effective_context"]["objects_by_db_schema"]["CORP_DB.ANALYTICS"]["tables"] == ["SALES_KPIS"]
    assert body["effective_context"]["postgres_instances"] == ["PG_PUBLIC_SHARED"]

    # Inaccessible objects carry explicit lock badge
    inaccessible_fqns = {item["fqn"] for item in body["inaccessible_objects"]}
    assert "CORP_DB.RESTRICTED" in inaccessible_fqns
    assert "CORP_DB.ANALYTICS.HR_SALARY_BANDS" in inaccessible_fqns
    assert "PG_CORP_RESTRICTED" in inaccessible_fqns
    assert all(
        item["locked_badge"] == "🔒 Non accessibile col ruolo ANALYST_ROLE"
        for item in body["inaccessible_objects"]
    )

    # Switching back to DATA_ENGINEER_ROLE restores full visibility immediately
    restore_resp = router.switch_active_role("DATA_ENGINEER_ROLE", flow_id=flow_id)
    assert restore_resp.status_code == 200
    assert restore_resp.body["hidden_by_role_count"] == 0
    assert (
        "HR_SALARY_BANDS"
        in restore_resp.body["effective_context"]["objects_by_db_schema"]["CORP_DB.ANALYTICS"]["tables"]
    )


def test_ctx_arch_07_2_auto_registration_of_provisioned_objects_in_flow_context() -> None:
    """Verify CTX-ARCH-07 #2: Objects created via components are automatically registered in the active flow's context."""
    flow_id = "88888888-8888-4888-8888-888888888888"
    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_id, None)
    LIVE_BRIDGE.update_selected_context(
        {
            "database": "PROVISION_DB",
            "schema": "DEV_SCHEMA",
            "databases": ["PROVISION_DB"],
            "schemas_by_db": {"PROVISION_DB": ["DEV_SCHEMA"]},
            "objects_by_db_schema": {"PROVISION_DB.DEV_SCHEMA": {"tables": ["EXISTING_TBL"]}},
            "selected_objects": {"PROVISION_DB.DEV_SCHEMA": {"tables": ["EXISTING_TBL"]}},
        },
        flow_id=flow_id,
    )

    tok = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_id)
    try:
        # 1. SnowflakeObjectProvisionerComponent creates a new table
        prov = SnowflakeObjectProvisionerComponent(
            object_kind="table",
            table_subtype="STANDARD",
            database="PROVISION_DB",
            schema_name="DEV_SCHEMA",
            object_name="AUTO_ADDED_ORDERS",
            columns_spec="ID INT, AMOUNT NUMBER(10,2)",
            dry_run=False,
            confirmed=True,
        )
        prov_res = prov.provision_object()
        assert prov_res.data["ok"] is True
        assert "AUTO_ADDED_ORDERS" in LIVE_BRIDGE.get_context_objects(
            "PROVISION_DB", "DEV_SCHEMA", "tables", flow_id=flow_id
        )

        # 2. SnowflakeObjectProvisionerComponent creates a new Snowflake Postgres instance
        prov_pg = SnowflakeObjectProvisionerComponent(
            object_kind="postgres",
            database="PROVISION_DB",
            schema_name="DEV_SCHEMA",
            object_name="PG_AUTO_ADDED_INST",
            postgres_instance_name="PG_AUTO_ADDED_INST",
            dry_run=False,
            confirmed=True,
        )
        prov_pg.provision_object()
        assert "PG_AUTO_ADDED_INST" in LIVE_BRIDGE.get_context_postgres_instances(flow_id=flow_id)

        # 3. SnowflakeStageFileSystemComponent creates a new Stage
        stg = SnowflakeStageFileSystemComponent(
            database="PROVISION_DB",
            schema_name="DEV_SCHEMA",
            stage_name="AUTO_ADDED_STAGE",
            new_stage_name="AUTO_ADDED_STAGE",
            operation="Create Internal Stage",
        )
        stg.run_stage_op()
        assert "AUTO_ADDED_STAGE" in LIVE_BRIDGE.get_context_objects(
            "PROVISION_DB", "DEV_SCHEMA", "stages", flow_id=flow_id
        )

        # 4. SnowflakeFunctionComponent creates a new UDF
        udf = SnowflakeFunctionComponent(
            action_mode="CREATE OR REPLACE",
            database="PROVISION_DB",
            schema_name="DEV_SCHEMA",
            function_name="FN_AUTO_SCORE",
            signature_args="X FLOAT",
            return_type="FLOAT",
            language="SQL",
            body="X * 2.0",
        )
        udf.run_function()
        assert "FN_AUTO_SCORE" in LIVE_BRIDGE.get_context_objects(
            "PROVISION_DB", "DEV_SCHEMA", "functions", flow_id=flow_id
        )

        # 5. SnowflakeStoredProcedureComponent creates a new Procedure
        sp = SnowflakeStoredProcedureComponent(
            action_mode="CREATE OR REPLACE",
            database="PROVISION_DB",
            schema_name="DEV_SCHEMA",
            procedure_name="SP_AUTO_REFRESH",
            signature_args="",
            return_type="VARCHAR",
            language="SQL",
            body="BEGIN RETURN 'OK'; END;",
        )
        sp.run_procedure()
        assert "SP_AUTO_REFRESH" in LIVE_BRIDGE.get_context_objects(
            "PROVISION_DB", "DEV_SCHEMA", "procedures", flow_id=flow_id
        )
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)


def test_ctx_arch_07_4_data_sources_purity_vs_memory_base() -> None:
    """Verify CTX-ARCH-07 #4 & W2-R4-06B: Technical memory tables are excluded from Data Sources but available in Memory Base."""
    flow_id = "99999999-9999-4999-8999-999999999999"
    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_id, None)
    LIVE_BRIDGE.update_selected_context(
        {
            "database": "AGENT_DB",
            "schema": "CORE",
            "databases": ["AGENT_DB"],
            "schemas_by_db": {"AGENT_DB": ["CORE"]},
            "full_schemas": ["AGENT_DB.CORE"],
            "objects_by_db_schema": {
                "AGENT_DB.CORE": {
                    "tables": [
                        "BUSINESS_CUSTOMERS",
                        "BUSINESS_TRANSACTIONS",
                        "MEM_GRAPH_STATE_CHECKPOINTS",
                        "MEM_CHAT_WINDOW_HISTORY",
                        "MEM_RUNNING_SUMMARIES",
                        "MEM_USER_ENTITY_PROFILES",
                        "MEM_EPISODIC_EXPERIENCES",
                        "LANGFLOW_MEM_AGENT_HISTORY",
                    ],
                    "views": ["V_BUSINESS_CUSTOMERS", "MEM_CHAT_WINDOW_HISTORY"],
                    "iceberg_tables": ["ICEBERG_SALES", "LANGFLOW_MEM_ICEBERG"],
                }
            },
        },
        flow_id=flow_id,
    )

    # Data Sources components exclude all technical memory tables
    tables_for_ds = LIVE_BRIDGE.get_context_objects("AGENT_DB", "CORE", "tables", flow_id=flow_id)
    assert tables_for_ds == ["BUSINESS_CUSTOMERS", "BUSINESS_TRANSACTIONS"]

    views_for_ds = LIVE_BRIDGE.get_context_objects("AGENT_DB", "CORE", "views", flow_id=flow_id)
    assert views_for_ds == ["V_BUSINESS_CUSTOMERS"]

    iceberg_for_ds = LIVE_BRIDGE.get_context_objects("AGENT_DB", "CORE", "iceberg_tables", flow_id=flow_id)
    assert iceberg_for_ds == ["ICEBERG_SALES"]

    # When exclude_memory_tables=False (used by Memory Base inspection), memory tables remain accessible
    all_tables = LIVE_BRIDGE.get_context_objects(
        "AGENT_DB", "CORE", "tables", flow_id=flow_id, exclude_memory_tables=False
    )
    assert "MEM_GRAPH_STATE_CHECKPOINTS" in all_tables
    assert "LANGFLOW_MEM_AGENT_HISTORY" in all_tables

    # Verify none of the 6 Data Sources components have memory_catalog_ref in inputs
    for ds_cls in (
        SnowflakeTablesComponent,
        SnowflakeViewsComponent,
        SnowflakeIcebergTablesComponent,
        SnowflakeCortexAnalystComponent,
        SnowflakeCortexSearchComponent,
        SnowflakePostgresComponent,
    ):
        input_names = [i.name for i in ds_cls.inputs]
        assert "memory_catalog_ref" not in input_names, f"{ds_cls.__name__} must not expose memory_catalog_ref"

    # Memory Base component continues to manage agent memories in Files & Knowledge
    assert SnowflakeMemoryBaseComponent.display_name == "Memory Base"
    assert SnowflakeMemoryBaseComponent.category == "Files & Knowledge"
    assert len(SnowflakeMemoryManagerComponent.memory_types) == 5


def test_ctx_arch_06_and_07_5_live_bridge_flow_activity_traces_and_soft_delete() -> None:
    """Verify CTX-ARCH-06 & CTX-ARCH-07 #5: LiveSnowflakeBridge /api/v2/traces enrichment and soft-delete without physical deletion."""
    bridge = LiveSnowflakeBridge()
    assert bridge.activity_table.endswith(f".{CANONICAL_FLOW_ACTIVITY_TABLE}")
    assert len(FLOW_ACTIVITY_GOVERNED_COLUMNS) >= 25

    sql_ddl = (REPO_ROOT / "sql" / "02_traces_and_audit_tables.sql").read_text(encoding="utf-8")
    assert f"CREATE TABLE IF NOT EXISTS <DATABASE>.<SCHEMA>.{CANONICAL_FLOW_ACTIVITY_TABLE}" in sql_ddl
    assert f"CREATE OR REPLACE SECURE VIEW <DATABASE>.<SCHEMA>.{CANONICAL_ACTIVE_FLOW_ACTIVITY_VIEW}" in sql_ddl

    flow_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    act_rec = FlowActivityRecord(
        activity_id="act-live-001",
        trace_id="trc-live-001",
        event_type=FlowActivityEventType.FLOW_RUN.value,
        flow_id=flow_id,
        flow_name="Live Sales Agent",
        project_id="proj-live",
        project_name="Sales Folder",
        session_id="sess-live-001",
        user_identity="<SNOWFLAKE_USER>",
        active_role="ANALYST_ROLE",
        context_epoch=5,
        flow_context_json={"databases": ["SALES_DB"]},
        input_text="Show top 5 accounts",
        output_text="Top 5 accounts listed.",
        prompt_tokens=25,
        completion_tokens=15,
        total_tokens=40,
        latency_ms=112.0,
    )
    v2_item = act_rec.to_v2_trace_item()
    bridge._recent_activity_events.insert(0, v2_item)

    traces_resp = bridge.get_ui_flow_activity_traces(flow_id=flow_id, limit=20)
    assert traces_resp["ok"] is True
    assert any(t["id"] == "act-live-001" for t in traces_resp["traces"])

    # Soft-delete the trace from the UI
    sd_resp = bridge.soft_delete_flow_activity(["act-live-001"], flow_id=flow_id)
    assert sd_resp["ok"] is True
    assert sd_resp["physical_delete_executed"] is False
    assert sd_resp["snowflake_retained"] is True
    assert any("UPDATE " in stmt and "SET DELETED_IN_UI_AT = CURRENT_TIMESTAMP()" in stmt for stmt in sd_resp["executed_updates"])

    # Subsequent UI query filters out the soft-deleted item
    traces_after = bridge.get_ui_flow_activity_traces(flow_id=flow_id, limit=20)
    assert not any(t["id"] == "act-live-001" for t in traces_after["traces"])


def test_verifier_ddl_auto_register_cortex_search_pipeline_and_preserved_zero_diff() -> None:
    """Verifier check: DDL auto-registration on execute_sql, Cortex Search 1..4 auto-registration, and preserved_js zero diff."""
    import subprocess
    from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_parse_document import (
        SnowflakeCortexSearchParseDocumentComponent,
    )
    from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_service_creator import (
        SnowflakeCortexSearchServiceCreatorComponent,
    )
    from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_split_text import (
        SnowflakeCortexSearchSplitTextComponent,
    )

    # 1. Verify frontend_patches/preserved_provisioning_and_memories_concept.js has zero git diff
    diff_out = subprocess.check_output(
        ["git", "diff", "frontend_patches/preserved_provisioning_and_memories_concept.js"],
        cwd=str(REPO_ROOT),
        text=True,
    )
    assert diff_out.strip() == "", "preserved_provisioning_and_memories_concept.js must have zero diff"

    # 2. Verify _auto_register_ddl_object_if_create on LiveSnowflakeBridge across DDL kinds
    flow_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    LIVE_BRIDGE.contexts_by_flow_id.pop(flow_id, None)
    LIVE_BRIDGE.update_selected_context(
        {
            "database": "DDL_AUTO_DB",
            "schema": "PUBLIC",
            "databases": ["DDL_AUTO_DB"],
            "schemas_by_db": {"DDL_AUTO_DB": ["PUBLIC"]},
            "objects_by_db_schema": {"DDL_AUTO_DB.PUBLIC": {"tables": ["SEED_TBL"]}},
            "selected_objects": {"DDL_AUTO_DB.PUBLIC": {"tables": ["SEED_TBL"]}},
        },
        flow_id=flow_id,
    )

    LIVE_BRIDGE._auto_register_ddl_object_if_create(
        "CREATE OR REPLACE ICEBERG TABLE DDL_AUTO_DB.PUBLIC.ICE_AUTO_TBL (ID INT)",
        flow_id=flow_id,
    )
    LIVE_BRIDGE._auto_register_ddl_object_if_create(
        "CREATE OR REPLACE SEMANTIC VIEW DDL_AUTO_DB.PUBLIC.SV_AUTO_METRICS AS SELECT 1",
        flow_id=flow_id,
    )
    LIVE_BRIDGE._auto_register_ddl_object_if_create(
        "CREATE POSTGRES INSTANCE IF NOT EXISTS PG_DDL_AUTO_INST",
        flow_id=flow_id,
    )
    assert "ICE_AUTO_TBL" in LIVE_BRIDGE.get_context_objects(
        "DDL_AUTO_DB", "PUBLIC", "iceberg_tables", flow_id=flow_id
    )
    assert "SV_AUTO_METRICS" in LIVE_BRIDGE.get_context_objects(
        "DDL_AUTO_DB", "PUBLIC", "semantic_views", flow_id=flow_id
    )
    assert "PG_DDL_AUTO_INST" in LIVE_BRIDGE.get_context_postgres_instances(flow_id=flow_id)

    # Technical memory table DDLs must NOT pollute Data Sources
    LIVE_BRIDGE._auto_register_ddl_object_if_create(
        "CREATE TABLE IF NOT EXISTS DDL_AUTO_DB.PUBLIC.MEM_GRAPH_STATE_CHECKPOINTS (ID VARCHAR)",
        flow_id=flow_id,
    )
    assert "MEM_GRAPH_STATE_CHECKPOINTS" not in LIVE_BRIDGE.get_context_objects(
        "DDL_AUTO_DB", "PUBLIC", "tables", flow_id=flow_id
    )

    # 3. Verify Cortex Search 1, 2, 3 components auto-register created tables and search service in active flow context
    tok = CURRENT_SNOWFLAKE_FLOW_ID.set(flow_id)
    try:
        c1 = SnowflakeCortexSearchParseDocumentComponent(
            database="DDL_AUTO_DB",
            schema_name="PUBLIC",
            stage_name="DOCS_STAGE",
            target_table="PARSED_DOCS_AUTO",
            action_mode="Create Table & Parse All Stage Files",
        )
        c1.run_parse_document()
        assert "PARSED_DOCS_AUTO" in LIVE_BRIDGE.get_context_objects(
            "DDL_AUTO_DB", "PUBLIC", "tables", flow_id=flow_id
        )

        c2 = SnowflakeCortexSearchSplitTextComponent(
            database="DDL_AUTO_DB",
            schema_name="PUBLIC",
            source_table="PARSED_DOCS_AUTO",
            target_table="CHUNKS_DOCS_AUTO",
            action_mode="Create Table & Split Chunks",
        )
        c2.run_split_text()
        assert "CHUNKS_DOCS_AUTO" in LIVE_BRIDGE.get_context_objects(
            "DDL_AUTO_DB", "PUBLIC", "tables", flow_id=flow_id
        )

        c3 = SnowflakeCortexSearchServiceCreatorComponent(
            database="DDL_AUTO_DB",
            schema_name="PUBLIC",
            service_name="DOCS_SEARCH_SVC_AUTO",
            source_table="CHUNKS_DOCS_AUTO",
            warehouse="COMPUTE_WH",
            action_mode="Create or Replace Cortex Search Service",
        )
        c3.run_search_service_creator()
        assert "DOCS_SEARCH_SVC_AUTO" in LIVE_BRIDGE.get_context_objects(
            "DDL_AUTO_DB", "PUBLIC", "cortex_search_services", flow_id=flow_id
        )
    finally:
        CURRENT_SNOWFLAKE_FLOW_ID.reset(tok)

