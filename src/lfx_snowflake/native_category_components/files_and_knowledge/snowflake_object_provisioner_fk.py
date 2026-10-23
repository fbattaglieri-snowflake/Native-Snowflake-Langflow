from __future__ import annotations

from typing import Any

try:
    from lfx.io import BoolInput, DropdownInput, IntInput, MultilineInput, Output, StrInput
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import BoolInput, DropdownInput, IntInput, MultilineInput, Output, StrInput

from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import (
    CREATE_NEW_DB_DISABLED_OPTION,
    CREATE_NEW_DB_OPTION,
    CREATE_NEW_SCHEMA_DISABLED_OPTION,
    CREATE_NEW_SCHEMA_OPTION,
    INFRASTRUCTURE_OBJECT_FAMILIES,
    SnowflakeObjectProvisionerSidebarComponent,
    _database_options,
    _schema_options,
)


class SnowflakeObjectProvisionerFKComponent(SnowflakeObjectProvisionerSidebarComponent):
    display_name = "Object Provisioner"
    description = (
        "Hierarchical governed creation of 1. Database -> 2. Schema -> 3. Infrastructure Object in Schema "
        "across the 4 official Snowflake families (1. Table: Standard, Hybrid, Iceberg, Interactive, Dynamic; "
        "2. View: Standard, Secure, Materialized; 3. Stage: Internal, External; 4. Snowflake Postgres) "
        "in Files & Knowledge with dynamic field adaptation, active-role privilege checks, and Dry-Run DDL preview."
    )
    icon = "Snowflake"
    name = "SnowflakeObjectProvisionerFK"

    inputs = [
        DropdownInput(
            name="database",
            display_name="1. Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="new_database_name",
            display_name="New Database Name (when '+ Create New Database...' selected)",
            value="",
            show=False,
        ),
        DropdownInput(
            name="schema_name",
            display_name="2. Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="new_schema_name",
            display_name="New Schema Name (when '+ Create New Schema...' selected)",
            value="",
            show=False,
        ),
        DropdownInput(
            name="object_kind",
            display_name="3. Infrastructure Object Family",
            options=list(INFRASTRUCTURE_OBJECT_FAMILIES),
            value="stage",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="table_subtype",
            display_name="Table Type",
            options=["STANDARD", "HYBRID", "ICEBERG", "INTERACTIVE", "DYNAMIC"],
            value="STANDARD",
            real_time_refresh=True,
            show=False,
        ),
        DropdownInput(
            name="view_type",
            display_name="View Type",
            options=["STANDARD", "SECURE", "MATERIALIZED"],
            value="STANDARD",
            real_time_refresh=True,
            show=False,
        ),
        DropdownInput(
            name="stage_type",
            display_name="Stage Type",
            options=["INTERNAL", "EXTERNAL"],
            value="INTERNAL",
            real_time_refresh=True,
            show=True,
        ),
        DropdownInput(
            name="postgres_target_kind",
            display_name="Snowflake Postgres Target",
            options=["INSTANCE", "TABLE"],
            value="INSTANCE",
            real_time_refresh=True,
            show=False,
        ),
        StrInput(
            name="object_name",
            display_name="New Object Name",
            value="DOCS_STAGE",
            show=True,
        ),
        StrInput(
            name="columns_spec",
            display_name="Columns Specification",
            value="id VARCHAR(128), content VARCHAR, metadata VARIANT",
            show=False,
        ),
        StrInput(
            name="primary_key_column",
            display_name="Primary Key Column (required for HYBRID Table)",
            value="id",
            show=False,
        ),
        StrInput(
            name="target_lag",
            display_name="TARGET_LAG (for DYNAMIC / INTERACTIVE Table)",
            value="5 minutes",
            show=False,
        ),
        StrInput(
            name="warehouse_name",
            display_name="WAREHOUSE (for DYNAMIC / INTERACTIVE Table)",
            value="COMPUTE_WH",
            show=False,
        ),
        StrInput(
            name="external_volume",
            display_name="EXTERNAL_VOLUME (for ICEBERG Table)",
            value="ICEBERG_EXT_VOL",
            show=False,
        ),
        StrInput(
            name="catalog_integration",
            display_name="CATALOG (for ICEBERG Table)",
            value="SNOWFLAKE",
            show=False,
        ),
        MultilineInput(
            name="as_select_query",
            display_name="AS SELECT Query (for VIEW / DYNAMIC / INTERACTIVE Table)",
            value="SELECT run_id, flow_id, effective_role, status FROM SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_FLOW_RUNS",
            show=False,
        ),
        StrInput(
            name="external_url",
            display_name="External Stage URL (s3:// | gcs:// | azure://)",
            value="s3://enterprise-bucket/prefix/",
            show=False,
        ),
        StrInput(
            name="storage_integration",
            display_name="STORAGE_INTEGRATION (for External Stage)",
            value="S3_STORAGE_INT",
            show=False,
        ),
        StrInput(
            name="postgres_instance_name",
            display_name="Postgres Instance Name",
            value="LANGFLOW_PG_INSTANCE",
            show=False,
        ),
        DropdownInput(
            name="compute_family",
            display_name="Postgres COMPUTE_FAMILY",
            options=["STANDARD_M", "HIGHMEM_L", "BURST_XS"],
            value="STANDARD_M",
            show=False,
        ),
        IntInput(
            name="storage_size_gb",
            display_name="Postgres STORAGE_SIZE_GB (10..65535)",
            value=10,
            show=False,
        ),
        DropdownInput(
            name="object_subtype",
            display_name="Legacy Subtype Alias",
            options=["STANDARD", "HYBRID", "ICEBERG", "INTERACTIVE", "DYNAMIC", "INTERNAL_STAGE", "EXTERNAL_STAGE", "POSTGRES_INSTANCE"],
            value="INTERNAL_STAGE",
            advanced=True,
            show=False,
        ),
        IntInput(
            name="vector_dimension",
            display_name="VECTOR Dimension (<=4096)",
            value=768,
            advanced=True,
            show=False,
        ),
        BoolInput(
            name="dry_run",
            display_name="Dry-Run Preview Only (Safe Default)",
            value=True,
        ),
        BoolInput(
            name="confirmed",
            display_name="Confirm DDL Execution on Snowflake",
            value=False,
        ),
    ]
    outputs = [
        Output(display_name="Provisioning Outcome", name="provision_result", method="provision_object"),
    ]
