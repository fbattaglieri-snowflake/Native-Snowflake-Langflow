from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MultilineInput, Output, StrInput
from lfx.schema import Data

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_NEW_DB_OPTION = "+ Create New Database..."
CREATE_NEW_DB_DISABLED_OPTION = "[DISABLED — Role lacks CREATE DATABASE privilege]"
CREATE_NEW_SCHEMA_OPTION = "+ Create New Schema..."
CREATE_NEW_SCHEMA_DISABLED_OPTION = "[DISABLED — Role lacks CREATE SCHEMA privilege]"

INFRASTRUCTURE_OBJECT_FAMILIES: tuple[str, ...] = ("table", "view", "stage", "postgres")


def _can_create_database() -> bool:
    fn = getattr(LIVE_BRIDGE, "can_create_database", None)
    if callable(fn):
        return bool(fn())
    privs = getattr(LIVE_BRIDGE, "role_privileges", None)
    if isinstance(privs, dict) and "can_create_database" in privs:
        return bool(privs["can_create_database"])
    return LIVE_BRIDGE.active_role.upper() in ("ACCOUNTADMIN", "SYSADMIN", "SNOWFLAKE_LANGFLOW_ADMIN_ROLE")


def _can_create_schema() -> bool:
    fn = getattr(LIVE_BRIDGE, "can_create_schema", None)
    if callable(fn):
        return bool(fn())
    privs = getattr(LIVE_BRIDGE, "role_privileges", None)
    if isinstance(privs, dict) and "can_create_schema" in privs:
        return bool(privs["can_create_schema"])
    return LIVE_BRIDGE.active_role.upper() not in ("PUBLIC", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE", "VIEWER_ROLE")


def _resolve_flow_id(component: Any = None, build_config: dict[str, Any] | None = None) -> str | None:
    resolver = getattr(LIVE_BRIDGE, "_resolve_component_flow_id", None) or getattr(
        LIVE_BRIDGE, "resolve_component_flow_id", None
    )
    if callable(resolver):
        res = resolver(component, build_config)
        if res:
            return str(res).strip()
    if isinstance(build_config, dict):
        cfg_fid = build_config.get("_frontend_node_flow_id") or build_config.get("flow_id")
        if isinstance(cfg_fid, dict):
            cfg_fid = cfg_fid.get("value")
        if cfg_fid and str(cfg_fid).strip():
            return str(cfg_fid).strip()
    if component is not None:
        for attr in ("flow_id", "_flow_id"):
            val = getattr(component, attr, None)
            if val and str(val).strip():
                return str(val).strip()
        graph = getattr(component, "graph", None)
        if graph is not None:
            g_fid = getattr(graph, "flow_id", None) or getattr(graph, "_flow_id", None)
            if g_fid and str(g_fid).strip():
                return str(g_fid).strip()
    cur_fn = getattr(LIVE_BRIDGE, "get_current_flow_id", None) or getattr(
        LIVE_BRIDGE, "get_current_snowflake_flow_id", None
    )
    if callable(cur_fn):
        cur = cur_fn()
        if cur and str(cur).strip():
            return str(cur).strip()
    return None


def _ddl_succeeded(res: dict[str, Any] | None) -> bool:
    if not isinstance(res, dict):
        return False
    if res.get("ok", False):
        return True
    return res.get("mode") == "offline" and not res.get("error") and not res.get("error_code")


def _register_created_object(
    database: str,
    schema: str,
    category: str,
    object_name: str,
    flow_id: str | None = None,
) -> None:
    eff_fid = flow_id or _resolve_flow_id()
    reg_fn = getattr(LIVE_BRIDGE, "register_object_in_context", None)
    if callable(reg_fn):
        reg_fn(
            database=database,
            schema=schema,
            category=category,
            object_name=object_name,
            flow_id=eff_fid,
        )
    ctx = (
        LIVE_BRIDGE.get_flow_context(eff_fid)
        if eff_fid and hasattr(LIVE_BRIDGE, "get_flow_context")
        else getattr(LIVE_BRIDGE, "selected_context", None)
    )
    if isinstance(ctx, dict):
        db_u = (database or "").strip().upper()
        sch_u = (schema or "").strip().upper()
        obj_u = (object_name or "").strip()
        if category == "postgres_instances":
            pg_list = ctx.setdefault("postgres_instances", [])
            if obj_u and obj_u not in pg_list:
                pg_list.append(obj_u)
        if db_u and db_u not in ctx.setdefault("databases", []):
            ctx["databases"].append(db_u)
        if db_u and sch_u:
            sch_list = ctx.setdefault("schemas_by_db", {}).setdefault(db_u, [])
            if sch_u not in sch_list:
                sch_list.append(sch_u)
            if category != "postgres_instances":
                key = f"{db_u}.{sch_u}"
                for map_name in ("objects_by_db_schema", "selected_objects"):
                    m = ctx.setdefault(map_name, {})
                    bucket = m.get(key)
                    if isinstance(bucket, dict):
                        arr = bucket.setdefault(category, [])
                        if obj_u and obj_u not in arr:
                            arr.append(obj_u)
                    elif bucket is None:
                        m[key] = {category: [obj_u] if obj_u else []}


def _database_options(flow_id: str | None = None) -> list[str]:
    dbs = list(LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id()))
    dbs.append(CREATE_NEW_DB_OPTION if _can_create_database() else CREATE_NEW_DB_DISABLED_OPTION)
    return dbs


def _schema_options(database: str | None = None, flow_id: str | None = None) -> list[str]:
    schemas = list(LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id()))
    schemas.append(CREATE_NEW_SCHEMA_OPTION if _can_create_schema() else CREATE_NEW_SCHEMA_DISABLED_OPTION)
    return schemas


class SnowflakeObjectProvisionerSidebarComponent(Component):
    display_name = "Object Provisioner"
    description = (
        "Hierarchical governed creation of 1. Database -> 2. Schema -> 3. Infrastructure Object in Schema "
        "across the 4 official Snowflake families (1. Table: Standard, Hybrid, Iceberg, Interactive, Dynamic; "
        "2. View: Standard, Secure, Materialized; 3. Stage: Internal, External; 4. Snowflake Postgres) "
        "with dynamic field adaptation, active-role privilege checks, and Dry-Run DDL preview."
    )
    icon = "Snowflake"
    name = "SnowflakeObjectProvisioner"

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
            value="table",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="table_subtype",
            display_name="Table Type",
            options=["STANDARD", "HYBRID", "ICEBERG", "INTERACTIVE", "DYNAMIC"],
            value="STANDARD",
            real_time_refresh=True,
            show=True,
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
            show=False,
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
            value="CUSTOM_GOV_TABLE",
            show=True,
        ),
        StrInput(
            name="columns_spec",
            display_name="Columns Specification",
            value="id VARCHAR(128), content VARCHAR, metadata VARIANT, created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()",
            show=True,
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
            value="STANDARD",
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

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if field_name and field_name in build_config and isinstance(build_config[field_name], dict):
            build_config[field_name]["value"] = field_value
        if "database" in build_config:
            build_config["database"]["options"] = _database_options(flow_id=flow_id)
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _schema_options(db_val, flow_id=flow_id)
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or "CORE"
        )

        if "new_database_name" in build_config:
            build_config["new_database_name"]["show"] = db_val == CREATE_NEW_DB_OPTION
        if "new_schema_name" in build_config:
            build_config["new_schema_name"]["show"] = sch_val == CREATE_NEW_SCHEMA_OPTION

        kind = (
            field_value
            if field_name == "object_kind" and isinstance(field_value, str)
            else build_config.get("object_kind", {}).get("value") or getattr(self, "object_kind", "table")
        ).strip().lower()
        table_sub = (
            field_value
            if field_name == "table_subtype" and isinstance(field_value, str)
            else build_config.get("table_subtype", {}).get("value") or getattr(self, "table_subtype", "STANDARD")
        ).strip().upper()
        stage_tp = (
            field_value
            if field_name == "stage_type" and isinstance(field_value, str)
            else build_config.get("stage_type", {}).get("value") or getattr(self, "stage_type", "INTERNAL")
        ).strip().upper()

        family_fields = (
            "table_subtype",
            "view_type",
            "stage_type",
            "postgres_target_kind",
            "columns_spec",
            "primary_key_column",
            "target_lag",
            "warehouse_name",
            "external_volume",
            "catalog_integration",
            "as_select_query",
            "external_url",
            "storage_integration",
            "postgres_instance_name",
            "compute_family",
            "storage_size_gb",
        )
        for fname in family_fields:
            if fname in build_config:
                build_config[fname]["show"] = False

        if kind == "table":
            for f in ("table_subtype", "columns_spec"):
                if f in build_config:
                    build_config[f]["show"] = True
            if table_sub == "HYBRID" and "primary_key_column" in build_config:
                build_config["primary_key_column"]["show"] = True
            if table_sub in ("DYNAMIC", "INTERACTIVE"):
                for f in ("target_lag", "warehouse_name", "as_select_query"):
                    if f in build_config:
                        build_config[f]["show"] = True
            if table_sub == "ICEBERG":
                for f in ("external_volume", "catalog_integration"):
                    if f in build_config:
                        build_config[f]["show"] = True
        elif kind == "view":
            for f in ("view_type", "as_select_query"):
                if f in build_config:
                    build_config[f]["show"] = True
        elif kind == "stage":
            if "stage_type" in build_config:
                build_config["stage_type"]["show"] = True
            if stage_tp == "EXTERNAL":
                for f in ("external_url", "storage_integration"):
                    if f in build_config:
                        build_config[f]["show"] = True
        elif kind == "postgres":
            for f in ("postgres_target_kind", "postgres_instance_name", "compute_family", "storage_size_gb", "columns_spec"):
                if f in build_config:
                    build_config[f]["show"] = True

        return build_config

    def provision_object(self) -> Data:
        kind = (getattr(self, "object_kind", "table") or "table").strip().lower()
        subtype = (
            getattr(self, "table_subtype", None)
            or getattr(self, "object_subtype", None)
            or getattr(self, "table_type", None)
            or "STANDARD"
        ).strip().upper()

        if kind in {"memory", "memories", "agent_memory"}:
            raise ValueError(
                "W2-R2-04B / W2-R2-13: Memories are not provisioned in Object Provisioner. "
                "Open the Langflow 'Memories' page and click '+ Snowflake' to create an Agent Memory "
                "(Episodic / Session, Semantic / Vector, or Structured / Semi-Structured Long-Term)."
            )
        if kind in {"function", "procedure", "udf", "udtf", "stored_procedure"}:
            raise ValueError(
                "HUB-W2-04: Functions (UDF/UDTF) and Stored Procedures cannot be provisioned via "
                "Object Provisioner; use 'LLM Operations -> Stored Procedure / Function'."
            )

        denied = LIVE_BRIDGE.check_write_privilege_or_deny(f"PROVISION_{kind.upper()}", self.object_name)
        if denied:
            raise ValueError(denied["message"])

        raw_db = (self.database or "").strip()
        raw_sch = (self.schema_name or "").strip()
        if raw_db == CREATE_NEW_DB_DISABLED_OPTION or (raw_db == CREATE_NEW_DB_OPTION and not _can_create_database()):
            raise ValueError(
                f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi (CREATE DATABASE) "
                "per creare un nuovo database."
            )
        if raw_sch == CREATE_NEW_SCHEMA_DISABLED_OPTION or (
            raw_sch == CREATE_NEW_SCHEMA_OPTION and not _can_create_schema()
        ):
            raise ValueError(
                f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi (CREATE SCHEMA) "
                f"per creare un nuovo schema in {raw_db}."
            )

        db = (getattr(self, "new_database_name", "") or "").strip() if raw_db == CREATE_NEW_DB_OPTION else raw_db
        sch = (getattr(self, "new_schema_name", "") or "").strip() if raw_sch == CREATE_NEW_SCHEMA_OPTION else raw_sch
        db = db or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
        sch = sch or getattr(LIVE_BRIDGE, "schema", "CORE")
        nm = (self.object_name or "NEW_OBJECT").strip()

        ddl_statements: list[str] = []
        if raw_db == CREATE_NEW_DB_OPTION:
            ddl_statements.append(f"CREATE DATABASE IF NOT EXISTS {db};")
        if raw_sch == CREATE_NEW_SCHEMA_OPTION:
            ddl_statements.append(f"CREATE SCHEMA IF NOT EXISTS {db}.{sch};")

        if kind == "stage" or subtype in ("INTERNAL_STAGE", "EXTERNAL_STAGE"):
            stg_type = (getattr(self, "stage_type", "") or subtype).upper()
            if "EXTERNAL" in stg_type:
                ext_url = getattr(self, "external_url", "s3://<BUCKET>/<PREFIX>/") or "s3://<BUCKET>/<PREFIX>/"
                stg_int = getattr(self, "storage_integration", "<STORAGE_INTEGRATION>") or "<STORAGE_INTEGRATION>"
                ddl_statements.append(
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{nm} "
                    f"URL = '{ext_url}' STORAGE_INTEGRATION = {stg_int} "
                    "DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);"
                )
            else:
                ddl_statements.append(
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{nm} "
                    "DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
                )
        elif kind == "view":
            vtype = (getattr(self, "view_type", "STANDARD") or "STANDARD").upper()
            v_kw = "SECURE VIEW" if vtype == "SECURE" else ("MATERIALIZED VIEW" if vtype == "MATERIALIZED" else "VIEW")
            sel_q = (getattr(self, "as_select_query", "") or f"SELECT * FROM {LIVE_BRIDGE.runs_table}").strip()
            ddl_statements.append(f"CREATE OR REPLACE {v_kw} {db}.{sch}.{nm} AS {sel_q};")
        elif kind == "postgres" or subtype == "POSTGRES_INSTANCE":
            pg_inst = (getattr(self, "postgres_instance_name", "") or nm).strip()
            cf = getattr(self, "compute_family", "STANDARD_M") or "STANDARD_M"
            sz = int(getattr(self, "storage_size_gb", 10) or 10)
            ddl_statements.append(
                f"CREATE POSTGRES INSTANCE IF NOT EXISTS {pg_inst} "
                f"COMPUTE_FAMILY = '{cf}' STORAGE_SIZE_GB = {sz} "
                "AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
            )
        else:
            kw = "HYBRID TABLE" if subtype == "HYBRID" else "TABLE"
            cols = getattr(self, "columns_spec", "") or "id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT"
            ddl_statements.append(f"CREATE {kw} IF NOT EXISTS {db}.{sch}.{nm} ({cols});")

        ddl = "\n".join(ddl_statements)
        if self.dry_run or not self.confirmed:
            return Data(
                data={
                    "ok": True,
                    "dry_run": True,
                    "confirmed": self.confirmed,
                    "active_role": LIVE_BRIDGE.active_role,
                    "database": db,
                    "schema_name": sch,
                    "object_kind": kind,
                    "object_subtype": subtype,
                    "object_name": nm,
                    "ddl_preview": ddl,
                }
            )
        res = LIVE_BRIDGE.execute_sql(ddl)
        if _ddl_succeeded(res):
            eff_fid = _resolve_flow_id(self)
            if kind == "stage" or subtype in ("INTERNAL_STAGE", "EXTERNAL_STAGE"):
                _register_created_object(db, sch, "stages", nm, flow_id=eff_fid)
            elif kind == "view":
                _register_created_object(db, sch, "views", nm, flow_id=eff_fid)
            elif kind == "postgres" or subtype == "POSTGRES_INSTANCE":
                pg_inst = (getattr(self, "postgres_instance_name", "") or nm).strip()
                _register_created_object(db, sch, "postgres_instances", pg_inst, flow_id=eff_fid)
            elif subtype == "ICEBERG":
                _register_created_object(db, sch, "iceberg_tables", nm, flow_id=eff_fid)
            else:
                _register_created_object(db, sch, "tables", nm, flow_id=eff_fid)
        return Data(
            data={
                "ok": _ddl_succeeded(res),
                "dry_run": False,
                "active_role": LIVE_BRIDGE.active_role,
                "database": db,
                "schema_name": sch,
                "object_kind": kind,
                "object_subtype": subtype,
                "object_name": nm,
                "ddl_executed": ddl,
                "result": res,
            }
        )


SnowflakeObjectProvisionerComponent = SnowflakeObjectProvisionerSidebarComponent
