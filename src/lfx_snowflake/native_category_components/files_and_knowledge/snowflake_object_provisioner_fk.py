from __future__ import annotations

from typing import Any

from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_NEW_DB_OPTION = "+ Create New Database..."
CREATE_NEW_DB_DISABLED_OPTION = "[DISABLED — Role lacks CREATE DATABASE privilege]"
CREATE_NEW_SCHEMA_OPTION = "+ Create New Schema..."
CREATE_NEW_SCHEMA_DISABLED_OPTION = "[DISABLED — Role lacks CREATE SCHEMA privilege]"


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


def _database_options() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    if not dbs:
        dbs = ["SNOWFLAKE_LANGFLOW"]
    dbs.append(CREATE_NEW_DB_OPTION if _can_create_database() else CREATE_NEW_DB_DISABLED_OPTION)
    return dbs


def _schema_options(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    if not schemas:
        schemas = ["CORE", "PUBLIC"]
    schemas.append(CREATE_NEW_SCHEMA_OPTION if _can_create_schema() else CREATE_NEW_SCHEMA_DISABLED_OPTION)
    return schemas


class SnowflakeObjectProvisionerFKComponent(Component):
    display_name = "Snowflake Object Provisioner"
    description = (
        "Hierarchical governed creation of 1. Database -> 2. Schema -> 3. Object in Schema "
        "(Tables: Standard, Hybrid, Iceberg, Interactive, Dynamic; Stages: Internal, External; Views; Memories; "
        "Snowflake Postgres Instances) in Files & Knowledge with active-role privilege checks and Dry-Run DDL preview."
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
            advanced=False,
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
            advanced=False,
        ),
        DropdownInput(
            name="object_kind",
            display_name="3. Object Kind (in Schema)",
            options=["table", "stage", "view", "memory", "postgres"],
            value="stage",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="object_subtype",
            display_name="Object Subtype",
            options=[
                "STANDARD",
                "HYBRID",
                "ICEBERG",
                "INTERACTIVE",
                "DYNAMIC",
                "INTERNAL_STAGE",
                "EXTERNAL_STAGE",
                "POSTGRES_INSTANCE",
            ],
            value="INTERNAL_STAGE",
            real_time_refresh=True,
        ),
        StrInput(
            name="object_name",
            display_name="New Object Name",
            value="DOCS_STAGE",
        ),
        IntInput(
            name="vector_dimension",
            display_name="VECTOR Dimension (<=4096)",
            value=768,
            advanced=True,
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
        if "database" in build_config:
            build_config["database"]["options"] = _database_options()
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _schema_options(db_val)
        return build_config

    def provision_object(self) -> Data:
        kind = (getattr(self, "object_kind", "stage") or "stage").strip().lower()
        subtype = (
            getattr(self, "object_subtype", None)
            or getattr(self, "table_type", None)
            or "INTERNAL_STAGE"
        ).strip().upper()

        if kind in {"function", "procedure", "udf", "udtf", "stored_procedure"}:
            raise ValueError(
                "HUB-W2-04: Functions (UDF/UDTF) and Stored Procedures cannot be provisioned via "
                "Snowflake Object Provisioner; use 'LLM Operations -> Stored Procedure / Function'."
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

        if kind == "database":
            ddl_statements.append(f"CREATE DATABASE IF NOT EXISTS {nm};")
        elif kind == "schema":
            ddl_statements.append(f"CREATE SCHEMA IF NOT EXISTS {db}.{nm};")
        elif kind == "stage" or subtype in ("INTERNAL_STAGE", "EXTERNAL_STAGE"):
            if subtype == "EXTERNAL_STAGE":
                ddl_statements.append(
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{nm} "
                    "URL = 's3://<BUCKET>/<PREFIX>/' STORAGE_INTEGRATION = <STORAGE_INTEGRATION> "
                    "DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);"
                )
            else:
                ddl_statements.append(
                    f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{nm} "
                    "DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
                )
        elif kind == "view":
            ddl_statements.append(
                f"CREATE OR REPLACE VIEW {db}.{sch}.{nm} AS SELECT * FROM {LIVE_BRIDGE.runs_table};"
            )
        elif kind == "postgres" or subtype == "POSTGRES_INSTANCE":
            ddl_statements.append(
                f"CREATE POSTGRES INSTANCE IF NOT EXISTS {nm} "
                "COMPUTE_FAMILY = 'STANDARD_M' STORAGE_SIZE_GB = 10 "
                "AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
            )
        else:
            dim = int(self.vector_dimension or 768)
            kw = "HYBRID TABLE" if subtype == "HYBRID" else "TABLE"
            ddl_statements.append(
                f"CREATE {kw} IF NOT EXISTS {db}.{sch}.{nm} ("
                f"id VARCHAR(128) PRIMARY KEY, content VARCHAR, metadata VARIANT, "
                f"embedding VECTOR(FLOAT, {dim}), created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());"
            )

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
        return Data(
            data={
                "ok": res.get("ok", False),
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
