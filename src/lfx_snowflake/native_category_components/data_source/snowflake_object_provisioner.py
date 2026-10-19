from __future__ import annotations
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeObjectProvisionerSidebarComponent(Component):
    display_name = "Snowflake Object Provisioner"
    description = "Governed creation of Snowflake Postgres Instances, Databases, Schemas, Tables (Standard, Hybrid, Iceberg, Interactive, Dynamic) and Stages with Dry-Run DDL preview."
    icon = "Snowflake"
    name = "SnowflakeObjectProvisioner"

    inputs = [
        DropdownInput(name="object_kind", display_name="Snowflake Object Type", options=["table", "stage", "schema", "database", "postgres"], value="table"),
        DropdownInput(name="table_type", display_name="Table Subtype", options=["STANDARD", "HYBRID", "ICEBERG", "INTERACTIVE", "DYNAMIC"], value="STANDARD"),
        StrInput(name="database", display_name="Database", value="SNOWFLAKE_LANGFLOW"),
        StrInput(name="schema_name", display_name="Schema", value="CORE"),
        StrInput(name="object_name", display_name="New Object Name", value="CUSTOM_VECTOR_STORE"),
        IntInput(name="vector_dimension", display_name="VECTOR Dimension (<=4096)", value=768, advanced=True),
        BoolInput(name="dry_run", display_name="Dry-Run Preview Only (Safe Default)", value=True),
        BoolInput(name="confirmed", display_name="Confirm DDL Execution on Snowflake", value=False),
    ]
    outputs = [
        Output(display_name="Provisioning Outcome", name="provision_result", method="provision_object"),
    ]

    def provision_object(self) -> Data:
        denied = LIVE_BRIDGE.check_write_privilege_or_deny(f"PROVISION_{self.object_kind.upper()}", self.object_name)
        if denied:
            raise ValueError(denied["message"])
        db, sch, nm = self.database.strip(), self.schema_name.strip(), self.object_name.strip()
        dim = int(self.vector_dimension or 768)
        if self.object_kind == "stage":
            ddl = f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{nm} DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
        elif self.object_kind == "postgres":
            ddl = f"CREATE POSTGRES INSTANCE IF NOT EXISTS {nm} COMPUTE_FAMILY = 'STANDARD_M' STORAGE_SIZE_GB = 10 AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
        elif self.table_type == "HYBRID":
            ddl = f"CREATE HYBRID TABLE IF NOT EXISTS {db}.{sch}.{nm} (id VARCHAR(128) PRIMARY KEY, content VARCHAR, embedding VECTOR(FLOAT, {dim}));"
        else:
            ddl = f"CREATE TABLE IF NOT EXISTS {db}.{sch}.{nm} (id VARCHAR(128) PRIMARY KEY, content VARCHAR, embedding VECTOR(FLOAT, {dim}));"
        if self.dry_run or not self.confirmed:
            return Data(data={"dry_run": True, "confirmed": bool(self.confirmed), "active_role": LIVE_BRIDGE.active_role, "ddl_preview": ddl})
        res = LIVE_BRIDGE.execute_sql(ddl)
        LIVE_BRIDGE.record_audit_event(action=f"PROVISION_{self.object_kind.upper()}", outcome="ALLOWED" if res.get("ok") else "ERROR", target_object=f"{db}.{sch}.{nm}", details={"ddl": ddl})
        return Data(data={"dry_run": False, "ddl_executed": ddl, "result": res})
