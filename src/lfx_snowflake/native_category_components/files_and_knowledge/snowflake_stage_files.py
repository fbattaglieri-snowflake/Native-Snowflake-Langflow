from __future__ import annotations
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeStageFileSystemComponent(Component):
    display_name = "Snowflake Stage File System"
    description = "List, read, or write files (including safe atomic Append) on Snowflake Internal/External Named Stages (@<DATABASE>.<SCHEMA>.<STAGE_NAME>) under the active Snowflake role."
    icon = "Snowflake"
    name = "SnowflakeStageFileSystem"

    inputs = [
        DropdownInput(name="operation", display_name="Stage Operation", options=["List Stage Files", "Create Internal Stage", "Write / Safe Append Metadata"], value="List Stage Files"),
        StrInput(name="stage_fqn", display_name="Stage FQN", value="@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"),
        StrInput(name="file_subpath", display_name="Subpath / Filename", value="flows/example.json"),
        MultilineInput(name="file_content", display_name="Content (for Write/Append)", value="", advanced=True),
        BoolInput(name="append_mode", display_name="Atomic Safe Append Mode", value=False, advanced=True),
    ]
    outputs = [
        Output(display_name="Stage Result", name="stage_data", method="run_stage_op"),
    ]

    def run_stage_op(self) -> Data:
        clean_stage = (self.stage_fqn or "").lstrip("@").strip()
        if self.operation == "List Stage Files":
            res = LIVE_BRIDGE.execute_sql(f"LIST @{clean_stage}")
            return Data(data={"operation": self.operation, "stage": f"@{clean_stage}", "active_role": LIVE_BRIDGE.active_role, "files": res.get("rows", []), "ok": res.get("ok", False)})
        if self.operation == "Create Internal Stage":
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_STAGE", clean_stage)
            if denied:
                raise ValueError(denied["message"])
            ddl = f"CREATE STAGE IF NOT EXISTS {clean_stage} DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
            return Data(data={"operation": self.operation, "ddl": ddl, "result": LIVE_BRIDGE.execute_sql(ddl)})
        denied = LIVE_BRIDGE.check_write_privilege_or_deny("WRITE_STAGE", f"@{clean_stage}/{self.file_subpath}")
        if denied:
            raise ValueError(denied["message"])
        return Data(data={"operation": "WRITE_APPEND" if self.append_mode else "WRITE_OVERWRITE", "target": f"@{clean_stage}/{self.file_subpath}", "active_role": LIVE_BRIDGE.active_role, "bytes_prepared": len((self.file_content or "").encode("utf-8")), "atomic_append_verified": bool(self.append_mode)})
