from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, Output, StrInput, TabInput
from lfx.schema import Data, DataFrame, Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.triggers_manager import (
    COMPUTE_MODE_OPTIONS,
    STAGE_TRIGGER_ON_OPTIONS,
    STATUS_MODE_OPTIONS,
    TRIGGERS_MANAGER,
    build_stage_file_trigger_ddl,
    get_trigger_databases,
    get_trigger_schemas,
    get_trigger_stage_objects,
    get_trigger_warehouses,
    resolve_trigger_flow_id,
)


class SnowflakeStageFileTrigger(Component):
    display_name = "Stage File Trigger"
    description = "Trigger this flow when files arrive in a Snowflake Stage"
    icon = "Snowflake"
    name = "SnowflakeStageFileTrigger"

    inputs = [
        TabInput(
            name="status_mode",
            display_name="Status",
            options=STATUS_MODE_OPTIONS,
            value="Active",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=get_trigger_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema",
            display_name="Schema",
            options=get_trigger_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_name",
            display_name="Stage",
            options=get_trigger_stage_objects("SNOWFLAKE_LANGFLOW", "CORE"),
            value="SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="trigger_on",
            display_name="Trigger On",
            options=STAGE_TRIGGER_ON_OPTIONS,
            value="New Files Only (Upload)",
        ),
        StrInput(
            name="file_pattern",
            display_name="File Pattern (Optional)",
            value="*.pdf",
        ),
        IntInput(
            name="max_files_per_run",
            display_name="Max Files Per Run",
            value=50,
            advanced=True,
        ),
        DropdownInput(
            name="compute_mode",
            display_name="Compute Mode",
            options=COMPUTE_MODE_OPTIONS,
            value="Serverless (Managed)",
            real_time_refresh=True,
            advanced=True,
        ),
        DropdownInput(
            name="warehouse",
            display_name="Warehouse",
            options=get_trigger_warehouses(),
            value="COMPUTE_WH",
            combobox=True,
            advanced=True,
            show=False,
        ),
        IntInput(
            name="retry_attempts",
            display_name="Task Auto Retry Attempts",
            value=3,
            advanced=True,
        ),
        IntInput(
            name="suspend_after_failures",
            display_name="Suspend After Failures",
            value=5,
            advanced=True,
        ),
        IntInput(
            name="timeout_ms",
            display_name="Timeout (ms)",
            value=3600000,
            advanced=True,
        ),
    ]

    outputs = [
        Output(
            display_name="Staged Files",
            name="staged_files",
            method="build_staged_files",
        ),
        Output(
            display_name="Trigger Message",
            name="trigger_message_out",
            method="build_trigger_message",
        ),
        Output(
            display_name="File Metadata",
            name="file_metadata",
            method="build_file_metadata",
        ),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = resolve_trigger_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = get_trigger_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value

        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        schemas = get_trigger_schemas(db_val, flow_id=flow_id)
        if "schema" in build_config:
            build_config["schema"]["options"] = schemas
            cur_sch = build_config["schema"].get("value")
            if field_name == "schema" and isinstance(field_value, str):
                build_config["schema"]["value"] = field_value
            elif schemas and cur_sch not in schemas:
                build_config["schema"]["value"] = schemas[0]

        sch_val = (
            field_value
            if field_name == "schema" and isinstance(field_value, str)
            else build_config.get("schema", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        if "stage_name" in build_config:
            stg_opts = get_trigger_stage_objects(db_val, sch_val, flow_id=flow_id)
            build_config["stage_name"]["options"] = stg_opts
            if field_name == "stage_name" and isinstance(field_value, str):
                build_config["stage_name"]["value"] = field_value
            elif stg_opts and build_config["stage_name"].get("value") not in stg_opts:
                build_config["stage_name"]["value"] = stg_opts[0]

        cmode = (
            field_value
            if field_name == "compute_mode" and isinstance(field_value, str)
            else build_config.get("compute_mode", {}).get("value") or "Serverless (Managed)"
        )
        if "warehouse" in build_config:
            build_config["warehouse"]["options"] = get_trigger_warehouses(flow_id=flow_id)
            build_config["warehouse"]["show"] = cmode == "Warehouse"
        return build_config

    def _provision_and_get_spec(self) -> dict[str, Any]:
        flow_id = resolve_trigger_flow_id(self)
        db = str(getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW").strip()
        sch = str(getattr(self, "schema", "CORE") or "CORE").strip()
        stg = str(getattr(self, "stage_name", "SNOWFLAKE_LANGFLOW_STAGE") or "SNOWFLAKE_LANGFLOW_STAGE").strip()
        ddl_spec = build_stage_file_trigger_ddl(
            flow_id=flow_id,
            database=db,
            schema=sch,
            stage_name=stg,
            status_mode=getattr(self, "status_mode", "Active"),
            trigger_on=getattr(self, "trigger_on", "New Files Only (Upload)"),
            file_pattern=getattr(self, "file_pattern", None),
            max_files_per_run=int(getattr(self, "max_files_per_run", 50) or 50),
            compute_mode=getattr(self, "compute_mode", "Serverless (Managed)"),
            warehouse=getattr(self, "warehouse", "COMPUTE_WH"),
            retry_attempts=int(getattr(self, "retry_attempts", 3) if getattr(self, "retry_attempts", None) is not None else 3),
            suspend_after_failures=int(
                getattr(self, "suspend_after_failures", 5)
                if getattr(self, "suspend_after_failures", None) is not None
                else 5
            ),
            timeout_ms=int(getattr(self, "timeout_ms", 3600000) or 3600000),
        )
        node_id = str(getattr(self, "_id", None) or getattr(self, "id", None) or "SnowflakeStageFileTrigger")
        prov = TRIGGERS_MANAGER.provision_trigger(
            ddl_spec=ddl_spec,
            node_id=node_id,
            bridge=LIVE_BRIDGE,
            config={
                "compute_mode": getattr(self, "compute_mode", "Serverless (Managed)"),
                "warehouse": getattr(self, "warehouse", "COMPUTE_WH"),
                "stage_name": stg,
                "file_pattern": getattr(self, "file_pattern", ""),
            },
        )
        return {**ddl_spec, "provision_result": prov}

    def build_staged_files(self) -> DataFrame:
        spec = self._provision_and_get_spec()
        injected_files = getattr(self, "_trigger_files", None)
        if isinstance(injected_files, list):
            return DataFrame(injected_files)
        limit = max(1, min(1000, int(getattr(self, "max_files_per_run", 50) or 50)))
        res = LIVE_BRIDGE.execute_sql(f"SELECT * FROM {spec['stream_fqn']} LIMIT {limit}")
        return DataFrame(res.get("rows", []))

    def build_trigger_message(self) -> Message:
        spec = self._provision_and_get_spec()
        files = getattr(self, "_trigger_files", None)
        cnt = len(files) if isinstance(files, list) else 0
        text = f"Snowflake Stage Stream '{spec['stream_fqn']}' detected {cnt} file event(s) on {spec['stage_fqn']}."
        return Message(
            text=text,
            trigger_type="STAGE_FILE_TRIGGER_RUN",
            stage_fqn=spec["stage_fqn"],
            stream_fqn=spec["stream_fqn"],
            task_fqn=spec["task_fqn"],
            file_count=cnt,
        )

    def build_file_metadata(self) -> Data:
        spec = self._provision_and_get_spec()
        return Data(
            data={
                "trigger_component": "SnowflakeStageFileTrigger",
                "trigger_type": "STAGE_FILE_TRIGGER_RUN",
                "flow_id": spec["flow_id"],
                "flow_short_id": spec["flow_short_id"],
                "database": spec["database"],
                "schema": spec["schema"],
                "stage_name": spec["stage_name"],
                "stage_fqn": spec["stage_fqn"],
                "stream_name": spec["stream_name"],
                "stream_fqn": spec["stream_fqn"],
                "task_name": spec["task_name"],
                "task_fqn": spec["task_fqn"],
                "temp_batch_fqn": spec["temp_batch_fqn"],
                "file_pattern": spec["file_pattern"],
                "status_mode": spec["status_mode"],
                "active_role": LIVE_BRIDGE.active_role,
                "enable_dir_sql": spec["enable_dir_sql"],
                "create_stream_sql": spec["create_stream_sql"],
                "create_task_sql": spec["create_task_sql"],
                "alter_state_sql": spec["alter_state_sql"],
                "ddl": spec["ddl"],
                "statements": spec["statements"],
            }
        )
