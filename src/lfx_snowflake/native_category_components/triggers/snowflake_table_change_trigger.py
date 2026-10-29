from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, Output, TabInput
from lfx.schema import Data, DataFrame, Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.triggers_manager import (
    COMPUTE_MODE_OPTIONS,
    STATUS_MODE_OPTIONS,
    TABLE_TRIGGER_ON_OPTIONS,
    TRIGGERS_MANAGER,
    build_table_change_trigger_ddl,
    detect_source_object_kind,
    get_trigger_databases,
    get_trigger_schemas,
    get_trigger_table_or_view_objects,
    get_trigger_warehouses,
    resolve_trigger_flow_id,
)


class SnowflakeTableChangeTrigger(Component):
    display_name = "Table Change Trigger"
    description = "Trigger this flow when rows change in a Snowflake table"
    icon = "Snowflake"
    name = "SnowflakeTableChangeTrigger"

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
            name="source_object",
            display_name="Table or View",
            options=get_trigger_table_or_view_objects("SNOWFLAKE_LANGFLOW", "CORE"),
            value="CUSTOMER_ORDERS",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="trigger_on",
            display_name="Trigger On",
            options=TABLE_TRIGGER_ON_OPTIONS,
            value="New Rows Only (Insert)",
        ),
        IntInput(
            name="max_batch_rows",
            display_name="Max Batch Rows",
            value=100,
            advanced=True,
        ),
        BoolInput(
            name="show_initial_rows",
            display_name="Show Initial Rows",
            value=False,
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
            display_name="Changed Rows",
            name="changed_rows",
            method="build_changed_rows",
        ),
        Output(
            display_name="Trigger Message",
            name="trigger_message_out",
            method="build_trigger_message",
        ),
        Output(
            display_name="Stream Metadata",
            name="stream_metadata",
            method="build_stream_metadata",
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
        if "source_object" in build_config:
            obj_opts = get_trigger_table_or_view_objects(db_val, sch_val, flow_id=flow_id)
            build_config["source_object"]["options"] = obj_opts
            if field_name == "source_object" and isinstance(field_value, str):
                build_config["source_object"]["value"] = field_value
            elif obj_opts and build_config["source_object"].get("value") not in obj_opts:
                build_config["source_object"]["value"] = obj_opts[0]

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
        src = str(getattr(self, "source_object", "CUSTOMER_ORDERS") or "CUSTOMER_ORDERS").strip()
        obj_kind = getattr(self, "object_kind", None) or detect_source_object_kind(db, sch, src, flow_id=flow_id)
        ddl_spec = build_table_change_trigger_ddl(
            flow_id=flow_id,
            database=db,
            schema=sch,
            source_object=src,
            object_kind=str(obj_kind),
            status_mode=getattr(self, "status_mode", "Active"),
            trigger_on=getattr(self, "trigger_on", "New Rows Only (Insert)"),
            max_batch_rows=int(getattr(self, "max_batch_rows", 100) or 100),
            show_initial_rows=bool(getattr(self, "show_initial_rows", False)),
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
        node_id = str(getattr(self, "_id", None) or getattr(self, "id", None) or "SnowflakeTableChangeTrigger")
        prov = TRIGGERS_MANAGER.provision_trigger(
            ddl_spec=ddl_spec,
            node_id=node_id,
            bridge=LIVE_BRIDGE,
            config={
                "compute_mode": getattr(self, "compute_mode", "Serverless (Managed)"),
                "warehouse": getattr(self, "warehouse", "COMPUTE_WH"),
                "source_object": src,
                "trigger_on": getattr(self, "trigger_on", "New Rows Only (Insert)"),
            },
        )
        return {**ddl_spec, "provision_result": prov}

    def build_changed_rows(self) -> DataFrame:
        spec = self._provision_and_get_spec()
        injected_rows = getattr(self, "_trigger_rows", None)
        if isinstance(injected_rows, list):
            return DataFrame(injected_rows)
        limit = max(1, min(1000, int(getattr(self, "max_batch_rows", 100) or 100)))
        res = LIVE_BRIDGE.execute_sql(f"SELECT * FROM {spec['stream_fqn']} LIMIT {limit}")
        return DataFrame(res.get("rows", []))

    def build_trigger_message(self) -> Message:
        spec = self._provision_and_get_spec()
        rows = getattr(self, "_trigger_rows", None)
        cnt = len(rows) if isinstance(rows, list) else 0
        text = (
            f"Snowflake Stream '{spec['stream_fqn']}' detected {cnt} changed row(s) on {spec['source_object_fqn']}."
        )
        return Message(
            text=text,
            trigger_type="STREAM_TRIGGER_RUN",
            source_object=spec["source_object_fqn"],
            stream_fqn=spec["stream_fqn"],
            task_fqn=spec["task_fqn"],
            row_count=cnt,
        )

    def build_stream_metadata(self) -> Data:
        spec = self._provision_and_get_spec()
        return Data(
            data={
                "trigger_component": "SnowflakeTableChangeTrigger",
                "trigger_type": "STREAM_TRIGGER_RUN",
                "flow_id": spec["flow_id"],
                "flow_short_id": spec["flow_short_id"],
                "database": spec["database"],
                "schema": spec["schema"],
                "source_object": spec["source_object"],
                "source_object_fqn": spec["source_object_fqn"],
                "stream_name": spec["stream_name"],
                "stream_fqn": spec["stream_fqn"],
                "task_name": spec["task_name"],
                "task_fqn": spec["task_fqn"],
                "temp_batch_fqn": spec["temp_batch_fqn"],
                "status_mode": spec["status_mode"],
                "active_role": LIVE_BRIDGE.active_role,
                "create_stream_sql": spec["create_stream_sql"],
                "create_task_sql": spec["create_task_sql"],
                "alter_state_sql": spec["alter_state_sql"],
                "ddl": spec["ddl"],
                "statements": spec["statements"],
            }
        )
