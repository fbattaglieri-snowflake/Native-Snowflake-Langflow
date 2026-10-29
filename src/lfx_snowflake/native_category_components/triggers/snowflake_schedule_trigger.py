from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, Output, StrInput, TabInput
from lfx.schema import Data, Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.triggers_manager import (
    COMMON_IANA_TIMEZONES,
    COMPUTE_MODE_OPTIONS,
    SCHEDULE_TYPE_OPTIONS,
    STATUS_MODE_OPTIONS,
    build_schedule_trigger_ddl,
    get_trigger_databases,
    get_trigger_schemas,
    get_trigger_warehouses,
    get_triggers_manager,
    resolve_trigger_flow_id,
)


class SnowflakeScheduleTrigger(Component):
    display_name = "Schedule Trigger"
    description = "Run this flow on a Snowflake schedule or Cron."
    icon = "Snowflake"
    name = "SnowflakeScheduleTrigger"

    inputs = [
        TabInput(
            name="status_mode",
            display_name="Status",
            options=STATUS_MODE_OPTIONS,
            value="Active",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="schedule_type",
            display_name="Schedule",
            options=SCHEDULE_TYPE_OPTIONS,
            value="Custom (Cron)",
            real_time_refresh=True,
        ),
        StrInput(
            name="cron_expression",
            display_name="Cron Expression",
            value="0 9 * * 1-5",
            info="Mon–Fri at 09:00 (min hour day month weekday)",
            show=True,
        ),
        IntInput(
            name="interval_value",
            display_name="Interval Value",
            value=15,
            show=False,
        ),
        StrInput(
            name="days_of_week",
            display_name="Days of Week",
            value="MON,TUE,WED,THU,FRI",
            show=False,
        ),
        StrInput(
            name="day_of_month",
            display_name="Day of Month",
            value="1",
            show=False,
        ),
        IntInput(
            name="at_hour",
            display_name="At Hour (0-23)",
            value=9,
            show=False,
        ),
        IntInput(
            name="at_minute",
            display_name="At Minute (0-59)",
            value=0,
            show=False,
        ),
        DropdownInput(
            name="timezone",
            display_name="Timezone",
            options=COMMON_IANA_TIMEZONES,
            value="Europe/Rome",
            combobox=True,
        ),
        MessageTextInput(
            name="trigger_message",
            display_name="Input Message",
            value="Run scheduled workflow",
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=get_trigger_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
            advanced=True,
        ),
        DropdownInput(
            name="schema",
            display_name="Schema",
            options=get_trigger_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
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
        BoolInput(
            name="allow_overlapping_executions",
            display_name="Allow Overlapping Executions",
            value=False,
            advanced=True,
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
            display_name="Trigger Message",
            name="trigger_message_out",
            method="build_trigger_message",
        ),
        Output(
            display_name="Execution Metadata",
            name="execution_metadata",
            method="build_execution_metadata",
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

        stype = (
            field_value
            if field_name == "schedule_type" and isinstance(field_value, str)
            else build_config.get("schedule_type", {}).get("value") or "Custom (Cron)"
        )
        if "cron_expression" in build_config:
            build_config["cron_expression"]["show"] = stype == "Custom (Cron)"
        if "interval_value" in build_config:
            build_config["interval_value"]["show"] = stype in ("Minutes", "Hours")
            if field_name == "schedule_type":
                build_config["interval_value"]["value"] = 15 if stype == "Minutes" else 1
        if "days_of_week" in build_config:
            build_config["days_of_week"]["show"] = stype == "Weeks"
        if "day_of_month" in build_config:
            build_config["day_of_month"]["show"] = stype == "Months"
        if "at_hour" in build_config:
            build_config["at_hour"]["show"] = stype in ("Days", "Weeks", "Months")
        if "at_minute" in build_config:
            build_config["at_minute"]["show"] = stype in ("Days", "Weeks", "Months")

        cmode = (
            field_value
            if field_name == "compute_mode" and isinstance(field_value, str)
            else build_config.get("compute_mode", {}).get("value") or "Serverless (Managed)"
        )
        if "warehouse" in build_config:
            build_config["warehouse"]["options"] = get_trigger_warehouses(flow_id=flow_id)
            build_config["warehouse"]["show"] = cmode == "Warehouse"
        get_triggers_manager().sync_trigger_from_build_config(
            component_type=self.name,
            component=self,
            build_config=build_config,
            field_name=field_name,
            field_value=field_value,
            bridge=LIVE_BRIDGE,
        )
        return build_config

    def _provision_and_get_spec(self) -> dict[str, Any]:
        flow_id = resolve_trigger_flow_id(self)
        ddl_spec = build_schedule_trigger_ddl(
            flow_id=flow_id,
            database=getattr(self, "database", "SNOWFLAKE_LANGFLOW"),
            schema=getattr(self, "schema", "CORE"),
            status_mode=getattr(self, "status_mode", "Active"),
            schedule_type=getattr(self, "schedule_type", "Custom (Cron)"),
            cron_expression=getattr(self, "cron_expression", "0 9 * * 1-5"),
            timezone_name=getattr(self, "timezone", "Europe/Rome"),
            interval_value=int(getattr(self, "interval_value", 15) or 15),
            days_of_week=getattr(self, "days_of_week", "MON,TUE,WED,THU,FRI"),
            day_of_month=getattr(self, "day_of_month", "1"),
            at_hour=int(getattr(self, "at_hour", 9) if getattr(self, "at_hour", None) is not None else 9),
            at_minute=int(getattr(self, "at_minute", 0) if getattr(self, "at_minute", None) is not None else 0),
            trigger_message=getattr(self, "trigger_message", "Run scheduled workflow"),
            compute_mode=getattr(self, "compute_mode", "Serverless (Managed)"),
            warehouse=getattr(self, "warehouse", "COMPUTE_WH"),
            allow_overlapping_executions=bool(getattr(self, "allow_overlapping_executions", False)),
            retry_attempts=int(getattr(self, "retry_attempts", 3) if getattr(self, "retry_attempts", None) is not None else 3),
            suspend_after_failures=int(
                getattr(self, "suspend_after_failures", 5)
                if getattr(self, "suspend_after_failures", None) is not None
                else 5
            ),
            timeout_ms=int(getattr(self, "timeout_ms", 3600000) or 3600000),
        )
        node_id = str(getattr(self, "_id", None) or getattr(self, "id", None) or "SnowflakeScheduleTrigger")
        prov = get_triggers_manager().provision_trigger(
            ddl_spec=ddl_spec,
            node_id=node_id,
            bridge=LIVE_BRIDGE,
            config={
                "compute_mode": getattr(self, "compute_mode", "Serverless (Managed)"),
                "warehouse": getattr(self, "warehouse", "COMPUTE_WH"),
                "schedule_type": getattr(self, "schedule_type", "Custom (Cron)"),
                "cron_expression": getattr(self, "cron_expression", "0 9 * * 1-5"),
                "timezone": getattr(self, "timezone", "Europe/Rome"),
            },
        )
        return {**ddl_spec, "provision_result": prov}

    def build_trigger_message(self) -> Message:
        spec = self._provision_and_get_spec()
        msg_text = str(getattr(self, "trigger_message", "") or "Run scheduled workflow").strip()
        return Message(
            text=msg_text,
            trigger_type="SCHEDULED_RUN",
            task_fqn=spec["task_fqn"],
            schedule_clause=spec["schedule_clause"],
            status_mode=spec["status_mode"],
            flow_id=spec["flow_id"],
        )

    def build_execution_metadata(self) -> Data:
        spec = self._provision_and_get_spec()
        return Data(
            data={
                "trigger_component": "SnowflakeScheduleTrigger",
                "trigger_type": "SCHEDULED_RUN",
                "flow_id": spec["flow_id"],
                "flow_short_id": spec["flow_short_id"],
                "database": spec["database"],
                "schema": spec["schema"],
                "task_name": spec["task_name"],
                "task_fqn": spec["task_fqn"],
                "schedule_clause": spec["schedule_clause"],
                "status_mode": spec["status_mode"],
                "active_role": LIVE_BRIDGE.active_role,
                "create_task_sql": spec["create_task_sql"],
                "alter_state_sql": spec["alter_state_sql"],
                "ddl": spec["ddl"],
                "statements": spec["statements"],
            }
        )
