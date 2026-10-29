from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MultilineInput, Output, StrInput, TabInput
from lfx.schema import Data, DataFrame, Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.triggers_manager import (
    ALERT_FREQUENCY_OPTIONS,
    ALERT_MODE_OPTIONS,
    COMMON_IANA_TIMEZONES,
    COMPUTE_MODE_OPTIONS,
    STATUS_MODE_OPTIONS,
    TRIGGERS_MANAGER,
    build_alert_trigger_ddl,
    get_trigger_databases,
    get_trigger_schemas,
    get_trigger_warehouses,
    resolve_trigger_flow_id,
)


class SnowflakeAlertTrigger(Component):
    display_name = "Condition Alert Trigger"
    description = "Trigger this flow when a Snowflake SQL condition is met"
    icon = "Snowflake"
    name = "SnowflakeAlertTrigger"

    inputs = [
        TabInput(
            name="status_mode",
            display_name="Status",
            options=STATUS_MODE_OPTIONS,
            value="Active",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="alert_mode",
            display_name="Alert Mode",
            options=ALERT_MODE_OPTIONS,
            value="Periodic Condition Check",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="check_frequency",
            display_name="Check Frequency",
            options=ALERT_FREQUENCY_OPTIONS,
            value="Every 5 Minutes",
            real_time_refresh=True,
        ),
        StrInput(
            name="cron_expression",
            display_name="Cron Expression",
            value="*/5 * * * *",
            show=False,
        ),
        DropdownInput(
            name="timezone",
            display_name="Timezone",
            options=COMMON_IANA_TIMEZONES,
            value="Europe/Rome",
            combobox=True,
            show=False,
        ),
        MultilineInput(
            name="condition_sql",
            display_name="SQL Condition (fires when query returns >= 1 row)",
            value=(
                "SELECT ORDER_ID, TOTAL_AMOUNT, STATUS\n"
                "FROM SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS\n"
                "WHERE STATUS = 'FAILED' AND TOTAL_AMOUNT > 10000"
            ),
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
        IntInput(
            name="max_alert_rows",
            display_name="Max Alert Rows",
            value=100,
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
            name="suspend_after_failures",
            display_name="Suspend After Failures",
            value=5,
            advanced=True,
        ),
    ]

    outputs = [
        Output(
            display_name="Alert Rows",
            name="alert_rows",
            method="build_alert_rows",
        ),
        Output(
            display_name="Trigger Message",
            name="trigger_message_out",
            method="build_trigger_message",
        ),
        Output(
            display_name="Alert Metadata",
            name="alert_metadata",
            method="build_alert_metadata",
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

        amode = (
            field_value
            if field_name == "alert_mode" and isinstance(field_value, str)
            else build_config.get("alert_mode", {}).get("value") or "Periodic Condition Check"
        )
        is_periodic = amode == "Periodic Condition Check"
        if "check_frequency" in build_config:
            build_config["check_frequency"]["show"] = is_periodic

        freq = (
            field_value
            if field_name == "check_frequency" and isinstance(field_value, str)
            else build_config.get("check_frequency", {}).get("value") or "Every 5 Minutes"
        )
        show_cron = is_periodic and freq == "Custom (Cron)"
        if "cron_expression" in build_config:
            build_config["cron_expression"]["show"] = show_cron
        if "timezone" in build_config:
            build_config["timezone"]["show"] = show_cron

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
        ddl_spec = build_alert_trigger_ddl(
            flow_id=flow_id,
            database=getattr(self, "database", "SNOWFLAKE_LANGFLOW"),
            schema=getattr(self, "schema", "CORE"),
            status_mode=getattr(self, "status_mode", "Active"),
            alert_mode=getattr(self, "alert_mode", "Periodic Condition Check"),
            check_frequency=getattr(self, "check_frequency", "Every 5 Minutes"),
            cron_expression=getattr(self, "cron_expression", "*/5 * * * *"),
            timezone_name=getattr(self, "timezone", "Europe/Rome"),
            condition_sql=getattr(
                self,
                "condition_sql",
                "SELECT ORDER_ID, TOTAL_AMOUNT, STATUS FROM SNOWFLAKE_LANGFLOW.CORE.CUSTOMER_ORDERS WHERE STATUS = 'FAILED' AND TOTAL_AMOUNT > 10000",
            ),
            max_alert_rows=int(getattr(self, "max_alert_rows", 100) or 100),
            compute_mode=getattr(self, "compute_mode", "Serverless (Managed)"),
            warehouse=getattr(self, "warehouse", "COMPUTE_WH"),
            suspend_after_failures=int(
                getattr(self, "suspend_after_failures", 5)
                if getattr(self, "suspend_after_failures", None) is not None
                else 5
            ),
        )
        node_id = str(getattr(self, "_id", None) or getattr(self, "id", None) or "SnowflakeAlertTrigger")
        prov = TRIGGERS_MANAGER.provision_trigger(
            ddl_spec=ddl_spec,
            node_id=node_id,
            bridge=LIVE_BRIDGE,
            config={
                "compute_mode": getattr(self, "compute_mode", "Serverless (Managed)"),
                "warehouse": getattr(self, "warehouse", "COMPUTE_WH"),
                "alert_mode": getattr(self, "alert_mode", "Periodic Condition Check"),
                "check_frequency": getattr(self, "check_frequency", "Every 5 Minutes"),
                "condition_sql": ddl_spec["condition_sql"],
            },
        )
        return {**ddl_spec, "provision_result": prov}

    def build_alert_rows(self) -> DataFrame:
        spec = self._provision_and_get_spec()
        injected_rows = getattr(self, "_trigger_rows", None)
        if isinstance(injected_rows, list):
            return DataFrame(injected_rows)
        limit = max(1, min(1000, int(getattr(self, "max_alert_rows", 100) or 100)))
        res = LIVE_BRIDGE.execute_sql(f"SELECT * FROM ({spec['condition_sql']}) LIMIT {limit}")
        return DataFrame(res.get("rows", []))

    def build_trigger_message(self) -> Message:
        spec = self._provision_and_get_spec()
        rows = getattr(self, "_trigger_rows", None)
        cnt = len(rows) if isinstance(rows, list) else 0
        text = f"Snowflake Alert '{spec['alert_fqn']}' fired with {cnt} matching row(s)."
        return Message(
            text=text,
            trigger_type="ALERT_TRIGGER_RUN",
            alert_fqn=spec["alert_fqn"],
            alert_mode=spec["alert_mode"],
            row_count=cnt,
        )

    def build_alert_metadata(self) -> Data:
        spec = self._provision_and_get_spec()
        return Data(
            data={
                "trigger_component": "SnowflakeAlertTrigger",
                "trigger_type": "ALERT_TRIGGER_RUN",
                "flow_id": spec["flow_id"],
                "flow_short_id": spec["flow_short_id"],
                "database": spec["database"],
                "schema": spec["schema"],
                "alert_name": spec["alert_name"],
                "alert_fqn": spec["alert_fqn"],
                "alert_mode": spec["alert_mode"],
                "condition_sql": spec["condition_sql"],
                "status_mode": spec["status_mode"],
                "active_role": LIVE_BRIDGE.active_role,
                "change_tracking_sql": spec["change_tracking_sql"],
                "create_alert_sql": spec["create_alert_sql"],
                "alter_state_sql": spec["alter_state_sql"],
                "ddl": spec["ddl"],
                "statements": spec["statements"],
            }
        )
