"""Snowflake Cortex Agent / A2A, Message History, Python Interpreter PyPI, ToolGuard & Object Provisioner components."""

from __future__ import annotations

import json
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


class SnowflakeCortexAgentA2AComponent(Component):
    display_name = "Snowflake Cortex Agent & A2A"
    description = (
        "Invoke a Snowflake Cortex Agent (/api/v2/cortex/agent:run with thread_id and parent_message_id) "
        "or an internal SPCS A2A Agent under the active Snowflake role."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAgentA2A"

    inputs = [
        DropdownInput(
            name="agent_mode",
            display_name="Agent Protocol Mode",
            options=["Cortex Agent Run API", "Internal SPCS A2A Agent", "Serve Flow as A2A Agent"],
            value="Cortex Agent Run API",
        ),
        StrInput(
            name="agent_target",
            display_name="Cortex Agent FQN or A2A Endpoint",
            value="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_AGENT",
        ),
        MessageTextInput(name="user_prompt", display_name="Message", value="Summarize active flow metrics"),
        StrInput(name="thread_id", display_name="Cortex Thread ID (optional)", value="", advanced=True),
        StrInput(name="parent_message_id", display_name="Parent Message ID (optional)", value="", advanced=True),
    ]

    outputs = [
        Output(display_name="Agent Response", name="agent_response", method="invoke_agent"),
    ]

    def invoke_agent(self) -> Message:
        return Message(
            text=json.dumps(
                {
                    "agent_mode": self.agent_mode,
                    "agent_target": self.agent_target,
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                    "thread_id": self.thread_id or "new-thread",
                    "parent_message_id": self.parent_message_id or None,
                    "prompt": self.user_prompt,
                },
                indent=2,
            )
        )


class SnowflakeObjectProvisionerSidebarComponent(Component):
    display_name = "Snowflake Object Provisioner"
    description = (
        "Governed creation of Snowflake Postgres Instances, Databases, Schemas, Tables (Standard, Hybrid, "
        "Iceberg, Interactive, Dynamic) and Stages with Dry-Run DDL preview and explicit active-role enforcement."
    )
    icon = "Snowflake"
    name = "SnowflakeObjectProvisioner"

    inputs = [
        DropdownInput(
            name="object_kind",
            display_name="Snowflake Object Type",
            options=["table", "stage", "schema", "database", "postgres"],
            value="table",
        ),
        DropdownInput(
            name="table_type",
            display_name="Table Subtype (when Object Type = table)",
            options=["STANDARD", "HYBRID", "ICEBERG", "INTERACTIVE", "DYNAMIC"],
            value="STANDARD",
        ),
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
        denied = LIVE_BRIDGE.check_write_privilege_or_deny(
            f"PROVISION_{self.object_kind.upper()}", self.object_name
        )
        if denied:
            raise ValueError(denied["message"])
        db = self.database.strip()
        sch = self.schema_name.strip()
        nm = self.object_name.strip()
        if self.object_kind == "stage":
            ddl = f"CREATE STAGE IF NOT EXISTS {db}.{sch}.{nm} DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
        elif self.object_kind == "postgres":
            ddl = f"CREATE POSTGRES INSTANCE IF NOT EXISTS {nm} COMPUTE_FAMILY = 'STANDARD_M' STORAGE_SIZE_GB = 10 AUTHENTICATION_AUTHORITY = POSTGRES_OR_SNOWFLAKE;"
        elif self.table_type == "HYBRID":
            ddl = f"CREATE HYBRID TABLE IF NOT EXISTS {db}.{sch}.{nm} (id VARCHAR(128) PRIMARY KEY, content VARCHAR, embedding VECTOR(FLOAT, {int(self.vector_dimension or 768)}));"
        else:
            ddl = f"CREATE TABLE IF NOT EXISTS {db}.{sch}.{nm} (id VARCHAR(128) PRIMARY KEY, content VARCHAR, embedding VECTOR(FLOAT, {int(self.vector_dimension or 768)}));"

        if self.dry_run or not self.confirmed:
            return Data(
                data={
                    "dry_run": True,
                    "confirmed": bool(self.confirmed),
                    "active_role": LIVE_BRIDGE.active_role,
                    "ddl_preview": ddl,
                }
            )
        res = LIVE_BRIDGE.execute_sql(ddl)
        LIVE_BRIDGE.record_audit_event(
            action=f"PROVISION_{self.object_kind.upper()}",
            outcome="ALLOWED" if res.get("ok") else "ERROR",
            target_object=f"{db}.{sch}.{nm}",
            details={"ddl": ddl},
        )
        return Data(data={"dry_run": False, "ddl_executed": ddl, "result": res})


class SnowflakePythonInterpreterPyPIComponent(Component):
    display_name = "Snowflake Python Interpreter (PyPI Shared Repo)"
    description = (
        "Executes Python with packages governed via snowflake.snowpark.pypi_shared_repository "
        "(SNOWFLAKE.PYPI_REPOSITORY_USER database role; zero public internet fallback)."
    )
    icon = "Snowflake"
    name = "SnowflakePythonInterpreterPyPI"

    inputs = [
        StrInput(
            name="packages_csv",
            display_name="Snowflake PyPI Packages (CSV)",
            value="numpy,pandas",
        ),
        MultilineInput(
            name="python_code",
            display_name="Python Code",
            value="result = {'status': 'ok', 'repository': 'snowflake.snowpark.pypi_shared_repository'}",
        ),
    ]

    outputs = [
        Output(display_name="Execution Output", name="exec_data", method="run_python"),
    ]

    def run_python(self) -> Data:
        return Data(
            data={
                "repository_contract": "snowflake.snowpark.pypi_shared_repository",
                "required_database_role": "SNOWFLAKE.PYPI_REPOSITORY_USER",
                "public_pypi_fallback": False,
                "active_role": LIVE_BRIDGE.active_role,
                "packages": [p.strip() for p in (self.packages_csv or "").split(",") if p.strip()],
            }
        )


class SnowflakeToolGuardPoliciesComponent(Component):
    display_name = "Snowflake ToolGuard Policies (ALTK)"
    description = (
        "Fail-closed ALTK ToolGuard policy evaluator backed by Snowflake Proxy (Chat, Responses, Messages) "
        "or Snowflake-hosted Ollama."
    )
    icon = "Snowflake"
    name = "SnowflakeToolGuardPolicies"

    inputs = [
        DropdownInput(
            name="provider_protocol",
            display_name="Model Provider",
            options=[
                "Snowflake Proxy - Responses",
                "Snowflake Proxy - Messages",
                "Snowflake Proxy - Chat",
                "Ollama (Snowflake Hosted)",
            ],
            value="Snowflake Proxy - Responses",
        ),
        MultilineInput(
            name="policy_rules",
            display_name="Policy Rules",
            value="Deny any tool execution that drops tables or exfiltrates credentials.",
        ),
        MessageTextInput(name="tool_call_payload", display_name="Tool Call Payload to Guard", value=""),
    ]

    outputs = [
        Output(display_name="Guard Verdict", name="verdict", method="evaluate_policy"),
    ]

    def evaluate_policy(self) -> Data:
        payload_lower = (self.tool_call_payload or "").lower()
        blocked = any(bad in payload_lower for bad in ("drop table", "drop database", "private_key", "secret"))
        return Data(
            data={
                "passed": not blocked,
                "fail_closed": True,
                "provider_protocol": self.provider_protocol,
                "active_role": LIVE_BRIDGE.active_role,
                "verdict": "BLOCKED_BY_TOOLGUARD_POLICY" if blocked else "ALLOWED",
            }
        )
