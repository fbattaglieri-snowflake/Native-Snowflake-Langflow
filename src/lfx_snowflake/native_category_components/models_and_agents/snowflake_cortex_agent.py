from __future__ import annotations

import json
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DropdownInput, MessageTextInput, Output, StrInput
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _agent_databases() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _agent_schemas(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


def _cortex_agents(database: str = "SNOWFLAKE_LANGFLOW", schema: str = "CORE") -> list[str]:
    agents = list(LIVE_BRIDGE.get_context_objects(database, schema, "cortex_agents"))
    if not agents:
        agents = [f"{database}.{schema}.EXAMPLE_AGENT", "EXAMPLE_AGENT"]
    return agents


class SnowflakeCortexAgentA2AComponent(Component):
    display_name = "Cortex Agent & A2A"
    description = (
        "Invoke a Snowflake Cortex Agent (/api/v2/databases/{db}/schemas/{schema}/agents/{name}:run "
        "or /api/v2/cortex/agent:run with thread_id and parent_message_id) via cascading "
        "Database -> Schema -> Cortex Agent selection, or an internal Langflow A2A Client under the active role."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAgentA2A"

    inputs = [
        DropdownInput(
            name="mode",
            display_name="Agent Protocol Mode",
            options=["Cortex Agent Run API", "Langflow A2A Client"],
            value="Cortex Agent Run API",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=_agent_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_agent_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="cortex_agent",
            display_name="Cortex Agent (in Schema)",
            options=_cortex_agents("SNOWFLAKE_LANGFLOW", "CORE"),
            value="EXAMPLE_AGENT",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="a2a_endpoint",
            display_name="A2A Endpoint (for Langflow A2A Client mode)",
            value="",
            advanced=True,
        ),
        StrInput(
            name="thread_id",
            display_name="Cortex Thread ID (optional)",
            value="",
            advanced=True,
        ),
        StrInput(
            name="parent_message_id",
            display_name="Parent Message ID (optional)",
            value="",
            advanced=True,
        ),
        MessageTextInput(
            name="user_message",
            display_name="User Message",
            value="Summarize active flow metrics",
        ),
    ]
    outputs = [
        Output(display_name="Agent Response", name="agent_response", method="invoke_agent"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _agent_databases()
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _agent_schemas(db)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            cur_sch = build_config["schema_name"].get("value")
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and cur_sch not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        if "cortex_agent" in build_config:
            agent_opts = _cortex_agents(db, sch)
            build_config["cortex_agent"]["options"] = agent_opts
            if agent_opts and build_config["cortex_agent"].get("value") not in agent_opts:
                build_config["cortex_agent"]["value"] = agent_opts[0]
        return build_config

    def invoke_agent(self) -> Message:
        mode = getattr(self, "mode", None) or getattr(self, "agent_mode", "Cortex Agent Run API")
        db = (getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "CORE") or "CORE").strip()
        agent = (getattr(self, "cortex_agent", None) or getattr(self, "agent_target", "EXAMPLE_AGENT") or "EXAMPLE_AGENT").strip()
        agent_fqn = agent if "." in agent else f"{db}.{sch}.{agent}"
        prompt = getattr(self, "user_message", None) or getattr(self, "user_prompt", "")
        a2a_ep = getattr(self, "a2a_endpoint", "") or ""
        return Message(
            text=json.dumps(
                {
                    "mode": mode,
                    "agent_mode": mode,
                    "database": db,
                    "schema_name": sch,
                    "cortex_agent": agent,
                    "agent_target": a2a_ep if mode == "Langflow A2A Client" and a2a_ep else agent_fqn,
                    "a2a_endpoint": a2a_ep or None,
                    "discovery_sql": f"SHOW CORTEX AGENTS IN SCHEMA {db}.{sch}",
                    "rest_endpoint": f"/api/v2/databases/{db}/schemas/{sch}/agents/{agent.split('.')[-1]}:run",
                    "active_role": LIVE_BRIDGE.active_role,
                    "context_epoch": LIVE_BRIDGE.context_epoch,
                    "thread_id": self.thread_id or "new-thread",
                    "parent_message_id": self.parent_message_id or None,
                    "user_message": prompt,
                    "prompt": prompt,
                },
                indent=2,
            )
        )
