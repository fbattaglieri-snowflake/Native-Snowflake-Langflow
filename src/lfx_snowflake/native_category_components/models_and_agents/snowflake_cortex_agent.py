from __future__ import annotations

import json
import re
from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, MessageTextInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.native_category_components.llm_operations.snowflake_object_provisioner import (
    _resolve_flow_id,
)
from lfx_snowflake.providers.language_models import (
    CortexAgentToolSchema,
    build_structured_tool,
    sanitize_tool_identifier,
)

START_FRESH_THREAD_LABEL = "+ Start Fresh Thread (Auto-Create on Next Turn)"
THREAD_CONTINUITY_MODES = [
    "Resume Latest Thread of this Agent",
    "One Thread per Playground Session (Session ID)",
    "Select Specific Thread / Start Fresh Thread",
]


def _agent_databases(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _agent_schemas(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _cortex_agents(
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    agents = list(LIVE_BRIDGE.get_context_objects(database, schema, "cortex_agents", flow_id=eff_fid))
    if not agents:
        agents = [f"{database}.{schema}.EXAMPLE_AGENT", "EXAMPLE_AGENT"]
    return agents


def _thread_dropdown_options(
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    agent_name: str = "EXAMPLE_AGENT",
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    short_ag = (agent_name or "EXAMPLE_AGENT").strip().split(".")[-1].upper()
    agent_fqn = f"{(database or 'SNOWFLAKE_LANGFLOW').strip().upper()}.{(schema or 'CORE').strip().upper()}.{short_ag}"
    threads = LIVE_BRIDGE.list_cortex_threads(
        origin_application="langflow",
        flow_id=eff_fid,
        agent_fqn=agent_fqn,
    )
    opts = [START_FRESH_THREAD_LABEL]
    for item in threads:
        if not isinstance(item, dict):
            continue
        tid = item.get("thread_id")
        if tid is None:
            continue
        t_name = item.get("thread_name") or f"Thread #{tid}"
        updated = item.get("updated_on") or item.get("created_on") or "active"
        label = f"Thread #{tid} — {t_name} ({updated})"
        if label not in opts:
            opts.append(label)
    return opts


class SnowflakeCortexAgentComponent(Component):
    display_name = "Cortex Agent"
    description = (
        "Invoke a Snowflake Cortex Agent (/api/v2/databases/{db}/schemas/{schema}/agents/{name}:run) "
        "via cascading Database -> Schema -> Cortex Agent selection with 2-level Snowflake Threads API "
        "stateful continuity and automatic parent_message_id recovery under CURRENT_ROLE()."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAgent"

    inputs = [
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
        BoolInput(
            name="resume_thread",
            display_name="Resume Thread (Stateful Conversation)",
            value=True,
            real_time_refresh=True,
            info=(
                "When enabled, preserves multi-turn conversation continuity via Snowflake Threads API "
                "(/api/v2/cortex/threads) and SQLite snowflake_cortex_agent_threads."
            ),
        ),
        DropdownInput(
            name="thread_continuity_mode",
            display_name="Thread Continuity Mode",
            options=list(THREAD_CONTINUITY_MODES),
            value="Resume Latest Thread of this Agent",
            real_time_refresh=True,
            show=True,
        ),
        DropdownInput(
            name="conversation_thread",
            display_name="Active Snowflake Thread",
            options=_thread_dropdown_options("SNOWFLAKE_LANGFLOW", "CORE", "EXAMPLE_AGENT"),
            value=START_FRESH_THREAD_LABEL,
            real_time_refresh=True,
            refresh_button=True,
            combobox=True,
            show=True,
        ),
        MessageTextInput(
            name="user_message",
            display_name="User Message",
            value="Summarize active flow metrics",
            tool_mode=True,
        ),
        StrInput(
            name="session_id",
            display_name="Session ID",
            value="",
            advanced=True,
            info="Optional Playground Session ID override for thread isolation.",
        ),
        DropdownInput(
            name="tool_choice",
            display_name="Tool Choice",
            options=["auto", "required", "none"],
            value="auto",
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
    ]
    outputs = [
        Output(
            display_name="Response",
            name="response",
            types=["Message"],
            method="invoke_agent",
        ),
        Output(
            display_name="Structured Response",
            name="structured_response",
            types=["Data"],
            method="invoke_agent_structured",
        ),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _agent_databases(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or LIVE_BRIDGE.database or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _agent_schemas(db, flow_id=flow_id)
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
            agent_opts = _cortex_agents(db, sch, flow_id=flow_id)
            build_config["cortex_agent"]["options"] = agent_opts
            if field_name == "cortex_agent" and isinstance(field_value, str):
                build_config["cortex_agent"]["value"] = field_value
            elif agent_opts and build_config["cortex_agent"].get("value") not in agent_opts:
                build_config["cortex_agent"]["value"] = agent_opts[0]
        selected_agent = (
            field_value
            if field_name == "cortex_agent" and isinstance(field_value, str)
            else build_config.get("cortex_agent", {}).get("value") or "EXAMPLE_AGENT"
        )

        resume_val = (
            bool(field_value)
            if field_name == "resume_thread"
            else bool(build_config.get("resume_thread", {}).get("value", True))
        )
        if "resume_thread" in build_config and field_name == "resume_thread":
            build_config["resume_thread"]["value"] = resume_val

        if "thread_continuity_mode" in build_config:
            build_config["thread_continuity_mode"]["show"] = resume_val
            if field_name == "thread_continuity_mode" and isinstance(field_value, str):
                build_config["thread_continuity_mode"]["value"] = field_value

        if "conversation_thread" in build_config:
            build_config["conversation_thread"]["show"] = resume_val
            thread_opts = _thread_dropdown_options(db, sch, selected_agent, flow_id=flow_id)
            build_config["conversation_thread"]["options"] = thread_opts
            if field_name == "conversation_thread" and isinstance(field_value, str) and field_value:
                build_config["conversation_thread"]["value"] = field_value
                m_tid = re.search(r"Thread\s*#(\d+)", field_value)
                if m_tid:
                    LIVE_BRIDGE.sync_cortex_thread_to_playground_messages(
                        thread_id=int(m_tid.group(1)),
                        flow_id=flow_id,
                        session_id=build_config.get("session_id", {}).get("value") or flow_id,
                        agent_name=selected_agent,
                    )
            elif not build_config["conversation_thread"].get("value"):
                build_config["conversation_thread"]["value"] = START_FRESH_THREAD_LABEL

        return build_config

    def _execute_agent_turn(self, user_message_override: str | None = None) -> dict[str, Any]:
        db = (getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "CORE") or "CORE").strip()
        agent = (
            getattr(self, "cortex_agent", None)
            or getattr(self, "agent_target", "EXAMPLE_AGENT")
            or "EXAMPLE_AGENT"
        ).strip()
        short_agent = agent.split(".")[-1]
        agent_fqn = f"{db}.{sch}.{short_agent}"
        prompt = (
            user_message_override
            if user_message_override is not None
            else (getattr(self, "user_message", None) or getattr(self, "user_prompt", "") or "Summarize active flow metrics")
        )
        flow_id = _resolve_flow_id(self) or "default"
        sess_id = (
            str(getattr(self, "session_id", "") or "").strip()
            or str(getattr(getattr(self, "graph", None), "session_id", "") or "").strip()
            or flow_id
        )
        resume_thread = bool(getattr(self, "resume_thread", True))
        continuity_mode = str(getattr(self, "thread_continuity_mode", "Resume Latest Thread of this Agent") or "Resume Latest Thread of this Agent")
        conv_thread = str(getattr(self, "conversation_thread", "") or "")
        explicit_tid = getattr(self, "thread_id", None)
        explicit_pmid = getattr(self, "parent_message_id", None)
        tool_choice = str(getattr(self, "tool_choice", "auto") or "auto")

        inv = LIVE_BRIDGE.invoke_cortex_agent(
            database=db,
            schema=sch,
            agent_name=short_agent,
            user_message=str(prompt),
            flow_id=flow_id,
            session_id=sess_id,
            resume_thread=resume_thread,
            thread_continuity_mode=continuity_mode,
            conversation_thread=conv_thread,
            thread_id=explicit_tid if str(explicit_tid or "").strip() else None,
            parent_message_id=explicit_pmid if str(explicit_pmid or "").strip() else None,
            tool_choice=tool_choice,
        )
        result_payload = {
            **inv,
            "database": db,
            "schema_name": sch,
            "cortex_agent": agent,
            "agent_target": agent_fqn,
            "discovery_sql": f"SHOW CORTEX AGENTS IN SCHEMA {db}.{sch}",
            "rest_endpoint": f"/api/v2/databases/{db}/schemas/{sch}/agents/{short_agent}:run",
            "active_role": inv.get("active_role") or LIVE_BRIDGE.active_role,
            "context_epoch": inv.get("context_epoch", LIVE_BRIDGE.context_epoch),
            "user_message": str(prompt),
            "prompt": str(prompt),
        }
        self.status = result_payload.get("thread_indicator") or result_payload.get("text") or "Completed"
        return result_payload

    def invoke_agent(self) -> Message:
        payload = self._execute_agent_turn()
        return Message(
            text=json.dumps(payload, indent=2),
            thread_id=payload.get("thread_id"),
            parent_message_id=payload.get("parent_message_id"),
            assistant_message_id=payload.get("assistant_message_id"),
            agent_fqn=payload.get("agent_fqn"),
        )

    def invoke_agent_structured(self) -> Data:
        payload = self._execute_agent_turn()
        return Data(data=payload)

    def to_toolkit(self) -> list[Any]:
        db = (getattr(self, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW").strip()
        sch = (getattr(self, "schema_name", "CORE") or "CORE").strip()
        agent = (getattr(self, "cortex_agent", "EXAMPLE_AGENT") or "EXAMPLE_AGENT").strip()
        short_agent = agent.split(".")[-1]
        tool_name = (
            f"cortex_agent_{sanitize_tool_identifier(db)}_"
            f"{sanitize_tool_identifier(sch)}_{sanitize_tool_identifier(short_agent)}"
        )

        def _run_cortex_agent_tool(user_message: str = "", **kwargs: Any) -> dict[str, Any]:
            q = (
                user_message
                or kwargs.get("question")
                or kwargs.get("query")
                or kwargs.get("input")
                or getattr(self, "user_message", "Summarize active flow metrics")
            )
            if isinstance(q, dict):
                q = q.get("user_message") or q.get("question") or q.get("query") or str(q)
            self.user_message = str(q)
            return self._execute_agent_turn(user_message_override=str(q))

        return [
            build_structured_tool(
                name=tool_name,
                description=(
                    f"Invoke Snowflake Cortex Agent ({db}.{sch}.{short_agent}) via "
                    f"/api/v2/databases/{db}/schemas/{sch}/agents/{short_agent}:run with governed Threads API continuity."
                ),
                func=_run_cortex_agent_tool,
                args_schema=CortexAgentToolSchema,
                metadata={
                    "tool_mode": True,
                    "database": db,
                    "schema_name": sch,
                    "cortex_agent": short_agent,
                    "rest_endpoint": f"/api/v2/databases/{db}/schemas/{sch}/agents/{short_agent}:run",
                },
            )
        ]


# Backward-compatibility alias for imports referencing SnowflakeCortexAgentA2AComponent
SnowflakeCortexAgentA2AComponent = SnowflakeCortexAgentComponent

