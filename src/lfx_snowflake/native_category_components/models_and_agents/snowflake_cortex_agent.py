from __future__ import annotations
import json
from lfx.custom import Component
from lfx.io import DropdownInput, MessageTextInput, Output, StrInput
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeCortexAgentA2AComponent(Component):
    display_name = "Snowflake Cortex Agent & A2A"
    description = "Invoke a Snowflake Cortex Agent (/api/v2/cortex/agent:run with thread_id and parent_message_id) or an internal SPCS A2A Agent under the active role."
    icon = "Snowflake"
    name = "SnowflakeCortexAgentA2A"

    inputs = [
        DropdownInput(name="agent_mode", display_name="Agent Protocol Mode", options=["Cortex Agent Run API", "Internal SPCS A2A Agent", "Serve Flow as A2A Agent"], value="Cortex Agent Run API"),
        StrInput(name="agent_target", display_name="Cortex Agent FQN or A2A Endpoint", value="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_AGENT"),
        MessageTextInput(name="user_prompt", display_name="Message", value="Summarize active flow metrics"),
        StrInput(name="thread_id", display_name="Cortex Thread ID (optional)", value="", advanced=True),
        StrInput(name="parent_message_id", display_name="Parent Message ID (optional)", value="", advanced=True),
    ]
    outputs = [
        Output(display_name="Agent Response", name="agent_response", method="invoke_agent"),
    ]

    def invoke_agent(self) -> Message:
        return Message(text=json.dumps({"agent_mode": self.agent_mode, "agent_target": self.agent_target, "active_role": LIVE_BRIDGE.active_role, "context_epoch": LIVE_BRIDGE.context_epoch, "thread_id": self.thread_id or "new-thread", "parent_message_id": self.parent_message_id or None, "prompt": self.user_prompt}, indent=2))
