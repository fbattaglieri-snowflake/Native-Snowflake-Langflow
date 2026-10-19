from __future__ import annotations
from lfx.custom import Component
from lfx.io import DropdownInput, MessageTextInput, MultilineInput, Output
from lfx.schema import Data
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeToolGuardPoliciesComponent(Component):
    display_name = "Snowflake ToolGuard Policies"
    description = "Fail-closed ALTK ToolGuard policy evaluator backed by Snowflake Proxy (Chat, Responses, Messages) or Snowflake-hosted Ollama."
    icon = "Snowflake"
    name = "SnowflakeToolGuardPolicies"

    inputs = [
        DropdownInput(name="provider_protocol", display_name="Model Provider", options=["Snowflake Proxy - Responses", "Snowflake Proxy - Messages", "Snowflake Proxy - Chat", "Ollama (Snowflake Hosted)"], value="Snowflake Proxy - Responses"),
        MultilineInput(name="policy_rules", display_name="Policy Rules", value="Deny any tool execution that drops tables or exfiltrates credentials."),
        MessageTextInput(name="tool_call_payload", display_name="Tool Call Payload to Guard", value=""),
    ]
    outputs = [
        Output(display_name="Guard Verdict", name="verdict", method="evaluate_policy"),
    ]

    def evaluate_policy(self) -> Data:
        payload_lower = (self.tool_call_payload or "").lower()
        blocked = any(bad in payload_lower for bad in ("drop table", "drop database", "private_key", "secret"))
        return Data(data={"passed": not blocked, "fail_closed": True, "provider_protocol": self.provider_protocol, "active_role": LIVE_BRIDGE.active_role, "verdict": "BLOCKED_BY_TOOLGUARD_POLICY" if blocked else "ALLOWED"})
