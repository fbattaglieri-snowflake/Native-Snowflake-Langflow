from __future__ import annotations
import json
from lfx.custom import Component
from lfx.io import DropdownInput, MessageTextInput, Output, StrInput
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeSmartRouterComponent(Component):
    display_name = "Snowflake Smart Router"
    description = "Multi-protocol LLM Smart Router across Snowflake Proxy (Chat, Responses, Messages) and Ollama."
    icon = "Snowflake"
    name = "SnowflakeSmartRouter"

    inputs = [
        DropdownInput(name="protocol", display_name="Snowflake Provider Protocol", options=["Snowflake Proxy - Responses", "Snowflake Proxy - Messages", "Snowflake Proxy - Chat", "Ollama (Snowflake Hosted)"], value="Snowflake Proxy - Messages"),
        MessageTextInput(name="input_text", display_name="Input Message", value="Route this query"),
        StrInput(name="routes_csv", display_name="Configured Routes (CSV)", value="analytics,support,governance"),
    ]
    outputs = [
        Output(display_name="Selected Route", name="route_msg", method="route_input"),
    ]

    def route_input(self) -> Message:
        routes = [r.strip() for r in (self.routes_csv or "default").split(",") if r.strip()]
        text_lower = (self.input_text or "").lower()
        chosen = routes[0] if routes else "default"
        for candidate in routes:
            if candidate.lower() in text_lower:
                chosen = candidate
                break
        return Message(text=json.dumps({"selected_route": chosen, "valid_routes": routes, "protocol": self.protocol, "active_role": LIVE_BRIDGE.active_role}))
