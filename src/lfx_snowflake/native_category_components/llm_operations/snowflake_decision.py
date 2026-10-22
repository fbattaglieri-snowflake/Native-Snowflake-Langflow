from __future__ import annotations

import json

from lfx.custom import Component
from lfx.io import DropdownInput, MessageTextInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx.schema.message import Message
from lfx_snowflake.components.ai_function import SUPPORTED_AI_COMPLETE_MODELS
from lfx_snowflake.components.llm_operations import SnowflakeDecisionEngine
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


class SnowflakeDecisionComponent(Component):
    display_name = "Snowflake Decision"
    description = (
        "Governed Snowflake Decision component (snowflake-decision) supporting "
        "AI_COMPLETE (Structured Decision JSON enforcing {decision, confidence, reasoning, selected_branch, policy_action}) "
        "and Cortex SystemOne API (Jev-compatible) with Decision JSON, Selected Branch, and Audit Record outputs."
    )
    icon = "Snowflake"
    name = "SnowflakeDecision"
    legacy_name = "snowflake-decision"

    inputs = [
        DropdownInput(
            name="engine_mode",
            display_name="Decision Engine Mode",
            options=[
                "AI_COMPLETE (Structured Decision JSON)",
                "Cortex SystemOne API (Jev-compatible)",
            ],
            value="AI_COMPLETE (Structured Decision JSON)",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="model",
            display_name="Model",
            options=list(SUPPORTED_AI_COMPLETE_MODELS),
            value="claude-4-sonnet",
            combobox=True,
        ),
        MessageTextInput(
            name="input_context",
            display_name="Input Context / Payload",
            value="Request to approve governed pipeline deployment",
        ),
        MultilineInput(
            name="decision_policy",
            display_name="Decision Policy & Criteria",
            value="Approve when RBAC checks pass; route to review when manual confirmation is required; reject on policy violation.",
        ),
        StrInput(
            name="branches_csv",
            display_name="Candidate Branches (CSV)",
            value="approve,review,reject",
        ),
    ]
    outputs = [
        Output(display_name="Decision JSON", name="decision_json", method="get_decision_json"),
        Output(display_name="Selected Branch", name="selected_branch", method="get_selected_branch"),
        Output(display_name="Audit Record", name="audit_record", method="get_audit_record"),
    ]

    def _compute(self) -> dict:
        return SnowflakeDecisionEngine.evaluate_decision(
            engine_mode=self.engine_mode or "AI_COMPLETE (Structured Decision JSON)",
            model=self.model or "claude-4-sonnet",
            input_context=self.input_context or "",
            decision_policy=self.decision_policy or "",
            branches_csv=self.branches_csv or "approve,review,reject",
            active_role=LIVE_BRIDGE.active_role,
        )

    def get_decision_json(self) -> Message:
        res = self._compute()
        LIVE_BRIDGE.execute_sql(res["sql_preview"])
        return Message(text=json.dumps(res, indent=2))

    def get_selected_branch(self) -> Message:
        res = self._compute()
        return Message(text=str(res["selected_branch"]))

    def get_audit_record(self) -> Data:
        res = self._compute()
        return Data(
            data={
                "component": "snowflake-decision",
                "active_role": LIVE_BRIDGE.active_role,
                "context_epoch": LIVE_BRIDGE.context_epoch,
                "decision": res["decision"],
                "confidence": res["confidence"],
                "selected_branch": res["selected_branch"],
                "policy_action": res["policy_action"],
                "engine_mode": res["engine_mode"],
                "sql_preview": res["sql_preview"],
            }
        )
