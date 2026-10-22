from __future__ import annotations

from lfx.custom import Component
from lfx.io import DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.components.files_and_stage import (
    CORTEX_ANALYST_SNOWSIGHT_CREATION_MESSAGE,
    CortexAnalystDisabledCreatorSpec,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


class SnowflakeCortexAnalystDisabledCreatorComponent(Component):
    display_name = "Cortex Analyst (Create in Snowsight)"
    description = (
        "Disabled greyed-out tab in Files & Knowledge (FK-W2-04): Semantic Views and Stage YAML "
        "Semantic Models for Cortex Analyst must be created, validated, and refined in Snowsight "
        "(AI & ML -> Cortex Analyst) first, then selected in Data Sources -> Cortex Analyst or "
        "LLM Operations -> Cortex Analyst."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexAnalystDisabledCreator"

    inputs = [
        DropdownInput(
            name="creation_status",
            display_name="Creation Status (Greyed-Out / Disabled)",
            options=["[DISABLED — Use Snowsight (AI & ML -> Cortex Analyst)]"],
            value="[DISABLED — Use Snowsight (AI & ML -> Cortex Analyst)]",
        ),
        StrInput(
            name="snowsight_navigation_path",
            display_name="Snowsight Creation Path",
            value="Snowsight -> AI & ML -> Cortex Analyst (Create Semantic View / Semantic Model YAML)",
        ),
        StrInput(
            name="langflow_usage_components",
            display_name="Where to Use Existing Semantic Views / YAML Models",
            value="Data Sources -> Cortex Analyst | LLM Operations -> Cortex Analyst",
        ),
        MultilineInput(
            name="governance_notice",
            display_name="Snowsight Governance Notice",
            value=CORTEX_ANALYST_SNOWSIGHT_CREATION_MESSAGE,
        ),
    ]
    outputs = [
        Output(
            display_name="Cortex Analyst Creation Guidance",
            name="guidance",
            method="get_snowsight_guidance",
        ),
    ]

    def get_snowsight_guidance(self) -> Data:
        spec = CortexAnalystDisabledCreatorSpec()
        state = spec.to_ui_state()
        state["active_role"] = LIVE_BRIDGE.active_role
        state["creation_status"] = "[DISABLED — Use Snowsight]"
        return Data(data=state)
