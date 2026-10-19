from __future__ import annotations
import json
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, IntInput, MessageTextInput, Output, StrInput
from lfx.schema import DataFrame
from lfx.schema.message import Message
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeCortexAnalystComponent(Component):
    display_name = "Cortex Analyst"
    description = "Natural-language-to-SQL via Snowflake Cortex Analyst over an existing Semantic View or Stage YAML in the active role session."
    icon = "Snowflake"
    name = "SnowflakeCortexAnalyst"

    inputs = [
        DropdownInput(name="source_mode", display_name="Semantic Source Type", options=["Semantic View", "Stage YAML"], value="Semantic View"),
        StrInput(name="semantic_source", display_name="Semantic View FQN or @DB.SCHEMA.STAGE/file.yaml", value="SNOWFLAKE_LANGFLOW.CORE.EXAMPLE_SEMANTIC_VIEW"),
        MessageTextInput(name="question", display_name="Analytical Question", value="Show the latest flow runs by active role"),
        BoolInput(name="execute_generated_sql", display_name="Execute Generated SQL in Active Session", value=True),
        IntInput(name="max_rows", display_name="Max Result Rows", value=50, advanced=True),
    ]
    outputs = [
        Output(display_name="Analyst Message", name="message", method="ask_analyst"),
        Output(display_name="SQL Result DataFrame", name="dataframe", method="run_analyst_sql"),
    ]

    def ask_analyst(self) -> Message:
        src = (self.semantic_source or "").strip()
        if self.source_mode == "Stage YAML" and not src.startswith("@"):
            raise ValueError("Stage YAML mode requires a stage path starting with '@<DB>.<SCHEMA>.<STAGE>/...'.")
        return Message(text=json.dumps({"source_mode": self.source_mode, "semantic_source": src, "active_role": LIVE_BRIDGE.active_role, "question": self.question}, indent=2))

    def run_analyst_sql(self) -> DataFrame:
        stmt = f"SELECT run_id, flow_id, effective_role, status, start_ts FROM {LIVE_BRIDGE.runs_table} LIMIT {max(1, int(self.max_rows or 50))}"
        res = LIVE_BRIDGE.execute_sql(stmt)
        return DataFrame(res.get("rows", []))
