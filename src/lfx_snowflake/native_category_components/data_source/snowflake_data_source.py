from __future__ import annotations
from lfx.custom import Component
from lfx.io import DropdownInput, IntInput, MultilineInput, Output, StrInput
from lfx.schema import Data, DataFrame
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

class SnowflakeDataSourceComponent(Component):
    display_name = "Snowflake"
    description = "Governed Snowflake Data Source (Standard Table, View, Hybrid Table, Dynamic Table, Interactive Table, Iceberg Table, Snowflake Postgres) bound to the active Snowflake role."
    icon = "Snowflake"
    name = "SnowflakeDataSource"

    inputs = [
        DropdownInput(name="engine", display_name="Engine", options=["Snowflake SQL", "Snowflake Postgres"], value="Snowflake SQL"),
        DropdownInput(name="object_type", display_name="Object Type", options=["Standard Table", "View", "Hybrid Table", "Dynamic Table", "Interactive Table", "Iceberg Table"], value="Standard Table"),
        StrInput(name="database", display_name="Database", value="SNOWFLAKE_LANGFLOW"),
        StrInput(name="schema_name", display_name="Schema", value="CORE"),
        StrInput(name="object_name", display_name="Table / View Name", value="LANGFLOW_FLOW_RUNS"),
        MultilineInput(name="sql_filter", display_name="Optional SQL Query or WHERE Filter", value="", advanced=True),
        IntInput(name="max_rows", display_name="Max Rows", value=100, advanced=True),
    ]
    outputs = [
        Output(display_name="DataFrame", name="dataframe", method="read_dataframe"),
        Output(display_name="Metadata", name="metadata", method="read_metadata"),
    ]

    def _build_query(self) -> str:
        custom = (self.sql_filter or "").strip()
        if custom.upper().startswith(("SELECT ", "SHOW ")):
            return custom
        fqn = f"{self.database.strip()}.{self.schema_name.strip()}.{self.object_name.strip()}"
        where = f" WHERE {custom}" if custom else ""
        return f"SELECT * FROM {fqn}{where} LIMIT {max(1, int(self.max_rows or 100))}"

    def read_dataframe(self) -> DataFrame:
        res = LIVE_BRIDGE.execute_sql(self._build_query())
        if not res.get("ok"):
            raise ValueError(res.get("message", "Snowflake query failed"))
        return DataFrame(res.get("rows", []))

    def read_metadata(self) -> Data:
        res = LIVE_BRIDGE.execute_sql(self._build_query())
        return Data(data={"engine": self.engine, "object_type": self.object_type, "qualified_name": f"{self.database}.{self.schema_name}.{self.object_name}", "active_role": LIVE_BRIDGE.active_role, "context_epoch": LIVE_BRIDGE.context_epoch, "row_count": len(res.get("rows", [])), "ok": res.get("ok", False)})
