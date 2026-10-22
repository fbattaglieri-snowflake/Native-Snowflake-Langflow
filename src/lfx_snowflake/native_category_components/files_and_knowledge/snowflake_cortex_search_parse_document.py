from __future__ import annotations

from typing import Any

from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.components.files_and_stage import (
    AI_PARSE_DOCUMENT_BILLING_NOTICE,
    CortexSearchParseDocumentBuilder,
    CortexSearchParseDocumentSpec,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _database_options() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    dbs = list(ctx.get("databases") or [])
    default_db = getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
    if default_db and default_db not in dbs:
        dbs.insert(0, default_db)
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def _schema_options(database: str | None = None) -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    db = (database or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
    schemas = list((ctx.get("schemas_by_db") or {}).get(db) or [])
    default_schema = getattr(LIVE_BRIDGE, "schema", "CORE")
    if default_schema and default_schema not in schemas:
        schemas.insert(0, default_schema)
    return schemas or ["CORE", "PUBLIC"]


def _stage_options() -> list[str]:
    ctx = getattr(LIVE_BRIDGE, "selected_context", {}) or {}
    vis = ctx.get("visible_objects") or {}
    stages = list(vis.get("stages") or [])
    if "SNOWFLAKE_LANGFLOW_STAGE" not in stages:
        stages.insert(0, "SNOWFLAKE_LANGFLOW_STAGE")
    return stages


class SnowflakeCortexSearchParseDocumentComponent(Component):
    display_name = "Cortex Search 1: Parse Document"
    description = (
        "Tab 1 of the Cortex Search indexing pipeline: parses documents on a Snowflake Stage "
        "exclusively using AI_PARSE_DOCUMENT with TO_FILE('@<DB>.<SCHEMA>.<STAGE>', relative_path), "
        "parse_mode (LAYOUT | OCR), page_split (LATERAL FLATTEN on parsed:pages), 'Testa su 1 file' "
        "(LIMIT 1 preview), error column parsed:errorInformation, and per-page billing notice."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearchParseDocument"

    inputs = [
        DropdownInput(
            name="database",
            display_name="1. Database",
            options=_database_options(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="2. Schema",
            options=_schema_options("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="stage_name",
            display_name="3. Stage",
            options=_stage_options(),
            value="SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        StrInput(
            name="target_table",
            display_name="Target Parsed Table",
            value="PARSED_DOCS",
        ),
        DropdownInput(
            name="parse_mode",
            display_name="AI_PARSE_DOCUMENT Mode",
            options=["LAYOUT", "OCR"],
            value="LAYOUT",
        ),
        BoolInput(
            name="page_split",
            display_name="Split by Page (LATERAL FLATTEN parsed:pages)",
            value=True,
        ),
        StrInput(
            name="file_extensions_filter",
            display_name="File Extensions Filter (CSV)",
            value=".pdf,.docx,.pptx,.html,.txt,.md",
        ),
        DropdownInput(
            name="action_mode",
            display_name="Execution Mode",
            options=[
                "Testa su 1 file (LIMIT 1 Preview)",
                "Dry-Run SQL Preview",
                "Create Table & Parse All Stage Files",
            ],
            value="Testa su 1 file (LIMIT 1 Preview)",
        ),
        StrInput(
            name="billing_notice",
            display_name="AI_PARSE_DOCUMENT Billing Notice",
            value=AI_PARSE_DOCUMENT_BILLING_NOTICE,
            advanced=False,
        ),
    ]
    outputs = [
        Output(display_name="Parse Document Result", name="parse_result", method="run_parse_document"),
        Output(display_name="Parsed Docs Table Reference", name="parsed_table_ref", method="as_table_reference"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _database_options()
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _schema_options(db_val)
        if "stage_name" in build_config:
            build_config["stage_name"]["options"] = _stage_options()
        return build_config

    def _build_spec_and_builder(self) -> tuple[CortexSearchParseDocumentSpec, CortexSearchParseDocumentBuilder]:
        db = (getattr(self, "database", "") or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
        sch = (getattr(self, "schema_name", "") or getattr(LIVE_BRIDGE, "schema", "CORE")).strip()
        stg = (getattr(self, "stage_name", "") or "SNOWFLAKE_LANGFLOW_STAGE").lstrip("@").strip()
        if "." in stg:
            stg = stg.split(".")[-1]
        tbl = (getattr(self, "target_table", "") or "PARSED_DOCS").strip()
        mode = (getattr(self, "parse_mode", "LAYOUT") or "LAYOUT").strip().upper()
        page_split = bool(getattr(self, "page_split", True))
        exts_raw = getattr(self, "file_extensions_filter", ".pdf,.docx,.pptx,.html,.txt,.md") or ""
        exts = tuple(e.strip() for e in exts_raw.split(",") if e.strip())
        spec = CortexSearchParseDocumentSpec(
            database=db,
            schema=sch,
            stage_name=stg,
            target_table=tbl,
            parse_mode=mode,
            page_split=page_split,
            file_extensions_filter=exts or (".pdf", ".docx", ".pptx", ".html", ".txt", ".md"),
        )
        return spec, CortexSearchParseDocumentBuilder(spec)

    def as_table_reference(self) -> Data:
        spec, builder = self._build_spec_and_builder()
        return Data(
            data={
                "database": spec.database,
                "schema_name": spec.schema,
                "stage_fqn": spec.stage_fqn,
                "target_table": spec.target_table,
                "target_table_fqn": spec.target_table_fqn,
                "sql_function": builder.sql_function,
                "parse_mode": spec.parse_mode,
                "page_split": spec.page_split,
            }
        )

    def run_parse_document(self) -> Data:
        spec, builder = self._build_spec_and_builder()
        ddl = builder.build_target_table_ddl()
        test_sql = builder.build_parse_sql(test_single_file=True)
        batch_sql = builder.build_parse_sql(test_single_file=False)
        action = (getattr(self, "action_mode", "Testa su 1 file (LIMIT 1 Preview)") or "").strip()

        if "Dry-Run" in action:
            return Data(
                data={
                    "tab": "Cortex Search 1: Parse Document",
                    "sql_function": "AI_PARSE_DOCUMENT",
                    "database": spec.database,
                    "schema_name": spec.schema,
                    "stage_fqn": spec.stage_fqn,
                    "target_table_fqn": spec.target_table_fqn,
                    "parse_mode": spec.parse_mode,
                    "page_split": spec.page_split,
                    "error_column": "parsed:errorInformation",
                    "billing_notice": AI_PARSE_DOCUMENT_BILLING_NOTICE,
                    "ddl_preview": ddl,
                    "test_single_file_sql": test_sql,
                    "batch_insert_sql": batch_sql,
                    "active_role": LIVE_BRIDGE.active_role,
                }
            )

        if "Testa su 1 file" in action:
            res = LIVE_BRIDGE.execute_sql(test_sql)
            return Data(
                data={
                    "tab": "Cortex Search 1: Parse Document",
                    "action": "TEST_SINGLE_FILE",
                    "sql_function": "AI_PARSE_DOCUMENT",
                    "stage_fqn": spec.stage_fqn,
                    "target_table_fqn": spec.target_table_fqn,
                    "parse_mode": spec.parse_mode,
                    "page_split": spec.page_split,
                    "error_column": "parsed:errorInformation",
                    "billing_notice": AI_PARSE_DOCUMENT_BILLING_NOTICE,
                    "executed_sql": test_sql,
                    "preview_rows": res.get("rows", []),
                    "ok": res.get("ok", False),
                    "active_role": LIVE_BRIDGE.active_role,
                }
            )

        denied = LIVE_BRIDGE.check_write_privilege_or_deny("PARSE_DOCUMENT_INSERT", spec.target_table_fqn)
        if denied:
            raise ValueError(denied["message"])
        ddl_res = LIVE_BRIDGE.execute_sql(ddl)
        ins_res = LIVE_BRIDGE.execute_sql(batch_sql)
        return Data(
            data={
                "tab": "Cortex Search 1: Parse Document",
                "action": "CREATE_AND_PARSE_ALL",
                "sql_function": "AI_PARSE_DOCUMENT",
                "stage_fqn": spec.stage_fqn,
                "target_table_fqn": spec.target_table_fqn,
                "parse_mode": spec.parse_mode,
                "page_split": spec.page_split,
                "error_column": "parsed:errorInformation",
                "billing_notice": AI_PARSE_DOCUMENT_BILLING_NOTICE,
                "ddl_executed": ddl,
                "insert_sql_executed": batch_sql,
                "ddl_result": ddl_res,
                "insert_result": ins_res,
                "active_role": LIVE_BRIDGE.active_role,
            }
        )
