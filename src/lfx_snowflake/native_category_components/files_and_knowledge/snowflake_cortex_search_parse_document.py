from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import Data

from lfx_snowflake.components.files_and_stage import (
    AI_PARSE_DOCUMENT_BILLING_NOTICE,
    CortexSearchParseDocumentBuilder,
    CortexSearchParseDocumentSpec,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_stage_files import (
    _ddl_succeeded,
    _register_created_object,
    _resolve_flow_id,
)


CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1 = (
    "CORTEX SEARCH 4-NODE PIPELINE GUIDE (W2-R2-11):\n"
    "• One-Time / Initial Build (1 -> 2 -> 3): Connect [1: Parse Document] (AI_PARSE_DOCUMENT -> PARSED_DOCS) "
    "-> [2: Split Text] (SPLIT_TEXT_RECURSIVE_CHARACTER -> DOC_CHUNKS) -> [3: Search Service] "
    "(CREATE CORTEX SEARCH SERVICE ... ON chunk TARGET_LAG = '1 hour').\n"
    "• Optional Continuous Automation (4: Stream + Task): Connect [4: Stream + Task] (DOC_STREAM on Stage + "
    "DOC_INGEST_TASK with atomic _DOC_STREAM_BATCH temp table) to automatically parse & chunk new/deleted Stage "
    "files into PARSED_DOCS and DOC_CHUNKS while [3: Search Service] refreshes via TARGET_LAG."
)


def _database_options(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _schema_options(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _stage_options(
    database: str | None = None,
    schema: str | None = None,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    stages = list(LIVE_BRIDGE.get_context_objects(database, schema, "stages", flow_id=eff_fid))
    if "SNOWFLAKE_LANGFLOW_STAGE" not in stages:
        stages.insert(0, "SNOWFLAKE_LANGFLOW_STAGE")
    return stages


class SnowflakeCortexSearchParseDocumentComponent(Component):
    display_name = "Cortex Search 1: Parse Document"
    description = (
        "Step 1 of the 4-node Cortex Search indexing pipeline (1: Parse Document -> 2: Split Text -> 3: Search Service, "
        "plus optional 4: Stream + Task continuous automation): parses documents on a Snowflake Stage "
        "exclusively using AI_PARSE_DOCUMENT with TO_FILE('@<DB>.<SCHEMA>.<STAGE>', relative_path), "
        "parse_mode (LAYOUT | OCR), page_split (LATERAL FLATTEN on parsed:pages), 'Testa su 1 file' "
        "(LIMIT 1 preview), error column parsed:errorInformation, and per-page billing notice."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearchParseDocument"

    inputs = [
        MultilineInput(
            name="pipeline_guide",
            display_name="Pipeline Usage Guide (1 -> 2 -> 3 Initial Build + 4 Continuous Stream + Task)",
            value=CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            advanced=False,
        ),
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
            options=_stage_options("SNOWFLAKE_LANGFLOW", "CORE"),
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
        Output(display_name="Parsed Docs Table Reference (To Step 2: Split Text)", name="parsed_table_ref", method="as_table_reference"),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        flow_id = _resolve_flow_id(self, build_config)
        if "database" in build_config:
            build_config["database"]["options"] = _database_options(flow_id=flow_id)
            if field_name == "database" and isinstance(field_value, str):
                build_config["database"]["value"] = field_value
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        schemas = _schema_options(db_val, flow_id=flow_id)
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = schemas
            cur_sch = build_config["schema_name"].get("value")
            if field_name == "schema_name" and isinstance(field_value, str):
                build_config["schema_name"]["value"] = field_value
            elif schemas and cur_sch not in schemas:
                build_config["schema_name"]["value"] = schemas[0]
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or (schemas[0] if schemas else "CORE")
        )
        if "stage_name" in build_config:
            stg_opts = _stage_options(db_val, sch_val, flow_id=flow_id)
            build_config["stage_name"]["options"] = stg_opts
            if stg_opts and build_config["stage_name"].get("value") not in stg_opts:
                build_config["stage_name"]["value"] = stg_opts[0]
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
                "stage_name": spec.stage_name,
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
                    "stage_name": spec.stage_name,
                    "stage_fqn": spec.stage_fqn,
                    "target_table": spec.target_table,
                    "target_table_fqn": spec.target_table_fqn,
                    "parse_mode": spec.parse_mode,
                    "page_split": spec.page_split,
                    "error_column": "parsed:errorInformation",
                    "billing_notice": AI_PARSE_DOCUMENT_BILLING_NOTICE,
                    "pipeline_guide": CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
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
                    "database": spec.database,
                    "schema_name": spec.schema,
                    "stage_name": spec.stage_name,
                    "stage_fqn": spec.stage_fqn,
                    "target_table": spec.target_table,
                    "target_table_fqn": spec.target_table_fqn,
                    "parse_mode": spec.parse_mode,
                    "page_split": spec.page_split,
                    "error_column": "parsed:errorInformation",
                    "billing_notice": AI_PARSE_DOCUMENT_BILLING_NOTICE,
                    "pipeline_guide": CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
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
        if _ddl_succeeded(ddl_res):
            _register_created_object(
                database=spec.database,
                schema=spec.schema,
                category="tables",
                object_name=spec.target_table,
                flow_id=_resolve_flow_id(self),
            )
        return Data(
            data={
                "tab": "Cortex Search 1: Parse Document",
                "action": "CREATE_AND_PARSE_ALL",
                "sql_function": "AI_PARSE_DOCUMENT",
                "database": spec.database,
                "schema_name": spec.schema,
                "stage_name": spec.stage_name,
                "stage_fqn": spec.stage_fqn,
                "target_table": spec.target_table,
                "target_table_fqn": spec.target_table_fqn,
                "parse_mode": spec.parse_mode,
                "page_split": spec.page_split,
                "error_column": "parsed:errorInformation",
                "billing_notice": AI_PARSE_DOCUMENT_BILLING_NOTICE,
                "pipeline_guide": CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
                "ddl_executed": ddl,
                "insert_sql_executed": batch_sql,
                "ddl_result": ddl_res,
                "insert_result": ins_res,
                "active_role": LIVE_BRIDGE.active_role,
            }
        )
