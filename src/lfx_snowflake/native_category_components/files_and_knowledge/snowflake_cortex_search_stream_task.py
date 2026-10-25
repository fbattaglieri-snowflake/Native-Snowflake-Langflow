from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DataInput, DropdownInput, IntInput, MultilineInput, Output, StrInput
from lfx.schema import Data

from lfx_snowflake.components.files_and_stage import (
    CortexSearchStreamTaskBuilder,
    CortexSearchStreamTaskSpec,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_parse_document import (
    CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
)
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_stage_files import (
    _ddl_succeeded,
    _register_created_object,
    _resolve_flow_id,
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


class SnowflakeCortexSearchStreamTaskComponent(Component):
    display_name = "Cortex Search 4: Stream + Task"
    description = (
        "Step 4 (Optional Continuous Automation) of the 4-node Cortex Search indexing pipeline: creates DOC_STREAM "
        "on Stage and DOC_INGEST_TASK with WHEN SYSTEM$STREAM_HAS_DATA('DOC_STREAM') and an atomic _DOC_STREAM_BATCH "
        "temp table inside BEGIN ... END; so AI_PARSE_DOCUMENT runs at most once per file and METADATA$ACTION = 'DELETE' "
        "cleans up PARSED_DOCS and DOC_CHUNKS while Step 3 Search Service refreshes via TARGET_LAG."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearchStreamTask"

    inputs = [
        MultilineInput(
            name="pipeline_guide",
            display_name="Pipeline Usage Guide (1 -> 2 -> 3 Initial Build + 4 Continuous Stream + Task)",
            value=CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            advanced=False,
        ),
        DataInput(
            name="upstream_stage_or_service",
            display_name="Upstream Step 1 / Step 3 Reference (Chaining Port)",
            info="Connect 'Parse Document Result' (Step 1) or 'Search Service Reference' (Step 3) to auto-inherit Database, Schema, Stage, PARSED_DOCS, and DOC_CHUNKS.",
        ),
        DataInput(
            name="service_ref",
            display_name="Search Service Reference (Optional)",
            info="Optional connection from 'Cortex Search 3: Search Service'.",
            advanced=True,
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
            name="stream_name",
            display_name="Stage Stream Name",
            value="DOC_STREAM",
        ),
        StrInput(
            name="task_name",
            display_name="Ingest Task Name",
            value="DOC_INGEST_TASK",
        ),
        StrInput(
            name="parsed_table",
            display_name="Parsed Docs Table",
            value="PARSED_DOCS",
        ),
        StrInput(
            name="chunks_table",
            display_name="Chunks Table",
            value="DOC_CHUNKS",
        ),
        StrInput(
            name="warehouse",
            display_name="Task Warehouse",
            value="COMPUTE_WH",
        ),
        StrInput(
            name="schedule",
            display_name="Task Schedule",
            value="5 MINUTE",
        ),
        DropdownInput(
            name="parse_mode",
            display_name="AI_PARSE_DOCUMENT Mode",
            options=["LAYOUT", "OCR"],
            value="LAYOUT",
        ),
        DropdownInput(
            name="split_format",
            display_name="Split Format",
            options=["markdown", "none"],
            value="markdown",
        ),
        IntInput(
            name="chunk_size",
            display_name="Chunk Size (chars)",
            value=1800,
        ),
        IntInput(
            name="chunk_overlap",
            display_name="Chunk Overlap (chars)",
            value=300,
        ),
        StrInput(
            name="language",
            display_name="Language Tag",
            value="it",
        ),
        DropdownInput(
            name="action_mode",
            display_name="Stream + Task Action",
            options=[
                "Dry-Run DDL Preview",
                "Create Stream & Task (Provision Pipeline)",
                "Resume Task",
                "Suspend Task",
                "Esegui ora (EXECUTE TASK)",
                "Inspect INFORMATION_SCHEMA.TASK_HISTORY",
            ],
            value="Dry-Run DDL Preview",
        ),
    ]
    outputs = [
        Output(display_name="Stream + Task Result", name="stream_task_result", method="run_stream_task"),
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

    def _build_spec_and_builder(
        self,
    ) -> tuple[CortexSearchStreamTaskSpec, CortexSearchStreamTaskBuilder]:
        ref = getattr(self, "upstream_stage_or_service", None) or getattr(self, "service_ref", None)
        ref_data = getattr(ref, "data", ref) if ref is not None else {}
        if not isinstance(ref_data, dict):
            ref_data = {}

        db = (
            ref_data.get("database")
            or getattr(self, "database", "")
            or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")
        ).strip()
        sch = (
            ref_data.get("schema_name")
            or getattr(self, "schema_name", "")
            or getattr(LIVE_BRIDGE, "schema", "CORE")
        ).strip()
        stg = (
            ref_data.get("stage_name")
            or getattr(self, "stage_name", "")
            or "SNOWFLAKE_LANGFLOW_STAGE"
        ).lstrip("@").strip()
        if "." in stg:
            stg = stg.split(".")[-1]
        stream_name = (getattr(self, "stream_name", "") or "DOC_STREAM").strip()
        task_name = (getattr(self, "task_name", "") or "DOC_INGEST_TASK").strip()
        if "service_name" in ref_data or "service_fqn" in ref_data:
            # Connected from Step 3 (Search Service): source_table is the chunks table
            parsed_table = (
                ref_data.get("parsed_table")
                or getattr(self, "parsed_table", "")
                or "PARSED_DOCS"
            ).strip()
            chunks_table = (
                ref_data.get("source_table")
                or ref_data.get("chunks_table")
                or getattr(self, "chunks_table", "")
                or "DOC_CHUNKS"
            ).strip()
        elif "stage_name" in ref_data and "target_table" in ref_data and "source_table" not in ref_data:
            # Connected from Step 1 (Parse Document): target_table is the parsed table
            parsed_table = (
                ref_data.get("target_table")
                or getattr(self, "parsed_table", "")
                or "PARSED_DOCS"
            ).strip()
            chunks_table = (
                ref_data.get("chunks_table")
                or getattr(self, "chunks_table", "")
                or "DOC_CHUNKS"
            ).strip()
        else:
            parsed_table = (
                ref_data.get("source_table")
                or getattr(self, "parsed_table", "")
                or "PARSED_DOCS"
            ).strip()
            chunks_table = (
                ref_data.get("target_table")
                or getattr(self, "chunks_table", "")
                or "DOC_CHUNKS"
            ).strip()
        wh = (getattr(self, "warehouse", "") or getattr(LIVE_BRIDGE, "warehouse", "COMPUTE_WH") or "COMPUTE_WH").strip()
        sched = (getattr(self, "schedule", "") or "5 MINUTE").strip()
        parse_mode = (getattr(self, "parse_mode", "LAYOUT") or "LAYOUT").strip().upper()
        split_format = (getattr(self, "split_format", "markdown") or "markdown").strip().lower()
        chunk_size = int(getattr(self, "chunk_size", 1800) or 1800)
        chunk_overlap = int(getattr(self, "chunk_overlap", 300) or 300)
        lang = (getattr(self, "language", "it") or "it").strip()

        spec = CortexSearchStreamTaskSpec(
            database=db,
            schema=sch,
            stage_name=stg,
            stream_name=stream_name,
            task_name=task_name,
            parsed_table=parsed_table,
            chunks_table=chunks_table,
            warehouse=wh,
            schedule=sched,
            parse_mode=parse_mode,
            split_format=split_format,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            language=lang,
        )
        return spec, CortexSearchStreamTaskBuilder(spec)

    def run_stream_task(self) -> Data:
        spec, builder = self._build_spec_and_builder()
        stage_dir_ddl = builder.build_enable_stage_directory_ddl()
        stream_ddl = builder.build_stream_ddl()
        task_ddl = builder.build_task_ddl()
        resume_sql = builder.build_task_operation_sql("RESUME")
        suspend_sql = builder.build_task_operation_sql("SUSPEND")
        exec_now_sql = builder.build_task_operation_sql("EXECUTE_NOW")
        history_sql = builder.build_task_operation_sql("HISTORY")
        action = (getattr(self, "action_mode", "Dry-Run DDL Preview") or "Dry-Run DDL Preview").strip()

        payload: dict[str, Any] = {
            "tab": "Cortex Search 4: Stream + Task",
            "database": spec.database,
            "schema_name": spec.schema,
            "stage_name": spec.stage_name,
            "stage_fqn": spec.stage_fqn,
            "stream_fqn": spec.stream_fqn,
            "task_fqn": spec.task_fqn,
            "parsed_table_fqn": spec.parsed_table_fqn,
            "chunks_table_fqn": spec.chunks_table_fqn,
            "atomic_temp_table": "_DOC_STREAM_BATCH",
            "pipeline_guide": CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            "stage_directory_ddl": stage_dir_ddl,
            "stream_ddl": stream_ddl,
            "task_ddl": task_ddl,
            "resume_task_sql": resume_sql,
            "suspend_task_sql": suspend_sql,
            "execute_now_sql": exec_now_sql,
            "task_history_sql": history_sql,
            "active_role": LIVE_BRIDGE.active_role,
        }

        if "Dry-Run" in action:
            return Data(data=payload)

        if "TASK_HISTORY" in action:
            hist_res = LIVE_BRIDGE.execute_sql(history_sql)
            payload.update(
                {
                    "action": "TASK_HISTORY",
                    "history_rows": hist_res.get("rows", []),
                    "ok": hist_res.get("ok", False),
                }
            )
            return Data(data=payload)

        denied = LIVE_BRIDGE.check_write_privilege_or_deny("MANAGE_STREAM_AND_TASK", spec.task_fqn)
        if denied:
            raise ValueError(denied["message"])

        if "Create Stream & Task" in action:
            LIVE_BRIDGE.execute_sql(stage_dir_ddl)
            stream_res = LIVE_BRIDGE.execute_sql(stream_ddl)
            task_res = LIVE_BRIDGE.execute_sql(task_ddl)
            if _ddl_succeeded(stream_res) or _ddl_succeeded(task_res):
                eff_fid = _resolve_flow_id(self)
                _register_created_object(
                    database=spec.database,
                    schema=spec.schema,
                    category="stages",
                    object_name=spec.stage_name,
                    flow_id=eff_fid,
                )
                _register_created_object(
                    database=spec.database,
                    schema=spec.schema,
                    category="tables",
                    object_name=spec.parsed_table,
                    flow_id=eff_fid,
                )
                _register_created_object(
                    database=spec.database,
                    schema=spec.schema,
                    category="tables",
                    object_name=spec.chunks_table,
                    flow_id=eff_fid,
                )
            payload.update(
                {
                    "action": "PROVISION_STREAM_AND_TASK",
                    "stream_result": stream_res,
                    "task_result": task_res,
                    "ok": stream_res.get("ok", False) and task_res.get("ok", False),
                }
            )
            return Data(data=payload)

        op_map = {
            "Resume Task": resume_sql,
            "Suspend Task": suspend_sql,
            "Esegui ora (EXECUTE TASK)": exec_now_sql,
        }
        exec_sql = op_map.get(action, exec_now_sql)
        res = LIVE_BRIDGE.execute_sql(exec_sql)
        payload.update(
            {
                "action": action,
                "executed_sql": exec_sql,
                "result": res,
                "ok": res.get("ok", False),
            }
        )
        return Data(data=payload)
