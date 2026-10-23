from __future__ import annotations

from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import DataInput, DropdownInput, IntInput, MultilineInput, Output, StrInput
    from lfx.schema import Data
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import Component, Data, DataInput, DropdownInput, IntInput, MultilineInput, Output, StrInput

from lfx_snowflake.components.files_and_stage import (
    CORTEX_SEARCH_CHUNK_SIZE_NOTICE,
    CortexSearchSplitTextBuilder,
    CortexSearchSplitTextSpec,
)
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE
from lfx_snowflake.native_category_components.files_and_knowledge.snowflake_cortex_search_parse_document import (
    CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
)


def _database_options() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _schema_options(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


class SnowflakeCortexSearchSplitTextComponent(Component):
    display_name = "Cortex Search 2: Split Text"
    description = (
        "Step 2 of the 4-node Cortex Search indexing pipeline (1: Parse Document -> 2: Split Text -> 3: Search Service): "
        "reads PARSED_DOCS and writes DOC_CHUNKS using SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER or Python UDF "
        "(LangChain RecursiveCharacterTextSplitter), split_format (markdown | none), chunk_size=1800, chunk_overlap=300, "
        "< ~512 token notice, chunk length distribution histogram, first N chunk preview, and total chunk count."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearchSplitText"

    inputs = [
        MultilineInput(
            name="pipeline_guide",
            display_name="Pipeline Usage Guide (1 -> 2 -> 3 Initial Build + 4 Continuous Stream + Task)",
            value=CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            advanced=False,
        ),
        DataInput(
            name="upstream_parse_result",
            display_name="Upstream Step 1: Parse Document Result (Chaining Port)",
            info="Connect 'Parse Document Result' or 'Parsed Docs Table Reference' from Step 1 to auto-inherit Database, Schema, Stage, and PARSED_DOCS.",
        ),
        DataInput(
            name="parsed_table_ref",
            display_name="Parsed Docs Reference (Optional)",
            info="Optional connection from 'Cortex Search 1: Parse Document'.",
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
        StrInput(
            name="source_table",
            display_name="Source Parsed Table",
            value="PARSED_DOCS",
        ),
        StrInput(
            name="target_table",
            display_name="Target Chunks Table",
            value="DOC_CHUNKS",
        ),
        DropdownInput(
            name="splitter_mode",
            display_name="Splitter Engine",
            options=[
                "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER",
                "PYTHON_UDF_LANGCHAIN",
            ],
            value="SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER",
        ),
        DropdownInput(
            name="split_format",
            display_name="Split Format",
            options=["markdown", "none"],
            value="markdown",
        ),
        IntInput(
            name="chunk_size",
            display_name="Chunk Size (chars, target < ~512 tokens / ~375 words)",
            value=1800,
        ),
        IntInput(
            name="chunk_overlap",
            display_name="Chunk Overlap (chars)",
            value=300,
        ),
        StrInput(
            name="language",
            display_name="Document Language Tag",
            value="it",
        ),
        IntInput(
            name="preview_limit",
            display_name="Preview First N Chunks",
            value=5,
        ),
        DropdownInput(
            name="action_mode",
            display_name="Execution Mode",
            options=[
                "Dry-Run SQL Preview",
                "Create Table & Split Chunks",
                "Inspect Chunk Histogram & Preview",
            ],
            value="Dry-Run SQL Preview",
        ),
        StrInput(
            name="chunk_size_notice",
            display_name="Cortex Search Chunk Size Notice (<512 tokens)",
            value=CORTEX_SEARCH_CHUNK_SIZE_NOTICE,
            advanced=False,
        ),
    ]
    outputs = [
        Output(display_name="Split Text Result", name="split_result", method="run_split_text"),
        Output(display_name="Chunks Table Reference (To Step 3: Search Service)", name="chunks_table_ref", method="as_chunks_reference"),
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
        return build_config

    def _build_spec_and_builder(self) -> tuple[CortexSearchSplitTextSpec, CortexSearchSplitTextBuilder]:
        ref = getattr(self, "upstream_parse_result", None) or getattr(self, "parsed_table_ref", None)
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
        src_tbl = (ref_data.get("target_table") or getattr(self, "source_table", "") or "PARSED_DOCS").strip()
        tgt_tbl = (getattr(self, "target_table", "") or "DOC_CHUNKS").strip()
        splitter_mode = (
            getattr(self, "splitter_mode", "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER")
            or "SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER"
        ).strip()
        split_format = (getattr(self, "split_format", "markdown") or "markdown").strip().lower()
        chunk_size = int(getattr(self, "chunk_size", 1800) or 1800)
        chunk_overlap = int(getattr(self, "chunk_overlap", 300) or 300)
        language = (getattr(self, "language", "it") or "it").strip()
        preview_limit = int(getattr(self, "preview_limit", 5) or 5)

        spec = CortexSearchSplitTextSpec(
            database=db,
            schema=sch,
            source_table=src_tbl,
            target_table=tgt_tbl,
            splitter_mode=splitter_mode,
            split_format=split_format,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            language=language,
            preview_limit=preview_limit,
        )
        return spec, CortexSearchSplitTextBuilder(spec)

    def as_chunks_reference(self) -> Data:
        spec, _ = self._build_spec_and_builder()
        return Data(
            data={
                "database": spec.database,
                "schema_name": spec.schema,
                "source_table": spec.source_table,
                "source_table_fqn": spec.source_table_fqn,
                "target_table": spec.target_table,
                "target_table_fqn": spec.target_table_fqn,
                "splitter_mode": spec.splitter_mode,
                "split_format": spec.split_format,
                "chunk_size": spec.chunk_size,
                "chunk_overlap": spec.chunk_overlap,
                "language": spec.language,
            }
        )

    def run_split_text(self) -> Data:
        spec, builder = self._build_spec_and_builder()
        table_ddl = builder.build_target_table_ddl()
        udf_ddl = builder.build_langchain_python_udf_ddl() if spec.splitter_mode == "PYTHON_UDF_LANGCHAIN" else None
        insert_sql = builder.build_split_insert_sql()
        stats_sql = builder.build_preview_and_distribution_sql()
        action = (getattr(self, "action_mode", "Dry-Run SQL Preview") or "Dry-Run SQL Preview").strip()

        base_payload: dict[str, Any] = {
            "tab": "Cortex Search 2: Split Text",
            "database": spec.database,
            "schema_name": spec.schema,
            "source_table": spec.source_table,
            "source_table_fqn": spec.source_table_fqn,
            "target_table": spec.target_table,
            "target_table_fqn": spec.target_table_fqn,
            "splitter_mode": spec.splitter_mode,
            "split_format": spec.split_format,
            "chunk_size": spec.chunk_size,
            "chunk_overlap": spec.chunk_overlap,
            "chunk_size_notice": CORTEX_SEARCH_CHUNK_SIZE_NOTICE,
            "pipeline_guide": CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            "ddl_preview": table_ddl,
            "python_udf_ddl": udf_ddl,
            "split_insert_sql": insert_sql,
            "distribution_sql": stats_sql["distribution_sql"],
            "preview_sql": stats_sql["preview_sql"],
            "active_role": LIVE_BRIDGE.active_role,
        }

        if "Dry-Run" in action:
            return Data(data=base_payload)

        if "Create Table & Split Chunks" in action:
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("SPLIT_TEXT_INSERT", spec.target_table_fqn)
            if denied:
                raise ValueError(denied["message"])
            LIVE_BRIDGE.execute_sql(table_ddl)
            if udf_ddl:
                LIVE_BRIDGE.execute_sql(udf_ddl)
            LIVE_BRIDGE.execute_sql(insert_sql)

        dist_res = LIVE_BRIDGE.execute_sql(stats_sql["distribution_sql"])
        prev_res = LIVE_BRIDGE.execute_sql(stats_sql["preview_sql"])
        dist_rows = dist_res.get("rows", [])
        dist_summary = dist_rows[0] if dist_rows else {"total_chunks": 0, "min_chars": 0, "avg_chars": 0, "max_chars": 0}
        total_chunks = dist_summary.get("total_chunks") or dist_summary.get("TOTAL_CHUNKS") or len(prev_res.get("rows", []))

        base_payload.update(
            {
                "action": action,
                "length_distribution": dist_summary,
                "total_chunks": total_chunks,
                "preview_chunks": prev_res.get("rows", []),
                "ok": dist_res.get("ok", False) or prev_res.get("ok", False),
            }
        )
        return Data(data=base_payload)
