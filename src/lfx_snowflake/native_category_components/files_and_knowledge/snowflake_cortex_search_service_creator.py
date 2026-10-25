from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import DataInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import Data

from lfx_snowflake.components.files_and_stage import (
    CortexSearchServiceCreatorBuilder,
    CortexSearchServiceCreatorSpec,
)
from lfx_snowflake.components.knowledge_and_memories import DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG
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


class SnowflakeCortexSearchServiceCreatorComponent(Component):
    display_name = "Cortex Search 3: Search Service"
    description = (
        "Step 3 of the 4-node Cortex Search indexing pipeline (1: Parse Document -> 2: Split Text -> 3: Search Service): "
        "executes CREATE OR REPLACE CORTEX SEARCH SERVICE ON <search_column> ATTRIBUTES <attr_cols> WAREHOUSE = <WH> "
        "TARGET_LAG = '<lag>' EMBEDDING_MODEL = '<model>' AS (SELECT ... FROM <DB>.<SCHEMA>.DOC_CHUNKS), "
        "inspects live status (SHOW/DESCRIBE CORTEX SEARCH SERVICE), and supports Suspend / Resume."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearchServiceCreator"

    inputs = [
        MultilineInput(
            name="pipeline_guide",
            display_name="Pipeline Usage Guide (1 -> 2 -> 3 Initial Build + 4 Continuous Stream + Task)",
            value=CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            advanced=False,
        ),
        DataInput(
            name="upstream_chunks_result",
            display_name="Upstream Step 2: Split Text Result (Chaining Port)",
            info="Connect 'Split Text Result' or 'Chunks Table Reference' from Step 2 to auto-inherit Database, Schema, and DOC_CHUNKS.",
        ),
        DataInput(
            name="chunks_table_ref",
            display_name="Chunks Table Reference (Optional)",
            info="Optional connection from 'Cortex Search 2: Split Text'.",
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
            name="service_name",
            display_name="3. Cortex Search Service Name",
            value="DOCS_SEARCH_SRV",
        ),
        StrInput(
            name="source_table",
            display_name="Source Chunks Table",
            value="DOC_CHUNKS",
        ),
        StrInput(
            name="search_column",
            display_name="ON Search Column",
            value="chunk",
        ),
        StrInput(
            name="attribute_columns_csv",
            display_name="ATTRIBUTES Columns (CSV)",
            value="relative_path,page_index,language",
        ),
        StrInput(
            name="warehouse",
            display_name="Indexing Warehouse",
            value="COMPUTE_WH",
        ),
        StrInput(
            name="target_lag",
            display_name="TARGET_LAG",
            value="1 hour",
        ),
        DropdownInput(
            name="embedding_model",
            display_name="EMBEDDING_MODEL",
            options=list(DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG.keys()),
            value="snowflake-arctic-embed-m-v1.5",
        ),
        DropdownInput(
            name="action_mode",
            display_name="Service Operation",
            options=[
                "Dry-Run DDL Preview",
                "Create or Replace Cortex Search Service",
                "Inspect Status (SHOW / DESCRIBE)",
                "Suspend Indexing",
                "Resume Indexing",
                "Suspend Serving",
                "Resume Serving",
            ],
            value="Dry-Run DDL Preview",
        ),
    ]
    outputs = [
        Output(display_name="Search Service Result", name="service_result", method="run_search_service_creator"),
        Output(display_name="Search Service Reference (To Step 4: Stream + Task)", name="service_ref", method="as_service_reference"),
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
        return build_config

    def _build_spec_and_builder(
        self,
    ) -> tuple[CortexSearchServiceCreatorSpec, CortexSearchServiceCreatorBuilder]:
        ref = getattr(self, "upstream_chunks_result", None) or getattr(self, "chunks_table_ref", None)
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
        srv_name = (getattr(self, "service_name", "") or "DOCS_SEARCH_SRV").strip()
        src_tbl = (ref_data.get("target_table") or getattr(self, "source_table", "") or "DOC_CHUNKS").strip()
        search_col = (getattr(self, "search_column", "") or "chunk").strip()
        attrs_raw = getattr(self, "attribute_columns_csv", "relative_path,page_index,language") or ""
        attrs = tuple(a.strip() for a in attrs_raw.split(",") if a.strip())
        wh = (getattr(self, "warehouse", "") or getattr(LIVE_BRIDGE, "warehouse", "COMPUTE_WH") or "COMPUTE_WH").strip()
        lag = (getattr(self, "target_lag", "") or "1 hour").strip()
        emb = (getattr(self, "embedding_model", "") or "snowflake-arctic-embed-m-v1.5").strip()

        spec = CortexSearchServiceCreatorSpec(
            database=db,
            schema=sch,
            service_name=srv_name,
            source_table=src_tbl,
            search_column=search_col,
            attribute_columns=attrs or ("relative_path", "page_index", "language"),
            warehouse=wh,
            target_lag=lag,
            embedding_model=emb,
        )
        return spec, CortexSearchServiceCreatorBuilder(spec)

    def as_service_reference(self) -> Data:
        spec, builder = self._build_spec_and_builder()
        return Data(
            data={
                "database": spec.database,
                "schema_name": spec.schema,
                "service_name": spec.service_name,
                "service_fqn": spec.service_fqn,
                "source_table": spec.source_table,
                "source_table_fqn": spec.source_table_fqn,
                "search_column": spec.search_column,
                "attribute_columns": list(spec.attribute_columns),
                "warehouse": spec.warehouse,
                "target_lag": spec.target_lag,
                "embedding_model": spec.embedding_model,
                "create_service_ddl": builder.build_create_service_ddl(),
            }
        )

    def run_search_service_creator(self) -> Data:
        spec, builder = self._build_spec_and_builder()
        create_ddl = builder.build_create_service_ddl()
        status_sql = builder.build_status_sql()
        suspend_idx_sql = builder.build_suspend_resume_sql("SUSPEND", "INDEXING")
        resume_idx_sql = builder.build_suspend_resume_sql("RESUME", "INDEXING")
        suspend_srv_sql = builder.build_suspend_resume_sql("SUSPEND", "SERVING")
        resume_srv_sql = builder.build_suspend_resume_sql("RESUME", "SERVING")
        action = (getattr(self, "action_mode", "Dry-Run DDL Preview") or "Dry-Run DDL Preview").strip()

        role_privs = LIVE_BRIDGE.get_role_privileges()
        can_create_cs = bool(role_privs.get("can_create_cortex_search", False))
        warning_banner = (
            None
            if can_create_cs
            else (
                f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi "
                f"CREATE CORTEX SEARCH SERVICE in {spec.database}.{spec.schema}."
            )
        )

        payload: dict[str, Any] = {
            "tab": "Cortex Search 3: Search Service",
            "database": spec.database,
            "schema_name": spec.schema,
            "service_name": spec.service_name,
            "service_fqn": spec.service_fqn,
            "source_table": spec.source_table,
            "source_table_fqn": spec.source_table_fqn,
            "search_column": spec.search_column,
            "attribute_columns": list(spec.attribute_columns),
            "warehouse": spec.warehouse,
            "target_lag": spec.target_lag,
            "embedding_model": spec.embedding_model,
            "pipeline_guide": CORTEX_SEARCH_PIPELINE_GUIDE_STEP_1,
            "create_service_ddl": create_ddl,
            "show_sql": status_sql["show_sql"],
            "describe_sql": status_sql["describe_sql"],
            "suspend_indexing_sql": suspend_idx_sql,
            "resume_indexing_sql": resume_idx_sql,
            "suspend_serving_sql": suspend_srv_sql,
            "resume_serving_sql": resume_srv_sql,
            "can_create_cortex_search": can_create_cs,
            "warning_banner": warning_banner,
            "active_role": LIVE_BRIDGE.active_role,
        }

        if "Dry-Run" in action:
            return Data(data=payload)

        if "Inspect Status" in action:
            show_res = LIVE_BRIDGE.execute_sql(status_sql["show_sql"])
            desc_res = LIVE_BRIDGE.execute_sql(status_sql["describe_sql"])
            payload.update(
                {
                    "action": "INSPECT_STATUS",
                    "show_rows": show_res.get("rows", []),
                    "describe_rows": desc_res.get("rows", []),
                    "ok": show_res.get("ok", False) or desc_res.get("ok", False),
                }
            )
            return Data(data=payload)

        denied = LIVE_BRIDGE.check_write_privilege_or_deny("MANAGE_CORTEX_SEARCH_SERVICE", spec.service_fqn)
        if denied:
            raise ValueError(denied["message"])

        sql_map = {
            "Create or Replace Cortex Search Service": create_ddl,
            "Suspend Indexing": suspend_idx_sql,
            "Resume Indexing": resume_idx_sql,
            "Suspend Serving": suspend_srv_sql,
            "Resume Serving": resume_srv_sql,
        }
        exec_sql = sql_map.get(action, create_ddl)
        res = LIVE_BRIDGE.execute_sql(exec_sql)
        if action == "Create or Replace Cortex Search Service" and _ddl_succeeded(res):
            _register_created_object(
                database=spec.database,
                schema=spec.schema,
                category="cortex_search_services",
                object_name=spec.service_name,
                flow_id=_resolve_flow_id(self),
            )
        payload.update(
            {
                "action": action,
                "executed_sql": exec_sql,
                "result": res,
                "ok": res.get("ok", False),
            }
        )
        return Data(data=payload)
