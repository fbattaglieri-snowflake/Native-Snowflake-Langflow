from __future__ import annotations

from typing import Any

from lfx.custom import Component
from lfx.io import DataInput, DropdownInput, Output, StrInput
from lfx.schema import Data
from lfx_snowflake.components.files_and_stage import (
    CortexSearchServiceCreatorBuilder,
    CortexSearchServiceCreatorSpec,
)
from lfx_snowflake.components.knowledge_and_memories import DEFAULT_SNOWFLAKE_EMBEDDING_CATALOG
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


class SnowflakeCortexSearchServiceCreatorComponent(Component):
    display_name = "Cortex Search 3: Search Service"
    description = (
        "Tab 3 of the Cortex Search indexing pipeline: executes CREATE OR REPLACE CORTEX SEARCH SERVICE "
        "ON <search_column> ATTRIBUTES <attr_cols> WAREHOUSE = <WH> TARGET_LAG = '<lag>' "
        "EMBEDDING_MODEL = '<model>' AS (SELECT ... FROM <DB>.<SCHEMA>.DOC_CHUNKS), "
        "inspects live status (SHOW/DESCRIBE CORTEX SEARCH SERVICE), and supports Suspend / Resume."
    )
    icon = "Snowflake"
    name = "SnowflakeCortexSearchServiceCreator"

    inputs = [
        DataInput(
            name="chunks_table_ref",
            display_name="Chunks Table Reference (Optional)",
            info="Optional connection from 'Cortex Search 2: Split Text'.",
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
        Output(display_name="Search Service Reference", name="service_ref", method="as_service_reference"),
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

    def _build_spec_and_builder(
        self,
    ) -> tuple[CortexSearchServiceCreatorSpec, CortexSearchServiceCreatorBuilder]:
        ref = getattr(self, "chunks_table_ref", None)
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
            "source_table_fqn": spec.source_table_fqn,
            "search_column": spec.search_column,
            "attribute_columns": list(spec.attribute_columns),
            "warehouse": spec.warehouse,
            "target_lag": spec.target_lag,
            "embedding_model": spec.embedding_model,
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
        payload.update(
            {
                "action": action,
                "executed_sql": exec_sql,
                "result": res,
                "ok": res.get("ok", False),
            }
        )
        return Data(data=payload)
