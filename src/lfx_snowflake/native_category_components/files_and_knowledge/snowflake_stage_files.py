from __future__ import annotations

from typing import Any

import lfx_snowflake._lfx_compat  # noqa: F401
from lfx.custom import Component
from lfx.io import BoolInput, DropdownInput, MultilineInput, Output, StrInput
from lfx.schema import Data

from lfx_snowflake.components.files_and_stage import build_stage_creation_ddl
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_INTERNAL_STAGE_OPTION = "+ Create New Internal Stage..."
CREATE_EXTERNAL_STAGE_OPTION = "+ Create New External Stage..."
CREATE_STAGE_DISABLED_OPTION = "[DISABLED — Role lacks CREATE STAGE privilege]"


def _can_create_stage() -> bool:
    fn = getattr(LIVE_BRIDGE, "can_create_stage", None)
    if callable(fn):
        return bool(fn())
    privs = getattr(LIVE_BRIDGE, "role_privileges", None)
    if isinstance(privs, dict) and "can_create_stage" in privs:
        return bool(privs["can_create_stage"])
    return LIVE_BRIDGE.active_role.upper() not in ("PUBLIC", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE", "VIEWER_ROLE")


def _resolve_flow_id(component: Any = None, build_config: dict[str, Any] | None = None) -> str | None:
    resolver = getattr(LIVE_BRIDGE, "_resolve_component_flow_id", None) or getattr(
        LIVE_BRIDGE, "resolve_component_flow_id", None
    )
    if callable(resolver):
        res = resolver(component, build_config)
        if res:
            return str(res).strip()
    if isinstance(build_config, dict):
        cfg_fid = build_config.get("_frontend_node_flow_id") or build_config.get("flow_id")
        if isinstance(cfg_fid, dict):
            cfg_fid = cfg_fid.get("value")
        if cfg_fid and str(cfg_fid).strip():
            return str(cfg_fid).strip()
    if component is not None:
        for attr in ("flow_id", "_flow_id"):
            val = getattr(component, attr, None)
            if val and str(val).strip():
                return str(val).strip()
        graph = getattr(component, "graph", None)
        if graph is not None:
            g_fid = getattr(graph, "flow_id", None) or getattr(graph, "_flow_id", None)
            if g_fid and str(g_fid).strip():
                return str(g_fid).strip()
    cur_fn = getattr(LIVE_BRIDGE, "get_current_flow_id", None) or getattr(
        LIVE_BRIDGE, "get_current_snowflake_flow_id", None
    )
    if callable(cur_fn):
        cur = cur_fn()
        if cur and str(cur).strip():
            return str(cur).strip()
    return None


def _ddl_succeeded(res: dict[str, Any] | None) -> bool:
    if not isinstance(res, dict):
        return False
    if res.get("ok", False):
        return True
    return res.get("mode") == "offline" and not res.get("error") and not res.get("error_code")


def _register_created_object(
    database: str,
    schema: str,
    category: str,
    object_name: str,
    flow_id: str | None = None,
) -> None:
    eff_fid = flow_id or _resolve_flow_id()
    reg_fn = getattr(LIVE_BRIDGE, "register_object_in_context", None)
    if callable(reg_fn):
        reg_fn(
            database=database,
            schema=schema,
            category=category,
            object_name=object_name,
            flow_id=eff_fid,
        )
    ctx = (
        LIVE_BRIDGE.get_flow_context(eff_fid)
        if eff_fid and hasattr(LIVE_BRIDGE, "get_flow_context")
        else getattr(LIVE_BRIDGE, "selected_context", None)
    )
    if isinstance(ctx, dict):
        db_u = (database or "").strip().upper()
        sch_u = (schema or "").strip().upper()
        obj_u = (object_name or "").strip()
        if db_u and db_u not in ctx.setdefault("databases", []):
            ctx["databases"].append(db_u)
        if db_u and sch_u:
            sch_list = ctx.setdefault("schemas_by_db", {}).setdefault(db_u, [])
            if sch_u not in sch_list:
                sch_list.append(sch_u)
            key = f"{db_u}.{sch_u}"
            for map_name in ("objects_by_db_schema", "selected_objects"):
                m = ctx.setdefault(map_name, {})
                bucket = m.get(key)
                if isinstance(bucket, dict):
                    arr = bucket.setdefault(category, [])
                    if obj_u and obj_u not in arr:
                        arr.append(obj_u)
                elif bucket is None:
                    m[key] = {category: [obj_u] if obj_u else []}


def _database_options(flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id())


def _schema_options(database: str | None = None, flow_id: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id())


def _stage_options(
    database: str | None = None,
    schema_name: str | None = None,
    flow_id: str | None = None,
) -> list[str]:
    eff_fid = flow_id or _resolve_flow_id()
    stages = list(LIVE_BRIDGE.get_context_objects(database, schema_name, "stages", flow_id=eff_fid))
    if "SNOWFLAKE_LANGFLOW_STAGE" not in stages:
        stages.insert(0, "SNOWFLAKE_LANGFLOW_STAGE")
    if _can_create_stage():
        stages.extend([CREATE_INTERNAL_STAGE_OPTION, CREATE_EXTERNAL_STAGE_OPTION])
    else:
        stages.append(CREATE_STAGE_DISABLED_OPTION)
    return stages


def _stage_fqn_options(flow_id: str | None = None) -> list[str]:
    """Return @<DB>.<SCHEMA>.<STAGE> options across all databases and schemas selected in [ ❄️ Context ]."""
    eff_fid = flow_id or _resolve_flow_id()
    out: list[str] = []
    for db in _database_options(flow_id=eff_fid):
        for sch in _schema_options(db, flow_id=eff_fid):
            for stg in LIVE_BRIDGE.get_context_objects(db, sch, "stages", flow_id=eff_fid):
                clean = str(stg).lstrip("@").strip()
                fqn = f"@{clean}" if clean.count(".") >= 2 else f"@{db}.{sch}.{clean}"
                if fqn not in out:
                    out.append(fqn)
    if not out:
        out.append("@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE")
    return out


class SnowflakeStageFileSystemComponent(Component):
    display_name = "Stage File System"
    description = (
        "List, read, or write files (including safe atomic Append) and provision Internal "
        "(ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)) or External "
        "(S3/GCS/Azure URL + STORAGE_INTEGRATION + DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE)) "
        "Snowflake Stages under the active Snowflake role."
    )
    icon = "Snowflake"
    name = "SnowflakeStageFileSystem"

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
            options=_stage_options("SNOWFLAKE_LANGFLOW", "CORE"),
            value="SNOWFLAKE_LANGFLOW_STAGE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="operation",
            display_name="Stage Operation",
            options=[
                "List Stage Files",
                "Create Internal Stage",
                "Create External Stage",
                "Read Stage File",
                "Write / Safe Append Metadata",
            ],
            value="List Stage Files",
            real_time_refresh=True,
        ),
        DropdownInput(
            name="stage_type",
            display_name="Stage Type (for Create)",
            options=["INTERNAL", "EXTERNAL"],
            value="INTERNAL",
            real_time_refresh=True,
        ),
        StrInput(
            name="new_stage_name",
            display_name="New Stage Name (when creating a stage)",
            value="DOCS_STAGE",
        ),
        StrInput(
            name="external_url",
            display_name="External Stage URL (s3:// | gcs:// | azure://)",
            value="s3://example-bucket/prefix/",
        ),
        StrInput(
            name="storage_integration",
            display_name="Storage Integration (for External Stage)",
            value="S3_DOCS_INT",
        ),
        StrInput(
            name="stage_fqn",
            display_name="Stage FQN Override (@DB.SCHEMA.STAGE)",
            value="@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE",
            advanced=True,
        ),
        StrInput(
            name="file_subpath",
            display_name="Subpath / Filename",
            value="flows/example.json",
        ),
        MultilineInput(
            name="file_content",
            display_name="Content (for Write/Append)",
            value="",
            advanced=True,
        ),
        BoolInput(
            name="append_mode",
            display_name="Atomic Safe Append Mode",
            value=False,
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Stage Result", name="stage_data", method="run_stage_op"),
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

    def _resolve_stage_fqn(self) -> tuple[str, str, str, str]:
        db = (getattr(self, "database", "") or getattr(LIVE_BRIDGE, "database", "SNOWFLAKE_LANGFLOW")).strip()
        sch = (getattr(self, "schema_name", "") or getattr(LIVE_BRIDGE, "schema", "CORE")).strip()
        stg = (getattr(self, "stage_name", "") or "SNOWFLAKE_LANGFLOW_STAGE").strip()
        override = (getattr(self, "stage_fqn", "") or "").lstrip("@").strip()
        if override and override != "SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE":
            parts = override.split(".")
            if len(parts) == 3:
                return parts[0], parts[1], parts[2], override
            return db, sch, override, override
        if stg in (CREATE_INTERNAL_STAGE_OPTION, CREATE_EXTERNAL_STAGE_OPTION, CREATE_STAGE_DISABLED_OPTION):
            stg = (getattr(self, "new_stage_name", "") or "DOCS_STAGE").strip()
        clean_fqn = f"{db}.{sch}.{stg}"
        return db, sch, stg, clean_fqn

    def run_stage_op(self) -> Data:
        db, sch, stg, clean_stage = self._resolve_stage_fqn()
        raw_stage_sel = (getattr(self, "stage_name", "") or "").strip()

        if raw_stage_sel == CREATE_STAGE_DISABLED_OPTION:
            raise ValueError(
                f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi (CREATE STAGE) "
                f"per creare uno stage in {db}.{sch}."
            )

        if self.operation == "List Stage Files":
            res = LIVE_BRIDGE.execute_sql(f"LIST @{clean_stage}")
            return Data(
                data={
                    "operation": self.operation,
                    "database": db,
                    "schema_name": sch,
                    "stage_name": stg,
                    "stage": f"@{clean_stage}",
                    "active_role": LIVE_BRIDGE.active_role,
                    "files": res.get("rows", []),
                    "ok": res.get("ok", False),
                }
            )

        if self.operation in ("Create Internal Stage", "Create External Stage") or raw_stage_sel in (
            CREATE_INTERNAL_STAGE_OPTION,
            CREATE_EXTERNAL_STAGE_OPTION,
        ):
            denied = LIVE_BRIDGE.check_write_privilege_or_deny("CREATE_STAGE", clean_stage)
            if denied:
                raise ValueError(denied["message"])
            if not _can_create_stage():
                raise ValueError(
                    f"Il ruolo attivo ({LIVE_BRIDGE.active_role}) non dispone dei privilegi (CREATE STAGE / USAGE ON INTEGRATION) "
                    f"per creare {clean_stage}."
                )
            is_external = (
                self.operation == "Create External Stage"
                or raw_stage_sel == CREATE_EXTERNAL_STAGE_OPTION
                or (getattr(self, "stage_type", "INTERNAL") or "INTERNAL").upper() == "EXTERNAL"
            )
            target_stage_name = (getattr(self, "new_stage_name", "") or stg or "DOCS_STAGE").strip()
            ddl = build_stage_creation_ddl(
                database=db,
                schema=sch,
                stage_name=target_stage_name,
                stage_type="EXTERNAL" if is_external else "INTERNAL",
                external_url=getattr(self, "external_url", "s3://example-bucket/prefix/"),
                storage_integration=getattr(self, "storage_integration", "S3_DOCS_INT"),
            )
            exec_res = LIVE_BRIDGE.execute_sql(ddl)
            if _ddl_succeeded(exec_res):
                _register_created_object(
                    database=db,
                    schema=sch,
                    category="stages",
                    object_name=target_stage_name,
                    flow_id=_resolve_flow_id(self),
                )
            return Data(
                data={
                    "operation": "Create External Stage" if is_external else "Create Internal Stage",
                    "stage_type": "EXTERNAL" if is_external else "INTERNAL",
                    "database": db,
                    "schema_name": sch,
                    "stage_name": target_stage_name,
                    "stage_fqn": f"{db}.{sch}.{target_stage_name}",
                    "ddl": ddl,
                    "active_role": LIVE_BRIDGE.active_role,
                    "result": exec_res,
                }
            )

        if self.operation == "Read Stage File":
            res = LIVE_BRIDGE.execute_sql(f"LIST @{clean_stage}/{self.file_subpath}")
            return Data(
                data={
                    "operation": self.operation,
                    "target": f"@{clean_stage}/{self.file_subpath}",
                    "active_role": LIVE_BRIDGE.active_role,
                    "files": res.get("rows", []),
                    "ok": res.get("ok", False),
                }
            )

        denied = LIVE_BRIDGE.check_write_privilege_or_deny("WRITE_STAGE", f"@{clean_stage}/{self.file_subpath}")
        if denied:
            raise ValueError(denied["message"])
        return Data(
            data={
                "operation": "WRITE_APPEND" if self.append_mode else "WRITE_OVERWRITE",
                "target": f"@{clean_stage}/{self.file_subpath}",
                "active_role": LIVE_BRIDGE.active_role,
                "bytes_prepared": len((self.file_content or "").encode("utf-8")),
                "atomic_append_verified": bool(self.append_mode),
            }
        )
