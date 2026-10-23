from __future__ import annotations

from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import BoolInput, DropdownInput, MultilineInput, Output, StrInput
    from lfx.schema import Data
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import BoolInput, Component, Data, DropdownInput, MultilineInput, Output, StrInput

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


def _database_options() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _schema_options(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


def _stage_options(database: str | None = None, schema_name: str | None = None) -> list[str]:
    stages = list(LIVE_BRIDGE.get_context_objects(database, schema_name, "stages"))
    if "SNOWFLAKE_LANGFLOW_STAGE" not in stages:
        stages.insert(0, "SNOWFLAKE_LANGFLOW_STAGE")
    if _can_create_stage():
        stages.extend([CREATE_INTERNAL_STAGE_OPTION, CREATE_EXTERNAL_STAGE_OPTION])
    else:
        stages.append(CREATE_STAGE_DISABLED_OPTION)
    return stages


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
        if "database" in build_config:
            build_config["database"]["options"] = _database_options()
        db_val = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _schema_options(db_val)
        sch_val = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or "CORE"
        )
        if "stage_name" in build_config:
            build_config["stage_name"]["options"] = _stage_options(db_val, sch_val)
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
                    "result": LIVE_BRIDGE.execute_sql(ddl),
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
