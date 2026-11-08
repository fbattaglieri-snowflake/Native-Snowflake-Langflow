from __future__ import annotations

from typing import Any

from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


CREATE_NEW_DB_OPTION = "+ Create New Database..."
CREATE_NEW_DB_DISABLED_OPTION = "[DISABLED — Role lacks CREATE DATABASE privilege]"
CREATE_NEW_SCHEMA_OPTION = "+ Create New Schema..."
CREATE_NEW_SCHEMA_DISABLED_OPTION = "[DISABLED — Role lacks CREATE SCHEMA privilege]"

INFRASTRUCTURE_OBJECT_FAMILIES: tuple[str, ...] = ("table", "view", "stage", "postgres")


def _can_create_database(context: Any = None) -> bool:
    if isinstance(context, dict):
        privs_ctx = context.get("role_privileges")
        if isinstance(privs_ctx, dict) and "can_create_database" in privs_ctx:
            return bool(privs_ctx["can_create_database"])
        role_ctx = str(context.get("active_role") or context.get("role") or "").strip().upper()
        if role_ctx:
            return role_ctx in ("ACCOUNTADMIN", "SYSADMIN", "SNOWFLAKE_LANGFLOW_ADMIN_ROLE")
    fn = getattr(LIVE_BRIDGE, "can_create_database", None)
    if callable(fn):
        return bool(fn())
    privs = getattr(LIVE_BRIDGE, "role_privileges", None)
    if isinstance(privs, dict) and "can_create_database" in privs:
        return bool(privs["can_create_database"])
    return LIVE_BRIDGE.active_role.upper() in ("ACCOUNTADMIN", "SYSADMIN", "SNOWFLAKE_LANGFLOW_ADMIN_ROLE")


def _can_create_schema(context: Any = None) -> bool:
    if isinstance(context, dict):
        privs_ctx = context.get("role_privileges")
        if isinstance(privs_ctx, dict) and "can_create_schema" in privs_ctx:
            return bool(privs_ctx["can_create_schema"])
        role_ctx = str(context.get("active_role") or context.get("role") or "").strip().upper()
        if role_ctx:
            return role_ctx not in ("PUBLIC", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE", "VIEWER_ROLE")
    fn = getattr(LIVE_BRIDGE, "can_create_schema", None)
    if callable(fn):
        return bool(fn())
    privs = getattr(LIVE_BRIDGE, "role_privileges", None)
    if isinstance(privs, dict) and "can_create_schema" in privs:
        return bool(privs["can_create_schema"])
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
        if category == "postgres_instances":
            pg_list = ctx.setdefault("postgres_instances", [])
            if obj_u and obj_u not in pg_list:
                pg_list.append(obj_u)
        if db_u and db_u not in ctx.setdefault("databases", []):
            ctx["databases"].append(db_u)
        if db_u and sch_u:
            sch_list = ctx.setdefault("schemas_by_db", {}).setdefault(db_u, [])
            if sch_u not in sch_list:
                sch_list.append(sch_u)
            if category != "postgres_instances":
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
    dbs = list(LIVE_BRIDGE.get_context_databases(flow_id=flow_id or _resolve_flow_id()))
    dbs.append(CREATE_NEW_DB_OPTION if _can_create_database() else CREATE_NEW_DB_DISABLED_OPTION)
    return dbs


def _schema_options(database: str | None = None, flow_id: str | None = None) -> list[str]:
    schemas = list(LIVE_BRIDGE.get_context_schemas(database, flow_id=flow_id or _resolve_flow_id()))
    schemas.append(CREATE_NEW_SCHEMA_OPTION if _can_create_schema() else CREATE_NEW_SCHEMA_DISABLED_OPTION)
    return schemas


__all__ = [
    "CREATE_NEW_DB_DISABLED_OPTION",
    "CREATE_NEW_DB_OPTION",
    "CREATE_NEW_SCHEMA_DISABLED_OPTION",
    "CREATE_NEW_SCHEMA_OPTION",
    "INFRASTRUCTURE_OBJECT_FAMILIES",
    "_can_create_database",
    "_can_create_schema",
    "_database_options",
    "_ddl_succeeded",
    "_register_created_object",
    "_resolve_flow_id",
    "_schema_options",
]
