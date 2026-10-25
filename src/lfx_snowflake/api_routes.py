"""Backend API Routes and Service Dispatcher for SnowflakeLangflow UI, Settings, Flow Import, Memories, and A2A."""

from __future__ import annotations

import copy
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .bundle_manifest import (
    ERROR_TAXONOMY_CONTRACT,
    INVARIANT_CATEGORIES_MANIFEST,
    NEW_FLOW_IMPORT_MODES,
    SETTINGS_CONTRACT,
    SNOWFLAKE_BRANDING_CONTRACT,
    get_bundle_manifest,
    verify_invariant_category_integrity,
)
from .fastapi_live_bridge import (
    AGENT_MEMORY_TYPES,
    CANONICAL_MEMORY_TYPES_BY_ID,
    EMBEDDING_PROVIDERS_CATALOG,
    EXTERNAL_EMBEDDING_PROVIDERS,
    LEGACY_MEMORY_TYPE_ALIASES,
    get_current_snowflake_flow_id,
    resolve_canonical_memory_type,
    resolve_memory_embedding_dimension,
)

STAGE_URI_REGEX = re.compile(
    r"^@(?P<database>(?:[A-Za-z_][A-Za-z0-9_$]*|<[A-Za-z_][A-Za-z0-9_]*>))\."
    r"(?P<schema>(?:[A-Za-z_][A-Za-z0-9_$]*|<[A-Za-z_][A-Za-z0-9_]*>))\."
    r"(?P<stage>(?:[A-Za-z_][A-Za-z0-9_$]*|<[A-Za-z_][A-Za-z0-9_]*>))"
    r"(?:/(?P<subpath>[A-Za-z0-9_./-]+))?$"
)

SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"(?i)(pat|token|secret|password|api_key|apikey|private_key)\s*[:=]\s*['\"]?[^\s,'\"}]+"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
)


def redact_secrets_in_obj(value: Any) -> Any:
    """Recursively redact secrets, tokens, private keys, and credentials from strings, dicts, and lists."""
    if isinstance(value, str):
        redacted = value
        for pattern in SECRET_PATTERNS:
            redacted = pattern.sub("[REDACTED_SECRET]", redacted)
        return redacted
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for k, v in value.items():
            key_lower = str(k).lower()
            if any(
                sensitive in key_lower
                for sensitive in (
                    "password",
                    "secret",
                    "token",
                    "api_key",
                    "apikey",
                    "private_key",
                    "authorization",
                    "access_roles",
                )
            ):
                result[k] = "[REDACTED_SECRET]"
            else:
                result[k] = redact_secrets_in_obj(v)
        return result
    if isinstance(value, list):
        return [redact_secrets_in_obj(item) for item in value]
    return value


@dataclass
class SnowflakeAPIResponse:
    """Standardized HTTP response envelope for SnowflakeLangflow routes."""

    status_code: int
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)


class SnowflakeAPIRouter:
    """Account-agnostic API router enforcing active role governance, explicit 4-code denials, and audit logging."""

    def __init__(
        self,
        authenticated_user: str = "<SNOWFLAKE_USER>",
        available_roles: list[str] | None = None,
        initial_role: str | None = None,
        role_permissions: dict[str, set[str]] | None = None,
        execution_mode: str = "INTERACTIVE_CALLER",
    ) -> None:
        self.authenticated_user = authenticated_user
        self.available_roles = list(available_roles or ["ANALYST_ROLE", "DATA_ENGINEER_ROLE", "PUBLIC"])
        self.active_role = initial_role or self.available_roles[0]
        if self.active_role not in self.available_roles:
            self.available_roles.insert(0, self.active_role)
        self.execution_mode = execution_mode
        self.context_epoch: int = 1
        self.secondary_roles_mode: str = "NONE"

        default_perms: dict[str, set[str]] = {
            "ANALYST_ROLE": {
                "UI_BOOTSTRAP",
                "IMPORT_FLOW_UPLOAD",
                "IMPORT_FLOW_STAGE:@<DATABASE>.<SCHEMA>.<STAGE_NAME>",
                "IMPORT_FLOW_GIT",
                "SETTINGS_READ",
                "SETTINGS_WRITE",
                "MESSAGES_READ",
                "MESSAGES_WRITE",
                "GLOBAL_VARIABLES_WRITE",
                "MEMORY_USE_EXISTING",
                "A2A_SAVE_METADATA",
                "A2A_PUBLISH",
                "A2A_INVOKE",
            },
            "DATA_ENGINEER_ROLE": {
                "UI_BOOTSTRAP",
                "IMPORT_FLOW_UPLOAD",
                "IMPORT_FLOW_STAGE:@<DATABASE>.<SCHEMA>.<STAGE_NAME>",
                "IMPORT_FLOW_GIT",
                "SETTINGS_READ",
                "SETTINGS_WRITE",
                "MESSAGES_READ",
                "MESSAGES_WRITE",
                "GLOBAL_VARIABLES_WRITE",
                "MEMORY_USE_EXISTING",
                "MEMORY_CREATE_PHYSICAL",
                "A2A_SAVE_METADATA",
                "A2A_PUBLISH",
                "A2A_INVOKE",
            },
            "PUBLIC": {
                "UI_BOOTSTRAP",
                "SETTINGS_READ",
                "MESSAGES_READ",
            },
        }
        self.role_permissions: dict[str, set[str]] = (
            copy.deepcopy(role_permissions) if role_permissions is not None else default_perms
        )

        # State stores invalidated or scoped by context_epoch / role
        self.role_scoped_cache: dict[str, Any] = {}
        self.role_accessible_catalog: dict[str, dict[str, Any]] = {}
        self.audit_outbox: list[dict[str, Any]] = []
        self.activity_outbox: list[dict[str, Any]] = []
        self.soft_deleted_activity_ids: set[str] = set()
        self.imported_flows: dict[str, dict[str, Any]] = {}
        self.memories_registry: dict[str, dict[str, Any]] = {}
        self.a2a_published_flows: dict[str, dict[str, Any]] = {}
        self.messages_store: dict[str, dict[str, Any]] = {}
        self.contexts_by_flow_id: dict[str, dict[str, Any]] = {}
        self.active_flow_id: str | None = None
        self.selected_context: dict[str, Any] = self._default_context_template()
        self.global_variables: dict[str, dict[str, Any]] = {
            "SNOWFLAKE_ACTIVE_ROLE": {
                "name": "SNOWFLAKE_ACTIVE_ROLE",
                "value": self.active_role,
                "read_only": True,
                "source": "backend_session_context",
            },
            "SNOWFLAKE_DEFAULT_DATABASE": {
                "name": "SNOWFLAKE_DEFAULT_DATABASE",
                "value": "<DATABASE>",
                "read_only": False,
                "source": "project_settings",
            },
            "SNOWFLAKE_DEFAULT_SCHEMA": {
                "name": "SNOWFLAKE_DEFAULT_SCHEMA",
                "value": "<SCHEMA>",
                "read_only": False,
                "source": "project_settings",
            },
            "SNOWFLAKE_DEFAULT_WAREHOUSE": {
                "name": "SNOWFLAKE_DEFAULT_WAREHOUSE",
                "value": "<WAREHOUSE>",
                "read_only": False,
                "source": "project_settings",
            },
        }

    @staticmethod
    def _default_context_template() -> dict[str, Any]:
        return {
            "database": "<DATABASE>",
            "schema": "<SCHEMA>",
            "databases": ["<DATABASE>"],
            "schemas_by_db": {"<DATABASE>": ["<SCHEMA>", "PUBLIC"]},
            "full_databases": [],
            "full_schemas": [],
            "postgres_instances": [],
            "selected_postgres_instances": [],
            "postgres_tables_by_instance": {},
            "objects_by_db_schema": {},
            "selected_objects": {},
            "visible_objects": {},
        }

    def compute_role_effective_context(
        self,
        flow_id: str | None = None,
        role: str | None = None,
    ) -> dict[str, Any]:
        """Compute non-destructive intersection `Effective Context = Stored Flow Context ∩ Active Role Accessible Catalog` (`CTX-ARCH-07` #1)."""
        clean_fid = str(flow_id or get_current_snowflake_flow_id() or "").strip() or None
        stored_ctx = copy.deepcopy(
            self.contexts_by_flow_id.get(clean_fid, self._default_context_template())
            if clean_fid
            else self.selected_context
        )
        target_role = (role or self.active_role or "PUBLIC").strip().upper()
        role_cat = self.role_accessible_catalog.get(target_role)
        is_restricted_public = target_role in ("PUBLIC", "VIEWER_ROLE", "ROLE_VIEWER", "SNOWFLAKE_LANGFLOW_VIEWER_ROLE")

        allowed_dbs: set[str] | None = None
        allowed_schemas_by_db: dict[str, set[str]] = {}
        allowed_objs_by_pair: dict[str, dict[str, set[str]]] = {}
        allowed_pg: set[str] | None = None

        if isinstance(role_cat, dict):
            if "databases" in role_cat and isinstance(role_cat["databases"], (list, set, tuple)):
                allowed_dbs = {str(d).strip().upper() for d in role_cat["databases"] if str(d).strip()}
            if isinstance(role_cat.get("schemas_by_db"), dict):
                for k, v in role_cat["schemas_by_db"].items():
                    if isinstance(v, (list, set, tuple)):
                        allowed_schemas_by_db[str(k).strip().upper()] = {str(s).strip().upper() for s in v if str(s).strip()}
            if isinstance(role_cat.get("objects_by_db_schema"), dict):
                for pk, pv in role_cat["objects_by_db_schema"].items():
                    if isinstance(pv, dict):
                        allowed_objs_by_pair[str(pk).strip().upper()] = {
                            str(ck).strip().lower(): {str(it).strip().upper() for it in cv if str(it).strip()}
                            for ck, cv in pv.items()
                            if isinstance(cv, (list, set, tuple))
                        }
            if "postgres_instances" in role_cat and isinstance(role_cat["postgres_instances"], (list, set, tuple)):
                allowed_pg = {str(p).strip().upper() for p in role_cat["postgres_instances"] if str(p).strip()}

        inaccessible: list[dict[str, Any]] = []
        retained_count = 0
        effective_ctx = copy.deepcopy(stored_ctx)

        stored_dbs = [str(d).strip().upper() for d in (stored_ctx.get("databases") or []) if str(d).strip()]
        if allowed_dbs is not None:
            eff_dbs = []
            for d in stored_dbs:
                if d in allowed_dbs:
                    eff_dbs.append(d)
                else:
                    inaccessible.append(
                        {
                            "fqn": d,
                            "category": "database",
                            "role": target_role,
                            "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                        }
                    )
            effective_ctx["databases"] = eff_dbs

        eff_schemas_map: dict[str, list[str]] = {}
        for db_k, sch_list in (stored_ctx.get("schemas_by_db") or {}).items():
            db_u = str(db_k).strip().upper()
            if allowed_dbs is not None and db_u not in allowed_dbs:
                continue
            if not isinstance(sch_list, list):
                continue
            kept_sch: list[str] = []
            for s in sch_list:
                s_u = str(s).strip().upper()
                sch_allowed = True
                if db_u in allowed_schemas_by_db and s_u not in allowed_schemas_by_db[db_u]:
                    sch_allowed = False
                elif is_restricted_public and s_u != "PUBLIC" and not role_cat:
                    sch_allowed = False
                if sch_allowed:
                    kept_sch.append(s_u)
                else:
                    inaccessible.append(
                        {
                            "fqn": f"{db_u}.{s_u}",
                            "category": "schema",
                            "role": target_role,
                            "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                        }
                    )
            eff_schemas_map[db_u] = kept_sch
        effective_ctx["schemas_by_db"] = eff_schemas_map

        for map_key in ("objects_by_db_schema", "selected_objects"):
            src_map = stored_ctx.get(map_key)
            if not isinstance(src_map, dict):
                continue
            new_map: dict[str, Any] = {}
            for pair_k, pair_v in src_map.items():
                pair_u = str(pair_k).strip().upper()
                parts = pair_u.split(".", 1)
                db_u = parts[0] if parts else ""
                sch_u = parts[1] if len(parts) > 1 else ""
                pair_allowed = True
                if allowed_dbs is not None and db_u not in allowed_dbs:
                    pair_allowed = False
                elif db_u in allowed_schemas_by_db and sch_u not in allowed_schemas_by_db[db_u]:
                    pair_allowed = False
                elif is_restricted_public and sch_u != "PUBLIC" and not role_cat:
                    pair_allowed = False
                if not isinstance(pair_v, dict):
                    if pair_allowed:
                        new_map[pair_u] = copy.deepcopy(pair_v)
                    continue
                new_bucket: dict[str, list[str]] = {}
                for cat_k, items in pair_v.items():
                    cat_l = str(cat_k).strip().lower()
                    if not isinstance(items, list):
                        continue
                    kept_items: list[str] = []
                    for item in items:
                        item_str = str(item).strip()
                        item_u = item_str.upper()
                        obj_allowed = pair_allowed
                        if obj_allowed and pair_u in allowed_objs_by_pair and cat_l in allowed_objs_by_pair[pair_u]:
                            obj_allowed = item_u in allowed_objs_by_pair[pair_u][cat_l]
                        if obj_allowed:
                            kept_items.append(item_str)
                            if map_key == "objects_by_db_schema":
                                retained_count += 1
                        else:
                            if map_key == "objects_by_db_schema":
                                inaccessible.append(
                                    {
                                        "fqn": f"{pair_u}.{item_str}",
                                        "database": db_u,
                                        "schema": sch_u,
                                        "name": item_str,
                                        "category": cat_l,
                                        "role": target_role,
                                        "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                                    }
                                )
                    new_bucket[cat_l] = kept_items
                new_map[pair_u] = new_bucket
            effective_ctx[map_key] = new_map

        stored_pg = [str(p).strip() for p in (stored_ctx.get("postgres_instances") or []) if str(p).strip()]
        if allowed_pg is not None:
            kept_pg = []
            for p in stored_pg:
                if p.upper() in allowed_pg:
                    kept_pg.append(p)
                else:
                    inaccessible.append(
                        {
                            "fqn": p,
                            "name": p,
                            "category": "postgres_instances",
                            "role": target_role,
                            "locked_badge": f"🔒 Non accessibile col ruolo {target_role}",
                        }
                    )
            effective_ctx["postgres_instances"] = kept_pg
            effective_ctx["selected_postgres_instances"] = list(kept_pg)

        return {
            "stored_context": stored_ctx,
            "effective_context": effective_ctx,
            "inaccessible_objects": inaccessible,
            "retained_objects_count": retained_count,
            "hidden_by_role_count": len(inaccessible),
        }

    def register_object_in_context(
        self,
        database: str | None = None,
        schema: str | None = None,
        category: str = "tables",
        object_name: str = "",
        flow_id: str | None = None,
    ) -> dict[str, Any]:
        """Auto-register a provisioned Snowflake object in the flow's isolated context (`CTX-ARCH-07` #2)."""
        clean_fid = str(flow_id or get_current_snowflake_flow_id() or "").strip() or None
        target_ctx = (
            self.contexts_by_flow_id.setdefault(clean_fid, self._default_context_template())
            if clean_fid
            else self.selected_context
        )
        db = str(database or target_ctx.get("database") or "<DATABASE>").strip().upper()
        sch = str(schema or target_ctx.get("schema") or "<SCHEMA>").strip().upper()
        cat = str(category or "tables").strip().lower()
        clean_name = str(object_name or "").strip()
        if not clean_name:
            return target_ctx
        if cat in ("postgres", "postgres_instance", "postgres_instances"):
            for pg_k in ("postgres_instances", "selected_postgres_instances"):
                pg_list = target_ctx.setdefault(pg_k, [])
                if clean_name not in pg_list:
                    pg_list.append(clean_name)
        else:
            dbs = target_ctx.setdefault("databases", [])
            if db not in dbs:
                dbs.append(db)
            sch_list = target_ctx.setdefault("schemas_by_db", {}).setdefault(db, [])
            if sch not in sch_list:
                sch_list.append(sch)
            pair_key = f"{db}.{sch}"
            cat_list = target_ctx.setdefault("objects_by_db_schema", {}).setdefault(pair_key, {}).setdefault(cat, [])
            if clean_name not in cat_list:
                cat_list.append(clean_name)
        self.context_epoch += 1
        self.role_scoped_cache.clear()
        return copy.deepcopy(target_ctx)

    # ------------------------------------------------------------------
    # Audit & Explicit 4-Code Denial Builders
    # ------------------------------------------------------------------
    def _record_audit_event(
        self,
        action: str,
        outcome: str,
        target_object: str | None = None,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "authenticated_user": self.authenticated_user,
            "effective_role": self.active_role,
            "execution_mode": self.execution_mode,
            "context_epoch": self.context_epoch,
            "action": action,
            "outcome": outcome,
            "target_object": target_object,
            "error_code": error_code,
            "details": redact_secrets_in_obj(details or {}),
        }
        self.audit_outbox.append(event)
        return event

    def build_error_response(
        self,
        error_code: str,
        operation: str,
        required_privilege: str | None = None,
        detail: str | None = None,
        target_object: str | None = None,
    ) -> SnowflakeAPIResponse:
        """Build a deterministic error response matching the 4-code taxonomy (ADR-004)."""
        spec = ERROR_TAXONOMY_CONTRACT.get(
            error_code, ERROR_TAXONOMY_CONTRACT["TECHNICAL_EXECUTION_ERROR"]
        )
        status_code = int(spec["http_status"])

        if error_code == "AUTHZ_ROLE_DENIED":
            message = f"Il ruolo attivo ({self.active_role}) non consente questa operazione"
        elif error_code == "OBJECT_NOT_FOUND_OR_UNAUTHORIZED":
            message = (
                f"Il ruolo attivo ({self.active_role}) non consente questa operazione: "
                "does not exist or not authorized"
            )
        else:
            message = spec["it_message_template"].format(
                active_role=self.active_role,
                detail=detail or operation,
            )

        if spec.get("audit_required"):
            self._record_audit_event(
                action=operation,
                outcome="DENIED",
                target_object=None if spec.get("disclose_object_existence") is False else target_object,
                error_code=error_code,
                details={
                    "operation": operation,
                    "required_privilege": required_privilege,
                },
            )

        payload: dict[str, Any] = {
            "error_code": error_code,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
            "operation": operation,
            "required_privilege": required_privilege,
            "banner_variant": spec["banner_variant"],
            "message": message,
        }
        if detail and error_code in ("UNSUPPORTED_CAPABILITY", "TECHNICAL_EXECUTION_ERROR"):
            payload["detail"] = detail

        return SnowflakeAPIResponse(status_code=status_code, body=payload)

    def _has_permission(self, permission_key: str) -> bool:
        granted = self.role_permissions.get(self.active_role, set())
        return permission_key in granted or "*" in granted

    # ------------------------------------------------------------------
    # 1. Context & Active Role Switcher (REQ-02, ADR-003, ADR-004, CTX-ARCH-03..07)
    # ------------------------------------------------------------------
    def get_session_context(self, flow_id: str | None = None) -> SnowflakeAPIResponse:
        """GET /api/v1/snowflake/context"""
        clean_fid = str(flow_id or get_current_snowflake_flow_id() or "").strip() or None
        if clean_fid:
            self.active_flow_id = clean_fid
            if clean_fid not in self.contexts_by_flow_id:
                imp = self.imported_flows.get(clean_fid)
                emb = (imp.get("data") or {}).get("snowflake_context") if isinstance(imp, dict) else None
                if isinstance(emb, dict) and emb:
                    tpl = self._default_context_template()
                    tpl.update(copy.deepcopy(emb))
                    tpl["flow_id"] = clean_fid
                    self.contexts_by_flow_id[clean_fid] = tpl
                else:
                    fresh = self._default_context_template()
                    fresh["flow_id"] = clean_fid
                    self.contexts_by_flow_id[clean_fid] = fresh
            flow_ctx = self.contexts_by_flow_id[clean_fid]
        else:
            flow_ctx = self.selected_context

        intersection = self.compute_role_effective_context(clean_fid, self.active_role)
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "flow_id": clean_fid,
                "active_flow_id": self.active_flow_id,
                "authenticated_user": self.authenticated_user,
                "active_role": self.active_role,
                "available_roles": list(self.available_roles),
                "context_epoch": self.context_epoch,
                "execution_mode": self.execution_mode,
                "secondary_roles_mode": self.secondary_roles_mode,
                "selected_context": copy.deepcopy(flow_ctx),
                "effective_context": intersection["effective_context"],
                "inaccessible_objects": intersection["inaccessible_objects"],
                "retained_objects_count": intersection["retained_objects_count"],
                "hidden_by_role_count": intersection["hidden_by_role_count"],
                "traces_tables": {
                    "activity": "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_ACTIVITY",
                    "runs": "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_RUNS",
                    "spans": "<DATABASE>.<SCHEMA>.LANGFLOW_FLOW_SPANS",
                    "audit": "<DATABASE>.<SCHEMA>.LANGFLOW_AUDIT_LOG",
                },
                "verified_current_role_sql": "SELECT CURRENT_ROLE()",
            },
        )

    def update_session_context(
        self,
        payload: dict[str, Any],
        flow_id: str | None = None,
    ) -> SnowflakeAPIResponse:
        """POST|PUT /api/v1/snowflake/context"""
        incoming_ctx = payload.get("selected_context") if isinstance(payload.get("selected_context"), dict) else {}
        clean_fid = str(
            flow_id
            or payload.get("flow_id")
            or incoming_ctx.get("flow_id")
            or get_current_snowflake_flow_id()
            or ""
        ).strip() or None
        target_ctx = (
            copy.deepcopy(self.contexts_by_flow_id.get(clean_fid, self._default_context_template()))
            if clean_fid
            else copy.deepcopy(self.selected_context)
        )
        for key in (
            "database",
            "schema",
            "databases",
            "schemas_by_db",
            "full_databases",
            "full_schemas",
            "postgres_instances",
            "selected_postgres_instances",
            "postgres_tables_by_instance",
            "objects_by_db_schema",
            "selected_objects",
            "visible_objects",
        ):
            if key in payload:
                target_ctx[key] = copy.deepcopy(payload[key])
            elif key in incoming_ctx:
                target_ctx[key] = copy.deepcopy(incoming_ctx[key])

        if target_ctx.get("postgres_instances") and not target_ctx.get("selected_postgres_instances"):
            target_ctx["selected_postgres_instances"] = list(target_ctx["postgres_instances"])
        elif target_ctx.get("selected_postgres_instances") and not target_ctx.get("postgres_instances"):
            target_ctx["postgres_instances"] = list(target_ctx["selected_postgres_instances"])

        for fd in target_ctx.get("full_databases") or []:
            fd_u = str(fd).strip().upper()
            if fd_u and fd_u not in (target_ctx.get("databases") or []):
                target_ctx.setdefault("databases", []).append(fd_u)
        for fs in target_ctx.get("full_schemas") or []:
            parts = str(fs).strip().upper().split(".", 1)
            if len(parts) == 2 and parts[0] and parts[1]:
                if parts[0] not in (target_ctx.get("databases") or []):
                    target_ctx.setdefault("databases", []).append(parts[0])
                sch_l = target_ctx.setdefault("schemas_by_db", {}).setdefault(parts[0], [])
                if parts[1] not in sch_l:
                    sch_l.append(parts[1])

        self.context_epoch += 1
        target_ctx["active_role"] = self.active_role
        target_ctx["context_epoch"] = self.context_epoch
        if clean_fid:
            target_ctx["flow_id"] = clean_fid
            self.active_flow_id = clean_fid
            self.contexts_by_flow_id[clean_fid] = copy.deepcopy(target_ctx)
            if clean_fid in self.imported_flows and isinstance(self.imported_flows[clean_fid].get("data"), dict):
                self.imported_flows[clean_fid]["data"]["snowflake_context"] = copy.deepcopy(target_ctx)
        else:
            self.selected_context = copy.deepcopy(target_ctx)

        intersection = self.compute_role_effective_context(clean_fid, self.active_role)
        self._record_audit_event(
            action="UPDATE_CONTEXT",
            outcome="SUCCESS",
            target_object=clean_fid or "<DATABASE>.<SCHEMA>",
            details={"flow_id": clean_fid, "context_epoch": self.context_epoch},
        )
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "ok": True,
                "flow_id": clean_fid,
                "active_flow_id": self.active_flow_id,
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
                "selected_context": copy.deepcopy(target_ctx),
                "effective_context": intersection["effective_context"],
                "inaccessible_objects": intersection["inaccessible_objects"],
                "retained_objects_count": intersection["retained_objects_count"],
                "hidden_by_role_count": intersection["hidden_by_role_count"],
            },
        )

    def switch_active_role(
        self,
        requested_role: str,
        use_secondary_roles_all: bool = False,
        allow_service_role_fallback: bool = False,
        flow_id: str | None = None,
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/role"""
        if use_secondary_roles_all:
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="SWITCH_ROLE_SECONDARY_ALL_FORBIDDEN",
                required_privilege="EXPLICIT_PRIMARY_ROLE_ONLY",
            )
        if allow_service_role_fallback:
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="SWITCH_ROLE_SERVICE_FALLBACK_FORBIDDEN",
                required_privilege="CALLER_ROLE_ONLY",
            )

        clean_role = (requested_role or "").strip()
        if not clean_role or clean_role not in self.available_roles:
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation=f"USE_ROLE:{clean_role or '<EMPTY>'}",
                required_privilege=f"ROLE {clean_role or '<EMPTY>'} GRANTED TO USER",
            )

        previous_role = self.active_role
        self.active_role = clean_role
        self.context_epoch += 1
        # Invalidate all role-scoped caches immediately on role switch without destroying stored flow context (`CTX-ARCH-07` #1)
        self.role_scoped_cache.clear()
        self.global_variables["SNOWFLAKE_ACTIVE_ROLE"]["value"] = self.active_role
        eff_fid = str(flow_id or get_current_snowflake_flow_id() or self.active_flow_id or "").strip() or None
        intersection = self.compute_role_effective_context(eff_fid, self.active_role)

        self._record_audit_event(
            action="SWITCH_ACTIVE_ROLE",
            outcome="SUCCESS",
            target_object=self.active_role,
            details={
                "previous_role": previous_role,
                "new_role": self.active_role,
                "flow_id": eff_fid,
                "retained_objects_count": intersection["retained_objects_count"],
                "hidden_by_role_count": intersection["hidden_by_role_count"],
            },
        )
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "previous_role": previous_role,
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
                "flow_id": eff_fid,
                "selected_context": intersection["stored_context"],
                "effective_context": intersection["effective_context"],
                "inaccessible_objects": intersection["inaccessible_objects"],
                "retained_objects_count": intersection["retained_objects_count"],
                "hidden_by_role_count": intersection["hidden_by_role_count"],
                "cache_invalidated": True,
                "verified_via": "SELECT CURRENT_ROLE()",
            },
        )

    def soft_delete_flow_activity(
        self,
        activity_ids: list[str] | str,
        flow_id: str | None = None,
    ) -> SnowflakeAPIResponse:
        """Soft-delete Flow Activity items (`DELETED_IN_UI_AT = CURRENT_TIMESTAMP()`) without deleting from Snowflake (`CTX-ARCH-07` #5)."""
        ids = (
            [x.strip() for x in activity_ids.split(",") if x.strip()]
            if isinstance(activity_ids, str)
            else [str(x).strip() for x in activity_ids if str(x).strip()]
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        for item_id in ids:
            self.soft_deleted_activity_ids.add(item_id)
        for item in self.activity_outbox:
            if str(item.get("activity_id") or item.get("id") or "") in self.soft_deleted_activity_ids:
                item["deleted_in_ui_at"] = now_iso
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "ok": True,
                "soft_deleted_ids": ids,
                "flow_id": flow_id,
                "deleted_in_ui_at": now_iso,
                "physical_delete_executed": False,
                "snowflake_retained": True,
            },
        )

    def revoke_role_or_privilege(
        self,
        role: str,
        privilege: str | None = None,
    ) -> SnowflakeAPIResponse:
        """Handle runtime privilege or role revocation by bumping context_epoch and purging caches."""
        if privilege is None:
            if role in self.available_roles:
                self.available_roles.remove(role)
            self.role_permissions.pop(role, None)
        else:
            if role in self.role_permissions:
                self.role_permissions[role].discard(privilege)

        self.context_epoch += 1
        self.role_scoped_cache.clear()
        self._record_audit_event(
            action="REVOKE_PRIVILEGE_OR_ROLE",
            outcome="SUCCESS",
            target_object=role,
            details={"revoked_privilege": privilege or "ALL_ROLE_ACCESS"},
        )
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "role": role,
                "revoked_privilege": privilege,
                "context_epoch": self.context_epoch,
                "cache_invalidated": True,
            },
        )

    # ------------------------------------------------------------------
    # 2. UI Bootstrap & Branding / Invariant Categories (REQ-01, REQ-11)
    # ------------------------------------------------------------------
    def get_ui_bootstrap(self) -> SnowflakeAPIResponse:
        """GET /api/v1/snowflake/ui/bootstrap"""
        if not self._has_permission("UI_BOOTSTRAP"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="UI_BOOTSTRAP",
                required_privilege="UI_BOOTSTRAP",
            )
        integrity = verify_invariant_category_integrity(INVARIANT_CATEGORIES_MANIFEST)
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "bundle": get_bundle_manifest(),
                "branding": SNOWFLAKE_BRANDING_CONTRACT,
                "invariant_integrity": integrity,
                "agent_memory_types": list(AGENT_MEMORY_TYPES),
                "legacy_memory_type_aliases": dict(LEGACY_MEMORY_TYPE_ALIASES),
                "external_embedding_providers": EXTERNAL_EMBEDDING_PROVIDERS,
                "embedding_providers_catalog": list(EMBEDDING_PROVIDERS_CATALOG),
                "session": {
                    "active_role": self.active_role,
                    "available_roles": list(self.available_roles),
                    "context_epoch": self.context_epoch,
                },
            },
        )

    # ------------------------------------------------------------------
    # 3. New Flow Import Routes (REQ-03)
    # ------------------------------------------------------------------
    def _validate_and_sanitize_flow_payload(
        self,
        raw_flow: dict[str, Any] | str,
        source_metadata: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        if isinstance(raw_flow, str):
            try:
                parsed = json.loads(raw_flow)
            except json.JSONDecodeError as exc:
                return None, f"Invalid flow JSON: {exc}"
        elif isinstance(raw_flow, dict):
            parsed = copy.deepcopy(raw_flow)
        else:
            return None, "Flow payload must be a JSON object"

        if not isinstance(parsed, dict) or "name" not in parsed or "data" not in parsed:
            return None, "Flow JSON schema requires 'name' and 'data' fields"

        data_section = parsed.get("data")
        if not isinstance(data_section, dict) or "nodes" not in data_section or "edges" not in data_section:
            return None, "Flow 'data' section requires 'nodes' and 'edges' arrays"

        # Sanitize embedded secrets & strip any auto-exec flags
        sanitized = redact_secrets_in_obj(parsed)
        for node in sanitized.get("data", {}).get("nodes", []):
            if isinstance(node, dict):
                node.pop("auto_execute_on_import", None)
                if isinstance(node.get("data"), dict):
                    node["data"].pop("auto_execute_on_import", None)

        flow_id = str(sanitized.get("id") or uuid.uuid4())
        sanitized["id"] = flow_id
        sanitized_data = sanitized.get("data")
        if isinstance(sanitized_data, dict):
            emb_ctx = sanitized_data.get("snowflake_context")
            if isinstance(emb_ctx, dict) and emb_ctx:
                tpl = self._default_context_template()
                tpl.update(copy.deepcopy(emb_ctx))
                tpl["flow_id"] = flow_id
                tpl["active_role"] = self.active_role
                tpl["context_epoch"] = self.context_epoch
                self.contexts_by_flow_id[flow_id] = copy.deepcopy(tpl)
                sanitized_data["snowflake_context"] = copy.deepcopy(tpl)
            elif flow_id in self.contexts_by_flow_id:
                sanitized_data["snowflake_context"] = copy.deepcopy(self.contexts_by_flow_id[flow_id])
        sanitized["snowflake_import_metadata"] = {
            **redact_secrets_in_obj(source_metadata),
            "imported_by_role": self.active_role,
            "context_epoch": self.context_epoch,
            "custom_code_executed_on_import": False,
        }
        return sanitized, None

    def duplicate_flow(
        self,
        source_flow_id: str,
        new_flow_id: str | None = None,
        new_name: str | None = None,
    ) -> SnowflakeAPIResponse:
        """Duplicate a flow and preserve its isolated `snowflake_context` (`CTX-ARCH-07` #3)."""
        src_fid = str(source_flow_id or "").strip()
        dst_fid = str(new_flow_id or uuid.uuid4()).strip()
        src_ctx = copy.deepcopy(
            self.contexts_by_flow_id.get(src_fid, self._default_context_template())
        )
        src_ctx["flow_id"] = dst_fid
        src_ctx["active_role"] = self.active_role
        self.context_epoch += 1
        src_ctx["context_epoch"] = self.context_epoch
        self.contexts_by_flow_id[dst_fid] = copy.deepcopy(src_ctx)

        src_flow = copy.deepcopy(
            self.imported_flows.get(
                src_fid,
                {
                    "id": src_fid,
                    "name": f"Flow {src_fid}",
                    "data": {"nodes": [], "edges": [], "snowflake_context": copy.deepcopy(src_ctx)},
                },
            )
        )
        src_flow["id"] = dst_fid
        src_flow["name"] = new_name or f"{src_flow.get('name', 'Flow')} (Copy)"
        src_flow.setdefault("data", {})["snowflake_context"] = copy.deepcopy(src_ctx)
        self.imported_flows[dst_fid] = src_flow
        return SnowflakeAPIResponse(
            status_code=201,
            body={
                "ok": True,
                "source_flow_id": src_fid,
                "flow_id": dst_fid,
                "flow": src_flow,
                "selected_context": copy.deepcopy(src_ctx),
            },
        )

    def import_flow_from_upload(
        self,
        file_name: str,
        flow_content: dict[str, Any] | str,
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/flows/import/upload"""
        if not self._has_permission("IMPORT_FLOW_UPLOAD"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="IMPORT_FLOW_UPLOAD",
                required_privilege="IMPORT_FLOW_UPLOAD",
            )
        sanitized, err = self._validate_and_sanitize_flow_payload(
            flow_content,
            source_metadata={
                "import_mode": NEW_FLOW_IMPORT_MODES["upload_from_computer"]["id"],
                "file_name": file_name,
            },
        )
        if err or sanitized is None:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="IMPORT_FLOW_UPLOAD",
                detail=err or "Invalid flow schema",
            )
        self.imported_flows[sanitized["id"]] = sanitized
        self._record_audit_event(
            action="IMPORT_FLOW_UPLOAD",
            outcome="SUCCESS",
            target_object=sanitized["id"],
            details={"file_name": file_name},
        )
        return SnowflakeAPIResponse(status_code=201, body={"flow": sanitized})

    def import_flow_from_stage(
        self,
        stage_uri: str,
        stage_file_payload: dict[str, Any] | str,
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/flows/import/stage"""
        raw_uri = (stage_uri or "").strip()
        if ".." in raw_uri or "//" in raw_uri:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="IMPORT_FLOW_STAGE",
                detail="Path traversal or malformed subpath in stage URI is forbidden",
            )
        match = STAGE_URI_REGEX.match(raw_uri)
        if not match:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="IMPORT_FLOW_STAGE",
                detail="Stage URI must match @<DATABASE>.<SCHEMA>.<STAGE_NAME>/<subpath.json>",
            )

        stage_root = f"@{match.group('database')}.{match.group('schema')}.{match.group('stage')}"
        perm_key = f"IMPORT_FLOW_STAGE:{stage_root}"
        if not (self._has_permission(perm_key) or self._has_permission("IMPORT_FLOW_STAGE:*")):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="IMPORT_FLOW_STAGE",
                required_privilege=f"READ ON STAGE {stage_root}",
                target_object=stage_root,
            )

        sanitized, err = self._validate_and_sanitize_flow_payload(
            stage_file_payload,
            source_metadata={
                "import_mode": NEW_FLOW_IMPORT_MODES["add_from_stage"]["id"],
                "stage_root": stage_root,
                "subpath": match.group("subpath") or "",
            },
        )
        if err or sanitized is None:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="IMPORT_FLOW_STAGE",
                detail=err or "Invalid stage flow JSON",
            )
        self.imported_flows[sanitized["id"]] = sanitized
        self._record_audit_event(
            action="IMPORT_FLOW_STAGE",
            outcome="SUCCESS",
            target_object=stage_root,
            details={"flow_id": sanitized["id"], "subpath": match.group("subpath")},
        )
        return SnowflakeAPIResponse(status_code=201, body={"flow": sanitized})

    def import_flow_from_git(
        self,
        repo_url: str,
        git_ref: str,
        commit_sha: str,
        file_path: str,
        secret_ref: str,
        eai_name: str,
        fetched_flow_payload: dict[str, Any] | str,
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/flows/import/git"""
        if not self._has_permission("IMPORT_FLOW_GIT"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="IMPORT_FLOW_GIT",
                required_privilege="USAGE ON EXTERNAL ACCESS INTEGRATION / SECRET",
            )
        if not repo_url.startswith("https://") or ".." in file_path or not commit_sha.strip():
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="IMPORT_FLOW_GIT",
                detail="HTTPS repo_url, valid file_path, and explicit commit_sha are required",
            )

        sanitized, err = self._validate_and_sanitize_flow_payload(
            fetched_flow_payload,
            source_metadata={
                "import_mode": NEW_FLOW_IMPORT_MODES["add_from_git"]["id"],
                "repo_url": repo_url,
                "git_ref": git_ref,
                "commit_sha": commit_sha.strip(),
                "file_path": file_path,
                "eai_name": eai_name,
                "secret_ref": secret_ref,
            },
        )
        if err or sanitized is None:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="IMPORT_FLOW_GIT",
                detail=err or "Invalid Git flow JSON",
            )
        self.imported_flows[sanitized["id"]] = sanitized
        self._record_audit_event(
            action="IMPORT_FLOW_GIT",
            outcome="SUCCESS",
            target_object=repo_url,
            details={
                "flow_id": sanitized["id"],
                "git_ref": git_ref,
                "commit_sha": commit_sha.strip(),
                "file_path": file_path,
                "eai_name": eai_name,
            },
        )
        return SnowflakeAPIResponse(status_code=201, body={"flow": sanitized})

    # ------------------------------------------------------------------
    # 4. Settings Routes: Model Providers, DB Providers, Messages, Global Variables (REQ-04)
    # ------------------------------------------------------------------
    def get_settings_section(self, section: str) -> SnowflakeAPIResponse:
        """GET /api/v1/snowflake/settings/{section}"""
        if not self._has_permission("SETTINGS_READ"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation=f"SETTINGS_READ:{section}",
                required_privilege="SETTINGS_READ",
            )
        if section not in SETTINGS_CONTRACT:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation=f"SETTINGS_READ:{section}",
                detail=f"Unknown settings section: {section}",
            )
        data = copy.deepcopy(SETTINGS_CONTRACT[section])
        if section == "Global Variables":
            data["current_values"] = copy.deepcopy(self.global_variables)
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "section": section,
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
                "config": data,
            },
        )

    def update_global_variable(self, name: str, value: str) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/settings/global-variables"""
        if not self._has_permission("GLOBAL_VARIABLES_WRITE"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation=f"GLOBAL_VARIABLE_WRITE:{name}",
                required_privilege="GLOBAL_VARIABLES_WRITE",
            )
        clean_name = (name or "").strip().upper()
        if clean_name in ("SNOWFLAKE_ACTIVE_ROLE", "CURRENT_ROLE", "SNOWFLAKE_ROLE"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation=f"GLOBAL_VARIABLE_OVERRIDE_ROLE:{clean_name}",
                required_privilege="BACKEND_SESSION_AUTHORITATIVE_ONLY",
            )

        self.global_variables[clean_name] = {
            "name": clean_name,
            "value": redact_secrets_in_obj(value),
            "read_only": False,
            "updated_by_role": self.active_role,
            "context_epoch": self.context_epoch,
        }
        self._record_audit_event(
            action="UPDATE_GLOBAL_VARIABLE",
            outcome="SUCCESS",
            target_object=clean_name,
        )
        return SnowflakeAPIResponse(
            status_code=200,
            body={"variable": self.global_variables[clean_name]},
        )

    def upsert_protocol_message(
        self,
        message_id: str,
        protocol: str,
        visible_text: str,
        signed_replay_blocks: dict[str, Any] | None = None,
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/settings/messages"""
        if not self._has_permission("MESSAGES_WRITE"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="MESSAGES_WRITE",
                required_privilege="MESSAGES_WRITE",
            )
        norm_protocol = (protocol or "").strip().lower()
        if norm_protocol not in ("chat", "responses", "messages"):
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="MESSAGES_WRITE",
                detail=f"Protocol '{protocol}' not in ['chat', 'responses', 'messages']",
            )
        existing = self.messages_store.get(message_id)
        branched_from_id: str | None = None
        signed_block_invalidated = False
        if existing and existing.get("signed_replay_blocks"):
            if existing.get("visible_text") != visible_text or (
                signed_replay_blocks is not None
                and signed_replay_blocks != existing.get("signed_replay_blocks")
            ):
                signed_block_invalidated = True
                branched_from_id = message_id

        record = {
            "message_id": message_id,
            "protocol": norm_protocol,
            "visible_text": visible_text,
            "signed_replay_blocks": None if signed_block_invalidated else (signed_replay_blocks or {}),
            "signed_block_invalidated": signed_block_invalidated,
            "branched_from_message_id": branched_from_id,
            "active_role": self.active_role,
            "context_epoch": self.context_epoch,
        }
        self.messages_store[message_id] = record
        return SnowflakeAPIResponse(status_code=200, body={"message": record})

    def delete_protocol_message(self, message_id: str) -> SnowflakeAPIResponse:
        """DELETE /api/v1/snowflake/settings/messages/{message_id}"""
        if not self._has_permission("MESSAGES_WRITE"):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation="MESSAGES_DELETE",
                required_privilege="MESSAGES_WRITE",
            )
        removed = self.messages_store.pop(message_id, None)
        if removed is None:
            return self.build_error_response(
                error_code="OBJECT_NOT_FOUND_OR_UNAUTHORIZED",
                operation="MESSAGES_DELETE",
                target_object=message_id,
            )
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "deleted_message_id": message_id,
                "protocol": removed["protocol"],
                "signed_replay_chain_invalidated": bool(removed.get("signed_replay_blocks")),
            },
        )

    # ------------------------------------------------------------------
    # 5. Create Memory Dialog Endpoint (REQ-10, G-A4, W2-R3-05)
    # ------------------------------------------------------------------
    def list_memories_catalog(self) -> SnowflakeAPIResponse:
        """GET /api/v1/snowflake/memories"""
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "memories": list(self.memories_registry.values()),
                "agent_memory_types": list(AGENT_MEMORY_TYPES),
                "legacy_memory_type_aliases": dict(LEGACY_MEMORY_TYPE_ALIASES),
                "external_embedding_providers": EXTERNAL_EMBEDDING_PROVIDERS,
                "embedding_providers_catalog": list(EMBEDDING_PROVIDERS_CATALOG),
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
            },
        )

    def create_or_bind_memory(
        self,
        name: str,
        embedding_model: str = "snowflake-arctic-embed-m-v1.5",
        vector_database: str = "snowflake_vector",
        batch_size: int = 64,
        llm_preprocessing_enabled: bool = False,
        llm_preprocessing_provider: str | None = None,
        create_new_physical_store: bool = False,
        *,
        agent_memory_type: str = "episodic_memory",
        enable_embedding: bool = False,
        embedding_provider: str = "snowflake_cortex",
        custom_dimension: int | None = None,
        endpoint_url: str | None = None,
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/memories/create"""
        # Granular permission separation (G-A4): using an existing vector store does NOT require CREATE privilege
        required_perm = (
            "MEMORY_CREATE_PHYSICAL" if create_new_physical_store else "MEMORY_USE_EXISTING"
        )
        if not self._has_permission(required_perm):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation=f"MEMORY_{'CREATE_PHYSICAL' if create_new_physical_store else 'BIND_EXISTING'}",
                required_privilege=(
                    "CREATE TABLE / CREATE CORTEX SEARCH SERVICE"
                    if create_new_physical_store
                    else "SELECT / INSERT ON VECTOR STORE"
                ),
            )

        valid_backends = {
            b["id"] for b in SETTINGS_CONTRACT["DB Providers"]["backends"]
        } | {
            "snowflake_vector",
            "snowflake_standard_vector",
            "snowflake_standard_table",
            "snowflake_hybrid_table",
            "snowflake_postgres",
        }
        for mem_spec in AGENT_MEMORY_TYPES:
            valid_backends.update(mem_spec.get("recommended_snowflake_objects", []))
            valid_backends.update(mem_spec.get("supported_backends", []))
        if vector_database not in valid_backends:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="MEMORY_CREATE",
                detail=f"Unsupported vector database backend: {vector_database}",
            )

        if llm_preprocessing_enabled and not llm_preprocessing_provider:
            return self.build_error_response(
                error_code="UNSUPPORTED_CAPABILITY",
                operation="MEMORY_CREATE",
                detail="llm_preprocessing_provider is required when LLM Preprocessing is enabled",
            )

        canonical_type = resolve_canonical_memory_type(agent_memory_type)
        type_spec = CANONICAL_MEMORY_TYPES_BY_ID[canonical_type]
        dim = (
            resolve_memory_embedding_dimension(
                embedding_model,
                embedding_provider=embedding_provider,
                custom_dimension=custom_dimension,
                default=768,
            )
            if enable_embedding
            else None
        )

        memory_id = str(uuid.uuid4())
        record = {
            "memory_id": memory_id,
            "name": name,
            "agent_memory_type": agent_memory_type,
            "canonical_memory_type": canonical_type,
            "label": type_spec["label"],
            "recommended_snowflake_objects": list(type_spec["recommended_snowflake_objects"]),
            "needs_embedding_badge": type_spec["needs_embedding_badge"],
            "how_to_feed_without_embedding": type_spec["how_to_feed_without_embedding"],
            "when_embedding_useful": type_spec["when_embedding_useful"],
            "required_inputs": list(type_spec["required_inputs"]),
            "enable_embedding": bool(enable_embedding),
            "embedding_provider": embedding_provider if enable_embedding else "none",
            "embedding_model": embedding_model,
            "dimension": dim,
            "endpoint_url": endpoint_url if enable_embedding else None,
            "vector_database": vector_database,
            "batch_size": int(batch_size),
            "llm_preprocessing_enabled": bool(llm_preprocessing_enabled),
            "llm_preprocessing_provider": (
                llm_preprocessing_provider if llm_preprocessing_enabled else None
            ),
            "llm_calls_when_disabled": 0,
            "created_physical_store": bool(create_new_physical_store),
            "owner_role": self.active_role,
            "context_epoch": self.context_epoch,
            "owners_rights_isolation_filter_enforced": vector_database == "snowflake_cortex_search",
        }
        self.memories_registry[memory_id] = record
        self._record_audit_event(
            action="MEMORY_CREATE_OR_BIND",
            outcome="SUCCESS",
            target_object=name,
            details={
                "agent_memory_type": agent_memory_type,
                "canonical_memory_type": canonical_type,
                "enable_embedding": enable_embedding,
                "embedding_provider": embedding_provider,
                "vector_database": vector_database,
                "create_new_physical_store": create_new_physical_store,
                "llm_preprocessing_enabled": llm_preprocessing_enabled,
            },
        )
        return SnowflakeAPIResponse(status_code=201, body={"memory": record})

    # ------------------------------------------------------------------
    # 6. Serve as an A2A Agent Endpoint (REQ-14)
    # ------------------------------------------------------------------
    def configure_a2a_serving(
        self,
        flow_id: str,
        flow_component_types: list[str],
        card_metadata: dict[str, Any],
        action: str = "save",  # "save" | "publish" | "unpublish"
    ) -> SnowflakeAPIResponse:
        """POST /api/v1/snowflake/a2a/serve"""
        has_chat_input = "Chat Input" in flow_component_types
        has_chat_output = "Chat Output" in flow_component_types
        if not (has_chat_input and has_chat_output):
            return SnowflakeAPIResponse(
                status_code=422,
                body={
                    "error_code": "UNSUPPORTED_CAPABILITY",
                    "availability": "Unavailable",
                    "reason": "Flow requires both 'Chat Input' and 'Chat Output' to serve as an A2A agent",
                    "has_chat_input": has_chat_input,
                    "has_chat_output": has_chat_output,
                },
            )

        perm_key = "A2A_PUBLISH" if action in ("publish", "unpublish") else "A2A_SAVE_METADATA"
        if not self._has_permission(perm_key):
            return self.build_error_response(
                error_code="AUTHZ_ROLE_DENIED",
                operation=f"A2A_{action.upper()}",
                required_privilege=perm_key,
                target_object=flow_id,
            )

        sanitized_card = redact_secrets_in_obj(card_metadata)
        address_url = f"https://<SPCS_INGRESS_HOST>/api/v1/a2a/flows/{flow_id}/invoke"
        curl_snippet = (
            f'curl -X POST "{address_url}" '
            '-H "Authorization: Bearer <SPCS_CALLER_TOKEN>" '
            '-H "Content-Type: application/json" '
            '-d \'{"input": "Hello"}\''
        )
        record = {
            "flow_id": flow_id,
            "availability": "Available",
            "published": action == "publish",
            "card_metadata": sanitized_card,
            "address_url": address_url,
            "curl_example": curl_snippet,
            "credentials_embedded_in_address": False,
            "configured_by_role": self.active_role,
            "context_epoch": self.context_epoch,
        }
        self.a2a_published_flows[flow_id] = record
        self._record_audit_event(
            action=f"A2A_{action.upper()}",
            outcome="SUCCESS",
            target_object=flow_id,
        )
        return SnowflakeAPIResponse(status_code=200, body={"a2a_service": record})
