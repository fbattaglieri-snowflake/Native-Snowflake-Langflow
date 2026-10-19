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
        self.audit_outbox: list[dict[str, Any]] = []
        self.imported_flows: dict[str, dict[str, Any]] = {}
        self.memories_registry: dict[str, dict[str, Any]] = {}
        self.a2a_published_flows: dict[str, dict[str, Any]] = {}
        self.messages_store: dict[str, dict[str, Any]] = {}
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
    # 1. Context & Active Role Switcher (REQ-02, ADR-003, ADR-004)
    # ------------------------------------------------------------------
    def get_session_context(self) -> SnowflakeAPIResponse:
        """GET /api/v1/snowflake/context"""
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "authenticated_user": self.authenticated_user,
                "active_role": self.active_role,
                "available_roles": list(self.available_roles),
                "context_epoch": self.context_epoch,
                "execution_mode": self.execution_mode,
                "secondary_roles_mode": self.secondary_roles_mode,
                "verified_current_role_sql": "SELECT CURRENT_ROLE()",
            },
        )

    def switch_active_role(
        self,
        requested_role: str,
        use_secondary_roles_all: bool = False,
        allow_service_role_fallback: bool = False,
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
        # Invalidate all role-scoped caches immediately on role switch
        self.role_scoped_cache.clear()
        self.global_variables["SNOWFLAKE_ACTIVE_ROLE"]["value"] = self.active_role

        self._record_audit_event(
            action="SWITCH_ACTIVE_ROLE",
            outcome="SUCCESS",
            target_object=self.active_role,
            details={"previous_role": previous_role, "new_role": self.active_role},
        )
        return SnowflakeAPIResponse(
            status_code=200,
            body={
                "previous_role": previous_role,
                "active_role": self.active_role,
                "context_epoch": self.context_epoch,
                "cache_invalidated": True,
                "verified_via": "SELECT CURRENT_ROLE()",
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
        sanitized["snowflake_import_metadata"] = {
            **redact_secrets_in_obj(source_metadata),
            "imported_by_role": self.active_role,
            "context_epoch": self.context_epoch,
            "custom_code_executed_on_import": False,
        }
        return sanitized, None

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
    # 5. Create Memory Dialog Endpoint (REQ-10, G-A4)
    # ------------------------------------------------------------------
    def create_or_bind_memory(
        self,
        name: str,
        embedding_model: str,
        vector_database: str,
        batch_size: int = 64,
        llm_preprocessing_enabled: bool = False,
        llm_preprocessing_provider: str | None = None,
        create_new_physical_store: bool = False,
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
        }
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

        memory_id = str(uuid.uuid4())
        record = {
            "memory_id": memory_id,
            "name": name,
            "embedding_model": embedding_model,
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
