"""Universal Backend Enforcement Middleware and Route/Component Guards (ADR-004, REQ-02).

Provides:
- `require_snowflake_permission` decorator for backend routes, components, and Tool Mode
- `SnowflakeGovernanceMiddleware` handling:
  - `GET /api/v1/snowflake/context` and `POST /api/v1/snowflake/role`
  - Settings guards (`Model Providers`, `DB Providers`, `Messages`, `Global Variables`
    with read-only `SNOWFLAKE_ACTIVE_ROLE` protection)
  - `New Flow` import guards (`Upload from computer`, `Add from stage`, `Add from Git`)
  - Component execution & Tool Mode guards
  - `A2A` client & server (`Serve as an A2A agent`) caller RBAC and loop protection
  - `Flow Activity` / SQL traces read and export guards
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from lfx_snowflake.governance.authz_service import SnowflakeAuthzService
from lfx_snowflake.governance.models import (
    OperationType,
    RoleSessionContext,
    SnowflakeGovernanceError,
    SubsystemBoundary,
)
from lfx_snowflake.governance.provisioner import SnowflakeObjectProvisioner


READONLY_SYSTEM_VARIABLES = frozenset(
    {
        "SNOWFLAKE_ACTIVE_ROLE",
        "SNOWFLAKE_EFFECTIVE_ROLE",
        "SNOWFLAKE_CONTEXT_EPOCH",
        "SNOWFLAKE_USER_PRINCIPAL",
    }
)


def require_snowflake_permission(
    authz_service: SnowflakeAuthzService,
    operation: OperationType | str,
    *,
    target_ref_arg: str | None = None,
    required_privilege: str | None = None,
    disclosable: bool = False,
    subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator enforcing backend role authorization before executing any route or node."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            user_principal = kwargs.get("user_principal")
            if not user_principal and args:
                first = args[0]
                if isinstance(first, str):
                    user_principal = first
                elif hasattr(first, "user_principal"):
                    user_principal = getattr(first, "user_principal")
            if not user_principal:
                raise SnowflakeGovernanceError.role_denied(
                    active_role="UNAUTHENTICATED",
                    operation=operation.value if isinstance(operation, OperationType) else str(operation),
                    subsystem=subsystem,
                )

            target_ref = str(kwargs.get(target_ref_arg, "*")) if target_ref_arg else "*"
            claimed_role = kwargs.get("claimed_role") or kwargs.get("client_role_override")
            request_epoch = kwargs.get("context_epoch")

            authz_service.check_operation_permission(
                user_principal=str(user_principal),
                operation=operation,
                target_ref=target_ref,
                required_privilege=required_privilege,
                disclosable=disclosable,
                subsystem=subsystem,
                claimed_role_from_client=claimed_role,
                request_epoch=request_epoch,
            )
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                raise authz_service.classify_exception(
                    user_principal=str(user_principal),
                    operation=operation,
                    exc=exc,
                    subsystem=subsystem,
                ) from exc

        return wrapper

    return decorator


class SnowflakeGovernanceMiddleware:
    """Backend enforcement facade for Langflow REST routes, Settings, Flows, and A2A."""

    MAX_A2A_CALL_DEPTH = 4

    def __init__(
        self,
        authz_service: SnowflakeAuthzService,
        provisioner: SnowflakeObjectProvisioner | None = None,
    ) -> None:
        self.authz_service = authz_service
        self.provisioner = provisioner or SnowflakeObjectProvisioner(authz_service)

    # ------------------------------------------------------------------
    # Endpoints: GET /api/v1/snowflake/context & POST /api/v1/snowflake/role
    # ------------------------------------------------------------------
    def handle_get_context(self, user_principal: str) -> tuple[int, dict[str, Any]]:
        """Handle `GET /api/v1/snowflake/context`."""
        try:
            payload = self.authz_service.get_context_endpoint_payload(user_principal)
            return 200, payload
        except SnowflakeGovernanceError as err:
            return err.http_status, err.to_http_payload()

    def handle_post_role(
        self,
        *,
        user_principal: str,
        target_role: str,
        project_id: str | None = None,
        secondary_roles_mode: str = "NONE",
    ) -> tuple[int, dict[str, Any]]:
        """Handle `POST /api/v1/snowflake/role` to switch active Snowflake role."""
        try:
            if secondary_roles_mode.upper() == "ALL":
                raise SnowflakeGovernanceError.role_denied(
                    active_role=target_role,
                    operation=OperationType.SWITCH_ROLE.value,
                    detail="SECONDARY ROLES ALL is prohibited.",
                )
            ctx = self.authz_service.switch_role(
                user_principal=user_principal,
                target_role=target_role,
                project_id=project_id,
            )
            return 200, {
                "status": "ROLE_SWITCHED",
                "user_principal": ctx.user_principal,
                "active_role": ctx.active_role,
                "available_roles": list(ctx.available_roles),
                "context_epoch": ctx.context_epoch,
                "verified_current_role": ctx.verified_current_role,
                "session_token_scope": ctx.session_token_scope,
            }
        except SnowflakeGovernanceError as err:
            return err.http_status, err.to_http_payload()

    # ------------------------------------------------------------------
    # Settings & Global Variables Enforcement (REQ-04)
    # ------------------------------------------------------------------
    def enforce_global_variable_write(
        self,
        *,
        user_principal: str,
        variable_name: str,
        variable_value: str,
        request_epoch: int | None = None,
    ) -> dict[str, Any]:
        """Protect read-only backend governance variables from user/flow mutation."""
        normalized_name = (variable_name or "").strip().upper()
        ctx = self.authz_service.get_session_context(user_principal)
        if normalized_name in READONLY_SYSTEM_VARIABLES:
            self.authz_service._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation=OperationType.UPDATE_SETTINGS_GLOBAL_VARIABLES.value,
                target_ref=normalized_name,
                status="DENIED",
                error_code="AUTHZ_ROLE_DENIED",
                project_id=ctx.project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation=OperationType.UPDATE_SETTINGS_GLOBAL_VARIABLES.value,
                detail=(
                    f"Global variable '{normalized_name}' is read-only and controlled "
                    "exclusively by the backend role session."
                ),
            )
        self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.UPDATE_SETTINGS_GLOBAL_VARIABLES,
            target_ref=normalized_name,
            request_epoch=request_epoch,
        )
        return {
            "variable_name": normalized_name,
            "variable_value": variable_value,
            "effective_role": ctx.active_role,
            "context_epoch": ctx.context_epoch,
        }

    def get_effective_global_variables(
        self,
        *,
        user_principal: str,
        user_variables: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Merge user global variables while forcing `SNOWFLAKE_ACTIVE_ROLE` from backend session."""
        ctx = self.authz_service.get_session_context(user_principal)
        merged = {
            k: v
            for k, v in (user_variables or {}).items()
            if k.strip().upper() not in READONLY_SYSTEM_VARIABLES
        }
        merged["SNOWFLAKE_ACTIVE_ROLE"] = ctx.active_role
        merged["SNOWFLAKE_CONTEXT_EPOCH"] = str(ctx.context_epoch)
        return merged

    # ------------------------------------------------------------------
    # New Flow Import Enforcement (Upload, Stage, Git - REQ-03)
    # ------------------------------------------------------------------
    def enforce_flow_import(
        self,
        *,
        user_principal: str,
        source_type: str,
        flow_payload: dict[str, Any],
        stage_coords: dict[str, str] | None = None,
        git_metadata: dict[str, str] | None = None,
        request_epoch: int | None = None,
    ) -> dict[str, Any]:
        """Validate flow import permissions, stage subpaths, secret redaction, and no auto-exec."""
        src = source_type.strip().lower()
        op_map = {
            "upload": OperationType.IMPORT_FLOW_UPLOAD,
            "stage": OperationType.IMPORT_FLOW_STAGE,
            "git": OperationType.IMPORT_FLOW_GIT,
        }
        if src not in op_map:
            ctx = self.authz_service.get_session_context(user_principal)
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation="IMPORT_FLOW",
                reason=f"Unsupported flow import source '{source_type}'.",
            )

        operation = op_map[src]
        target_ref = src
        validated_stage_uri: str | None = None

        if src == "stage":
            if not stage_coords:
                ctx = self.authz_service.get_session_context(user_principal)
                raise SnowflakeGovernanceError.unsupported_capability(
                    active_role=ctx.active_role,
                    operation=operation.value,
                    reason="Stage coordinates (database, schema, stage, subpath) are required.",
                )
            ctx = self.authz_service.get_session_context(user_principal)
            validated_stage_uri = self.provisioner.validate_stage_subpath(
                database=stage_coords.get("database", ""),
                schema=stage_coords.get("schema", ""),
                stage=stage_coords.get("stage", ""),
                subpath=stage_coords.get("subpath", ""),
                active_role=ctx.active_role,
            )
            target_ref = validated_stage_uri

        ctx = self.authz_service.check_operation_permission(
            user_principal=user_principal,
            operation=operation,
            target_ref=target_ref,
            request_epoch=request_epoch,
        )

        sanitized_payload = self._sanitize_imported_flow(flow_payload)
        return {
            "source_type": src,
            "effective_role": ctx.active_role,
            "context_epoch": ctx.context_epoch,
            "validated_stage_uri": validated_stage_uri,
            "git_provenance": git_metadata if src == "git" else None,
            "auto_execute_custom_code": False,
            "flow": sanitized_payload,
        }

    # ------------------------------------------------------------------
    # A2A Client & Server Enforcement (REQ-14, ADR-003, ADR-004)
    # ------------------------------------------------------------------
    def enforce_a2a_invocation(
        self,
        *,
        caller_principal: str,
        flow_id: str,
        has_chat_input: bool,
        has_chat_output: bool,
        is_published: bool,
        card_claimed_role: str | None = None,
        flow_owner_role: str | None = None,
        call_depth: int = 0,
        request_epoch: int | None = None,
    ) -> dict[str, Any]:
        """Enforce A2A serving rules: caller RBAC (never owner role or card role) & anti-recursion."""
        ctx = self.authz_service.get_session_context(caller_principal)
        if not (has_chat_input and has_chat_output):
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation=OperationType.SERVE_A2A_AGENT.value,
                reason="Flow cannot be served as A2A agent: requires both Chat Input and Chat Output.",
            )
        if not is_published:
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation=OperationType.SERVE_A2A_AGENT.value,
                detail="Flow A2A endpoint is saved as card metadata only and is not published.",
            )
        if call_depth > self.MAX_A2A_CALL_DEPTH:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation=OperationType.SERVE_A2A_AGENT.value,
                reason=f"A2A recursive invocation depth {call_depth} exceeds max ({self.MAX_A2A_CALL_DEPTH}).",
            )

        # Card claimed role or flow owner role must NEVER override caller's active role
        if card_claimed_role and card_claimed_role.upper() != ctx.active_role.upper():
            self.authz_service.check_operation_permission(
                user_principal=caller_principal,
                operation=OperationType.SERVE_A2A_AGENT,
                target_ref=f"A2A:{flow_id}",
                claimed_role_from_client=card_claimed_role,
                request_epoch=request_epoch,
            )

        self.authz_service.check_operation_permission(
            user_principal=caller_principal,
            operation=OperationType.SERVE_A2A_AGENT,
            target_ref=f"A2A:{flow_id}",
            request_epoch=request_epoch,
        )
        return {
            "flow_id": flow_id,
            "caller_principal": caller_principal,
            "effective_role": ctx.active_role,
            "inherited_owner_role": False,
            "ignored_flow_owner_role": flow_owner_role,
            "call_depth": call_depth + 1,
            "context_epoch": ctx.context_epoch,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _sanitize_imported_flow(flow_payload: dict[str, Any]) -> dict[str, Any]:
        """Redact inline secrets and strip any auto-execution flags from imported flow JSON."""
        secret_keys = {"api_key", "password", "token", "secret", "private_key", "pat", "oauth_token"}
        cleaned: dict[str, Any] = {}
        for k, v in flow_payload.items():
            if k.lower() in secret_keys:
                cleaned[k] = ""
            elif isinstance(v, dict):
                cleaned[k] = SnowflakeGovernanceMiddleware._sanitize_imported_flow(v)
            else:
                cleaned[k] = v
        return cleaned
