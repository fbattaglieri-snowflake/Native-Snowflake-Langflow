"""Backend Identity, Active Role Session, and Authorization Service (ADR-003, ADR-004).

Enforces real Snowflake role switching with per-role sessions verified via
`SELECT CURRENT_ROLE()`, monotonic `context_epoch` cache/pool invalidation,
zero-credit `EXPLAIN_PRIVILEGES(..., missing_only => true)` pre-checks,
dual identity (INTERACTIVE_CALLER vs TECHNICAL_AUTOMATION), and explicit
subsystem boundaries across Snowflake SQL, Snowflake Postgres (:5432),
Standard LLM Proxy OAuth, and Cortex Search (owner's rights).
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from lfx_snowflake.governance.models import (
    CANONICAL_ROLE_DENIED_MESSAGE,
    ExecutionIdentityType,
    ExplainPrivilegeFinding,
    OperationType,
    RoleSessionContext,
    SnowflakeErrorCode,
    SnowflakeGovernanceError,
    SubsystemBoundary,
)


class RoleScopedSqlAdapter(Protocol):
    """Protocol for role-scoped Snowflake SQL session execution (SPCS caller/OAuth)."""

    def fetch_available_roles(self, user_principal: str) -> list[str]:
        """Execute `SELECT CURRENT_AVAILABLE_ROLES()` for the authenticated user."""

    def open_role_session(
        self,
        user_principal: str,
        target_role: str,
        token_scope: str,
    ) -> dict[str, Any]:
        """Negotiate a role-scoped session (`session:role:<ROLE>`) and verify `SELECT CURRENT_ROLE()`."""

    def explain_privileges(
        self,
        user_principal: str,
        active_role: str,
        sql_statement: str,
        missing_only: bool = True,
    ) -> ExplainPrivilegeFinding:
        """Run `EXPLAIN_PRIVILEGES(..., missing_only => true)` without consuming compute credits."""


@dataclass
class AuditEventRecord:
    """Structured audit record destined for `<DB>.<SCHEMA>.LANGFLOW_AUDIT_LOG`."""

    event_id: str
    timestamp: str
    authenticated_user: str
    effective_role: str
    identity_type: str
    context_epoch: int
    operation: str
    target_ref: str
    status: str
    error_code: str | None = None
    subsystem: str = SubsystemBoundary.SNOWFLAKE_SQL.value
    project_id: str = "default_project"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "authenticated_user": self.authenticated_user,
            "effective_role": self.effective_role,
            "identity_type": self.identity_type,
            "context_epoch": self.context_epoch,
            "operation": self.operation,
            "target_ref": self.target_ref,
            "status": self.status,
            "error_code": self.error_code,
            "subsystem": self.subsystem,
            "project_id": self.project_id,
            "metadata": self.metadata,
        }


class InMemoryRoleScopedSqlAdapter:
    """Deterministic role-scoped SQL adapter for unit/contract testing and offline validation."""

    def __init__(
        self,
        *,
        user_roles: dict[str, list[str]] | None = None,
        role_privileges: dict[str, set[str]] | None = None,
        service_owner_role: str = "SERVICE_OWNER_ROLE",
    ) -> None:
        self.user_roles: dict[str, list[str]] = {
            user: list(roles) for user, roles in (user_roles or {}).items()
        }
        self.role_privileges: dict[str, set[str]] = {
            role.upper(): {p.upper() for p in privs}
            for role, privs in (role_privileges or {}).items()
        }
        self.service_owner_role = service_owner_role.upper()
        self.executed_queries: list[dict[str, Any]] = []

    def fetch_available_roles(self, user_principal: str) -> list[str]:
        self.executed_queries.append(
            {
                "user": user_principal,
                "sql": "SELECT CURRENT_AVAILABLE_ROLES()",
            }
        )
        roles = self.user_roles.get(user_principal, [])
        return list(roles)

    def open_role_session(
        self,
        user_principal: str,
        target_role: str,
        token_scope: str,
    ) -> dict[str, Any]:
        normalized_target = target_role.upper()
        available = [r.upper() for r in self.fetch_available_roles(user_principal)]
        if normalized_target not in available:
            raise SnowflakeGovernanceError.role_denied(
                active_role=normalized_target,
                operation=OperationType.SWITCH_ROLE.value,
                required_privilege=f"ROLE:{normalized_target}",
                disclosable=False,
            )
        self.executed_queries.append(
            {
                "user": user_principal,
                "role": normalized_target,
                "token_scope": token_scope,
                "sql": "SELECT CURRENT_ROLE()",
            }
        )
        return {
            "current_role": normalized_target,
            "token_scope": token_scope,
            "secondary_roles": "NONE",
        }

    def explain_privileges(
        self,
        user_principal: str,
        active_role: str,
        sql_statement: str,
        missing_only: bool = True,
    ) -> ExplainPrivilegeFinding:
        self.executed_queries.append(
            {
                "user": user_principal,
                "role": active_role.upper(),
                "sql": f"CALL SYSTEM$EXPLAIN_PRIVILEGES($$ {sql_statement} $$, missing_only => {str(missing_only).lower()})",
            }
        )
        granted = self.role_privileges.get(active_role.upper(), set())
        required = self._infer_required_privileges(sql_statement)
        missing = tuple(sorted(p for p in required if p not in granted and "ALL" not in granted))
        return ExplainPrivilegeFinding(
            authorized=len(missing) == 0,
            missing_privileges=missing,
            disclosable_privilege=missing[0] if missing else None,
            raw_status="AUTHORIZED" if not missing else "MISSING_PRIVILEGES",
        )

    @staticmethod
    def _infer_required_privileges(sql_statement: str) -> set[str]:
        upper = sql_statement.strip().upper()
        required: set[str] = set()
        if upper.startswith("CREATE POSTGRES INSTANCE"):
            required.add("CREATE POSTGRES INSTANCE ON ACCOUNT")
        elif upper.startswith("CREATE DATABASE"):
            required.add("CREATE DATABASE ON ACCOUNT")
        elif upper.startswith("CREATE SCHEMA"):
            required.add("CREATE SCHEMA ON DATABASE")
        elif upper.startswith("CREATE HYBRID TABLE"):
            required.add("USAGE ON DATABASE")
            required.add("USAGE ON SCHEMA")
            required.add("CREATE HYBRID TABLE ON SCHEMA")
        elif upper.startswith("CREATE ICEBERG TABLE"):
            required.add("USAGE ON DATABASE")
            required.add("USAGE ON SCHEMA")
            required.add("USAGE ON EXTERNAL VOLUME")
            required.add("CREATE ICEBERG TABLE ON SCHEMA")
        elif upper.startswith("CREATE INTERACTIVE TABLE"):
            required.add("USAGE ON DATABASE")
            required.add("USAGE ON SCHEMA")
            required.add("USAGE ON WAREHOUSE")
            required.add("CREATE INTERACTIVE TABLE ON SCHEMA")
            required.add("SELECT ON SOURCE TABLE")
        elif upper.startswith("CREATE DYNAMIC TABLE"):
            required.add("USAGE ON DATABASE")
            required.add("USAGE ON SCHEMA")
            required.add("USAGE ON WAREHOUSE")
            required.add("CREATE DYNAMIC TABLE ON SCHEMA")
            required.add("SELECT ON SOURCE TABLE")
        elif upper.startswith("CREATE TABLE"):
            required.add("USAGE ON DATABASE")
            required.add("USAGE ON SCHEMA")
            required.add("CREATE TABLE ON SCHEMA")
        elif upper.startswith("CREATE STAGE"):
            required.add("USAGE ON DATABASE")
            required.add("USAGE ON SCHEMA")
            required.add("CREATE STAGE ON SCHEMA")
            if "STORAGE_INTEGRATION" in upper:
                required.add("USAGE ON INTEGRATION")
        elif "AI_" in upper:
            required.add("SNOWFLAKE.CORTEX_USER")
            required.add("SELECT ON SOURCE TABLE")
        elif upper.startswith("SELECT"):
            required.add("SELECT ON TABLE")
        return required


class SnowflakeAuthzService:
    """Central governance and role session service for SnowflakeLangflow (ADR-003, ADR-004)."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")

    def __init__(
        self,
        *,
        sql_adapter: RoleScopedSqlAdapter | None = None,
        service_owner_role: str = "SERVICE_OWNER_ROLE",
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
        fail_closed_on_audit_error: bool = True,
    ) -> None:
        self.sql_adapter: RoleScopedSqlAdapter = sql_adapter or InMemoryRoleScopedSqlAdapter(
            service_owner_role=service_owner_role
        )
        self.service_owner_role = service_owner_role.upper()
        self._audit_sink = audit_sink
        self.fail_closed_on_audit_error = fail_closed_on_audit_error

        self._sessions: dict[str, RoleSessionContext] = {}
        self._connection_pool: dict[tuple[str, str], dict[str, Any]] = {}
        self._explain_cache: dict[tuple[str, str, int, str], ExplainPrivilegeFinding] = {}
        self._artifact_cache: dict[tuple[str, str, int, str], Any] = {}
        self._role_operation_matrix: dict[str, set[str]] = {}
        self._postgres_role_grants: dict[tuple[str, str], set[str]] = {}
        self._automation_role_bindings: dict[tuple[str, str], str] = {}
        self.audit_log: list[AuditEventRecord] = []

    # ------------------------------------------------------------------
    # Configuration helpers for RBAC policies & subsystem mappings
    # ------------------------------------------------------------------
    def configure_role_permissions(
        self,
        role: str,
        allowed_operations: set[str] | list[str],
    ) -> None:
        """Register backend operation permissions for a Snowflake role."""
        self._role_operation_matrix[role.upper()] = {op.upper() for op in allowed_operations}

    def configure_postgres_grant(
        self,
        user_principal: str,
        pg_role: str,
        allowed_privileges: set[str] | list[str],
    ) -> None:
        """Register explicit PostgreSQL (:5432) role grants separate from Snowflake UI role."""
        self._postgres_role_grants[(user_principal, pg_role)] = {
            p.upper() for p in allowed_privileges
        }

    def bind_automation_role(
        self,
        project_id: str,
        flow_id: str,
        technical_role: str,
    ) -> None:
        """Bind a dedicated least-privilege technical role for M2M/scheduled automation."""
        normalized = technical_role.upper()
        if normalized == self.service_owner_role:
            raise SnowflakeGovernanceError.role_denied(
                active_role=normalized,
                operation="BIND_AUTOMATION_ROLE",
                detail="Technical automation cannot bind the unrestricted service owner role.",
            )
        self._automation_role_bindings[(project_id, flow_id)] = normalized

    # ------------------------------------------------------------------
    # Role discovery & per-role session lifecycle (ADR-003)
    # ------------------------------------------------------------------
    @staticmethod
    def parse_available_roles_Result(raw_value: str | list[str]) -> list[str]:
        """Parse output of `SELECT CURRENT_AVAILABLE_ROLES()` into normalized role names."""
        if isinstance(raw_value, list):
            return [str(r).strip().upper() for r in raw_value if str(r).strip()]
        text = (raw_value or "").strip()
        if not text:
            return []
        parsed = json.loads(text)
        if not isinstance(parsed, list):
            raise ValueError("CURRENT_AVAILABLE_ROLES() payload must be a JSON array")
        return [str(r).strip().upper() for r in parsed if str(r).strip()]

    def discover_available_roles(self, user_principal: str) -> list[str]:
        """Discover roles actually granted to the authenticated user via `CURRENT_AVAILABLE_ROLES()`."""
        roles = [r.upper() for r in self.sql_adapter.fetch_available_roles(user_principal)]
        # Never expose or allow silent fallback to service_owner_role unless explicitly granted
        return roles

    def establish_interactive_session(
        self,
        user_principal: str,
        requested_role: str | None = None,
        project_id: str = "default_project",
        secondary_roles_mode: str = "NONE",
    ) -> RoleSessionContext:
        """Establish or renegotiate a per-role interactive session verified via `SELECT CURRENT_ROLE()`."""
        if secondary_roles_mode.upper() == "ALL":
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=requested_role or "UNKNOWN",
                identity_type=ExecutionIdentityType.INTERACTIVE_CALLER,
                context_epoch=self._next_epoch(user_principal),
                operation="SESSION_ESTABLISH",
                target_ref="SECONDARY_ROLES_ALL",
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                project_id=project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=requested_role or "UNKNOWN",
                operation="SESSION_ESTABLISH",
                detail="USE SECONDARY ROLES ALL is prohibited (ADR-003).",
            )

        available_roles = self.discover_available_roles(user_principal)
        if not available_roles:
            self._record_audit(
                authenticated_user=user_principal,
                effective_role="NONE",
                identity_type=ExecutionIdentityType.INTERACTIVE_CALLER,
                context_epoch=self._next_epoch(user_principal),
                operation=OperationType.SWITCH_ROLE.value,
                target_ref="NO_AVAILABLE_ROLES",
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                project_id=project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role="NONE",
                operation=OperationType.SWITCH_ROLE.value,
            )

        target_role = (requested_role or available_roles[0]).strip().upper()
        if not self._IDENTIFIER_RE.match(target_role):
            raise SnowflakeGovernanceError.role_denied(
                active_role=target_role,
                operation=OperationType.SWITCH_ROLE.value,
                detail="Invalid role identifier syntax.",
            )

        # Strictly forbid silent fallback to service owner role or ungranted role
        if target_role not in available_roles:
            epoch = self._next_epoch(user_principal)
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=target_role,
                identity_type=ExecutionIdentityType.INTERACTIVE_CALLER,
                context_epoch=epoch,
                operation=OperationType.SWITCH_ROLE.value,
                target_ref=f"ROLE:{target_role}",
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                project_id=project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=target_role,
                operation=OperationType.SWITCH_ROLE.value,
            )

        token_scope = f"session:role:{target_role}"
        conn_info = self.sql_adapter.open_role_session(
            user_principal=user_principal,
            target_role=target_role,
            token_scope=token_scope,
        )
        verified_role = str(conn_info.get("current_role", "")).upper()
        if verified_role != target_role or verified_role == self.service_owner_role and target_role != self.service_owner_role:
            raise SnowflakeGovernanceError.role_denied(
                active_role=target_role,
                operation=OperationType.SWITCH_ROLE.value,
                detail="SELECT CURRENT_ROLE() verification mismatch on role-scoped session.",
            )

        new_epoch = self._next_epoch(user_principal)
        self._invalidate_user_state(user_principal)

        ctx = RoleSessionContext(
            user_principal=user_principal,
            active_role=target_role,
            available_roles=tuple(available_roles),
            identity_type=ExecutionIdentityType.INTERACTIVE_CALLER,
            context_epoch=new_epoch,
            project_id=project_id,
            session_token_scope=token_scope,
            verified_current_role=verified_role,
            secondary_roles_mode="NONE",
        )
        self._sessions[user_principal] = ctx
        self._connection_pool[(user_principal, target_role)] = {
            "context_epoch": new_epoch,
            "verified_current_role": verified_role,
            "token_scope": token_scope,
            "spcs_caller_renegotiated": True,
        }
        self._record_audit(
            authenticated_user=user_principal,
            effective_role=target_role,
            identity_type=ExecutionIdentityType.INTERACTIVE_CALLER,
            context_epoch=new_epoch,
            operation=OperationType.SWITCH_ROLE.value,
            target_ref=f"ROLE:{target_role}",
            status="SUCCESS",
            project_id=project_id,
        )
        return ctx

    def switch_role(
        self,
        user_principal: str,
        target_role: str,
        project_id: str | None = None,
    ) -> RoleSessionContext:
        """Switch active role (`POST /api/v1/snowflake/role`) by opening a new role-scoped session.

        Note: Under SPCS caller's rights, `USE ROLE` on an existing connection is not
        supported; this method renegotiates a role-scoped session (`session:role:<ROLE>`),
        verifies `SELECT CURRENT_ROLE()`, increments `context_epoch`, and purges all caches.
        """
        existing = self._sessions.get(user_principal)
        resolved_project = project_id or (existing.project_id if existing else "default_project")
        return self.establish_interactive_session(
            user_principal=user_principal,
            requested_role=target_role,
            project_id=resolved_project,
        )

    def revoke_role(self, user_principal: str, revoked_role: str) -> int:
        """Handle role revocation: increment `context_epoch` and invalidate pools/caches immediately."""
        normalized_revoked = revoked_role.upper()
        if isinstance(self.sql_adapter, InMemoryRoleScopedSqlAdapter):
            current_list = self.sql_adapter.user_roles.get(user_principal, [])
            self.sql_adapter.user_roles[user_principal] = [
                r for r in current_list if r.upper() != normalized_revoked
            ]
        new_epoch = self._next_epoch(user_principal)
        self._invalidate_user_state(user_principal)
        current_ctx = self._sessions.get(user_principal)
        if current_ctx and current_ctx.active_role.upper() == normalized_revoked:
            self._sessions.pop(user_principal, None)
        elif current_ctx:
            updated_available = tuple(
                r for r in current_ctx.available_roles if r.upper() != normalized_revoked
            )
            current_ctx.available_roles = updated_available
            current_ctx.context_epoch = new_epoch
        self._record_audit(
            authenticated_user=user_principal,
            effective_role=normalized_revoked,
            identity_type=ExecutionIdentityType.INTERACTIVE_CALLER,
            context_epoch=new_epoch,
            operation="REVOKE_ROLE",
            target_ref=f"ROLE:{normalized_revoked}",
            status="REVOKED",
        )
        return new_epoch

    def establish_automation_session(
        self,
        *,
        automation_principal: str,
        project_id: str,
        flow_id: str,
        requested_role: str | None = None,
    ) -> RoleSessionContext:
        """Establish a dedicated technical automation session (ADR-003 Dual Identity).

        Never falls back from interactive to automation or to `service_owner_role`.
        """
        bound_role = self._automation_role_bindings.get((project_id, flow_id))
        if not bound_role:
            raise SnowflakeGovernanceError.role_denied(
                active_role=requested_role or "UNBOUND_AUTOMATION",
                operation="AUTOMATION_SESSION_INIT",
                detail="No dedicated technical role is approved for this (project_id, flow_id).",
            )
        if requested_role and requested_role.upper() != bound_role:
            raise SnowflakeGovernanceError.role_denied(
                active_role=requested_role.upper(),
                operation="AUTOMATION_SESSION_INIT",
                detail="Automation session cannot escalate beyond its bound project/flow role.",
            )
        epoch = self._next_epoch(automation_principal)
        ctx = RoleSessionContext(
            user_principal=automation_principal,
            active_role=bound_role,
            available_roles=(bound_role,),
            identity_type=ExecutionIdentityType.TECHNICAL_AUTOMATION,
            context_epoch=epoch,
            project_id=project_id,
            session_token_scope=f"session:role:{bound_role}",
            verified_current_role=bound_role,
            secondary_roles_mode="NONE",
        )
        self._sessions[automation_principal] = ctx
        return ctx

    def get_session_context(self, user_principal: str) -> RoleSessionContext:
        """Return the current validated session context or fail-closed if missing/revoked."""
        ctx = self._sessions.get(user_principal)
        if ctx is None:
            raise SnowflakeGovernanceError.role_denied(
                active_role="UNAUTHENTICATED_OR_REVOKED",
                operation="GET_SESSION_CONTEXT",
            )
        # Re-verify that active_role is still in available roles (protects against mid-session revocation)
        live_roles = [r.upper() for r in self.sql_adapter.fetch_available_roles(user_principal)]
        if ctx.identity_type == ExecutionIdentityType.INTERACTIVE_CALLER and ctx.active_role.upper() not in live_roles:
            self.revoke_role(user_principal, ctx.active_role)
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation="VERIFY_ACTIVE_ROLE",
            )
        return ctx

    def get_context_endpoint_payload(self, user_principal: str) -> dict[str, Any]:
        """Payload for `GET /api/v1/snowflake/context`."""
        ctx = self.get_session_context(user_principal)
        return {
            "user_principal": ctx.user_principal,
            "active_role": ctx.active_role,
            "available_roles": list(ctx.available_roles),
            "identity_type": ctx.identity_type.value,
            "context_epoch": ctx.context_epoch,
            "project_id": ctx.project_id,
            "verified_current_role": ctx.verified_current_role,
            "secondary_roles_mode": ctx.secondary_roles_mode,
        }

    # ------------------------------------------------------------------
    # Epoch & Cache Isolation (ADR-004)
    # ------------------------------------------------------------------
    def validate_epoch(self, user_principal: str, request_epoch: int) -> RoleSessionContext:
        """Verify that a request or cached artifact matches the current session `context_epoch`."""
        ctx = self.get_session_context(user_principal)
        if request_epoch != ctx.context_epoch:
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation="VALIDATE_CONTEXT_EPOCH",
                target_ref=f"stale_epoch:{request_epoch}!=current:{ctx.context_epoch}",
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                project_id=ctx.project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation="VALIDATE_CONTEXT_EPOCH",
                detail="Stale context_epoch detected after role switch or revocation.",
            )
        return ctx

    def put_scoped_cache(self, user_principal: str, cache_key: str, value: Any) -> None:
        """Store a value in the role- and epoch-segregated cache."""
        ctx = self.get_session_context(user_principal)
        self._artifact_cache[(user_principal, ctx.active_role, ctx.context_epoch, cache_key)] = value

    def get_scoped_cache(
        self,
        user_principal: str,
        cache_key: str,
        expected_epoch: int | None = None,
    ) -> Any | None:
        """Retrieve a cached value only if `(user, active_role, context_epoch)` is current."""
        ctx = self.get_session_context(user_principal)
        if expected_epoch is not None and expected_epoch != ctx.context_epoch:
            return None
        return self._artifact_cache.get(
            (user_principal, ctx.active_role, ctx.context_epoch, cache_key)
        )

    # ------------------------------------------------------------------
    # Universal Backend Enforcement & EXPLAIN_PRIVILEGES (ADR-004)
    # ------------------------------------------------------------------
    def check_operation_permission(
        self,
        *,
        user_principal: str,
        operation: OperationType | str,
        target_ref: str = "*",
        required_privilege: str | None = None,
        disclosable: bool = False,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
        claimed_role_from_client: str | None = None,
        request_epoch: int | None = None,
    ) -> RoleSessionContext:
        """Verify backend authorization at action time; ignore or reject forged client roles."""
        ctx = self.get_session_context(user_principal)
        if request_epoch is not None:
            self.validate_epoch(user_principal, request_epoch)

        op_name = operation.value if isinstance(operation, OperationType) else str(operation).upper()

        # Never trust a role claimed in Global Variables, A2A cards, or client headers
        if claimed_role_from_client and claimed_role_from_client.upper() != ctx.active_role.upper():
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation=op_name,
                target_ref=target_ref,
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                subsystem=subsystem.value,
                project_id=ctx.project_id,
                metadata={"forged_claimed_role": claimed_role_from_client},
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation=op_name,
                subsystem=subsystem,
                detail="Client/variable/A2A claimed role does not match backend active role.",
            )

        allowed_ops = self._role_operation_matrix.get(ctx.active_role.upper(), set())
        if op_name not in allowed_ops and "*" not in allowed_ops:
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation=op_name,
                target_ref=self._sanitize_target_ref_for_audit(target_ref),
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                subsystem=subsystem.value,
                project_id=ctx.project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation=op_name,
                required_privilege=required_privilege,
                disclosable=disclosable,
                subsystem=subsystem,
            )

        return ctx

    def precheck_sql_privileges(
        self,
        *,
        user_principal: str,
        sql_statement: str,
        operation: OperationType | str,
        target_ref: str = "SQL_STATEMENT",
        disclosable: bool = False,
    ) -> ExplainPrivilegeFinding:
        """Run zero-credit `EXPLAIN_PRIVILEGES(..., missing_only => true)` pre-check."""
        ctx = self.get_session_context(user_principal)
        op_name = operation.value if isinstance(operation, OperationType) else str(operation).upper()
        cache_key = (user_principal, ctx.active_role, ctx.context_epoch, sql_statement.strip())

        finding = self._explain_cache.get(cache_key)
        if finding is None:
            finding = self.sql_adapter.explain_privileges(
                user_principal=user_principal,
                active_role=ctx.active_role,
                sql_statement=sql_statement,
                missing_only=True,
            )
            self._explain_cache[cache_key] = finding

        if not finding.authorized:
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation=op_name,
                target_ref=self._sanitize_target_ref_for_audit(target_ref),
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                project_id=ctx.project_id,
                metadata={"missing_privileges_count": len(finding.missing_privileges)},
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation=op_name,
                required_privilege=finding.disclosable_privilege,
                disclosable=disclosable,
            )
        return finding

    # ------------------------------------------------------------------
    # Subsystem Identity Separation (ADR-003, [G-A4])
    # ------------------------------------------------------------------
    def authorize_postgres_operation(
        self,
        *,
        user_principal: str,
        pg_role: str,
        required_pg_privilege: str,
        sslmode: str = "verify-ca",
        connect_timeout: int = 10,
        target_ref: str = "postgres:5432",
    ) -> dict[str, Any]:
        """Verify Snowflake Postgres (:5432) access separately from the Snowflake UI role."""
        ctx = self.get_session_context(user_principal)
        if sslmode != "verify-ca" or connect_timeout <= 0 or connect_timeout > 30:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation="POSTGRES_CONNECTION_POLICY",
                reason="Snowflake Postgres (:5432) requires sslmode='verify-ca' and bounded connect_timeout (<=30s).",
                subsystem=SubsystemBoundary.SNOWFLAKE_POSTGRES,
            )
        granted = self._postgres_role_grants.get((user_principal, pg_role), set())
        if required_pg_privilege.upper() not in granted and "ALL" not in granted:
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=ctx.active_role,
                identity_type=ctx.identity_type,
                context_epoch=ctx.context_epoch,
                operation=f"POSTGRES_{required_pg_privilege.upper()}",
                target_ref=target_ref,
                status="DENIED",
                error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED.value,
                subsystem=SubsystemBoundary.SNOWFLAKE_POSTGRES.value,
                project_id=ctx.project_id,
            )
            raise SnowflakeGovernanceError.role_denied(
                active_role=ctx.active_role,
                operation=f"POSTGRES_{required_pg_privilege.upper()}",
                required_privilege=f"PG_ROLE:{pg_role}:{required_pg_privilege.upper()}",
                disclosable=False,
                subsystem=SubsystemBoundary.SNOWFLAKE_POSTGRES,
            )
        return {
            "snowflake_active_role": ctx.active_role,
            "postgres_role": pg_role,
            "sslmode": sslmode,
            "connect_timeout": connect_timeout,
            "context_epoch": ctx.context_epoch,
        }

    def authorize_proxy_invocation(
        self,
        *,
        user_principal: str,
        protocol: str,
        model_id: str,
    ) -> dict[str, Any]:
        """Enforce caller active-role check before invoking `LLM_PROXY_STANDARD` OAuth endpoint."""
        supported_protocols = {"chat", "responses", "messages", "ollama"}
        normalized_proto = protocol.strip().lower()
        ctx = self.get_session_context(user_principal)
        if normalized_proto not in supported_protocols:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation=OperationType.INVOKE_PROXY_MODEL.value,
                reason=f"Unsupported proxy protocol '{protocol}'.",
                subsystem=SubsystemBoundary.LLM_PROXY_OAUTH,
            )
        self.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.INVOKE_PROXY_MODEL,
            target_ref=f"{normalized_proto}:{model_id}",
            subsystem=SubsystemBoundary.LLM_PROXY_OAUTH,
        )
        return {
            "effective_role": ctx.active_role,
            "context_epoch": ctx.context_epoch,
            "protocol": normalized_proto,
            "model_id": model_id,
        }

    def authorize_cortex_search_query(
        self,
        *,
        user_principal: str,
        service_fqn: str,
        query_filter: dict[str, Any] | None = None,
        use_search_preview: bool = False,
        filter_by_session: bool = True,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Enforce owner's-rights isolation filters on Cortex Search queries (ADR-003, [G-A4])."""
        ctx = self.get_session_context(user_principal)
        if use_search_preview:
            raise SnowflakeGovernanceError.unsupported_capability(
                active_role=ctx.active_role,
                operation="CORTEX_SEARCH_SERVING",
                reason="SEARCH_PREVIEW is not allowed as an end-user serving path.",
                subsystem=SubsystemBoundary.CORTEX_SEARCH_OWNER_RIGHTS,
            )
        self.check_operation_permission(
            user_principal=user_principal,
            operation=OperationType.USE_EXISTING_VECTOR_STORE,
            target_ref=service_fqn,
            subsystem=SubsystemBoundary.CORTEX_SEARCH_OWNER_RIGHTS,
        )
        # Mandatory isolation filter even when Filter by Session = false ([G-A4])
        isolation_clauses: list[dict[str, Any]] = [
            {"@eq": {"authenticated_user": ctx.user_principal}},
            {"@eq": {"project_id": ctx.project_id}},
            {"@eq": {"authorized_role": ctx.active_role}},
        ]
        if filter_by_session and session_id:
            isolation_clauses.append({"@eq": {"session_id": session_id}})
        if query_filter:
            isolation_clauses.append(query_filter)
        return {
            "service_fqn": service_fqn,
            "effective_role": ctx.active_role,
            "context_epoch": ctx.context_epoch,
            "enforced_filter": {"@and": isolation_clauses},
        }

    def authorize_memory_operation(
        self,
        *,
        user_principal: str,
        memory_ref: str,
        is_create_physical_resource: bool,
        filter_by_session: bool = True,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Granularly separate CREATE physical resource from USE existing vector store ([G-A4])."""
        required_op = (
            OperationType.CREATE_VECTOR_MEMORY_RESOURCE
            if is_create_physical_resource
            else OperationType.USE_EXISTING_VECTOR_STORE
        )
        ctx = self.check_operation_permission(
            user_principal=user_principal,
            operation=required_op,
            target_ref=memory_ref,
            required_privilege="CREATE TABLE / CREATE CORTEX SEARCH SERVICE"
            if is_create_physical_resource
            else "USAGE + SELECT/INSERT",
            disclosable=True,
        )
        scope_boundary = {
            "authenticated_user": ctx.user_principal,
            "project_id": ctx.project_id,
            "active_role": ctx.active_role,
            "session_id": session_id if filter_by_session else None,
        }
        return {
            "operation": required_op.value,
            "effective_role": ctx.active_role,
            "context_epoch": ctx.context_epoch,
            "scope_boundary": scope_boundary,
        }

    # ------------------------------------------------------------------
    # Error classification (4-code taxonomy, ADR-004)
    # ------------------------------------------------------------------
    def classify_exception(
        self,
        *,
        user_principal: str,
        operation: OperationType | str,
        exc: Exception,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
    ) -> SnowflakeGovernanceError:
        """Map raw Snowflake/network/capability errors into the 4-code taxonomy without false role blame."""
        if isinstance(exc, SnowflakeGovernanceError):
            return exc

        ctx = self._sessions.get(user_principal)
        active_role = ctx.active_role if ctx else "UNKNOWN"
        op_name = operation.value if isinstance(operation, OperationType) else str(operation).upper()
        msg_lower = str(exc).lower()

        if "does not exist or not authorized" in msg_lower or "002003" in msg_lower:
            err = SnowflakeGovernanceError.object_not_found_or_unauthorized(
                active_role=active_role,
                operation=op_name,
                subsystem=subsystem,
            )
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=active_role,
                identity_type=ctx.identity_type if ctx else ExecutionIdentityType.INTERACTIVE_CALLER,
                context_epoch=ctx.context_epoch if ctx else 0,
                operation=op_name,
                target_ref="REDACTED_OBJECT",
                status="DENIED",
                error_code=err.error_code.value,
                subsystem=subsystem.value,
            )
            return err

        if "insufficient privileges" in msg_lower or "access control error" in msg_lower or "003001" in msg_lower:
            err = SnowflakeGovernanceError.role_denied(
                active_role=active_role,
                operation=op_name,
                subsystem=subsystem,
            )
            self._record_audit(
                authenticated_user=user_principal,
                effective_role=active_role,
                identity_type=ctx.identity_type if ctx else ExecutionIdentityType.INTERACTIVE_CALLER,
                context_epoch=ctx.context_epoch if ctx else 0,
                operation=op_name,
                target_ref="PRIVILEGE_CHECK",
                status="DENIED",
                error_code=err.error_code.value,
                subsystem=subsystem.value,
            )
            return err

        if "unsupported" in msg_lower or "not supported" in msg_lower:
            return SnowflakeGovernanceError.unsupported_capability(
                active_role=active_role,
                operation=op_name,
                reason=str(exc),
                subsystem=subsystem,
            )

        status_code = 504 if "timeout" in msg_lower or "timed out" in msg_lower else 502
        return SnowflakeGovernanceError.technical_execution_error(
            active_role=active_role,
            operation=op_name,
            reason=str(exc),
            http_status=status_code,
            subsystem=subsystem,
        )

    # ------------------------------------------------------------------
    # Internal state & audit helpers
    # ------------------------------------------------------------------
    def _next_epoch(self, user_principal: str) -> int:
        existing = self._sessions.get(user_principal)
        return (existing.context_epoch + 1) if existing else 1

    def _invalidate_user_state(self, user_principal: str) -> None:
        self._connection_pool = {
            k: v for k, v in self._connection_pool.items() if k[0] != user_principal
        }
        self._explain_cache = {
            k: v for k, v in self._explain_cache.items() if k[0] != user_principal
        }
        self._artifact_cache = {
            k: v for k, v in self._artifact_cache.items() if k[0] != user_principal
        }

    @staticmethod
    def _sanitize_target_ref_for_audit(target_ref: str) -> str:
        # Redact any inline credentials or query tokens
        sanitized = re.sub(r"(?i)(bearer|token|password|secret)=\S+", r"\1=[REDACTED]", target_ref)
        return sanitized[:256]

    def _record_audit(
        self,
        *,
        authenticated_user: str,
        effective_role: str,
        identity_type: ExecutionIdentityType,
        context_epoch: int,
        operation: str,
        target_ref: str,
        status: str,
        error_code: str | None = None,
        subsystem: str = SubsystemBoundary.SNOWFLAKE_SQL.value,
        project_id: str = "default_project",
        metadata: dict[str, Any] | None = None,
    ) -> AuditEventRecord:
        record = AuditEventRecord(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            authenticated_user=authenticated_user,
            effective_role=effective_role,
            identity_type=identity_type.value,
            context_epoch=context_epoch,
            operation=operation,
            target_ref=target_ref,
            status=status,
            error_code=error_code,
            subsystem=subsystem,
            project_id=project_id,
            metadata=metadata or {},
        )
        self.audit_log.append(record)
        if self._audit_sink is not None:
            try:
                self._audit_sink(record.to_dict())
            except Exception as exc:
                if self.fail_closed_on_audit_error:
                    raise SnowflakeGovernanceError.technical_execution_error(
                        active_role=effective_role,
                        operation=operation,
                        reason=f"Audit outbox sink failure (fail-closed): {exc}",
                        http_status=500,
                    ) from exc
        return record
