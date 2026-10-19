"""Governance, Role Context, Error Taxonomy, and Object Provisioning Models.

Covers ADR-003 (Real Role Selector, SPCS Caller Rights limit, Dual Identity,
Subsystem Identity Separation), ADR-004 (Universal Backend Enforcement &
4-Code Error Taxonomy), and ADR-005 (Governed Object Creation Matrix).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class SnowflakeErrorCode(str, Enum):
    """Mandatory 4-code taxonomy for governance, capability, and execution errors (ADR-004)."""

    AUTHZ_ROLE_DENIED = "AUTHZ_ROLE_DENIED"
    OBJECT_NOT_FOUND_OR_UNAUTHORIZED = "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"
    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"
    TECHNICAL_EXECUTION_ERROR = "TECHNICAL_EXECUTION_ERROR"


class ExecutionIdentityType(str, Enum):
    """Dual identity model separating interactive callers from technical automations (ADR-003)."""

    INTERACTIVE_CALLER = "INTERACTIVE_CALLER"
    TECHNICAL_AUTOMATION = "TECHNICAL_AUTOMATION"


class SubsystemBoundary(str, Enum):
    """Explicit identity boundaries across Snowflake SQL, Postgres :5432, Proxy OAuth, and Cortex Search."""

    SNOWFLAKE_SQL = "SNOWFLAKE_SQL"
    SNOWFLAKE_POSTGRES = "SNOWFLAKE_POSTGRES"
    LLM_PROXY_OAUTH = "LLM_PROXY_OAUTH"
    CORTEX_SEARCH_OWNER_RIGHTS = "CORTEX_SEARCH_OWNER_RIGHTS"


class OperationType(str, Enum):
    """Granular operation types across UI, REST API, Flow execution, Tool Mode, and A2A ([G-A4])."""

    SWITCH_ROLE = "SWITCH_ROLE"
    UPDATE_SETTINGS_MODEL_PROVIDERS = "UPDATE_SETTINGS_MODEL_PROVIDERS"
    UPDATE_SETTINGS_DB_PROVIDERS = "UPDATE_SETTINGS_DB_PROVIDERS"
    UPDATE_SETTINGS_MESSAGES = "UPDATE_SETTINGS_MESSAGES"
    UPDATE_SETTINGS_GLOBAL_VARIABLES = "UPDATE_SETTINGS_GLOBAL_VARIABLES"
    IMPORT_FLOW_UPLOAD = "IMPORT_FLOW_UPLOAD"
    IMPORT_FLOW_STAGE = "IMPORT_FLOW_STAGE"
    IMPORT_FLOW_GIT = "IMPORT_FLOW_GIT"
    BROWSE_CATALOG = "BROWSE_CATALOG"
    QUERY_DATA_SOURCE = "QUERY_DATA_SOURCE"
    INVOKE_CORTEX_ANALYST = "INVOKE_CORTEX_ANALYST"
    INVOKE_PROXY_MODEL = "INVOKE_PROXY_MODEL"
    INVOKE_EMBEDDING_MODEL = "INVOKE_EMBEDDING_MODEL"
    INVOKE_AI_FUNCTION = "INVOKE_AI_FUNCTION"
    READ_STAGE_FILE = "READ_STAGE_FILE"
    WRITE_STAGE_FILE = "WRITE_STAGE_FILE"
    USE_EXISTING_VECTOR_STORE = "USE_EXISTING_VECTOR_STORE"
    CREATE_VECTOR_MEMORY_RESOURCE = "CREATE_VECTOR_MEMORY_RESOURCE"
    USE_MESSAGE_HISTORY = "USE_MESSAGE_HISTORY"
    RUN_PYTHON_INTERPRETER = "RUN_PYTHON_INTERPRETER"
    EVALUATE_TOOLGUARD = "EVALUATE_TOOLGUARD"
    INVOKE_A2A_CLIENT = "INVOKE_A2A_CLIENT"
    SERVE_A2A_AGENT = "SERVE_A2A_AGENT"
    READ_FLOW_TRACES = "READ_FLOW_TRACES"
    PROVISION_SNOWFLAKE_OBJECT = "PROVISION_SNOWFLAKE_OBJECT"


class ObjectFamily(str, Enum):
    """Supported families for governed object creation from Langflow (ADR-005, REQ-16)."""

    POSTGRES_INSTANCE = "POSTGRES_INSTANCE"
    POSTGRES_INTERNAL_OBJECT = "POSTGRES_INTERNAL_OBJECT"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    STANDARD_TABLE = "STANDARD_TABLE"
    HYBRID_TABLE = "HYBRID_TABLE"
    ICEBERG_TABLE = "ICEBERG_TABLE"
    INTERACTIVE_TABLE = "INTERACTIVE_TABLE"
    DYNAMIC_TABLE = "DYNAMIC_TABLE"
    INTERNAL_STAGE = "INTERNAL_STAGE"
    EXTERNAL_STAGE = "EXTERNAL_STAGE"


CANONICAL_ROLE_DENIED_MESSAGE = "Il ruolo attivo non consente questa operazione"


@dataclass(frozen=True)
class ExplainPrivilegeFinding:
    """Result of a zero-credit pre-check via EXPLAIN_PRIVILEGES(..., missing_only => true)."""

    authorized: bool
    missing_privileges: tuple[str, ...] = ()
    object_type: str | None = None
    disclosable_privilege: str | None = None
    raw_status: str = "AUTHORIZED"


@dataclass
class RoleSessionContext:
    """Per-role segregated session context with monotonic context_epoch (ADR-003, ADR-004)."""

    user_principal: str
    active_role: str
    available_roles: tuple[str, ...]
    identity_type: ExecutionIdentityType = ExecutionIdentityType.INTERACTIVE_CALLER
    context_epoch: int = 1
    project_id: str = "default_project"
    session_token_scope: str = ""
    verified_current_role: str = ""
    secondary_roles_mode: str = "NONE"
    postgres_role: str | None = None
    established_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def __post_init__(self) -> None:
        if not self.session_token_scope:
            self.session_token_scope = f"session:role:{self.active_role}"
        if not self.verified_current_role:
            self.verified_current_role = self.active_role
        if self.secondary_roles_mode.upper() == "ALL":
            raise SnowflakeGovernanceError.role_denied(
                active_role=self.active_role,
                operation="SESSION_INIT_SECONDARY_ROLES_ALL",
                required_privilege=None,
                detail="SECONDARY ROLES ALL is strictly prohibited by governance policy (ADR-003).",
            )


class SnowflakeGovernanceError(Exception):
    """Structured exception enforcing the 4-code taxonomy and anti-leak rules (ADR-004)."""

    def __init__(
        self,
        *,
        error_code: SnowflakeErrorCode,
        http_status: int,
        message: str,
        active_role: str,
        operation: str,
        required_privilege: str | None = None,
        disclosable: bool = False,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.http_status = http_status
        self.message = message
        self.active_role = active_role
        self.operation = operation
        self.required_privilege = required_privilege if disclosable else None
        self.disclosable = disclosable
        self.subsystem = subsystem
        self.detail = detail

    @classmethod
    def role_denied(
        cls,
        *,
        active_role: str,
        operation: str,
        required_privilege: str | None = None,
        disclosable: bool = False,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
        detail: str | None = None,
    ) -> "SnowflakeGovernanceError":
        """Create an AUTHZ_ROLE_DENIED (HTTP 403) error without exposing hidden object names."""
        exposed_priv = required_privilege if disclosable else None
        msg = (
            f"{CANONICAL_ROLE_DENIED_MESSAGE} "
            f"(ruolo attivo: '{active_role}', operazione: '{operation}')"
        )
        if exposed_priv:
            msg = f"{msg}. Privilegio richiesto: {exposed_priv}"
        return cls(
            error_code=SnowflakeErrorCode.AUTHZ_ROLE_DENIED,
            http_status=403,
            message=msg,
            active_role=active_role,
            operation=operation,
            required_privilege=exposed_priv,
            disclosable=disclosable,
            subsystem=subsystem,
            detail=detail,
        )

    @classmethod
    def object_not_found_or_unauthorized(
        cls,
        *,
        active_role: str,
        operation: str,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
    ) -> "SnowflakeGovernanceError":
        """Create OBJECT_NOT_FOUND_OR_UNAUTHORIZED (HTTP 403) preserving existence ambiguity."""
        return cls(
            error_code=SnowflakeErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED,
            http_status=403,
            message=(
                "L'oggetto richiesto non esiste o il ruolo attivo "
                f"('{active_role}') non dispone dei privilegi per accedervi."
            ),
            active_role=active_role,
            operation=operation,
            required_privilege=None,
            disclosable=False,
            subsystem=subsystem,
        )

    @classmethod
    def unsupported_capability(
        cls,
        *,
        active_role: str,
        operation: str,
        reason: str,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
    ) -> "SnowflakeGovernanceError":
        """Create UNSUPPORTED_CAPABILITY (HTTP 422) for unsupported combinations."""
        return cls(
            error_code=SnowflakeErrorCode.UNSUPPORTED_CAPABILITY,
            http_status=422,
            message=f"Capacità non supportata o non qualificata per '{operation}': {reason}",
            active_role=active_role,
            operation=operation,
            required_privilege=None,
            disclosable=False,
            subsystem=subsystem,
            detail=reason,
        )

    @classmethod
    def technical_execution_error(
        cls,
        *,
        active_role: str,
        operation: str,
        reason: str,
        http_status: int = 502,
        subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL,
    ) -> "SnowflakeGovernanceError":
        """Create TECHNICAL_EXECUTION_ERROR (HTTP 500/502/504) without misattributing to role."""
        return cls(
            error_code=SnowflakeErrorCode.TECHNICAL_EXECUTION_ERROR,
            http_status=http_status,
            message=f"Errore tecnico durante l'esecuzione di '{operation}': {reason}",
            active_role=active_role,
            operation=operation,
            required_privilege=None,
            disclosable=False,
            subsystem=subsystem,
            detail=reason,
        )

    def to_http_payload(self) -> dict[str, Any]:
        """Serialize to structured JSON response for Langflow UI banners/dialogs."""
        payload: dict[str, Any] = {
            "error_code": self.error_code.value,
            "http_status": self.http_status,
            "active_role": self.active_role,
            "operation": self.operation,
            "required_privilege": self.required_privilege,
            "subsystem": self.subsystem.value,
            "message": self.message,
            "base_message": CANONICAL_ROLE_DENIED_MESSAGE
            if self.error_code == SnowflakeErrorCode.AUTHZ_ROLE_DENIED
            else self.message,
        }
        if self.detail and self.error_code != SnowflakeErrorCode.OBJECT_NOT_FOUND_OR_UNAUTHORIZED:
            payload["detail"] = self.detail
        return payload


@dataclass(frozen=True)
class ProvisioningPreview:
    """Dry-run / preview contract returned before governed object creation (ADR-005)."""

    object_family: ObjectFamily
    destination_fqn: str
    hierarchy_path: tuple[str, ...]
    owner_role: str
    required_privileges: tuple[str, ...]
    ddl_statements: tuple[str, ...]
    billable_impact: str
    requires_confirmation: bool = True
    warnings: tuple[str, ...] = ()
    subsystem: SubsystemBoundary = SubsystemBoundary.SNOWFLAKE_SQL

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_family": self.object_family.value,
            "destination_fqn": self.destination_fqn,
            "hierarchy_path": list(self.hierarchy_path),
            "owner_role": self.owner_role,
            "required_privileges": list(self.required_privileges),
            "ddl_statements": list(self.ddl_statements),
            "billable_impact": self.billable_impact,
            "requires_confirmation": self.requires_confirmation,
            "warnings": list(self.warnings),
            "subsystem": self.subsystem.value,
        }


@dataclass(frozen=True)
class ProvisioningExecutionResult:
    """Result of executing a governed object creation after explicit user confirmation."""

    object_family: ObjectFamily
    destination_fqn: str
    owner_role: str
    executed_statements: tuple[str, ...]
    audit_event_id: str
    status: str = "CREATED"
    query_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_family": self.object_family.value,
            "destination_fqn": self.destination_fqn,
            "owner_role": self.owner_role,
            "executed_statements": list(self.executed_statements),
            "audit_event_id": self.audit_event_id,
            "status": self.status,
            "query_ids": list(self.query_ids),
        }
