"""SnowflakeLangflow Governance, Real Role Session, and Object Provisioning Package."""

from lfx_snowflake.governance.authz_service import (
    AuditEventRecord,
    InMemoryRoleScopedSqlAdapter,
    RoleScopedSqlAdapter,
    SnowflakeAuthzService,
)
from lfx_snowflake.governance.middleware import (
    READONLY_SYSTEM_VARIABLES,
    SnowflakeGovernanceMiddleware,
    require_snowflake_permission,
)
from lfx_snowflake.governance.models import (
    CANONICAL_ROLE_DENIED_MESSAGE,
    ExecutionIdentityType,
    ExplainPrivilegeFinding,
    ObjectFamily,
    OperationType,
    ProvisioningExecutionResult,
    ProvisioningPreview,
    RoleSessionContext,
    SnowflakeErrorCode,
    SnowflakeGovernanceError,
    SubsystemBoundary,
)
from lfx_snowflake.governance.provisioner import SnowflakeObjectProvisioner

# Shared contract aliases
ErrorCategory = SnowflakeErrorCode
ExecutionMode = ExecutionIdentityType
SnowflakePermissionError = SnowflakeGovernanceError

__all__ = [
    "AuditEventRecord",
    "CANONICAL_ROLE_DENIED_MESSAGE",
    "ErrorCategory",
    "ExecutionIdentityType",
    "ExecutionMode",
    "ExplainPrivilegeFinding",
    "InMemoryRoleScopedSqlAdapter",
    "ObjectFamily",
    "OperationType",
    "ProvisioningExecutionResult",
    "ProvisioningPreview",
    "READONLY_SYSTEM_VARIABLES",
    "RoleScopedSqlAdapter",
    "RoleSessionContext",
    "SnowflakeAuthzService",
    "SnowflakeErrorCode",
    "SnowflakeGovernanceError",
    "SnowflakeGovernanceMiddleware",
    "SnowflakeObjectProvisioner",
    "SnowflakePermissionError",
    "SubsystemBoundary",
    "require_snowflake_permission",
]
