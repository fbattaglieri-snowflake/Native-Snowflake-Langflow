import React from "react";

export type SnowflakeErrorCode =
  | "AUTHZ_ROLE_DENIED"
  | "OBJECT_NOT_FOUND_OR_UNAUTHORIZED"
  | "UNSUPPORTED_CAPABILITY"
  | "TECHNICAL_EXECUTION_ERROR";

export interface SnowflakeDenialPayload {
  error_code: SnowflakeErrorCode;
  active_role: string;
  context_epoch: number;
  operation: string;
  required_privilege?: string | null;
  detail?: string | null;
  message: string;
}

export interface SnowflakeRoleSelectorProps {
  activeRole: string;
  availableRoles: string[];
  contextEpoch: number;
  isSwitching?: boolean;
  onSelectRole: (newRole: string) => Promise<void> | void;
}

/**
 * Active Snowflake Role Selector placed in both the Home topbar and Flow Editor topbar (REQ-02).
 * Functionally inspired by Snowsight's role switcher, rendered with Langflow's native UI styling.
 */
export const SnowflakeRoleSelector: React.FC<SnowflakeRoleSelectorProps> = ({
  activeRole,
  availableRoles,
  contextEpoch,
  isSwitching = false,
  onSelectRole,
}) => {
  return (
    <div
      data-testid="snowflake-role-selector"
      data-context-epoch={contextEpoch}
      className="flex items-center gap-2 rounded-md border border-border bg-muted/40 px-2.5 py-1 text-xs"
    >
      <span className="font-medium text-muted-foreground">Role:</span>
      <select
        aria-label="Active Snowflake Role"
        data-testid="snowflake-role-select-input"
        disabled={isSwitching}
        value={activeRole}
        onChange={(e) => onSelectRole(e.target.value)}
        className="bg-transparent font-semibold text-foreground focus:outline-none"
      >
        {availableRoles.map((role) => (
          <option key={role} value={role}>
            {role}
          </option>
        ))}
      </select>
    </div>
  );
};

export function formatDenialBannerMessage(payload: SnowflakeDenialPayload): string {
  if (payload.error_code === "AUTHZ_ROLE_DENIED") {
    return `Il ruolo attivo (${payload.active_role}) non consente questa operazione`;
  }
  if (payload.error_code === "OBJECT_NOT_FOUND_OR_UNAUTHORIZED") {
    return `Il ruolo attivo (${payload.active_role}) non consente questa operazione: does not exist or not authorized`;
  }
  return payload.message;
}

/**
 * Standardized Langflow Alert/Banner for the 4-code error taxonomy (ADR-004, REQ-02):
 * - AUTHZ_ROLE_DENIED (HTTP 403)
 * - OBJECT_NOT_FOUND_OR_UNAUTHORIZED (HTTP 403/404)
 * - UNSUPPORTED_CAPABILITY (HTTP 422)
 * - TECHNICAL_EXECUTION_ERROR (HTTP 500/502/504)
 */
export const SnowflakeDenialBanner: React.FC<{
  denial: SnowflakeDenialPayload | null;
  onDismiss?: () => void;
}> = ({ denial, onDismiss }) => {
  if (!denial) return null;

  const variantClasses: Record<SnowflakeErrorCode, string> = {
    AUTHZ_ROLE_DENIED: "border-destructive/60 bg-destructive/10 text-destructive",
    OBJECT_NOT_FOUND_OR_UNAUTHORIZED: "border-amber-500/60 bg-amber-500/10 text-amber-700 dark:text-amber-300",
    UNSUPPORTED_CAPABILITY: "border-blue-500/60 bg-blue-500/10 text-blue-700 dark:text-blue-300",
    TECHNICAL_EXECUTION_ERROR: "border-red-500/60 bg-red-500/10 text-red-700 dark:text-red-300",
  };

  return (
    <div
      role="alert"
      data-testid="snowflake-denial-banner"
      data-error-code={denial.error_code}
      data-active-role={denial.active_role}
      className={`flex items-start justify-between gap-4 rounded-md border px-4 py-3 text-xs ${
        variantClasses[denial.error_code]
      }`}
    >
      <div className="flex flex-col gap-1">
        <div className="font-semibold" data-testid="denial-primary-message">
          {formatDenialBannerMessage(denial)}
        </div>
        <div className="text-muted-foreground">
          <span>Operazione: {denial.operation}</span>
          {denial.required_privilege &&
            denial.error_code === "AUTHZ_ROLE_DENIED" && (
              <span className="ml-2">
                | Privilegio richiesto: {denial.required_privilege}
              </span>
            )}
        </div>
      </div>
      {onDismiss && (
        <button
          type="button"
          onClick={onDismiss}
          className="rounded px-1.5 py-0.5 text-xs font-medium hover:bg-muted"
          aria-label="Chiudi avviso"
        >
          Dismiss
        </button>
      )}
    </div>
  );
};
