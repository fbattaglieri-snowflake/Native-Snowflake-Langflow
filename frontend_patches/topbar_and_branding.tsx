import React from "react";
import { SnowflakeRoleSelector, SnowflakeRoleSelectorProps } from "./role_selector_and_denial_banner";

export interface TopbarBrandingProps {
  surface: "home" | "flow_editor";
  flowTitle?: string;
  roleSelectorProps: SnowflakeRoleSelectorProps;
  keepNativeLangflowDocsLink?: boolean;
}

/**
 * Official Snowflake Black Logo (unmodified, non-recolored, official aspect ratio)
 * Placed top-left in both Home topbar and Flow Editor topbar (REQ-01).
 */
export const OfficialSnowflakeBlackLogo: React.FC<{ className?: string }> = ({
  className = "h-6 w-auto",
}) => (
  <svg
    data-testid="snowflake-official-logo-black"
    data-variant="official_snowflake_black_unmodified"
    className={className}
    viewBox="0 0 32 32"
    fill="none"
    preserveAspectRatio="xMidYMid meet"
    aria-label="Snowflake"
    role="img"
  >
    <path
      d="M16 2V30M4.5 9L27.5 23M27.5 9L4.5 23M16 6L12 2M16 6L20 2M16 26L12 30M16 26L20 30"
      stroke="#000000"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

/**
 * Official Snowflake Color Logo (`#29B5E8`) used for the Snowflake Documentation
 * menu item in the user profile dropdown and Snowflake provider badges (REQ-01).
 */
export const OfficialSnowflakeColorLogo: React.FC<{ className?: string }> = ({
  className = "h-4 w-4",
}) => (
  <svg
    data-testid="snowflake-official-logo-color"
    data-variant="official_snowflake_color"
    className={className}
    viewBox="0 0 32 32"
    fill="none"
    preserveAspectRatio="xMidYMid meet"
    aria-label="Snowflake Color Icon"
    role="img"
  >
    <path
      d="M16 2V30M4.5 9L27.5 23M27.5 9L4.5 23M16 6L12 2M16 6L20 2M16 26L12 30M16 26L20 30"
      stroke="#29B5E8"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

export const REMOVED_TOOLBAR_SOCIAL_LINKS = ["GitHub", "Discord"] as const;
export const REMOVED_PROFILE_SOCIAL_LINKS = ["GitHub", "Discord", "X", "Twitter"] as const;
export const SNOWFLAKE_DOCUMENTATION_URL = "https://docs.snowflake.com/";

/**
 * Langflow Topbar Patch for both Home (`surface="home"`) and Flow Editor (`surface="flow_editor"`).
 * Strictly preserves Langflow's native Tailwind/Radix classes, typography, spacing, and theme tokens.
 */
export const SnowflakeLangflowTopbar: React.FC<TopbarBrandingProps> = ({
  surface,
  flowTitle,
  roleSelectorProps,
  keepNativeLangflowDocsLink = true,
}) => {
  return (
    <header
      data-testid={`langflow-topbar-${surface}`}
      data-ui-style="PRESERVE_LANGFLOW_NATIVE_COMPONENTS_TYPOGRAPHY_SPACING_THEMES"
      className="flex h-12 w-full items-center justify-between border-b border-border bg-background px-4 text-sm text-foreground"
    >
      {/* Top-Left Brand + Breadcrumb */}
      <div className="flex items-center gap-3">
        <a
          href="/"
          className="flex items-center gap-2 focus:outline-none"
          aria-label="Snowflake Langflow Home"
        >
          <OfficialSnowflakeBlackLogo className="h-6 w-auto" />
        </a>
        {surface === "flow_editor" && flowTitle && (
          <div className="flex items-center gap-2 text-sm font-medium text-foreground">
            <span className="text-muted-foreground">/</span>
            <span data-testid="flow-editor-title">{flowTitle}</span>
          </div>
        )}
      </div>

      {/* Top-Right Controls: Active Role Selector + Profile Menu (GitHub/Discord/X removed) */}
      <div className="flex items-center gap-3">
        <SnowflakeRoleSelector {...roleSelectorProps} />

        <nav
          aria-label="User Profile Menu"
          data-testid="user-profile-menu"
          className="flex items-center gap-2"
        >
          <a
            data-testid="profile-menu-snowflake-docs"
            href={SNOWFLAKE_DOCUMENTATION_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 rounded-md px-2.5 py-1.5 text-xs font-medium text-foreground hover:bg-muted"
          >
            <OfficialSnowflakeColorLogo className="h-4 w-4" />
            <span>Snowflake Documentation</span>
          </a>
          {keepNativeLangflowDocsLink && (
            <a
              data-testid="profile-menu-langflow-docs"
              href="https://docs.langflow.org/"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 rounded-md px-2 py-1.5 text-xs text-muted-foreground hover:bg-muted"
            >
              <span>Docs</span>
            </a>
          )}
        </nav>
      </div>
    </header>
  );
};
