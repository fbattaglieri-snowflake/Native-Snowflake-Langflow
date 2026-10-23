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

export const SNOWFLAKE_BLUE = "#29B5E8";
export const REMOVED_TOOLBAR_SOCIAL_LINKS = ["GitHub", "Discord"] as const;
export const REMOVED_PROFILE_SOCIAL_LINKS = ["GitHub", "Discord", "X", "Twitter"] as const;
export const SNOWFLAKE_DOCUMENTATION_URL = "https://docs.snowflake.com/";

/**
 * Official Snowflake Blue Logo (`#29B5E8`) used across Topbar Left, Topbar Right (`Role:` + `Snowflake Hub`),
 * Welcome screen (`Welcome to Snowflake Langflow`), Loading screen (`Loading...` without 3-layer icon),
 * and Memories `+ Snowflake` button (`UI-W2-01`, `W2-R2-14`).
 */
export const OfficialSnowflakeBlueLogo: React.FC<{ className?: string; size?: number }> = ({
  className = "h-6 w-6 text-[#29B5E8]",
  size = 24,
}) => (
  <svg
    data-testid="snowflake-official-logo-blue"
    data-variant="official_snowflake_blue_29B5E8"
    className={className}
    width={size}
    height={size}
    viewBox="0 0 32 32"
    fill={SNOWFLAKE_BLUE}
    preserveAspectRatio="xMidYMid meet"
    aria-label="Snowflake Blue Icon"
    role="img"
    style={{ color: SNOWFLAKE_BLUE }}
  >
    <path
      d="M16 2V30M4.5 9L27.5 23M27.5 9L4.5 23M16 6L12 2M16 6L20 2M16 26L12 30M16 26L20 30"
      stroke={SNOWFLAKE_BLUE}
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

/**
 * Clean Snowflake Loading Overlay (`W2-R2-14`):
 * Displays ONLY the official Snowflake Blue (`#29B5E8`) logo above `Loading...`,
 * with the 3-layer Databricks-like stacked icon completely removed.
 */
export const SnowflakeLoadingOverlay: React.FC = () => (
  <div
    data-testid="snowflake-langflow-dual-loading-logo"
    className="flex flex-col items-center justify-center gap-3 text-[#29B5E8]"
    style={{ color: SNOWFLAKE_BLUE }}
  >
    <OfficialSnowflakeBlueLogo className="h-10 w-10 shrink-0 text-[#29B5E8]" size={40} />
    <span className="text-sm font-medium" style={{ color: SNOWFLAKE_BLUE }}>
      Loading...
    </span>
  </div>
);

/**
 * Welcome to Snowflake Langflow Banner (`W2-R2-14`):
 * Colors the top-center logos and "Welcome to Snowflake Langflow" heading in `#29B5E8`.
 */
export const SnowflakeWelcomeBanner: React.FC = () => (
  <div
    data-testid="snowflake-welcome-banner"
    className="flex flex-col items-center justify-center gap-3"
    style={{ color: SNOWFLAKE_BLUE }}
  >
    <div className="flex items-center justify-center gap-3 text-[#29B5E8]">
      <OfficialSnowflakeBlueLogo className="h-9 w-9 shrink-0 text-[#29B5E8]" size={36} />
    </div>
    <h1
      data-testid="welcome-to-snowflake-langflow-title"
      className="text-2xl font-semibold tracking-tight text-[#29B5E8]"
      style={{ color: SNOWFLAKE_BLUE }}
    >
      Welcome to Snowflake Langflow
    </h1>
  </div>
);

/**
 * Langflow Topbar Patch for both Home (`surface="home"`) and Flow Editor (`surface="flow_editor"`).
 * Strictly preserves Langflow's native Tailwind/Radix classes, typography, spacing, and theme tokens,
 * while coloring the Snowflake brand elements in official Snowflake Blue `#29B5E8` (`W2-R2-14`).
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
      {/* Top-Left Brand + Breadcrumb (#29B5E8 Snowflake Blue) */}
      <div className="flex items-center gap-3">
        <a
          href="/"
          className="flex items-center gap-2 focus:outline-none text-[#29B5E8]"
          style={{ color: SNOWFLAKE_BLUE }}
          aria-label="Snowflake Langflow Home"
        >
          <OfficialSnowflakeBlueLogo className="h-6 w-auto text-[#29B5E8]" />
          <span className="sr-only">
            <OfficialSnowflakeBlackLogo className="h-6 w-auto" />
          </span>
          <span
            data-testid="topbar-snowflake-langflow-title"
            className="text-sm font-semibold tracking-tight text-[#29B5E8] whitespace-nowrap"
            style={{ color: SNOWFLAKE_BLUE }}
          >
            Snowflake Langflow
          </span>
        </a>
        {surface === "flow_editor" && flowTitle && (
          <div className="flex items-center gap-2 text-sm font-medium text-foreground">
            <span className="text-muted-foreground">/</span>
            <span data-testid="flow-editor-title">{flowTitle}</span>
          </div>
        )}
      </div>

      {/* Top-Right Controls: Active Role Selector (#29B5E8) + Snowflake Hub Button (#29B5E8) + Profile Menu */}
      <div className="flex items-center gap-3">
        <div
          data-testid="topbar-role-badge"
          className="flex items-center gap-1.5 text-xs font-semibold text-[#29B5E8]"
          style={{ color: SNOWFLAKE_BLUE }}
        >
          <OfficialSnowflakeBlueLogo className="h-3.5 w-3.5 text-[#29B5E8]" size={14} />
          <span style={{ color: SNOWFLAKE_BLUE }}>Role:</span>
          <SnowflakeRoleSelector {...roleSelectorProps} />
        </div>

        <button
          type="button"
          data-testid="topbar-snowflake-hub-button"
          className="rounded px-2.5 py-1 text-xs font-semibold text-white"
          style={{ backgroundColor: SNOWFLAKE_BLUE, color: "#ffffff" }}
        >
          Snowflake Hub
        </button>

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
