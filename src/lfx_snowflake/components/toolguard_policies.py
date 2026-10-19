"""Snowflake ToolGuard / Policies (Beta, ALTK) Component (`REQ-13`, `[G-A6]`).

Extends `Discover more components -> ToolGuard -> Policies (Beta)`:
- Adds the 3 Snowflake Proxy providers (`Snowflake Proxy - Responses`,
  `Snowflake Proxy - Messages`, `Snowflake Proxy - Chat`) and `Ollama (Snowflake Hosted)`
  to the `Language Model` selector while preserving existing upstream providers.
- Preserves `Beta` badge, `Activity` (`Generate` / `Guard`), `Enabled`, `Policies Project`,
  `Tools`, `Policies`, `Language Model`, and `Guarded Tools` output port.
- Leaves all other bundles in `Discover more components` 100% unchanged (`[G-A6]`).
- Enforces strict **fail-closed** behavior when `Enabled = True`: if policy generation,
  model invocation, or runtime guard evaluation fails, unguarded tools are NEVER passed
  through silently and blocked tool invocations never execute their side effects.
- Zero protocol conversion or silent inference retries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping

from .files_and_stage import ErrorCode, SnowflakeComponentError, raise_role_denied

SNOWFLAKE_TOOLGUARD_PROVIDERS: tuple[str, ...] = (
    "Snowflake Proxy - Responses",
    "Snowflake Proxy - Messages",
    "Snowflake Proxy - Chat",
    "Ollama (Snowflake Hosted)",
)

EXISTING_UPSTREAM_TOOLGUARD_PROVIDERS: tuple[str, ...] = (
    "OpenAI",
    "Anthropic",
    "Azure OpenAI",
    "Watsonx",
)

UNCHANGED_DISCOVER_MORE_BUNDLES: tuple[str, ...] = (
    "Agentics",
    "Mem0",
    "Composio",
    "CrewAI",
    "LangWatch",
    "Docling",
    "Cleanlab",
)


class ToolGuardActivity(str, Enum):
    GENERATE = "Generate"
    GUARD = "Guard"


@dataclass(frozen=True)
class ToolGuardModelSelection:
    provider: str
    model_id: str
    supports_toolguard_contract: bool = True

    def validate(self) -> None:
        all_allowed = set(SNOWFLAKE_TOOLGUARD_PROVIDERS) | set(EXISTING_UPSTREAM_TOOLGUARD_PROVIDERS)
        if self.provider not in all_allowed:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Provider '{self.provider}' is not registered for ToolGuard / Policies.",
                http_status=422,
            )
        if not (self.model_id or "").strip():
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "A valid 'model_id' must be selected in ToolGuard Language Model.",
                http_status=400,
            )
        if not self.supports_toolguard_contract:
            raise SnowflakeComponentError(
                ErrorCode.UNSUPPORTED_CAPABILITY,
                f"Model '{self.provider}:{self.model_id}' is not qualified for ALTK ToolGuard policy generation/guarding.",
                http_status=422,
            )


@dataclass(frozen=True)
class PolicySpec:
    policy_id: str
    description: str
    target_tool_name: str
    forbidden_arg_patterns: tuple[str, ...] = ()
    max_numeric_limits: Mapping[str, float] = field(default_factory=dict)
    required_args: tuple[str, ...] = ()


@dataclass
class ToolDefinition:
    name: str
    description: str
    handler: Callable[[dict[str, Any]], Any]
    invocation_count: int = 0

    def invoke(self, args: dict[str, Any]) -> Any:
        self.invocation_count += 1
        return self.handler(args)


class GuardedTool:
    """Wrapper produced on the `Guarded Tools` port that enforces ALTK policies at runtime."""

    def __init__(
        self,
        tool: ToolDefinition,
        *,
        policies: list[PolicySpec],
        guard_enabled: bool,
        active_role: str,
        model_selection: ToolGuardModelSelection,
        guard_evaluator: Callable[[ToolDefinition, dict[str, Any], list[PolicySpec]], None] | None = None,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.tool = tool
        self.name = tool.name
        self.description = tool.description
        self.policies = list(policies)
        self.guard_enabled = guard_enabled
        self.active_role = active_role
        self.model_selection = model_selection
        self._guard_evaluator = guard_evaluator
        self._audit_sink = audit_sink

    def invoke(self, args: dict[str, Any]) -> Any:
        if not self.guard_enabled:
            return self.tool.invoke(args)

        try:
            if self._guard_evaluator is not None:
                self._guard_evaluator(self.tool, args, self.policies)
            self._enforce_deterministic_policies(args)
        except SnowflakeComponentError:
            raise
        except Exception as exc:
            # Fail-closed: any unexpected error inside the guard evaluator blocks the tool call!
            if self._audit_sink is not None:
                self._audit_sink(
                    {
                        "event_type": "TOOLGUARD_FAIL_CLOSED_ERROR",
                        "effective_role": self.active_role,
                        "tool_name": self.name,
                        "provider": self.model_selection.provider,
                        "model_id": self.model_selection.model_id,
                        "error": str(exc),
                        "status": "BLOCKED",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                f"ToolGuard fail-closed: guard evaluation error on tool '{self.name}': {exc}",
                http_status=502,
                role=self.active_role,
            ) from exc

        return self.tool.invoke(args)

    def _enforce_deterministic_policies(self, args: dict[str, Any]) -> None:
        applicable = [p for p in self.policies if p.target_tool_name in (self.name, "*")]
        for pol in applicable:
            for req_arg in pol.required_args:
                if req_arg not in args or args[req_arg] is None:
                    self._deny(pol.policy_id, f"Missing required argument '{req_arg}' for tool '{self.name}'.")
            for key, max_val in pol.max_numeric_limits.items():
                if key in args and isinstance(args[key], (int, float)) and float(args[key]) > float(max_val):
                    self._deny(
                        pol.policy_id,
                        f"Argument '{key}={args[key]}' exceeds policy limit ({max_val}) on tool '{self.name}'.",
                    )
            serialized_args = str(args).lower()
            for pat in pol.forbidden_arg_patterns:
                if pat.lower() in serialized_args:
                    self._deny(
                        pol.policy_id,
                        f"Forbidden pattern '{pat}' detected in arguments for tool '{self.name}'.",
                    )

    def _deny(self, policy_id: str, reason: str) -> None:
        if self._audit_sink is not None:
            self._audit_sink(
                {
                    "event_type": "TOOLGUARD_POLICY_VIOLATION",
                    "effective_role": self.active_role,
                    "tool_name": self.name,
                    "policy_id": policy_id,
                    "reason": reason,
                    "status": "DENIED",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
        raise SnowflakeComponentError(
            ErrorCode.AUTHZ_ROLE_DENIED,
            f"ToolGuard blocked invocation of '{self.name}' under policy '{policy_id}': {reason}",
            http_status=403,
            role=self.active_role,
            details={"policy_id": policy_id, "tool_name": self.name},
        )


class SnowflakeToolGuardPoliciesComponent:
    """Langflow `ToolGuard / Policies` (`Beta`) component powered by ALTK (`REQ-13`, `[G-A6]`)."""

    display_name = "Policies"
    bundle_name = "ToolGuard"
    beta_badge = True
    ports = ("policies_project", "tools", "policies", "language_model", "guarded_tools")
    available_providers = EXISTING_UPSTREAM_TOOLGUARD_PROVIDERS + SNOWFLAKE_TOOLGUARD_PROVIDERS
    unchanged_discover_more_bundles = UNCHANGED_DISCOVER_MORE_BUNDLES

    def __init__(
        self,
        *,
        active_role: str = "ROLE_A",
        allowed_roles: set[str] | None = None,
        policy_generator_fn: Callable[[list[ToolDefinition], list[PolicySpec], ToolGuardModelSelection], list[PolicySpec]]
        | None = None,
        guard_evaluator_fn: Callable[[ToolDefinition, dict[str, Any], list[PolicySpec]], None] | None = None,
        audit_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.active_role = active_role
        self.allowed_roles = {r.upper() for r in (allowed_roles or {active_role})}
        self._policy_generator_fn = policy_generator_fn
        self._guard_evaluator_fn = guard_evaluator_fn
        self.audit_log: list[dict[str, Any]] = []
        self._external_audit_sink = audit_sink

    def _record_audit(self, event: dict[str, Any]) -> None:
        self.audit_log.append(event)
        if self._external_audit_sink is not None:
            self._external_audit_sink(event)

    def build_guarded_tools(
        self,
        *,
        activity: ToolGuardActivity | str,
        enabled: bool,
        policies_project: str,
        tools: list[ToolDefinition],
        policies: list[PolicySpec],
        language_model: ToolGuardModelSelection,
    ) -> dict[str, Any]:
        if self.active_role.upper() not in self.allowed_roles:
            raise_role_denied(
                self.active_role,
                "EXECUTE TOOLGUARD POLICIES",
                policies_project or "ToolGuard",
                self._record_audit,
            )

        act = activity if isinstance(activity, ToolGuardActivity) else ToolGuardActivity(str(activity))
        language_model.validate()

        if not (policies_project or "").strip():
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "'Policies Project' cannot be empty in ToolGuard / Policies.",
                http_status=400,
                role=self.active_role,
            )

        if not enabled:
            # Explicit pass-through when Enabled=False
            wrapped = [
                GuardedTool(
                    t,
                    policies=policies,
                    guard_enabled=False,
                    active_role=self.active_role,
                    model_selection=language_model,
                    audit_sink=self._record_audit,
                )
                for t in tools
            ]
            return {
                "beta": self.beta_badge,
                "activity": act.value,
                "enabled": False,
                "policies_project": policies_project,
                "provider": language_model.provider,
                "model_id": language_model.model_id,
                "guarded_tools": wrapped,
                "effective_role": self.active_role,
            }

        # Enabled = True: Fail-closed on any generation or validation failure
        if not policies:
            raise SnowflakeComponentError(
                ErrorCode.TECHNICAL_EXECUTION_ERROR,
                "ToolGuard fail-closed: 'Enabled=True' requires at least one policy in 'Policies'. "
                "Unguarded tools will not be passed through.",
                http_status=422,
                role=self.active_role,
            )

        effective_policies = list(policies)
        if act == ToolGuardActivity.GENERATE:
            try:
                if self._policy_generator_fn is not None:
                    effective_policies = self._policy_generator_fn(tools, policies, language_model)
                if not effective_policies:
                    raise RuntimeError("Policy generator returned empty policy set")
            except SnowflakeComponentError:
                raise
            except Exception as exc:
                self._record_audit(
                    {
                        "event_type": "TOOLGUARD_GENERATE_FAILURE",
                        "effective_role": self.active_role,
                        "policies_project": policies_project,
                        "provider": language_model.provider,
                        "model_id": language_model.model_id,
                        "error": str(exc),
                        "status": "FAIL_CLOSED",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                )
                raise SnowflakeComponentError(
                    ErrorCode.TECHNICAL_EXECUTION_ERROR,
                    f"ToolGuard fail-closed during policy generation with '{language_model.provider}:{language_model.model_id}': {exc}",
                    http_status=502,
                    role=self.active_role,
                ) from exc

        wrapped_tools = [
            GuardedTool(
                t,
                policies=effective_policies,
                guard_enabled=True,
                active_role=self.active_role,
                model_selection=language_model,
                guard_evaluator=self._guard_evaluator_fn,
                audit_sink=self._record_audit,
            )
            for t in tools
        ]

        return {
            "beta": self.beta_badge,
            "activity": act.value,
            "enabled": True,
            "policies_project": policies_project,
            "provider": language_model.provider,
            "model_id": language_model.model_id,
            "guarded_tools": wrapped_tools,
            "effective_role": self.active_role,
        }
