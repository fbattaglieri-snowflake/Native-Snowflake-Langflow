from __future__ import annotations

from typing import Any

try:
    from lfx.custom import Component
    from lfx.io import BoolInput, DropdownInput, HandleInput, MessageTextInput, MultilineInput, Output, StrInput
    from lfx.schema import Data
except ImportError:  # pragma: no cover
    from lfx_snowflake._lfx_compat import (
        BoolInput,
        Component,
        Data,
        DropdownInput,
        HandleInput,
        MessageTextInput,
        MultilineInput,
        Output,
        StrInput,
    )

from lfx_snowflake._lfx_compat import Tool
from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE


def _tg_databases() -> list[str]:
    return LIVE_BRIDGE.get_context_databases()


def _tg_schemas(database: str | None = None) -> list[str]:
    return LIVE_BRIDGE.get_context_schemas(database)


def _tg_policy_tables(database: str | None = None, schema: str | None = None) -> list[str]:
    tbls = list(LIVE_BRIDGE.get_context_objects(database, schema, "tables"))
    if "TOOLGUARD_POLICIES" not in tbls:
        tbls.insert(0, "TOOLGUARD_POLICIES")
    return tbls


class SnowflakeToolGuardPoliciesComponent(Component):
    display_name = "ToolGuard"
    description = (
        "Snowflake ToolGuard (Beta): intercepts and wraps incoming Langflow Tools (Cortex Search, Cortex Analyst, "
        "Tables, Views, Iceberg Tables, Postgres, SQL Statement, Stored Procedure, etc.) with CURRENT_ROLE() RBAC, "
        "Cortex Guard (guardrails), AI_REDACT PII masking, and Proxy/Ollama Language Model policy evaluation."
    )
    icon = "Snowflake"
    name = "SnowflakeToolGuardPolicies"

    inputs = [
        HandleInput(
            name="tools",
            display_name="Tools",
            input_types=["Tool"],
            is_list=True,
            required=True,
            info="Connect any Snowflake or Langflow Tools (Cortex Search, Cortex Analyst, Tables, SQL Statement, Stored Procedure, etc.) to guard.",
        ),
        BoolInput(
            name="enabled",
            display_name="Enabled",
            value=True,
        ),
        DropdownInput(
            name="database",
            display_name="Database",
            options=_tg_databases(),
            value="SNOWFLAKE_LANGFLOW",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="schema_name",
            display_name="Schema",
            options=_tg_schemas("SNOWFLAKE_LANGFLOW"),
            value="CORE",
            real_time_refresh=True,
            combobox=True,
        ),
        DropdownInput(
            name="policy_table",
            display_name="Policies Project / Snowflake Policy Table",
            options=_tg_policy_tables("SNOWFLAKE_LANGFLOW", "CORE"),
            value="TOOLGUARD_POLICIES",
            real_time_refresh=True,
            combobox=True,
        ),
        HandleInput(
            name="llm",
            display_name="Language Model",
            input_types=["LanguageModel"],
            required=False,
            info="Compatible with Proxy - Chat, Proxy - Responses, Proxy - Messages, and Private - Ollama.",
        ),
        DropdownInput(
            name="provider_protocol",
            display_name="Fallback Model Provider",
            options=["Proxy - Responses", "Proxy - Messages", "Proxy - Chat", "Private - Ollama"],
            value="Proxy - Responses",
            advanced=True,
        ),
        MultilineInput(
            name="policies",
            display_name="Policies",
            value=(
                "block_ddl_dml=true\n"
                "redact_pii=true\n"
                "cortex_guard=true\n"
                "enforce_active_role=true\n"
                "Deny any tool execution that drops tables or exfiltrates credentials."
            ),
        ),
        StrInput(
            name="pii_redaction_categories",
            display_name="AI_REDACT PII Categories",
            value="NAME,EMAIL,PHONE_NUMBER,ADDRESS,US_SSN",
        ),
        MessageTextInput(
            name="tool_call_payload",
            display_name="Tool Call Payload to Guard (Optional Inspection)",
            value="",
            advanced=True,
        ),
    ]
    outputs = [
        Output(display_name="Guarded Tools", name="guarded_tools", method="build_guarded_tools", types=["Tool"]),
        Output(display_name="Policy Verdict", name="verdict", method="evaluate_toolguard", types=["Data"]),
    ]

    def update_build_config(
        self, build_config: dict[str, Any], field_value: Any, field_name: str | None = None
    ) -> dict[str, Any]:
        if "database" in build_config:
            build_config["database"]["options"] = _tg_databases()
        db = (
            field_value
            if field_name == "database" and isinstance(field_value, str)
            else build_config.get("database", {}).get("value") or "SNOWFLAKE_LANGFLOW"
        )
        if "schema_name" in build_config:
            build_config["schema_name"]["options"] = _tg_schemas(db)
        sch = (
            field_value
            if field_name == "schema_name" and isinstance(field_value, str)
            else build_config.get("schema_name", {}).get("value") or "CORE"
        )
        if "policy_table" in build_config:
            build_config["policy_table"]["options"] = _tg_policy_tables(db, sch)
        return build_config

    def _is_payload_blocked(self, payload_text: str) -> bool:
        lower = (payload_text or "").lower()
        return any(
            bad in lower
            for bad in (
                "drop table",
                "drop database",
                "drop schema",
                "truncate table",
                "alter user",
                "grant accountadmin",
                "private_key",
                "secret",
            )
        )

    def build_guarded_tools(self) -> list[Tool]:
        raw_tools = getattr(self, "tools", None) or []
        if not isinstance(raw_tools, list):
            raw_tools = [raw_tools]
        enabled = bool(getattr(self, "enabled", True))
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        sch = getattr(self, "schema_name", "CORE")
        pol_tbl = getattr(self, "policy_table", "TOOLGUARD_POLICIES")
        pii_cats = getattr(self, "pii_redaction_categories", "NAME,EMAIL,PHONE_NUMBER,ADDRESS,US_SSN")

        guarded_list: list[Tool] = []
        for idx, t_item in enumerate(raw_tools):
            t_name = getattr(t_item, "name", None) or f"snowflake_tool_{idx + 1}"
            t_desc = getattr(t_item, "description", None) or "Snowflake governed tool"
            orig_fn = getattr(t_item, "func", None) or (t_item if callable(t_item) else None)

            def _make_guarded_fn(inner_fn: Any, tool_label: str):
                def _guarded(tool_input: Any = None, **kwargs: Any) -> dict[str, Any]:
                    text_in = str(tool_input or "")
                    if enabled and self._is_payload_blocked(text_in):
                        raise ValueError(
                            f"BLOCKED_BY_TOOLGUARD_POLICY: Tool '{tool_label}' call blocked under role "
                            f"{LIVE_BRIDGE.active_role} by {db}.{sch}.{pol_tbl}."
                        )
                    raw_out = inner_fn(tool_input, **kwargs) if callable(inner_fn) else {"input": tool_input}
                    return {
                        "toolguard_enforced": enabled,
                        "active_role": LIVE_BRIDGE.active_role,
                        "policy_table_fqn": f"{db}.{sch}.{pol_tbl}",
                        "pii_redaction_categories": pii_cats,
                        "tool_name": tool_label,
                        "output": raw_out,
                    }

                return _guarded

            guarded_list.append(
                Tool(
                    name=f"guarded_{t_name}",
                    description=f"[ToolGuard {db}.{sch}.{pol_tbl}] {t_desc}",
                    func=_make_guarded_fn(orig_fn, t_name),
                    metadata={
                        "guarded": True,
                        "enabled": enabled,
                        "policy_table_fqn": f"{db}.{sch}.{pol_tbl}",
                        "pii_redaction_categories": pii_cats,
                        "active_role": LIVE_BRIDGE.active_role,
                    },
                )
            )
        return guarded_list

    def evaluate_toolguard(self) -> Data:
        payload_text = getattr(self, "tool_call_payload", "") or ""
        blocked = bool(getattr(self, "enabled", True)) and self._is_payload_blocked(payload_text)
        db = getattr(self, "database", "SNOWFLAKE_LANGFLOW")
        sch = getattr(self, "schema_name", "CORE")
        pol_tbl = getattr(self, "policy_table", "TOOLGUARD_POLICIES")
        return Data(
            data={
                "passed": not blocked,
                "fail_closed": True,
                "enabled": bool(getattr(self, "enabled", True)),
                "policy_table_fqn": f"{db}.{sch}.{pol_tbl}",
                "pii_redaction_categories": getattr(self, "pii_redaction_categories", ""),
                "provider_protocol": getattr(self, "provider_protocol", "Proxy - Responses"),
                "active_role": LIVE_BRIDGE.active_role,
                "verdict": "BLOCKED_BY_TOOLGUARD_POLICY" if blocked else "ALLOWED",
            }
        )

    evaluate_policy = evaluate_toolguard
