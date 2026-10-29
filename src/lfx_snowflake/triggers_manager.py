"""Governed Snowflake Triggers Manager (`TRG-W2-R8-02..06`).

Implements:
- Identifier validation & quoting (`_quote_ident`, `^[A-Za-z_][A-Za-z0-9_$]{0,254}$`)
- Alphanumeric `flow_short_id` sanitization (`re.sub(r'[^A-Za-z0-9]', '', flow_id)[:12].upper()`)
- Strict 5-field Snowflake Cron validator (including `L` and `#`) & IANA Timezone allowlist
- Single read-only `SELECT` / `WITH ... SELECT` validator for `Condition Alert Trigger`
- Pre-flight RBAC privilege verification under `CURRENT_ROLE()` (`[ ❄️ Context ]`)
- DDL generation & provisioning for:
  1. `SnowflakeScheduleTrigger` (`CREATE OR ALTER TASK <DB>.<SCHEMA>.LF_SCHED_<FLOW_SHORT_ID>`)
  2. `SnowflakeTableChangeTrigger` (`CREATE STREAM` + Triggered `TASK` with transactional rollback on failure)
  3. `SnowflakeStageFileTrigger` (`ALTER STAGE ... DIRECTORY = (ENABLE = TRUE)` + `CREATE STREAM ON STAGE` + Triggered `TASK`)
  4. `SnowflakeAlertTrigger` (`CREATE OR ALTER ALERT <DB>.<SCHEMA>.LF_ALRT_<FLOW_SHORT_ID>` with `RESULT_SCAN(SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID())`)
- SPCS Service Function `/api/v1/snowflake/service-trigger` batch protocol & idempotency
- Automatic lifecycle cleanup (`SUSPEND` + `DROP`) when trigger nodes or flows are updated/deleted.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from zoneinfo import available_timezones

from lfx_snowflake.providers.registry import (
    ERROR_AUTHZ_ROLE_DENIED,
    SnowflakeProviderError,
)


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
FLOW_ID_SAFE_RE = re.compile(r"^[A-Za-z0-9_\-]{1,128}$")
FILE_PATTERN_SAFE_RE = re.compile(r"^[A-Za-z0-9_.*?\-/]{0,255}$")
TIMEZONE_FORMAT_RE = re.compile(r"^[A-Za-z0-9_+/\-]{1,64}$")

TRIGGER_EVENT_TYPES: tuple[str, ...] = (
    "SCHEDULED_RUN",
    "STREAM_TRIGGER_RUN",
    "STAGE_FILE_TRIGGER_RUN",
    "ALERT_TRIGGER_RUN",
    "WEBHOOK_RUN",
)

STATUS_MODE_OPTIONS: list[str] = ["Active", "Paused"]

SCHEDULE_TYPE_OPTIONS: list[str] = [
    "Custom (Cron)",
    "Minutes",
    "Hours",
    "Days",
    "Weeks",
    "Months",
]

TABLE_TRIGGER_ON_OPTIONS: list[str] = [
    "New Rows Only (Insert)",
    "All Changes (Insert / Update / Delete)",
]

STAGE_TRIGGER_ON_OPTIONS: list[str] = [
    "New Files Only (Upload)",
    "New & Updated Files",
    "All File Events (Upload / Update / Delete)",
]

ALERT_MODE_OPTIONS: list[str] = [
    "Periodic Condition Check",
    "Alert on New Data (Event-Driven)",
]

ALERT_FREQUENCY_OPTIONS: list[str] = [
    "Every 5 Minutes",
    "Every 15 Minutes",
    "Every 1 Hour",
    "Custom (Cron)",
]

COMPUTE_MODE_OPTIONS: list[str] = [
    "Serverless (Managed)",
    "Warehouse",
]

COMMON_IANA_TIMEZONES: list[str] = [
    "Europe/Rome",
    "UTC",
    "Europe/London",
    "Europe/Paris",
    "Europe/Berlin",
    "Europe/Zurich",
    "Europe/Madrid",
    "Europe/Amsterdam",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/Toronto",
    "America/Sao_Paulo",
    "Asia/Tokyo",
    "Asia/Singapore",
    "Asia/Shanghai",
    "Asia/Kolkata",
    "Asia/Dubai",
    "Australia/Sydney",
]

_ALL_IANA_TIMEZONES: frozenset[str] = frozenset(COMMON_IANA_TIMEZONES) | frozenset(available_timezones())

# Snowflake 5-field Cron token patterns (minute, hour, day-of-month, month, day-of-week + L / #)
_CRON_MINUTE_RE = re.compile(r"^(\*|\d{1,2}(-\d{1,2})?(\/\d{1,2})?|\*\/\d{1,2})(,(\*|\d{1,2}(-\d{1,2})?(\/\d{1,2})?|\*\/\d{1,2}))*$")
_CRON_HOUR_RE = re.compile(r"^(\*|\d{1,2}(-\d{1,2})?(\/\d{1,2})?|\*\/\d{1,2})(,(\*|\d{1,2}(-\d{1,2})?(\/\d{1,2})?|\*\/\d{1,2}))*$")
_CRON_DOM_RE = re.compile(r"^(\*|\?|L|LW|\d{1,2}W?|\d{1,2}-\d{1,2}(\/\d{1,2})?|\*\/\d{1,2})(,(\*|\?|L|LW|\d{1,2}W?|\d{1,2}-\d{1,2}(\/\d{1,2})?|\*\/\d{1,2}))*$", re.IGNORECASE)
_CRON_MONTH_TOKEN = r"(\*|\d{1,2}|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
_CRON_MONTH_RE = re.compile(
    rf"^{_CRON_MONTH_TOKEN}(-{_CRON_MONTH_TOKEN})?(\/\d{{1,2}})?(,{_CRON_MONTH_TOKEN}(-{_CRON_MONTH_TOKEN})?(\/\d{{1,2}})?)*$",
    re.IGNORECASE,
)
_CRON_DOW_ATOM = r"(\*|\?|[0-7]L?|[0-7]#[1-5]|(SUN|MON|TUE|WED|THU|FRI|SAT)L?|(SUN|MON|TUE|WED|THU|FRI|SAT)#[1-5])"
_CRON_DOW_PART = rf"{_CRON_DOW_ATOM}(-{_CRON_DOW_ATOM})?(\/\d{{1,2}})?"
_CRON_DOW_RE = re.compile(rf"^{_CRON_DOW_PART}(,{_CRON_DOW_PART})*$", re.IGNORECASE)

_FORBIDDEN_ALERT_SQL_KEYWORDS_RE = re.compile(
    r"\b("
    r"DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|INSERT|UPDATE|DELETE|MERGE|"
    r"CALL|COPY|PUT|GET|REMOVE|RM|EXECUTE|EXEC|USE|SET|UNSET|SHOW|DESCRIBE|DESC|"
    r"BEGIN|COMMIT|ROLLBACK|UNDROP|COMMENT"
    r")\b",
    re.IGNORECASE,
)


def _quote_ident(identifier: str, label: str = "identifier", *, quote: bool = False) -> str:
    """Validate a Snowflake identifier against `^[A-Za-z_][A-Za-z0-9_$]{0,254}$` and optionally quote it."""
    cleaned = (identifier or "").strip()
    if not cleaned or not IDENTIFIER_RE.match(cleaned):
        raise ValueError(
            f"Invalid Snowflake {label} {identifier!r}: must match {IDENTIFIER_RE.pattern}"
        )
    upper = cleaned.upper()
    return f'"{upper}"' if quote else upper


def quote_sql_ident(identifier: str, label: str = "identifier") -> str:
    """Validate and return double-quoted Snowflake identifier."""
    return _quote_ident(identifier, label=label, quote=True)


def validate_flow_id(flow_id: str | None) -> str:
    """Validate `flow_id` so it cannot inject SQL into comments, string literals, or object names."""
    cleaned = str(flow_id or "").strip() or "default-flow"
    if not FLOW_ID_SAFE_RE.match(cleaned):
        raise ValueError(
            f"Invalid flow_id {flow_id!r}: only alphanumeric characters, hyphens, and underscores are allowed."
        )
    return cleaned


def sanitize_flow_short_id(flow_id: str | None) -> str:
    """Derive a strictly alphanumeric uppercase `FLOW_SHORT_ID` (max 12 chars) from `flow_id`."""
    raw = str(flow_id or "").strip()
    if raw and not FLOW_ID_SAFE_RE.match(raw):
        raise ValueError(f"Invalid flow_id {flow_id!r}: contains unsafe characters.")
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw)[:12].upper()
    return alnum or "DEFAULTFLOW"


def escape_sql_literal(value: str | None) -> str:
    """Escape a string for safe inclusion inside a single-quoted Snowflake SQL literal."""
    text = str(value or "")
    if "\x00" in text:
        raise ValueError("Null byte is not allowed in SQL string literal.")
    return text.replace("\\", "\\\\").replace("'", "''")


def validate_timezone(tz: str | None) -> str:
    """Validate timezone against the official IANA timezone allowlist."""
    cleaned = (tz or "Europe/Rome").strip()
    if not cleaned or not TIMEZONE_FORMAT_RE.match(cleaned):
        raise ValueError(f"Invalid timezone {tz!r}: must be a valid IANA timezone.")
    if cleaned not in _ALL_IANA_TIMEZONES:
        raise ValueError(
            f"Unsupported or untrusted timezone {tz!r}: must be an official IANA timezone (e.g. 'Europe/Rome', 'UTC')."
        )
    return cleaned


def validate_cron_expression(cron_expr: str | None) -> str:
    """Validate a 5-field Snowflake Cron expression (`minute hour dom month dow` + `L`/`#`)."""
    raw = (cron_expr or "").strip()
    if not raw:
        raise ValueError("Cron expression cannot be empty.")
    if any(bad in raw for bad in ("'", '"', ";", "--", "/*", "*/", "\n", "\r", "(", ")")):
        raise ValueError(f"Invalid Cron expression {cron_expr!r}: contains forbidden characters.")
    parts = raw.split()
    if len(parts) != 5:
        raise ValueError(
            f"Invalid Cron expression {cron_expr!r}: expected exactly 5 fields (minute hour day-of-month month day-of-week), got {len(parts)}."
        )
    minute, hour, dom, month, dow = parts
    if not _CRON_MINUTE_RE.match(minute):
        raise ValueError(f"Invalid minute field {minute!r} in Cron expression {cron_expr!r}.")
    if not _CRON_HOUR_RE.match(hour):
        raise ValueError(f"Invalid hour field {hour!r} in Cron expression {cron_expr!r}.")
    if not _CRON_DOM_RE.match(dom):
        raise ValueError(f"Invalid day-of-month field {dom!r} in Cron expression {cron_expr!r}.")
    if not _CRON_MONTH_RE.match(month):
        raise ValueError(f"Invalid month field {month!r} in Cron expression {cron_expr!r}.")
    if not _CRON_DOW_RE.match(dow):
        raise ValueError(f"Invalid day-of-week field {dow!r} in Cron expression {cron_expr!r}.")

    # Numeric bounds check for simple integers in minute/hour/dom/month
    for tok in re.findall(r"\d+", minute):
        if not (0 <= int(tok) <= 59):
            raise ValueError(f"Minute value {tok} out of range (0..59) in Cron expression {cron_expr!r}.")
    for tok in re.findall(r"\d+", hour):
        if not (0 <= int(tok) <= 23):
            raise ValueError(f"Hour value {tok} out of range (0..23) in Cron expression {cron_expr!r}.")
    for tok in re.findall(r"\d+", dom):
        if not (1 <= int(tok) <= 31):
            raise ValueError(f"Day-of-month value {tok} out of range (1..31) in Cron expression {cron_expr!r}.")
    for tok in re.findall(r"\d+", month):
        if not (1 <= int(tok) <= 12):
            raise ValueError(f"Month value {tok} out of range (1..12) in Cron expression {cron_expr!r}.")
    return " ".join(parts)


def build_schedule_clause(
    *,
    schedule_type: str = "Custom (Cron)",
    cron_expression: str = "0 9 * * 1-5",
    timezone_name: str = "Europe/Rome",
    interval_value: int = 15,
    days_of_week: str = "MON,TUE,WED,THU,FRI",
    day_of_month: str = "1",
    at_hour: int = 9,
    at_minute: int = 0,
) -> str:
    """Build a validated Snowflake `SCHEDULE` literal value (e.g. `USING CRON 0 9 * * 1-5 Europe/Rome` or `15 MINUTE`)."""
    stype = (schedule_type or "Custom (Cron)").strip()
    tz = validate_timezone(timezone_name)
    hr = max(0, min(23, int(at_hour if at_hour is not None else 9)))
    mn = max(0, min(59, int(at_minute if at_minute is not None else 0)))

    if stype == "Minutes":
        mins = max(1, min(11520, int(interval_value or 15)))
        return f"{mins} MINUTE"
    if stype == "Hours":
        hrs = max(1, min(192, int(interval_value or 1)))
        return f"{hrs * 60} MINUTE"
    if stype == "Days":
        cron = validate_cron_expression(f"{mn} {hr} * * *")
        return f"USING CRON {cron} {tz}"
    if stype == "Weeks":
        dow_clean = (days_of_week or "MON-FRI").strip().upper().replace(" ", "")
        cron = validate_cron_expression(f"{mn} {hr} * * {dow_clean}")
        return f"USING CRON {cron} {tz}"
    if stype == "Months":
        dom_clean = (day_of_month or "1").strip().upper()
        cron = validate_cron_expression(f"{mn} {hr} {dom_clean} * *")
        return f"USING CRON {cron} {tz}"

    cron = validate_cron_expression(cron_expression)
    return f"USING CRON {cron} {tz}"


def validate_file_pattern(file_pattern: str | None) -> str | None:
    """Validate optional stage file glob pattern (e.g. `*.pdf`) and convert it to a safe SQL ILIKE pattern."""
    raw = (file_pattern or "").strip()
    if not raw:
        return None
    if "--" in raw or "/*" in raw or "*/" in raw or not FILE_PATTERN_SAFE_RE.match(raw):
        raise ValueError(
            f"Invalid file_pattern {file_pattern!r}: only alphanumeric characters, '*', '?', '.', '_', '-', and '/' are allowed."
        )
    ilike = raw.replace("_", "\\_").replace("%", "\\%").replace("*", "%").replace("?", "_")
    return ilike


def validate_readonly_alert_sql(condition_sql: str | None) -> str:
    """Validate that `condition_sql` is a single read-only `SELECT` or `WITH ... SELECT` statement."""
    raw = (condition_sql or "").strip()
    if not raw:
        raise ValueError("Alert SQL condition cannot be empty.")

    # Reject SQL comments or dollar-quoted scripting blocks used to hide multi-statement or privilege escalation payloads
    if "--" in raw or "/*" in raw or "*/" in raw or "$$" in raw:
        raise ValueError("Alert SQL condition must not contain SQL comments ('--', '/* ... */') or dollar-quoted blocks ('$$').")

    # Strip trailing semicolon if any, then reject any internal semicolon
    trimmed = raw[:-1].strip() if raw.endswith(";") else raw
    if not trimmed:
        raise ValueError("Alert SQL condition cannot be empty.")

    # Replace single-quoted string literals with placeholders before scanning for semicolons/keywords
    without_strings = re.sub(r"'(?:''|[^'])*'", "'_STR_'", trimmed)
    if ";" in without_strings:
        raise ValueError("Alert SQL condition must be a single SELECT statement (multiple statements with ';' are forbidden).")

    upper_stripped = without_strings.lstrip().upper()
    if not re.match(r"^(SELECT|WITH)\b", upper_stripped):
        raise ValueError("Alert SQL condition must start with a read-only SELECT or WITH ... SELECT query.")

    if upper_stripped.startswith("WITH") and not re.search(r"\bSELECT\b", without_strings, re.IGNORECASE):
        raise ValueError("Alert SQL CTE condition must contain a SELECT query.")

    match = _FORBIDDEN_ALERT_SQL_KEYWORDS_RE.search(without_strings)
    if match:
        kw = match.group(1).upper()
        raise ValueError(
            f"Forbidden SQL keyword '{kw}' in Alert SQL condition: only read-only SELECT queries are permitted."
        )

    return trimmed


def extract_alert_base_table_fqn(condition_sql: str) -> str | None:
    """Extract the primary `DB.SCHEMA.TABLE` referenced after `FROM` in a validated Alert SQL query, if present."""
    match = re.search(
        r"\bFROM\s+([A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*\.[A-Za-z_][A-Za-z0-9_$]*)",
        condition_sql or "",
        re.IGNORECASE,
    )
    if not match:
        return None
    parts = match.group(1).split(".")
    if len(parts) == 3:
        return ".".join(_quote_ident(p) for p in parts)
    return None


def verify_trigger_rbac_privileges(
    *,
    active_role: str,
    database: str,
    schema: str,
    trigger_kind: str,
    compute_mode: str = "Serverless (Managed)",
    warehouse: str | None = None,
    bridge: Any = None,
) -> dict[str, Any]:
    """Verify `CURRENT_ROLE()` privileges before executing any Task/Stream/Alert DDL.

    Raises `SnowflakeProviderError` (`ERROR_AUTHZ_ROLE_DENIED`, HTTP 403) with explicit
    `GRANT` remediation commands when the active role is unauthorized.
    """
    role = _quote_ident(active_role or "PUBLIC", "active_role")
    db = _quote_ident(database or "SNOWFLAKE_LANGFLOW", "database")
    sch = _quote_ident(schema or "CORE", "schema")
    kind = (trigger_kind or "TASK").strip().upper()
    is_serverless = (compute_mode or "Serverless (Managed)").strip().startswith("Serverless")

    required_grants: list[str] = [
        f"GRANT USAGE ON DATABASE {db} TO ROLE {role};",
        f"GRANT USAGE ON SCHEMA {db}.{sch} TO ROLE {role};",
    ]
    if kind in ("SCHEDULE", "TASK"):
        required_grants.append(f"GRANT CREATE TASK ON SCHEMA {db}.{sch} TO ROLE {role};")
        if is_serverless:
            required_grants.append(f"GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE {role};")
        else:
            wh = _quote_ident(warehouse or "COMPUTE_WH", "warehouse")
            required_grants.append(f"GRANT USAGE ON WAREHOUSE {wh} TO ROLE {role};")
            required_grants.append(f"GRANT EXECUTE TASK ON ACCOUNT TO ROLE {role};")
    elif kind in ("TABLE_CHANGE", "STAGE_FILE", "STREAM_TASK"):
        required_grants.append(f"GRANT CREATE STREAM, CREATE TASK ON SCHEMA {db}.{sch} TO ROLE {role};")
        if is_serverless:
            required_grants.append(f"GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE {role};")
        else:
            wh = _quote_ident(warehouse or "COMPUTE_WH", "warehouse")
            required_grants.append(f"GRANT USAGE ON WAREHOUSE {wh} TO ROLE {role};")
            required_grants.append(f"GRANT EXECUTE TASK ON ACCOUNT TO ROLE {role};")
    elif kind == "ALERT":
        required_grants.append(f"GRANT CREATE ALERT ON SCHEMA {db}.{sch} TO ROLE {role};")
        if is_serverless:
            required_grants.append(f"GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE {role};")
        else:
            wh = _quote_ident(warehouse or "COMPUTE_WH", "warehouse")
            required_grants.append(f"GRANT USAGE ON WAREHOUSE {wh} TO ROLE {role};")
            required_grants.append(f"GRANT EXECUTE ALERT ON ACCOUNT TO ROLE {role};")

    required_grants.append(
        f"GRANT USAGE ON FUNCTION SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW(VARCHAR, VARIANT) TO ROLE {role};"
    )

    is_denied_role = role == "PUBLIC" or "VIEWER" in role or "READONLY" in role
    bridge_denied = None
    if bridge is not None and hasattr(bridge, "check_write_privilege_or_deny"):
        bridge_denied = bridge.check_write_privilege_or_deny(f"CREATE_TRIGGER_{kind}", f"{db}.{sch}")

    if is_denied_role or bridge_denied:
        remediation = " ".join(required_grants)
        msg = (
            f"Active Snowflake role '{role}' lacks required privileges to provision or resume {kind} triggers in {db}.{sch}. "
            f"Remediation SQL: {remediation}"
        )
        if bridge is not None and hasattr(bridge, "record_audit_event"):
            bridge.record_audit_event(
                action=f"CREATE_TRIGGER_{kind}",
                outcome="DENIED",
                error_code=ERROR_AUTHZ_ROLE_DENIED,
                target_object=f"{db}.{sch}",
                details={"active_role": role, "required_grants": required_grants},
            )
        raise SnowflakeProviderError(
            ERROR_AUTHZ_ROLE_DENIED,
            msg,
            active_role=role,
            operation=f"CREATE_TRIGGER_{kind}:{db}.{sch}",
            required_privilege="; ".join(required_grants),
            status_code=403,
        )

    return {
        "ok": True,
        "active_role": role,
        "database": db,
        "schema": sch,
        "trigger_kind": kind,
        "required_grants": required_grants,
    }


def _build_compute_ddl_lines(
    compute_mode: str = "Serverless (Managed)",
    warehouse: str | None = None,
) -> list[str]:
    mode = (compute_mode or "Serverless (Managed)").strip()
    if mode == "Warehouse" and warehouse and str(warehouse).strip():
        wh = _quote_ident(str(warehouse).strip(), "warehouse")
        return [f"  WAREHOUSE = {wh}"]
    return [
        "  SERVERLESS_TASK_MIN_STATEMENT_SIZE = 'XSMALL'",
        "  SERVERLESS_TASK_MAX_STATEMENT_SIZE = 'SMALL'",
    ]


def build_schedule_trigger_ddl(
    *,
    flow_id: str,
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    status_mode: str = "Active",
    schedule_type: str = "Custom (Cron)",
    cron_expression: str = "0 9 * * 1-5",
    timezone_name: str = "Europe/Rome",
    interval_value: int = 15,
    days_of_week: str = "MON,TUE,WED,THU,FRI",
    day_of_month: str = "1",
    at_hour: int = 9,
    at_minute: int = 0,
    trigger_message: str = "Run scheduled workflow",
    compute_mode: str = "Serverless (Managed)",
    warehouse: str | None = None,
    allow_overlapping_executions: bool = False,
    retry_attempts: int = 3,
    suspend_after_failures: int = 5,
    timeout_ms: int = 3600000,
) -> dict[str, Any]:
    """Generate governed Snowflake Task DDL for `SnowflakeScheduleTrigger`."""
    safe_flow_id = validate_flow_id(flow_id)
    flow_short_id = sanitize_flow_short_id(safe_flow_id)
    db = _quote_ident(database or "SNOWFLAKE_LANGFLOW", "database")
    sch = _quote_ident(schema or "CORE", "schema")
    task_name = f"LF_SCHED_{flow_short_id}"
    task_fqn = f"{db}.{sch}.{task_name}"

    schedule_clause = build_schedule_clause(
        schedule_type=schedule_type,
        cron_expression=cron_expression,
        timezone_name=timezone_name,
        interval_value=interval_value,
        days_of_week=days_of_week,
        day_of_month=day_of_month,
        at_hour=at_hour,
        at_minute=at_minute,
    )
    compute_lines = _build_compute_ddl_lines(compute_mode, warehouse)
    overlap = "ALLOW_OVERLAP" if bool(allow_overlapping_executions) else "NO_OVERLAP"
    retries = max(0, min(30, int(retry_attempts if retry_attempts is not None else 3)))
    suspend_num = max(1, min(100, int(suspend_after_failures if suspend_after_failures is not None else 5)))
    timeout_val = max(1000, min(86400000, int(timeout_ms if timeout_ms is not None else 3600000)))
    escaped_msg = escape_sql_literal(trigger_message or "Run scheduled workflow")
    config_json = json.dumps({"flow_id": safe_flow_id, "trigger_type": "SCHEDULED_RUN"})

    create_task_sql = "\n".join(
        [
            f"CREATE OR ALTER TASK {task_fqn}",
            f"  SCHEDULE = '{schedule_clause}'",
            *compute_lines,
            f"  OVERLAP_POLICY = {overlap}",
            f"  TASK_AUTO_RETRY_ATTEMPTS = {retries}",
            f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {suspend_num}",
            f"  USER_TASK_TIMEOUT_MS = {timeout_val}",
            f"  COMMENT = 'Managed by SnowflakeLangflow Schedule Trigger | flow_id={safe_flow_id}'",
            f"  CONFIG = '{config_json}'",
            "AS",
            "DECLARE",
            "  res VARIANT;",
            "  STATEMENT_ERROR EXCEPTION (-20001, 'SnowflakeLangflow scheduled flow run failed');",
            "BEGIN",
            "  res := SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW(",
            f"    '{safe_flow_id}',",
            "    OBJECT_CONSTRUCT(",
            "      'trigger_type', 'SCHEDULED_RUN',",
            "      'task_name', SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),",
            "      'task_graph_run_group_id', SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'),",
            "      'scheduled_time', SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'),",
            f"      'trigger_message', '{escaped_msg}'",
            "    )",
            "  );",
            "  IF ( COALESCE(:res:status::VARCHAR, 'SUCCEEDED') = 'FAILED' ) THEN",
            "    RAISE STATEMENT_ERROR;",
            "  END IF;",
            "  CALL SYSTEM$SET_RETURN_VALUE(TO_VARCHAR(:res));",
            "END;",
        ]
    )

    is_active = (status_mode or "Active").strip().lower() != "paused"
    state_action = "RESUME" if is_active else "SUSPEND"
    alter_state_sql = f"ALTER TASK {task_fqn} {state_action};"

    return {
        "trigger_component": "SnowflakeScheduleTrigger",
        "trigger_type": "SCHEDULED_RUN",
        "flow_id": safe_flow_id,
        "flow_short_id": flow_short_id,
        "database": db,
        "schema": sch,
        "task_name": task_name,
        "task_fqn": task_fqn,
        "primary_object_fqn": task_fqn,
        "secondary_object_fqn": None,
        "source_object_fqn": None,
        "schedule_clause": schedule_clause,
        "status_mode": "Active" if is_active else "Paused",
        "create_task_sql": create_task_sql,
        "alter_state_sql": alter_state_sql,
        "statements": [create_task_sql, alter_state_sql],
        "ddl": f"{create_task_sql}\n\n{alter_state_sql}",
    }


def build_table_change_trigger_ddl(
    *,
    flow_id: str,
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    source_object: str = "CUSTOMER_ORDERS",
    object_kind: str = "TABLE",
    status_mode: str = "Active",
    trigger_on: str = "New Rows Only (Insert)",
    max_batch_rows: int = 100,
    show_initial_rows: bool = False,
    compute_mode: str = "Serverless (Managed)",
    warehouse: str | None = None,
    retry_attempts: int = 3,
    suspend_after_failures: int = 5,
    timeout_ms: int = 3600000,
) -> dict[str, Any]:
    """Generate governed Snowflake Stream + pure Triggered Task DDL for `SnowflakeTableChangeTrigger`."""
    safe_flow_id = validate_flow_id(flow_id)
    flow_short_id = sanitize_flow_short_id(safe_flow_id)
    db = _quote_ident(database or "SNOWFLAKE_LANGFLOW", "database")
    sch = _quote_ident(schema or "CORE", "schema")
    raw_obj = (source_object or "CUSTOMER_ORDERS").strip().split(".")[-1]
    obj = _quote_ident(raw_obj, "source_object")
    source_fqn = f"{db}.{sch}.{obj}"

    stream_name = f"LF_STRM_{flow_short_id}_{obj}"[:255]
    task_name = f"LF_TRG_{flow_short_id}_{obj}"[:255]
    temp_batch_table = f"_LF_STREAM_BATCH_{flow_short_id}"
    stream_fqn = f"{db}.{sch}.{stream_name}"
    task_fqn = f"{db}.{sch}.{task_name}"
    temp_batch_fqn = f"{db}.{sch}.{temp_batch_table}"

    kind_upper = (object_kind or "TABLE").strip().upper()
    if "DYNAMIC" in kind_upper:
        on_target_clause = f"ON DYNAMIC TABLE {source_fqn}"
    elif "VIEW" in kind_upper:
        on_target_clause = f"ON VIEW {source_fqn}"
    else:
        on_target_clause = f"ON TABLE {source_fqn}"

    append_only = "TRUE" if "Insert" in (trigger_on or "New Rows Only (Insert)") and "All" not in (trigger_on or "") else "FALSE"
    initial_rows = "TRUE" if bool(show_initial_rows) else "FALSE"
    batch_limit = max(1, min(10000, int(max_batch_rows if max_batch_rows is not None else 100)))
    compute_lines = _build_compute_ddl_lines(compute_mode, warehouse)
    retries = max(0, min(30, int(retry_attempts if retry_attempts is not None else 3)))
    suspend_num = max(1, min(100, int(suspend_after_failures if suspend_after_failures is not None else 5)))
    timeout_val = max(1000, min(86400000, int(timeout_ms if timeout_ms is not None else 3600000)))

    create_stream_sql = "\n".join(
        [
            f"CREATE STREAM IF NOT EXISTS {stream_fqn}",
            f"  {on_target_clause}",
            f"  APPEND_ONLY = {append_only}",
            f"  SHOW_INITIAL_ROWS = {initial_rows}",
            f"  COMMENT = 'Managed by SnowflakeLangflow Table Change Trigger | flow_id={safe_flow_id}';",
        ]
    )

    create_task_sql = "\n".join(
        [
            f"CREATE OR ALTER TASK {task_fqn}",
            *compute_lines,
            "  OVERLAP_POLICY = NO_OVERLAP",
            f"  TASK_AUTO_RETRY_ATTEMPTS = {retries}",
            f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {suspend_num}",
            f"  USER_TASK_TIMEOUT_MS = {timeout_val}",
            f"  COMMENT = 'Managed by SnowflakeLangflow Table Change Trigger | flow_id={safe_flow_id}'",
            f"  WHEN SYSTEM$STREAM_HAS_DATA('{stream_fqn}')",
            "AS",
            "DECLARE",
            "  batch_count INTEGER;",
            "  rows_payload VARIANT;",
            "  res VARIANT;",
            "  STATEMENT_ERROR EXCEPTION (-20002, 'SnowflakeLangflow stream trigger run failed; rolling back stream offset');",
            "BEGIN",
            f"  CREATE OR REPLACE TEMPORARY TABLE {temp_batch_fqn} AS",
            f"    SELECT * FROM {stream_fqn}",
            f"    LIMIT {batch_limit};",
            "",
            "  SELECT COUNT(*), COALESCE(ARRAY_AGG(OBJECT_CONSTRUCT(*)), ARRAY_CONSTRUCT())",
            "    INTO :batch_count, :rows_payload",
            f"    FROM {temp_batch_fqn};",
            "",
            "  IF (:batch_count > 0) THEN",
            "    res := SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW(",
            f"      '{safe_flow_id}',",
            "      OBJECT_CONSTRUCT(",
            "        'trigger_type', 'STREAM_TRIGGER_RUN',",
            f"        'source_object', '{source_fqn}',",
            f"        'stream_name', '{stream_fqn}',",
            "        'task_name', SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),",
            "        'row_count', :batch_count,",
            "        'rows', :rows_payload",
            "      )",
            "    );",
            "    IF ( COALESCE(:res:status::VARCHAR, 'SUCCEEDED') = 'FAILED' ) THEN",
            "      RAISE STATEMENT_ERROR;",
            "    END IF;",
            "    CALL SYSTEM$SET_RETURN_VALUE(TO_VARCHAR(:res));",
            "  END IF;",
            "END;",
        ]
    )

    is_active = (status_mode or "Active").strip().lower() != "paused"
    state_action = "RESUME" if is_active else "SUSPEND"
    alter_state_sql = f"ALTER TASK {task_fqn} {state_action};"

    return {
        "trigger_component": "SnowflakeTableChangeTrigger",
        "trigger_type": "STREAM_TRIGGER_RUN",
        "flow_id": safe_flow_id,
        "flow_short_id": flow_short_id,
        "database": db,
        "schema": sch,
        "source_object": obj,
        "source_object_fqn": source_fqn,
        "stream_name": stream_name,
        "stream_fqn": stream_fqn,
        "task_name": task_name,
        "task_fqn": task_fqn,
        "temp_batch_fqn": temp_batch_fqn,
        "primary_object_fqn": task_fqn,
        "secondary_object_fqn": stream_fqn,
        "status_mode": "Active" if is_active else "Paused",
        "create_stream_sql": create_stream_sql,
        "create_task_sql": create_task_sql,
        "alter_state_sql": alter_state_sql,
        "statements": [create_stream_sql, create_task_sql, alter_state_sql],
        "ddl": f"{create_stream_sql}\n\n{create_task_sql}\n\n{alter_state_sql}",
    }


def build_stage_file_trigger_ddl(
    *,
    flow_id: str,
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    stage_name: str = "SNOWFLAKE_LANGFLOW_STAGE",
    status_mode: str = "Active",
    trigger_on: str = "New Files Only (Upload)",
    file_pattern: str | None = None,
    max_files_per_run: int = 50,
    compute_mode: str = "Serverless (Managed)",
    warehouse: str | None = None,
    retry_attempts: int = 3,
    suspend_after_failures: int = 5,
    timeout_ms: int = 3600000,
) -> dict[str, Any]:
    """Generate governed Directory Table + Stream on Stage + Triggered Task DDL for `SnowflakeStageFileTrigger`."""
    safe_flow_id = validate_flow_id(flow_id)
    flow_short_id = sanitize_flow_short_id(safe_flow_id)
    db = _quote_ident(database or "SNOWFLAKE_LANGFLOW", "database")
    sch = _quote_ident(schema or "CORE", "schema")
    raw_stg = (stage_name or "SNOWFLAKE_LANGFLOW_STAGE").lstrip("@").strip().split(".")[-1]
    stg = _quote_ident(raw_stg, "stage_name")
    stage_fqn = f"{db}.{sch}.{stg}"

    stream_name = f"LF_STG_STRM_{flow_short_id}_{stg}"[:255]
    task_name = f"LF_STG_TRG_{flow_short_id}_{stg}"[:255]
    temp_batch_table = f"_LF_STAGE_BATCH_{flow_short_id}"
    stream_fqn = f"{db}.{sch}.{stream_name}"
    task_fqn = f"{db}.{sch}.{task_name}"
    temp_batch_fqn = f"{db}.{sch}.{temp_batch_table}"

    ilike_pattern = validate_file_pattern(file_pattern)
    where_clauses: list[str] = []
    trig_mode = (trigger_on or "New Files Only (Upload)").strip()
    if trig_mode == "New Files Only (Upload)":
        where_clauses.append("METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE")
    elif trig_mode == "New & Updated Files":
        where_clauses.append("METADATA$ACTION = 'INSERT'")
    if ilike_pattern:
        where_clauses.append(f"RELATIVE_PATH ILIKE '{escape_sql_literal(ilike_pattern)}'")
    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    batch_limit = max(1, min(5000, int(max_files_per_run if max_files_per_run is not None else 50)))
    compute_lines = _build_compute_ddl_lines(compute_mode, warehouse)
    retries = max(0, min(30, int(retry_attempts if retry_attempts is not None else 3)))
    suspend_num = max(1, min(100, int(suspend_after_failures if suspend_after_failures is not None else 5)))
    timeout_val = max(1000, min(86400000, int(timeout_ms if timeout_ms is not None else 3600000)))

    enable_dir_sql = f"ALTER STAGE IF EXISTS {stage_fqn} SET DIRECTORY = (ENABLE = TRUE);"
    create_stream_sql = "\n".join(
        [
            f"CREATE STREAM IF NOT EXISTS {stream_fqn}",
            f"  ON STAGE {stage_fqn}",
            f"  COMMENT = 'Managed by SnowflakeLangflow Stage File Trigger | flow_id={safe_flow_id}';",
        ]
    )

    create_task_sql = "\n".join(
        [
            f"CREATE OR ALTER TASK {task_fqn}",
            *compute_lines,
            "  OVERLAP_POLICY = NO_OVERLAP",
            f"  TASK_AUTO_RETRY_ATTEMPTS = {retries}",
            f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {suspend_num}",
            f"  USER_TASK_TIMEOUT_MS = {timeout_val}",
            f"  COMMENT = 'Managed by SnowflakeLangflow Stage File Trigger | flow_id={safe_flow_id}'",
            f"  WHEN SYSTEM$STREAM_HAS_DATA('{stream_fqn}')",
            "AS",
            "DECLARE",
            "  file_count INTEGER;",
            "  files_payload VARIANT;",
            "  res VARIANT;",
            "  STATEMENT_ERROR EXCEPTION (-20003, 'SnowflakeLangflow stage file trigger run failed; rolling back stream offset');",
            "BEGIN",
            f"  CREATE OR REPLACE TEMPORARY TABLE {temp_batch_fqn} AS",
            "    SELECT",
            "      RELATIVE_PATH,",
            f"      '@{stage_fqn}/' || RELATIVE_PATH AS STAGE_PATH,",
            f"      BUILD_SCOPED_FILE_URL(@{stage_fqn}, RELATIVE_PATH) AS SCOPED_FILE_URL,",
            "      FILE_URL,",
            "      SIZE,",
            "      LAST_MODIFIED,",
            "      MD5,",
            "      METADATA$ACTION AS EVENT_ACTION,",
            "      METADATA$ISUPDATE AS EVENT_ISUPDATE",
            f"    FROM {stream_fqn}{where_sql}",
            f"    LIMIT {batch_limit};",
            "",
            "  SELECT COUNT(*), COALESCE(ARRAY_AGG(OBJECT_CONSTRUCT(*)), ARRAY_CONSTRUCT())",
            "    INTO :file_count, :files_payload",
            f"    FROM {temp_batch_fqn};",
            "",
            "  IF (:file_count > 0) THEN",
            "    res := SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW(",
            f"      '{safe_flow_id}',",
            "      OBJECT_CONSTRUCT(",
            "        'trigger_type', 'STAGE_FILE_TRIGGER_RUN',",
            f"        'stage_name', '@{stage_fqn}',",
            f"        'stream_name', '{stream_fqn}',",
            "        'task_name', SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),",
            "        'file_count', :file_count,",
            "        'files', :files_payload",
            "      )",
            "    );",
            "    IF ( COALESCE(:res:status::VARCHAR, 'SUCCEEDED') = 'FAILED' ) THEN",
            "      RAISE STATEMENT_ERROR;",
            "    END IF;",
            "    CALL SYSTEM$SET_RETURN_VALUE(TO_VARCHAR(:res));",
            "  END IF;",
            "END;",
        ]
    )

    is_active = (status_mode or "Active").strip().lower() != "paused"
    state_action = "RESUME" if is_active else "SUSPEND"
    alter_state_sql = f"ALTER TASK {task_fqn} {state_action};"

    return {
        "trigger_component": "SnowflakeStageFileTrigger",
        "trigger_type": "STAGE_FILE_TRIGGER_RUN",
        "flow_id": safe_flow_id,
        "flow_short_id": flow_short_id,
        "database": db,
        "schema": sch,
        "stage_name": stg,
        "stage_fqn": f"@{stage_fqn}",
        "source_object_fqn": f"@{stage_fqn}",
        "stream_name": stream_name,
        "stream_fqn": stream_fqn,
        "task_name": task_name,
        "task_fqn": task_fqn,
        "temp_batch_fqn": temp_batch_fqn,
        "file_pattern": file_pattern or "",
        "ilike_pattern": ilike_pattern,
        "primary_object_fqn": task_fqn,
        "secondary_object_fqn": stream_fqn,
        "status_mode": "Active" if is_active else "Paused",
        "enable_dir_sql": enable_dir_sql,
        "create_stream_sql": create_stream_sql,
        "create_task_sql": create_task_sql,
        "alter_state_sql": alter_state_sql,
        "statements": [enable_dir_sql, create_stream_sql, create_task_sql, alter_state_sql],
        "ddl": f"{enable_dir_sql}\n\n{create_stream_sql}\n\n{create_task_sql}\n\n{alter_state_sql}",
    }


def build_alert_trigger_ddl(
    *,
    flow_id: str,
    database: str = "SNOWFLAKE_LANGFLOW",
    schema: str = "CORE",
    status_mode: str = "Active",
    alert_mode: str = "Periodic Condition Check",
    check_frequency: str = "Every 5 Minutes",
    cron_expression: str = "*/5 * * * *",
    timezone_name: str = "Europe/Rome",
    condition_sql: str = "SELECT 1 AS ALERT_TRIGGERED WHERE 1 = 0",
    max_alert_rows: int = 100,
    compute_mode: str = "Serverless (Managed)",
    warehouse: str | None = None,
    suspend_after_failures: int = 5,
) -> dict[str, Any]:
    """Generate governed Snowflake Alert DDL (`CREATE OR ALTER ALERT`) for `SnowflakeAlertTrigger`."""
    safe_flow_id = validate_flow_id(flow_id)
    flow_short_id = sanitize_flow_short_id(safe_flow_id)
    db = _quote_ident(database or "SNOWFLAKE_LANGFLOW", "database")
    sch = _quote_ident(schema or "CORE", "schema")
    alert_name = f"LF_ALRT_{flow_short_id}"
    alert_fqn = f"{db}.{sch}.{alert_name}"

    validated_sql = validate_readonly_alert_sql(condition_sql)
    mode_clean = (alert_mode or "Periodic Condition Check").strip()
    freq_clean = (check_frequency or "Every 5 Minutes").strip()

    schedule_line: str | None = None
    change_tracking_sql: str | None = None
    base_table_fqn = extract_alert_base_table_fqn(validated_sql)

    if mode_clean == "Alert on New Data (Event-Driven)":
        if base_table_fqn:
            change_tracking_sql = f"ALTER TABLE IF EXISTS {base_table_fqn} SET CHANGE_TRACKING = TRUE;"
    else:
        if freq_clean == "Every 5 Minutes":
            schedule_val = "5 MINUTE"
        elif freq_clean == "Every 15 Minutes":
            schedule_val = "15 MINUTE"
        elif freq_clean == "Every 1 Hour":
            schedule_val = "60 MINUTE"
        else:
            cron = validate_cron_expression(cron_expression)
            tz = validate_timezone(timezone_name)
            schedule_val = f"USING CRON {cron} {tz}"
        schedule_line = f"  SCHEDULE = '{schedule_val}'"

    wh_line: str | None = None
    if (compute_mode or "Serverless (Managed)").strip() == "Warehouse" and warehouse and str(warehouse).strip():
        wh_line = f"  WAREHOUSE = {_quote_ident(str(warehouse).strip(), 'warehouse')}"

    suspend_num = max(1, min(100, int(suspend_after_failures if suspend_after_failures is not None else 5)))
    row_limit = max(1, min(5000, int(max_alert_rows if max_alert_rows is not None else 100)))

    header_lines = [f"CREATE OR ALTER ALERT {alert_fqn}"]
    if wh_line:
        header_lines.append(wh_line)
    if schedule_line:
        header_lines.append(schedule_line)
    header_lines.extend(
        [
            f"  SUSPEND_ALERT_AFTER_NUM_FAILURES = {suspend_num}",
            f"  COMMENT = 'Managed by SnowflakeLangflow Condition Alert Trigger | flow_id={safe_flow_id}'",
            "  IF (EXISTS (",
            f"    {validated_sql}",
            "  ))",
            "  THEN",
            "  DECLARE",
            "    cond_qid VARCHAR;",
            "    alert_rows VARIANT;",
            "    row_cnt INTEGER;",
            "    res VARIANT;",
            "    STATEMENT_ERROR EXCEPTION (-20004, 'SnowflakeLangflow condition alert flow run failed');",
            "  BEGIN",
            "    cond_qid := SNOWFLAKE.ALERT.GET_CONDITION_QUERY_UUID();",
            "    SELECT COUNT(*), COALESCE(ARRAY_AGG(OBJECT_CONSTRUCT(*)), ARRAY_CONSTRUCT())",
            "      INTO :row_cnt, :alert_rows",
            "      FROM (",
            f"        SELECT * FROM TABLE(RESULT_SCAN(:cond_qid)) LIMIT {row_limit}",
            "      );",
            "    res := SNOWFLAKE_LANGFLOW.CORE.RUN_LANGFLOW_FLOW(",
            f"      '{safe_flow_id}',",
            "      OBJECT_CONSTRUCT(",
            "        'trigger_type', 'ALERT_TRIGGER_RUN',",
            f"        'alert_name', '{alert_fqn}',",
            "        'condition_query_id', :cond_qid,",
            "        'scheduled_time', TO_VARCHAR(SNOWFLAKE.ALERT.SCHEDULED_TIME()),",
            "        'row_count', :row_cnt,",
            "        'rows', :alert_rows",
            "      )",
            "    );",
            "    IF ( COALESCE(:res:status::VARCHAR, 'SUCCEEDED') = 'FAILED' ) THEN",
            "      RAISE STATEMENT_ERROR;",
            "    END IF;",
            "  END;",
        ]
    )
    create_alert_sql = "\n".join(header_lines)

    is_active = (status_mode or "Active").strip().lower() != "paused"
    state_action = "RESUME" if is_active else "SUSPEND"
    alter_state_sql = f"ALTER ALERT {alert_fqn} {state_action};"

    statements: list[str] = []
    if change_tracking_sql:
        statements.append(change_tracking_sql)
    statements.extend([create_alert_sql, alter_state_sql])

    return {
        "trigger_component": "SnowflakeAlertTrigger",
        "trigger_type": "ALERT_TRIGGER_RUN",
        "flow_id": safe_flow_id,
        "flow_short_id": flow_short_id,
        "database": db,
        "schema": sch,
        "alert_name": alert_name,
        "alert_fqn": alert_fqn,
        "alert_mode": mode_clean,
        "condition_sql": validated_sql,
        "source_object_fqn": base_table_fqn,
        "primary_object_fqn": alert_fqn,
        "secondary_object_fqn": None,
        "status_mode": "Active" if is_active else "Paused",
        "change_tracking_sql": change_tracking_sql,
        "create_alert_sql": create_alert_sql,
        "alter_state_sql": alter_state_sql,
        "statements": statements,
        "ddl": "\n\n".join(statements),
    }


@dataclass
class RegisteredTriggerRecord:
    trigger_id: str
    flow_id: str
    flow_short_id: str
    node_id: str
    trigger_component: str
    trigger_type: str
    status_mode: str
    database: str
    schema: str
    primary_object_fqn: str
    secondary_object_fqn: str | None = None
    source_object_fqn: str | None = None
    active_role: str = "SNOWFLAKE_LANGFLOW_ADMIN_ROLE"
    config: dict[str, Any] = field(default_factory=dict)
    ddl: str = ""
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SnowflakeTriggersManager:
    """Manages Snowflake Task/Stream/Alert lifecycle, idempotency, and SPCS Service Function execution."""

    def __init__(self, sqlite_path: str | None = None) -> None:
        self._lock = threading.RLock()
        self._sqlite_path = sqlite_path or os.environ.get("LANGFLOW_Triggers_DB_PATH") or "/persist/langflow.db"
        self._triggers_by_id: dict[str, RegisteredTriggerRecord] = {}
        self._idempotency_ledger: dict[str, dict[str, Any]] = {}
        self._init_sqlite_tables()

    def _get_sqlite_conn(self) -> sqlite3.Connection | None:
        candidates = [self._sqlite_path, "/tmp/snowflake_langflow_triggers.db"]
        for path in candidates:
            try:
                parent = os.path.dirname(path)
                if parent and not os.path.exists(parent):
                    continue
                conn = sqlite3.connect(path, timeout=5.0)
                return conn
            except Exception:  # noqa: BLE001
                continue
        return None

    def _init_sqlite_tables(self) -> None:
        conn = self._get_sqlite_conn()
        if conn is None:
            return
        try:
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS snowflake_trigger_registry (
                        trigger_id TEXT PRIMARY KEY,
                        flow_id TEXT NOT NULL,
                        flow_short_id TEXT NOT NULL,
                        node_id TEXT NOT NULL,
                        trigger_component TEXT NOT NULL,
                        trigger_type TEXT NOT NULL,
                        status_mode TEXT NOT NULL,
                        database_name TEXT NOT NULL,
                        schema_name TEXT NOT NULL,
                        primary_object_fqn TEXT NOT NULL,
                        secondary_object_fqn TEXT,
                        source_object_fqn TEXT,
                        active_role TEXT NOT NULL,
                        config_json TEXT,
                        ddl_text TEXT,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS snowflake_trigger_idempotency (
                        idempotency_key TEXT PRIMARY KEY,
                        flow_id TEXT NOT NULL,
                        trigger_type TEXT NOT NULL,
                        slot_or_batch_id TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        result_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )
        except Exception:  # noqa: BLE001
            pass
        finally:
            conn.close()

    def register_trigger(
        self,
        *,
        flow_id: str,
        node_id: str,
        ddl_spec: dict[str, Any],
        active_role: str,
        config: dict[str, Any] | None = None,
        bridge: Any = None,
    ) -> RegisteredTriggerRecord:
        safe_fid = validate_flow_id(flow_id)
        safe_nid = str(node_id or ddl_spec.get("trigger_component") or "trigger_node").strip()
        trigger_id = f"{safe_fid}::{safe_nid}"
        rec = RegisteredTriggerRecord(
            trigger_id=trigger_id,
            flow_id=safe_fid,
            flow_short_id=ddl_spec["flow_short_id"],
            node_id=safe_nid,
            trigger_component=ddl_spec["trigger_component"],
            trigger_type=ddl_spec["trigger_type"],
            status_mode=ddl_spec["status_mode"],
            database=ddl_spec["database"],
            schema=ddl_spec["schema"],
            primary_object_fqn=ddl_spec["primary_object_fqn"],
            secondary_object_fqn=ddl_spec.get("secondary_object_fqn"),
            source_object_fqn=ddl_spec.get("source_object_fqn"),
            active_role=active_role,
            config=dict(config or {}),
            ddl=ddl_spec["ddl"],
        )
        with self._lock:
            self._triggers_by_id[trigger_id] = rec
            conn = self._get_sqlite_conn()
            if conn is not None:
                try:
                    with conn:
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO snowflake_trigger_registry (
                                trigger_id, flow_id, flow_short_id, node_id,
                                trigger_component, trigger_type, status_mode,
                                database_name, schema_name, primary_object_fqn,
                                secondary_object_fqn, source_object_fqn, active_role,
                                config_json, ddl_text, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                rec.trigger_id,
                                rec.flow_id,
                                rec.flow_short_id,
                                rec.node_id,
                                rec.trigger_component,
                                rec.trigger_type,
                                rec.status_mode,
                                rec.database,
                                rec.schema,
                                rec.primary_object_fqn,
                                rec.secondary_object_fqn,
                                rec.source_object_fqn,
                                rec.active_role,
                                json.dumps(rec.config, default=str),
                                rec.ddl,
                                rec.updated_at,
                            ),
                        )
                except Exception:  # noqa: BLE001
                    pass
                finally:
                    conn.close()
        return rec

    def list_triggers(self, flow_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            records = list(self._triggers_by_id.values())
        if flow_id:
            norm = validate_flow_id(flow_id)
            records = [r for r in records if r.flow_id == norm]
        return [r.to_dict() for r in records]

    def provision_trigger(
        self,
        *,
        ddl_spec: dict[str, Any],
        node_id: str = "trigger_node",
        bridge: Any = None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Validate RBAC, execute trigger DDL statements, audit, and register the trigger."""
        active_role = (
            getattr(bridge, "active_role", None) if bridge is not None else None
        ) or "SNOWFLAKE_LANGFLOW_ADMIN_ROLE"
        comp = ddl_spec.get("trigger_component", "")
        if comp == "SnowflakeScheduleTrigger":
            kind = "SCHEDULE"
        elif comp == "SnowflakeTableChangeTrigger":
            kind = "TABLE_CHANGE"
        elif comp == "SnowflakeStageFileTrigger":
            kind = "STAGE_FILE"
        else:
            kind = "ALERT"

        verify_trigger_rbac_privileges(
            active_role=active_role,
            database=ddl_spec["database"],
            schema=ddl_spec["schema"],
            trigger_kind=kind,
            compute_mode=(config or {}).get("compute_mode", "Serverless (Managed)"),
            warehouse=(config or {}).get("warehouse"),
            bridge=bridge,
        )

        executed_results: list[dict[str, Any]] = []
        if bridge is not None and hasattr(bridge, "execute_sql"):
            for stmt in ddl_spec.get("statements", []):
                res = bridge.execute_sql(stmt)
                executed_results.append(res)

        rec = self.register_trigger(
            flow_id=ddl_spec["flow_id"],
            node_id=node_id,
            ddl_spec=ddl_spec,
            active_role=active_role,
            config=config,
            bridge=bridge,
        )

        if bridge is not None and hasattr(bridge, "record_audit_event"):
            is_active = ddl_spec.get("status_mode") == "Active"
            if kind == "ALERT":
                op_create = "CREATE_TRIGGER_ALERT"
                op_state = "RESUME_TRIGGER_ALERT" if is_active else "SUSPEND_TRIGGER_ALERT"
            else:
                op_create = "CREATE_TRIGGER_TASK"
                op_state = "RESUME_TRIGGER_TASK" if is_active else "SUSPEND_TRIGGER_TASK"

            if ddl_spec.get("secondary_object_fqn"):
                bridge.record_audit_event(
                    action="CREATE_TRIGGER_STREAM",
                    outcome="SUCCEEDED",
                    target_object=ddl_spec["secondary_object_fqn"],
                    flow_id=rec.flow_id,
                    details={"trigger_component": comp, "source_object": ddl_spec.get("source_object_fqn")},
                )
            bridge.record_audit_event(
                action=op_create,
                outcome="SUCCEEDED",
                target_object=ddl_spec["primary_object_fqn"],
                flow_id=rec.flow_id,
                details={"trigger_component": comp, "status_mode": rec.status_mode},
            )
            bridge.record_audit_event(
                action=op_state,
                outcome="SUCCEEDED",
                target_object=ddl_spec["primary_object_fqn"],
                flow_id=rec.flow_id,
                details={"trigger_component": comp, "status_mode": rec.status_mode},
            )

        return {
            "ok": True,
            "trigger": rec.to_dict(),
            "ddl": ddl_spec["ddl"],
            "statements": ddl_spec["statements"],
            "execution_results": executed_results,
        }

    def build_cleanup_ddl_for_record(self, rec: RegisteredTriggerRecord) -> list[str]:
        """Build `ALTER ... SUSPEND` and `DROP ... IF EXISTS` statements for a registered trigger."""
        stmts: list[str] = []
        if rec.trigger_component == "SnowflakeAlertTrigger":
            stmts.append(f"ALTER ALERT IF EXISTS {rec.primary_object_fqn} SUSPEND;")
            stmts.append(f"DROP ALERT IF EXISTS {rec.primary_object_fqn};")
        else:
            stmts.append(f"ALTER TASK IF EXISTS {rec.primary_object_fqn} SUSPEND;")
            stmts.append(f"DROP TASK IF EXISTS {rec.primary_object_fqn};")
        if rec.secondary_object_fqn:
            stmts.append(f"DROP STREAM IF EXISTS {rec.secondary_object_fqn};")
        return stmts

    def cleanup_flow_triggers(
        self,
        flow_id: str,
        *,
        keep_node_ids: set[str] | None = None,
        bridge: Any = None,
        database: str = "SNOWFLAKE_LANGFLOW",
        schema: str = "CORE",
    ) -> dict[str, Any]:
        """Suspend and drop Snowflake Tasks/Streams/Alerts for removed trigger nodes or a deleted flow."""
        safe_fid = validate_flow_id(flow_id)
        flow_short_id = sanitize_flow_short_id(safe_fid)
        db = _quote_ident(database or "SNOWFLAKE_LANGFLOW", "database")
        sch = _quote_ident(schema or "CORE", "schema")

        to_remove: list[RegisteredTriggerRecord] = []
        with self._lock:
            for tid, rec in list(self._triggers_by_id.items()):
                if rec.flow_id == safe_fid:
                    if keep_node_ids is None or rec.node_id not in keep_node_ids:
                        to_remove.append(rec)
                        self._triggers_by_id.pop(tid, None)

            conn = self._get_sqlite_conn()
            if conn is not None:
                try:
                    with conn:
                        for rec in to_remove:
                            conn.execute(
                                "DELETE FROM snowflake_trigger_registry WHERE trigger_id = ?",
                                (rec.trigger_id,),
                            )
                except Exception:  # noqa: BLE001
                    pass
                finally:
                    conn.close()

        statements: list[str] = []
        for rec in to_remove:
            statements.extend(self.build_cleanup_ddl_for_record(rec))

        # When deleting the entire flow even if no in-memory record was loaded, emit defensive IF EXISTS cleanup
        if keep_node_ids is None and not statements:
            statements.extend(
                [
                    f"ALTER TASK IF EXISTS {db}.{sch}.LF_SCHED_{flow_short_id} SUSPEND;",
                    f"DROP TASK IF EXISTS {db}.{sch}.LF_SCHED_{flow_short_id};",
                    f"ALTER ALERT IF EXISTS {db}.{sch}.LF_ALRT_{flow_short_id} SUSPEND;",
                    f"DROP ALERT IF EXISTS {db}.{sch}.LF_ALRT_{flow_short_id};",
                ]
            )

        executed: list[dict[str, Any]] = []
        if bridge is not None and hasattr(bridge, "execute_sql"):
            for stmt in statements:
                executed.append(bridge.execute_sql(stmt))

        if bridge is not None and hasattr(bridge, "record_audit_event") and statements:
            bridge.record_audit_event(
                action="CLEANUP_FLOW_TRIGGERS",
                outcome="SUCCEEDED",
                target_object=f"{db}.{sch}.*_{flow_short_id}",
                flow_id=safe_fid,
                details={
                    "removed_triggers": [r.trigger_id for r in to_remove],
                    "statements_count": len(statements),
                },
            )

        return {
            "ok": True,
            "flow_id": safe_fid,
            "flow_short_id": flow_short_id,
            "removed_triggers": [r.to_dict() for r in to_remove],
            "statements": statements,
            "execution_results": executed,
        }

    def sync_flow_payload_triggers(
        self,
        flow_id: str,
        flow_payload: dict[str, Any] | None,
        *,
        bridge: Any = None,
    ) -> dict[str, Any]:
        """Inspect a saved flow JSON (`PUT /api/v1/flows/{flow_id}`), provision active/paused triggers, and drop removed ones."""
        safe_fid = validate_flow_id(flow_id)
        data_obj = (flow_payload or {}).get("data") if isinstance(flow_payload, dict) else None
        nodes = data_obj.get("nodes", []) if isinstance(data_obj, dict) else []
        if not isinstance(nodes, list):
            nodes = []

        trigger_node_types = {
            "SnowflakeScheduleTrigger",
            "SnowflakeTableChangeTrigger",
            "SnowflakeStageFileTrigger",
            "SnowflakeAlertTrigger",
        }
        seen_node_ids: set[str] = set()
        provisioned: list[dict[str, Any]] = []

        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id") or "").strip()
            node_data = node.get("data") if isinstance(node.get("data"), dict) else {}
            node_type = str(node_data.get("type") or node.get("type") or "").strip()
            if node_type not in trigger_node_types:
                continue
            seen_node_ids.add(node_id or node_type)
            template = (
                node_data.get("node", {}).get("template", {})
                if isinstance(node_data.get("node"), dict)
                else {}
            )
            vals: dict[str, Any] = {}
            for k, v in (template or {}).items():
                if isinstance(v, dict) and "value" in v:
                    vals[k] = v.get("value")
                else:
                    vals[k] = v

            db = str(vals.get("database") or getattr(bridge, "database", "SNOWFLAKE_LANGFLOW") or "SNOWFLAKE_LANGFLOW")
            sch = str(vals.get("schema") or vals.get("schema_name") or getattr(bridge, "schema", "CORE") or "CORE")
            status_mode = str(vals.get("status_mode") or "Active")

            if node_type == "SnowflakeScheduleTrigger":
                ddl_spec = build_schedule_trigger_ddl(
                    flow_id=safe_fid,
                    database=db,
                    schema=sch,
                    status_mode=status_mode,
                    schedule_type=str(vals.get("schedule_type") or "Custom (Cron)"),
                    cron_expression=str(vals.get("cron_expression") or "0 9 * * 1-5"),
                    timezone_name=str(vals.get("timezone") or "Europe/Rome"),
                    interval_value=int(vals.get("interval_value") or 15),
                    days_of_week=str(vals.get("days_of_week") or "MON,TUE,WED,THU,FRI"),
                    day_of_month=str(vals.get("day_of_month") or "1"),
                    at_hour=int(vals.get("at_hour") if vals.get("at_hour") is not None else 9),
                    at_minute=int(vals.get("at_minute") if vals.get("at_minute") is not None else 0),
                    trigger_message=str(vals.get("trigger_message") or "Run scheduled workflow"),
                    compute_mode=str(vals.get("compute_mode") or "Serverless (Managed)"),
                    warehouse=vals.get("warehouse"),
                    allow_overlapping_executions=bool(vals.get("allow_overlapping_executions", False)),
                    retry_attempts=int(vals.get("retry_attempts") if vals.get("retry_attempts") is not None else 3),
                    suspend_after_failures=int(vals.get("suspend_after_failures") if vals.get("suspend_after_failures") is not None else 5),
                    timeout_ms=int(vals.get("timeout_ms") if vals.get("timeout_ms") is not None else 3600000),
                )
            elif node_type == "SnowflakeTableChangeTrigger":
                ddl_spec = build_table_change_trigger_ddl(
                    flow_id=safe_fid,
                    database=db,
                    schema=sch,
                    source_object=str(vals.get("source_object") or "CUSTOMER_ORDERS"),
                    object_kind=str(vals.get("object_kind") or "TABLE"),
                    status_mode=status_mode,
                    trigger_on=str(vals.get("trigger_on") or "New Rows Only (Insert)"),
                    max_batch_rows=int(vals.get("max_batch_rows") or 100),
                    show_initial_rows=bool(vals.get("show_initial_rows", False)),
                    compute_mode=str(vals.get("compute_mode") or "Serverless (Managed)"),
                    warehouse=vals.get("warehouse"),
                    retry_attempts=int(vals.get("retry_attempts") if vals.get("retry_attempts") is not None else 3),
                    suspend_after_failures=int(vals.get("suspend_after_failures") if vals.get("suspend_after_failures") is not None else 5),
                    timeout_ms=int(vals.get("timeout_ms") if vals.get("timeout_ms") is not None else 3600000),
                )
            elif node_type == "SnowflakeStageFileTrigger":
                ddl_spec = build_stage_file_trigger_ddl(
                    flow_id=safe_fid,
                    database=db,
                    schema=sch,
                    stage_name=str(vals.get("stage_name") or "SNOWFLAKE_LANGFLOW_STAGE"),
                    status_mode=status_mode,
                    trigger_on=str(vals.get("trigger_on") or "New Files Only (Upload)"),
                    file_pattern=vals.get("file_pattern"),
                    max_files_per_run=int(vals.get("max_files_per_run") or 50),
                    compute_mode=str(vals.get("compute_mode") or "Serverless (Managed)"),
                    warehouse=vals.get("warehouse"),
                    retry_attempts=int(vals.get("retry_attempts") if vals.get("retry_attempts") is not None else 3),
                    suspend_after_failures=int(vals.get("suspend_after_failures") if vals.get("suspend_after_failures") is not None else 5),
                    timeout_ms=int(vals.get("timeout_ms") if vals.get("timeout_ms") is not None else 3600000),
                )
            else:
                ddl_spec = build_alert_trigger_ddl(
                    flow_id=safe_fid,
                    database=db,
                    schema=sch,
                    status_mode=status_mode,
                    alert_mode=str(vals.get("alert_mode") or "Periodic Condition Check"),
                    check_frequency=str(vals.get("check_frequency") or "Every 5 Minutes"),
                    cron_expression=str(vals.get("cron_expression") or "*/5 * * * *"),
                    timezone_name=str(vals.get("timezone") or "Europe/Rome"),
                    condition_sql=str(vals.get("condition_sql") or "SELECT 1 AS ALERT_TRIGGERED WHERE 1 = 0"),
                    max_alert_rows=int(vals.get("max_alert_rows") or 100),
                    compute_mode=str(vals.get("compute_mode") or "Serverless (Managed)"),
                    warehouse=vals.get("warehouse"),
                    suspend_after_failures=int(vals.get("suspend_after_failures") if vals.get("suspend_after_failures") is not None else 5),
                )

            prov_res = self.provision_trigger(
                ddl_spec=ddl_spec,
                node_id=node_id or node_type,
                bridge=bridge,
                config=vals,
            )
            provisioned.append(prov_res)

        cleanup_res = self.cleanup_flow_triggers(
            safe_fid,
            keep_node_ids=seen_node_ids,
            bridge=bridge,
        )
        return {
            "ok": True,
            "flow_id": safe_fid,
            "provisioned": provisioned,
            "cleaned_up": cleanup_res["removed_triggers"],
        }

    def compute_idempotency_key(
        self,
        flow_id: str,
        trigger_type: str,
        trigger_payload: dict[str, Any],
    ) -> tuple[str, str]:
        """Compute deterministic idempotency key on `(flow_id, trigger_type, slot_or_batch_id)`."""
        slot_or_batch = str(
            trigger_payload.get("scheduled_time")
            or trigger_payload.get("task_graph_run_group_id")
            or trigger_payload.get("condition_query_id")
            or trigger_payload.get("batch_id")
            or trigger_payload.get("idempotency_key")
            or ""
        ).strip()
        if not slot_or_batch:
            raw_rows = json.dumps(
                trigger_payload.get("rows") or trigger_payload.get("files") or trigger_payload,
                sort_keys=True,
                default=str,
            )
            slot_or_batch = hashlib.sha256(raw_rows.encode("utf-8")).hexdigest()[:24]
        key = f"{flow_id}::{trigger_type}::{slot_or_batch}"
        return key, slot_or_batch

    def execute_service_trigger_batch(
        self,
        batch_body: dict[str, Any],
        *,
        bridge: Any = None,
    ) -> dict[str, Any]:
        """Handle SPCS Service Function batch request `{"data": [[row_idx, flow_id, trigger_payload], ...]}`."""
        rows = batch_body.get("data") if isinstance(batch_body, dict) else None
        if not isinstance(rows, list):
            raise ValueError("Invalid SPCS Service Function payload: expected {'data': [[row_idx, flow_id, trigger_payload], ...]}.")

        out_rows: list[list[Any]] = []
        for entry in rows:
            if not isinstance(entry, (list, tuple)) or len(entry) < 3:
                raise ValueError(f"Invalid SPCS Service Function row entry: {entry!r}")
            row_idx = entry[0]
            raw_flow_id = str(entry[1] or "").strip()
            raw_payload = entry[2]
            if isinstance(raw_payload, str):
                try:
                    payload = json.loads(raw_payload)
                except Exception:  # noqa: BLE001
                    payload = {"trigger_message": raw_payload}
            elif isinstance(raw_payload, dict):
                payload = dict(raw_payload)
            else:
                payload = {}

            result_obj = self._execute_single_trigger_invocation(
                flow_id=raw_flow_id,
                trigger_payload=payload,
                bridge=bridge,
            )
            out_rows.append([row_idx, result_obj])

        return {"data": out_rows}

    def _execute_single_trigger_invocation(
        self,
        *,
        flow_id: str,
        trigger_payload: dict[str, Any],
        bridge: Any = None,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        safe_fid = validate_flow_id(flow_id)
        trigger_type = str(trigger_payload.get("trigger_type") or "SCHEDULED_RUN").strip().upper()
        if trigger_type not in TRIGGER_EVENT_TYPES:
            trigger_type = "SCHEDULED_RUN"

        has_explicit_slot = any(
            trigger_payload.get(k)
            for k in ("scheduled_time", "task_graph_run_group_id", "condition_query_id", "batch_id", "idempotency_key")
        )
        idem_key, slot_id = self.compute_idempotency_key(safe_fid, trigger_type, trigger_payload)

        if has_explicit_slot:
            with self._lock:
                cached = self._idempotency_ledger.get(idem_key)
                if cached is not None:
                    dup = dict(cached)
                    dup["deduplicated"] = True
                    return dup

        run_id = f"trg_run_{uuid.uuid4().hex[:16]}"
        trace_id = f"trg_trace_{uuid.uuid4().hex[:16]}"
        simulate_fail = bool(trigger_payload.get("simulate_failure") or trigger_payload.get("force_error"))
        status = "FAILED" if simulate_fail else "SUCCEEDED"
        error_msg = str(trigger_payload.get("error_message") or "Simulated trigger execution failure") if simulate_fail else None

        flow_ctx: dict[str, Any] = {}
        active_role = "SNOWFLAKE_LANGFLOW_ADMIN_ROLE"
        warehouse = "COMPUTE_WH"
        ctx_token = None
        try:
            from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID  # noqa: PLC0415

            ctx_token = CURRENT_SNOWFLAKE_FLOW_ID.set(safe_fid)
        except Exception:  # noqa: BLE001
            ctx_token = None

        try:
            if bridge is not None:
                if hasattr(bridge, "set_active_flow"):
                    flow_ctx = dict(bridge.set_active_flow(safe_fid) or {})
                elif hasattr(bridge, "set_current_flow_id"):
                    bridge.set_current_flow_id(safe_fid)
                if not flow_ctx and hasattr(bridge, "get_flow_context"):
                    flow_ctx = dict(bridge.get_flow_context(safe_fid) or {})
                active_role = str(flow_ctx.get("active_role") or getattr(bridge, "active_role", active_role))
                warehouse = str(flow_ctx.get("warehouse") or getattr(bridge, "warehouse", warehouse))

            duration_ms = round(max(1.0, (time.perf_counter() - t0) * 1000.0), 2)
            summary = (
                f"{trigger_type} for flow {safe_fid}: {error_msg}"
                if simulate_fail
                else f"{trigger_type} completed for flow {safe_fid}"
            )

            result_dict: dict[str, Any] = {
                "status": status,
                "run_id": run_id,
                "trace_id": trace_id,
                "flow_id": safe_fid,
                "trigger_type": trigger_type,
                "idempotency_key": idem_key,
                "slot_or_batch_id": slot_id,
                "deduplicated": False,
                "active_role": active_role,
                "warehouse": warehouse,
                "duration_ms": duration_ms,
                "summary": summary,
                "error_message": error_msg,
            }

            if bridge is not None and hasattr(bridge, "record_trigger_execution_telemetry"):
                bridge.record_trigger_execution_telemetry(
                    flow_id=safe_fid,
                    run_id=run_id,
                    trace_id=trace_id,
                    trigger_type=trigger_type,
                    status=status,
                    duration_ms=duration_ms,
                    trigger_payload=trigger_payload,
                    result_payload=result_dict,
                    active_role=active_role,
                    error_message=error_msg,
                )
        finally:
            if ctx_token is not None:
                try:
                    from lfx_snowflake.fastapi_live_bridge import CURRENT_SNOWFLAKE_FLOW_ID  # noqa: PLC0415

                    CURRENT_SNOWFLAKE_FLOW_ID.reset(ctx_token)
                except Exception:  # noqa: BLE001
                    pass

        if has_explicit_slot and status == "SUCCEEDED":
            with self._lock:
                self._idempotency_ledger[idem_key] = dict(result_dict)
                conn = self._get_sqlite_conn()
                if conn is not None:
                    try:
                        with conn:
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO snowflake_trigger_idempotency (
                                    idempotency_key, flow_id, trigger_type, slot_or_batch_id,
                                    run_id, result_json, created_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    idem_key,
                                    safe_fid,
                                    trigger_type,
                                    slot_id,
                                    run_id,
                                    json.dumps(result_dict, default=str),
                                    datetime.now(timezone.utc).isoformat(),
                                ),
                            )
                    except Exception:  # noqa: BLE001
                        pass
                    finally:
                        conn.close()

        return result_dict


def resolve_trigger_flow_id(component: Any = None, build_config: dict[str, Any] | None = None, bridge: Any = None) -> str:
    """Resolve the active Langflow `flow_id` for a Trigger component."""
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415

    resolver = getattr(bridge, "_resolve_component_flow_id", None) or getattr(
        bridge, "resolve_component_flow_id", None
    )
    if callable(resolver):
        res = resolver(component, build_config)
        if res and str(res).strip():
            return validate_flow_id(str(res).strip())
    if isinstance(build_config, dict):
        cfg_fid = build_config.get("_frontend_node_flow_id") or build_config.get("flow_id")
        if isinstance(cfg_fid, dict):
            cfg_fid = cfg_fid.get("value")
        if cfg_fid and str(cfg_fid).strip():
            return validate_flow_id(str(cfg_fid).strip())
    if component is not None:
        for attr in ("flow_id", "_flow_id"):
            val = getattr(component, attr, None)
            if val and str(val).strip():
                return validate_flow_id(str(val).strip())
        graph = getattr(component, "graph", None)
        if graph is not None:
            g_fid = getattr(graph, "flow_id", None) or getattr(graph, "_flow_id", None)
            if g_fid and str(g_fid).strip():
                return validate_flow_id(str(g_fid).strip())
    cur_fn = getattr(bridge, "get_current_flow_id", None) or getattr(
        bridge, "get_current_snowflake_flow_id", None
    )
    if callable(cur_fn):
        cur = cur_fn()
        if cur and str(cur).strip():
            return validate_flow_id(str(cur).strip())
    try:
        from lfx_snowflake.fastapi_live_bridge import get_current_snowflake_flow_id  # noqa: PLC0415

        ctx_fid = get_current_snowflake_flow_id()
        if ctx_fid and str(ctx_fid).strip():
            return validate_flow_id(str(ctx_fid).strip())
    except Exception:  # noqa: BLE001
        pass
    active_fid = getattr(bridge, "active_flow_id", None)
    if active_fid and str(active_fid).strip():
        return validate_flow_id(str(active_fid).strip())
    return "default-flow"


def get_trigger_databases(flow_id: str | None = None, bridge: Any = None) -> list[str]:
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
    dbs = list(bridge.get_context_databases(flow_id=flow_id or resolve_trigger_flow_id(bridge=bridge)))
    return dbs or ["SNOWFLAKE_LANGFLOW"]


def get_trigger_schemas(database: str | None = None, flow_id: str | None = None, bridge: Any = None) -> list[str]:
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
    eff_fid = flow_id or resolve_trigger_flow_id(bridge=bridge)
    db = database or (get_trigger_databases(flow_id=eff_fid, bridge=bridge)[0])
    schemas = list(bridge.get_context_schemas(db, flow_id=eff_fid))
    return schemas or ["CORE"]


def get_trigger_warehouses(flow_id: str | None = None, bridge: Any = None) -> list[str]:
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
    wh_fn = getattr(bridge, "get_context_warehouses", None)
    if callable(wh_fn):
        whs = list(wh_fn(flow_id=flow_id or resolve_trigger_flow_id(bridge=bridge)))
        if whs:
            return whs
    wh = getattr(bridge, "warehouse", None) or "COMPUTE_WH"
    return [str(wh)]


def get_trigger_table_or_view_objects(
    database: str | None = None,
    schema: str | None = None,
    flow_id: str | None = None,
    bridge: Any = None,
) -> list[str]:
    """Return governed business tables, dynamic tables, iceberg tables, and views in `[ ❄️ Context ]`."""
    from lfx_snowflake.components.data_sources import filter_business_data_source_objects  # noqa: PLC0415

    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
    eff_fid = flow_id or resolve_trigger_flow_id(bridge=bridge)
    db = database or "SNOWFLAKE_LANGFLOW"
    sch = schema or "CORE"
    combined: list[str] = []
    for cat in ("tables", "views", "iceberg_tables"):
        for item in bridge.get_context_objects(db, sch, cat, flow_id=eff_fid):
            s = str(item).strip()
            if s and s not in combined:
                combined.append(s)
    filtered = filter_business_data_source_objects(
        combined,
        memories_registry=getattr(bridge, "memories_registry", None),
        category="all",
    )
    if not filtered:
        filtered = ["CUSTOMER_ORDERS", "LANGFLOW_FLOW_RUNS", "LANGFLOW_FLOW_ACTIVITY"]
    return filtered


def detect_source_object_kind(
    database: str | None,
    schema: str | None,
    object_name: str | None,
    flow_id: str | None = None,
    bridge: Any = None,
) -> str:
    """Detect whether `object_name` in `[ ❄️ Context ]` is a `VIEW`, `DYNAMIC TABLE`, or `TABLE`."""
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
    eff_fid = flow_id or resolve_trigger_flow_id(bridge=bridge)
    db = (database or "SNOWFLAKE_LANGFLOW").strip()
    sch = (schema or "CORE").strip()
    short = (object_name or "").strip().split(".")[-1].upper()
    views = [str(v).strip().split(".")[-1].upper() for v in bridge.get_context_objects(db, sch, "views", flow_id=eff_fid)]
    if short in views or short.startswith("V_"):
        return "VIEW"
    if "DYNAMIC" in short or short.startswith("DT_"):
        return "DYNAMIC TABLE"
    return "TABLE"


def get_trigger_stage_objects(
    database: str | None = None,
    schema: str | None = None,
    flow_id: str | None = None,
    bridge: Any = None,
) -> list[str]:
    """Return governed stages in `[ ❄️ Context ]` for `SnowflakeStageFileTrigger`."""
    if bridge is None:
        from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE as bridge  # noqa: PLC0415
    eff_fid = flow_id or resolve_trigger_flow_id(bridge=bridge)
    db = database or "SNOWFLAKE_LANGFLOW"
    sch = schema or "CORE"
    stages = [
        str(s).lstrip("@").strip()
        for s in bridge.get_context_objects(db, sch, "stages", flow_id=eff_fid)
        if str(s).strip() and not str(s).strip().startswith(("+", "["))
    ]
    if "SNOWFLAKE_LANGFLOW_STAGE" not in stages:
        stages.insert(0, "SNOWFLAKE_LANGFLOW_STAGE")
    return stages


TRIGGERS_MANAGER = SnowflakeTriggersManager()
