"""Native category component files for Snowflake Triggers (TRG-W2-R8)."""

from __future__ import annotations

import importlib
from typing import Any

_DYNAMIC_IMPORTS: dict[str, str] = {
    "SnowflakeScheduleTrigger": "snowflake_schedule_trigger",
    "SnowflakeTableChangeTrigger": "snowflake_table_change_trigger",
    "SnowflakeStageFileTrigger": "snowflake_stage_file_trigger",
    "SnowflakeAlertTrigger": "snowflake_alert_trigger",
}

__all__ = [
    "SnowflakeAlertTrigger",
    "SnowflakeScheduleTrigger",
    "SnowflakeStageFileTrigger",
    "SnowflakeTableChangeTrigger",
]


def __getattr__(attr_name: str) -> Any:
    if attr_name in _DYNAMIC_IMPORTS:
        module_name = _DYNAMIC_IMPORTS[attr_name]
        mod = importlib.import_module(f".{module_name}", __name__)
        val = getattr(mod, attr_name)
        globals()[attr_name] = val
        return val
    raise AttributeError(f"module {__name__!r} has no attribute {attr_name!r}")


def __dir__() -> list[str]:
    return list(__all__)
