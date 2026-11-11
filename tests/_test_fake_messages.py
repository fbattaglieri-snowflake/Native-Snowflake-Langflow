"""Test double for the governed LANGFLOW_MESSAGES table (RAP emulated by CURRENT_USER)."""
from __future__ import annotations

from typing import Any


class FakeGovernedMessagesBridge:
    """Proxies a real bridge, emulating INSERT/SELECT on LANGFLOW_MESSAGES with positional bindings."""

    def __init__(self, inner: Any, *, current_user: str = "ALICE", table: str = "SNOWFLAKE_LANGFLOW.CORE.LANGFLOW_MESSAGES"):
        self._inner = inner
        self.current_user = current_user
        self.table = table
        self.rows: list[dict[str, Any]] = []
        self.calls: list[tuple[str, dict[str, Any] | None, str | None]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def _configured_messages_table(self) -> str:
        return self.table

    def execute_sql(self, sql: str, *, bindings: dict[str, Any] | None = None, mode: str | None = None, **_: Any) -> dict[str, Any]:
        self.calls.append((sql, bindings, mode))
        vals = [b["value"] for _, b in sorted((bindings or {}).items(), key=lambda kv: int(kv[0]))]
        if sql.startswith("INSERT INTO"):
            mid, fid, sid, sender, sname, text, created = vals
            self.rows.append(
                {"MESSAGE_ID": mid, "FLOW_ID": fid, "SESSION_ID": sid, "SENDER": sender, "SENDER_NAME": sname,
                 "TEXT": text, "CREATED_AT": created, "OWNER_USER": self.current_user}
            )
            return {"ok": True, "rows": []}
        if sql.startswith("SELECT"):
            fid, sid = vals
            limit = int(sql.rsplit("LIMIT", 1)[1])
            mine = [r for r in self.rows if r["FLOW_ID"] == fid and r["SESSION_ID"] == sid and r["OWNER_USER"] == self.current_user]
            mine.sort(key=lambda r: (r["CREATED_AT"], r["MESSAGE_ID"]), reverse=True)
            return {"ok": True, "rows": mine[:limit]}
        raise AssertionError(f"unexpected SQL: {sql}")
