"""One rule for data nodes: a statement Snowflake refused stops the node with Snowflake's message.

Without it the node stayed green and returned zero rows (or ``"ok": false`` buried in a JSON payload), so a
failed read looked like an empty table and a failed write looked like a successful one.
"""

from __future__ import annotations

from typing import Any


def require_ok(result: Any, what: str) -> Any:
    """Return ``result`` unchanged, or raise ``ValueError`` when it is a failed SQL result."""
    if isinstance(result, dict) and result.get("ok") is False:
        detail = str(result.get("message") or result.get("error") or "no error message returned").strip()
        code = str(result.get("error_code") or "").strip()
        raise ValueError(f"{what} failed: {detail}" + (f" [{code}]" if code and code not in detail else ""))
    return result
