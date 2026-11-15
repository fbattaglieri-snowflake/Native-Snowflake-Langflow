"""One rule for data nodes: a statement Snowflake refused stops the node with Snowflake's message.

Without it the node stayed green and returned zero rows (or ``"ok": false`` buried in a JSON payload), so a
failed read looked like an empty table and a failed write looked like a successful one.
"""

from __future__ import annotations

import re
from typing import Any

# Statements run inside a multi-statement request (session settings + the statement). When the statement fails,
# Snowflake wraps its error in the wrapper's own text; this keeps only the error of the statement itself.
_MULTISTMT_ERROR = re.compile(r"\(at line \d+, position \d+\)\.\s*(?P<error>.+?)\s+in SYSTEM\$MULTISTMT", re.S)


def statement_error(message: str) -> str:
    match = _MULTISTMT_ERROR.search(message or "")
    return " ".join(match.group("error").split()) if match else str(message or "").strip()


def require_ok(result: Any, what: str) -> Any:
    """Return ``result`` unchanged, or raise ``ValueError`` when it is a failed SQL result."""
    if isinstance(result, dict) and result.get("ok") is False:
        detail = statement_error(str(result.get("message") or result.get("error") or "")) or "no error message returned"
        code = str(result.get("error_code") or "").strip()
        raise ValueError(f"{what} failed: {detail}" + (f" [{code}]" if code and code not in detail else ""))
    return result
