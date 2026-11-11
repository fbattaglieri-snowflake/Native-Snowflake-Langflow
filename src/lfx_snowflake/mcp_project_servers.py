"""BUG-MCP-05: register in-container Langflow Project MCP Servers with direct streamable HTTP.

Langflow 1.13 auto-registers every project's MCP server for its users as a stdio
``uvx mcp-proxy --transport streamablehttp [--headers x-api-key <key>] <url>`` entry.
Inside SPCS ``uvx`` cannot download ``mcp-proxy`` (egress), so the ``MCP Tools``
component fails with "Connection closed". This module rewrites exactly those
loopback project entries to the native HTTP shape ``{"url", "headers"}`` (the
same shape the MCP Servers UI stores for "Streamable HTTP/SSE"), keeping the
project's ``x-api-key`` header: the loopback call is still authenticated by
Langflow's own MCP endpoint, there is no auth bypass for localhost.

Requires ``localhost,127.0.0.1`` in ``LANGFLOW_SSRF_ALLOWED_HOSTS`` (see
``service-spec.template.yaml``) because the MCP client applies Langflow's SSRF policy.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_PROJECT_STREAMABLE_PATH_RE = re.compile(
    r"^/api/v1/mcp/project/[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}/streamable/?$"
)
_RECONCILE_INTERVAL_S = 30.0
_FORCE_PATH_PREFIXES = ("/api/v2/mcp/servers", "/api/v1/projects")


def is_loopback_project_streamable_url(url: str) -> bool:
    """True for `http(s)://{localhost|127.0.0.1|[::1]}[:port]/api/v1/mcp/project/<uuid>/streamable`."""
    try:
        parts = urlsplit(str(url or "").strip())
    except ValueError:
        return False
    if parts.scheme not in ("http", "https") or parts.username or parts.password or parts.query or parts.fragment:
        return False
    return (parts.hostname or "").lower() in LOOPBACK_HOSTS and bool(_PROJECT_STREAMABLE_PATH_RE.match(parts.path))


def to_direct_streamable(config: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return the direct-HTTP equivalent of a native `uvx mcp-proxy` project entry, else None.

    Only entries whose single positional target is the local Project MCP Server are converted;
    any other stdio server (user-defined tools, remote proxies) is left untouched.
    """
    if not isinstance(config, dict) or config.get("command") != "uvx":
        return None
    args = [str(a) for a in (config.get("args") or [])]
    if "mcp-proxy" not in args:
        return None
    rest = args[args.index("mcp-proxy") + 1 :]
    headers: dict[str, str] = {}
    positional: list[str] = []
    i = 0
    while i < len(rest):
        tok = rest[i]
        if tok == "--transport" and i + 1 < len(rest):
            if rest[i + 1] != "streamablehttp":
                return None
            i += 2
        elif tok == "--headers" and i + 2 < len(rest):
            headers[rest[i + 1]] = rest[i + 2]
            i += 3
        elif tok.startswith("-"):
            return None  # unknown proxy option: do not guess its semantics
        else:
            positional.append(tok)
            i += 1
    if len(positional) != 1 or not is_loopback_project_streamable_url(positional[0]):
        return None
    out: dict[str, Any] = {"url": positional[0]}
    if headers:
        out["headers"] = headers
    return out  # `env` only configured the mcp-proxy process; HTTP servers have none


async def reconcile_project_mcp_servers() -> int:
    """Rewrite every user's loopback `uvx mcp-proxy` project entry to direct HTTP; returns rows changed."""
    from datetime import datetime, timezone  # noqa: PLC0415

    from langflow.services.auth.mcp_encryption import decrypt_mcp_config, encrypt_mcp_config  # noqa: PLC0415
    from langflow.services.database.models.mcp_server.model import MCPServer  # noqa: PLC0415
    from langflow.services.deps import session_scope  # noqa: PLC0415
    from sqlmodel import select  # noqa: PLC0415

    changed = 0
    async with session_scope() as session:
        rows = (await session.exec(select(MCPServer))).all()
        for row in rows:
            direct = to_direct_streamable(decrypt_mcp_config(row.config or {}))
            if direct is None:
                continue
            row.config = encrypt_mcp_config(direct)
            row.transport = "streamable-http"
            row.version = int(row.version or 1) + 1
            row.updated_at = datetime.now(timezone.utc)
            session.add(row)
            changed += 1
    if changed:
        logger.info("BUG-MCP-05: %d project MCP server(s) switched to direct streamable HTTP", changed)
    return changed


class ProjectMCPServerReconcileASGIMiddleware:
    """Keep auto-registered project MCP servers on direct HTTP (startup, new projects, server list)."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def _reconcile(self, force: bool) -> None:
        if not force and time.monotonic() - self._last < _RECONCILE_INTERVAL_S:
            return
        async with self._lock:
            if not force and time.monotonic() - self._last < _RECONCILE_INTERVAL_S:
                return
            self._last = time.monotonic()
            try:
                await reconcile_project_mcp_servers()
            except Exception:  # noqa: BLE001  never break a request over this best-effort rewrite
                logger.warning("BUG-MCP-05: project MCP server reconcile failed", exc_info=True)

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path") or "")
        force = path.startswith(_FORCE_PATH_PREFIXES)
        await self._reconcile(force and scope.get("method") == "GET")
        await self.app(scope, receive, send)
        if force and scope.get("method") in ("POST", "PUT", "PATCH"):
            await self._reconcile(True)  # a project was just created/renamed: fix its fresh entry
