"""Account-agnostic stdio MCP bridge for SnowflakeLangflow on Snowpark Container Services (SPCS).

Connects Cortex Code CLI and Cortex Code Desktop (via `~/.snowflake/cortex/mcp.json`)
to a SnowflakeLangflow SPCS public ingress endpoint (`/api/v1/snowflake/mcp/streamable`
or `/api/v1/mcp/project/<PROJECT_ID>/streamable`) using the Snowflake PAT from
`~/.snowflake/connections.toml`.
"""

from __future__ import annotations

import gzip
import json
import os
import pathlib
import sys
import tomllib
import urllib.error
import urllib.request

HOME = pathlib.Path.home()
ALLOWED_METHODS = {
    "initialize",
    "ping",
    "tools/list",
    "tools/call",
    "resources/list",
    "resources/templates/list",
    "resources/read",
    "prompts/list",
    "prompts/get",
}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class SnowflakeLangflowMCPBridge:
    def __init__(self) -> None:
        self.opener = urllib.request.build_opener(_NoRedirect)
        self.session_id: str | None = None
        self.protocol_version: str | None = None
        self.ingress_url = os.environ.get("SNOWFLAKE_LANGFLOW_INGRESS_URL", "https://<INGRESS_HOST>").rstrip("/")
        self.mcp_path = os.environ.get("SNOWFLAKE_LANGFLOW_MCP_PATH", "/api/v1/snowflake/mcp/streamable")
        self.connection_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "default")

    def _resolve_pat_token(self) -> str:
        env_token = os.environ.get("SNOWFLAKE_PAT_TOKEN", "").strip()
        if env_token:
            return env_token
        cfg_path = HOME / ".snowflake" / "connections.toml"
        config = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
        connections = config.get("connections", config)
        conn = connections.get(self.connection_name) or next(iter(connections.values()))
        return str(conn["token"])

    def send(self, message: dict) -> dict | None:
        method = str(message.get("method") or "")
        is_notification = "id" not in message
        if method.startswith("notifications/"):
            if method != "notifications/initialized":
                return None
        elif method not in ALLOWED_METHODS:
            return self.error(message, -32601, f"Unsupported MCP method: {method}")

        token = self._resolve_pat_token()
        headers = {
            "X-SF-SPCS-Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        api_key = os.environ.get("LANGFLOW_API_KEY", "").strip()
        if api_key:
            headers["x-api-key"] = api_key
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        if self.protocol_version:
            headers["MCP-Protocol-Version"] = self.protocol_version

        req = urllib.request.Request(
            self.ingress_url + self.mcp_path,
            data=json.dumps(message).encode("utf-8"),
            headers=headers,
        )
        with self.opener.open(req, timeout=180) as resp:
            self.session_id = resp.headers.get("Mcp-Session-Id", self.session_id)
            if is_notification:
                resp.read()
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "text/event-stream" in content_type:
                result = None
                for raw in resp:
                    line = raw.decode("utf-8", errors="ignore").strip()
                    if line.startswith("data:"):
                        candidate = json.loads(line[5:].strip())
                        if candidate.get("id") == message.get("id"):
                            result = candidate
                            break
                if result is None:
                    raise RuntimeError("No matching MCP SSE response")
            else:
                raw = resp.read()
                if raw.startswith(b"\x1f\x8b"):
                    raw = gzip.decompress(raw)
                result = json.loads(raw.decode("utf-8"))

        if method == "initialize" and isinstance(result, dict) and "result" in result:
            self.protocol_version = result["result"].get("protocolVersion")
        return result

    @staticmethod
    def error(message: dict, code: int, text: str) -> dict | None:
        if "id" not in message:
            return None
        return {"jsonrpc": "2.0", "id": message["id"], "error": {"code": code, "message": text}}


def main() -> None:
    bridge = SnowflakeLangflowMCPBridge()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        message: dict = {}
        try:
            message = json.loads(line)
            if not isinstance(message, dict):
                raise ValueError("JSON-RPC object required")
            result = bridge.send(message)
        except urllib.error.HTTPError as err:
            result = bridge.error(message, -32000, f"SnowflakeLangflow HTTP {err.code}")
        except Exception as exc:  # noqa: BLE001
            result = bridge.error(message if isinstance(message, dict) else {}, -32603, f"Bridge error: {exc}")
        if result is not None:
            print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
