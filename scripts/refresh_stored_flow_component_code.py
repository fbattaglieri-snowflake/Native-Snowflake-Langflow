#!/usr/bin/env python3
"""Admin: rewrite stored flows whose Snowflake component code is outdated (run after a deploy).

Langflow builds each node from the component code stored in the flow. At runtime the stale-component
guard (``lfx_snowflake.stale_flow_guard``) already builds outdated Snowflake nodes from the installed
source (or refuses them with ``COMPONENT_CODE_OUTDATED`` when SNOWFLAKE_LANGFLOW_STALE_COMPONENT_POLICY=refuse);
this script makes the stored flows themselves current, through Langflow's own flows API.

Run inside the Langflow container (where ``lfx_snowflake`` and the installed components are importable):

    LANGFLOW_URL=http://localhost:7860 LANGFLOW_API_KEY=... \\
        python3 scripts/refresh_stored_flow_component_code.py [--apply] [FLOW_ID ...]

Without ``--apply`` it only reports. Without flow ids every flow visible to the API key is checked.
Only ``template.code.value`` of Snowflake nodes changes; field values, edges and other nodes are kept.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

from lfx_snowflake.stale_flow_guard import refresh_flow_data


def _request(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    base = os.environ.get("LANGFLOW_URL", "http://localhost:7860").rstrip("/")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if os.environ.get("LANGFLOW_API_KEY"):
        headers["x-api-key"] = os.environ["LANGFLOW_API_KEY"]
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{base}{path}", data=data, method=method, headers=headers)  # noqa: S310
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, None


def _flow_ids() -> list[str]:
    status, flows = _request("GET", "/api/v1/flows/?get_all=true&header_flows=true")
    if status != 200 or not isinstance(flows, list):
        raise SystemExit(f"cannot list flows (http={status})")
    return [str(f["id"]) for f in flows if isinstance(f, dict) and f.get("id")]


def refresh(flow_id: str, *, apply: bool) -> bool:
    status, flow = _request("GET", f"/api/v1/flows/{flow_id}")
    if status != 200 or not isinstance(flow, dict):
        print(f"[skip] {flow_id} http={status}")
        return False
    data = flow.get("data") or {}
    changed = refresh_flow_data(data)
    if not changed:
        print(f"[ok] {flow.get('name')} ({flow_id}) already current")
        return True
    if not apply:
        print(f"[outdated] {flow.get('name')} ({flow_id}) nodes {changed} (dry run; pass --apply)")
        return True
    status, _ = _request("PATCH", f"/api/v1/flows/{flow_id}", {"data": data})
    print(f"[{'refreshed' if status == 200 else 'FAIL http=' + str(status)}] {flow.get('name')} ({flow_id}) nodes {changed}")
    return status == 200


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("flow_ids", nargs="*")
    parser.add_argument("--apply", action="store_true", help="write the refreshed code (default: report only)")
    args = parser.parse_args(argv)
    results = [refresh(fid, apply=args.apply) for fid in (args.flow_ids or _flow_ids())]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
