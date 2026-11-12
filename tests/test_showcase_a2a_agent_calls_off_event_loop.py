"""The A2A Agent patch runs the blocking Cortex Agent calls off the event loop."""

from __future__ import annotations

import re
from pathlib import Path

SRC = (Path(__file__).resolve().parents[1] / "scripts" / "install_live_hooks_in_container.py").read_text(encoding="utf-8")


def test_a2a_agent_cortex_calls_are_awaited_in_a_thread() -> None:
    start = SRC.index("async def _run_internal_agent(self)")
    block = SRC[start : SRC.index("if \"quick_fill_endpoint\" in a2a_txt", start)]
    blocking = re.findall(r"LIVE_BRIDGE\.(invoke_cortex_agent|handle_cortex_agent_a2a_jsonrpc)\(", block)
    assert blocking == [], f"blocking bridge calls on the event loop: {blocking}"
    assert block.count("await asyncio.to_thread(\n                LIVE_BRIDGE.invoke_cortex_agent,") == 2
    assert block.count("await asyncio.to_thread(\n                LIVE_BRIDGE.handle_cortex_agent_a2a_jsonrpc,") == 1
