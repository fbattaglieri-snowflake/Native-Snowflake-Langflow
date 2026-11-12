"""v0.8.6-20261112: ambiguous spans land on RUN close, one expiry timer, larger pending re-read cap."""

from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone

import pytest

from lfx_snowflake import telemetry as tm


@pytest.fixture
def bridge(monkeypatch: pytest.MonkeyPatch):
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_since", {})
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_telemetry_pending_flow", {})
    monkeypatch.setitem(LIVE_BRIDGE.__dict__, "_pending_expiry_timer_armed", False)
    return LIVE_BRIDGE


def test_close_ends_grace_once_all_candidates_closed(bridge) -> None:
    flow = uuid.uuid4().hex
    a = tm.RUNS.start(flow_id=flow)
    b = tm.RUNS.start(flow_id=flow)
    ts = datetime.now(timezone.utc)
    assert bridge._telemetry_row_pending("tx:r1", ts, flow) is True
    tm.RUNS.end(a.run_id, "SUCCESS")
    assert bridge._telemetry_row_pending("tx:r1", ts, flow) is True  # b still running
    tm.RUNS.end(b.run_id, "SUCCESS")
    assert bridge._telemetry_row_pending("tx:r1", ts, flow) is False
    assert "tx:r1" not in bridge._telemetry_pending_since
    assert "tx:r1" not in bridge._telemetry_pending_flow


def test_expiry_timer_armed_once(bridge, monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[float] = []

    class _T:
        def __init__(self, delay, fn):
            started.append(delay)
            self.daemon = False

        def start(self):
            pass

    monkeypatch.setattr(threading, "Timer", _T)
    now = time.monotonic()
    bridge._telemetry_pending_since.update({"tx:a": now - 100, "vb:b": now - 10})
    bridge._arm_pending_expiry_sync()
    bridge._arm_pending_expiry_sync()
    assert len(started) == 1
    assert 15 <= started[0] <= 25  # earliest expiry: 120 - 100 s


def test_no_timer_without_pending(bridge, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(threading, "Timer", lambda *a, **k: pytest.fail("timer armed"))
    bridge._arm_pending_expiry_sync()


def test_pending_reread_cap(bridge) -> None:
    bridge._telemetry_pending_since.update({f"tx:{i}": 0.0 for i in range(6000)})
    assert len(bridge._pending_row_ids("tx:")) == 5000
