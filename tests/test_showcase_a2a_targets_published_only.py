"""A2A Agent target lists: only flows served as A2A agents in the calling flow's project are offered."""

from __future__ import annotations

import sqlite3
import uuid

from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

FOLDER_A, FOLDER_B = uuid.uuid4().hex, uuid.uuid4().hex
CALLER, SERVED, UNPUBLISHED, OTHER_PROJECT = (uuid.uuid4() for _ in range(4))


def _bridge(tmp_path, monkeypatch) -> LiveSnowflakeBridge:
    monkeypatch.setenv("LANGFLOW_PERSIST_DIR", str(tmp_path))
    conn = sqlite3.connect(tmp_path / "langflow.db")
    conn.execute(
        "CREATE TABLE flow (id CHAR(32), name TEXT, updated_at TEXT, is_component BOOLEAN, a2a_enabled BOOLEAN, folder_id CHAR(32))"
    )
    conn.executemany(
        "INSERT INTO flow VALUES (?, ?, '2026-11-12', 0, ?, ?)",
        [
            (CALLER.hex, "Caller", 0, FOLDER_A),
            (SERVED.hex, "Served", 1, FOLDER_A),
            (UNPUBLISHED.hex, "Unpublished", 0, FOLDER_A),
            (OTHER_PROJECT.hex, "OtherProject", 1, FOLDER_B),
        ],
    )
    conn.commit()
    conn.close()
    bridge = LiveSnowflakeBridge()
    monkeypatch.setattr(bridge, "get_context_databases", lambda **k: [])
    return bridge


def test_only_published_flows_of_the_same_project_are_listed(tmp_path, monkeypatch) -> None:
    targets = _bridge(tmp_path, monkeypatch).list_internal_and_external_a2a_targets(flow_id=str(CALLER))
    assert targets["internal_options"] == [f"[Langflow Flow] Served ({SERVED})"]
    assert list(targets["quick_fill_map"].values()) == [f"http://127.0.0.1:7860/api/v1/a2a/{SERVED}"]


def test_without_a_calling_flow_every_published_flow_is_listed(tmp_path, monkeypatch) -> None:
    bridge = _bridge(tmp_path, monkeypatch)
    monkeypatch.setattr(bridge, "active_flow_id", None, raising=False)
    names = sorted(o.split("] ")[1].split(" (")[0] for o in bridge.list_internal_and_external_a2a_targets()["internal_options"])
    assert names == ["OtherProject", "Served"]
