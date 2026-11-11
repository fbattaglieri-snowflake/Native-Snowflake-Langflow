"""rerun-3: LF_MEM stored nothing (TO_TIMESTAMP_TZ on '... UTC' strings; native resolver rejecting uuid5 flows)."""

from __future__ import annotations

import asyncio
import sys
import types
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lfx_snowflake.memories_lf_mem import MEMORY_PRESETS, LfMemSpec, iso_timestamp
from lfx_snowflake.memories_lf_mem_ingest import LfMemIngestionPipeline, install_snowflake_auto_capture
from test_v086_passo7_ingestion import MB_ID, FakeSnowflake, _spec, _vals

ISO = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00$"


@pytest.mark.parametrize("raw, want", [
    ("2026-11-11 19:40:12.123456 UTC", "2026-11-11T19:40:12.123456+00:00"),
    ("2026-11-11T19:40:12Z", "2026-11-11T19:40:12.000000+00:00"),
    ("2026-11-11 19:40:12", "2026-11-11T19:40:12.000000+00:00"),
    ("2026-11-11T21:40:12+02:00", "2026-11-11T19:40:12.000000+00:00"),
    (datetime(2026, 11, 11, 19, 40, 12), "2026-11-11T19:40:12.000000+00:00"),
    (datetime(2026, 11, 11, 19, 40, 12, tzinfo=timezone.utc), "2026-11-11T19:40:12.000000+00:00"),
])
def test_iso_timestamp_normalizes(raw, want):
    assert iso_timestamp(raw) == want


def test_iso_timestamp_invalid():
    assert iso_timestamp("garbage", default_now=False) is None and iso_timestamp(None, default_now=False) is None
    import re
    assert re.match(ISO, iso_timestamp("garbage"))


def _langflow_msgs(*texts):
    return [{"message_id": f"m{i}", "sender": "User", "sender_name": "U", "text": t,
             "created_at": f"2026-11-11 19:40:{i:02d}.123456 UTC"} for i, t in enumerate(texts)]


def _merge_ts(sf):
    import re
    merge = [c for c in sf.calls if c["sql"].startswith("MERGE INTO")]
    assert merge and "TO_TIMESTAMP_TZ(?)" in merge[0]["sql"]
    ts = [v for c in merge for v in c["vals"][6::7]]
    assert ts and all(re.match(ISO, t) for t in ts), ts
    return ts


@pytest.mark.parametrize("preset", list(MEMORY_PRESETS))
def test_langflow_utc_timestamps_ingest_rows_for_every_preset(preset):
    sf = FakeSnowflake()
    pipe = LfMemIngestionPipeline(sf, background=False)
    spec = _spec()
    cfg = MEMORY_PRESETS[preset]
    pipe.submit_turn(spec, flow_id="f", session_id="s", messages=_langflow_msgs("Preferisco l'italiano"), threshold=2)
    assert pipe.status(MB_ID)["state"] == "PENDING" and sf.chunks == []
    pipe.submit_turn(spec, flow_id="f", session_id="s", messages=_langflow_msgs("Preferisco l'italiano", "EUR"),
                     threshold=2, preprocessing=True, preproc_model="mistral-large2",
                     preproc_instructions=cfg["preproc_instructions"], kill_phrase="NO_INGEST")
    assert _merge_ts(sf)[0] == "2026-11-11T19:40:00.123456+00:00"
    st = pipe.status(MB_ID)
    assert st["state"] == "INGESTED" and st["ingested"] == 1 and len(sf.chunks) == 1 and not sf.chunks[0]["deleted"]
    assert sf.chunks[0]["metadata"]["source_last_created_at"] == "2026-11-11T19:40:01.123456+00:00"
    pend = [c for c in sf.calls if c["sql"].startswith("SELECT m.message_id")]
    assert all(c["vals"][2] in ("1970-01-01T00:00:00+00:00",) or c["vals"][2].endswith("+00:00") for c in pend)


@pytest.mark.parametrize("preset", list(MEMORY_PRESETS))
def test_no_ingest_respected_for_every_preset(preset):
    sf = FakeSnowflake(complete_response="NO_INGEST")
    pipe = LfMemIngestionPipeline(sf, background=False)
    pipe.submit_turn(_spec(), flow_id="f", session_id="s", messages=_langflow_msgs("ciao"), threshold=1,
                     preprocessing=True, preproc_model="mistral-large2",
                     preproc_instructions=MEMORY_PRESETS[preset]["preproc_instructions"], kill_phrase="NO_INGEST")
    assert pipe.status(MB_ID)["state"] == "SKIPPED_KILL_PHRASE"
    assert [c["deleted"] for c in sf.chunks] == [True]


@pytest.mark.parametrize("preset", list(MEMORY_PRESETS))
def test_recall_returns_scored_rows_for_every_preset(preset, monkeypatch):
    from lfx_snowflake.fastapi_live_bridge import LiveSnowflakeBridge

    bridge = LiveSnowflakeBridge()
    fbs = MEMORY_PRESETS[preset]["filter_by_session"]
    entry = {"name": "MB", "table_fqn": _spec().table_fqn, "lf_mem": _spec().to_dict(), "flow_id": "flow-1",
             "filter_by_session": fbs}
    monkeypatch.setattr(type(bridge), "memories_catalog", property(lambda self: [entry]))
    seen = {}

    def fake_exec(sql, *, bindings=None, mode=None, **_):
        seen.update(sql=sql, vals=_vals(bindings))
        return {"ok": True, "rows": [{"CHUNK_ID": "c", "TEXT": "italiano", "METADATA": "{}", "SESSION_ID": "s",
                                      "SENDER": "User", "SENDER_NAME": "U", "JOB_ID": "j", "INGESTED_AT": "t", "SCORE": 0.81}]}

    monkeypatch.setattr(bridge, "execute_sql", fake_exec)
    res = bridge.recall_memory("MB", query="lingua?", session_id="s")
    assert res["ok"] is True and len(res["rows"]) == 1 and res["rows"][0]["_score"] == pytest.approx(0.81)
    assert ("session_id = ?" in seen["sql"]) is fbs


def _install(monkeypatch, sf, owned):
    ingestion = types.ModuleType("ingestion")
    native = []

    async def native_maybe_trigger(*, mb, **_):
        native.append(mb)
        raise PermissionError("Flow not found")  # upstream MemoryBaseFlowNotFoundError for uuid5 flow ids

    ingestion._maybe_trigger = native_maybe_trigger
    install_snowflake_auto_capture(ingestion)
    fake_memory = types.ModuleType("langflow.memory")

    async def aget_messages(**kw):
        return [SimpleNamespace(id=uuid.UUID(int=7), sender="User", sender_name="U", text="Preferisco l'italiano",
                                timestamp="2026-11-11 19:40:12.123456 UTC")]

    fake_memory.aget_messages = aget_messages
    monkeypatch.setitem(sys.modules, "langflow", types.ModuleType("langflow"))
    monkeypatch.setitem(sys.modules, "langflow.memory", fake_memory)
    import lfx_snowflake.memories_lf_mem_ingest as ing

    monkeypatch.setattr(ing, "PIPELINE", LfMemIngestionPipeline(sf, background=False))
    monkeypatch.setattr(ing, "_owns_flow", owned)
    return ingestion, native, ing


def test_auto_capture_uuid5_flow_bypasses_native_resolver_and_ingests(monkeypatch):
    sf = FakeSnowflake()
    flow_id, user_id = uuid.uuid5(uuid.NAMESPACE_DNS, "flow:x"), uuid.uuid4()
    assert flow_id.version == 5  # upstream is_virtual_flow_id() would reject this existing flow

    async def owns(mb):
        return mb.flow_id == flow_id and mb.user_id == user_id

    ingestion, native, ing = _install(monkeypatch, sf, owns)
    mb = SimpleNamespace(kb_name=_spec().to_kb_name(), flow_id=flow_id, user_id=user_id, threshold=1, preprocessing=False,
                         preproc_model=None, preproc_instructions=None, preproc_kill_phrase="NO_INGEST", id="mb")
    asyncio.run(ingestion._maybe_trigger(mb=mb, session_id="s1", job_id=None, get_or_create_session=None))
    assert native == [] and ing.PIPELINE.status(MB_ID)["state"] == "INGESTED"
    assert [c["text"] for c in sf.chunks] == ["Preferisco l'italiano"]
    assert _merge_ts(sf) == ["2026-11-11T19:40:12.123456+00:00"]


def test_auto_capture_fails_closed_when_flow_not_owned(monkeypatch):
    sf = FakeSnowflake()

    async def not_owned(mb):
        return False

    ingestion, native, ing = _install(monkeypatch, sf, not_owned)
    mb = SimpleNamespace(kb_name=_spec().to_kb_name(), flow_id=uuid.uuid4(), user_id=uuid.uuid4(), threshold=1, id="mb")
    asyncio.run(ingestion._maybe_trigger(mb=mb, session_id="s1", job_id=None, get_or_create_session=None))
    assert native == [] and sf.calls == [] and sf.chunks == []


def test_owns_flow_rejects_unparseable_ids():
    from lfx_snowflake.memories_lf_mem_ingest import _owns_flow

    assert asyncio.run(_owns_flow(SimpleNamespace(flow_id="not-a-uuid", user_id=uuid.uuid4()))) is False
    assert asyncio.run(_owns_flow(SimpleNamespace(flow_id=uuid.uuid4(), user_id=None))) is False


def test_stale_guard_installs_auto_capture(monkeypatch):
    import lfx_snowflake.stale_flow_guard as sg

    mod = types.ModuleType("langflow.services.memory_base.ingestion")

    async def native(**_):
        return None

    mod._maybe_trigger = native
    for name in ("langflow", "langflow.services", "langflow.services.memory_base"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, "langflow.services.memory_base.ingestion", mod)
    sys.modules["langflow.services.memory_base"].ingestion = mod
    assert sg._install_lf_mem_auto_capture() is True and mod._snowflake_lf_mem_capture is True


def test_governed_message_history_binds_iso_timestamp():
    import re
    from lfx_snowflake.components import governed_message_history as gmh

    assert re.match(ISO, gmh.iso_timestamp(datetime.now(timezone.utc)))
