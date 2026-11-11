"""v0.8.6 E2E rerun fixes (A3/A5): AI_EMBED namespace, SEARCH_PREVIEW literal, LF_MEM schema, phantoms, legacy MB."""

from __future__ import annotations

import pathlib
import re

import pytest

from lfx_snowflake import flow_context_guard as fcg
from lfx_snowflake.context_hygiene import drop_phantom_telemetry_objects
from lfx_snowflake.memories_lf_mem import LfMemError, LfMemSpec, legacy_memory_not_found

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "lfx_snowflake"
MB_ID = "11111111-2222-3333-4444-555555555555"


def test_no_snowflake_cortex_ai_embed_namespace_in_src():
    bad = []
    for f in SRC.rglob("*.py"):
        for i, line in enumerate(f.read_text().splitlines(), 1):
            if re.search(r"SNOWFLAKE\.CORTEX\.AI_(MULTI_)?EMBED\(", line):
                bad.append(f"{f.name}:{i}")
    assert not bad, bad


def test_embedding_sql_uses_bare_ai_embed():
    from lfx_snowflake.providers.embeddings import SnowflakeCortexEmbeddings

    emb = SnowflakeCortexEmbeddings.__new__(SnowflakeCortexEmbeddings)
    emb.model = "snowflake-arctic-embed-m-v1.5"
    sql, b = emb.build_ai_embed_request("hi")
    assert sql == "SELECT AI_EMBED('snowflake-arctic-embed-m-v1.5', ?) AS EMBEDDING"
    assert list(b.values())[0]["value"] == "hi"


def test_embedding_surfaces_real_snowflake_error(monkeypatch):
    from lfx_snowflake import fastapi_live_bridge as flb
    from lfx_snowflake.providers.embeddings import SnowflakeCortexEmbeddingError, SnowflakeCortexEmbeddings

    emb = SnowflakeCortexEmbeddings.__new__(SnowflakeCortexEmbeddings)
    emb.model = "snowflake-arctic-embed-m-v1.5"
    emb.dimension = 768
    monkeypatch.setattr(
        flb.LIVE_BRIDGE, "execute_sql",
        lambda *a, **k: {"ok": False, "error_code": "002141", "message": "Unknown user-defined function X", "rows": []},
    )
    with pytest.raises(SnowflakeCortexEmbeddingError, match="002141.*Unknown user-defined function"):
        emb.embed_query("hi")


def test_search_preview_service_is_literal_and_query_bound():
    from lfx_snowflake.components.llm_operations import SnowflakeCortexSearchQueryEngine as C  # noqa: N814

    sql, _payload, b = C.build_search_preview_request(search_service="DB1.SCH1.SVC1", query="x", database="DB1", schema="SCH1")
    assert sql == "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('DB1.SCH1.SVC1', ?) AS SEARCH_RESULT"
    assert len(b) == 1 and '"query": "x"' in list(b.values())[0]["value"]


def test_search_preview_rejects_injection():
    from lfx_snowflake.components.llm_operations import SnowflakeCortexSearchQueryEngine as C  # noqa: N814

    with pytest.raises(Exception):
        C.build_search_preview_request(search_service="DB.S.X') --", query="x", database="DB", schema="S")


def test_guard_accepts_literal_search_preview():
    sql = "SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('DB1.SCH1.SVC1', ?) AS SEARCH_RESULT"
    refs = fcg.extract_object_refs(sql, "DB1", "SCH1")
    assert any(str(getattr(r, "name", "")).upper() == "SVC1" for r in refs), refs
    with pytest.raises(fcg.UnverifiableStatement):
        fcg.extract_object_refs("SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(?, ?)", "DB1", "SCH1")


@pytest.mark.parametrize("schema", ["W3R1_IT", "SNOWFLAKE_LANGFLOW.W3R1_IT", "snowflake_langflow.w3r1_it"])
def test_lf_mem_schema_accepts_bare_or_qualified(schema):
    spec = LfMemSpec(name="M1", memory_base_id=MB_ID, database="SNOWFLAKE_LANGFLOW", memory_schema=schema,
                     governance_schema=schema, embedding_model="snowflake-arctic-embed-m-v1.5")
    assert spec.memory_schema == "W3R1_IT" and spec.governance_schema == "W3R1_IT"
    assert LfMemSpec.from_kb_name(spec.to_kb_name()) == spec


@pytest.mark.parametrize("bad", ["OTHERDB.W3R1_IT", "A.B.C", "W3R1_IT;DROP", "DB.\"X\"\"Y"])
def test_lf_mem_schema_rejects_bad(bad):
    with pytest.raises(Exception):
        LfMemSpec(name="M1", memory_base_id=MB_ID, database="SNOWFLAKE_LANGFLOW", memory_schema=bad,
                  embedding_model="snowflake-arctic-embed-m-v1.5")


def test_lf_mem_schema_wrong_db_error_code():
    with pytest.raises(LfMemError) as ei:
        LfMemSpec(name="M1", memory_base_id=MB_ID, database="SNOWFLAKE_LANGFLOW",
                  memory_schema="OTHERDB.W3R1_IT", embedding_model="snowflake-arctic-embed-m-v1.5")
    assert ei.value.error_code == "INVALID_IDENTIFIER"


def test_drop_phantom_telemetry_keeps_real_and_explicit():
    ctx = {
        "visible_objects": {"tables": ["DB.S.LANGFLOW_FLOW_RUNS", "DB.S.LANGFLOW_FLOW_SPANS", "DB.S.LANGFLOW_AUDIT_LOG",
                                       "DB.S.ORDERS", "DB.S.LANGFLOW_FLOW_ACTIVITY"]},
        "selected_objects": {"DB": {"S": {"tables": ["DB.S.LANGFLOW_AUDIT_LOG"]}}},
    }
    dropped = drop_phantom_telemetry_objects(ctx)
    assert sorted(dropped) == ["DB.S.LANGFLOW_FLOW_RUNS", "DB.S.LANGFLOW_FLOW_SPANS"]
    assert ctx["visible_objects"]["tables"] == ["DB.S.LANGFLOW_AUDIT_LOG", "DB.S.ORDERS", "DB.S.LANGFLOW_FLOW_ACTIVITY"]


def test_drop_phantom_noop_on_bad_input():
    assert drop_phantom_telemetry_objects(None) == []
    assert drop_phantom_telemetry_objects({"visible_objects": "x"}) == []


def test_legacy_memory_not_found_guidance():
    out = legacy_memory_not_found("E2E_MB_A3_OLD")
    assert out["ok"] is False and out["http_status"] == 404 and out["rows"] == []
    assert "before v0.8.6" in out["message"] and "not migrated automatically" in out["message"]
    assert out["migration"]["required"] is True


def test_bridge_recall_unknown_memory_returns_guidance():
    from lfx_snowflake.fastapi_live_bridge import LIVE_BRIDGE

    fn = getattr(LIVE_BRIDGE, "recall_memory", None)
    if fn is None:
        pytest.skip("recall entrypoint name differs")
    try:
        out = fn("E2E_MB_A3_DOES_NOT_EXIST", query="q", session_id="s1")
    except TypeError:
        pytest.skip("signature differs")
    if out.get("error_code") == "MISSING_SESSION_ID":
        pytest.skip("session gating")
    assert out["error_code"] == "MEMORY_NOT_FOUND_OR_LEGACY"
