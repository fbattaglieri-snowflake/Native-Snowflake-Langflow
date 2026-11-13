from __future__ import annotations

import ast
import json
from typing import Any
import sqlite3
from pathlib import Path

import pytest

from lfx_snowflake.bundle_manifest import SNOWFLAKE_BUNDLE_COMPONENTS, get_bundle_manifest
from lfx_snowflake.components.ai_function import (
    SUPPORTED_AI_COMPLETE_MODELS,
    SUPPORTED_AI_REDACT_CATEGORIES,
    TASK_SPECIFIC_AI_FUNCTIONS,
    SnowflakeAIFunctionComponent as DomainAIFunctionEngine,
    SnowflakeAIRedactEngine,
)
from lfx_snowflake.components.llm_operations import (
    SUPPORTED_CUSTOM_TOOL_RESOURCE_TYPES,
    SUPPORTED_DECISION_MODELS,
    SUPPORTED_DECISION_QUESTION_TYPES,
    SnowflakeCustomToolEngine,
    SnowflakeDecisionEngine,
)
from lfx_snowflake.context_helpers import (
    INFRASTRUCTURE_OBJECT_FAMILIES,
    _can_create_database,
    _can_create_schema,
    _ddl_succeeded,
    _register_created_object,
    _resolve_flow_id,
)
from lfx_snowflake.fastapi_live_bridge import (
    LIVE_BRIDGE,
    LiveSnowflakeBridge,
    _should_seed_showcase_flow,
    heal_sqlite_invalid_uuid_rows,
)
from lfx_snowflake.governance.provisioner import SnowflakeObjectProvisioner
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_complete import (
    SnowflakeAICompleteComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_function import (
    SnowflakeAIFunctionComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_ai_redact import (
    SnowflakeAIRedactComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_custom_tool import (
    SnowflakeCustomToolComponent,
)
from lfx_snowflake.native_category_components.llm_operations.snowflake_decision import (
    SnowflakeDecisionComponent,
)
from lfx_snowflake.showcase_full_memory_agent import SHOWCASE_FLOW_UUID


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_w2_r9_01_custom_tool_snowsight_alignment_and_object_rejection() -> None:
    """Verify W2-R9-01: [ ❄️ ] Custom Tool replaces Stored Procedure and Function (UDF/UDTF) with 1:1 Snowsight parity."""
    comp_dir = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components" / "llm_operations"
    assert not (comp_dir / "snowflake_stored_procedure.py").exists()
    assert not (comp_dir / "snowflake_function_udf.py").exists()
    ct_path = comp_dir / "snowflake_custom_tool.py"
    assert ct_path.exists()

    tree = ast.parse(ct_path.read_text(encoding="utf-8"))
    top_classes = [n for n in tree.body if isinstance(n, ast.ClassDef)]
    top_tries = [n for n in tree.body if isinstance(n, ast.Try)]
    assert len(top_classes) == 1
    assert len(top_tries) == 0

    comp = SnowflakeCustomToolComponent()
    assert comp.display_name == "Custom Tool"
    assert comp.name == "SnowflakeCustomTool"
    assert comp.icon == "Snowflake"
    assert SUPPORTED_CUSTOM_TOOL_RESOURCE_TYPES == ("procedure", "function", "table_function")

    # Verify parameter auto-discovery, sorting, manual add/delete, and OBJECT type rejection
    params = SnowflakeCustomToolEngine.parse_signature_parameters(
        "SNOWFLAKE_LANGFLOW.CORE.SEND_EMAIL_ALL(RECIPIENT_EMAIL VARCHAR, SUBJECT VARCHAR, PRIORITY NUMBER)"
    )
    assert len(params) == 3
    alpha_sorted = SnowflakeCustomToolEngine.sort_parameters(params, sort_by="Alphabetical")
    assert [p["name"] for p in alpha_sorted] == ["PRIORITY", "RECIPIENT_EMAIL", "SUBJECT"]  # PASSO6: Snowflake case kept

    comp.parameters_json = json.dumps(params)  # PASSO6: no SEND_EMAIL_ALL default any more
    comp.add_parameter(name="cc_list", param_type="array", description="Optional CC list", required=False)
    req_first = SnowflakeCustomToolEngine.sort_parameters(comp._parsed_parameters(), sort_by="Required first")
    assert req_first[0]["required"] is True
    assert req_first[-1]["name"] == "cc_list"
    comp.delete_parameter("cc_list")

    with pytest.raises(Exception, match="parameter of type object"):
        comp.add_parameter(name="bad_obj", param_type="object", description="Unsupported object param")

    # User's default warehouse omits 'warehouse' key in execution_environment
    comp.warehouse_mode = "User's default"
    comp.query_timeout = 120
    payload_default = comp.build_tool_payload()
    assert payload_default["tool_spec"]["type"] == "generic"
    tname = payload_default["tool_spec"]["name"]
    assert "warehouse" not in payload_default["tool_resources"][tname]["execution_environment"]
    assert payload_default["tool_resources"][tname]["execution_environment"]["query_timeout"] == 120

    # Custom warehouse includes 'warehouse' key in execution_environment
    comp.warehouse_mode = "Custom"
    comp.custom_warehouse = "COMPUTE_WH"
    payload_custom = comp.build_tool_payload()
    assert payload_custom["tool_resources"][tname]["execution_environment"]["warehouse"] == "COMPUTE_WH"

    # Invocation SQL for procedure, function, and table_function
    comp.resource_type = "procedure"
    assert comp.build_invocation_sql({"recipient_email": "a@b.com", "subject": "s", "priority": 1}).startswith("CALL ")
    comp.resource_type = "function"
    comp.custom_tool_identifier = "SNOWFLAKE_LANGFLOW.CORE.CALC_TAX(NUMBER)"
    comp.parameters_json = ""
    assert "SELECT SNOWFLAKE_LANGFLOW.CORE.CALC_TAX(" in comp.build_invocation_sql({"arg_1": 100})
    comp.resource_type = "table_function"
    comp.custom_tool_identifier = "SNOWFLAKE_LANGFLOW.CORE.SPLIT_ROWS_UDTF(VARCHAR)"
    assert "FROM TABLE(SNOWFLAKE_LANGFLOW.CORE.SPLIT_ROWS_UDTF(" in comp.build_invocation_sql({"arg_1": "abc"})


def test_w2_r9_02_decision_official_ai_complete_models_and_question_types(monkeypatch) -> None:
    """Verify W2-R9-02: [ ❄️ ] Decision uses snowflake-decision-fast & snowflake-decision with choice, score (0..n-1), and noul."""
    assert SUPPORTED_DECISION_MODELS == ("snowflake-decision-fast", "snowflake-decision")
    assert SUPPORTED_DECISION_QUESTION_TYPES == ("choice", "score", "noul")

    # Normalized confidence formula: (n * p_max - 1) / (n - 1)
    assert pytest.approx(SnowflakeDecisionEngine.compute_confidence([0.85, 0.10, 0.05])) == (3 * 0.85 - 1) / 2
    assert pytest.approx(SnowflakeDecisionEngine.compute_confidence([0.25, 0.25, 0.25, 0.25])) == 0.0

    # Multi-question evaluation across choice, score (0..n-1), and noul
    res = SnowflakeDecisionEngine.evaluate_decision(
        model="snowflake-decision-fast",
        state="Customer reports a production billing outage and is very angry.",
        questions={
            "department": {
                "type": "choice",
                "instructions": "Which team should handle this?",
                "criteria": {
                    "billing": "Payment or invoice issues",
                    "technical_support": "Outages or product bugs",
                },
            },
            "frustration": {
                "type": "score",
                "instructions": "How frustrated is the customer?",
                "criteria": ["Calm", "Frustrated", "Very angry"],
            },
            "is_urgent": {
                "type": "noul",
                "instructions": "Does the message convey urgency?",
                "criteria": {
                    "true": "Production outage or blocker",
                    "false": "Routine inquiry",
                },
            },
        },
        include_none_of_these=True,
    )
    sql = res["sql_preview"]
    assert "AI_COMPLETE(" in sql
    assert "model => 'snowflake-decision-fast'" in sql
    assert "TO_JSON(" in sql
    assert "return_error_details => TRUE" in sql
    for forbidden in ("model_parameters", "response_format", "streaming", "guardrails", "temperature", "top_p"):
        assert forbidden not in sql

    answers = res["answers"]
    assert "choice" in answers["department"]
    assert "none_of_these" in answers["department"]["probabilities"]
    assert pytest.approx(sum(answers["department"]["probabilities"].values()), rel=1e-4) == 1.0

    # Score is 0-indexed (0..n-1) weighted average with legend {"0": ..., "1": ..., "2": ...}
    frust = answers["frustration"]
    assert frust["legend"] == {"0": "Calm", "1": "Frustrated", "2": "Very angry"}
    probs = frust["probabilities"]
    expected_weighted = sum(int(k) * float(v) for k, v in probs.items())
    assert pytest.approx(frust["score"], rel=1e-5) == expected_weighted

    # Noul is in ~0.08..0.92
    assert 0.08 <= answers["is_urgent"]["noul"] <= 0.92

    # Enforce noul_threshold <= 0.92 and choice option limits (16 for fast, 32 for standard)
    with pytest.raises(ValueError, match="0.92"):
        SnowflakeDecisionEngine.evaluate_decision(
            model="snowflake-decision-fast",
            state="test",
            questions={"q1": {"type": "noul", "instructions": "urgent?"}},
            noul_threshold=0.95,
        )

    too_many_fast = {f"opt_{i}": f"Option {i}" for i in range(17)}
    with pytest.raises(ValueError, match="16"):
        SnowflakeDecisionEngine.evaluate_decision(
            model="snowflake-decision-fast",
            state="test",
            questions={"q1": {"type": "choice", "instructions": "pick", "criteria": too_many_fast}},
            include_none_of_these=False,
        )

    # Component outputs: answers come from the (mocked) real AI_COMPLETE result, not a heuristic.
    import lfx_snowflake.native_category_components.llm_operations.snowflake_decision as dec_mod

    live_cell = json.dumps({"value": {"answers": {"department": {"type": "choice", "choice": "billing",
        "confidence": 0.97, "probabilities": {"billing": 0.99, "technical_support": 0.01}}}}, "error": None})
    calls: list[str] = []

    def _fake_exec(sql: str, **_k: Any) -> dict[str, Any]:
        calls.append(sql)
        return {"ok": True, "rows": [{"DECISION": live_cell}]}

    monkeypatch.setattr(dec_mod, "_flow_component_execute_sql", _fake_exec)
    comp = SnowflakeDecisionComponent()
    assert len(comp.outputs) == 5
    summary = type(comp).summarize_answers({
        "rating": {"type": "score", "score": 3.4, "confidence": 0.61, "legend": {"0": "bad", "1": "poor", "2": "fair", "3": "good", "4": "best"}},
        "path": {"type": "choice", "choice": "refund", "confidence": 0.9},
        "routine": {"type": "noul", "noul": 0.84},
    })
    assert summary.splitlines() == [
        'rating: 3.40 out of 4 - closest level: "good" (confidence 0.61)',
        "path: refund (confidence 0.90)",
        "routine: probability 0.84",
    ]
    out_data = comp.run_decision()
    assert out_data.data["answers"]["department"]["choice"] == "billing"
    assert out_data.data["answers_source"] == "snowflake_ai_complete"
    comp.evaluation_mode = "Table-Level Processing (Database.Schema.Table)"
    df_out = comp.run_table_decisions()
    assert len(df_out) >= 1


def test_w2_r9_03_object_provisioner_removed_from_workflow_nodes_and_context_helpers() -> None:
    """Verify W2-R9-03: Object Provisioner is removed from workflow node categories while context_helpers and governance service remain intact."""
    comp_dir = REPO_ROOT / "src" / "lfx_snowflake" / "native_category_components"
    assert not (comp_dir / "llm_operations" / "snowflake_object_provisioner.py").exists()
    assert not (comp_dir / "files_and_knowledge" / "snowflake_object_provisioner_fk.py").exists()
    assert not (comp_dir / "data_source" / "snowflake_object_provisioner.py").exists()

    assert INFRASTRUCTURE_OBJECT_FAMILIES == ("table", "view", "stage", "postgres")
    assert callable(_resolve_flow_id)
    assert _ddl_succeeded({"ok": True}) is True
    assert callable(_register_created_object)
    assert isinstance(_can_create_database(), bool)
    assert isinstance(_can_create_schema(), bool)
    assert hasattr(SnowflakeObjectProvisioner, "enforce_hub_object_exclusion")


def test_w2_r9_04_ai_redact_separation_and_multimodal_stage_bucket_support() -> None:
    """Verify W2-R9-04: [ ❄️ ] AI Redact separation (13 PII categories, 4,096-token limit, allowlist splicing) and Stage/Bucket multimodal TO_FILE/DIRECTORY support."""
    assert len(SUPPORTED_AI_REDACT_CATEGORIES) == 13
    assert "US_SSN" not in SUPPORTED_AI_REDACT_CATEGORIES
    assert "CREDIT_CARD" not in SUPPORTED_AI_REDACT_CATEGORIES
    assert "NATIONAL_ID" in SUPPORTED_AI_REDACT_CATEGORIES
    assert "PAYMENT_CARD_DATA" in SUPPORTED_AI_REDACT_CATEGORIES

    assert len(TASK_SPECIFIC_AI_FUNCTIONS) == 14
    assert "AI_REDACT" not in TASK_SPECIFIC_AI_FUNCTIONS
    assert "AI_COMPLETE" not in TASK_SPECIFIC_AI_FUNCTIONS
    for fn_name in (
        "AI_CLASSIFY",
        "AI_FILTER",
        "AI_EXTRACT",
        "AI_SENTIMENT",
        "AI_SUMMARIZE",
        "AI_SUMMARIZE_AGG",
        "AI_AGG",
        "AI_TRANSLATE",
        "AI_SIMILARITY",
        "AI_TRANSCRIBE",
        "AI_PARSE_DOCUMENT",
        "AI_EMBED",
        "AI_MULTI_EMBED",
        "AI_COUNT_TOKENS",
    ):
        assert fn_name in TASK_SPECIFIC_AI_FUNCTIONS

    for gemini_model in ("gemini-3.1-pro", "gemini-3.1-flash-lite", "gemini-3.5-flash"):
        assert gemini_model in SUPPORTED_AI_COMPLETE_MODELS

    # Reverse-index span splicing in detect_with_allowlist
    sample_text = "Contact Alice Smith at alice@example.com or Bob Jones at bob@example.com."
    spans = [
        {"category": "NAME", "start": 8, "end": 19, "text": "Alice Smith"},
        {"category": "EMAIL", "start": 23, "end": 40, "text": "alice@example.com"},
        {"category": "NAME", "start": 44, "end": 53, "text": "Bob Jones"},
        {"category": "EMAIL", "start": 57, "end": 72, "text": "bob@example.com"},
    ]
    spliced = SnowflakeAIRedactEngine.apply_allowlist_splicing(
        input_text=sample_text,
        spans=spans,
        allowlist="Alice Smith, alice@example.com",
    )
    assert "Alice Smith" in spliced["redacted_text"]
    assert "alice@example.com" in spliced["redacted_text"]
    assert "[NAME]" in spliced["redacted_text"]
    assert "[EMAIL]" in spliced["redacted_text"]
    assert "Bob Jones" not in spliced["redacted_text"]

    # 4,096-token limit & SPLIT_TEXT_RECURSIVE_CHARACTER auto-chunking
    plan_chunked = SnowflakeAIRedactEngine.build_sql(
        input_text="A" * 18000,
        mode="redact",
        categories="NAME,EMAIL",
        return_error_details=True,
        auto_chunk_long_text=True,
        chunk_size=1000,
    )
    assert plan_chunked["exceeds_4096_limit"] is True
    assert plan_chunked["chunked"] is True
    assert "SPLIT_TEXT_RECURSIVE_CHARACTER" in plan_chunked["sql"]
    assert "LISTAGG" in plan_chunked["sql"]
    assert "AI_SQL_ERROR_HANDLING_USE_FAIL_ON_ERROR = FALSE" in plan_chunked["session_setup_sql"]

    # Stage pre-conversion (AI_PARSE_DOCUMENT -> AI_REDACT and AI_TRANSCRIBE -> AI_REDACT)
    red_comp = SnowflakeAIRedactComponent()
    red_comp.source_mode = "Stage Document (AI_PARSE_DOCUMENT -> AI_REDACT)"
    red_comp.stage_selector = "@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE"
    red_comp.stage_file_path = "contracts/nda.pdf"
    doc_sql = red_comp._build_sql()
    assert "AI_PARSE_DOCUMENT(TO_FILE('@SNOWFLAKE_LANGFLOW.CORE.SNOWFLAKE_LANGFLOW_STAGE', 'contracts/nda.pdf')" in doc_sql
    assert "AI_REDACT(" in doc_sql

    # Multimodal TO_FILE & DIRECTORY(@stage) in AI Function and AI Complete (including .mp3 audio)
    aif_comp = SnowflakeAIFunctionComponent()
    aif_comp.ai_function = "AI_TRANSCRIBE"
    aif_comp.stage_selector = "@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE"
    aif_comp.stage_file_path = "audio/customer_call_recording.mp3"
    trans_sql = aif_comp._build_sql()
    assert "TO_FILE('@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE', 'audio/customer_call_recording.mp3')" in trans_sql

    aif_comp.batch_directory_mode = True
    batch_sql = aif_comp._build_sql()
    assert "DIRECTORY(@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE)" in batch_sql

    aic_comp = SnowflakeAICompleteComponent()
    aic_comp.model = "gemini-3.5-flash"
    aic_comp.stage_selector = "@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE"
    aic_comp.stage_file_path = "audio/customer_call_recording.mp3"
    aic_sql = aic_comp._build_sql()
    assert "TO_FILE('@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE', 'audio/customer_call_recording.mp3')" in aic_sql


def test_w2_r9_05_and_06_transient_overlay_dismiss_and_clean_flow_persistence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify W2-R9-05 (dismissTransientSnowflakeOverlays) and W2-R9-06 (no unsolicited showcase/backup flow seeding)."""
    ui_js = (REPO_ROOT / "frontend_patches" / "snowflake-ui-integration.js").read_text(encoding="utf-8")
    assert "function dismissTransientSnowflakeOverlays(reason)" in ui_js
    assert "window.dismissTransientSnowflakeOverlays = dismissTransientSnowflakeOverlays;" in ui_js
    assert "window.closeSnowflakeMemoryTypeDropdown = closeSnowflakeMemoryTypeDropdown;" in ui_js
    assert 'existing.style.display !== "none"' in ui_js
    assert 'dismissTransientSnowflakeOverlays("rail-tab-switch")' in ui_js

    # Ensure closeSnowflakeMemoryTypeDropdown() runs BEFORE `if (nativeReactBtn)` early return in ensureMemoriesPageSnowflakeButton
    fn_start = ui_js.index("function ensureMemoriesPageSnowflakeButton()")
    fn_slice = ui_js[fn_start : fn_start + 3500]
    assert fn_slice.index("closeSnowflakeMemoryTypeDropdown()") < fn_slice.index("if (nativeReactBtn)")

    # W2-R9-06: By default SNOWFLAKE_SEED_SHOWCASE_FLOW is unset -> _should_seed_showcase_flow() is False
    monkeypatch.delenv("SNOWFLAKE_SEED_SHOWCASE_FLOW", raising=False)
    assert _should_seed_showcase_flow() is False

    bridge = LiveSnowflakeBridge()
    boot_skipped = bridge.bootstrap_full_memory_showcase_flow(explicit_user_request=False)
    assert boot_skipped["skipped_unsolicited_seed"] is True
    assert boot_skipped["flow_persisted_sqlite"] is False

    # Verify heal_sqlite_invalid_uuid_rows purges unsolicited New Flow (Backup) and unrequested Showcase flow while keeping user flows
    db_file = tmp_path / "langflow.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE flow (id TEXT PRIMARY KEY, name TEXT, user_id TEXT, folder_id TEXT, data TEXT)")
    user_flow_id = "11111111-2222-4333-8444-555555555555"
    conn.execute(
        "INSERT INTO flow (id, name, user_id, folder_id, data) VALUES (?, ?, ?, ?, ?)",
        (user_flow_id, "My Production Flow", None, None, '{"nodes":[]}'),
    )
    conn.execute(
        "INSERT INTO flow (id, name, user_id, folder_id, data) VALUES (?, ?, ?, ?, ?)",
        ("22222222-3333-4444-8555-666666666666", "New Flow (Backup)", None, None, '{"nodes":[]}'),
    )
    conn.execute(
        "INSERT INTO flow (id, name, user_id, folder_id, data) VALUES (?, ?, ?, ?, ?)",
        (SHOWCASE_FLOW_UUID, "Snowflake_Full_Memory_Enterprise_Agent", None, None, '{"nodes":[]}'),
    )
    conn.commit()

    LIVE_BRIDGE._showcase_explicitly_bootstrapped = False
    repaired_count = heal_sqlite_invalid_uuid_rows(conn)
    conn.commit()
    assert repaired_count >= 2

    remaining = conn.execute("SELECT id, name FROM flow").fetchall()
    conn.close()
    assert remaining == [(user_flow_id, "My Production Flow")]


def test_w2_r9_live_bridge_custom_tools_stage_files_and_decision_edge_cases() -> None:
    """Verify W2-R9-01/02/04/07 LiveBridge discovery, Decision error mapping & limits, bundle manifest, and public docs."""
    bridge = LiveSnowflakeBridge()

    # W2-R9-01: LiveBridge list_custom_tool_identifiers & describe_custom_tool_signature
    for rtype in ("procedure", "function", "table_function"):
        ids = bridge.list_custom_tool_identifiers(resource_type=rtype, database_schema="Database All")
        assert isinstance(ids, list)  # PASSO6: offline there are no invented identifiers
        ids = ids or ["SNOWFLAKE_LANGFLOW.CORE.TOOL_X(VARCHAR)"]
        desc = bridge.describe_custom_tool_signature(resource_type=rtype, identifier=ids[0])
        assert desc["resource_type"] == rtype
        assert isinstance(desc["parameters"], list)

    with pytest.raises(Exception, match="parameter of type object"):
        SnowflakeCustomToolEngine.parse_signature_parameters(
            "SNOWFLAKE_LANGFLOW.CORE.BAD_PROC(PAYLOAD OBJECT)"
        )

    bad_desc = bridge.describe_custom_tool_signature(
        resource_type="procedure",
        identifier="SNOWFLAKE_LANGFLOW.CORE.BAD_PROC(PAYLOAD OBJECT)",
    )
    assert bad_desc["ok"] is False
    assert bad_desc["error_code"] in ("UNSUPPORTED_CAPABILITY", "ERROR_UNSUPPORTED_CAPABILITY")
    assert "parameter of type object" in bad_desc["message"]

    # W2-R9-02: Decision error mapping (including CORTEX_ENABLED_CROSS_REGION), noul criteria validation, and 256 total options cap
    err_mapped = SnowflakeDecisionEngine.map_decision_error("Error: CORTEX_ENABLED_CROSS_REGION must be ANY_REGION")
    assert err_mapped["code"] == "CORTEX_ENABLED_CROSS_REGION"
    assert "ANY_REGION" in err_mapped["remediation"]

    with pytest.raises(ValueError, match="both 'true' and 'false'"):
        SnowflakeDecisionEngine.evaluate_decision(
            model="snowflake-decision",
            state="test",
            questions={"q_noul": {"type": "noul", "instructions": "check", "criteria": {"true": "only true"}}},
        )

    many_q = {
        f"q_{i}": {
            "type": "choice",
            "instructions": "pick",
            "criteria": {f"opt_{j}": f"Desc {j}" for j in range(10)},
        }
        for i in range(26)
    }
    with pytest.raises(ValueError, match="256"):
        SnowflakeDecisionEngine.evaluate_decision(
            model="snowflake-decision",
            state="test",
            questions=many_q,
            include_none_of_these=False,
        )

    # W2-R9-04: LiveBridge stage files with media_filter & DomainAIFunctionEngine backward compatibility
    orig_sql = bridge.execute_sql
    bridge.execute_sql = lambda *a, **k: {  # type: ignore[method-assign]
        "ok": True,
        "rows": [
            {"name": "external_s3_media_stage/audio/customer_call_recording.mp3", "size": 1024},
            {"name": "external_s3_media_stage/docs/invoice.pdf", "size": 2048},
        ],
    }
    try:
        audio_resp = bridge.list_stage_files(
            "@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE",
            media_filter="audio",
        )
    finally:
        bridge.execute_sql = orig_sql  # type: ignore[method-assign]
    assert audio_resp["ok"] is True
    assert len(audio_resp["files"]) == 1
    assert audio_resp["files"][0]["path"].endswith(".mp3")
    assert "TO_FILE('@SNOWFLAKE_LANGFLOW.CORE.EXTERNAL_S3_MEDIA_STAGE'" in audio_resp["files"][0]["to_file_sql"]

    legacy_redact = DomainAIFunctionEngine(
        active_role="ROLE_ANALYST",
        allowed_roles={"ROLE_ANALYST"},
    ).execute(
        ai_function="AI_REDACT",
        table=[{"id": 1, "comment": "Email jane@example.com"}],
        column_name="comment",
        redact_mode="redact",
        categories=["NAME", "EMAIL"],
    )
    assert "AI_REDACT" in legacy_redact["plan"].execution_sql

    # Bundle manifest has exact 8 LLM Operations components
    llm_ops = [c for c in SNOWFLAKE_BUNDLE_COMPONENTS if c.category == "LLM Operations"]
    assert {c.component_id for c in llm_ops} == {
        "snowflake_custom_tool",
        "snowflake_ai_complete",
        "snowflake_ai_function",
        "snowflake_ai_redact",
        "snowflake_sql_statement",
        "snowflake_agent_memory",
        "snowflake_llm_node",
        "snowflake_decision",
    }
    all_ids = {c.component_id for c in SNOWFLAKE_BUNDLE_COMPONENTS}
    for removed in (
        "snowflake_object_provisioner",
        "snowflake_object_provisioner_fk",
        "snowflake_stored_procedure",
        "snowflake_function_udf",
    ):
        assert removed not in all_ids

    # W2-R9-07: Public documentation files have Mermaid schemas and Markdown tables
    for doc_rel in (
        "README.md",
        "docs/CAPABILITY_MATRIX.md",
        "docs/UPSTREAM_SYNC.md",
        "docs/ADR/ADR-005.md",
        "docs/ADR/ADR-006.md",
    ):
        doc_text = (REPO_ROOT / doc_rel).read_text(encoding="utf-8")
        assert "```mermaid" in doc_text, f"Missing Mermaid schema in {doc_rel}"
        assert "|---" in doc_text or "| ---" in doc_text, f"Missing Markdown table in {doc_rel}"

